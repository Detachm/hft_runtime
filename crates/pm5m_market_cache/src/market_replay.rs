use crate::types::{
    canonical_market_symbol, micros, RawPolymarketClobWsEvent,
    DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS, DEFAULT_POLY_INCREMENTAL_LATENCY_MS,
    WS_RAW_STREAM,
};
use anyhow::{anyhow, bail, Context, Result};
use market_data_etl_core::{
    atomic_write_verified, discover_hftrec4_manifests, for_each_parquet_table_row, fsync_dir,
    hash_path, hash_serializable, scan_hftrec4_segment_selected, sha256_bytes, sha256_file,
    write_json_file_pretty, Hftrec4Record, Hftrec4RecordMeta, Hftrec4SegmentManifest,
    Hftrec4WriteRecord, ParquetTableStreamWriter, HFTREC4_FORMAT,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::mpsc;
use std::sync::OnceLock;
use std::time::Instant;

const HOUR_NS: i64 = 3_600_000_000_000;
const DEFAULT_MARKET_REPLAY_PART_ROWS: usize = 500_000;
const DEFAULT_REFERENCE_LATENCY_MS: i64 = 200;
const DEFAULT_REFERENCE_ORDER_HOLDBACK_MS: i64 = 2_000;
const COMPACT_TYPED_NONE_I64: i64 = i64::MIN;
const COMPACT_TYPED_NONE_U32: u32 = u32::MAX;
const COMPACT_TYPED_META_LEN: usize = 105;

fn market_replay_deep_profile_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("PM5M_MARKET_REPLAY_DEEP_PROFILE").is_some())
}

pub const MARKET_REPLAY_FORMAT: &str = "pm5m.market_replay_dataset.v1";
pub const MARKET_REPLAY_TYPED_UPDATES_FORMAT: &str = "pm5m.market_replay_typed_updates.v1";
pub const MARKET_REPLAY_TYPED_UPDATES_MAGIC: &[u8; 8] = b"PM5MTZ1\n";
pub const MARKET_REPLAY_TYPED_UPDATES_MANIFEST: &str = "manifest.market_replay_typed_updates.json";
pub const MARKET_REPLAY_COMPACT_TYPED_FORMAT: &str = "pm5m.market_replay_compact_typed.v1";
pub const MARKET_REPLAY_COMPACT_TYPED_SCHEMA_HASH: &str =
    "pm5m.compact-typed.local-dicts.fixed-metadata.typed-body.v1";
pub const MARKET_REPLAY_COMPACT_TYPED_MAGIC: &[u8; 8] = b"PM5MTB1\n";
pub const MARKET_REPLAY_COMPACT_TYPED_STREAM: &str = "polymarket_clob_ws_typed";
pub const MARKET_REPLAY_CATALOG: &str = "catalog.market_replay.json";
pub const MARKET_REPLAY_EVENTS_TABLE: &str = "events";
pub const REFERENCE_WS_RAW_STREAM: &str = "reference_ws_raw";
pub const MARKET_REPLAY_SEMANTICS_ID: &str = "pm5m_market_replay_semantics_v1";
pub const MARKET_REPLAY_FILL_MODEL_ID: &str = "delayed_fak_residual_visible_ask_v1";
pub const MARKET_REPLAY_FEE_MODEL_ID: &str = "zero_fee_v1";
pub const MARKET_REPLAY_SETTLEMENT_MODEL_ID: &str = "official_polymarket_labels_v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketReplaySemantics {
    pub schema_version: u32,
    pub semantics_id: String,
    pub poly_incremental_latency_ms: i64,
    pub poly_incremental_freshness_guard_ms: i64,
    pub reference_latency_ms: i64,
    pub submit_latency_ms: i64,
    pub fill_model_id: String,
    pub fee_model_id: String,
    pub settlement_model_id: String,
}

impl Default for MarketReplaySemantics {
    fn default() -> Self {
        Self {
            schema_version: 1,
            semantics_id: MARKET_REPLAY_SEMANTICS_ID.to_string(),
            poly_incremental_latency_ms: DEFAULT_POLY_INCREMENTAL_LATENCY_MS,
            poly_incremental_freshness_guard_ms: DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS,
            reference_latency_ms: DEFAULT_REFERENCE_LATENCY_MS,
            submit_latency_ms: 300,
            fill_model_id: MARKET_REPLAY_FILL_MODEL_ID.to_string(),
            fee_model_id: MARKET_REPLAY_FEE_MODEL_ID.to_string(),
            settlement_model_id: MARKET_REPLAY_SETTLEMENT_MODEL_ID.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildMarketReplayDatasetOptions {
    pub raw_roots: Vec<PathBuf>,
    pub dataset_root: PathBuf,
    pub raw_start_ts_ns: Option<i64>,
    pub raw_end_ts_ns: Option<i64>,
    pub market_symbol_allowlist: Vec<String>,
    pub overwrite: bool,
    #[serde(default)]
    pub poly_server_visible_time: bool,
    #[serde(default = "default_poly_incremental_latency_ms")]
    pub poly_incremental_latency_ms: i64,
    #[serde(default = "default_poly_incremental_freshness_guard_ms")]
    pub poly_incremental_freshness_guard_ms: i64,
    #[serde(default = "default_reference_latency_ms")]
    pub reference_latency_ms: i64,
    #[serde(default)]
    pub max_rows_per_part: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamMarketReplayEventsOptions {
    pub raw_roots: Vec<PathBuf>,
    pub raw_start_ts_ns: Option<i64>,
    pub raw_end_ts_ns: Option<i64>,
    pub market_symbol_allowlist: Vec<String>,
    #[serde(default)]
    pub poly_server_visible_time: bool,
    #[serde(default = "default_poly_incremental_latency_ms")]
    pub poly_incremental_latency_ms: i64,
    #[serde(default = "default_poly_incremental_freshness_guard_ms")]
    pub poly_incremental_freshness_guard_ms: i64,
    #[serde(default = "default_reference_latency_ms")]
    pub reference_latency_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildMarketReplayTypedUpdatesOptions {
    pub raw_roots: Vec<PathBuf>,
    pub output_file: Option<PathBuf>,
    pub output_root: Option<PathBuf>,
    pub raw_start_ts_ns: Option<i64>,
    pub raw_end_ts_ns: Option<i64>,
    pub market_symbol_allowlist: Vec<String>,
    #[serde(default)]
    pub overwrite: bool,
    #[serde(default)]
    pub poly_server_visible_time: bool,
    #[serde(default = "default_poly_incremental_latency_ms")]
    pub poly_incremental_latency_ms: i64,
    #[serde(default = "default_poly_incremental_freshness_guard_ms")]
    pub poly_incremental_freshness_guard_ms: i64,
    #[serde(default = "default_reference_latency_ms")]
    pub reference_latency_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketReplayTypedUpdatesBuildReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub output_file: Option<PathBuf>,
    pub output_root: Option<PathBuf>,
    pub shard_files: Vec<MarketReplayTypedUpdateShardReport>,
    pub row_count: usize,
    pub min_visible_ts_ns: Option<i64>,
    pub max_visible_ts_ns: Option<i64>,
    pub event_type_counts: BTreeMap<String, usize>,
    pub elapsed_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketReplayTypedUpdateShardReport {
    pub path: PathBuf,
    pub symbol: String,
    pub hour_bucket: i64,
    pub row_count: usize,
    pub min_visible_ts_ns: Option<i64>,
    pub max_visible_ts_ns: Option<i64>,
    pub event_type_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildMarketReplayCompactTypedOptions {
    pub raw_roots: Vec<PathBuf>,
    pub output_root: PathBuf,
    pub raw_start_ts_ns: Option<i64>,
    pub raw_end_ts_ns: Option<i64>,
    #[serde(default)]
    pub overwrite: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketReplayCompactTypedBuildReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub output_root: PathBuf,
    pub segment_count: usize,
    pub skipped_empty_segment_count: usize,
    pub row_count: usize,
    pub min_local_recv_ts_ns: Option<i64>,
    pub max_local_recv_ts_ns: Option<i64>,
    pub segment_files: Vec<PathBuf>,
    pub elapsed_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketReplayCompactTypedSegmentManifest {
    pub schema_version: u32,
    pub dataset_format: String,
    pub segment_path: PathBuf,
    pub segment_sha256: String,
    pub segment_bytes: u64,
    pub schema_hash: String,
    pub raw_segment_path: PathBuf,
    pub record_count: usize,
    pub min_local_recv_ts_ns: Option<i64>,
    pub max_local_recv_ts_ns: Option<i64>,
    pub min_ingest_seq: Option<u64>,
    pub max_ingest_seq: Option<u64>,
    pub event_type_counts: BTreeMap<String, usize>,
    pub symbol_count: usize,
    pub condition_count: usize,
    pub asset_count: usize,
    pub metadata_bytes: u64,
    pub body_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ValidateMarketReplayCompactTypedOptions {
    pub raw_roots: Vec<PathBuf>,
    pub compact_typed_roots: Vec<PathBuf>,
    pub raw_start_ts_ns: Option<i64>,
    pub raw_end_ts_ns: Option<i64>,
    pub market_symbol_allowlist: Vec<String>,
    #[serde(default)]
    pub poly_server_visible_time: bool,
    #[serde(default = "default_poly_incremental_latency_ms")]
    pub poly_incremental_latency_ms: i64,
    #[serde(default = "default_poly_incremental_freshness_guard_ms")]
    pub poly_incremental_freshness_guard_ms: i64,
    #[serde(default = "default_reference_latency_ms")]
    pub reference_latency_ms: i64,
    #[serde(default)]
    pub raw_worker_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketReplayCompactTypedValidationReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub matched: bool,
    pub raw_row_count: usize,
    pub compact_row_count: usize,
    pub raw_hash: String,
    pub compact_hash: String,
    pub first_mismatch_index: Option<usize>,
    pub first_raw_row_hash: Option<String>,
    pub first_compact_row_hash: Option<String>,
    pub raw_elapsed_ms: u128,
    pub compact_elapsed_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketReplayStreamReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub row_count: usize,
    pub manifest_count: usize,
    pub min_visible_ts_ns: Option<i64>,
    pub max_visible_ts_ns: Option<i64>,
    pub event_type_counts: BTreeMap<String, usize>,
    pub elapsed_ms: u128,
    #[serde(default)]
    pub profile: MarketReplayStreamProfile,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketReplayStreamProfile {
    pub worker_count: usize,
    pub manifest_count: usize,
    pub raw_filter_row_count: usize,
    pub selected_record_count: usize,
    pub selected_update_count: usize,
    pub emitted_update_count: usize,
    pub typed_fast_decode_count: usize,
    pub typed_slow_fallback_count: usize,
    pub selected_payload_bytes: u64,
    pub worker_wall_ns: u128,
    pub segment_scan_wall_ns: u128,
    pub raw_filter_ns: u128,
    pub raw_update_build_ns: u128,
    pub payload_parse_ns: u128,
    pub typed_decode_ns: u128,
    pub compact_segment_read_ns: u128,
    pub compact_header_read_ns: u128,
    pub compact_metadata_read_ns: u128,
    pub compact_body_read_ns: u128,
    pub compact_record_decode_ns: u128,
    pub compact_record_filter_ns: u128,
    pub compact_pending_push_ns: u128,
    pub segment_sort_ns: u128,
    pub merge_flush_ns: u128,
    pub visit_callback_ns: u128,
}

impl MarketReplayStreamProfile {
    fn add(&mut self, other: &Self) {
        self.selected_update_count = self
            .selected_update_count
            .saturating_add(other.selected_update_count);
        self.typed_fast_decode_count = self
            .typed_fast_decode_count
            .saturating_add(other.typed_fast_decode_count);
        self.typed_slow_fallback_count = self
            .typed_slow_fallback_count
            .saturating_add(other.typed_slow_fallback_count);
        self.raw_filter_row_count = self
            .raw_filter_row_count
            .saturating_add(other.raw_filter_row_count);
        self.selected_record_count = self
            .selected_record_count
            .saturating_add(other.selected_record_count);
        self.emitted_update_count = self
            .emitted_update_count
            .saturating_add(other.emitted_update_count);
        self.selected_payload_bytes = self
            .selected_payload_bytes
            .saturating_add(other.selected_payload_bytes);
        self.worker_wall_ns = self.worker_wall_ns.saturating_add(other.worker_wall_ns);
        self.segment_scan_wall_ns = self
            .segment_scan_wall_ns
            .saturating_add(other.segment_scan_wall_ns);
        self.raw_filter_ns = self.raw_filter_ns.saturating_add(other.raw_filter_ns);
        self.raw_update_build_ns = self
            .raw_update_build_ns
            .saturating_add(other.raw_update_build_ns);
        self.payload_parse_ns = self.payload_parse_ns.saturating_add(other.payload_parse_ns);
        self.typed_decode_ns = self.typed_decode_ns.saturating_add(other.typed_decode_ns);
        self.compact_segment_read_ns = self
            .compact_segment_read_ns
            .saturating_add(other.compact_segment_read_ns);
        self.compact_header_read_ns = self
            .compact_header_read_ns
            .saturating_add(other.compact_header_read_ns);
        self.compact_metadata_read_ns = self
            .compact_metadata_read_ns
            .saturating_add(other.compact_metadata_read_ns);
        self.compact_body_read_ns = self
            .compact_body_read_ns
            .saturating_add(other.compact_body_read_ns);
        self.compact_record_decode_ns = self
            .compact_record_decode_ns
            .saturating_add(other.compact_record_decode_ns);
        self.compact_record_filter_ns = self
            .compact_record_filter_ns
            .saturating_add(other.compact_record_filter_ns);
        self.compact_pending_push_ns = self
            .compact_pending_push_ns
            .saturating_add(other.compact_pending_push_ns);
        self.segment_sort_ns = self.segment_sort_ns.saturating_add(other.segment_sort_ns);
        self.merge_flush_ns = self.merge_flush_ns.saturating_add(other.merge_flush_ns);
        self.visit_callback_ns = self
            .visit_callback_ns
            .saturating_add(other.visit_callback_ns);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketReplayCoverageReport {
    pub schema_version: u32,
    pub raw_start_ts_ns: Option<i64>,
    pub raw_end_ts_ns: Option<i64>,
    pub covered_streams: BTreeMap<String, Vec<i64>>,
    pub missing_stream_buckets: Vec<MarketReplayCoverageGap>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketReplayCoverageGap {
    pub stream_root: PathBuf,
    pub stream: String,
    pub hour_bucket: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketReplayRawUpdate {
    pub schema_version: u32,
    pub dataset_format: String,
    pub global_event_seq: u64,
    pub source: String,
    pub stream: String,
    pub symbol: Option<String>,
    pub horizon_seconds: Option<i64>,
    pub condition_id: Option<String>,
    pub asset_id: Option<String>,
    pub event_type: String,
    pub original_local_recv_ts_ns: i64,
    pub visible_ts_ns: i64,
    pub ingest_seq: u64,
    pub source_row_idx: u64,
    pub source_segment: String,
    pub raw_record_hash: String,
    pub payload_hash: String,
    pub raw: RawPolymarketClobWsEvent,
}

impl From<&StreamMarketReplayEventsOptions> for BuildMarketReplayDatasetOptions {
    fn from(options: &StreamMarketReplayEventsOptions) -> Self {
        Self {
            raw_roots: options.raw_roots.clone(),
            dataset_root: PathBuf::new(),
            raw_start_ts_ns: options.raw_start_ts_ns,
            raw_end_ts_ns: options.raw_end_ts_ns,
            market_symbol_allowlist: options.market_symbol_allowlist.clone(),
            overwrite: false,
            poly_server_visible_time: options.poly_server_visible_time,
            poly_incremental_latency_ms: options.poly_incremental_latency_ms,
            poly_incremental_freshness_guard_ms: options.poly_incremental_freshness_guard_ms,
            reference_latency_ms: options.reference_latency_ms,
            max_rows_per_part: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketEvent {
    pub schema_version: u32,
    pub dataset_format: String,
    pub global_event_seq: u64,
    pub source: String,
    pub venue: String,
    pub stream: String,
    pub symbol: Option<String>,
    pub horizon_seconds: Option<i64>,
    pub condition_id: Option<String>,
    pub asset_id: Option<String>,
    pub event_type: String,
    pub exchange_ts_ns: Option<i64>,
    pub local_recv_ts_ns: i64,
    pub visible_ts_ns: i64,
    pub sequence: u64,
    pub ingest_seq: u64,
    pub source_row_idx: u64,
    pub source_segment: String,
    pub side: Option<String>,
    pub price_micros: Option<i64>,
    pub qty_micros: Option<i64>,
    pub order_id_or_seq: Option<String>,
    pub raw_record_hash: String,
    pub payload_hash: String,
    pub flags: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketReplayDatasetCatalog {
    pub schema_version: u32,
    pub dataset_format: String,
    pub dataset_root: PathBuf,
    pub events_table: String,
    pub raw_roots: Vec<PathBuf>,
    pub raw_start_ts_ns: Option<i64>,
    pub raw_end_ts_ns: Option<i64>,
    pub market_symbol_allowlist: Vec<String>,
    pub poly_server_visible_time: bool,
    pub poly_incremental_latency_ms: i64,
    pub poly_incremental_freshness_guard_ms: i64,
    pub reference_latency_ms: i64,
    pub row_count: usize,
    pub manifest_count: usize,
    pub min_visible_ts_ns: Option<i64>,
    pub max_visible_ts_ns: Option<i64>,
    pub event_type_counts: BTreeMap<String, usize>,
    pub events_table_hash: String,
    pub elapsed_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketReplayDatasetBuildReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub dataset_root: PathBuf,
    pub catalog_path: PathBuf,
    pub reused_existing: bool,
    pub row_count: usize,
    pub manifest_count: usize,
    pub min_visible_ts_ns: Option<i64>,
    pub max_visible_ts_ns: Option<i64>,
    pub event_type_counts: BTreeMap<String, usize>,
    pub elapsed_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketReplayDatasetBenchReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub dataset_root: PathBuf,
    pub row_count: usize,
    pub book_count: usize,
    pub min_visible_ts_ns: Option<i64>,
    pub max_visible_ts_ns: Option<i64>,
    pub elapsed_ms: u128,
    pub rows_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayBookLevel {
    pub price_micros: i64,
    pub qty_micros: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplayBookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketReplayLevelChange {
    pub asset_id: String,
    pub side: ReplayBookSide,
    pub price_micros: i64,
    pub qty_micros: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MarketReplayTypedUpdateBody {
    Book {
        asset_id: String,
        bids: Vec<ReplayBookLevel>,
        asks: Vec<ReplayBookLevel>,
    },
    PriceChanges {
        changes: Vec<MarketReplayLevelChange>,
    },
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketReplayTypedUpdate {
    pub schema_version: u32,
    pub dataset_format: String,
    pub global_event_seq: u64,
    pub symbol: Option<String>,
    pub horizon_seconds: Option<i64>,
    pub condition_id: Option<String>,
    pub event_type: String,
    pub original_local_recv_ts_ns: i64,
    pub visible_ts_ns: i64,
    pub ingest_seq: u64,
    pub source_row_idx: u64,
    pub payload_hash: String,
    pub market_start_ts_ns: Option<i64>,
    pub market_end_ts_ns: Option<i64>,
    pub body: MarketReplayTypedUpdateBody,
}

impl MarketReplayTypedUpdate {
    pub fn affected_asset_ids(&self) -> BTreeSet<String> {
        match &self.body {
            MarketReplayTypedUpdateBody::Book { asset_id, .. } => {
                BTreeSet::from([asset_id.clone()])
            }
            MarketReplayTypedUpdateBody::PriceChanges { changes } => changes
                .iter()
                .map(|change| change.asset_id.clone())
                .collect(),
            MarketReplayTypedUpdateBody::Other => BTreeSet::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketReplayCompactTypedRecord {
    pub raw_segment_path: PathBuf,
    pub key_asset_id: Option<String>,
    pub exchange_ts_ms: Option<i64>,
    pub update: MarketReplayTypedUpdate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CompactTypedSegmentHeader {
    schema_version: u32,
    dataset_format: String,
    schema_hash: String,
    raw_segment_path: PathBuf,
    record_count: u64,
    metadata_len: u64,
    body_len: u64,
    min_local_recv_ts_ns: Option<i64>,
    max_local_recv_ts_ns: Option<i64>,
    min_ingest_seq: Option<u64>,
    max_ingest_seq: Option<u64>,
    event_type_counts: BTreeMap<String, usize>,
    symbols: Vec<String>,
    conditions: Vec<String>,
    assets: Vec<String>,
}

#[derive(Debug, Clone)]
struct CompactTypedRecordMeta {
    ingest_seq: u64,
    source_row_idx: u64,
    original_local_recv_ts_ns: i64,
    exchange_ts_ms: i64,
    event_type_code: u8,
    symbol_key: u32,
    condition_key: u32,
    market_start_ts_ns: i64,
    market_end_ts_ns: i64,
    key_asset_key: u32,
    body_offset: u64,
    body_len: u32,
    payload_sha256: [u8; 32],
}

#[derive(Debug, Clone)]
struct OrderedCompactTypedUpdate {
    key: MarketEventSortKey,
    update: MarketReplayTypedUpdate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayBookState {
    pub asset_id: String,
    pub condition_id: Option<String>,
    pub symbol: Option<String>,
    pub horizon_seconds: Option<i64>,
    #[serde(default)]
    pub market_start_ts_ns: Option<i64>,
    #[serde(default)]
    pub market_end_ts_ns: Option<i64>,
    pub last_visible_ts_ns: i64,
    pub last_local_recv_ts_ns: i64,
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuySweepResult {
    pub requested_cash_micros: i64,
    pub filled_cash_micros: i64,
    pub filled_shares_micros: i64,
    pub avg_price_micros: Option<i64>,
    pub worst_price_micros: Option<i64>,
    pub fully_filled: bool,
    pub levels_consumed: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayReferenceState {
    pub symbol: String,
    pub visible_ts_ns: i64,
    pub exchange_ts_ns: Option<i64>,
    pub close_price_micros: i64,
    pub source_row_idx: u64,
    pub payload_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayConditionState {
    pub condition_id: String,
    pub symbol: Option<String>,
    pub horizon_seconds: Option<i64>,
    pub asset_ids: BTreeSet<String>,
    pub first_visible_ts_ns: i64,
    pub last_visible_ts_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplaySettlementState {
    pub condition_id: String,
    pub asset_id: String,
    pub outcome: String,
    pub winner: bool,
    pub settled_ts_ns: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingReplayBuyOrder {
    pub order_id: String,
    pub condition_id: String,
    pub asset_id: String,
    pub arrival_ts_ns: i64,
    pub cash_micros: i64,
    pub limit_price_micros: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayOrderExecution {
    pub order_id: String,
    pub condition_id: String,
    pub asset_id: String,
    pub arrival_ts_ns: i64,
    pub filled: bool,
    pub full_fill: bool,
    pub cash_micros: i64,
    pub qty_micros: i64,
    pub avg_price_micros: Option<i64>,
    pub worst_price_micros: Option<i64>,
    pub book_age_ms: Option<i64>,
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ResidualReplayBook {
    last_visible_ts_ns: i64,
    asks: BTreeMap<i64, i64>,
}

#[derive(Debug)]
pub struct StreamingMarketReplayState {
    books: BTreeMap<String, ReplayBookState>,
    references: BTreeMap<String, ReplayReferenceState>,
    conditions: BTreeMap<String, ReplayConditionState>,
    settlements_by_condition_outcome: BTreeMap<(String, String), ReplaySettlementState>,
    pending_buy_orders: Vec<PendingReplayBuyOrder>,
    residual_asks: BTreeMap<String, ResidualReplayBook>,
    applied_events: u64,
    track_conditions: bool,
}

impl Default for StreamingMarketReplayState {
    fn default() -> Self {
        Self {
            books: BTreeMap::new(),
            references: BTreeMap::new(),
            conditions: BTreeMap::new(),
            settlements_by_condition_outcome: BTreeMap::new(),
            pending_buy_orders: Vec::new(),
            residual_asks: BTreeMap::new(),
            applied_events: 0,
            track_conditions: true,
        }
    }
}

impl StreamingMarketReplayState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_without_condition_tracking() -> Self {
        Self {
            track_conditions: false,
            ..Self::default()
        }
    }

    pub fn apply_event(&mut self, event: &MarketEvent) -> Result<()> {
        match event.event_type.as_str() {
            "book_snapshot_start" => {
                let asset_id = required_asset_id(event)?;
                self.observe_condition_from_event(event);
                self.residual_asks.remove(&asset_id);
                self.books.insert(
                    asset_id.clone(),
                    ReplayBookState {
                        asset_id,
                        condition_id: event.condition_id.clone(),
                        symbol: event.symbol.clone(),
                        horizon_seconds: event.horizon_seconds,
                        market_start_ts_ns: None,
                        market_end_ts_ns: None,
                        last_visible_ts_ns: event.visible_ts_ns,
                        last_local_recv_ts_ns: event.local_recv_ts_ns,
                        bids: BTreeMap::new(),
                        asks: BTreeMap::new(),
                    },
                );
            }
            "book_snapshot_level" => {
                let asset_id = required_asset_id(event)?;
                self.observe_condition_from_event(event);
                self.residual_asks.remove(&asset_id);
                let book = self
                    .books
                    .entry(asset_id.clone())
                    .or_insert_with(|| empty_book_from_event(asset_id, event));
                apply_level_event(book, event)?;
            }
            "depth_delta" => {
                let asset_id = required_asset_id(event)?;
                self.observe_condition_from_event(event);
                self.residual_asks.remove(&asset_id);
                let Some(book) = self.books.get_mut(&asset_id) else {
                    self.applied_events += 1;
                    return Ok(());
                };
                apply_level_event(book, event)?;
            }
            "reference_bar" => {
                self.apply_reference_event(event)?;
            }
            _ => {}
        }
        self.applied_events += 1;
        Ok(())
    }

    fn apply_reference_event(&mut self, event: &MarketEvent) -> Result<()> {
        let symbol = event
            .symbol
            .clone()
            .ok_or_else(|| anyhow!("reference_bar missing symbol"))?;
        let close_price_micros = event
            .price_micros
            .ok_or_else(|| anyhow!("reference_bar missing close price"))?;
        self.references.insert(
            symbol.clone(),
            ReplayReferenceState {
                symbol,
                visible_ts_ns: event.visible_ts_ns,
                exchange_ts_ns: event.exchange_ts_ns,
                close_price_micros,
                source_row_idx: event.source_row_idx,
                payload_hash: event.payload_hash.clone(),
            },
        );
        Ok(())
    }

    fn observe_condition_from_event(&mut self, event: &MarketEvent) {
        if !self.track_conditions {
            return;
        }
        let Some(condition_id) = event.condition_id.as_ref() else {
            return;
        };
        let entry = self
            .conditions
            .entry(condition_id.clone())
            .or_insert_with(|| ReplayConditionState {
                condition_id: condition_id.clone(),
                symbol: event.symbol.clone(),
                horizon_seconds: event.horizon_seconds,
                asset_ids: BTreeSet::new(),
                first_visible_ts_ns: event.visible_ts_ns,
                last_visible_ts_ns: event.visible_ts_ns,
            });
        if let Some(asset_id) = event.asset_id.as_ref() {
            entry.asset_ids.insert(asset_id.clone());
        }
        if entry.symbol.is_none() {
            entry.symbol = event.symbol.clone();
        }
        if entry.horizon_seconds.is_none() {
            entry.horizon_seconds = event.horizon_seconds;
        }
        entry.first_visible_ts_ns = entry.first_visible_ts_ns.min(event.visible_ts_ns);
        entry.last_visible_ts_ns = entry.last_visible_ts_ns.max(event.visible_ts_ns);
    }

    pub fn apply_raw_update(&mut self, update: &MarketReplayRawUpdate) -> Result<()> {
        let payload = if update.raw.raw_payload.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice::<Value>(&update.raw.raw_payload).with_context(|| {
                format!(
                    "parse raw market replay payload at ingest_seq {}",
                    update.raw.ingest_seq
                )
            })?
        };
        self.apply_raw_update_with_payload(update, &payload)
    }

    pub fn apply_raw_update_with_payload(
        &mut self,
        update: &MarketReplayRawUpdate,
        payload: &Value,
    ) -> Result<()> {
        let typed = decode_market_replay_typed_update(update, payload)?;
        self.apply_typed_update(&typed)
    }

    pub fn apply_typed_update(&mut self, update: &MarketReplayTypedUpdate) -> Result<()> {
        match &update.body {
            MarketReplayTypedUpdateBody::Book {
                asset_id,
                bids,
                asks,
            } => self.apply_typed_book_snapshot(update, asset_id, bids, asks)?,
            MarketReplayTypedUpdateBody::PriceChanges { changes } => {
                self.apply_typed_price_changes(update, changes)?
            }
            MarketReplayTypedUpdateBody::Other => {}
        }
        self.applied_events += 1;
        Ok(())
    }

    fn apply_typed_book_snapshot(
        &mut self,
        update: &MarketReplayTypedUpdate,
        asset_id: &str,
        bids: &[ReplayBookLevel],
        asks: &[ReplayBookLevel],
    ) -> Result<()> {
        self.observe_condition_from_typed_update(update, Some(asset_id));
        self.residual_asks.remove(asset_id);
        let mut book = ReplayBookState {
            asset_id: asset_id.to_string(),
            condition_id: update.condition_id.clone(),
            symbol: update.symbol.clone(),
            horizon_seconds: update.horizon_seconds,
            market_start_ts_ns: update.market_start_ts_ns,
            market_end_ts_ns: update.market_end_ts_ns,
            last_visible_ts_ns: update.visible_ts_ns,
            last_local_recv_ts_ns: update.original_local_recv_ts_ns,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        };
        for level in bids {
            apply_level_to_book_side(
                &mut book,
                ReplayBookSide::Bid,
                level.price_micros,
                level.qty_micros,
            )?;
        }
        for level in asks {
            apply_level_to_book_side(
                &mut book,
                ReplayBookSide::Ask,
                level.price_micros,
                level.qty_micros,
            )?;
        }
        self.books.insert(asset_id.to_string(), book);
        Ok(())
    }

    fn apply_typed_price_changes(
        &mut self,
        update: &MarketReplayTypedUpdate,
        changes: &[MarketReplayLevelChange],
    ) -> Result<()> {
        for change in changes {
            self.observe_condition_from_typed_update(update, Some(&change.asset_id));
            self.residual_asks.remove(&change.asset_id);
            let Some(book) = self.books.get_mut(&change.asset_id) else {
                continue;
            };
            apply_level_to_book_side(book, change.side, change.price_micros, change.qty_micros)?;
            book.last_visible_ts_ns = update.visible_ts_ns;
            book.last_local_recv_ts_ns = update.original_local_recv_ts_ns;
            if book.condition_id.is_none() {
                book.condition_id = update.condition_id.clone();
            }
            if book.symbol.is_none() {
                book.symbol = update.symbol.clone();
            }
            if book.horizon_seconds.is_none() {
                book.horizon_seconds = update.horizon_seconds;
            }
            if book.market_start_ts_ns.is_none() {
                book.market_start_ts_ns = update.market_start_ts_ns;
            }
            if book.market_end_ts_ns.is_none() {
                book.market_end_ts_ns = update.market_end_ts_ns;
            }
        }
        Ok(())
    }

    fn observe_condition_from_typed_update(
        &mut self,
        update: &MarketReplayTypedUpdate,
        asset_id: Option<&str>,
    ) {
        if !self.track_conditions {
            return;
        }
        let Some(condition_id) = update.condition_id.as_ref() else {
            return;
        };
        let entry = self
            .conditions
            .entry(condition_id.clone())
            .or_insert_with(|| ReplayConditionState {
                condition_id: condition_id.clone(),
                symbol: update.symbol.clone(),
                horizon_seconds: update.horizon_seconds,
                asset_ids: BTreeSet::new(),
                first_visible_ts_ns: update.visible_ts_ns,
                last_visible_ts_ns: update.visible_ts_ns,
            });
        if let Some(asset_id) = asset_id {
            entry.asset_ids.insert(asset_id.to_string());
        }
        if entry.symbol.is_none() {
            entry.symbol = update.symbol.clone();
        }
        if entry.horizon_seconds.is_none() {
            entry.horizon_seconds = update.horizon_seconds;
        }
        entry.first_visible_ts_ns = entry.first_visible_ts_ns.min(update.visible_ts_ns);
        entry.last_visible_ts_ns = entry.last_visible_ts_ns.max(update.visible_ts_ns);
    }

    pub fn applied_events(&self) -> u64 {
        self.applied_events
    }

    pub fn book_for(&self, asset_id: &str) -> Option<&ReplayBookState> {
        self.books.get(asset_id)
    }

    pub fn book_count(&self) -> usize {
        self.books.len()
    }

    pub fn reference_for(&self, symbol: &str) -> Option<&ReplayReferenceState> {
        self.references.get(&symbol.to_ascii_uppercase())
    }

    pub fn reference_count(&self) -> usize {
        self.references.len()
    }

    pub fn condition_for(&self, condition_id: &str) -> Option<&ReplayConditionState> {
        self.conditions.get(condition_id)
    }

    pub fn condition_count(&self) -> usize {
        self.conditions.len()
    }

    pub fn apply_settlement(&mut self, settlement: ReplaySettlementState) {
        self.settlements_by_condition_outcome.insert(
            (
                settlement.condition_id.clone(),
                settlement.outcome.to_ascii_uppercase(),
            ),
            settlement,
        );
    }

    pub fn settlement_for(
        &self,
        condition_id: &str,
        outcome: &str,
    ) -> Option<&ReplaySettlementState> {
        self.settlements_by_condition_outcome
            .get(&(condition_id.to_string(), outcome.to_ascii_uppercase()))
    }

    pub fn settlement_count(&self) -> usize {
        self.settlements_by_condition_outcome.len()
    }

    pub fn enqueue_buy_order(&mut self, order: PendingReplayBuyOrder) -> Result<()> {
        if order.arrival_ts_ns < 0 {
            bail!(
                "pending replay order {} has negative arrival timestamp",
                order.order_id
            );
        }
        if order.cash_micros <= 0 {
            bail!(
                "pending replay order {} has non-positive cash",
                order.order_id
            );
        }
        if order.limit_price_micros <= 0 {
            bail!(
                "pending replay order {} has non-positive limit price",
                order.order_id
            );
        }
        let pos = self
            .pending_buy_orders
            .partition_point(|existing| existing.arrival_ts_ns <= order.arrival_ts_ns);
        self.pending_buy_orders.insert(pos, order);
        Ok(())
    }

    pub fn pending_order_count(&self) -> usize {
        self.pending_buy_orders.len()
    }

    pub fn settle_due_buy_orders(
        &mut self,
        now_ts_ns: i64,
        max_book_age_ms: i64,
    ) -> Vec<ReplayOrderExecution> {
        let due_count = self
            .pending_buy_orders
            .partition_point(|order| order.arrival_ts_ns <= now_ts_ns);
        let due = self
            .pending_buy_orders
            .drain(..due_count)
            .collect::<Vec<_>>();
        due.into_iter()
            .map(|order| self.execute_buy_order(order, max_book_age_ms))
            .collect()
    }

    pub fn settle_all_buy_orders(&mut self, max_book_age_ms: i64) -> Vec<ReplayOrderExecution> {
        self.settle_due_buy_orders(i64::MAX, max_book_age_ms)
    }

    pub fn best_bid_price_micros(&self, asset_id: &str) -> Option<i64> {
        self.books.get(asset_id)?.bids.keys().next_back().copied()
    }

    pub fn best_ask_price_micros(&self, asset_id: &str) -> Option<i64> {
        self.books.get(asset_id)?.asks.keys().next().copied()
    }

    pub fn sweep_buy(&self, asset_id: &str, cash_micros: i64) -> Option<BuySweepResult> {
        self.sweep_buy_at_or_below(asset_id, cash_micros, i64::MAX)
    }

    pub fn sweep_buy_at_or_below(
        &self,
        asset_id: &str,
        cash_micros: i64,
        limit_price_micros: i64,
    ) -> Option<BuySweepResult> {
        if cash_micros <= 0 {
            return None;
        }
        let book = self.books.get(asset_id)?;
        let mut remaining_cash = cash_micros as i128;
        let mut filled_cash = 0i128;
        let mut filled_shares = 0i128;
        let mut worst_price = None::<i64>;
        let mut levels_consumed = 0usize;
        for (price, qty) in &book.asks {
            if *price > limit_price_micros {
                break;
            }
            if *price <= 0 || *qty <= 0 || remaining_cash <= 0 {
                continue;
            }
            let level_cash = (*price as i128)
                .saturating_mul(*qty as i128)
                .saturating_div(1_000_000);
            if level_cash <= 0 {
                continue;
            }
            levels_consumed += 1;
            worst_price = Some(*price);
            if level_cash <= remaining_cash {
                remaining_cash -= level_cash;
                filled_cash += level_cash;
                filled_shares += *qty as i128;
            } else {
                let shares = remaining_cash
                    .saturating_mul(1_000_000)
                    .saturating_div(*price as i128);
                if shares <= 0 {
                    break;
                }
                let cash = shares
                    .saturating_mul(*price as i128)
                    .saturating_div(1_000_000);
                filled_cash += cash;
                filled_shares += shares;
                break;
            }
        }
        let avg_price = if filled_shares > 0 {
            Some(
                i64::try_from(
                    filled_cash
                        .saturating_mul(1_000_000)
                        .saturating_div(filled_shares),
                )
                .ok()?,
            )
        } else {
            None
        };
        Some(BuySweepResult {
            requested_cash_micros: cash_micros,
            filled_cash_micros: i64::try_from(filled_cash).ok()?,
            filled_shares_micros: i64::try_from(filled_shares).ok()?,
            avg_price_micros: avg_price,
            worst_price_micros: worst_price,
            fully_filled: (cash_micros as i128).saturating_sub(filled_cash) <= 1,
            levels_consumed,
        })
    }

    fn execute_buy_order(
        &mut self,
        order: PendingReplayBuyOrder,
        max_book_age_ms: i64,
    ) -> ReplayOrderExecution {
        let Some(book) = self.books.get(&order.asset_id).cloned() else {
            return rejected_order(order, None, "no_visible_book_at_arrival");
        };
        if book
            .condition_id
            .as_ref()
            .is_some_and(|condition_id| condition_id != &order.condition_id)
        {
            return rejected_order(order, None, "condition_mismatch_at_arrival");
        }
        let book_age_ms = order
            .arrival_ts_ns
            .saturating_sub(book.last_visible_ts_ns)
            .div_euclid(1_000_000);
        if book_age_ms < 0 {
            return rejected_order(order, Some(book_age_ms), "book_not_visible_at_arrival");
        }
        if book_age_ms > max_book_age_ms {
            return rejected_order(order, Some(book_age_ms), "stale_book_at_arrival");
        }
        if crossed_book(&book) {
            return rejected_order(order, Some(book_age_ms), "crossed_book_at_arrival");
        }
        let Some(sweep) =
            self.consume_residual_buy(&book, order.cash_micros, order.limit_price_micros)
        else {
            return rejected_order(order, Some(book_age_ms), "no_ask_depth_at_limit");
        };
        ReplayOrderExecution {
            order_id: order.order_id,
            condition_id: order.condition_id,
            asset_id: order.asset_id,
            arrival_ts_ns: order.arrival_ts_ns,
            filled: sweep.filled_cash_micros > 0 && sweep.filled_shares_micros > 0,
            full_fill: sweep.fully_filled,
            cash_micros: sweep.filled_cash_micros,
            qty_micros: sweep.filled_shares_micros,
            avg_price_micros: sweep.avg_price_micros,
            worst_price_micros: sweep.worst_price_micros,
            book_age_ms: Some(book_age_ms),
            reject_reason: None,
        }
    }

    fn consume_residual_buy(
        &mut self,
        book: &ReplayBookState,
        cash_micros: i64,
        limit_price_micros: i64,
    ) -> Option<BuySweepResult> {
        let residual = self
            .residual_asks
            .entry(book.asset_id.clone())
            .or_insert_with(|| ResidualReplayBook {
                last_visible_ts_ns: book.last_visible_ts_ns,
                asks: book.asks.clone(),
            });
        if residual.last_visible_ts_ns != book.last_visible_ts_ns {
            residual.last_visible_ts_ns = book.last_visible_ts_ns;
            residual.asks = book.asks.clone();
        }
        sweep_and_consume_asks(&mut residual.asks, cash_micros, limit_price_micros)
    }
}

fn rejected_order(
    order: PendingReplayBuyOrder,
    book_age_ms: Option<i64>,
    reason: &str,
) -> ReplayOrderExecution {
    ReplayOrderExecution {
        order_id: order.order_id,
        condition_id: order.condition_id,
        asset_id: order.asset_id,
        arrival_ts_ns: order.arrival_ts_ns,
        filled: false,
        full_fill: false,
        cash_micros: 0,
        qty_micros: 0,
        avg_price_micros: None,
        worst_price_micros: None,
        book_age_ms,
        reject_reason: Some(reason.to_string()),
    }
}

fn crossed_book(book: &ReplayBookState) -> bool {
    book.bids
        .keys()
        .next_back()
        .zip(book.asks.keys().next())
        .is_some_and(|(bid, ask)| bid >= ask)
}

fn sweep_and_consume_asks(
    asks: &mut BTreeMap<i64, i64>,
    cash_micros: i64,
    limit_price_micros: i64,
) -> Option<BuySweepResult> {
    if cash_micros <= 0 || limit_price_micros <= 0 {
        return None;
    }
    let mut remaining_cash = cash_micros as i128;
    let mut filled_cash = 0i128;
    let mut filled_shares = 0i128;
    let mut worst_price = None::<i64>;
    let mut levels_consumed = 0usize;
    let mut updates = Vec::<(i64, i64)>::new();
    for (price, qty) in asks.iter() {
        if *price > limit_price_micros || remaining_cash <= 0 {
            break;
        }
        if *price <= 0 || *qty <= 0 {
            continue;
        }
        let level_cash = (*price as i128)
            .saturating_mul(*qty as i128)
            .saturating_div(1_000_000);
        if level_cash <= 0 {
            continue;
        }
        levels_consumed += 1;
        worst_price = Some(*price);
        if level_cash <= remaining_cash {
            remaining_cash -= level_cash;
            filled_cash += level_cash;
            filled_shares += *qty as i128;
            updates.push((*price, 0));
        } else {
            let shares = remaining_cash
                .saturating_mul(1_000_000)
                .saturating_div(*price as i128);
            if shares <= 0 {
                break;
            }
            let cash = shares
                .saturating_mul(*price as i128)
                .saturating_div(1_000_000);
            filled_cash += cash;
            filled_shares += shares;
            updates.push((*price, (*qty as i128).saturating_sub(shares) as i64));
            break;
        }
    }
    if filled_cash <= 0 || filled_shares <= 0 {
        return None;
    }
    for (price, remaining_qty) in updates {
        if remaining_qty <= 0 {
            asks.remove(&price);
        } else {
            asks.insert(price, remaining_qty);
        }
    }
    Some(BuySweepResult {
        requested_cash_micros: cash_micros,
        filled_cash_micros: i64::try_from(filled_cash).ok()?,
        filled_shares_micros: i64::try_from(filled_shares).ok()?,
        avg_price_micros: Some(
            i64::try_from(
                filled_cash
                    .saturating_mul(1_000_000)
                    .saturating_div(filled_shares),
            )
            .ok()?,
        ),
        worst_price_micros: worst_price,
        fully_filled: (cash_micros as i128).saturating_sub(filled_cash) <= 1,
        levels_consumed,
    })
}

pub fn stream_market_replay_events_from_raw<F>(
    options: &StreamMarketReplayEventsOptions,
    mut visit: F,
) -> Result<MarketReplayStreamReport>
where
    F: FnMut(MarketEvent) -> Result<()>,
{
    let build_options = BuildMarketReplayDatasetOptions::from(options);
    validate_market_replay_options(&build_options)?;
    validate_market_replay_raw_coverage_options(&build_options)?;
    let started = Instant::now();
    let symbol_filter = SymbolFilter::new(&build_options.market_symbol_allowlist)?;
    let manifests = sorted_candidate_manifests(&build_options)?;
    let mut pending_events = BinaryHeap::<Reverse<OrderedMarketEvent>>::new();
    let mut stats = MarketReplayStats::default();
    let mut consumed_manifests = 0usize;
    let order_holdback_ns = market_replay_order_holdback_ns(&build_options);

    for manifest_idx in 0..manifests.len() {
        let manifest = &manifests[manifest_idx];
        if !manifest_overlaps(manifest, &build_options) {
            continue;
        }
        consumed_manifests += 1;
        scan_hftrec4_segment_selected(
            &manifest.segment_path,
            |record| should_read_market_replay_payload(record, &build_options, &symbol_filter),
            |record| {
                let raw_source_row_idx = record.row_idx;
                let raw_record_hash = source_record_hash(&manifest.segment_path, &record)?;
                if manifest.stream == REFERENCE_WS_RAW_STREAM {
                    let rows = market_events_from_reference_record(
                        record,
                        raw_source_row_idx,
                        &manifest.segment_path,
                        raw_record_hash,
                        build_options.reference_latency_ms,
                    )?;
                    for row in rows {
                        if !ts_in_window(row.visible_ts_ns, &build_options) {
                            continue;
                        }
                        if event_symbol_disallowed(&row, &symbol_filter) {
                            continue;
                        }
                        pending_events.push(Reverse(OrderedMarketEvent::new(row)));
                    }
                } else {
                    let mut raw = hftrec4_record_to_ws_raw(record);
                    if raw.condition_id.is_none() {
                        raw.condition_id = condition_id_from_payload(&raw.raw_payload);
                    }
                    if raw.asset_id.is_none() {
                        raw.asset_id = asset_id_from_payload(&raw.raw_payload);
                    }
                    if raw_symbol_disallowed(&raw, &symbol_filter) {
                        return Ok(());
                    }
                    let original_local_recv_ts_ns = raw.local_recv_ts_ns;
                    apply_poly_visible_time_model(
                        &mut raw,
                        build_options.poly_server_visible_time,
                        build_options.poly_incremental_latency_ms,
                        build_options.poly_incremental_freshness_guard_ms,
                    );
                    let visible_ts_ns = raw.local_recv_ts_ns;
                    if !ts_in_window(visible_ts_ns, &build_options) {
                        return Ok(());
                    }
                    let rows = market_events_from_ws_raw(
                        raw,
                        raw_source_row_idx,
                        original_local_recv_ts_ns,
                        visible_ts_ns,
                        &manifest.segment_path,
                        raw_record_hash,
                    )?;
                    for row in rows {
                        if event_symbol_disallowed(&row, &symbol_filter) {
                            continue;
                        }
                        pending_events.push(Reverse(OrderedMarketEvent::new(row)));
                    }
                }
                Ok(())
            },
        )
        .with_context(|| {
            format!(
                "stream HFTREC4 market replay segment {}",
                manifest.segment_path.display()
            )
        })?;
        let next_manifest_min_ts_ns = manifests
            .iter()
            .skip(manifest_idx + 1)
            .find(|candidate| manifest_overlaps(candidate, &build_options))
            .and_then(|candidate| candidate.min_ts_ns);
        if let Some(next_min_ts) = next_manifest_min_ts_ns {
            flush_ordered_events_to_visit(
                &mut pending_events,
                &mut stats,
                next_min_ts.saturating_sub(order_holdback_ns),
                &mut visit,
            )?;
        }
    }

    if consumed_manifests == 0 {
        bail!("stream-market-replay-events found no finalized HFTREC4 WS manifests");
    }
    flush_ordered_events_to_visit(&mut pending_events, &mut stats, i64::MAX, &mut visit)?;
    Ok(MarketReplayStreamReport {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_FORMAT.to_string(),
        row_count: stats.row_count,
        manifest_count: consumed_manifests,
        min_visible_ts_ns: stats.min_visible_ts_ns,
        max_visible_ts_ns: stats.max_visible_ts_ns,
        event_type_counts: stats.event_type_counts,
        elapsed_ms: started.elapsed().as_millis(),
        profile: MarketReplayStreamProfile::default(),
    })
}

pub fn stream_market_replay_raw_updates_from_raw<F>(
    options: &StreamMarketReplayEventsOptions,
    mut visit: F,
) -> Result<MarketReplayStreamReport>
where
    F: FnMut(MarketReplayRawUpdate) -> Result<()>,
{
    let build_options = BuildMarketReplayDatasetOptions::from(options);
    validate_market_replay_options(&build_options)?;
    validate_market_replay_raw_coverage_options(&build_options)?;
    let started = Instant::now();
    let symbol_filter = SymbolFilter::new(&build_options.market_symbol_allowlist)?;
    let manifests = sorted_candidate_manifests(&build_options)?;
    let mut pending_updates = BinaryHeap::<Reverse<OrderedMarketReplayRawUpdate>>::new();
    let mut stats = MarketReplayStats::default();
    let mut consumed_manifests = 0usize;
    let order_holdback_ns = market_replay_order_holdback_ns(&build_options);

    for manifest_idx in 0..manifests.len() {
        let manifest = &manifests[manifest_idx];
        if manifest.stream == REFERENCE_WS_RAW_STREAM
            || !manifest_overlaps(manifest, &build_options)
        {
            continue;
        }
        consumed_manifests += 1;
        scan_hftrec4_segment_selected(
            &manifest.segment_path,
            |record| should_read_market_replay_raw_update(record, &build_options, &symbol_filter),
            |record| {
                let update = market_replay_raw_update_from_record(
                    record,
                    &manifest.segment_path,
                    &build_options,
                    &symbol_filter,
                )?;
                if let Some(update) = update {
                    pending_updates.push(Reverse(OrderedMarketReplayRawUpdate::new(update)));
                }
                Ok(())
            },
        )
        .with_context(|| {
            format!(
                "stream HFTREC4 market replay raw update segment {}",
                manifest.segment_path.display()
            )
        })?;
        let next_manifest_min_ts_ns = manifests
            .iter()
            .skip(manifest_idx + 1)
            .find(|candidate| {
                candidate.stream != REFERENCE_WS_RAW_STREAM
                    && manifest_overlaps(candidate, &build_options)
            })
            .and_then(|candidate| candidate.min_ts_ns);
        if let Some(next_min_ts) = next_manifest_min_ts_ns {
            flush_ordered_raw_updates_to_visit(
                &mut pending_updates,
                &mut stats,
                next_min_ts.saturating_sub(order_holdback_ns),
                &mut visit,
            )?;
        }
    }

    if consumed_manifests == 0 {
        bail!("stream-market-replay-raw-updates found no finalized HFTREC4 WS manifests");
    }
    flush_ordered_raw_updates_to_visit(&mut pending_updates, &mut stats, i64::MAX, &mut visit)?;
    Ok(MarketReplayStreamReport {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_FORMAT.to_string(),
        row_count: stats.row_count,
        manifest_count: consumed_manifests,
        min_visible_ts_ns: stats.min_visible_ts_ns,
        max_visible_ts_ns: stats.max_visible_ts_ns,
        event_type_counts: stats.event_type_counts,
        elapsed_ms: started.elapsed().as_millis(),
        profile: MarketReplayStreamProfile::default(),
    })
}

pub fn build_market_replay_typed_updates(
    options: &BuildMarketReplayTypedUpdatesOptions,
) -> Result<MarketReplayTypedUpdatesBuildReport> {
    let output_mode = match (options.output_file.as_ref(), options.output_root.as_ref()) {
        (Some(path), None) => TypedUpdateBuildOutput::Single(path.clone()),
        (None, Some(root)) => TypedUpdateBuildOutput::Sharded(root.clone()),
        (Some(_), Some(_)) => bail!("pass only one of output_file or output_root"),
        (None, None) => bail!("typed update build requires output_file or output_root"),
    };

    match &output_mode {
        TypedUpdateBuildOutput::Single(path) => {
            if path.exists() && !options.overwrite {
                bail!(
                    "typed update file already exists; pass overwrite to replace: {}",
                    path.display()
                );
            }
        }
        TypedUpdateBuildOutput::Sharded(root) => {
            if root.exists() {
                if !options.overwrite && root.read_dir()?.next().is_some() {
                    bail!(
                        "typed update root already exists and is not empty; pass overwrite to replace: {}",
                        root.display()
                    );
                }
                if options.overwrite {
                    fs::remove_dir_all(root)
                        .with_context(|| format!("remove {}", root.display()))?;
                }
            }
        }
    }
    let started = Instant::now();
    let mut writer = TypedUpdateBuildWriter::create(&output_mode, options)?;
    let mut stats = MarketReplayStats::default();
    stream_market_replay_raw_updates_from_raw(
        &StreamMarketReplayEventsOptions {
            raw_roots: options.raw_roots.clone(),
            raw_start_ts_ns: options.raw_start_ts_ns,
            raw_end_ts_ns: options.raw_end_ts_ns,
            market_symbol_allowlist: options.market_symbol_allowlist.clone(),
            poly_server_visible_time: options.poly_server_visible_time,
            poly_incremental_latency_ms: options.poly_incremental_latency_ms,
            poly_incremental_freshness_guard_ms: options.poly_incremental_freshness_guard_ms,
            reference_latency_ms: options.reference_latency_ms,
        },
        |update| {
            let payload = raw_update_payload_value(&update)?;
            let typed = decode_market_replay_typed_update(&update, &payload)?;
            writer.write_update(&typed)?;
            stats.observe_typed_update(&typed);
            Ok(())
        },
    )?;
    let shard_files = writer.finish()?;
    let report = MarketReplayTypedUpdatesBuildReport {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_TYPED_UPDATES_FORMAT.to_string(),
        output_file: options.output_file.clone(),
        output_root: options.output_root.clone(),
        shard_files,
        row_count: stats.row_count,
        min_visible_ts_ns: stats.min_visible_ts_ns,
        max_visible_ts_ns: stats.max_visible_ts_ns,
        event_type_counts: stats.event_type_counts,
        elapsed_ms: started.elapsed().as_millis(),
    };
    if let TypedUpdateBuildOutput::Sharded(root) = &output_mode {
        write_json_file_pretty(&root.join(MARKET_REPLAY_TYPED_UPDATES_MANIFEST), &report)?;
    }
    Ok(report)
}

pub fn market_replay_compact_typed_output_path(root: &Path, ts_ns: i64) -> PathBuf {
    let hour_bucket = ts_ns.div_euclid(HOUR_NS);
    root.join(MARKET_REPLAY_COMPACT_TYPED_STREAM)
        .join(format!("hour_bucket={hour_bucket}"))
        .join(format!(
            "{MARKET_REPLAY_COMPACT_TYPED_STREAM}-{ts_ns}.pm5mtb"
        ))
}

pub fn market_replay_compact_typed_manifest_path_for_segment(segment: &Path) -> PathBuf {
    let name = segment
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("segment.pm5mtb");
    segment.with_file_name(format!("{name}.manifest.json"))
}

pub fn write_market_replay_compact_typed_segment_from_hftrec4_write_records(
    path: &Path,
    raw_segment_path: &Path,
    records: &[Hftrec4WriteRecord],
) -> Result<Option<MarketReplayCompactTypedSegmentManifest>> {
    let records = records
        .iter()
        .enumerate()
        .map(|(row_idx, record)| Hftrec4Record {
            row_idx: row_idx as u64,
            ingest_seq: record.ingest_seq,
            local_recv_ts_ns: record.local_recv_ts_ns,
            event_type: record.event_type.clone(),
            symbol: record.symbol.clone(),
            condition_id: record.condition_id.clone(),
            asset_id: record.asset_id.clone(),
            market_start_ts_ns: record.market_start_ts_ns,
            market_end_ts_ns: record.market_end_ts_ns,
            yes_asset_id: record.yes_asset_id.clone(),
            no_asset_id: record.no_asset_id.clone(),
            payload_sha256: sha256_bytes(&record.payload),
            payload: record.payload.clone(),
        })
        .collect::<Vec<_>>();
    write_market_replay_compact_typed_segment_from_hftrec4_records(path, raw_segment_path, &records)
}

pub fn write_market_replay_compact_typed_segment_from_hftrec4_records(
    path: &Path,
    raw_segment_path: &Path,
    records: &[Hftrec4Record],
) -> Result<Option<MarketReplayCompactTypedSegmentManifest>> {
    write_compact_typed_segment_from_hftrec4_records(path, raw_segment_path, records)
}

pub fn read_market_replay_compact_typed_records(
    path: &Path,
) -> Result<Vec<MarketReplayCompactTypedRecord>> {
    read_compact_typed_records(path)
}

pub fn build_market_replay_compact_typed(
    options: &BuildMarketReplayCompactTypedOptions,
) -> Result<MarketReplayCompactTypedBuildReport> {
    let build_options = BuildMarketReplayDatasetOptions {
        raw_roots: options.raw_roots.clone(),
        dataset_root: PathBuf::new(),
        raw_start_ts_ns: options.raw_start_ts_ns,
        raw_end_ts_ns: options.raw_end_ts_ns,
        market_symbol_allowlist: Vec::new(),
        overwrite: false,
        poly_server_visible_time: false,
        poly_incremental_latency_ms: DEFAULT_POLY_INCREMENTAL_LATENCY_MS,
        poly_incremental_freshness_guard_ms: DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS,
        reference_latency_ms: DEFAULT_REFERENCE_LATENCY_MS,
        max_rows_per_part: None,
    };
    validate_market_replay_options(&build_options)?;
    if options.output_root.exists() {
        if options.overwrite {
            fs::remove_dir_all(&options.output_root)
                .with_context(|| format!("remove {}", options.output_root.display()))?;
        } else if options.output_root.read_dir()?.next().is_some() {
            bail!(
                "compact typed output root already exists and is not empty; pass overwrite to replace: {}",
                options.output_root.display()
            );
        }
    }
    fs::create_dir_all(&options.output_root)
        .with_context(|| format!("create {}", options.output_root.display()))?;

    let started = Instant::now();
    let manifests = sorted_candidate_manifests(&build_options)?;
    let mut segment_files = Vec::new();
    let mut row_count = 0usize;
    let mut skipped_empty_segment_count = 0usize;
    let mut min_local_recv_ts_ns = None::<i64>;
    let mut max_local_recv_ts_ns = None::<i64>;
    for manifest in manifests {
        if manifest.stream == REFERENCE_WS_RAW_STREAM
            || !manifest_overlaps(&manifest, &build_options)
        {
            continue;
        }
        let mut records = Vec::new();
        scan_hftrec4_segment_selected(
            &manifest.segment_path,
            |record| {
                let event_type = record.event_type.trim().to_ascii_lowercase();
                Ok(ts_before_end(record.local_recv_ts_ns, &build_options)
                    && (event_type == "book" || event_type == "price_change"))
            },
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .with_context(|| {
            format!(
                "read HFTREC4 segment for compact typed build {}",
                manifest.segment_path.display()
            )
        })?;
        let ts_ns = manifest
            .min_ts_ns
            .or_else(|| records.first().map(|record| record.local_recv_ts_ns))
            .unwrap_or(0);
        let output_path = market_replay_compact_typed_output_path(&options.output_root, ts_ns);
        let Some(segment_manifest) = write_compact_typed_segment_from_hftrec4_records(
            &output_path,
            &manifest.segment_path,
            &records,
        )?
        else {
            skipped_empty_segment_count = skipped_empty_segment_count.saturating_add(1);
            continue;
        };
        row_count = row_count.saturating_add(segment_manifest.record_count);
        min_local_recv_ts_ns =
            min_opt_i64(min_local_recv_ts_ns, segment_manifest.min_local_recv_ts_ns);
        max_local_recv_ts_ns =
            max_opt_i64(max_local_recv_ts_ns, segment_manifest.max_local_recv_ts_ns);
        segment_files.push(segment_manifest.segment_path);
    }

    Ok(MarketReplayCompactTypedBuildReport {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_COMPACT_TYPED_FORMAT.to_string(),
        output_root: options.output_root.clone(),
        segment_count: segment_files.len(),
        skipped_empty_segment_count,
        row_count,
        min_local_recv_ts_ns,
        max_local_recv_ts_ns,
        segment_files,
        elapsed_ms: started.elapsed().as_millis(),
    })
}

pub fn discover_market_replay_compact_typed_files_for_window(
    root: &Path,
    start_ts_ns: Option<i64>,
    end_ts_ns: Option<i64>,
) -> Result<Vec<PathBuf>> {
    if root.is_file() {
        return Ok(vec![root.to_path_buf()]);
    }
    if !root.exists() {
        bail!("compact typed root does not exist: {}", root.display());
    }
    let mut files = Vec::new();
    collect_compact_typed_files(root, &mut files)?;
    let start_bucket = start_ts_ns.map(|ts| ts.div_euclid(HOUR_NS));
    let end_bucket = end_ts_ns.map(|ts| ts.saturating_sub(1).div_euclid(HOUR_NS));
    files.retain(|path| {
        typed_update_path_hour_bucket(path).map_or(true, |bucket| {
            start_bucket.is_none_or(|start| bucket >= start)
                && end_bucket.is_none_or(|end| bucket <= end)
        })
    });
    files.sort();
    if files.is_empty() {
        bail!(
            "compact typed root has no compact typed segment files: {}",
            root.display()
        );
    }
    Ok(files)
}

pub fn stream_market_replay_typed_updates_from_compact_files<I, F>(
    paths: I,
    options: &StreamMarketReplayEventsOptions,
    mut visit: F,
) -> Result<MarketReplayStreamReport>
where
    I: IntoIterator<Item = PathBuf>,
    F: FnMut(MarketReplayTypedUpdate) -> Result<()>,
{
    stream_compact_typed_updates_from_files(paths, options, &mut visit)
}

pub fn validate_market_replay_compact_typed(
    options: &ValidateMarketReplayCompactTypedOptions,
) -> Result<MarketReplayCompactTypedValidationReport> {
    if options.compact_typed_roots.is_empty() {
        bail!("compact typed validation requires at least one compact typed root");
    }
    let stream_options = StreamMarketReplayEventsOptions {
        raw_roots: options.raw_roots.clone(),
        raw_start_ts_ns: options.raw_start_ts_ns,
        raw_end_ts_ns: options.raw_end_ts_ns,
        market_symbol_allowlist: options.market_symbol_allowlist.clone(),
        poly_server_visible_time: options.poly_server_visible_time,
        poly_incremental_latency_ms: options.poly_incremental_latency_ms,
        poly_incremental_freshness_guard_ms: options.poly_incremental_freshness_guard_ms,
        reference_latency_ms: options.reference_latency_ms,
    };
    let raw_started = Instant::now();
    let mut raw_row_hashes = Vec::new();
    stream_market_replay_typed_updates_from_raw_parallel(
        &stream_options,
        options.raw_worker_count.max(1),
        |update| {
            raw_row_hashes.push(hash_serializable(&update)?);
            Ok(())
        },
    )?;
    let raw_elapsed_ms = raw_started.elapsed().as_millis();

    let mut compact_files = Vec::new();
    for root in &options.compact_typed_roots {
        compact_files.extend(discover_market_replay_compact_typed_files_for_window(
            root,
            options.raw_start_ts_ns,
            options.raw_end_ts_ns,
        )?);
    }
    compact_files.sort();
    compact_files.dedup();
    let compact_started = Instant::now();
    let mut compact_row_hashes = Vec::new();
    let mut collect_compact_hash = |update: MarketReplayTypedUpdate| {
        compact_row_hashes.push(hash_serializable(&update)?);
        Ok(())
    };
    stream_compact_typed_updates_from_files(
        compact_files,
        &stream_options,
        &mut collect_compact_hash,
    )?;
    let compact_elapsed_ms = compact_started.elapsed().as_millis();

    let first_mismatch_index = raw_row_hashes
        .iter()
        .zip(compact_row_hashes.iter())
        .position(|(raw, compact)| raw != compact)
        .or_else(|| {
            if raw_row_hashes.len() == compact_row_hashes.len() {
                None
            } else {
                Some(raw_row_hashes.len().min(compact_row_hashes.len()))
            }
        });
    let raw_hash = hash_serializable(&raw_row_hashes)?;
    let compact_hash = hash_serializable(&compact_row_hashes)?;
    Ok(MarketReplayCompactTypedValidationReport {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_COMPACT_TYPED_FORMAT.to_string(),
        matched: first_mismatch_index.is_none() && raw_hash == compact_hash,
        raw_row_count: raw_row_hashes.len(),
        compact_row_count: compact_row_hashes.len(),
        raw_hash,
        compact_hash,
        first_mismatch_index,
        first_raw_row_hash: first_mismatch_index.and_then(|idx| raw_row_hashes.get(idx).cloned()),
        first_compact_row_hash: first_mismatch_index
            .and_then(|idx| compact_row_hashes.get(idx).cloned()),
        raw_elapsed_ms,
        compact_elapsed_ms,
    })
}

pub fn stream_market_replay_typed_updates_from_file<F>(
    path: &Path,
    mut visit: F,
) -> Result<MarketReplayStreamReport>
where
    F: FnMut(MarketReplayTypedUpdate) -> Result<()>,
{
    stream_market_replay_typed_updates_from_files([path.to_path_buf()], |update| visit(update))
}

pub fn stream_market_replay_typed_updates_from_raw_parallel<F>(
    options: &StreamMarketReplayEventsOptions,
    worker_count: usize,
    mut visit: F,
) -> Result<MarketReplayStreamReport>
where
    F: FnMut(MarketReplayTypedUpdate) -> Result<()>,
{
    let build_options = BuildMarketReplayDatasetOptions::from(options);
    validate_market_replay_options(&build_options)?;
    validate_market_replay_raw_coverage_options(&build_options)?;
    let started = Instant::now();
    let symbol_filter = SymbolFilter::new(&build_options.market_symbol_allowlist)?;
    let manifests = sorted_candidate_manifests(&build_options)?
        .into_iter()
        .enumerate()
        .filter(|(_, manifest)| {
            manifest.stream != REFERENCE_WS_RAW_STREAM
                && manifest_overlaps(manifest, &build_options)
        })
        .collect::<Vec<_>>();
    if manifests.is_empty() {
        bail!("parallel typed raw stream found no finalized HFTREC4 WS manifests");
    }

    let worker_count = worker_count.max(1).min(manifests.len());
    let next = AtomicUsize::new(0);
    let (tx, rx) = mpsc::channel::<(usize, Result<ParallelTypedRawScanResult>)>();
    let order_holdback_ns = market_replay_order_holdback_ns(&build_options);
    let mut stats = MarketReplayStats::default();
    let mut chunks = std::thread::scope(|scope| {
        for _ in 0..worker_count {
            let tx = tx.clone();
            let next = &next;
            let manifests = &manifests;
            let build_options = &build_options;
            let symbol_filter = &symbol_filter;
            scope.spawn(move || loop {
                let idx = next.fetch_add(1, AtomicOrdering::Relaxed);
                let Some((_manifest_idx, manifest)) = manifests.get(idx) else {
                    break;
                };
                let result =
                    scan_typed_updates_from_raw_manifest(manifest, build_options, symbol_filter);
                let _ = tx.send((idx, result));
            });
        }
        drop(tx);

        let mut chunks = (0..manifests.len()).map(|_| None).collect::<Vec<_>>();
        for received in rx {
            let (task_idx, result) = received;
            chunks[task_idx] = Some(result);
        }
        chunks
    });

    let mut profile = MarketReplayStreamProfile {
        worker_count,
        manifest_count: manifests.len(),
        ..MarketReplayStreamProfile::default()
    };
    let mut pending = BinaryHeap::<Reverse<OrderedTypedUpdateFromRaw>>::new();
    for idx in 0..chunks.len() {
        let result = chunks[idx]
            .take()
            .ok_or_else(|| anyhow!("parallel typed raw stream missing manifest task {idx}"))??;
        profile.add(&result.profile);
        for update in result.updates {
            pending.push(Reverse(update));
        }
        let flush_before_or_at_ts = manifests
            .get(idx + 1)
            .and_then(|(_, manifest)| manifest.min_ts_ns)
            .map(|ts| ts.saturating_sub(order_holdback_ns))
            .unwrap_or(i64::MAX);
        flush_ordered_typed_raw_updates_to_visit(
            &mut pending,
            &mut stats,
            flush_before_or_at_ts,
            &mut visit,
            &mut profile,
        )?;
    }

    Ok(MarketReplayStreamReport {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_TYPED_UPDATES_FORMAT.to_string(),
        row_count: stats.row_count,
        manifest_count: manifests.len(),
        min_visible_ts_ns: stats.min_visible_ts_ns,
        max_visible_ts_ns: stats.max_visible_ts_ns,
        event_type_counts: stats.event_type_counts,
        elapsed_ms: started.elapsed().as_millis(),
        profile,
    })
}

pub fn stream_market_replay_typed_updates_from_files<I, F>(
    paths: I,
    mut visit: F,
) -> Result<MarketReplayStreamReport>
where
    I: IntoIterator<Item = PathBuf>,
    F: FnMut(MarketReplayTypedUpdate) -> Result<()>,
{
    let started = Instant::now();
    let paths = paths.into_iter().collect::<Vec<_>>();
    if paths.is_empty() {
        bail!("typed update stream requires at least one file");
    }
    let mut readers = Vec::with_capacity(paths.len());
    let mut pending = BinaryHeap::<Reverse<OrderedTypedUpdate>>::new();
    for (file_index, path) in paths.iter().enumerate() {
        let mut reader = TypedUpdateReader::open(path)?;
        if let Some(update) = reader.read_update()? {
            pending.push(Reverse(OrderedTypedUpdate { file_index, update }));
        }
        readers.push(reader);
    }

    let mut stats = MarketReplayStats::default();
    let mut last_global_event_seq = None::<u64>;
    while let Some(Reverse(item)) = pending.pop() {
        if last_global_event_seq.is_some_and(|last| item.update.global_event_seq <= last) {
            bail!(
                "typed update files are not one monotonic build stream: global_event_seq {} after {:?}",
                item.update.global_event_seq,
                last_global_event_seq
            );
        }
        last_global_event_seq = Some(item.update.global_event_seq);
        let file_index = item.file_index;
        let update = item.update;
        stats.observe_typed_update(&update);
        visit(update)?;
        if let Some(next) = readers[file_index].read_update()? {
            pending.push(Reverse(OrderedTypedUpdate {
                file_index,
                update: next,
            }));
        }
    }
    Ok(MarketReplayStreamReport {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_TYPED_UPDATES_FORMAT.to_string(),
        row_count: stats.row_count,
        manifest_count: paths.len(),
        min_visible_ts_ns: stats.min_visible_ts_ns,
        max_visible_ts_ns: stats.max_visible_ts_ns,
        event_type_counts: stats.event_type_counts,
        elapsed_ms: started.elapsed().as_millis(),
        profile: MarketReplayStreamProfile::default(),
    })
}

pub fn discover_market_replay_typed_update_files(root: &Path) -> Result<Vec<PathBuf>> {
    discover_market_replay_typed_update_files_for_window(root, None, None)
}

pub fn discover_market_replay_typed_update_files_for_window(
    root: &Path,
    start_ts_ns: Option<i64>,
    end_ts_ns: Option<i64>,
) -> Result<Vec<PathBuf>> {
    if root.is_file() {
        return Ok(vec![root.to_path_buf()]);
    }
    if !root.exists() {
        bail!("typed update root does not exist: {}", root.display());
    }
    let mut files = Vec::new();
    collect_typed_update_files(root, &mut files)?;
    let start_bucket = start_ts_ns.map(|ts| ts.div_euclid(HOUR_NS));
    let end_bucket = end_ts_ns.map(|ts| ts.saturating_sub(1).div_euclid(HOUR_NS));
    files.retain(|path| {
        typed_update_path_hour_bucket(path).map_or(true, |bucket| {
            start_bucket.is_none_or(|start| bucket >= start)
                && end_bucket.is_none_or(|end| bucket <= end)
        })
    });
    files.sort();
    if files.is_empty() {
        bail!(
            "typed update root has no typed update shard files: {}",
            root.display()
        );
    }
    Ok(files)
}

pub fn build_market_replay_dataset(
    options: &BuildMarketReplayDatasetOptions,
) -> Result<MarketReplayDatasetBuildReport> {
    validate_market_replay_options(options)?;
    validate_market_replay_raw_coverage_options(options)?;
    let started = Instant::now();
    let catalog_path = options.dataset_root.join(MARKET_REPLAY_CATALOG);
    if catalog_path.exists() && !options.overwrite {
        let catalog = read_market_replay_catalog(&options.dataset_root)?;
        return Ok(MarketReplayDatasetBuildReport {
            schema_version: 1,
            dataset_format: MARKET_REPLAY_FORMAT.to_string(),
            dataset_root: options.dataset_root.clone(),
            catalog_path,
            reused_existing: true,
            row_count: catalog.row_count,
            manifest_count: 0,
            min_visible_ts_ns: catalog.min_visible_ts_ns,
            max_visible_ts_ns: catalog.max_visible_ts_ns,
            event_type_counts: catalog.event_type_counts,
            elapsed_ms: started.elapsed().as_millis(),
        });
    }

    let symbol_filter = SymbolFilter::new(&options.market_symbol_allowlist)?;
    let manifests = sorted_candidate_manifests(options)?;
    if options.dataset_root.exists() {
        if !options.overwrite {
            bail!(
                "market replay dataset root already exists; pass overwrite to replace: {}",
                options.dataset_root.display()
            );
        }
        fs::remove_dir_all(&options.dataset_root)
            .with_context(|| format!("remove {}", options.dataset_root.display()))?;
    }
    fs::create_dir_all(&options.dataset_root)
        .with_context(|| format!("create {}", options.dataset_root.display()))?;

    let events_path = options.dataset_root.join(MARKET_REPLAY_EVENTS_TABLE);
    let mut writer = ParquetTableStreamWriter::new(&events_path, Some("visible_ts_ns"))?;
    let part_limit = options
        .max_rows_per_part
        .unwrap_or(DEFAULT_MARKET_REPLAY_PART_ROWS)
        .max(1);
    let mut buffer = Vec::<MarketEvent>::with_capacity(part_limit.min(16_384));
    let mut pending_events = BinaryHeap::<Reverse<OrderedMarketEvent>>::new();
    let mut stats = MarketReplayStats::default();
    let mut consumed_manifests = 0usize;
    let order_holdback_ns = market_replay_order_holdback_ns(options);

    for manifest_idx in 0..manifests.len() {
        let manifest = &manifests[manifest_idx];
        if !manifest_overlaps(manifest, options) {
            continue;
        }
        consumed_manifests += 1;
        scan_hftrec4_segment_selected(
            &manifest.segment_path,
            |record| should_read_market_replay_payload(record, options, &symbol_filter),
            |record| {
                let raw_source_row_idx = record.row_idx;
                let raw_record_hash = source_record_hash(&manifest.segment_path, &record)?;
                if manifest.stream == REFERENCE_WS_RAW_STREAM {
                    let rows = market_events_from_reference_record(
                        record,
                        raw_source_row_idx,
                        &manifest.segment_path,
                        raw_record_hash,
                        options.reference_latency_ms,
                    )?;
                    for row in rows {
                        if event_symbol_disallowed(&row, &symbol_filter) {
                            continue;
                        }
                        pending_events.push(Reverse(OrderedMarketEvent::new(row)));
                    }
                    return Ok(());
                }
                let mut raw = hftrec4_record_to_ws_raw(record);
                if raw.condition_id.is_none() {
                    raw.condition_id = condition_id_from_payload(&raw.raw_payload);
                }
                if raw.asset_id.is_none() {
                    raw.asset_id = asset_id_from_payload(&raw.raw_payload);
                }
                let original_local_recv_ts_ns = raw.local_recv_ts_ns;
                apply_poly_visible_time_model(
                    &mut raw,
                    options.poly_server_visible_time,
                    options.poly_incremental_latency_ms,
                    options.poly_incremental_freshness_guard_ms,
                );
                let visible_ts_ns = raw.local_recv_ts_ns;
                if !ts_in_window(visible_ts_ns, options) {
                    return Ok(());
                }
                if raw_symbol_disallowed(&raw, &symbol_filter) {
                    return Ok(());
                }
                let rows = market_events_from_ws_raw(
                    raw,
                    raw_source_row_idx,
                    original_local_recv_ts_ns,
                    visible_ts_ns,
                    &manifest.segment_path,
                    raw_record_hash,
                )?;
                for row in rows {
                    if event_symbol_disallowed(&row, &symbol_filter) {
                        continue;
                    }
                    pending_events.push(Reverse(OrderedMarketEvent::new(row)));
                }
                Ok(())
            },
        )
        .with_context(|| {
            format!(
                "read HFTREC4 market replay segment {}",
                manifest.segment_path.display()
            )
        })?;
        let next_manifest_min_ts_ns = manifests
            .iter()
            .skip(manifest_idx + 1)
            .find(|candidate| manifest_overlaps(candidate, options))
            .and_then(|candidate| candidate.min_ts_ns);
        if let Some(next_min_ts) = next_manifest_min_ts_ns {
            flush_ordered_events(
                &mut pending_events,
                &mut writer,
                &mut buffer,
                &mut stats,
                part_limit,
                next_min_ts.saturating_sub(order_holdback_ns),
            )?;
        }
    }

    if consumed_manifests == 0 {
        bail!("build-market-replay-dataset found no finalized HFTREC4 WS manifests");
    }
    flush_ordered_events(
        &mut pending_events,
        &mut writer,
        &mut buffer,
        &mut stats,
        part_limit,
        i64::MAX,
    )?;
    if !buffer.is_empty() {
        writer.write_rows(&buffer)?;
        buffer.clear();
    }
    let table_report = writer.finish()?;
    let events_table_hash = hash_path(&events_path)?;
    let catalog = MarketReplayDatasetCatalog {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_FORMAT.to_string(),
        dataset_root: options.dataset_root.clone(),
        events_table: MARKET_REPLAY_EVENTS_TABLE.to_string(),
        raw_roots: options.raw_roots.clone(),
        raw_start_ts_ns: options.raw_start_ts_ns,
        raw_end_ts_ns: options.raw_end_ts_ns,
        market_symbol_allowlist: options.market_symbol_allowlist.clone(),
        poly_server_visible_time: options.poly_server_visible_time,
        poly_incremental_latency_ms: options.poly_incremental_latency_ms,
        poly_incremental_freshness_guard_ms: options.poly_incremental_freshness_guard_ms,
        reference_latency_ms: options.reference_latency_ms,
        row_count: stats.row_count,
        manifest_count: consumed_manifests,
        min_visible_ts_ns: stats.min_visible_ts_ns,
        max_visible_ts_ns: stats.max_visible_ts_ns,
        event_type_counts: stats.event_type_counts.clone(),
        events_table_hash,
        elapsed_ms: started.elapsed().as_millis(),
    };
    write_json_file_pretty(&catalog_path, &catalog)?;

    Ok(MarketReplayDatasetBuildReport {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_FORMAT.to_string(),
        dataset_root: options.dataset_root.clone(),
        catalog_path,
        reused_existing: false,
        row_count: table_report.row_count,
        manifest_count: consumed_manifests,
        min_visible_ts_ns: stats.min_visible_ts_ns,
        max_visible_ts_ns: stats.max_visible_ts_ns,
        event_type_counts: stats.event_type_counts,
        elapsed_ms: started.elapsed().as_millis(),
    })
}

pub fn read_market_replay_catalog(dataset_root: &Path) -> Result<MarketReplayDatasetCatalog> {
    let path = dataset_root.join(MARKET_REPLAY_CATALOG);
    let catalog: MarketReplayDatasetCatalog = serde_json::from_reader(
        fs::File::open(&path).with_context(|| format!("open {}", path.display()))?,
    )
    .with_context(|| format!("parse {}", path.display()))?;
    if catalog.dataset_format != MARKET_REPLAY_FORMAT {
        bail!(
            "unsupported market replay dataset format {}",
            catalog.dataset_format
        );
    }
    Ok(catalog)
}

pub fn validate_market_replay_raw_coverage(
    options: &StreamMarketReplayEventsOptions,
) -> Result<MarketReplayCoverageReport> {
    let build_options = BuildMarketReplayDatasetOptions::from(options);
    validate_market_replay_options(&build_options)?;
    validate_market_replay_raw_coverage_options(&build_options)
}

pub fn for_each_market_replay_event<F>(dataset_root: &Path, mut visit: F) -> Result<()>
where
    F: FnMut(MarketEvent) -> Result<()>,
{
    let catalog = read_market_replay_catalog(dataset_root)?;
    for_each_parquet_table_row::<MarketEvent, _>(&dataset_root.join(catalog.events_table), |row| {
        visit(row)
    })
}

pub fn bench_market_replay_dataset(dataset_root: &Path) -> Result<MarketReplayDatasetBenchReport> {
    let started = Instant::now();
    let mut state = StreamingMarketReplayState::new();
    let mut row_count = 0usize;
    let mut min_visible_ts_ns = None::<i64>;
    let mut max_visible_ts_ns = None::<i64>;
    let mut last_sort_key = None::<MarketEventSortKey>;
    for_each_market_replay_event(dataset_root, |event| {
        let sort_key = MarketEventSortKey::from_event(&event);
        if let Some(last) = &last_sort_key {
            if sort_key < *last {
                bail!(
                    "market replay events are not ordered: previous={:?} current={:?}",
                    last,
                    sort_key
                );
            }
        }
        last_sort_key = Some(sort_key);
        min_visible_ts_ns =
            Some(min_visible_ts_ns.map_or(event.visible_ts_ns, |ts| ts.min(event.visible_ts_ns)));
        max_visible_ts_ns =
            Some(max_visible_ts_ns.map_or(event.visible_ts_ns, |ts| ts.max(event.visible_ts_ns)));
        state.apply_event(&event)?;
        row_count += 1;
        Ok(())
    })?;
    let elapsed_ms = started.elapsed().as_millis();
    let rows_per_sec = if elapsed_ms == 0 {
        0.0
    } else {
        row_count as f64 / (elapsed_ms as f64 / 1000.0)
    };
    Ok(MarketReplayDatasetBenchReport {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_FORMAT.to_string(),
        dataset_root: dataset_root.to_path_buf(),
        row_count,
        book_count: state.book_count(),
        min_visible_ts_ns,
        max_visible_ts_ns,
        elapsed_ms,
        rows_per_sec,
    })
}

pub fn decode_market_replay_typed_update(
    update: &MarketReplayRawUpdate,
    payload: &Value,
) -> Result<MarketReplayTypedUpdate> {
    let body = match update.event_type.as_str() {
        "book" => {
            let asset_id = update
                .asset_id
                .clone()
                .ok_or_else(|| anyhow!("raw book update missing asset_id"))?;
            MarketReplayTypedUpdateBody::Book {
                asset_id,
                bids: parse_book_levels_typed(payload, &["bids", "buys"])?,
                asks: parse_book_levels_typed(payload, &["asks", "sells"])?,
            }
        }
        "price_change" => {
            let mut changes = Vec::new();
            for change in payload
                .get("price_changes")
                .or_else(|| payload.get("changes"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let Some(side) = parse_ws_side_code(change) else {
                    continue;
                };
                let Some(price_micros) = decimal_micros(change, &["price"], "price_change price")?
                else {
                    continue;
                };
                let Some(qty_micros) =
                    decimal_micros(change, &["size", "qty", "quantity"], "price_change size")?
                else {
                    continue;
                };
                let asset_id = string_value(
                    change,
                    &["asset_id", "assetId", "asset", "token_id", "tokenId"],
                )
                .or_else(|| update.asset_id.clone());
                let Some(asset_id) = asset_id else {
                    continue;
                };
                changes.push(MarketReplayLevelChange {
                    asset_id,
                    side,
                    price_micros,
                    qty_micros,
                });
            }
            MarketReplayTypedUpdateBody::PriceChanges { changes }
        }
        _ => MarketReplayTypedUpdateBody::Other,
    };
    Ok(MarketReplayTypedUpdate {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_FORMAT.to_string(),
        global_event_seq: update.global_event_seq,
        symbol: update.symbol.clone(),
        horizon_seconds: update.horizon_seconds,
        condition_id: update.condition_id.clone(),
        event_type: update.event_type.clone(),
        original_local_recv_ts_ns: update.original_local_recv_ts_ns,
        visible_ts_ns: update.visible_ts_ns,
        ingest_seq: update.ingest_seq,
        source_row_idx: update.source_row_idx,
        payload_hash: update.payload_hash.clone(),
        market_start_ts_ns: update.raw.market_start_ts_ns,
        market_end_ts_ns: update.raw.market_end_ts_ns,
        body,
    })
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(untagged)]
enum FastJsonScalar<'a> {
    Str(&'a str),
    I64(i64),
    U64(u64),
    F64(f64),
}

#[derive(Debug, Deserialize)]
struct FastBookPayload<'a> {
    #[serde(default, borrow)]
    bids: Option<Vec<FastBookLevel<'a>>>,
    #[serde(default, borrow)]
    buys: Option<Vec<FastBookLevel<'a>>>,
    #[serde(default, borrow)]
    asks: Option<Vec<FastBookLevel<'a>>>,
    #[serde(default, borrow)]
    sells: Option<Vec<FastBookLevel<'a>>>,
    #[serde(default, borrow)]
    condition_id: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow, rename = "conditionId")]
    condition_id_camel: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    market: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    asset_id: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow, rename = "assetId")]
    asset_id_camel: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    asset: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    token_id: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow, rename = "tokenId")]
    token_id_camel: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    price_changes: Option<Vec<FastPriceChange<'a>>>,
    #[serde(default, borrow)]
    changes: Option<Vec<FastPriceChange<'a>>>,
}

#[derive(Debug, Deserialize)]
struct FastBookLevel<'a> {
    #[serde(default, borrow)]
    price: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    p: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    size: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    qty: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    quantity: Option<FastJsonScalar<'a>>,
}

#[derive(Debug, Deserialize)]
struct FastPriceChangePayload<'a> {
    #[serde(default, borrow)]
    price_changes: Option<Vec<FastPriceChange<'a>>>,
    #[serde(default, borrow)]
    changes: Option<Vec<FastPriceChange<'a>>>,
    #[serde(default, borrow)]
    timestamp: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    ts: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    time: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    exchange_ts_ms: Option<FastJsonScalar<'a>>,
}

#[derive(Debug, Deserialize)]
struct FastPriceChange<'a> {
    #[serde(default, borrow)]
    side: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    side_type: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    book_side: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    price: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    size: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    qty: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    quantity: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    asset_id: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow, rename = "assetId")]
    asset_id_camel: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    asset: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow)]
    token_id: Option<FastJsonScalar<'a>>,
    #[serde(default, borrow, rename = "tokenId")]
    token_id_camel: Option<FastJsonScalar<'a>>,
}

impl<'a> FastBookPayload<'a> {
    fn condition_id_from_payload(&self) -> Option<String> {
        fast_scalar_str_only([self.condition_id, self.condition_id_camel, self.market])
    }

    fn asset_id_from_payload(&self) -> Option<String> {
        fast_scalar_str_only([
            self.asset_id,
            self.asset_id_camel,
            self.asset,
            self.token_id,
            self.token_id_camel,
        ])
        .or_else(|| {
            self.price_changes
                .as_deref()
                .or(self.changes.as_deref())
                .into_iter()
                .flatten()
                .find_map(FastPriceChange::asset_id_str_only)
        })
    }
}

impl<'a> FastPriceChangePayload<'a> {
    fn timestamp_ms(&self) -> Option<i64> {
        [self.timestamp, self.ts, self.time, self.exchange_ts_ms]
            .into_iter()
            .flatten()
            .find_map(fast_timestamp_ms)
    }
}

impl<'a> FastPriceChange<'a> {
    fn side_code(&self) -> Option<ReplayBookSide> {
        let side = fast_scalar_string([self.side, self.side_type, self.book_side])?;
        match side.trim().to_ascii_uppercase().as_str() {
            "BUY" | "BID" | "BIDS" => Some(ReplayBookSide::Bid),
            "SELL" | "ASK" | "ASKS" | "OFFER" | "OFFERS" => Some(ReplayBookSide::Ask),
            _ => None,
        }
    }

    fn asset_id(&self) -> Option<String> {
        fast_scalar_string([
            self.asset_id,
            self.asset_id_camel,
            self.asset,
            self.token_id,
            self.token_id_camel,
        ])
    }

    fn asset_id_str_only(&self) -> Option<String> {
        fast_scalar_str_only([
            self.asset_id,
            self.asset_id_camel,
            self.asset,
            self.token_id,
            self.token_id_camel,
        ])
    }
}

fn fast_scalar_str_only<const N: usize>(values: [Option<FastJsonScalar<'_>>; N]) -> Option<String> {
    values.into_iter().flatten().find_map(|value| match value {
        FastJsonScalar::Str(raw) => Some(raw.to_string()),
        FastJsonScalar::I64(_) | FastJsonScalar::U64(_) | FastJsonScalar::F64(_) => None,
    })
}

fn fast_scalar_string<const N: usize>(values: [Option<FastJsonScalar<'_>>; N]) -> Option<String> {
    values.into_iter().flatten().find_map(|value| match value {
        FastJsonScalar::Str(raw) => Some(raw.to_string()),
        FastJsonScalar::I64(raw) => Some(raw.to_string()),
        FastJsonScalar::U64(raw) => i64::try_from(raw).ok().map(|value| value.to_string()),
        FastJsonScalar::F64(_) => None,
    })
}

fn fast_timestamp_ms(value: FastJsonScalar<'_>) -> Option<i64> {
    match value {
        FastJsonScalar::I64(raw) => Some(normalize_exchange_ts_ms(raw)),
        FastJsonScalar::U64(raw) => i64::try_from(raw).ok().map(normalize_exchange_ts_ms),
        FastJsonScalar::Str(raw) => raw.parse::<i64>().ok().map(normalize_exchange_ts_ms),
        FastJsonScalar::F64(_) => None,
    }
}

fn fast_decimal_micros(
    values: &[Option<FastJsonScalar<'_>>],
    field_name: &str,
) -> Result<Option<i64>> {
    let Some(raw) = values.iter().copied().flatten().next() else {
        return Ok(None);
    };
    let number = match raw {
        FastJsonScalar::Str(raw) => raw
            .parse::<f64>()
            .with_context(|| format!("parse {field_name}"))?,
        FastJsonScalar::I64(raw) => raw as f64,
        FastJsonScalar::U64(raw) => raw as f64,
        FastJsonScalar::F64(raw) => raw,
    };
    if !number.is_finite() {
        bail!("{field_name} must be finite");
    }
    Ok(Some(micros(number)))
}

fn fast_book_levels(
    levels: Option<&[FastBookLevel<'_>]>,
    field_name: &str,
) -> Result<Vec<ReplayBookLevel>> {
    let Some(levels) = levels else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(levels.len());
    for level in levels {
        let Some(price_micros) = fast_decimal_micros(&[level.price, level.p], field_name)? else {
            continue;
        };
        let Some(qty_micros) =
            fast_decimal_micros(&[level.size, level.qty, level.quantity], "book level size")?
        else {
            continue;
        };
        out.push(ReplayBookLevel {
            price_micros,
            qty_micros,
        });
    }
    Ok(out)
}

fn raw_update_payload_value(update: &MarketReplayRawUpdate) -> Result<Value> {
    if update.raw.raw_payload.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice::<Value>(&update.raw.raw_payload).with_context(|| {
        format!(
            "parse raw market replay payload at ingest_seq {}",
            update.raw.ingest_seq
        )
    })
}

fn required_asset_id(event: &MarketEvent) -> Result<String> {
    event.asset_id.clone().ok_or_else(|| {
        anyhow!(
            "market event {} at {} missing asset_id",
            event.event_type,
            event.visible_ts_ns
        )
    })
}

fn empty_book_from_event(asset_id: String, event: &MarketEvent) -> ReplayBookState {
    ReplayBookState {
        asset_id,
        condition_id: event.condition_id.clone(),
        symbol: event.symbol.clone(),
        horizon_seconds: event.horizon_seconds,
        market_start_ts_ns: None,
        market_end_ts_ns: None,
        last_visible_ts_ns: event.visible_ts_ns,
        last_local_recv_ts_ns: event.local_recv_ts_ns,
        bids: BTreeMap::new(),
        asks: BTreeMap::new(),
    }
}

fn apply_level_event(book: &mut ReplayBookState, event: &MarketEvent) -> Result<()> {
    let side = event.side.as_deref().ok_or_else(|| {
        anyhow!(
            "market event {} at {} missing side",
            event.event_type,
            event.visible_ts_ns
        )
    })?;
    let price = event.price_micros.ok_or_else(|| {
        anyhow!(
            "market event {} at {} missing price",
            event.event_type,
            event.visible_ts_ns
        )
    })?;
    let qty = event.qty_micros.ok_or_else(|| {
        anyhow!(
            "market event {} at {} missing qty",
            event.event_type,
            event.visible_ts_ns
        )
    })?;
    apply_level_to_book(book, side, price, qty)?;
    book.last_visible_ts_ns = event.visible_ts_ns;
    book.last_local_recv_ts_ns = event.local_recv_ts_ns;
    if book.condition_id.is_none() {
        book.condition_id = event.condition_id.clone();
    }
    if book.symbol.is_none() {
        book.symbol = event.symbol.clone();
    }
    if book.horizon_seconds.is_none() {
        book.horizon_seconds = event.horizon_seconds;
    }
    Ok(())
}

fn apply_level_to_book(book: &mut ReplayBookState, side: &str, price: i64, qty: i64) -> Result<()> {
    let levels = match side {
        "BID" => &mut book.bids,
        "ASK" => &mut book.asks,
        other => bail!("unsupported market event side {other}"),
    };
    if price <= 0 || qty <= 0 {
        levels.remove(&price);
    } else {
        levels.insert(price, qty);
    }
    Ok(())
}

fn apply_level_to_book_side(
    book: &mut ReplayBookState,
    side: ReplayBookSide,
    price: i64,
    qty: i64,
) -> Result<()> {
    let levels = match side {
        ReplayBookSide::Bid => &mut book.bids,
        ReplayBookSide::Ask => &mut book.asks,
    };
    if price <= 0 || qty <= 0 {
        levels.remove(&price);
    } else {
        levels.insert(price, qty);
    }
    Ok(())
}

pub(crate) fn hftrec4_record_to_ws_raw(record: Hftrec4Record) -> RawPolymarketClobWsEvent {
    let outcome = infer_outcome_from_assets(
        record.asset_id.as_deref(),
        record.yes_asset_id.as_deref(),
        record.no_asset_id.as_deref(),
    );
    RawPolymarketClobWsEvent {
        source_id: WS_RAW_STREAM.to_string(),
        ingest_seq_scope: WS_RAW_STREAM.to_string(),
        ingest_seq: record.ingest_seq,
        local_recv_ts_ns: record.local_recv_ts_ns,
        asset_id: record.asset_id,
        condition_id: record.condition_id,
        symbol: record.symbol,
        outcome,
        market_start_ts_ns: record.market_start_ts_ns,
        market_end_ts_ns: record.market_end_ts_ns,
        yes_asset_id: record.yes_asset_id,
        no_asset_id: record.no_asset_id,
        event_type: record.event_type,
        exchange_ts_ms: None,
        raw_payload_sha256: record.payload_sha256,
        raw_payload: record.payload,
    }
}

pub(crate) fn apply_poly_visible_time_model(
    raw: &mut RawPolymarketClobWsEvent,
    enabled: bool,
    poly_incremental_latency_ms: i64,
    poly_incremental_freshness_guard_ms: i64,
) {
    if !enabled {
        return;
    }
    if !raw.event_type.eq_ignore_ascii_case("price_change") {
        return;
    }
    let Some(exchange_ts_ms) = raw
        .exchange_ts_ms
        .or_else(|| timestamp_ms_for_raw_payload(&raw.raw_payload))
    else {
        return;
    };
    raw.exchange_ts_ms = Some(exchange_ts_ms);
    let Some(synthetic_ts_ns) = exchange_ts_ms
        .checked_add(poly_incremental_latency_ms)
        .and_then(|ts_ms| ts_ms.checked_mul(1_000_000))
    else {
        return;
    };
    let original_recv_ts_ns = raw.local_recv_ts_ns;
    if synthetic_ts_ns > original_recv_ts_ns {
        return;
    }
    let guard_ns = poly_incremental_freshness_guard_ms.saturating_mul(1_000_000);
    if original_recv_ts_ns.saturating_sub(synthetic_ts_ns) <= guard_ns {
        raw.local_recv_ts_ns = synthetic_ts_ns;
    }
}

pub(crate) fn timestamp_ms_for_raw_payload(raw_payload: &[u8]) -> Option<i64> {
    let value = serde_json::from_slice::<Value>(raw_payload).ok()?;
    for key in ["timestamp", "ts", "time", "exchange_ts_ms"] {
        let Some(field) = value.get(key) else {
            continue;
        };
        if let Some(raw) = field.as_i64() {
            return Some(normalize_exchange_ts_ms(raw));
        }
        if let Some(raw) = field.as_u64().and_then(|raw| i64::try_from(raw).ok()) {
            return Some(normalize_exchange_ts_ms(raw));
        }
        if let Some(raw) = field.as_str().and_then(|raw| raw.parse::<i64>().ok()) {
            return Some(normalize_exchange_ts_ms(raw));
        }
    }
    None
}

pub(crate) fn condition_id_from_payload(payload: &[u8]) -> Option<String> {
    string_from_payload(payload, &["condition_id", "conditionId", "market"])
}

pub(crate) fn asset_id_from_payload(payload: &[u8]) -> Option<String> {
    string_from_payload(
        payload,
        &["asset_id", "assetId", "asset", "token_id", "tokenId"],
    )
    .or_else(|| {
        let value = serde_json::from_slice::<serde_json::Value>(payload).ok()?;
        value
            .get("price_changes")
            .or_else(|| value.get("changes"))?
            .as_array()?
            .iter()
            .find_map(|change| {
                ["asset_id", "assetId", "asset", "token_id", "tokenId"]
                    .iter()
                    .find_map(|field| change.get(*field)?.as_str().map(str::to_string))
            })
    })
}

fn market_events_from_ws_raw(
    raw: RawPolymarketClobWsEvent,
    source_row_idx: u64,
    original_local_recv_ts_ns: i64,
    visible_ts_ns: i64,
    source_segment: &Path,
    raw_record_hash: String,
) -> Result<Vec<MarketEvent>> {
    let event_type = raw.event_type.trim().to_ascii_lowercase();
    let payload = if raw.raw_payload.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&raw.raw_payload)
            .with_context(|| format!("parse WS raw payload at ingest_seq {}", raw.ingest_seq))?
    };
    match event_type.as_str() {
        "book" => market_book_events(
            &raw,
            source_row_idx,
            original_local_recv_ts_ns,
            visible_ts_ns,
            source_segment,
            raw_record_hash,
            &payload,
        ),
        "price_change" => market_price_change_events(
            &raw,
            source_row_idx,
            original_local_recv_ts_ns,
            visible_ts_ns,
            source_segment,
            raw_record_hash,
            &payload,
        ),
        _ => Ok(Vec::new()),
    }
}

fn market_book_events(
    raw: &RawPolymarketClobWsEvent,
    source_row_idx: u64,
    original_local_recv_ts_ns: i64,
    visible_ts_ns: i64,
    source_segment: &Path,
    raw_record_hash: String,
    payload: &Value,
) -> Result<Vec<MarketEvent>> {
    let mut out = Vec::new();
    let context = EventContext::from_raw(
        raw,
        source_row_idx,
        original_local_recv_ts_ns,
        visible_ts_ns,
        source_segment,
        raw_record_hash,
        payload,
    );
    out.push(context.event("book_snapshot_start", None, None, None, "snapshot_start", 0));
    let mut sequence = 1u64;
    for (side, price_micros, qty_micros) in parse_book_levels(payload, &["bids", "buys"], "BID")? {
        out.push(context.event(
            "book_snapshot_level",
            Some(side),
            Some(price_micros),
            Some(qty_micros),
            "snapshot_level",
            sequence,
        ));
        sequence += 1;
    }
    for (side, price_micros, qty_micros) in parse_book_levels(payload, &["asks", "sells"], "ASK")? {
        out.push(context.event(
            "book_snapshot_level",
            Some(side),
            Some(price_micros),
            Some(qty_micros),
            "snapshot_level",
            sequence,
        ));
        sequence += 1;
    }
    Ok(out)
}

fn market_price_change_events(
    raw: &RawPolymarketClobWsEvent,
    source_row_idx: u64,
    original_local_recv_ts_ns: i64,
    visible_ts_ns: i64,
    source_segment: &Path,
    raw_record_hash: String,
    payload: &Value,
) -> Result<Vec<MarketEvent>> {
    let Some(changes) = payload
        .get("price_changes")
        .or_else(|| payload.get("changes"))
        .and_then(Value::as_array)
    else {
        return Ok(Vec::new());
    };
    let context = EventContext::from_raw(
        raw,
        source_row_idx,
        original_local_recv_ts_ns,
        visible_ts_ns,
        source_segment,
        raw_record_hash,
        payload,
    );
    let mut out = Vec::new();
    for (idx, change) in changes.iter().enumerate() {
        let Some(side) = parse_ws_side(change) else {
            continue;
        };
        let Some(price_micros) = decimal_micros(change, &["price"], "price_change price")? else {
            continue;
        };
        let Some(qty_micros) =
            decimal_micros(change, &["size", "qty", "quantity"], "price_change size")?
        else {
            continue;
        };
        let asset_id = string_value(
            change,
            &["asset_id", "assetId", "asset", "token_id", "tokenId"],
        )
        .or_else(|| raw.asset_id.clone());
        let mut event = context.event(
            "depth_delta",
            Some(side),
            Some(price_micros),
            Some(qty_micros),
            "delta",
            idx as u64,
        );
        event.asset_id = asset_id;
        out.push(event);
    }
    Ok(out)
}

fn market_events_from_reference_record(
    record: Hftrec4Record,
    source_row_idx: u64,
    source_segment: &Path,
    raw_record_hash: String,
    reference_latency_ms: i64,
) -> Result<Vec<MarketEvent>> {
    if record.event_type != "reference_bar" {
        return Ok(Vec::new());
    }
    let raw: RawReferenceReplayEvent =
        serde_json::from_slice(&record.payload).context("decode reference raw payload")?;
    if raw.is_closed != Some(true) {
        return Ok(Vec::new());
    }
    let Some(close) = raw.close.as_deref().and_then(decimal_str_micros) else {
        return Ok(Vec::new());
    };
    let Some(bar_close_visible_base_ms) = raw
        .bar_open_time_ms
        .and_then(|open| open.checked_add(1_000))
        .or_else(|| raw.bar_close_time_ms.and_then(|close| close.checked_add(1)))
    else {
        return Ok(Vec::new());
    };
    let visible_ts_ns = bar_close_visible_base_ms
        .checked_add(reference_latency_ms)
        .and_then(|ts_ms| ts_ms.checked_mul(1_000_000))
        .context("reference visible timestamp overflow")?;
    let exchange_ts_ns = raw
        .exchange_event_ts_ms
        .and_then(|ts_ms| ts_ms.checked_mul(1_000_000));
    Ok(vec![MarketEvent {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_FORMAT.to_string(),
        global_event_seq: 0,
        source: "reference_ws".to_string(),
        venue: raw.venue,
        stream: REFERENCE_WS_RAW_STREAM.to_string(),
        symbol: Some(raw.symbol.to_ascii_uppercase()),
        horizon_seconds: None,
        condition_id: None,
        asset_id: None,
        event_type: "reference_bar".to_string(),
        exchange_ts_ns,
        local_recv_ts_ns: record.local_recv_ts_ns,
        visible_ts_ns,
        sequence: 0,
        ingest_seq: record.ingest_seq,
        source_row_idx,
        source_segment: source_segment.display().to_string(),
        side: None,
        price_micros: Some(close),
        qty_micros: None,
        order_id_or_seq: Some(record.ingest_seq.to_string()),
        raw_record_hash,
        payload_hash: record.payload_sha256,
        flags: "closed".to_string(),
    }])
}

fn push_market_event_row(
    writer: &mut ParquetTableStreamWriter,
    buffer: &mut Vec<MarketEvent>,
    stats: &mut MarketReplayStats,
    mut row: MarketEvent,
    part_limit: usize,
) -> Result<()> {
    row.global_event_seq = stats.row_count as u64;
    stats.observe(&row);
    buffer.push(row);
    if buffer.len() >= part_limit {
        writer.write_rows(buffer)?;
        buffer.clear();
    }
    Ok(())
}

fn flush_ordered_events(
    pending_events: &mut BinaryHeap<Reverse<OrderedMarketEvent>>,
    writer: &mut ParquetTableStreamWriter,
    buffer: &mut Vec<MarketEvent>,
    stats: &mut MarketReplayStats,
    part_limit: usize,
    flush_before_or_at_ts: i64,
) -> Result<()> {
    while pending_events
        .peek()
        .is_some_and(|Reverse(item)| item.key.visible_ts_ns <= flush_before_or_at_ts)
    {
        let Reverse(item) = pending_events.pop().expect("peek checked");
        push_market_event_row(writer, buffer, stats, item.event, part_limit)?;
    }
    Ok(())
}

fn flush_ordered_events_to_visit<F>(
    pending_events: &mut BinaryHeap<Reverse<OrderedMarketEvent>>,
    stats: &mut MarketReplayStats,
    flush_before_or_at_ts: i64,
    visit: &mut F,
) -> Result<()>
where
    F: FnMut(MarketEvent) -> Result<()>,
{
    while pending_events
        .peek()
        .is_some_and(|Reverse(item)| item.key.visible_ts_ns <= flush_before_or_at_ts)
    {
        let Reverse(item) = pending_events.pop().expect("peek checked");
        let mut event = item.event;
        event.global_event_seq = stats.row_count as u64;
        stats.observe(&event);
        visit(event)?;
    }
    Ok(())
}

fn flush_ordered_raw_updates_to_visit<F>(
    pending_updates: &mut BinaryHeap<Reverse<OrderedMarketReplayRawUpdate>>,
    stats: &mut MarketReplayStats,
    flush_before_or_at_ts: i64,
    visit: &mut F,
) -> Result<()>
where
    F: FnMut(MarketReplayRawUpdate) -> Result<()>,
{
    while pending_updates
        .peek()
        .is_some_and(|Reverse(item)| item.key.visible_ts_ns <= flush_before_or_at_ts)
    {
        let Reverse(item) = pending_updates.pop().expect("peek checked");
        let mut update = item.update;
        update.global_event_seq = stats.row_count as u64;
        stats.observe_raw_update(&update);
        visit(update)?;
    }
    Ok(())
}

fn market_replay_order_holdback_ns(options: &BuildMarketReplayDatasetOptions) -> i64 {
    let poly_holdback_ns = if options.poly_server_visible_time {
        options
            .poly_incremental_freshness_guard_ms
            .saturating_mul(1_000_000)
    } else {
        0
    };
    let reference_holdback_ns = DEFAULT_REFERENCE_ORDER_HOLDBACK_MS.saturating_mul(1_000_000);
    poly_holdback_ns.max(reference_holdback_ns)
}

#[derive(Debug, Clone)]
struct OrderedMarketReplayRawUpdate {
    key: MarketEventSortKey,
    update: MarketReplayRawUpdate,
}

impl OrderedMarketReplayRawUpdate {
    fn new(update: MarketReplayRawUpdate) -> Self {
        Self {
            key: MarketEventSortKey {
                visible_ts_ns: update.visible_ts_ns,
                local_recv_ts_ns: update.original_local_recv_ts_ns,
                ingest_seq: update.ingest_seq,
                source_segment: update.source_segment.clone(),
                source_row_idx: update.source_row_idx,
                sequence: 0,
                asset_id: update.asset_id.clone().unwrap_or_default(),
                event_type: update.event_type.clone(),
            },
            update,
        }
    }
}

impl Eq for OrderedMarketReplayRawUpdate {}

impl PartialEq for OrderedMarketReplayRawUpdate {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Ord for OrderedMarketReplayRawUpdate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for OrderedMarketReplayRawUpdate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
struct OrderedMarketEvent {
    key: MarketEventSortKey,
    event: MarketEvent,
}

impl OrderedMarketEvent {
    fn new(event: MarketEvent) -> Self {
        Self {
            key: MarketEventSortKey::from_event(&event),
            event,
        }
    }
}

impl Eq for OrderedMarketEvent {}

impl PartialEq for OrderedMarketEvent {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Ord for OrderedMarketEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for OrderedMarketEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct MarketEventSortKey {
    visible_ts_ns: i64,
    local_recv_ts_ns: i64,
    ingest_seq: u64,
    source_segment: String,
    source_row_idx: u64,
    sequence: u64,
    asset_id: String,
    event_type: String,
}

impl MarketEventSortKey {
    fn from_event(event: &MarketEvent) -> Self {
        Self {
            visible_ts_ns: event.visible_ts_ns,
            local_recv_ts_ns: event.local_recv_ts_ns,
            ingest_seq: event.ingest_seq,
            source_segment: event.source_segment.clone(),
            source_row_idx: event.source_row_idx,
            sequence: event.sequence,
            asset_id: event.asset_id.clone().unwrap_or_default(),
            event_type: event.event_type.clone(),
        }
    }

    fn from_raw_update(update: &MarketReplayRawUpdate) -> Self {
        Self {
            visible_ts_ns: update.visible_ts_ns,
            local_recv_ts_ns: update.original_local_recv_ts_ns,
            ingest_seq: update.ingest_seq,
            source_segment: update.source_segment.clone(),
            source_row_idx: update.source_row_idx,
            sequence: 0,
            asset_id: update.asset_id.clone().unwrap_or_default(),
            event_type: update.event_type.clone(),
        }
    }
}

fn should_read_market_replay_payload(
    record: &Hftrec4RecordMeta,
    options: &BuildMarketReplayDatasetOptions,
    symbol_filter: &SymbolFilter,
) -> Result<bool> {
    if !ts_before_end(record.local_recv_ts_ns, options) {
        return Ok(false);
    }
    let event_type = record.event_type.trim().to_ascii_lowercase();
    if event_type != "book" && event_type != "price_change" && event_type != "reference_bar" {
        return Ok(false);
    }
    if event_type == "reference_bar" {
        return Ok(!meta_reference_symbol_disallowed(record, symbol_filter));
    }
    if meta_symbol_disallowed(record, symbol_filter) {
        return Ok(false);
    }
    Ok(true)
}

fn should_read_market_replay_raw_update(
    record: &Hftrec4RecordMeta,
    options: &BuildMarketReplayDatasetOptions,
    symbol_filter: &SymbolFilter,
) -> Result<bool> {
    if !ts_before_end(record.local_recv_ts_ns, options) {
        return Ok(false);
    }
    let event_type = record.event_type.trim().to_ascii_lowercase();
    if event_type != "book" && event_type != "price_change" {
        return Ok(false);
    }
    if meta_symbol_disallowed(record, symbol_filter) {
        return Ok(false);
    }
    Ok(true)
}

fn market_replay_raw_update_from_record(
    record: Hftrec4Record,
    source_segment: &Path,
    options: &BuildMarketReplayDatasetOptions,
    symbol_filter: &SymbolFilter,
) -> Result<Option<MarketReplayRawUpdate>> {
    let source_row_idx = record.row_idx;
    let original_local_recv_ts_ns = record.local_recv_ts_ns;
    let payload_hash = record.payload_sha256.clone();
    let mut raw = hftrec4_record_to_ws_raw(record);
    let event_type = raw.event_type.trim().to_ascii_lowercase();
    if event_type == "book" && raw.condition_id.is_none() {
        raw.condition_id = condition_id_from_payload(&raw.raw_payload);
    }
    if event_type == "book" && raw.asset_id.is_none() {
        raw.asset_id = asset_id_from_payload(&raw.raw_payload);
    }
    if raw_symbol_disallowed(&raw, symbol_filter) {
        return Ok(None);
    }
    apply_poly_visible_time_model(
        &mut raw,
        options.poly_server_visible_time,
        options.poly_incremental_latency_ms,
        options.poly_incremental_freshness_guard_ms,
    );
    let visible_ts_ns = raw.local_recv_ts_ns;
    if !ts_in_window(visible_ts_ns, options) {
        return Ok(None);
    }
    let symbol = raw.symbol.as_deref().and_then(canonical_market_symbol);
    let horizon_seconds = horizon_seconds(
        raw.market_start_ts_ns,
        raw.market_end_ts_ns,
        symbol.as_deref().or(raw.symbol.as_deref()),
    );
    Ok(Some(MarketReplayRawUpdate {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_FORMAT.to_string(),
        global_event_seq: 0,
        source: "polymarket_ws".to_string(),
        stream: WS_RAW_STREAM.to_string(),
        symbol,
        horizon_seconds,
        condition_id: raw.condition_id.clone(),
        asset_id: raw.asset_id.clone(),
        event_type,
        original_local_recv_ts_ns,
        visible_ts_ns,
        ingest_seq: raw.ingest_seq,
        source_row_idx,
        source_segment: source_segment.display().to_string(),
        raw_record_hash: format!("hftrec4:{}:{}", source_segment.display(), source_row_idx),
        payload_hash,
        raw,
    }))
}

fn typed_update_from_hftrec4_record_fast(
    record: Hftrec4Record,
    source_segment: &Path,
    options: &BuildMarketReplayDatasetOptions,
    symbol_filter: &SymbolFilter,
    profile: &mut MarketReplayStreamProfile,
) -> Result<Option<OrderedTypedUpdateFromRaw>> {
    match typed_update_from_hftrec4_record_fast_inner(
        &record,
        source_segment,
        options,
        symbol_filter,
        profile,
    ) {
        Ok(Some((key, update))) => {
            profile.typed_fast_decode_count = profile.typed_fast_decode_count.saturating_add(1);
            Ok(Some(OrderedTypedUpdateFromRaw { key, update }))
        }
        Ok(None) => Ok(None),
        Err(_) => {
            profile.typed_slow_fallback_count = profile.typed_slow_fallback_count.saturating_add(1);
            typed_update_from_hftrec4_record_slow(
                record,
                source_segment,
                options,
                symbol_filter,
                profile,
            )
        }
    }
}

fn typed_update_from_hftrec4_record_slow(
    record: Hftrec4Record,
    source_segment: &Path,
    options: &BuildMarketReplayDatasetOptions,
    symbol_filter: &SymbolFilter,
    profile: &mut MarketReplayStreamProfile,
) -> Result<Option<OrderedTypedUpdateFromRaw>> {
    let stage_started = Instant::now();
    let update =
        market_replay_raw_update_from_record(record, source_segment, options, symbol_filter)?;
    profile.raw_update_build_ns = profile
        .raw_update_build_ns
        .saturating_add(stage_started.elapsed().as_nanos());
    let Some(update) = update else {
        return Ok(None);
    };
    let key = MarketEventSortKey::from_raw_update(&update);
    let stage_started = Instant::now();
    let payload = raw_update_payload_value(&update)?;
    profile.payload_parse_ns = profile
        .payload_parse_ns
        .saturating_add(stage_started.elapsed().as_nanos());
    let stage_started = Instant::now();
    let typed = decode_market_replay_typed_update(&update, &payload)?;
    profile.typed_decode_ns = profile
        .typed_decode_ns
        .saturating_add(stage_started.elapsed().as_nanos());
    Ok(Some(OrderedTypedUpdateFromRaw { key, update: typed }))
}

fn typed_update_from_hftrec4_record_fast_inner(
    record: &Hftrec4Record,
    source_segment: &Path,
    options: &BuildMarketReplayDatasetOptions,
    symbol_filter: &SymbolFilter,
    profile: &mut MarketReplayStreamProfile,
) -> Result<Option<(MarketEventSortKey, MarketReplayTypedUpdate)>> {
    let build_started = Instant::now();
    let event_type = record.event_type.trim().to_ascii_lowercase();
    if event_type != "book" && event_type != "price_change" {
        return Ok(None);
    }
    if record
        .symbol
        .as_deref()
        .and_then(canonical_market_symbol)
        .is_some_and(|symbol| !symbol_filter.allows(&symbol))
    {
        return Ok(None);
    }
    let original_local_recv_ts_ns = record.local_recv_ts_ns;
    let mut visible_ts_ns = original_local_recv_ts_ns;
    let symbol = record.symbol.as_deref().and_then(canonical_market_symbol);
    let horizon_seconds = horizon_seconds(
        record.market_start_ts_ns,
        record.market_end_ts_ns,
        symbol.as_deref().or(record.symbol.as_deref()),
    );
    profile.raw_update_build_ns = profile
        .raw_update_build_ns
        .saturating_add(build_started.elapsed().as_nanos());

    let parse_started = Instant::now();
    let body_and_key_asset = match event_type.as_str() {
        "book" => {
            let payload = serde_json::from_slice::<FastBookPayload<'_>>(&record.payload)
                .with_context(|| {
                    format!(
                        "decode fast book payload at ingest_seq {}",
                        record.ingest_seq
                    )
                })?;
            profile.payload_parse_ns = profile
                .payload_parse_ns
                .saturating_add(parse_started.elapsed().as_nanos());
            let decode_started = Instant::now();
            let condition_id = record
                .condition_id
                .clone()
                .or_else(|| payload.condition_id_from_payload());
            let asset_id = record
                .asset_id
                .clone()
                .or_else(|| payload.asset_id_from_payload());
            let asset_id = asset_id.ok_or_else(|| anyhow!("raw book update missing asset_id"))?;
            let bids = fast_book_levels(
                payload.bids.as_deref().or(payload.buys.as_deref()),
                "book level price",
            )?;
            let asks = fast_book_levels(
                payload.asks.as_deref().or(payload.sells.as_deref()),
                "book level price",
            )?;
            let body = MarketReplayTypedUpdateBody::Book {
                asset_id: asset_id.clone(),
                bids,
                asks,
            };
            profile.typed_decode_ns = profile
                .typed_decode_ns
                .saturating_add(decode_started.elapsed().as_nanos());
            (body, Some(asset_id), condition_id)
        }
        "price_change" => {
            let payload = serde_json::from_slice::<FastPriceChangePayload<'_>>(&record.payload)
                .with_context(|| {
                    format!(
                        "decode fast price_change payload at ingest_seq {}",
                        record.ingest_seq
                    )
                })?;
            profile.payload_parse_ns = profile
                .payload_parse_ns
                .saturating_add(parse_started.elapsed().as_nanos());
            let decode_started = Instant::now();
            if options.poly_server_visible_time {
                if let Some(exchange_ts_ms) = payload.timestamp_ms() {
                    if let Some(synthetic_ts_ns) = exchange_ts_ms
                        .checked_add(options.poly_incremental_latency_ms)
                        .and_then(|ts_ms| ts_ms.checked_mul(1_000_000))
                    {
                        if synthetic_ts_ns <= original_local_recv_ts_ns {
                            let guard_ns = options
                                .poly_incremental_freshness_guard_ms
                                .saturating_mul(1_000_000);
                            if original_local_recv_ts_ns.saturating_sub(synthetic_ts_ns) <= guard_ns
                            {
                                visible_ts_ns = synthetic_ts_ns;
                            }
                        }
                    }
                }
            }
            let mut changes = Vec::new();
            for change in payload
                .price_changes
                .as_deref()
                .or(payload.changes.as_deref())
                .into_iter()
                .flatten()
            {
                let Some(side) = change.side_code() else {
                    continue;
                };
                let Some(price_micros) =
                    fast_decimal_micros(&[change.price], "price_change price")?
                else {
                    continue;
                };
                let Some(qty_micros) = fast_decimal_micros(
                    &[change.size, change.qty, change.quantity],
                    "price_change size",
                )?
                else {
                    continue;
                };
                let asset_id = change.asset_id().or_else(|| record.asset_id.clone());
                let Some(asset_id) = asset_id else {
                    continue;
                };
                changes.push(MarketReplayLevelChange {
                    asset_id,
                    side,
                    price_micros,
                    qty_micros,
                });
            }
            let body = MarketReplayTypedUpdateBody::PriceChanges { changes };
            profile.typed_decode_ns = profile
                .typed_decode_ns
                .saturating_add(decode_started.elapsed().as_nanos());
            (body, record.asset_id.clone(), record.condition_id.clone())
        }
        _ => unreachable!("event type checked above"),
    };

    if !ts_in_window(visible_ts_ns, options) {
        return Ok(None);
    }

    let (body, key_asset_id, condition_id) = body_and_key_asset;
    let update = MarketReplayTypedUpdate {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_FORMAT.to_string(),
        global_event_seq: 0,
        symbol,
        horizon_seconds,
        condition_id,
        event_type: event_type.clone(),
        original_local_recv_ts_ns,
        visible_ts_ns,
        ingest_seq: record.ingest_seq,
        source_row_idx: record.row_idx,
        payload_hash: record.payload_sha256.clone(),
        market_start_ts_ns: record.market_start_ts_ns,
        market_end_ts_ns: record.market_end_ts_ns,
        body,
    };
    let key = MarketEventSortKey {
        visible_ts_ns,
        local_recv_ts_ns: original_local_recv_ts_ns,
        ingest_seq: record.ingest_seq,
        source_segment: source_segment.display().to_string(),
        source_row_idx: record.row_idx,
        sequence: 0,
        asset_id: key_asset_id.unwrap_or_default(),
        event_type,
    };
    Ok(Some((key, update)))
}

fn validate_market_replay_raw_coverage_options(
    options: &BuildMarketReplayDatasetOptions,
) -> Result<MarketReplayCoverageReport> {
    let mut report = MarketReplayCoverageReport {
        schema_version: 1,
        raw_start_ts_ns: options.raw_start_ts_ns,
        raw_end_ts_ns: options.raw_end_ts_ns,
        covered_streams: BTreeMap::new(),
        missing_stream_buckets: Vec::new(),
    };
    let (Some(start), Some(end)) = (options.raw_start_ts_ns, options.raw_end_ts_ns) else {
        return Ok(report);
    };
    if end <= start {
        bail!("raw_end_ts_ns must be greater than raw_start_ts_ns");
    }
    let first_bucket = start.div_euclid(HOUR_NS);
    let last_bucket = (end - 1).div_euclid(HOUR_NS);
    for raw_root in &options.raw_roots {
        for stream_root in candidate_stream_roots(raw_root) {
            if !stream_root.exists() {
                continue;
            }
            let stream = stream_name_for_root(&stream_root);
            let key = format!("{}:{}", stream_root.display(), stream);
            for bucket in first_bucket..=last_bucket {
                let bucket_dir = stream_root.join(format!("hour_bucket={bucket}"));
                let covered = bucket_dir_has_hftrec4_manifest(&bucket_dir)?;
                if covered {
                    report
                        .covered_streams
                        .entry(key.clone())
                        .or_default()
                        .push(bucket);
                } else {
                    report.missing_stream_buckets.push(MarketReplayCoverageGap {
                        stream_root: stream_root.clone(),
                        stream: stream.clone(),
                        hour_bucket: bucket,
                    });
                }
            }
        }
    }
    if !report.missing_stream_buckets.is_empty() {
        let preview = report
            .missing_stream_buckets
            .iter()
            .take(8)
            .map(|gap| format!("{}#{}", gap.stream_root.display(), gap.hour_bucket))
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "market replay raw coverage has {} missing stream buckets: {}",
            report.missing_stream_buckets.len(),
            preview
        );
    }
    Ok(report)
}

fn stream_name_for_root(stream_root: &Path) -> String {
    stream_root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown")
        .to_string()
}

fn bucket_dir_has_hftrec4_manifest(bucket_dir: &Path) -> Result<bool> {
    if !bucket_dir.exists() {
        return Ok(false);
    }
    for entry in
        fs::read_dir(bucket_dir).with_context(|| format!("read {}", bucket_dir.display()))?
    {
        let entry = entry.with_context(|| format!("list {}", bucket_dir.display()))?;
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.ends_with(".manifest.json"))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn validate_market_replay_options(options: &BuildMarketReplayDatasetOptions) -> Result<()> {
    if options.raw_roots.is_empty() {
        bail!("build-market-replay-dataset requires at least one raw root");
    }
    if let (Some(start), Some(end)) = (options.raw_start_ts_ns, options.raw_end_ts_ns) {
        if end <= start {
            bail!("raw_end_ts_ns must be greater than raw_start_ts_ns");
        }
    }
    if options.poly_incremental_latency_ms < 0 {
        bail!("poly_incremental_latency_ms must be non-negative");
    }
    if options.poly_incremental_freshness_guard_ms < 0 {
        bail!("poly_incremental_freshness_guard_ms must be non-negative");
    }
    if options.reference_latency_ms < 0 {
        bail!("reference_latency_ms must be non-negative");
    }
    Ok(())
}

fn default_poly_incremental_latency_ms() -> i64 {
    DEFAULT_POLY_INCREMENTAL_LATENCY_MS
}

fn default_poly_incremental_freshness_guard_ms() -> i64 {
    DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS
}

fn default_reference_latency_ms() -> i64 {
    DEFAULT_REFERENCE_LATENCY_MS
}

#[derive(Debug, Clone, Default)]
struct MarketReplayStats {
    row_count: usize,
    min_visible_ts_ns: Option<i64>,
    max_visible_ts_ns: Option<i64>,
    event_type_counts: BTreeMap<String, usize>,
}

impl MarketReplayStats {
    fn observe(&mut self, row: &MarketEvent) {
        self.row_count += 1;
        self.min_visible_ts_ns = Some(
            self.min_visible_ts_ns
                .map_or(row.visible_ts_ns, |value| value.min(row.visible_ts_ns)),
        );
        self.max_visible_ts_ns = Some(
            self.max_visible_ts_ns
                .map_or(row.visible_ts_ns, |value| value.max(row.visible_ts_ns)),
        );
        *self
            .event_type_counts
            .entry(row.event_type.clone())
            .or_insert(0) += 1;
    }

    fn observe_raw_update(&mut self, row: &MarketReplayRawUpdate) {
        self.row_count += 1;
        self.min_visible_ts_ns = Some(
            self.min_visible_ts_ns
                .map_or(row.visible_ts_ns, |value| value.min(row.visible_ts_ns)),
        );
        self.max_visible_ts_ns = Some(
            self.max_visible_ts_ns
                .map_or(row.visible_ts_ns, |value| value.max(row.visible_ts_ns)),
        );
        *self
            .event_type_counts
            .entry(row.event_type.clone())
            .or_insert(0) += 1;
    }

    fn observe_typed_update(&mut self, row: &MarketReplayTypedUpdate) {
        self.row_count += 1;
        self.min_visible_ts_ns = Some(
            self.min_visible_ts_ns
                .map_or(row.visible_ts_ns, |value| value.min(row.visible_ts_ns)),
        );
        self.max_visible_ts_ns = Some(
            self.max_visible_ts_ns
                .map_or(row.visible_ts_ns, |value| value.max(row.visible_ts_ns)),
        );
        *self
            .event_type_counts
            .entry(row.event_type.clone())
            .or_insert(0) += 1;
    }
}

enum TypedUpdateBuildOutput {
    Single(PathBuf),
    Sharded(PathBuf),
}

enum TypedUpdateBuildWriter {
    Single(TypedUpdateWriter),
    Sharded(ShardedTypedUpdateWriter),
}

impl TypedUpdateBuildWriter {
    fn create(
        output: &TypedUpdateBuildOutput,
        options: &BuildMarketReplayTypedUpdatesOptions,
    ) -> Result<Self> {
        match output {
            TypedUpdateBuildOutput::Single(path) => {
                Ok(Self::Single(TypedUpdateWriter::create(path)?))
            }
            TypedUpdateBuildOutput::Sharded(root) => {
                let fallback_symbol = if options.market_symbol_allowlist.len() == 1 {
                    options.market_symbol_allowlist.first().cloned()
                } else {
                    None
                };
                Ok(Self::Sharded(ShardedTypedUpdateWriter::new(
                    root.clone(),
                    fallback_symbol,
                )?))
            }
        }
    }

    fn write_update(&mut self, update: &MarketReplayTypedUpdate) -> Result<()> {
        match self {
            Self::Single(writer) => writer.write_update(update),
            Self::Sharded(writer) => writer.write_update(update),
        }
    }

    fn finish(self) -> Result<Vec<MarketReplayTypedUpdateShardReport>> {
        match self {
            Self::Single(writer) => {
                writer.finish()?;
                Ok(Vec::new())
            }
            Self::Sharded(writer) => writer.finish(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct TypedUpdateShardKey {
    hour_bucket: i64,
    symbol: String,
}

struct OpenTypedUpdateShard {
    key: TypedUpdateShardKey,
    path: PathBuf,
    writer: TypedUpdateWriter,
    stats: MarketReplayStats,
}

impl OpenTypedUpdateShard {
    fn finish(self) -> Result<MarketReplayTypedUpdateShardReport> {
        self.writer.finish()?;
        Ok(MarketReplayTypedUpdateShardReport {
            path: self.path,
            symbol: self.key.symbol,
            hour_bucket: self.key.hour_bucket,
            row_count: self.stats.row_count,
            min_visible_ts_ns: self.stats.min_visible_ts_ns,
            max_visible_ts_ns: self.stats.max_visible_ts_ns,
            event_type_counts: self.stats.event_type_counts,
        })
    }
}

struct ShardedTypedUpdateWriter {
    root: PathBuf,
    fallback_symbol: Option<String>,
    current_hour_bucket: Option<i64>,
    open: BTreeMap<TypedUpdateShardKey, OpenTypedUpdateShard>,
    reports: Vec<MarketReplayTypedUpdateShardReport>,
}

impl ShardedTypedUpdateWriter {
    fn new(root: PathBuf, fallback_symbol: Option<String>) -> Result<Self> {
        fs::create_dir_all(&root).with_context(|| format!("create {}", root.display()))?;
        Ok(Self {
            root,
            fallback_symbol,
            current_hour_bucket: None,
            open: BTreeMap::new(),
            reports: Vec::new(),
        })
    }

    fn write_update(&mut self, update: &MarketReplayTypedUpdate) -> Result<()> {
        let hour_bucket = update.visible_ts_ns.div_euclid(HOUR_NS);
        match self.current_hour_bucket {
            Some(current) if hour_bucket < current => bail!(
                "typed update stream moved backwards from hour_bucket={} to hour_bucket={}",
                current,
                hour_bucket
            ),
            Some(current) if hour_bucket > current => {
                self.finish_open_shards()?;
                self.current_hour_bucket = Some(hour_bucket);
            }
            None => self.current_hour_bucket = Some(hour_bucket),
            _ => {}
        }

        let symbol = typed_update_symbol_component(
            update.symbol.as_deref().or(self.fallback_symbol.as_deref()),
        );
        let key = TypedUpdateShardKey {
            hour_bucket,
            symbol,
        };
        if !self.open.contains_key(&key) {
            let path = self
                .root
                .join(format!("symbol={}", key.symbol))
                .join(format!("hour_bucket={}", key.hour_bucket))
                .join("part-00000.pm5mtu.zst");
            let writer = TypedUpdateWriter::create(&path)?;
            self.open.insert(
                key.clone(),
                OpenTypedUpdateShard {
                    key: key.clone(),
                    path,
                    writer,
                    stats: MarketReplayStats::default(),
                },
            );
        }
        let shard = self
            .open
            .get_mut(&key)
            .expect("typed update shard opened above");
        shard.writer.write_update(update)?;
        shard.stats.observe_typed_update(update);
        Ok(())
    }

    fn finish_open_shards(&mut self) -> Result<()> {
        let open = std::mem::take(&mut self.open);
        for (_, shard) in open {
            self.reports.push(shard.finish()?);
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<MarketReplayTypedUpdateShardReport>> {
        self.finish_open_shards()?;
        self.reports.sort_by(|a, b| {
            (a.hour_bucket, &a.symbol, &a.path).cmp(&(b.hour_bucket, &b.symbol, &b.path))
        });
        Ok(self.reports)
    }
}

#[derive(Debug)]
struct OrderedTypedUpdate {
    file_index: usize,
    update: MarketReplayTypedUpdate,
}

impl Eq for OrderedTypedUpdate {}

impl PartialEq for OrderedTypedUpdate {
    fn eq(&self, other: &Self) -> bool {
        self.update.global_event_seq == other.update.global_event_seq
            && self.file_index == other.file_index
    }
}

impl Ord for OrderedTypedUpdate {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.update.global_event_seq, self.file_index)
            .cmp(&(other.update.global_event_seq, other.file_index))
    }
}

impl PartialOrd for OrderedTypedUpdate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
struct OrderedTypedUpdateFromRaw {
    key: MarketEventSortKey,
    update: MarketReplayTypedUpdate,
}

#[derive(Debug)]
struct ParallelTypedRawScanResult {
    updates: Vec<OrderedTypedUpdateFromRaw>,
    profile: MarketReplayStreamProfile,
}

impl Eq for OrderedTypedUpdateFromRaw {}

impl PartialEq for OrderedTypedUpdateFromRaw {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Ord for OrderedTypedUpdateFromRaw {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for OrderedTypedUpdateFromRaw {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn flush_ordered_typed_raw_updates_to_visit<F>(
    pending: &mut BinaryHeap<Reverse<OrderedTypedUpdateFromRaw>>,
    stats: &mut MarketReplayStats,
    flush_before_or_at_ts: i64,
    visit: &mut F,
    profile: &mut MarketReplayStreamProfile,
) -> Result<()>
where
    F: FnMut(MarketReplayTypedUpdate) -> Result<()>,
{
    let merge_started = Instant::now();
    let mut visit_ns = 0u128;
    while pending
        .peek()
        .is_some_and(|Reverse(item)| item.key.visible_ts_ns <= flush_before_or_at_ts)
    {
        let Reverse(item) = pending.pop().expect("peek checked");
        let mut update = item.update;
        update.global_event_seq = stats.row_count as u64;
        update.dataset_format = MARKET_REPLAY_TYPED_UPDATES_FORMAT.to_string();
        stats.observe_typed_update(&update);
        profile.emitted_update_count = profile.emitted_update_count.saturating_add(1);
        let visit_started = Instant::now();
        visit(update)?;
        visit_ns = visit_ns.saturating_add(visit_started.elapsed().as_nanos());
    }
    profile.visit_callback_ns = profile.visit_callback_ns.saturating_add(visit_ns);
    profile.merge_flush_ns = profile
        .merge_flush_ns
        .saturating_add(merge_started.elapsed().as_nanos().saturating_sub(visit_ns));
    Ok(())
}

fn scan_typed_updates_from_raw_manifest(
    manifest: &RawCandidateManifest,
    options: &BuildMarketReplayDatasetOptions,
    symbol_filter: &SymbolFilter,
) -> Result<ParallelTypedRawScanResult> {
    let worker_started = Instant::now();
    let mut profile = MarketReplayStreamProfile::default();
    let mut updates = Vec::new();
    let mut raw_filter_ns = 0u128;
    let mut raw_filter_row_count = 0usize;
    let scan_started = Instant::now();
    scan_hftrec4_segment_selected(
        &manifest.segment_path,
        |record| {
            let stage_started = Instant::now();
            let selected = should_read_market_replay_raw_update(record, options, symbol_filter);
            raw_filter_ns = raw_filter_ns.saturating_add(stage_started.elapsed().as_nanos());
            raw_filter_row_count = raw_filter_row_count.saturating_add(1);
            selected
        },
        |record| {
            profile.selected_record_count = profile.selected_record_count.saturating_add(1);
            profile.selected_payload_bytes = profile
                .selected_payload_bytes
                .saturating_add(record.payload.len() as u64);
            let Some(update) = typed_update_from_hftrec4_record_fast(
                record,
                &manifest.segment_path,
                options,
                symbol_filter,
                &mut profile,
            )?
            else {
                return Ok(());
            };
            profile.selected_update_count = profile.selected_update_count.saturating_add(1);
            updates.push(update);
            Ok(())
        },
    )
    .with_context(|| {
        format!(
            "parallel typed raw scan HFTREC4 segment {}",
            manifest.segment_path.display()
        )
    })?;
    profile.segment_scan_wall_ns = profile
        .segment_scan_wall_ns
        .saturating_add(scan_started.elapsed().as_nanos());
    profile.raw_filter_ns = profile.raw_filter_ns.saturating_add(raw_filter_ns);
    profile.raw_filter_row_count = profile
        .raw_filter_row_count
        .saturating_add(raw_filter_row_count);
    let sort_started = Instant::now();
    updates.sort_by(|a, b| a.key.cmp(&b.key));
    profile.segment_sort_ns = profile
        .segment_sort_ns
        .saturating_add(sort_started.elapsed().as_nanos());
    profile.worker_wall_ns = profile
        .worker_wall_ns
        .saturating_add(worker_started.elapsed().as_nanos());
    Ok(ParallelTypedRawScanResult { updates, profile })
}

fn typed_update_symbol_component(symbol: Option<&str>) -> String {
    let mut out = String::new();
    for ch in symbol.unwrap_or("unknown").chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "unknown".to_string()
    } else {
        out
    }
}

fn collect_typed_update_files(root: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let mut entries = fs::read_dir(root)
        .with_context(|| format!("read {}", root.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("list {}", root.display()))?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_typed_update_files(&path, out)?;
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".pm5mtu.zst"))
        {
            out.push(path);
        }
    }
    Ok(())
}

fn typed_update_path_hour_bucket(path: &Path) -> Option<i64> {
    path.components().find_map(|component| {
        component
            .as_os_str()
            .to_str()
            .and_then(|value| value.strip_prefix("hour_bucket="))
            .and_then(|value| value.parse::<i64>().ok())
    })
}

struct TypedUpdateWriter {
    writer: zstd::stream::write::Encoder<'static, BufWriter<File>>,
}

impl TypedUpdateWriter {
    fn create(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        let mut writer = BufWriter::new(
            File::create(path).with_context(|| format!("create {}", path.display()))?,
        );
        writer.write_all(MARKET_REPLAY_TYPED_UPDATES_MAGIC)?;
        let writer = zstd::stream::write::Encoder::new(writer, 1)
            .with_context(|| format!("create zstd encoder {}", path.display()))?;
        Ok(Self { writer })
    }

    fn write_update(&mut self, update: &MarketReplayTypedUpdate) -> Result<()> {
        let mut payload = Vec::with_capacity(256);
        encode_typed_update(update, &mut payload)?;
        let len = u32::try_from(payload.len()).context("typed update record too large")?;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&payload)?;
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.finish()?;
        Ok(())
    }
}

struct TypedUpdateReader {
    reader: zstd::stream::read::Decoder<'static, BufReader<BufReader<File>>>,
}

impl TypedUpdateReader {
    fn open(path: &Path) -> Result<Self> {
        let mut reader =
            BufReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?);
        let mut magic = [0u8; MARKET_REPLAY_TYPED_UPDATES_MAGIC.len()];
        reader
            .read_exact(&mut magic)
            .with_context(|| format!("read typed update header {}", path.display()))?;
        if &magic != MARKET_REPLAY_TYPED_UPDATES_MAGIC {
            bail!("unsupported typed update file header: {}", path.display());
        }
        let reader = zstd::stream::read::Decoder::new(reader)
            .with_context(|| format!("open zstd typed update stream {}", path.display()))?;
        Ok(Self { reader })
    }

    fn read_update(&mut self) -> Result<Option<MarketReplayTypedUpdate>> {
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(err).context("read typed update length"),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        self.reader
            .read_exact(&mut payload)
            .context("read typed update payload")?;
        decode_typed_update(&payload).map(Some)
    }
}

#[derive(Debug, Default)]
struct CompactTypedDictBuilder {
    symbols: Vec<String>,
    symbol_keys: BTreeMap<String, u32>,
    conditions: Vec<String>,
    condition_keys: BTreeMap<String, u32>,
    assets: Vec<String>,
    asset_keys: BTreeMap<String, u32>,
}

impl CompactTypedDictBuilder {
    fn optional_symbol_key(&mut self, value: Option<&str>) -> Result<u32> {
        self.optional_key(value, CompactDictKind::Symbol)
    }

    fn optional_condition_key(&mut self, value: Option<&str>) -> Result<u32> {
        self.optional_key(value, CompactDictKind::Condition)
    }

    fn optional_asset_key(&mut self, value: Option<&str>) -> Result<u32> {
        self.optional_key(value, CompactDictKind::Asset)
    }

    fn asset_key(&mut self, value: &str) -> Result<u32> {
        self.key(value, CompactDictKind::Asset)
    }

    fn optional_key(&mut self, value: Option<&str>, kind: CompactDictKind) -> Result<u32> {
        match value {
            Some(value) if !value.is_empty() => self.key(value, kind),
            _ => Ok(COMPACT_TYPED_NONE_U32),
        }
    }

    fn key(&mut self, value: &str, kind: CompactDictKind) -> Result<u32> {
        let (values, keys) = match kind {
            CompactDictKind::Symbol => (&mut self.symbols, &mut self.symbol_keys),
            CompactDictKind::Condition => (&mut self.conditions, &mut self.condition_keys),
            CompactDictKind::Asset => (&mut self.assets, &mut self.asset_keys),
        };
        if let Some(key) = keys.get(value) {
            return Ok(*key);
        }
        let key = u32::try_from(values.len()).context("compact typed dictionary overflow")?;
        values.push(value.to_string());
        keys.insert(value.to_string(), key);
        Ok(key)
    }
}

#[derive(Debug, Clone, Copy)]
enum CompactDictKind {
    Symbol,
    Condition,
    Asset,
}

#[derive(Debug, Clone)]
struct CompactTypedCandidateManifest {
    segment_path: PathBuf,
    raw_segment_path: PathBuf,
    min_local_recv_ts_ns: Option<i64>,
    max_local_recv_ts_ns: Option<i64>,
}

impl Eq for OrderedCompactTypedUpdate {}

impl PartialEq for OrderedCompactTypedUpdate {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Ord for OrderedCompactTypedUpdate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for OrderedCompactTypedUpdate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn write_compact_typed_segment_from_hftrec4_records(
    path: &Path,
    raw_segment_path: &Path,
    records: &[Hftrec4Record],
) -> Result<Option<MarketReplayCompactTypedSegmentManifest>> {
    if records.is_empty() {
        return Ok(None);
    }

    let mut dict = CompactTypedDictBuilder::default();
    let mut metadata = Vec::new();
    let mut body = Vec::new();
    let mut event_type_counts = BTreeMap::<String, usize>::new();
    let mut min_local_recv_ts_ns = None::<i64>;
    let mut max_local_recv_ts_ns = None::<i64>;
    let mut min_ingest_seq = None::<u64>;
    let mut max_ingest_seq = None::<u64>;
    let decode_options = compact_typed_decode_options();
    let symbol_filter = SymbolFilter::new(&[])?;
    let mut profile = MarketReplayStreamProfile::default();

    for record in records {
        let event_type = record.event_type.trim().to_ascii_lowercase();
        if event_type != "book" && event_type != "price_change" {
            continue;
        }
        let decoded = match typed_update_from_hftrec4_record_fast_inner(
            record,
            raw_segment_path,
            &decode_options,
            &symbol_filter,
            &mut profile,
        ) {
            Ok(Some((key, update))) => Some((key, update)),
            Ok(None) => None,
            Err(_) => typed_update_from_hftrec4_record_slow(
                record.clone(),
                raw_segment_path,
                &decode_options,
                &symbol_filter,
                &mut profile,
            )?
            .map(|ordered| (ordered.key, ordered.update)),
        };
        let Some((key, update)) = decoded else {
            continue;
        };
        let body_offset =
            u64::try_from(body.len()).context("compact typed body offset overflow")?;
        encode_compact_typed_body(&mut dict, &update.body, &mut body)?;
        let body_len = u32::try_from(
            u64::try_from(body.len())
                .context("compact typed body length overflow")?
                .saturating_sub(body_offset),
        )
        .context("compact typed body record too large")?;
        let exchange_ts_ms = if update.event_type == "price_change" {
            timestamp_ms_for_raw_payload(&record.payload)
        } else {
            None
        };
        metadata.push(CompactTypedRecordMeta {
            ingest_seq: update.ingest_seq,
            source_row_idx: update.source_row_idx,
            original_local_recv_ts_ns: update.original_local_recv_ts_ns,
            exchange_ts_ms: exchange_ts_ms.unwrap_or(COMPACT_TYPED_NONE_I64),
            event_type_code: compact_event_type_code(&update.event_type)?,
            symbol_key: dict.optional_symbol_key(update.symbol.as_deref())?,
            condition_key: dict.optional_condition_key(update.condition_id.as_deref())?,
            market_start_ts_ns: update.market_start_ts_ns.unwrap_or(COMPACT_TYPED_NONE_I64),
            market_end_ts_ns: update.market_end_ts_ns.unwrap_or(COMPACT_TYPED_NONE_I64),
            key_asset_key: dict
                .optional_asset_key((!key.asset_id.is_empty()).then_some(key.asset_id.as_str()))?,
            body_offset,
            body_len,
            payload_sha256: digest_hex_to_32_bytes(&update.payload_hash)?,
        });
        *event_type_counts
            .entry(update.event_type.clone())
            .or_insert(0) += 1;
        min_local_recv_ts_ns =
            min_opt_i64(min_local_recv_ts_ns, Some(update.original_local_recv_ts_ns));
        max_local_recv_ts_ns =
            max_opt_i64(max_local_recv_ts_ns, Some(update.original_local_recv_ts_ns));
        min_ingest_seq = min_opt_u64(min_ingest_seq, Some(update.ingest_seq));
        max_ingest_seq = max_opt_u64(max_ingest_seq, Some(update.ingest_seq));
    }

    if metadata.is_empty() {
        return Ok(None);
    }

    let metadata_bytes = encode_compact_typed_metadata(&metadata);
    let header = CompactTypedSegmentHeader {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_COMPACT_TYPED_FORMAT.to_string(),
        schema_hash: MARKET_REPLAY_COMPACT_TYPED_SCHEMA_HASH.to_string(),
        raw_segment_path: raw_segment_path.to_path_buf(),
        record_count: metadata.len() as u64,
        metadata_len: metadata_bytes.len() as u64,
        body_len: body.len() as u64,
        min_local_recv_ts_ns,
        max_local_recv_ts_ns,
        min_ingest_seq,
        max_ingest_seq,
        event_type_counts: event_type_counts.clone(),
        symbols: dict.symbols,
        conditions: dict.conditions,
        assets: dict.assets,
    };
    let header_bytes = serde_json::to_vec(&header).context("serialize compact typed header")?;
    let header_len = u32::try_from(header_bytes.len()).context("compact typed header too large")?;
    let mut bytes = Vec::with_capacity(
        MARKET_REPLAY_COMPACT_TYPED_MAGIC.len()
            + 4
            + header_bytes.len()
            + metadata_bytes.len()
            + body.len(),
    );
    bytes.extend_from_slice(MARKET_REPLAY_COMPACT_TYPED_MAGIC);
    bytes.extend_from_slice(&header_len.to_le_bytes());
    bytes.extend_from_slice(&header_bytes);
    bytes.extend_from_slice(&metadata_bytes);
    bytes.extend_from_slice(&body);

    atomic_write_verified(path, &bytes, |tmp| {
        let records = read_compact_typed_records(tmp)?;
        if records.len() != metadata.len() {
            bail!("compact typed write verification row count mismatch");
        }
        Ok(())
    })?;
    let segment_sha256 = sha256_file(path)?;
    let segment_bytes = fs::metadata(path)?.len();
    let manifest = MarketReplayCompactTypedSegmentManifest {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_COMPACT_TYPED_FORMAT.to_string(),
        segment_path: path.to_path_buf(),
        segment_sha256,
        segment_bytes,
        schema_hash: MARKET_REPLAY_COMPACT_TYPED_SCHEMA_HASH.to_string(),
        raw_segment_path: raw_segment_path.to_path_buf(),
        record_count: metadata.len(),
        min_local_recv_ts_ns,
        max_local_recv_ts_ns,
        min_ingest_seq,
        max_ingest_seq,
        event_type_counts,
        symbol_count: header.symbols.len(),
        condition_count: header.conditions.len(),
        asset_count: header.assets.len(),
        metadata_bytes: metadata_bytes.len() as u64,
        body_bytes: body.len() as u64,
    };
    let manifest_path = market_replay_compact_typed_manifest_path_for_segment(path);
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    atomic_write_verified(&manifest_path, &manifest_bytes, |tmp| {
        let parsed: MarketReplayCompactTypedSegmentManifest =
            serde_json::from_reader(File::open(tmp)?)?;
        if parsed.dataset_format != MARKET_REPLAY_COMPACT_TYPED_FORMAT
            || parsed.schema_hash != MARKET_REPLAY_COMPACT_TYPED_SCHEMA_HASH
        {
            bail!("invalid compact typed manifest");
        }
        Ok(())
    })?;
    if let Some(parent) = manifest_path.parent() {
        fsync_dir(parent)?;
    }
    Ok(Some(manifest))
}

fn read_compact_typed_records(path: &Path) -> Result<Vec<MarketReplayCompactTypedRecord>> {
    read_compact_typed_records_profiled(path, None)
}

fn read_compact_typed_records_profiled(
    path: &Path,
    mut profile: Option<&mut MarketReplayStreamProfile>,
) -> Result<Vec<MarketReplayCompactTypedRecord>> {
    let header_started = profile.is_some().then(Instant::now);
    let mut reader =
        BufReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?);
    let header = read_compact_typed_header_from_reader(&mut reader, path)?;
    if let (Some(started), Some(profile)) = (header_started, profile.as_deref_mut()) {
        profile.compact_header_read_ns = profile
            .compact_header_read_ns
            .saturating_add(started.elapsed().as_nanos());
    }
    let expected_metadata_len = usize::try_from(header.record_count)
        .ok()
        .and_then(|rows| rows.checked_mul(COMPACT_TYPED_META_LEN))
        .context("compact typed metadata length overflow")?;
    if usize::try_from(header.metadata_len).ok() != Some(expected_metadata_len) {
        bail!("compact typed metadata byte length mismatch");
    }
    let metadata_len =
        usize::try_from(header.metadata_len).context("compact typed metadata len")?;
    let body_len = usize::try_from(header.body_len).context("compact typed body len")?;
    let mut metadata = vec![0u8; metadata_len];
    let metadata_started = profile.is_some().then(Instant::now);
    reader
        .read_exact(&mut metadata)
        .with_context(|| format!("read compact typed metadata {}", path.display()))?;
    if let (Some(started), Some(profile)) = (metadata_started, profile.as_deref_mut()) {
        profile.compact_metadata_read_ns = profile
            .compact_metadata_read_ns
            .saturating_add(started.elapsed().as_nanos());
    }
    let mut body = vec![0u8; body_len];
    let body_started = profile.is_some().then(Instant::now);
    reader
        .read_exact(&mut body)
        .with_context(|| format!("read compact typed body {}", path.display()))?;
    if let (Some(started), Some(profile)) = (body_started, profile.as_deref_mut()) {
        profile.compact_body_read_ns = profile
            .compact_body_read_ns
            .saturating_add(started.elapsed().as_nanos());
    }
    let mut trailing = [0u8; 1];
    if reader.read(&mut trailing)? != 0 {
        bail!(
            "compact typed segment has trailing bytes: {}",
            path.display()
        );
    }
    let mut out = Vec::with_capacity(header.record_count as usize);
    let decode_started = profile.is_some().then(Instant::now);
    for row_idx in 0..(header.record_count as usize) {
        let start = row_idx
            .checked_mul(COMPACT_TYPED_META_LEN)
            .context("compact typed metadata row offset overflow")?;
        let end = start + COMPACT_TYPED_META_LEN;
        let meta = decode_compact_typed_meta(&metadata[start..end])?;
        let body_start =
            usize::try_from(meta.body_offset).context("compact typed body offset usize")?;
        let body_end = body_start
            .checked_add(meta.body_len as usize)
            .context("compact typed body row end overflow")?;
        let body_slice = body.get(body_start..body_end).ok_or_else(|| {
            anyhow!("compact typed body row {row_idx} points outside body section")
        })?;
        let typed_body =
            decode_compact_typed_body(meta.event_type_code, body_slice, &header.assets)?;
        let event_type = compact_event_type_from_code(meta.event_type_code)?.to_string();
        let symbol = optional_compact_dict_value(&header.symbols, meta.symbol_key, "symbol")?;
        let market_start_ts_ns = compact_optional_i64(meta.market_start_ts_ns);
        let market_end_ts_ns = compact_optional_i64(meta.market_end_ts_ns);
        let horizon_seconds =
            horizon_seconds(market_start_ts_ns, market_end_ts_ns, symbol.as_deref());
        let update = MarketReplayTypedUpdate {
            schema_version: 1,
            dataset_format: MARKET_REPLAY_TYPED_UPDATES_FORMAT.to_string(),
            global_event_seq: 0,
            symbol,
            horizon_seconds,
            condition_id: optional_compact_dict_value(
                &header.conditions,
                meta.condition_key,
                "condition",
            )?,
            event_type,
            original_local_recv_ts_ns: meta.original_local_recv_ts_ns,
            visible_ts_ns: meta.original_local_recv_ts_ns,
            ingest_seq: meta.ingest_seq,
            source_row_idx: meta.source_row_idx,
            payload_hash: digest_32_bytes_to_hex(&meta.payload_sha256),
            market_start_ts_ns,
            market_end_ts_ns,
            body: typed_body,
        };
        out.push(MarketReplayCompactTypedRecord {
            raw_segment_path: header.raw_segment_path.clone(),
            key_asset_id: optional_compact_dict_value(&header.assets, meta.key_asset_key, "asset")?,
            exchange_ts_ms: compact_optional_i64(meta.exchange_ts_ms),
            update,
        });
    }
    if let (Some(started), Some(profile)) = (decode_started, profile.as_deref_mut()) {
        profile.compact_record_decode_ns = profile
            .compact_record_decode_ns
            .saturating_add(started.elapsed().as_nanos());
    }
    Ok(out)
}

fn read_compact_typed_header(path: &Path) -> Result<CompactTypedSegmentHeader> {
    let mut reader =
        BufReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?);
    read_compact_typed_header_from_reader(&mut reader, path)
}

fn read_compact_typed_header_from_reader<R: Read>(
    reader: &mut R,
    path: &Path,
) -> Result<CompactTypedSegmentHeader> {
    let mut magic = [0u8; MARKET_REPLAY_COMPACT_TYPED_MAGIC.len()];
    reader
        .read_exact(&mut magic)
        .with_context(|| format!("read compact typed magic {}", path.display()))?;
    if &magic != MARKET_REPLAY_COMPACT_TYPED_MAGIC {
        bail!("invalid compact typed magic: {}", path.display());
    }
    let mut header_len_buf = [0u8; 4];
    reader
        .read_exact(&mut header_len_buf)
        .with_context(|| format!("read compact typed header len {}", path.display()))?;
    let header_len = u32::from_le_bytes(header_len_buf) as usize;
    let mut header_bytes = vec![0u8; header_len];
    reader
        .read_exact(&mut header_bytes)
        .with_context(|| format!("read compact typed header {}", path.display()))?;
    let header: CompactTypedSegmentHeader =
        serde_json::from_slice(&header_bytes).context("parse compact typed header")?;
    if header.dataset_format != MARKET_REPLAY_COMPACT_TYPED_FORMAT
        || header.schema_hash != MARKET_REPLAY_COMPACT_TYPED_SCHEMA_HASH
    {
        bail!("unsupported compact typed segment {}", path.display());
    }
    Ok(header)
}

fn stream_compact_typed_updates_from_files<I, F>(
    paths: I,
    options: &StreamMarketReplayEventsOptions,
    visit: &mut F,
) -> Result<MarketReplayStreamReport>
where
    I: IntoIterator<Item = PathBuf>,
    F: FnMut(MarketReplayTypedUpdate) -> Result<()>,
{
    validate_compact_typed_stream_options(options)?;
    let started = Instant::now();
    let build_options = BuildMarketReplayDatasetOptions::from(options);
    let symbol_filter = SymbolFilter::new(&build_options.market_symbol_allowlist)?;
    let candidates = sorted_compact_typed_candidates(paths, &build_options)?;
    if candidates.is_empty() {
        bail!("compact typed stream found no segment files");
    }
    let mut pending = BinaryHeap::<Reverse<OrderedCompactTypedUpdate>>::new();
    let mut stats = MarketReplayStats::default();
    let mut profile = MarketReplayStreamProfile {
        manifest_count: candidates.len(),
        ..MarketReplayStreamProfile::default()
    };
    let deep_profile = market_replay_deep_profile_enabled();
    let order_holdback_ns = market_replay_order_holdback_ns(&build_options);
    for idx in 0..candidates.len() {
        let candidate = &candidates[idx];
        let read_started = Instant::now();
        let records = if deep_profile {
            read_compact_typed_records_profiled(&candidate.segment_path, Some(&mut profile))?
        } else {
            read_compact_typed_records(&candidate.segment_path)?
        };
        profile.compact_segment_read_ns = profile
            .compact_segment_read_ns
            .saturating_add(read_started.elapsed().as_nanos());
        profile.selected_record_count = profile.selected_record_count.saturating_add(records.len());
        if deep_profile {
            for record in records {
                let mut update = record.update;
                let filter_started = Instant::now();
                if !ts_before_end(update.original_local_recv_ts_ns, &build_options) {
                    profile.compact_record_filter_ns = profile
                        .compact_record_filter_ns
                        .saturating_add(filter_started.elapsed().as_nanos());
                    continue;
                }
                if update
                    .symbol
                    .as_deref()
                    .and_then(canonical_market_symbol)
                    .is_some_and(|symbol| !symbol_filter.allows(&symbol))
                {
                    profile.compact_record_filter_ns = profile
                        .compact_record_filter_ns
                        .saturating_add(filter_started.elapsed().as_nanos());
                    continue;
                }
                update.visible_ts_ns = compact_typed_visible_ts_ns(
                    update.original_local_recv_ts_ns,
                    record.exchange_ts_ms,
                    &update.event_type,
                    &build_options,
                );
                if !ts_in_window(update.visible_ts_ns, &build_options) {
                    profile.compact_record_filter_ns = profile
                        .compact_record_filter_ns
                        .saturating_add(filter_started.elapsed().as_nanos());
                    continue;
                }
                profile.compact_record_filter_ns = profile
                    .compact_record_filter_ns
                    .saturating_add(filter_started.elapsed().as_nanos());

                let push_started = Instant::now();
                let key = MarketEventSortKey {
                    visible_ts_ns: update.visible_ts_ns,
                    local_recv_ts_ns: update.original_local_recv_ts_ns,
                    ingest_seq: update.ingest_seq,
                    source_segment: record.raw_segment_path.display().to_string(),
                    source_row_idx: update.source_row_idx,
                    sequence: 0,
                    asset_id: record.key_asset_id.unwrap_or_default(),
                    event_type: update.event_type.clone(),
                };
                pending.push(Reverse(OrderedCompactTypedUpdate { key, update }));
                profile.selected_update_count = profile.selected_update_count.saturating_add(1);
                profile.compact_pending_push_ns = profile
                    .compact_pending_push_ns
                    .saturating_add(push_started.elapsed().as_nanos());
            }
        } else {
            for record in records {
                let mut update = record.update;
                if !ts_before_end(update.original_local_recv_ts_ns, &build_options) {
                    continue;
                }
                if update
                    .symbol
                    .as_deref()
                    .and_then(canonical_market_symbol)
                    .is_some_and(|symbol| !symbol_filter.allows(&symbol))
                {
                    continue;
                }
                update.visible_ts_ns = compact_typed_visible_ts_ns(
                    update.original_local_recv_ts_ns,
                    record.exchange_ts_ms,
                    &update.event_type,
                    &build_options,
                );
                if !ts_in_window(update.visible_ts_ns, &build_options) {
                    continue;
                }
                let key = MarketEventSortKey {
                    visible_ts_ns: update.visible_ts_ns,
                    local_recv_ts_ns: update.original_local_recv_ts_ns,
                    ingest_seq: update.ingest_seq,
                    source_segment: record.raw_segment_path.display().to_string(),
                    source_row_idx: update.source_row_idx,
                    sequence: 0,
                    asset_id: record.key_asset_id.unwrap_or_default(),
                    event_type: update.event_type.clone(),
                };
                pending.push(Reverse(OrderedCompactTypedUpdate { key, update }));
                profile.selected_update_count = profile.selected_update_count.saturating_add(1);
            }
        }
        let flush_before_or_at_ts = candidates
            .get(idx + 1)
            .and_then(|candidate| candidate.min_local_recv_ts_ns)
            .map(|ts| ts.saturating_sub(order_holdback_ns))
            .unwrap_or(i64::MAX);
        flush_ordered_compact_typed_updates_to_visit(
            &mut pending,
            &mut stats,
            &mut profile,
            deep_profile,
            flush_before_or_at_ts,
            visit,
        )?;
    }
    Ok(MarketReplayStreamReport {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_TYPED_UPDATES_FORMAT.to_string(),
        row_count: stats.row_count,
        manifest_count: candidates.len(),
        min_visible_ts_ns: stats.min_visible_ts_ns,
        max_visible_ts_ns: stats.max_visible_ts_ns,
        event_type_counts: stats.event_type_counts,
        elapsed_ms: started.elapsed().as_millis(),
        profile,
    })
}

fn flush_ordered_compact_typed_updates_to_visit<F>(
    pending: &mut BinaryHeap<Reverse<OrderedCompactTypedUpdate>>,
    stats: &mut MarketReplayStats,
    profile: &mut MarketReplayStreamProfile,
    deep_profile: bool,
    flush_before_or_at_ts: i64,
    visit: &mut F,
) -> Result<()>
where
    F: FnMut(MarketReplayTypedUpdate) -> Result<()>,
{
    if deep_profile {
        let merge_started = Instant::now();
        let mut visit_callback_ns = 0u128;
        while pending
            .peek()
            .is_some_and(|Reverse(item)| item.key.visible_ts_ns <= flush_before_or_at_ts)
        {
            let Reverse(item) = pending.pop().expect("peek checked");
            let mut update = item.update;
            update.global_event_seq = stats.row_count as u64;
            update.dataset_format = MARKET_REPLAY_TYPED_UPDATES_FORMAT.to_string();
            stats.observe_typed_update(&update);
            profile.emitted_update_count = profile.emitted_update_count.saturating_add(1);
            let visit_started = Instant::now();
            visit(update)?;
            visit_callback_ns =
                visit_callback_ns.saturating_add(visit_started.elapsed().as_nanos());
        }
        profile.visit_callback_ns = profile.visit_callback_ns.saturating_add(visit_callback_ns);
        profile.merge_flush_ns = profile.merge_flush_ns.saturating_add(
            merge_started
                .elapsed()
                .as_nanos()
                .saturating_sub(visit_callback_ns),
        );
    } else {
        while pending
            .peek()
            .is_some_and(|Reverse(item)| item.key.visible_ts_ns <= flush_before_or_at_ts)
        {
            let Reverse(item) = pending.pop().expect("peek checked");
            let mut update = item.update;
            update.global_event_seq = stats.row_count as u64;
            update.dataset_format = MARKET_REPLAY_TYPED_UPDATES_FORMAT.to_string();
            stats.observe_typed_update(&update);
            profile.emitted_update_count = profile.emitted_update_count.saturating_add(1);
            visit(update)?;
        }
    }
    Ok(())
}

fn sorted_compact_typed_candidates<I>(
    paths: I,
    options: &BuildMarketReplayDatasetOptions,
) -> Result<Vec<CompactTypedCandidateManifest>>
where
    I: IntoIterator<Item = PathBuf>,
{
    let mut out = Vec::new();
    for path in paths {
        let header = read_compact_typed_header(&path)?;
        let candidate = CompactTypedCandidateManifest {
            segment_path: path,
            raw_segment_path: header.raw_segment_path,
            min_local_recv_ts_ns: header.min_local_recv_ts_ns,
            max_local_recv_ts_ns: header.max_local_recv_ts_ns,
        };
        if compact_typed_manifest_overlaps(&candidate, options) {
            out.push(candidate);
        }
    }
    out.sort_by(|a, b| {
        (
            a.min_local_recv_ts_ns.unwrap_or(i64::MIN),
            a.max_local_recv_ts_ns.unwrap_or(i64::MIN),
            &a.raw_segment_path,
            &a.segment_path,
        )
            .cmp(&(
                b.min_local_recv_ts_ns.unwrap_or(i64::MIN),
                b.max_local_recv_ts_ns.unwrap_or(i64::MIN),
                &b.raw_segment_path,
                &b.segment_path,
            ))
    });
    out.dedup_by(|a, b| a.segment_path == b.segment_path);
    Ok(out)
}

fn collect_compact_typed_files(root: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let mut entries = fs::read_dir(root)
        .with_context(|| format!("read {}", root.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("list {}", root.display()))?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_compact_typed_files(&path, out)?;
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".pm5mtb"))
        {
            out.push(path);
        }
    }
    Ok(())
}

fn compact_typed_manifest_overlaps(
    manifest: &CompactTypedCandidateManifest,
    options: &BuildMarketReplayDatasetOptions,
) -> bool {
    let start = options.raw_start_ts_ns.unwrap_or(i64::MIN);
    let end = options.raw_end_ts_ns.unwrap_or(i64::MAX);
    let min_ts = manifest.min_local_recv_ts_ns.unwrap_or(i64::MIN);
    let max_ts = manifest.max_local_recv_ts_ns.unwrap_or(i64::MAX);
    min_ts < end && max_ts >= start
}

fn validate_compact_typed_stream_options(options: &StreamMarketReplayEventsOptions) -> Result<()> {
    if let (Some(start), Some(end)) = (options.raw_start_ts_ns, options.raw_end_ts_ns) {
        if end <= start {
            bail!("raw_end_ts_ns must be greater than raw_start_ts_ns");
        }
    }
    if options.poly_incremental_latency_ms < 0 {
        bail!("poly_incremental_latency_ms must be non-negative");
    }
    if options.poly_incremental_freshness_guard_ms < 0 {
        bail!("poly_incremental_freshness_guard_ms must be non-negative");
    }
    if options.reference_latency_ms < 0 {
        bail!("reference_latency_ms must be non-negative");
    }
    Ok(())
}

fn compact_typed_decode_options() -> BuildMarketReplayDatasetOptions {
    BuildMarketReplayDatasetOptions {
        raw_roots: Vec::new(),
        dataset_root: PathBuf::new(),
        raw_start_ts_ns: None,
        raw_end_ts_ns: None,
        market_symbol_allowlist: Vec::new(),
        overwrite: false,
        poly_server_visible_time: false,
        poly_incremental_latency_ms: DEFAULT_POLY_INCREMENTAL_LATENCY_MS,
        poly_incremental_freshness_guard_ms: DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS,
        reference_latency_ms: DEFAULT_REFERENCE_LATENCY_MS,
        max_rows_per_part: None,
    }
}

fn compact_typed_visible_ts_ns(
    original_local_recv_ts_ns: i64,
    exchange_ts_ms: Option<i64>,
    event_type: &str,
    options: &BuildMarketReplayDatasetOptions,
) -> i64 {
    if !options.poly_server_visible_time || event_type != "price_change" {
        return original_local_recv_ts_ns;
    }
    let Some(exchange_ts_ms) = exchange_ts_ms else {
        return original_local_recv_ts_ns;
    };
    let Some(synthetic_ts_ns) = exchange_ts_ms
        .checked_add(options.poly_incremental_latency_ms)
        .and_then(|ts_ms| ts_ms.checked_mul(1_000_000))
    else {
        return original_local_recv_ts_ns;
    };
    if synthetic_ts_ns > original_local_recv_ts_ns {
        return original_local_recv_ts_ns;
    }
    let guard_ns = options
        .poly_incremental_freshness_guard_ms
        .saturating_mul(1_000_000);
    if original_local_recv_ts_ns.saturating_sub(synthetic_ts_ns) <= guard_ns {
        synthetic_ts_ns
    } else {
        original_local_recv_ts_ns
    }
}

fn encode_compact_typed_metadata(rows: &[CompactTypedRecordMeta]) -> Vec<u8> {
    let mut out = Vec::with_capacity(rows.len() * COMPACT_TYPED_META_LEN);
    for row in rows {
        put_u64(&mut out, row.ingest_seq);
        put_u64(&mut out, row.source_row_idx);
        put_i64(&mut out, row.original_local_recv_ts_ns);
        put_i64(&mut out, row.exchange_ts_ms);
        put_u8(&mut out, row.event_type_code);
        put_u32(&mut out, row.symbol_key);
        put_u32(&mut out, row.condition_key);
        put_i64(&mut out, row.market_start_ts_ns);
        put_i64(&mut out, row.market_end_ts_ns);
        put_u32(&mut out, row.key_asset_key);
        put_u64(&mut out, row.body_offset);
        put_u32(&mut out, row.body_len);
        out.extend_from_slice(&row.payload_sha256);
    }
    out
}

fn decode_compact_typed_meta(payload: &[u8]) -> Result<CompactTypedRecordMeta> {
    if payload.len() != COMPACT_TYPED_META_LEN {
        bail!("invalid compact typed metadata row length");
    }
    let mut cursor = 0usize;
    let ingest_seq = get_u64(payload, &mut cursor)?;
    let source_row_idx = get_u64(payload, &mut cursor)?;
    let original_local_recv_ts_ns = get_i64(payload, &mut cursor)?;
    let exchange_ts_ms = get_i64(payload, &mut cursor)?;
    let event_type_code = get_u8(payload, &mut cursor)?;
    let symbol_key = get_u32(payload, &mut cursor)?;
    let condition_key = get_u32(payload, &mut cursor)?;
    let market_start_ts_ns = get_i64(payload, &mut cursor)?;
    let market_end_ts_ns = get_i64(payload, &mut cursor)?;
    let key_asset_key = get_u32(payload, &mut cursor)?;
    let body_offset = get_u64(payload, &mut cursor)?;
    let body_len = get_u32(payload, &mut cursor)?;
    let digest = take(payload, &mut cursor, 32)?;
    if cursor != payload.len() {
        bail!("compact typed metadata row has trailing bytes");
    }
    Ok(CompactTypedRecordMeta {
        ingest_seq,
        source_row_idx,
        original_local_recv_ts_ns,
        exchange_ts_ms,
        event_type_code,
        symbol_key,
        condition_key,
        market_start_ts_ns,
        market_end_ts_ns,
        key_asset_key,
        body_offset,
        body_len,
        payload_sha256: digest.try_into().expect("digest length checked"),
    })
}

fn encode_compact_typed_body(
    dict: &mut CompactTypedDictBuilder,
    body: &MarketReplayTypedUpdateBody,
    out: &mut Vec<u8>,
) -> Result<()> {
    match body {
        MarketReplayTypedUpdateBody::Book {
            asset_id,
            bids,
            asks,
        } => {
            put_u32(out, dict.asset_key(asset_id)?);
            put_levels(out, bids)?;
            put_levels(out, asks)?;
        }
        MarketReplayTypedUpdateBody::PriceChanges { changes } => {
            put_u32(
                out,
                u32::try_from(changes.len()).context("too many compact typed price changes")?,
            );
            for change in changes {
                put_u32(out, dict.asset_key(&change.asset_id)?);
                put_u8(
                    out,
                    match change.side {
                        ReplayBookSide::Bid => 1,
                        ReplayBookSide::Ask => 2,
                    },
                );
                put_i64(out, change.price_micros);
                put_i64(out, change.qty_micros);
            }
        }
        MarketReplayTypedUpdateBody::Other => {}
    }
    Ok(())
}

fn decode_compact_typed_body(
    event_type_code: u8,
    payload: &[u8],
    assets: &[String],
) -> Result<MarketReplayTypedUpdateBody> {
    let mut cursor = 0usize;
    let body = match event_type_code {
        1 => MarketReplayTypedUpdateBody::Book {
            asset_id: required_compact_dict_value(assets, get_u32(payload, &mut cursor)?, "asset")?,
            bids: get_levels(payload, &mut cursor)?,
            asks: get_levels(payload, &mut cursor)?,
        },
        2 => {
            let len = get_u32(payload, &mut cursor)? as usize;
            let mut changes = Vec::with_capacity(len);
            for _ in 0..len {
                let asset_id =
                    required_compact_dict_value(assets, get_u32(payload, &mut cursor)?, "asset")?;
                let side = match get_u8(payload, &mut cursor)? {
                    1 => ReplayBookSide::Bid,
                    2 => ReplayBookSide::Ask,
                    other => bail!("unknown compact typed side code {other}"),
                };
                changes.push(MarketReplayLevelChange {
                    asset_id,
                    side,
                    price_micros: get_i64(payload, &mut cursor)?,
                    qty_micros: get_i64(payload, &mut cursor)?,
                });
            }
            MarketReplayTypedUpdateBody::PriceChanges { changes }
        }
        other => bail!("unknown compact typed event type code {other}"),
    };
    if cursor != payload.len() {
        bail!(
            "compact typed body has {} trailing bytes",
            payload.len() - cursor
        );
    }
    Ok(body)
}

fn compact_event_type_code(event_type: &str) -> Result<u8> {
    match event_type {
        "book" => Ok(1),
        "price_change" => Ok(2),
        other => bail!("unsupported compact typed event type {other}"),
    }
}

fn compact_event_type_from_code(code: u8) -> Result<&'static str> {
    match code {
        1 => Ok("book"),
        2 => Ok("price_change"),
        other => bail!("unknown compact typed event type code {other}"),
    }
}

fn compact_optional_i64(value: i64) -> Option<i64> {
    (value != COMPACT_TYPED_NONE_I64).then_some(value)
}

fn optional_compact_dict_value(values: &[String], key: u32, field: &str) -> Result<Option<String>> {
    if key == COMPACT_TYPED_NONE_U32 {
        return Ok(None);
    }
    required_compact_dict_value(values, key, field).map(Some)
}

fn required_compact_dict_value(values: &[String], key: u32, field: &str) -> Result<String> {
    values
        .get(key as usize)
        .cloned()
        .ok_or_else(|| anyhow!("compact typed {field} key {key} out of range"))
}

fn digest_hex_to_32_bytes(value: &str) -> Result<[u8; 32]> {
    let bytes = value.as_bytes();
    if bytes.len() != 64 {
        bail!("expected sha256 hex digest with 64 chars");
    }
    let mut out = [0u8; 32];
    for idx in 0..32 {
        let high = hex_nibble(bytes[idx * 2])?;
        let low = hex_nibble(bytes[idx * 2 + 1])?;
        out[idx] = (high << 4) | low;
    }
    Ok(out)
}

fn digest_32_bytes_to_hex(value: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in value {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("invalid hex digest byte"),
    }
}

fn min_opt_i64(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn max_opt_i64(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn min_opt_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn max_opt_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn encode_typed_update(update: &MarketReplayTypedUpdate, out: &mut Vec<u8>) -> Result<()> {
    put_u64(out, update.global_event_seq);
    put_opt_string(out, update.symbol.as_deref())?;
    put_opt_i64(out, update.horizon_seconds);
    put_opt_string(out, update.condition_id.as_deref())?;
    put_string(out, &update.event_type)?;
    put_i64(out, update.original_local_recv_ts_ns);
    put_i64(out, update.visible_ts_ns);
    put_u64(out, update.ingest_seq);
    put_u64(out, update.source_row_idx);
    put_string(out, &update.payload_hash)?;
    put_opt_i64(out, update.market_start_ts_ns);
    put_opt_i64(out, update.market_end_ts_ns);
    match &update.body {
        MarketReplayTypedUpdateBody::Book {
            asset_id,
            bids,
            asks,
        } => {
            put_u8(out, 1);
            put_string(out, asset_id)?;
            put_levels(out, bids)?;
            put_levels(out, asks)?;
        }
        MarketReplayTypedUpdateBody::PriceChanges { changes } => {
            put_u8(out, 2);
            put_u32(
                out,
                u32::try_from(changes.len()).context("too many price changes")?,
            );
            for change in changes {
                put_string(out, &change.asset_id)?;
                put_u8(
                    out,
                    match change.side {
                        ReplayBookSide::Bid => 1,
                        ReplayBookSide::Ask => 2,
                    },
                );
                put_i64(out, change.price_micros);
                put_i64(out, change.qty_micros);
            }
        }
        MarketReplayTypedUpdateBody::Other => put_u8(out, 0),
    }
    Ok(())
}

fn decode_typed_update(payload: &[u8]) -> Result<MarketReplayTypedUpdate> {
    let mut cursor = 0usize;
    let global_event_seq = get_u64(payload, &mut cursor)?;
    let symbol = get_opt_string(payload, &mut cursor)?;
    let horizon_seconds = get_opt_i64(payload, &mut cursor)?;
    let condition_id = get_opt_string(payload, &mut cursor)?;
    let event_type = get_string(payload, &mut cursor)?;
    let original_local_recv_ts_ns = get_i64(payload, &mut cursor)?;
    let visible_ts_ns = get_i64(payload, &mut cursor)?;
    let ingest_seq = get_u64(payload, &mut cursor)?;
    let source_row_idx = get_u64(payload, &mut cursor)?;
    let payload_hash = get_string(payload, &mut cursor)?;
    let market_start_ts_ns = get_opt_i64(payload, &mut cursor)?;
    let market_end_ts_ns = get_opt_i64(payload, &mut cursor)?;
    let body = match get_u8(payload, &mut cursor)? {
        1 => MarketReplayTypedUpdateBody::Book {
            asset_id: get_string(payload, &mut cursor)?,
            bids: get_levels(payload, &mut cursor)?,
            asks: get_levels(payload, &mut cursor)?,
        },
        2 => {
            let len = get_u32(payload, &mut cursor)? as usize;
            let mut changes = Vec::with_capacity(len);
            for _ in 0..len {
                let asset_id = get_string(payload, &mut cursor)?;
                let side = match get_u8(payload, &mut cursor)? {
                    1 => ReplayBookSide::Bid,
                    2 => ReplayBookSide::Ask,
                    other => bail!("unknown replay book side code {other}"),
                };
                changes.push(MarketReplayLevelChange {
                    asset_id,
                    side,
                    price_micros: get_i64(payload, &mut cursor)?,
                    qty_micros: get_i64(payload, &mut cursor)?,
                });
            }
            MarketReplayTypedUpdateBody::PriceChanges { changes }
        }
        0 => MarketReplayTypedUpdateBody::Other,
        other => bail!("unknown typed update body code {other}"),
    };
    if cursor != payload.len() {
        bail!("typed update has {} trailing bytes", payload.len() - cursor);
    }
    Ok(MarketReplayTypedUpdate {
        schema_version: 1,
        dataset_format: MARKET_REPLAY_TYPED_UPDATES_FORMAT.to_string(),
        global_event_seq,
        symbol,
        horizon_seconds,
        condition_id,
        event_type,
        original_local_recv_ts_ns,
        visible_ts_ns,
        ingest_seq,
        source_row_idx,
        payload_hash,
        market_start_ts_ns,
        market_end_ts_ns,
        body,
    })
}

fn put_levels(out: &mut Vec<u8>, levels: &[ReplayBookLevel]) -> Result<()> {
    put_u32(
        out,
        u32::try_from(levels.len()).context("too many book levels")?,
    );
    for level in levels {
        put_i64(out, level.price_micros);
        put_i64(out, level.qty_micros);
    }
    Ok(())
}

fn get_levels(payload: &[u8], cursor: &mut usize) -> Result<Vec<ReplayBookLevel>> {
    let len = get_u32(payload, cursor)? as usize;
    let mut levels = Vec::with_capacity(len);
    for _ in 0..len {
        levels.push(ReplayBookLevel {
            price_micros: get_i64(payload, cursor)?,
            qty_micros: get_i64(payload, cursor)?,
        });
    }
    Ok(levels)
}

fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_opt_i64(out: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(value) => {
            put_u8(out, 1);
            put_i64(out, value);
        }
        None => put_u8(out, 0),
    }
}

fn put_string(out: &mut Vec<u8>, value: &str) -> Result<()> {
    put_u32(
        out,
        u32::try_from(value.len()).context("typed update string too large")?,
    );
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_opt_string(out: &mut Vec<u8>, value: Option<&str>) -> Result<()> {
    match value {
        Some(value) => {
            put_u8(out, 1);
            put_string(out, value)?;
        }
        None => put_u8(out, 0),
    }
    Ok(())
}

fn get_u8(payload: &[u8], cursor: &mut usize) -> Result<u8> {
    let bytes = take(payload, cursor, 1)?;
    Ok(bytes[0])
}

fn get_u32(payload: &[u8], cursor: &mut usize) -> Result<u32> {
    let bytes = take(payload, cursor, 4)?;
    Ok(u32::from_le_bytes(
        bytes.try_into().expect("length checked"),
    ))
}

fn get_u64(payload: &[u8], cursor: &mut usize) -> Result<u64> {
    let bytes = take(payload, cursor, 8)?;
    Ok(u64::from_le_bytes(
        bytes.try_into().expect("length checked"),
    ))
}

fn get_i64(payload: &[u8], cursor: &mut usize) -> Result<i64> {
    let bytes = take(payload, cursor, 8)?;
    Ok(i64::from_le_bytes(
        bytes.try_into().expect("length checked"),
    ))
}

fn get_opt_i64(payload: &[u8], cursor: &mut usize) -> Result<Option<i64>> {
    Ok(match get_u8(payload, cursor)? {
        0 => None,
        1 => Some(get_i64(payload, cursor)?),
        other => bail!("invalid optional i64 tag {other}"),
    })
}

fn get_string(payload: &[u8], cursor: &mut usize) -> Result<String> {
    let len = get_u32(payload, cursor)? as usize;
    let bytes = take(payload, cursor, len)?;
    String::from_utf8(bytes.to_vec()).context("typed update string is not utf8")
}

fn get_opt_string(payload: &[u8], cursor: &mut usize) -> Result<Option<String>> {
    Ok(match get_u8(payload, cursor)? {
        0 => None,
        1 => Some(get_string(payload, cursor)?),
        other => bail!("invalid optional string tag {other}"),
    })
}

fn take<'a>(payload: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| anyhow!("typed update cursor overflow"))?;
    if end > payload.len() {
        bail!("truncated typed update payload");
    }
    let out = &payload[*cursor..end];
    *cursor = end;
    Ok(out)
}

#[derive(Debug, Clone)]
struct EventContext {
    source: String,
    venue: String,
    stream: String,
    symbol: Option<String>,
    horizon_seconds: Option<i64>,
    condition_id: Option<String>,
    asset_id: Option<String>,
    exchange_ts_ns: Option<i64>,
    local_recv_ts_ns: i64,
    visible_ts_ns: i64,
    ingest_seq: u64,
    source_row_idx: u64,
    source_segment: String,
    order_id_or_seq: Option<String>,
    raw_record_hash: String,
    payload_hash: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RawReferenceReplayEvent {
    venue: String,
    symbol: String,
    exchange_event_ts_ms: Option<i64>,
    bar_open_time_ms: Option<i64>,
    bar_close_time_ms: Option<i64>,
    is_closed: Option<bool>,
    close: Option<String>,
}

impl EventContext {
    fn from_raw(
        raw: &RawPolymarketClobWsEvent,
        source_row_idx: u64,
        original_local_recv_ts_ns: i64,
        visible_ts_ns: i64,
        source_segment: &Path,
        raw_record_hash: String,
        payload: &Value,
    ) -> Self {
        let symbol = canonical_symbol_for_event(raw, payload);
        let horizon_seconds = horizon_seconds(
            raw.market_start_ts_ns,
            raw.market_end_ts_ns,
            symbol.as_deref(),
        );
        let exchange_ts_ns = raw
            .exchange_ts_ms
            .or_else(|| timestamp_ms_for_raw_payload(&raw.raw_payload))
            .and_then(|ts_ms| ts_ms.checked_mul(1_000_000));
        Self {
            source: "polymarket_clob_ws".to_string(),
            venue: "polymarket".to_string(),
            stream: WS_RAW_STREAM.to_string(),
            symbol,
            horizon_seconds,
            condition_id: raw
                .condition_id
                .clone()
                .or_else(|| condition_id_from_payload(&raw.raw_payload)),
            asset_id: raw
                .asset_id
                .clone()
                .or_else(|| asset_id_from_payload(&raw.raw_payload)),
            exchange_ts_ns,
            local_recv_ts_ns: original_local_recv_ts_ns,
            visible_ts_ns,
            ingest_seq: raw.ingest_seq,
            source_row_idx,
            source_segment: source_segment.display().to_string(),
            order_id_or_seq: Some(raw.ingest_seq.to_string()),
            raw_record_hash,
            payload_hash: raw.raw_payload_sha256.clone(),
        }
    }

    fn event(
        &self,
        event_type: &str,
        side: Option<String>,
        price_micros: Option<i64>,
        qty_micros: Option<i64>,
        flags: &str,
        sequence: u64,
    ) -> MarketEvent {
        MarketEvent {
            schema_version: 1,
            dataset_format: MARKET_REPLAY_FORMAT.to_string(),
            global_event_seq: 0,
            source: self.source.clone(),
            venue: self.venue.clone(),
            stream: self.stream.clone(),
            symbol: self.symbol.clone(),
            horizon_seconds: self.horizon_seconds,
            condition_id: self.condition_id.clone(),
            asset_id: self.asset_id.clone(),
            event_type: event_type.to_string(),
            exchange_ts_ns: self.exchange_ts_ns,
            local_recv_ts_ns: self.local_recv_ts_ns,
            visible_ts_ns: self.visible_ts_ns,
            sequence,
            ingest_seq: self.ingest_seq,
            source_row_idx: self.source_row_idx,
            source_segment: self.source_segment.clone(),
            side,
            price_micros,
            qty_micros,
            order_id_or_seq: self.order_id_or_seq.clone(),
            raw_record_hash: self.raw_record_hash.clone(),
            payload_hash: self.payload_hash.clone(),
            flags: flags.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct RawCandidateManifest {
    segment_path: PathBuf,
    stream: String,
    min_ts_ns: Option<i64>,
    max_ts_ns: Option<i64>,
    manifest_path: PathBuf,
}

fn sorted_candidate_manifests(
    options: &BuildMarketReplayDatasetOptions,
) -> Result<Vec<RawCandidateManifest>> {
    let mut out = Vec::new();
    for raw_root in &options.raw_roots {
        for manifest_path in candidate_raw_manifests(raw_root, options)? {
            if let Some(manifest) = read_candidate_manifest_metadata(&manifest_path)? {
                out.push(manifest);
            }
        }
    }
    out.sort_by(|a, b| {
        (
            a.min_ts_ns.unwrap_or(i64::MIN),
            a.max_ts_ns.unwrap_or(i64::MIN),
            &a.manifest_path,
        )
            .cmp(&(
                b.min_ts_ns.unwrap_or(i64::MIN),
                b.max_ts_ns.unwrap_or(i64::MIN),
                &b.manifest_path,
            ))
    });
    out.dedup_by(|a, b| a.manifest_path == b.manifest_path);
    Ok(out)
}

fn candidate_raw_manifests(
    raw_root: &Path,
    options: &BuildMarketReplayDatasetOptions,
) -> Result<Vec<PathBuf>> {
    if options.raw_start_ts_ns.is_none() || options.raw_end_ts_ns.is_none() {
        return discover_hftrec4_manifests(raw_root);
    }

    let start_ts_ns = options.raw_start_ts_ns.expect("checked is_some");
    let end_ts_ns = options.raw_end_ts_ns.expect("checked is_some");
    let first_bucket = start_ts_ns.div_euclid(HOUR_NS);
    let last_bucket = (end_ts_ns - 1).div_euclid(HOUR_NS);
    let stream_roots = candidate_stream_roots(raw_root);
    let mut out = Vec::new();
    for stream_root in stream_roots {
        for bucket in first_bucket..=last_bucket {
            let dir = stream_root.join(format!("hour_bucket={bucket}"));
            if !dir.exists() {
                continue;
            }
            let mut entries = fs::read_dir(&dir)
                .with_context(|| format!("read {}", dir.display()))?
                .collect::<std::io::Result<Vec<_>>>()
                .with_context(|| format!("list {}", dir.display()))?;
            entries.sort_by_key(|entry| entry.path());
            for entry in entries {
                let path = entry.path();
                if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".manifest.json"))
                {
                    out.push(path);
                }
            }
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

fn candidate_stream_roots(raw_root: &Path) -> Vec<PathBuf> {
    match raw_root.file_name().and_then(|name| name.to_str()) {
        Some(WS_RAW_STREAM) | Some(REFERENCE_WS_RAW_STREAM) => vec![raw_root.to_path_buf()],
        _ => vec![
            raw_root.join(WS_RAW_STREAM),
            raw_root.join(REFERENCE_WS_RAW_STREAM),
        ],
    }
}

fn read_candidate_manifest_metadata(path: &Path) -> Result<Option<RawCandidateManifest>> {
    let value: serde_json::Value = serde_json::from_reader(
        fs::File::open(path).with_context(|| format!("open raw manifest {}", path.display()))?,
    )
    .with_context(|| format!("parse raw manifest {}", path.display()))?;
    let format = value
        .get("dataset_format")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let path_text = path.to_string_lossy();
    match format {
        HFTREC4_FORMAT => {
            let stream = if path_text.contains(WS_RAW_STREAM) {
                WS_RAW_STREAM.to_string()
            } else if path_text.contains(REFERENCE_WS_RAW_STREAM) {
                REFERENCE_WS_RAW_STREAM.to_string()
            } else {
                return Ok(None);
            };
            let manifest: Hftrec4SegmentManifest = serde_json::from_value(value)?;
            Ok(Some(RawCandidateManifest {
                segment_path: manifest.segment_path,
                stream,
                min_ts_ns: manifest.min_ts_ns,
                max_ts_ns: manifest.max_ts_ns,
                manifest_path: path.to_path_buf(),
            }))
        }
        _ => Ok(None),
    }
}

fn manifest_overlaps(
    manifest: &RawCandidateManifest,
    options: &BuildMarketReplayDatasetOptions,
) -> bool {
    let start = options.raw_start_ts_ns.unwrap_or(i64::MIN);
    let end = options.raw_end_ts_ns.unwrap_or(i64::MAX);
    let min_ts = manifest.min_ts_ns.unwrap_or(i64::MIN);
    let max_ts = manifest.max_ts_ns.unwrap_or(i64::MAX);
    min_ts < end && max_ts >= start
}

fn ts_in_window(ts_ns: i64, options: &BuildMarketReplayDatasetOptions) -> bool {
    options.raw_start_ts_ns.is_none_or(|start| ts_ns >= start)
        && options.raw_end_ts_ns.is_none_or(|end| ts_ns < end)
}

fn ts_before_end(ts_ns: i64, options: &BuildMarketReplayDatasetOptions) -> bool {
    options.raw_end_ts_ns.is_none_or(|end| ts_ns < end)
}

#[derive(Debug, Clone, Default)]
struct SymbolFilter {
    allowed: BTreeSet<String>,
}

impl SymbolFilter {
    fn new(values: &[String]) -> Result<Self> {
        let mut allowed = BTreeSet::new();
        for value in values {
            let symbol = canonical_market_symbol(value)
                .ok_or_else(|| anyhow!("invalid market symbol allowlist entry '{value}'"))?;
            allowed.insert(symbol);
        }
        Ok(Self { allowed })
    }

    fn allows(&self, symbol: &str) -> bool {
        self.allowed.is_empty()
            || canonical_market_symbol(symbol)
                .as_ref()
                .is_some_and(|symbol| self.allowed.contains(symbol))
    }

    fn allows_reference_symbol(&self, symbol: &str) -> bool {
        if self.allowed.is_empty() {
            return true;
        }
        let prefix = format!("{}-", symbol.trim().to_ascii_uppercase());
        self.allowed
            .iter()
            .any(|allowed| allowed.starts_with(&prefix))
    }
}

fn meta_symbol_disallowed(record: &Hftrec4RecordMeta, symbol_filter: &SymbolFilter) -> bool {
    record
        .symbol
        .as_deref()
        .and_then(canonical_market_symbol)
        .is_some_and(|symbol| !symbol_filter.allows(&symbol))
}

fn meta_reference_symbol_disallowed(
    record: &Hftrec4RecordMeta,
    symbol_filter: &SymbolFilter,
) -> bool {
    let Some(symbol) = record.symbol.as_deref().and_then(reference_record_symbol) else {
        return false;
    };
    !symbol_filter.allows_reference_symbol(&symbol)
}

fn raw_symbol_disallowed(raw: &RawPolymarketClobWsEvent, symbol_filter: &SymbolFilter) -> bool {
    raw.symbol
        .as_deref()
        .and_then(canonical_market_symbol)
        .is_some_and(|symbol| !symbol_filter.allows(&symbol))
}

fn event_symbol_disallowed(row: &MarketEvent, symbol_filter: &SymbolFilter) -> bool {
    row.symbol
        .as_deref()
        .is_some_and(|symbol| !symbol_filter.allows(symbol))
}

fn source_record_hash(segment_path: &Path, record: &Hftrec4Record) -> Result<String> {
    hash_serializable(&serde_json::json!({
        "schema": "pm5m.market_replay.source_record_hash.v1",
        "segment_path": segment_path,
        "row_idx": record.row_idx,
        "ingest_seq": record.ingest_seq,
        "local_recv_ts_ns": record.local_recv_ts_ns,
        "event_type": record.event_type,
        "payload_sha256": record.payload_sha256,
    }))
}

fn infer_outcome_from_assets(
    asset_id: Option<&str>,
    yes_asset_id: Option<&str>,
    no_asset_id: Option<&str>,
) -> Option<String> {
    let asset_id = asset_id?;
    if yes_asset_id.is_some_and(|yes_asset_id| yes_asset_id == asset_id) {
        return Some("YES".to_string());
    }
    if no_asset_id.is_some_and(|no_asset_id| no_asset_id == asset_id) {
        return Some("NO".to_string());
    }
    None
}

fn reference_record_symbol(raw: &str) -> Option<String> {
    let symbol = raw
        .split_once(':')
        .map(|(_, symbol)| symbol)
        .unwrap_or(raw)
        .trim()
        .to_ascii_uppercase();
    if symbol.is_empty() {
        None
    } else {
        Some(symbol)
    }
}

fn normalize_exchange_ts_ms(raw: i64) -> i64 {
    if raw < 10_000_000_000_000 {
        raw
    } else {
        raw / 1_000_000
    }
}

fn string_from_payload(payload: &[u8], fields: &[&str]) -> Option<String> {
    if payload.is_empty() {
        return None;
    }
    let value = serde_json::from_slice::<serde_json::Value>(payload).ok()?;
    fields
        .iter()
        .find_map(|field| value.get(*field)?.as_str().map(str::to_string))
}

fn canonical_symbol_for_event(raw: &RawPolymarketClobWsEvent, payload: &Value) -> Option<String> {
    let raw_symbol = raw
        .symbol
        .clone()
        .or_else(|| string_value(payload, &["symbol"]));
    let Some(symbol) = raw_symbol else {
        return None;
    };
    if symbol.contains('-') {
        return canonical_market_symbol(&symbol);
    }
    if let Some(horizon) = horizon_seconds(raw.market_start_ts_ns, raw.market_end_ts_ns, None) {
        let asset = symbol.trim().to_ascii_uppercase();
        let suffix = match horizon {
            300 => "5M",
            900 => "15M",
            3600 => "1H",
            _ => return canonical_market_symbol(&symbol),
        };
        return canonical_market_symbol(&format!("{asset}-{suffix}"));
    }
    canonical_market_symbol(&symbol)
}

fn horizon_seconds(
    market_start_ts_ns: Option<i64>,
    market_end_ts_ns: Option<i64>,
    symbol: Option<&str>,
) -> Option<i64> {
    if let (Some(start), Some(end)) = (market_start_ts_ns, market_end_ts_ns) {
        if end > start {
            return Some((end - start) / 1_000_000_000);
        }
    }
    let (_, horizon) = symbol?.split_once('-')?;
    match horizon.to_ascii_uppercase().as_str() {
        "5M" => Some(300),
        "15M" => Some(900),
        "1H" => Some(3600),
        _ => None,
    }
}

fn parse_book_levels(
    payload: &Value,
    keys: &[&str],
    side: &str,
) -> Result<Vec<(String, i64, i64)>> {
    let Some(levels) = keys
        .iter()
        .find_map(|key| payload.get(*key).and_then(Value::as_array))
    else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for level in levels {
        let Some(price) = decimal_micros(level, &["price", "p"], "book level price")? else {
            continue;
        };
        let Some(size) = decimal_micros(level, &["size", "qty", "quantity"], "book level size")?
        else {
            continue;
        };
        out.push((side.to_string(), price, size));
    }
    Ok(out)
}

fn parse_book_levels_typed(payload: &Value, keys: &[&str]) -> Result<Vec<ReplayBookLevel>> {
    let Some(levels) = keys
        .iter()
        .find_map(|key| payload.get(*key).and_then(Value::as_array))
    else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for level in levels {
        let Some(price_micros) = decimal_micros(level, &["price", "p"], "book level price")? else {
            continue;
        };
        let Some(qty_micros) =
            decimal_micros(level, &["size", "qty", "quantity"], "book level size")?
        else {
            continue;
        };
        out.push(ReplayBookLevel {
            price_micros,
            qty_micros,
        });
    }
    Ok(out)
}

fn parse_ws_side(value: &Value) -> Option<String> {
    match parse_ws_side_code(value)? {
        ReplayBookSide::Bid => Some("BID".to_string()),
        ReplayBookSide::Ask => Some("ASK".to_string()),
    }
}

fn parse_ws_side_code(value: &Value) -> Option<ReplayBookSide> {
    let side = string_value(value, &["side", "side_type", "book_side"])?;
    match side.trim().to_ascii_uppercase().as_str() {
        "BUY" | "BID" | "BIDS" => Some(ReplayBookSide::Bid),
        "SELL" | "ASK" | "ASKS" | "OFFER" | "OFFERS" => Some(ReplayBookSide::Ask),
        _ => None,
    }
}

fn string_value(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value.get(*key).and_then(|field| {
            field
                .as_str()
                .map(str::to_string)
                .or_else(|| field.as_i64().map(|number| number.to_string()))
        })
    })
}

fn decimal_micros(value: &Value, keys: &[&str], field_name: &str) -> Result<Option<i64>> {
    let Some(raw) = keys.iter().find_map(|key| value.get(*key)) else {
        return Ok(None);
    };
    let number = if let Some(number) = raw.as_f64() {
        number
    } else {
        raw.as_str()
            .ok_or_else(|| anyhow!("{field_name} must be decimal"))?
            .parse::<f64>()
            .with_context(|| format!("parse {field_name}"))?
    };
    if !number.is_finite() {
        bail!("{field_name} must be finite");
    }
    Ok(Some(micros(number)))
}

fn decimal_str_micros(raw: &str) -> Option<i64> {
    let number = raw.parse::<f64>().ok()?;
    if number.is_finite() {
        Some(micros(number))
    } else {
        None
    }
}
