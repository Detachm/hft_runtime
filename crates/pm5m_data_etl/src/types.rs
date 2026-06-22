pub use market_data_etl_core::{BookLevel, RawPolymarketBookTop10};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const PLAN_DATASET_FORMAT: &str = "pm5m_pipeline_plan.v1";
pub const CACHE_MANIFEST_FORMAT: &str = "pm5m_input_cache_manifest.v1";
pub const BOOK_STATE_CACHE_FORMAT: &str = "pm5m_book_state_cache.v1";
pub const MARKET_DIM_FORMAT: &str = "pm5m_market_dim.v1";
pub const BOOK_TOP10_FORMAT: &str = "polymarket_book_state_top10.v1";
pub const BINANCE_REFERENCE_FORMAT: &str = "pm5m_binance_kline_1s_reference.v1";
pub const SETTLEMENT_FORMAT: &str = "pm5m_polymarket_settlement.v1";
pub const INPUT_AVAILABILITY_FORMAT: &str = "pm5m_input_availability.v1";
pub const DEPTH_FEATURE_FORMAT: &str = "pm5m_depth_feature_stream_v1_rust_all.v1";
pub const EVENT_INDEX_FORMAT: &str = "pm5m_standard_event_index.v1";
pub const MATERIALIZATION_REPORT_FORMAT: &str = "pm5m_materialization_report.v1";
pub const DATASET_MANIFEST_FORMAT: &str = "pm5m_jupiter_dataset_manifest.v1";
pub const ACCEPTANCE_REPORT_FORMAT: &str = "pm5m_acceptance_report.v1";
pub const EXPORT_MANIFEST_FORMAT: &str = "pm5m_export_manifest.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PipelinePlan {
    pub schema_version: u32,
    pub dataset_format: String,
    pub raw_roots: Vec<PathBuf>,
    pub plan_root: PathBuf,
    pub cache_root: PathBuf,
    pub dataset_root: PathBuf,
    #[serde(default)]
    pub book_state_cache_root: Option<PathBuf>,
    #[serde(default)]
    pub raw_start_ts_ns: Option<i64>,
    #[serde(default)]
    pub raw_end_ts_ns: Option<i64>,
    pub reference_latency_ms: i64,
    pub accept_fail_closed_on_missing_reference: bool,
    #[serde(default = "default_true")]
    pub accept_fail_closed_on_unsettled_settlement: bool,
    #[serde(default)]
    pub market_symbol_allowlist: Vec<String>,
    pub binance_sources: Vec<CacheSourceSpec>,
    pub settlement_sources: Vec<CacheSourceSpec>,
}

impl PipelinePlan {
    pub fn new(
        raw_roots: Vec<PathBuf>,
        plan_root: PathBuf,
        cache_root: PathBuf,
        dataset_root: PathBuf,
    ) -> Self {
        Self {
            schema_version: 1,
            dataset_format: PLAN_DATASET_FORMAT.to_string(),
            raw_roots,
            plan_root,
            cache_root,
            dataset_root,
            book_state_cache_root: None,
            raw_start_ts_ns: None,
            raw_end_ts_ns: None,
            reference_latency_ms: 1_000,
            accept_fail_closed_on_missing_reference: false,
            accept_fail_closed_on_unsettled_settlement: true,
            market_symbol_allowlist: Vec::new(),
            binance_sources: Vec::new(),
            settlement_sources: Vec::new(),
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheSourceSpec {
    pub name: String,
    pub source_url: String,
    pub symbol: Option<String>,
    pub start_ts_ns: i64,
    pub end_ts_ns: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum CacheGroup {
    #[serde(rename = "binance_1s_reference")]
    Binance1sReference,
    #[serde(rename = "polymarket_settlement")]
    PolymarketSettlement,
}

impl CacheGroup {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Binance1sReference => "binance_1s_reference",
            Self::PolymarketSettlement => "polymarket_settlement",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CacheRecordStatus {
    #[serde(rename = "available")]
    Available,
    #[serde(rename = "missing")]
    Missing,
    #[serde(rename = "failed")]
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheRecord {
    pub group: CacheGroup,
    pub name: String,
    pub source_url: String,
    pub symbol: Option<String>,
    pub start_ts_ns: i64,
    pub end_ts_ns: i64,
    pub status: CacheRecordStatus,
    pub cache_path: Option<PathBuf>,
    pub missing_count: u64,
    pub failure_reason: Option<String>,
    pub response_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheManifest {
    pub schema_version: u32,
    pub dataset_format: String,
    pub records: Vec<CacheRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketDimRow {
    pub schema_version: u32,
    pub dataset_format: String,
    pub symbol: String,
    pub condition_id: String,
    pub window_start_ts_ns: i64,
    pub window_end_ts_ns: i64,
    pub yes_asset_id: String,
    pub no_asset_id: String,
    #[serde(default)]
    pub window_source: String,
    pub first_seen_ts_ns: i64,
    pub last_seen_ts_ns: i64,
    pub row_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PolymarketBookTop10Row {
    pub schema_version: u32,
    pub dataset_format: String,
    pub primary_key: String,
    pub symbol: String,
    pub condition_id: String,
    pub asset_id: String,
    pub outcome: String,
    pub window_start_ts_ns: i64,
    pub window_end_ts_ns: i64,
    pub yes_asset_id: String,
    pub no_asset_id: String,
    pub local_recv_ts_ns: i64,
    pub ingest_seq: u64,
    pub best_bid_price_micros: Option<i64>,
    pub best_ask_price_micros: Option<i64>,
    pub bid_depth_top10_micros: i64,
    pub ask_depth_top10_micros: i64,
    pub bid_price_00_micros: Option<i64>,
    pub bid_size_00_micros: Option<i64>,
    pub bid_price_01_micros: Option<i64>,
    pub bid_size_01_micros: Option<i64>,
    pub bid_price_02_micros: Option<i64>,
    pub bid_size_02_micros: Option<i64>,
    pub bid_price_03_micros: Option<i64>,
    pub bid_size_03_micros: Option<i64>,
    pub bid_price_04_micros: Option<i64>,
    pub bid_size_04_micros: Option<i64>,
    pub bid_price_05_micros: Option<i64>,
    pub bid_size_05_micros: Option<i64>,
    pub bid_price_06_micros: Option<i64>,
    pub bid_size_06_micros: Option<i64>,
    pub bid_price_07_micros: Option<i64>,
    pub bid_size_07_micros: Option<i64>,
    pub bid_price_08_micros: Option<i64>,
    pub bid_size_08_micros: Option<i64>,
    pub bid_price_09_micros: Option<i64>,
    pub bid_size_09_micros: Option<i64>,
    pub ask_price_00_micros: Option<i64>,
    pub ask_size_00_micros: Option<i64>,
    pub ask_price_01_micros: Option<i64>,
    pub ask_size_01_micros: Option<i64>,
    pub ask_price_02_micros: Option<i64>,
    pub ask_size_02_micros: Option<i64>,
    pub ask_price_03_micros: Option<i64>,
    pub ask_size_03_micros: Option<i64>,
    pub ask_price_04_micros: Option<i64>,
    pub ask_size_04_micros: Option<i64>,
    pub ask_price_05_micros: Option<i64>,
    pub ask_size_05_micros: Option<i64>,
    pub ask_price_06_micros: Option<i64>,
    pub ask_size_06_micros: Option<i64>,
    pub ask_price_07_micros: Option<i64>,
    pub ask_size_07_micros: Option<i64>,
    pub ask_price_08_micros: Option<i64>,
    pub ask_size_08_micros: Option<i64>,
    pub ask_price_09_micros: Option<i64>,
    pub ask_size_09_micros: Option<i64>,
    pub raw_row_hash: String,
    pub row_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RawBinanceKline1s {
    pub symbol: Option<String>,
    pub bar_open_ts_ns: i64,
    pub bar_close_ts_ns: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinanceKline1sReferenceRow {
    pub schema_version: u32,
    pub dataset_format: String,
    pub primary_key: String,
    pub symbol: String,
    pub bar_open_ts_ns: i64,
    pub bar_close_ts_ns: i64,
    pub synthetic_local_recv_ts_ns: i64,
    pub ingest_seq: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub source_cache_name: String,
    pub row_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawPolymarketSettlement {
    pub condition_id: String,
    pub asset_id: String,
    pub outcome: String,
    pub status: SettlementStatus,
    pub winner: Option<bool>,
    pub settled_ts_ns: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SettlementStatus {
    #[serde(rename = "settled")]
    Settled,
    #[serde(rename = "unsettled")]
    Unsettled,
    #[serde(rename = "unknown")]
    Unknown,
    #[serde(rename = "failed")]
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolymarketSettlementRow {
    pub schema_version: u32,
    pub dataset_format: String,
    pub primary_key: String,
    pub condition_id: String,
    pub asset_id: String,
    pub outcome: String,
    pub status: SettlementStatus,
    pub winner: Option<bool>,
    pub settled_ts_ns: Option<i64>,
    pub source_cache_name: String,
    pub row_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InputAvailabilityRow {
    pub schema_version: u32,
    pub dataset_format: String,
    pub group: CacheGroup,
    pub name: String,
    pub source_url: String,
    pub symbol: Option<String>,
    pub start_ts_ns: i64,
    pub end_ts_ns: i64,
    pub status: CacheRecordStatus,
    pub missing_count: u64,
    pub failure_reason: Option<String>,
    pub response_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DepthFeatureRow {
    pub schema_version: u32,
    pub dataset_format: String,
    pub primary_key: String,
    pub source_book_primary_key: String,
    pub symbol: String,
    pub condition_id: String,
    pub asset_id: String,
    pub outcome: String,
    pub local_recv_ts_ns: i64,
    pub ingest_seq: u64,
    pub best_bid_price: Option<f64>,
    pub best_ask_price: Option<f64>,
    pub spread: Option<f64>,
    pub bid_depth_top10: f64,
    pub ask_depth_top10: f64,
    pub buy_cost_1: Option<f64>,
    pub buy_cost_5: Option<f64>,
    pub buy_cost_10: Option<f64>,
    pub sell_proceeds_1: Option<f64>,
    pub sell_proceeds_5: Option<f64>,
    pub sell_proceeds_10: Option<f64>,
    pub fillable_buy_1: bool,
    pub fillable_buy_5: bool,
    pub fillable_buy_10: bool,
    pub fillable_sell_1: bool,
    pub fillable_sell_5: bool,
    pub fillable_sell_10: bool,
    pub book_age_ms: i64,
    pub row_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StandardEventIndexRow {
    pub schema_version: u32,
    pub dataset_format: String,
    pub global_event_seq: u64,
    pub event_type: String,
    pub event_ts_ns: i64,
    pub source_rank: u8,
    pub symbol: String,
    pub condition_id: Option<String>,
    pub asset_id: Option<String>,
    pub outcome: Option<String>,
    pub payload_table: String,
    pub payload_primary_key: String,
    pub payload_row_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MaterializationReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub raw_roots: Vec<PathBuf>,
    pub plan_root: PathBuf,
    pub cache_root: PathBuf,
    pub dataset_root: PathBuf,
    pub cache_manifest_hash: Option<String>,
    pub table_hashes: BTreeMap<String, String>,
    pub contains_strategy_fields: bool,
    pub contains_settlement_in_event_stream: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatasetManifest {
    pub schema_version: u32,
    pub dataset_format: String,
    pub raw_roots: Vec<PathBuf>,
    pub plan_root: PathBuf,
    pub cache_root: PathBuf,
    pub dataset_root: PathBuf,
    pub binance_cache_manifest_hash: Option<String>,
    pub settlement_cache_manifest_hash: Option<String>,
    pub fact_table_hash: Option<String>,
    pub depth_feature_hash: Option<String>,
    pub event_index_hash: Option<String>,
    pub contains_strategy_fields: bool,
    pub contains_settlement_in_event_stream: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AcceptanceReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub accepted: bool,
    pub violations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExportManifest {
    pub schema_version: u32,
    pub dataset_format: String,
    pub source_dataset_hash: String,
    pub acceptance_hash: String,
    pub copied_file_refs: BTreeMap<String, ExportedFileRef>,
    pub export_timestamp_ns: i64,
    pub exporter_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExportedFileRef {
    pub byte_count: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawHealthReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub raw_root: PathBuf,
    pub state_root: Option<PathBuf>,
    pub generated_ts_ns: i64,
    pub max_age_ms: i64,
    pub healthy: bool,
    pub manifest_count: usize,
    pub record_count: u64,
    pub segment_bytes: u64,
    pub min_ts_ns: Option<i64>,
    pub max_ts_ns: Option<i64>,
    pub latest_manifest_path: Option<PathBuf>,
    pub latest_manifest_mtime_ts_ns: Option<i64>,
    pub state_path: Option<PathBuf>,
    pub state_exists: bool,
    pub state_last_recv_ts_ns: Option<i64>,
    pub state_connection_id: Option<u64>,
    pub state_subscription_epoch: Option<u64>,
    pub state_asset_count: Option<usize>,
    pub state_assets_missing_market_metadata: Option<usize>,
    pub state_last_error: Option<String>,
    pub latest_data_ts_ns: Option<i64>,
    pub latest_data_age_ms: Option<i64>,
    pub violations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawCoverageReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub raw_root: PathBuf,
    pub stream_filter: Option<String>,
    pub generated_ts_ns: i64,
    pub include_record_stats: bool,
    pub manifest_count: usize,
    pub record_count: u64,
    pub segment_bytes: u64,
    pub min_ts_ns: Option<i64>,
    pub max_ts_ns: Option<i64>,
    pub first_hour_bucket: Option<i64>,
    pub last_hour_bucket: Option<i64>,
    pub expected_hour_count: usize,
    pub present_hour_count: usize,
    pub missing_hour_count: usize,
    pub gaps: Vec<RawCoverageGap>,
    pub hours: Vec<RawCoverageHour>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawCoverageGap {
    pub start_hour_bucket: i64,
    pub end_hour_bucket_exclusive: i64,
    pub start_ts_ns: i64,
    pub end_ts_ns: i64,
    pub hour_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawCoverageHour {
    pub hour_bucket: i64,
    pub start_ts_ns: i64,
    pub end_ts_ns: i64,
    pub manifest_count: usize,
    pub record_count: u64,
    pub segment_bytes: u64,
    pub min_ts_ns: Option<i64>,
    pub max_ts_ns: Option<i64>,
    pub condition_count: Option<usize>,
    pub missing_market_metadata_count: Option<u64>,
    pub symbol_counts: BTreeMap<String, u64>,
    pub event_type_counts: BTreeMap<String, u64>,
}
