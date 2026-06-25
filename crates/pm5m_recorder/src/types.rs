use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const RECORDER_CONFIG_FORMAT: &str = "pm5m_recorder_config.v1";
pub const RECORDER_STATE_FORMAT: &str = "pm5m_recorder_state.v1";
pub const RECORDER_MANIFEST_FORMAT: &str = "pm5m_recorder_manifest.v1";
pub const RECORDER_EVIDENCE_FORMAT: &str = "pm5m_recorder_evidence.v1";
pub const RECORDER_EVIDENCE_SOURCE: &str = "polymarket_recorder_evidence";
pub const WS_RECORDER_STATE_FORMAT: &str = "pm5m_clob_ws_recorder_state.v1";
pub const REFERENCE_WS_RECORDER_STATE_FORMAT: &str = "pm5m_reference_ws_recorder_state.v1";
pub const RAW_POLYMARKET_CLOB_WS_STREAM: &str = "polymarket_clob_ws_raw";
pub const RAW_POLYMARKET_CLOB_WS_SOURCE_ID: &str = "polymarket_clob_ws";
pub const RAW_REFERENCE_WS_STREAM: &str = "reference_ws_raw";
pub const RECORDER_AUDIT_PROFILE_FORMAT: &str = "pm5m_recorder_audit_profile.v1";
pub const RECORDER_RUN_MANIFEST_FORMAT: &str = "pm5m_recorder_run_manifest.v1";
pub const RECORDER_HEALTH_STREAM: &str = "recorder_health.jsonl";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecorderConfig {
    pub schema_version: u32,
    pub dataset_format: String,
    pub raw_root: PathBuf,
    pub state_root: PathBuf,
    pub poll_interval_ms: u64,
    pub discovery_interval_cycles: u64,
    pub max_assets_per_cycle: usize,
    pub top_n: usize,
    #[serde(default = "default_http_timeout_ms")]
    pub http_timeout_ms: u64,
    #[serde(default = "default_http_max_retries")]
    pub http_max_retries: u32,
    #[serde(default = "default_http_retry_backoff_ms")]
    pub http_retry_backoff_ms: u64,
    pub source: PolymarketRecorderSource,
}

impl RecorderConfig {
    pub fn default_for_roots(raw_root: PathBuf, state_root: PathBuf) -> Self {
        Self {
            schema_version: 1,
            dataset_format: RECORDER_CONFIG_FORMAT.to_string(),
            raw_root,
            state_root,
            poll_interval_ms: 1_000,
            discovery_interval_cycles: 60,
            max_assets_per_cycle: 24,
            top_n: 10,
            http_timeout_ms: default_http_timeout_ms(),
            http_max_retries: default_http_max_retries(),
            http_retry_backoff_ms: default_http_retry_backoff_ms(),
            source: PolymarketRecorderSource::default(),
        }
    }
}

fn default_http_timeout_ms() -> u64 {
    5_000
}

fn default_http_max_retries() -> u32 {
    2
}

fn default_http_retry_backoff_ms() -> u64 {
    250
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolymarketRecorderSource {
    pub clob_base_url: String,
    pub gamma_markets_url: String,
    pub discovery: GammaDiscoveryConfig,
    pub explicit_assets: Vec<AssetSpec>,
}

impl Default for PolymarketRecorderSource {
    fn default() -> Self {
        Self {
            clob_base_url: "https://clob.polymarket.com".to_string(),
            gamma_markets_url: "https://gamma-api.polymarket.com/markets".to_string(),
            discovery: GammaDiscoveryConfig::default(),
            explicit_assets: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GammaDiscoveryConfig {
    #[serde(default = "default_pm5m_symbols")]
    pub pm5m_symbols: Vec<String>,
    #[serde(default = "default_pm5m_intervals")]
    pub pm5m_intervals: Vec<String>,
    #[serde(default = "default_pm5m_past_window_count")]
    pub pm5m_past_window_count: i64,
    #[serde(default = "default_pm5m_future_window_count")]
    pub pm5m_future_window_count: i64,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_discovery_limit")]
    pub limit: usize,
    #[serde(default = "default_discovery_order")]
    pub order: String,
    #[serde(default)]
    pub ascending: bool,
    #[serde(default = "default_true")]
    pub require_accepting_orders: bool,
    #[serde(default = "default_true")]
    pub require_order_book: bool,
    #[serde(default)]
    pub question_or_slug_contains_any: Vec<String>,
}

impl Default for GammaDiscoveryConfig {
    fn default() -> Self {
        Self {
            pm5m_symbols: default_pm5m_symbols(),
            pm5m_intervals: default_pm5m_intervals(),
            pm5m_past_window_count: default_pm5m_past_window_count(),
            pm5m_future_window_count: default_pm5m_future_window_count(),
            enabled: true,
            limit: default_discovery_limit(),
            order: default_discovery_order(),
            ascending: false,
            require_accepting_orders: true,
            require_order_book: true,
            question_or_slug_contains_any: Vec::new(),
        }
    }
}

fn default_pm5m_symbols() -> Vec<String> {
    vec!["BTC".to_string(), "ETH".to_string(), "SOL".to_string()]
}

fn default_pm5m_intervals() -> Vec<String> {
    vec!["5m".to_string()]
}

fn default_pm5m_past_window_count() -> i64 {
    1
}

fn default_pm5m_future_window_count() -> i64 {
    5
}

fn default_discovery_limit() -> usize {
    24
}

fn default_discovery_order() -> String {
    "volume24hr".to_string()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AssetSpec {
    pub symbol: String,
    pub condition_id: String,
    pub asset_id: String,
    pub outcome: String,
    #[serde(default)]
    pub market_start_ts_ns: Option<i64>,
    #[serde(default)]
    pub market_end_ts_ns: Option<i64>,
    #[serde(default)]
    pub yes_asset_id: Option<String>,
    #[serde(default)]
    pub no_asset_id: Option<String>,
    #[serde(default)]
    pub tick_size: Option<String>,
    #[serde(default)]
    pub neg_risk: Option<bool>,
    #[serde(default)]
    pub accepting_orders: Option<bool>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub question: Option<String>,
    #[serde(default)]
    pub slug: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecorderState {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default = "default_recorder_state_format")]
    pub dataset_format: String,
    #[serde(default)]
    pub next_ingest_seq: u64,
    #[serde(default)]
    pub cycle_count: u64,
    #[serde(default)]
    pub total_rows: u64,
    #[serde(default)]
    pub total_errors: u64,
    #[serde(default)]
    pub last_cycle_start_ts_ns: Option<i64>,
    #[serde(default)]
    pub last_cycle_end_ts_ns: Option<i64>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub last_assets: Vec<AssetSpec>,
    #[serde(default)]
    pub asset_states: BTreeMap<String, AssetRunState>,
    #[serde(default)]
    pub last_evidence_path: Option<PathBuf>,
    #[serde(default)]
    pub last_evidence_hash: Option<String>,
}

impl Default for RecorderState {
    fn default() -> Self {
        Self {
            schema_version: 1,
            dataset_format: RECORDER_STATE_FORMAT.to_string(),
            next_ingest_seq: 0,
            cycle_count: 0,
            total_rows: 0,
            total_errors: 0,
            last_cycle_start_ts_ns: None,
            last_cycle_end_ts_ns: None,
            last_error: None,
            last_assets: Vec::new(),
            asset_states: BTreeMap::new(),
            last_evidence_path: None,
            last_evidence_hash: None,
        }
    }
}

fn default_schema_version() -> u32 {
    1
}

fn default_recorder_state_format() -> String {
    RECORDER_STATE_FORMAT.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AssetRunState {
    pub consecutive_failures: u64,
    pub last_success_ts_ns: Option<i64>,
    pub last_failure_ts_ns: Option<i64>,
    pub last_failure_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WsRecorderState {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default = "default_ws_recorder_state_format")]
    pub dataset_format: String,
    #[serde(default)]
    pub next_ingest_seq: u64,
    #[serde(default)]
    pub connection_id: u64,
    #[serde(default)]
    pub subscription_epoch: u64,
    #[serde(default)]
    pub current_assets: Vec<AssetSpec>,
    #[serde(default)]
    pub last_segment_path: Option<PathBuf>,
    #[serde(default)]
    pub last_segment_hash: Option<String>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub last_recv_ts_ns: Option<i64>,
    #[serde(default)]
    pub local_overrun_count: u64,
    #[serde(default)]
    pub local_overrun_last_ts_ns: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReferenceWsRecorderState {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default = "default_reference_ws_recorder_state_format")]
    pub dataset_format: String,
    #[serde(default)]
    pub next_ingest_seq: u64,
    #[serde(default)]
    pub connection_epoch: u64,
    #[serde(default)]
    pub last_segment_path: Option<PathBuf>,
    #[serde(default)]
    pub last_segment_hash: Option<String>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub last_recv_ts_ns: Option<i64>,
    #[serde(default)]
    pub local_overrun_count: u64,
    #[serde(default)]
    pub local_overrun_last_ts_ns: Option<i64>,
}

impl Default for ReferenceWsRecorderState {
    fn default() -> Self {
        Self {
            schema_version: 1,
            dataset_format: REFERENCE_WS_RECORDER_STATE_FORMAT.to_string(),
            next_ingest_seq: 0,
            connection_epoch: 0,
            last_segment_path: None,
            last_segment_hash: None,
            last_error: None,
            last_recv_ts_ns: None,
            local_overrun_count: 0,
            local_overrun_last_ts_ns: None,
        }
    }
}

fn default_reference_ws_recorder_state_format() -> String {
    REFERENCE_WS_RECORDER_STATE_FORMAT.to_string()
}

impl Default for WsRecorderState {
    fn default() -> Self {
        Self {
            schema_version: 1,
            dataset_format: WS_RECORDER_STATE_FORMAT.to_string(),
            next_ingest_seq: 0,
            connection_id: 0,
            subscription_epoch: 0,
            current_assets: Vec::new(),
            last_segment_path: None,
            last_segment_hash: None,
            last_error: None,
            last_recv_ts_ns: None,
            local_overrun_count: 0,
            local_overrun_last_ts_ns: None,
        }
    }
}

fn default_ws_recorder_state_format() -> String {
    WS_RECORDER_STATE_FORMAT.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawPolymarketClobWsEvent {
    pub source_id: String,
    pub ingest_seq_scope: String,
    pub ingest_seq: u64,
    pub local_recv_ts_ns: i64,
    pub connection_id: u64,
    pub subscription_epoch: u64,
    pub asset_id: Option<String>,
    pub condition_id: Option<String>,
    pub symbol: Option<String>,
    pub outcome: Option<String>,
    #[serde(default)]
    pub market_start_ts_ns: Option<i64>,
    #[serde(default)]
    pub market_end_ts_ns: Option<i64>,
    #[serde(default)]
    pub yes_asset_id: Option<String>,
    #[serde(default)]
    pub no_asset_id: Option<String>,
    pub event_type: String,
    pub exchange_ts_ms: Option<i64>,
    pub raw_payload: Vec<u8>,
    pub raw_payload_sha256: String,
    pub raw_record_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RawReferenceWsEvent {
    pub source_id: String,
    pub venue: String,
    pub symbol: String,
    pub ingest_seq_scope: String,
    pub ingest_seq: u64,
    pub local_recv_ts_ns: i64,
    pub connection_epoch: u64,
    pub event_type: String,
    pub exchange_event_ts_ms: Option<i64>,
    pub bar_open_time_ms: Option<i64>,
    pub bar_close_time_ms: Option<i64>,
    pub is_closed: Option<bool>,
    pub open: Option<String>,
    pub high: Option<String>,
    pub low: Option<String>,
    pub close: Option<String>,
    pub volume: Option<String>,
    pub raw_payload: Vec<u8>,
    pub raw_payload_sha256: String,
    pub raw_record_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecorderAuditProfile {
    pub schema_version: u32,
    pub dataset_format: String,
    pub audit_profile_hash: String,
    pub shadow_order_latencies_ms: Vec<u64>,
    pub shadow_tick_offsets: Vec<i32>,
    pub reference_venues: Vec<String>,
    pub reference_symbols: Vec<String>,
    pub book_cache_depth_levels: usize,
    pub price_rounding: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecorderRunManifest {
    pub schema_version: u32,
    pub dataset_format: String,
    pub component: String,
    pub local_start_ts_ns: i64,
    pub raw_root: PathBuf,
    pub state_root: PathBuf,
    #[serde(default)]
    pub typed_root: Option<PathBuf>,
    pub audit_profile_hash: Option<String>,
    pub config_hash: Option<String>,
    #[serde(default)]
    pub git_sha: Option<String>,
    #[serde(default)]
    pub binary_sha256: Option<String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub command_line: Vec<String>,
    #[serde(default)]
    pub alignment_policy: Option<RecorderAlignmentPolicy>,
    pub outputs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecorderAlignmentPolicy {
    pub poly_price_change_latency_ms: u64,
    pub poly_snapshot_policy: String,
    pub binance_reference_latency_ms: u64,
    pub reference_bar_policy: String,
}

impl RecorderAlignmentPolicy {
    pub fn live_replay_default() -> Self {
        Self {
            poly_price_change_latency_ms: 20,
            poly_snapshot_policy: "local_receive_time".to_string(),
            binance_reference_latency_ms: 200,
            reference_bar_policy: "exchange_event_time_plus_latency".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketMetadataSnapshot {
    pub schema_version: u32,
    pub dataset_format: String,
    pub local_recv_ts_ns: i64,
    pub discovery_seq: u64,
    pub condition_id: String,
    pub symbol: String,
    pub market_start_ts_ns: Option<i64>,
    pub market_end_ts_ns: Option<i64>,
    pub yes_asset_id: Option<String>,
    pub no_asset_id: Option<String>,
    pub tick_size: Option<String>,
    pub neg_risk: Option<bool>,
    pub accepting_orders: Option<bool>,
    pub status: Option<String>,
    pub selected: bool,
    pub skip_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecorderHealthEvent {
    pub schema_version: u32,
    pub dataset_format: String,
    pub component: String,
    pub event_type: String,
    pub health_status: String,
    pub local_ts_ns: i64,
    pub rows: u64,
    pub payload_bytes: u64,
    pub segment_path: Option<PathBuf>,
    pub segment_hash: Option<String>,
    pub flush_elapsed_ms: Option<u64>,
    pub channel_capacity: Option<u64>,
    pub connection_id: Option<u64>,
    pub connection_epoch: Option<u64>,
    pub subscription_epoch: Option<u64>,
    pub current_asset_count: Option<u64>,
    pub last_recv_ts_ns: Option<i64>,
    pub last_recv_age_ms: Option<i64>,
    #[serde(default)]
    pub queue_depth: Option<u64>,
    #[serde(default)]
    pub writer_buffer_rows: Option<u64>,
    #[serde(default)]
    pub local_overrun_count: Option<u64>,
    #[serde(default)]
    pub local_overrun_last_ts_ns: Option<i64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecorderManifest {
    pub schema_version: u32,
    pub dataset_format: String,
    pub raw_root: PathBuf,
    pub state_root: PathBuf,
    pub state_hash: Option<String>,
    pub last_output_path: Option<PathBuf>,
    pub last_output_hash: Option<String>,
    pub last_evidence_path: Option<PathBuf>,
    pub last_evidence_hash: Option<String>,
    pub last_cycle_rows: usize,
    pub last_cycle_errors: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecorderEvidenceEvent {
    pub schema_version: u32,
    pub dataset_format: String,
    pub event_kind: RecorderEvidenceKind,
    pub evidence_seq: u64,
    pub source_id: String,
    pub source_identity: String,
    pub request_url: String,
    pub request_start_ts_ns: i64,
    pub request_end_ts_ns: i64,
    pub http_status: Option<u16>,
    pub response_hash: Option<String>,
    pub retry_count: u32,
    pub failure_reason: Option<String>,
    pub raw_payload: Vec<u8>,
    pub raw_event_id: String,
    pub raw_record_hash: String,
    pub receive_monotonic_ns: u64,
    pub symbol: Option<String>,
    pub condition_id: Option<String>,
    pub asset_id: Option<String>,
    pub outcome: Option<String>,
    pub market_start_ts_ns: Option<i64>,
    pub market_end_ts_ns: Option<i64>,
    pub yes_asset_id: Option<String>,
    pub no_asset_id: Option<String>,
    pub skip_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RecorderEvidenceKind {
    #[serde(rename = "discovery_request_success")]
    DiscoveryRequestSuccess,
    #[serde(rename = "discovery_request_failure")]
    DiscoveryRequestFailure,
    #[serde(rename = "market_selected")]
    MarketSelected,
    #[serde(rename = "market_skipped")]
    MarketSkipped,
    #[serde(rename = "book_fetch_success")]
    BookFetchSuccess,
    #[serde(rename = "book_fetch_failure")]
    BookFetchFailure,
    #[serde(rename = "book_parse_failure")]
    BookParseFailure,
}
