use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const RECORDER_CONFIG_FORMAT: &str = "pm5m_recorder_config.v1";
pub const RECORDER_STATE_FORMAT: &str = "pm5m_recorder_state.v1";
pub const RECORDER_MANIFEST_FORMAT: &str = "pm5m_recorder_manifest.v1";

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
            source: PolymarketRecorderSource::default(),
        }
    }
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecorderState {
    pub schema_version: u32,
    pub dataset_format: String,
    pub next_ingest_seq: u64,
    pub cycle_count: u64,
    pub total_rows: u64,
    pub total_errors: u64,
    pub last_cycle_start_ts_ns: Option<i64>,
    pub last_cycle_end_ts_ns: Option<i64>,
    pub last_error: Option<String>,
    pub last_assets: Vec<AssetSpec>,
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
        }
    }
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
    pub last_cycle_rows: usize,
    pub last_cycle_errors: usize,
}
