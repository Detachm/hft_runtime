use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const PLAN_DATASET_FORMAT: &str = "pm5m_pipeline_plan.v1";
pub const CACHE_MANIFEST_FORMAT: &str = "pm5m_input_cache_manifest.v1";
pub const MARKET_DIM_FORMAT: &str = "pm5m_market_dim.v1";
pub const BOOK_TOP10_FORMAT: &str = "pm5m_polymarket_book_top10.v1";
pub const BINANCE_REFERENCE_FORMAT: &str = "pm5m_binance_kline_1s_reference.v1";
pub const SETTLEMENT_FORMAT: &str = "pm5m_polymarket_settlement.v1";
pub const INPUT_AVAILABILITY_FORMAT: &str = "pm5m_input_availability.v1";
pub const DEPTH_FEATURE_FORMAT: &str = "pm5m_depth_feature_stream.v1";
pub const EVENT_INDEX_FORMAT: &str = "pm5m_standard_event_index.v1";
pub const MATERIALIZATION_REPORT_FORMAT: &str = "pm5m_materialization_report.v1";
pub const DATASET_MANIFEST_FORMAT: &str = "pm5m_jupiter_dataset_manifest.v1";
pub const ACCEPTANCE_REPORT_FORMAT: &str = "pm5m_acceptance_report.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PipelinePlan {
    pub schema_version: u32,
    pub dataset_format: String,
    pub raw_roots: Vec<PathBuf>,
    pub plan_root: PathBuf,
    pub cache_root: PathBuf,
    pub dataset_root: PathBuf,
    pub reference_latency_ms: i64,
    pub accept_fail_closed_on_missing_reference: bool,
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
            reference_latency_ms: 1_000,
            accept_fail_closed_on_missing_reference: false,
            binance_sources: Vec::new(),
            settlement_sources: Vec::new(),
        }
    }
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
pub struct BookLevel {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RawPolymarketBookTop10 {
    pub symbol: String,
    pub condition_id: String,
    pub asset_id: String,
    pub outcome: String,
    pub local_recv_ts_ns: i64,
    pub ingest_seq: u64,
    #[serde(default)]
    pub bids: Vec<BookLevel>,
    #[serde(default)]
    pub asks: Vec<BookLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketDimRow {
    pub schema_version: u32,
    pub dataset_format: String,
    pub symbol: String,
    pub condition_id: String,
    pub asset_id: String,
    pub outcome: String,
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
    pub local_recv_ts_ns: i64,
    pub ingest_seq: u64,
    pub best_bid_price: Option<f64>,
    pub best_ask_price: Option<f64>,
    pub bid_depth_top10: f64,
    pub ask_depth_top10: f64,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
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
