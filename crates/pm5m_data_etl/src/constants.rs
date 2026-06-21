pub const PLAN_FILE: &str = "pipeline_plan.json";
pub const CACHE_MANIFEST: &str = "cache_manifest.json";
pub const MATERIALIZATION_REPORT: &str = "manifests/materialization_report.json";
pub const DATASET_MANIFEST: &str = "manifests/dataset_manifest.json";
pub const ACCEPTANCE_REPORT: &str = "manifests/acceptance_report.json";
pub(crate) const EXPORT_MANIFEST: &str = "export_manifest.json";

pub(crate) const TABLE_MARKET_DIM: &str = "tables/market_dim";
pub(crate) const TABLE_BOOK_TOP10: &str = "tables/polymarket_book_top10";
pub(crate) const TABLE_BINANCE_REFERENCE: &str = "tables/okx_kline_1s_reference";
pub(crate) const TABLE_SETTLEMENT: &str = "tables/polymarket_settlement";
pub(crate) const TABLE_INPUT_AVAILABILITY: &str = "tables/input_availability";
pub(crate) const TABLE_DEPTH_FEATURE: &str = "derived/depth_feature_stream_v1_rust_all";
pub(crate) const TABLE_EVENT_INDEX: &str = "streams/pm5m_standard_event_index_v1";

pub(crate) const BANNED_STRATEGY_FIELDS: &[&str] = &[
    "fair_value",
    "model_probability",
    "edge",
    "trigger",
    "side_decision",
    "pnl",
    "tradeable_interval",
];
