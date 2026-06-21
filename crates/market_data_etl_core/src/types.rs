use serde::{Deserialize, Serialize};

pub const RAW_POLYMARKET_BOOK_SOURCE: &str = "polymarket_rest_snapshot";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BookLevel {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RawPolymarketBookTop10 {
    pub source_id: String,
    pub ingest_seq_scope: String,
    pub receive_monotonic_ns: u64,
    pub raw_message_seq: u64,
    pub raw_event_id: String,
    pub raw_record_hash: String,
    pub source_identity: String,
    pub request_url: String,
    pub request_start_ts_ns: i64,
    pub request_end_ts_ns: i64,
    pub http_status: u16,
    pub response_hash: String,
    pub retry_count: u32,
    pub raw_payload: Vec<u8>,
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
    pub exchange_ts_ms: Option<i64>,
    pub local_recv_ts_ns: i64,
    pub ingest_seq: u64,
    #[serde(default)]
    pub bids: Vec<BookLevel>,
    #[serde(default)]
    pub asks: Vec<BookLevel>,
}
