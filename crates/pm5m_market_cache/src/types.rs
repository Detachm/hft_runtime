use market_data_etl_core::{hash_serializable, BookLevel, RawPolymarketBookTop10};
use serde::{Deserialize, Serialize};

pub const BOOK_CACHE2_FORMAT: &str = "pm5m_book_state_cache.v3.hftbook2";
pub const BOOK_CACHE2_CATALOG: &str = "catalog.hftbook2.json";
pub const HFTBOOK2_SCHEMA_HASH: &str =
    "hftbook2.partition-local-dict.fixed-top10-row.audit-fields.v2";
pub const WS_RAW_STREAM: &str = "polymarket_clob_ws_raw";
pub const WS_BOOK_REPLAY_SOURCE_ID: &str = "polymarket_clob_ws_book_replay";
pub const DEFAULT_POLY_INCREMENTAL_LATENCY_MS: i64 = 20;
pub const DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS: i64 = 500;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BookCacheRow {
    pub symbol: String,
    pub condition_id: String,
    pub asset_id: String,
    pub outcome: String,
    pub window_start_ts_ns: i64,
    pub window_end_ts_ns: i64,
    pub yes_asset_id: String,
    pub no_asset_id: String,
    pub local_recv_ts_ns: i64,
    #[serde(default)]
    pub exchange_ts_ms: Option<i64>,
    pub ingest_seq: u64,
    pub best_bid_price_micros: Option<i64>,
    pub best_ask_price_micros: Option<i64>,
    pub bid_levels: [Option<BookLevelMicros>; 10],
    pub ask_levels: [Option<BookLevelMicros>; 10],
    #[serde(default)]
    pub raw_row_hash: String,
    #[serde(default)]
    pub raw_payload_sha256: String,
    #[serde(default)]
    pub book_state_hash: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookLevelMicros {
    pub price_micros: i64,
    pub qty_micros: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawPolymarketClobWsEvent {
    #[serde(default)]
    pub source_id: String,
    #[serde(default)]
    pub ingest_seq_scope: String,
    #[serde(default)]
    pub ingest_seq: u64,
    #[serde(default)]
    pub local_recv_ts_ns: i64,
    #[serde(default)]
    pub asset_id: Option<String>,
    #[serde(default)]
    pub condition_id: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub market_start_ts_ns: Option<i64>,
    #[serde(default)]
    pub market_end_ts_ns: Option<i64>,
    #[serde(default)]
    pub yes_asset_id: Option<String>,
    #[serde(default)]
    pub no_asset_id: Option<String>,
    #[serde(default)]
    pub event_type: String,
    #[serde(default)]
    pub exchange_ts_ms: Option<i64>,
    #[serde(default)]
    pub raw_payload: Vec<u8>,
    #[serde(default)]
    pub raw_payload_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct MarketDictionary {
    pub schema_version: u32,
    pub dataset_format: String,
    pub symbols: Vec<String>,
    pub conditions: Vec<MarketConditionEntry>,
    pub assets: Vec<MarketAssetEntry>,
    #[serde(default)]
    pub raw_row_hashes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketConditionEntry {
    pub condition_id: String,
    pub symbol_key: u32,
    pub window_start_ts_ns: i64,
    pub window_end_ts_ns: i64,
    #[serde(default)]
    pub yes_asset_id: Option<String>,
    #[serde(default)]
    pub no_asset_id: Option<String>,
    pub yes_asset_key: Option<u32>,
    pub no_asset_key: Option<u32>,
    pub first_seen_ts_ns: i64,
    pub last_seen_ts_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketAssetEntry {
    pub asset_id: String,
    pub condition_key: u32,
    pub outcome: String,
    pub outcome_code: u8,
}

impl BookCacheRow {
    pub fn primary_key(&self) -> String {
        format!(
            "pm_book:{}:{}:{}:{}",
            self.condition_id, self.asset_id, self.local_recv_ts_ns, self.ingest_seq
        )
    }

    pub fn from_raw_book(raw: &RawPolymarketBookTop10) -> anyhow::Result<Option<Self>> {
        let Some(symbol) = canonical_market_symbol(&raw.symbol) else {
            return Ok(None);
        };
        let window_start_ts_ns = raw.market_start_ts_ns.ok_or_else(|| {
            anyhow::anyhow!(
                "raw book missing market_start_ts_ns for {}",
                raw.condition_id
            )
        })?;
        let window_end_ts_ns = raw.market_end_ts_ns.ok_or_else(|| {
            anyhow::anyhow!("raw book missing market_end_ts_ns for {}", raw.condition_id)
        })?;
        if window_end_ts_ns <= window_start_ts_ns {
            anyhow::bail!(
                "raw book has invalid market window for {}: {}..{}",
                raw.condition_id,
                window_start_ts_ns,
                window_end_ts_ns
            );
        }
        let yes_asset_id = raw.yes_asset_id.clone().ok_or_else(|| {
            anyhow::anyhow!("raw book missing yes_asset_id for {}", raw.condition_id)
        })?;
        let no_asset_id = raw.no_asset_id.clone().ok_or_else(|| {
            anyhow::anyhow!("raw book missing no_asset_id for {}", raw.condition_id)
        })?;
        let bid_levels = fixed_levels(&raw.bids);
        let ask_levels = fixed_levels(&raw.asks);
        let raw_payload_sha256 = raw.response_hash.clone();
        let book_state_hash =
            book_state_hash(&raw.asset_id, raw.exchange_ts_ms, &bid_levels, &ask_levels)?;
        Ok(Some(Self {
            symbol,
            condition_id: raw.condition_id.clone(),
            asset_id: raw.asset_id.clone(),
            outcome: raw.outcome.clone(),
            window_start_ts_ns,
            window_end_ts_ns,
            yes_asset_id,
            no_asset_id,
            local_recv_ts_ns: raw.local_recv_ts_ns,
            exchange_ts_ms: raw.exchange_ts_ms,
            ingest_seq: raw.ingest_seq,
            best_bid_price_micros: raw.bids.iter().map(|level| micros(level.price)).max(),
            best_ask_price_micros: raw.asks.iter().map(|level| micros(level.price)).min(),
            bid_levels,
            ask_levels,
            raw_row_hash: raw.raw_record_hash.clone(),
            raw_payload_sha256,
            book_state_hash,
        }))
    }
}

pub fn book_state_hash(
    asset_id: &str,
    exchange_ts_ms: Option<i64>,
    bid_levels: &[Option<BookLevelMicros>; 10],
    ask_levels: &[Option<BookLevelMicros>; 10],
) -> anyhow::Result<String> {
    let bids = bid_levels
        .iter()
        .flatten()
        .map(|level| [level.price_micros, level.qty_micros])
        .collect::<Vec<_>>();
    let asks = ask_levels
        .iter()
        .flatten()
        .map(|level| [level.price_micros, level.qty_micros])
        .collect::<Vec<_>>();
    hash_serializable(&serde_json::json!({
        "schema": "pm5m_book_state_hash.v1",
        "asset_id": asset_id,
        "exchange_ts_ms": exchange_ts_ms,
        "bids": bids,
        "asks": asks,
    }))
}

pub fn canonical_market_symbol(symbol: &str) -> Option<String> {
    let upper = symbol.trim().to_ascii_uppercase();
    if upper.is_empty() {
        return None;
    }
    match upper.as_str() {
        "BTC" | "ETH" | "SOL" => return Some(format!("{upper}-5M")),
        _ => {}
    }
    let (asset, horizon) = upper.split_once('-')?;
    if asset.is_empty()
        || !asset.chars().all(|ch| ch.is_ascii_alphanumeric())
        || horizon.is_empty()
        || !horizon.chars().all(|ch| ch.is_ascii_alphanumeric())
    {
        return None;
    }
    match horizon {
        "5M" | "15M" | "1H" => Some(format!("{asset}-{horizon}")),
        _ => None,
    }
}

pub fn outcome_code(outcome: &str) -> u8 {
    match outcome.trim().to_ascii_uppercase().as_str() {
        "YES" | "UP" => 1,
        "NO" | "DOWN" => 2,
        _ => 0,
    }
}

pub fn micros(value: f64) -> i64 {
    (value * 1_000_000.0).round() as i64
}

pub fn from_micros(value: i64) -> f64 {
    value as f64 / 1_000_000.0
}

pub fn book_level_from_micros(level: BookLevelMicros) -> BookLevel {
    BookLevel {
        price: from_micros(level.price_micros),
        size: from_micros(level.qty_micros),
    }
}

fn fixed_levels(levels: &[BookLevel]) -> [Option<BookLevelMicros>; 10] {
    let mut out = [None; 10];
    for (idx, level) in levels.iter().take(10).enumerate() {
        out[idx] = Some(BookLevelMicros {
            price_micros: micros(level.price),
            qty_micros: micros(level.size),
        });
    }
    out
}
