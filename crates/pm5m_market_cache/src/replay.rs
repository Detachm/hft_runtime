use crate::types::{
    canonical_market_symbol, from_micros, micros, outcome_code, BookCacheRow,
    RawPolymarketClobWsEvent, WS_BOOK_REPLAY_SOURCE_ID, WS_RAW_STREAM,
};
use anyhow::{anyhow, bail, Context, Result};
use market_data_etl_core::{raw_record_hash, sha256_bytes, BookLevel, RawPolymarketBookTop10};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Default)]
pub struct CanonicalWsBookReplayer {
    inner: WsBookReplayer,
}

impl CanonicalWsBookReplayer {
    pub fn apply_ws_raw_row<T: Serialize>(&mut self, row: &T) -> Result<Vec<BookCacheRow>> {
        let raw = serde_json::from_value::<RawPolymarketClobWsEvent>(
            serde_json::to_value(row).context("serialize WS raw row for canonical replay")?,
        )
        .context("decode WS raw row for canonical replay")?;
        self.apply(raw)
    }

    pub fn apply(&mut self, row: RawPolymarketClobWsEvent) -> Result<Vec<BookCacheRow>> {
        let mut out = Vec::new();
        for derived in self.inner.apply(row)? {
            if let Some(row) = BookCacheRow::from_raw_book(&derived)? {
                out.push(row);
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Default)]
struct WsBookReplayer {
    books: BTreeMap<String, WsBookState>,
    conditions: BTreeMap<String, WsConditionState>,
}

impl WsBookReplayer {
    fn apply(&mut self, row: RawPolymarketClobWsEvent) -> Result<Vec<RawPolymarketBookTop10>> {
        let event_type = row.event_type.trim().to_ascii_lowercase();
        if event_type != "book" && event_type != "price_change" {
            return Ok(Vec::new());
        }
        let payload = if row.raw_payload.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&row.raw_payload)
                .with_context(|| format!("parse WS raw payload at ingest_seq {}", row.ingest_seq))?
        };
        match event_type.as_str() {
            "book" => self.apply_book(row, &payload),
            "price_change" => self.apply_price_change(row, &payload),
            _ => Ok(Vec::new()),
        }
    }

    fn apply_book(
        &mut self,
        row: RawPolymarketClobWsEvent,
        payload: &Value,
    ) -> Result<Vec<RawPolymarketBookTop10>> {
        let Some(asset_id) = row
            .asset_id
            .clone()
            .or_else(|| string_value(payload, &["asset_id", "assetId", "token_id", "tokenId"]))
        else {
            return Ok(Vec::new());
        };
        let Some(symbol) = row
            .symbol
            .as_deref()
            .and_then(canonical_market_symbol)
            .or_else(|| {
                string_value(payload, &["symbol"]).and_then(|value| canonical_market_symbol(&value))
            })
        else {
            return Ok(Vec::new());
        };
        let Some(condition_id) = row
            .condition_id
            .clone()
            .or_else(|| string_value(payload, &["condition_id", "conditionId", "market"]))
        else {
            return Ok(Vec::new());
        };
        let Some(outcome) = row
            .outcome
            .clone()
            .or_else(|| string_value(payload, &["outcome"]))
        else {
            return Ok(Vec::new());
        };

        let normalized_outcome = normalize_binary_outcome(&outcome);
        let existing_window = self.conditions.get(&condition_id).and_then(|condition| {
            Some((condition.market_start_ts_ns?, condition.market_end_ts_ns?))
        });
        let inferred_window =
            existing_window.or_else(|| infer_market_window_ns(&symbol, row.local_recv_ts_ns));
        let market_start_ts_ns = row
            .market_start_ts_ns
            .or_else(|| inferred_window.map(|window| window.0));
        let market_end_ts_ns = row
            .market_end_ts_ns
            .or_else(|| inferred_window.map(|window| window.1));
        let was_complete = self.condition_complete(&condition_id);
        self.update_condition_metadata(
            &symbol,
            &condition_id,
            normalized_outcome.as_deref(),
            &asset_id,
            market_start_ts_ns,
            market_end_ts_ns,
            row.yes_asset_id.clone(),
            row.no_asset_id.clone(),
        )?;

        let state = WsBookState {
            symbol,
            condition_id: condition_id.clone(),
            outcome,
            context: WsBookContext::from_row(&row),
            bids: parse_ws_levels(payload, &["bids", "buys"])?,
            asks: parse_ws_levels(payload, &["asks", "sells"])?,
        };
        self.books.insert(asset_id.clone(), state);
        if !was_complete && self.condition_complete(&condition_id) {
            return self.raw_books_for_condition(&condition_id);
        }
        self.state_to_raw_book(&asset_id)
            .map(|maybe| maybe.into_iter().collect())
    }

    fn apply_price_change(
        &mut self,
        row: RawPolymarketClobWsEvent,
        payload: &Value,
    ) -> Result<Vec<RawPolymarketBookTop10>> {
        let Some(changes) = payload
            .get("price_changes")
            .or_else(|| payload.get("changes"))
            .and_then(Value::as_array)
        else {
            return Ok(Vec::new());
        };

        let mut touched_assets = BTreeSet::<String>::new();
        for change in changes {
            let Some(asset_id) =
                string_value(change, &["asset_id", "assetId", "token_id", "tokenId"])
                    .or_else(|| row.asset_id.clone())
            else {
                continue;
            };
            let Some(state) = self.books.get_mut(&asset_id) else {
                continue;
            };
            let Some(side) = parse_ws_side(change) else {
                continue;
            };
            let Some(price) = decimal_micros(change, &["price"], "price_change price")? else {
                continue;
            };
            let Some(size) =
                decimal_micros(change, &["size", "qty", "quantity"], "price_change size")?
            else {
                continue;
            };
            match side {
                WsBookSide::Bid => update_level(&mut state.bids, price, size),
                WsBookSide::Ask => update_level(&mut state.asks, price, size),
            }
            let mut context_row = row.clone();
            if context_row.local_recv_ts_ns < state.context.local_recv_ts_ns {
                context_row.local_recv_ts_ns = state.context.local_recv_ts_ns;
            }
            state.context = WsBookContext::from_row(&context_row);
            touched_assets.insert(asset_id);
        }

        let mut out = Vec::new();
        for asset_id in touched_assets {
            if let Some(raw) = self.state_to_raw_book(&asset_id)? {
                out.push(raw);
            }
        }
        Ok(out)
    }

    fn state_to_raw_book(&self, asset_id: &str) -> Result<Option<RawPolymarketBookTop10>> {
        let state = self
            .books
            .get(asset_id)
            .ok_or_else(|| anyhow!("missing replay state for asset {asset_id}"))?;
        let Some(condition) = self.conditions.get(&state.condition_id) else {
            return Ok(None);
        };
        if !condition.complete() {
            return Ok(None);
        }
        let market_start_ts_ns = condition.market_start_ts_ns.ok_or_else(|| {
            anyhow!(
                "missing inferred market_start_ts_ns for {}",
                state.condition_id
            )
        })?;
        let market_end_ts_ns = condition.market_end_ts_ns.ok_or_else(|| {
            anyhow!(
                "missing inferred market_end_ts_ns for {}",
                state.condition_id
            )
        })?;
        let yes_asset_id = condition
            .yes_asset_id
            .clone()
            .ok_or_else(|| anyhow!("missing inferred YES asset for {}", state.condition_id))?;
        let no_asset_id = condition
            .no_asset_id
            .clone()
            .ok_or_else(|| anyhow!("missing inferred NO asset for {}", state.condition_id))?;
        let raw_payload_hash = if state.context.raw_payload_sha256.is_empty() {
            sha256_bytes(&state.context.raw_payload)
        } else {
            state.context.raw_payload_sha256.clone()
        };
        let mut raw = RawPolymarketBookTop10 {
            source_id: WS_BOOK_REPLAY_SOURCE_ID.to_string(),
            ingest_seq_scope: if state.context.ingest_seq_scope.is_empty() {
                WS_BOOK_REPLAY_SOURCE_ID.to_string()
            } else {
                state.context.ingest_seq_scope.clone()
            },
            receive_monotonic_ns: u64::try_from(state.context.local_recv_ts_ns).unwrap_or_default(),
            raw_message_seq: state.context.ingest_seq,
            raw_event_id: format!(
                "{WS_BOOK_REPLAY_SOURCE_ID}:{asset_id}:{}:{}",
                state.context.local_recv_ts_ns, state.context.ingest_seq
            ),
            raw_record_hash: String::new(),
            source_identity: if state.context.source_id.is_empty() {
                WS_RAW_STREAM.to_string()
            } else {
                state.context.source_id.clone()
            },
            request_url: format!("ws://{WS_RAW_STREAM}/{asset_id}"),
            request_start_ts_ns: state.context.local_recv_ts_ns,
            request_end_ts_ns: state.context.local_recv_ts_ns,
            http_status: 0,
            response_hash: raw_payload_hash,
            retry_count: 0,
            raw_payload: state.context.raw_payload.clone(),
            symbol: state.symbol.clone(),
            condition_id: state.condition_id.clone(),
            asset_id: asset_id.to_string(),
            outcome: state.outcome.clone(),
            market_start_ts_ns: Some(market_start_ts_ns),
            market_end_ts_ns: Some(market_end_ts_ns),
            yes_asset_id: Some(yes_asset_id),
            no_asset_id: Some(no_asset_id),
            exchange_ts_ms: state.context.exchange_ts_ms,
            local_recv_ts_ns: state.context.local_recv_ts_ns,
            ingest_seq: state.context.ingest_seq,
            bids: sorted_book_levels(&state.bids, WsBookSide::Bid),
            asks: sorted_book_levels(&state.asks, WsBookSide::Ask),
        };
        raw.raw_record_hash = raw_record_hash(&raw)?;
        Ok(Some(raw))
    }

    fn condition_complete(&self, condition_id: &str) -> bool {
        self.conditions
            .get(condition_id)
            .is_some_and(WsConditionState::complete)
    }

    fn raw_books_for_condition(&self, condition_id: &str) -> Result<Vec<RawPolymarketBookTop10>> {
        let mut out = Vec::new();
        for (asset_id, state) in &self.books {
            if state.condition_id == condition_id {
                if let Some(raw) = self.state_to_raw_book(asset_id)? {
                    out.push(raw);
                }
            }
        }
        out.sort_by(|a, b| {
            (a.local_recv_ts_ns, a.ingest_seq, &a.asset_id).cmp(&(
                b.local_recv_ts_ns,
                b.ingest_seq,
                &b.asset_id,
            ))
        });
        Ok(out)
    }

    fn update_condition_metadata(
        &mut self,
        symbol: &str,
        condition_id: &str,
        outcome: Option<&str>,
        asset_id: &str,
        market_start_ts_ns: Option<i64>,
        market_end_ts_ns: Option<i64>,
        yes_asset_id: Option<String>,
        no_asset_id: Option<String>,
    ) -> Result<()> {
        let condition = self
            .conditions
            .entry(condition_id.to_string())
            .or_insert_with(|| WsConditionState {
                symbol: symbol.to_string(),
                ..WsConditionState::default()
            });
        if condition.symbol != symbol {
            bail!(
                "WS replay condition {} appears under multiple symbols: {} and {}",
                condition_id,
                condition.symbol,
                symbol
            );
        }
        merge_optional_i64(
            &mut condition.market_start_ts_ns,
            market_start_ts_ns,
            "market_start_ts_ns",
            condition_id,
        )?;
        merge_optional_i64(
            &mut condition.market_end_ts_ns,
            market_end_ts_ns,
            "market_end_ts_ns",
            condition_id,
        )?;
        merge_optional_string(
            &mut condition.yes_asset_id,
            yes_asset_id,
            "yes_asset_id",
            condition_id,
        )?;
        merge_optional_string(
            &mut condition.no_asset_id,
            no_asset_id,
            "no_asset_id",
            condition_id,
        )?;
        match outcome {
            Some("YES") => merge_optional_string(
                &mut condition.yes_asset_id,
                Some(asset_id.to_string()),
                "yes_asset_id",
                condition_id,
            )?,
            Some("NO") => merge_optional_string(
                &mut condition.no_asset_id,
                Some(asset_id.to_string()),
                "no_asset_id",
                condition_id,
            )?,
            _ => {}
        }
        if let (Some(start), Some(end)) = (condition.market_start_ts_ns, condition.market_end_ts_ns)
        {
            if end <= start {
                bail!("WS replay condition {condition_id} has invalid window {start}..{end}");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct WsBookState {
    symbol: String,
    condition_id: String,
    outcome: String,
    context: WsBookContext,
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

#[derive(Debug, Clone, Default)]
struct WsBookContext {
    source_id: String,
    ingest_seq_scope: String,
    local_recv_ts_ns: i64,
    exchange_ts_ms: Option<i64>,
    ingest_seq: u64,
    raw_payload: Vec<u8>,
    raw_payload_sha256: String,
}

impl WsBookContext {
    fn from_row(row: &RawPolymarketClobWsEvent) -> Self {
        Self {
            source_id: row.source_id.clone(),
            ingest_seq_scope: row.ingest_seq_scope.clone(),
            local_recv_ts_ns: row.local_recv_ts_ns,
            exchange_ts_ms: row
                .exchange_ts_ms
                .or_else(|| timestamp_ms_for_payload(&row.raw_payload)),
            ingest_seq: row.ingest_seq,
            raw_payload: row.raw_payload.clone(),
            raw_payload_sha256: row.raw_payload_sha256.clone(),
        }
    }
}

fn timestamp_ms_for_payload(raw_payload: &[u8]) -> Option<i64> {
    let value = serde_json::from_slice::<Value>(raw_payload).ok()?;
    for key in ["timestamp", "ts", "time", "exchange_ts_ms"] {
        let Some(field) = value.get(key) else {
            continue;
        };
        if let Some(raw) = field.as_i64() {
            return Some(if raw < 10_000_000_000_000 {
                raw
            } else {
                raw / 1_000_000
            });
        }
        if let Some(raw) = field.as_str().and_then(|raw| raw.parse::<i64>().ok()) {
            return Some(if raw < 10_000_000_000_000 {
                raw
            } else {
                raw / 1_000_000
            });
        }
    }
    None
}

#[derive(Debug, Clone, Default)]
struct WsConditionState {
    symbol: String,
    market_start_ts_ns: Option<i64>,
    market_end_ts_ns: Option<i64>,
    yes_asset_id: Option<String>,
    no_asset_id: Option<String>,
}

impl WsConditionState {
    fn complete(&self) -> bool {
        self.market_start_ts_ns.is_some()
            && self.market_end_ts_ns.is_some()
            && self
                .yes_asset_id
                .as_deref()
                .is_some_and(|value| !value.is_empty())
            && self
                .no_asset_id
                .as_deref()
                .is_some_and(|value| !value.is_empty())
    }
}

#[derive(Debug, Clone, Copy)]
enum WsBookSide {
    Bid,
    Ask,
}

fn parse_ws_levels(payload: &Value, keys: &[&str]) -> Result<BTreeMap<i64, i64>> {
    let Some(levels) = keys
        .iter()
        .find_map(|key| payload.get(*key).and_then(Value::as_array))
    else {
        return Ok(BTreeMap::new());
    };
    let mut out = BTreeMap::new();
    for level in levels {
        let Some(price) = decimal_micros(level, &["price", "p"], "book level price")? else {
            continue;
        };
        let Some(size) = decimal_micros(level, &["size", "qty", "quantity"], "book level size")?
        else {
            continue;
        };
        update_level(&mut out, price, size);
    }
    Ok(out)
}

fn update_level(book: &mut BTreeMap<i64, i64>, price: i64, size: i64) {
    if price <= 0 || size <= 0 {
        book.remove(&price);
    } else {
        book.insert(price, size);
    }
}

fn sorted_book_levels(levels: &BTreeMap<i64, i64>, side: WsBookSide) -> Vec<BookLevel> {
    let mut fixed = levels
        .iter()
        .map(|(price, size)| (*price, *size))
        .collect::<Vec<_>>();
    match side {
        WsBookSide::Bid => fixed.sort_by(|a, b| b.0.cmp(&a.0)),
        WsBookSide::Ask => fixed.sort_by(|a, b| a.0.cmp(&b.0)),
    }
    fixed
        .into_iter()
        .take(10)
        .map(|(price, size)| BookLevel {
            price: from_micros(price),
            size: from_micros(size),
        })
        .collect()
}

fn parse_ws_side(value: &Value) -> Option<WsBookSide> {
    let side = string_value(value, &["side", "side_type", "book_side"])?;
    match side.trim().to_ascii_uppercase().as_str() {
        "BUY" | "BID" | "BIDS" => Some(WsBookSide::Bid),
        "SELL" | "ASK" | "ASKS" | "OFFER" | "OFFERS" => Some(WsBookSide::Ask),
        _ => None,
    }
}

fn normalize_binary_outcome(outcome: &str) -> Option<&'static str> {
    match outcome.trim().to_ascii_uppercase().as_str() {
        "YES" | "UP" => Some("YES"),
        "NO" | "DOWN" => Some("NO"),
        _ => None,
    }
}

fn infer_market_window_ns(symbol: &str, ts_ns: i64) -> Option<(i64, i64)> {
    let (_, horizon) = symbol.split_once('-')?;
    let duration_ns = match horizon.to_ascii_uppercase().as_str() {
        "5M" => 300_i64.checked_mul(1_000_000_000)?,
        "15M" => 900_i64.checked_mul(1_000_000_000)?,
        "1H" => 3_600_i64.checked_mul(1_000_000_000)?,
        _ => return None,
    };
    let start = ts_ns.div_euclid(duration_ns).checked_mul(duration_ns)?;
    let end = start.checked_add(duration_ns)?;
    Some((start, end))
}

fn merge_optional_i64(
    target: &mut Option<i64>,
    value: Option<i64>,
    field: &str,
    condition_id: &str,
) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    match *target {
        Some(existing) if existing != value => bail!(
            "WS replay condition {} has inconsistent {}: {} vs {}",
            condition_id,
            field,
            existing,
            value
        ),
        Some(_) => Ok(()),
        None => {
            *target = Some(value);
            Ok(())
        }
    }
}

fn merge_optional_string(
    target: &mut Option<String>,
    value: Option<String>,
    field: &str,
    condition_id: &str,
) -> Result<()> {
    let Some(value) = value.filter(|value| !value.trim().is_empty()) else {
        return Ok(());
    };
    match target.as_deref() {
        Some(existing) if existing != value => bail!(
            "WS replay condition {} has inconsistent {}: {} vs {}",
            condition_id,
            field,
            existing,
            value
        ),
        Some(_) => Ok(()),
        None => {
            *target = Some(value);
            Ok(())
        }
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

#[allow(dead_code)]
fn _outcome_code_for_replay(outcome: &str) -> u8 {
    outcome_code(outcome)
}
