use crate::types::*;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use market_data_etl_core::{hash_path, now_unix_ns, sha256_bytes, write_json_file_pretty};
use pm5m_data_etl::{BookLevel, RawPolymarketBookTop10};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

pub trait HttpFetcher {
    fn get(&self, url: &str) -> Result<Vec<u8>>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BlockingHttpFetcher;

impl HttpFetcher for BlockingHttpFetcher {
    fn get(&self, url: &str) -> Result<Vec<u8>> {
        let response = reqwest::blocking::get(url)
            .with_context(|| format!("GET {}", url))?
            .error_for_status()
            .with_context(|| format!("HTTP status for {}", url))?;
        Ok(response.bytes().context("read HTTP response")?.to_vec())
    }
}

#[derive(Debug, Clone)]
pub struct RecordCycleReport {
    pub rows_written: usize,
    pub errors: Vec<String>,
    pub output_path: Option<PathBuf>,
    pub assets_recorded: usize,
}

pub fn run_forever(config: &RecorderConfig, fetcher: &dyn HttpFetcher) -> Result<()> {
    loop {
        let report = run_once(config, fetcher)?;
        eprintln!(
            "pm5m-recorder cycle rows={} assets={} errors={}",
            report.rows_written,
            report.assets_recorded,
            report.errors.len()
        );
        thread::sleep(Duration::from_millis(config.poll_interval_ms));
    }
}

pub fn run_once(config: &RecorderConfig, fetcher: &dyn HttpFetcher) -> Result<RecordCycleReport> {
    fs::create_dir_all(&config.raw_root)
        .with_context(|| format!("create raw root {}", config.raw_root.display()))?;
    fs::create_dir_all(&config.state_root)
        .with_context(|| format!("create state root {}", config.state_root.display()))?;

    let mut state = read_state(&config.state_root)?;
    let cycle_start = now_unix_ns() as i64;
    state.last_cycle_start_ts_ns = Some(cycle_start);

    let assets = if should_discover(config, &state) {
        match discover_assets(config, fetcher) {
            Ok(assets) => {
                let merged = merge_assets(config.source.explicit_assets.clone(), assets);
                state.last_assets = merged;
                state.last_assets.clone()
            }
            Err(err) if !state.last_assets.is_empty() => {
                state.total_errors += 1;
                state.last_error = Some(format!("discovery failed, reused last assets: {err}"));
                state.last_assets.clone()
            }
            Err(err) => return Err(err).context("discover assets"),
        }
    } else {
        state.last_assets.clone()
    };

    let assets = assets
        .into_iter()
        .take(config.max_assets_per_cycle)
        .collect::<Vec<_>>();

    let mut rows = Vec::new();
    let mut errors = Vec::new();
    for asset in &assets {
        match fetch_book(config, fetcher, asset, config.top_n, state.next_ingest_seq) {
            Ok(row) => {
                state.next_ingest_seq += 1;
                rows.push(row);
            }
            Err(err) => {
                state.total_errors += 1;
                errors.push(format!("{} {}: {err}", asset.symbol, asset.asset_id));
            }
        }
    }

    let output_path = if rows.is_empty() {
        None
    } else {
        let output_path = output_path(&config.raw_root, cycle_start);
        append_jsonl(&output_path, &rows)?;
        Some(output_path)
    };

    state.cycle_count += 1;
    state.total_rows += rows.len() as u64;
    state.last_cycle_end_ts_ns = Some(now_unix_ns() as i64);
    state.last_error = errors.last().cloned().or(state.last_error);
    write_state(&config.state_root, &state)?;
    write_manifest(
        config,
        &state,
        output_path.as_deref(),
        rows.len(),
        errors.len(),
    )?;

    Ok(RecordCycleReport {
        rows_written: rows.len(),
        errors,
        output_path,
        assets_recorded: assets.len(),
    })
}

fn discover_assets(config: &RecorderConfig, fetcher: &dyn HttpFetcher) -> Result<Vec<AssetSpec>> {
    if !config.source.discovery.enabled {
        return Ok(config.source.explicit_assets.clone());
    }
    if !config.source.discovery.pm5m_symbols.is_empty() {
        return discover_pm5m_assets(config, fetcher);
    }

    let discovery = &config.source.discovery;
    let mut url = format!(
        "{}?active=true&closed=false&limit={}&order={}&ascending={}",
        config.source.gamma_markets_url,
        discovery.limit,
        percent_encode(&discovery.order),
        discovery.ascending
    );
    if discovery.require_accepting_orders {
        url.push_str("&accepting_orders=true");
    }

    let bytes = fetcher.get(&url)?;
    let value: Value = serde_json::from_slice(&bytes).context("parse Gamma markets response")?;
    let markets = value
        .as_array()
        .ok_or_else(|| anyhow!("Gamma markets response must be a JSON array"))?;

    let mut assets = Vec::new();
    for market in markets {
        if !market_matches(market, discovery) {
            continue;
        }
        let condition_id = string_field(market, &["conditionId", "condition_id"])
            .ok_or_else(|| anyhow!("Gamma market missing condition id"))?;
        let symbol =
            string_field(market, &["slug", "question"]).unwrap_or_else(|| condition_id.clone());
        let outcomes = parse_string_array(market.get("outcomes"));
        let token_ids = parse_string_array(market.get("clobTokenIds"));
        for (idx, asset_id) in token_ids.into_iter().enumerate() {
            let outcome = outcomes
                .get(idx)
                .cloned()
                .unwrap_or_else(|| format!("outcome-{idx}"));
            assets.push(AssetSpec {
                symbol: symbol.clone(),
                condition_id: condition_id.clone(),
                asset_id,
                outcome,
            });
        }
    }

    Ok(merge_assets(config.source.explicit_assets.clone(), assets))
}

fn discover_pm5m_assets(
    config: &RecorderConfig,
    fetcher: &dyn HttpFetcher,
) -> Result<Vec<AssetSpec>> {
    let discovery = &config.source.discovery;
    let base = gamma_base_url(&config.source.gamma_markets_url);
    let current_s = (now_unix_ns() / 1_000_000_000) as i64;
    let epoch = (current_s / 300) * 300;
    let mut urls = Vec::new();

    for raw_symbol in &discovery.pm5m_symbols {
        let symbol = raw_symbol.to_ascii_uppercase();
        if let Some(series_slug) = pm5m_series_slug(&symbol) {
            urls.push((
                symbol.clone(),
                format!(
                    "{base}/events?active=true&closed=false&series_slug={}&limit=8&order=end_date&ascending=true",
                    percent_encode(series_slug)
                ),
            ));
        }
        if let Some(prefix) = pm5m_slug_prefix(&symbol) {
            for offset in -discovery.pm5m_past_window_count..discovery.pm5m_future_window_count {
                urls.push((
                    symbol.clone(),
                    format!(
                        "{base}/events/slug/{prefix}-updown-5m-{}",
                        epoch + offset * 300
                    ),
                ));
            }
        }
    }

    let mut markets = BTreeMap::new();
    for (symbol, url) in urls {
        let Ok(bytes) = fetcher.get(&url) else {
            continue;
        };
        let Ok(payload) = serde_json::from_slice::<Value>(&bytes) else {
            continue;
        };
        for market in parse_pm5m_gamma_payload(&payload, Some(&symbol)) {
            let lower_ms = current_s * 1000 - 300_000;
            let upper_ms = current_s * 1000 + 2_400_000;
            if market.window_end_ms >= lower_ms && market.window_start_ms <= upper_ms {
                markets.insert(market.condition_id.clone(), market);
            }
        }
    }

    let mut assets = Vec::new();
    for market in markets.into_values() {
        assets.push(AssetSpec {
            symbol: market.symbol.clone(),
            condition_id: market.condition_id.clone(),
            asset_id: market.yes_asset_id,
            outcome: "YES".to_string(),
        });
        assets.push(AssetSpec {
            symbol: market.symbol,
            condition_id: market.condition_id,
            asset_id: market.no_asset_id,
            outcome: "NO".to_string(),
        });
    }

    let merged = merge_assets(config.source.explicit_assets.clone(), assets);
    if merged.is_empty() {
        return Err(anyhow!("no PM5M markets discovered"));
    }
    Ok(merged)
}

fn market_matches(market: &Value, discovery: &GammaDiscoveryConfig) -> bool {
    if bool_field(market, &["closed"]).unwrap_or(false) {
        return false;
    }
    if !bool_field(market, &["active"]).unwrap_or(true) {
        return false;
    }
    if discovery.require_accepting_orders
        && !bool_field(market, &["acceptingOrders", "accepting_orders"]).unwrap_or(false)
    {
        return false;
    }
    if discovery.require_order_book
        && !bool_field(market, &["enableOrderBook", "enable_order_book"]).unwrap_or(false)
    {
        return false;
    }
    if discovery.question_or_slug_contains_any.is_empty() {
        return true;
    }
    let haystack = format!(
        "{} {}",
        string_field(market, &["question"]).unwrap_or_default(),
        string_field(market, &["slug"]).unwrap_or_default()
    )
    .to_ascii_lowercase();
    discovery
        .question_or_slug_contains_any
        .iter()
        .any(|needle| haystack.contains(&needle.to_ascii_lowercase()))
}

fn fetch_book(
    config: &RecorderConfig,
    fetcher: &dyn HttpFetcher,
    asset: &AssetSpec,
    top_n: usize,
    ingest_seq: u64,
) -> Result<RawPolymarketBookTop10> {
    let url = format!(
        "{}/book?token_id={}",
        config.source.clob_base_url.trim_end_matches('/'),
        percent_encode(&asset.asset_id)
    );
    let bytes = fetcher.get(&url)?;
    let value: Value = serde_json::from_slice(&bytes).context("parse CLOB book response")?;

    let mut bids = parse_levels(value.get("bids")).context("parse bids")?;
    let mut asks = parse_levels(value.get("asks")).context("parse asks")?;
    bids.sort_by(|a, b| {
        b.price
            .partial_cmp(&a.price)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    asks.sort_by(|a, b| {
        a.price
            .partial_cmp(&b.price)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    bids.truncate(top_n);
    asks.truncate(top_n);

    Ok(RawPolymarketBookTop10 {
        symbol: asset.symbol.clone(),
        condition_id: asset.condition_id.clone(),
        asset_id: asset.asset_id.clone(),
        outcome: asset.outcome.clone(),
        local_recv_ts_ns: now_unix_ns() as i64,
        ingest_seq,
        bids,
        asks,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pm5mMarket {
    symbol: String,
    condition_id: String,
    window_start_ms: i64,
    window_end_ms: i64,
    yes_asset_id: String,
    no_asset_id: String,
}

fn parse_pm5m_gamma_payload(payload: &Value, symbol: Option<&str>) -> Vec<Pm5mMarket> {
    let mut markets = Vec::new();
    for event in gamma_events(payload) {
        for record in gamma_markets_from_event(event) {
            let mut merged = record.clone();
            if let Some(slug) = event.get("slug").cloned() {
                merged["event_slug"] = slug;
            }
            if let Some(title) = event.get("title").cloned() {
                merged["event_title"] = title;
            }
            if let Ok(market) = parse_pm5m_market(&merged, symbol) {
                markets.push(market);
            }
        }
    }
    if markets.is_empty() && payload.is_object() {
        if let Ok(market) = parse_pm5m_market(payload, symbol) {
            markets.push(market);
        }
    }
    markets
}

fn parse_pm5m_market(record: &Value, symbol: Option<&str>) -> Result<Pm5mMarket> {
    let condition_id = string_field(record, &["condition_id", "conditionId"])
        .ok_or_else(|| anyhow!("missing condition id"))?;
    let outcomes = required_string_array(record.get("outcomes")).context("parse outcomes")?;
    let token_ids = required_string_array(
        record
            .get("clobTokenIds")
            .or_else(|| record.get("clob_token_ids")),
    )
    .context("parse clob token ids")?;
    if outcomes.len() != token_ids.len() {
        return Err(anyhow!("outcomes and token ids length mismatch"));
    }
    let mut assets = BTreeMap::new();
    for (outcome, token_id) in outcomes.into_iter().zip(token_ids) {
        assets.insert(normalize_pm5m_outcome(&outcome), token_id);
    }
    let yes_asset_id = assets
        .remove("YES")
        .ok_or_else(|| anyhow!("missing YES asset"))?;
    let no_asset_id = assets
        .remove("NO")
        .ok_or_else(|| anyhow!("missing NO asset"))?;
    let window_end_ms = timestamp_ms(
        record,
        &["market_end_ms", "endDate", "endDateIso", "end_date", "end"],
    )?;
    let window_start_ms = pm5m_window_start_ms(record, window_end_ms);
    if window_end_ms - window_start_ms != 300_000 {
        return Err(anyhow!("not a 5m market"));
    }
    let symbol = symbol
        .map(ToString::to_string)
        .or_else(|| infer_pm5m_symbol(record))
        .ok_or_else(|| anyhow!("missing PM5M symbol"))?;

    Ok(Pm5mMarket {
        symbol,
        condition_id,
        window_start_ms,
        window_end_ms,
        yes_asset_id,
        no_asset_id,
    })
}

fn gamma_events(payload: &Value) -> Vec<&Value> {
    if let Some(items) = payload.as_array() {
        return items.iter().filter(|item| item.is_object()).collect();
    }
    if let Some(items) = payload
        .get("events")
        .or_else(|| payload.get("data"))
        .and_then(Value::as_array)
    {
        return items.iter().filter(|item| item.is_object()).collect();
    }
    if payload.get("markets").is_some() {
        return vec![payload];
    }
    Vec::new()
}

fn gamma_markets_from_event(event: &Value) -> Vec<&Value> {
    if let Some(items) = event.get("markets").and_then(Value::as_array) {
        return items.iter().filter(|item| item.is_object()).collect();
    }
    if event.get("conditionId").is_some()
        && event.get("outcomes").is_some()
        && event.get("clobTokenIds").is_some()
    {
        return vec![event];
    }
    Vec::new()
}

fn required_string_array(value: Option<&Value>) -> Result<Vec<String>> {
    let Some(value) = value else {
        return Err(anyhow!("missing array"));
    };
    let parsed;
    let items = if let Some(text) = value.as_str() {
        parsed = serde_json::from_str::<Value>(text).context("parse JSON string array")?;
        parsed
            .as_array()
            .ok_or_else(|| anyhow!("string field is not an array"))?
    } else {
        value
            .as_array()
            .ok_or_else(|| anyhow!("field is not an array"))?
    };
    if items.is_empty() {
        return Err(anyhow!("array must not be empty"));
    }
    Ok(items
        .iter()
        .map(|item| {
            item.as_str()
                .map(ToString::to_string)
                .unwrap_or_else(|| item.to_string())
        })
        .collect())
}

fn parse_levels(value: Option<&Value>) -> Result<Vec<BookLevel>> {
    let Some(Value::Array(items)) = value else {
        return Ok(Vec::new());
    };
    let mut levels = Vec::new();
    for item in items {
        let price = number_field(item, &["price"]).ok_or_else(|| anyhow!("level missing price"))?;
        let size = number_field(item, &["size"]).ok_or_else(|| anyhow!("level missing size"))?;
        if price.is_finite() && size.is_finite() && size > 0.0 {
            levels.push(BookLevel { price, size });
        }
    }
    Ok(levels)
}

fn should_discover(config: &RecorderConfig, state: &RecorderState) -> bool {
    config.source.discovery.enabled
        && (state.last_assets.is_empty()
            || config.discovery_interval_cycles == 0
            || state.cycle_count % config.discovery_interval_cycles == 0)
}

fn merge_assets(first: Vec<AssetSpec>, second: Vec<AssetSpec>) -> Vec<AssetSpec> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for asset in first.into_iter().chain(second) {
        if seen.insert(asset.asset_id.clone()) {
            out.push(asset);
        }
    }
    out
}

fn append_jsonl(path: &Path, rows: &[RawPolymarketBookTop10]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open append {}", path.display()))?;
    for row in rows {
        serde_json::to_writer(&mut file, row).context("write raw book row")?;
        file.write_all(b"\n").context("write raw book newline")?;
    }
    file.flush().context("flush raw book file")?;
    Ok(())
}

fn output_path(raw_root: &Path, ts_ns: i64) -> PathBuf {
    let hour_bucket = ts_ns / 1_000_000_000 / 3_600;
    raw_root
        .join("polymarket_book_top10")
        .join(format!("hour_bucket={hour_bucket}"))
        .join("polymarket_book_top10.jsonl")
}

fn read_state(state_root: &Path) -> Result<RecorderState> {
    let path = state_path(state_root);
    if !path.exists() {
        return Ok(RecorderState::default());
    }
    let file = fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    serde_json::from_reader(file).with_context(|| format!("parse {}", path.display()))
}

fn write_state(state_root: &Path, state: &RecorderState) -> Result<()> {
    write_json_file_pretty(&state_path(state_root), state)
}

fn write_manifest(
    config: &RecorderConfig,
    state: &RecorderState,
    output_path: Option<&Path>,
    rows: usize,
    errors: usize,
) -> Result<()> {
    let state_file = state_path(&config.state_root);
    let manifest = RecorderManifest {
        schema_version: 1,
        dataset_format: RECORDER_MANIFEST_FORMAT.to_string(),
        raw_root: config.raw_root.clone(),
        state_root: config.state_root.clone(),
        state_hash: if state_file.exists() {
            Some(hash_path(&state_file)?)
        } else {
            None
        },
        last_output_path: output_path.map(Path::to_path_buf),
        last_output_hash: match output_path {
            Some(path) if path.exists() => Some(hash_path(path)?),
            _ => None,
        },
        last_cycle_rows: rows,
        last_cycle_errors: errors,
    };
    let _ = state;
    write_json_file_pretty(&config.state_root.join("recorder_manifest.json"), &manifest)
}

fn state_path(state_root: &Path) -> PathBuf {
    state_root.join("recorder_state.json")
}

fn string_field(value: &Value, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        value.get(*name).and_then(|field| match field {
            Value::String(text) => Some(text.clone()),
            Value::Number(num) => Some(num.to_string()),
            _ => None,
        })
    })
}

fn bool_field(value: &Value, names: &[&str]) -> Option<bool> {
    names.iter().find_map(|name| {
        value.get(*name).and_then(|field| match field {
            Value::Bool(flag) => Some(*flag),
            Value::String(text) => text.parse::<bool>().ok(),
            _ => None,
        })
    })
}

fn number_field(value: &Value, names: &[&str]) -> Option<f64> {
    names.iter().find_map(|name| {
        value.get(*name).and_then(|field| match field {
            Value::Number(num) => num.as_f64(),
            Value::String(text) => text.parse::<f64>().ok(),
            _ => None,
        })
    })
}

fn timestamp_ms(record: &Value, names: &[&str]) -> Result<i64> {
    for name in names {
        let Some(value) = record.get(*name) else {
            continue;
        };
        if let Some(raw) = value.as_i64() {
            return Ok(if raw < 10_000_000_000_000 {
                raw
            } else {
                raw / 1_000_000
            });
        }
        if let Some(text) = value.as_str() {
            let text = text.trim();
            if text.is_empty() {
                continue;
            }
            if let Ok(raw) = text.parse::<i64>() {
                return Ok(if raw < 10_000_000_000_000 {
                    raw
                } else {
                    raw / 1_000_000
                });
            }
            let normalized = if let Some(stripped) = text.strip_suffix('Z') {
                format!("{stripped}+00:00")
            } else {
                text.to_string()
            };
            let dt = DateTime::parse_from_rfc3339(&normalized)
                .map(|dt| dt.with_timezone(&Utc))
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S").map(|dt| dt.and_utc())
                })
                .with_context(|| format!("parse timestamp {text}"))?;
            return Ok(dt.timestamp_millis());
        }
    }
    Err(anyhow!("missing timestamp"))
}

fn pm5m_window_start_ms(record: &Value, end_ms: i64) -> i64 {
    if let Some(slug) = string_field(record, &["slug", "event_slug", "ticker"]) {
        if let Some((_, suffix)) = slug.rsplit_once("-5m-") {
            if suffix.len() == 10 && suffix.chars().all(|ch| ch.is_ascii_digit()) {
                if let Ok(epoch) = suffix.parse::<i64>() {
                    return epoch * 1000;
                }
            }
        }
    }
    end_ms - 300_000
}

fn normalize_pm5m_outcome(outcome: &str) -> String {
    match outcome.trim().to_ascii_uppercase().as_str() {
        "UP" => "YES".to_string(),
        "DOWN" => "NO".to_string(),
        other => other.to_string(),
    }
}

fn infer_pm5m_symbol(record: &Value) -> Option<String> {
    let text = [
        string_field(record, &["question"]),
        string_field(record, &["title"]),
        string_field(record, &["event_title"]),
        string_field(record, &["slug"]),
        string_field(record, &["event_slug"]),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(" ")
    .to_ascii_lowercase();
    if contains_wordish(&text, &["bitcoin", "btc"]) {
        return Some("BTC".to_string());
    }
    if contains_wordish(&text, &["ethereum", "ether", "eth"]) {
        return Some("ETH".to_string());
    }
    if contains_wordish(&text, &["solana", "sol"]) {
        return Some("SOL".to_string());
    }
    None
}

fn contains_wordish(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| {
        text.split(|ch: char| !ch.is_ascii_alphanumeric())
            .any(|word| word == *needle)
    })
}

fn pm5m_series_slug(symbol: &str) -> Option<&'static str> {
    match symbol {
        "BTC" => Some("btc-up-or-down-5m"),
        "ETH" => Some("eth-up-or-down-5m"),
        "SOL" => Some("sol-up-or-down-5m"),
        _ => None,
    }
}

fn pm5m_slug_prefix(symbol: &str) -> Option<&'static str> {
    match symbol {
        "BTC" => Some("btc"),
        "ETH" => Some("eth"),
        "SOL" => Some("sol"),
        _ => None,
    }
}

fn gamma_base_url(gamma_markets_url: &str) -> String {
    let trimmed = gamma_markets_url.trim_end_matches('/');
    trimmed
        .strip_suffix("/markets")
        .unwrap_or(trimmed)
        .to_string()
}

fn parse_string_array(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(ToString::to_string))
            .collect(),
        Some(Value::String(text)) => serde_json::from_str::<Vec<String>>(text).unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn percent_encode(input: &str) -> String {
    let mut out = String::new();
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

#[allow(dead_code)]
fn response_hash(bytes: &[u8]) -> String {
    sha256_bytes(bytes)
}
