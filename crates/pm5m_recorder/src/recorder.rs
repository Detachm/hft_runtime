use crate::types::*;
use anyhow::{anyhow, Context, Result};
use market_data_etl_core::{hash_path, now_unix_ns, sha256_bytes, write_json_file_pretty};
use pm5m_data_etl::{BookLevel, RawPolymarketBookTop10};
use serde_json::Value;
use std::collections::BTreeSet;
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
