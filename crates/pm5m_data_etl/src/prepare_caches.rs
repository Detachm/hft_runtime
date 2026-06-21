use crate::cache::read_cached_records;
use crate::constants::{TABLE_BOOK_TOP10, TABLE_MARKET_DIM};
use crate::manifest::{cache_manifest_path, dataset_path, relative_to};
use crate::types::*;
use anyhow::{anyhow, bail, Context, Result};
use chrono::{TimeZone, Utc};
use market_data_etl_core::{
    atomic_write_verified, hash_path, read_parquet_table, verify_parquet_zstd_table,
    write_parquet_table,
};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_LANGUAGE, CACHE_CONTROL};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

const BINANCE_DATA_API_BASE: &str = "https://data-api.binance.vision";
const BINANCE_API_BASE: &str = "https://api.binance.com";
const POLYMARKET_CLOB_BASE: &str = "https://clob.polymarket.com";
const RANGE_PAD_MS: i64 = 5_000;
const DAY_MS: i64 = 86_400_000;
const BINANCE_DAY_CACHE_DIR: &str = "binance_1s";

pub trait CachePreparationHttp: Sync {
    fn get(&self, url: &str) -> Result<Vec<u8>>;
}

#[derive(Debug, Clone)]
pub struct DefaultCachePreparationHttp {
    client: reqwest::blocking::Client,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CachePreparationOptions {
    pub refresh_reference: bool,
    pub refresh_settlement: bool,
    pub refresh_unsettled: bool,
}

impl DefaultCachePreparationHttp {
    pub fn new() -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/json, text/plain, */*"),
        );
        headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .default_headers(headers)
            .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 pm5m-data-etl-cache-prep/0.1")
            .build()
            .context("build cache preparation HTTP client")?;
        Ok(Self { client })
    }
}

impl CachePreparationHttp for DefaultCachePreparationHttp {
    fn get(&self, url: &str) -> Result<Vec<u8>> {
        let response = self
            .client
            .get(url)
            .send()
            .with_context(|| format!("GET {url}"))?
            .error_for_status()
            .with_context(|| format!("HTTP status for {url}"))?;
        Ok(response.bytes().context("read response bytes")?.to_vec())
    }
}

pub fn prepare_caches(
    plan: &PipelinePlan,
    http: &dyn CachePreparationHttp,
) -> Result<CacheManifest> {
    prepare_caches_with_options(plan, http, CachePreparationOptions::default())
}

pub fn prepare_caches_with_options(
    plan: &PipelinePlan,
    http: &dyn CachePreparationHttp,
    options: CachePreparationOptions,
) -> Result<CacheManifest> {
    fs::create_dir_all(&plan.cache_root)
        .with_context(|| format!("create cache root {}", plan.cache_root.display()))?;

    let market_rows = read_parquet_table::<MarketDimRow>(&dataset_path(plan, TABLE_MARKET_DIM))
        .with_context(|| {
            "prepare-caches requires market_dim; run build-facts before prepare-caches"
        })?;
    verify_parquet_zstd_table(&dataset_path(plan, TABLE_BOOK_TOP10), TABLE_BOOK_TOP10)
        .with_context(|| {
            "prepare-caches requires polymarket_book_top10; run build-facts before prepare-caches"
        })?;

    if market_rows.is_empty() {
        bail!("prepare-caches requires non-empty market_dim");
    }

    let mut records = Vec::new();
    records.extend(prepare_binance_reference(
        plan,
        http,
        &market_rows,
        options,
    )?);
    records.extend(prepare_polymarket_settlement(
        plan,
        http,
        &market_rows,
        options,
    )?);

    let manifest = CacheManifest {
        schema_version: 1,
        dataset_format: CACHE_MANIFEST_FORMAT.to_string(),
        records,
    };
    write_cache_manifest_atomic(plan, &manifest)?;
    Ok(manifest)
}

fn prepare_binance_reference(
    plan: &PipelinePlan,
    http: &dyn CachePreparationHttp,
    market_rows: &[MarketDimRow],
    options: CachePreparationOptions,
) -> Result<Vec<CacheRecord>> {
    let ranges = infer_reference_ranges(market_rows)?;

    let mut records = Vec::new();
    for (asset, range) in ranges {
        let exchange_symbol = format!("{asset}USDT");
        for day_start_ms in utc_day_starts(range.start_ms, range.end_ms) {
            records.push(prepare_binance_reference_day(
                plan,
                http,
                &exchange_symbol,
                day_start_ms,
                options,
            )?);
        }
    }
    Ok(records)
}

fn prepare_binance_reference_day(
    plan: &PipelinePlan,
    http: &dyn CachePreparationHttp,
    exchange_symbol: &str,
    day_start_ms: i64,
    options: CachePreparationOptions,
) -> Result<CacheRecord> {
    let day = utc_day_string(day_start_ms)?;
    let day_end_ms = day_start_ms
        .checked_add(DAY_MS)
        .ok_or_else(|| anyhow!("reference day end overflow"))?;
    let name = format!("binance.{exchange_symbol}.1s.{day}");
    let source_url = format!("binance_spot_1s://{exchange_symbol}?date={day}");
    let target = plan
        .cache_root
        .join(BINANCE_DAY_CACHE_DIR)
        .join(exchange_symbol)
        .join(&day);

    if target.exists() && !options.refresh_reference {
        return Ok(CacheRecord {
            group: CacheGroup::Binance1sReference,
            name,
            source_url,
            symbol: Some(exchange_symbol.to_string()),
            start_ts_ns: ms_to_ns(day_start_ms)?,
            end_ts_ns: ms_to_ns(day_end_ms)?,
            status: CacheRecordStatus::Available,
            cache_path: Some(relative_to(&target, &plan.cache_root)?),
            missing_count: 0,
            failure_reason: None,
            response_hash: Some(hash_path(&target)?),
        });
    }

    match download_binance_1s(http, exchange_symbol, day_start_ms, day_end_ms) {
        Ok(rows) if rows.is_empty() => Ok(CacheRecord {
            group: CacheGroup::Binance1sReference,
            name,
            source_url,
            symbol: Some(exchange_symbol.to_string()),
            start_ts_ns: ms_to_ns(day_start_ms)?,
            end_ts_ns: ms_to_ns(day_end_ms)?,
            status: CacheRecordStatus::Missing,
            cache_path: None,
            missing_count: 1,
            failure_reason: Some("Binance returned no 1s klines for requested day".into()),
            response_hash: None,
        }),
        Ok(rows) => {
            write_parquet_table(&target, &rows, Some("bar_open_ts_ns"))?;
            Ok(CacheRecord {
                group: CacheGroup::Binance1sReference,
                name,
                source_url,
                symbol: Some(exchange_symbol.to_string()),
                start_ts_ns: ms_to_ns(day_start_ms)?,
                end_ts_ns: ms_to_ns(day_end_ms)?,
                status: CacheRecordStatus::Available,
                cache_path: Some(relative_to(&target, &plan.cache_root)?),
                missing_count: 0,
                failure_reason: None,
                response_hash: Some(hash_path(&target)?),
            })
        }
        Err(err) => Ok(CacheRecord {
            group: CacheGroup::Binance1sReference,
            name,
            source_url,
            symbol: Some(exchange_symbol.to_string()),
            start_ts_ns: ms_to_ns(day_start_ms)?,
            end_ts_ns: ms_to_ns(day_end_ms)?,
            status: CacheRecordStatus::Failed,
            cache_path: None,
            missing_count: 1,
            failure_reason: Some(err.to_string()),
            response_hash: None,
        }),
    }
}

fn prepare_polymarket_settlement(
    plan: &PipelinePlan,
    http: &dyn CachePreparationHttp,
    market_rows: &[MarketDimRow],
    options: CachePreparationOptions,
) -> Result<Vec<CacheRecord>> {
    let group_dir = plan
        .cache_root
        .join(CacheGroup::PolymarketSettlement.as_str());
    fs::create_dir_all(&group_dir).with_context(|| format!("create {}", group_dir.display()))?;

    let expected = expected_settlement_assets(market_rows);
    let start_ts_ns = market_rows
        .iter()
        .map(|row| row.first_seen_ts_ns)
        .min()
        .unwrap_or_default();
    let end_ts_ns = market_rows
        .iter()
        .map(|row| row.last_seen_ts_ns)
        .max()
        .unwrap_or_default();

    prepare_settlement_records_concurrent(plan, http, &expected, start_ts_ns, end_ts_ns, options)
}

fn prepare_settlement_records_concurrent(
    plan: &PipelinePlan,
    http: &dyn CachePreparationHttp,
    expected: &BTreeMap<String, Vec<ExpectedSettlementAsset>>,
    start_ts_ns: i64,
    end_ts_ns: i64,
    options: CachePreparationOptions,
) -> Result<Vec<CacheRecord>> {
    let target = plan
        .cache_root
        .join(CacheGroup::PolymarketSettlement.as_str())
        .join("settlement_cache.jsonl");
    let mut cached = read_settlement_cache(&target)?;

    let tasks = expected
        .iter()
        .filter(|(condition_id, assets)| {
            settlement_needs_refresh(&cached, condition_id, assets, options)
        })
        .map(|(condition_id, assets)| (condition_id.clone(), assets.clone()))
        .collect::<Vec<_>>();

    if !tasks.is_empty() {
        let worker_count = settlement_worker_count(tasks.len());
        let progress_every = settlement_progress_every();
        eprintln!(
            "preparing Polymarket settlement cache for {} missing/stale condition(s) with {} worker(s)",
            tasks.len(),
            worker_count
        );

        let next = AtomicUsize::new(0);
        let completed = AtomicUsize::new(0);
        let results = Mutex::new(vec![None::<Vec<RawPolymarketSettlement>>; tasks.len()]);

        thread::scope(|scope| {
            for _ in 0..worker_count {
                scope.spawn(|| loop {
                    let idx = next.fetch_add(1, Ordering::Relaxed);
                    if idx >= tasks.len() {
                        break;
                    }
                    let (condition_id, assets) = &tasks[idx];
                    let rows = download_settlement_rows(http, condition_id, assets)
                        .expect("download_settlement_rows converts HTTP failures into rows");
                    results.lock().expect("settlement results mutex")[idx] = Some(rows);

                    let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                    if done == tasks.len() || done % progress_every == 0 {
                        eprintln!(
                            "prepared Polymarket settlement cache for {done}/{} condition(s)",
                            tasks.len()
                        );
                    }
                });
            }
        });

        for (idx, maybe_rows) in results
            .into_inner()
            .expect("settlement results mutex")
            .into_iter()
            .enumerate()
        {
            let task_rows =
                maybe_rows.ok_or_else(|| anyhow!("settlement task {idx} did not produce rows"))?;
            let condition_id = tasks[idx].0.clone();
            cached.insert(condition_id, task_rows);
        }
        write_settlement_cache(&target, &cached)?;
    } else {
        eprintln!(
            "reused Polymarket settlement cache for {} condition(s)",
            expected.len()
        );
    }

    let current_rows = expected
        .keys()
        .filter_map(|condition_id| cached.get(condition_id))
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    let failed_or_unsettled = current_rows
        .iter()
        .filter(|row| row.status != SettlementStatus::Settled)
        .count() as u64;
    let failure_reason = settlement_failure_summary(&current_rows);
    let status = if current_rows.is_empty() {
        CacheRecordStatus::Missing
    } else {
        CacheRecordStatus::Available
    };
    Ok(vec![CacheRecord {
        group: CacheGroup::PolymarketSettlement,
        name: "polymarket-settlement-shared".to_string(),
        source_url: format!(
            "polymarket_clob_markets://conditions?count={}",
            expected.len()
        ),
        symbol: None,
        start_ts_ns,
        end_ts_ns,
        status,
        cache_path: (status == CacheRecordStatus::Available)
            .then(|| relative_to(&target, &plan.cache_root))
            .transpose()?,
        missing_count: failed_or_unsettled,
        failure_reason,
        response_hash: (status == CacheRecordStatus::Available)
            .then(|| hash_path(&target))
            .transpose()?,
    }])
}

fn read_settlement_cache(path: &Path) -> Result<BTreeMap<String, Vec<RawPolymarketSettlement>>> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let mut out = BTreeMap::<String, Vec<RawPolymarketSettlement>>::new();
    for row in read_cached_records::<RawPolymarketSettlement>(path)? {
        out.entry(row.condition_id.clone()).or_default().push(row);
    }
    for rows in out.values_mut() {
        rows.sort_by(|a, b| (&a.asset_id, &a.outcome).cmp(&(&b.asset_id, &b.outcome)));
    }
    Ok(out)
}

fn write_settlement_cache(
    path: &Path,
    rows_by_condition: &BTreeMap<String, Vec<RawPolymarketSettlement>>,
) -> Result<()> {
    let mut rows = rows_by_condition
        .values()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        (&a.condition_id, &a.asset_id, &a.outcome).cmp(&(&b.condition_id, &b.asset_id, &b.outcome))
    });
    let bytes = jsonl_bytes(&rows)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    write_jsonl_atomic(path, &bytes)
}

fn settlement_needs_refresh(
    cached: &BTreeMap<String, Vec<RawPolymarketSettlement>>,
    condition_id: &str,
    assets: &[ExpectedSettlementAsset],
    options: CachePreparationOptions,
) -> bool {
    if options.refresh_settlement {
        return true;
    }
    let Some(rows) = cached.get(condition_id) else {
        return true;
    };
    if assets.iter().any(|asset| {
        !rows
            .iter()
            .any(|row| row.asset_id == asset.asset_id && row.outcome == asset.outcome)
    }) {
        return true;
    }
    if rows.iter().any(|row| {
        matches!(
            row.status,
            SettlementStatus::Failed | SettlementStatus::Unknown
        )
    }) {
        return true;
    }
    options.refresh_unsettled
        && rows
            .iter()
            .any(|row| row.status != SettlementStatus::Settled)
}

fn settlement_failure_summary(rows: &[RawPolymarketSettlement]) -> Option<String> {
    let mut counts = BTreeMap::<&'static str, u64>::new();
    let mut reasons = BTreeSet::<String>::new();
    for row in rows {
        if row.status == SettlementStatus::Settled {
            continue;
        }
        let status = match row.status {
            SettlementStatus::Settled => "settled",
            SettlementStatus::Unsettled => "unsettled",
            SettlementStatus::Unknown => "unknown",
            SettlementStatus::Failed => "failed",
        };
        *counts.entry(status).or_default() += 1;
        if let Some(reason) = &row.failure_reason {
            reasons.insert(reason.clone());
        }
    }
    if counts.is_empty() {
        return None;
    }

    let status_summary = counts
        .into_iter()
        .map(|(status, count)| format!("{status}={count}"))
        .collect::<Vec<_>>()
        .join(",");
    let reason_summary = reasons.into_iter().take(3).collect::<Vec<_>>().join(" | ");
    if reason_summary.is_empty() {
        Some(format!("settlement incomplete: {status_summary}"))
    } else {
        Some(format!(
            "settlement incomplete: {status_summary}; examples: {reason_summary}"
        ))
    }
}

fn settlement_worker_count(task_count: usize) -> usize {
    std::env::var("PM5M_SETTLEMENT_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(8)
        .min(task_count.max(1))
}

fn settlement_progress_every() -> usize {
    std::env::var("PM5M_SETTLEMENT_PROGRESS_EVERY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(100)
}

fn download_binance_1s(
    http: &dyn CachePreparationHttp,
    exchange_symbol: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<RawBinanceKline1s>> {
    if end_ms <= start_ms {
        bail!("Binance range end_ms must be greater than start_ms");
    }

    let mut rows = BTreeMap::new();
    let mut cursor = start_ms;
    while cursor < end_ms {
        let params = format!(
            "symbol={}&interval=1s&startTime={cursor}&endTime={}&limit=1000",
            percent_encode(exchange_symbol),
            end_ms - 1
        );
        let data = fetch_binance_klines(http, &params)?;
        let items = data
            .as_array()
            .ok_or_else(|| anyhow!("Binance kline response must be an array"))?;
        if items.is_empty() {
            break;
        }

        for item in items {
            let row = parse_binance_kline(exchange_symbol, item)?;
            let open_ms = row.bar_open_ts_ns / 1_000_000;
            if start_ms <= open_ms && open_ms < end_ms {
                rows.insert(open_ms, row);
            }
        }

        let last_open = items
            .last()
            .ok_or_else(|| anyhow!("Binance kline page unexpectedly empty"))
            .and_then(kline_open_ms)?;
        if last_open < cursor {
            break;
        }
        cursor = last_open + 1_000;
        if items.len() < 1_000 {
            break;
        }
    }

    Ok(rows.into_values().collect())
}

fn fetch_binance_klines(http: &dyn CachePreparationHttp, params: &str) -> Result<Value> {
    let mut failures = Vec::new();
    for base in [BINANCE_DATA_API_BASE, BINANCE_API_BASE] {
        let url = format!("{base}/api/v3/klines?{params}");
        match get_json(http, &url) {
            Ok(value) => return Ok(value),
            Err(err) => failures.push(format!("{base}: {err}")),
        }
    }
    bail!("Binance kline download failed: {}", failures.join("; "))
}

fn parse_binance_kline(exchange_symbol: &str, item: &Value) -> Result<RawBinanceKline1s> {
    let fields = item
        .as_array()
        .ok_or_else(|| anyhow!("Binance kline item must be an array"))?;
    if fields.len() < 8 {
        bail!("Binance kline item must contain at least 8 fields");
    }
    let open_ms = value_i64(&fields[0], "open_time_ms")?;
    Ok(RawBinanceKline1s {
        symbol: Some(exchange_symbol.to_string()),
        bar_open_ts_ns: ms_to_ns(open_ms)?,
        bar_close_ts_ns: ms_to_ns(open_ms + 1_000)?,
        open: value_f64(&fields[1], "open")?,
        high: value_f64(&fields[2], "high")?,
        low: value_f64(&fields[3], "low")?,
        close: value_f64(&fields[4], "close")?,
        volume: value_f64(&fields[5], "volume")?,
    })
}

fn kline_open_ms(item: &Value) -> Result<i64> {
    let fields = item
        .as_array()
        .ok_or_else(|| anyhow!("Binance kline item must be an array"))?;
    let open = fields
        .first()
        .ok_or_else(|| anyhow!("Binance kline item missing open time"))?;
    value_i64(open, "open_time_ms")
}

fn download_settlement_rows(
    http: &dyn CachePreparationHttp,
    condition_id: &str,
    assets: &[ExpectedSettlementAsset],
) -> Result<Vec<RawPolymarketSettlement>> {
    let url = format!(
        "{}/markets/{}",
        POLYMARKET_CLOB_BASE,
        percent_encode(condition_id)
    );
    match get_json(http, &url) {
        Ok(payload) => Ok(parse_settlement_payload(&payload, assets)),
        Err(err) => {
            let failure_reason = format!("download_failed: {err:#}");
            Ok(assets
                .iter()
                .map(|asset| RawPolymarketSettlement {
                    condition_id: asset.condition_id.clone(),
                    asset_id: asset.asset_id.clone(),
                    outcome: asset.outcome.clone(),
                    status: SettlementStatus::Failed,
                    winner: None,
                    settled_ts_ns: None,
                    failure_reason: Some(failure_reason.clone()),
                })
                .collect())
        }
    }
}

fn parse_settlement_payload(
    payload: &Value,
    assets: &[ExpectedSettlementAsset],
) -> Vec<RawPolymarketSettlement> {
    let closed = payload
        .get("closed")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let tokens = payload
        .get("tokens")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    assets
        .iter()
        .map(|asset| {
            let token = tokens
                .iter()
                .find(|token| settlement_token_matches(token, asset));
            let (status, winner, failure_reason) = match token {
                None => (
                    SettlementStatus::Failed,
                    None,
                    Some("token_not_found_in_clob_market_payload".to_string()),
                ),
                Some(token) if closed => match token_winner(token) {
                    Some(value) => (SettlementStatus::Settled, Some(value), None),
                    None => (
                        SettlementStatus::Unknown,
                        None,
                        Some("closed_token_missing_winner_or_binary_price".to_string()),
                    ),
                },
                Some(_) => (
                    SettlementStatus::Unsettled,
                    None,
                    Some("market_not_closed".to_string()),
                ),
            };
            RawPolymarketSettlement {
                condition_id: asset.condition_id.clone(),
                asset_id: asset.asset_id.clone(),
                outcome: asset.outcome.clone(),
                status,
                winner,
                settled_ts_ns: None,
                failure_reason,
            }
        })
        .collect()
}

fn settlement_token_matches(token: &Value, asset: &ExpectedSettlementAsset) -> bool {
    let token_id = string_value(token, &["token_id", "tokenId", "asset_id", "assetId"]);
    if token_id.as_deref() == Some(asset.asset_id.as_str()) {
        return true;
    }
    string_value(token, &["outcome"])
        .map(|outcome| normalize_outcome(&outcome) == normalize_outcome(&asset.outcome))
        .unwrap_or(false)
}

fn token_winner(token: &Value) -> Option<bool> {
    if let Some(value) = token.get("winner").and_then(Value::as_bool) {
        return Some(value);
    }
    let price = token.get("price")?;
    if decimal_is(price, "1") {
        Some(true)
    } else if decimal_is(price, "0") {
        Some(false)
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy)]
struct ReferenceRange {
    start_ms: i64,
    end_ms: i64,
}

fn infer_reference_ranges(
    market_rows: &[MarketDimRow],
) -> Result<BTreeMap<String, ReferenceRange>> {
    let mut raw = BTreeMap::<String, (i64, i64)>::new();
    for row in market_rows {
        let asset = normalize_reference_asset(&row.symbol)
            .ok_or_else(|| anyhow!("unsupported reference symbol '{}'", row.symbol))?;
        raw.entry(asset)
            .and_modify(|(min_ns, max_ns)| {
                *min_ns = (*min_ns).min(row.window_start_ts_ns);
                *max_ns = (*max_ns).max(row.window_end_ts_ns);
            })
            .or_insert((row.window_start_ts_ns, row.window_end_ts_ns));
    }

    raw.into_iter()
        .map(|(asset, (min_ns, max_ns))| {
            let min_ms = ns_to_ms_floor(min_ns);
            let max_ms = ns_to_ms_floor(max_ns);
            let start_ms = ((min_ms / 1_000) * 1_000 - RANGE_PAD_MS).max(0);
            let end_ms = ((max_ms / 1_000) + 1) * 1_000 + RANGE_PAD_MS;
            Ok((asset, ReferenceRange { start_ms, end_ms }))
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ExpectedSettlementAsset {
    condition_id: String,
    asset_id: String,
    outcome: String,
}

fn expected_settlement_assets(
    market_rows: &[MarketDimRow],
) -> BTreeMap<String, Vec<ExpectedSettlementAsset>> {
    let mut seen = BTreeSet::new();
    let mut out = BTreeMap::<String, Vec<ExpectedSettlementAsset>>::new();
    for row in market_rows {
        for (asset_id, outcome) in [
            (row.yes_asset_id.as_str(), "YES"),
            (row.no_asset_id.as_str(), "NO"),
        ] {
            let asset = ExpectedSettlementAsset {
                condition_id: row.condition_id.clone(),
                asset_id: asset_id.to_string(),
                outcome: outcome.to_string(),
            };
            if seen.insert(asset.clone()) {
                out.entry(asset.condition_id.clone())
                    .or_default()
                    .push(asset);
            }
        }
    }
    for assets in out.values_mut() {
        assets.sort();
    }
    out
}

fn write_cache_manifest_atomic(plan: &PipelinePlan, manifest: &CacheManifest) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(manifest).context("serialize cache manifest")?;
    atomic_write_verified(&cache_manifest_path(plan), &bytes, |tmp| {
        let file = fs::File::open(tmp).with_context(|| format!("open {}", tmp.display()))?;
        let parsed: CacheManifest =
            serde_json::from_reader(file).with_context(|| format!("parse {}", tmp.display()))?;
        if parsed.dataset_format != CACHE_MANIFEST_FORMAT {
            bail!("cache manifest dataset format mismatch");
        }
        Ok(())
    })
}

fn write_jsonl_atomic(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    atomic_write_verified(path, bytes, |tmp| {
        let text = fs::read_to_string(tmp).with_context(|| format!("read {}", tmp.display()))?;
        for (idx, line) in text.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            serde_json::from_str::<Value>(line)
                .with_context(|| format!("parse JSONL line {} in {}", idx + 1, tmp.display()))?;
        }
        Ok(())
    })
}

fn jsonl_bytes<T: Serialize>(rows: &[T]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for row in rows {
        serde_json::to_writer(&mut bytes, row).context("serialize cache row")?;
        bytes.push(b'\n');
    }
    Ok(bytes)
}

fn get_json(http: &dyn CachePreparationHttp, url: &str) -> Result<Value> {
    let bytes = http.get(url)?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse JSON response from {url}"))
}

fn normalize_reference_asset(symbol: &str) -> Option<String> {
    let upper = symbol.to_ascii_uppercase();
    let asset = upper
        .split_once('-')
        .map(|(asset, _)| asset)
        .unwrap_or(upper.as_str())
        .trim();
    if asset.is_empty() || !asset.chars().all(|ch| ch.is_ascii_alphanumeric()) {
        None
    } else {
        Some(asset.to_string())
    }
}

fn normalize_outcome(value: &str) -> String {
    match value.trim().to_ascii_uppercase().as_str() {
        "YES" | "UP" => "YES".to_string(),
        "NO" | "DOWN" => "NO".to_string(),
        other => other.to_string(),
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

fn value_i64(value: &Value, field_name: &str) -> Result<i64> {
    if let Some(number) = value.as_i64() {
        return Ok(number);
    }
    value
        .as_str()
        .ok_or_else(|| anyhow!("{field_name} must be an integer"))?
        .parse::<i64>()
        .with_context(|| format!("parse {field_name}"))
}

fn value_f64(value: &Value, field_name: &str) -> Result<f64> {
    let number = if let Some(number) = value.as_f64() {
        number
    } else {
        value
            .as_str()
            .ok_or_else(|| anyhow!("{field_name} must be decimal"))?
            .parse::<f64>()
            .with_context(|| format!("parse {field_name}"))?
    };
    if !number.is_finite() {
        bail!("{field_name} must be finite");
    }
    Ok(number)
}

fn decimal_is(value: &Value, expected: &str) -> bool {
    let Ok(expected_number) = expected.parse::<f64>() else {
        return false;
    };
    if let Some(raw) = value.as_str() {
        return raw
            .parse::<f64>()
            .map(|number| number == expected_number)
            .unwrap_or(false);
    }
    value
        .as_f64()
        .map(|number| number == expected_number)
        .unwrap_or(false)
}

fn ns_to_ms_floor(ns: i64) -> i64 {
    ns.div_euclid(1_000_000)
}

fn ms_to_ns(ms: i64) -> Result<i64> {
    ms.checked_mul(1_000_000)
        .ok_or_else(|| anyhow!("timestamp overflow converting ms to ns"))
}

fn utc_day_starts(start_ms: i64, end_ms: i64) -> Vec<i64> {
    if end_ms <= start_ms {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut day_start = start_ms.div_euclid(DAY_MS) * DAY_MS;
    while day_start < end_ms {
        out.push(day_start);
        day_start += DAY_MS;
    }
    out
}

fn utc_day_string(day_start_ms: i64) -> Result<String> {
    let dt = Utc
        .timestamp_millis_opt(day_start_ms)
        .single()
        .ok_or_else(|| anyhow!("invalid UTC day start millis {day_start_ms}"))?;
    Ok(dt.format("%Y-%m-%d").to_string())
}

fn percent_encode(value: &str) -> String {
    let mut out = String::new();
    for byte in value.as_bytes() {
        match *byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}
