use anyhow::{anyhow, bail, Context, Result};
use crc32fast::Hasher as Crc32;
use market_data_etl_core::{now_unix_ns, read_parquet_table, sha256_bytes, sha256_file};
use reqwest::header::{
    HeaderMap, HeaderValue, ACCEPT, ACCEPT_LANGUAGE, CACHE_CONTROL, RETRY_AFTER,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

pub const HFTREF1_FORMAT: &str = "pm5m_reference_cache.hftref1.v1";
pub const HFTSETTLE1_FORMAT: &str = "pm5m_settlement_cache.hftsettle1.v1";
pub const HFTREF1_CATALOG: &str = "catalog.hftref1.json";
pub const HFTSETTLE1_CATALOG: &str = "catalog.hftsettle1.json";

const HFTREF1_MAGIC: &[u8; 7] = b"HFTREF1";
const HFTSETTLE1_MAGIC: &[u8; 10] = b"HFTSETTLE1";
const REF_DATA_FILE: &str = "reference.hfr1";
const SETTLE_DATA_FILE: &str = "settlement.hfs1";
const HFTREF1_ROW_BYTES: usize = 4 + 8 + 8 + 8 + 4;
const POLYMARKET_CLOB_BASE: &str = "https://clob.polymarket.com";
const BINANCE_DATA_API_BASE: &str = "https://data-api.binance.vision";
const BINANCE_API_BASE: &str = "https://api.binance.com";
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const CLOB_SETTLEMENT_WORKER_CAP: usize = 16;
const CLOB_SETTLEMENT_MAX_ATTEMPTS: usize = 8;
const BINANCE_REFERENCE_MAX_ATTEMPTS: usize = 5;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildReferenceCacheOptions {
    pub input_table: PathBuf,
    pub cache_root: PathBuf,
    pub overwrite: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildReferenceCacheFromBinanceOptions {
    pub cache_root: PathBuf,
    pub overwrite: bool,
    pub symbols: Vec<String>,
    pub start_ts_ns: i64,
    pub end_ts_ns: i64,
    pub reference_latency_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildSettlementCacheOptions {
    pub input_table: PathBuf,
    pub cache_root: PathBuf,
    pub overwrite: bool,
    pub allow_unsettled: bool,
    pub book_cache_root: Option<PathBuf>,
    pub filter_to_book_cache: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildSettlementCacheFromClobOptions {
    pub input_table: Option<PathBuf>,
    pub cache_root: PathBuf,
    pub overwrite: bool,
    pub book_cache_root: PathBuf,
    pub refresh_workers: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildSettlementCacheFromConditionMetadataOptions {
    pub condition_metadata_root: PathBuf,
    pub cache_root: PathBuf,
    pub overwrite: bool,
    pub refresh_workers: usize,
    pub allow_unresolved: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactCacheCatalog {
    pub schema_version: u32,
    pub dataset_format: String,
    pub cache_root: PathBuf,
    pub data_path: String,
    pub generated_ts_ns: i64,
    pub row_count: usize,
    pub data_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SettlementCacheFromClobReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub cache_root: PathBuf,
    pub data_path: String,
    pub generated_ts_ns: i64,
    pub row_count: usize,
    pub condition_count: usize,
    pub reused_input_condition_count: usize,
    pub refreshed_clob_condition_count: usize,
    pub refresh_worker_count: usize,
    pub data_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReferenceCacheRow {
    pub symbol: String,
    pub ts_ns: i64,
    pub close_micros: i64,
    pub ingest_seq: u64,
    pub row_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SettlementCacheRow {
    pub condition_id: String,
    pub yes_asset_id: String,
    pub no_asset_id: String,
    pub winner_asset_id: String,
    pub winner_outcome: String,
    pub finalized_ts_ns: Option<i64>,
    pub row_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReferenceInputRow {
    symbol: String,
    synthetic_local_recv_ts_ns: i64,
    ingest_seq: u64,
    close: f64,
    row_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SettlementInputRow {
    condition_id: String,
    asset_id: String,
    outcome: String,
    status: String,
    winner: Option<bool>,
    settled_ts_ns: Option<i64>,
    row_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RefHeader {
    schema_version: u32,
    dataset_format: String,
    row_count: usize,
    symbols: Vec<String>,
    row_hashes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SettleHeader {
    schema_version: u32,
    dataset_format: String,
    row_count: usize,
    conditions: Vec<String>,
    assets: Vec<String>,
    row_hashes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConditionAssetMap {
    yes_asset_id: String,
    no_asset_id: String,
}

struct FastCompactPayload<T> {
    header: T,
    bytes: Vec<u8>,
    row_start: usize,
    row_end: usize,
}

#[derive(Default)]
struct Dict {
    values: Vec<String>,
    by_value: BTreeMap<String, u32>,
}

pub fn build_reference_cache(options: &BuildReferenceCacheOptions) -> Result<CompactCacheCatalog> {
    prepare_root(&options.cache_root, options.overwrite)?;
    let mut rows = read_parquet_table::<ReferenceInputRow>(&options.input_table)?
        .into_iter()
        .map(|row| ReferenceCacheRow {
            symbol: canonical_symbol(&row.symbol),
            ts_ns: row.synthetic_local_recv_ts_ns,
            close_micros: (row.close * 1_000_000.0).round() as i64,
            ingest_seq: row.ingest_seq,
            row_hash: row.row_hash,
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        (a.symbol.as_str(), a.ts_ns, a.ingest_seq).cmp(&(b.symbol.as_str(), b.ts_ns, b.ingest_seq))
    });
    if rows.is_empty() {
        bail!("reference input table is empty");
    }
    let data_path = options.cache_root.join(REF_DATA_FILE);
    write_reference_file(&data_path, &rows)?;
    write_catalog(
        &options.cache_root,
        HFTREF1_CATALOG,
        HFTREF1_FORMAT,
        REF_DATA_FILE,
        rows.len(),
    )
}

pub fn build_reference_cache_from_binance(
    options: &BuildReferenceCacheFromBinanceOptions,
) -> Result<CompactCacheCatalog> {
    if options.end_ts_ns <= options.start_ts_ns {
        bail!("reference end_ts_ns must be greater than start_ts_ns");
    }
    if options.symbols.is_empty() {
        bail!("at least one Binance reference symbol is required");
    }
    prepare_root(&options.cache_root, options.overwrite)?;
    let client = reference_http_client()?;
    let mut rows = Vec::new();
    for symbol in &options.symbols {
        rows.extend(fetch_binance_reference_rows(
            &client,
            symbol,
            options.start_ts_ns,
            options.end_ts_ns,
            options.reference_latency_ms,
        )?);
    }
    rows.sort_by(|a, b| {
        (a.symbol.as_str(), a.ts_ns, a.ingest_seq).cmp(&(b.symbol.as_str(), b.ts_ns, b.ingest_seq))
    });
    for (idx, row) in rows.iter_mut().enumerate() {
        row.ingest_seq = idx as u64;
    }
    if rows.is_empty() {
        bail!("Binance reference download produced no rows");
    }
    let data_path = options.cache_root.join(REF_DATA_FILE);
    write_reference_file(&data_path, &rows)?;
    write_catalog(
        &options.cache_root,
        HFTREF1_CATALOG,
        HFTREF1_FORMAT,
        REF_DATA_FILE,
        rows.len(),
    )
}

pub fn build_settlement_cache(
    options: &BuildSettlementCacheOptions,
) -> Result<CompactCacheCatalog> {
    prepare_root(&options.cache_root, options.overwrite)?;
    let input = read_parquet_table::<SettlementInputRow>(&options.input_table)?;
    let asset_map = if let Some(book_cache_root) = &options.book_cache_root {
        read_condition_asset_map(book_cache_root)?
    } else {
        BTreeMap::new()
    };
    let mut groups = BTreeMap::<String, Vec<SettlementInputRow>>::new();
    for row in input {
        groups
            .entry(row.condition_id.clone())
            .or_default()
            .push(row);
    }
    let mut rows = Vec::new();
    for (condition_id, group) in groups {
        if options.filter_to_book_cache && !asset_map.contains_key(&condition_id) {
            continue;
        }
        if let Some(row) = condition_settlement(
            condition_id.clone(),
            group,
            options.allow_unsettled,
            asset_map.get(&condition_id),
        )? {
            rows.push(row);
        }
    }
    rows.sort_by(|a, b| a.condition_id.cmp(&b.condition_id));
    if rows.is_empty() {
        bail!("settlement input table produced no settled condition rows");
    }
    let data_path = options.cache_root.join(SETTLE_DATA_FILE);
    write_settlement_file(&data_path, &rows)?;
    write_catalog(
        &options.cache_root,
        HFTSETTLE1_CATALOG,
        HFTSETTLE1_FORMAT,
        SETTLE_DATA_FILE,
        rows.len(),
    )
}

pub fn build_settlement_cache_from_clob(
    options: &BuildSettlementCacheFromClobOptions,
) -> Result<SettlementCacheFromClobReport> {
    prepare_root(&options.cache_root, options.overwrite)?;
    let asset_map = read_condition_asset_map(&options.book_cache_root)?;
    if asset_map.is_empty() {
        bail!("HFTBOOK2 cache produced no conditions for settlement refresh");
    }

    let mut groups = BTreeMap::<String, Vec<SettlementInputRow>>::new();
    if let Some(input_table) = &options.input_table {
        for row in read_parquet_table::<SettlementInputRow>(input_table)? {
            groups
                .entry(row.condition_id.clone())
                .or_default()
                .push(row);
        }
    }

    let mut rows = Vec::new();
    let mut refresh_tasks = Vec::<(String, ConditionAssetMap)>::new();
    let mut reused_input_condition_count = 0usize;
    for (condition_id, assets) in &asset_map {
        match groups.get(condition_id) {
            Some(group) => {
                match condition_settlement(condition_id.clone(), group.clone(), false, Some(assets))
                {
                    Ok(Some(row)) => {
                        rows.push(row);
                        reused_input_condition_count += 1;
                    }
                    Ok(None) | Err(_) => refresh_tasks.push((condition_id.clone(), assets.clone())),
                }
            }
            None => refresh_tasks.push((condition_id.clone(), assets.clone())),
        }
    }

    let refresh_worker_count = refresh_worker_count(options.refresh_workers, refresh_tasks.len());
    if !refresh_tasks.is_empty() {
        let client = settlement_http_client()?;
        let next = AtomicUsize::new(0);
        let completed = AtomicUsize::new(0);
        let results = Mutex::new(vec![
            None::<Result<Vec<SettlementInputRow>, String>>;
            refresh_tasks.len()
        ]);
        let progress_every = settlement_progress_every(refresh_tasks.len());
        eprintln!(
            "refreshing CLOB settlement for {} condition(s) with {} worker(s)",
            refresh_tasks.len(),
            refresh_worker_count
        );

        thread::scope(|scope| {
            for _ in 0..refresh_worker_count {
                scope.spawn(|| loop {
                    let idx = next.fetch_add(1, Ordering::Relaxed);
                    if idx >= refresh_tasks.len() {
                        break;
                    }
                    let (condition_id, assets) = &refresh_tasks[idx];
                    let result = fetch_clob_settlement_rows(&client, condition_id, assets)
                        .map_err(|err| format!("{err:#}"));
                    results.lock().expect("settlement refresh results")[idx] = Some(result);

                    let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                    if done == refresh_tasks.len() || done % progress_every == 0 {
                        eprintln!(
                            "refreshed CLOB settlement for {done}/{} condition(s)",
                            refresh_tasks.len()
                        );
                    }
                });
            }
        });

        let mut errors = Vec::new();
        for (idx, result) in results
            .into_inner()
            .expect("settlement refresh results")
            .into_iter()
            .enumerate()
        {
            let (condition_id, assets) = &refresh_tasks[idx];
            match result.ok_or_else(|| anyhow!("condition {condition_id} did not run"))? {
                Ok(group) => {
                    match condition_settlement(condition_id.clone(), group, false, Some(assets)) {
                        Ok(Some(row)) => rows.push(row),
                        Ok(None) => errors.push(format!("condition {condition_id} unresolved")),
                        Err(err) => errors.push(format!("condition {condition_id}: {err:#}")),
                    }
                }
                Err(err) => errors.push(format!("condition {condition_id}: {err}")),
            }
        }
        if !errors.is_empty() {
            errors.sort();
            bail!(
                "failed to refresh CLOB settlement for {} condition(s): {}",
                errors.len(),
                errors.into_iter().take(5).collect::<Vec<_>>().join(" | ")
            );
        }
    }

    rows.sort_by(|a, b| a.condition_id.cmp(&b.condition_id));
    if rows.len() != asset_map.len() {
        bail!(
            "HFTSETTLE1 condition count mismatch: rows={} expected={}",
            rows.len(),
            asset_map.len()
        );
    }
    let data_path = options.cache_root.join(SETTLE_DATA_FILE);
    write_settlement_file(&data_path, &rows)?;
    let catalog = write_catalog(
        &options.cache_root,
        HFTSETTLE1_CATALOG,
        HFTSETTLE1_FORMAT,
        SETTLE_DATA_FILE,
        rows.len(),
    )?;
    Ok(SettlementCacheFromClobReport {
        schema_version: catalog.schema_version,
        dataset_format: catalog.dataset_format,
        cache_root: catalog.cache_root,
        data_path: catalog.data_path,
        generated_ts_ns: catalog.generated_ts_ns,
        row_count: catalog.row_count,
        condition_count: asset_map.len(),
        reused_input_condition_count,
        refreshed_clob_condition_count: refresh_tasks.len(),
        refresh_worker_count,
        data_sha256: catalog.data_sha256,
    })
}

pub fn build_settlement_cache_from_condition_metadata(
    options: &BuildSettlementCacheFromConditionMetadataOptions,
) -> Result<SettlementCacheFromClobReport> {
    prepare_root(&options.cache_root, options.overwrite)?;
    let asset_map = read_condition_asset_map_from_metadata_root(&options.condition_metadata_root)?;
    if asset_map.is_empty() {
        bail!(
            "condition metadata produced no conditions: {}",
            options.condition_metadata_root.display()
        );
    }

    let refresh_worker_count = refresh_worker_count(options.refresh_workers, asset_map.len());
    let refresh_tasks = asset_map
        .iter()
        .map(|(condition_id, assets)| (condition_id.clone(), assets.clone()))
        .collect::<Vec<_>>();
    let client = settlement_http_client()?;
    let next = AtomicUsize::new(0);
    let completed = AtomicUsize::new(0);
    let results = Mutex::new(vec![
        None::<Result<Vec<SettlementInputRow>, String>>;
        refresh_tasks.len()
    ]);
    let progress_every = settlement_progress_every(refresh_tasks.len());
    eprintln!(
        "refreshing CLOB settlement for {} metadata condition(s) with {} worker(s)",
        refresh_tasks.len(),
        refresh_worker_count
    );

    thread::scope(|scope| {
        for _ in 0..refresh_worker_count {
            scope.spawn(|| loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= refresh_tasks.len() {
                    break;
                }
                let (condition_id, assets) = &refresh_tasks[idx];
                let result = fetch_clob_settlement_rows(&client, condition_id, assets)
                    .map_err(|err| format!("{err:#}"));
                results.lock().expect("settlement refresh results")[idx] = Some(result);

                let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                if done == refresh_tasks.len() || done % progress_every == 0 {
                    eprintln!(
                        "refreshed CLOB settlement for {done}/{} metadata condition(s)",
                        refresh_tasks.len()
                    );
                }
            });
        }
    });

    let mut rows = Vec::new();
    let mut errors = Vec::new();
    for (idx, result) in results
        .into_inner()
        .expect("settlement refresh results")
        .into_iter()
        .enumerate()
    {
        let (condition_id, assets) = &refresh_tasks[idx];
        match result.ok_or_else(|| anyhow!("condition {condition_id} did not run"))? {
            Ok(group) => {
                match condition_settlement(condition_id.clone(), group, false, Some(assets)) {
                    Ok(Some(row)) => rows.push(row),
                    Ok(None) => errors.push(format!("condition {condition_id} unresolved")),
                    Err(err) => errors.push(format!("condition {condition_id}: {err:#}")),
                }
            }
            Err(err) => errors.push(format!("condition {condition_id}: {err}")),
        }
    }

    errors.sort();
    if !errors.is_empty() && !options.allow_unresolved {
        bail!(
            "failed to refresh CLOB settlement for {} condition(s): {}",
            errors.len(),
            errors.into_iter().take(5).collect::<Vec<_>>().join(" | ")
        );
    }
    if !errors.is_empty() {
        eprintln!(
            "skipped unresolved CLOB settlement for {} condition(s): {}",
            errors.len(),
            errors
                .iter()
                .take(5)
                .cloned()
                .collect::<Vec<_>>()
                .join(" | ")
        );
    }

    rows.sort_by(|a, b| a.condition_id.cmp(&b.condition_id));
    if rows.is_empty() {
        bail!("condition metadata CLOB settlement produced no settled rows");
    }
    let data_path = options.cache_root.join(SETTLE_DATA_FILE);
    write_settlement_file(&data_path, &rows)?;
    let catalog = write_catalog(
        &options.cache_root,
        HFTSETTLE1_CATALOG,
        HFTSETTLE1_FORMAT,
        SETTLE_DATA_FILE,
        rows.len(),
    )?;
    Ok(SettlementCacheFromClobReport {
        schema_version: catalog.schema_version,
        dataset_format: catalog.dataset_format,
        cache_root: catalog.cache_root,
        data_path: catalog.data_path,
        generated_ts_ns: catalog.generated_ts_ns,
        row_count: catalog.row_count,
        condition_count: asset_map.len(),
        reused_input_condition_count: 0,
        refreshed_clob_condition_count: rows.len(),
        refresh_worker_count,
        data_sha256: catalog.data_sha256,
    })
}

pub fn read_reference_cache(cache_root: &Path) -> Result<Vec<ReferenceCacheRow>> {
    let catalog = read_catalog(cache_root, HFTREF1_CATALOG, HFTREF1_FORMAT)?;
    if sha256_file(&cache_root.join(&catalog.data_path))? != catalog.data_sha256 {
        bail!("HFTREF1 data hash mismatch");
    }
    read_reference_file(&cache_root.join(&catalog.data_path))
}

pub fn scan_reference_cache_fast<F>(
    cache_root: &Path,
    start_ts_ns: Option<i64>,
    end_ts_ns: Option<i64>,
    mut visit: F,
) -> Result<usize>
where
    F: FnMut(ReferenceCacheRow) -> Result<()>,
{
    let catalog = read_catalog(cache_root, HFTREF1_CATALOG, HFTREF1_FORMAT)?;
    let payload = read_compact_payload_fast::<RefHeader>(
        &cache_root.join(&catalog.data_path),
        HFTREF1_MAGIC,
    )?;
    if payload.header.row_count != catalog.row_count {
        bail!("HFTREF1 row count mismatch");
    }
    let row_bytes = &payload.bytes[payload.row_start..payload.row_end];
    let expected_row_len = payload
        .header
        .row_count
        .checked_mul(HFTREF1_ROW_BYTES)
        .context("HFTREF1 row byte length overflow")?;
    if row_bytes.len() != expected_row_len {
        bail!("HFTREF1 fixed row byte length mismatch");
    }

    let mut cursor = 0usize;
    let mut count = 0usize;
    while cursor < row_bytes.len() {
        let symbol_key = read_u32(row_bytes, &mut cursor)?;
        let ts_ns = read_i64(row_bytes, &mut cursor)?;
        let close_micros = read_i64(row_bytes, &mut cursor)?;
        let ingest_seq = read_u64(row_bytes, &mut cursor)?;
        let hash_key = read_u32(row_bytes, &mut cursor)?;
        if !ts_in_window(ts_ns, start_ts_ns, end_ts_ns) {
            continue;
        }
        visit(ReferenceCacheRow {
            symbol: dict_value(&payload.header.symbols, symbol_key, "symbol")?,
            ts_ns,
            close_micros,
            ingest_seq,
            row_hash: dict_value(&payload.header.row_hashes, hash_key, "row_hash")?,
        })?;
        count += 1;
    }
    Ok(count)
}

pub fn read_settlement_cache(cache_root: &Path) -> Result<Vec<SettlementCacheRow>> {
    let catalog = read_catalog(cache_root, HFTSETTLE1_CATALOG, HFTSETTLE1_FORMAT)?;
    if sha256_file(&cache_root.join(&catalog.data_path))? != catalog.data_sha256 {
        bail!("HFTSETTLE1 data hash mismatch");
    }
    read_settlement_file(&cache_root.join(&catalog.data_path))
}

pub fn validate_reference_cache(cache_root: &Path) -> Result<CompactCacheCatalog> {
    let catalog = read_catalog(cache_root, HFTREF1_CATALOG, HFTREF1_FORMAT)?;
    let rows = read_reference_cache(cache_root)?;
    if rows.len() != catalog.row_count {
        bail!("HFTREF1 row count mismatch");
    }
    Ok(catalog)
}

pub fn validate_settlement_cache(cache_root: &Path) -> Result<CompactCacheCatalog> {
    let catalog = read_catalog(cache_root, HFTSETTLE1_CATALOG, HFTSETTLE1_FORMAT)?;
    let rows = read_settlement_cache(cache_root)?;
    if rows.len() != catalog.row_count {
        bail!("HFTSETTLE1 row count mismatch");
    }
    Ok(catalog)
}

fn condition_settlement(
    condition_id: String,
    group: Vec<SettlementInputRow>,
    allow_unsettled: bool,
    asset_map: Option<&ConditionAssetMap>,
) -> Result<Option<SettlementCacheRow>> {
    let mut yes_asset = None::<String>;
    let mut no_asset = None::<String>;
    let mut winner = None::<&SettlementInputRow>;
    for row in &group {
        if row.outcome.eq_ignore_ascii_case("YES") {
            yes_asset = Some(row.asset_id.clone());
        } else if row.outcome.eq_ignore_ascii_case("NO") {
            no_asset = Some(row.asset_id.clone());
        }
        if row.status == "settled" && row.winner == Some(true) {
            if winner.is_some() {
                bail!("multiple settlement winners for condition {condition_id}");
            }
            winner = Some(row);
        }
    }
    let Some(winner) = winner else {
        if allow_unsettled {
            return Ok(None);
        }
        bail!("missing settled winner for condition {condition_id}");
    };
    let yes_asset_id = yes_asset
        .or_else(|| asset_map.map(|assets| assets.yes_asset_id.clone()))
        .ok_or_else(|| anyhow!("missing YES asset for {condition_id}"))?;
    let no_asset_id = no_asset
        .or_else(|| asset_map.map(|assets| assets.no_asset_id.clone()))
        .ok_or_else(|| anyhow!("missing NO asset for {condition_id}"))?;
    if winner.asset_id != yes_asset_id && winner.asset_id != no_asset_id {
        bail!("settlement winner asset is not in book metadata for {condition_id}");
    }
    Ok(Some(SettlementCacheRow {
        condition_id,
        yes_asset_id,
        no_asset_id,
        winner_asset_id: winner.asset_id.clone(),
        winner_outcome: winner.outcome.to_ascii_uppercase(),
        finalized_ts_ns: winner.settled_ts_ns,
        row_hash: winner.row_hash.clone(),
    }))
}

fn read_condition_asset_map(book_cache_root: &Path) -> Result<BTreeMap<String, ConditionAssetMap>> {
    if !book_cache_root
        .join(crate::types::BOOK_CACHE2_CATALOG)
        .exists()
    {
        bail!(
            "settlement asset-map fill requires HFTBOOK2 cache at {}",
            book_cache_root.display()
        );
    }
    let mut out = BTreeMap::<String, ConditionAssetMap>::new();
    crate::hftbook2::scan_book_cache2_rows_fast(
        book_cache_root,
        None,
        None,
        &crate::hftbook2::BookCache2ScanFilter::default(),
        |row| {
            if row.yes_asset_id == row.no_asset_id {
                bail!(
                    "HFTBOOK2 YES/NO asset ids are identical for {}",
                    row.condition_id
                );
            }
            let assets = ConditionAssetMap {
                yes_asset_id: row.yes_asset_id.clone(),
                no_asset_id: row.no_asset_id.clone(),
            };
            if let Some(existing) = out.get(&row.condition_id) {
                if existing != &assets {
                    bail!(
                        "inconsistent HFTBOOK2 YES/NO assets for condition {}",
                        row.condition_id
                    );
                }
            } else {
                out.insert(row.condition_id.clone(), assets);
            }
            Ok(())
        },
    )?;
    Ok(out)
}

fn read_condition_asset_map_from_metadata_root(
    metadata_root: &Path,
) -> Result<BTreeMap<String, ConditionAssetMap>> {
    #[derive(Debug, Deserialize)]
    struct ConditionMetadataFile {
        condition_id: String,
        yes_asset_id: String,
        no_asset_id: String,
    }

    let mut out = BTreeMap::<String, ConditionAssetMap>::new();
    for entry in fs::read_dir(metadata_root)
        .with_context(|| format!("read condition metadata root {}", metadata_root.display()))?
    {
        let entry = entry.with_context(|| {
            format!(
                "read condition metadata entry in {}",
                metadata_root.display()
            )
        })?;
        let path = entry.path();
        if !path.is_file() || path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let metadata: ConditionMetadataFile = serde_json::from_reader(
            fs::File::open(&path).with_context(|| format!("open {}", path.display()))?,
        )
        .with_context(|| format!("parse {}", path.display()))?;
        if metadata.condition_id.trim().is_empty() {
            bail!(
                "condition metadata {} has empty condition_id",
                path.display()
            );
        }
        if metadata.yes_asset_id == metadata.no_asset_id {
            bail!(
                "condition metadata {} has identical YES/NO assets",
                metadata.condition_id
            );
        }
        let assets = ConditionAssetMap {
            yes_asset_id: metadata.yes_asset_id,
            no_asset_id: metadata.no_asset_id,
        };
        if let Some(existing) = out.get(&metadata.condition_id) {
            if existing != &assets {
                bail!(
                    "inconsistent condition metadata assets for {}",
                    metadata.condition_id
                );
            }
        } else {
            out.insert(metadata.condition_id, assets);
        }
    }
    Ok(out)
}

fn reference_http_client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(20))
        .user_agent("pm5m-market-cache-reference-refresh/0.1")
        .build()
        .context("build reference refresh HTTP client")
}

fn fetch_binance_reference_rows(
    client: &reqwest::blocking::Client,
    symbol: &str,
    start_ts_ns: i64,
    end_ts_ns: i64,
    reference_latency_ms: i64,
) -> Result<Vec<ReferenceCacheRow>> {
    let symbol = symbol.trim().to_ascii_uppercase();
    let latency_ns = reference_latency_ms
        .checked_mul(NANOS_PER_MILLI)
        .context("reference latency overflow")?;
    let start_ms = start_ts_ns
        .saturating_sub(latency_ns)
        .saturating_sub(NANOS_PER_SECOND)
        .div_euclid(NANOS_PER_MILLI)
        .max(0);
    let end_ms = end_ts_ns
        .saturating_sub(latency_ns)
        .saturating_add(NANOS_PER_MILLI - 1)
        .div_euclid(NANOS_PER_MILLI)
        .max(start_ms + 1);
    let mut rows_by_open_ms = BTreeMap::<i64, ReferenceCacheRow>::new();
    let mut cursor = start_ms;
    while cursor < end_ms {
        let page_end = end_ms - 1;
        let params =
            format!("symbol={symbol}&interval=1s&startTime={cursor}&endTime={page_end}&limit=1000");
        let payload = fetch_binance_klines(client, &params)
            .with_context(|| format!("download Binance 1s reference for {symbol}"))?;
        let items = payload
            .as_array()
            .ok_or_else(|| anyhow!("Binance kline response must be an array"))?;
        if items.is_empty() {
            break;
        }
        for item in items {
            let row = parse_binance_reference_row(&symbol, item, reference_latency_ms)?;
            if ts_in_window(row.ts_ns, Some(start_ts_ns), Some(end_ts_ns)) {
                let open_ms = row
                    .ts_ns
                    .saturating_sub(latency_ns)
                    .saturating_sub(NANOS_PER_SECOND)
                    .div_euclid(NANOS_PER_MILLI);
                rows_by_open_ms.insert(open_ms, row);
            }
        }
        let last_open = binance_kline_open_ms(
            items
                .last()
                .ok_or_else(|| anyhow!("Binance kline page unexpectedly empty"))?,
        )?;
        if last_open < cursor {
            break;
        }
        cursor = last_open + 1_000;
        if items.len() < 1_000 {
            break;
        }
    }
    Ok(rows_by_open_ms.into_values().collect())
}

fn fetch_binance_klines(
    client: &reqwest::blocking::Client,
    params: &str,
) -> Result<serde_json::Value> {
    let mut failures = Vec::new();
    for base in [BINANCE_DATA_API_BASE, BINANCE_API_BASE] {
        let url = format!("{base}/api/v3/klines?{params}");
        match fetch_json_with_retry(client, &url, BINANCE_REFERENCE_MAX_ATTEMPTS) {
            Ok(value) => return Ok(value),
            Err(err) => failures.push(format!("{base}: {err:#}")),
        }
    }
    bail!("Binance kline download failed: {}", failures.join("; "))
}

fn fetch_json_with_retry(
    client: &reqwest::blocking::Client,
    url: &str,
    max_attempts: usize,
) -> Result<serde_json::Value> {
    for attempt in 0..max_attempts {
        let response = match client.get(url).send() {
            Ok(response) => response,
            Err(err) if attempt + 1 < max_attempts => {
                thread::sleep(Duration::from_millis(http_retry_delay_ms(attempt, None)));
                let _ = err;
                continue;
            }
            Err(err) => bail!("GET {url}: {err}"),
        };
        let status = response.status();
        if status.is_success() {
            let bytes = response
                .bytes()
                .with_context(|| format!("read JSON response for {url}"))?;
            return serde_json::from_slice(&bytes)
                .with_context(|| format!("decode JSON response for {url}"));
        }
        let retry_after = retry_after_ms(response.headers());
        if clob_status_is_retryable(status) && attempt + 1 < max_attempts {
            thread::sleep(Duration::from_millis(http_retry_delay_ms(
                attempt,
                retry_after,
            )));
            continue;
        }
        let body = response
            .text()
            .unwrap_or_else(|err| format!("<failed to read body: {err}>"));
        let snippet = body.chars().take(240).collect::<String>();
        bail!("HTTP status for {url}: {status}; body={snippet}");
    }
    bail!("GET {url}: exhausted retry budget")
}

fn parse_binance_reference_row(
    symbol: &str,
    item: &serde_json::Value,
    reference_latency_ms: i64,
) -> Result<ReferenceCacheRow> {
    let fields = item
        .as_array()
        .ok_or_else(|| anyhow!("Binance kline item must be an array"))?;
    if fields.len() < 6 {
        bail!("Binance kline item must contain at least 6 fields");
    }
    let open_ms = binance_kline_open_ms(item)?;
    let bar_close_ts_ns = open_ms
        .checked_add(1_000)
        .and_then(|value| value.checked_mul(NANOS_PER_MILLI))
        .context("Binance bar close timestamp overflow")?;
    let ts_ns = bar_close_ts_ns
        .checked_add(
            reference_latency_ms
                .checked_mul(NANOS_PER_MILLI)
                .context("reference latency overflow")?,
        )
        .context("synthetic reference timestamp overflow")?;
    let close = json_f64(&fields[4], "close")?;
    let close_micros = (close * 1_000_000.0).round() as i64;
    let row_hash =
        sha256_bytes(format!("bn_1s:{symbol}:{bar_close_ts_ns}:{close_micros}:{ts_ns}").as_bytes());
    Ok(ReferenceCacheRow {
        symbol: symbol.to_string(),
        ts_ns,
        close_micros,
        ingest_seq: 0,
        row_hash,
    })
}

fn binance_kline_open_ms(item: &serde_json::Value) -> Result<i64> {
    let fields = item
        .as_array()
        .ok_or_else(|| anyhow!("Binance kline item must be an array"))?;
    fields
        .first()
        .ok_or_else(|| anyhow!("Binance kline item missing open time"))
        .and_then(|value| json_i64(value, "open_time_ms"))
}

fn json_i64(value: &serde_json::Value, field: &str) -> Result<i64> {
    match value {
        serde_json::Value::Number(number) => number
            .as_i64()
            .ok_or_else(|| anyhow!("Binance kline {field} is not an i64")),
        serde_json::Value::String(text) => text
            .parse::<i64>()
            .with_context(|| format!("parse Binance kline {field}")),
        _ => bail!("Binance kline {field} has unsupported JSON type"),
    }
}

fn json_f64(value: &serde_json::Value, field: &str) -> Result<f64> {
    match value {
        serde_json::Value::Number(number) => number
            .as_f64()
            .ok_or_else(|| anyhow!("Binance kline {field} is not a number")),
        serde_json::Value::String(text) => text
            .parse::<f64>()
            .with_context(|| format!("parse Binance kline {field}")),
        _ => bail!("Binance kline {field} has unsupported JSON type"),
    }
}

fn settlement_http_client() -> Result<reqwest::blocking::Client> {
    let mut headers = HeaderMap::new();
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/json, text/plain, */*"),
    );
    headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(20))
        .default_headers(headers)
        .user_agent("pm5m-market-cache-settlement-refresh/0.1")
        .build()
        .context("build settlement refresh HTTP client")
}

fn fetch_clob_settlement_rows(
    client: &reqwest::blocking::Client,
    condition_id: &str,
    assets: &ConditionAssetMap,
) -> Result<Vec<SettlementInputRow>> {
    let url = format!("{POLYMARKET_CLOB_BASE}/markets/{condition_id}");
    for attempt in 0..CLOB_SETTLEMENT_MAX_ATTEMPTS {
        thread::sleep(Duration::from_millis(clob_request_pace_ms(
            condition_id,
            attempt,
        )));
        let response = match client.get(&url).send() {
            Ok(response) => response,
            Err(err) if attempt + 1 < CLOB_SETTLEMENT_MAX_ATTEMPTS => {
                thread::sleep(Duration::from_millis(clob_retry_delay_ms(
                    condition_id,
                    attempt,
                    None,
                )));
                let _ = err;
                continue;
            }
            Err(err) => bail!("GET {url}: {err}"),
        };
        let status = response.status();
        if status.is_success() {
            let bytes = response
                .bytes()
                .with_context(|| format!("read CLOB market payload for {condition_id}"))?;
            let payload: serde_json::Value = serde_json::from_slice(&bytes)
                .with_context(|| format!("decode CLOB market payload for {condition_id}"))?;
            return parse_clob_settlement_payload(condition_id, assets, &payload);
        }

        let retry_after_ms = retry_after_ms(response.headers());
        if clob_status_is_retryable(status) && attempt + 1 < CLOB_SETTLEMENT_MAX_ATTEMPTS {
            thread::sleep(Duration::from_millis(clob_retry_delay_ms(
                condition_id,
                attempt,
                retry_after_ms,
            )));
            continue;
        }

        let body = response
            .text()
            .unwrap_or_else(|err| format!("<failed to read body: {err}>"));
        let snippet = body.chars().take(240).collect::<String>();
        bail!("HTTP status for {url}: {status}; body={snippet}");
    }
    bail!("GET {url}: exhausted CLOB settlement retry budget")
}

fn parse_clob_settlement_payload(
    condition_id: &str,
    assets: &ConditionAssetMap,
    payload: &serde_json::Value,
) -> Result<Vec<SettlementInputRow>> {
    let closed = payload
        .get("closed")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if !closed {
        bail!("CLOB market is not closed");
    }
    let tokens = payload
        .get("tokens")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow!("CLOB market payload missing tokens"))?;
    let payload_hash = sha256_bytes(&serde_json::to_vec(payload)?);
    let mut rows = Vec::with_capacity(2);
    for (asset_id, outcome) in [
        (assets.yes_asset_id.as_str(), "YES"),
        (assets.no_asset_id.as_str(), "NO"),
    ] {
        let token = tokens
            .iter()
            .find(|token| settlement_token_matches(token, asset_id, outcome))
            .ok_or_else(|| anyhow!("CLOB token not found for {outcome} asset {asset_id}"))?;
        let winner = token_winner(token)
            .ok_or_else(|| anyhow!("closed CLOB token missing winner for {outcome}"))?;
        rows.push(SettlementInputRow {
            condition_id: condition_id.to_string(),
            asset_id: asset_id.to_string(),
            outcome: outcome.to_string(),
            status: "settled".to_string(),
            winner: Some(winner),
            settled_ts_ns: None,
            row_hash: sha256_bytes(
                format!("{payload_hash}:{condition_id}:{asset_id}:{outcome}:{winner}").as_bytes(),
            ),
        });
    }
    if rows.iter().filter(|row| row.winner == Some(true)).count() != 1 {
        bail!("CLOB payload does not contain exactly one settlement winner");
    }
    Ok(rows)
}

fn settlement_token_matches(token: &serde_json::Value, asset_id: &str, outcome: &str) -> bool {
    let token_id = string_value(token, &["token_id", "tokenId", "asset_id", "assetId"]);
    if token_id.as_deref() == Some(asset_id) {
        return true;
    }
    string_value(token, &["outcome"])
        .map(|value| normalize_outcome(&value) == normalize_outcome(outcome))
        .unwrap_or(false)
}

fn token_winner(token: &serde_json::Value) -> Option<bool> {
    if let Some(value) = token.get("winner").and_then(serde_json::Value::as_bool) {
        return Some(value);
    }
    let price = token.get("price")?;
    if decimal_is(price, 1.0) {
        Some(true)
    } else if decimal_is(price, 0.0) {
        Some(false)
    } else {
        None
    }
}

fn string_value(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(serde_json::Value::as_str))
        .map(ToString::to_string)
}

fn normalize_outcome(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

fn decimal_is(value: &serde_json::Value, expected: f64) -> bool {
    let actual = match value {
        serde_json::Value::Number(number) => number.as_f64(),
        serde_json::Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    };
    actual
        .map(|actual| (actual - expected).abs() < 0.000_000_001)
        .unwrap_or(false)
}

fn refresh_worker_count(requested: usize, task_count: usize) -> usize {
    requested
        .clamp(1, task_count.max(1))
        .min(CLOB_SETTLEMENT_WORKER_CAP)
}

fn settlement_progress_every(task_count: usize) -> usize {
    (task_count / 10).clamp(1, 100)
}

fn clob_status_is_retryable(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn retry_after_ms(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|seconds| seconds.saturating_mul(1_000).clamp(500, 60_000))
}

fn clob_request_pace_ms(condition_id: &str, attempt: usize) -> u64 {
    300 + deterministic_jitter_ms(condition_id, attempt.wrapping_add(97), 250)
}

fn clob_retry_delay_ms(condition_id: &str, attempt: usize, retry_after_ms: Option<u64>) -> u64 {
    if let Some(delay_ms) = retry_after_ms {
        return delay_ms;
    }
    let exponent = attempt.min(6);
    let base_ms = 500u64.saturating_mul(1u64 << exponent);
    base_ms
        .saturating_add(deterministic_jitter_ms(condition_id, attempt, 500))
        .min(30_000)
}

fn http_retry_delay_ms(attempt: usize, retry_after_ms: Option<u64>) -> u64 {
    if let Some(delay_ms) = retry_after_ms {
        return delay_ms;
    }
    let exponent = attempt.min(6);
    500u64
        .saturating_mul(1u64 << exponent)
        .saturating_add((attempt as u64).wrapping_mul(137) % 500)
        .min(30_000)
}

fn deterministic_jitter_ms(condition_id: &str, salt: usize, modulus_ms: u64) -> u64 {
    if modulus_ms == 0 {
        return 0;
    }
    let mut hash = 14_695_981_039_346_656_037u64;
    for byte in condition_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    for byte in salt.to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    hash % modulus_ms
}

fn write_reference_file(path: &Path, rows: &[ReferenceCacheRow]) -> Result<()> {
    let mut symbol_dict = Dict::default();
    let mut hash_dict = Dict::default();
    let mut row_bytes = Vec::new();
    for row in rows {
        push_u32(&mut row_bytes, symbol_dict.intern(&row.symbol)?);
        push_i64(&mut row_bytes, row.ts_ns);
        push_i64(&mut row_bytes, row.close_micros);
        push_u64(&mut row_bytes, row.ingest_seq);
        push_u32(&mut row_bytes, hash_dict.intern(&row.row_hash)?);
    }
    let header = RefHeader {
        schema_version: 1,
        dataset_format: HFTREF1_FORMAT.to_string(),
        row_count: rows.len(),
        symbols: symbol_dict.values,
        row_hashes: hash_dict.values,
    };
    write_compact_file(path, HFTREF1_MAGIC, &header, &row_bytes)
}

fn write_settlement_file(path: &Path, rows: &[SettlementCacheRow]) -> Result<()> {
    let mut condition_dict = Dict::default();
    let mut asset_dict = Dict::default();
    let mut hash_dict = Dict::default();
    let mut row_bytes = Vec::new();
    for row in rows {
        push_u32(&mut row_bytes, condition_dict.intern(&row.condition_id)?);
        push_u32(&mut row_bytes, asset_dict.intern(&row.yes_asset_id)?);
        push_u32(&mut row_bytes, asset_dict.intern(&row.no_asset_id)?);
        push_u32(&mut row_bytes, asset_dict.intern(&row.winner_asset_id)?);
        row_bytes.push(if row.winner_outcome == "YES" { 1 } else { 2 });
        push_i64(&mut row_bytes, row.finalized_ts_ns.unwrap_or(i64::MIN));
        push_u32(&mut row_bytes, hash_dict.intern(&row.row_hash)?);
    }
    let header = SettleHeader {
        schema_version: 1,
        dataset_format: HFTSETTLE1_FORMAT.to_string(),
        row_count: rows.len(),
        conditions: condition_dict.values,
        assets: asset_dict.values,
        row_hashes: hash_dict.values,
    };
    write_compact_file(path, HFTSETTLE1_MAGIC, &header, &row_bytes)
}

fn read_reference_file(path: &Path) -> Result<Vec<ReferenceCacheRow>> {
    let (header, rows) = read_compact_file::<RefHeader>(path, HFTREF1_MAGIC)?;
    let mut cursor = 0usize;
    let mut out = Vec::with_capacity(header.row_count);
    while cursor < rows.len() {
        let symbol_key = read_u32(&rows, &mut cursor)?;
        let ts_ns = read_i64(&rows, &mut cursor)?;
        let close_micros = read_i64(&rows, &mut cursor)?;
        let ingest_seq = read_u64(&rows, &mut cursor)?;
        let hash_key = read_u32(&rows, &mut cursor)?;
        out.push(ReferenceCacheRow {
            symbol: dict_value(&header.symbols, symbol_key, "symbol")?,
            ts_ns,
            close_micros,
            ingest_seq,
            row_hash: dict_value(&header.row_hashes, hash_key, "row_hash")?,
        });
    }
    Ok(out)
}

fn read_settlement_file(path: &Path) -> Result<Vec<SettlementCacheRow>> {
    let (header, rows) = read_compact_file::<SettleHeader>(path, HFTSETTLE1_MAGIC)?;
    let mut cursor = 0usize;
    let mut out = Vec::with_capacity(header.row_count);
    while cursor < rows.len() {
        let condition_key = read_u32(&rows, &mut cursor)?;
        let yes_key = read_u32(&rows, &mut cursor)?;
        let no_key = read_u32(&rows, &mut cursor)?;
        let winner_key = read_u32(&rows, &mut cursor)?;
        let outcome = *rows
            .get(cursor)
            .ok_or_else(|| anyhow!("truncated HFTSETTLE1 winner outcome"))?;
        cursor += 1;
        let finalized_ts_ns = read_i64(&rows, &mut cursor)?;
        let hash_key = read_u32(&rows, &mut cursor)?;
        out.push(SettlementCacheRow {
            condition_id: dict_value(&header.conditions, condition_key, "condition")?,
            yes_asset_id: dict_value(&header.assets, yes_key, "yes_asset")?,
            no_asset_id: dict_value(&header.assets, no_key, "no_asset")?,
            winner_asset_id: dict_value(&header.assets, winner_key, "winner_asset")?,
            winner_outcome: if outcome == 1 { "YES" } else { "NO" }.to_string(),
            finalized_ts_ns: if finalized_ts_ns == i64::MIN {
                None
            } else {
                Some(finalized_ts_ns)
            },
            row_hash: dict_value(&header.row_hashes, hash_key, "row_hash")?,
        });
    }
    Ok(out)
}

fn write_compact_file<T: Serialize>(
    path: &Path,
    magic: &[u8],
    header: &T,
    row_bytes: &[u8],
) -> Result<()> {
    let header_bytes = serde_json::to_vec(header)?;
    let header_crc = crc32(&header_bytes);
    let row_crc = crc32(row_bytes);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(magic);
    bytes.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&header_bytes);
    bytes.extend_from_slice(&header_crc.to_le_bytes());
    bytes.extend_from_slice(row_bytes);
    bytes.extend_from_slice(&row_crc.to_le_bytes());
    market_data_etl_core::atomic_write_verified(path, &bytes, |tmp| {
        let _ = fs::read(tmp)?;
        Ok(())
    })
}

fn read_compact_file<T: for<'de> Deserialize<'de>>(
    path: &Path,
    magic: &[u8],
) -> Result<(T, Vec<u8>)> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let mut cursor = 0usize;
    if bytes.get(..magic.len()) != Some(magic) {
        bail!("invalid compact cache magic");
    }
    cursor += magic.len();
    let header_len = read_u32(&bytes, &mut cursor)? as usize;
    let header_bytes = bytes
        .get(cursor..cursor + header_len)
        .ok_or_else(|| anyhow!("truncated compact cache header"))?;
    cursor += header_len;
    let expected_header_crc = read_u32(&bytes, &mut cursor)?;
    if crc32(header_bytes) != expected_header_crc {
        bail!("compact cache header CRC mismatch");
    }
    let row_end = bytes
        .len()
        .checked_sub(4)
        .ok_or_else(|| anyhow!("truncated compact cache row CRC"))?;
    let row_bytes = bytes
        .get(cursor..row_end)
        .ok_or_else(|| anyhow!("truncated compact cache rows"))?;
    let expected_row_crc = u32::from_le_bytes(bytes[row_end..].try_into().unwrap());
    if crc32(row_bytes) != expected_row_crc {
        bail!("compact cache row CRC mismatch");
    }
    Ok((serde_json::from_slice(header_bytes)?, row_bytes.to_vec()))
}

fn read_compact_payload_fast<T: for<'de> Deserialize<'de>>(
    path: &Path,
    magic: &[u8],
) -> Result<FastCompactPayload<T>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let mut cursor = 0usize;
    if bytes.get(..magic.len()) != Some(magic) {
        bail!("invalid compact cache magic");
    }
    cursor += magic.len();
    let header_len = read_u32(&bytes, &mut cursor)? as usize;
    let header_end = cursor
        .checked_add(header_len)
        .context("compact cache header length overflow")?;
    let header_bytes = bytes
        .get(cursor..header_end)
        .ok_or_else(|| anyhow!("truncated compact cache header"))?;
    cursor = header_end;
    let expected_header_crc = read_u32(&bytes, &mut cursor)?;
    if crc32(header_bytes) != expected_header_crc {
        bail!("compact cache header CRC mismatch");
    }
    let row_start = cursor;
    let row_end = bytes
        .len()
        .checked_sub(4)
        .ok_or_else(|| anyhow!("truncated compact cache row CRC"))?;
    if row_end < row_start {
        bail!("truncated compact cache rows");
    }
    Ok(FastCompactPayload {
        header: serde_json::from_slice(header_bytes)?,
        bytes,
        row_start,
        row_end,
    })
}

fn prepare_root(cache_root: &Path, overwrite: bool) -> Result<()> {
    if cache_root.exists() {
        if !overwrite {
            bail!(
                "compact cache root already exists: {}",
                cache_root.display()
            );
        }
        fs::remove_dir_all(cache_root)?;
    }
    fs::create_dir_all(cache_root)?;
    Ok(())
}

fn write_catalog(
    cache_root: &Path,
    catalog_name: &str,
    dataset_format: &str,
    data_path: &str,
    row_count: usize,
) -> Result<CompactCacheCatalog> {
    let catalog = CompactCacheCatalog {
        schema_version: 1,
        dataset_format: dataset_format.to_string(),
        cache_root: cache_root.to_path_buf(),
        data_path: data_path.to_string(),
        generated_ts_ns: now_unix_ns() as i64,
        row_count,
        data_sha256: sha256_file(&cache_root.join(data_path))?,
    };
    market_data_etl_core::write_json_file_pretty(&cache_root.join(catalog_name), &catalog)?;
    Ok(catalog)
}

fn read_catalog(
    cache_root: &Path,
    catalog_name: &str,
    dataset_format: &str,
) -> Result<CompactCacheCatalog> {
    let path = cache_root.join(catalog_name);
    let catalog: CompactCacheCatalog = serde_json::from_reader(fs::File::open(&path)?)?;
    if catalog.dataset_format != dataset_format {
        bail!(
            "unsupported compact cache format {}",
            catalog.dataset_format
        );
    }
    Ok(catalog)
}

fn canonical_symbol(symbol: &str) -> String {
    let upper = symbol.trim().to_ascii_uppercase();
    match upper.as_str() {
        "BTC" | "ETH" | "SOL" => format!("{upper}-5M"),
        _ => upper,
    }
}

impl Dict {
    fn intern(&mut self, value: &str) -> Result<u32> {
        if let Some(key) = self.by_value.get(value) {
            return Ok(*key);
        }
        let key = u32::try_from(self.values.len()).context("compact cache dictionary overflow")?;
        self.values.push(value.to_string());
        self.by_value.insert(value.to_string(), key);
        Ok(key)
    }
}

fn dict_value(values: &[String], key: u32, field: &str) -> Result<String> {
    values
        .get(key as usize)
        .cloned()
        .ok_or_else(|| anyhow!("compact cache {field} key out of range"))
}

fn ts_in_window(ts_ns: i64, start_ts_ns: Option<i64>, end_ts_ns: Option<i64>) -> bool {
    start_ts_ns.is_none_or(|start| ts_ns >= start) && end_ts_ns.is_none_or(|end| ts_ns < end)
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Crc32::new();
    hasher.update(bytes);
    hasher.finalize()
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let out = bytes
        .get(*cursor..*cursor + 4)
        .ok_or_else(|| anyhow!("truncated compact cache u32"))?;
    *cursor += 4;
    Ok(u32::from_le_bytes(out.try_into().unwrap()))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    let out = bytes
        .get(*cursor..*cursor + 8)
        .ok_or_else(|| anyhow!("truncated compact cache u64"))?;
    *cursor += 8;
    Ok(u64::from_le_bytes(out.try_into().unwrap()))
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64> {
    let out = bytes
        .get(*cursor..*cursor + 8)
        .ok_or_else(|| anyhow!("truncated compact cache i64"))?;
    *cursor += 8;
    Ok(i64::from_le_bytes(out.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clob_settlement_refresh_workers_are_capped() {
        assert_eq!(refresh_worker_count(24, 100), CLOB_SETTLEMENT_WORKER_CAP);
        assert_eq!(refresh_worker_count(2, 100), 2);
        assert_eq!(refresh_worker_count(24, 2), 2);
    }

    #[test]
    fn clob_retry_after_seconds_are_parsed_and_bounded() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("7"));
        assert_eq!(retry_after_ms(&headers), Some(7_000));

        headers.insert(RETRY_AFTER, HeaderValue::from_static("120"));
        assert_eq!(retry_after_ms(&headers), Some(60_000));
    }
}
