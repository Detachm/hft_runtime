use crate::hftbook2::{
    write_book_cache2_catalog, write_book_cache2_partition_batch, BookCache2Partition,
    WriteBookCache2Options,
};
use crate::replay::CanonicalWsBookReplayer;
use crate::types::{
    canonical_market_symbol, BookCacheRow, RawPolymarketClobWsEvent, BOOK_CACHE2_CATALOG,
    BOOK_CACHE2_FORMAT, DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS,
    DEFAULT_POLY_INCREMENTAL_LATENCY_MS, WS_RAW_STREAM,
};
use anyhow::{anyhow, bail, Context, Result};
use chrono::{FixedOffset, NaiveDate, TimeZone};
use market_data_etl_core::{
    discover_hftrec4_manifests, read_hftrec4_segment_header_summary, scan_hftrec4_segment_selected,
    Hftrec4RecordMeta, Hftrec4SegmentManifest, HFTREC4_FORMAT,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const HOUR_NS: i64 = 3_600_000_000_000;
const MAX_BUFFERED_BOOK_ROWS: usize = 250_000;
const PARALLEL_SCAN_CHUNK_MANIFESTS: usize = 2048;
const POLYMARKET_CLOB_BASE: &str = "https://clob.polymarket.com";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildBookCacheOptions {
    pub raw_roots: Vec<PathBuf>,
    pub cache_root: PathBuf,
    pub raw_start_ts_ns: Option<i64>,
    pub raw_end_ts_ns: Option<i64>,
    pub market_symbol_allowlist: Vec<String>,
    pub overwrite: bool,
    pub replay_workers: Option<usize>,
    #[serde(default)]
    pub enrich_missing_clob_metadata: bool,
    #[serde(default)]
    pub clob_metadata_cache_root: Option<PathBuf>,
    #[serde(default)]
    pub condition_allowlist_path: Option<PathBuf>,
    #[serde(default)]
    pub poly_server_visible_time: bool,
    #[serde(default = "default_poly_incremental_latency_ms")]
    pub poly_incremental_latency_ms: i64,
    #[serde(default = "default_poly_incremental_freshness_guard_ms")]
    pub poly_incremental_freshness_guard_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookCacheBuildReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub cache_root: PathBuf,
    pub reused_existing: bool,
    pub catalog_path: PathBuf,
    pub row_count: usize,
    pub partition_count: usize,
    pub manifest_count: usize,
    pub replay_workers: usize,
    pub poly_server_visible_time: bool,
    pub poly_incremental_latency_ms: i64,
    pub poly_incremental_freshness_guard_ms: i64,
    pub elapsed_ms: u128,
}

pub fn build_book_cache(options: &BuildBookCacheOptions) -> Result<BookCacheBuildReport> {
    validate_options(options)?;
    let started = Instant::now();
    let catalog_path = options.cache_root.join(BOOK_CACHE2_CATALOG);
    if catalog_path.exists() && !options.overwrite {
        let catalog = crate::read_book_cache2_catalog(&options.cache_root)?;
        return Ok(BookCacheBuildReport {
            schema_version: 1,
            dataset_format: BOOK_CACHE2_FORMAT.to_string(),
            cache_root: options.cache_root.clone(),
            reused_existing: true,
            catalog_path,
            row_count: catalog.row_count,
            partition_count: catalog.partitions.len(),
            manifest_count: 0,
            replay_workers: 0,
            poly_server_visible_time: options.poly_server_visible_time,
            poly_incremental_latency_ms: options.poly_incremental_latency_ms,
            poly_incremental_freshness_guard_ms: options.poly_incremental_freshness_guard_ms,
            elapsed_ms: started.elapsed().as_millis(),
        });
    }

    let symbol_filter = SymbolFilter::new(&options.market_symbol_allowlist)?;
    let manifests = sorted_candidate_manifests(options)?;
    if options.cache_root.exists() {
        if !options.overwrite {
            bail!(
                "book cache root already exists; pass overwrite to replace: {}",
                options.cache_root.display()
            );
        }
        fs::remove_dir_all(&options.cache_root)
            .with_context(|| format!("remove {}", options.cache_root.display()))?;
    }
    fs::create_dir_all(&options.cache_root)
        .with_context(|| format!("create {}", options.cache_root.display()))?;

    let write_options = WriteBookCache2Options {
        cache_root: options.cache_root.clone(),
        raw_roots: options.raw_roots.clone(),
        raw_start_ts_ns: options.raw_start_ts_ns,
        raw_end_ts_ns: options.raw_end_ts_ns,
        market_symbol_allowlist: options.market_symbol_allowlist.clone(),
        overwrite: options.overwrite,
        poly_server_visible_time: options.poly_server_visible_time,
        poly_incremental_latency_ms: options.poly_incremental_latency_ms,
        poly_incremental_freshness_guard_ms: options.poly_incremental_freshness_guard_ms,
    };
    let mut row_buffer = Vec::<BookCacheRow>::new();
    let mut current_hour = None::<i64>;
    let mut partition_batches = 0usize;
    let mut partitions = Vec::<BookCache2Partition>::new();
    let mut replayer = CanonicalWsBookReplayer::default();
    let mut metadata_resolver = ClobMarketMetadataResolver::new(options)?;
    let mut condition_allowlist = BTreeMap::<String, bool>::new();
    let mut consumed_manifests = 0usize;
    let replay_workers = replay_worker_count(options, &manifests);

    if replay_workers > 1 {
        let consumed_manifests = manifests
            .iter()
            .filter(|manifest| manifest_overlaps(manifest, options))
            .count();
        if consumed_manifests == 0 {
            bail!("build-book-cache found no finalized HFTREC4 WS manifests");
        }
        let catalog = build_hftrec4_ws_book_cache_parallel(
            options,
            &write_options,
            &symbol_filter,
            manifests,
            replay_workers,
        )?;
        return Ok(BookCacheBuildReport {
            schema_version: 1,
            dataset_format: BOOK_CACHE2_FORMAT.to_string(),
            cache_root: options.cache_root.clone(),
            reused_existing: false,
            catalog_path,
            row_count: catalog.row_count,
            partition_count: catalog.partitions.len(),
            manifest_count: consumed_manifests,
            replay_workers,
            poly_server_visible_time: options.poly_server_visible_time,
            poly_incremental_latency_ms: options.poly_incremental_latency_ms,
            poly_incremental_freshness_guard_ms: options.poly_incremental_freshness_guard_ms,
            elapsed_ms: started.elapsed().as_millis(),
        });
    }

    for manifest in manifests {
        if !manifest_overlaps(&manifest, options) {
            continue;
        }
        consumed_manifests += 1;
        scan_hftrec4_segment_selected(
            &manifest.segment_path,
            |record| {
                should_read_ws_payload(record, options, &symbol_filter, &mut condition_allowlist)
            },
            |record| {
                let mut raw = hftrec4_record_to_ws_raw(record);
                if raw.condition_id.is_none() {
                    raw.condition_id = condition_id_from_payload(&raw.raw_payload);
                }
                if raw_symbol_disallowed(&raw, &symbol_filter) {
                    return Ok(());
                }
                if symbol_filter.is_restrictive()
                    && raw.event_type.eq_ignore_ascii_case("price_change")
                    && raw.condition_id.is_none()
                {
                    return Ok(());
                }
                metadata_resolver.enrich(&mut raw)?;
                if raw_symbol_disallowed(&raw, &symbol_filter) {
                    return Ok(());
                }
                apply_poly_visible_time_model(&mut raw, options);
                for row in replayer.apply(raw)? {
                    if ts_in_window(row.local_recv_ts_ns, options)
                        && symbol_filter.allows(&row.symbol)
                    {
                        buffer_book_cache_row(
                            row,
                            &options.cache_root,
                            &mut row_buffer,
                            &mut current_hour,
                            &mut partition_batches,
                            &mut partitions,
                            "serial",
                        )?;
                    }
                }
                Ok(())
            },
        )
        .with_context(|| {
            format!(
                "read HFTREC4 WS segment {}",
                manifest.segment_path.display()
            )
        })?;
        if consumed_manifests > 0 && consumed_manifests % 10_000 == 0 {
            eprintln!(
                "HFTBOOK2 builder consumed {consumed_manifests} manifest(s), flushed {} partition(s), buffered {} row(s)",
                partitions.len(),
                row_buffer.len()
            );
        }
    }

    if consumed_manifests == 0 {
        bail!("build-book-cache found no finalized HFTREC4 WS manifests");
    }
    flush_book_cache_rows(
        &options.cache_root,
        &mut row_buffer,
        &mut current_hour,
        &mut partition_batches,
        &mut partitions,
        "serial",
    )?;
    if partitions.is_empty() {
        bail!("build-book-cache produced no rows for requested window/market filter");
    }

    let catalog = write_book_cache2_catalog(&write_options, partitions)?;

    Ok(BookCacheBuildReport {
        schema_version: 1,
        dataset_format: BOOK_CACHE2_FORMAT.to_string(),
        cache_root: options.cache_root.clone(),
        reused_existing: false,
        catalog_path,
        row_count: catalog.row_count,
        partition_count: catalog.partitions.len(),
        manifest_count: consumed_manifests,
        replay_workers,
        poly_server_visible_time: options.poly_server_visible_time,
        poly_incremental_latency_ms: options.poly_incremental_latency_ms,
        poly_incremental_freshness_guard_ms: options.poly_incremental_freshness_guard_ms,
        elapsed_ms: started.elapsed().as_millis(),
    })
}

fn replay_worker_count(
    options: &BuildBookCacheOptions,
    manifests: &[RawCandidateManifest],
) -> usize {
    let default_workers = if manifests.is_empty() {
        1
    } else {
        std::thread::available_parallelism()
            .ok()
            .map(usize::from)
            .unwrap_or(1)
            .min(8)
    };
    options
        .replay_workers
        .unwrap_or(default_workers)
        .clamp(1, 96)
}

fn build_hftrec4_ws_book_cache_parallel(
    options: &BuildBookCacheOptions,
    write_options: &WriteBookCache2Options,
    symbol_filter: &SymbolFilter,
    manifests: Vec<RawCandidateManifest>,
    replay_workers: usize,
) -> Result<crate::hftbook2::BookCache2Catalog> {
    let started = Instant::now();
    let options_arc = std::sync::Arc::new(options.clone());
    let symbol_filter = std::sync::Arc::new(symbol_filter.clone());
    let mut senders = Vec::with_capacity(replay_workers);
    let mut handles = Vec::with_capacity(replay_workers);

    for worker_idx in 0..replay_workers {
        let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<RawPolymarketClobWsEvent>>(4);
        senders.push(tx);
        let options = std::sync::Arc::clone(&options_arc);
        let symbol_filter = std::sync::Arc::clone(&symbol_filter);
        handles.push(std::thread::spawn(
            move || -> Result<Vec<BookCache2Partition>> {
                let worker_result = (|| -> Result<Vec<BookCache2Partition>> {
                    let mut row_buffer = Vec::<BookCacheRow>::new();
                    let mut current_hour = None::<i64>;
                    let mut partition_batches = 0usize;
                    let mut partitions = Vec::<BookCache2Partition>::new();
                    let mut replayer = CanonicalWsBookReplayer::default();
                    let mut metadata_resolver = ClobMarketMetadataResolver::new(&options)?;
                    let prefix_scope = format!("condshard_{worker_idx:02}");

                    for batch in rx {
                        for mut raw in batch {
                            if raw_symbol_disallowed(&raw, &symbol_filter) {
                                continue;
                            }
                            metadata_resolver.enrich(&mut raw)?;
                            if raw_symbol_disallowed(&raw, &symbol_filter) {
                                continue;
                            }
                            for row in replayer.apply(raw)? {
                                if condition_shard(&row.condition_id, replay_workers) != worker_idx
                                {
                                    continue;
                                }
                                if ts_in_window(row.local_recv_ts_ns, &options)
                                    && symbol_filter.allows(&row.symbol)
                                {
                                    buffer_book_cache_row(
                                        row,
                                        &options.cache_root,
                                        &mut row_buffer,
                                        &mut current_hour,
                                        &mut partition_batches,
                                        &mut partitions,
                                        &prefix_scope,
                                    )?;
                                }
                            }
                        }
                    }
                    flush_book_cache_rows(
                        &options.cache_root,
                        &mut row_buffer,
                        &mut current_hour,
                        &mut partition_batches,
                        &mut partitions,
                        &prefix_scope,
                    )?;
                    Ok(partitions)
                })();
                if let Err(error) = &worker_result {
                    eprintln!("HFTBOOK2 replay worker {worker_idx} failed: {error:#}");
                }
                worker_result
            },
        ));
    }

    let manifests = manifests
        .into_iter()
        .filter(|manifest| manifest_overlaps(manifest, options))
        .collect::<Vec<_>>();
    let consumed_manifests = manifests.len();
    let condition_allowlist = load_condition_allowlist(options)?
        .map(|conditions| {
            conditions
                .into_iter()
                .map(|condition| (condition, true))
                .collect()
        })
        .map(Ok)
        .unwrap_or_else(|| {
            build_condition_allowlist(&manifests, options, &symbol_filter, replay_workers)
        })?;
    let mut sent_rows = 0u64;
    for (chunk_idx, chunk) in manifests.chunks(PARALLEL_SCAN_CHUNK_MANIFESTS).enumerate() {
        let shard_batches = scan_manifest_chunk_parallel(
            chunk,
            options,
            &symbol_filter,
            &condition_allowlist,
            replay_workers,
        )?;
        for (shard, batch) in shard_batches.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            sent_rows += batch.len() as u64;
            senders[shard].send(batch).map_err(|_| {
                anyhow::anyhow!("HFTBOOK2 replay worker {shard} stopped before input finished")
            })?;
        }
        let processed_manifests =
            ((chunk_idx + 1) * PARALLEL_SCAN_CHUNK_MANIFESTS).min(consumed_manifests);
        if processed_manifests % 1_024 == 0 || processed_manifests == consumed_manifests {
            eprintln!(
                "HFTBOOK2 fanout consumed {processed_manifests}/{consumed_manifests} manifest(s), sent_rows={sent_rows}, elapsed_ms={}",
                started.elapsed().as_millis()
            );
        }
    }
    drop(senders);

    let mut partitions = Vec::<BookCache2Partition>::new();
    for handle in handles {
        partitions.extend(
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("HFTBOOK2 replay worker panicked"))??,
        );
    }
    if partitions.is_empty() {
        bail!("build-book-cache produced no rows for requested window/market filter");
    }
    eprintln!(
        "HFTBOOK2 fanout builder replay_workers={replay_workers} consumed_manifests={consumed_manifests} sent_rows={sent_rows} wrote {} partition(s) elapsed_ms={}",
        partitions.len(),
        started.elapsed().as_millis()
    );
    write_book_cache2_catalog(write_options, partitions)
}

fn hftrec4_record_to_ws_raw(
    record: market_data_etl_core::Hftrec4Record,
) -> RawPolymarketClobWsEvent {
    let outcome = infer_outcome_from_assets(
        record.asset_id.as_deref(),
        record.yes_asset_id.as_deref(),
        record.no_asset_id.as_deref(),
    );
    RawPolymarketClobWsEvent {
        source_id: WS_RAW_STREAM.to_string(),
        ingest_seq_scope: WS_RAW_STREAM.to_string(),
        ingest_seq: record.ingest_seq,
        local_recv_ts_ns: record.local_recv_ts_ns,
        asset_id: record.asset_id,
        condition_id: record.condition_id,
        symbol: record.symbol,
        outcome,
        market_start_ts_ns: record.market_start_ts_ns,
        market_end_ts_ns: record.market_end_ts_ns,
        yes_asset_id: record.yes_asset_id,
        no_asset_id: record.no_asset_id,
        event_type: record.event_type,
        exchange_ts_ms: None,
        raw_payload_sha256: record.payload_sha256,
        raw_payload: record.payload,
    }
}

fn build_condition_allowlist(
    manifests: &[RawCandidateManifest],
    options: &BuildBookCacheOptions,
    symbol_filter: &SymbolFilter,
    scan_workers: usize,
) -> Result<BTreeMap<String, bool>> {
    if manifests.is_empty() {
        return Ok(BTreeMap::new());
    }
    let worker_count = scan_workers.clamp(1, manifests.len());
    let chunk_len = manifests.len().div_ceil(worker_count);
    let mut condition_allowlist = BTreeMap::<String, bool>::new();
    std::thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for chunk in manifests.chunks(chunk_len) {
            handles.push(scope.spawn(move || -> Result<BTreeMap<String, bool>> {
                let mut local = BTreeMap::<String, bool>::new();
                for manifest in chunk {
                    scan_hftrec4_segment_selected(
                        &manifest.segment_path,
                        |record| {
                            if ts_before_end(record.local_recv_ts_ns, options) {
                                remember_condition_allowlist_meta(
                                    record,
                                    symbol_filter,
                                    &mut local,
                                );
                            }
                            Ok(false)
                        },
                        |_| Ok(()),
                    )
                    .with_context(|| {
                        format!(
                            "scan HFTREC4 metadata allowlist {}",
                            manifest.segment_path.display()
                        )
                    })?;
                }
                Ok(local)
            }));
        }
        for handle in handles {
            for (condition_id, allowed) in handle
                .join()
                .map_err(|_| anyhow!("HFTREC4 allowlist scan worker panicked"))??
            {
                condition_allowlist
                    .entry(condition_id)
                    .and_modify(|existing| *existing |= allowed)
                    .or_insert(allowed);
            }
        }
        Ok(())
    })?;
    Ok(condition_allowlist)
}

fn load_condition_allowlist(options: &BuildBookCacheOptions) -> Result<Option<BTreeSet<String>>> {
    let Some(path) = options.condition_allowlist_path.as_ref() else {
        return Ok(None);
    };
    let value: Value = serde_json::from_reader(
        fs::File::open(path).with_context(|| format!("open {}", path.display()))?,
    )
    .with_context(|| format!("parse {}", path.display()))?;
    let values = if let Some(items) = value.as_array() {
        items
    } else {
        value
            .get("conditions")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("condition allowlist must be an array or contain conditions"))?
    };
    let mut out = BTreeSet::new();
    for item in values {
        let condition_id = item
            .as_str()
            .or_else(|| item.get("condition_id").and_then(Value::as_str))
            .ok_or_else(|| anyhow!("condition allowlist entries must be strings or objects"))?;
        out.insert(condition_id.to_string());
    }
    Ok(Some(out))
}

fn scan_manifest_chunk_parallel(
    manifests: &[RawCandidateManifest],
    options: &BuildBookCacheOptions,
    symbol_filter: &SymbolFilter,
    condition_allowlist: &BTreeMap<String, bool>,
    scan_workers: usize,
) -> Result<Vec<Vec<RawPolymarketClobWsEvent>>> {
    if manifests.is_empty() {
        return Ok((0..scan_workers).map(|_| Vec::new()).collect());
    }
    let worker_count = scan_workers.clamp(1, manifests.len());
    let chunk_len = manifests.len().div_ceil(worker_count);
    let mut out = Vec::<RawPolymarketClobWsEvent>::new();
    std::thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for chunk in manifests.chunks(chunk_len) {
            handles.push(
                scope.spawn(move || -> Result<Vec<RawPolymarketClobWsEvent>> {
                    let mut rows = Vec::new();
                    for manifest in chunk {
                        if !segment_may_contain_allowed_condition(
                            &manifest.segment_path,
                            condition_allowlist,
                        )? {
                            continue;
                        }
                        scan_hftrec4_segment_selected(
                            &manifest.segment_path,
                            |record| {
                                should_read_ws_payload_known(
                                    record,
                                    options,
                                    symbol_filter,
                                    condition_allowlist,
                                )
                            },
                            |record| {
                                rows.push(hftrec4_record_to_ws_raw(record));
                                Ok(())
                            },
                        )
                        .with_context(|| {
                            format!(
                                "parallel scan HFTREC4 selected payloads {}",
                                manifest.segment_path.display()
                            )
                        })?;
                    }
                    Ok(rows)
                }),
            );
        }
        for handle in handles {
            out.extend(
                handle
                    .join()
                    .map_err(|_| anyhow!("HFTREC4 selected scan worker panicked"))??,
            );
        }
        Ok(())
    })?;
    let mut shards = (0..scan_workers)
        .map(|_| Vec::<RawPolymarketClobWsEvent>::new())
        .collect::<Vec<_>>();
    for mut raw in out {
        if raw.condition_id.is_none() {
            raw.condition_id = condition_id_from_payload(&raw.raw_payload);
        }
        if raw_symbol_disallowed(&raw, symbol_filter) {
            continue;
        }
        apply_poly_visible_time_model(&mut raw, options);
        let Some(condition_id) = raw.condition_id.as_deref() else {
            continue;
        };
        if symbol_filter.is_restrictive()
            && !condition_allowlist
                .get(condition_id)
                .copied()
                .unwrap_or(false)
        {
            continue;
        }
        let shard = condition_shard(condition_id, scan_workers);
        shards[shard].push(raw);
    }
    for shard in &mut shards {
        shard.sort_by(raw_ws_condition_order);
    }
    Ok(shards)
}

fn apply_poly_visible_time_model(
    raw: &mut RawPolymarketClobWsEvent,
    options: &BuildBookCacheOptions,
) {
    if !options.poly_server_visible_time {
        return;
    }
    if !raw.event_type.eq_ignore_ascii_case("price_change") {
        return;
    }
    let Some(exchange_ts_ms) = raw
        .exchange_ts_ms
        .or_else(|| timestamp_ms_for_raw_payload(&raw.raw_payload))
    else {
        return;
    };
    raw.exchange_ts_ms = Some(exchange_ts_ms);
    let Some(synthetic_ts_ns) = exchange_ts_ms
        .checked_add(options.poly_incremental_latency_ms)
        .and_then(|ts_ms| ts_ms.checked_mul(1_000_000))
    else {
        return;
    };
    let original_recv_ts_ns = raw.local_recv_ts_ns;
    if synthetic_ts_ns > original_recv_ts_ns {
        return;
    }
    let guard_ns = options
        .poly_incremental_freshness_guard_ms
        .saturating_mul(1_000_000);
    if original_recv_ts_ns.saturating_sub(synthetic_ts_ns) <= guard_ns {
        raw.local_recv_ts_ns = synthetic_ts_ns;
    }
}

fn timestamp_ms_for_raw_payload(raw_payload: &[u8]) -> Option<i64> {
    let value = serde_json::from_slice::<Value>(raw_payload).ok()?;
    for key in ["timestamp", "ts", "time", "exchange_ts_ms"] {
        let Some(field) = value.get(key) else {
            continue;
        };
        if let Some(raw) = field.as_i64() {
            return Some(normalize_exchange_ts_ms(raw));
        }
        if let Some(raw) = field.as_u64().and_then(|raw| i64::try_from(raw).ok()) {
            return Some(normalize_exchange_ts_ms(raw));
        }
        if let Some(raw) = field.as_str().and_then(|raw| raw.parse::<i64>().ok()) {
            return Some(normalize_exchange_ts_ms(raw));
        }
    }
    None
}

fn normalize_exchange_ts_ms(raw: i64) -> i64 {
    if raw < 10_000_000_000_000 {
        raw
    } else {
        raw / 1_000_000
    }
}

fn segment_may_contain_allowed_condition(
    segment_path: &Path,
    condition_allowlist: &BTreeMap<String, bool>,
) -> Result<bool> {
    if condition_allowlist.is_empty() {
        return Ok(true);
    }
    let header = read_hftrec4_segment_header_summary(segment_path)?;
    Ok(header
        .conditions
        .iter()
        .any(|condition| condition_allowlist.get(condition).copied().unwrap_or(false)))
}

fn should_read_ws_payload(
    record: &Hftrec4RecordMeta,
    options: &BuildBookCacheOptions,
    symbol_filter: &SymbolFilter,
    condition_allowlist: &mut BTreeMap<String, bool>,
) -> Result<bool> {
    if !ts_before_end(record.local_recv_ts_ns, options) {
        return Ok(false);
    }
    let event_type = record.event_type.trim().to_ascii_lowercase();
    if event_type != "book" && event_type != "price_change" {
        return Ok(false);
    }
    if meta_symbol_disallowed(record, symbol_filter) {
        return Ok(false);
    }
    remember_condition_allowlist_meta(record, symbol_filter, condition_allowlist);
    if meta_condition_disallowed(record, condition_allowlist) {
        return Ok(false);
    }
    if symbol_filter.is_restrictive()
        && event_type == "price_change"
        && meta_condition_unknown(record, condition_allowlist)
    {
        return Ok(false);
    }
    Ok(true)
}

fn should_read_ws_payload_known(
    record: &Hftrec4RecordMeta,
    options: &BuildBookCacheOptions,
    symbol_filter: &SymbolFilter,
    condition_allowlist: &BTreeMap<String, bool>,
) -> Result<bool> {
    if !ts_before_end(record.local_recv_ts_ns, options) {
        return Ok(false);
    }
    let event_type = record.event_type.trim().to_ascii_lowercase();
    if event_type != "book" && event_type != "price_change" {
        return Ok(false);
    }
    if meta_symbol_disallowed(record, symbol_filter) {
        return Ok(false);
    }
    if !symbol_filter.is_restrictive() {
        return Ok(true);
    }
    let condition_allowed = record
        .condition_id
        .as_deref()
        .and_then(|condition_id| condition_allowlist.get(condition_id))
        .copied();
    if event_type == "price_change" {
        return Ok(condition_allowed.unwrap_or(false));
    }
    if let Some(allowed) = condition_allowed {
        return Ok(allowed);
    }
    Ok(record
        .symbol
        .as_deref()
        .is_some_and(|symbol| symbol_filter.allows(symbol)))
}

fn raw_ws_condition_order(
    a: &RawPolymarketClobWsEvent,
    b: &RawPolymarketClobWsEvent,
) -> std::cmp::Ordering {
    (
        a.condition_id.as_deref().unwrap_or_default(),
        a.local_recv_ts_ns,
        a.ingest_seq,
        a.asset_id.as_deref().unwrap_or_default(),
    )
        .cmp(&(
            b.condition_id.as_deref().unwrap_or_default(),
            b.local_recv_ts_ns,
            b.ingest_seq,
            b.asset_id.as_deref().unwrap_or_default(),
        ))
}

fn infer_outcome_from_assets(
    asset_id: Option<&str>,
    yes_asset_id: Option<&str>,
    no_asset_id: Option<&str>,
) -> Option<String> {
    let asset_id = asset_id?;
    if yes_asset_id.is_some_and(|yes_asset_id| yes_asset_id == asset_id) {
        return Some("YES".to_string());
    }
    if no_asset_id.is_some_and(|no_asset_id| no_asset_id == asset_id) {
        return Some("NO".to_string());
    }
    None
}

fn meta_symbol_disallowed(record: &Hftrec4RecordMeta, symbol_filter: &SymbolFilter) -> bool {
    record
        .symbol
        .as_deref()
        .and_then(canonical_market_symbol)
        .is_some_and(|symbol| !symbol_filter.allows(&symbol))
}

fn remember_condition_allowlist_meta(
    record: &Hftrec4RecordMeta,
    symbol_filter: &SymbolFilter,
    condition_allowlist: &mut BTreeMap<String, bool>,
) {
    let Some(condition_id) = record.condition_id.as_deref() else {
        return;
    };
    let Some(symbol) = record.symbol.as_deref().and_then(canonical_market_symbol) else {
        return;
    };
    condition_allowlist.insert(condition_id.to_string(), symbol_filter.allows(&symbol));
}

fn meta_condition_disallowed(
    record: &Hftrec4RecordMeta,
    condition_allowlist: &BTreeMap<String, bool>,
) -> bool {
    record
        .condition_id
        .as_deref()
        .and_then(|condition_id| condition_allowlist.get(condition_id))
        .is_some_and(|allowed| !*allowed)
}

fn meta_condition_unknown(
    record: &Hftrec4RecordMeta,
    condition_allowlist: &BTreeMap<String, bool>,
) -> bool {
    record
        .condition_id
        .as_deref()
        .is_none_or(|condition_id| !condition_allowlist.contains_key(condition_id))
}

fn raw_symbol_disallowed(raw: &RawPolymarketClobWsEvent, symbol_filter: &SymbolFilter) -> bool {
    raw.symbol
        .as_deref()
        .and_then(canonical_market_symbol)
        .is_some_and(|symbol| !symbol_filter.allows(&symbol))
}

fn condition_id_from_payload(payload: &[u8]) -> Option<String> {
    string_from_payload(payload, &["condition_id", "conditionId", "market"])
}

fn asset_id_from_payload(payload: &[u8]) -> Option<String> {
    string_from_payload(
        payload,
        &["asset_id", "assetId", "asset", "token_id", "tokenId"],
    )
    .or_else(|| {
        let value = serde_json::from_slice::<serde_json::Value>(payload).ok()?;
        value
            .get("price_changes")
            .or_else(|| value.get("changes"))?
            .as_array()?
            .iter()
            .find_map(|change| {
                ["asset_id", "assetId", "asset", "token_id", "tokenId"]
                    .iter()
                    .find_map(|field| change.get(*field)?.as_str().map(str::to_string))
            })
    })
}

fn string_from_payload(payload: &[u8], fields: &[&str]) -> Option<String> {
    if payload.is_empty() {
        return None;
    }
    let value = serde_json::from_slice::<serde_json::Value>(payload).ok()?;
    fields
        .iter()
        .find_map(|field| value.get(*field)?.as_str().map(str::to_string))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ClobMarketMetadata {
    condition_id: String,
    symbol: Option<String>,
    market_start_ts_ns: i64,
    market_end_ts_ns: i64,
    yes_asset_id: String,
    no_asset_id: String,
}

struct ClobMarketMetadataResolver {
    enabled: bool,
    cache_root: Option<PathBuf>,
    client: Option<reqwest::blocking::Client>,
    by_condition: BTreeMap<String, Option<ClobMarketMetadata>>,
}

impl ClobMarketMetadataResolver {
    fn new(options: &BuildBookCacheOptions) -> Result<Self> {
        if !options.enrich_missing_clob_metadata {
            return Ok(Self {
                enabled: false,
                cache_root: None,
                client: None,
                by_condition: BTreeMap::new(),
            });
        }
        let cache_root = options
            .clob_metadata_cache_root
            .clone()
            .unwrap_or_else(|| options.cache_root.join("clob_market_metadata"));
        fs::create_dir_all(&cache_root)
            .with_context(|| format!("create {}", cache_root.display()))?;
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("pm5m-market-cache-clob-metadata/0.1")
            .build()
            .context("build CLOB metadata HTTP client")?;
        Ok(Self {
            enabled: true,
            cache_root: Some(cache_root),
            client: Some(client),
            by_condition: BTreeMap::new(),
        })
    }

    fn enrich(&mut self, raw: &mut RawPolymarketClobWsEvent) -> Result<()> {
        if !self.enabled || !raw.event_type.eq_ignore_ascii_case("book") {
            return Ok(());
        }
        if raw.condition_id.is_none() {
            raw.condition_id = condition_id_from_payload(&raw.raw_payload);
        }
        if raw.asset_id.is_none() {
            raw.asset_id = asset_id_from_payload(&raw.raw_payload);
        }
        if raw.outcome.is_some()
            && raw.market_start_ts_ns.is_some()
            && raw.market_end_ts_ns.is_some()
            && raw.yes_asset_id.is_some()
            && raw.no_asset_id.is_some()
        {
            return Ok(());
        }
        let Some(condition_id) = raw.condition_id.clone() else {
            return Ok(());
        };
        let Some(metadata) = self.metadata_for_condition(&condition_id)? else {
            return Ok(());
        };
        if raw.symbol.is_none() {
            raw.symbol = metadata.symbol.clone();
        }
        raw.market_start_ts_ns
            .get_or_insert(metadata.market_start_ts_ns);
        raw.market_end_ts_ns
            .get_or_insert(metadata.market_end_ts_ns);
        raw.yes_asset_id
            .get_or_insert_with(|| metadata.yes_asset_id.clone());
        raw.no_asset_id
            .get_or_insert_with(|| metadata.no_asset_id.clone());
        if raw.outcome.is_none() {
            if let Some(asset_id) = raw.asset_id.as_deref() {
                if asset_id == metadata.yes_asset_id {
                    raw.outcome = Some("YES".to_string());
                } else if asset_id == metadata.no_asset_id {
                    raw.outcome = Some("NO".to_string());
                }
            }
        }
        Ok(())
    }

    fn metadata_for_condition(&mut self, condition_id: &str) -> Result<Option<ClobMarketMetadata>> {
        if let Some(metadata) = self.by_condition.get(condition_id) {
            return Ok(metadata.clone());
        }
        let metadata = self.load_or_fetch(condition_id)?;
        self.by_condition
            .insert(condition_id.to_string(), metadata.clone());
        Ok(metadata)
    }

    fn load_or_fetch(&self, condition_id: &str) -> Result<Option<ClobMarketMetadata>> {
        let cache_path = self.cache_path(condition_id);
        if let Some(path) = cache_path.as_ref() {
            if path.exists() {
                let metadata = serde_json::from_reader(
                    fs::File::open(path).with_context(|| format!("open {}", path.display()))?,
                )
                .with_context(|| format!("parse {}", path.display()))?;
                return Ok(Some(metadata));
            }
        }
        let metadata = self.fetch(condition_id)?;
        if let (Some(path), Some(metadata)) = (cache_path.as_ref(), metadata.as_ref()) {
            let bytes = serde_json::to_vec_pretty(metadata)?;
            fs::write(path, bytes).with_context(|| format!("write {}", path.display()))?;
        }
        Ok(metadata)
    }

    fn fetch(&self, condition_id: &str) -> Result<Option<ClobMarketMetadata>> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("missing CLOB metadata HTTP client"))?;
        let url = format!("{POLYMARKET_CLOB_BASE}/markets/{condition_id}");
        let mut last_error = None::<String>;
        for attempt in 0..3 {
            match client.get(&url).send() {
                Ok(response) => {
                    let status = response.status();
                    if status.as_u16() == 404 {
                        return Ok(None);
                    }
                    if status.is_success() {
                        let bytes = response
                            .bytes()
                            .with_context(|| format!("read CLOB metadata for {condition_id}"))?;
                        let payload: Value = serde_json::from_slice(&bytes)
                            .with_context(|| format!("decode CLOB metadata for {condition_id}"))?;
                        return parse_clob_market_metadata(condition_id, &payload).map(Some);
                    }
                    last_error = Some(format!("HTTP status {}", status.as_u16()));
                    if status.as_u16() != 429 && !status.is_server_error() {
                        break;
                    }
                }
                Err(err) => {
                    last_error = Some(err.to_string());
                }
            }
            std::thread::sleep(Duration::from_millis(200 * (attempt + 1)));
        }
        Err(anyhow!(
            "fetch CLOB metadata for {condition_id}: {}",
            last_error.unwrap_or_else(|| "unknown error".to_string())
        ))
    }

    fn cache_path(&self, condition_id: &str) -> Option<PathBuf> {
        let file_name = condition_id
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || ch == 'x' {
                    ch
                } else {
                    '_'
                }
            })
            .collect::<String>();
        self.cache_root
            .as_ref()
            .map(|root| root.join(format!("{file_name}.json")))
    }
}

fn parse_clob_market_metadata(condition_id: &str, payload: &Value) -> Result<ClobMarketMetadata> {
    let payload_condition_id = json_string(payload, &["condition_id", "conditionId"])
        .unwrap_or_else(|| condition_id.to_string());
    let slug = json_string(payload, &["market_slug", "slug"]);
    let start_s = market_start_epoch_s(slug.as_deref(), payload)
        .ok_or_else(|| anyhow!("CLOB metadata missing slug epoch for {condition_id}"))?;
    let duration_s = market_duration_s(slug.as_deref(), payload)
        .ok_or_else(|| anyhow!("CLOB metadata missing slug duration for {condition_id}"))?;
    let tokens = payload
        .get("tokens")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("CLOB metadata missing tokens for {condition_id}"))?;
    let mut yes_asset_id = None::<String>;
    let mut no_asset_id = None::<String>;
    for token in tokens {
        let Some(token_id) = json_string(token, &["token_id", "tokenId"]) else {
            continue;
        };
        match json_string(token, &["outcome"]).and_then(|outcome| normalize_clob_outcome(&outcome))
        {
            Some(BinaryOutcome::Yes) => yes_asset_id = Some(token_id),
            Some(BinaryOutcome::No) => no_asset_id = Some(token_id),
            None => {}
        }
    }
    let yes_asset_id = yes_asset_id
        .ok_or_else(|| anyhow!("CLOB metadata missing YES token for {condition_id}"))?;
    let no_asset_id =
        no_asset_id.ok_or_else(|| anyhow!("CLOB metadata missing NO token for {condition_id}"))?;
    let market_start_ts_ns = start_s
        .checked_mul(1_000_000_000)
        .context("CLOB metadata market start overflow")?;
    let market_end_ts_ns = start_s
        .checked_add(duration_s)
        .and_then(|value| value.checked_mul(1_000_000_000))
        .context("CLOB metadata market end overflow")?;
    Ok(ClobMarketMetadata {
        condition_id: payload_condition_id,
        symbol: slug.as_deref().and_then(symbol_from_slug),
        market_start_ts_ns,
        market_end_ts_ns,
        yes_asset_id,
        no_asset_id,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryOutcome {
    Yes,
    No,
}

fn normalize_clob_outcome(outcome: &str) -> Option<BinaryOutcome> {
    match outcome.trim().to_ascii_uppercase().as_str() {
        "YES" | "UP" => Some(BinaryOutcome::Yes),
        "NO" | "DOWN" => Some(BinaryOutcome::No),
        _ => None,
    }
}

fn json_string(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| value.get(*field)?.as_str().map(str::to_string))
}

fn slug_start_epoch_s(slug: &str) -> Option<i64> {
    slug.rsplit('-').next()?.parse::<i64>().ok()
}

fn market_start_epoch_s(slug: Option<&str>, payload: &Value) -> Option<i64> {
    slug.and_then(slug_start_epoch_s).or_else(|| {
        let question = json_string(payload, &["question"])?;
        question_start_epoch_s_et(
            &question,
            slug.and_then(|slug| year_from_slug(slug)).unwrap_or(2026),
        )
    })
}

fn market_duration_s(slug: Option<&str>, payload: &Value) -> Option<i64> {
    slug.and_then(slug_duration_s)
        .or_else(|| tags_duration_s(payload))
}

fn slug_duration_s(slug: &str) -> Option<i64> {
    let lower = slug.to_ascii_lowercase();
    if lower.contains("15m") {
        Some(900)
    } else if lower.contains("hourly") || lower.contains("1h") {
        Some(3_600)
    } else if lower.contains("5m") {
        Some(300)
    } else {
        None
    }
}

fn tags_duration_s(payload: &Value) -> Option<i64> {
    let tags = payload.get("tags")?.as_array()?;
    for tag in tags {
        let value = tag.as_str()?.trim().to_ascii_uppercase();
        match value.as_str() {
            "5M" => return Some(300),
            "15M" => return Some(900),
            "1H" | "HOURLY" => return Some(3_600),
            _ => {}
        }
    }
    None
}

fn symbol_from_slug(slug: &str) -> Option<String> {
    let lower = slug.to_ascii_lowercase();
    let prefix = lower.split("-updown").next()?.split("-up-or-down").next()?;
    let asset = match prefix {
        "bitcoin" | "btc" => "BTC",
        "ethereum" | "eth" => "ETH",
        "solana" | "sol" => "SOL",
        "xrp" => "XRP",
        "bnb" => "BNB",
        "doge" | "dogecoin" => "DOGE",
        "hype" | "hyperliquid" => "HYPE",
        other if other.chars().all(|ch| ch.is_ascii_alphanumeric()) => {
            return Some(format!(
                "{}-{}",
                other.to_ascii_uppercase(),
                slug_horizon(slug)?
            ));
        }
        _ => return None,
    };
    Some(format!("{asset}-{}", slug_horizon(slug)?))
}

fn slug_horizon(slug: &str) -> Option<&'static str> {
    match slug_duration_s(slug)? {
        300 => Some("5M"),
        900 => Some("15M"),
        3_600 => Some("1H"),
        _ => None,
    }
}

fn year_from_slug(slug: &str) -> Option<i32> {
    slug.split('-').find_map(|part| {
        let year = part.parse::<i32>().ok()?;
        (2000..=2100).contains(&year).then_some(year)
    })
}

fn question_start_epoch_s_et(question: &str, year: i32) -> Option<i64> {
    let lower = question.to_ascii_lowercase();
    let (_, after_dash) = lower.rsplit_once(" - ")?;
    let mut parts = after_dash.split_whitespace();
    let month = parts.find_map(month_from_text)?;
    let day_text = parts.next()?.trim_end_matches(',');
    let day = day_text.parse::<u32>().ok()?;
    let time_text = parts.next()?.trim_end_matches(',');
    let (hour, minute) = parse_et_time(time_text)?;
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    let local = date.and_hms_opt(hour, minute, 0)?;
    let offset_seconds = if (3..=11).contains(&month) {
        -4 * 3_600
    } else {
        -5 * 3_600
    };
    FixedOffset::east_opt(offset_seconds)?
        .from_local_datetime(&local)
        .single()
        .map(|dt| dt.timestamp())
}

fn month_from_text(text: &str) -> Option<u32> {
    let cleaned = text
        .trim_matches(|ch: char| !ch.is_ascii_alphabetic())
        .to_ascii_lowercase();
    match cleaned.as_str() {
        "jan" | "january" => Some(1),
        "feb" | "february" => Some(2),
        "mar" | "march" => Some(3),
        "apr" | "april" => Some(4),
        "may" => Some(5),
        "jun" | "june" => Some(6),
        "jul" | "july" => Some(7),
        "aug" | "august" => Some(8),
        "sep" | "sept" | "september" => Some(9),
        "oct" | "october" => Some(10),
        "nov" | "november" => Some(11),
        "dec" | "december" => Some(12),
        _ => None,
    }
}

fn parse_et_time(text: &str) -> Option<(u32, u32)> {
    let start = text.split('-').next()?.trim();
    let is_pm = start.ends_with("pm");
    let is_am = start.ends_with("am");
    let time = start.trim_end_matches("am").trim_end_matches("pm");
    let (hour_text, minute_text) = time.split_once(':').unwrap_or((time, "0"));
    let mut hour = hour_text.parse::<u32>().ok()?;
    let minute = minute_text.parse::<u32>().ok()?;
    if is_pm && hour != 12 {
        hour += 12;
    }
    if is_am && hour == 12 {
        hour = 0;
    }
    (hour < 24 && minute < 60).then_some((hour, minute))
}

fn condition_shard(condition_id: &str, replay_workers: usize) -> usize {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in condition_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as usize) % replay_workers.max(1)
}

fn buffer_book_cache_row(
    row: BookCacheRow,
    cache_root: &Path,
    row_buffer: &mut Vec<BookCacheRow>,
    current_hour: &mut Option<i64>,
    partition_batches: &mut usize,
    partitions: &mut Vec<BookCache2Partition>,
    partition_prefix_scope: &str,
) -> Result<()> {
    let row_hour = row.local_recv_ts_ns.div_euclid(HOUR_NS);
    match *current_hour {
        Some(hour) if hour != row_hour => {
            flush_book_cache_rows(
                cache_root,
                row_buffer,
                current_hour,
                partition_batches,
                partitions,
                partition_prefix_scope,
            )?;
            *current_hour = Some(row_hour);
        }
        None => *current_hour = Some(row_hour),
        _ => {}
    }
    row_buffer.push(row);
    if row_buffer.len() >= MAX_BUFFERED_BOOK_ROWS {
        flush_book_cache_rows(
            cache_root,
            row_buffer,
            current_hour,
            partition_batches,
            partitions,
            partition_prefix_scope,
        )?;
    }
    Ok(())
}

fn flush_book_cache_rows(
    cache_root: &Path,
    row_buffer: &mut Vec<BookCacheRow>,
    current_hour: &mut Option<i64>,
    partition_batches: &mut usize,
    partitions: &mut Vec<BookCache2Partition>,
    partition_prefix_scope: &str,
) -> Result<()> {
    if row_buffer.is_empty() {
        return Ok(());
    }
    let partition_prefix = format!("{partition_prefix_scope}_part_{:05}", *partition_batches);
    *partition_batches += 1;
    let rows = std::mem::take(row_buffer);
    partitions.extend(write_book_cache2_partition_batch(
        cache_root,
        &partition_prefix,
        rows,
    )?);
    *current_hour = None;
    Ok(())
}

fn validate_options(options: &BuildBookCacheOptions) -> Result<()> {
    if options.raw_roots.is_empty() {
        bail!("build-book-cache requires at least one raw root");
    }
    if let (Some(start), Some(end)) = (options.raw_start_ts_ns, options.raw_end_ts_ns) {
        if end <= start {
            bail!("raw_end_ts_ns must be greater than raw_start_ts_ns");
        }
    }
    if options.poly_incremental_latency_ms < 0 {
        bail!("poly_incremental_latency_ms must be non-negative");
    }
    if options.poly_incremental_freshness_guard_ms < 0 {
        bail!("poly_incremental_freshness_guard_ms must be non-negative");
    }
    Ok(())
}

fn default_poly_incremental_latency_ms() -> i64 {
    DEFAULT_POLY_INCREMENTAL_LATENCY_MS
}

fn default_poly_incremental_freshness_guard_ms() -> i64 {
    DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS
}

#[derive(Debug, Clone)]
struct RawCandidateManifest {
    segment_path: PathBuf,
    min_ts_ns: Option<i64>,
    max_ts_ns: Option<i64>,
    manifest_path: PathBuf,
}

fn sorted_candidate_manifests(
    options: &BuildBookCacheOptions,
) -> Result<Vec<RawCandidateManifest>> {
    let mut out = Vec::new();
    for raw_root in &options.raw_roots {
        for manifest_path in candidate_raw_manifests(raw_root, options)? {
            if let Some(manifest) = read_candidate_manifest_metadata(&manifest_path)? {
                out.push(manifest);
            }
        }
    }
    out.sort_by(|a, b| {
        (
            a.min_ts_ns.unwrap_or(i64::MIN),
            a.max_ts_ns.unwrap_or(i64::MIN),
            &a.manifest_path,
        )
            .cmp(&(
                b.min_ts_ns.unwrap_or(i64::MIN),
                b.max_ts_ns.unwrap_or(i64::MIN),
                &b.manifest_path,
            ))
    });
    Ok(out)
}

fn candidate_raw_manifests(
    raw_root: &Path,
    options: &BuildBookCacheOptions,
) -> Result<Vec<PathBuf>> {
    if options.raw_start_ts_ns.is_none() || options.raw_end_ts_ns.is_none() {
        return discover_hftrec4_manifests(raw_root);
    }

    let start_ts_ns = options.raw_start_ts_ns.expect("checked is_some");
    let end_ts_ns = options.raw_end_ts_ns.expect("checked is_some");
    let first_bucket = start_ts_ns.div_euclid(HOUR_NS);
    let last_bucket = (end_ts_ns - 1).div_euclid(HOUR_NS);
    let mut out = Vec::new();
    {
        let stream_root =
            if raw_root.file_name().and_then(|name| name.to_str()) == Some(WS_RAW_STREAM) {
                raw_root.to_path_buf()
            } else {
                raw_root.join(WS_RAW_STREAM)
            };
        for bucket in first_bucket..=last_bucket {
            let dir = stream_root.join(format!("hour_bucket={bucket}"));
            if !dir.exists() {
                continue;
            }
            let mut entries = fs::read_dir(&dir)
                .with_context(|| format!("read {}", dir.display()))?
                .collect::<std::io::Result<Vec<_>>>()
                .with_context(|| format!("list {}", dir.display()))?;
            entries.sort_by_key(|entry| entry.path());
            for entry in entries {
                let path = entry.path();
                if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".manifest.json"))
                {
                    out.push(path);
                }
            }
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

fn read_candidate_manifest_metadata(path: &Path) -> Result<Option<RawCandidateManifest>> {
    let value: serde_json::Value = serde_json::from_reader(
        fs::File::open(path).with_context(|| format!("open raw manifest {}", path.display()))?,
    )
    .with_context(|| format!("parse raw manifest {}", path.display()))?;
    let format = value
        .get("dataset_format")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let path_text = path.to_string_lossy();
    match format {
        HFTREC4_FORMAT => {
            if !path_text.contains(WS_RAW_STREAM) {
                return Ok(None);
            }
            let manifest: Hftrec4SegmentManifest = serde_json::from_value(value)?;
            Ok(Some(RawCandidateManifest {
                segment_path: manifest.segment_path,
                min_ts_ns: manifest.min_ts_ns,
                max_ts_ns: manifest.max_ts_ns,
                manifest_path: path.to_path_buf(),
            }))
        }
        _ => Ok(None),
    }
}

fn manifest_overlaps(manifest: &RawCandidateManifest, options: &BuildBookCacheOptions) -> bool {
    let start = options.raw_start_ts_ns.unwrap_or(i64::MIN);
    let end = options.raw_end_ts_ns.unwrap_or(i64::MAX);
    let min_ts = manifest.min_ts_ns.unwrap_or(i64::MIN);
    let max_ts = manifest.max_ts_ns.unwrap_or(i64::MAX);
    min_ts < end && max_ts >= start
}

fn ts_in_window(ts_ns: i64, options: &BuildBookCacheOptions) -> bool {
    options.raw_start_ts_ns.is_none_or(|start| ts_ns >= start)
        && options.raw_end_ts_ns.is_none_or(|end| ts_ns < end)
}

fn ts_before_end(ts_ns: i64, options: &BuildBookCacheOptions) -> bool {
    options.raw_end_ts_ns.is_none_or(|end| ts_ns < end)
}

#[derive(Debug, Clone, Default)]
struct SymbolFilter {
    allowed: BTreeSet<String>,
}

impl SymbolFilter {
    fn new(values: &[String]) -> Result<Self> {
        let mut allowed = BTreeSet::new();
        for value in values {
            let symbol = canonical_market_symbol(value).ok_or_else(|| {
                anyhow::anyhow!("invalid market symbol allowlist entry '{value}'")
            })?;
            allowed.insert(symbol);
        }
        Ok(Self { allowed })
    }

    fn allows(&self, symbol: &str) -> bool {
        self.allowed.is_empty()
            || canonical_market_symbol(symbol)
                .as_ref()
                .is_some_and(|symbol| self.allowed.contains(symbol))
    }

    fn is_restrictive(&self) -> bool {
        !self.allowed.is_empty()
    }
}
