use crate::types::{
    outcome_code, BookCacheRow, BookLevelMicros, BOOK_CACHE2_CATALOG, BOOK_CACHE2_FORMAT,
    DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS, DEFAULT_POLY_INCREMENTAL_LATENCY_MS,
    HFTBOOK2_SCHEMA_HASH,
};
use anyhow::{anyhow, bail, Context, Result};
use crc32fast::Hasher as Crc32;
use market_data_etl_core::{atomic_write_verified, fsync_dir, now_unix_ns, sha256_file};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

const HOUR_NS: i64 = 3_600_000_000_000;
const HFTBOOK2_MAGIC: &[u8; 8] = b"HFTBOOK2";
const NONE_I64: i64 = i64::MIN;
const SHARD_COUNT: u16 = 64;
const LEGACY_HFTBOOK2_SCHEMA_HASH: &str = "hftbook2.partition-local-dict.fixed-top10-row.v1";
const ENCODED_BOOK2_ROW_BYTES: usize =
    8 + 8 + 8 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 1 + 8 + 8 + 8 + 8 + 8 * 40;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteBookCache2Options {
    pub cache_root: PathBuf,
    pub raw_roots: Vec<PathBuf>,
    pub raw_start_ts_ns: Option<i64>,
    pub raw_end_ts_ns: Option<i64>,
    pub market_symbol_allowlist: Vec<String>,
    pub overwrite: bool,
    pub poly_server_visible_time: bool,
    pub poly_incremental_latency_ms: i64,
    pub poly_incremental_freshness_guard_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendBookCache2PartitionOptions {
    pub cache_root: PathBuf,
    pub partition_id: String,
    pub raw_roots: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookCache2Catalog {
    pub schema_version: u32,
    pub dataset_format: String,
    pub schema_hash: String,
    pub cache_root: PathBuf,
    pub raw_roots: Vec<PathBuf>,
    pub raw_start_ts_ns: Option<i64>,
    pub raw_end_ts_ns: Option<i64>,
    pub market_symbol_allowlist: Vec<String>,
    #[serde(default)]
    pub poly_server_visible_time: bool,
    #[serde(default = "default_poly_incremental_latency_ms")]
    pub poly_incremental_latency_ms: i64,
    #[serde(default = "default_poly_incremental_freshness_guard_ms")]
    pub poly_incremental_freshness_guard_ms: i64,
    pub generated_ts_ns: i64,
    pub row_count: usize,
    pub partitions: Vec<BookCache2Partition>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookCache2Partition {
    pub partition_id: String,
    pub path: String,
    pub hour_bucket: i64,
    pub asset_shard: u16,
    pub row_count: usize,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
    pub sha256: String,
    pub schema_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookCache2ValidationReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub cache_root: PathBuf,
    pub row_count: usize,
    pub partition_count: usize,
    pub catalog_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookCache2CoverageReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub cache_root: PathBuf,
    pub row_count: usize,
    pub partition_count: usize,
    pub min_ts_ns: Option<i64>,
    pub max_ts_ns: Option<i64>,
    pub hour_buckets: Vec<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BookCache2BenchReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub cache_root: PathBuf,
    pub start_ts_ns: Option<i64>,
    pub end_ts_ns: Option<i64>,
    pub rows_read: usize,
    pub partitions_scanned: usize,
    pub elapsed_ms: u128,
    pub rows_per_sec: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct PartitionDictionary {
    symbols: Vec<String>,
    conditions: Vec<String>,
    assets: Vec<String>,
    raw_hashes: Vec<String>,
    raw_payload_hashes: Vec<String>,
    book_state_hashes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PartitionHeader {
    schema_version: u32,
    dataset_format: String,
    schema_hash: String,
    partition_id: String,
    hour_bucket: i64,
    asset_shard: u16,
    row_count: usize,
    dictionary: PartitionDictionary,
}

struct FastPartitionPayload {
    header: PartitionHeader,
    bytes: Vec<u8>,
    row_start: usize,
    row_end: usize,
}

struct OrderedPartitionCursor {
    payload: FastPartitionPayload,
    cursor: usize,
    current: Option<BookCacheRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderedRowKey {
    local_recv_ts_ns: i64,
    ingest_seq: u64,
    condition_id: String,
    asset_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderedHeapEntry {
    key: OrderedRowKey,
    cursor_idx: usize,
}

#[derive(Debug, Clone, Copy)]
struct EncodedBook2Row {
    local_recv_ts_ns: i64,
    ingest_seq: u64,
    exchange_ts_ms: i64,
    symbol_key: u32,
    condition_key: u32,
    asset_key: u32,
    yes_asset_key: u32,
    no_asset_key: u32,
    raw_hash_key: u32,
    raw_payload_hash_key: u32,
    book_state_hash_key: u32,
    outcome: u8,
    window_start_ts_ns: i64,
    window_end_ts_ns: i64,
    best_bid_price_micros: i64,
    best_ask_price_micros: i64,
    bid_price: [i64; 10],
    ask_price: [i64; 10],
    bid_size: [i64; 10],
    ask_size: [i64; 10],
}

#[derive(Debug, Clone, Copy)]
pub struct BookCache2RowView {
    pub local_recv_ts_ns: i64,
    pub ingest_seq: u64,
    pub exchange_ts_ms: i64,
    pub symbol_key: u32,
    pub condition_key: u32,
    pub asset_key: u32,
    pub yes_asset_key: u32,
    pub no_asset_key: u32,
    pub raw_hash_key: u32,
    pub raw_payload_hash_key: u32,
    pub book_state_hash_key: u32,
    pub outcome: u8,
    pub window_start_ts_ns: i64,
    pub window_end_ts_ns: i64,
    pub best_bid_price_micros: i64,
    pub best_ask_price_micros: i64,
    pub bid_price: [i64; 10],
    pub ask_price: [i64; 10],
    pub bid_size: [i64; 10],
    pub ask_size: [i64; 10],
}

#[derive(Debug, Clone, Default)]
pub struct BookCache2ScanFilter {
    pub symbol: Option<String>,
    pub condition_id: Option<String>,
}

#[derive(Default)]
struct LocalDictBuilder {
    dictionary: PartitionDictionary,
    symbols: BTreeMap<String, u32>,
    conditions: BTreeMap<String, u32>,
    assets: BTreeMap<String, u32>,
    raw_hashes: BTreeMap<String, u32>,
    raw_payload_hashes: BTreeMap<String, u32>,
    book_state_hashes: BTreeMap<String, u32>,
}

pub fn write_book_cache2(
    options: &WriteBookCache2Options,
    rows: Vec<BookCacheRow>,
) -> Result<BookCache2Catalog> {
    if rows.is_empty() {
        bail!("cannot write empty HFTBOOK2 cache");
    }
    if let (Some(start), Some(end)) = (options.raw_start_ts_ns, options.raw_end_ts_ns) {
        if end <= start {
            bail!("raw_end_ts_ns must be greater than raw_start_ts_ns");
        }
    }
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

    let mut partitions = Vec::new();
    for ((hour_bucket, shard), mut group) in group_rows(rows) {
        group.sort_by(row_order);
        let partition_id = format!("hour_{hour_bucket}_shard_{shard:02}_part_00000");
        let rel_path =
            format!("book/hour_bucket={hour_bucket}/asset_shard={shard:02}/part-00000.hfb2");
        let partition = write_partition(
            &options.cache_root.join(&rel_path),
            &partition_id,
            hour_bucket,
            shard,
            &group,
        )?;
        partitions.push(BookCache2Partition {
            path: rel_path,
            ..partition
        });
    }
    partitions.sort_by(|a, b| {
        (a.hour_bucket, a.asset_shard, &a.partition_id).cmp(&(
            b.hour_bucket,
            b.asset_shard,
            &b.partition_id,
        ))
    });
    let row_count = partitions.iter().map(|partition| partition.row_count).sum();
    let catalog = BookCache2Catalog {
        schema_version: 1,
        dataset_format: BOOK_CACHE2_FORMAT.to_string(),
        schema_hash: HFTBOOK2_SCHEMA_HASH.to_string(),
        cache_root: options.cache_root.clone(),
        raw_roots: options.raw_roots.clone(),
        raw_start_ts_ns: options.raw_start_ts_ns,
        raw_end_ts_ns: options.raw_end_ts_ns,
        market_symbol_allowlist: options.market_symbol_allowlist.clone(),
        poly_server_visible_time: options.poly_server_visible_time,
        poly_incremental_latency_ms: options.poly_incremental_latency_ms,
        poly_incremental_freshness_guard_ms: options.poly_incremental_freshness_guard_ms,
        generated_ts_ns: now_unix_ns() as i64,
        row_count,
        partitions,
    };
    write_catalog(&options.cache_root, &catalog)?;
    Ok(catalog)
}

pub fn append_book_cache2_partition(
    options: &AppendBookCache2PartitionOptions,
    rows: Vec<BookCacheRow>,
) -> Result<BookCache2ValidationReport> {
    if rows.is_empty() {
        bail!("cannot append empty HFTBOOK2 partition");
    }
    fs::create_dir_all(&options.cache_root)
        .with_context(|| format!("create {}", options.cache_root.display()))?;
    let mut catalog = if options.cache_root.join(BOOK_CACHE2_CATALOG).exists() {
        read_book_cache2_catalog(&options.cache_root)?
    } else {
        BookCache2Catalog {
            schema_version: 1,
            dataset_format: BOOK_CACHE2_FORMAT.to_string(),
            schema_hash: HFTBOOK2_SCHEMA_HASH.to_string(),
            cache_root: options.cache_root.clone(),
            raw_roots: Vec::new(),
            raw_start_ts_ns: None,
            raw_end_ts_ns: None,
            market_symbol_allowlist: Vec::new(),
            poly_server_visible_time: false,
            poly_incremental_latency_ms: DEFAULT_POLY_INCREMENTAL_LATENCY_MS,
            poly_incremental_freshness_guard_ms: DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS,
            generated_ts_ns: now_unix_ns() as i64,
            row_count: 0,
            partitions: Vec::new(),
        }
    };

    let mut appended_rows = 0usize;
    for ((hour_bucket, shard), mut group) in group_rows(rows) {
        group.sort_by(row_order);
        let suffix = format!(
            "{}_hour_{hour_bucket}_shard_{shard:02}",
            options.partition_id
        );
        let rel_path =
            format!("book/hour_bucket={hour_bucket}/asset_shard={shard:02}/part-{suffix}.hfb2");
        if catalog
            .partitions
            .iter()
            .any(|partition| partition.path == rel_path || partition.partition_id == suffix)
        {
            bail!("HFTBOOK2 partition already exists: {suffix}");
        }
        let partition = write_partition(
            &options.cache_root.join(&rel_path),
            &suffix,
            hour_bucket,
            shard,
            &group,
        )?;
        appended_rows += partition.row_count;
        catalog.partitions.push(BookCache2Partition {
            path: rel_path,
            ..partition
        });
    }
    catalog.raw_roots = merge_raw_roots(catalog.raw_roots, &options.raw_roots);
    catalog.row_count = catalog
        .row_count
        .checked_add(appended_rows)
        .context("HFTBOOK2 row count overflow")?;
    catalog.generated_ts_ns = now_unix_ns() as i64;
    catalog.partitions.sort_by(|a, b| {
        (a.hour_bucket, a.asset_shard, a.min_ts_ns, &a.partition_id).cmp(&(
            b.hour_bucket,
            b.asset_shard,
            b.min_ts_ns,
            &b.partition_id,
        ))
    });
    write_catalog(&options.cache_root, &catalog)?;
    validate_book_cache2(&options.cache_root)
}

pub(crate) fn write_book_cache2_partition_batch(
    cache_root: &Path,
    partition_prefix: &str,
    rows: Vec<BookCacheRow>,
) -> Result<Vec<BookCache2Partition>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut partitions = Vec::new();
    for ((hour_bucket, shard), mut group) in group_rows(rows) {
        group.sort_by(row_order);
        let partition_id = format!("{partition_prefix}_hour_{hour_bucket}_shard_{shard:02}");
        let rel_path = format!(
            "book/hour_bucket={hour_bucket}/asset_shard={shard:02}/part-{partition_id}.hfb2"
        );
        let partition = write_partition(
            &cache_root.join(&rel_path),
            &partition_id,
            hour_bucket,
            shard,
            &group,
        )?;
        partitions.push(BookCache2Partition {
            path: rel_path,
            ..partition
        });
    }
    partitions.sort_by(|a, b| {
        (a.hour_bucket, a.asset_shard, a.min_ts_ns, &a.partition_id).cmp(&(
            b.hour_bucket,
            b.asset_shard,
            b.min_ts_ns,
            &b.partition_id,
        ))
    });
    Ok(partitions)
}

pub(crate) fn write_book_cache2_catalog(
    options: &WriteBookCache2Options,
    mut partitions: Vec<BookCache2Partition>,
) -> Result<BookCache2Catalog> {
    partitions.sort_by(|a, b| {
        (a.hour_bucket, a.asset_shard, a.min_ts_ns, &a.partition_id).cmp(&(
            b.hour_bucket,
            b.asset_shard,
            b.min_ts_ns,
            &b.partition_id,
        ))
    });
    let row_count = partitions.iter().map(|partition| partition.row_count).sum();
    let catalog = BookCache2Catalog {
        schema_version: 1,
        dataset_format: BOOK_CACHE2_FORMAT.to_string(),
        schema_hash: HFTBOOK2_SCHEMA_HASH.to_string(),
        cache_root: options.cache_root.clone(),
        raw_roots: options.raw_roots.clone(),
        raw_start_ts_ns: options.raw_start_ts_ns,
        raw_end_ts_ns: options.raw_end_ts_ns,
        market_symbol_allowlist: options.market_symbol_allowlist.clone(),
        poly_server_visible_time: options.poly_server_visible_time,
        poly_incremental_latency_ms: options.poly_incremental_latency_ms,
        poly_incremental_freshness_guard_ms: options.poly_incremental_freshness_guard_ms,
        generated_ts_ns: now_unix_ns() as i64,
        row_count,
        partitions,
    };
    write_catalog(&options.cache_root, &catalog)?;
    Ok(catalog)
}

pub fn read_book_cache2_catalog(cache_root: &Path) -> Result<BookCache2Catalog> {
    let path = cache_root.join(BOOK_CACHE2_CATALOG);
    let catalog: BookCache2Catalog = serde_json::from_reader(
        fs::File::open(&path).with_context(|| format!("open {}", path.display()))?,
    )
    .with_context(|| format!("parse {}", path.display()))?;
    if catalog.dataset_format != BOOK_CACHE2_FORMAT {
        bail!("unsupported book cache format {}", catalog.dataset_format);
    }
    if !supported_hftbook2_schema_hash(&catalog.schema_hash) {
        bail!("unsupported HFTBOOK2 schema hash {}", catalog.schema_hash);
    }
    Ok(catalog)
}

fn supported_hftbook2_schema_hash(schema_hash: &str) -> bool {
    schema_hash == HFTBOOK2_SCHEMA_HASH || schema_hash == LEGACY_HFTBOOK2_SCHEMA_HASH
}

pub fn validate_book_cache2(cache_root: &Path) -> Result<BookCache2ValidationReport> {
    let catalog = read_book_cache2_catalog(cache_root)?;
    let mut row_count = 0usize;
    for partition in &catalog.partitions {
        validate_partition(cache_root, partition)?;
        row_count = row_count
            .checked_add(partition.row_count)
            .context("HFTBOOK2 row count overflow")?;
    }
    if row_count != catalog.row_count {
        bail!("HFTBOOK2 catalog row count mismatch");
    }
    Ok(BookCache2ValidationReport {
        schema_version: 1,
        dataset_format: BOOK_CACHE2_FORMAT.to_string(),
        cache_root: cache_root.to_path_buf(),
        row_count,
        partition_count: catalog.partitions.len(),
        catalog_hash: sha256_file(&cache_root.join(BOOK_CACHE2_CATALOG))?,
    })
}

pub fn scan_book_cache2<F>(
    cache_root: &Path,
    start_ts_ns: Option<i64>,
    end_ts_ns: Option<i64>,
    filter: &BookCache2ScanFilter,
    mut visit: F,
) -> Result<usize>
where
    F: FnMut(BookCacheRow) -> Result<()>,
{
    let catalog = read_book_cache2_catalog(cache_root)?;
    let mut count = 0usize;
    for partition in catalog
        .partitions
        .iter()
        .filter(|partition| partition_overlaps(partition, start_ts_ns, end_ts_ns))
    {
        for row in read_partition(cache_root, partition)? {
            if !ts_in_window(row.local_recv_ts_ns, start_ts_ns, end_ts_ns) {
                continue;
            }
            if filter
                .symbol
                .as_ref()
                .is_some_and(|symbol| symbol != &row.symbol)
            {
                continue;
            }
            if filter
                .condition_id
                .as_ref()
                .is_some_and(|condition_id| condition_id != &row.condition_id)
            {
                continue;
            }
            visit(row)?;
            count += 1;
        }
    }
    Ok(count)
}

pub fn scan_book_cache2_views<F>(
    cache_root: &Path,
    start_ts_ns: Option<i64>,
    end_ts_ns: Option<i64>,
    filter: &BookCache2ScanFilter,
    mut visit: F,
) -> Result<usize>
where
    F: FnMut(BookCache2RowView) -> Result<()>,
{
    let catalog = read_book_cache2_catalog(cache_root)?;
    let mut count = 0usize;
    for partition in catalog
        .partitions
        .iter()
        .filter(|partition| partition_overlaps(partition, start_ts_ns, end_ts_ns))
    {
        if partition.schema_hash != HFTBOOK2_SCHEMA_HASH {
            bail!("HFTBOOK2 partition schema hash mismatch");
        }
        let payload = read_partition_payload_fast(&cache_root.join(&partition.path))?;
        if payload.header.row_count != partition.row_count {
            bail!("HFTBOOK2 partition row count mismatch");
        }
        let symbol_key_filter = filter
            .symbol
            .as_ref()
            .map(|symbol| dict_key(&payload.header.dictionary.symbols, symbol))
            .transpose()?;
        let condition_key_filter = filter
            .condition_id
            .as_ref()
            .map(|condition_id| dict_key(&payload.header.dictionary.conditions, condition_id))
            .transpose()?;
        if symbol_key_filter == Some(None) || condition_key_filter == Some(None) {
            continue;
        }
        let symbol_key_filter = symbol_key_filter.flatten();
        let condition_key_filter = condition_key_filter.flatten();
        let mut cursor = 0usize;
        let row_bytes = &payload.bytes[payload.row_start..payload.row_end];
        while cursor < row_bytes.len() {
            let row = decode_row_view(row_bytes, &mut cursor)?;
            if !ts_in_window(row.local_recv_ts_ns, start_ts_ns, end_ts_ns) {
                continue;
            }
            if symbol_key_filter.is_some_and(|key| key != row.symbol_key) {
                continue;
            }
            if condition_key_filter.is_some_and(|key| key != row.condition_key) {
                continue;
            }
            visit(row)?;
            count += 1;
        }
    }
    Ok(count)
}

pub fn scan_book_cache2_rows_fast<F>(
    cache_root: &Path,
    start_ts_ns: Option<i64>,
    end_ts_ns: Option<i64>,
    filter: &BookCache2ScanFilter,
    mut visit: F,
) -> Result<usize>
where
    F: FnMut(BookCacheRow) -> Result<()>,
{
    let catalog = read_book_cache2_catalog(cache_root)?;
    let mut count = 0usize;
    for partition in catalog
        .partitions
        .iter()
        .filter(|partition| partition_overlaps(partition, start_ts_ns, end_ts_ns))
    {
        if partition.schema_hash != HFTBOOK2_SCHEMA_HASH {
            bail!("HFTBOOK2 partition schema hash mismatch");
        }
        let payload = read_partition_payload_fast(&cache_root.join(&partition.path))?;
        if payload.header.row_count != partition.row_count {
            bail!("HFTBOOK2 partition row count mismatch");
        }
        let symbol_key_filter = filter
            .symbol
            .as_ref()
            .map(|symbol| dict_key(&payload.header.dictionary.symbols, symbol))
            .transpose()?;
        let condition_key_filter = filter
            .condition_id
            .as_ref()
            .map(|condition_id| dict_key(&payload.header.dictionary.conditions, condition_id))
            .transpose()?;
        if symbol_key_filter == Some(None) || condition_key_filter == Some(None) {
            continue;
        }
        let symbol_key_filter = symbol_key_filter.flatten();
        let condition_key_filter = condition_key_filter.flatten();
        let mut cursor = 0usize;
        let row_bytes = &payload.bytes[payload.row_start..payload.row_end];
        while cursor < row_bytes.len() {
            let row = decode_row_view(row_bytes, &mut cursor)?;
            if !ts_in_window(row.local_recv_ts_ns, start_ts_ns, end_ts_ns) {
                continue;
            }
            if symbol_key_filter.is_some_and(|key| key != row.symbol_key) {
                continue;
            }
            if condition_key_filter.is_some_and(|key| key != row.condition_key) {
                continue;
            }
            visit(decode_row_view_to_cache_row(
                row,
                &payload.header.dictionary,
            )?)?;
            count += 1;
        }
    }
    Ok(count)
}

pub fn stream_book_cache2_rows_ordered<F>(
    cache_root: &Path,
    start_ts_ns: Option<i64>,
    end_ts_ns: Option<i64>,
    filter: &BookCache2ScanFilter,
    mut visit: F,
) -> Result<usize>
where
    F: FnMut(BookCacheRow) -> Result<()>,
{
    let catalog = read_book_cache2_catalog(cache_root)?;
    let mut partitions_by_hour = BTreeMap::<i64, Vec<&BookCache2Partition>>::new();
    for partition in catalog
        .partitions
        .iter()
        .filter(|partition| partition_overlaps(partition, start_ts_ns, end_ts_ns))
    {
        partitions_by_hour
            .entry(partition.hour_bucket)
            .or_default()
            .push(partition);
    }

    let mut count = 0usize;
    for (_, partitions) in partitions_by_hour {
        let mut cursors = Vec::<OrderedPartitionCursor>::new();
        for partition in partitions {
            if partition.schema_hash != HFTBOOK2_SCHEMA_HASH {
                bail!("HFTBOOK2 partition schema hash mismatch");
            }
            let payload = read_partition_payload_fast(&cache_root.join(&partition.path))?;
            if payload.header.row_count != partition.row_count {
                bail!("HFTBOOK2 partition row count mismatch");
            }
            let mut cursor = OrderedPartitionCursor {
                payload,
                cursor: 0,
                current: None,
            };
            cursor.advance(start_ts_ns, end_ts_ns, filter)?;
            if cursor.current.is_some() {
                cursors.push(cursor);
            }
        }

        let mut heap = std::collections::BinaryHeap::<OrderedHeapEntry>::new();
        for (cursor_idx, cursor) in cursors.iter().enumerate() {
            if let Some(row) = cursor.current.as_ref() {
                heap.push(OrderedHeapEntry {
                    key: OrderedRowKey::from(row),
                    cursor_idx,
                });
            }
        }
        while let Some(entry) = heap.pop() {
            let row = cursors[entry.cursor_idx]
                .current
                .take()
                .ok_or_else(|| anyhow!("missing HFTBOOK2 cursor row"))?;
            visit(row)?;
            count += 1;
            cursors[entry.cursor_idx].advance(start_ts_ns, end_ts_ns, filter)?;
            if let Some(next) = cursors[entry.cursor_idx].current.as_ref() {
                heap.push(OrderedHeapEntry {
                    key: OrderedRowKey::from(next),
                    cursor_idx: entry.cursor_idx,
                });
            }
        }
    }
    Ok(count)
}

pub fn read_book_cache2_rows(
    cache_root: &Path,
    start_ts_ns: Option<i64>,
    end_ts_ns: Option<i64>,
) -> Result<Vec<BookCacheRow>> {
    let mut rows = Vec::new();
    scan_book_cache2(
        cache_root,
        start_ts_ns,
        end_ts_ns,
        &BookCache2ScanFilter::default(),
        |row| {
            rows.push(row);
            Ok(())
        },
    )?;
    rows.sort_by(row_order);
    Ok(rows)
}

pub fn bench_book_cache2(
    cache_root: &Path,
    start_ts_ns: Option<i64>,
    end_ts_ns: Option<i64>,
) -> Result<BookCache2BenchReport> {
    let catalog = read_book_cache2_catalog(cache_root)?;
    let partitions_scanned = catalog
        .partitions
        .iter()
        .filter(|partition| partition_overlaps(partition, start_ts_ns, end_ts_ns))
        .count();
    let started = Instant::now();
    let rows_read = scan_book_cache2_views(
        cache_root,
        start_ts_ns,
        end_ts_ns,
        &BookCache2ScanFilter::default(),
        |_| Ok(()),
    )?;
    let elapsed = started.elapsed();
    Ok(BookCache2BenchReport {
        schema_version: 1,
        dataset_format: BOOK_CACHE2_FORMAT.to_string(),
        cache_root: cache_root.to_path_buf(),
        start_ts_ns,
        end_ts_ns,
        rows_read,
        partitions_scanned,
        elapsed_ms: elapsed.as_millis(),
        rows_per_sec: if elapsed.as_secs_f64() > 0.0 {
            rows_read as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        },
    })
}

pub fn inspect_book_cache2_coverage(cache_root: &Path) -> Result<BookCache2CoverageReport> {
    let catalog = read_book_cache2_catalog(cache_root)?;
    let mut hours = BTreeSet::new();
    let mut min_ts = None::<i64>;
    let mut max_ts = None::<i64>;
    for partition in &catalog.partitions {
        hours.insert(partition.hour_bucket);
        min_ts = Some(min_ts.map_or(partition.min_ts_ns, |value| value.min(partition.min_ts_ns)));
        max_ts = Some(max_ts.map_or(partition.max_ts_ns, |value| value.max(partition.max_ts_ns)));
    }
    Ok(BookCache2CoverageReport {
        schema_version: 1,
        dataset_format: BOOK_CACHE2_FORMAT.to_string(),
        cache_root: cache_root.to_path_buf(),
        row_count: catalog.row_count,
        partition_count: catalog.partitions.len(),
        min_ts_ns: min_ts,
        max_ts_ns: max_ts,
        hour_buckets: hours.into_iter().collect(),
    })
}

fn write_partition(
    path: &Path,
    partition_id: &str,
    hour_bucket: i64,
    shard: u16,
    rows: &[BookCacheRow],
) -> Result<BookCache2Partition> {
    if rows.is_empty() {
        bail!("cannot write empty HFTBOOK2 partition");
    }
    let mut dict = LocalDictBuilder::default();
    let mut encoded = Vec::with_capacity(rows.len());
    for row in rows {
        encoded.push(encode_row(row, &mut dict)?);
    }
    let header = PartitionHeader {
        schema_version: 1,
        dataset_format: BOOK_CACHE2_FORMAT.to_string(),
        schema_hash: HFTBOOK2_SCHEMA_HASH.to_string(),
        partition_id: partition_id.to_string(),
        hour_bucket,
        asset_shard: shard,
        row_count: rows.len(),
        dictionary: dict.dictionary,
    };
    let header_bytes = serde_json::to_vec(&header)?;
    let row_bytes = encode_rows(&encoded);
    let header_crc = crc32(&header_bytes);
    let row_crc = crc32(&row_bytes);
    let mut bytes =
        Vec::with_capacity(HFTBOOK2_MAGIC.len() + 4 + header_bytes.len() + 4 + row_bytes.len() + 4);
    bytes.extend_from_slice(HFTBOOK2_MAGIC);
    bytes.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&header_bytes);
    bytes.extend_from_slice(&header_crc.to_le_bytes());
    bytes.extend_from_slice(&row_bytes);
    bytes.extend_from_slice(&row_crc.to_le_bytes());
    atomic_write_verified(path, &bytes, |tmp| {
        let decoded = read_partition_file(tmp)?;
        if decoded.len() != rows.len() {
            bail!("HFTBOOK2 partition verification row count mismatch");
        }
        Ok(())
    })?;
    let min_ts_ns = rows.iter().map(|row| row.local_recv_ts_ns).min().unwrap();
    let max_ts_ns = rows.iter().map(|row| row.local_recv_ts_ns).max().unwrap();
    Ok(BookCache2Partition {
        partition_id: partition_id.to_string(),
        path: String::new(),
        hour_bucket,
        asset_shard: shard,
        row_count: rows.len(),
        min_ts_ns,
        max_ts_ns,
        sha256: sha256_file(path)?,
        schema_hash: HFTBOOK2_SCHEMA_HASH.to_string(),
    })
}

fn read_partition(cache_root: &Path, partition: &BookCache2Partition) -> Result<Vec<BookCacheRow>> {
    validate_partition(cache_root, partition)?;
    let rows = read_partition_file(&cache_root.join(&partition.path))?;
    if rows.len() != partition.row_count {
        bail!("HFTBOOK2 partition row count mismatch");
    }
    Ok(rows)
}

fn read_partition_file(path: &Path) -> Result<Vec<BookCacheRow>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let mut cursor = 0usize;
    if bytes.get(..HFTBOOK2_MAGIC.len()) != Some(HFTBOOK2_MAGIC.as_slice()) {
        bail!("invalid HFTBOOK2 magic");
    }
    cursor += HFTBOOK2_MAGIC.len();
    let header_len = read_u32(&bytes, &mut cursor)? as usize;
    let header_bytes = bytes
        .get(cursor..cursor + header_len)
        .ok_or_else(|| anyhow!("truncated HFTBOOK2 header"))?;
    cursor += header_len;
    let expected_header_crc = read_u32(&bytes, &mut cursor)?;
    if crc32(header_bytes) != expected_header_crc {
        bail!("HFTBOOK2 header CRC mismatch");
    }
    let header: PartitionHeader = serde_json::from_slice(header_bytes)?;
    if header.dataset_format != BOOK_CACHE2_FORMAT || header.schema_hash != HFTBOOK2_SCHEMA_HASH {
        bail!("unsupported HFTBOOK2 partition format");
    }
    let row_bytes_end = bytes
        .len()
        .checked_sub(4)
        .ok_or_else(|| anyhow!("truncated HFTBOOK2 row CRC"))?;
    let row_bytes = bytes
        .get(cursor..row_bytes_end)
        .ok_or_else(|| anyhow!("truncated HFTBOOK2 rows"))?;
    let expected_row_crc = u32::from_le_bytes(bytes[row_bytes_end..].try_into().unwrap());
    if crc32(row_bytes) != expected_row_crc {
        bail!("HFTBOOK2 row CRC mismatch");
    }
    let encoded = decode_rows(row_bytes)?;
    if encoded.len() != header.row_count {
        bail!("HFTBOOK2 row count mismatch");
    }
    encoded
        .into_iter()
        .map(|row| decode_row(row, &header.dictionary))
        .collect()
}

fn read_partition_payload_fast(path: &Path) -> Result<FastPartitionPayload> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let mut cursor = 0usize;
    if bytes.get(..HFTBOOK2_MAGIC.len()) != Some(HFTBOOK2_MAGIC.as_slice()) {
        bail!("invalid HFTBOOK2 magic");
    }
    cursor += HFTBOOK2_MAGIC.len();
    let header_len = read_u32(&bytes, &mut cursor)? as usize;
    let header_end = cursor
        .checked_add(header_len)
        .context("HFTBOOK2 header length overflow")?;
    let header_bytes = bytes
        .get(cursor..header_end)
        .ok_or_else(|| anyhow!("truncated HFTBOOK2 header"))?;
    cursor = header_end;
    let expected_header_crc = read_u32(&bytes, &mut cursor)?;
    if crc32(header_bytes) != expected_header_crc {
        bail!("HFTBOOK2 header CRC mismatch");
    }
    let header: PartitionHeader = serde_json::from_slice(header_bytes)?;
    if header.dataset_format != BOOK_CACHE2_FORMAT || header.schema_hash != HFTBOOK2_SCHEMA_HASH {
        bail!("unsupported HFTBOOK2 partition format");
    }
    let row_start = cursor;
    let row_end = bytes
        .len()
        .checked_sub(4)
        .ok_or_else(|| anyhow!("truncated HFTBOOK2 row CRC"))?;
    if row_end < row_start {
        bail!("truncated HFTBOOK2 rows");
    }
    let row_len = row_end - row_start;
    let expected_row_len = header
        .row_count
        .checked_mul(ENCODED_BOOK2_ROW_BYTES)
        .context("HFTBOOK2 row byte length overflow")?;
    if row_len != expected_row_len {
        bail!("HFTBOOK2 fixed row byte length mismatch");
    }
    Ok(FastPartitionPayload {
        header,
        bytes,
        row_start,
        row_end,
    })
}

fn validate_partition(cache_root: &Path, partition: &BookCache2Partition) -> Result<()> {
    if partition.schema_hash != HFTBOOK2_SCHEMA_HASH {
        bail!("HFTBOOK2 partition schema hash mismatch");
    }
    let path = cache_root.join(&partition.path);
    if sha256_file(&path)? != partition.sha256 {
        bail!("HFTBOOK2 partition hash mismatch: {}", partition.path);
    }
    Ok(())
}

fn write_catalog(cache_root: &Path, catalog: &BookCache2Catalog) -> Result<()> {
    let path = cache_root.join(BOOK_CACHE2_CATALOG);
    market_data_etl_core::write_json_file_pretty(&path, catalog)?;
    if let Some(parent) = path.parent() {
        fsync_dir(parent)?;
    }
    Ok(())
}

fn default_poly_incremental_latency_ms() -> i64 {
    DEFAULT_POLY_INCREMENTAL_LATENCY_MS
}

fn default_poly_incremental_freshness_guard_ms() -> i64 {
    DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS
}

fn group_rows(rows: Vec<BookCacheRow>) -> BTreeMap<(i64, u16), Vec<BookCacheRow>> {
    let mut out = BTreeMap::<(i64, u16), Vec<BookCacheRow>>::new();
    for row in rows {
        out.entry((
            hour_bucket(row.local_recv_ts_ns),
            asset_shard(&row.asset_id),
        ))
        .or_default()
        .push(row);
    }
    out
}

fn encode_row(row: &BookCacheRow, dict: &mut LocalDictBuilder) -> Result<EncodedBook2Row> {
    Ok(EncodedBook2Row {
        local_recv_ts_ns: row.local_recv_ts_ns,
        ingest_seq: row.ingest_seq,
        exchange_ts_ms: row.exchange_ts_ms.unwrap_or(NONE_I64),
        symbol_key: dict.symbol(&row.symbol)?,
        condition_key: dict.condition(&row.condition_id)?,
        asset_key: dict.asset(&row.asset_id)?,
        yes_asset_key: dict.asset(&row.yes_asset_id)?,
        no_asset_key: dict.asset(&row.no_asset_id)?,
        raw_hash_key: dict.raw_hash(&row.raw_row_hash)?,
        raw_payload_hash_key: dict.raw_payload_hash(&row.raw_payload_sha256)?,
        book_state_hash_key: dict.book_state_hash(&row.book_state_hash)?,
        outcome: outcome_code(&row.outcome),
        window_start_ts_ns: row.window_start_ts_ns,
        window_end_ts_ns: row.window_end_ts_ns,
        best_bid_price_micros: row.best_bid_price_micros.unwrap_or(NONE_I64),
        best_ask_price_micros: row.best_ask_price_micros.unwrap_or(NONE_I64),
        bid_price: row
            .bid_levels
            .map(|level| level.map(|level| level.price_micros).unwrap_or(NONE_I64)),
        ask_price: row
            .ask_levels
            .map(|level| level.map(|level| level.price_micros).unwrap_or(NONE_I64)),
        bid_size: row
            .bid_levels
            .map(|level| level.map(|level| level.qty_micros).unwrap_or(NONE_I64)),
        ask_size: row
            .ask_levels
            .map(|level| level.map(|level| level.qty_micros).unwrap_or(NONE_I64)),
    })
}

fn decode_row(row: EncodedBook2Row, dict: &PartitionDictionary) -> Result<BookCacheRow> {
    Ok(BookCacheRow {
        symbol: dict_value(&dict.symbols, row.symbol_key, "symbol")?,
        condition_id: dict_value(&dict.conditions, row.condition_key, "condition")?,
        asset_id: dict_value(&dict.assets, row.asset_key, "asset")?,
        outcome: match row.outcome {
            1 => "YES".to_string(),
            2 => "NO".to_string(),
            _ => "UNKNOWN".to_string(),
        },
        window_start_ts_ns: row.window_start_ts_ns,
        window_end_ts_ns: row.window_end_ts_ns,
        yes_asset_id: dict_value(&dict.assets, row.yes_asset_key, "yes_asset")?,
        no_asset_id: dict_value(&dict.assets, row.no_asset_key, "no_asset")?,
        local_recv_ts_ns: row.local_recv_ts_ns,
        ingest_seq: row.ingest_seq,
        exchange_ts_ms: optional_i64(row.exchange_ts_ms),
        best_bid_price_micros: optional_i64(row.best_bid_price_micros),
        best_ask_price_micros: optional_i64(row.best_ask_price_micros),
        bid_levels: decode_levels(row.bid_price, row.bid_size),
        ask_levels: decode_levels(row.ask_price, row.ask_size),
        raw_row_hash: dict_value(&dict.raw_hashes, row.raw_hash_key, "raw_hash")?,
        raw_payload_sha256: dict_value(
            &dict.raw_payload_hashes,
            row.raw_payload_hash_key,
            "raw_payload_hash",
        )?,
        book_state_hash: dict_value(
            &dict.book_state_hashes,
            row.book_state_hash_key,
            "book_state_hash",
        )?,
    })
}

fn decode_row_view_to_cache_row(
    row: BookCache2RowView,
    dict: &PartitionDictionary,
) -> Result<BookCacheRow> {
    Ok(BookCacheRow {
        symbol: dict_value(&dict.symbols, row.symbol_key, "symbol")?,
        condition_id: dict_value(&dict.conditions, row.condition_key, "condition")?,
        asset_id: dict_value(&dict.assets, row.asset_key, "asset")?,
        outcome: match row.outcome {
            1 => "YES".to_string(),
            2 => "NO".to_string(),
            _ => "UNKNOWN".to_string(),
        },
        window_start_ts_ns: row.window_start_ts_ns,
        window_end_ts_ns: row.window_end_ts_ns,
        yes_asset_id: dict_value(&dict.assets, row.yes_asset_key, "yes_asset")?,
        no_asset_id: dict_value(&dict.assets, row.no_asset_key, "no_asset")?,
        local_recv_ts_ns: row.local_recv_ts_ns,
        ingest_seq: row.ingest_seq,
        exchange_ts_ms: optional_i64(row.exchange_ts_ms),
        best_bid_price_micros: optional_i64(row.best_bid_price_micros),
        best_ask_price_micros: optional_i64(row.best_ask_price_micros),
        bid_levels: decode_levels(row.bid_price, row.bid_size),
        ask_levels: decode_levels(row.ask_price, row.ask_size),
        raw_row_hash: dict_value(&dict.raw_hashes, row.raw_hash_key, "raw_hash")?,
        raw_payload_sha256: dict_value(
            &dict.raw_payload_hashes,
            row.raw_payload_hash_key,
            "raw_payload_hash",
        )?,
        book_state_hash: dict_value(
            &dict.book_state_hashes,
            row.book_state_hash_key,
            "book_state_hash",
        )?,
    })
}

fn encode_rows(rows: &[EncodedBook2Row]) -> Vec<u8> {
    let mut out = Vec::with_capacity(rows.len() * (8 * 45));
    for row in rows {
        push_i64(&mut out, row.local_recv_ts_ns);
        push_u64(&mut out, row.ingest_seq);
        push_i64(&mut out, row.exchange_ts_ms);
        push_u32(&mut out, row.symbol_key);
        push_u32(&mut out, row.condition_key);
        push_u32(&mut out, row.asset_key);
        push_u32(&mut out, row.yes_asset_key);
        push_u32(&mut out, row.no_asset_key);
        push_u32(&mut out, row.raw_hash_key);
        push_u32(&mut out, row.raw_payload_hash_key);
        push_u32(&mut out, row.book_state_hash_key);
        out.push(row.outcome);
        push_i64(&mut out, row.window_start_ts_ns);
        push_i64(&mut out, row.window_end_ts_ns);
        push_i64(&mut out, row.best_bid_price_micros);
        push_i64(&mut out, row.best_ask_price_micros);
        for value in row.bid_price {
            push_i64(&mut out, value);
        }
        for value in row.ask_price {
            push_i64(&mut out, value);
        }
        for value in row.bid_size {
            push_i64(&mut out, value);
        }
        for value in row.ask_size {
            push_i64(&mut out, value);
        }
    }
    out
}

fn decode_rows(bytes: &[u8]) -> Result<Vec<EncodedBook2Row>> {
    let mut cursor = 0usize;
    let mut out = Vec::new();
    while cursor < bytes.len() {
        let local_recv_ts_ns = read_i64(bytes, &mut cursor)?;
        let ingest_seq = read_u64(bytes, &mut cursor)?;
        let exchange_ts_ms = read_i64(bytes, &mut cursor)?;
        let symbol_key = read_u32(bytes, &mut cursor)?;
        let condition_key = read_u32(bytes, &mut cursor)?;
        let asset_key = read_u32(bytes, &mut cursor)?;
        let yes_asset_key = read_u32(bytes, &mut cursor)?;
        let no_asset_key = read_u32(bytes, &mut cursor)?;
        let raw_hash_key = read_u32(bytes, &mut cursor)?;
        let raw_payload_hash_key = read_u32(bytes, &mut cursor)?;
        let book_state_hash_key = read_u32(bytes, &mut cursor)?;
        let outcome = *bytes
            .get(cursor)
            .ok_or_else(|| anyhow!("truncated HFTBOOK2 outcome"))?;
        cursor += 1;
        let window_start_ts_ns = read_i64(bytes, &mut cursor)?;
        let window_end_ts_ns = read_i64(bytes, &mut cursor)?;
        let best_bid_price_micros = read_i64(bytes, &mut cursor)?;
        let best_ask_price_micros = read_i64(bytes, &mut cursor)?;
        out.push(EncodedBook2Row {
            local_recv_ts_ns,
            ingest_seq,
            exchange_ts_ms,
            symbol_key,
            condition_key,
            asset_key,
            yes_asset_key,
            no_asset_key,
            raw_hash_key,
            raw_payload_hash_key,
            book_state_hash_key,
            outcome,
            window_start_ts_ns,
            window_end_ts_ns,
            best_bid_price_micros,
            best_ask_price_micros,
            bid_price: read_i64_array(bytes, &mut cursor)?,
            ask_price: read_i64_array(bytes, &mut cursor)?,
            bid_size: read_i64_array(bytes, &mut cursor)?,
            ask_size: read_i64_array(bytes, &mut cursor)?,
        });
    }
    Ok(out)
}

fn decode_row_view(bytes: &[u8], cursor: &mut usize) -> Result<BookCache2RowView> {
    if bytes.len().saturating_sub(*cursor) < ENCODED_BOOK2_ROW_BYTES {
        bail!("truncated HFTBOOK2 row");
    }
    let local_recv_ts_ns = read_i64(bytes, cursor)?;
    let ingest_seq = read_u64(bytes, cursor)?;
    let exchange_ts_ms = read_i64(bytes, cursor)?;
    let symbol_key = read_u32(bytes, cursor)?;
    let condition_key = read_u32(bytes, cursor)?;
    let asset_key = read_u32(bytes, cursor)?;
    let yes_asset_key = read_u32(bytes, cursor)?;
    let no_asset_key = read_u32(bytes, cursor)?;
    let raw_hash_key = read_u32(bytes, cursor)?;
    let raw_payload_hash_key = read_u32(bytes, cursor)?;
    let book_state_hash_key = read_u32(bytes, cursor)?;
    let outcome = *bytes
        .get(*cursor)
        .ok_or_else(|| anyhow!("truncated HFTBOOK2 outcome"))?;
    *cursor += 1;
    let window_start_ts_ns = read_i64(bytes, cursor)?;
    let window_end_ts_ns = read_i64(bytes, cursor)?;
    let best_bid_price_micros = read_i64(bytes, cursor)?;
    let best_ask_price_micros = read_i64(bytes, cursor)?;
    Ok(BookCache2RowView {
        local_recv_ts_ns,
        ingest_seq,
        exchange_ts_ms,
        symbol_key,
        condition_key,
        asset_key,
        yes_asset_key,
        no_asset_key,
        raw_hash_key,
        raw_payload_hash_key,
        book_state_hash_key,
        outcome,
        window_start_ts_ns,
        window_end_ts_ns,
        best_bid_price_micros,
        best_ask_price_micros,
        bid_price: read_i64_array(bytes, cursor)?,
        ask_price: read_i64_array(bytes, cursor)?,
        bid_size: read_i64_array(bytes, cursor)?,
        ask_size: read_i64_array(bytes, cursor)?,
    })
}

fn decode_levels(price: [i64; 10], size: [i64; 10]) -> [Option<BookLevelMicros>; 10] {
    std::array::from_fn(|idx| {
        let price = optional_i64(price[idx])?;
        let size = optional_i64(size[idx])?;
        Some(BookLevelMicros {
            price_micros: price,
            qty_micros: size,
        })
    })
}

fn read_i64_array(bytes: &[u8], cursor: &mut usize) -> Result<[i64; 10]> {
    let mut out = [0i64; 10];
    for value in &mut out {
        *value = read_i64(bytes, cursor)?;
    }
    Ok(out)
}

fn row_order(a: &BookCacheRow, b: &BookCacheRow) -> std::cmp::Ordering {
    (
        a.local_recv_ts_ns,
        a.ingest_seq,
        a.condition_id.as_str(),
        a.asset_id.as_str(),
    )
        .cmp(&(
            b.local_recv_ts_ns,
            b.ingest_seq,
            b.condition_id.as_str(),
            b.asset_id.as_str(),
        ))
}

fn partition_overlaps(
    partition: &BookCache2Partition,
    start_ts_ns: Option<i64>,
    end_ts_ns: Option<i64>,
) -> bool {
    start_ts_ns.is_none_or(|start| partition.max_ts_ns >= start)
        && end_ts_ns.is_none_or(|end| partition.min_ts_ns < end)
}

fn ts_in_window(ts_ns: i64, start_ts_ns: Option<i64>, end_ts_ns: Option<i64>) -> bool {
    start_ts_ns.is_none_or(|start| ts_ns >= start) && end_ts_ns.is_none_or(|end| ts_ns < end)
}

fn hour_bucket(ts_ns: i64) -> i64 {
    ts_ns.div_euclid(HOUR_NS)
}

fn asset_shard(asset_id: &str) -> u16 {
    let sum = asset_id.as_bytes().iter().fold(0u32, |acc, byte| {
        acc.wrapping_mul(16777619) ^ (*byte as u32)
    });
    (sum % SHARD_COUNT as u32) as u16
}

fn optional_i64(value: i64) -> Option<i64> {
    if value == NONE_I64 {
        None
    } else {
        Some(value)
    }
}

fn dict_value(values: &[String], key: u32, field: &str) -> Result<String> {
    values
        .get(key as usize)
        .cloned()
        .ok_or_else(|| anyhow!("HFTBOOK2 {field} key out of range"))
}

fn dict_key(values: &[String], wanted: &str) -> Result<Option<u32>> {
    values
        .iter()
        .position(|value| value == wanted)
        .map(|idx| u32::try_from(idx).context("HFTBOOK2 dictionary key overflow"))
        .transpose()
}

impl OrderedPartitionCursor {
    fn advance(
        &mut self,
        start_ts_ns: Option<i64>,
        end_ts_ns: Option<i64>,
        filter: &BookCache2ScanFilter,
    ) -> Result<()> {
        let symbol_key_filter = filter
            .symbol
            .as_ref()
            .map(|symbol| dict_key(&self.payload.header.dictionary.symbols, symbol))
            .transpose()?
            .flatten();
        let condition_key_filter = filter
            .condition_id
            .as_ref()
            .map(|condition_id| dict_key(&self.payload.header.dictionary.conditions, condition_id))
            .transpose()?
            .flatten();
        if filter.symbol.is_some() && symbol_key_filter.is_none()
            || filter.condition_id.is_some() && condition_key_filter.is_none()
        {
            self.current = None;
            self.cursor = self.payload.row_end - self.payload.row_start;
            return Ok(());
        }
        let row_bytes = &self.payload.bytes[self.payload.row_start..self.payload.row_end];
        while self.cursor < row_bytes.len() {
            let row = decode_row_view(row_bytes, &mut self.cursor)?;
            if !ts_in_window(row.local_recv_ts_ns, start_ts_ns, end_ts_ns) {
                continue;
            }
            if symbol_key_filter.is_some_and(|key| key != row.symbol_key) {
                continue;
            }
            if condition_key_filter.is_some_and(|key| key != row.condition_key) {
                continue;
            }
            self.current = Some(decode_row_view_to_cache_row(
                row,
                &self.payload.header.dictionary,
            )?);
            return Ok(());
        }
        self.current = None;
        Ok(())
    }
}

impl From<&BookCacheRow> for OrderedRowKey {
    fn from(row: &BookCacheRow) -> Self {
        Self {
            local_recv_ts_ns: row.local_recv_ts_ns,
            ingest_seq: row.ingest_seq,
            condition_id: row.condition_id.clone(),
            asset_id: row.asset_id.clone(),
        }
    }
}

impl Ord for OrderedRowKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (
            self.local_recv_ts_ns,
            self.ingest_seq,
            self.condition_id.as_str(),
            self.asset_id.as_str(),
        )
            .cmp(&(
                other.local_recv_ts_ns,
                other.ingest_seq,
                other.condition_id.as_str(),
                other.asset_id.as_str(),
            ))
    }
}

impl PartialOrd for OrderedRowKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedHeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.cursor_idx.cmp(&self.cursor_idx))
    }
}

impl PartialOrd for OrderedHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn merge_raw_roots(mut existing: Vec<PathBuf>, incoming: &[PathBuf]) -> Vec<PathBuf> {
    for root in incoming {
        if !existing.iter().any(|value| value == root) {
            existing.push(root.clone());
        }
    }
    existing
}

impl LocalDictBuilder {
    fn symbol(&mut self, value: &str) -> Result<u32> {
        intern(value, &mut self.dictionary.symbols, &mut self.symbols)
    }

    fn condition(&mut self, value: &str) -> Result<u32> {
        intern(value, &mut self.dictionary.conditions, &mut self.conditions)
    }

    fn asset(&mut self, value: &str) -> Result<u32> {
        intern(value, &mut self.dictionary.assets, &mut self.assets)
    }

    fn raw_hash(&mut self, value: &str) -> Result<u32> {
        intern(value, &mut self.dictionary.raw_hashes, &mut self.raw_hashes)
    }

    fn raw_payload_hash(&mut self, value: &str) -> Result<u32> {
        intern(
            value,
            &mut self.dictionary.raw_payload_hashes,
            &mut self.raw_payload_hashes,
        )
    }

    fn book_state_hash(&mut self, value: &str) -> Result<u32> {
        intern(
            value,
            &mut self.dictionary.book_state_hashes,
            &mut self.book_state_hashes,
        )
    }
}

fn intern(
    value: &str,
    values: &mut Vec<String>,
    lookup: &mut BTreeMap<String, u32>,
) -> Result<u32> {
    if let Some(key) = lookup.get(value) {
        return Ok(*key);
    }
    let key = u32::try_from(values.len()).context("HFTBOOK2 dictionary overflow")?;
    values.push(value.to_string());
    lookup.insert(value.to_string(), key);
    Ok(key)
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Crc32::new();
    hasher.update(bytes);
    hasher.finalize()
}

fn push_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let out = bytes
        .get(*cursor..*cursor + 4)
        .ok_or_else(|| anyhow!("truncated HFTBOOK2 u32"))?;
    *cursor += 4;
    Ok(u32::from_le_bytes(out.try_into().unwrap()))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    let out = bytes
        .get(*cursor..*cursor + 8)
        .ok_or_else(|| anyhow!("truncated HFTBOOK2 u64"))?;
    *cursor += 8;
    Ok(u64::from_le_bytes(out.try_into().unwrap()))
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64> {
    let out = bytes
        .get(*cursor..*cursor + 8)
        .ok_or_else(|| anyhow!("truncated HFTBOOK2 i64"))?;
    *cursor += 8;
    Ok(i64::from_le_bytes(out.try_into().unwrap()))
}
