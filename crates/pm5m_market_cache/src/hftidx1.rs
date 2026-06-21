use crate::hftbook2::{stream_book_cache2_rows_ordered, BookCache2ScanFilter};
use crate::types::{outcome_code, BookCacheRow, BookLevelMicros, BOOK_CACHE2_CATALOG};
use anyhow::{anyhow, bail, Context, Result};
use crc32fast::Hasher as Crc32;
use market_data_etl_core::{now_unix_ns, sha256_file};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

pub const HFTIDX1_FORMAT: &str = "pm5m_book_state_index.hftidx1.v1";
pub const HFTIDX1_CATALOG: &str = "catalog.hftidx1.json";

const HFTIDX1_MAGIC: &[u8; 7] = b"HFTIDX1";
const HFTIDX1_DATA_FILE: &str = "book_state_index.hfi1";
const HFTIDX1_SPOOL_DIR: &str = "spool.hftidx1.tmp";
const HFTIDX1_SPOOL_OPEN_FILE_LIMIT: usize = 256;
const NONE_I64: i64 = i64::MIN;
const HFTIDX1_ROW_BYTES: usize = 8 + 8 + 8 + 4 + 4 + 4 + 4 + 4 + 1 + 8 + 8 + 8 + 8 + 8 * 40;
const LEGACY_HFTIDX1_ROW_BYTES: usize = 8 + 8 + 4 + 4 + 4 + 1 + 8 + 8 + 8 + 8 + 8 * 40;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildBookStateIndexOptions {
    pub book_cache_root: PathBuf,
    pub index_root: PathBuf,
    pub start_ts_ns: Option<i64>,
    pub end_ts_ns: Option<i64>,
    pub overwrite: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookStateIndexCatalog {
    pub schema_version: u32,
    pub dataset_format: String,
    pub index_root: PathBuf,
    pub source_book_cache_root: PathBuf,
    pub source_book_catalog_hash: String,
    pub data_path: String,
    pub generated_ts_ns: i64,
    pub row_count: usize,
    pub asset_count: usize,
    pub condition_count: usize,
    pub symbol_count: usize,
    pub min_ts_ns: Option<i64>,
    pub max_ts_ns: Option<i64>,
    pub data_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookStateIndexValidationReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub index_root: PathBuf,
    pub row_count: usize,
    pub asset_count: usize,
    pub condition_count: usize,
    pub symbol_count: usize,
    pub catalog_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BookStateIndexBenchReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub index_root: PathBuf,
    pub start_ts_ns: Option<i64>,
    pub end_ts_ns: Option<i64>,
    pub rows_read: usize,
    pub asset_series_scanned: usize,
    pub elapsed_ms: u128,
    pub rows_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookStateIndexHeader {
    pub schema_version: u32,
    pub dataset_format: String,
    pub row_count: usize,
    pub symbols: Vec<String>,
    #[serde(default)]
    pub raw_payload_hashes: Vec<String>,
    #[serde(default)]
    pub book_state_hashes: Vec<String>,
    pub conditions: Vec<BookStateIndexCondition>,
    pub assets: Vec<BookStateIndexAsset>,
    pub asset_ranges: Vec<BookStateIndexAssetRange>,
    pub reference_symbol_assets: Vec<BookStateIndexReferenceAssets>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookStateIndexCondition {
    pub condition_id: String,
    pub symbol_key: u32,
    pub window_start_ts_ns: i64,
    pub window_end_ts_ns: i64,
    pub yes_asset_key: u32,
    pub no_asset_key: u32,
    pub first_seen_ts_ns: i64,
    pub last_seen_ts_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookStateIndexAsset {
    pub asset_id: String,
    pub condition_key: u32,
    pub outcome_code: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookStateIndexAssetRange {
    pub asset_key: u32,
    pub offset: usize,
    pub len: usize,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookStateIndexReferenceAssets {
    pub reference_symbol: String,
    pub asset_keys: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookStateIndex {
    pub catalog: BookStateIndexCatalog,
    pub header: BookStateIndexHeader,
    pub rows: Vec<BookStateIndexRow>,
    pub asset_series_scanned: usize,
}

pub struct BookStateIndexReader {
    catalog: BookStateIndexCatalog,
    header: BookStateIndexHeader,
    row_start: usize,
    row_end: usize,
    row_bytes: usize,
    file: Mutex<File>,
    range_by_asset_key: BTreeMap<u32, BookStateIndexAssetRange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookStateIndexRow {
    pub row_idx: usize,
    pub local_recv_ts_ns: i64,
    pub ingest_seq: u64,
    pub exchange_ts_ms: Option<i64>,
    pub symbol_key: u32,
    pub condition_key: u32,
    pub asset_key: u32,
    pub raw_payload_hash_key: u32,
    pub book_state_hash_key: u32,
    pub outcome_code: u8,
    pub window_start_ts_ns: i64,
    pub window_end_ts_ns: i64,
    pub best_bid_price_micros: Option<i64>,
    pub best_ask_price_micros: Option<i64>,
    pub bid_levels: [Option<BookLevelMicros>; 10],
    pub ask_levels: [Option<BookLevelMicros>; 10],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EncodedBookStateIndexRow {
    local_recv_ts_ns: i64,
    ingest_seq: u64,
    exchange_ts_ms: i64,
    symbol_key: u32,
    condition_key: u32,
    asset_key: u32,
    raw_payload_hash_key: u32,
    book_state_hash_key: u32,
    outcome_code: u8,
    window_start_ts_ns: i64,
    window_end_ts_ns: i64,
    best_bid_price_micros: i64,
    best_ask_price_micros: i64,
    bid_price: [i64; 10],
    ask_price: [i64; 10],
    bid_size: [i64; 10],
    ask_size: [i64; 10],
}

#[derive(Debug, Default)]
struct AssetSpoolMeta {
    row_count: usize,
    min_ts_ns: Option<i64>,
    max_ts_ns: Option<i64>,
    last_order_key: Option<(i64, u64, u32)>,
}

impl AssetSpoolMeta {
    fn observe(&mut self, row: &EncodedBookStateIndexRow) -> Result<()> {
        if let Some(previous) = self.last_order_key {
            let next = (row.local_recv_ts_ns, row.ingest_seq, row.condition_key);
            if next < previous {
                bail!(
                    "HFTBOOK2 rows for asset {} are not time sorted",
                    row.asset_key
                );
            }
        }
        self.last_order_key = Some((row.local_recv_ts_ns, row.ingest_seq, row.condition_key));
        self.min_ts_ns = Some(self.min_ts_ns.map_or(row.local_recv_ts_ns, |value| {
            value.min(row.local_recv_ts_ns)
        }));
        self.max_ts_ns = Some(self.max_ts_ns.map_or(row.local_recv_ts_ns, |value| {
            value.max(row.local_recv_ts_ns)
        }));
        self.row_count = self
            .row_count
            .checked_add(1)
            .context("HFTIDX1 asset row count overflow")?;
        Ok(())
    }
}

struct AssetSpoolSet {
    root: PathBuf,
    open: BTreeMap<u32, BufWriter<File>>,
    meta: BTreeMap<u32, AssetSpoolMeta>,
    open_limit: usize,
}

impl AssetSpoolSet {
    fn new(index_root: &Path) -> Result<Self> {
        let root = index_root.join(HFTIDX1_SPOOL_DIR);
        if root.exists() {
            fs::remove_dir_all(&root).with_context(|| format!("remove {}", root.display()))?;
        }
        fs::create_dir_all(&root).with_context(|| format!("create {}", root.display()))?;
        Ok(Self {
            root,
            open: BTreeMap::new(),
            meta: BTreeMap::new(),
            open_limit: HFTIDX1_SPOOL_OPEN_FILE_LIMIT,
        })
    }

    fn push(&mut self, row: &EncodedBookStateIndexRow, scratch: &mut Vec<u8>) -> Result<()> {
        self.meta.entry(row.asset_key).or_default().observe(row)?;
        scratch.clear();
        push_encoded_row(scratch, row);
        if scratch.len() != HFTIDX1_ROW_BYTES {
            bail!("HFTIDX1 encoded row byte length mismatch");
        }
        self.writer_for(row.asset_key)?
            .write_all(scratch)
            .with_context(|| format!("write HFTIDX1 asset spool {}", row.asset_key))?;
        Ok(())
    }

    fn flush_all(&mut self) -> Result<()> {
        for (asset_key, writer) in &mut self.open {
            writer
                .flush()
                .with_context(|| format!("flush HFTIDX1 asset spool {asset_key}"))?;
        }
        self.open.clear();
        Ok(())
    }

    fn writer_for(&mut self, asset_key: u32) -> Result<&mut BufWriter<File>> {
        if !self.open.contains_key(&asset_key) {
            self.evict_one_if_needed(asset_key)?;
            let path = asset_spool_path(&self.root, asset_key);
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("open HFTIDX1 asset spool {}", path.display()))?;
            self.open
                .insert(asset_key, BufWriter::with_capacity(256 * 1024, file));
        }
        self.open
            .get_mut(&asset_key)
            .ok_or_else(|| anyhow!("missing HFTIDX1 asset spool writer {asset_key}"))
    }

    fn evict_one_if_needed(&mut self, keep_asset_key: u32) -> Result<()> {
        if self.open.len() < self.open_limit {
            return Ok(());
        }
        let evict_key = self
            .open
            .keys()
            .copied()
            .find(|key| *key != keep_asset_key)
            .ok_or_else(|| anyhow!("HFTIDX1 asset spool open file limit is too small"))?;
        let mut writer = self
            .open
            .remove(&evict_key)
            .ok_or_else(|| anyhow!("missing HFTIDX1 asset spool writer {evict_key}"))?;
        writer
            .flush()
            .with_context(|| format!("flush evicted HFTIDX1 asset spool {evict_key}"))?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct FastIndexPayload {
    header: BookStateIndexHeader,
    bytes: Vec<u8>,
    row_start: usize,
    row_end: usize,
    row_bytes: usize,
}

#[derive(Debug, Clone)]
struct IndexFileHeader {
    header: BookStateIndexHeader,
    row_start: usize,
    row_end: usize,
    row_bytes: usize,
}

#[derive(Default)]
struct IndexDict {
    symbols: Dict,
    conditions: Dict,
    assets: Dict,
    raw_payload_hashes: Dict,
    book_state_hashes: Dict,
}

#[derive(Default)]
struct Dict {
    values: Vec<String>,
    by_value: BTreeMap<String, u32>,
}

pub fn build_book_state_index(
    options: &BuildBookStateIndexOptions,
) -> Result<BookStateIndexCatalog> {
    if let (Some(start), Some(end)) = (options.start_ts_ns, options.end_ts_ns) {
        if end <= start {
            bail!("end_ts_ns must be greater than start_ts_ns");
        }
    }
    if !options.book_cache_root.join(BOOK_CACHE2_CATALOG).exists() {
        bail!(
            "HFTIDX1 builder requires HFTBOOK2 cache at {}",
            options.book_cache_root.display()
        );
    }
    prepare_root(&options.index_root, options.overwrite)?;

    let mut dict = IndexDict::default();
    let mut asset_spools = AssetSpoolSet::new(&options.index_root)?;
    let mut scratch = Vec::with_capacity(HFTIDX1_ROW_BYTES);
    let mut row_count = 0usize;
    let mut condition_meta = BTreeMap::<u32, BookStateIndexCondition>::new();
    let mut asset_meta = BTreeMap::<u32, BookStateIndexAsset>::new();
    let mut reference_assets = BTreeMap::<String, BTreeSet<u32>>::new();
    let mut min_ts = None::<i64>;
    let mut max_ts = None::<i64>;

    stream_book_cache2_rows_ordered(
        &options.book_cache_root,
        options.start_ts_ns,
        options.end_ts_ns,
        &BookCache2ScanFilter::default(),
        |row| {
            if row.yes_asset_id == row.no_asset_id {
                bail!(
                    "HFTBOOK2 YES/NO asset ids are identical for {}",
                    row.condition_id
                );
            }
            let encoded = encode_source_row(
                &row,
                &mut dict,
                &mut condition_meta,
                &mut asset_meta,
                &mut reference_assets,
            )?;
            min_ts = Some(min_ts.map_or(row.local_recv_ts_ns, |value| {
                value.min(row.local_recv_ts_ns)
            }));
            max_ts = Some(max_ts.map_or(row.local_recv_ts_ns, |value| {
                value.max(row.local_recv_ts_ns)
            }));
            asset_spools.push(&encoded, &mut scratch)?;
            row_count = row_count
                .checked_add(1)
                .context("HFTIDX1 row count overflow")?;
            Ok(())
        },
    )?;
    if row_count == 0 {
        bail!(
            "HFTIDX1 builder produced no rows from {}",
            options.book_cache_root.display()
        );
    }
    asset_spools.flush_all()?;

    let asset_ranges = asset_ranges_from_spools(&asset_spools.meta)?;
    let header = BookStateIndexHeader {
        schema_version: 1,
        dataset_format: HFTIDX1_FORMAT.to_string(),
        row_count,
        symbols: dict.symbols.values,
        raw_payload_hashes: dict.raw_payload_hashes.values,
        book_state_hashes: dict.book_state_hashes.values,
        conditions: ordered_values(condition_meta, "condition")?,
        assets: ordered_values(asset_meta, "asset")?,
        asset_ranges,
        reference_symbol_assets: reference_assets
            .into_iter()
            .map(
                |(reference_symbol, asset_keys)| BookStateIndexReferenceAssets {
                    reference_symbol,
                    asset_keys: asset_keys.into_iter().collect(),
                },
            )
            .collect(),
    };
    validate_header(&header)?;

    let data_path = options.index_root.join(HFTIDX1_DATA_FILE);
    write_index_file_from_asset_spools(
        &data_path,
        &header,
        &asset_spools.root,
        &asset_spools.meta,
    )?;
    let _ = fs::remove_dir_all(&asset_spools.root);
    let catalog = BookStateIndexCatalog {
        schema_version: 1,
        dataset_format: HFTIDX1_FORMAT.to_string(),
        index_root: options.index_root.clone(),
        source_book_cache_root: options.book_cache_root.clone(),
        source_book_catalog_hash: sha256_file(&options.book_cache_root.join(BOOK_CACHE2_CATALOG))?,
        data_path: HFTIDX1_DATA_FILE.to_string(),
        generated_ts_ns: now_unix_ns() as i64,
        row_count,
        asset_count: header.assets.len(),
        condition_count: header.conditions.len(),
        symbol_count: header.symbols.len(),
        min_ts_ns: min_ts,
        max_ts_ns: max_ts,
        data_sha256: sha256_file(&data_path)?,
    };
    write_catalog(&options.index_root, &catalog)?;
    Ok(catalog)
}

pub fn read_book_state_index(
    index_root: &Path,
    start_ts_ns: Option<i64>,
    end_ts_ns: Option<i64>,
) -> Result<BookStateIndex> {
    if let (Some(start), Some(end)) = (start_ts_ns, end_ts_ns) {
        if end <= start {
            bail!("end_ts_ns must be greater than start_ts_ns");
        }
    }
    let catalog = read_book_state_index_catalog(index_root)?;
    let data_path = index_root.join(&catalog.data_path);
    if start_ts_ns.is_none() && end_ts_ns.is_none() {
        return read_full_book_state_index(catalog, &data_path);
    }

    let mut file =
        File::open(&data_path).with_context(|| format!("open {}", data_path.display()))?;
    let index_file = read_index_file_header(&mut file)?;
    validate_catalog_matches_header(&catalog, &index_file.header)?;
    let expected_len = index_file
        .header
        .row_count
        .checked_mul(index_file.row_bytes)
        .context("HFTIDX1 row byte length overflow")?;
    if index_file.row_end - index_file.row_start != expected_len {
        bail!("HFTIDX1 fixed row byte length mismatch");
    }

    let mut rows = Vec::new();
    let mut asset_series_scanned = 0usize;
    for range in &index_file.header.asset_ranges {
        if !range_overlaps(range, start_ts_ns, end_ts_ns) {
            continue;
        }
        asset_series_scanned += 1;
        let start_rel = match start_ts_ns {
            Some(target) => lower_bound_ts_file(
                &mut file,
                index_file.row_start,
                index_file.row_bytes,
                range.offset,
                range.len,
                target,
            )?,
            None => 0,
        };
        let end_rel = match end_ts_ns {
            Some(target) => lower_bound_ts_file(
                &mut file,
                index_file.row_start,
                index_file.row_bytes,
                range.offset,
                range.len,
                target,
            )?,
            None => range.len,
        };
        rows.extend(read_rows_from_file(
            &mut file,
            index_file.row_start,
            index_file.row_bytes,
            range.offset + start_rel,
            end_rel - start_rel,
        )?);
    }
    Ok(BookStateIndex {
        catalog,
        header: index_file.header,
        rows,
        asset_series_scanned,
    })
}

fn read_full_book_state_index(
    catalog: BookStateIndexCatalog,
    data_path: &Path,
) -> Result<BookStateIndex> {
    let payload = read_index_payload_fast(data_path, false)?;
    validate_catalog_matches_header(&catalog, &payload.header)?;
    let row_bytes = &payload.bytes[payload.row_start..payload.row_end];
    let expected_len = payload
        .header
        .row_count
        .checked_mul(payload.row_bytes)
        .context("HFTIDX1 row byte length overflow")?;
    if row_bytes.len() != expected_len {
        bail!("HFTIDX1 fixed row byte length mismatch");
    }
    let mut rows = Vec::with_capacity(payload.header.row_count);
    for row_idx in 0..payload.header.row_count {
        rows.push(decode_row_at(row_bytes, payload.row_bytes, row_idx)?);
    }
    let asset_series_scanned = payload.header.asset_ranges.len();
    Ok(BookStateIndex {
        catalog,
        header: payload.header,
        rows,
        asset_series_scanned,
    })
}

pub fn read_book_state_index_catalog(index_root: &Path) -> Result<BookStateIndexCatalog> {
    let path = index_root.join(HFTIDX1_CATALOG);
    let catalog: BookStateIndexCatalog = serde_json::from_reader(
        fs::File::open(&path).with_context(|| format!("open {}", path.display()))?,
    )
    .with_context(|| format!("parse {}", path.display()))?;
    if catalog.dataset_format != HFTIDX1_FORMAT {
        bail!("unsupported HFTIDX1 format {}", catalog.dataset_format);
    }
    Ok(catalog)
}

pub fn open_book_state_index_reader(index_root: &Path) -> Result<BookStateIndexReader> {
    let catalog = read_book_state_index_catalog(index_root)?;
    let data_path = index_root.join(&catalog.data_path);
    let mut file =
        File::open(&data_path).with_context(|| format!("open {}", data_path.display()))?;
    let index_file = read_index_file_header(&mut file)?;
    validate_catalog_matches_header(&catalog, &index_file.header)?;
    let expected_len = index_file
        .header
        .row_count
        .checked_mul(index_file.row_bytes)
        .context("HFTIDX1 row byte length overflow")?;
    if index_file.row_end - index_file.row_start != expected_len {
        bail!("HFTIDX1 fixed row byte length mismatch");
    }
    let mut range_by_asset_key = BTreeMap::new();
    for range in &index_file.header.asset_ranges {
        if range_by_asset_key
            .insert(range.asset_key, range.clone())
            .is_some()
        {
            bail!(
                "duplicate HFTIDX1 asset range for asset {}",
                range.asset_key
            );
        }
    }
    Ok(BookStateIndexReader {
        catalog,
        header: index_file.header,
        row_start: index_file.row_start,
        row_end: index_file.row_end,
        row_bytes: index_file.row_bytes,
        file: Mutex::new(file),
        range_by_asset_key,
    })
}

impl BookStateIndexReader {
    pub fn catalog(&self) -> &BookStateIndexCatalog {
        &self.catalog
    }

    pub fn header(&self) -> &BookStateIndexHeader {
        &self.header
    }

    pub fn state_at(&self, asset_key: u32, ts_ns: i64) -> Result<Option<BookStateIndexRow>> {
        let Some(range) = self.range_by_asset_key.get(&asset_key) else {
            return Ok(None);
        };
        if ts_ns < range.min_ts_ns {
            return Ok(None);
        }
        let mut file = self.file.lock().expect("HFTIDX1 reader mutex poisoned");
        let rel_end = upper_bound_ts_file(
            &mut file,
            self.row_start,
            self.row_bytes,
            range.offset,
            range.len,
            ts_ns,
        )?;
        if rel_end == 0 {
            return Ok(None);
        }
        let row_idx = range
            .offset
            .checked_add(rel_end - 1)
            .context("HFTIDX1 state_at row offset overflow")?;
        let valid_until = if rel_end < range.len {
            row_ts_file(
                &mut file,
                self.row_start,
                self.row_bytes,
                range.offset + rel_end,
            )?
        } else {
            i64::MAX
        };
        if ts_ns >= valid_until {
            return Ok(None);
        }
        read_row_from_file(
            &mut file,
            self.row_start,
            self.row_end,
            self.row_bytes,
            row_idx,
        )
        .map(Some)
    }
}

pub fn validate_book_state_index(index_root: &Path) -> Result<BookStateIndexValidationReport> {
    let catalog = read_book_state_index_catalog(index_root)?;
    if sha256_file(&index_root.join(&catalog.data_path))? != catalog.data_sha256 {
        bail!("HFTIDX1 data hash mismatch");
    }
    let data_path = index_root.join(&catalog.data_path);
    let mut file =
        File::open(&data_path).with_context(|| format!("open {}", data_path.display()))?;
    let index_file = read_index_file_header(&mut file)?;
    validate_catalog_matches_header(&catalog, &index_file.header)?;
    validate_header(&index_file.header)?;
    validate_rows_from_file(&mut file, &index_file)?;
    Ok(BookStateIndexValidationReport {
        schema_version: 1,
        dataset_format: HFTIDX1_FORMAT.to_string(),
        index_root: index_root.to_path_buf(),
        row_count: catalog.row_count,
        asset_count: catalog.asset_count,
        condition_count: catalog.condition_count,
        symbol_count: catalog.symbol_count,
        catalog_hash: sha256_file(&index_root.join(HFTIDX1_CATALOG))?,
    })
}

pub fn bench_book_state_index(
    index_root: &Path,
    start_ts_ns: Option<i64>,
    end_ts_ns: Option<i64>,
) -> Result<BookStateIndexBenchReport> {
    let started = Instant::now();
    let index = read_book_state_index(index_root, start_ts_ns, end_ts_ns)?;
    let elapsed = started.elapsed();
    let rows_read = index.rows.len();
    Ok(BookStateIndexBenchReport {
        schema_version: 1,
        dataset_format: HFTIDX1_FORMAT.to_string(),
        index_root: index_root.to_path_buf(),
        start_ts_ns,
        end_ts_ns,
        rows_read,
        asset_series_scanned: index.asset_series_scanned,
        elapsed_ms: elapsed.as_millis(),
        rows_per_sec: if elapsed.as_secs_f64() > 0.0 {
            rows_read as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        },
    })
}

fn encode_source_row(
    row: &BookCacheRow,
    dict: &mut IndexDict,
    condition_meta: &mut BTreeMap<u32, BookStateIndexCondition>,
    asset_meta: &mut BTreeMap<u32, BookStateIndexAsset>,
    reference_assets: &mut BTreeMap<String, BTreeSet<u32>>,
) -> Result<EncodedBookStateIndexRow> {
    let symbol_key = dict.symbols.intern(&row.symbol)?;
    let condition_key = dict.conditions.intern(&row.condition_id)?;
    let asset_key = dict.assets.intern(&row.asset_id)?;
    let yes_asset_key = dict.assets.intern(&row.yes_asset_id)?;
    let no_asset_key = dict.assets.intern(&row.no_asset_id)?;
    let outcome_code = outcome_code(&row.outcome);
    update_condition_meta(
        row,
        condition_key,
        symbol_key,
        yes_asset_key,
        no_asset_key,
        condition_meta,
    )?;
    update_asset_meta(row, asset_key, condition_key, outcome_code, asset_meta)?;
    update_asset_meta(row, yes_asset_key, condition_key, 1, asset_meta)?;
    update_asset_meta(row, no_asset_key, condition_key, 2, asset_meta)?;
    if let Some(reference_symbol) = reference_symbol_for_book_symbol(&row.symbol) {
        reference_assets
            .entry(reference_symbol.to_string())
            .or_default()
            .insert(asset_key);
    }
    Ok(EncodedBookStateIndexRow {
        local_recv_ts_ns: row.local_recv_ts_ns,
        ingest_seq: row.ingest_seq,
        exchange_ts_ms: row.exchange_ts_ms.unwrap_or(NONE_I64),
        symbol_key,
        condition_key,
        asset_key,
        raw_payload_hash_key: dict.raw_payload_hashes.intern(&row.raw_payload_sha256)?,
        book_state_hash_key: dict.book_state_hashes.intern(&row.book_state_hash)?,
        outcome_code,
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

fn update_condition_meta(
    row: &BookCacheRow,
    condition_key: u32,
    symbol_key: u32,
    yes_asset_key: u32,
    no_asset_key: u32,
    condition_meta: &mut BTreeMap<u32, BookStateIndexCondition>,
) -> Result<()> {
    let next = BookStateIndexCondition {
        condition_id: row.condition_id.clone(),
        symbol_key,
        window_start_ts_ns: row.window_start_ts_ns,
        window_end_ts_ns: row.window_end_ts_ns,
        yes_asset_key,
        no_asset_key,
        first_seen_ts_ns: row.local_recv_ts_ns,
        last_seen_ts_ns: row.local_recv_ts_ns,
    };
    if let Some(existing) = condition_meta.get_mut(&condition_key) {
        if existing.condition_id != next.condition_id
            || existing.symbol_key != next.symbol_key
            || existing.window_start_ts_ns != next.window_start_ts_ns
            || existing.window_end_ts_ns != next.window_end_ts_ns
            || existing.yes_asset_key != next.yes_asset_key
            || existing.no_asset_key != next.no_asset_key
        {
            bail!(
                "inconsistent HFTBOOK2 market metadata for condition {}",
                row.condition_id
            );
        }
        existing.first_seen_ts_ns = existing.first_seen_ts_ns.min(row.local_recv_ts_ns);
        existing.last_seen_ts_ns = existing.last_seen_ts_ns.max(row.local_recv_ts_ns);
    } else {
        condition_meta.insert(condition_key, next);
    }
    Ok(())
}

fn update_asset_meta(
    row: &BookCacheRow,
    asset_key: u32,
    condition_key: u32,
    outcome_code: u8,
    asset_meta: &mut BTreeMap<u32, BookStateIndexAsset>,
) -> Result<()> {
    let next = BookStateIndexAsset {
        asset_id: row_asset_id_for_key(row, outcome_code),
        condition_key,
        outcome_code,
    };
    if let Some(existing) = asset_meta.get(&asset_key) {
        if existing != &next {
            bail!("inconsistent HFTBOOK2 asset metadata for asset key {asset_key}");
        }
    } else {
        asset_meta.insert(asset_key, next);
    }
    Ok(())
}

fn row_asset_id_for_key(row: &BookCacheRow, outcome_code: u8) -> String {
    if outcome_code == 1 {
        row.yes_asset_id.clone()
    } else if outcome_code == 2 {
        row.no_asset_id.clone()
    } else {
        row.asset_id.clone()
    }
}

fn ordered_values<T>(values: BTreeMap<u32, T>, field: &str) -> Result<Vec<T>> {
    let mut out = Vec::with_capacity(values.len());
    for (expected, (key, value)) in values.into_iter().enumerate() {
        if key as usize != expected {
            bail!("HFTIDX1 sparse {field} dictionary at key {key}");
        }
        out.push(value);
    }
    Ok(out)
}

fn asset_ranges_from_spools(
    asset_rows: &BTreeMap<u32, AssetSpoolMeta>,
) -> Result<Vec<BookStateIndexAssetRange>> {
    let mut ranges = Vec::new();
    let mut offset = 0usize;
    for (asset_key, rows) in asset_rows {
        if rows.row_count == 0 {
            continue;
        }
        ranges.push(BookStateIndexAssetRange {
            asset_key: *asset_key,
            offset,
            len: rows.row_count,
            min_ts_ns: rows
                .min_ts_ns
                .ok_or_else(|| anyhow!("missing HFTIDX1 asset min timestamp"))?,
            max_ts_ns: rows
                .max_ts_ns
                .ok_or_else(|| anyhow!("missing HFTIDX1 asset max timestamp"))?,
        });
        offset = offset
            .checked_add(rows.row_count)
            .context("HFTIDX1 asset range offset overflow")?;
    }
    Ok(ranges)
}

fn write_index_file_from_asset_spools(
    path: &Path,
    header: &BookStateIndexHeader,
    spool_root: &Path,
    asset_rows: &BTreeMap<u32, AssetSpoolMeta>,
) -> Result<()> {
    market_data_etl_core::reject_tmp_path(path)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let tmp = path.with_file_name(format!(
        "{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(HFTIDX1_DATA_FILE)
    ));
    let header_bytes = serde_json::to_vec(header)?;
    let header_crc = crc32(&header_bytes);
    let mut row_crc = Crc32::new();
    {
        let mut file = BufWriter::new(
            File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?,
        );
        file.write_all(HFTIDX1_MAGIC)
            .with_context(|| format!("write {}", tmp.display()))?;
        file.write_all(&(header_bytes.len() as u32).to_le_bytes())
            .with_context(|| format!("write {}", tmp.display()))?;
        file.write_all(&header_bytes)
            .with_context(|| format!("write {}", tmp.display()))?;
        file.write_all(&header_crc.to_le_bytes())
            .with_context(|| format!("write {}", tmp.display()))?;
        let mut buffer = vec![0u8; 8 * 1024 * 1024];
        for (asset_key, rows) in asset_rows {
            let path = asset_spool_path(spool_root, *asset_key);
            let expected_len = rows
                .row_count
                .checked_mul(HFTIDX1_ROW_BYTES)
                .context("HFTIDX1 asset spool byte length overflow")?;
            let actual_len =
                usize::try_from(fs::metadata(&path)?.len()).context("HFTIDX1 spool too large")?;
            if actual_len != expected_len {
                bail!(
                    "HFTIDX1 asset spool length mismatch for asset {}: actual={} expected={}",
                    asset_key,
                    actual_len,
                    expected_len
                );
            }
            let mut input =
                File::open(&path).with_context(|| format!("open {}", path.display()))?;
            loop {
                let read = input
                    .read(&mut buffer)
                    .with_context(|| format!("read {}", path.display()))?;
                if read == 0 {
                    break;
                }
                row_crc.update(&buffer[..read]);
                file.write_all(&buffer[..read])
                    .with_context(|| format!("write {}", tmp.display()))?;
            }
        }
        file.write_all(&row_crc.finalize().to_le_bytes())
            .with_context(|| format!("write {}", tmp.display()))?;
        file.flush()
            .with_context(|| format!("flush {}", tmp.display()))?;
        file.get_ref()
            .sync_all()
            .with_context(|| format!("fsync {}", tmp.display()))?;
    }
    {
        let mut file = File::open(&tmp).with_context(|| format!("open {}", tmp.display()))?;
        let parsed = read_index_file_header(&mut file)?;
        if parsed.header.row_count != header.row_count {
            bail!("HFTIDX1 streaming write verification row count mismatch");
        }
    }
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} to {}", tmp.display(), path.display()))?;
    if let Some(parent) = path.parent() {
        market_data_etl_core::fsync_dir(parent)?;
    }
    Ok(())
}

fn asset_spool_path(root: &Path, asset_key: u32) -> PathBuf {
    root.join(format!("asset-{asset_key:08x}.hfi1spool"))
}

fn read_index_payload_fast(path: &Path, verify_row_crc: bool) -> Result<FastIndexPayload> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let mut cursor = 0usize;
    if bytes.get(..HFTIDX1_MAGIC.len()) != Some(HFTIDX1_MAGIC.as_slice()) {
        bail!("invalid HFTIDX1 magic");
    }
    cursor += HFTIDX1_MAGIC.len();
    let header_len = read_u32(&bytes, &mut cursor)? as usize;
    let header_end = cursor
        .checked_add(header_len)
        .context("HFTIDX1 header length overflow")?;
    let header_bytes = bytes
        .get(cursor..header_end)
        .ok_or_else(|| anyhow!("truncated HFTIDX1 header"))?;
    cursor = header_end;
    let expected_header_crc = read_u32(&bytes, &mut cursor)?;
    if crc32(header_bytes) != expected_header_crc {
        bail!("HFTIDX1 header CRC mismatch");
    }
    let header: BookStateIndexHeader = serde_json::from_slice(header_bytes)?;
    if header.dataset_format != HFTIDX1_FORMAT {
        bail!("unsupported HFTIDX1 format {}", header.dataset_format);
    }
    let row_start = cursor;
    let row_end = bytes
        .len()
        .checked_sub(4)
        .ok_or_else(|| anyhow!("truncated HFTIDX1 row CRC"))?;
    if row_end < row_start {
        bail!("truncated HFTIDX1 rows");
    }
    if verify_row_crc {
        let expected_row_crc = u32::from_le_bytes(bytes[row_end..].try_into().unwrap());
        if crc32(&bytes[row_start..row_end]) != expected_row_crc {
            bail!("HFTIDX1 row CRC mismatch");
        }
    }
    let row_bytes = infer_row_bytes(row_end - row_start, header.row_count)?;
    Ok(FastIndexPayload {
        header,
        bytes,
        row_start,
        row_end,
        row_bytes,
    })
}

fn read_index_file_header(file: &mut File) -> Result<IndexFileHeader> {
    file.seek(SeekFrom::Start(0))?;
    let mut magic = [0u8; HFTIDX1_MAGIC.len()];
    file.read_exact(&mut magic).context("read HFTIDX1 magic")?;
    if magic != *HFTIDX1_MAGIC {
        bail!("invalid HFTIDX1 magic");
    }
    let header_len = read_u32_from_file(file)? as usize;
    let mut header_bytes = vec![0u8; header_len];
    file.read_exact(&mut header_bytes)
        .context("read HFTIDX1 header")?;
    let expected_header_crc = read_u32_from_file(file)?;
    if crc32(&header_bytes) != expected_header_crc {
        bail!("HFTIDX1 header CRC mismatch");
    }
    let header: BookStateIndexHeader = serde_json::from_slice(&header_bytes)?;
    if header.dataset_format != HFTIDX1_FORMAT {
        bail!("unsupported HFTIDX1 format {}", header.dataset_format);
    }
    let row_start = HFTIDX1_MAGIC
        .len()
        .checked_add(4)
        .and_then(|value| value.checked_add(header_len))
        .and_then(|value| value.checked_add(4))
        .context("HFTIDX1 row start offset overflow")?;
    let file_len = usize::try_from(file.metadata()?.len()).context("HFTIDX1 file too large")?;
    let row_end = file_len
        .checked_sub(4)
        .ok_or_else(|| anyhow!("truncated HFTIDX1 row CRC"))?;
    if row_end < row_start {
        bail!("truncated HFTIDX1 rows");
    }
    let row_bytes = infer_row_bytes(row_end - row_start, header.row_count)?;
    Ok(IndexFileHeader {
        header,
        row_start,
        row_end,
        row_bytes,
    })
}

fn read_rows_from_file(
    file: &mut File,
    row_start: usize,
    row_bytes: usize,
    first_row_idx: usize,
    row_count: usize,
) -> Result<Vec<BookStateIndexRow>> {
    if row_count == 0 {
        return Ok(Vec::new());
    }
    let byte_len = row_count
        .checked_mul(row_bytes)
        .context("HFTIDX1 row slice length overflow")?;
    let offset = row_start
        .checked_add(row_offset(first_row_idx, row_bytes)?)
        .context("HFTIDX1 row slice offset overflow")?;
    file.seek(SeekFrom::Start(offset as u64))?;
    let mut bytes = vec![0u8; byte_len];
    file.read_exact(&mut bytes)
        .context("read HFTIDX1 row slice")?;
    let mut rows = Vec::with_capacity(row_count);
    for local_idx in 0..row_count {
        let start = local_idx
            .checked_mul(row_bytes)
            .context("HFTIDX1 local row offset overflow")?;
        rows.push(decode_row_bytes(
            &bytes[start..start + row_bytes],
            first_row_idx + local_idx,
        )?);
    }
    Ok(rows)
}

fn read_row_from_file(
    file: &mut File,
    row_start: usize,
    row_end: usize,
    row_bytes: usize,
    row_idx: usize,
) -> Result<BookStateIndexRow> {
    let offset = row_start
        .checked_add(row_offset(row_idx, row_bytes)?)
        .context("HFTIDX1 row offset overflow")?;
    let end = offset
        .checked_add(row_bytes)
        .context("HFTIDX1 row end offset overflow")?;
    if end > row_end {
        bail!("HFTIDX1 row index out of range");
    }
    file.seek(SeekFrom::Start(offset as u64))?;
    let mut bytes = vec![0u8; row_bytes];
    file.read_exact(&mut bytes).context("read HFTIDX1 row")?;
    decode_row_bytes(&bytes, row_idx)
}

fn lower_bound_ts_file(
    file: &mut File,
    row_start: usize,
    row_bytes: usize,
    offset: usize,
    len: usize,
    target: i64,
) -> Result<usize> {
    let mut lo = 0usize;
    let mut hi = len;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if row_ts_file(file, row_start, row_bytes, offset + mid)? < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn upper_bound_ts_file(
    file: &mut File,
    row_start: usize,
    row_bytes: usize,
    offset: usize,
    len: usize,
    target: i64,
) -> Result<usize> {
    let mut lo = 0usize;
    let mut hi = len;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if row_ts_file(file, row_start, row_bytes, offset + mid)? <= target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn row_ts_file(file: &mut File, row_start: usize, row_bytes: usize, row_idx: usize) -> Result<i64> {
    let offset = row_start
        .checked_add(row_offset(row_idx, row_bytes)?)
        .context("HFTIDX1 timestamp offset overflow")?;
    file.seek(SeekFrom::Start(offset as u64))?;
    let mut raw = [0u8; 8];
    file.read_exact(&mut raw)
        .context("read HFTIDX1 row timestamp")?;
    Ok(i64::from_le_bytes(raw))
}

fn push_encoded_row(out: &mut Vec<u8>, row: &EncodedBookStateIndexRow) {
    push_i64(out, row.local_recv_ts_ns);
    push_u64(out, row.ingest_seq);
    push_i64(out, row.exchange_ts_ms);
    push_u32(out, row.symbol_key);
    push_u32(out, row.condition_key);
    push_u32(out, row.asset_key);
    push_u32(out, row.raw_payload_hash_key);
    push_u32(out, row.book_state_hash_key);
    out.push(row.outcome_code);
    push_i64(out, row.window_start_ts_ns);
    push_i64(out, row.window_end_ts_ns);
    push_i64(out, row.best_bid_price_micros);
    push_i64(out, row.best_ask_price_micros);
    for value in row.bid_price {
        push_i64(out, value);
    }
    for value in row.ask_price {
        push_i64(out, value);
    }
    for value in row.bid_size {
        push_i64(out, value);
    }
    for value in row.ask_size {
        push_i64(out, value);
    }
}

fn decode_row_at(
    row_bytes: &[u8],
    row_bytes_per_row: usize,
    row_idx: usize,
) -> Result<BookStateIndexRow> {
    let offset = row_offset(row_idx, row_bytes_per_row)?;
    decode_row_bytes(
        row_bytes
            .get(offset..offset + row_bytes_per_row)
            .ok_or_else(|| anyhow!("truncated HFTIDX1 row"))?,
        row_idx,
    )
}

fn decode_row_bytes(row_bytes: &[u8], row_idx: usize) -> Result<BookStateIndexRow> {
    let mut cursor = 0usize;
    let local_recv_ts_ns = read_i64(row_bytes, &mut cursor)?;
    let ingest_seq = read_u64(row_bytes, &mut cursor)?;
    let legacy = row_bytes.len() == LEGACY_HFTIDX1_ROW_BYTES;
    let exchange_ts_ms = if legacy {
        None
    } else {
        optional_i64(read_i64(row_bytes, &mut cursor)?)
    };
    let symbol_key = read_u32(row_bytes, &mut cursor)?;
    let condition_key = read_u32(row_bytes, &mut cursor)?;
    let asset_key = read_u32(row_bytes, &mut cursor)?;
    let (raw_payload_hash_key, book_state_hash_key) = if legacy {
        (0, 0)
    } else {
        (
            read_u32(row_bytes, &mut cursor)?,
            read_u32(row_bytes, &mut cursor)?,
        )
    };
    let outcome_code = *row_bytes
        .get(cursor)
        .ok_or_else(|| anyhow!("truncated HFTIDX1 outcome"))?;
    cursor += 1;
    let window_start_ts_ns = read_i64(row_bytes, &mut cursor)?;
    let window_end_ts_ns = read_i64(row_bytes, &mut cursor)?;
    let best_bid_price_micros = optional_i64(read_i64(row_bytes, &mut cursor)?);
    let best_ask_price_micros = optional_i64(read_i64(row_bytes, &mut cursor)?);
    let bid_price = read_i64_array(row_bytes, &mut cursor)?;
    let ask_price = read_i64_array(row_bytes, &mut cursor)?;
    let bid_size = read_i64_array(row_bytes, &mut cursor)?;
    let ask_size = read_i64_array(row_bytes, &mut cursor)?;
    Ok(BookStateIndexRow {
        row_idx,
        local_recv_ts_ns,
        ingest_seq,
        exchange_ts_ms,
        symbol_key,
        condition_key,
        asset_key,
        raw_payload_hash_key,
        book_state_hash_key,
        outcome_code,
        window_start_ts_ns,
        window_end_ts_ns,
        best_bid_price_micros,
        best_ask_price_micros,
        bid_levels: decode_levels(bid_price, bid_size),
        ask_levels: decode_levels(ask_price, ask_size),
    })
}

fn validate_catalog_matches_header(
    catalog: &BookStateIndexCatalog,
    header: &BookStateIndexHeader,
) -> Result<()> {
    if catalog.row_count != header.row_count
        || catalog.asset_count != header.assets.len()
        || catalog.condition_count != header.conditions.len()
        || catalog.symbol_count != header.symbols.len()
    {
        bail!("HFTIDX1 catalog/header count mismatch");
    }
    Ok(())
}

fn validate_header(header: &BookStateIndexHeader) -> Result<()> {
    if header.dataset_format != HFTIDX1_FORMAT {
        bail!("unsupported HFTIDX1 format {}", header.dataset_format);
    }
    let range_rows = header.asset_ranges.iter().try_fold(0usize, |acc, range| {
        acc.checked_add(range.len)
            .context("HFTIDX1 asset range row count overflow")
    })?;
    if range_rows != header.row_count {
        bail!("HFTIDX1 asset ranges do not cover all rows");
    }
    for (idx, range) in header.asset_ranges.iter().enumerate() {
        dictionary_get(&header.assets, range.asset_key, "asset")?;
        if range.offset != range_rows_before(&header.asset_ranges, idx) {
            bail!("HFTIDX1 non-contiguous asset range at {idx}");
        }
        if range.len == 0 || range.max_ts_ns < range.min_ts_ns {
            bail!("HFTIDX1 invalid asset range at {idx}");
        }
    }
    for condition in &header.conditions {
        dictionary_get(&header.symbols, condition.symbol_key, "symbol")?;
        dictionary_get(&header.assets, condition.yes_asset_key, "yes_asset")?;
        dictionary_get(&header.assets, condition.no_asset_key, "no_asset")?;
        if condition.window_end_ts_ns <= condition.window_start_ts_ns {
            bail!(
                "HFTIDX1 invalid market window for {}",
                condition.condition_id
            );
        }
    }
    for asset in &header.assets {
        dictionary_get(&header.conditions, asset.condition_key, "condition")?;
    }
    Ok(())
}

fn validate_rows_from_file(file: &mut File, index_file: &IndexFileHeader) -> Result<()> {
    let expected_len = index_file
        .header
        .row_count
        .checked_mul(index_file.row_bytes)
        .context("HFTIDX1 row byte length overflow")?;
    if index_file.row_end - index_file.row_start != expected_len {
        bail!("HFTIDX1 fixed row byte length mismatch");
    }

    let chunk_rows = (8 * 1024 * 1024 / index_file.row_bytes).max(1);
    let mut buffer = vec![0u8; chunk_rows * index_file.row_bytes];
    let mut row_crc = Crc32::new();
    for range in &index_file.header.asset_ranges {
        let mut previous = None::<(i64, u64)>;
        let mut remaining = range.len;
        let mut row_idx = range.offset;
        let offset = index_file
            .row_start
            .checked_add(row_offset(range.offset, index_file.row_bytes)?)
            .context("HFTIDX1 validation range offset overflow")?;
        file.seek(SeekFrom::Start(offset as u64))?;
        while remaining > 0 {
            let rows_to_read = remaining.min(chunk_rows);
            let bytes_to_read = rows_to_read
                .checked_mul(index_file.row_bytes)
                .context("HFTIDX1 validation chunk overflow")?;
            let bytes = &mut buffer[..bytes_to_read];
            file.read_exact(bytes).context("read HFTIDX1 rows")?;
            row_crc.update(bytes);
            for local_idx in 0..rows_to_read {
                let start = local_idx
                    .checked_mul(index_file.row_bytes)
                    .context("HFTIDX1 validation row offset overflow")?;
                let row = decode_row_bytes(&bytes[start..start + index_file.row_bytes], row_idx)?;
                if row.asset_key != range.asset_key {
                    bail!("HFTIDX1 row asset key mismatch");
                }
                if let Some(prev) = previous {
                    if (row.local_recv_ts_ns, row.ingest_seq) < prev {
                        bail!("HFTIDX1 asset series is not time sorted");
                    }
                }
                previous = Some((row.local_recv_ts_ns, row.ingest_seq));
                row_idx += 1;
            }
            remaining -= rows_to_read;
        }
    }
    file.seek(SeekFrom::Start(index_file.row_end as u64))?;
    let expected_crc = read_u32_from_file(file)?;
    let actual_crc = row_crc.finalize();
    if actual_crc != expected_crc {
        bail!("HFTIDX1 row CRC mismatch");
    }
    Ok(())
}

fn range_rows_before(ranges: &[BookStateIndexAssetRange], idx: usize) -> usize {
    ranges[..idx].iter().map(|range| range.len).sum()
}

fn range_overlaps(
    range: &BookStateIndexAssetRange,
    start_ts_ns: Option<i64>,
    end_ts_ns: Option<i64>,
) -> bool {
    start_ts_ns.is_none_or(|start| range.max_ts_ns >= start)
        && end_ts_ns.is_none_or(|end| range.min_ts_ns < end)
}

fn dictionary_get<T>(values: &[T], key: u32, field: &str) -> Result<()> {
    values
        .get(key as usize)
        .map(|_| ())
        .ok_or_else(|| anyhow!("HFTIDX1 {field} key out of range"))
}

fn prepare_root(index_root: &Path, overwrite: bool) -> Result<()> {
    if index_root.exists() {
        if !overwrite {
            bail!("HFTIDX1 root already exists: {}", index_root.display());
        }
        fs::remove_dir_all(index_root)
            .with_context(|| format!("remove {}", index_root.display()))?;
    }
    fs::create_dir_all(index_root).with_context(|| format!("create {}", index_root.display()))?;
    Ok(())
}

fn write_catalog(index_root: &Path, catalog: &BookStateIndexCatalog) -> Result<()> {
    market_data_etl_core::write_json_file_pretty(&index_root.join(HFTIDX1_CATALOG), catalog)
}

fn reference_symbol_for_book_symbol(market_symbol: &str) -> Option<&'static str> {
    let upper = market_symbol.to_ascii_uppercase();
    if upper.starts_with("BTC") {
        Some("BTCUSDT")
    } else if upper.starts_with("ETH") {
        Some("ETHUSDT")
    } else if upper.starts_with("SOL") {
        Some("SOLUSDT")
    } else {
        None
    }
}

fn optional_i64(value: i64) -> Option<i64> {
    if value == NONE_I64 {
        None
    } else {
        Some(value)
    }
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

fn row_offset(row_idx: usize, row_bytes: usize) -> Result<usize> {
    row_idx
        .checked_mul(row_bytes)
        .context("HFTIDX1 row offset overflow")
}

fn infer_row_bytes(row_payload_len: usize, row_count: usize) -> Result<usize> {
    if row_count == 0 {
        return Ok(HFTIDX1_ROW_BYTES);
    }
    let current_len = row_count
        .checked_mul(HFTIDX1_ROW_BYTES)
        .context("HFTIDX1 current row byte length overflow")?;
    if row_payload_len == current_len {
        return Ok(HFTIDX1_ROW_BYTES);
    }
    let legacy_len = row_count
        .checked_mul(LEGACY_HFTIDX1_ROW_BYTES)
        .context("HFTIDX1 legacy row byte length overflow")?;
    if row_payload_len == legacy_len {
        return Ok(LEGACY_HFTIDX1_ROW_BYTES);
    }
    bail!(
        "HFTIDX1 fixed row byte length mismatch: payload={} current_expected={} legacy_expected={}",
        row_payload_len,
        current_len,
        legacy_len
    )
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
        .ok_or_else(|| anyhow!("truncated HFTIDX1 u32"))?;
    *cursor += 4;
    Ok(u32::from_le_bytes(out.try_into().unwrap()))
}

fn read_u32_from_file(file: &mut File) -> Result<u32> {
    let mut raw = [0u8; 4];
    file.read_exact(&mut raw).context("read HFTIDX1 u32")?;
    Ok(u32::from_le_bytes(raw))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    let out = bytes
        .get(*cursor..*cursor + 8)
        .ok_or_else(|| anyhow!("truncated HFTIDX1 u64"))?;
    *cursor += 8;
    Ok(u64::from_le_bytes(out.try_into().unwrap()))
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64> {
    let out = bytes
        .get(*cursor..*cursor + 8)
        .ok_or_else(|| anyhow!("truncated HFTIDX1 i64"))?;
    *cursor += 8;
    Ok(i64::from_le_bytes(out.try_into().unwrap()))
}

fn read_i64_array(bytes: &[u8], cursor: &mut usize) -> Result<[i64; 10]> {
    let mut out = [0i64; 10];
    for value in &mut out {
        *value = read_i64(bytes, cursor)?;
    }
    Ok(out)
}

impl Dict {
    fn intern(&mut self, value: &str) -> Result<u32> {
        if let Some(key) = self.by_value.get(value) {
            return Ok(*key);
        }
        let key = u32::try_from(self.values.len()).context("HFTIDX1 dictionary overflow")?;
        self.values.push(value.to_string());
        self.by_value.insert(value.to_string(), key);
        Ok(key)
    }
}
