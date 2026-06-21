use crate::constants::{TABLE_BOOK_TOP10, TABLE_MARKET_DIM};
use crate::facts::market_dim_rows_from_book_rows;
use crate::types::*;
use anyhow::{bail, Context, Result};
use market_data_etl_core::{
    hash_path, now_unix_ns, verify_parquet_zstd_table, write_json_file_pretty, write_parquet_table,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Instant;

pub const BOOK_STATE_CACHE_CATALOG: &str = "catalog.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildBookStateCacheOptions {
    pub raw_roots: Vec<PathBuf>,
    pub cache_root: PathBuf,
    pub raw_start_ts_ns: Option<i64>,
    pub raw_end_ts_ns: Option<i64>,
    pub market_symbol_allowlist: Vec<String>,
    pub overwrite: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookStateCacheCatalog {
    pub schema_version: u32,
    pub dataset_format: String,
    pub cache_root: PathBuf,
    pub raw_roots: Vec<PathBuf>,
    pub raw_start_ts_ns: Option<i64>,
    pub raw_end_ts_ns: Option<i64>,
    pub market_symbol_allowlist: Vec<String>,
    pub generated_ts_ns: i64,
    pub book_table: String,
    pub market_table: String,
    pub book_table_hash: String,
    pub market_table_hash: String,
    pub book_row_count: usize,
    pub market_row_count: usize,
    #[serde(default)]
    pub partitions: Vec<BookStateCachePartition>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookStateCachePartition {
    pub partition_id: String,
    pub book_table: String,
    pub market_table: String,
    pub book_table_hash: String,
    pub market_table_hash: String,
    pub book_row_count: usize,
    pub market_row_count: usize,
    pub min_ts_ns: Option<i64>,
    pub max_ts_ns: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookStateCacheBuildReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub cache_root: PathBuf,
    pub reused_existing: bool,
    pub catalog_path: PathBuf,
    pub catalog_hash: String,
    pub book_row_count: usize,
    pub market_row_count: usize,
    pub elapsed_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendBookStateCachePartitionOptions {
    pub cache_root: PathBuf,
    pub partition_id: String,
    pub raw_roots: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookStateCacheAppendReport {
    pub schema_version: u32,
    pub dataset_format: String,
    pub cache_root: PathBuf,
    pub partition_id: String,
    pub book_row_count: usize,
    pub market_row_count: usize,
    pub catalog_hash: String,
}

pub fn build_book_state_cache(
    options: &BuildBookStateCacheOptions,
) -> Result<BookStateCacheBuildReport> {
    validate_cache_build_options(options)?;
    let started = Instant::now();
    let hftbook2_catalog_path = options
        .cache_root
        .join(pm5m_market_cache::BOOK_CACHE2_CATALOG);
    let legacy_catalog_path = options.cache_root.join(BOOK_STATE_CACHE_CATALOG);
    let catalog_path = if hftbook2_catalog_path.exists() {
        hftbook2_catalog_path.clone()
    } else {
        legacy_catalog_path.clone()
    };
    if catalog_path.exists() && !options.overwrite {
        if let Ok(catalog) = pm5m_market_cache::read_book_cache2_catalog(&options.cache_root) {
            return Ok(BookStateCacheBuildReport {
                schema_version: 1,
                dataset_format: pm5m_market_cache::BOOK_CACHE2_FORMAT.to_string(),
                cache_root: options.cache_root.clone(),
                reused_existing: true,
                catalog_hash: hash_path(&hftbook2_catalog_path)?,
                catalog_path: hftbook2_catalog_path,
                book_row_count: catalog.row_count,
                market_row_count: hftbook2_market_count(&options.cache_root)?,
                elapsed_ms: started.elapsed().as_millis(),
            });
        }
        let catalog = read_book_state_cache_catalog(&options.cache_root)?;
        return Ok(BookStateCacheBuildReport {
            schema_version: 1,
            dataset_format: BOOK_STATE_CACHE_FORMAT.to_string(),
            cache_root: options.cache_root.clone(),
            reused_existing: true,
            catalog_hash: hash_path(&catalog_path)?,
            catalog_path,
            book_row_count: catalog.book_row_count,
            market_row_count: catalog.market_row_count,
            elapsed_ms: started.elapsed().as_millis(),
        });
    }

    let report = pm5m_market_cache::build_book_cache(&pm5m_market_cache::BuildBookCacheOptions {
        raw_roots: options.raw_roots.clone(),
        cache_root: options.cache_root.clone(),
        raw_start_ts_ns: options.raw_start_ts_ns,
        raw_end_ts_ns: options.raw_end_ts_ns,
        market_symbol_allowlist: options.market_symbol_allowlist.clone(),
        overwrite: options.overwrite,
        replay_workers: None,
        enrich_missing_clob_metadata: false,
        clob_metadata_cache_root: None,
        condition_allowlist_path: None,
        poly_server_visible_time: false,
        poly_incremental_latency_ms: pm5m_market_cache::DEFAULT_POLY_INCREMENTAL_LATENCY_MS,
        poly_incremental_freshness_guard_ms:
            pm5m_market_cache::DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS,
    })
    .context("build HFTBOOK2 book state cache from raw")?;
    let _catalog = pm5m_market_cache::read_book_cache2_catalog(&options.cache_root)?;

    Ok(BookStateCacheBuildReport {
        schema_version: 1,
        dataset_format: pm5m_market_cache::BOOK_CACHE2_FORMAT.to_string(),
        cache_root: options.cache_root.clone(),
        reused_existing: false,
        catalog_hash: hash_path(&hftbook2_catalog_path)?,
        catalog_path: hftbook2_catalog_path,
        book_row_count: report.row_count,
        market_row_count: hftbook2_market_count(&options.cache_root)?,
        elapsed_ms: report.elapsed_ms,
    })
}

fn hftbook2_market_count(cache_root: &Path) -> Result<usize> {
    let mut conditions = std::collections::BTreeSet::<String>::new();
    pm5m_market_cache::scan_book_cache2(
        cache_root,
        None,
        None,
        &pm5m_market_cache::BookCache2ScanFilter::default(),
        |row| {
            conditions.insert(row.condition_id);
            Ok(())
        },
    )?;
    Ok(conditions.len())
}

pub fn append_book_state_cache_partition(
    options: &AppendBookStateCachePartitionOptions,
    book_rows: &[PolymarketBookTop10Row],
) -> Result<BookStateCacheAppendReport> {
    if book_rows.is_empty() {
        bail!("cannot append empty book state cache partition");
    }
    validate_partition_id(&options.partition_id)?;
    std::fs::create_dir_all(&options.cache_root)
        .with_context(|| format!("create {}", options.cache_root.display()))?;

    let partition_root = options
        .cache_root
        .join("partitions")
        .join(format!("partition_id={}", options.partition_id));
    if partition_root.exists() {
        bail!(
            "book state cache partition already exists: {}",
            options.partition_id
        );
    }

    let book_table = partition_root.join(TABLE_BOOK_TOP10);
    let market_table = partition_root.join(TABLE_MARKET_DIM);
    let mut sorted_books = book_rows.to_vec();
    sorted_books.sort_by(|a, b| {
        (a.local_recv_ts_ns, a.ingest_seq, &a.primary_key).cmp(&(
            b.local_recv_ts_ns,
            b.ingest_seq,
            &b.primary_key,
        ))
    });
    let market_rows = market_dim_rows_from_book_rows(&sorted_books)?;
    let book_report = write_parquet_table(&book_table, &sorted_books, Some("local_recv_ts_ns"))?;
    let market_report =
        write_parquet_table(&market_table, &market_rows, Some("window_start_ts_ns"))?;

    let mut catalog = if options.cache_root.join(BOOK_STATE_CACHE_CATALOG).exists() {
        read_book_state_cache_catalog(&options.cache_root)?
    } else {
        BookStateCacheCatalog {
            schema_version: 1,
            dataset_format: BOOK_STATE_CACHE_FORMAT.to_string(),
            cache_root: options.cache_root.clone(),
            raw_roots: options.raw_roots.clone(),
            raw_start_ts_ns: None,
            raw_end_ts_ns: None,
            market_symbol_allowlist: Vec::new(),
            generated_ts_ns: now_unix_ns() as i64,
            book_table: String::new(),
            market_table: String::new(),
            book_table_hash: String::new(),
            market_table_hash: String::new(),
            book_row_count: 0,
            market_row_count: 0,
            partitions: Vec::new(),
        }
    };
    let partition = BookStateCachePartition {
        partition_id: options.partition_id.clone(),
        book_table: relative_table_path(&options.cache_root, &book_table)?,
        market_table: relative_table_path(&options.cache_root, &market_table)?,
        book_table_hash: hash_path(&book_table)?,
        market_table_hash: hash_path(&market_table)?,
        book_row_count: book_report.row_count,
        market_row_count: market_report.row_count,
        min_ts_ns: sorted_books.iter().map(|row| row.local_recv_ts_ns).min(),
        max_ts_ns: sorted_books.iter().map(|row| row.local_recv_ts_ns).max(),
    };
    if catalog
        .partitions
        .iter()
        .any(|existing| existing.partition_id == partition.partition_id)
    {
        bail!(
            "book state cache partition already listed: {}",
            partition.partition_id
        );
    }

    catalog.raw_roots = merge_raw_roots(catalog.raw_roots, &options.raw_roots);
    catalog.book_row_count = catalog
        .book_row_count
        .checked_add(partition.book_row_count)
        .context("book state cache row count overflow")?;
    catalog.market_row_count = catalog
        .market_row_count
        .checked_add(partition.market_row_count)
        .context("book state cache market row count overflow")?;
    catalog.partitions.push(partition);
    catalog
        .partitions
        .sort_by(|a, b| a.partition_id.cmp(&b.partition_id));

    let catalog_path = options.cache_root.join(BOOK_STATE_CACHE_CATALOG);
    write_json_file_pretty(&catalog_path, &catalog)?;
    Ok(BookStateCacheAppendReport {
        schema_version: 1,
        dataset_format: BOOK_STATE_CACHE_FORMAT.to_string(),
        cache_root: options.cache_root.clone(),
        partition_id: options.partition_id.clone(),
        book_row_count: book_report.row_count,
        market_row_count: market_report.row_count,
        catalog_hash: hash_path(&catalog_path)?,
    })
}

pub fn read_book_state_cache_catalog(cache_root: &Path) -> Result<BookStateCacheCatalog> {
    let catalog_path = cache_root.join(BOOK_STATE_CACHE_CATALOG);
    let file = std::fs::File::open(&catalog_path)
        .with_context(|| format!("open book state cache catalog {}", catalog_path.display()))?;
    let catalog: BookStateCacheCatalog = serde_json::from_reader(file)
        .with_context(|| format!("parse book state cache catalog {}", catalog_path.display()))?;
    if catalog.dataset_format != BOOK_STATE_CACHE_FORMAT {
        bail!(
            "unsupported book state cache format {}, expected {}",
            catalog.dataset_format,
            BOOK_STATE_CACHE_FORMAT
        );
    }
    if !catalog.book_table.is_empty() {
        verify_parquet_zstd_table(&cache_root.join(&catalog.book_table), &catalog.book_table)?;
        verify_parquet_zstd_table(
            &cache_root.join(&catalog.market_table),
            &catalog.market_table,
        )?;
        let book_hash = hash_path(&cache_root.join(&catalog.book_table))?;
        if book_hash != catalog.book_table_hash {
            bail!("book state cache book table hash mismatch");
        }
        let market_hash = hash_path(&cache_root.join(&catalog.market_table))?;
        if market_hash != catalog.market_table_hash {
            bail!("book state cache market table hash mismatch");
        }
    }
    for partition in &catalog.partitions {
        verify_cache_partition(cache_root, partition)?;
    }
    Ok(catalog)
}

pub(crate) fn book_state_cache_book_table_paths(
    cache_root: &Path,
    catalog: &BookStateCacheCatalog,
) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if !catalog.book_table.is_empty() {
        out.push(cache_root.join(&catalog.book_table));
    }
    out.extend(
        catalog
            .partitions
            .iter()
            .map(|partition| cache_root.join(&partition.book_table)),
    );
    out
}

fn validate_cache_build_options(options: &BuildBookStateCacheOptions) -> Result<()> {
    if options.raw_roots.is_empty() {
        bail!("build-book-state-cache requires at least one raw root");
    }
    if let (Some(start), Some(end)) = (options.raw_start_ts_ns, options.raw_end_ts_ns) {
        if end <= start {
            bail!("raw_end_ts_ns must be greater than raw_start_ts_ns");
        }
    }
    Ok(())
}

fn verify_cache_partition(cache_root: &Path, partition: &BookStateCachePartition) -> Result<()> {
    let book_table = cache_root.join(&partition.book_table);
    let market_table = cache_root.join(&partition.market_table);
    verify_parquet_zstd_table(&book_table, &partition.book_table)?;
    verify_parquet_zstd_table(&market_table, &partition.market_table)?;
    if hash_path(&book_table)? != partition.book_table_hash {
        bail!(
            "book state cache partition {} book table hash mismatch",
            partition.partition_id
        );
    }
    if hash_path(&market_table)? != partition.market_table_hash {
        bail!(
            "book state cache partition {} market table hash mismatch",
            partition.partition_id
        );
    }
    Ok(())
}

fn relative_table_path(root: &Path, table: &Path) -> Result<String> {
    Ok(table
        .strip_prefix(root)
        .with_context(|| format!("strip prefix {} from {}", root.display(), table.display()))?
        .to_string_lossy()
        .replace('\\', "/"))
}

fn merge_raw_roots(mut existing: Vec<PathBuf>, additions: &[PathBuf]) -> Vec<PathBuf> {
    for raw_root in additions {
        if !existing.iter().any(|item| item == raw_root) {
            existing.push(raw_root.clone());
        }
    }
    existing
}

fn validate_partition_id(partition_id: &str) -> Result<()> {
    if partition_id.is_empty()
        || !partition_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        bail!("partition_id must contain only ascii letters, digits, '-' or '_'");
    }
    Ok(())
}
