use anyhow::{bail, Result};
use market_data_etl_core::hash_path;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Instant;

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

pub fn build_book_state_cache(
    options: &BuildBookStateCacheOptions,
) -> Result<BookStateCacheBuildReport> {
    validate_cache_build_options(options)?;
    let started = Instant::now();
    let catalog_path = options
        .cache_root
        .join(pm5m_market_cache::BOOK_CACHE2_CATALOG);

    if catalog_path.exists() && !options.overwrite {
        let catalog = pm5m_market_cache::read_book_cache2_catalog(&options.cache_root)?;
        validate_existing_hftbook2_matches_options(&catalog, options)?;
        let validation = pm5m_market_cache::validate_book_cache2(&options.cache_root)?;
        return Ok(BookStateCacheBuildReport {
            schema_version: 1,
            dataset_format: pm5m_market_cache::BOOK_CACHE2_FORMAT.to_string(),
            cache_root: options.cache_root.clone(),
            reused_existing: true,
            catalog_hash: validation.catalog_hash,
            catalog_path,
            book_row_count: validation.row_count,
            market_row_count: hftbook2_market_count(&options.cache_root)?,
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
        poly_server_visible_time: true,
        poly_incremental_latency_ms: pm5m_market_cache::DEFAULT_POLY_INCREMENTAL_LATENCY_MS,
        poly_incremental_freshness_guard_ms:
            pm5m_market_cache::DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS,
    })?;
    let validation = pm5m_market_cache::validate_book_cache2(&options.cache_root)?;

    Ok(BookStateCacheBuildReport {
        schema_version: 1,
        dataset_format: pm5m_market_cache::BOOK_CACHE2_FORMAT.to_string(),
        cache_root: options.cache_root.clone(),
        reused_existing: false,
        catalog_hash: hash_path(&catalog_path)?,
        catalog_path,
        book_row_count: validation.row_count,
        market_row_count: hftbook2_market_count(&options.cache_root)?,
        elapsed_ms: report.elapsed_ms,
    })
}

fn hftbook2_market_count(cache_root: &std::path::Path) -> Result<usize> {
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

fn validate_existing_hftbook2_matches_options(
    catalog: &pm5m_market_cache::BookCache2Catalog,
    options: &BuildBookStateCacheOptions,
) -> Result<()> {
    if catalog.raw_roots != options.raw_roots
        || catalog.raw_start_ts_ns != options.raw_start_ts_ns
        || catalog.raw_end_ts_ns != options.raw_end_ts_ns
        || catalog.market_symbol_allowlist != options.market_symbol_allowlist
        || !catalog.poly_server_visible_time
        || catalog.poly_incremental_latency_ms
            != pm5m_market_cache::DEFAULT_POLY_INCREMENTAL_LATENCY_MS
        || catalog.poly_incremental_freshness_guard_ms
            != pm5m_market_cache::DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS
    {
        bail!(
            "existing HFTBOOK2 cache was built with different options or non-live timing; pass --overwrite to rebuild"
        );
    }
    Ok(())
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
