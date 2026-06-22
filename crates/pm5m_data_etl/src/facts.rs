use crate::cache::{input_availability_from_cache_record, read_cached_records};
use crate::constants::{
    MATERIALIZATION_REPORT, REFERENCE_LOOKBACK_NS, TABLE_BINANCE_REFERENCE, TABLE_BOOK_TOP10,
    TABLE_INPUT_AVAILABILITY, TABLE_MARKET_DIM, TABLE_SETTLEMENT,
};
use crate::manifest::{
    base_dataset_manifest, cache_group_hash, cache_manifest_path, dataset_path, existing_hash,
    read_cache_manifest, row_hash, write_dataset_manifest,
};
use crate::types::*;
use anyhow::{anyhow, bail, Context, Result};
use market_data_etl_core::{
    hash_path, write_json_file_pretty, write_parquet_table, BookLevel, ParquetTableStreamWriter,
    TableWriteReport,
};
use std::collections::{BTreeMap, BTreeSet};

const DEFAULT_BOOK_PART_ROWS: usize = 100_000;
pub fn build_facts(plan: &PipelinePlan) -> Result<MaterializationReport> {
    std::fs::create_dir_all(&plan.dataset_root)
        .with_context(|| format!("create dataset root {}", plan.dataset_root.display()))?;

    let cache_manifest = read_cache_manifest(plan)?;
    let (book_report, market_report, market_rows) = build_book_and_market_tables(plan)?;
    let binance_rows = build_binance_reference_rows(plan, &cache_manifest, &market_rows)?;
    let settlement_rows = build_settlement_rows(plan, &cache_manifest, &market_rows)?;
    let availability_rows = cache_manifest
        .records
        .iter()
        .map(input_availability_from_cache_record)
        .collect::<Vec<_>>();

    let binance_report = write_parquet_table(
        &dataset_path(plan, TABLE_BINANCE_REFERENCE),
        &binance_rows,
        Some("bar_close_ts_ns"),
    )?;
    let settlement_report = write_parquet_table(
        &dataset_path(plan, TABLE_SETTLEMENT),
        &settlement_rows,
        Some("settled_ts_ns"),
    )?;
    let availability_report = write_parquet_table(
        &dataset_path(plan, TABLE_INPUT_AVAILABILITY),
        &availability_rows,
        Some("start_ts_ns"),
    )?;

    let mut table_hashes = BTreeMap::new();
    table_hashes.insert(TABLE_MARKET_DIM.to_string(), market_report.table_hash);
    table_hashes.insert(TABLE_BOOK_TOP10.to_string(), book_report.table_hash);
    table_hashes.insert(
        TABLE_BINANCE_REFERENCE.to_string(),
        binance_report.table_hash,
    );
    table_hashes.insert(TABLE_SETTLEMENT.to_string(), settlement_report.table_hash);
    table_hashes.insert(
        TABLE_INPUT_AVAILABILITY.to_string(),
        availability_report.table_hash,
    );

    let report = MaterializationReport {
        schema_version: 1,
        dataset_format: MATERIALIZATION_REPORT_FORMAT.to_string(),
        raw_roots: plan.raw_roots.clone(),
        plan_root: plan.plan_root.clone(),
        cache_root: plan.cache_root.clone(),
        dataset_root: plan.dataset_root.clone(),
        cache_manifest_hash: existing_hash(&cache_manifest_path(plan))?,
        table_hashes,
        contains_strategy_fields: false,
        contains_settlement_in_event_stream: false,
    };
    write_json_file_pretty(&plan.dataset_root.join(MATERIALIZATION_REPORT), &report)?;

    let mut manifest = base_dataset_manifest(plan);
    manifest.binance_cache_manifest_hash = Some(cache_group_hash(
        &cache_manifest,
        CacheGroup::Binance1sReference,
    )?);
    manifest.settlement_cache_manifest_hash = Some(cache_group_hash(
        &cache_manifest,
        CacheGroup::PolymarketSettlement,
    )?);
    manifest.fact_table_hash = Some(hash_path(&plan.dataset_root.join("tables"))?);
    write_dataset_manifest(plan, &manifest)?;

    Ok(report)
}

fn build_book_and_market_tables(
    plan: &PipelinePlan,
) -> Result<(TableWriteReport, TableWriteReport, Vec<MarketDimRow>)> {
    if let Some(cache_root) = plan.book_state_cache_root.as_deref() {
        return build_book_and_market_tables_from_hftbook_cache(plan, cache_root);
    }
    bail!("build-facts requires book_state_cache_root pointing to HFTBOOK2 book cache")
}

fn build_book_and_market_tables_from_hftbook_cache(
    plan: &PipelinePlan,
    cache_root: &std::path::Path,
) -> Result<(TableWriteReport, TableWriteReport, Vec<MarketDimRow>)> {
    pm5m_market_cache::read_book_cache2_catalog(cache_root)
        .with_context(|| format!("read HFTBOOK2 book cache {}", cache_root.display()))?;
    let book_path = dataset_path(plan, TABLE_BOOK_TOP10);
    let market_path = dataset_path(plan, TABLE_MARKET_DIM);
    let symbol_filter = MarketSymbolFilter::from_plan(plan)?;
    let part_limit = book_part_row_limit();
    let mut writer = ParquetTableStreamWriter::new(&book_path, Some("local_recv_ts_ns"))?;
    let mut chunk = Vec::<PolymarketBookTop10Row>::with_capacity(part_limit.min(16_384));
    let mut market_acc = MarketDimAccumulator::default();
    let mut output_rows = 0usize;

    pm5m_market_cache::scan_book_cache2(
        cache_root,
        plan.raw_start_ts_ns,
        plan.raw_end_ts_ns,
        &pm5m_market_cache::BookCache2ScanFilter::default(),
        |cache_row| {
            if !symbol_filter.allows(&cache_row.symbol) {
                return Ok(());
            }
            let row = book_fact_row_from_hftbook_cache(&cache_row)?;
            market_acc.update(&row)?;
            chunk.push(row);
            output_rows += 1;
            flush_book_chunk_if_needed(&mut writer, &mut chunk, part_limit)
        },
    )
    .with_context(|| format!("read HFTBOOK2 book cache {}", cache_root.display()))?;

    if output_rows == 0 {
        bail!(
            "book cache {} produced no rows for requested window/market filter",
            cache_root.display()
        );
    }

    flush_book_chunk(&mut writer, &mut chunk)?;
    let book_report = writer.finish()?;
    let market_rows = market_acc.into_rows()?;
    let market_report =
        write_parquet_table(&market_path, &market_rows, Some("window_start_ts_ns"))?;
    eprintln!(
        "canonical Polymarket book state materialized from book cache: {} row(s) across {} byte(s)",
        book_report.row_count, book_report.byte_count
    );
    Ok((book_report, market_report, market_rows))
}

#[derive(Debug, Clone, Default)]
struct MarketSymbolFilter {
    allowed: BTreeSet<String>,
}

impl MarketSymbolFilter {
    fn from_plan(plan: &PipelinePlan) -> Result<Self> {
        let mut allowed = BTreeSet::new();
        for symbol in &plan.market_symbol_allowlist {
            let canonical = canonical_market_symbol(symbol)
                .ok_or_else(|| anyhow!("invalid market symbol allowlist entry '{symbol}'"))?;
            allowed.insert(canonical);
        }
        Ok(Self { allowed })
    }

    fn allows(&self, symbol: &str) -> bool {
        self.allowed.is_empty()
            || canonical_market_symbol(symbol)
                .as_ref()
                .is_some_and(|canonical| self.allowed.contains(canonical))
    }
}

fn book_fact_row_from_hftbook_cache(
    cache_row: &pm5m_market_cache::BookCacheRow,
) -> Result<PolymarketBookTop10Row> {
    let bids = cache_row.bid_levels.map(|level| {
        level
            .map(|level| (Some(level.price_micros), Some(level.qty_micros)))
            .unwrap_or((None, None))
    });
    let asks = cache_row.ask_levels.map(|level| {
        level
            .map(|level| (Some(level.price_micros), Some(level.qty_micros)))
            .unwrap_or((None, None))
    });
    let mut row = PolymarketBookTop10Row {
        schema_version: 1,
        dataset_format: BOOK_TOP10_FORMAT.to_string(),
        primary_key: cache_row.primary_key(),
        symbol: cache_row.symbol.clone(),
        condition_id: cache_row.condition_id.clone(),
        asset_id: cache_row.asset_id.clone(),
        outcome: cache_row.outcome.clone(),
        window_start_ts_ns: cache_row.window_start_ts_ns,
        window_end_ts_ns: cache_row.window_end_ts_ns,
        yes_asset_id: cache_row.yes_asset_id.clone(),
        no_asset_id: cache_row.no_asset_id.clone(),
        local_recv_ts_ns: cache_row.local_recv_ts_ns,
        ingest_seq: cache_row.ingest_seq,
        best_bid_price_micros: cache_row.best_bid_price_micros,
        best_ask_price_micros: cache_row.best_ask_price_micros,
        bid_depth_top10_micros: cache_row
            .bid_levels
            .iter()
            .flatten()
            .map(|level| level.qty_micros)
            .sum(),
        ask_depth_top10_micros: cache_row
            .ask_levels
            .iter()
            .flatten()
            .map(|level| level.qty_micros)
            .sum(),
        bid_price_00_micros: bids[0].0,
        bid_size_00_micros: bids[0].1,
        bid_price_01_micros: bids[1].0,
        bid_size_01_micros: bids[1].1,
        bid_price_02_micros: bids[2].0,
        bid_size_02_micros: bids[2].1,
        bid_price_03_micros: bids[3].0,
        bid_size_03_micros: bids[3].1,
        bid_price_04_micros: bids[4].0,
        bid_size_04_micros: bids[4].1,
        bid_price_05_micros: bids[5].0,
        bid_size_05_micros: bids[5].1,
        bid_price_06_micros: bids[6].0,
        bid_size_06_micros: bids[6].1,
        bid_price_07_micros: bids[7].0,
        bid_size_07_micros: bids[7].1,
        bid_price_08_micros: bids[8].0,
        bid_size_08_micros: bids[8].1,
        bid_price_09_micros: bids[9].0,
        bid_size_09_micros: bids[9].1,
        ask_price_00_micros: asks[0].0,
        ask_size_00_micros: asks[0].1,
        ask_price_01_micros: asks[1].0,
        ask_size_01_micros: asks[1].1,
        ask_price_02_micros: asks[2].0,
        ask_size_02_micros: asks[2].1,
        ask_price_03_micros: asks[3].0,
        ask_size_03_micros: asks[3].1,
        ask_price_04_micros: asks[4].0,
        ask_size_04_micros: asks[4].1,
        ask_price_05_micros: asks[5].0,
        ask_size_05_micros: asks[5].1,
        ask_price_06_micros: asks[6].0,
        ask_size_06_micros: asks[6].1,
        ask_price_07_micros: asks[7].0,
        ask_size_07_micros: asks[7].1,
        ask_price_08_micros: asks[8].0,
        ask_size_08_micros: asks[8].1,
        ask_price_09_micros: asks[9].0,
        ask_size_09_micros: asks[9].1,
        raw_row_hash: cache_row.raw_row_hash.clone(),
        row_hash: String::new(),
    };
    row.row_hash = row_hash(&row)?;
    Ok(row)
}

fn flush_book_chunk_if_needed(
    writer: &mut ParquetTableStreamWriter,
    chunk: &mut Vec<PolymarketBookTop10Row>,
    part_limit: usize,
) -> Result<()> {
    if chunk.len() >= part_limit {
        flush_book_chunk(writer, chunk)?;
    }
    Ok(())
}

fn flush_book_chunk(
    writer: &mut ParquetTableStreamWriter,
    chunk: &mut Vec<PolymarketBookTop10Row>,
) -> Result<()> {
    if chunk.is_empty() {
        return Ok(());
    }
    chunk.sort_by(|a, b| {
        (a.local_recv_ts_ns, a.ingest_seq, &a.primary_key).cmp(&(
            b.local_recv_ts_ns,
            b.ingest_seq,
            &b.primary_key,
        ))
    });
    writer.write_rows(chunk)?;
    chunk.clear();
    Ok(())
}

fn book_part_row_limit() -> usize {
    std::env::var("PM5M_BOOK_PART_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_BOOK_PART_ROWS)
}

fn canonical_market_symbol(symbol: &str) -> Option<String> {
    let upper = symbol.trim().to_ascii_uppercase();
    if upper.is_empty() {
        return None;
    }
    let (asset, horizon) = upper.split_once('-')?;
    if asset.is_empty()
        || !asset.chars().all(|ch| ch.is_ascii_alphanumeric())
        || horizon.is_empty()
        || !horizon.chars().all(|ch| ch.is_ascii_alphanumeric())
    {
        return None;
    }
    match horizon {
        "5M" | "15M" | "1H" => Some(format!("{asset}-{horizon}")),
        _ => None,
    }
}

#[derive(Default)]
struct MarketDimAccumulator {
    map: BTreeMap<String, MarketDimAcc>,
}

#[derive(Default)]
struct MarketDimAcc {
    symbol: String,
    condition_id: String,
    window_start_ts_ns: i64,
    window_end_ts_ns: i64,
    yes_asset_id: Option<String>,
    no_asset_id: Option<String>,
    first_seen_ts_ns: i64,
    last_seen_ts_ns: i64,
}

impl MarketDimAccumulator {
    fn update(&mut self, row: &PolymarketBookTop10Row) -> Result<()> {
        self.map
            .entry(row.condition_id.clone())
            .and_modify(|acc| {
                acc.first_seen_ts_ns = acc.first_seen_ts_ns.min(row.local_recv_ts_ns);
                acc.last_seen_ts_ns = acc.last_seen_ts_ns.max(row.local_recv_ts_ns);
            })
            .or_insert_with(|| MarketDimAcc {
                symbol: row.symbol.clone(),
                condition_id: row.condition_id.clone(),
                window_start_ts_ns: row.window_start_ts_ns,
                window_end_ts_ns: row.window_end_ts_ns,
                yes_asset_id: Some(row.yes_asset_id.clone()),
                no_asset_id: Some(row.no_asset_id.clone()),
                first_seen_ts_ns: row.local_recv_ts_ns,
                last_seen_ts_ns: row.local_recv_ts_ns,
            });
        let acc = self
            .map
            .get(&row.condition_id)
            .expect("market accumulator entry exists after insert");
        if acc.symbol != row.symbol
            || acc.window_start_ts_ns != row.window_start_ts_ns
            || acc.window_end_ts_ns != row.window_end_ts_ns
            || acc.yes_asset_id.as_deref() != Some(row.yes_asset_id.as_str())
            || acc.no_asset_id.as_deref() != Some(row.no_asset_id.as_str())
        {
            bail!(
                "inconsistent market metadata for condition {}",
                row.condition_id
            );
        }
        Ok(())
    }

    fn into_rows(self) -> Result<Vec<MarketDimRow>> {
        self.map
            .into_values()
            .map(|acc| {
                let mut row = MarketDimRow {
                    schema_version: 1,
                    dataset_format: MARKET_DIM_FORMAT.to_string(),
                    symbol: acc.symbol,
                    condition_id: acc.condition_id,
                    window_start_ts_ns: acc.window_start_ts_ns,
                    window_end_ts_ns: acc.window_end_ts_ns,
                    yes_asset_id: acc
                        .yes_asset_id
                        .ok_or_else(|| anyhow!("market_dim missing YES asset"))?,
                    no_asset_id: acc
                        .no_asset_id
                        .ok_or_else(|| anyhow!("market_dim missing NO asset"))?,
                    window_source: "recorder_discovery".to_string(),
                    first_seen_ts_ns: acc.first_seen_ts_ns,
                    last_seen_ts_ns: acc.last_seen_ts_ns,
                    row_hash: String::new(),
                };
                row.row_hash = row_hash(&row)?;
                Ok(row)
            })
            .collect()
    }
}

fn build_binance_reference_rows(
    plan: &PipelinePlan,
    cache_manifest: &CacheManifest,
    market_rows: &[MarketDimRow],
) -> Result<Vec<BinanceKline1sReferenceRow>> {
    let mut rows = Vec::new();
    let required_ranges = required_reference_ranges(market_rows)?;
    for record in cache_manifest.records.iter().filter(|record| {
        record.group == CacheGroup::Binance1sReference
            && record.status == CacheRecordStatus::Available
            && record.cache_path.is_some()
    }) {
        let path = plan.cache_root.join(
            record
                .cache_path
                .as_ref()
                .expect("checked cache_path is_some"),
        );
        for raw in read_cached_records::<RawBinanceKline1s>(&path)? {
            let symbol = raw
                .symbol
                .clone()
                .or_else(|| record.symbol.clone())
                .ok_or_else(|| anyhow!("missing Binance symbol for cache {}", record.name))?;
            let normalized_symbol = normalize_reference_symbol(&symbol);
            let Some((required_start, required_end)) = required_ranges.get(&normalized_symbol)
            else {
                continue;
            };
            if raw.bar_close_ts_ns < *required_start || raw.bar_close_ts_ns > *required_end {
                continue;
            }
            let synthetic_local_recv_ts_ns = raw
                .bar_close_ts_ns
                .checked_add(plan.reference_latency_ms * 1_000_000)
                .ok_or_else(|| anyhow!("synthetic reference timestamp overflow"))?;
            if !reference_ts_in_plan_window(synthetic_local_recv_ts_ns, plan) {
                continue;
            }
            let ingest_seq = rows.len() as u64;
            let mut row = BinanceKline1sReferenceRow {
                schema_version: 1,
                dataset_format: BINANCE_REFERENCE_FORMAT.to_string(),
                primary_key: format!("bn_1s:{}:{}", normalized_symbol, raw.bar_close_ts_ns),
                symbol: normalized_symbol,
                bar_open_ts_ns: raw.bar_open_ts_ns,
                bar_close_ts_ns: raw.bar_close_ts_ns,
                synthetic_local_recv_ts_ns,
                ingest_seq,
                open: raw.open,
                high: raw.high,
                low: raw.low,
                close: raw.close,
                volume: raw.volume,
                source_cache_name: record.name.clone(),
                row_hash: String::new(),
            };
            row.row_hash = row_hash(&row)?;
            rows.push(row);
        }
    }
    rows.sort_by(|a, b| {
        (a.synthetic_local_recv_ts_ns, a.ingest_seq, &a.primary_key).cmp(&(
            b.synthetic_local_recv_ts_ns,
            b.ingest_seq,
            &b.primary_key,
        ))
    });
    Ok(rows)
}

fn required_reference_ranges(market_rows: &[MarketDimRow]) -> Result<BTreeMap<String, (i64, i64)>> {
    let mut ranges = BTreeMap::<String, (i64, i64)>::new();
    for row in market_rows {
        let symbol = reference_symbol_for_market(&row.symbol)
            .ok_or_else(|| anyhow!("unsupported market symbol '{}'", row.symbol))?;
        let start = row
            .window_start_ts_ns
            .saturating_sub(REFERENCE_LOOKBACK_NS)
            .max(0);
        let end = row.window_end_ts_ns;
        ranges
            .entry(symbol)
            .and_modify(|(min_start, max_end)| {
                *min_start = (*min_start).min(start);
                *max_end = (*max_end).max(end);
            })
            .or_insert((start, end));
    }
    Ok(ranges)
}

fn reference_symbol_for_market(symbol: &str) -> Option<String> {
    let upper = symbol.trim().to_ascii_uppercase();
    let asset = upper
        .split_once('-')
        .map(|(asset, _)| asset)
        .unwrap_or(upper.as_str());
    if asset.is_empty() || !asset.chars().all(|ch| ch.is_ascii_alphanumeric()) {
        None
    } else if asset.ends_with("USDT") {
        Some(asset.to_string())
    } else {
        Some(format!("{asset}USDT"))
    }
}

fn normalize_reference_symbol(symbol: &str) -> String {
    let upper = symbol.trim().to_ascii_uppercase();
    if upper.ends_with("USDT") {
        upper
    } else {
        reference_symbol_for_market(&upper).unwrap_or(upper)
    }
}

fn reference_ts_in_plan_window(ts_ns: i64, plan: &PipelinePlan) -> bool {
    let after_start = match plan.raw_start_ts_ns {
        Some(start) => {
            let warm_start = start.saturating_sub(REFERENCE_LOOKBACK_NS);
            ts_ns >= warm_start
        }
        None => true,
    };
    after_start && plan.raw_end_ts_ns.is_none_or(|end| ts_ns < end)
}

fn build_settlement_rows(
    plan: &PipelinePlan,
    cache_manifest: &CacheManifest,
    market_rows: &[MarketDimRow],
) -> Result<Vec<PolymarketSettlementRow>> {
    let expected = market_rows
        .iter()
        .flat_map(|row| {
            [
                (
                    row.condition_id.clone(),
                    row.yes_asset_id.clone(),
                    "YES".to_string(),
                ),
                (
                    row.condition_id.clone(),
                    row.no_asset_id.clone(),
                    "NO".to_string(),
                ),
            ]
        })
        .collect::<BTreeSet<_>>();
    let mut rows = Vec::new();
    for record in cache_manifest.records.iter().filter(|record| {
        record.group == CacheGroup::PolymarketSettlement
            && record.status == CacheRecordStatus::Available
            && record.cache_path.is_some()
    }) {
        let path = plan.cache_root.join(
            record
                .cache_path
                .as_ref()
                .expect("checked cache_path is_some"),
        );
        for raw in read_cached_records::<RawPolymarketSettlement>(&path)? {
            if !expected.contains(&(
                raw.condition_id.clone(),
                raw.asset_id.clone(),
                raw.outcome.clone(),
            )) {
                continue;
            }
            let winner = match raw.status {
                SettlementStatus::Settled => raw.winner,
                SettlementStatus::Unsettled
                | SettlementStatus::Unknown
                | SettlementStatus::Failed => None,
            };
            let mut row = PolymarketSettlementRow {
                schema_version: 1,
                dataset_format: SETTLEMENT_FORMAT.to_string(),
                primary_key: format!(
                    "pm_settlement:{}:{}:{}",
                    raw.condition_id, raw.asset_id, raw.outcome
                ),
                condition_id: raw.condition_id,
                asset_id: raw.asset_id,
                outcome: raw.outcome,
                status: raw.status,
                winner,
                settled_ts_ns: raw.settled_ts_ns,
                source_cache_name: record.name.clone(),
                row_hash: String::new(),
            };
            row.row_hash = row_hash(&row)?;
            rows.push(row);
        }
    }
    rows.sort_by(|a, b| a.primary_key.cmp(&b.primary_key));
    Ok(rows)
}

pub(crate) fn from_micros(value: i64) -> f64 {
    value as f64 / 1_000_000.0
}

pub(crate) fn row_bid_levels(row: &PolymarketBookTop10Row) -> Vec<BookLevel> {
    [
        (row.bid_price_00_micros, row.bid_size_00_micros),
        (row.bid_price_01_micros, row.bid_size_01_micros),
        (row.bid_price_02_micros, row.bid_size_02_micros),
        (row.bid_price_03_micros, row.bid_size_03_micros),
        (row.bid_price_04_micros, row.bid_size_04_micros),
        (row.bid_price_05_micros, row.bid_size_05_micros),
        (row.bid_price_06_micros, row.bid_size_06_micros),
        (row.bid_price_07_micros, row.bid_size_07_micros),
        (row.bid_price_08_micros, row.bid_size_08_micros),
        (row.bid_price_09_micros, row.bid_size_09_micros),
    ]
    .into_iter()
    .filter_map(|(price, size)| {
        Some(BookLevel {
            price: from_micros(price?),
            size: from_micros(size?),
        })
    })
    .collect()
}

pub(crate) fn row_ask_levels(row: &PolymarketBookTop10Row) -> Vec<BookLevel> {
    [
        (row.ask_price_00_micros, row.ask_size_00_micros),
        (row.ask_price_01_micros, row.ask_size_01_micros),
        (row.ask_price_02_micros, row.ask_size_02_micros),
        (row.ask_price_03_micros, row.ask_size_03_micros),
        (row.ask_price_04_micros, row.ask_size_04_micros),
        (row.ask_price_05_micros, row.ask_size_05_micros),
        (row.ask_price_06_micros, row.ask_size_06_micros),
        (row.ask_price_07_micros, row.ask_size_07_micros),
        (row.ask_price_08_micros, row.ask_size_08_micros),
        (row.ask_price_09_micros, row.ask_size_09_micros),
    ]
    .into_iter()
    .filter_map(|(price, size)| {
        Some(BookLevel {
            price: from_micros(price?),
            size: from_micros(size?),
        })
    })
    .collect()
}
