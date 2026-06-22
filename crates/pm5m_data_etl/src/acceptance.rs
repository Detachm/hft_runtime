use crate::constants::{
    ACCEPTANCE_REPORT, BANNED_STRATEGY_FIELDS, REFERENCE_LOOKBACK_NS, SECOND_NS,
    TABLE_BINANCE_REFERENCE, TABLE_BOOK_TOP10, TABLE_DEPTH_FEATURE, TABLE_EVENT_INDEX,
    TABLE_INPUT_AVAILABILITY, TABLE_MARKET_DIM, TABLE_SETTLEMENT,
};
use crate::manifest::dataset_path;
use crate::types::*;
use anyhow::{anyhow, bail, Result};
use market_data_etl_core::{
    read_parquet_table, scan_json_fields, verify_parquet_zstd_table, write_json_file_pretty,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

pub fn accept(plan: &PipelinePlan) -> Result<AcceptanceReport> {
    let mut violations = Vec::new();

    if let Some(cache_root) = plan.book_state_cache_root.as_deref() {
        if pm5m_market_cache::read_book_cache2_catalog(cache_root).is_err() {
            violations.push(format!(
                "book_state_cache_root must point to HFTBOOK2 cache: {}",
                cache_root.display()
            ));
        }
    } else {
        violations
            .push("book_state_cache_root is required; raw-root acceptance is retired".to_string());
    }

    for table in [
        TABLE_MARKET_DIM,
        TABLE_BOOK_TOP10,
        TABLE_BINANCE_REFERENCE,
        TABLE_SETTLEMENT,
        TABLE_INPUT_AVAILABILITY,
        TABLE_EVENT_INDEX,
    ] {
        let table_path = dataset_path(plan, table);
        if let Err(err) = verify_parquet_zstd_table(&table_path, table) {
            violations.push(err.to_string());
        }
    }

    let depth_feature_path = dataset_path(plan, TABLE_DEPTH_FEATURE);
    if depth_feature_path.exists() {
        if let Err(err) = verify_parquet_zstd_table(&depth_feature_path, TABLE_DEPTH_FEATURE) {
            violations.push(format!("diagnostic depth feature table is invalid: {err}"));
        }
    }

    for file in market_data_etl_core::list_files_recursive(&plan.dataset_root)? {
        if file.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            violations.push(format!(
                "production dataset contains JSONL file {}",
                file.display()
            ));
        }
    }

    if let Err(err) = validate_book_schema_contract(&dataset_path(plan, TABLE_BOOK_TOP10)) {
        violations.push(err.to_string());
    }
    if let Err(err) = validate_market_condition_contract(plan) {
        violations.push(err.to_string());
    }

    for violation in scan_json_fields(&plan.dataset_root, BANNED_STRATEGY_FIELDS)? {
        violations.push(format!(
            "strategy field '{}' found in {}",
            violation.field,
            violation.path.display()
        ));
    }

    let event_index_path = dataset_path(plan, TABLE_EVENT_INDEX);
    if event_index_path.exists() {
        for row in read_parquet_table::<StandardEventIndexRow>(&event_index_path)? {
            if row.event_type.contains("settlement")
                || row.payload_table.contains("settlement")
                || row.payload_table == TABLE_DEPTH_FEATURE
                || row.dataset_format != EVENT_INDEX_FORMAT
            {
                violations.push(format!(
                    "settlement, diagnostic, or invalid event present in event index at seq {}",
                    row.global_event_seq
                ));
            }
            if row.payload_table != TABLE_BOOK_TOP10 && row.payload_table != TABLE_BINANCE_REFERENCE
            {
                violations.push(format!(
                    "unsupported event payload table '{}' at seq {}",
                    row.payload_table, row.global_event_seq
                ));
            }
        }
    } else {
        violations.push("event index missing".to_string());
    }

    if plan.accept_fail_closed_on_missing_reference {
        if let Err(err) = validate_binance_reference_complete(plan) {
            violations.push(err.to_string());
        }
    }

    if plan.accept_fail_closed_on_unsettled_settlement {
        if let Err(err) = validate_settlement_complete(plan) {
            violations.push(err.to_string());
        }
    }

    let report = AcceptanceReport {
        schema_version: 1,
        dataset_format: ACCEPTANCE_REPORT_FORMAT.to_string(),
        accepted: violations.is_empty(),
        violations,
    };
    write_json_file_pretty(&plan.dataset_root.join(ACCEPTANCE_REPORT), &report)?;
    if !report.accepted {
        bail!(
            "dataset acceptance failed: {}",
            report.violations.join("; ")
        );
    }
    Ok(report)
}

fn validate_settlement_complete(plan: &PipelinePlan) -> Result<()> {
    let market_path = dataset_path(plan, TABLE_MARKET_DIM);
    let settlement_path = dataset_path(plan, TABLE_SETTLEMENT);
    let market_rows = if market_path.exists() {
        read_parquet_table::<MarketDimRow>(&market_path)?
    } else {
        Vec::new()
    };
    let settlement_rows = if settlement_path.exists() {
        read_parquet_table::<PolymarketSettlementRow>(&settlement_path)?
    } else {
        Vec::new()
    };
    let mut by_asset = BTreeMap::new();
    let mut duplicate_count = 0usize;
    let mut duplicate_examples = Vec::new();
    for row in &settlement_rows {
        let key = (
            row.condition_id.as_str(),
            row.asset_id.as_str(),
            row.outcome.as_str(),
        );
        if by_asset.insert(key, row).is_some() {
            duplicate_count += 1;
            if duplicate_examples.len() < 8 {
                duplicate_examples.push(format!(
                    "{} {} {}",
                    row.condition_id, row.asset_id, row.outcome
                ));
            }
        }
    }

    let mut bad_count = 0usize;
    let mut examples = Vec::new();
    for row in &market_rows {
        for (asset_id, outcome) in [
            (row.yes_asset_id.as_str(), "YES"),
            (row.no_asset_id.as_str(), "NO"),
        ] {
            let key = (row.condition_id.as_str(), asset_id, outcome);
            match by_asset.get(&key) {
                None => {
                    bad_count += 1;
                    if examples.len() < 8 {
                        examples.push(format!(
                            "missing {} {} {}",
                            row.condition_id, asset_id, outcome
                        ));
                    }
                }
                Some(settlement)
                    if settlement.status != SettlementStatus::Settled
                        || settlement.winner.is_none() =>
                {
                    bad_count += 1;
                    if examples.len() < 8 {
                        examples.push(format!(
                            "{:?} {} {} {}",
                            settlement.status,
                            settlement.condition_id,
                            settlement.asset_id,
                            settlement.outcome
                        ));
                    }
                }
                Some(_) => {}
            }
        }
    }

    if duplicate_count > 0 {
        bail!(
            "Polymarket settlement fail-closed: {duplicate_count} duplicate asset row(s): {}",
            duplicate_examples.join("; ")
        );
    }

    if bad_count > 0 {
        bail!(
            "Polymarket settlement fail-closed: {bad_count} missing or unsettled asset row(s): {}",
            examples.join("; ")
        );
    }
    Ok(())
}

fn validate_binance_reference_complete(plan: &PipelinePlan) -> Result<()> {
    let mut violations = Vec::new();

    let availability_path = dataset_path(plan, TABLE_INPUT_AVAILABILITY);
    let availability = if availability_path.exists() {
        read_parquet_table::<InputAvailabilityRow>(&availability_path)?
    } else {
        Vec::new()
    };
    let binance_availability = availability
        .iter()
        .filter(|row| row.group == CacheGroup::Binance1sReference)
        .collect::<Vec<_>>();
    if binance_availability.is_empty() {
        violations.push("missing Binance reference input records".to_string());
    }
    for row in binance_availability {
        if row.status != CacheRecordStatus::Available {
            violations.push(format!(
                "missing Binance reference input '{}' with status {:?}",
                row.name, row.status
            ));
        }
    }

    let market_rows = read_parquet_table::<MarketDimRow>(&dataset_path(plan, TABLE_MARKET_DIM))?;
    let reference_rows = read_parquet_table::<BinanceKline1sReferenceRow>(&dataset_path(
        plan,
        TABLE_BINANCE_REFERENCE,
    ))?;
    if reference_rows.is_empty() {
        violations.push("Binance reference table is empty".to_string());
    }

    let mut primary_keys = BTreeSet::new();
    let mut symbol_close_keys = BTreeSet::new();
    let mut closes_by_symbol = BTreeMap::<String, BTreeSet<i64>>::new();
    let mut duplicate_count = 0usize;
    let mut duplicate_examples = Vec::new();
    for row in reference_rows {
        if !primary_keys.insert(row.primary_key.clone())
            || !symbol_close_keys
                .insert((normalize_reference_symbol(&row.symbol), row.bar_close_ts_ns))
        {
            duplicate_count += 1;
            if duplicate_examples.len() < 8 {
                duplicate_examples.push(format!("{} {}", row.symbol, row.bar_close_ts_ns));
            }
        }
        closes_by_symbol
            .entry(normalize_reference_symbol(&row.symbol))
            .or_default()
            .insert(row.bar_close_ts_ns);
    }
    if duplicate_count > 0 {
        violations.push(format!(
            "Binance reference table has {duplicate_count} duplicate primary/symbol-close row(s): {}",
            duplicate_examples.join("; ")
        ));
    }

    for (symbol, (start_ns, end_ns)) in required_reference_ranges(&market_rows)? {
        let first_close = ceil_to_second(start_ns).max(SECOND_NS);
        let last_close = floor_to_second(end_ns);
        if first_close > last_close {
            continue;
        }
        let Some(available) = closes_by_symbol.get(&symbol) else {
            violations.push(format!(
                "Binance reference coverage missing all rows for {symbol} over {}..{}",
                first_close, last_close
            ));
            continue;
        };

        let mut missing_count = 0usize;
        let mut examples = Vec::new();
        let mut ts = first_close;
        while ts <= last_close {
            if !available.contains(&ts) {
                missing_count += 1;
                if examples.len() < 8 {
                    examples.push(ts.to_string());
                }
            }
            ts = ts.saturating_add(SECOND_NS);
            if ts == i64::MAX {
                break;
            }
        }
        if missing_count > 0 {
            violations.push(format!(
                "Binance reference coverage missing {missing_count} 1s bar(s) for {symbol} over {}..{}; examples {}",
                first_close,
                last_close,
                examples.join(",")
            ));
        }
    }

    if !violations.is_empty() {
        bail!("Binance reference fail-closed: {}", violations.join("; "));
    }
    Ok(())
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

fn ceil_to_second(ts_ns: i64) -> i64 {
    if ts_ns <= 0 {
        0
    } else {
        ((ts_ns + SECOND_NS - 1) / SECOND_NS) * SECOND_NS
    }
}

fn floor_to_second(ts_ns: i64) -> i64 {
    ts_ns.div_euclid(SECOND_NS) * SECOND_NS
}

fn validate_market_condition_contract(plan: &PipelinePlan) -> Result<()> {
    let market_path = dataset_path(plan, TABLE_MARKET_DIM);
    let market_rows = read_parquet_table::<MarketDimRow>(&market_path)?;
    if market_rows.is_empty() {
        bail!("market_dim is empty");
    }
    let mut seen = std::collections::BTreeSet::new();
    for row in market_rows {
        if !seen.insert(row.condition_id.clone()) {
            bail!(
                "market_dim must be one row per condition; duplicate {}",
                row.condition_id
            );
        }
        if row.window_end_ts_ns <= row.window_start_ts_ns {
            bail!("market_dim invalid window for {}", row.condition_id);
        }
        if row.yes_asset_id.trim().is_empty()
            || row.no_asset_id.trim().is_empty()
            || row.yes_asset_id == row.no_asset_id
        {
            bail!("market_dim invalid YES/NO assets for {}", row.condition_id);
        }
    }
    Ok(())
}

fn validate_book_schema_contract(table_path: &Path) -> Result<()> {
    let schema_path = table_path.join("_schema.json");
    let schema: Value = serde_json::from_reader(fs::File::open(&schema_path)?)?;
    let fields = schema
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("book table missing schema fields"))?;
    for field in fields {
        let name = field.get("name").and_then(Value::as_str).unwrap_or("");
        let ty = field.get("type").and_then(Value::as_str).unwrap_or("");
        if name == "bids" || name == "asks" {
            bail!("book table contains nested column {name}");
        }
        if ty == "Float64" && !name.starts_with("__") {
            bail!("book table contains f64 production column {name}");
        }
    }
    Ok(())
}
