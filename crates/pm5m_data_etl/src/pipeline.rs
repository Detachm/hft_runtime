use crate::fetch::Fetcher;
use crate::types::*;
use anyhow::{anyhow, bail, Context, Result};
use market_data_etl_core::{
    copy_dir_recursive, hash_path, hash_serializable, list_files_recursive, read_jsonl_file,
    read_jsonl_table, scan_json_fields, sha256_bytes, write_json_file_pretty, write_jsonl_table,
};
use serde::Serialize;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

pub const PLAN_FILE: &str = "pipeline_plan.json";
pub const CACHE_MANIFEST: &str = "cache_manifest.json";
pub const MATERIALIZATION_REPORT: &str = "manifests/materialization_report.json";
pub const DATASET_MANIFEST: &str = "manifests/dataset_manifest.json";
pub const ACCEPTANCE_REPORT: &str = "manifests/acceptance_report.json";

const TABLE_MARKET_DIM: &str = "tables/market_dim";
const TABLE_BOOK_TOP10: &str = "tables/polymarket_book_top10";
const TABLE_BINANCE_REFERENCE: &str = "tables/binance_kline_1s_reference";
const TABLE_SETTLEMENT: &str = "tables/polymarket_settlement";
const TABLE_INPUT_AVAILABILITY: &str = "tables/input_availability";
const TABLE_DEPTH_FEATURE: &str = "derived/depth_feature_stream_v1";
const TABLE_EVENT_INDEX: &str = "streams/pm5m_standard_event_index_v1";

const BANNED_STRATEGY_FIELDS: &[&str] = &[
    "fair_value",
    "model_probability",
    "edge",
    "trigger",
    "side_decision",
    "pnl",
    "tradeable_interval",
];

pub fn write_plan(plan: &PipelinePlan) -> Result<PathBuf> {
    let path = plan.plan_root.join(PLAN_FILE);
    write_json_file_pretty(&path, plan)?;
    Ok(path)
}

pub fn load_plan(path: &Path) -> Result<PipelinePlan> {
    let file = fs::File::open(path).with_context(|| format!("open plan {}", path.display()))?;
    let plan = serde_json::from_reader::<_, PipelinePlan>(file)
        .with_context(|| format!("parse plan {}", path.display()))?;
    if plan.dataset_format != PLAN_DATASET_FORMAT {
        bail!(
            "unsupported plan dataset_format {}, expected {}",
            plan.dataset_format,
            PLAN_DATASET_FORMAT
        );
    }
    Ok(plan)
}

pub fn sync_inputs(plan: &PipelinePlan, fetcher: &dyn Fetcher) -> Result<CacheManifest> {
    fs::create_dir_all(&plan.cache_root)
        .with_context(|| format!("create cache root {}", plan.cache_root.display()))?;

    let mut records = Vec::new();
    for spec in &plan.binance_sources {
        records.push(sync_one_cache_source(
            plan,
            fetcher,
            CacheGroup::Binance1sReference,
            spec,
        )?);
    }
    for spec in &plan.settlement_sources {
        records.push(sync_one_cache_source(
            plan,
            fetcher,
            CacheGroup::PolymarketSettlement,
            spec,
        )?);
    }

    let manifest = CacheManifest {
        schema_version: 1,
        dataset_format: CACHE_MANIFEST_FORMAT.to_string(),
        records,
    };
    write_json_file_pretty(&cache_manifest_path(plan), &manifest)?;
    Ok(manifest)
}

pub fn build_facts(plan: &PipelinePlan) -> Result<MaterializationReport> {
    fs::create_dir_all(&plan.dataset_root)
        .with_context(|| format!("create dataset root {}", plan.dataset_root.display()))?;

    let cache_manifest = read_cache_manifest(plan)?;
    let book_rows = build_book_fact_rows(plan)?;
    let market_rows = build_market_dim_rows(&book_rows)?;
    let binance_rows = build_binance_reference_rows(plan, &cache_manifest)?;
    let settlement_rows = build_settlement_rows(plan, &cache_manifest)?;
    let availability_rows = cache_manifest
        .records
        .iter()
        .map(input_availability_from_cache_record)
        .collect::<Vec<_>>();

    let market_report = write_jsonl_table(&dataset_path(plan, TABLE_MARKET_DIM), &market_rows)?;
    let book_report = write_jsonl_table(&dataset_path(plan, TABLE_BOOK_TOP10), &book_rows)?;
    let binance_report =
        write_jsonl_table(&dataset_path(plan, TABLE_BINANCE_REFERENCE), &binance_rows)?;
    let settlement_report =
        write_jsonl_table(&dataset_path(plan, TABLE_SETTLEMENT), &settlement_rows)?;
    let availability_report = write_jsonl_table(
        &dataset_path(plan, TABLE_INPUT_AVAILABILITY),
        &availability_rows,
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

    let mut manifest = base_dataset_manifest(plan)?;
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

pub fn build_depth_feature(plan: &PipelinePlan) -> Result<Vec<DepthFeatureRow>> {
    let book_rows =
        read_jsonl_table::<PolymarketBookTop10Row>(&dataset_path(plan, TABLE_BOOK_TOP10))?;
    let mut depth_rows = book_rows
        .iter()
        .map(depth_feature_from_book_row)
        .collect::<Result<Vec<_>>>()?;
    depth_rows.sort_by(|a, b| {
        (a.local_recv_ts_ns, a.ingest_seq, &a.primary_key).cmp(&(
            b.local_recv_ts_ns,
            b.ingest_seq,
            &b.primary_key,
        ))
    });
    write_jsonl_table(&dataset_path(plan, TABLE_DEPTH_FEATURE), &depth_rows)?;

    let mut manifest = read_or_base_dataset_manifest(plan)?;
    manifest.depth_feature_hash = Some(hash_path(&dataset_path(plan, TABLE_DEPTH_FEATURE))?);
    write_dataset_manifest(plan, &manifest)?;

    Ok(depth_rows)
}

pub fn build_event_index(plan: &PipelinePlan) -> Result<Vec<StandardEventIndexRow>> {
    let depth_rows = read_jsonl_table::<DepthFeatureRow>(&dataset_path(plan, TABLE_DEPTH_FEATURE))?;
    let binance_rows = read_jsonl_table::<BinanceKline1sReferenceRow>(&dataset_path(
        plan,
        TABLE_BINANCE_REFERENCE,
    ))?;

    let mut events = Vec::new();
    for row in &depth_rows {
        events.push(EventSortRecord {
            event_type: "pm_depth_feature".to_string(),
            event_ts_ns: row.local_recv_ts_ns,
            source_rank: 0,
            ingest_seq: row.ingest_seq,
            symbol: row.symbol.clone(),
            condition_id: Some(row.condition_id.clone()),
            asset_id: Some(row.asset_id.clone()),
            outcome: Some(row.outcome.clone()),
            payload_table: TABLE_DEPTH_FEATURE.to_string(),
            payload_primary_key: row.primary_key.clone(),
            payload_row_hash: row.row_hash.clone(),
        });
    }
    for row in &binance_rows {
        events.push(EventSortRecord {
            event_type: "binance_reference_1s".to_string(),
            event_ts_ns: row.synthetic_local_recv_ts_ns,
            source_rank: 1,
            ingest_seq: row.ingest_seq,
            symbol: row.symbol.clone(),
            condition_id: None,
            asset_id: None,
            outcome: None,
            payload_table: TABLE_BINANCE_REFERENCE.to_string(),
            payload_primary_key: row.primary_key.clone(),
            payload_row_hash: row.row_hash.clone(),
        });
    }

    events.sort();
    let rows = events
        .into_iter()
        .enumerate()
        .map(|(idx, event)| StandardEventIndexRow {
            schema_version: 1,
            dataset_format: EVENT_INDEX_FORMAT.to_string(),
            global_event_seq: idx as u64,
            event_type: event.event_type,
            event_ts_ns: event.event_ts_ns,
            source_rank: event.source_rank,
            symbol: event.symbol,
            condition_id: event.condition_id,
            asset_id: event.asset_id,
            outcome: event.outcome,
            payload_table: event.payload_table,
            payload_primary_key: event.payload_primary_key,
            payload_row_hash: event.payload_row_hash,
        })
        .collect::<Vec<_>>();

    write_jsonl_table(&dataset_path(plan, TABLE_EVENT_INDEX), &rows)?;
    let mut manifest = read_or_base_dataset_manifest(plan)?;
    manifest.event_index_hash = Some(hash_path(&dataset_path(plan, TABLE_EVENT_INDEX))?);
    manifest.contains_settlement_in_event_stream = false;
    write_dataset_manifest(plan, &manifest)?;

    Ok(rows)
}

pub fn accept(plan: &PipelinePlan) -> Result<AcceptanceReport> {
    let mut violations = Vec::new();

    for violation in scan_json_fields(&plan.dataset_root, BANNED_STRATEGY_FIELDS)? {
        violations.push(format!(
            "strategy field '{}' found in {}",
            violation.field,
            violation.path.display()
        ));
    }

    let event_index_path = dataset_path(plan, TABLE_EVENT_INDEX);
    if event_index_path.exists() {
        for row in read_jsonl_table::<StandardEventIndexRow>(&event_index_path)? {
            if row.event_type.contains("settlement")
                || row.payload_table.contains("settlement")
                || row.dataset_format != EVENT_INDEX_FORMAT
            {
                violations.push(format!(
                    "settlement or invalid event present in event index at seq {}",
                    row.global_event_seq
                ));
            }
        }
    } else {
        violations.push("event index missing".to_string());
    }

    if plan.accept_fail_closed_on_missing_reference {
        let availability_path = dataset_path(plan, TABLE_INPUT_AVAILABILITY);
        let availability = if availability_path.exists() {
            read_jsonl_table::<InputAvailabilityRow>(&availability_path)?
        } else {
            Vec::new()
        };
        for row in availability {
            if row.group == CacheGroup::Binance1sReference
                && row.status != CacheRecordStatus::Available
            {
                violations.push(format!(
                    "missing Binance reference input '{}' with status {:?}",
                    row.name, row.status
                ));
            }
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

pub fn export_dataset(plan: &PipelinePlan, export_root: &Path) -> Result<()> {
    copy_dir_recursive(&plan.dataset_root, export_root)
}

fn sync_one_cache_source(
    plan: &PipelinePlan,
    fetcher: &dyn Fetcher,
    group: CacheGroup,
    spec: &CacheSourceSpec,
) -> Result<CacheRecord> {
    let group_dir = plan.cache_root.join(group.as_str());
    fs::create_dir_all(&group_dir).with_context(|| format!("create {}", group_dir.display()))?;
    let target = group_dir.join(safe_cache_file_name(&spec.name));

    match fetcher.fetch(&spec.source_url) {
        Ok(bytes) if bytes.is_empty() => Ok(CacheRecord {
            group,
            name: spec.name.clone(),
            source_url: spec.source_url.clone(),
            symbol: spec.symbol.clone(),
            start_ts_ns: spec.start_ts_ns,
            end_ts_ns: spec.end_ts_ns,
            status: CacheRecordStatus::Missing,
            cache_path: None,
            missing_count: 1,
            failure_reason: Some("empty response".to_string()),
            response_hash: None,
        }),
        Ok(bytes) => {
            fs::write(&target, &bytes).with_context(|| format!("write {}", target.display()))?;
            Ok(CacheRecord {
                group,
                name: spec.name.clone(),
                source_url: spec.source_url.clone(),
                symbol: spec.symbol.clone(),
                start_ts_ns: spec.start_ts_ns,
                end_ts_ns: spec.end_ts_ns,
                status: CacheRecordStatus::Available,
                cache_path: Some(relative_to(&target, &plan.cache_root)?),
                missing_count: 0,
                failure_reason: None,
                response_hash: Some(sha256_bytes(&bytes)),
            })
        }
        Err(err) => Ok(CacheRecord {
            group,
            name: spec.name.clone(),
            source_url: spec.source_url.clone(),
            symbol: spec.symbol.clone(),
            start_ts_ns: spec.start_ts_ns,
            end_ts_ns: spec.end_ts_ns,
            status: CacheRecordStatus::Failed,
            cache_path: None,
            missing_count: 1,
            failure_reason: Some(err.to_string()),
            response_hash: None,
        }),
    }
}

fn build_book_fact_rows(plan: &PipelinePlan) -> Result<Vec<PolymarketBookTop10Row>> {
    let mut rows = Vec::new();
    for raw_root in &plan.raw_roots {
        for file in discover_book_files(raw_root)? {
            let raw_rows = read_jsonl_file::<RawPolymarketBookTop10>(&file)?;
            for raw in raw_rows {
                let raw_row_hash = row_hash(&raw)?;
                let best_bid_price = raw.bids.iter().map(|level| level.price).max_by(float_cmp);
                let best_ask_price = raw.asks.iter().map(|level| level.price).min_by(float_cmp);
                let primary_key = format!(
                    "pm_book:{}:{}:{}:{}",
                    raw.condition_id, raw.asset_id, raw.local_recv_ts_ns, raw.ingest_seq
                );
                let mut row = PolymarketBookTop10Row {
                    schema_version: 1,
                    dataset_format: BOOK_TOP10_FORMAT.to_string(),
                    primary_key,
                    symbol: raw.symbol,
                    condition_id: raw.condition_id,
                    asset_id: raw.asset_id,
                    outcome: raw.outcome,
                    local_recv_ts_ns: raw.local_recv_ts_ns,
                    ingest_seq: raw.ingest_seq,
                    best_bid_price,
                    best_ask_price,
                    bid_depth_top10: raw.bids.iter().map(|level| level.size).sum(),
                    ask_depth_top10: raw.asks.iter().map(|level| level.size).sum(),
                    bids: raw.bids,
                    asks: raw.asks,
                    raw_row_hash,
                    row_hash: String::new(),
                };
                row.row_hash = row_hash(&row)?;
                rows.push(row);
            }
        }
    }
    rows.sort_by(|a, b| {
        (a.local_recv_ts_ns, a.ingest_seq, &a.primary_key).cmp(&(
            b.local_recv_ts_ns,
            b.ingest_seq,
            &b.primary_key,
        ))
    });
    Ok(rows)
}

fn build_market_dim_rows(book_rows: &[PolymarketBookTop10Row]) -> Result<Vec<MarketDimRow>> {
    #[derive(Default)]
    struct Acc {
        symbol: String,
        condition_id: String,
        asset_id: String,
        outcome: String,
        first_seen_ts_ns: i64,
        last_seen_ts_ns: i64,
    }

    let mut map = BTreeMap::<(String, String, String), Acc>::new();
    for row in book_rows {
        let key = (
            row.condition_id.clone(),
            row.asset_id.clone(),
            row.outcome.clone(),
        );
        map.entry(key)
            .and_modify(|acc| {
                acc.first_seen_ts_ns = acc.first_seen_ts_ns.min(row.local_recv_ts_ns);
                acc.last_seen_ts_ns = acc.last_seen_ts_ns.max(row.local_recv_ts_ns);
            })
            .or_insert_with(|| Acc {
                symbol: row.symbol.clone(),
                condition_id: row.condition_id.clone(),
                asset_id: row.asset_id.clone(),
                outcome: row.outcome.clone(),
                first_seen_ts_ns: row.local_recv_ts_ns,
                last_seen_ts_ns: row.local_recv_ts_ns,
            });
    }

    map.into_values()
        .map(|acc| {
            let mut row = MarketDimRow {
                schema_version: 1,
                dataset_format: MARKET_DIM_FORMAT.to_string(),
                symbol: acc.symbol,
                condition_id: acc.condition_id,
                asset_id: acc.asset_id,
                outcome: acc.outcome,
                first_seen_ts_ns: acc.first_seen_ts_ns,
                last_seen_ts_ns: acc.last_seen_ts_ns,
                row_hash: String::new(),
            };
            row.row_hash = row_hash(&row)?;
            Ok(row)
        })
        .collect()
}

fn build_binance_reference_rows(
    plan: &PipelinePlan,
    cache_manifest: &CacheManifest,
) -> Result<Vec<BinanceKline1sReferenceRow>> {
    let mut rows = Vec::new();
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
            let ingest_seq = rows.len() as u64;
            let mut row = BinanceKline1sReferenceRow {
                schema_version: 1,
                dataset_format: BINANCE_REFERENCE_FORMAT.to_string(),
                primary_key: format!("bn_1s:{}:{}", symbol, raw.bar_close_ts_ns),
                symbol,
                bar_open_ts_ns: raw.bar_open_ts_ns,
                bar_close_ts_ns: raw.bar_close_ts_ns,
                synthetic_local_recv_ts_ns: raw
                    .bar_close_ts_ns
                    .checked_add(plan.reference_latency_ms * 1_000_000)
                    .ok_or_else(|| anyhow!("synthetic reference timestamp overflow"))?,
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

fn build_settlement_rows(
    plan: &PipelinePlan,
    cache_manifest: &CacheManifest,
) -> Result<Vec<PolymarketSettlementRow>> {
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

fn depth_feature_from_book_row(row: &PolymarketBookTop10Row) -> Result<DepthFeatureRow> {
    let buy_cost_1 = cost_to_buy(1.0, &row.asks);
    let buy_cost_5 = cost_to_buy(5.0, &row.asks);
    let buy_cost_10 = cost_to_buy(10.0, &row.asks);
    let sell_proceeds_1 = proceeds_to_sell(1.0, &row.bids);
    let sell_proceeds_5 = proceeds_to_sell(5.0, &row.bids);
    let sell_proceeds_10 = proceeds_to_sell(10.0, &row.bids);
    let mut feature = DepthFeatureRow {
        schema_version: 1,
        dataset_format: DEPTH_FEATURE_FORMAT.to_string(),
        primary_key: format!("pm_depth_feature:{}", row.primary_key),
        source_book_primary_key: row.primary_key.clone(),
        symbol: row.symbol.clone(),
        condition_id: row.condition_id.clone(),
        asset_id: row.asset_id.clone(),
        outcome: row.outcome.clone(),
        local_recv_ts_ns: row.local_recv_ts_ns,
        ingest_seq: row.ingest_seq,
        best_bid_price: row.best_bid_price,
        best_ask_price: row.best_ask_price,
        spread: row
            .best_bid_price
            .zip(row.best_ask_price)
            .map(|(bid, ask)| ask - bid),
        bid_depth_top10: row.bid_depth_top10,
        ask_depth_top10: row.ask_depth_top10,
        buy_cost_1,
        buy_cost_5,
        buy_cost_10,
        sell_proceeds_1,
        sell_proceeds_5,
        sell_proceeds_10,
        fillable_buy_1: buy_cost_1.is_some(),
        fillable_buy_5: buy_cost_5.is_some(),
        fillable_buy_10: buy_cost_10.is_some(),
        fillable_sell_1: sell_proceeds_1.is_some(),
        fillable_sell_5: sell_proceeds_5.is_some(),
        fillable_sell_10: sell_proceeds_10.is_some(),
        book_age_ms: 0,
        row_hash: String::new(),
    };
    feature.row_hash = row_hash(&feature)?;
    Ok(feature)
}

fn input_availability_from_cache_record(record: &CacheRecord) -> InputAvailabilityRow {
    InputAvailabilityRow {
        schema_version: 1,
        dataset_format: INPUT_AVAILABILITY_FORMAT.to_string(),
        group: record.group,
        name: record.name.clone(),
        source_url: record.source_url.clone(),
        symbol: record.symbol.clone(),
        start_ts_ns: record.start_ts_ns,
        end_ts_ns: record.end_ts_ns,
        status: record.status,
        missing_count: record.missing_count,
        failure_reason: record.failure_reason.clone(),
        response_hash: record.response_hash.clone(),
    }
}

fn discover_book_files(raw_root: &Path) -> Result<Vec<PathBuf>> {
    let files = list_files_recursive(raw_root)?
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name == "polymarket_book_top10.jsonl" || name == "pm_book_top10.jsonl"
                })
        })
        .collect::<Vec<_>>();
    Ok(files)
}

fn read_cached_records<T>(path: &Path) -> Result<Vec<T>>
where
    T: serde::de::DeserializeOwned,
{
    let bytes = fs::read(path).with_context(|| format!("read cache {}", path.display()))?;
    if bytes
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        == Some(b'[')
    {
        return serde_json::from_slice::<Vec<T>>(&bytes)
            .with_context(|| format!("parse JSON array cache {}", path.display()));
    }
    read_jsonl_file(path)
}

fn cache_manifest_path(plan: &PipelinePlan) -> PathBuf {
    plan.cache_root.join(CACHE_MANIFEST)
}

fn read_cache_manifest(plan: &PipelinePlan) -> Result<CacheManifest> {
    let path = cache_manifest_path(plan);
    if !path.exists() {
        return Ok(CacheManifest {
            schema_version: 1,
            dataset_format: CACHE_MANIFEST_FORMAT.to_string(),
            records: Vec::new(),
        });
    }
    let file = fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    serde_json::from_reader(file).with_context(|| format!("parse {}", path.display()))
}

fn dataset_path(plan: &PipelinePlan, relative: &str) -> PathBuf {
    plan.dataset_root.join(relative)
}

fn safe_cache_file_name(name: &str) -> String {
    let mut out = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if !out.ends_with(".jsonl") && !out.ends_with(".json") {
        out.push_str(".jsonl");
    }
    out
}

fn relative_to(path: &Path, root: &Path) -> Result<PathBuf> {
    Ok(path
        .strip_prefix(root)
        .with_context(|| format!("strip prefix {} from {}", root.display(), path.display()))?
        .to_path_buf())
}

fn row_hash<T: Serialize>(row: &T) -> Result<String> {
    let mut value = serde_json::to_value(row).context("serialize row for row_hash")?;
    if let Value::Object(map) = &mut value {
        map.remove("row_hash");
    }
    Ok(sha256_bytes(
        &serde_json::to_vec(&value).context("serialize row_hash value")?,
    ))
}

fn float_cmp(a: &f64, b: &f64) -> Ordering {
    a.partial_cmp(b).unwrap_or(Ordering::Equal)
}

fn cost_to_buy(quantity: f64, asks: &[BookLevel]) -> Option<f64> {
    fill_cost(quantity, asks, true)
}

fn proceeds_to_sell(quantity: f64, bids: &[BookLevel]) -> Option<f64> {
    fill_cost(quantity, bids, false)
}

fn fill_cost(quantity: f64, levels: &[BookLevel], ascending_price: bool) -> Option<f64> {
    if quantity <= 0.0 {
        return Some(0.0);
    }
    let mut sorted = levels.to_vec();
    if ascending_price {
        sorted.sort_by(|a, b| float_cmp(&a.price, &b.price));
    } else {
        sorted.sort_by(|a, b| float_cmp(&b.price, &a.price));
    }
    let mut remaining = quantity;
    let mut total = 0.0;
    for level in sorted {
        if level.size <= 0.0 {
            continue;
        }
        let fill = remaining.min(level.size);
        total += fill * level.price;
        remaining -= fill;
        if remaining <= f64::EPSILON {
            return Some(total);
        }
    }
    None
}

fn existing_hash(path: &Path) -> Result<Option<String>> {
    if path.exists() {
        Ok(Some(hash_path(path)?))
    } else {
        Ok(None)
    }
}

fn cache_group_hash(manifest: &CacheManifest, group: CacheGroup) -> Result<String> {
    let rows = manifest
        .records
        .iter()
        .filter(|record| record.group == group)
        .cloned()
        .collect::<Vec<_>>();
    hash_serializable(&rows)
}

fn base_dataset_manifest(plan: &PipelinePlan) -> Result<DatasetManifest> {
    Ok(DatasetManifest {
        schema_version: 1,
        dataset_format: DATASET_MANIFEST_FORMAT.to_string(),
        raw_roots: plan.raw_roots.clone(),
        plan_root: plan.plan_root.clone(),
        cache_root: plan.cache_root.clone(),
        dataset_root: plan.dataset_root.clone(),
        binance_cache_manifest_hash: None,
        settlement_cache_manifest_hash: None,
        fact_table_hash: None,
        depth_feature_hash: None,
        event_index_hash: None,
        contains_strategy_fields: false,
        contains_settlement_in_event_stream: false,
    })
}

fn read_or_base_dataset_manifest(plan: &PipelinePlan) -> Result<DatasetManifest> {
    let path = plan.dataset_root.join(DATASET_MANIFEST);
    if !path.exists() {
        return base_dataset_manifest(plan);
    }
    let file = fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    serde_json::from_reader(file).with_context(|| format!("parse {}", path.display()))
}

fn write_dataset_manifest(plan: &PipelinePlan, manifest: &DatasetManifest) -> Result<()> {
    write_json_file_pretty(&plan.dataset_root.join(DATASET_MANIFEST), manifest)
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct EventSortRecord {
    event_type: String,
    event_ts_ns: i64,
    source_rank: u8,
    ingest_seq: u64,
    symbol: String,
    condition_id: Option<String>,
    asset_id: Option<String>,
    outcome: Option<String>,
    payload_table: String,
    payload_primary_key: String,
    payload_row_hash: String,
}

impl Ord for EventSortRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        (
            self.event_ts_ns,
            self.source_rank,
            self.ingest_seq,
            &self.payload_row_hash,
        )
            .cmp(&(
                other.event_ts_ns,
                other.source_rank,
                other.ingest_seq,
                &other.payload_row_hash,
            ))
    }
}

impl PartialOrd for EventSortRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
