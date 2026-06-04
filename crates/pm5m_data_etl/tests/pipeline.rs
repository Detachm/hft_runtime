use market_data_etl_core::{hash_path, read_jsonl_table};
use pm5m_data_etl::{
    accept, build_depth_feature, build_event_index, build_facts, sync_inputs,
    BinanceKline1sReferenceRow, CacheRecordStatus, CacheSourceSpec, DefaultFetcher,
    InputAvailabilityRow, PipelinePlan, PolymarketSettlementRow, SettlementStatus,
    StandardEventIndexRow,
};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

#[test]
fn full_chain_materializes_accepts_and_is_deterministic() {
    let fixture = Fixture::new();
    let plan = fixture.plan(false);

    let cache_manifest = sync_inputs(&plan, &DefaultFetcher).unwrap();
    assert_eq!(cache_manifest.records.len(), 2);
    assert!(cache_manifest
        .records
        .iter()
        .all(|record| record.status == CacheRecordStatus::Available));
    assert!(plan
        .cache_root
        .join(cache_manifest.records[0].cache_path.as_ref().unwrap())
        .exists());

    build_facts(&plan).unwrap();
    let fact_hash_1 = hash_path(&plan.dataset_root.join("tables")).unwrap();

    let depth_rows = build_depth_feature(&plan).unwrap();
    assert_eq!(depth_rows.len(), 1);
    assert!(depth_rows[0].fillable_buy_5);
    assert_eq!(depth_rows[0].buy_cost_5, Some(3.05));

    let events = build_event_index(&plan).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event_type, "pm_depth_feature");
    assert_eq!(events[1].event_type, "binance_reference_1s");
    assert_eq!(events[0].event_ts_ns, events[1].event_ts_ns);
    assert!(events
        .iter()
        .all(|event| !event.event_type.contains("settlement")));

    accept(&plan).unwrap();
    let event_hash_1 = hash_path(
        &plan
            .dataset_root
            .join("streams/pm5m_standard_event_index_v1"),
    )
    .unwrap();

    build_facts(&plan).unwrap();
    build_depth_feature(&plan).unwrap();
    build_event_index(&plan).unwrap();
    let fact_hash_2 = hash_path(&plan.dataset_root.join("tables")).unwrap();
    let event_hash_2 = hash_path(
        &plan
            .dataset_root
            .join("streams/pm5m_standard_event_index_v1"),
    )
    .unwrap();
    assert_eq!(fact_hash_1, fact_hash_2);
    assert_eq!(event_hash_1, event_hash_2);
}

#[test]
fn build_depth_feature_reads_only_book_fact_table() {
    let fixture = Fixture::new();
    let plan = fixture.plan(false);
    sync_inputs(&plan, &DefaultFetcher).unwrap();
    build_facts(&plan).unwrap();

    fs::remove_dir_all(plan.dataset_root.join("tables/binance_kline_1s_reference")).unwrap();
    fs::remove_dir_all(plan.dataset_root.join("tables/polymarket_settlement")).unwrap();

    let rows = build_depth_feature(&plan).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn build_event_index_never_outputs_settlement_events() {
    let fixture = Fixture::new();
    let plan = fixture.plan(false);
    sync_inputs(&plan, &DefaultFetcher).unwrap();
    build_facts(&plan).unwrap();
    build_depth_feature(&plan).unwrap();

    let events = build_event_index(&plan).unwrap();
    assert!(events
        .iter()
        .all(|event| !event.event_type.contains("settlement")
            && !event.payload_table.contains("settlement")));
}

#[test]
fn missing_binance_reference_is_recorded_and_fail_closed_accept_rejects() {
    let fixture = Fixture::new();
    let mut plan = fixture.plan(true);
    plan.binance_sources[0].source_url = format!(
        "file://{}",
        fixture.root.path().join("missing.jsonl").display()
    );

    let manifest = sync_inputs(&plan, &DefaultFetcher).unwrap();
    let bn_record = manifest
        .records
        .iter()
        .find(|record| record.name == "bn-btc")
        .unwrap();
    assert_eq!(bn_record.status, CacheRecordStatus::Failed);
    assert!(bn_record.cache_path.is_none());

    build_facts(&plan).unwrap();
    let availability = read_jsonl_table::<InputAvailabilityRow>(
        &plan.dataset_root.join("tables/input_availability"),
    )
    .unwrap();
    assert!(availability
        .iter()
        .any(|row| row.name == "bn-btc" && row.status == CacheRecordStatus::Failed));

    build_depth_feature(&plan).unwrap();
    build_event_index(&plan).unwrap();
    let err = accept(&plan).unwrap_err().to_string();
    assert!(err.contains("missing Binance reference input"));
}

#[test]
fn unsettled_or_unknown_settlement_does_not_fabricate_winner() {
    let fixture = Fixture::new();
    let plan = fixture.plan(false);
    sync_inputs(&plan, &DefaultFetcher).unwrap();
    build_facts(&plan).unwrap();

    let rows = read_jsonl_table::<PolymarketSettlementRow>(
        &plan.dataset_root.join("tables/polymarket_settlement"),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, SettlementStatus::Unknown);
    assert_eq!(rows[0].winner, None);
}

#[test]
fn schema_guard_rejects_jupiter_strategy_fields() {
    let fixture = Fixture::new();
    let plan = fixture.plan(false);
    sync_inputs(&plan, &DefaultFetcher).unwrap();
    build_facts(&plan).unwrap();
    build_depth_feature(&plan).unwrap();
    build_event_index(&plan).unwrap();

    let bad_dir = plan.dataset_root.join("tables/bad_strategy_table");
    fs::create_dir_all(&bad_dir).unwrap();
    fs::write(
        bad_dir.join("part-00000.jsonl"),
        "{\"edge\":0.1,\"payload\":\"not allowed on Jupiter\"}\n",
    )
    .unwrap();

    let err = accept(&plan).unwrap_err().to_string();
    assert!(err.contains("strategy field 'edge'"));
}

#[test]
fn event_index_rows_match_declared_interface_fields() {
    let fixture = Fixture::new();
    let plan = fixture.plan(false);
    sync_inputs(&plan, &DefaultFetcher).unwrap();
    build_facts(&plan).unwrap();
    build_depth_feature(&plan).unwrap();
    build_event_index(&plan).unwrap();

    let rows = read_jsonl_table::<StandardEventIndexRow>(
        &plan
            .dataset_root
            .join("streams/pm5m_standard_event_index_v1"),
    )
    .unwrap();
    assert_eq!(rows[0].schema_version, 1);
    assert_eq!(rows[0].dataset_format, "pm5m_standard_event_index.v1");
    assert_eq!(rows[0].source_rank, 0);
    assert!(!rows[0].payload_row_hash.is_empty());

    let reference = read_jsonl_table::<BinanceKline1sReferenceRow>(
        &plan.dataset_root.join("tables/binance_kline_1s_reference"),
    )
    .unwrap();
    assert_eq!(reference[0].symbol, "BTCUSDT");
}

struct Fixture {
    root: TempDir,
    raw_root: PathBuf,
    binance_cache_fixture: PathBuf,
    settlement_cache_fixture: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let raw_root = root.path().join("raw");
        let fixtures_root = root.path().join("fixtures");
        fs::create_dir_all(&raw_root).unwrap();
        fs::create_dir_all(&fixtures_root).unwrap();

        write_jsonl(
            &raw_root.join("polymarket_book_top10.jsonl"),
            &[json!({
                "symbol": "BTC-5M",
                "condition_id": "cond-1",
                "asset_id": "asset-yes",
                "outcome": "YES",
                "local_recv_ts_ns": 2_000_000_000_i64,
                "ingest_seq": 7_u64,
                "bids": [
                    {"price": 0.58, "size": 2.0},
                    {"price": 0.57, "size": 4.0}
                ],
                "asks": [
                    {"price": 0.60, "size": 3.0},
                    {"price": 0.625, "size": 4.0}
                ]
            })],
        );

        let binance_cache_fixture = fixtures_root.join("bn.jsonl");
        write_jsonl(
            &binance_cache_fixture,
            &[json!({
                "symbol": "BTCUSDT",
                "bar_open_ts_ns": 0_i64,
                "bar_close_ts_ns": 1_000_000_000_i64,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 42.0
            })],
        );

        let settlement_cache_fixture = fixtures_root.join("settlement.jsonl");
        write_jsonl(
            &settlement_cache_fixture,
            &[json!({
                "condition_id": "cond-1",
                "asset_id": "asset-yes",
                "outcome": "YES",
                "status": "unknown",
                "winner": true,
                "settled_ts_ns": null
            })],
        );

        Self {
            root,
            raw_root,
            binance_cache_fixture,
            settlement_cache_fixture,
        }
    }

    fn plan(&self, fail_closed: bool) -> PipelinePlan {
        let mut plan = PipelinePlan::new(
            vec![self.raw_root.clone()],
            self.root.path().join("plan"),
            self.root.path().join("cache"),
            self.root.path().join("dataset"),
        );
        plan.reference_latency_ms = 1_000;
        plan.accept_fail_closed_on_missing_reference = fail_closed;
        plan.binance_sources = vec![CacheSourceSpec {
            name: "bn-btc".to_string(),
            source_url: file_url(&self.binance_cache_fixture),
            symbol: Some("BTCUSDT".to_string()),
            start_ts_ns: 0,
            end_ts_ns: 1_000_000_000,
        }];
        plan.settlement_sources = vec![CacheSourceSpec {
            name: "settlement".to_string(),
            source_url: file_url(&self.settlement_cache_fixture),
            symbol: None,
            start_ts_ns: 0,
            end_ts_ns: 1_000_000_000,
        }];
        plan
    }
}

fn write_jsonl(path: &Path, rows: &[serde_json::Value]) {
    let mut out = String::new();
    for row in rows {
        out.push_str(&serde_json::to_string(row).unwrap());
        out.push('\n');
    }
    fs::write(path, out).unwrap();
}

fn file_url(path: &Path) -> String {
    format!("file://{}", path.display())
}
