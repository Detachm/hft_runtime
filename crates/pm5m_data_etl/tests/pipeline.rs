use anyhow::{bail, Result};
use market_data_etl_core::{hash_path, read_parquet_table, write_parquet_table};
use pm5m_data_etl::{
    accept, append_book_state_cache_partition, build_book_state_cache, build_depth_feature,
    build_event_index, build_facts, export_dataset, prepare_caches, sync_inputs,
    AppendBookStateCachePartitionOptions, BinanceKline1sReferenceRow, BuildBookStateCacheOptions,
    CachePreparationHttp, CacheRecordStatus, CacheSourceSpec, DefaultFetcher, ExportManifest,
    InputAvailabilityRow, MarketDimRow, PipelinePlan, PolymarketBookTop10Row,
    PolymarketSettlementRow, SettlementStatus, StandardEventIndexRow,
};
use pm5m_market_cache::{BookCacheRow, BookLevelMicros, WriteBookCache2Options};
use serde::Serialize;
use serde_json::json;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
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
    assert_eq!(depth_rows.len(), 2);
    assert!(depth_rows[0].fillable_buy_5);
    assert_eq!(depth_rows[0].buy_cost_5, Some(3.05));

    assert_eq!(build_event_index(&plan).unwrap(), 3);
    let events = read_event_index_rows(&plan);
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].event_type, "pm_book_top10");
    assert_eq!(events[1].event_type, "binance_reference_1s");
    assert_eq!(events[0].event_ts_ns, events[1].event_ts_ns);
    assert_eq!(events[0].payload_table, "tables/polymarket_book_top10");
    assert_eq!(events[1].payload_table, "tables/binance_kline_1s_reference");
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
    assert_eq!(rows.len(), 2);
}

#[test]
fn build_facts_rejects_inconsistent_condition_metadata() {
    let fixture = Fixture::new();
    fs::remove_dir_all(&fixture.book_cache_root).unwrap();
    let yes = book_cache_row_yes();
    let mut no = book_cache_row_no();
    no.window_end_ts_ns = 301_000_000_000;
    write_fixture_book_cache(&fixture.book_cache_root, vec![yes, no]);

    let plan = fixture.plan(false);
    sync_inputs(&plan, &DefaultFetcher).unwrap();
    let err = format!("{:#}", build_facts(&plan).unwrap_err());
    assert!(err.contains("inconsistent market metadata"), "{err}");
}

#[test]
fn build_event_index_never_outputs_settlement_events() {
    let fixture = Fixture::new();
    let plan = fixture.plan(false);
    sync_inputs(&plan, &DefaultFetcher).unwrap();
    build_facts(&plan).unwrap();
    build_depth_feature(&plan).unwrap();

    build_event_index(&plan).unwrap();
    let events = read_event_index_rows(&plan);
    assert!(events
        .iter()
        .all(|event| !event.event_type.contains("settlement")
            && !event.payload_table.contains("settlement")));
}

#[test]
fn build_event_index_reads_book_fact_not_depth_feature() {
    let fixture = Fixture::new();
    let plan = fixture.plan(false);
    sync_inputs(&plan, &DefaultFetcher).unwrap();
    build_facts(&plan).unwrap();
    build_depth_feature(&plan).unwrap();

    fs::remove_dir_all(
        plan.dataset_root
            .join("derived/depth_feature_stream_v1_rust_all"),
    )
    .unwrap();

    build_event_index(&plan).unwrap();
    let events = read_event_index_rows(&plan);
    assert_eq!(events[0].event_type, "pm_book_top10");
    assert_eq!(events[0].payload_table, "tables/polymarket_book_top10");
}

#[test]
fn acceptance_does_not_require_depth_feature_diagnostic_table() {
    let fixture = Fixture::new();
    let plan = fixture.plan(false);
    sync_inputs(&plan, &DefaultFetcher).unwrap();
    build_facts(&plan).unwrap();
    build_event_index(&plan).unwrap();

    assert!(!plan
        .dataset_root
        .join("derived/depth_feature_stream_v1_rust_all")
        .exists());
    accept(&plan).unwrap();
}

#[test]
fn acceptance_rejects_legacy_depth_feature_event_payload() {
    let fixture = Fixture::new();
    let plan = fixture.plan(false);
    sync_inputs(&plan, &DefaultFetcher).unwrap();
    build_facts(&plan).unwrap();
    build_depth_feature(&plan).unwrap();

    let book_rows = read_parquet_table::<PolymarketBookTop10Row>(
        &plan.dataset_root.join("tables/polymarket_book_top10"),
    )
    .unwrap();
    let book = &book_rows[0];
    let legacy_events = vec![StandardEventIndexRow {
        schema_version: 1,
        dataset_format: "pm5m_standard_event_index.v1".to_string(),
        global_event_seq: 0,
        event_type: "pm_depth_feature".to_string(),
        event_ts_ns: book.local_recv_ts_ns,
        source_rank: 0,
        symbol: book.symbol.clone(),
        condition_id: Some(book.condition_id.clone()),
        asset_id: Some(book.asset_id.clone()),
        outcome: Some(book.outcome.clone()),
        payload_table: "derived/depth_feature_stream_v1_rust_all".to_string(),
        payload_primary_key: "legacy-depth-row".to_string(),
        payload_row_hash: "legacy-depth-hash".to_string(),
    }];
    write_parquet_table(
        &plan
            .dataset_root
            .join("streams/pm5m_standard_event_index_v1"),
        &legacy_events,
        Some("event_ts_ns"),
    )
    .unwrap();

    let err = accept(&plan).unwrap_err().to_string();
    assert!(err.contains("diagnostic"));
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
    let availability = read_parquet_table::<InputAvailabilityRow>(
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
    write_jsonl(
        &fixture.settlement_cache_fixture,
        &[json!({
            "condition_id": "cond-1",
            "asset_id": "asset-yes",
            "outcome": "YES",
            "status": "unknown",
            "winner": true,
            "settled_ts_ns": null
        })],
    );
    let plan = fixture.plan(false);
    sync_inputs(&plan, &DefaultFetcher).unwrap();
    build_facts(&plan).unwrap();

    let rows = read_parquet_table::<PolymarketSettlementRow>(
        &plan.dataset_root.join("tables/polymarket_settlement"),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, SettlementStatus::Unknown);
    assert_eq!(rows[0].winner, None);
}

#[test]
fn acceptance_rejects_unsettled_settlement_when_fail_closed() {
    let fixture = Fixture::new();
    write_jsonl(
        &fixture.settlement_cache_fixture,
        &[json!({
            "condition_id": "cond-1",
            "asset_id": "asset-yes",
            "outcome": "YES",
            "status": "unknown",
            "winner": true,
            "settled_ts_ns": null
        })],
    );
    let plan = fixture.plan(false);
    sync_inputs(&plan, &DefaultFetcher).unwrap();
    build_facts(&plan).unwrap();
    build_depth_feature(&plan).unwrap();
    build_event_index(&plan).unwrap();

    let err = accept(&plan).unwrap_err().to_string();
    assert!(err.contains("Polymarket settlement fail-closed"));
    assert!(err.contains("Unknown cond-1 asset-yes YES"));
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

    let rows = read_parquet_table::<StandardEventIndexRow>(
        &plan
            .dataset_root
            .join("streams/pm5m_standard_event_index_v1"),
    )
    .unwrap();
    assert_eq!(rows[0].schema_version, 1);
    assert_eq!(rows[0].dataset_format, "pm5m_standard_event_index.v1");
    assert_eq!(rows[0].source_rank, 0);
    assert_eq!(rows[0].payload_table, "tables/polymarket_book_top10");
    assert!(!rows[0].payload_row_hash.is_empty());

    let reference = read_parquet_table::<BinanceKline1sReferenceRow>(
        &plan.dataset_root.join("tables/binance_kline_1s_reference"),
    )
    .unwrap();
    assert_eq!(reference[0].symbol, "BTCUSDT");
}

#[test]
fn production_contract_rejects_raw_plan_without_book_state_cache() {
    let fixture = Fixture::new_jsonl_raw_only();
    let mut plan = fixture.plan(false);
    plan.book_state_cache_root = None;
    sync_inputs(&plan, &DefaultFetcher).unwrap();
    let err = build_facts(&plan).unwrap_err().to_string();
    assert!(err.contains("book_state_cache_root"));
}

#[test]
fn book_fact_table_is_flat_fixed_point_parquet() {
    let fixture = Fixture::new();
    let plan = fixture.plan(false);
    sync_inputs(&plan, &DefaultFetcher).unwrap();
    build_facts(&plan).unwrap();
    let rows = read_parquet_table::<PolymarketBookTop10Row>(
        &plan.dataset_root.join("tables/polymarket_book_top10"),
    )
    .unwrap();
    assert_eq!(rows[0].best_bid_price_micros, Some(580_000));
    let schema = fs::read_to_string(
        plan.dataset_root
            .join("tables/polymarket_book_top10/_schema.json"),
    )
    .unwrap();
    assert!(schema.contains("\"physical_format\": \"parquet\""));
    assert!(schema.contains("\"compression\": \"zstd\""));
    assert!(!schema.contains("\"name\":\"bids\""));
    assert!(!schema.contains("\"name\":\"asks\""));
}

#[test]
fn ws_raw_replay_materializes_canonical_book_state_fact() {
    let fixture = Fixture::new_ws_raw();
    let plan = fixture.plan(false);

    build_facts(&plan).unwrap();
    let rows = read_parquet_table::<PolymarketBookTop10Row>(
        &plan.dataset_root.join("tables/polymarket_book_top10"),
    )
    .unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows.iter()
            .map(|row| row.symbol.as_str())
            .collect::<Vec<_>>(),
        vec!["ETH-5M", "ETH-5M", "BTC-15M"]
    );
    assert_eq!(rows[0].best_bid_price_micros, Some(580_000));
    assert_eq!(rows[0].best_ask_price_micros, Some(600_000));
    assert_eq!(rows[1].best_bid_price_micros, Some(590_000));
    assert_eq!(rows[1].best_ask_price_micros, None);
}

#[test]
fn ws_raw_market_symbol_allowlist_filters_after_canonicalization() {
    let fixture = Fixture::new_ws_raw();
    let mut plan = fixture.plan(false);
    plan.market_symbol_allowlist = vec!["ETH-5M".to_string()];

    build_facts(&plan).unwrap();
    let rows = read_parquet_table::<PolymarketBookTop10Row>(
        &plan.dataset_root.join("tables/polymarket_book_top10"),
    )
    .unwrap();
    let markets =
        read_parquet_table::<MarketDimRow>(&plan.dataset_root.join("tables/market_dim")).unwrap();

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.symbol == "ETH-5M"));
    assert_eq!(markets.len(), 1);
    assert_eq!(markets[0].symbol, "ETH-5M");
}

#[test]
fn book_state_cache_feeds_build_facts_without_raw_roots() {
    let fixture = Fixture::new_ws_raw();
    let cache_root = fixture.root.path().join("book_state_cache");
    let report = build_book_state_cache(&BuildBookStateCacheOptions {
        raw_roots: vec![fixture.raw_root.clone()],
        cache_root: cache_root.clone(),
        raw_start_ts_ns: Some(0),
        raw_end_ts_ns: Some(3_000_000_000),
        market_symbol_allowlist: Vec::new(),
        overwrite: false,
    })
    .unwrap();
    assert!(!report.reused_existing);
    assert_eq!(report.book_row_count, 3);
    assert!(cache_root.join("catalog.hftbook2.json").exists());

    let direct_plan = fixture.plan(false);
    build_facts(&direct_plan).unwrap();
    let direct_rows = read_parquet_table::<PolymarketBookTop10Row>(
        &direct_plan
            .dataset_root
            .join("tables/polymarket_book_top10"),
    )
    .unwrap();

    let mut cached_plan = fixture.plan(false);
    cached_plan.raw_roots.clear();
    cached_plan.dataset_root = fixture.root.path().join("dataset-from-book-state-cache");
    cached_plan.book_state_cache_root = Some(cache_root);
    build_facts(&cached_plan).unwrap();
    let cached_rows = read_parquet_table::<PolymarketBookTop10Row>(
        &cached_plan
            .dataset_root
            .join("tables/polymarket_book_top10"),
    )
    .unwrap();
    let cached_markets =
        read_parquet_table::<MarketDimRow>(&cached_plan.dataset_root.join("tables/market_dim"))
            .unwrap();

    assert_eq!(cached_rows, direct_rows);
    assert_eq!(cached_markets.len(), 2);
}

#[test]
fn appended_book_state_cache_partition_feeds_build_facts() {
    let fixture = Fixture::new_ws_raw();
    let direct_plan = fixture.plan(false);
    build_facts(&direct_plan).unwrap();
    let direct_rows = read_parquet_table::<PolymarketBookTop10Row>(
        &direct_plan
            .dataset_root
            .join("tables/polymarket_book_top10"),
    )
    .unwrap();

    let cache_root = fixture.root.path().join("book_state_cache_partitioned");
    let append_report = append_book_state_cache_partition(
        &AppendBookStateCachePartitionOptions {
            cache_root: cache_root.clone(),
            partition_id: "fixture_ws_flush_1".to_string(),
            raw_roots: vec![fixture.raw_root.clone()],
        },
        &direct_rows,
    )
    .unwrap();
    assert_eq!(append_report.book_row_count, 3);

    let mut cached_plan = fixture.plan(false);
    cached_plan.raw_roots.clear();
    cached_plan.dataset_root = fixture
        .root
        .path()
        .join("dataset-from-partitioned-book-state-cache");
    cached_plan.book_state_cache_root = Some(cache_root);
    build_facts(&cached_plan).unwrap();
    let cached_rows = read_parquet_table::<PolymarketBookTop10Row>(
        &cached_plan
            .dataset_root
            .join("tables/polymarket_book_top10"),
    )
    .unwrap();

    assert_eq!(cached_rows, direct_rows);
}

#[test]
fn export_requires_fresh_accepted_dataset_and_writes_manifest() {
    let fixture = Fixture::new();
    let plan = fixture.plan(false);
    sync_inputs(&plan, &DefaultFetcher).unwrap();
    build_facts(&plan).unwrap();
    build_depth_feature(&plan).unwrap();
    build_event_index(&plan).unwrap();

    let export_root = fixture.root.path().join("exported");
    let err = export_dataset(&plan, &export_root).unwrap_err().to_string();
    assert!(err.contains("acceptance"));

    accept(&plan).unwrap();
    export_dataset(&plan, &export_root).unwrap();
    let manifest: ExportManifest =
        serde_json::from_reader(fs::File::open(export_root.join("export_manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest.dataset_format, "pm5m_export_manifest.v1");
    assert!(manifest
        .copied_file_refs
        .contains_key("tables/polymarket_book_top10/part-00000.parquet"));
}

#[test]
fn prepare_caches_downloads_reference_and_settlement_inputs() {
    let fixture = Fixture::new();
    let mut plan = fixture.plan(false);
    plan.binance_sources.clear();
    plan.settlement_sources.clear();

    build_facts(&plan).unwrap();

    let manifest = prepare_caches(&plan, &FakeCacheHttp::default()).unwrap();
    assert_eq!(manifest.records.len(), 2);
    assert!(manifest
        .records
        .iter()
        .all(|record| record.status == CacheRecordStatus::Available));
    assert!(manifest.records.iter().any(|record| {
        record.name == "binance.BTCUSDT.1s.1970-01-01"
            && record.cache_path.as_deref() == Some(Path::new("binance_1s/BTCUSDT/1970-01-01"))
    }));
    let settlement_record = manifest
        .records
        .iter()
        .find(|record| record.name == "polymarket-settlement-shared")
        .unwrap();
    assert_eq!(
        settlement_record.cache_path.as_deref(),
        Some(Path::new("polymarket_settlement/settlement_cache.jsonl"))
    );
    let settlement_cache_path = plan
        .cache_root
        .join(settlement_record.cache_path.as_ref().unwrap());
    let mut settlement_cache = fs::OpenOptions::new()
        .append(true)
        .open(&settlement_cache_path)
        .unwrap();
    writeln!(
        settlement_cache,
        "{}",
        json!({
            "condition_id": "cond-extra",
            "asset_id": "asset-extra",
            "outcome": "YES",
            "status": "settled",
            "winner": true,
            "settled_ts_ns": null
        })
    )
    .unwrap();

    build_facts(&plan).unwrap();

    let reference_rows = read_parquet_table::<BinanceKline1sReferenceRow>(
        &plan.dataset_root.join("tables/binance_kline_1s_reference"),
    )
    .unwrap();
    assert_eq!(reference_rows.len(), 2);
    assert_eq!(reference_rows[0].symbol, "BTCUSDT");
    assert_eq!(reference_rows[0].bar_open_ts_ns, 0);
    assert_eq!(reference_rows[1].bar_open_ts_ns, 1_000_000_000);

    let settlement_rows = read_parquet_table::<PolymarketSettlementRow>(
        &plan.dataset_root.join("tables/polymarket_settlement"),
    )
    .unwrap();
    assert_eq!(settlement_rows.len(), 2);
    assert!(settlement_rows
        .iter()
        .all(|row| { row.condition_id == "cond-1" && row.status == SettlementStatus::Settled }));
    assert!(settlement_rows
        .iter()
        .any(|row| row.outcome == "YES" && row.winner == Some(true)));
    assert!(settlement_rows
        .iter()
        .any(|row| row.outcome == "NO" && row.winner == Some(false)));
}

#[test]
fn prepare_caches_records_binance_failure_without_dropping_settlement() {
    let fixture = Fixture::new();
    let mut plan = fixture.plan(false);
    plan.binance_sources.clear();
    plan.settlement_sources.clear();

    build_facts(&plan).unwrap();

    let manifest = prepare_caches(
        &plan,
        &FakeCacheHttp {
            fail_binance: true,
            ..Default::default()
        },
    )
    .unwrap();
    let binance = manifest
        .records
        .iter()
        .find(|record| record.name.starts_with("binance.BTCUSDT.1s."))
        .unwrap();
    assert_eq!(binance.status, CacheRecordStatus::Failed);
    assert!(binance.cache_path.is_none());

    let settlement = manifest
        .records
        .iter()
        .find(|record| record.name == "polymarket-settlement-shared")
        .unwrap();
    assert_eq!(settlement.status, CacheRecordStatus::Available);
    assert!(settlement.cache_path.is_some());
}

#[test]
fn prepare_caches_reuses_shared_cache_without_http_when_complete() {
    let fixture = Fixture::new();
    let mut plan = fixture.plan(false);
    plan.binance_sources.clear();
    plan.settlement_sources.clear();

    build_facts(&plan).unwrap();

    let first_http = FakeCacheHttp::default();
    prepare_caches(&plan, &first_http).unwrap();
    assert!(first_http.call_count() > 0);

    let second_http = FakeCacheHttp {
        fail_binance: true,
        fail_settlement: true,
        ..Default::default()
    };
    let manifest = prepare_caches(&plan, &second_http).unwrap();
    assert_eq!(second_http.call_count(), 0);
    assert_eq!(manifest.records.len(), 2);
    assert!(manifest
        .records
        .iter()
        .all(|record| record.status == CacheRecordStatus::Available));
}

#[test]
fn prepare_caches_refreshes_unknown_settlement_cache() {
    let fixture = Fixture::new();
    let mut plan = fixture.plan(false);
    plan.binance_sources.clear();
    plan.settlement_sources.clear();

    build_facts(&plan).unwrap();
    prepare_caches(&plan, &FakeCacheHttp::default()).unwrap();

    let settlement_cache_path = plan
        .cache_root
        .join("polymarket_settlement")
        .join("settlement_cache.jsonl");
    fs::create_dir_all(settlement_cache_path.parent().unwrap()).unwrap();
    write_jsonl(
        &settlement_cache_path,
        &[json!({
            "condition_id": "cond-1",
            "asset_id": "asset-yes",
            "outcome": "YES",
            "status": "unknown",
            "winner": null,
            "settled_ts_ns": null
        })],
    );

    let http = FakeCacheHttp {
        fail_binance: true,
        ..Default::default()
    };
    let manifest = prepare_caches(&plan, &http).unwrap();
    assert!(http.call_count() > 0);
    assert_eq!(manifest.records.len(), 2);
    let refreshed = fs::read_to_string(settlement_cache_path).unwrap();
    assert!(refreshed.contains("\"status\":\"settled\""));
    assert!(refreshed.contains("\"winner\":true"));
}

#[test]
fn prepare_caches_records_and_retries_failed_settlement_cache() {
    let fixture = Fixture::new();
    let mut plan = fixture.plan(false);
    plan.binance_sources.clear();
    plan.settlement_sources.clear();

    build_facts(&plan).unwrap();

    let settlement_cache_path = plan
        .cache_root
        .join("polymarket_settlement")
        .join("settlement_cache.jsonl");
    let first_http = FakeCacheHttp {
        fail_settlement: true,
        ..Default::default()
    };
    let first_manifest = prepare_caches(&plan, &first_http).unwrap();
    let first_record = first_manifest
        .records
        .iter()
        .find(|record| record.name == "polymarket-settlement-shared")
        .unwrap();
    assert_eq!(first_record.missing_count, 2);
    assert!(first_record
        .failure_reason
        .as_deref()
        .unwrap()
        .contains("fixture settlement failure"));
    let failed = fs::read_to_string(&settlement_cache_path).unwrap();
    assert!(failed.contains("\"status\":\"failed\""));
    assert!(failed.contains("\"failure_reason\":\"download_failed: fixture settlement failure\""));

    let second_http = FakeCacheHttp {
        fail_binance: true,
        ..Default::default()
    };
    let second_manifest = prepare_caches(&plan, &second_http).unwrap();
    let second_record = second_manifest
        .records
        .iter()
        .find(|record| record.name == "polymarket-settlement-shared")
        .unwrap();
    assert_eq!(second_record.missing_count, 0);
    assert!(second_record.failure_reason.is_none());
    let refreshed = fs::read_to_string(settlement_cache_path).unwrap();
    assert!(refreshed.contains("\"status\":\"settled\""));
    assert!(refreshed.contains("\"winner\":true"));
    assert!(!refreshed.contains("\"failure_reason\""));
}

#[derive(Debug, Default)]
struct FakeCacheHttp {
    fail_binance: bool,
    fail_settlement: bool,
    calls: AtomicUsize,
}

impl FakeCacheHttp {
    fn call_count(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl CachePreparationHttp for FakeCacheHttp {
    fn get(&self, url: &str) -> Result<Vec<u8>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if url.contains("/api/v3/klines") {
            if self.fail_binance {
                bail!("fixture Binance failure");
            }
            return Ok(
                br#"[[0,"100.0","101.0","99.0","100.5","42.0",999,"4200.0"],[1000,"100.5","102.0","100.0","101.5","43.0",1999,"4343.0"]]"#
                    .to_vec(),
            );
        }
        if url.ends_with("/markets/cond-1") {
            if self.fail_settlement {
                bail!("fixture settlement failure");
            }
            return Ok(br#"{"condition_id":"cond-1","closed":true,"tokens":[{"token_id":"asset-yes","outcome":"YES","winner":true,"price":"1"},{"token_id":"asset-no","outcome":"NO","winner":false,"price":"0"}]}"#.to_vec());
        }
        bail!("unexpected fixture URL: {url}");
    }
}

struct Fixture {
    root: TempDir,
    raw_root: PathBuf,
    book_cache_root: PathBuf,
    binance_cache_fixture: PathBuf,
    settlement_cache_fixture: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let raw_root = root.path().join("raw");
        let book_cache_root = root.path().join("book_cache");
        let fixtures_root = root.path().join("fixtures");
        fs::create_dir_all(&raw_root).unwrap();
        fs::create_dir_all(&fixtures_root).unwrap();
        write_fixture_book_cache(
            &book_cache_root,
            vec![book_cache_row_yes(), book_cache_row_no()],
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
            &[
                json!({
                    "condition_id": "cond-1",
                    "asset_id": "asset-yes",
                    "outcome": "YES",
                    "status": "settled",
                    "winner": true,
                    "settled_ts_ns": null
                }),
                json!({
                    "condition_id": "cond-1",
                    "asset_id": "asset-no",
                    "outcome": "NO",
                    "status": "settled",
                    "winner": false,
                    "settled_ts_ns": null
                }),
            ],
        );

        Self {
            root,
            raw_root,
            book_cache_root,
            binance_cache_fixture,
            settlement_cache_fixture,
        }
    }

    fn new_jsonl_raw_only() -> Self {
        let fixture = Self::new();
        fs::remove_dir_all(&fixture.raw_root).unwrap();
        fs::create_dir_all(&fixture.raw_root).unwrap();
        write_jsonl(
            &fixture.raw_root.join("polymarket_book_top10.jsonl"),
            &[json!({
                "symbol": "BTC-5M",
                "condition_id": "cond-1",
                "asset_id": "asset-yes",
                "outcome": "YES",
                "local_recv_ts_ns": 2_000_000_000_i64,
                "ingest_seq": 7_u64,
                "bids": [{"price": 0.58, "size": 2.0}],
                "asks": [{"price": 0.60, "size": 3.0}]
            })],
        );
        fixture
    }

    fn new_ws_raw() -> Self {
        let fixture = Self::new();
        fs::remove_dir_all(&fixture.raw_root).unwrap();
        fs::create_dir_all(&fixture.raw_root).unwrap();
        let mut btc_15m = raw_ws_row(
            3,
            2_200_000_000,
            "book",
            Some("BTC-15M"),
            Some("asset-ignored"),
            json!({
                "event_type": "book",
                "asset_id": "asset-ignored",
                "market": "cond-ignored",
                "bids": [{"price": "0.50", "size": "1"}],
                "asks": [{"price": "0.51", "size": "1"}]
            }),
        );
        btc_15m.condition_id = Some("cond-ignored".to_string());
        btc_15m.yes_asset_id = Some("asset-ignored".to_string());
        btc_15m.no_asset_id = Some("asset-ignored-no".to_string());
        let ws_rows = vec![
            raw_ws_row(
                1,
                2_000_000_000,
                "book",
                Some("ETH"),
                Some("asset-yes"),
                json!({
                    "event_type": "book",
                    "asset_id": "asset-yes",
                    "market": "cond-1",
                    "bids": [{"price": "0.58", "size": "2"}],
                    "asks": [{"price": "0.60", "size": "3"}]
                }),
            ),
            raw_ws_row(
                2,
                2_100_000_000,
                "price_change",
                None,
                None,
                json!({
                    "event_type": "price_change",
                    "market": "cond-1",
                    "price_changes": [
                        {"asset_id": "asset-yes", "side": "BUY", "price": "0.59", "size": "4"},
                        {"asset_id": "asset-yes", "side": "SELL", "price": "0.60", "size": "0"}
                    ]
                }),
            ),
            btc_15m,
        ];
        let hftrec4_rows = ws_rows.iter().map(hftrec4_ws_record).collect::<Vec<_>>();
        market_data_etl_core::write_hftrec4_segment(
            &fixture
                .raw_root
                .join("polymarket_clob_ws_raw/hour_bucket=0/part-00000.hfr4"),
            &hftrec4_rows,
        )
        .unwrap();
        fs::remove_dir_all(&fixture.book_cache_root).unwrap();
        pm5m_market_cache::build_book_cache(&pm5m_market_cache::BuildBookCacheOptions {
            raw_roots: vec![fixture.raw_root.clone()],
            cache_root: fixture.book_cache_root.clone(),
            raw_start_ts_ns: Some(0),
            raw_end_ts_ns: Some(3_000_000_000),
            market_symbol_allowlist: Vec::new(),
            overwrite: true,
            replay_workers: Some(1),
            enrich_missing_clob_metadata: false,
            clob_metadata_cache_root: None,
            condition_allowlist_path: None,
            poly_server_visible_time: false,
            poly_incremental_latency_ms: pm5m_market_cache::DEFAULT_POLY_INCREMENTAL_LATENCY_MS,
            poly_incremental_freshness_guard_ms:
                pm5m_market_cache::DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS,
        })
        .unwrap();
        fixture
    }

    fn plan(&self, fail_closed: bool) -> PipelinePlan {
        let mut plan = PipelinePlan::new(
            vec![self.raw_root.clone()],
            self.root.path().join("plan"),
            self.root.path().join("cache"),
            self.root.path().join("dataset"),
        );
        plan.book_state_cache_root = Some(self.book_cache_root.clone());
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

#[derive(Debug, Clone, Serialize)]
struct TestWsEvent {
    source_id: String,
    ingest_seq_scope: String,
    ingest_seq: u64,
    local_recv_ts_ns: i64,
    connection_id: u64,
    subscription_epoch: u64,
    asset_id: Option<String>,
    condition_id: Option<String>,
    symbol: Option<String>,
    outcome: Option<String>,
    market_start_ts_ns: Option<i64>,
    market_end_ts_ns: Option<i64>,
    yes_asset_id: Option<String>,
    no_asset_id: Option<String>,
    event_type: String,
    exchange_ts_ms: Option<i64>,
    raw_payload: Vec<u8>,
    raw_payload_sha256: String,
    raw_record_hash: String,
}

fn raw_ws_row(
    ingest_seq: u64,
    local_recv_ts_ns: i64,
    event_type: &str,
    symbol: Option<&str>,
    asset_id: Option<&str>,
    payload: serde_json::Value,
) -> TestWsEvent {
    let raw_payload = serde_json::to_vec(&payload).unwrap();
    TestWsEvent {
        source_id: "polymarket_clob_ws_raw".to_string(),
        ingest_seq_scope: "fixture-ws".to_string(),
        ingest_seq,
        local_recv_ts_ns,
        connection_id: 1,
        subscription_epoch: 1,
        asset_id: asset_id.map(str::to_string),
        condition_id: Some("cond-1".to_string()),
        symbol: symbol.map(str::to_string),
        outcome: Some("YES".to_string()),
        market_start_ts_ns: Some(0),
        market_end_ts_ns: Some(300_000_000_000),
        yes_asset_id: Some("asset-yes".to_string()),
        no_asset_id: Some("asset-no".to_string()),
        event_type: event_type.to_string(),
        exchange_ts_ms: Some(local_recv_ts_ns / 1_000_000),
        raw_payload_sha256: market_data_etl_core::sha256_bytes(&raw_payload),
        raw_payload,
        raw_record_hash: format!("ws-raw-hash-{ingest_seq}"),
    }
}

fn hftrec4_ws_record(row: &TestWsEvent) -> market_data_etl_core::Hftrec4WriteRecord {
    market_data_etl_core::Hftrec4WriteRecord {
        ingest_seq: row.ingest_seq,
        local_recv_ts_ns: row.local_recv_ts_ns,
        event_type: row.event_type.clone(),
        symbol: row.symbol.clone(),
        condition_id: row.condition_id.clone(),
        asset_id: row.asset_id.clone(),
        market_start_ts_ns: row.market_start_ts_ns,
        market_end_ts_ns: row.market_end_ts_ns,
        yes_asset_id: row.yes_asset_id.clone(),
        no_asset_id: row.no_asset_id.clone(),
        payload: row.raw_payload.clone(),
    }
}

fn write_fixture_book_cache(cache_root: &Path, rows: Vec<BookCacheRow>) {
    pm5m_market_cache::write_book_cache2(
        &WriteBookCache2Options {
            cache_root: cache_root.to_path_buf(),
            raw_roots: Vec::new(),
            raw_start_ts_ns: None,
            raw_end_ts_ns: None,
            market_symbol_allowlist: Vec::new(),
            overwrite: true,
            poly_server_visible_time: false,
            poly_incremental_latency_ms: pm5m_market_cache::DEFAULT_POLY_INCREMENTAL_LATENCY_MS,
            poly_incremental_freshness_guard_ms:
                pm5m_market_cache::DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS,
        },
        rows,
    )
    .unwrap();
}

fn book_cache_row_yes() -> BookCacheRow {
    BookCacheRow {
        symbol: "BTC-5M".to_string(),
        condition_id: "cond-1".to_string(),
        asset_id: "asset-yes".to_string(),
        outcome: "YES".to_string(),
        window_start_ts_ns: 0,
        window_end_ts_ns: 300_000_000_000,
        yes_asset_id: "asset-yes".to_string(),
        no_asset_id: "asset-no".to_string(),
        local_recv_ts_ns: 2_000_000_000,
        exchange_ts_ms: Some(2_000),
        ingest_seq: 7,
        best_bid_price_micros: Some(580_000),
        best_ask_price_micros: Some(600_000),
        bid_levels: [
            Some(BookLevelMicros {
                price_micros: 580_000,
                qty_micros: 2_000_000,
            }),
            Some(BookLevelMicros {
                price_micros: 570_000,
                qty_micros: 4_000_000,
            }),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        ask_levels: [
            Some(BookLevelMicros {
                price_micros: 600_000,
                qty_micros: 3_000_000,
            }),
            Some(BookLevelMicros {
                price_micros: 625_000,
                qty_micros: 4_000_000,
            }),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        raw_row_hash: "raw-hash".to_string(),
        raw_payload_sha256: "payload-hash".to_string(),
        book_state_hash: "book-state-hash".to_string(),
    }
}

fn book_cache_row_no() -> BookCacheRow {
    let mut row = book_cache_row_yes();
    row.asset_id = "asset-no".to_string();
    row.outcome = "NO".to_string();
    row.local_recv_ts_ns = 2_000_100_000;
    row.exchange_ts_ms = Some(2_000);
    row.ingest_seq = 8;
    row.raw_row_hash = "raw-hash-no".to_string();
    row.raw_payload_sha256 = "payload-hash-no".to_string();
    row.book_state_hash = "book-state-hash-no".to_string();
    row
}

fn read_event_index_rows(plan: &PipelinePlan) -> Vec<StandardEventIndexRow> {
    read_parquet_table::<StandardEventIndexRow>(
        &plan
            .dataset_root
            .join("streams/pm5m_standard_event_index_v1"),
    )
    .unwrap()
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
