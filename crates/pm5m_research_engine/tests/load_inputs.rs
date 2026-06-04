use pm5m_data_etl::{
    build_depth_feature, build_event_index, build_facts, sync_inputs, CacheSourceSpec,
    DefaultFetcher, PipelinePlan,
};
use pm5m_research_engine::load_backtest_inputs;
use serde_json::json;
use std::fs;
use std::path::Path;

#[test]
fn local_research_engine_loads_backtest_inputs_from_jupiter_outputs() {
    let temp = tempfile::tempdir().unwrap();
    let raw_root = temp.path().join("raw");
    let fixture_root = temp.path().join("fixtures");
    fs::create_dir_all(&raw_root).unwrap();
    fs::create_dir_all(&fixture_root).unwrap();

    write_jsonl(
        &raw_root.join("polymarket_book_top10.jsonl"),
        &[json!({
            "symbol": "BTC-5M",
            "condition_id": "cond-1",
            "asset_id": "asset-yes",
            "outcome": "YES",
            "local_recv_ts_ns": 2_000_000_000_i64,
            "ingest_seq": 1_u64,
            "bids": [{"price": 0.48, "size": 1.0}],
            "asks": [{"price": 0.52, "size": 1.0}]
        })],
    );
    let bn = fixture_root.join("bn.jsonl");
    write_jsonl(
        &bn,
        &[json!({
            "symbol": "BTCUSDT",
            "bar_open_ts_ns": 0_i64,
            "bar_close_ts_ns": 1_000_000_000_i64,
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": 7.0
        })],
    );
    let settlement = fixture_root.join("settlement.jsonl");
    write_jsonl(
        &settlement,
        &[json!({
            "condition_id": "cond-1",
            "asset_id": "asset-yes",
            "outcome": "YES",
            "status": "settled",
            "winner": true,
            "settled_ts_ns": 3_000_000_000_i64
        })],
    );

    let mut plan = PipelinePlan::new(
        vec![raw_root],
        temp.path().join("plan"),
        temp.path().join("cache"),
        temp.path().join("dataset"),
    );
    plan.reference_latency_ms = 1_000;
    plan.binance_sources = vec![CacheSourceSpec {
        name: "bn".to_string(),
        source_url: file_url(&bn),
        symbol: Some("BTCUSDT".to_string()),
        start_ts_ns: 0,
        end_ts_ns: 1_000_000_000,
    }];
    plan.settlement_sources = vec![CacheSourceSpec {
        name: "settlement".to_string(),
        source_url: file_url(&settlement),
        symbol: None,
        start_ts_ns: 0,
        end_ts_ns: 3_000_000_000,
    }];

    sync_inputs(&plan, &DefaultFetcher).unwrap();
    build_facts(&plan).unwrap();
    build_depth_feature(&plan).unwrap();
    build_event_index(&plan).unwrap();

    let inputs = load_backtest_inputs(&plan.dataset_root).unwrap();
    assert_eq!(inputs.events.len(), 2);
    assert_eq!(inputs.depth_features.len(), 1);
    assert_eq!(inputs.binance_reference.len(), 1);
    assert_eq!(inputs.settlement_labels.len(), 1);
    assert_eq!(inputs.settlement_labels[0].winner, Some(true));
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
