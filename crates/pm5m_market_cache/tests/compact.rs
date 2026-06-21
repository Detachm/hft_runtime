use pm5m_market_cache::{
    build_reference_cache, build_settlement_cache, read_reference_cache, read_settlement_cache,
    write_book_cache2, BookCacheRow, BookLevelMicros, BuildReferenceCacheOptions,
    BuildSettlementCacheOptions, WriteBookCache2Options,
};
use serde::Serialize;

#[derive(Serialize)]
struct ReferenceInputRow {
    symbol: String,
    synthetic_local_recv_ts_ns: i64,
    ingest_seq: u64,
    close: f64,
    row_hash: String,
}

#[derive(Serialize)]
struct SettlementInputRow {
    condition_id: String,
    asset_id: String,
    outcome: String,
    status: String,
    winner: Option<bool>,
    settled_ts_ns: Option<i64>,
    row_hash: String,
}

#[test]
fn hftref1_builds_from_reference_parquet() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("reference_table");
    market_data_etl_core::write_parquet_table(
        &input,
        &[ReferenceInputRow {
            symbol: "ETH".to_string(),
            synthetic_local_recv_ts_ns: 1_000,
            ingest_seq: 7,
            close: 2525.25,
            row_hash: "ref-hash".to_string(),
        }],
        Some("synthetic_local_recv_ts_ns"),
    )
    .unwrap();
    let cache_root = temp.path().join("hftref1");
    build_reference_cache(&BuildReferenceCacheOptions {
        input_table: input,
        cache_root: cache_root.clone(),
        overwrite: true,
    })
    .unwrap();
    let rows = read_reference_cache(&cache_root).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].symbol, "ETH-5M");
    assert_eq!(rows[0].close_micros, 2_525_250_000);
}

#[test]
fn hftsettle1_collapses_asset_rows_to_condition_result() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("settlement_table");
    market_data_etl_core::write_parquet_table(
        &input,
        &[
            SettlementInputRow {
                condition_id: "cond-1".to_string(),
                asset_id: "yes-asset".to_string(),
                outcome: "YES".to_string(),
                status: "settled".to_string(),
                winner: Some(true),
                settled_ts_ns: Some(5_000),
                row_hash: "yes-hash".to_string(),
            },
            SettlementInputRow {
                condition_id: "cond-1".to_string(),
                asset_id: "no-asset".to_string(),
                outcome: "NO".to_string(),
                status: "settled".to_string(),
                winner: Some(false),
                settled_ts_ns: Some(5_000),
                row_hash: "no-hash".to_string(),
            },
        ],
        Some("settled_ts_ns"),
    )
    .unwrap();
    let cache_root = temp.path().join("hftsettle1");
    build_settlement_cache(&BuildSettlementCacheOptions {
        input_table: input,
        cache_root: cache_root.clone(),
        overwrite: true,
        allow_unsettled: false,
        book_cache_root: None,
        filter_to_book_cache: false,
    })
    .unwrap();
    let rows = read_settlement_cache(&cache_root).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].condition_id, "cond-1");
    assert_eq!(rows[0].winner_asset_id, "yes-asset");
    assert_eq!(rows[0].winner_outcome, "YES");
}

#[test]
fn hftsettle1_fills_missing_asset_side_from_hftbook2_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let book_cache_root = temp.path().join("hftbook2");
    write_book_cache2(
        &WriteBookCache2Options {
            cache_root: book_cache_root.clone(),
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
        vec![BookCacheRow {
            symbol: "ETH-5M".to_string(),
            condition_id: "cond-2".to_string(),
            asset_id: "yes-asset".to_string(),
            outcome: "YES".to_string(),
            window_start_ts_ns: 1_000,
            window_end_ts_ns: 301_000,
            yes_asset_id: "yes-asset".to_string(),
            no_asset_id: "no-asset".to_string(),
            local_recv_ts_ns: 2_000,
            exchange_ts_ms: Some(2),
            ingest_seq: 1,
            best_bid_price_micros: Some(400_000),
            best_ask_price_micros: Some(410_000),
            bid_levels: [Some(BookLevelMicros {
                price_micros: 400_000,
                qty_micros: 1_000_000,
            }); 10],
            ask_levels: [Some(BookLevelMicros {
                price_micros: 410_000,
                qty_micros: 1_000_000,
            }); 10],
            raw_row_hash: "raw-hash".to_string(),
            raw_payload_sha256: "payload-hash".to_string(),
            book_state_hash: "book-state-hash".to_string(),
        }],
    )
    .unwrap();

    let input = temp.path().join("settlement_table");
    market_data_etl_core::write_parquet_table(
        &input,
        &[SettlementInputRow {
            condition_id: "cond-2".to_string(),
            asset_id: "yes-asset".to_string(),
            outcome: "YES".to_string(),
            status: "settled".to_string(),
            winner: Some(true),
            settled_ts_ns: Some(5_000),
            row_hash: "winner-hash".to_string(),
        }],
        Some("settled_ts_ns"),
    )
    .unwrap();
    let cache_root = temp.path().join("hftsettle1");
    build_settlement_cache(&BuildSettlementCacheOptions {
        input_table: input,
        cache_root: cache_root.clone(),
        overwrite: true,
        allow_unsettled: false,
        book_cache_root: Some(book_cache_root),
        filter_to_book_cache: false,
    })
    .unwrap();
    let rows = read_settlement_cache(&cache_root).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].yes_asset_id, "yes-asset");
    assert_eq!(rows[0].no_asset_id, "no-asset");
    assert_eq!(rows[0].winner_asset_id, "yes-asset");
}
