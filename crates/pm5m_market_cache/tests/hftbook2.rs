use pm5m_market_cache::{
    build_book_cache, read_book_cache2_rows, scan_book_cache2, stream_book_cache2_rows_ordered,
    validate_book_cache2, write_book_cache2, BookCache2ScanFilter, BookCacheRow, BookLevelMicros,
    BuildBookCacheOptions, WriteBookCache2Options,
};

#[test]
fn hftbook2_roundtrip_and_validate() {
    let temp = tempfile::tempdir().unwrap();
    let cache_root = temp.path().join("book2");
    let rows = vec![row("ETH-5M", "cond-1", "asset-yes", "YES", 1_000, 1)];
    let catalog = write_book_cache2(
        &WriteBookCache2Options {
            cache_root: cache_root.clone(),
            raw_roots: vec![temp.path().join("raw")],
            raw_start_ts_ns: None,
            raw_end_ts_ns: None,
            market_symbol_allowlist: Vec::new(),
            overwrite: true,
            poly_server_visible_time: false,
            poly_incremental_latency_ms: pm5m_market_cache::DEFAULT_POLY_INCREMENTAL_LATENCY_MS,
            poly_incremental_freshness_guard_ms:
                pm5m_market_cache::DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS,
        },
        rows.clone(),
    )
    .unwrap();
    assert_eq!(catalog.row_count, 1);
    assert_eq!(validate_book_cache2(&cache_root).unwrap().row_count, 1);

    let got = read_book_cache2_rows(&cache_root, None, None).unwrap();
    assert_eq!(got, rows);
}

#[test]
fn hftbook2_scan_filters_time_and_condition() {
    let temp = tempfile::tempdir().unwrap();
    let cache_root = temp.path().join("book2");
    write_book_cache2(
        &WriteBookCache2Options {
            cache_root: cache_root.clone(),
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
        vec![
            row("ETH-5M", "cond-1", "asset-yes", "YES", 1_000, 1),
            row("ETH-5M", "cond-2", "asset-no", "NO", 2_000, 2),
        ],
    )
    .unwrap();

    let mut seen = Vec::new();
    let count = scan_book_cache2(
        &cache_root,
        Some(1_500),
        Some(3_000),
        &BookCache2ScanFilter {
            symbol: None,
            condition_id: Some("cond-2".to_string()),
        },
        |row| {
            seen.push(row.condition_id);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(seen, vec!["cond-2"]);
}

#[test]
fn hftbook2_ordered_stream_merges_shards_by_event_time() {
    let temp = tempfile::tempdir().unwrap();
    let cache_root = temp.path().join("book2");
    let mut early = row("ETH-5M", "cond-1", "asset-early", "YES", 1_000, 1);
    early.yes_asset_id = "asset-early".to_string();
    early.no_asset_id = "asset-early-no".to_string();
    let mut late = row("ETH-5M", "cond-2", "asset-late", "YES", 2_000, 2);
    late.yes_asset_id = "asset-late".to_string();
    late.no_asset_id = "asset-late-no".to_string();
    write_book_cache2(
        &WriteBookCache2Options {
            cache_root: cache_root.clone(),
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
        vec![late, early],
    )
    .unwrap();

    let mut seen = Vec::new();
    stream_book_cache2_rows_ordered(
        &cache_root,
        None,
        None,
        &BookCache2ScanFilter::default(),
        |row| {
            seen.push(row.local_recv_ts_ns);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(seen, vec![1_000, 2_000]);
}

#[test]
fn hftbook2_build_from_hftrec4_infers_outcome_from_asset_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let raw_root = temp.path().join("raw");
    let segment = raw_root
        .join(pm5m_market_cache::WS_RAW_STREAM)
        .join("hour_bucket=0")
        .join("part.hfr4");
    market_data_etl_core::write_hftrec4_segment(
        &segment,
        &[
            hftrec4_book_record("asset-yes", 1, 1_000),
            hftrec4_book_record("asset-no", 2, 1_100),
        ],
    )
    .unwrap();

    let cache_root = temp.path().join("book2");
    let report = build_book_cache(&BuildBookCacheOptions {
        raw_roots: vec![raw_root],
        cache_root: cache_root.clone(),
        raw_start_ts_ns: None,
        raw_end_ts_ns: None,
        market_symbol_allowlist: Vec::new(),
        overwrite: true,
        replay_workers: Some(4),
        enrich_missing_clob_metadata: false,
        clob_metadata_cache_root: None,
        condition_allowlist_path: None,
        poly_server_visible_time: false,
        poly_incremental_latency_ms: pm5m_market_cache::DEFAULT_POLY_INCREMENTAL_LATENCY_MS,
        poly_incremental_freshness_guard_ms:
            pm5m_market_cache::DEFAULT_POLY_INCREMENTAL_FRESHNESS_GUARD_MS,
    })
    .unwrap();
    assert_eq!(report.row_count, 2);
    assert_eq!(report.replay_workers, 4);

    let rows = read_book_cache2_rows(&cache_root, None, None).unwrap();
    let outcomes = rows
        .iter()
        .map(|row| (row.asset_id.as_str(), row.outcome.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(outcomes, vec![("asset-yes", "YES"), ("asset-no", "NO")]);
}

fn row(
    symbol: &str,
    condition_id: &str,
    asset_id: &str,
    outcome: &str,
    ts: i64,
    seq: u64,
) -> BookCacheRow {
    BookCacheRow {
        symbol: symbol.to_string(),
        condition_id: condition_id.to_string(),
        asset_id: asset_id.to_string(),
        outcome: outcome.to_string(),
        window_start_ts_ns: 0,
        window_end_ts_ns: 10_000,
        yes_asset_id: "asset-yes".to_string(),
        no_asset_id: "asset-no".to_string(),
        local_recv_ts_ns: ts,
        exchange_ts_ms: Some(ts / 1_000_000),
        ingest_seq: seq,
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
        raw_row_hash: format!("hash-{seq}"),
        raw_payload_sha256: format!("payload-hash-{seq}"),
        book_state_hash: format!("book-state-hash-{seq}"),
    }
}

fn hftrec4_book_record(
    asset_id: &str,
    ingest_seq: u64,
    local_recv_ts_ns: i64,
) -> market_data_etl_core::Hftrec4WriteRecord {
    market_data_etl_core::Hftrec4WriteRecord {
        ingest_seq,
        local_recv_ts_ns,
        event_type: "book".to_string(),
        symbol: Some("ETH-5M".to_string()),
        condition_id: Some("cond-1".to_string()),
        asset_id: Some(asset_id.to_string()),
        market_start_ts_ns: Some(0),
        market_end_ts_ns: Some(10_000),
        yes_asset_id: Some("asset-yes".to_string()),
        no_asset_id: Some("asset-no".to_string()),
        payload: br#"{"event_type":"book","bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.41","size":"10"}]}"#.to_vec(),
    }
}
