use pm5m_market_cache::{
    bench_book_state_index, build_book_state_index, open_book_state_index_reader,
    read_book_state_index, validate_book_state_index, write_book_cache2, BookCacheRow,
    BookLevelMicros, BuildBookStateIndexOptions, WriteBookCache2Options,
};

#[test]
fn hftidx1_builds_with_sparse_book_asset_series_and_filters_by_time() {
    let temp = tempfile::tempdir().unwrap();
    let book_cache_root = temp.path().join("hftbook2");
    let index_root = temp.path().join("hftidx1");
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
        vec![
            row(
                "SOL-5M",
                "cond-a",
                "yes-a",
                "no-a",
                "YES",
                2_000_000_000,
                700_000,
            ),
            row(
                "SOL-5M",
                "cond-b",
                "yes-b",
                "no-b",
                "YES",
                3_000_000_000,
                500_000,
            ),
        ],
    )
    .unwrap();

    let catalog = build_book_state_index(&BuildBookStateIndexOptions {
        book_cache_root,
        index_root: index_root.clone(),
        start_ts_ns: None,
        end_ts_ns: None,
        overwrite: true,
    })
    .unwrap();
    assert_eq!(catalog.row_count, 2);
    assert_eq!(catalog.asset_count, 4);

    let validation = validate_book_state_index(&index_root).unwrap();
    assert_eq!(validation.row_count, 2);

    let full = read_book_state_index(&index_root, None, None).unwrap();
    assert_eq!(full.rows.len(), 2);
    assert_eq!(full.header.asset_ranges.len(), 2);

    let filtered =
        read_book_state_index(&index_root, Some(2_500_000_000), Some(3_500_000_000)).unwrap();
    assert_eq!(filtered.rows.len(), 1);
    assert_eq!(filtered.rows[0].local_recv_ts_ns, 3_000_000_000);

    let bench =
        bench_book_state_index(&index_root, Some(2_500_000_000), Some(3_500_000_000)).unwrap();
    assert_eq!(bench.rows_read, 1);
}

#[test]
fn hftidx1_reader_state_at_uses_visible_row_without_lookahead() {
    let temp = tempfile::tempdir().unwrap();
    let book_cache_root = temp.path().join("hftbook2");
    let index_root = temp.path().join("hftidx1");
    let mut first = row(
        "SOL-5M",
        "cond-a",
        "yes-a",
        "no-a",
        "YES",
        2_000_000_000,
        700_000,
    );
    let mut second = row(
        "SOL-5M",
        "cond-a",
        "yes-a",
        "no-a",
        "YES",
        3_000_000_000,
        500_000,
    );
    first.window_start_ts_ns = 1_000_000_000;
    first.window_end_ts_ns = 10_000_000_000;
    second.window_start_ts_ns = first.window_start_ts_ns;
    second.window_end_ts_ns = first.window_end_ts_ns;
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
        vec![first, second],
    )
    .unwrap();
    build_book_state_index(&BuildBookStateIndexOptions {
        book_cache_root,
        index_root: index_root.clone(),
        start_ts_ns: None,
        end_ts_ns: None,
        overwrite: true,
    })
    .unwrap();

    let reader = open_book_state_index_reader(&index_root).unwrap();
    let yes_asset_key = reader
        .header()
        .assets
        .iter()
        .position(|asset| asset.asset_id == "yes-a")
        .unwrap() as u32;
    assert!(reader
        .state_at(yes_asset_key, 1_999_999_999)
        .unwrap()
        .is_none());
    let before_improvement = reader
        .state_at(yes_asset_key, 2_500_000_000)
        .unwrap()
        .unwrap();
    assert_eq!(before_improvement.best_ask_price_micros, Some(700_000));
    let after_improvement = reader
        .state_at(yes_asset_key, 3_000_000_000)
        .unwrap()
        .unwrap();
    assert_eq!(after_improvement.best_ask_price_micros, Some(500_000));
}

fn row(
    symbol: &str,
    condition_id: &str,
    yes_asset_id: &str,
    no_asset_id: &str,
    outcome: &str,
    ts_ns: i64,
    ask_price_micros: i64,
) -> BookCacheRow {
    let asset_id = if outcome == "YES" {
        yes_asset_id
    } else {
        no_asset_id
    };
    BookCacheRow {
        symbol: symbol.to_string(),
        condition_id: condition_id.to_string(),
        asset_id: asset_id.to_string(),
        outcome: outcome.to_string(),
        window_start_ts_ns: ts_ns - 2_000_000_000,
        window_end_ts_ns: ts_ns + 60_000_000_000,
        yes_asset_id: yes_asset_id.to_string(),
        no_asset_id: no_asset_id.to_string(),
        local_recv_ts_ns: ts_ns,
        exchange_ts_ms: Some(ts_ns / 1_000_000),
        ingest_seq: ts_ns as u64,
        best_bid_price_micros: Some(ask_price_micros - 10_000),
        best_ask_price_micros: Some(ask_price_micros),
        bid_levels: [Some(BookLevelMicros {
            price_micros: ask_price_micros - 10_000,
            qty_micros: 1_000_000,
        }); 10],
        ask_levels: [Some(BookLevelMicros {
            price_micros: ask_price_micros,
            qty_micros: 1_000_000,
        }); 10],
        raw_row_hash: format!("hash-{condition_id}-{ts_ns}"),
        raw_payload_sha256: format!("payload-hash-{condition_id}-{ts_ns}"),
        book_state_hash: format!("book-state-hash-{condition_id}-{ts_ns}"),
    }
}
