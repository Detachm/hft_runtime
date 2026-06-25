use market_data_etl_core::{read_parquet_table, write_hftrec4_segment, Hftrec4WriteRecord};
use pm5m_market_cache::{
    bench_market_replay_dataset, build_book_state_index, build_market_replay_compact_typed,
    build_market_replay_dataset, build_market_replay_typed_updates,
    discover_market_replay_compact_typed_files_for_window,
    discover_market_replay_typed_update_files_for_window, open_book_state_index_reader,
    stream_market_replay_events_from_raw, stream_market_replay_raw_updates_from_raw,
    stream_market_replay_typed_updates_from_compact_files,
    stream_market_replay_typed_updates_from_files,
    stream_market_replay_typed_updates_from_raw_parallel, validate_market_replay_compact_typed,
    validate_market_replay_raw_coverage, write_book_cache2, BookCacheRow, BookLevelMicros,
    BuildBookStateIndexOptions, BuildMarketReplayCompactTypedOptions,
    BuildMarketReplayDatasetOptions, BuildMarketReplayTypedUpdatesOptions, MarketEvent,
    MarketReplayRawUpdate, MarketReplaySemantics, PendingReplayBuyOrder, ReplaySettlementState,
    StreamMarketReplayEventsOptions, StreamingMarketReplayState,
    ValidateMarketReplayCompactTypedOptions, WriteBookCache2Options, MARKET_REPLAY_EVENTS_TABLE,
    MARKET_REPLAY_SEMANTICS_ID,
};

#[test]
fn builds_market_replay_events_from_hftrec4_ws_raw() {
    let semantics = MarketReplaySemantics::default();
    assert_eq!(semantics.semantics_id, MARKET_REPLAY_SEMANTICS_ID);
    assert_eq!(semantics.submit_latency_ms, 300);

    let temp = tempfile::tempdir().unwrap();
    let raw_root = temp.path().join("raw");
    let segment_dir = raw_root
        .join("polymarket_clob_ws_raw")
        .join("hour_bucket=0");
    std::fs::create_dir_all(&segment_dir).unwrap();
    let segment_path = segment_dir.join("part.hfr4");
    let reference_segment_dir = raw_root.join("reference_ws_raw").join("hour_bucket=0");
    std::fs::create_dir_all(&reference_segment_dir).unwrap();
    let reference_segment_path = reference_segment_dir.join("part.hfr4");

    write_hftrec4_segment(
        &segment_path,
        &[
            Hftrec4WriteRecord {
                ingest_seq: 10,
                local_recv_ts_ns: 1_000_000_000,
                event_type: "book".to_string(),
                symbol: Some("BTC-5M".to_string()),
                condition_id: Some("cond-1".to_string()),
                asset_id: Some("yes-token".to_string()),
                market_start_ts_ns: Some(0),
                market_end_ts_ns: Some(300_000_000_000),
                yes_asset_id: Some("yes-token".to_string()),
                no_asset_id: Some("no-token".to_string()),
                payload: br#"{"asset_id":"yes-token","condition_id":"cond-1","bids":[{"price":"0.41","size":"11"}],"asks":[{"price":"0.59","size":"7"}],"timestamp":1000}"#.to_vec(),
            },
            Hftrec4WriteRecord {
                ingest_seq: 11,
                local_recv_ts_ns: 1_300_000_000,
                event_type: "book".to_string(),
                symbol: Some("BTC-5M".to_string()),
                condition_id: Some("cond-1".to_string()),
                asset_id: Some("no-token".to_string()),
                market_start_ts_ns: Some(0),
                market_end_ts_ns: Some(300_000_000_000),
                yes_asset_id: Some("yes-token".to_string()),
                no_asset_id: Some("no-token".to_string()),
                payload: br#"{"asset_id":"no-token","condition_id":"cond-1","bids":[{"price":"0.39","size":"5"}],"asks":[{"price":"0.61","size":"6"}],"timestamp":1300}"#.to_vec(),
            },
            Hftrec4WriteRecord {
                ingest_seq: 12,
                local_recv_ts_ns: 1_250_000_000,
                event_type: "price_change".to_string(),
                symbol: Some("BTC-5M".to_string()),
                condition_id: Some("cond-1".to_string()),
                asset_id: None,
                market_start_ts_ns: Some(0),
                market_end_ts_ns: Some(300_000_000_000),
                yes_asset_id: Some("yes-token".to_string()),
                no_asset_id: Some("no-token".to_string()),
                payload: br#"{"market":"cond-1","price_changes":[{"asset_id":"yes-token","side":"BUY","price":"0.42","size":"13"}],"timestamp":1200}"#.to_vec(),
            },
        ],
    )
    .unwrap();
    write_hftrec4_segment(
        &reference_segment_path,
        &[Hftrec4WriteRecord {
            ingest_seq: 20,
            local_recv_ts_ns: 1_310_000_000,
            event_type: "reference_bar".to_string(),
            symbol: Some("binance:BTC".to_string()),
            condition_id: None,
            asset_id: None,
            market_start_ts_ns: Some(0),
            market_end_ts_ns: Some(1_000_000_000),
            yes_asset_id: None,
            no_asset_id: None,
            payload: br#"{"source_id":"binance_1s_ws","venue":"binance","symbol":"BTC","ingest_seq_scope":"pm5m_reference_ws","ingest_seq":20,"local_recv_ts_ns":1310000000,"connection_epoch":1,"event_type":"reference_bar","exchange_event_ts_ms":1300,"bar_open_time_ms":0,"bar_close_time_ms":999,"is_closed":true,"open":"100.0","high":"101.0","low":"99.0","close":"100.5","volume":"1","raw_payload":[],"raw_payload_sha256":"payload","raw_record_hash":"record"}"#.to_vec(),
        }],
    )
    .unwrap();

    let dataset_root = temp.path().join("market_replay");
    let report = build_market_replay_dataset(&BuildMarketReplayDatasetOptions {
        raw_roots: vec![raw_root.clone()],
        dataset_root: dataset_root.clone(),
        raw_start_ts_ns: Some(0),
        raw_end_ts_ns: Some(2_000_000_000),
        market_symbol_allowlist: vec!["BTC-5M".to_string()],
        overwrite: true,
        poly_server_visible_time: true,
        poly_incremental_latency_ms: 20,
        poly_incremental_freshness_guard_ms: 500,
        reference_latency_ms: 200,
        max_rows_per_part: Some(10),
    })
    .unwrap();

    assert_eq!(report.row_count, 8);
    assert_eq!(
        report.event_type_counts.get("book_snapshot_start").copied(),
        Some(2)
    );
    assert_eq!(
        report.event_type_counts.get("book_snapshot_level").copied(),
        Some(4)
    );
    assert_eq!(
        report.event_type_counts.get("depth_delta").copied(),
        Some(1)
    );
    assert_eq!(
        report.event_type_counts.get("reference_bar").copied(),
        Some(1)
    );

    let coverage = validate_market_replay_raw_coverage(&StreamMarketReplayEventsOptions {
        raw_roots: vec![raw_root.clone()],
        raw_start_ts_ns: Some(0),
        raw_end_ts_ns: Some(2_000_000_000),
        market_symbol_allowlist: vec!["BTC-5M".to_string()],
        poly_server_visible_time: true,
        poly_incremental_latency_ms: 20,
        poly_incremental_freshness_guard_ms: 500,
        reference_latency_ms: 200,
    })
    .unwrap();
    assert!(coverage.missing_stream_buckets.is_empty());

    let mut streamed = Vec::<MarketEvent>::new();
    let stream_report = stream_market_replay_events_from_raw(
        &StreamMarketReplayEventsOptions {
            raw_roots: vec![raw_root.clone()],
            raw_start_ts_ns: Some(0),
            raw_end_ts_ns: Some(2_000_000_000),
            market_symbol_allowlist: vec!["BTC-5M".to_string()],
            poly_server_visible_time: true,
            poly_incremental_latency_ms: 20,
            poly_incremental_freshness_guard_ms: 500,
            reference_latency_ms: 200,
        },
        |event| {
            streamed.push(event);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(stream_report.row_count, report.row_count);
    assert_eq!(stream_report.event_type_counts, report.event_type_counts);

    let duplicate_root_report = stream_market_replay_events_from_raw(
        &StreamMarketReplayEventsOptions {
            raw_roots: vec![raw_root.clone(), raw_root.clone()],
            raw_start_ts_ns: Some(0),
            raw_end_ts_ns: Some(2_000_000_000),
            market_symbol_allowlist: vec!["BTC-5M".to_string()],
            poly_server_visible_time: true,
            poly_incremental_latency_ms: 20,
            poly_incremental_freshness_guard_ms: 500,
            reference_latency_ms: 200,
        },
        |_event| Ok(()),
    )
    .unwrap();
    assert_eq!(duplicate_root_report.row_count, report.row_count);

    let events =
        read_parquet_table::<MarketEvent>(&dataset_root.join(MARKET_REPLAY_EVENTS_TABLE)).unwrap();
    assert_eq!(events.len(), 8);
    assert_eq!(
        streamed
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>()
    );

    let mut raw_updates = Vec::<MarketReplayRawUpdate>::new();
    let raw_report = stream_market_replay_raw_updates_from_raw(
        &StreamMarketReplayEventsOptions {
            raw_roots: vec![temp.path().join("raw")],
            raw_start_ts_ns: Some(0),
            raw_end_ts_ns: Some(2_000_000_000),
            market_symbol_allowlist: vec!["BTC-5M".to_string()],
            poly_server_visible_time: true,
            poly_incremental_latency_ms: 20,
            poly_incremental_freshness_guard_ms: 500,
            reference_latency_ms: 200,
        },
        |update| {
            raw_updates.push(update);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(raw_report.row_count, 3);
    assert_eq!(raw_report.event_type_counts.get("book").copied(), Some(2));
    assert_eq!(
        raw_report.event_type_counts.get("price_change").copied(),
        Some(1)
    );

    let typed_root = temp.path().join("typed_updates");
    let typed_build = build_market_replay_typed_updates(&BuildMarketReplayTypedUpdatesOptions {
        raw_roots: vec![raw_root.clone()],
        output_file: None,
        output_root: Some(typed_root.clone()),
        raw_start_ts_ns: Some(0),
        raw_end_ts_ns: Some(2_000_000_000),
        market_symbol_allowlist: vec!["BTC-5M".to_string()],
        overwrite: true,
        poly_server_visible_time: true,
        poly_incremental_latency_ms: 20,
        poly_incremental_freshness_guard_ms: 500,
        reference_latency_ms: 200,
    })
    .unwrap();
    assert_eq!(typed_build.row_count, 3);
    let typed_files = discover_market_replay_typed_update_files_for_window(
        &typed_root,
        Some(0),
        Some(2_000_000_000),
    )
    .unwrap();
    let mut serial_typed = Vec::new();
    stream_market_replay_typed_updates_from_files(typed_files, |update| {
        serial_typed.push(update);
        Ok(())
    })
    .unwrap();
    let mut parallel_typed = Vec::new();
    let parallel_typed_report = stream_market_replay_typed_updates_from_raw_parallel(
        &StreamMarketReplayEventsOptions {
            raw_roots: vec![raw_root.clone()],
            raw_start_ts_ns: Some(0),
            raw_end_ts_ns: Some(2_000_000_000),
            market_symbol_allowlist: vec!["BTC-5M".to_string()],
            poly_server_visible_time: true,
            poly_incremental_latency_ms: 20,
            poly_incremental_freshness_guard_ms: 500,
            reference_latency_ms: 200,
        },
        2,
        |update| {
            parallel_typed.push(update);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(parallel_typed_report.row_count, serial_typed.len());
    assert_eq!(parallel_typed, serial_typed);

    assert_eq!(events[0].event_type, "book_snapshot_start");
    assert_eq!(events[1].side.as_deref(), Some("BID"));
    assert_eq!(events[1].price_micros, Some(410_000));
    assert_eq!(events[1].qty_micros, Some(11_000_000));
    assert_eq!(events[2].side.as_deref(), Some("ASK"));
    assert_eq!(events[2].price_micros, Some(590_000));
    assert_eq!(events[3].event_type, "reference_bar");
    assert_eq!(events[3].symbol.as_deref(), Some("BTC"));
    assert_eq!(events[3].visible_ts_ns, 1_200_000_000);
    assert_eq!(events[3].price_micros, Some(100_500_000));
    assert_eq!(events[4].event_type, "depth_delta");
    assert_eq!(events[5].asset_id.as_deref(), Some("no-token"));

    let delta = events
        .iter()
        .find(|event| event.event_type == "depth_delta")
        .unwrap();
    assert_eq!(delta.asset_id.as_deref(), Some("yes-token"));
    assert_eq!(delta.side.as_deref(), Some("BID"));
    assert_eq!(delta.price_micros, Some(420_000));
    assert_eq!(delta.qty_micros, Some(13_000_000));
    assert_eq!(delta.local_recv_ts_ns, 1_250_000_000);
    assert_eq!(delta.visible_ts_ns, 1_220_000_000);
    assert_eq!(delta.source_row_idx, 2);

    let mut state = StreamingMarketReplayState::new();
    for event in &events {
        state.apply_event(event).unwrap();
    }
    assert_eq!(state.applied_events(), 8);
    assert_eq!(state.book_count(), 2);
    assert_eq!(state.reference_count(), 1);
    assert_eq!(
        state.reference_for("BTC").unwrap().close_price_micros,
        100_500_000
    );
    assert_eq!(state.condition_count(), 1);
    let condition = state.condition_for("cond-1").unwrap();
    assert_eq!(condition.symbol.as_deref(), Some("BTC-5M"));
    assert_eq!(condition.horizon_seconds, Some(300));
    assert_eq!(condition.asset_ids.len(), 2);
    assert_eq!(state.best_bid_price_micros("yes-token"), Some(420_000));
    assert_eq!(state.best_ask_price_micros("yes-token"), Some(590_000));
    assert_eq!(state.best_bid_price_micros("no-token"), Some(390_000));
    assert_eq!(state.best_ask_price_micros("no-token"), Some(610_000));

    state.apply_settlement(ReplaySettlementState {
        condition_id: "cond-1".to_string(),
        asset_id: "yes-token".to_string(),
        outcome: "YES".to_string(),
        winner: true,
        settled_ts_ns: Some(400_000_000_000),
    });
    state.apply_settlement(ReplaySettlementState {
        condition_id: "cond-1".to_string(),
        asset_id: "no-token".to_string(),
        outcome: "NO".to_string(),
        winner: false,
        settled_ts_ns: Some(400_000_000_000),
    });
    assert_eq!(state.settlement_count(), 2);
    assert!(state.settlement_for("cond-1", "YES").unwrap().winner);

    state
        .enqueue_buy_order(PendingReplayBuyOrder {
            order_id: "o1".to_string(),
            condition_id: "cond-1".to_string(),
            asset_id: "no-token".to_string(),
            arrival_ts_ns: 1_500_000_000,
            cash_micros: 10_000_000,
            limit_price_micros: 610_000,
        })
        .unwrap();
    state
        .enqueue_buy_order(PendingReplayBuyOrder {
            order_id: "o2".to_string(),
            condition_id: "cond-1".to_string(),
            asset_id: "no-token".to_string(),
            arrival_ts_ns: 1_500_000_000,
            cash_micros: 1_000_000,
            limit_price_micros: 610_000,
        })
        .unwrap();
    assert_eq!(state.pending_order_count(), 2);
    let executions = state.settle_due_buy_orders(1_500_000_000, 1_000);
    assert_eq!(executions.len(), 2);
    assert_eq!(executions[0].order_id, "o1");
    assert!(executions[0].filled);
    assert_eq!(executions[0].cash_micros, 3_660_000);
    assert_eq!(executions[0].qty_micros, 6_000_000);
    assert_eq!(executions[1].order_id, "o2");
    assert_eq!(
        executions[1].reject_reason.as_deref(),
        Some("no_ask_depth_at_limit")
    );

    let mut raw_state = StreamingMarketReplayState::new();
    for update in &raw_updates {
        raw_state.apply_raw_update(update).unwrap();
    }
    assert_eq!(raw_state.applied_events(), 3);
    assert_eq!(raw_state.book_count(), 2);
    assert_eq!(
        raw_state.best_bid_price_micros("yes-token"),
        state.best_bid_price_micros("yes-token")
    );
    assert_eq!(
        raw_state.best_ask_price_micros("yes-token"),
        state.best_ask_price_micros("yes-token")
    );
    assert_eq!(
        raw_state.best_bid_price_micros("no-token"),
        state.best_bid_price_micros("no-token")
    );
    assert_eq!(
        raw_state.best_ask_price_micros("no-token"),
        state.best_ask_price_micros("no-token")
    );

    let sweep = state.sweep_buy("yes-token", 1_000_000).unwrap();
    assert_eq!(sweep.requested_cash_micros, 1_000_000);
    assert_eq!(sweep.worst_price_micros, Some(590_000));
    assert_eq!(sweep.avg_price_micros, Some(589_999));
    assert!(sweep.filled_cash_micros >= 999_999);
    assert!(sweep.filled_shares_micros >= 1_694_900);

    let bench = bench_market_replay_dataset(&dataset_root).unwrap();
    assert_eq!(bench.row_count, 8);
    assert_eq!(bench.book_count, 2);
}

#[test]
fn compact_typed_stream_matches_raw_typed_stream() {
    let temp = tempfile::tempdir().unwrap();
    let raw_root = temp.path().join("raw");
    let segment_dir = raw_root
        .join("polymarket_clob_ws_raw")
        .join("hour_bucket=0");
    std::fs::create_dir_all(&segment_dir).unwrap();
    let segment_path = segment_dir.join("part.hfr4");
    write_hftrec4_segment(
        &segment_path,
        &[
            Hftrec4WriteRecord {
                ingest_seq: 1,
                local_recv_ts_ns: 1_000_000_000,
                event_type: "book".to_string(),
                symbol: Some("BTC-5M".to_string()),
                condition_id: Some("cond-compact".to_string()),
                asset_id: Some("yes-compact".to_string()),
                market_start_ts_ns: Some(0),
                market_end_ts_ns: Some(300_000_000_000),
                yes_asset_id: Some("yes-compact".to_string()),
                no_asset_id: Some("no-compact".to_string()),
                payload: br#"{"asset_id":"yes-compact","condition_id":"cond-compact","bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.60","size":"10"}],"timestamp":1000}"#.to_vec(),
            },
            Hftrec4WriteRecord {
                ingest_seq: 2,
                local_recv_ts_ns: 1_250_000_000,
                event_type: "price_change".to_string(),
                symbol: Some("BTC-5M".to_string()),
                condition_id: Some("cond-compact".to_string()),
                asset_id: None,
                market_start_ts_ns: Some(0),
                market_end_ts_ns: Some(300_000_000_000),
                yes_asset_id: Some("yes-compact".to_string()),
                no_asset_id: Some("no-compact".to_string()),
                payload: br#"{"market":"cond-compact","price_changes":[{"asset_id":"yes-compact","side":"BUY","price":"0.41","size":"11"}],"timestamp":1200}"#.to_vec(),
            },
        ],
    )
    .unwrap();

    let compact_root = temp.path().join("compact_typed");
    let build_report = build_market_replay_compact_typed(&BuildMarketReplayCompactTypedOptions {
        raw_roots: vec![raw_root.clone()],
        output_root: compact_root.clone(),
        raw_start_ts_ns: Some(0),
        raw_end_ts_ns: Some(2_000_000_000),
        overwrite: true,
    })
    .unwrap();
    assert_eq!(build_report.row_count, 2);
    assert_eq!(build_report.segment_count, 1);

    let stream_options = StreamMarketReplayEventsOptions {
        raw_roots: vec![raw_root.clone()],
        raw_start_ts_ns: Some(0),
        raw_end_ts_ns: Some(2_000_000_000),
        market_symbol_allowlist: vec!["BTC-5M".to_string()],
        poly_server_visible_time: true,
        poly_incremental_latency_ms: 20,
        poly_incremental_freshness_guard_ms: 500,
        reference_latency_ms: 200,
    };
    let mut raw_typed = Vec::new();
    stream_market_replay_typed_updates_from_raw_parallel(&stream_options, 2, |update| {
        raw_typed.push(update);
        Ok(())
    })
    .unwrap();
    let compact_files = discover_market_replay_compact_typed_files_for_window(
        &compact_root,
        Some(0),
        Some(2_000_000_000),
    )
    .unwrap();
    let mut compact_typed = Vec::new();
    stream_market_replay_typed_updates_from_compact_files(
        compact_files,
        &stream_options,
        |update| {
            compact_typed.push(update);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(compact_typed, raw_typed);
    assert_eq!(compact_typed[1].visible_ts_ns, 1_220_000_000);

    let validation =
        validate_market_replay_compact_typed(&ValidateMarketReplayCompactTypedOptions {
            raw_roots: vec![raw_root],
            compact_typed_roots: vec![compact_root],
            raw_start_ts_ns: Some(0),
            raw_end_ts_ns: Some(2_000_000_000),
            market_symbol_allowlist: vec!["BTC-5M".to_string()],
            poly_server_visible_time: true,
            poly_incremental_latency_ms: 20,
            poly_incremental_freshness_guard_ms: 500,
            reference_latency_ms: 200,
            raw_worker_count: 2,
        })
        .unwrap();
    assert!(validation.matched);
    assert_eq!(validation.raw_row_count, 2);
    assert_eq!(validation.compact_row_count, 2);
}

#[test]
fn typed_update_shards_merge_in_original_replay_order() {
    let temp = tempfile::tempdir().unwrap();
    let raw_root = temp.path().join("raw");
    let segment_dir = raw_root
        .join("polymarket_clob_ws_raw")
        .join("hour_bucket=0");
    std::fs::create_dir_all(&segment_dir).unwrap();
    let segment_path = segment_dir.join("part.hfr4");

    write_hftrec4_segment(
        &segment_path,
        &[
            Hftrec4WriteRecord {
                ingest_seq: 1,
                local_recv_ts_ns: 1_000_000_000,
                event_type: "book".to_string(),
                symbol: Some("BTC-5M".to_string()),
                condition_id: Some("cond-btc".to_string()),
                asset_id: Some("btc-yes".to_string()),
                market_start_ts_ns: Some(0),
                market_end_ts_ns: Some(300_000_000_000),
                yes_asset_id: Some("btc-yes".to_string()),
                no_asset_id: Some("btc-no".to_string()),
                payload: book_payload("btc-yes", "cond-btc", "0.41", "0.59", "10"),
            },
            Hftrec4WriteRecord {
                ingest_seq: 2,
                local_recv_ts_ns: 1_100_000_000,
                event_type: "book".to_string(),
                symbol: Some("ETH-5M".to_string()),
                condition_id: Some("cond-eth".to_string()),
                asset_id: Some("eth-yes".to_string()),
                market_start_ts_ns: Some(0),
                market_end_ts_ns: Some(300_000_000_000),
                yes_asset_id: Some("eth-yes".to_string()),
                no_asset_id: Some("eth-no".to_string()),
                payload: book_payload("eth-yes", "cond-eth", "0.42", "0.58", "10"),
            },
            Hftrec4WriteRecord {
                ingest_seq: 3,
                local_recv_ts_ns: 1_200_000_000,
                event_type: "price_change".to_string(),
                symbol: Some("BTC-5M".to_string()),
                condition_id: Some("cond-btc".to_string()),
                asset_id: None,
                market_start_ts_ns: Some(0),
                market_end_ts_ns: Some(300_000_000_000),
                yes_asset_id: Some("btc-yes".to_string()),
                no_asset_id: Some("btc-no".to_string()),
                payload: br#"{"market":"cond-btc","price_changes":[{"asset_id":"btc-yes","side":"BUY","price":"0.43","size":"12"}]}"#.to_vec(),
            },
        ],
    )
    .unwrap();

    let stream_options = StreamMarketReplayEventsOptions {
        raw_roots: vec![raw_root.clone()],
        raw_start_ts_ns: Some(0),
        raw_end_ts_ns: Some(2_000_000_000),
        market_symbol_allowlist: vec!["BTC-5M".to_string(), "ETH-5M".to_string()],
        poly_server_visible_time: false,
        poly_incremental_latency_ms: 20,
        poly_incremental_freshness_guard_ms: 500,
        reference_latency_ms: 200,
    };
    let mut raw_symbols = Vec::new();
    stream_market_replay_raw_updates_from_raw(&stream_options, |update| {
        raw_symbols.push((
            update.global_event_seq,
            update.symbol.clone(),
            update.event_type.clone(),
        ));
        Ok(())
    })
    .unwrap();

    let output_root = temp.path().join("typed_shards");
    let report = build_market_replay_typed_updates(&BuildMarketReplayTypedUpdatesOptions {
        raw_roots: vec![raw_root],
        output_file: None,
        output_root: Some(output_root.clone()),
        raw_start_ts_ns: Some(0),
        raw_end_ts_ns: Some(2_000_000_000),
        market_symbol_allowlist: vec!["BTC-5M".to_string(), "ETH-5M".to_string()],
        overwrite: true,
        poly_server_visible_time: false,
        poly_incremental_latency_ms: 20,
        poly_incremental_freshness_guard_ms: 500,
        reference_latency_ms: 200,
    })
    .unwrap();
    assert_eq!(report.row_count, 3);
    assert_eq!(report.shard_files.len(), 2);

    let files = discover_market_replay_typed_update_files_for_window(
        &output_root,
        Some(0),
        Some(2_000_000_000),
    )
    .unwrap();
    assert_eq!(files.len(), 2);

    let mut typed_symbols = Vec::new();
    stream_market_replay_typed_updates_from_files(files, |update| {
        typed_symbols.push((
            update.global_event_seq,
            update.symbol.clone(),
            update.event_type.clone(),
        ));
        Ok(())
    })
    .unwrap();
    assert_eq!(typed_symbols, raw_symbols);
}

#[test]
fn raw_price_change_inherits_book_snapshot_market_metadata() {
    let mut state = StreamingMarketReplayState::new();
    let book_update = replay_raw_update_for_test(
        "book",
        Some("BTC-5M"),
        Some("cond-a"),
        Some("yes-a"),
        Some(1_000_000_000),
        Some(301_000_000_000),
        1_000_000_000,
        br#"{"asset_id":"yes-a","condition_id":"cond-a","bids":[{"price":"0.41","size":"11"}],"asks":[{"price":"0.59","size":"7"}]}"#,
    );
    state.apply_raw_update(&book_update).unwrap();

    let price_change = replay_raw_update_for_test(
        "price_change",
        None,
        None,
        None,
        None,
        None,
        1_100_000_000,
        br#"{"price_changes":[{"asset_id":"yes-a","side":"SELL","price":"0.57","size":"9"}]}"#,
    );
    let payload: serde_json::Value = serde_json::from_slice(&price_change.raw.raw_payload).unwrap();
    state
        .apply_raw_update_with_payload(&price_change, &payload)
        .unwrap();

    let book = state.book_for("yes-a").unwrap();
    assert_eq!(book.symbol.as_deref(), Some("BTC-5M"));
    assert_eq!(book.horizon_seconds, Some(300));
    assert_eq!(book.market_start_ts_ns, Some(1_000_000_000));
    assert_eq!(book.market_end_ts_ns, Some(301_000_000_000));
    assert_eq!(state.best_ask_price_micros("yes-a"), Some(570_000));
}

#[test]
fn market_replay_coverage_fails_closed_on_missing_hour_bucket() {
    let temp = tempfile::tempdir().unwrap();
    let stream_root = temp.path().join("raw").join("polymarket_clob_ws_raw");
    let bucket0 = stream_root.join("hour_bucket=0");
    std::fs::create_dir_all(&bucket0).unwrap();
    std::fs::write(bucket0.join("part.manifest.json"), "{}").unwrap();

    let err = validate_market_replay_raw_coverage(&StreamMarketReplayEventsOptions {
        raw_roots: vec![temp.path().join("raw")],
        raw_start_ts_ns: Some(0),
        raw_end_ts_ns: Some(3_600_000_000_001),
        market_symbol_allowlist: vec!["BTC-5M".to_string()],
        poly_server_visible_time: false,
        poly_incremental_latency_ms: 20,
        poly_incremental_freshness_guard_ms: 500,
        reference_latency_ms: 200,
    })
    .unwrap_err();
    assert!(err.to_string().contains("missing stream buckets"));
}

#[test]
fn streaming_state_matches_replay_state_index_on_sampled_book_states() {
    let temp = tempfile::tempdir().unwrap();
    let raw_root = temp.path().join("raw");
    let segment_dir = raw_root
        .join("polymarket_clob_ws_raw")
        .join("hour_bucket=0");
    std::fs::create_dir_all(&segment_dir).unwrap();
    let segment_path = segment_dir.join("part.hfr4");
    let reference_segment_dir = raw_root.join("reference_ws_raw").join("hour_bucket=0");
    std::fs::create_dir_all(&reference_segment_dir).unwrap();
    let reference_segment_path = reference_segment_dir.join("part.hfr4");
    write_hftrec4_segment(
        &segment_path,
        &[
            Hftrec4WriteRecord {
                ingest_seq: 1,
                local_recv_ts_ns: 2_000_000_000,
                event_type: "book".to_string(),
                symbol: Some("SOL-5M".to_string()),
                condition_id: Some("cond-a".to_string()),
                asset_id: Some("yes-a".to_string()),
                market_start_ts_ns: Some(1_000_000_000),
                market_end_ts_ns: Some(10_000_000_000),
                yes_asset_id: Some("yes-a".to_string()),
                no_asset_id: Some("no-a".to_string()),
                payload: book_payload("yes-a", "cond-a", "0.69", "0.70", "10"),
            },
            Hftrec4WriteRecord {
                ingest_seq: 2,
                local_recv_ts_ns: 3_000_000_000,
                event_type: "book".to_string(),
                symbol: Some("SOL-5M".to_string()),
                condition_id: Some("cond-a".to_string()),
                asset_id: Some("yes-a".to_string()),
                market_start_ts_ns: Some(1_000_000_000),
                market_end_ts_ns: Some(10_000_000_000),
                yes_asset_id: Some("yes-a".to_string()),
                no_asset_id: Some("no-a".to_string()),
                payload: book_payload("yes-a", "cond-a", "0.49", "0.50", "10"),
            },
        ],
    )
    .unwrap();
    write_hftrec4_segment(
        &reference_segment_path,
        &[
            Hftrec4WriteRecord {
                ingest_seq: 10,
                local_recv_ts_ns: 2_010_000_000,
                event_type: "reference_bar".to_string(),
                symbol: Some("binance:SOL".to_string()),
                condition_id: None,
                asset_id: None,
                market_start_ts_ns: Some(1_000_000_000),
                market_end_ts_ns: Some(2_000_000_000),
                yes_asset_id: None,
                no_asset_id: None,
                payload: reference_payload(10, "SOL", 1_000, "50.0"),
            },
            Hftrec4WriteRecord {
                ingest_seq: 11,
                local_recv_ts_ns: 3_010_000_000,
                event_type: "reference_bar".to_string(),
                symbol: Some("binance:SOL".to_string()),
                condition_id: None,
                asset_id: None,
                market_start_ts_ns: Some(2_000_000_000),
                market_end_ts_ns: Some(3_000_000_000),
                yes_asset_id: None,
                no_asset_id: None,
                payload: reference_payload(11, "SOL", 2_000, "51.0"),
            },
        ],
    )
    .unwrap();

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
        vec![
            idx_row("SOL-5M", "cond-a", "yes-a", "no-a", 2_000_000_000, 700_000),
            idx_row("SOL-5M", "cond-a", "yes-a", "no-a", 3_000_000_000, 500_000),
        ],
    )
    .unwrap();
    let index_root = temp.path().join("hftidx1");
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

    let mut updates = Vec::new();
    stream_market_replay_raw_updates_from_raw(
        &StreamMarketReplayEventsOptions {
            raw_roots: vec![raw_root.clone()],
            raw_start_ts_ns: Some(0),
            raw_end_ts_ns: Some(4_000_000_000),
            market_symbol_allowlist: vec!["SOL-5M".to_string()],
            poly_server_visible_time: false,
            poly_incremental_latency_ms: 20,
            poly_incremental_freshness_guard_ms: 500,
            reference_latency_ms: 200,
        },
        |update| {
            updates.push(update);
            Ok(())
        },
    )
    .unwrap();

    for (sample_ts, expected_ask) in [(2_500_000_000, 700_000), (3_000_000_000, 500_000)] {
        let indexed = reader
            .state_at(yes_asset_key, sample_ts)
            .unwrap()
            .expect("indexed state");
        let mut streaming = StreamingMarketReplayState::new();
        for update in updates
            .iter()
            .filter(|update| update.visible_ts_ns <= sample_ts)
        {
            streaming.apply_raw_update(update).unwrap();
        }
        assert_eq!(
            streaming.best_bid_price_micros("yes-a"),
            indexed.best_bid_price_micros
        );
        assert_eq!(
            streaming.best_ask_price_micros("yes-a"),
            indexed.best_ask_price_micros
        );
        assert_eq!(streaming.best_ask_price_micros("yes-a"), Some(expected_ask));
        let streaming_sweep = streaming.sweep_buy_at_or_below("yes-a", 1_000_000, 990_000);
        let indexed_sweep = sweep_index_asks(indexed.ask_levels, 1_000_000, 990_000);
        assert_eq!(streaming_sweep, indexed_sweep);

        let streaming_sweep_5u = streaming.sweep_buy_at_or_below("yes-a", 5_000_000, 990_000);
        let indexed_sweep_5u = sweep_index_asks(indexed.ask_levels, 5_000_000, 990_000);
        assert_eq!(streaming_sweep_5u, indexed_sweep_5u);
    }

    let mut events = Vec::<MarketEvent>::new();
    stream_market_replay_events_from_raw(
        &StreamMarketReplayEventsOptions {
            raw_roots: vec![raw_root],
            raw_start_ts_ns: Some(0),
            raw_end_ts_ns: Some(4_000_000_000),
            market_symbol_allowlist: vec!["SOL-5M".to_string()],
            poly_server_visible_time: false,
            poly_incremental_latency_ms: 20,
            poly_incremental_freshness_guard_ms: 500,
            reference_latency_ms: 200,
        },
        |event| {
            events.push(event);
            Ok(())
        },
    )
    .unwrap();
    let reference_visible_ts = events
        .iter()
        .filter(|event| event.event_type == "reference_bar")
        .map(|event| event.visible_ts_ns)
        .collect::<Vec<_>>();
    assert_eq!(reference_visible_ts, vec![2_200_000_000, 3_200_000_000]);

    let mut state_at_2_5s = StreamingMarketReplayState::new();
    for event in events
        .iter()
        .filter(|event| event.visible_ts_ns <= 2_500_000_000)
    {
        state_at_2_5s.apply_event(event).unwrap();
    }
    assert_eq!(
        state_at_2_5s
            .reference_for("SOL")
            .map(|reference| reference.close_price_micros),
        Some(50_000_000)
    );

    let mut state_at_3_7s = StreamingMarketReplayState::new();
    for event in events
        .iter()
        .filter(|event| event.visible_ts_ns <= 3_700_000_000)
    {
        state_at_3_7s.apply_event(event).unwrap();
    }
    assert_eq!(
        state_at_3_7s
            .reference_for("SOL")
            .map(|reference| reference.close_price_micros),
        Some(51_000_000)
    );
}

fn book_payload(asset_id: &str, condition_id: &str, bid: &str, ask: &str, size: &str) -> Vec<u8> {
    format!(
        r#"{{"asset_id":"{asset_id}","condition_id":"{condition_id}","bids":[{{"price":"{bid}","size":"{size}"}}],"asks":[{{"price":"{ask}","size":"{size}"}}]}}"#
    )
    .into_bytes()
}

fn reference_payload(ingest_seq: u64, symbol: &str, bar_open_time_ms: i64, close: &str) -> Vec<u8> {
    format!(
        r#"{{"source_id":"binance_1s_ws","venue":"binance","symbol":"{symbol}","ingest_seq_scope":"pm5m_reference_ws","ingest_seq":{ingest_seq},"local_recv_ts_ns":0,"connection_epoch":1,"event_type":"reference_bar","exchange_event_ts_ms":{bar_open_time_ms},"bar_open_time_ms":{bar_open_time_ms},"bar_close_time_ms":{},"is_closed":true,"open":"{close}","high":"{close}","low":"{close}","close":"{close}","volume":"1","raw_payload":[],"raw_payload_sha256":"payload-{ingest_seq}","raw_record_hash":"record-{ingest_seq}"}}"#,
        bar_open_time_ms + 999
    )
    .into_bytes()
}

fn replay_raw_update_for_test(
    event_type: &str,
    symbol: Option<&str>,
    condition_id: Option<&str>,
    asset_id: Option<&str>,
    market_start_ts_ns: Option<i64>,
    market_end_ts_ns: Option<i64>,
    ts_ns: i64,
    payload: &[u8],
) -> MarketReplayRawUpdate {
    MarketReplayRawUpdate {
        schema_version: 1,
        dataset_format: pm5m_market_cache::MARKET_REPLAY_FORMAT.to_string(),
        global_event_seq: 0,
        source: "test".to_string(),
        stream: "polymarket_clob_ws_raw".to_string(),
        symbol: symbol.map(str::to_string),
        horizon_seconds: market_start_ts_ns
            .zip(market_end_ts_ns)
            .map(|(start, end)| (end - start) / 1_000_000_000),
        condition_id: condition_id.map(str::to_string),
        asset_id: asset_id.map(str::to_string),
        event_type: event_type.to_string(),
        original_local_recv_ts_ns: ts_ns,
        visible_ts_ns: ts_ns,
        ingest_seq: ts_ns as u64,
        source_row_idx: 0,
        source_segment: "test".to_string(),
        raw_record_hash: "raw".to_string(),
        payload_hash: "payload".to_string(),
        raw: pm5m_market_cache::RawPolymarketClobWsEvent {
            source_id: "test".to_string(),
            ingest_seq_scope: "test".to_string(),
            ingest_seq: ts_ns as u64,
            local_recv_ts_ns: ts_ns,
            asset_id: asset_id.map(str::to_string),
            condition_id: condition_id.map(str::to_string),
            symbol: symbol.map(str::to_string),
            outcome: Some("YES".to_string()),
            market_start_ts_ns,
            market_end_ts_ns,
            yes_asset_id: Some("yes-a".to_string()),
            no_asset_id: Some("no-a".to_string()),
            event_type: event_type.to_string(),
            exchange_ts_ms: None,
            raw_payload_sha256: "payload".to_string(),
            raw_payload: payload.to_vec(),
        },
    }
}

fn idx_row(
    symbol: &str,
    condition_id: &str,
    yes_asset_id: &str,
    no_asset_id: &str,
    ts_ns: i64,
    ask_price_micros: i64,
) -> BookCacheRow {
    BookCacheRow {
        symbol: symbol.to_string(),
        condition_id: condition_id.to_string(),
        asset_id: yes_asset_id.to_string(),
        outcome: "YES".to_string(),
        window_start_ts_ns: 1_000_000_000,
        window_end_ts_ns: 10_000_000_000,
        yes_asset_id: yes_asset_id.to_string(),
        no_asset_id: no_asset_id.to_string(),
        local_recv_ts_ns: ts_ns,
        exchange_ts_ms: Some(ts_ns / 1_000_000),
        ingest_seq: ts_ns as u64,
        best_bid_price_micros: Some(ask_price_micros - 10_000),
        best_ask_price_micros: Some(ask_price_micros),
        bid_levels: [Some(BookLevelMicros {
            price_micros: ask_price_micros - 10_000,
            qty_micros: 10_000_000,
        }); 10],
        ask_levels: [Some(BookLevelMicros {
            price_micros: ask_price_micros,
            qty_micros: 10_000_000,
        }); 10],
        raw_row_hash: format!("hash-{condition_id}-{ts_ns}"),
        raw_payload_sha256: format!("payload-hash-{condition_id}-{ts_ns}"),
        book_state_hash: format!("book-state-hash-{condition_id}-{ts_ns}"),
    }
}

fn sweep_index_asks(
    ask_levels: [Option<BookLevelMicros>; 10],
    cash_micros: i64,
    limit_price_micros: i64,
) -> Option<pm5m_market_cache::BuySweepResult> {
    let mut state = StreamingMarketReplayState::new();
    state
        .apply_raw_update(&MarketReplayRawUpdate {
            schema_version: 1,
            dataset_format: pm5m_market_cache::MARKET_REPLAY_FORMAT.to_string(),
            global_event_seq: 0,
            source: "test".to_string(),
            stream: "polymarket_clob_ws_raw".to_string(),
            symbol: Some("SOL-5M".to_string()),
            horizon_seconds: Some(300),
            condition_id: Some("cond-a".to_string()),
            asset_id: Some("idx-asset".to_string()),
            event_type: "book".to_string(),
            original_local_recv_ts_ns: 1,
            visible_ts_ns: 1,
            ingest_seq: 1,
            source_row_idx: 0,
            source_segment: "test".to_string(),
            raw_record_hash: "raw".to_string(),
            payload_hash: "payload".to_string(),
            raw: pm5m_market_cache::RawPolymarketClobWsEvent {
                source_id: "test".to_string(),
                ingest_seq_scope: "test".to_string(),
                ingest_seq: 1,
                local_recv_ts_ns: 1,
                asset_id: Some("idx-asset".to_string()),
                condition_id: Some("cond-a".to_string()),
                symbol: Some("SOL-5M".to_string()),
                outcome: Some("YES".to_string()),
                market_start_ts_ns: Some(0),
                market_end_ts_ns: Some(300_000_000_000),
                yes_asset_id: Some("idx-asset".to_string()),
                no_asset_id: Some("no".to_string()),
                event_type: "book".to_string(),
                exchange_ts_ms: None,
                raw_payload: index_levels_payload(&ask_levels),
                raw_payload_sha256: "payload".to_string(),
            },
        })
        .unwrap();
    state.sweep_buy_at_or_below("idx-asset", cash_micros, limit_price_micros)
}

fn index_levels_payload(ask_levels: &[Option<BookLevelMicros>; 10]) -> Vec<u8> {
    let asks = ask_levels
        .iter()
        .flatten()
        .map(|level| {
            format!(
                r#"{{"price":"{}","size":"{}"}}"#,
                level.price_micros as f64 / 1_000_000.0,
                level.qty_micros as f64 / 1_000_000.0
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(r#"{{"asset_id":"idx-asset","condition_id":"cond-a","bids":[],"asks":[{asks}]}}"#)
        .into_bytes()
}
