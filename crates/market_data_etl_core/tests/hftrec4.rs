use market_data_etl_core::{
    hftrec4_manifest_path_for_segment, read_hftrec4_records, scan_hftrec4_segment,
    verify_hftrec4_manifest, write_hftrec4_segment, Hftrec4WriteRecord, HFTREC4_FORMAT,
};

#[test]
fn hftrec4_roundtrip_and_manifest_verify() {
    let temp = tempfile::tempdir().unwrap();
    let segment = temp.path().join("part-00000.hfr4");
    let rows = vec![
        Hftrec4WriteRecord {
            ingest_seq: 7,
            local_recv_ts_ns: 1_000,
            event_type: "book".to_string(),
            symbol: Some("ETH-5M".to_string()),
            condition_id: Some("cond-1".to_string()),
            asset_id: Some("asset-yes".to_string()),
            market_start_ts_ns: Some(900),
            market_end_ts_ns: Some(1_200),
            yes_asset_id: Some("asset-yes".to_string()),
            no_asset_id: Some("asset-no".to_string()),
            payload: br#"{"event_type":"book"}"#.to_vec(),
        },
        Hftrec4WriteRecord {
            ingest_seq: 8,
            local_recv_ts_ns: 1_001,
            event_type: "price_change".to_string(),
            symbol: Some("ETH-5M".to_string()),
            condition_id: Some("cond-1".to_string()),
            asset_id: Some("asset-yes".to_string()),
            market_start_ts_ns: Some(900),
            market_end_ts_ns: Some(1_200),
            yes_asset_id: Some("asset-yes".to_string()),
            no_asset_id: Some("asset-no".to_string()),
            payload: br#"{"event_type":"price_change"}"#.to_vec(),
        },
    ];

    let manifest = write_hftrec4_segment(&segment, &rows).unwrap();
    assert_eq!(manifest.dataset_format, HFTREC4_FORMAT);
    assert_eq!(manifest.record_count, 2);
    assert_eq!(manifest.min_ingest_seq, Some(7));
    assert_eq!(manifest.max_ts_ns, Some(1_001));

    let manifest_path = hftrec4_manifest_path_for_segment(&segment);
    let verified = verify_hftrec4_manifest(&manifest_path).unwrap();
    assert_eq!(verified.record_count, 2);

    let decoded = read_hftrec4_records(&segment).unwrap();
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].event_type, "book");
    assert_eq!(decoded[1].payload, rows[1].payload);
}

#[test]
fn hftrec4_stream_scan_is_ordered() {
    let temp = tempfile::tempdir().unwrap();
    let segment = temp.path().join("part-00000.hfr4");
    let rows = (0..5)
        .map(|idx| Hftrec4WriteRecord {
            ingest_seq: idx,
            local_recv_ts_ns: 10_000 + idx as i64,
            event_type: "book".to_string(),
            symbol: None,
            condition_id: None,
            asset_id: None,
            market_start_ts_ns: None,
            market_end_ts_ns: None,
            yes_asset_id: None,
            no_asset_id: None,
            payload: format!("payload-{idx}").into_bytes(),
        })
        .collect::<Vec<_>>();
    write_hftrec4_segment(&segment, &rows).unwrap();

    let mut seen = Vec::new();
    scan_hftrec4_segment(&segment, |record| {
        seen.push((record.ingest_seq, record.local_recv_ts_ns, record.payload));
        Ok(())
    })
    .unwrap();
    assert_eq!(seen.len(), 5);
    assert_eq!(seen[0].0, 0);
    assert_eq!(seen[4].2, b"payload-4".to_vec());
}
