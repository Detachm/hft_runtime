use market_data_etl_core::{for_each_parquet_table_row, ParquetTableStreamWriter};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Row {
    id: i64,
    ts_ns: i64,
    value: String,
}

#[test]
fn stream_writer_writes_multiple_parts_and_stream_reader_visits_rows() {
    let temp = TempDir::new().unwrap();
    let table = temp.path().join("table");
    let mut writer = ParquetTableStreamWriter::new(&table, Some("ts_ns")).unwrap();
    writer
        .write_rows(&[
            Row {
                id: 1,
                ts_ns: 100,
                value: "a".to_string(),
            },
            Row {
                id: 2,
                ts_ns: 200,
                value: "b".to_string(),
            },
        ])
        .unwrap();
    writer
        .write_rows(&[Row {
            id: 3,
            ts_ns: 300,
            value: "c".to_string(),
        }])
        .unwrap();
    let report = writer.finish().unwrap();
    assert_eq!(report.row_count, 3);
    assert!(table.join("part-00000.parquet").exists());
    assert!(table.join("part-00001.parquet").exists());

    let mut rows = Vec::new();
    for_each_parquet_table_row::<Row, _>(&table, |row| {
        rows.push(row);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        rows,
        vec![
            Row {
                id: 1,
                ts_ns: 100,
                value: "a".to_string(),
            },
            Row {
                id: 2,
                ts_ns: 200,
                value: "b".to_string(),
            },
            Row {
                id: 3,
                ts_ns: 300,
                value: "c".to_string(),
            },
        ]
    );
}
