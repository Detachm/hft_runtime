use crate::constants::{TABLE_BINANCE_REFERENCE, TABLE_BOOK_TOP10, TABLE_EVENT_INDEX};
use crate::manifest::{dataset_path, read_or_base_dataset_manifest, write_dataset_manifest};
use crate::types::*;
use anyhow::{bail, Result};
use market_data_etl_core::{
    hash_path, parquet_file_row_iter, parquet_part_files, read_parquet_table,
    ParquetFileRowIterator, ParquetTableStreamWriter,
};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

const DEFAULT_EVENT_PART_ROWS: usize = 200_000;

pub fn build_event_index(plan: &PipelinePlan) -> Result<usize> {
    let binance_rows = read_parquet_table::<BinanceKline1sReferenceRow>(&dataset_path(
        plan,
        TABLE_BINANCE_REFERENCE,
    ))?;

    let mut reference_events = binance_rows
        .into_iter()
        .map(|row| EventSortRecord {
            event_type: "binance_reference_1s".to_string(),
            event_ts_ns: row.synthetic_local_recv_ts_ns,
            source_rank: 1,
            ingest_seq: row.ingest_seq,
            symbol: row.symbol,
            condition_id: None,
            asset_id: None,
            outcome: None,
            payload_table: TABLE_BINANCE_REFERENCE.to_string(),
            payload_primary_key: row.primary_key,
            payload_row_hash: row.row_hash,
        })
        .collect::<Vec<_>>();
    reference_events.sort();

    let event_path = dataset_path(plan, TABLE_EVENT_INDEX);
    let mut writer = ParquetTableStreamWriter::new(&event_path, Some("event_ts_ns"))?;
    let part_limit = event_part_row_limit();
    let mut chunk = Vec::<StandardEventIndexRow>::with_capacity(part_limit.min(16_384));
    let mut next_seq = 0u64;
    let mut reference_idx = 0usize;
    let mut book_merge = BookPartMerge::new(&dataset_path(plan, TABLE_BOOK_TOP10))?;
    while let Some(book_event) = book_merge.next_event()? {
        while reference_idx < reference_events.len()
            && reference_events[reference_idx].event_ts_ns < book_event.event_ts_ns
        {
            push_event(
                &mut writer,
                &mut chunk,
                &mut next_seq,
                reference_events[reference_idx].clone(),
                part_limit,
            )?;
            reference_idx += 1;
        }
        push_event(
            &mut writer,
            &mut chunk,
            &mut next_seq,
            book_event,
            part_limit,
        )?;
    }

    while reference_idx < reference_events.len() {
        push_event(
            &mut writer,
            &mut chunk,
            &mut next_seq,
            reference_events[reference_idx].clone(),
            part_limit,
        )?;
        reference_idx += 1;
    }
    if !chunk.is_empty() {
        writer.write_rows(&chunk)?;
    }
    let row_count = next_seq as usize;
    writer.finish()?;
    let mut manifest = read_or_base_dataset_manifest(plan)?;
    manifest.event_index_hash = Some(hash_path(&dataset_path(plan, TABLE_EVENT_INDEX))?);
    manifest.contains_settlement_in_event_stream = false;
    write_dataset_manifest(plan, &manifest)?;

    Ok(row_count)
}

fn push_event(
    writer: &mut ParquetTableStreamWriter,
    chunk: &mut Vec<StandardEventIndexRow>,
    next_seq: &mut u64,
    event: EventSortRecord,
    part_limit: usize,
) -> Result<()> {
    chunk.push(StandardEventIndexRow {
        schema_version: 1,
        dataset_format: EVENT_INDEX_FORMAT.to_string(),
        global_event_seq: *next_seq,
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
    });
    *next_seq += 1;
    if chunk.len() >= part_limit {
        writer.write_rows(chunk)?;
        chunk.clear();
    }
    Ok(())
}

fn event_part_row_limit() -> usize {
    std::env::var("PM5M_EVENT_PART_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_EVENT_PART_ROWS)
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

struct BookPartMerge {
    cursors: Vec<ParquetFileRowIterator<PolymarketBookTop10Row>>,
    heap: BinaryHeap<Reverse<BookHeapItem>>,
}

impl BookPartMerge {
    fn new(table_path: &std::path::Path) -> Result<Self> {
        let files = parquet_part_files(table_path)?;
        if files.is_empty() {
            bail!("book table has no parquet part files");
        }
        let mut cursors = Vec::new();
        let mut heap = BinaryHeap::new();
        for file in files {
            let mut cursor = parquet_file_row_iter::<PolymarketBookTop10Row>(&file)?;
            let part_idx = cursors.len();
            if let Some(row) = cursor.next_row()? {
                heap.push(Reverse(BookHeapItem {
                    event: book_event_from_row(row),
                    part_idx,
                }));
            }
            cursors.push(cursor);
        }
        Ok(Self { cursors, heap })
    }

    fn next_event(&mut self) -> Result<Option<EventSortRecord>> {
        let Some(Reverse(item)) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some(row) = self.cursors[item.part_idx].next_row()? {
            self.heap.push(Reverse(BookHeapItem {
                event: book_event_from_row(row),
                part_idx: item.part_idx,
            }));
        }
        Ok(Some(item.event))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct BookHeapItem {
    event: EventSortRecord,
    part_idx: usize,
}

impl Ord for BookHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.event, self.part_idx).cmp(&(&other.event, other.part_idx))
    }
}

impl PartialOrd for BookHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn book_event_from_row(row: PolymarketBookTop10Row) -> EventSortRecord {
    EventSortRecord {
        event_type: "pm_book_top10".to_string(),
        event_ts_ns: row.local_recv_ts_ns,
        source_rank: 0,
        ingest_seq: row.ingest_seq,
        symbol: row.symbol,
        condition_id: Some(row.condition_id),
        asset_id: Some(row.asset_id),
        outcome: Some(row.outcome),
        payload_table: TABLE_BOOK_TOP10.to_string(),
        payload_primary_key: row.primary_key,
        payload_row_hash: row.row_hash,
    }
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
