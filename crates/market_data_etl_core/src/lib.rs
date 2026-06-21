pub mod atomic;
pub mod fs_util;
pub mod hash;
pub mod hftrec4;
pub mod schema;
pub mod table;
pub mod time;
pub mod types;

pub use atomic::{atomic_write_verified, fsync_dir, reject_tmp_path};
pub use fs_util::{copy_dir_recursive, list_files_recursive};
pub use hash::{hash_path, hash_serializable, raw_record_hash, sha256_bytes, sha256_file};
pub use hftrec4::{
    discover_hftrec4_manifests, hftrec4_manifest_path_for_segment, read_hftrec4_records,
    read_hftrec4_segment_header_summary, scan_hftrec4_segment, scan_hftrec4_segment_selected,
    verify_hftrec4_manifest, write_hftrec4_segment, Hftrec4Record, Hftrec4RecordMeta,
    Hftrec4SegmentHeaderSummary, Hftrec4SegmentManifest, Hftrec4WriteRecord, HFTREC4_FORMAT,
    HFTREC4_MAGIC, HFTREC4_SCHEMA_HASH,
};
pub use schema::{scan_json_fields, SchemaFieldViolation};
pub use table::{
    for_each_parquet_table_row, parquet_file_row_iter, parquet_part_files, read_jsonl_file,
    read_jsonl_table, read_jsonl_values, read_parquet_table, verify_parquet_zstd_table,
    write_json_file_pretty, write_jsonl_table, write_parquet_table, ParquetFileRowIterator,
    ParquetPartManifest, ParquetTableStreamWriter, TableWriteReport,
};
pub use time::now_unix_ns;
pub use types::{BookLevel, RawPolymarketBookTop10, RAW_POLYMARKET_BOOK_SOURCE};

/// Legacy/debug JSONL helpers kept for explicit fixtures and small debug use.
///
/// Production raw audit output should use HFTREC4 and production tables should
/// use Parquet/ZSTD or PM5M compact cache formats. The top-level JSONL exports
/// remain only for small tests and explicit diagnostics.
pub mod legacy {
    pub use crate::table::{
        read_jsonl_file, read_jsonl_table, read_jsonl_values, write_jsonl_table,
    };
}
