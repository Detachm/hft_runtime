use crate::{atomic_write_verified, fsync_dir, sha256_bytes, sha256_file};
use anyhow::{anyhow, bail, Context, Result};
use crc32fast::Hasher as Crc32;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

pub const HFTREC4_MAGIC: &[u8; 7] = b"HFTREC4";
pub const HFTREC4_FORMAT: &str = "hftrec4.raw_segment_manifest.v1";
pub const HFTREC4_SCHEMA_HASH: &str = "hftrec4.header.local-dicts.fixed-metadata.raw-payload.v1";

const NONE_I64: i64 = i64::MIN;
const NONE_U32: u32 = u32::MAX;
const DIGEST_LEN: usize = 32;
const METADATA_ROW_BYTES: usize = 8 + 8 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 4 + 8 + 4 + DIGEST_LEN;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Hftrec4WriteRecord {
    pub ingest_seq: u64,
    pub local_recv_ts_ns: i64,
    pub event_type: String,
    pub symbol: Option<String>,
    pub condition_id: Option<String>,
    pub asset_id: Option<String>,
    pub market_start_ts_ns: Option<i64>,
    pub market_end_ts_ns: Option<i64>,
    pub yes_asset_id: Option<String>,
    pub no_asset_id: Option<String>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Hftrec4Record {
    pub row_idx: u64,
    pub ingest_seq: u64,
    pub local_recv_ts_ns: i64,
    pub event_type: String,
    pub symbol: Option<String>,
    pub condition_id: Option<String>,
    pub asset_id: Option<String>,
    pub market_start_ts_ns: Option<i64>,
    pub market_end_ts_ns: Option<i64>,
    pub yes_asset_id: Option<String>,
    pub no_asset_id: Option<String>,
    pub payload_sha256: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Hftrec4RecordMeta {
    pub row_idx: u64,
    pub ingest_seq: u64,
    pub local_recv_ts_ns: i64,
    pub event_type: String,
    pub symbol: Option<String>,
    pub condition_id: Option<String>,
    pub asset_id: Option<String>,
    pub market_start_ts_ns: Option<i64>,
    pub market_end_ts_ns: Option<i64>,
    pub yes_asset_id: Option<String>,
    pub no_asset_id: Option<String>,
    pub payload_sha256: String,
    pub payload_len: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Hftrec4SegmentManifest {
    pub schema_version: u32,
    pub dataset_format: String,
    pub segment_path: PathBuf,
    pub segment_sha256: String,
    pub segment_bytes: u64,
    pub schema_hash: String,
    pub header_crc32: u32,
    pub metadata_crc32: u32,
    pub payload_crc32: u32,
    pub record_count: u64,
    pub min_ingest_seq: Option<u64>,
    pub max_ingest_seq: Option<u64>,
    pub min_ts_ns: Option<i64>,
    pub max_ts_ns: Option<i64>,
    pub event_type_count: usize,
    pub symbol_count: usize,
    pub condition_count: usize,
    pub asset_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Hftrec4SegmentHeaderSummary {
    pub record_count: u64,
    pub event_types: Vec<String>,
    pub symbols: Vec<String>,
    pub conditions: Vec<String>,
    pub assets: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SegmentHeader {
    schema_version: u32,
    dataset_format: String,
    schema_hash: String,
    record_count: u64,
    metadata_len: u64,
    payload_len: u64,
    event_types: Vec<String>,
    symbols: Vec<String>,
    conditions: Vec<String>,
    assets: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct EncodedRecordMeta {
    ingest_seq: u64,
    local_recv_ts_ns: i64,
    event_type_key: u32,
    symbol_key: u32,
    condition_key: u32,
    asset_key: u32,
    market_start_ts_ns: i64,
    market_end_ts_ns: i64,
    yes_asset_key: u32,
    no_asset_key: u32,
    payload_offset: u64,
    payload_len: u32,
    payload_sha256: [u8; DIGEST_LEN],
}

#[derive(Default)]
struct DictBuilder {
    event_types: Vec<String>,
    symbols: Vec<String>,
    conditions: Vec<String>,
    assets: Vec<String>,
    event_type_by_value: BTreeMap<String, u32>,
    symbol_by_value: BTreeMap<String, u32>,
    condition_by_value: BTreeMap<String, u32>,
    asset_by_value: BTreeMap<String, u32>,
}

pub fn write_hftrec4_segment(
    path: &Path,
    records: &[Hftrec4WriteRecord],
) -> Result<Hftrec4SegmentManifest> {
    if records.is_empty() {
        bail!("cannot write empty HFTREC4 segment");
    }
    let mut dict = DictBuilder::default();
    let mut payloads = Vec::new();
    let mut payload_offset = 0u64;
    let mut metadata = Vec::with_capacity(records.len());
    let mut min_seq = None::<u64>;
    let mut max_seq = None::<u64>;
    let mut min_ts = None::<i64>;
    let mut max_ts = None::<i64>;

    for record in records {
        let payload_len = u32::try_from(record.payload.len())
            .with_context(|| format!("HFTREC4 payload too large at seq {}", record.ingest_seq))?;
        let meta = EncodedRecordMeta {
            ingest_seq: record.ingest_seq,
            local_recv_ts_ns: record.local_recv_ts_ns,
            event_type_key: dict.event_type_key(&record.event_type)?,
            symbol_key: dict.optional_symbol_key(record.symbol.as_deref())?,
            condition_key: dict.optional_condition_key(record.condition_id.as_deref())?,
            asset_key: dict.optional_asset_key(record.asset_id.as_deref())?,
            market_start_ts_ns: record.market_start_ts_ns.unwrap_or(NONE_I64),
            market_end_ts_ns: record.market_end_ts_ns.unwrap_or(NONE_I64),
            yes_asset_key: dict.optional_asset_key(record.yes_asset_id.as_deref())?,
            no_asset_key: dict.optional_asset_key(record.no_asset_id.as_deref())?,
            payload_offset,
            payload_len,
            payload_sha256: digest_bytes(&record.payload)?,
        };
        payload_offset = payload_offset
            .checked_add(payload_len as u64)
            .context("HFTREC4 payload offset overflow")?;
        payloads.extend_from_slice(&record.payload);
        min_seq = Some(min_seq.map_or(record.ingest_seq, |value| value.min(record.ingest_seq)));
        max_seq = Some(max_seq.map_or(record.ingest_seq, |value| value.max(record.ingest_seq)));
        min_ts = Some(min_ts.map_or(record.local_recv_ts_ns, |value| {
            value.min(record.local_recv_ts_ns)
        }));
        max_ts = Some(max_ts.map_or(record.local_recv_ts_ns, |value| {
            value.max(record.local_recv_ts_ns)
        }));
        metadata.push(meta);
    }

    let metadata_bytes = encode_metadata_rows(&metadata);
    let header = SegmentHeader {
        schema_version: 4,
        dataset_format: HFTREC4_FORMAT.to_string(),
        schema_hash: HFTREC4_SCHEMA_HASH.to_string(),
        record_count: records.len() as u64,
        metadata_len: metadata_bytes.len() as u64,
        payload_len: payloads.len() as u64,
        event_types: dict.event_types,
        symbols: dict.symbols,
        conditions: dict.conditions,
        assets: dict.assets,
    };
    let header_bytes = serde_json::to_vec(&header).context("serialize HFTREC4 header")?;
    let header_crc32 = crc32(&header_bytes);
    let metadata_crc32 = crc32(&metadata_bytes);
    let payload_crc32 = crc32(&payloads);

    let mut bytes = Vec::with_capacity(
        HFTREC4_MAGIC.len()
            + 4
            + header_bytes.len()
            + 4
            + metadata_bytes.len()
            + payloads.len()
            + 8,
    );
    bytes.extend_from_slice(HFTREC4_MAGIC);
    bytes.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&header_bytes);
    bytes.extend_from_slice(&header_crc32.to_le_bytes());
    bytes.extend_from_slice(&metadata_bytes);
    bytes.extend_from_slice(&payloads);
    bytes.extend_from_slice(&metadata_crc32.to_le_bytes());
    bytes.extend_from_slice(&payload_crc32.to_le_bytes());

    crate::reject_tmp_path(path)?;
    atomic_write_verified(path, &bytes, |tmp| {
        scan_hftrec4_segment(tmp, |record| {
            let actual = digest_hex(&record.payload);
            if actual != record.payload_sha256 {
                bail!("HFTREC4 payload hash mismatch during write verification");
            }
            Ok(())
        })?;
        Ok(())
    })?;

    let segment_sha256 = sha256_file(path)?;
    let segment_bytes = fs::metadata(path)?.len();
    let manifest = Hftrec4SegmentManifest {
        schema_version: 1,
        dataset_format: HFTREC4_FORMAT.to_string(),
        segment_path: path.to_path_buf(),
        segment_sha256,
        segment_bytes,
        schema_hash: HFTREC4_SCHEMA_HASH.to_string(),
        header_crc32,
        metadata_crc32,
        payload_crc32,
        record_count: records.len() as u64,
        min_ingest_seq: min_seq,
        max_ingest_seq: max_seq,
        min_ts_ns: min_ts,
        max_ts_ns: max_ts,
        event_type_count: header.event_types.len(),
        symbol_count: header.symbols.len(),
        condition_count: header.conditions.len(),
        asset_count: header.assets.len(),
    };
    let manifest_path = hftrec4_manifest_path_for_segment(path);
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    atomic_write_verified(&manifest_path, &manifest_bytes, |tmp| {
        let parsed: Hftrec4SegmentManifest = serde_json::from_reader(File::open(tmp)?)?;
        if parsed.dataset_format != HFTREC4_FORMAT || parsed.schema_hash != HFTREC4_SCHEMA_HASH {
            bail!("invalid HFTREC4 manifest");
        }
        Ok(())
    })?;
    if let Some(parent) = manifest_path.parent() {
        fsync_dir(parent)?;
    }
    Ok(manifest)
}

pub fn verify_hftrec4_manifest(manifest_path: &Path) -> Result<Hftrec4SegmentManifest> {
    let manifest: Hftrec4SegmentManifest = serde_json::from_reader(
        File::open(manifest_path)
            .with_context(|| format!("open HFTREC4 manifest {}", manifest_path.display()))?,
    )
    .with_context(|| format!("parse HFTREC4 manifest {}", manifest_path.display()))?;
    if manifest.dataset_format != HFTREC4_FORMAT {
        bail!("unsupported HFTREC4 manifest format");
    }
    if manifest.schema_hash != HFTREC4_SCHEMA_HASH {
        bail!("unsupported HFTREC4 schema hash {}", manifest.schema_hash);
    }
    if fs::metadata(&manifest.segment_path)?.len() != manifest.segment_bytes {
        bail!(
            "HFTREC4 segment byte count mismatch: {}",
            manifest.segment_path.display()
        );
    }
    if sha256_file(&manifest.segment_path)? != manifest.segment_sha256 {
        bail!(
            "HFTREC4 segment hash mismatch: {}",
            manifest.segment_path.display()
        );
    }
    let mut count = 0u64;
    let mut min_seq = None::<u64>;
    let mut max_seq = None::<u64>;
    let mut min_ts = None::<i64>;
    let mut max_ts = None::<i64>;
    scan_hftrec4_segment(&manifest.segment_path, |record| {
        count += 1;
        min_seq = Some(min_seq.map_or(record.ingest_seq, |value| value.min(record.ingest_seq)));
        max_seq = Some(max_seq.map_or(record.ingest_seq, |value| value.max(record.ingest_seq)));
        min_ts = Some(min_ts.map_or(record.local_recv_ts_ns, |value| {
            value.min(record.local_recv_ts_ns)
        }));
        max_ts = Some(max_ts.map_or(record.local_recv_ts_ns, |value| {
            value.max(record.local_recv_ts_ns)
        }));
        if digest_hex(&record.payload) != record.payload_sha256 {
            bail!("HFTREC4 payload hash mismatch at row {}", record.row_idx);
        }
        Ok(())
    })?;
    if count != manifest.record_count
        || min_seq != manifest.min_ingest_seq
        || max_seq != manifest.max_ingest_seq
        || min_ts != manifest.min_ts_ns
        || max_ts != manifest.max_ts_ns
    {
        bail!("HFTREC4 manifest range mismatch");
    }
    Ok(manifest)
}

pub fn scan_hftrec4_segment<F>(path: &Path, mut visit: F) -> Result<()>
where
    F: FnMut(Hftrec4Record) -> Result<()>,
{
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let (header, header_crc32) = read_header(&mut file)?;
    if header.dataset_format != HFTREC4_FORMAT || header.schema_hash != HFTREC4_SCHEMA_HASH {
        bail!("unsupported HFTREC4 segment {}", path.display());
    }
    let metadata_len = usize::try_from(header.metadata_len).context("metadata len overflow")?;
    if metadata_len != header.record_count as usize * METADATA_ROW_BYTES {
        bail!("HFTREC4 metadata length mismatch");
    }
    let mut metadata_bytes = vec![0u8; metadata_len];
    file.read_exact(&mut metadata_bytes)
        .with_context(|| format!("read HFTREC4 metadata {}", path.display()))?;
    let metadata_crc32 = crc32(&metadata_bytes);
    let metadata = decode_metadata_rows(&metadata_bytes)?;
    let mut payload_crc = Crc32::new();

    for (idx, meta) in metadata.into_iter().enumerate() {
        let mut payload = vec![0u8; meta.payload_len as usize];
        file.read_exact(&mut payload)
            .with_context(|| format!("read HFTREC4 payload row {idx}"))?;
        payload_crc.update(&payload);
        visit(Hftrec4Record {
            row_idx: idx as u64,
            ingest_seq: meta.ingest_seq,
            local_recv_ts_ns: meta.local_recv_ts_ns,
            event_type: required_dict_value(
                &header.event_types,
                meta.event_type_key,
                "event_type",
            )?,
            symbol: optional_dict_value(&header.symbols, meta.symbol_key, "symbol")?,
            condition_id: optional_dict_value(&header.conditions, meta.condition_key, "condition")?,
            asset_id: optional_dict_value(&header.assets, meta.asset_key, "asset")?,
            market_start_ts_ns: optional_i64(meta.market_start_ts_ns),
            market_end_ts_ns: optional_i64(meta.market_end_ts_ns),
            yes_asset_id: optional_dict_value(&header.assets, meta.yes_asset_key, "yes_asset")?,
            no_asset_id: optional_dict_value(&header.assets, meta.no_asset_key, "no_asset")?,
            payload_sha256: hex::encode(meta.payload_sha256),
            payload,
        })?;
    }

    let expected_metadata_crc32 = read_u32_from_file(&mut file)?;
    let expected_payload_crc32 = read_u32_from_file(&mut file)?;
    if metadata_crc32 != expected_metadata_crc32 {
        bail!("HFTREC4 metadata CRC mismatch");
    }
    if payload_crc.finalize() != expected_payload_crc32 {
        bail!("HFTREC4 payload CRC mismatch");
    }
    if file.stream_position()? != file.metadata()?.len() {
        bail!("HFTREC4 segment has trailing bytes");
    }
    if header_crc32 != crc32(&serde_json::to_vec(&header)?) {
        bail!("HFTREC4 header CRC mismatch");
    }
    Ok(())
}

pub fn scan_hftrec4_segment_selected<P, F>(
    path: &Path,
    mut should_read_payload: P,
    mut visit: F,
) -> Result<()>
where
    P: FnMut(&Hftrec4RecordMeta) -> Result<bool>,
    F: FnMut(Hftrec4Record) -> Result<()>,
{
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let (header, header_crc32) = read_header(&mut file)?;
    if header.dataset_format != HFTREC4_FORMAT || header.schema_hash != HFTREC4_SCHEMA_HASH {
        bail!("unsupported HFTREC4 segment {}", path.display());
    }
    let metadata_len = usize::try_from(header.metadata_len).context("metadata len overflow")?;
    if metadata_len != header.record_count as usize * METADATA_ROW_BYTES {
        bail!("HFTREC4 metadata length mismatch");
    }
    let mut metadata_bytes = vec![0u8; metadata_len];
    file.read_exact(&mut metadata_bytes)
        .with_context(|| format!("read HFTREC4 metadata {}", path.display()))?;
    let metadata_crc32 = crc32(&metadata_bytes);
    let metadata = decode_metadata_rows(&metadata_bytes)?;
    let payload_start = file.stream_position()?;

    let mut selected = Vec::new();
    for (idx, meta) in metadata.iter().enumerate() {
        let view = hftrec4_meta_view(idx as u64, &header, &meta)?;
        if should_read_payload(&view)? {
            selected.push((idx, *meta, view));
        }
    }

    if selected.is_empty() {
        file.seek(SeekFrom::Start(
            payload_start
                .checked_add(header.payload_len)
                .context("HFTREC4 payload footer offset overflow")?,
        ))?;
    } else {
        let payload_len = usize::try_from(header.payload_len).context("payload len overflow")?;
        let mut payload_bytes = vec![0u8; payload_len];
        file.read_exact(&mut payload_bytes)
            .with_context(|| format!("read HFTREC4 payload block {}", path.display()))?;
        for (idx, meta, view) in selected {
            let start =
                usize::try_from(meta.payload_offset).context("HFTREC4 selected payload offset")?;
            let len = meta.payload_len as usize;
            let end = start
                .checked_add(len)
                .context("HFTREC4 selected payload end overflow")?;
            let payload = payload_bytes
                .get(start..end)
                .ok_or_else(|| anyhow!("truncated HFTREC4 selected payload row {idx}"))?
                .to_vec();
            visit(Hftrec4Record {
                row_idx: view.row_idx,
                ingest_seq: view.ingest_seq,
                local_recv_ts_ns: view.local_recv_ts_ns,
                event_type: view.event_type,
                symbol: view.symbol,
                condition_id: view.condition_id,
                asset_id: view.asset_id,
                market_start_ts_ns: view.market_start_ts_ns,
                market_end_ts_ns: view.market_end_ts_ns,
                yes_asset_id: view.yes_asset_id,
                no_asset_id: view.no_asset_id,
                payload_sha256: view.payload_sha256,
                payload,
            })?;
        }
    }

    let expected_metadata_crc32 = read_u32_from_file(&mut file)?;
    let _expected_payload_crc32 = read_u32_from_file(&mut file)?;
    if metadata_crc32 != expected_metadata_crc32 {
        bail!("HFTREC4 metadata CRC mismatch");
    }
    if file.stream_position()? != file.metadata()?.len() {
        bail!("HFTREC4 segment has trailing bytes");
    }
    if header_crc32 != crc32(&serde_json::to_vec(&header)?) {
        bail!("HFTREC4 header CRC mismatch");
    }
    Ok(())
}

pub fn read_hftrec4_segment_header_summary(path: &Path) -> Result<Hftrec4SegmentHeaderSummary> {
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let (header, header_crc32) = read_header(&mut file)?;
    if header.dataset_format != HFTREC4_FORMAT || header.schema_hash != HFTREC4_SCHEMA_HASH {
        bail!("unsupported HFTREC4 segment {}", path.display());
    }
    if header_crc32 != crc32(&serde_json::to_vec(&header)?) {
        bail!("HFTREC4 header CRC mismatch");
    }
    Ok(Hftrec4SegmentHeaderSummary {
        record_count: header.record_count,
        event_types: header.event_types,
        symbols: header.symbols,
        conditions: header.conditions,
        assets: header.assets,
    })
}

pub fn read_hftrec4_records(path: &Path) -> Result<Vec<Hftrec4Record>> {
    let mut out = Vec::new();
    scan_hftrec4_segment(path, |record| {
        out.push(record);
        Ok(())
    })?;
    Ok(out)
}

fn hftrec4_meta_view(
    row_idx: u64,
    header: &SegmentHeader,
    meta: &EncodedRecordMeta,
) -> Result<Hftrec4RecordMeta> {
    Ok(Hftrec4RecordMeta {
        row_idx,
        ingest_seq: meta.ingest_seq,
        local_recv_ts_ns: meta.local_recv_ts_ns,
        event_type: required_dict_value(&header.event_types, meta.event_type_key, "event_type")?,
        symbol: optional_dict_value(&header.symbols, meta.symbol_key, "symbol")?,
        condition_id: optional_dict_value(&header.conditions, meta.condition_key, "condition")?,
        asset_id: optional_dict_value(&header.assets, meta.asset_key, "asset")?,
        market_start_ts_ns: optional_i64(meta.market_start_ts_ns),
        market_end_ts_ns: optional_i64(meta.market_end_ts_ns),
        yes_asset_id: optional_dict_value(&header.assets, meta.yes_asset_key, "yes_asset")?,
        no_asset_id: optional_dict_value(&header.assets, meta.no_asset_key, "no_asset")?,
        payload_sha256: hex::encode(meta.payload_sha256),
        payload_len: meta.payload_len,
    })
}

pub fn hftrec4_manifest_path_for_segment(segment: &Path) -> PathBuf {
    let name = segment
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("segment.hfr4");
    segment.with_file_name(format!("{name}.manifest.json"))
}

pub fn discover_hftrec4_manifests(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = crate::list_files_recursive(root)?
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".hfr4.manifest.json"))
        })
        .collect::<Vec<_>>();
    out.sort();
    Ok(out)
}

fn read_header(file: &mut File) -> Result<(SegmentHeader, u32)> {
    let mut magic = [0u8; HFTREC4_MAGIC.len()];
    file.read_exact(&mut magic)?;
    if magic != *HFTREC4_MAGIC {
        bail!("invalid HFTREC4 magic");
    }
    let header_len = read_u32_from_file(file)? as usize;
    let mut header_bytes = vec![0u8; header_len];
    file.read_exact(&mut header_bytes)?;
    let expected_crc = read_u32_from_file(file)?;
    if crc32(&header_bytes) != expected_crc {
        bail!("HFTREC4 header CRC mismatch");
    }
    let header: SegmentHeader =
        serde_json::from_slice(&header_bytes).context("parse HFTREC4 header")?;
    Ok((header, expected_crc))
}

fn encode_metadata_rows(rows: &[EncodedRecordMeta]) -> Vec<u8> {
    let mut out = Vec::with_capacity(rows.len() * METADATA_ROW_BYTES);
    for row in rows {
        out.extend_from_slice(&row.ingest_seq.to_le_bytes());
        out.extend_from_slice(&row.local_recv_ts_ns.to_le_bytes());
        out.extend_from_slice(&row.event_type_key.to_le_bytes());
        out.extend_from_slice(&row.symbol_key.to_le_bytes());
        out.extend_from_slice(&row.condition_key.to_le_bytes());
        out.extend_from_slice(&row.asset_key.to_le_bytes());
        out.extend_from_slice(&row.market_start_ts_ns.to_le_bytes());
        out.extend_from_slice(&row.market_end_ts_ns.to_le_bytes());
        out.extend_from_slice(&row.yes_asset_key.to_le_bytes());
        out.extend_from_slice(&row.no_asset_key.to_le_bytes());
        out.extend_from_slice(&row.payload_offset.to_le_bytes());
        out.extend_from_slice(&row.payload_len.to_le_bytes());
        out.extend_from_slice(&row.payload_sha256);
    }
    out
}

fn decode_metadata_rows(bytes: &[u8]) -> Result<Vec<EncodedRecordMeta>> {
    if bytes.len() % METADATA_ROW_BYTES != 0 {
        bail!("invalid HFTREC4 metadata byte length");
    }
    let mut cursor = 0usize;
    let mut out = Vec::with_capacity(bytes.len() / METADATA_ROW_BYTES);
    while cursor < bytes.len() {
        out.push(EncodedRecordMeta {
            ingest_seq: read_u64(bytes, &mut cursor)?,
            local_recv_ts_ns: read_i64(bytes, &mut cursor)?,
            event_type_key: read_u32(bytes, &mut cursor)?,
            symbol_key: read_u32(bytes, &mut cursor)?,
            condition_key: read_u32(bytes, &mut cursor)?,
            asset_key: read_u32(bytes, &mut cursor)?,
            market_start_ts_ns: read_i64(bytes, &mut cursor)?,
            market_end_ts_ns: read_i64(bytes, &mut cursor)?,
            yes_asset_key: read_u32(bytes, &mut cursor)?,
            no_asset_key: read_u32(bytes, &mut cursor)?,
            payload_offset: read_u64(bytes, &mut cursor)?,
            payload_len: read_u32(bytes, &mut cursor)?,
            payload_sha256: {
                let digest = bytes
                    .get(cursor..cursor + DIGEST_LEN)
                    .ok_or_else(|| anyhow!("truncated HFTREC4 digest"))?;
                cursor += DIGEST_LEN;
                digest.try_into().unwrap()
            },
        });
    }
    for pair in out.windows(2) {
        if pair[1].payload_offset
            != pair[0]
                .payload_offset
                .checked_add(pair[0].payload_len as u64)
                .context("HFTREC4 payload offset overflow")?
        {
            bail!("HFTREC4 payload offset regression");
        }
    }
    Ok(out)
}

fn read_u32_from_file(file: &mut File) -> Result<u32> {
    let mut bytes = [0u8; 4];
    file.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let out = bytes
        .get(*cursor..*cursor + 4)
        .ok_or_else(|| anyhow!("truncated HFTREC4 u32"))?;
    *cursor += 4;
    Ok(u32::from_le_bytes(out.try_into().unwrap()))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    let out = bytes
        .get(*cursor..*cursor + 8)
        .ok_or_else(|| anyhow!("truncated HFTREC4 u64"))?;
    *cursor += 8;
    Ok(u64::from_le_bytes(out.try_into().unwrap()))
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64> {
    let out = bytes
        .get(*cursor..*cursor + 8)
        .ok_or_else(|| anyhow!("truncated HFTREC4 i64"))?;
    *cursor += 8;
    Ok(i64::from_le_bytes(out.try_into().unwrap()))
}

fn optional_i64(value: i64) -> Option<i64> {
    if value == NONE_I64 {
        None
    } else {
        Some(value)
    }
}

fn required_dict_value(values: &[String], key: u32, field: &str) -> Result<String> {
    values
        .get(key as usize)
        .cloned()
        .ok_or_else(|| anyhow!("HFTREC4 {field} key out of range"))
}

fn optional_dict_value(values: &[String], key: u32, field: &str) -> Result<Option<String>> {
    if key == NONE_U32 {
        return Ok(None);
    }
    Ok(Some(required_dict_value(values, key, field)?))
}

fn digest_bytes(payload: &[u8]) -> Result<[u8; DIGEST_LEN]> {
    let hex = sha256_bytes(payload);
    let decoded = hex::decode(hex).context("decode sha256 hex")?;
    decoded
        .try_into()
        .map_err(|_| anyhow!("invalid sha256 digest length"))
}

fn digest_hex(payload: &[u8]) -> String {
    sha256_bytes(payload)
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Crc32::new();
    hasher.update(bytes);
    hasher.finalize()
}

impl DictBuilder {
    fn event_type_key(&mut self, value: &str) -> Result<u32> {
        intern(value, &mut self.event_types, &mut self.event_type_by_value)
    }

    fn optional_symbol_key(&mut self, value: Option<&str>) -> Result<u32> {
        optional_intern(value, &mut self.symbols, &mut self.symbol_by_value)
    }

    fn optional_condition_key(&mut self, value: Option<&str>) -> Result<u32> {
        optional_intern(value, &mut self.conditions, &mut self.condition_by_value)
    }

    fn optional_asset_key(&mut self, value: Option<&str>) -> Result<u32> {
        optional_intern(value, &mut self.assets, &mut self.asset_by_value)
    }
}

fn optional_intern(
    value: Option<&str>,
    values: &mut Vec<String>,
    lookup: &mut BTreeMap<String, u32>,
) -> Result<u32> {
    match value {
        Some(value) if !value.is_empty() => intern(value, values, lookup),
        _ => Ok(NONE_U32),
    }
}

fn intern(
    value: &str,
    values: &mut Vec<String>,
    lookup: &mut BTreeMap<String, u32>,
) -> Result<u32> {
    if let Some(key) = lookup.get(value) {
        return Ok(*key);
    }
    let key = u32::try_from(values.len()).context("HFTREC4 dictionary overflow")?;
    values.push(value.to_string());
    lookup.insert(value.to_string(), key);
    Ok(key)
}
