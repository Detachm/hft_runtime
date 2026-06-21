use crate::fetch::Fetcher;
use crate::manifest::{cache_manifest_path, relative_to};
use crate::types::*;
use anyhow::{Context, Result};
use market_data_etl_core::{
    read_jsonl_file, read_parquet_table, sha256_bytes, write_json_file_pretty,
};
use std::fs;
use std::path::Path;

pub fn sync_inputs(plan: &PipelinePlan, fetcher: &dyn Fetcher) -> Result<CacheManifest> {
    fs::create_dir_all(&plan.cache_root)
        .with_context(|| format!("create cache root {}", plan.cache_root.display()))?;

    let mut records = Vec::new();
    for spec in &plan.binance_sources {
        records.push(sync_one_cache_source(
            plan,
            fetcher,
            CacheGroup::Binance1sReference,
            spec,
        )?);
    }
    for spec in &plan.settlement_sources {
        records.push(sync_one_cache_source(
            plan,
            fetcher,
            CacheGroup::PolymarketSettlement,
            spec,
        )?);
    }

    let manifest = CacheManifest {
        schema_version: 1,
        dataset_format: CACHE_MANIFEST_FORMAT.to_string(),
        records,
    };
    write_json_file_pretty(&cache_manifest_path(plan), &manifest)?;
    Ok(manifest)
}

pub(crate) fn input_availability_from_cache_record(record: &CacheRecord) -> InputAvailabilityRow {
    InputAvailabilityRow {
        schema_version: 1,
        dataset_format: INPUT_AVAILABILITY_FORMAT.to_string(),
        group: record.group,
        name: record.name.clone(),
        source_url: record.source_url.clone(),
        symbol: record.symbol.clone(),
        start_ts_ns: record.start_ts_ns,
        end_ts_ns: record.end_ts_ns,
        status: record.status,
        missing_count: record.missing_count,
        failure_reason: record.failure_reason.clone(),
        response_hash: record.response_hash.clone(),
    }
}

pub(crate) fn read_cached_records<T>(path: &Path) -> Result<Vec<T>>
where
    T: serde::de::DeserializeOwned,
{
    if path.is_dir() {
        return read_parquet_table(path)
            .with_context(|| format!("read parquet cache table {}", path.display()));
    }

    let bytes = fs::read(path).with_context(|| format!("read cache {}", path.display()))?;
    if bytes
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        == Some(b'[')
    {
        return serde_json::from_slice::<Vec<T>>(&bytes)
            .with_context(|| format!("parse JSON array cache {}", path.display()));
    }
    // Legacy/cache-only JSONL input. Production raw/fact outputs must not use JSONL writers.
    read_jsonl_file(path)
}

fn sync_one_cache_source(
    plan: &PipelinePlan,
    fetcher: &dyn Fetcher,
    group: CacheGroup,
    spec: &CacheSourceSpec,
) -> Result<CacheRecord> {
    let group_dir = plan.cache_root.join(group.as_str());
    fs::create_dir_all(&group_dir).with_context(|| format!("create {}", group_dir.display()))?;
    let target = group_dir.join(safe_cache_file_name(&spec.name));

    match fetcher.fetch(&spec.source_url) {
        Ok(bytes) if bytes.is_empty() => Ok(CacheRecord {
            group,
            name: spec.name.clone(),
            source_url: spec.source_url.clone(),
            symbol: spec.symbol.clone(),
            start_ts_ns: spec.start_ts_ns,
            end_ts_ns: spec.end_ts_ns,
            status: CacheRecordStatus::Missing,
            cache_path: None,
            missing_count: 1,
            failure_reason: Some("empty response".to_string()),
            response_hash: None,
        }),
        Ok(bytes) => {
            fs::write(&target, &bytes).with_context(|| format!("write {}", target.display()))?;
            Ok(CacheRecord {
                group,
                name: spec.name.clone(),
                source_url: spec.source_url.clone(),
                symbol: spec.symbol.clone(),
                start_ts_ns: spec.start_ts_ns,
                end_ts_ns: spec.end_ts_ns,
                status: CacheRecordStatus::Available,
                cache_path: Some(relative_to(&target, &plan.cache_root)?),
                missing_count: 0,
                failure_reason: None,
                response_hash: Some(sha256_bytes(&bytes)),
            })
        }
        Err(err) => Ok(CacheRecord {
            group,
            name: spec.name.clone(),
            source_url: spec.source_url.clone(),
            symbol: spec.symbol.clone(),
            start_ts_ns: spec.start_ts_ns,
            end_ts_ns: spec.end_ts_ns,
            status: CacheRecordStatus::Failed,
            cache_path: None,
            missing_count: 1,
            failure_reason: Some(err.to_string()),
            response_hash: None,
        }),
    }
}

fn safe_cache_file_name(name: &str) -> String {
    let mut out = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if !out.ends_with(".jsonl") && !out.ends_with(".json") {
        out.push_str(".jsonl");
    }
    out
}
