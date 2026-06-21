use crate::constants::{CACHE_MANIFEST, DATASET_MANIFEST};
use crate::types::*;
use anyhow::{Context, Result};
use market_data_etl_core::{hash_path, hash_serializable, sha256_bytes, write_json_file_pretty};
use serde::Serialize;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) fn cache_manifest_path(plan: &PipelinePlan) -> PathBuf {
    plan.plan_root.join(CACHE_MANIFEST)
}

pub(crate) fn read_cache_manifest(plan: &PipelinePlan) -> Result<CacheManifest> {
    let path = cache_manifest_path(plan);
    if !path.exists() {
        return Ok(CacheManifest {
            schema_version: 1,
            dataset_format: CACHE_MANIFEST_FORMAT.to_string(),
            records: Vec::new(),
        });
    }
    let file = fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    serde_json::from_reader(file).with_context(|| format!("parse {}", path.display()))
}

pub(crate) fn dataset_path(plan: &PipelinePlan, relative: &str) -> PathBuf {
    plan.dataset_root.join(relative)
}

pub(crate) fn relative_to(path: &Path, root: &Path) -> Result<PathBuf> {
    Ok(path
        .strip_prefix(root)
        .with_context(|| format!("strip prefix {} from {}", root.display(), path.display()))?
        .to_path_buf())
}

pub(crate) fn row_hash<T: Serialize>(row: &T) -> Result<String> {
    let mut value = serde_json::to_value(row).context("serialize row for row_hash")?;
    if let Value::Object(map) = &mut value {
        map.remove("row_hash");
    }
    Ok(sha256_bytes(
        &serde_json::to_vec(&value).context("serialize row_hash value")?,
    ))
}

pub(crate) fn existing_hash(path: &Path) -> Result<Option<String>> {
    if path.exists() {
        Ok(Some(hash_path(path)?))
    } else {
        Ok(None)
    }
}

pub(crate) fn cache_group_hash(manifest: &CacheManifest, group: CacheGroup) -> Result<String> {
    let rows = manifest
        .records
        .iter()
        .filter(|record| record.group == group)
        .cloned()
        .collect::<Vec<_>>();
    hash_serializable(&rows)
}

pub(crate) fn base_dataset_manifest(plan: &PipelinePlan) -> DatasetManifest {
    DatasetManifest {
        schema_version: 1,
        dataset_format: DATASET_MANIFEST_FORMAT.to_string(),
        raw_roots: plan.raw_roots.clone(),
        plan_root: plan.plan_root.clone(),
        cache_root: plan.cache_root.clone(),
        dataset_root: plan.dataset_root.clone(),
        binance_cache_manifest_hash: None,
        settlement_cache_manifest_hash: None,
        fact_table_hash: None,
        depth_feature_hash: None,
        event_index_hash: None,
        contains_strategy_fields: false,
        contains_settlement_in_event_stream: false,
    }
}

pub(crate) fn read_or_base_dataset_manifest(plan: &PipelinePlan) -> Result<DatasetManifest> {
    let path = plan.dataset_root.join(DATASET_MANIFEST);
    if !path.exists() {
        return Ok(base_dataset_manifest(plan));
    }
    let file = fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    serde_json::from_reader(file).with_context(|| format!("parse {}", path.display()))
}

pub(crate) fn write_dataset_manifest(
    plan: &PipelinePlan,
    manifest: &DatasetManifest,
) -> Result<()> {
    write_json_file_pretty(&plan.dataset_root.join(DATASET_MANIFEST), manifest)
}
