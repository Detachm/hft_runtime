use crate::acceptance::accept;
use crate::constants::{ACCEPTANCE_REPORT, EXPORT_MANIFEST};
use crate::types::*;
use anyhow::{bail, Context, Result};
use market_data_etl_core::{copy_dir_recursive, hash_path, write_json_file_pretty};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

pub fn export_dataset(plan: &PipelinePlan, export_root: &Path) -> Result<()> {
    let acceptance_path = plan.dataset_root.join(ACCEPTANCE_REPORT);
    let acceptance_file = fs::File::open(&acceptance_path)
        .with_context(|| format!("open acceptance report {}", acceptance_path.display()))?;
    let acceptance: AcceptanceReport = serde_json::from_reader(acceptance_file)
        .with_context(|| format!("parse acceptance report {}", acceptance_path.display()))?;
    if !acceptance.accepted {
        bail!("export requires accepted dataset");
    }
    let before_hash = hash_path(&plan.dataset_root)?;
    accept(plan).context("fresh acceptance verification before export")?;
    let source_dataset_hash = hash_path(&plan.dataset_root)?;
    if before_hash != source_dataset_hash {
        bail!("dataset changed during export preflight acceptance");
    }
    reject_export_path(&plan.dataset_root, export_root)?;

    let stage_root = export_root.with_file_name(format!(
        "{}.staged",
        export_root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("dataset_export")
    ));
    if stage_root.exists() {
        fs::remove_dir_all(&stage_root)
            .with_context(|| format!("clear {}", stage_root.display()))?;
    }
    copy_dir_recursive(&plan.dataset_root, &stage_root)?;

    let mut copied_file_refs = BTreeMap::new();
    for file in market_data_etl_core::list_files_recursive(&stage_root)? {
        let rel = file
            .strip_prefix(&stage_root)?
            .to_string_lossy()
            .to_string();
        copied_file_refs.insert(
            rel,
            ExportedFileRef {
                byte_count: fs::metadata(&file)?.len(),
                sha256: market_data_etl_core::sha256_file(&file)?,
            },
        );
    }
    let manifest = ExportManifest {
        schema_version: 1,
        dataset_format: EXPORT_MANIFEST_FORMAT.to_string(),
        source_dataset_hash,
        acceptance_hash: hash_path(&acceptance_path)?,
        copied_file_refs,
        export_timestamp_ns: market_data_etl_core::now_unix_ns() as i64,
        exporter_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    write_json_file_pretty(&stage_root.join(EXPORT_MANIFEST), &manifest)?;
    if export_root.exists() {
        fs::remove_dir_all(export_root)
            .with_context(|| format!("clear {}", export_root.display()))?;
    }
    fs::rename(&stage_root, export_root)
        .with_context(|| format!("publish export {}", export_root.display()))?;
    Ok(())
}

fn reject_export_path(dataset_root: &Path, export_root: &Path) -> Result<()> {
    let dataset_abs = canonical_or_absolute(dataset_root)?;
    let export_abs = if export_root.exists() {
        canonical_or_absolute(export_root)?
    } else {
        canonical_or_absolute(export_root.parent().unwrap_or_else(|| Path::new(".")))?
            .join(export_root.file_name().unwrap_or_default())
    };
    if dataset_abs == export_abs {
        bail!("export path cannot be the dataset root");
    }
    if export_abs.starts_with(&dataset_abs) || dataset_abs.starts_with(&export_abs) {
        bail!("export path cannot be parent/child of dataset root");
    }
    Ok(())
}

fn canonical_or_absolute(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return fs::canonicalize(path).with_context(|| format!("canonicalize {}", path.display()));
    }
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}
