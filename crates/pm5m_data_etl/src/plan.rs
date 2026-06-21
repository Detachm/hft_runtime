use crate::constants::PLAN_FILE;
use crate::types::*;
use anyhow::{bail, Context, Result};
use market_data_etl_core::write_json_file_pretty;
use std::fs;
use std::path::{Path, PathBuf};

pub fn write_plan(plan: &PipelinePlan) -> Result<PathBuf> {
    let path = plan.plan_root.join(PLAN_FILE);
    write_json_file_pretty(&path, plan)?;
    Ok(path)
}

pub fn load_plan(path: &Path) -> Result<PipelinePlan> {
    let file = fs::File::open(path).with_context(|| format!("open plan {}", path.display()))?;
    let plan = serde_json::from_reader::<_, PipelinePlan>(file)
        .with_context(|| format!("parse plan {}", path.display()))?;
    if plan.dataset_format != PLAN_DATASET_FORMAT {
        bail!(
            "unsupported plan dataset_format {}, expected {}",
            plan.dataset_format,
            PLAN_DATASET_FORMAT
        );
    }
    if let (Some(start), Some(end)) = (plan.raw_start_ts_ns, plan.raw_end_ts_ns) {
        if end <= start {
            bail!("raw_end_ts_ns must be greater than raw_start_ts_ns");
        }
    }
    Ok(plan)
}
