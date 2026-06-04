use crate::list_files_recursive;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaFieldViolation {
    pub path: PathBuf,
    pub field: String,
}

pub fn scan_json_fields(root: &Path, banned_fields: &[&str]) -> Result<Vec<SchemaFieldViolation>> {
    let banned = banned_fields.iter().copied().collect::<BTreeSet<_>>();
    let mut violations = Vec::new();
    for file in list_files_recursive(root)? {
        let is_jsonish = file
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext == "json" || ext == "jsonl");
        if !is_jsonish {
            continue;
        }

        if file.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            scan_jsonl_file(&file, &banned, &mut violations)?;
        } else {
            let value: Value = serde_json::from_reader(
                File::open(&file).with_context(|| format!("open {}", file.display()))?,
            )
            .with_context(|| format!("parse {}", file.display()))?;
            scan_value(&file, &value, &banned, &mut violations);
        }
    }

    violations.sort_by(|a, b| a.path.cmp(&b.path).then(a.field.cmp(&b.field)));
    violations.dedup_by(|a, b| a.path == b.path && a.field == b.field);
    Ok(violations)
}

fn scan_jsonl_file(
    path: &Path,
    banned: &BTreeSet<&str>,
    violations: &mut Vec<SchemaFieldViolation>,
) -> Result<()> {
    let reader =
        BufReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?);
    for (idx, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read {} line {}", path.display(), idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let value = serde_json::from_str::<Value>(&line)
            .with_context(|| format!("parse {} line {}", path.display(), idx + 1))?;
        scan_value(path, &value, banned, violations);
    }
    Ok(())
}

fn scan_value(
    path: &Path,
    value: &Value,
    banned: &BTreeSet<&str>,
    violations: &mut Vec<SchemaFieldViolation>,
) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if banned.contains(key.as_str()) {
                    violations.push(SchemaFieldViolation {
                        path: path.to_path_buf(),
                        field: key.clone(),
                    });
                }
                scan_value(path, child, banned, violations);
            }
        }
        Value::Array(items) => {
            for item in items {
                scan_value(path, item, banned, violations);
            }
        }
        _ => {}
    }
}
