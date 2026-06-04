use crate::hash_path;
use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize)]
pub struct TableWriteReport {
    pub table_path: PathBuf,
    pub part_path: PathBuf,
    pub schema_path: PathBuf,
    pub row_count: usize,
    pub table_hash: String,
}

pub fn write_json_file_pretty<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    serde_json::to_writer_pretty(file, value)
        .with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

pub fn write_jsonl_table<T: Serialize>(table_path: &Path, rows: &[T]) -> Result<TableWriteReport> {
    fs::create_dir_all(table_path).with_context(|| format!("create {}", table_path.display()))?;

    let part_path = table_path.join("part-00000.jsonl");
    let schema_path = table_path.join("_schema.json");
    let mut writer = BufWriter::new(
        File::create(&part_path).with_context(|| format!("create {}", part_path.display()))?,
    );

    let mut fields = BTreeSet::new();
    for row in rows {
        let value = serde_json::to_value(row).context("serialize row")?;
        collect_top_level_fields(&value, &mut fields);
        serde_json::to_writer(&mut writer, &value).context("write json row")?;
        writer.write_all(b"\n").context("write jsonl newline")?;
    }
    writer.flush().context("flush jsonl table")?;

    write_json_file_pretty(
        &schema_path,
        &json!({
            "physical_format": "jsonl",
            "part_files": ["part-00000.jsonl"],
            "fields": fields.into_iter().collect::<Vec<_>>()
        }),
    )?;

    let table_hash = hash_path(table_path)?;
    Ok(TableWriteReport {
        table_path: table_path.to_path_buf(),
        part_path,
        schema_path,
        row_count: rows.len(),
        table_hash,
    })
}

pub fn read_jsonl_table<T: DeserializeOwned>(table_path: &Path) -> Result<Vec<T>> {
    let mut part_files = fs::read_dir(table_path)
        .with_context(|| format!("read table dir {}", table_path.display()))?
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("collect table dir {}", table_path.display()))?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("part-") && name.ends_with(".jsonl"))
        })
        .collect::<Vec<_>>();
    part_files.sort();

    let mut rows = Vec::new();
    for file in part_files {
        rows.extend(read_jsonl_file::<T>(&file)?);
    }
    Ok(rows)
}

pub fn read_jsonl_file<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>> {
    let reader =
        BufReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?);
    let mut rows = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read {} line {}", path.display(), idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let row = serde_json::from_str::<T>(&line)
            .with_context(|| format!("parse {} line {}", path.display(), idx + 1))?;
        rows.push(row);
    }
    Ok(rows)
}

pub fn read_jsonl_values(path: &Path) -> Result<Vec<Value>> {
    read_jsonl_file(path)
}

fn collect_top_level_fields(value: &Value, fields: &mut BTreeSet<String>) {
    if let Value::Object(map) = value {
        for key in map.keys() {
            fields.insert(key.clone());
        }
    }
}
