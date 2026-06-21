use crate::hash_path;
use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, StringArray, StringBuilder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeSet, VecDeque};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize)]
pub struct TableWriteReport {
    pub table_path: PathBuf,
    pub part_path: PathBuf,
    pub schema_path: PathBuf,
    pub row_count: usize,
    pub byte_count: u64,
    pub part_hash: String,
    pub table_hash: String,
}

pub struct ParquetTableStreamWriter {
    table_path: PathBuf,
    part_path: PathBuf,
    schema_path: PathBuf,
    timestamp_field: Option<String>,
    fields: Option<Vec<InferredField>>,
    schema: Option<Arc<Schema>>,
    parts: Vec<ParquetPartManifest>,
    row_count: usize,
    byte_count: u64,
}

pub struct ParquetFileRowIterator<T> {
    file: PathBuf,
    reader: Box<dyn Iterator<Item = std::result::Result<RecordBatch, ArrowError>>>,
    rows: VecDeque<T>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParquetPartManifest {
    pub file: String,
    pub row_count: usize,
    pub byte_count: u64,
    pub sha256: String,
    pub min_ts_ns: Option<i64>,
    pub max_ts_ns: Option<i64>,
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

/// Legacy/debug JSONL table writer.
///
/// Production recorder raw audit segments must use HFTREC4, and ETL
/// fact/derived/event tables must use Parquet/ZSTD. Keep this helper for small
/// fixtures and explicit diagnostics only.
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
        byte_count: 0,
        part_hash: String::new(),
        table_hash,
    })
}

pub fn write_parquet_table<T: Serialize>(
    table_path: &Path,
    rows: &[T],
    timestamp_field: Option<&str>,
) -> Result<TableWriteReport> {
    let mut writer = ParquetTableStreamWriter::new(table_path, timestamp_field)?;
    writer.write_rows(rows)?;
    writer.finish()
}

impl ParquetTableStreamWriter {
    pub fn new(table_path: &Path, timestamp_field: Option<&str>) -> Result<Self> {
        if table_path.exists() {
            fs::remove_dir_all(table_path)
                .with_context(|| format!("remove existing table {}", table_path.display()))?;
        }
        fs::create_dir_all(table_path)
            .with_context(|| format!("create {}", table_path.display()))?;
        Ok(Self {
            table_path: table_path.to_path_buf(),
            part_path: table_path.join("part-00000.parquet"),
            schema_path: table_path.join("_schema.json"),
            timestamp_field: timestamp_field.map(str::to_string),
            fields: None,
            schema: None,
            parts: Vec::new(),
            row_count: 0,
            byte_count: 0,
        })
    }

    pub fn write_rows<T: Serialize>(&mut self, rows: &[T]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let values = rows
            .iter()
            .map(|row| serde_json::to_value(row).context("serialize row"))
            .collect::<Result<Vec<_>>>()?;
        if self.fields.is_none() {
            let fields = infer_fields(&values)?;
            let arrow_fields = fields
                .iter()
                .map(|field| Field::new(field.name.clone(), field.data_type.clone(), true))
                .collect::<Vec<_>>();
            self.schema = Some(Arc::new(Schema::new(arrow_fields)));
            self.fields = Some(fields);
        }

        let fields = self.fields.as_ref().expect("fields initialized");
        let schema = self.schema.as_ref().expect("schema initialized").clone();
        let part_idx = self.parts.len();
        let file_name = format!("part-{part_idx:05}.parquet");
        let part_path = self.table_path.join(&file_name);
        write_parquet_part(&part_path, schema, fields, &values)?;

        let byte_count = fs::metadata(&part_path)?.len();
        let part_hash = crate::sha256_file(&part_path)?;
        let min_max = self
            .timestamp_field
            .as_deref()
            .and_then(|field| min_max_i64(&values, field));
        self.parts.push(ParquetPartManifest {
            file: file_name,
            row_count: rows.len(),
            byte_count,
            sha256: part_hash,
            min_ts_ns: min_max.map(|pair| pair.0),
            max_ts_ns: min_max.map(|pair| pair.1),
        });
        self.row_count += rows.len();
        self.byte_count += byte_count;
        Ok(())
    }

    pub fn finish(mut self) -> Result<TableWriteReport> {
        if self.parts.is_empty() {
            self.fields = Some(Vec::new());
            self.schema = Some(Arc::new(Schema::new(vec![Field::new(
                "__empty",
                DataType::Boolean,
                true,
            )])));
            let part_path = self.table_path.join("part-00000.parquet");
            let empty = BooleanBuilder::new().finish();
            write_parquet_arrays(
                &part_path,
                self.schema.as_ref().expect("schema initialized").clone(),
                vec![Arc::new(empty)],
            )?;
            let byte_count = fs::metadata(&part_path)?.len();
            let part_hash = crate::sha256_file(&part_path)?;
            self.parts.push(ParquetPartManifest {
                file: "part-00000.parquet".to_string(),
                row_count: 0,
                byte_count,
                sha256: part_hash,
                min_ts_ns: None,
                max_ts_ns: None,
            });
            self.byte_count = byte_count;
        }

        let fields = self.fields.as_ref().expect("fields initialized");
        let part_hash = self
            .parts
            .first()
            .map(|part| part.sha256.clone())
            .unwrap_or_default();
        write_json_file_pretty(
            &self.schema_path,
            &json!({
                "physical_format": "parquet",
                "compression": "zstd",
                "part_files": &self.parts,
                "fields": fields.iter().map(|f| json!({"name": f.name, "type": format!("{:?}", f.data_type)})).collect::<Vec<_>>()
            }),
        )?;
        let table_hash = hash_path(&self.table_path)?;
        Ok(TableWriteReport {
            table_path: self.table_path,
            part_path: self.part_path,
            schema_path: self.schema_path,
            row_count: self.row_count,
            byte_count: self.byte_count,
            part_hash,
            table_hash,
        })
    }
}

fn write_parquet_part(
    part_path: &Path,
    schema: Arc<Schema>,
    fields: &[InferredField],
    values: &[Value],
) -> Result<()> {
    let arrays = build_arrays(fields, values)?;
    write_parquet_arrays(part_path, schema, arrays)
}

fn write_parquet_arrays(
    part_path: &Path,
    schema: Arc<Schema>,
    arrays: Vec<ArrayRef>,
) -> Result<()> {
    let batch = RecordBatch::try_new(schema.clone(), arrays).context("build parquet batch")?;
    let file =
        File::create(part_path).with_context(|| format!("create {}", part_path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();
    let mut writer =
        ArrowWriter::try_new(file, schema, Some(props)).context("create parquet writer")?;
    writer.write(&batch).context("write parquet batch")?;
    writer.close().context("close parquet writer")?;
    Ok(())
}

pub fn read_parquet_table<T: DeserializeOwned>(table_path: &Path) -> Result<Vec<T>> {
    let mut rows = Vec::new();
    for_each_parquet_table_row(table_path, |row| {
        rows.push(row);
        Ok(())
    })?;
    Ok(rows)
}

pub fn for_each_parquet_table_row<T, F>(table_path: &Path, mut visit: F) -> Result<()>
where
    T: DeserializeOwned,
    F: FnMut(T) -> Result<()>,
{
    let mut part_files = fs::read_dir(table_path)
        .with_context(|| format!("read table dir {}", table_path.display()))?
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("parquet"))
        .collect::<Vec<_>>();
    part_files.sort();
    for file in part_files {
        let reader = ParquetRecordBatchReaderBuilder::try_new(
            File::open(&file).with_context(|| format!("open {}", file.display()))?,
        )?
        .build()?;
        for batch in reader {
            let batch = batch?;
            for value in batch_to_json_values(&batch)
                .with_context(|| format!("decode typed parquet rows from {}", file.display()))?
            {
                visit(serde_json::from_value::<T>(value)?)?;
            }
        }
    }
    Ok(())
}

pub fn parquet_part_files(table_path: &Path) -> Result<Vec<PathBuf>> {
    let mut part_files = fs::read_dir(table_path)
        .with_context(|| format!("read table dir {}", table_path.display()))?
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("parquet"))
        .collect::<Vec<_>>();
    part_files.sort();
    Ok(part_files)
}

pub fn parquet_file_row_iter<T: DeserializeOwned>(
    file: &Path,
) -> Result<ParquetFileRowIterator<T>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(
        File::open(file).with_context(|| format!("open {}", file.display()))?,
    )?
    .build()?;
    Ok(ParquetFileRowIterator {
        file: file.to_path_buf(),
        reader: Box::new(reader),
        rows: VecDeque::new(),
    })
}

impl<T: DeserializeOwned> ParquetFileRowIterator<T> {
    pub fn next_row(&mut self) -> Result<Option<T>> {
        loop {
            if let Some(row) = self.rows.pop_front() {
                return Ok(Some(row));
            }
            let Some(batch) = self.reader.next() else {
                return Ok(None);
            };
            let batch = batch?;
            for value in batch_to_json_values(&batch).with_context(|| {
                format!("decode typed parquet rows from {}", self.file.display())
            })? {
                self.rows.push_back(serde_json::from_value::<T>(value)?);
            }
        }
    }
}

pub fn verify_parquet_zstd_table(table_path: &Path, table_name: &str) -> Result<()> {
    if table_path.file_name().is_none() || !table_path.exists() {
        bail!("required table {table_name} missing");
    }
    let schema_path = table_path.join("_schema.json");
    let schema: Value = serde_json::from_reader(File::open(&schema_path)?)?;
    if schema.get("physical_format").and_then(Value::as_str) != Some("parquet") {
        bail!("table {table_name} is not Parquet");
    }
    if schema.get("compression").and_then(Value::as_str) != Some("zstd") {
        bail!("table {table_name} is not Parquet/ZSTD");
    }
    let parts = schema
        .get("part_files")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("table {table_name} missing part manifest"))?;
    if parts.is_empty() {
        bail!("table {table_name} has no parquet parts");
    }
    for part in parts {
        let file_name = part
            .get("file")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("table {table_name} part missing file"))?;
        if !file_name.ends_with(".parquet") {
            bail!("table {table_name} contains non-parquet part {file_name}");
        }
        let file = table_path.join(file_name);
        let expected_hash = part.get("sha256").and_then(Value::as_str).unwrap_or("");
        if crate::sha256_file(&file)? != expected_hash {
            bail!("table {table_name} parquet part hash mismatch");
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct InferredField {
    name: String,
    data_type: DataType,
}

fn infer_fields(values: &[Value]) -> Result<Vec<InferredField>> {
    let mut names = BTreeSet::new();
    for value in values {
        let Value::Object(map) = value else {
            bail!("parquet rows must serialize as JSON objects");
        };
        for (key, value) in map {
            if value.is_array() || value.is_object() {
                bail!("nested production parquet column '{key}' is not allowed");
            }
            names.insert(key.clone());
        }
    }
    let mut fields = Vec::new();
    for name in names {
        let mut data_type = DataType::Utf8;
        for value in values.iter().filter_map(|value| value.get(&name)) {
            if value.is_null() {
                continue;
            }
            data_type = if value.is_boolean() {
                DataType::Boolean
            } else if value.as_i64().is_some() {
                DataType::Int64
            } else if value.as_u64().is_some() {
                DataType::UInt64
            } else if value.as_f64().is_some() {
                DataType::Float64
            } else {
                DataType::Utf8
            };
            break;
        }
        fields.push(InferredField { name, data_type });
    }
    Ok(fields)
}

fn build_arrays(fields: &[InferredField], values: &[Value]) -> Result<Vec<ArrayRef>> {
    let mut arrays = Vec::<ArrayRef>::new();
    for field in fields {
        match field.data_type {
            DataType::Boolean => {
                let mut b = BooleanBuilder::new();
                for row in values {
                    if let Some(v) = row.get(&field.name).and_then(Value::as_bool) {
                        b.append_value(v);
                    } else {
                        b.append_null();
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
            DataType::Int64 => {
                let mut b = Int64Builder::new();
                for row in values {
                    if let Some(v) = row.get(&field.name).and_then(Value::as_i64) {
                        b.append_value(v);
                    } else {
                        b.append_null();
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
            DataType::UInt64 => {
                let mut b = UInt64Builder::new();
                for row in values {
                    if let Some(v) = row.get(&field.name).and_then(Value::as_u64) {
                        b.append_value(v);
                    } else {
                        b.append_null();
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
            DataType::Float64 => {
                let mut b = Float64Builder::new();
                for row in values {
                    if let Some(v) = row.get(&field.name).and_then(Value::as_f64) {
                        b.append_value(v);
                    } else {
                        b.append_null();
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
            _ => {
                let mut b = StringBuilder::new();
                for row in values {
                    if let Some(v) = row.get(&field.name) {
                        if v.is_null() {
                            b.append_null();
                        } else if let Some(s) = v.as_str() {
                            b.append_value(s);
                        } else {
                            b.append_value(v.to_string());
                        }
                    } else {
                        b.append_null();
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
        }
    }
    Ok(arrays)
}

fn batch_to_json_values(batch: &RecordBatch) -> Result<Vec<Value>> {
    let schema = batch.schema();
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let mut row = serde_json::Map::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let value =
                column_value_to_json(batch.column(col_idx).as_ref(), row_idx, field.name())?;
            row.insert(field.name().clone(), value);
        }
        rows.push(Value::Object(row));
    }
    Ok(rows)
}

fn column_value_to_json(array: &dyn Array, row_idx: usize, field_name: &str) -> Result<Value> {
    if array.is_null(row_idx) {
        return Ok(Value::Null);
    }
    if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(Value::Bool(values.value(row_idx)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(Value::Number(values.value(row_idx).into()));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(Value::Number(values.value(row_idx).into()));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        return serde_json::Number::from_f64(values.value(row_idx))
            .map(Value::Number)
            .ok_or_else(|| anyhow!("column {field_name} contains non-finite float"));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Value::String(values.value(row_idx).to_string()));
    }
    bail!(
        "unsupported parquet column type for {field_name}: {:?}",
        array.data_type()
    )
}

fn min_max_i64(values: &[Value], field: &str) -> Option<(i64, i64)> {
    let mut out = None::<(i64, i64)>;
    for ts in values
        .iter()
        .filter_map(|value| value.get(field).and_then(Value::as_i64))
    {
        out = Some(out.map_or((ts, ts), |(min, max)| (min.min(ts), max.max(ts))));
    }
    out
}

/// Legacy/cache/debug JSONL table reader.
///
/// This is allowed for explicit cache/import inputs, not for production raw
/// discovery or production table materialization.
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

/// Legacy/cache/debug JSONL file reader.
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

/// Legacy/cache/debug JSONL value reader.
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
