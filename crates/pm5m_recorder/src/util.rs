use crate::types::{RecorderHealthEvent, RECORDER_HEALTH_STREAM};
use anyhow::Result;
use serde_json::Value;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

pub(crate) fn string_field(value: &Value, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        value.get(*name).and_then(|field| match field {
            Value::String(text) => Some(text.clone()),
            Value::Number(num) => Some(num.to_string()),
            _ => None,
        })
    })
}

pub(crate) fn bool_field(value: &Value, names: &[&str]) -> Option<bool> {
    names.iter().find_map(|name| {
        value.get(*name).and_then(|field| match field {
            Value::Bool(flag) => Some(*flag),
            Value::String(text) => text.parse::<bool>().ok(),
            _ => None,
        })
    })
}

pub(crate) fn parse_string_array(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(ToString::to_string))
            .collect(),
        Some(Value::String(text)) => serde_json::from_str::<Vec<String>>(text).unwrap_or_default(),
        _ => Vec::new(),
    }
}

pub(crate) fn percent_encode(input: &str) -> String {
    let mut out = String::new();
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

pub(crate) fn append_recorder_health(state_root: &Path, event: &RecorderHealthEvent) -> Result<()> {
    fs::create_dir_all(state_root)?;
    let path = state_root.join(RECORDER_HEALTH_STREAM);
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, event)?;
    file.write_all(b"\n")?;
    Ok(())
}

pub(crate) fn last_recv_age_ms(now_ns: i64, last_recv_ts_ns: Option<i64>) -> Option<i64> {
    last_recv_ts_ns.map(|ts| now_ns.saturating_sub(ts).max(0) / 1_000_000)
}
