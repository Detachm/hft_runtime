use crate::list_files_recursive;
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{BufReader, Read};
use std::path::Path;

pub fn sha256_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex::encode(digest)
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

pub fn hash_serializable<T: Serialize>(value: &T) -> Result<String> {
    let bytes = serde_json::to_vec(value).context("serialize value for hash")?;
    Ok(sha256_bytes(&bytes))
}

pub fn raw_record_hash<T: Serialize>(value: &T) -> Result<String> {
    let mut value = serde_json::to_value(value).context("serialize raw row for hash")?;
    if let Value::Object(object) = &mut value {
        if object.contains_key("raw_record_hash") {
            object.insert("raw_record_hash".to_string(), Value::String(String::new()));
        }
    }
    let bytes = serde_json::to_vec(&value).context("serialize normalized raw row for hash")?;
    Ok(sha256_bytes(&bytes))
}

pub fn hash_path(path: &Path) -> Result<String> {
    if path.is_file() {
        return sha256_file(path);
    }

    let mut hasher = Sha256::new();
    for file in list_files_recursive(path)? {
        let rel = file
            .strip_prefix(path)
            .with_context(|| format!("strip prefix {} from {}", path.display(), file.display()))?;
        hasher.update(rel.to_string_lossy().as_bytes());
        hasher.update([0]);
        let mut reader = BufReader::with_capacity(
            1024 * 1024,
            fs::File::open(&file).with_context(|| format!("open {}", file.display()))?,
        );
        let mut buffer = vec![0u8; 1024 * 1024];
        loop {
            let read = reader
                .read(&mut buffer)
                .with_context(|| format!("read {}", file.display()))?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        hasher.update([0]);
    }
    Ok(hex::encode(hasher.finalize()))
}
