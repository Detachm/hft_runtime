use crate::list_files_recursive;
use anyhow::{Context, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

pub fn sha256_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex::encode(digest)
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    Ok(sha256_bytes(&bytes))
}

pub fn hash_serializable<T: Serialize>(value: &T) -> Result<String> {
    let bytes = serde_json::to_vec(value).context("serialize value for hash")?;
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
        hasher.update(fs::read(&file).with_context(|| format!("read {}", file.display()))?);
        hasher.update([0]);
    }
    Ok(hex::encode(hasher.finalize()))
}
