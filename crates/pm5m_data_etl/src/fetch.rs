use anyhow::{anyhow, Context, Result};
use std::fs;
use std::path::PathBuf;

pub trait Fetcher {
    fn fetch(&self, url: &str) -> Result<Vec<u8>>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultFetcher;

impl Fetcher for DefaultFetcher {
    fn fetch(&self, url: &str) -> Result<Vec<u8>> {
        if let Some(path) = url.strip_prefix("file://") {
            return fs::read(PathBuf::from(path))
                .with_context(|| format!("read file URL source {}", url));
        }
        if url.starts_with("http://") || url.starts_with("https://") {
            let response = reqwest::blocking::get(url)
                .with_context(|| format!("GET {}", url))?
                .error_for_status()
                .with_context(|| format!("HTTP status for {}", url))?;
            return Ok(response.bytes().context("read response bytes")?.to_vec());
        }
        if !url.is_empty() {
            return fs::read(PathBuf::from(url)).with_context(|| format!("read source {}", url));
        }
        Err(anyhow!("empty source URL"))
    }
}
