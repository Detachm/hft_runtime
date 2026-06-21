use crate::types::*;
use anyhow::{bail, Result};
use std::collections::BTreeSet;

pub fn validate_config(config: &RecorderConfig) -> Result<()> {
    if config.dataset_format != RECORDER_CONFIG_FORMAT {
        bail!(
            "unsupported recorder config format {}, expected {}",
            config.dataset_format,
            RECORDER_CONFIG_FORMAT
        );
    }
    if config.raw_root.as_os_str().is_empty() {
        bail!("raw_root must not be empty");
    }
    if config.state_root.as_os_str().is_empty() {
        bail!("state_root must not be empty");
    }
    if config.raw_root == config.state_root {
        bail!("raw_root and state_root must be different");
    }
    if !(1..=10).contains(&config.top_n) {
        bail!("top_n must be in 1..=10");
    }
    if config.max_assets_per_cycle == 0 {
        bail!("max_assets_per_cycle must be positive");
    }
    if config.http_timeout_ms == 0 {
        bail!("http_timeout_ms must be positive");
    }
    if config.http_retry_backoff_ms == 0 {
        bail!("http_retry_backoff_ms must be positive");
    }
    if config.http_max_retries > 10 {
        bail!("http_max_retries must not exceed 10");
    }
    validate_http_url(&config.source.clob_base_url, "clob_base_url")?;
    validate_http_url(&config.source.gamma_markets_url, "gamma_markets_url")?;
    validate_discovery(&config.source.discovery)?;
    let mut seen_assets = BTreeSet::new();
    for asset in &config.source.explicit_assets {
        if asset.symbol.trim().is_empty()
            || asset.condition_id.trim().is_empty()
            || asset.asset_id.trim().is_empty()
            || asset.outcome.trim().is_empty()
        {
            bail!("explicit asset fields must not be empty");
        }
        if !seen_assets.insert(asset.asset_id.clone()) {
            bail!("duplicate explicit asset_id {}", asset.asset_id);
        }
    }
    Ok(())
}

fn validate_discovery(discovery: &GammaDiscoveryConfig) -> Result<()> {
    if discovery.limit == 0 {
        bail!("discovery.limit must be positive");
    }
    if discovery.order.trim().is_empty() {
        bail!("discovery.order must not be empty");
    }
    if discovery.pm5m_symbols.is_empty() {
        return Ok(());
    }
    if discovery.pm5m_intervals.is_empty() {
        bail!("pm5m_intervals must not be empty when pm5m_symbols are configured");
    }
    if discovery.pm5m_past_window_count < 0 {
        bail!("pm5m_past_window_count must not be negative");
    }
    if discovery.pm5m_future_window_count <= 0 {
        bail!("pm5m_future_window_count must be positive");
    }
    for symbol in &discovery.pm5m_symbols {
        let trimmed = symbol.trim();
        if trimmed.is_empty() || !trimmed.chars().all(|ch| ch.is_ascii_alphanumeric()) {
            bail!("pm5m_symbols must contain non-empty ASCII alphanumeric symbols");
        }
    }
    for interval in &discovery.pm5m_intervals {
        let normalized = interval.trim().to_ascii_lowercase();
        if !matches!(normalized.as_str(), "5m" | "15m" | "1h") {
            bail!("unsupported pm5m interval {interval}; expected one of 5m, 15m, 1h");
        }
    }
    Ok(())
}

fn validate_http_url(url: &str, field: &str) -> Result<()> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        bail!("{field} must be an http/https URL");
    }
    Ok(())
}
