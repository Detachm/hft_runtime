use crate::evidence::{
    push_http_evidence, push_market_evidence, push_pm5m_market_selected_evidence,
};
use crate::http::{HttpFetchConfig, HttpFetcher};
use crate::types::*;
use crate::util::{bool_field, parse_string_array, percent_encode, string_field};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use market_data_etl_core::now_unix_ns;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn discover_assets(
    config: &RecorderConfig,
    fetcher: &dyn HttpFetcher,
    evidence: &mut Vec<RecorderEvidenceEvent>,
) -> Result<Vec<AssetSpec>> {
    if !config.source.discovery.enabled {
        return Ok(config.source.explicit_assets.clone());
    }
    if !config.source.discovery.pm5m_symbols.is_empty() {
        return discover_pm5m_assets(config, fetcher, evidence);
    }

    let discovery = &config.source.discovery;
    let mut url = format!(
        "{}?active=true&closed=false&limit={}&order={}&ascending={}",
        config.source.gamma_markets_url,
        discovery.limit,
        percent_encode(&discovery.order),
        discovery.ascending
    );
    if discovery.require_accepting_orders {
        url.push_str("&accepting_orders=true");
    }

    let outcome = fetcher.fetch(&url, HttpFetchConfig::from(config));
    push_http_evidence(
        evidence,
        if outcome.is_success() {
            RecorderEvidenceKind::DiscoveryRequestSuccess
        } else {
            RecorderEvidenceKind::DiscoveryRequestFailure
        },
        &outcome,
        None,
        None,
        None,
    )?;
    if !outcome.is_success() {
        return Err(anyhow!(
            "{}",
            outcome
                .final_error
                .clone()
                .unwrap_or_else(|| "Gamma discovery request failed".to_string())
        ));
    }
    let value: Value =
        serde_json::from_slice(&outcome.body).context("parse Gamma markets response")?;
    let markets = value
        .as_array()
        .ok_or_else(|| anyhow!("Gamma markets response must be a JSON array"))?;

    let mut assets = Vec::new();
    for market in markets {
        if let Some(skip_reason) = market_skip_reason(market, discovery) {
            push_market_evidence(
                evidence,
                RecorderEvidenceKind::MarketSkipped,
                None,
                market,
                Some(skip_reason),
            )?;
            continue;
        }
        let condition_id = string_field(market, &["conditionId", "condition_id"])
            .ok_or_else(|| anyhow!("Gamma market missing condition id"))?;
        let symbol =
            string_field(market, &["slug", "question"]).unwrap_or_else(|| condition_id.clone());
        let outcomes = parse_string_array(market.get("outcomes"));
        let token_ids = parse_string_array(market.get("clobTokenIds"));
        for (idx, asset_id) in token_ids.into_iter().enumerate() {
            let outcome = outcomes
                .get(idx)
                .cloned()
                .unwrap_or_else(|| format!("outcome-{idx}"));
            assets.push(AssetSpec {
                symbol: symbol.clone(),
                condition_id: condition_id.clone(),
                asset_id,
                outcome,
                market_start_ts_ns: None,
                market_end_ts_ns: None,
                yes_asset_id: None,
                no_asset_id: None,
                tick_size: string_field(market, &["tickSize", "tick_size"]),
                neg_risk: bool_field(market, &["negRisk", "neg_risk"]),
                accepting_orders: bool_field(market, &["acceptingOrders", "accepting_orders"]),
                status: string_field(market, &["status"]),
                question: string_field(market, &["question"]),
                slug: string_field(market, &["slug"]),
            });
        }
    }

    Ok(merge_assets(config.source.explicit_assets.clone(), assets))
}

pub(crate) fn merge_assets(first: Vec<AssetSpec>, second: Vec<AssetSpec>) -> Vec<AssetSpec> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for asset in first.into_iter().chain(second) {
        if seen.insert(asset.asset_id.clone()) {
            out.push(asset);
        }
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Pm5mMarket {
    pub(crate) symbol: String,
    pub(crate) condition_id: String,
    pub(crate) window_start_ms: i64,
    pub(crate) window_end_ms: i64,
    pub(crate) yes_asset_id: String,
    pub(crate) no_asset_id: String,
    pub(crate) tick_size: Option<String>,
    pub(crate) neg_risk: Option<bool>,
    pub(crate) accepting_orders: Option<bool>,
    pub(crate) status: Option<String>,
    pub(crate) question: Option<String>,
    pub(crate) slug: Option<String>,
}

fn discover_pm5m_assets(
    config: &RecorderConfig,
    fetcher: &dyn HttpFetcher,
    evidence: &mut Vec<RecorderEvidenceEvent>,
) -> Result<Vec<AssetSpec>> {
    let discovery = &config.source.discovery;
    let base = gamma_base_url(&config.source.gamma_markets_url);
    let current_s = (now_unix_ns() / 1_000_000_000) as i64;
    let mut urls = Vec::new();
    let intervals = pm_intervals(discovery);
    let multi_interval = intervals.len() > 1;

    for raw_symbol in &discovery.pm5m_symbols {
        let symbol = raw_symbol.to_ascii_uppercase();
        let Some(prefix) = pm_slug_prefix(&symbol) else {
            continue;
        };
        for interval in &intervals {
            let label = if multi_interval {
                format!("{}-{}", symbol, interval.label.to_ascii_uppercase())
            } else {
                symbol.clone()
            };
            let series_prefix = pm_series_slug_prefix(&symbol, &prefix, interval);
            let series_slug = format!("{series_prefix}-up-or-down-{}", interval.series_suffix);
            urls.push((
                label.clone(),
                interval.duration_s,
                format!(
                    "{base}/events?active=true&closed=false&series_slug={}&limit=8&order=end_date&ascending=true",
                    percent_encode(&series_slug)
                ),
            ));

            if let Some(direct_slug_label) = interval.direct_slug_label {
                let epoch = (current_s / interval.duration_s) * interval.duration_s;
                for offset in -discovery.pm5m_past_window_count..discovery.pm5m_future_window_count
                {
                    urls.push((
                        label.clone(),
                        interval.duration_s,
                        format!(
                            "{base}/events/slug/{prefix}-updown-{direct_slug_label}-{}",
                            epoch + offset * interval.duration_s
                        ),
                    ));
                }
            }
        }
    }

    let mut markets = BTreeMap::new();
    for (symbol, duration_s, url) in urls {
        let outcome = fetcher.fetch(&url, HttpFetchConfig::from(config));
        push_http_evidence(
            evidence,
            if outcome.is_success() {
                RecorderEvidenceKind::DiscoveryRequestSuccess
            } else {
                RecorderEvidenceKind::DiscoveryRequestFailure
            },
            &outcome,
            Some(&AssetSpec {
                symbol: symbol.clone(),
                condition_id: String::new(),
                asset_id: String::new(),
                outcome: String::new(),
                market_start_ts_ns: None,
                market_end_ts_ns: None,
                yes_asset_id: None,
                no_asset_id: None,
                tick_size: None,
                neg_risk: None,
                accepting_orders: None,
                status: None,
                question: None,
                slug: None,
            }),
            None,
            None,
        )?;
        if !outcome.is_success() {
            continue;
        };
        let Ok(payload) = serde_json::from_slice::<Value>(&outcome.body) else {
            let mut failure = outcome.clone();
            failure.final_error = Some("parse PM5M discovery payload".to_string());
            push_http_evidence(
                evidence,
                RecorderEvidenceKind::DiscoveryRequestFailure,
                &failure,
                Some(&AssetSpec {
                    symbol,
                    condition_id: String::new(),
                    asset_id: String::new(),
                    outcome: String::new(),
                    market_start_ts_ns: None,
                    market_end_ts_ns: None,
                    yes_asset_id: None,
                    no_asset_id: None,
                    tick_size: None,
                    neg_risk: None,
                    accepting_orders: None,
                    status: None,
                    question: None,
                    slug: None,
                }),
                None,
                None,
            )?;
            continue;
        };
        for market in parse_pm5m_gamma_payload(&payload, Some(&symbol), Some(duration_s * 1000)) {
            let current_epoch_s = (current_s / duration_s) * duration_s;
            let first_start_ms =
                (current_epoch_s - discovery.pm5m_past_window_count * duration_s) * 1000;
            let last_start_ms =
                (current_epoch_s + (discovery.pm5m_future_window_count - 1) * duration_s) * 1000;
            if market.window_start_ms >= first_start_ms && market.window_start_ms <= last_start_ms {
                push_pm5m_market_selected_evidence(evidence, &market)?;
                markets.insert(market.condition_id.clone(), market);
            }
        }
    }

    let mut assets = Vec::new();
    for market in markets.into_values() {
        assets.push(AssetSpec {
            symbol: market.symbol.clone(),
            condition_id: market.condition_id.clone(),
            asset_id: market.yes_asset_id.clone(),
            outcome: "YES".to_string(),
            market_start_ts_ns: Some(market.window_start_ms * 1_000_000),
            market_end_ts_ns: Some(market.window_end_ms * 1_000_000),
            yes_asset_id: Some(market.yes_asset_id.clone()),
            no_asset_id: Some(market.no_asset_id.clone()),
            tick_size: market.tick_size.clone(),
            neg_risk: market.neg_risk,
            accepting_orders: market.accepting_orders,
            status: market.status.clone(),
            question: market.question.clone(),
            slug: market.slug.clone(),
        });
        assets.push(AssetSpec {
            symbol: market.symbol,
            condition_id: market.condition_id,
            asset_id: market.no_asset_id.clone(),
            outcome: "NO".to_string(),
            market_start_ts_ns: Some(market.window_start_ms * 1_000_000),
            market_end_ts_ns: Some(market.window_end_ms * 1_000_000),
            yes_asset_id: Some(market.yes_asset_id),
            no_asset_id: Some(market.no_asset_id),
            tick_size: market.tick_size,
            neg_risk: market.neg_risk,
            accepting_orders: market.accepting_orders,
            status: market.status,
            question: market.question,
            slug: market.slug,
        });
    }

    let merged = merge_assets(config.source.explicit_assets.clone(), assets);
    if merged.is_empty() {
        return Err(anyhow!("no PM5M markets discovered"));
    }
    Ok(merged)
}

fn market_skip_reason(market: &Value, discovery: &GammaDiscoveryConfig) -> Option<String> {
    if bool_field(market, &["closed"]).unwrap_or(false) {
        return Some("closed".to_string());
    }
    if !bool_field(market, &["active"]).unwrap_or(true) {
        return Some("inactive".to_string());
    }
    if discovery.require_accepting_orders
        && !bool_field(market, &["acceptingOrders", "accepting_orders"]).unwrap_or(false)
    {
        return Some("not_accepting_orders".to_string());
    }
    if discovery.require_order_book
        && !bool_field(market, &["enableOrderBook", "enable_order_book"]).unwrap_or(false)
    {
        return Some("order_book_disabled".to_string());
    }
    if discovery.question_or_slug_contains_any.is_empty() {
        return None;
    }
    let haystack = format!(
        "{} {}",
        string_field(market, &["question"]).unwrap_or_default(),
        string_field(market, &["slug"]).unwrap_or_default()
    )
    .to_ascii_lowercase();
    if discovery
        .question_or_slug_contains_any
        .iter()
        .any(|needle| haystack.contains(&needle.to_ascii_lowercase()))
    {
        None
    } else {
        Some("filter_not_matched".to_string())
    }
}

fn parse_pm5m_gamma_payload(
    payload: &Value,
    symbol: Option<&str>,
    expected_duration_ms: Option<i64>,
) -> Vec<Pm5mMarket> {
    let mut markets = Vec::new();
    for event in gamma_events(payload) {
        for record in gamma_markets_from_event(event) {
            let mut merged = record.clone();
            if let Some(slug) = event.get("slug").cloned() {
                merged["event_slug"] = slug;
            }
            if let Some(title) = event.get("title").cloned() {
                merged["event_title"] = title;
            }
            if let Ok(market) = parse_pm5m_market(&merged, symbol, expected_duration_ms) {
                markets.push(market);
            }
        }
    }
    if markets.is_empty() && payload.is_object() {
        if let Ok(market) = parse_pm5m_market(payload, symbol, expected_duration_ms) {
            markets.push(market);
        }
    }
    markets
}

fn parse_pm5m_market(
    record: &Value,
    symbol: Option<&str>,
    expected_duration_ms: Option<i64>,
) -> Result<Pm5mMarket> {
    if bool_field(record, &["closed"]).unwrap_or(false) {
        return Err(anyhow!("closed PM5M market"));
    }
    if bool_field(record, &["active"]).is_some_and(|active| !active) {
        return Err(anyhow!("inactive PM5M market"));
    }
    if bool_field(record, &["acceptingOrders", "accepting_orders"])
        .is_some_and(|accepting| !accepting)
    {
        return Err(anyhow!("PM5M market not accepting orders"));
    }
    if bool_field(record, &["enableOrderBook", "enable_order_book"]).is_some_and(|enabled| !enabled)
    {
        return Err(anyhow!("PM5M market has no order book"));
    }
    let condition_id = string_field(record, &["condition_id", "conditionId"])
        .ok_or_else(|| anyhow!("missing condition id"))?;
    let outcomes = required_string_array(record.get("outcomes")).context("parse outcomes")?;
    let token_ids = required_string_array(
        record
            .get("clobTokenIds")
            .or_else(|| record.get("clob_token_ids")),
    )
    .context("parse clob token ids")?;
    if outcomes.len() != token_ids.len() {
        return Err(anyhow!("outcomes and token ids length mismatch"));
    }
    let mut assets = BTreeMap::new();
    for (outcome, token_id) in outcomes.into_iter().zip(token_ids) {
        assets.insert(normalize_pm5m_outcome(&outcome), token_id);
    }
    let yes_asset_id = assets
        .remove("YES")
        .ok_or_else(|| anyhow!("missing YES asset"))?;
    let no_asset_id = assets
        .remove("NO")
        .ok_or_else(|| anyhow!("missing NO asset"))?;
    let window_end_ms = timestamp_ms(
        record,
        &["market_end_ms", "endDate", "endDateIso", "end_date", "end"],
    )?;
    let window_start_ms = pm_window_start_ms(record, window_end_ms, expected_duration_ms);
    let duration_ms = window_end_ms - window_start_ms;
    if let Some(expected) = expected_duration_ms {
        if duration_ms != expected {
            return Err(anyhow!(
                "PM market duration mismatch: got {duration_ms}, expected {expected}"
            ));
        }
    } else if !matches!(duration_ms, 300_000 | 900_000 | 3_600_000) {
        return Err(anyhow!("not a supported PM interval market"));
    }
    let symbol = symbol
        .map(ToString::to_string)
        .or_else(|| infer_pm5m_symbol(record))
        .ok_or_else(|| anyhow!("missing PM5M symbol"))?;

    Ok(Pm5mMarket {
        symbol,
        condition_id,
        window_start_ms,
        window_end_ms,
        yes_asset_id,
        no_asset_id,
        tick_size: string_field(record, &["tickSize", "tick_size"]),
        neg_risk: bool_field(record, &["negRisk", "neg_risk"]),
        accepting_orders: bool_field(record, &["acceptingOrders", "accepting_orders"]),
        status: string_field(record, &["status"]),
        question: string_field(record, &["question", "title", "event_title"]),
        slug: string_field(record, &["slug", "event_slug", "ticker"]),
    })
}

fn gamma_events(payload: &Value) -> Vec<&Value> {
    if let Some(items) = payload.as_array() {
        return items.iter().filter(|item| item.is_object()).collect();
    }
    if let Some(items) = payload
        .get("events")
        .or_else(|| payload.get("data"))
        .and_then(Value::as_array)
    {
        return items.iter().filter(|item| item.is_object()).collect();
    }
    if payload.get("markets").is_some() {
        return vec![payload];
    }
    Vec::new()
}

fn gamma_markets_from_event(event: &Value) -> Vec<&Value> {
    if let Some(items) = event.get("markets").and_then(Value::as_array) {
        return items.iter().filter(|item| item.is_object()).collect();
    }
    if event.get("conditionId").is_some()
        && event.get("outcomes").is_some()
        && event.get("clobTokenIds").is_some()
    {
        return vec![event];
    }
    Vec::new()
}

fn required_string_array(value: Option<&Value>) -> Result<Vec<String>> {
    let Some(value) = value else {
        return Err(anyhow!("missing array"));
    };
    let parsed;
    let items = if let Some(text) = value.as_str() {
        parsed = serde_json::from_str::<Value>(text).context("parse JSON string array")?;
        parsed
            .as_array()
            .ok_or_else(|| anyhow!("string field is not an array"))?
    } else {
        value
            .as_array()
            .ok_or_else(|| anyhow!("field is not an array"))?
    };
    if items.is_empty() {
        return Err(anyhow!("array must not be empty"));
    }
    Ok(items
        .iter()
        .map(|item| {
            item.as_str()
                .map(ToString::to_string)
                .unwrap_or_else(|| item.to_string())
        })
        .collect())
}

fn timestamp_ms(record: &Value, names: &[&str]) -> Result<i64> {
    for name in names {
        let Some(value) = record.get(*name) else {
            continue;
        };
        if let Some(raw) = value.as_i64() {
            return Ok(if raw < 10_000_000_000_000 {
                raw
            } else {
                raw / 1_000_000
            });
        }
        if let Some(text) = value.as_str() {
            let text = text.trim();
            if text.is_empty() {
                continue;
            }
            if let Ok(raw) = text.parse::<i64>() {
                return Ok(if raw < 10_000_000_000_000 {
                    raw
                } else {
                    raw / 1_000_000
                });
            }
            let normalized = if let Some(stripped) = text.strip_suffix('Z') {
                format!("{stripped}+00:00")
            } else {
                text.to_string()
            };
            let dt = DateTime::parse_from_rfc3339(&normalized)
                .map(|dt| dt.with_timezone(&Utc))
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S").map(|dt| dt.and_utc())
                })
                .with_context(|| format!("parse timestamp {text}"))?;
            return Ok(dt.timestamp_millis());
        }
    }
    Err(anyhow!("missing timestamp"))
}

fn pm_window_start_ms(record: &Value, end_ms: i64, expected_duration_ms: Option<i64>) -> i64 {
    if let Some(slug) = string_field(record, &["slug", "event_slug", "ticker"]) {
        for interval in supported_pm_intervals() {
            let marker = format!("-updown-{}-", interval.label);
            if let Some((_, suffix)) = slug.rsplit_once(&marker) {
                if suffix.len() == 10 && suffix.chars().all(|ch| ch.is_ascii_digit()) {
                    if let Ok(epoch) = suffix.parse::<i64>() {
                        return epoch * 1000;
                    }
                }
            }
        }
    }
    end_ms - expected_duration_ms.unwrap_or(300_000)
}

fn normalize_pm5m_outcome(outcome: &str) -> String {
    match outcome.trim().to_ascii_uppercase().as_str() {
        "UP" => "YES".to_string(),
        "DOWN" => "NO".to_string(),
        other => other.to_string(),
    }
}

fn infer_pm5m_symbol(record: &Value) -> Option<String> {
    let text = [
        string_field(record, &["question"]),
        string_field(record, &["title"]),
        string_field(record, &["event_title"]),
        string_field(record, &["slug"]),
        string_field(record, &["event_slug"]),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(" ")
    .to_ascii_lowercase();
    if contains_wordish(&text, &["bitcoin", "btc"]) {
        return Some("BTC".to_string());
    }
    if contains_wordish(&text, &["ethereum", "ether", "eth"]) {
        return Some("ETH".to_string());
    }
    if contains_wordish(&text, &["solana", "sol"]) {
        return Some("SOL".to_string());
    }
    None
}

fn contains_wordish(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| {
        text.split(|ch: char| !ch.is_ascii_alphanumeric())
            .any(|word| word == *needle)
    })
}

fn pm_slug_prefix(symbol: &str) -> Option<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() || !trimmed.chars().all(|ch| ch.is_ascii_alphanumeric()) {
        return None;
    }
    Some(trimmed.to_ascii_lowercase())
}

#[derive(Debug, Clone, Copy)]
struct PmInterval {
    label: &'static str,
    series_suffix: &'static str,
    direct_slug_label: Option<&'static str>,
    duration_s: i64,
}

fn pm_series_slug_prefix(symbol: &str, default_prefix: &str, interval: &PmInterval) -> String {
    if interval.label == "1h" && symbol == "SOL" {
        "solana".to_string()
    } else {
        default_prefix.to_string()
    }
}

fn pm_intervals(discovery: &GammaDiscoveryConfig) -> Vec<PmInterval> {
    discovery
        .pm5m_intervals
        .iter()
        .filter_map(|item| pm_interval(item))
        .collect::<Vec<_>>()
}

fn supported_pm_intervals() -> [PmInterval; 3] {
    [
        PmInterval {
            label: "5m",
            series_suffix: "5m",
            direct_slug_label: Some("5m"),
            duration_s: 300,
        },
        PmInterval {
            label: "15m",
            series_suffix: "15m",
            direct_slug_label: Some("15m"),
            duration_s: 900,
        },
        PmInterval {
            label: "1h",
            series_suffix: "hourly",
            direct_slug_label: None,
            duration_s: 3_600,
        },
    ]
}

fn pm_interval(value: &str) -> Option<PmInterval> {
    let normalized = value.trim().to_ascii_lowercase();
    supported_pm_intervals()
        .into_iter()
        .find(|interval| interval.label == normalized)
}

fn gamma_base_url(gamma_markets_url: &str) -> String {
    let trimmed = gamma_markets_url.trim_end_matches('/');
    trimmed
        .strip_suffix("/markets")
        .unwrap_or(trimmed)
        .to_string()
}
