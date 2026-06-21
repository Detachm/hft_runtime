use crate::discovery::Pm5mMarket;
use crate::http::HttpFetchOutcome;
use crate::types::*;
use anyhow::{Context, Result};
use market_data_etl_core::{now_unix_ns, raw_record_hash, sha256_bytes};
use serde_json::Value;

pub(crate) fn push_http_evidence(
    evidence: &mut Vec<RecorderEvidenceEvent>,
    kind: RecorderEvidenceKind,
    outcome: &HttpFetchOutcome,
    asset: Option<&AssetSpec>,
    market: Option<&Pm5mMarket>,
    skip_reason: Option<String>,
) -> Result<()> {
    let evidence_seq = evidence.len() as u64;
    let receive_monotonic_ns = now_unix_ns() as u64;
    let mut event = RecorderEvidenceEvent {
        schema_version: 1,
        dataset_format: RECORDER_EVIDENCE_FORMAT.to_string(),
        event_kind: kind,
        evidence_seq,
        source_id: RECORDER_EVIDENCE_SOURCE.to_string(),
        source_identity: source_identity_for_url(&outcome.url),
        request_url: outcome.url.clone(),
        request_start_ts_ns: outcome.request_start_ts_ns,
        request_end_ts_ns: outcome.request_end_ts_ns,
        http_status: outcome.status_code,
        response_hash: outcome.body_sha256.clone(),
        retry_count: outcome.retry_count(),
        failure_reason: outcome.final_error.clone(),
        raw_payload: outcome.body.clone(),
        raw_event_id: format!(
            "{}:{}:{}",
            RECORDER_EVIDENCE_SOURCE,
            evidence_kind_name(kind),
            evidence_seq
        ),
        raw_record_hash: String::new(),
        receive_monotonic_ns,
        symbol: asset.map(|asset| asset.symbol.clone()).or_else(|| {
            market
                .map(|market| market.symbol.clone())
                .filter(|symbol| !symbol.is_empty())
        }),
        condition_id: asset.map(|asset| asset.condition_id.clone()).or_else(|| {
            market
                .map(|market| market.condition_id.clone())
                .filter(|condition_id| !condition_id.is_empty())
        }),
        asset_id: asset
            .map(|asset| asset.asset_id.clone())
            .filter(|asset_id| !asset_id.is_empty()),
        outcome: asset
            .map(|asset| asset.outcome.clone())
            .filter(|outcome| !outcome.is_empty()),
        market_start_ts_ns: market.map(|market| market.window_start_ms * 1_000_000),
        market_end_ts_ns: market.map(|market| market.window_end_ms * 1_000_000),
        yes_asset_id: market.map(|market| market.yes_asset_id.clone()),
        no_asset_id: market.map(|market| market.no_asset_id.clone()),
        skip_reason,
    };
    event.raw_record_hash = raw_record_hash(&event)?;
    evidence.push(event);
    Ok(())
}

pub(crate) fn push_market_evidence(
    evidence: &mut Vec<RecorderEvidenceEvent>,
    kind: RecorderEvidenceKind,
    market: Option<&Pm5mMarket>,
    raw_market: &Value,
    skip_reason: Option<String>,
) -> Result<()> {
    let payload = serde_json::to_vec(raw_market).context("serialize market evidence payload")?;
    let url = "internal://gamma-market-filter";
    let outcome = HttpFetchOutcome {
        url: url.to_string(),
        request_start_ts_ns: now_unix_ns() as i64,
        request_end_ts_ns: now_unix_ns() as i64,
        status_code: None,
        body_sha256: Some(sha256_bytes(&payload)),
        body: payload,
        attempt_count: 1,
        final_error: skip_reason.clone(),
    };
    push_http_evidence(evidence, kind, &outcome, None, market, skip_reason)
}

pub(crate) fn push_pm5m_market_selected_evidence(
    evidence: &mut Vec<RecorderEvidenceEvent>,
    market: &Pm5mMarket,
) -> Result<()> {
    let payload = serde_json::to_vec(&serde_json::json!({
        "symbol": market.symbol,
        "condition_id": market.condition_id,
        "market_start_ts_ns": market.window_start_ms * 1_000_000,
        "market_end_ts_ns": market.window_end_ms * 1_000_000,
        "yes_asset_id": market.yes_asset_id,
        "no_asset_id": market.no_asset_id,
        "tick_size": market.tick_size,
        "neg_risk": market.neg_risk,
        "accepting_orders": market.accepting_orders,
        "status": market.status,
        "question": market.question,
        "slug": market.slug
    }))?;
    let outcome = HttpFetchOutcome {
        url: "internal://pm5m-market-selected".to_string(),
        request_start_ts_ns: now_unix_ns() as i64,
        request_end_ts_ns: now_unix_ns() as i64,
        status_code: None,
        body_sha256: Some(sha256_bytes(&payload)),
        body: payload,
        attempt_count: 1,
        final_error: None,
    };
    push_http_evidence(
        evidence,
        RecorderEvidenceKind::MarketSelected,
        &outcome,
        None,
        Some(market),
        None,
    )
}

fn evidence_kind_name(kind: RecorderEvidenceKind) -> &'static str {
    match kind {
        RecorderEvidenceKind::DiscoveryRequestSuccess => "discovery_request_success",
        RecorderEvidenceKind::DiscoveryRequestFailure => "discovery_request_failure",
        RecorderEvidenceKind::MarketSelected => "market_selected",
        RecorderEvidenceKind::MarketSkipped => "market_skipped",
        RecorderEvidenceKind::BookFetchSuccess => "book_fetch_success",
        RecorderEvidenceKind::BookFetchFailure => "book_fetch_failure",
        RecorderEvidenceKind::BookParseFailure => "book_parse_failure",
    }
}

fn source_identity_for_url(url: &str) -> String {
    if url.contains("clob") || url.contains("/book?") {
        "polymarket_clob_rest".to_string()
    } else if url.contains("gamma") || url.contains("/events") || url.contains("/markets") {
        "polymarket_gamma_rest".to_string()
    } else {
        "internal".to_string()
    }
}
