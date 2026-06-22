use crate::types::*;
use crate::util::{
    append_recorder_health, command_line, current_binary_sha256, current_git_sha, current_host,
    last_recv_age_ms,
};
use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use market_data_etl_core::{
    atomic_write_verified, hash_path, now_unix_ns, raw_record_hash, sha256_bytes,
    write_hftrec4_segment, Hftrec4WriteRecord,
};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub const DEFAULT_REFERENCE_WS_CHANNEL_CAPACITY: usize = 8_192;
pub const DEFAULT_REFERENCE_WS_FLUSH_INTERVAL_MS: u64 = 5_000;
pub const DEFAULT_REFERENCE_WS_FLUSH_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_REFERENCE_WS_FLUSH_ROWS: usize = 50_000;
pub const DEFAULT_BINANCE_REFERENCE_WS_URL: &str = "wss://stream.binance.com:9443/stream";
pub const DEFAULT_OKX_REFERENCE_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/business";

#[derive(Debug, Clone)]
pub struct ReferenceWsRecorderOptions {
    pub raw_root: PathBuf,
    pub state_root: PathBuf,
    pub audit_profile_hash: Option<String>,
    pub venues: Vec<String>,
    pub symbols: Vec<String>,
    pub binance_ws_url: String,
    pub okx_ws_url: String,
    pub channel_capacity: usize,
    pub flush_policy: ReferenceWsFlushPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReferenceWsFlushPolicy {
    pub interval: Duration,
    pub max_payload_bytes: usize,
    pub max_rows: usize,
}

impl ReferenceWsFlushPolicy {
    pub fn default_live() -> Self {
        Self {
            interval: Duration::from_millis(DEFAULT_REFERENCE_WS_FLUSH_INTERVAL_MS),
            max_payload_bytes: DEFAULT_REFERENCE_WS_FLUSH_BYTES,
            max_rows: DEFAULT_REFERENCE_WS_FLUSH_ROWS,
        }
    }
}

#[derive(Debug)]
enum ReferenceWriterCommand {
    Row(RawReferenceWsEvent),
    Error(String),
}

#[derive(Debug, Default)]
struct ReferenceOverrunTracker {
    pending_count: u64,
    pending_first_ts_ns: Option<i64>,
    pending_last_ts_ns: Option<i64>,
    total_count: u64,
    last_queue_depth: u64,
}

impl ReferenceOverrunTracker {
    fn record_drop(&mut self, local_ts_ns: i64, queue_depth: u64) {
        if self.pending_count == 0 {
            self.pending_first_ts_ns = Some(local_ts_ns);
        }
        self.pending_count = self.pending_count.saturating_add(1);
        self.pending_last_ts_ns = Some(local_ts_ns);
        self.total_count = self.total_count.saturating_add(1);
        self.last_queue_depth = queue_depth;
    }

    fn clear_pending(&mut self) {
        self.pending_count = 0;
        self.pending_first_ts_ns = None;
        self.pending_last_ts_ns = None;
    }
}

pub async fn run_reference_ws_forever(options: ReferenceWsRecorderOptions) -> Result<()> {
    validate_reference_options(&options)?;
    fs::create_dir_all(&options.raw_root)
        .with_context(|| format!("create reference raw root {}", options.raw_root.display()))?;
    fs::create_dir_all(&options.state_root).with_context(|| {
        format!(
            "create reference state root {}",
            options.state_root.display()
        )
    })?;
    write_reference_run_manifest(&options)?;

    let state = read_reference_ws_state(&options.state_root)?;
    let (tx, rx) = mpsc::channel(options.channel_capacity);
    let writer_options = options.clone();
    let writer =
        tokio::spawn(async move { reference_writer_loop(writer_options, state, rx).await });

    let mut tasks = Vec::new();
    for venue in &options.venues {
        match venue.as_str() {
            "binance" => tasks.push(tokio::spawn(binance_reference_loop(
                options.clone(),
                tx.clone(),
            ))),
            "okx" => tasks.push(tokio::spawn(okx_reference_loop(
                options.clone(),
                tx.clone(),
            ))),
            other => bail!("unknown reference venue {other}"),
        }
    }
    drop(tx);

    for task in tasks {
        task.await.context("join reference recorder task")??;
    }
    writer.await.context("join reference writer task")??;
    Ok(())
}

async fn binance_reference_loop(
    options: ReferenceWsRecorderOptions,
    tx: mpsc::Sender<ReferenceWriterCommand>,
) -> Result<()> {
    let streams = options
        .symbols
        .iter()
        .map(|symbol| format!("{}usdt@kline_1s", symbol.to_ascii_lowercase()))
        .collect::<Vec<_>>()
        .join("/");
    let url = format!("{}?streams={streams}", options.binance_ws_url);
    let mut overrun = ReferenceOverrunTracker::default();
    loop {
        match connect_async(&url).await {
            Ok((mut ws, _)) => {
                let _ = send_control(
                    &tx,
                    "binance",
                    "connect",
                    None,
                    "connect",
                    json!({"url": &url, "streams": &streams}),
                )
                .await;
                let mut wrote_gap_control = false;
                while let Some(message) = ws.next().await {
                    let local_recv_ts_ns = now_unix_ns() as i64;
                    match message {
                        Ok(Message::Text(text)) => {
                            for row in binance_rows_from_text(&text, local_recv_ts_ns)? {
                                enqueue_reference_row_nonblocking(
                                    &tx,
                                    row,
                                    &mut overrun,
                                    "binance",
                                    None,
                                )?;
                            }
                        }
                        Ok(Message::Ping(bytes)) => {
                            ws.send(Message::Pong(bytes)).await?;
                        }
                        Ok(Message::Close(frame)) => {
                            let _ = send_control(
                                &tx,
                                "binance",
                                "disconnect",
                                None,
                                "close_frame",
                                json!({"close_frame": format!("{frame:?}")}),
                            )
                            .await;
                            let _ = send_control(
                                &tx,
                                "binance",
                                "gap_suspected",
                                None,
                                "coverage_gap_start",
                                json!({"reason": "close_frame", "close_frame": format!("{frame:?}")}),
                            )
                            .await;
                            wrote_gap_control = true;
                            break;
                        }
                        Ok(_) => {}
                        Err(error) => {
                            let _ = tx
                                .send(ReferenceWriterCommand::Error(format!(
                                    "binance ws error: {error}"
                                )))
                                .await;
                            let _ = send_control(
                                &tx,
                                "binance",
                                "disconnect",
                                None,
                                "ws_error",
                                json!({"error": error.to_string()}),
                            )
                            .await;
                            let _ = send_control(
                                &tx,
                                "binance",
                                "gap_suspected",
                                None,
                                "coverage_gap_start",
                                json!({"reason": "ws_error", "error": error.to_string()}),
                            )
                            .await;
                            wrote_gap_control = true;
                            break;
                        }
                    }
                }
                if !wrote_gap_control {
                    let _ = send_control(
                        &tx,
                        "binance",
                        "disconnect",
                        None,
                        "stream_ended",
                        json!({"reason": "stream_ended"}),
                    )
                    .await;
                    let _ = send_control(
                        &tx,
                        "binance",
                        "gap_suspected",
                        None,
                        "coverage_gap_start",
                        json!({"reason": "stream_ended"}),
                    )
                    .await;
                }
            }
            Err(error) => {
                let _ = tx
                    .send(ReferenceWriterCommand::Error(format!(
                        "binance connect error: {error}"
                    )))
                    .await;
                let _ = send_control(
                    &tx,
                    "binance",
                    "disconnect",
                    None,
                    "connect_error",
                    json!({"error": error.to_string(), "url": &url}),
                )
                .await;
                let _ = send_control(
                    &tx,
                    "binance",
                    "gap_suspected",
                    None,
                    "coverage_gap_start",
                    json!({"reason": "connect_error", "error": error.to_string()}),
                )
                .await;
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        let _ = send_control(
            &tx,
            "binance",
            "reconnect",
            None,
            "reconnect_after_backoff",
            json!({"sleep_ms": 2_000}),
        )
        .await;
    }
}

async fn okx_reference_loop(
    options: ReferenceWsRecorderOptions,
    tx: mpsc::Sender<ReferenceWriterCommand>,
) -> Result<()> {
    let args = options
        .symbols
        .iter()
        .map(|symbol| json!({"channel": "candle1s", "instId": format!("{symbol}-USDT")}))
        .collect::<Vec<_>>();
    let subscribe = json!({"op": "subscribe", "args": args}).to_string();
    let mut overrun = ReferenceOverrunTracker::default();
    loop {
        match connect_async(&options.okx_ws_url).await {
            Ok((mut ws, _)) => {
                let _ = send_control(
                    &tx,
                    "okx",
                    "connect",
                    None,
                    "connect",
                    json!({"url": &options.okx_ws_url}),
                )
                .await;
                ws.send(Message::Text(subscribe.clone()))
                    .await
                    .context("send OKX subscribe")?;
                let _ = send_control(
                    &tx,
                    "okx",
                    "subscribe",
                    None,
                    "initial_subscribe",
                    json!({
                        "payload_sha256": sha256_bytes(subscribe.as_bytes()),
                        "payload": &subscribe,
                    }),
                )
                .await;
                let mut wrote_gap_control = false;
                while let Some(message) = ws.next().await {
                    let local_recv_ts_ns = now_unix_ns() as i64;
                    match message {
                        Ok(Message::Text(text)) => {
                            for row in okx_rows_from_text(&text, local_recv_ts_ns)? {
                                enqueue_reference_row_nonblocking(
                                    &tx,
                                    row,
                                    &mut overrun,
                                    "okx",
                                    None,
                                )?;
                            }
                        }
                        Ok(Message::Ping(bytes)) => {
                            ws.send(Message::Pong(bytes)).await?;
                        }
                        Ok(Message::Close(frame)) => {
                            let _ = send_control(
                                &tx,
                                "okx",
                                "disconnect",
                                None,
                                "close_frame",
                                json!({"close_frame": format!("{frame:?}")}),
                            )
                            .await;
                            let _ = send_control(
                                &tx,
                                "okx",
                                "gap_suspected",
                                None,
                                "coverage_gap_start",
                                json!({"reason": "close_frame", "close_frame": format!("{frame:?}")}),
                            )
                            .await;
                            wrote_gap_control = true;
                            break;
                        }
                        Ok(_) => {}
                        Err(error) => {
                            let _ = tx
                                .send(ReferenceWriterCommand::Error(format!(
                                    "okx ws error: {error}"
                                )))
                                .await;
                            let _ = send_control(
                                &tx,
                                "okx",
                                "disconnect",
                                None,
                                "ws_error",
                                json!({"error": error.to_string()}),
                            )
                            .await;
                            let _ = send_control(
                                &tx,
                                "okx",
                                "gap_suspected",
                                None,
                                "coverage_gap_start",
                                json!({"reason": "ws_error", "error": error.to_string()}),
                            )
                            .await;
                            wrote_gap_control = true;
                            break;
                        }
                    }
                }
                if !wrote_gap_control {
                    let _ = send_control(
                        &tx,
                        "okx",
                        "disconnect",
                        None,
                        "stream_ended",
                        json!({"reason": "stream_ended"}),
                    )
                    .await;
                    let _ = send_control(
                        &tx,
                        "okx",
                        "gap_suspected",
                        None,
                        "coverage_gap_start",
                        json!({"reason": "stream_ended"}),
                    )
                    .await;
                }
            }
            Err(error) => {
                let _ = tx
                    .send(ReferenceWriterCommand::Error(format!(
                        "okx connect error: {error}"
                    )))
                    .await;
                let _ = send_control(
                    &tx,
                    "okx",
                    "disconnect",
                    None,
                    "connect_error",
                    json!({"error": error.to_string(), "url": &options.okx_ws_url}),
                )
                .await;
                let _ = send_control(
                    &tx,
                    "okx",
                    "gap_suspected",
                    None,
                    "coverage_gap_start",
                    json!({"reason": "connect_error", "error": error.to_string()}),
                )
                .await;
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        let _ = send_control(
            &tx,
            "okx",
            "reconnect",
            None,
            "reconnect_after_backoff",
            json!({"sleep_ms": 2_000}),
        )
        .await;
    }
}

async fn send_control(
    tx: &mpsc::Sender<ReferenceWriterCommand>,
    venue: &str,
    event_type: &str,
    symbol: Option<&str>,
    operation: &str,
    details: Value,
) -> Result<()> {
    let row = reference_control_row(
        tx,
        venue,
        event_type,
        symbol,
        operation,
        details,
        now_unix_ns() as i64,
    )?;
    tx.send(ReferenceWriterCommand::Row(row))
        .await
        .map_err(|_| anyhow!("reference writer channel closed"))
}

fn reference_control_row(
    tx: &mpsc::Sender<ReferenceWriterCommand>,
    venue: &str,
    event_type: &str,
    symbol: Option<&str>,
    operation: &str,
    details: Value,
    local_recv_ts_ns: i64,
) -> Result<RawReferenceWsEvent> {
    let raw_payload = serde_json::to_vec(&json!({
        "schema_version": 1,
        "event_type": event_type,
        "operation": operation,
        "component": "reference_ws",
        "venue": venue,
        "symbol": symbol.unwrap_or(""),
        "queue_depth": reference_queue_depth(tx),
        "channel_capacity": tx.max_capacity(),
        "details": details,
    }))?;
    let mut row = RawReferenceWsEvent {
        source_id: format!("{venue}_1s_ws"),
        venue: venue.to_string(),
        symbol: symbol.unwrap_or("").to_string(),
        ingest_seq_scope: "pm5m_reference_ws".to_string(),
        ingest_seq: 0,
        local_recv_ts_ns,
        connection_epoch: 0,
        event_type: event_type.to_string(),
        exchange_event_ts_ms: None,
        bar_open_time_ms: None,
        bar_close_time_ms: None,
        is_closed: None,
        open: None,
        high: None,
        low: None,
        close: None,
        volume: None,
        raw_payload_sha256: sha256_bytes(&raw_payload),
        raw_payload,
        raw_record_hash: String::new(),
    };
    row.raw_record_hash = raw_record_hash(&row)?;
    Ok(row)
}

fn enqueue_reference_row_nonblocking(
    tx: &mpsc::Sender<ReferenceWriterCommand>,
    row: RawReferenceWsEvent,
    overrun: &mut ReferenceOverrunTracker,
    venue: &str,
    symbol: Option<&str>,
) -> Result<()> {
    try_flush_reference_overrun_marker(tx, overrun, venue, symbol)?;
    match tx.try_send(ReferenceWriterCommand::Row(row)) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(ReferenceWriterCommand::Row(row))) => {
            overrun.record_drop(row.local_recv_ts_ns, reference_queue_depth(tx));
            Ok(())
        }
        Err(TrySendError::Full(_)) => {
            overrun.record_drop(now_unix_ns() as i64, reference_queue_depth(tx));
            Ok(())
        }
        Err(TrySendError::Closed(_)) => Err(anyhow!("reference writer channel closed")),
    }
}

fn try_flush_reference_overrun_marker(
    tx: &mpsc::Sender<ReferenceWriterCommand>,
    overrun: &mut ReferenceOverrunTracker,
    venue: &str,
    symbol: Option<&str>,
) -> Result<()> {
    if overrun.pending_count == 0 || tx.capacity() == 0 {
        return Ok(());
    }
    let row = reference_control_row(
        tx,
        venue,
        "local_overrun",
        symbol,
        "reader_queue_full",
        json!({
            "dropped_row_count": overrun.pending_count,
            "pending_first_ts_ns": overrun.pending_first_ts_ns,
            "pending_last_ts_ns": overrun.pending_last_ts_ns,
            "total_dropped_row_count": overrun.total_count,
            "last_queue_depth": overrun.last_queue_depth,
        }),
        now_unix_ns() as i64,
    )?;
    match tx.try_send(ReferenceWriterCommand::Row(row)) {
        Ok(()) => {
            overrun.clear_pending();
            Ok(())
        }
        Err(TrySendError::Full(_)) => Ok(()),
        Err(TrySendError::Closed(_)) => Err(anyhow!("reference writer channel closed")),
    }
}

fn reference_queue_depth(tx: &mpsc::Sender<ReferenceWriterCommand>) -> u64 {
    tx.max_capacity().saturating_sub(tx.capacity()) as u64
}

fn binance_rows_from_text(text: &str, local_recv_ts_ns: i64) -> Result<Vec<RawReferenceWsEvent>> {
    let value: Value = serde_json::from_str(text).context("parse Binance reference WS payload")?;
    let data = value.get("data").unwrap_or(&value);
    let Some(kline) = data.get("k") else {
        return Ok(Vec::new());
    };
    let symbol = data
        .get("s")
        .and_then(Value::as_str)
        .or_else(|| kline.get("s").and_then(Value::as_str))
        .map(reference_symbol)
        .unwrap_or_default();
    let raw_payload = serde_json::to_vec(&value)?;
    Ok(vec![reference_row(
        "binance",
        &symbol,
        "reference_bar",
        local_recv_ts_ns,
        data.get("E").and_then(value_to_i64),
        kline.get("t").and_then(value_to_i64),
        kline.get("T").and_then(value_to_i64),
        kline.get("x").and_then(Value::as_bool),
        kline.get("o").and_then(value_to_string),
        kline.get("h").and_then(value_to_string),
        kline.get("l").and_then(value_to_string),
        kline.get("c").and_then(value_to_string),
        kline.get("v").and_then(value_to_string),
        raw_payload,
    )?])
}

fn okx_rows_from_text(text: &str, local_recv_ts_ns: i64) -> Result<Vec<RawReferenceWsEvent>> {
    let value: Value = serde_json::from_str(text).context("parse OKX reference WS payload")?;
    let Some(arg) = value.get("arg") else {
        return Ok(Vec::new());
    };
    let symbol = arg
        .get("instId")
        .and_then(Value::as_str)
        .map(reference_symbol)
        .unwrap_or_default();
    let mut out = Vec::new();
    for item in value
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(values) = item.as_array() else {
            continue;
        };
        let bar_open_time_ms = values.first().and_then(value_to_i64);
        let raw_payload = serde_json::to_vec(&json!({"arg": arg, "data": item}))?;
        out.push(reference_row(
            "okx",
            &symbol,
            "reference_bar",
            local_recv_ts_ns,
            bar_open_time_ms,
            bar_open_time_ms,
            bar_open_time_ms.map(|ts| ts + 999),
            values.get(8).and_then(value_to_string).map(|v| v == "1"),
            values.get(1).and_then(value_to_string),
            values.get(2).and_then(value_to_string),
            values.get(3).and_then(value_to_string),
            values.get(4).and_then(value_to_string),
            values.get(5).and_then(value_to_string),
            raw_payload,
        )?);
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn reference_row(
    venue: &str,
    symbol: &str,
    event_type: &str,
    local_recv_ts_ns: i64,
    exchange_event_ts_ms: Option<i64>,
    bar_open_time_ms: Option<i64>,
    bar_close_time_ms: Option<i64>,
    is_closed: Option<bool>,
    open: Option<String>,
    high: Option<String>,
    low: Option<String>,
    close: Option<String>,
    volume: Option<String>,
    raw_payload: Vec<u8>,
) -> Result<RawReferenceWsEvent> {
    let mut row = RawReferenceWsEvent {
        source_id: format!("{venue}_1s_ws"),
        venue: venue.to_string(),
        symbol: symbol.to_string(),
        ingest_seq_scope: "pm5m_reference_ws".to_string(),
        ingest_seq: 0,
        local_recv_ts_ns,
        connection_epoch: 0,
        event_type: event_type.to_string(),
        exchange_event_ts_ms,
        bar_open_time_ms,
        bar_close_time_ms,
        is_closed,
        open,
        high,
        low,
        close,
        volume,
        raw_payload_sha256: sha256_bytes(&raw_payload),
        raw_payload,
        raw_record_hash: String::new(),
    };
    row.raw_record_hash = raw_record_hash(&row)?;
    Ok(row)
}

async fn reference_writer_loop(
    options: ReferenceWsRecorderOptions,
    mut state: ReferenceWsRecorderState,
    mut rx: mpsc::Receiver<ReferenceWriterCommand>,
) -> Result<()> {
    let mut rows = Vec::new();
    let mut payload_bytes = 0usize;
    let mut segment_started_at = Instant::now();
    let mut interval = tokio::time::interval(options.flush_policy.interval);
    interval.tick().await;
    loop {
        tokio::select! {
            command = rx.recv() => {
                match command {
                        Some(ReferenceWriterCommand::Row(mut row)) => {
                            if row.event_type == "connect" {
                                state.connection_epoch = state.connection_epoch.saturating_add(1);
                                state.last_error = None;
                            }
                        apply_reference_local_overrun_row(&mut state, &row);
                        state.next_ingest_seq = state.next_ingest_seq.saturating_add(1);
                        row.ingest_seq = state.next_ingest_seq;
                        row.connection_epoch = state.connection_epoch;
                        row.raw_record_hash = raw_record_hash(&row)?;
                        payload_bytes = payload_bytes.saturating_add(row.raw_payload.len());
                        state.last_recv_ts_ns = Some(state.last_recv_ts_ns.map_or(row.local_recv_ts_ns, |ts| ts.max(row.local_recv_ts_ns)));
                        rows.push(row);
                        if should_flush_reference_segment(
                            rows.len(),
                            payload_bytes,
                            segment_started_at.elapsed(),
                            &options.flush_policy,
                        ) {
                            flush_reference_rows(
                                &options,
                                &mut state,
                                &mut rows,
                                &mut payload_bytes,
                                &mut segment_started_at,
                                rx.len() as u64,
                            )?;
                        }
                    }
                        Some(ReferenceWriterCommand::Error(error)) => {
                            state.last_error = Some(error);
                            write_reference_ws_state(&options.state_root, &state)?;
                            append_reference_health(
                                &options,
                                &state,
                                "recorder_error",
                                rows.len() as u64,
                                payload_bytes as u64,
                                None,
                                rx.len() as u64,
                                rows.len() as u64,
                                state.last_error.clone(),
                            )?;
                        }
                    None => break,
                }
            }
                _ = interval.tick() => {
                    if should_flush_reference_segment(
                    rows.len(),
                    payload_bytes,
                    segment_started_at.elapsed(),
                    &options.flush_policy,
                ) {
                    flush_reference_rows(
                        &options,
                        &mut state,
                        &mut rows,
                        &mut payload_bytes,
                            &mut segment_started_at,
                            rx.len() as u64,
                        )?;
                    } else {
                        append_reference_health(
                            &options,
                            &state,
                            "heartbeat",
                            rows.len() as u64,
                            payload_bytes as u64,
                            None,
                            rx.len() as u64,
                            rows.len() as u64,
                            None,
                        )?;
                    }
                }
        }
    }
    flush_reference_rows(
        &options,
        &mut state,
        &mut rows,
        &mut payload_bytes,
        &mut segment_started_at,
        rx.len() as u64,
    )?;
    Ok(())
}

fn should_flush_reference_segment(
    row_count: usize,
    payload_bytes: usize,
    elapsed: Duration,
    policy: &ReferenceWsFlushPolicy,
) -> bool {
    row_count > 0
        && (elapsed >= policy.interval
            || payload_bytes >= policy.max_payload_bytes
            || row_count >= policy.max_rows)
}

fn flush_reference_rows(
    options: &ReferenceWsRecorderOptions,
    state: &mut ReferenceWsRecorderState,
    rows: &mut Vec<RawReferenceWsEvent>,
    payload_bytes: &mut usize,
    segment_started_at: &mut Instant,
    queue_depth: u64,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let segment_start_ts_ns = rows
        .first()
        .map(|row| row.local_recv_ts_ns)
        .unwrap_or_else(|| now_unix_ns() as i64);
    let output_path = reference_output_path(&options.raw_root, segment_start_ts_ns);
    let started = Instant::now();
    write_hftrec4_segment(&output_path, &reference_hftrec4_rows(rows)?)?;
    state.last_segment_path = Some(output_path.clone());
    state.last_segment_hash = Some(hash_path(&output_path)?);
    write_reference_ws_state(&options.state_root, state)?;
    eprintln!(
        "pm5m-recorder reference flush rows={} bytes={} elapsed_ms={} path={}",
        rows.len(),
        *payload_bytes,
        started.elapsed().as_millis(),
        output_path.display()
    );
    append_reference_health(
        options,
        state,
        "flush",
        rows.len() as u64,
        *payload_bytes as u64,
        Some(started.elapsed().as_millis() as u64),
        queue_depth,
        rows.len() as u64,
        None,
    )?;
    rows.clear();
    *payload_bytes = 0;
    *segment_started_at = Instant::now();
    Ok(())
}

fn append_reference_health(
    options: &ReferenceWsRecorderOptions,
    state: &ReferenceWsRecorderState,
    event_type: &str,
    rows: u64,
    payload_bytes: u64,
    flush_elapsed_ms: Option<u64>,
    queue_depth: u64,
    writer_buffer_rows: u64,
    error: Option<String>,
) -> Result<()> {
    let now_ns = now_unix_ns() as i64;
    let health_status = if error.is_some() || state.last_error.is_some() {
        "degraded"
    } else {
        "ok"
    };
    append_recorder_health(
        &options.state_root,
        &RecorderHealthEvent {
            schema_version: 1,
            dataset_format: "pm5m_recorder_health.v1".to_string(),
            component: "reference_ws".to_string(),
            event_type: event_type.to_string(),
            health_status: health_status.to_string(),
            local_ts_ns: now_ns,
            rows,
            payload_bytes,
            segment_path: state.last_segment_path.clone(),
            segment_hash: state.last_segment_hash.clone(),
            flush_elapsed_ms,
            channel_capacity: Some(options.channel_capacity as u64),
            connection_id: None,
            connection_epoch: Some(state.connection_epoch),
            subscription_epoch: None,
            current_asset_count: None,
            last_recv_ts_ns: state.last_recv_ts_ns,
            last_recv_age_ms: last_recv_age_ms(now_ns, state.last_recv_ts_ns),
            queue_depth: Some(queue_depth),
            writer_buffer_rows: Some(writer_buffer_rows),
            local_overrun_count: Some(state.local_overrun_count),
            local_overrun_last_ts_ns: state.local_overrun_last_ts_ns,
            error: error.or_else(|| state.last_error.clone()),
        },
    )
}

fn apply_reference_local_overrun_row(
    state: &mut ReferenceWsRecorderState,
    row: &RawReferenceWsEvent,
) {
    if row.event_type != "local_overrun" {
        return;
    }
    let dropped = serde_json::from_slice::<Value>(&row.raw_payload)
        .ok()
        .and_then(|value| {
            value
                .get("details")
                .and_then(|details| details.get("dropped_row_count"))
                .and_then(Value::as_u64)
        })
        .unwrap_or(1);
    state.local_overrun_count = state.local_overrun_count.saturating_add(dropped);
    state.local_overrun_last_ts_ns = Some(row.local_recv_ts_ns);
}

fn reference_hftrec4_rows(rows: &[RawReferenceWsEvent]) -> Result<Vec<Hftrec4WriteRecord>> {
    Ok(rows
        .iter()
        .map(|row| Hftrec4WriteRecord {
            ingest_seq: row.ingest_seq,
            local_recv_ts_ns: row.local_recv_ts_ns,
            event_type: row.event_type.clone(),
            symbol: Some(format!("{}:{}", row.venue, row.symbol)),
            condition_id: None,
            asset_id: None,
            market_start_ts_ns: row.bar_open_time_ms.map(|ts| ts * 1_000_000),
            market_end_ts_ns: row.bar_close_time_ms.map(|ts| ts * 1_000_000),
            yes_asset_id: None,
            no_asset_id: None,
            payload: serde_json::to_vec(row).expect("serialize RawReferenceWsEvent"),
        })
        .collect())
}

pub fn reference_output_path(raw_root: &Path, ts_ns: i64) -> PathBuf {
    let hour_bucket = ts_ns / 1_000_000_000 / 3_600;
    raw_root
        .join(RAW_REFERENCE_WS_STREAM)
        .join(format!("hour_bucket={hour_bucket}"))
        .join(format!("{RAW_REFERENCE_WS_STREAM}-{ts_ns}.hfr4"))
}

pub fn read_reference_ws_state(state_root: &Path) -> Result<ReferenceWsRecorderState> {
    let path = reference_state_path(state_root);
    if !path.exists() {
        return Ok(ReferenceWsRecorderState::default());
    }
    Ok(serde_json::from_slice(&fs::read(&path)?)?)
}

pub fn write_reference_ws_state(state_root: &Path, state: &ReferenceWsRecorderState) -> Result<()> {
    fs::create_dir_all(state_root)?;
    let bytes = serde_json::to_vec_pretty(state)?;
    atomic_write_verified(&reference_state_path(state_root), &bytes, |tmp| {
        let file = fs::File::open(tmp)?;
        serde_json::from_reader::<_, ReferenceWsRecorderState>(file)?;
        Ok(())
    })?;
    Ok(())
}

fn reference_state_path(state_root: &Path) -> PathBuf {
    state_root.join("reference_ws_state.json")
}

fn write_reference_run_manifest(options: &ReferenceWsRecorderOptions) -> Result<()> {
    let manifest = RecorderRunManifest {
        schema_version: 1,
        dataset_format: RECORDER_RUN_MANIFEST_FORMAT.to_string(),
        component: "reference_ws".to_string(),
        local_start_ts_ns: now_unix_ns() as i64,
        raw_root: options.raw_root.clone(),
        state_root: options.state_root.clone(),
        audit_profile_hash: options.audit_profile_hash.clone(),
        config_hash: Some(sha256_bytes(&serde_json::to_vec(&json!({
            "venues": options.venues,
            "symbols": options.symbols,
            "binance_ws_url": options.binance_ws_url,
            "okx_ws_url": options.okx_ws_url,
        }))?)),
        git_sha: current_git_sha(),
        binary_sha256: current_binary_sha256(),
        host: current_host(),
        command_line: command_line(),
        alignment_policy: Some(RecorderAlignmentPolicy::live_replay_default()),
        outputs: vec![
            "reference_ws_state.json".to_string(),
            RECORDER_HEALTH_STREAM.to_string(),
            RAW_REFERENCE_WS_STREAM.to_string(),
        ],
    };
    let bytes = serde_json::to_vec_pretty(&manifest)?;
    atomic_write_verified(
        &options.state_root.join("reference_run_manifest.json"),
        &bytes,
        |tmp| {
            let file = fs::File::open(tmp)?;
            serde_json::from_reader::<_, RecorderRunManifest>(file)?;
            Ok(())
        },
    )?;
    Ok(())
}

fn validate_reference_options(options: &ReferenceWsRecorderOptions) -> Result<()> {
    if options.venues.is_empty() {
        bail!("at least one reference venue is required");
    }
    if options.symbols.is_empty() {
        bail!("at least one reference symbol is required");
    }
    if options.channel_capacity == 0 {
        bail!("reference channel_capacity must be positive");
    }
    Ok(())
}

fn reference_symbol(raw: &str) -> String {
    raw.trim()
        .trim_end_matches("USDT")
        .trim_end_matches("-USDT")
        .to_ascii_uppercase()
}

fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok())),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}
