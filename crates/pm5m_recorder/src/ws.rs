use crate::discovery::discover_assets;
use crate::http::BlockingHttpFetcher;
use crate::state::validate_config;
use crate::types::*;
use crate::util::{
    append_recorder_health, command_line, current_binary_sha256, current_git_sha, current_host,
    last_recv_age_ms, string_field,
};
use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use market_data_etl_core::{
    atomic_write_verified, hash_path, hash_serializable, now_unix_ns, raw_record_hash,
    sha256_bytes, write_hftrec4_segment, Hftrec4WriteRecord,
};
use pm5m_market_cache::{
    append_book_cache2_partition, market_replay_compact_typed_output_path,
    write_market_replay_compact_typed_segment_from_hftrec4_write_records,
    AppendBookCache2PartitionOptions, BookCacheRow, CanonicalWsBookReplayer,
};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error as StdError;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{
    client_async_tls_with_config, connect_async, MaybeTlsStream, WebSocketStream,
};

pub const POLYMARKET_CLOB_WS_MARKET_ENDPOINT: &str =
    "wss://ws-subscriptions-clob.polymarket.com/ws/market";
pub const DEFAULT_WS_CHANNEL_CAPACITY: usize = 8_192;
pub const DEFAULT_WS_FLUSH_INTERVAL_MS: u64 = 5_000;
pub const DEFAULT_WS_FLUSH_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_WS_FLUSH_ROWS: usize = 50_000;
pub const DEFAULT_WS_PING_INTERVAL_MS: u64 = 10_000;
pub const DEFAULT_WS_REDISCOVERY_INTERVAL_MS: u64 = 60_000;
pub const DEFAULT_WS_CONNECT_TIMEOUT_MS: u64 = 15_000;

#[derive(Debug, Clone)]
pub struct WsRecorderOptions {
    pub endpoint: String,
    pub raw_root: PathBuf,
    pub state_root: PathBuf,
    pub typed_root: Option<PathBuf>,
    pub book_state_cache_root: Option<PathBuf>,
    pub audit_profile_hash: Option<String>,
    pub raw_format: WsRawFormat,
    pub channel_capacity: usize,
    pub flush_policy: WsFlushPolicy,
    pub connect_timeout: Duration,
    pub ping_interval: Duration,
    pub rediscovery_interval: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsRawFormat {
    Hftrec4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WsFlushPolicy {
    pub interval: Duration,
    pub max_payload_bytes: usize,
    pub max_rows: usize,
}

impl WsFlushPolicy {
    pub fn default_live() -> Self {
        Self {
            interval: Duration::from_millis(DEFAULT_WS_FLUSH_INTERVAL_MS),
            max_payload_bytes: DEFAULT_WS_FLUSH_BYTES,
            max_rows: DEFAULT_WS_FLUSH_ROWS,
        }
    }
}

impl WsRecorderOptions {
    pub fn default_for_roots(raw_root: PathBuf, state_root: PathBuf) -> Self {
        Self {
            endpoint: POLYMARKET_CLOB_WS_MARKET_ENDPOINT.to_string(),
            raw_root,
            state_root,
            typed_root: None,
            book_state_cache_root: None,
            audit_profile_hash: None,
            raw_format: WsRawFormat::Hftrec4,
            channel_capacity: DEFAULT_WS_CHANNEL_CAPACITY,
            flush_policy: WsFlushPolicy::default_live(),
            connect_timeout: Duration::from_millis(DEFAULT_WS_CONNECT_TIMEOUT_MS),
            ping_interval: Duration::from_millis(DEFAULT_WS_PING_INTERVAL_MS),
            rediscovery_interval: Duration::from_millis(DEFAULT_WS_REDISCOVERY_INTERVAL_MS),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WsRowContext {
    pub ingest_seq_scope: String,
    pub connection_id: u64,
    pub subscription_epoch: u64,
    pub next_ingest_seq: u64,
    assets_by_id: BTreeMap<String, AssetSpec>,
}

impl WsRowContext {
    pub fn new(
        ingest_seq_scope: impl Into<String>,
        connection_id: u64,
        subscription_epoch: u64,
        next_ingest_seq: u64,
        assets: &[AssetSpec],
    ) -> Self {
        Self {
            ingest_seq_scope: ingest_seq_scope.into(),
            connection_id,
            subscription_epoch,
            next_ingest_seq,
            assets_by_id: assets_by_id(assets),
        }
    }

    pub fn set_subscription(&mut self, subscription_epoch: u64, assets: &[AssetSpec]) {
        self.subscription_epoch = subscription_epoch;
        self.assets_by_id = assets_by_id(assets);
    }

    pub fn control_row(
        &mut self,
        event_type: &str,
        raw_payload: impl Into<Vec<u8>>,
        local_recv_ts_ns: i64,
    ) -> Result<RawPolymarketClobWsEvent> {
        self.row(
            ParsedWsPayload {
                asset_id: None,
                condition_id: None,
                event_type: event_type.to_string(),
                exchange_ts_ms: None,
                raw_payload: raw_payload.into(),
            },
            local_recv_ts_ns,
        )
    }

    fn row(
        &mut self,
        payload: ParsedWsPayload,
        local_recv_ts_ns: i64,
    ) -> Result<RawPolymarketClobWsEvent> {
        let asset = payload
            .asset_id
            .as_ref()
            .and_then(|asset_id| self.assets_by_id.get(asset_id));
        let mut row = RawPolymarketClobWsEvent {
            source_id: RAW_POLYMARKET_CLOB_WS_SOURCE_ID.to_string(),
            ingest_seq_scope: self.ingest_seq_scope.clone(),
            ingest_seq: self.next_ingest_seq,
            local_recv_ts_ns,
            connection_id: self.connection_id,
            subscription_epoch: self.subscription_epoch,
            asset_id: payload
                .asset_id
                .or_else(|| asset.map(|asset| asset.asset_id.clone())),
            condition_id: payload
                .condition_id
                .or_else(|| asset.map(|asset| asset.condition_id.clone())),
            symbol: asset.map(|asset| asset.symbol.clone()),
            outcome: asset.map(|asset| asset.outcome.clone()),
            market_start_ts_ns: asset.and_then(|asset| asset.market_start_ts_ns),
            market_end_ts_ns: asset.and_then(|asset| asset.market_end_ts_ns),
            yes_asset_id: asset.and_then(|asset| asset.yes_asset_id.clone()),
            no_asset_id: asset.and_then(|asset| asset.no_asset_id.clone()),
            event_type: payload.event_type,
            exchange_ts_ms: payload.exchange_ts_ms,
            raw_payload_sha256: sha256_bytes(&payload.raw_payload),
            raw_payload: payload.raw_payload,
            raw_record_hash: String::new(),
        };
        row.raw_record_hash = raw_record_hash(&row)?;
        self.next_ingest_seq = self.next_ingest_seq.saturating_add(1);
        Ok(row)
    }
}

#[derive(Debug)]
struct ParsedWsPayload {
    asset_id: Option<String>,
    condition_id: Option<String>,
    event_type: String,
    exchange_ts_ms: Option<i64>,
    raw_payload: Vec<u8>,
}

#[derive(Debug)]
enum WsWriterCommand {
    Row(RawPolymarketClobWsEvent),
    State(WsStatePatch),
}

#[derive(Debug)]
enum WsStatePatch {
    Connection {
        connection_id: u64,
        subscription_epoch: u64,
    },
    Assets {
        subscription_epoch: u64,
        assets: Vec<AssetSpec>,
    },
    LastError(Option<String>),
}

#[derive(Debug, Default)]
struct LocalOverrunTracker {
    pending_count: u64,
    pending_first_ts_ns: Option<i64>,
    pending_last_ts_ns: Option<i64>,
    total_count: u64,
    last_queue_depth: u64,
}

impl LocalOverrunTracker {
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

#[derive(Debug)]
struct WsConnectionFailure {
    operation: &'static str,
    category: &'static str,
    message: String,
    close_code: Option<u16>,
    close_reason: Option<String>,
}

impl fmt::Display for WsConnectionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for WsConnectionFailure {}

pub async fn run_ws_forever(config: RecorderConfig, options: WsRecorderOptions) -> Result<()> {
    validate_ws_inputs(&config, &options)?;
    fs::create_dir_all(&options.raw_root)
        .with_context(|| format!("create WS raw root {}", options.raw_root.display()))?;
    fs::create_dir_all(&options.state_root)
        .with_context(|| format!("create WS state root {}", options.state_root.display()))?;
    if let Some(typed_root) = &options.typed_root {
        fs::create_dir_all(typed_root)
            .with_context(|| format!("create WS typed root {}", typed_root.display()))?;
    }
    write_ws_run_manifest(&config, &options)?;

    let mut state = read_ws_state(&options.state_root)?;
    let (tx, rx) = mpsc::channel(options.channel_capacity);
    let writer_state = state.clone();
    let writer_options = options.clone();
    let writer =
        tokio::spawn(async move { ws_writer_loop(writer_options, writer_state, rx).await });
    let backoffs = [
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(5),
        Duration::from_secs(10),
    ];
    let mut backoff_idx = 0usize;

    loop {
        state.connection_id = state.connection_id.saturating_add(1);
        state.subscription_epoch = state.subscription_epoch.saturating_add(1);
        send_state(
            &tx,
            WsStatePatch::Connection {
                connection_id: state.connection_id,
                subscription_epoch: state.subscription_epoch,
            },
        )
        .await?;

        let result = run_ws_connection(&config, &options, &tx, &mut state).await;
        if writer.is_finished() {
            let writer_result = writer.await.context("join WS writer task")?;
            writer_result?;
            bail!("WS writer stopped");
        }

        let err = match result {
            Ok(()) => anyhow!("websocket connection ended"),
            Err(err) => err,
        };
        let message = err.to_string();
        let mut ctx = WsRowContext::new(
            "pm5m_recorder_ws",
            state.connection_id,
            state.subscription_epoch,
            state.next_ingest_seq,
            &state.current_assets,
        );
        send_row(
            &tx,
            ctx.control_row(
                "disconnect",
                ws_error_control_payload(
                    "disconnect",
                    "connection_end",
                    &state,
                    &options,
                    &err,
                    ws_queue_depth(&tx),
                )?,
                now_unix_ns() as i64,
            )?,
        )
        .await?;
        send_row(
            &tx,
            ctx.control_row(
                "gap_suspected",
                ws_error_control_payload(
                    "gap_suspected",
                    "coverage_gap_start",
                    &state,
                    &options,
                    &err,
                    ws_queue_depth(&tx),
                )?,
                now_unix_ns() as i64,
            )?,
        )
        .await?;
        state.next_ingest_seq = ctx.next_ingest_seq;
        state.last_error = Some(message.clone());
        send_state(&tx, WsStatePatch::LastError(Some(message.clone()))).await?;
        eprintln!(
            "pm5m-recorder ws disconnect connection_id={} error={}",
            state.connection_id, message
        );

        let sleep_for = backoffs[backoff_idx.min(backoffs.len() - 1)];
        backoff_idx = (backoff_idx + 1).min(backoffs.len() - 1);
        tokio::time::sleep(sleep_for).await;
        let reconnect_row = ctx.control_row(
            "reconnect",
            ws_control_payload(
                "reconnect",
                "reconnect_after_backoff",
                &state,
                &options,
                ws_queue_depth(&tx),
                json!({"sleep_ms": sleep_for.as_millis()}),
            )?,
            now_unix_ns() as i64,
        )?;
        state.next_ingest_seq = ctx.next_ingest_seq;
        send_row(&tx, reconnect_row).await?;
    }
}

async fn run_ws_connection(
    config: &RecorderConfig,
    options: &WsRecorderOptions,
    tx: &mpsc::Sender<WsWriterCommand>,
    state: &mut WsRecorderState,
) -> Result<()> {
    let assets = discover_ws_assets(config, &state.current_assets).await?;
    if assets.is_empty() {
        bail!("WS discovery produced no assets");
    }
    state.current_assets = assets;
    send_state(
        tx,
        WsStatePatch::Assets {
            subscription_epoch: state.subscription_epoch,
            assets: state.current_assets.clone(),
        },
    )
    .await?;

    let (ws_stream, _) = tokio::time::timeout(
        options.connect_timeout,
        connect_ws_endpoint(&options.endpoint),
    )
    .await
    .with_context(|| format!("connect timeout for {}", options.endpoint))?
    .with_context(|| format!("connect {}", options.endpoint))?;
    let (mut write, mut read) = ws_stream.split();

    let mut ctx = WsRowContext::new(
        "pm5m_recorder_ws",
        state.connection_id,
        state.subscription_epoch,
        state.next_ingest_seq,
        &state.current_assets,
    );
    let mut overrun = LocalOverrunTracker::default();
    send_row(
        tx,
        ctx.control_row(
            "connect",
            ws_control_payload(
                "connect",
                "connect",
                state,
                options,
                ws_queue_depth(tx),
                json!({"endpoint": options.endpoint}),
            )?,
            now_unix_ns() as i64,
        )?,
    )
    .await?;

    let subscription = market_subscription_json(asset_ids(&state.current_assets));
    write
        .send(Message::Text(subscription.clone()))
        .await
        .context("send WS subscription")?;
    send_row(
        tx,
        ctx.control_row(
            "subscribe",
            ws_control_payload(
                "subscribe",
                "initial_subscribe",
                state,
                options,
                ws_queue_depth(tx),
                json!({
                    "asset_count": state.current_assets.len(),
                    "payload_sha256": sha256_bytes(subscription.as_bytes()),
                    "payload": subscription,
                }),
            )?,
            now_unix_ns() as i64,
        )?,
    )
    .await?;
    state.next_ingest_seq = ctx.next_ingest_seq;
    send_state(tx, WsStatePatch::LastError(None)).await?;

    let mut ping = tokio::time::interval(options.ping_interval);
    ping.tick().await;
    let mut rediscovery = tokio::time::interval(options.rediscovery_interval);
    rediscovery.tick().await;

    loop {
        tokio::select! {
            _ = ping.tick() => {
                write.send(Message::Text("PING".to_string())).await.context("send WS PING")?;
                let row = ctx.control_row("ping", b"PING".to_vec(), now_unix_ns() as i64)?;
                state.next_ingest_seq = ctx.next_ingest_seq;
                enqueue_ws_row_nonblocking(tx, row, &mut ctx, state, options, &mut overrun)?;
            }
            _ = rediscovery.tick() => {
                refresh_ws_subscription(config, options, tx, &mut write, state, &mut ctx).await?;
            }
            message = read.next() => {
                let Some(message) = message else {
                    bail!("websocket stream ended");
                };
                match message.context("read WS message")? {
                    Message::Text(text) => {
                        let recv_ts = now_unix_ns() as i64;
                        let is_ping = text.trim().eq_ignore_ascii_case("PING");
                        let rows = ws_rows_from_text_frame(&mut ctx, &text, recv_ts)?;
                        state.next_ingest_seq = ctx.next_ingest_seq;
                        state.last_recv_ts_ns = Some(recv_ts);
                        for row in rows {
                            enqueue_ws_row_nonblocking(tx, row, &mut ctx, state, options, &mut overrun)?;
                        }
                        if is_ping {
                            write.send(Message::Text("PONG".to_string())).await.context("send text PONG")?;
                            let row = ctx.control_row("pong", b"PONG".to_vec(), now_unix_ns() as i64)?;
                            state.next_ingest_seq = ctx.next_ingest_seq;
                            enqueue_ws_row_nonblocking(tx, row, &mut ctx, state, options, &mut overrun)?;
                        }
                    }
                    Message::Binary(bytes) => {
                        let recv_ts = now_unix_ns() as i64;
                        let text = String::from_utf8(bytes.clone()).unwrap_or_else(|_| String::new());
                        let rows = if text.is_empty() {
                            vec![ctx.control_row("parse_error", bytes, recv_ts)?]
                        } else {
                            ws_rows_from_text_frame(&mut ctx, &text, recv_ts)?
                        };
                        state.next_ingest_seq = ctx.next_ingest_seq;
                        state.last_recv_ts_ns = Some(recv_ts);
                        for row in rows {
                            enqueue_ws_row_nonblocking(tx, row, &mut ctx, state, options, &mut overrun)?;
                        }
                    }
                    Message::Ping(bytes) => {
                        let recv_ts = now_unix_ns() as i64;
                        write.send(Message::Pong(bytes.clone())).await.context("send WS pong frame")?;
                        let row = ctx.control_row("ping", bytes, recv_ts)?;
                        state.next_ingest_seq = ctx.next_ingest_seq;
                        state.last_recv_ts_ns = Some(recv_ts);
                        enqueue_ws_row_nonblocking(tx, row, &mut ctx, state, options, &mut overrun)?;
                    }
                    Message::Pong(bytes) => {
                        let recv_ts = now_unix_ns() as i64;
                        let row = ctx.control_row("pong", bytes, recv_ts)?;
                        state.next_ingest_seq = ctx.next_ingest_seq;
                        state.last_recv_ts_ns = Some(recv_ts);
                        enqueue_ws_row_nonblocking(tx, row, &mut ctx, state, options, &mut overrun)?;
                    }
                    Message::Close(frame) => {
                        return Err(anyhow!(ws_close_failure(frame)));
                    }
                    Message::Frame(_) => {}
                }
            }
        }
    }
}

async fn refresh_ws_subscription<S>(
    config: &RecorderConfig,
    options: &WsRecorderOptions,
    tx: &mpsc::Sender<WsWriterCommand>,
    write: &mut S,
    state: &mut WsRecorderState,
    ctx: &mut WsRowContext,
) -> Result<()>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let new_assets = match discover_ws_assets(config, &state.current_assets).await {
        Ok(assets) => assets,
        Err(err) => {
            let message = format!("WS rediscovery failed: {err}");
            send_state(tx, WsStatePatch::LastError(Some(message.clone()))).await?;
            eprintln!("{message}");
            return Ok(());
        }
    };
    let old_ids = asset_id_set(&state.current_assets);
    let new_ids = asset_id_set(&new_assets);
    let added = new_ids.difference(&old_ids).cloned().collect::<Vec<_>>();
    let removed = old_ids.difference(&new_ids).cloned().collect::<Vec<_>>();
    if added.is_empty() && removed.is_empty() {
        return Ok(());
    }

    state.subscription_epoch = state.subscription_epoch.saturating_add(1);
    ctx.set_subscription(state.subscription_epoch, &new_assets);
    if !added.is_empty() {
        let payload = dynamic_subscription_json(&added, "subscribe");
        write
            .send(Message::Text(payload.clone()))
            .await
            .context("send WS dynamic subscribe")?;
        let row = ctx.control_row(
            "subscribe",
            ws_control_payload(
                "subscribe",
                "dynamic_subscribe",
                state,
                options,
                ws_queue_depth(tx),
                json!({
                    "asset_count": state.current_assets.len(),
                    "added_count": added.len(),
                    "payload_sha256": sha256_bytes(payload.as_bytes()),
                    "payload": payload,
                }),
            )?,
            now_unix_ns() as i64,
        )?;
        state.next_ingest_seq = ctx.next_ingest_seq;
        send_row(tx, row).await?;
    }
    if !removed.is_empty() {
        let payload = dynamic_subscription_json(&removed, "unsubscribe");
        write
            .send(Message::Text(payload.clone()))
            .await
            .context("send WS dynamic unsubscribe")?;
        let row = ctx.control_row(
            "unsubscribe",
            ws_control_payload(
                "unsubscribe",
                "dynamic_unsubscribe",
                state,
                options,
                ws_queue_depth(tx),
                json!({
                    "asset_count": state.current_assets.len(),
                    "removed_count": removed.len(),
                    "payload_sha256": sha256_bytes(payload.as_bytes()),
                    "payload": payload,
                }),
            )?,
            now_unix_ns() as i64,
        )?;
        state.next_ingest_seq = ctx.next_ingest_seq;
        send_row(tx, row).await?;
    }
    state.current_assets = new_assets;
    send_state(
        tx,
        WsStatePatch::Assets {
            subscription_epoch: state.subscription_epoch,
            assets: state.current_assets.clone(),
        },
    )
    .await?;
    Ok(())
}

async fn ws_writer_loop(
    options: WsRecorderOptions,
    mut state: WsRecorderState,
    mut rx: mpsc::Receiver<WsWriterCommand>,
) -> Result<()> {
    let mut rows = Vec::new();
    let mut payload_bytes = 0usize;
    let mut pending_next_ingest_seq = state.next_ingest_seq;
    let mut segment_started_at = Instant::now();
    let mut interval = tokio::time::interval(options.flush_policy.interval);
    let mut book_replayer = options
        .book_state_cache_root
        .as_ref()
        .map(|_| CanonicalWsBookReplayer::default());
    interval.tick().await;

    loop {
        tokio::select! {
            maybe_command = rx.recv() => {
                match maybe_command {
                    Some(WsWriterCommand::Row(row)) => {
                        apply_ws_local_overrun_row(&mut state, &row);
                        payload_bytes = payload_bytes.saturating_add(row.raw_payload.len());
                        pending_next_ingest_seq =
                            pending_next_ingest_seq.max(row.ingest_seq.saturating_add(1));
                        state.last_recv_ts_ns = Some(state.last_recv_ts_ns.map_or(row.local_recv_ts_ns, |ts| ts.max(row.local_recv_ts_ns)));
                        rows.push(row);
                        if should_flush_ws_segment(rows.len(), payload_bytes, segment_started_at.elapsed(), &options.flush_policy) {
                            flush_ws_rows(
                                &options,
                                &mut state,
                                &mut rows,
                                &mut payload_bytes,
                                &mut pending_next_ingest_seq,
                                &mut segment_started_at,
                                rx.len() as u64,
                                book_replayer.as_mut(),
                            )?;
                        }
                    }
                        Some(WsWriterCommand::State(patch)) => {
                            let patch_error = match &patch {
                                WsStatePatch::LastError(error) => error.clone(),
                                _ => None,
                            };
                            if let WsStatePatch::Assets {
                                subscription_epoch,
                                assets,
                            } = &patch
                        {
                            append_market_metadata_snapshots(
                                &options.state_root,
                                *subscription_epoch,
                                assets,
                            )?;
                            }
                            apply_state_patch(&mut state, patch);
                            write_ws_state(&options.state_root, &state)?;
                            if patch_error.is_some() {
                                append_ws_health(
                                    &options,
                                    &state,
                                    "recorder_error",
                                    rows.len() as u64,
                                    payload_bytes as u64,
                                    None,
                                    rx.len() as u64,
                                    rows.len() as u64,
                                    patch_error,
                                )?;
                            }
                        }
                    None => break,
                }
            }
                _ = interval.tick() => {
                    if should_flush_ws_segment(rows.len(), payload_bytes, segment_started_at.elapsed(), &options.flush_policy) {
                        flush_ws_rows(
                        &options,
                        &mut state,
                        &mut rows,
                        &mut payload_bytes,
                        &mut pending_next_ingest_seq,
                        &mut segment_started_at,
                        rx.len() as u64,
                            book_replayer.as_mut(),
                        )?;
                    } else {
                        append_ws_health(
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
    flush_ws_rows(
        &options,
        &mut state,
        &mut rows,
        &mut payload_bytes,
        &mut pending_next_ingest_seq,
        &mut segment_started_at,
        rx.len() as u64,
        book_replayer.as_mut(),
    )?;
    Ok(())
}

fn flush_ws_rows(
    options: &WsRecorderOptions,
    state: &mut WsRecorderState,
    rows: &mut Vec<RawPolymarketClobWsEvent>,
    payload_bytes: &mut usize,
    pending_next_ingest_seq: &mut u64,
    segment_started_at: &mut Instant,
    queue_depth: u64,
    book_replayer: Option<&mut CanonicalWsBookReplayer>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let segment_start_ts_ns = rows
        .first()
        .map(|row| row.local_recv_ts_ns)
        .unwrap_or_else(|| now_unix_ns() as i64);
    let output_path =
        ws_output_path_for_format(&options.raw_root, segment_start_ts_ns, options.raw_format);
    let flush_started = Instant::now();
    let hftrec4_rows = hftrec4_rows(rows)?;
    write_hftrec4_segment(&output_path, &hftrec4_rows)?;
    let typed_row_count = if let Some(typed_root) = &options.typed_root {
        let typed_path = market_replay_compact_typed_output_path(typed_root, segment_start_ts_ns);
        write_market_replay_compact_typed_segment_from_hftrec4_write_records(
            &typed_path,
            &output_path,
            &hftrec4_rows,
        )?
        .map(|manifest| manifest.record_count)
        .unwrap_or(0)
    } else {
        0
    };
    let elapsed_ms = flush_started.elapsed().as_millis();
    let row_count = rows.len();
    let bytes = *payload_bytes;
    let cache_book_rows = if let Some(book_replayer) = book_replayer {
        canonical_book_rows_for_cache(book_replayer, rows)?
    } else {
        Vec::new()
    };
    let cache_book_row_count = cache_book_rows.len();
    if let Some(cache_root) = &options.book_state_cache_root {
        if !cache_book_rows.is_empty() {
            let partition_id = book_state_cache_partition_id(&output_path, segment_start_ts_ns);
            append_book_cache2_partition(
                &AppendBookCache2PartitionOptions {
                    cache_root: cache_root.clone(),
                    partition_id: partition_id.clone(),
                    raw_roots: vec![options.raw_root.clone()],
                },
                cache_book_rows,
            )
            .with_context(|| format!("append live HFTBOOK2 book cache partition {partition_id}"))?;
        }
    }
    state.last_segment_path = Some(output_path.clone());
    state.last_segment_hash = Some(hash_path(&output_path)?);
    state.next_ingest_seq = *pending_next_ingest_seq;
    state.last_recv_ts_ns = rows
        .iter()
        .map(|row| row.local_recv_ts_ns)
        .max()
        .or(state.last_recv_ts_ns);
    write_ws_state(&options.state_root, state)?;
    let last_recv_age_ms = state
        .last_recv_ts_ns
        .map(|ts| ((now_unix_ns() as i64).saturating_sub(ts) / 1_000_000).max(0))
        .unwrap_or(-1);
    eprintln!(
        "pm5m-recorder ws flush rows={} bytes={} typed_rows={} cache_book_rows={} raw_format={:?} elapsed_ms={} last_recv_age_ms={} connection_id={} subscription_assets={}",
        row_count,
        bytes,
        typed_row_count,
        cache_book_row_count,
        options.raw_format,
        elapsed_ms,
        last_recv_age_ms,
        state.connection_id,
        state.current_assets.len()
    );
    append_ws_health(
        options,
        state,
        "flush",
        row_count as u64,
        bytes as u64,
        Some(elapsed_ms as u64),
        queue_depth,
        row_count as u64,
        None,
    )?;
    rows.clear();
    *payload_bytes = 0;
    *segment_started_at = Instant::now();
    Ok(())
}

fn append_ws_health(
    options: &WsRecorderOptions,
    state: &WsRecorderState,
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
            component: "polymarket_clob_ws".to_string(),
            event_type: event_type.to_string(),
            health_status: health_status.to_string(),
            local_ts_ns: now_ns,
            rows,
            payload_bytes,
            segment_path: state.last_segment_path.clone(),
            segment_hash: state.last_segment_hash.clone(),
            flush_elapsed_ms,
            channel_capacity: Some(options.channel_capacity as u64),
            connection_id: Some(state.connection_id),
            connection_epoch: None,
            subscription_epoch: Some(state.subscription_epoch),
            current_asset_count: Some(state.current_assets.len() as u64),
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

fn canonical_book_rows_for_cache(
    replayer: &mut CanonicalWsBookReplayer,
    rows: &[RawPolymarketClobWsEvent],
) -> Result<Vec<BookCacheRow>> {
    let mut out = Vec::new();
    for row in rows {
        out.extend(replayer.apply_ws_raw_row(row)?);
    }
    Ok(out)
}

fn hftrec4_rows(rows: &[RawPolymarketClobWsEvent]) -> Result<Vec<Hftrec4WriteRecord>> {
    Ok(rows
        .iter()
        .map(|row| Hftrec4WriteRecord {
            ingest_seq: row.ingest_seq,
            local_recv_ts_ns: row.local_recv_ts_ns,
            event_type: row.event_type.clone(),
            symbol: row.symbol.clone(),
            condition_id: row.condition_id.clone(),
            asset_id: row.asset_id.clone(),
            market_start_ts_ns: row.market_start_ts_ns,
            market_end_ts_ns: row.market_end_ts_ns,
            yes_asset_id: row.yes_asset_id.clone(),
            no_asset_id: row.no_asset_id.clone(),
            payload: row.raw_payload.clone(),
        })
        .collect())
}

fn book_state_cache_partition_id(output_path: &Path, segment_start_ts_ns: i64) -> String {
    output_path
        .file_stem()
        .and_then(|name| name.to_str())
        .map(|name| {
            name.chars()
                .map(|ch| {
                    if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                        ch
                    } else {
                        '_'
                    }
                })
                .collect()
        })
        .unwrap_or_else(|| format!("ws_{segment_start_ts_ns}"))
}

fn apply_state_patch(state: &mut WsRecorderState, patch: WsStatePatch) {
    match patch {
        WsStatePatch::Connection {
            connection_id,
            subscription_epoch,
        } => {
            state.connection_id = connection_id;
            state.subscription_epoch = subscription_epoch;
        }
        WsStatePatch::Assets {
            subscription_epoch,
            assets,
        } => {
            state.subscription_epoch = subscription_epoch;
            state.current_assets = assets;
        }
        WsStatePatch::LastError(error) => {
            state.last_error = error;
        }
    }
}

async fn discover_ws_assets(
    config: &RecorderConfig,
    fallback_assets: &[AssetSpec],
) -> Result<Vec<AssetSpec>> {
    let config = config.clone();
    let fallback = fallback_assets.to_vec();
    tokio::task::spawn_blocking(move || {
        let mut evidence = Vec::new();
        match discover_assets(&config, &BlockingHttpFetcher, &mut evidence) {
            Ok(assets) if !assets.is_empty() => Ok(assets),
            Ok(_) if replayable_fallback_assets(&fallback) => Ok(fallback),
            Ok(_) => Err(anyhow!("WS discovery returned no assets")),
            Err(_err) if replayable_fallback_assets(&fallback) => Ok(fallback),
            Err(err) if !fallback.is_empty() => Err(anyhow!(
                "WS discovery failed and fallback assets are missing replay metadata: {err}"
            )),
            Err(err) => Err(err),
        }
    })
    .await
    .context("join WS discovery task")?
}

fn enqueue_ws_row_nonblocking(
    tx: &mpsc::Sender<WsWriterCommand>,
    row: RawPolymarketClobWsEvent,
    ctx: &mut WsRowContext,
    state: &mut WsRecorderState,
    options: &WsRecorderOptions,
    overrun: &mut LocalOverrunTracker,
) -> Result<()> {
    try_flush_ws_overrun_marker(tx, ctx, state, options, overrun)?;
    state.next_ingest_seq = ctx.next_ingest_seq;
    match tx.try_send(WsWriterCommand::Row(row)) {
        Ok(()) => {
            state.next_ingest_seq = ctx.next_ingest_seq;
            Ok(())
        }
        Err(TrySendError::Full(WsWriterCommand::Row(row))) => {
            overrun.record_drop(row.local_recv_ts_ns, ws_queue_depth(tx));
            Ok(())
        }
        Err(TrySendError::Full(_)) => {
            overrun.record_drop(now_unix_ns() as i64, ws_queue_depth(tx));
            Ok(())
        }
        Err(TrySendError::Closed(_)) => Err(anyhow!("WS writer channel closed")),
    }
}

fn try_flush_ws_overrun_marker(
    tx: &mpsc::Sender<WsWriterCommand>,
    ctx: &mut WsRowContext,
    state: &WsRecorderState,
    options: &WsRecorderOptions,
    overrun: &mut LocalOverrunTracker,
) -> Result<()> {
    if overrun.pending_count == 0 || tx.capacity() == 0 {
        return Ok(());
    }
    let payload = ws_control_payload(
        "local_overrun",
        "reader_queue_full",
        state,
        options,
        ws_queue_depth(tx),
        json!({
            "dropped_row_count": overrun.pending_count,
            "pending_first_ts_ns": overrun.pending_first_ts_ns,
            "pending_last_ts_ns": overrun.pending_last_ts_ns,
            "total_dropped_row_count": overrun.total_count,
            "last_queue_depth": overrun.last_queue_depth,
        }),
    )?;
    let row = ctx.control_row("local_overrun", payload, now_unix_ns() as i64)?;
    match tx.try_send(WsWriterCommand::Row(row)) {
        Ok(()) => {
            overrun.clear_pending();
            Ok(())
        }
        Err(TrySendError::Full(_)) => Ok(()),
        Err(TrySendError::Closed(_)) => Err(anyhow!("WS writer channel closed")),
    }
}

fn ws_queue_depth(tx: &mpsc::Sender<WsWriterCommand>) -> u64 {
    tx.max_capacity().saturating_sub(tx.capacity()) as u64
}

fn ws_control_payload(
    event_type: &str,
    operation: &str,
    state: &WsRecorderState,
    options: &WsRecorderOptions,
    queue_depth: u64,
    details: Value,
) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&json!({
        "schema_version": 1,
        "event_type": event_type,
        "operation": operation,
        "component": "polymarket_clob_ws",
        "endpoint": options.endpoint,
        "connection_id": state.connection_id,
        "subscription_epoch": state.subscription_epoch,
        "asset_count": state.current_assets.len(),
        "last_recv_age_ms": last_recv_age_ms(now_unix_ns() as i64, state.last_recv_ts_ns),
        "queue_depth": queue_depth,
        "channel_capacity": options.channel_capacity,
        "details": details,
    }))?)
}

fn ws_error_control_payload(
    event_type: &str,
    operation: &str,
    state: &WsRecorderState,
    options: &WsRecorderOptions,
    err: &anyhow::Error,
    queue_depth: u64,
) -> Result<Vec<u8>> {
    let message = err.to_string();
    let failure = err.downcast_ref::<WsConnectionFailure>();
    let io_error_kind = err
        .downcast_ref::<std::io::Error>()
        .map(|error| format!("{:?}", error.kind()));
    ws_control_payload(
        event_type,
        operation,
        state,
        options,
        queue_depth,
        json!({
            "error": message,
            "error_chain": format!("{err:?}"),
            "operation": failure.map(|failure| failure.operation).unwrap_or_else(|| classify_ws_error_operation(&message)),
            "category": failure.map(|failure| failure.category).unwrap_or_else(|| classify_ws_error_category(&message)),
            "close_code": failure.and_then(|failure| failure.close_code),
            "close_reason": failure.and_then(|failure| failure.close_reason.clone()),
            "io_error_kind": io_error_kind,
        }),
    )
}

fn classify_ws_error_operation(message: &str) -> &'static str {
    if message.contains("PING") || message.contains("pong") || message.contains("PONG") {
        "ping_write"
    } else if message.contains("dynamic subscribe") || message.contains("subscription") {
        "subscribe"
    } else if message.contains("dynamic unsubscribe") {
        "unsubscribe"
    } else if message.contains("connect") {
        "connect"
    } else if message.contains("read WS message") || message.contains("stream ended") {
        "read"
    } else {
        "unknown"
    }
}

fn classify_ws_error_category(message: &str) -> &'static str {
    let lower = message.to_ascii_lowercase();
    if lower.contains("timeout") {
        "timeout"
    } else if lower.contains("closed") || lower.contains("stream ended") {
        "closed"
    } else if lower.contains("tls") {
        "tls"
    } else if lower.contains("protocol") {
        "protocol"
    } else if lower.contains("io") || lower.contains("connection reset") {
        "io"
    } else {
        "unknown"
    }
}

fn ws_close_failure(
    frame: Option<tokio_tungstenite::tungstenite::protocol::CloseFrame<'static>>,
) -> WsConnectionFailure {
    let close_code = frame.as_ref().map(|frame| u16::from(frame.code));
    let close_reason = frame.as_ref().map(|frame| frame.reason.to_string());
    WsConnectionFailure {
        operation: "read",
        category: "closed",
        message: format!("websocket closed: {frame:?}"),
        close_code,
        close_reason,
    }
}

fn apply_ws_local_overrun_row(state: &mut WsRecorderState, row: &RawPolymarketClobWsEvent) {
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

async fn send_row(tx: &mpsc::Sender<WsWriterCommand>, row: RawPolymarketClobWsEvent) -> Result<()> {
    tx.send(WsWriterCommand::Row(row))
        .await
        .map_err(|_| anyhow!("WS writer channel closed"))
}

async fn send_state(tx: &mpsc::Sender<WsWriterCommand>, patch: WsStatePatch) -> Result<()> {
    tx.send(WsWriterCommand::State(patch))
        .await
        .map_err(|_| anyhow!("WS writer channel closed"))
}

pub(crate) async fn connect_ws_endpoint(
    endpoint: &str,
) -> Result<(
    WebSocketStream<MaybeTlsStream<TcpStream>>,
    tokio_tungstenite::tungstenite::handshake::client::Response,
)> {
    let target = parse_ws_target(endpoint)?;
    if let Some(proxy) = proxy_for_endpoint(endpoint, &target.host) {
        let stream = connect_http_proxy_tunnel(&proxy, &target).await?;
        return client_async_tls_with_config(endpoint, stream, None, None)
            .await
            .map_err(anyhow::Error::from);
    }
    connect_async(endpoint).await.map_err(anyhow::Error::from)
}

async fn connect_http_proxy_tunnel(proxy: &ProxyTarget, target: &WsTarget) -> Result<TcpStream> {
    let mut stream = TcpStream::connect((&*proxy.host, proxy.port))
        .await
        .with_context(|| format!("connect proxy {}:{}", proxy.host, proxy.port))?;
    let connect = format!(
        "CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\n\r\n",
        target.host, target.port, target.host, target.port
    );
    stream
        .write_all(connect.as_bytes())
        .await
        .context("write proxy CONNECT request")?;
    let mut response = Vec::new();
    let mut buf = [0u8; 1024];
    while !response.windows(4).any(|window| window == b"\r\n\r\n") {
        let n = stream
            .read(&mut buf)
            .await
            .context("read proxy CONNECT response")?;
        if n == 0 {
            bail!("proxy closed before CONNECT response");
        }
        response.extend_from_slice(&buf[..n]);
        if response.len() > 16 * 1024 {
            bail!("proxy CONNECT response too large");
        }
    }
    let status_line = String::from_utf8_lossy(&response)
        .lines()
        .next()
        .unwrap_or("")
        .to_string();
    if !status_line.contains(" 200 ") {
        bail!("proxy CONNECT failed: {status_line}");
    }
    Ok(stream)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WsTarget {
    scheme: String,
    host: String,
    port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProxyTarget {
    host: String,
    port: u16,
}

fn parse_ws_target(endpoint: &str) -> Result<WsTarget> {
    let (scheme, rest) = endpoint
        .split_once("://")
        .ok_or_else(|| anyhow!("WS endpoint missing scheme"))?;
    let default_port = match scheme {
        "ws" => 80,
        "wss" => 443,
        _ => bail!("unsupported WS endpoint scheme {scheme}"),
    };
    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) = split_host_port(authority, default_port)?;
    Ok(WsTarget {
        scheme: scheme.to_string(),
        host,
        port,
    })
}

fn proxy_for_endpoint(endpoint: &str, host: &str) -> Option<ProxyTarget> {
    if no_proxy_matches(host) {
        return None;
    }
    let var_names = if endpoint.starts_with("wss://") {
        ["https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"]
    } else {
        ["http_proxy", "HTTP_PROXY", "all_proxy", "ALL_PROXY"]
    };
    var_names
        .into_iter()
        .filter_map(|name| std::env::var(name).ok())
        .find_map(|value| parse_http_proxy(&value).ok())
}

fn parse_http_proxy(proxy: &str) -> Result<ProxyTarget> {
    let trimmed = proxy.trim();
    let without_scheme = trimmed
        .strip_prefix("http://")
        .or_else(|| trimmed.strip_prefix("https://"))
        .unwrap_or(trimmed);
    let authority = without_scheme
        .split('/')
        .next()
        .unwrap_or(without_scheme)
        .rsplit('@')
        .next()
        .unwrap_or(without_scheme);
    let (host, port) = split_host_port(authority, 8080)?;
    Ok(ProxyTarget { host, port })
}

fn split_host_port(authority: &str, default_port: u16) -> Result<(String, u16)> {
    if authority.trim().is_empty() {
        bail!("empty host");
    }
    if authority.starts_with('[') {
        let end = authority
            .find(']')
            .ok_or_else(|| anyhow!("invalid bracketed IPv6 host"))?;
        let host = authority[1..end].to_string();
        let port = authority[end + 1..]
            .strip_prefix(':')
            .and_then(|port| port.parse::<u16>().ok())
            .unwrap_or(default_port);
        return Ok((host, port));
    }
    let mut parts = authority.rsplitn(2, ':');
    let last = parts.next().unwrap_or(authority);
    let maybe_host = parts.next();
    if let Some(host) = maybe_host {
        if let Ok(port) = last.parse::<u16>() {
            return Ok((host.to_string(), port));
        }
    }
    Ok((authority.to_string(), default_port))
}

fn no_proxy_matches(host: &str) -> bool {
    let Ok(raw) = std::env::var("no_proxy").or_else(|_| std::env::var("NO_PROXY")) else {
        return false;
    };
    raw.split(',').any(|entry| {
        let entry = entry.trim();
        entry == "*"
            || !entry.is_empty()
                && (host == entry
                    || host
                        .strip_suffix(entry.trim_start_matches('.'))
                        .is_some_and(|prefix| prefix.ends_with('.')))
    })
}

pub fn market_subscription_json(asset_ids: Vec<String>) -> String {
    json!({
        "type": "market",
        "assets_ids": asset_ids,
        "custom_feature_enabled": true,
    })
    .to_string()
}

pub fn dynamic_subscription_json(asset_ids: &[String], operation: &str) -> String {
    let value = if operation == "subscribe" {
        json!({
            "operation": operation,
            "assets_ids": asset_ids,
            "custom_feature_enabled": true,
        })
    } else {
        json!({
            "operation": operation,
            "assets_ids": asset_ids,
        })
    };
    value.to_string()
}

pub fn ws_rows_from_text_frame(
    ctx: &mut WsRowContext,
    text: &str,
    local_recv_ts_ns: i64,
) -> Result<Vec<RawPolymarketClobWsEvent>> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("PING") {
        return Ok(vec![ctx.control_row(
            "ping",
            text.as_bytes().to_vec(),
            local_recv_ts_ns,
        )?]);
    }
    if trimmed.eq_ignore_ascii_case("PONG") {
        return Ok(vec![ctx.control_row(
            "pong",
            text.as_bytes().to_vec(),
            local_recv_ts_ns,
        )?]);
    }

    match serde_json::from_str::<Value>(text) {
        Ok(Value::Array(items)) => items
            .into_iter()
            .map(|value| ws_row_from_json_value(ctx, value, local_recv_ts_ns))
            .collect(),
        Ok(value) => Ok(vec![ws_row_from_json_value(ctx, value, local_recv_ts_ns)?]),
        Err(_) => Ok(vec![ctx.control_row(
            "parse_error",
            text.as_bytes().to_vec(),
            local_recv_ts_ns,
        )?]),
    }
}

fn ws_row_from_json_value(
    ctx: &mut WsRowContext,
    value: Value,
    local_recv_ts_ns: i64,
) -> Result<RawPolymarketClobWsEvent> {
    let payload = ParsedWsPayload {
        asset_id: asset_id_for_ws_payload(&value),
        condition_id: string_field(&value, &["condition_id", "conditionId", "market"]),
        event_type: string_field(&value, &["event_type", "type"])
            .unwrap_or_else(|| "unknown".to_string()),
        exchange_ts_ms: timestamp_ms_for_ws_payload(&value),
        raw_payload: serde_json::to_vec(&value)?,
    };
    ctx.row(payload, local_recv_ts_ns)
}

fn asset_id_for_ws_payload(value: &Value) -> Option<String> {
    if let Some(asset_id) = string_field(value, &["asset_id", "assetId", "asset"]) {
        return Some(asset_id);
    }
    let changes = value.get("price_changes")?.as_array()?;
    let mut ids = BTreeSet::new();
    for change in changes {
        if let Some(asset_id) = string_field(change, &["asset_id", "assetId", "asset"]) {
            ids.insert(asset_id);
        }
    }
    if ids.len() == 1 {
        ids.into_iter().next()
    } else {
        None
    }
}

fn timestamp_ms_for_ws_payload(value: &Value) -> Option<i64> {
    value
        .get("timestamp")
        .or_else(|| value.get("ts"))
        .or_else(|| value.get("time"))
        .and_then(value_to_i64)
}

fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().map(|v| v as i64)),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    }
}

pub fn should_flush_ws_segment(
    row_count: usize,
    payload_bytes: usize,
    elapsed: Duration,
    policy: &WsFlushPolicy,
) -> bool {
    row_count > 0
        && (elapsed >= policy.interval
            || payload_bytes >= policy.max_payload_bytes
            || row_count >= policy.max_rows)
}

pub fn ws_output_path(raw_root: &Path, ts_ns: i64) -> PathBuf {
    ws_hftrec4_output_path(raw_root, ts_ns)
}

pub fn ws_output_path_for_format(raw_root: &Path, ts_ns: i64, raw_format: WsRawFormat) -> PathBuf {
    match raw_format {
        WsRawFormat::Hftrec4 => ws_hftrec4_output_path(raw_root, ts_ns),
    }
}

pub fn ws_hftrec4_output_path(raw_root: &Path, ts_ns: i64) -> PathBuf {
    let hour_bucket = ts_ns / 1_000_000_000 / 3_600;
    raw_root
        .join(RAW_POLYMARKET_CLOB_WS_STREAM)
        .join(format!("hour_bucket={hour_bucket}"))
        .join(format!("{RAW_POLYMARKET_CLOB_WS_STREAM}-{ts_ns}.hfr4"))
}

fn write_ws_run_manifest(config: &RecorderConfig, options: &WsRecorderOptions) -> Result<()> {
    let mut outputs = vec![
        "recorder_state.json".to_string(),
        RECORDER_HEALTH_STREAM.to_string(),
        "market_metadata_snapshots.jsonl".to_string(),
        RAW_POLYMARKET_CLOB_WS_STREAM.to_string(),
    ];
    if options.typed_root.is_some() {
        outputs.push("polymarket_clob_ws_typed".to_string());
    }
    let manifest = RecorderRunManifest {
        schema_version: 1,
        dataset_format: RECORDER_RUN_MANIFEST_FORMAT.to_string(),
        component: "polymarket_clob_ws".to_string(),
        local_start_ts_ns: now_unix_ns() as i64,
        raw_root: options.raw_root.clone(),
        state_root: options.state_root.clone(),
        typed_root: options.typed_root.clone(),
        audit_profile_hash: options.audit_profile_hash.clone(),
        config_hash: Some(hash_serializable(&json!({
            "config": config,
            "endpoint": options.endpoint,
            "raw_format": format!("{:?}", options.raw_format),
            "typed_root": options.typed_root.clone(),
            "book_state_cache_root": options.book_state_cache_root,
            "flush_interval_ms": options.flush_policy.interval.as_millis(),
            "flush_bytes": options.flush_policy.max_payload_bytes,
            "flush_rows": options.flush_policy.max_rows,
        }))?),
        git_sha: current_git_sha(),
        binary_sha256: current_binary_sha256(),
        host: current_host(),
        command_line: command_line(),
        alignment_policy: Some(RecorderAlignmentPolicy::live_replay_default()),
        outputs,
    };
    let bytes = serde_json::to_vec_pretty(&manifest)?;
    atomic_write_verified(
        &options.state_root.join("clob_ws_run_manifest.json"),
        &bytes,
        |tmp| {
            let file = fs::File::open(tmp)?;
            serde_json::from_reader::<_, RecorderRunManifest>(file)?;
            Ok(())
        },
    )?;
    Ok(())
}

fn append_market_metadata_snapshots(
    state_root: &Path,
    discovery_seq: u64,
    assets: &[AssetSpec],
) -> Result<()> {
    fs::create_dir_all(state_root)?;
    let path = state_root.join("market_metadata_snapshots.jsonl");
    let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
    let local_recv_ts_ns = now_unix_ns() as i64;
    for asset in assets {
        let snapshot = MarketMetadataSnapshot {
            schema_version: 1,
            dataset_format: "pm5m_market_metadata_snapshot.v1".to_string(),
            local_recv_ts_ns,
            discovery_seq,
            condition_id: asset.condition_id.clone(),
            symbol: asset.symbol.clone(),
            market_start_ts_ns: asset.market_start_ts_ns,
            market_end_ts_ns: asset.market_end_ts_ns,
            yes_asset_id: asset.yes_asset_id.clone(),
            no_asset_id: asset.no_asset_id.clone(),
            tick_size: asset.tick_size.clone(),
            neg_risk: asset.neg_risk,
            accepting_orders: asset.accepting_orders,
            status: asset.status.clone(),
            selected: true,
            skip_reason: None,
        };
        serde_json::to_writer(&mut file, &snapshot)?;
        file.write_all(b"\n")?;
    }
    file.flush()?;
    Ok(())
}

pub fn read_ws_state(state_root: &Path) -> Result<WsRecorderState> {
    let path = ws_state_path(state_root);
    if !path.exists() {
        return Ok(WsRecorderState::default());
    }
    let file = fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let state: WsRecorderState =
        serde_json::from_reader(file).with_context(|| format!("parse {}", path.display()))?;
    if state.dataset_format != WS_RECORDER_STATE_FORMAT {
        bail!(
            "unsupported WS recorder state format {}, expected {}",
            state.dataset_format,
            WS_RECORDER_STATE_FORMAT
        );
    }
    Ok(state)
}

pub fn write_ws_state(state_root: &Path, state: &WsRecorderState) -> Result<()> {
    write_json_atomic(&ws_state_path(state_root), state)
}

fn ws_state_path(state_root: &Path) -> PathBuf {
    state_root.join("ws_recorder_state.json")
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value).context("serialize WS json artifact")?;
    atomic_write_verified(path, &bytes, |tmp| {
        let file = fs::File::open(tmp).with_context(|| format!("open {}", tmp.display()))?;
        let _: Value =
            serde_json::from_reader(file).with_context(|| format!("parse {}", tmp.display()))?;
        Ok(())
    })
}

fn validate_ws_inputs(config: &RecorderConfig, options: &WsRecorderOptions) -> Result<()> {
    validate_config(config)?;
    if options.raw_root.as_os_str().is_empty() {
        bail!("WS raw_root must not be empty");
    }
    if options.state_root.as_os_str().is_empty() {
        bail!("WS state_root must not be empty");
    }
    if options.raw_root == options.state_root {
        bail!("WS raw_root and state_root must be different");
    }
    if let Some(typed_root) = &options.typed_root {
        if typed_root.as_os_str().is_empty() {
            bail!("WS typed_root must not be empty when set");
        }
        if typed_root == &options.raw_root || typed_root == &options.state_root {
            bail!("WS typed_root must be different from raw_root and state_root");
        }
        if options
            .book_state_cache_root
            .as_ref()
            .is_some_and(|cache_root| cache_root == typed_root)
        {
            bail!("WS typed_root and book_state_cache_root must be different");
        }
    }
    if !(options.endpoint.starts_with("ws://") || options.endpoint.starts_with("wss://")) {
        bail!("WS endpoint must be ws/wss URL");
    }
    if options.channel_capacity == 0 {
        bail!("WS channel_capacity must be positive");
    }
    if options.flush_policy.interval.is_zero() {
        bail!("WS flush interval must be positive");
    }
    if options.connect_timeout.is_zero() {
        bail!("WS connect timeout must be positive");
    }
    if options.flush_policy.max_payload_bytes == 0 {
        bail!("WS flush bytes must be positive");
    }
    if options.flush_policy.max_rows == 0 {
        bail!("WS flush rows must be positive");
    }
    if options.ping_interval.is_zero() {
        bail!("WS ping interval must be positive");
    }
    if options.rediscovery_interval.is_zero() {
        bail!("WS rediscovery interval must be positive");
    }
    Ok(())
}

fn asset_ids(assets: &[AssetSpec]) -> Vec<String> {
    assets
        .iter()
        .map(|asset| asset.asset_id.clone())
        .collect::<Vec<_>>()
}

fn asset_id_set(assets: &[AssetSpec]) -> BTreeSet<String> {
    assets
        .iter()
        .map(|asset| asset.asset_id.clone())
        .collect::<BTreeSet<_>>()
}

fn replayable_fallback_assets(assets: &[AssetSpec]) -> bool {
    !assets.is_empty()
        && assets.iter().all(|asset| {
            asset.market_start_ts_ns.is_some()
                && asset.market_end_ts_ns.is_some()
                && asset
                    .yes_asset_id
                    .as_deref()
                    .is_some_and(|value| !value.is_empty())
                && asset
                    .no_asset_id
                    .as_deref()
                    .is_some_and(|value| !value.is_empty())
        })
}

fn assets_by_id(assets: &[AssetSpec]) -> BTreeMap<String, AssetSpec> {
    assets
        .iter()
        .map(|asset| (asset.asset_id.clone(), asset.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_data_etl_core::{
        discover_hftrec4_manifests, read_hftrec4_records, verify_hftrec4_manifest,
    };
    use tempfile::TempDir;
    use tokio::net::TcpListener;

    fn test_asset(asset_id: &str, outcome: &str) -> AssetSpec {
        AssetSpec {
            symbol: "BTC-5M".to_string(),
            condition_id: "cond-btc".to_string(),
            asset_id: asset_id.to_string(),
            outcome: outcome.to_string(),
            market_start_ts_ns: Some(1_757_908_800_000_000_000),
            market_end_ts_ns: Some(1_757_909_100_000_000_000),
            yes_asset_id: Some("asset-yes".to_string()),
            no_asset_id: Some("asset-no".to_string()),
            tick_size: Some("0.01".to_string()),
            neg_risk: Some(false),
            accepting_orders: Some(true),
            status: Some("active".to_string()),
            question: None,
            slug: None,
        }
    }

    #[test]
    fn subscription_json_matches_market_channel_contract() {
        let value: Value = serde_json::from_str(&market_subscription_json(vec![
            "a1".to_string(),
            "a2".to_string(),
        ]))
        .unwrap();
        assert_eq!(value["type"], "market");
        assert_eq!(value["assets_ids"], json!(["a1", "a2"]));
        assert_eq!(value["custom_feature_enabled"], true);
    }

    #[test]
    fn raw_ws_row_hash_and_payload_hash_are_stable() {
        let assets = vec![test_asset("asset-yes", "YES")];
        let mut ctx = WsRowContext::new("scope", 7, 2, 41, &assets);
        let row = ws_rows_from_text_frame(
            &mut ctx,
            r#"{"event_type":"book","asset_id":"asset-yes","timestamp":"1757908892351"}"#,
            100,
        )
        .unwrap()
        .remove(0);
        assert_eq!(row.raw_payload_sha256, sha256_bytes(&row.raw_payload));
        let mut expected = row.clone();
        expected.raw_record_hash.clear();
        assert_eq!(row.raw_record_hash, raw_record_hash(&expected).unwrap());
        assert_eq!(row.symbol.as_deref(), Some("BTC-5M"));
        assert_eq!(row.outcome.as_deref(), Some("YES"));
    }

    #[test]
    fn json_array_frame_splits_into_rows() {
        let assets = vec![test_asset("asset-yes", "YES"), test_asset("asset-no", "NO")];
        let mut ctx = WsRowContext::new("scope", 1, 1, 0, &assets);
        let rows = ws_rows_from_text_frame(
            &mut ctx,
            r#"[{"event_type":"book","asset_id":"asset-yes"},{"event_type":"last_trade_price","asset_id":"asset-no"}]"#,
            100,
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].ingest_seq, 0);
        assert_eq!(rows[1].ingest_seq, 1);
        assert_eq!(rows[0].asset_id.as_deref(), Some("asset-yes"));
        assert_eq!(rows[1].asset_id.as_deref(), Some("asset-no"));
    }

    #[test]
    fn ping_pong_and_invalid_json_generate_control_rows() {
        let mut ctx = WsRowContext::new("scope", 1, 1, 0, &[]);
        let ping = ws_rows_from_text_frame(&mut ctx, "PING", 100).unwrap();
        let pong = ws_rows_from_text_frame(&mut ctx, "PONG", 101).unwrap();
        let parse_error = ws_rows_from_text_frame(&mut ctx, "not-json", 102).unwrap();
        assert_eq!(ping[0].event_type, "ping");
        assert_eq!(pong[0].event_type, "pong");
        assert_eq!(parse_error[0].event_type, "parse_error");
        assert_eq!(parse_error[0].raw_payload, b"not-json".to_vec());
    }

    #[test]
    fn price_change_with_multiple_assets_keeps_single_raw_payload() {
        let assets = vec![test_asset("asset-yes", "YES"), test_asset("asset-no", "NO")];
        let mut ctx = WsRowContext::new("scope", 1, 1, 0, &assets);
        let rows = ws_rows_from_text_frame(
            &mut ctx,
            r#"{"event_type":"price_change","market":"cond-btc","timestamp":"1757908892351","price_changes":[{"asset_id":"asset-yes"},{"asset_id":"asset-no"}]}"#,
            100,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_type, "price_change");
        assert_eq!(rows[0].asset_id, None);
        assert_eq!(rows[0].condition_id.as_deref(), Some("cond-btc"));
        assert_eq!(rows[0].exchange_ts_ms, Some(1_757_908_892_351));
    }

    #[test]
    fn flush_policy_triggers_on_time_bytes_or_rows() {
        let policy = WsFlushPolicy {
            interval: Duration::from_secs(5),
            max_payload_bytes: 64,
            max_rows: 3,
        };
        assert!(!should_flush_ws_segment(
            1,
            10,
            Duration::from_millis(100),
            &policy
        ));
        assert!(should_flush_ws_segment(
            1,
            10,
            Duration::from_secs(5),
            &policy
        ));
        assert!(should_flush_ws_segment(
            1,
            64,
            Duration::from_millis(1),
            &policy
        ));
        assert!(should_flush_ws_segment(
            3,
            1,
            Duration::from_millis(1),
            &policy
        ));
        assert!(!should_flush_ws_segment(
            0,
            1_000,
            Duration::from_secs(60),
            &policy
        ));
    }

    #[tokio::test]
    async fn fake_websocket_connect_subscribe_receive_and_flushes_hftrec4() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let subscription = ws.next().await.unwrap().unwrap();
            let Message::Text(subscription) = subscription else {
                panic!("expected text subscription");
            };
            let subscription: Value = serde_json::from_str(&subscription).unwrap();
            assert_eq!(subscription["type"], "market");
            assert_eq!(subscription["assets_ids"], json!(["asset-yes"]));
            assert_eq!(subscription["custom_feature_enabled"], true);
            ws.send(Message::Text(
                r#"{"event_type":"book","asset_id":"asset-yes","market":"cond-btc","timestamp":"1757908892351","bids":[],"asks":[]}"#
                    .to_string(),
            ))
            .await
            .unwrap();
        });

        let temp = TempDir::new().unwrap();
        let raw_root = temp.path().join("raw");
        let state_root = temp.path().join("state");
        let typed_root = temp.path().join("typed");
        fs::create_dir_all(&raw_root).unwrap();
        fs::create_dir_all(&state_root).unwrap();
        fs::create_dir_all(&typed_root).unwrap();
        let options = WsRecorderOptions {
            endpoint,
            raw_root: raw_root.clone(),
            state_root: state_root.clone(),
            typed_root: Some(typed_root.clone()),
            book_state_cache_root: None,
            audit_profile_hash: None,
            raw_format: WsRawFormat::Hftrec4,
            channel_capacity: 16,
            flush_policy: WsFlushPolicy {
                interval: Duration::from_secs(60),
                max_payload_bytes: 1024 * 1024,
                max_rows: 10,
            },
            connect_timeout: Duration::from_secs(2),
            ping_interval: Duration::from_secs(10),
            rediscovery_interval: Duration::from_secs(60),
        };
        let assets = vec![test_asset("asset-yes", "YES")];
        let state = WsRecorderState {
            connection_id: 1,
            subscription_epoch: 1,
            current_assets: assets.clone(),
            ..WsRecorderState::default()
        };
        let (tx, rx) = mpsc::channel(16);
        let writer_options = options.clone();
        let writer = tokio::spawn(async move { ws_writer_loop(writer_options, state, rx).await });

        let (ws_stream, _) = connect_async(&options.endpoint).await.unwrap();
        let (mut write, mut read) = ws_stream.split();
        let mut ctx = WsRowContext::new("scope", 1, 1, 0, &assets);
        send_row(
            &tx,
            ctx.control_row("connect", options.endpoint.as_bytes(), 100)
                .unwrap(),
        )
        .await
        .unwrap();
        let subscription = market_subscription_json(vec!["asset-yes".to_string()]);
        write
            .send(Message::Text(subscription.clone()))
            .await
            .unwrap();
        send_row(
            &tx,
            ctx.control_row("subscribe", subscription.into_bytes(), 101)
                .unwrap(),
        )
        .await
        .unwrap();
        let Message::Text(frame) = read.next().await.unwrap().unwrap() else {
            panic!("expected text market frame");
        };
        for row in ws_rows_from_text_frame(&mut ctx, &frame, 102).unwrap() {
            send_row(&tx, row).await.unwrap();
        }
        drop(tx);
        writer.await.unwrap().unwrap();
        server.await.unwrap();

        let manifests = discover_hftrec4_manifests(&raw_root).unwrap();
        assert_eq!(manifests.len(), 1);
        let manifest = verify_hftrec4_manifest(&manifests[0]).unwrap();
        let rows = read_hftrec4_records(&manifest.segment_path).unwrap();
        let event_types = rows
            .iter()
            .map(|row| row.event_type.as_str())
            .collect::<Vec<_>>();
        assert_eq!(event_types, vec!["connect", "subscribe", "book"]);
        assert_eq!(rows[2].asset_id.as_deref(), Some("asset-yes"));
        assert_eq!(rows[2].condition_id.as_deref(), Some("cond-btc"));
        assert!(String::from_utf8_lossy(&rows[2].payload).contains("1757908892351"));
        let typed_files = pm5m_market_cache::discover_market_replay_compact_typed_files_for_window(
            &typed_root,
            None,
            None,
        )
        .unwrap();
        assert_eq!(typed_files.len(), 1);
        let typed_rows =
            pm5m_market_cache::read_market_replay_compact_typed_records(&typed_files[0]).unwrap();
        assert_eq!(typed_rows.len(), 1);
        assert_eq!(typed_rows[0].update.event_type, "book");
        assert_eq!(
            typed_rows[0].update.condition_id.as_deref(),
            Some("cond-btc")
        );
        let saved_state = read_ws_state(&state_root).unwrap();
        assert_eq!(saved_state.next_ingest_seq, 3);
    }
}
