use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use market_data_etl_core::{discover_hftrec4_manifests, sha256_bytes, verify_hftrec4_manifest};
use pm5m_recorder::{
    run_reference_ws_forever, run_ws_forever, validate_config, RecorderAuditProfile,
    RecorderConfig, ReferenceWsFlushPolicy, ReferenceWsRecorderOptions, WsFlushPolicy, WsRawFormat,
    WsRecorderOptions, DEFAULT_BINANCE_REFERENCE_WS_URL, DEFAULT_OKX_REFERENCE_WS_URL,
    DEFAULT_REFERENCE_WS_CHANNEL_CAPACITY, DEFAULT_REFERENCE_WS_FLUSH_BYTES,
    DEFAULT_REFERENCE_WS_FLUSH_INTERVAL_MS, DEFAULT_REFERENCE_WS_FLUSH_ROWS,
    DEFAULT_WS_CHANNEL_CAPACITY, DEFAULT_WS_CONNECT_TIMEOUT_MS, DEFAULT_WS_FLUSH_BYTES,
    DEFAULT_WS_FLUSH_INTERVAL_MS, DEFAULT_WS_FLUSH_ROWS, DEFAULT_WS_PING_INTERVAL_MS,
    DEFAULT_WS_REDISCOVERY_INTERVAL_MS, POLYMARKET_CLOB_WS_MARKET_ENDPOINT,
    RECORDER_AUDIT_PROFILE_FORMAT, RECORDER_CONFIG_FORMAT,
};
use serde::Serialize;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Parser)]
#[command(name = "pm5m-recorder")]
#[command(about = "Jupiter-side continuous raw market recorder")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RawFormatArg {
    Hftrec4,
}

#[derive(Debug, Clone, ValueEnum)]
enum ReferenceVenueArg {
    Binance,
    Okx,
}

impl ReferenceVenueArg {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Okx => "okx",
        }
    }
}

impl From<RawFormatArg> for WsRawFormat {
    fn from(value: RawFormatArg) -> Self {
        match value {
            RawFormatArg::Hftrec4 => WsRawFormat::Hftrec4,
        }
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    InitConfig {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        raw_root: PathBuf,
        #[arg(long)]
        state_root: PathBuf,
        #[arg(long, default_value_t = 1_000)]
        poll_interval_ms: u64,
        #[arg(long, default_value_t = 24)]
        max_assets_per_cycle: usize,
        #[arg(long, default_value_t = 200)]
        discovery_limit: usize,
        #[arg(long, default_value_t = 60)]
        discovery_interval_cycles: u64,
        #[arg(long, default_value_t = 5_000)]
        http_timeout_ms: u64,
        #[arg(long, default_value_t = 2)]
        http_max_retries: u32,
        #[arg(long, default_value_t = 250)]
        http_retry_backoff_ms: u64,
        #[arg(long = "symbol")]
        pm5m_symbols: Vec<String>,
        #[arg(long = "interval")]
        pm5m_intervals: Vec<String>,
        #[arg(long, default_value_t = 1)]
        past_window_count: i64,
        #[arg(long, default_value_t = 5)]
        future_window_count: i64,
        #[arg(long = "filter")]
        question_or_slug_contains_any: Vec<String>,
    },
    RunWs {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        raw_root: PathBuf,
        #[arg(long)]
        state_root: PathBuf,
        #[arg(long)]
        typed_root: Option<PathBuf>,
        #[arg(long)]
        book_state_cache_root: Option<PathBuf>,
        #[arg(long, value_enum, default_value = "hftrec4")]
        raw_format: RawFormatArg,
        #[arg(long, default_value = POLYMARKET_CLOB_WS_MARKET_ENDPOINT)]
        endpoint: String,
        #[arg(long, default_value_t = DEFAULT_WS_CHANNEL_CAPACITY)]
        channel_capacity: usize,
        #[arg(long, default_value_t = DEFAULT_WS_FLUSH_INTERVAL_MS)]
        flush_interval_ms: u64,
        #[arg(long, default_value_t = DEFAULT_WS_FLUSH_BYTES)]
        flush_bytes: usize,
        #[arg(long, default_value_t = DEFAULT_WS_FLUSH_ROWS)]
        flush_rows: usize,
        #[arg(long, default_value_t = DEFAULT_WS_CONNECT_TIMEOUT_MS)]
        connect_timeout_ms: u64,
        #[arg(long, default_value_t = DEFAULT_WS_PING_INTERVAL_MS)]
        ping_interval_ms: u64,
        #[arg(long, default_value_t = DEFAULT_WS_REDISCOVERY_INTERVAL_MS)]
        rediscovery_interval_ms: u64,
        #[arg(long = "symbol")]
        pm5m_symbols: Vec<String>,
        #[arg(long = "interval")]
        pm5m_intervals: Vec<String>,
    },
    RunReferenceWs {
        #[arg(long)]
        raw_root: PathBuf,
        #[arg(long)]
        state_root: PathBuf,
        #[arg(long = "reference-venue", value_enum, default_values_t = vec![ReferenceVenueArg::Binance])]
        reference_venues: Vec<ReferenceVenueArg>,
        #[arg(long = "reference-symbol", default_values_t = vec!["BTC".to_string(), "ETH".to_string(), "SOL".to_string()])]
        reference_symbols: Vec<String>,
        #[arg(long, default_value = DEFAULT_BINANCE_REFERENCE_WS_URL)]
        binance_ws_url: String,
        #[arg(long, default_value = DEFAULT_OKX_REFERENCE_WS_URL)]
        okx_ws_url: String,
        #[arg(long, default_value_t = DEFAULT_REFERENCE_WS_CHANNEL_CAPACITY)]
        channel_capacity: usize,
        #[arg(long, default_value_t = DEFAULT_REFERENCE_WS_FLUSH_INTERVAL_MS)]
        flush_interval_ms: u64,
        #[arg(long, default_value_t = DEFAULT_REFERENCE_WS_FLUSH_BYTES)]
        flush_bytes: usize,
        #[arg(long, default_value_t = DEFAULT_REFERENCE_WS_FLUSH_ROWS)]
        flush_rows: usize,
        #[arg(long = "shadow-latency-ms", default_values_t = vec![300])]
        shadow_latency_ms: Vec<u64>,
        #[arg(long = "shadow-tick-offset", default_values_t = vec![0, 1, 2, 5])]
        shadow_tick_offsets: Vec<i32>,
        #[arg(long, default_value_t = 50)]
        book_cache_depth_levels: usize,
    },
    RunDual {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        raw_root: PathBuf,
        #[arg(long)]
        state_root: PathBuf,
        #[arg(long)]
        typed_root: Option<PathBuf>,
        #[arg(long)]
        book_state_cache_root: Option<PathBuf>,
        #[arg(long, value_enum, default_value = "hftrec4")]
        raw_format: RawFormatArg,
        #[arg(long, default_value = POLYMARKET_CLOB_WS_MARKET_ENDPOINT)]
        endpoint: String,
        #[arg(long, default_value_t = DEFAULT_WS_CHANNEL_CAPACITY)]
        channel_capacity: usize,
        #[arg(long, default_value_t = DEFAULT_WS_FLUSH_INTERVAL_MS)]
        flush_interval_ms: u64,
        #[arg(long, default_value_t = DEFAULT_WS_FLUSH_BYTES)]
        flush_bytes: usize,
        #[arg(long, default_value_t = DEFAULT_WS_FLUSH_ROWS)]
        flush_rows: usize,
        #[arg(long, default_value_t = DEFAULT_WS_CONNECT_TIMEOUT_MS)]
        connect_timeout_ms: u64,
        #[arg(long, default_value_t = DEFAULT_WS_PING_INTERVAL_MS)]
        ping_interval_ms: u64,
        #[arg(long, default_value_t = DEFAULT_WS_REDISCOVERY_INTERVAL_MS)]
        rediscovery_interval_ms: u64,
        #[arg(long = "symbol")]
        pm5m_symbols: Vec<String>,
        #[arg(long = "interval")]
        pm5m_intervals: Vec<String>,
        #[arg(long = "reference-venue", value_enum, default_values_t = vec![ReferenceVenueArg::Binance])]
        reference_venues: Vec<ReferenceVenueArg>,
        #[arg(long = "reference-symbol", default_values_t = vec!["BTC".to_string(), "ETH".to_string(), "SOL".to_string()])]
        reference_symbols: Vec<String>,
        #[arg(long, default_value = DEFAULT_BINANCE_REFERENCE_WS_URL)]
        binance_ws_url: String,
        #[arg(long, default_value = DEFAULT_OKX_REFERENCE_WS_URL)]
        okx_ws_url: String,
        #[arg(long = "shadow-latency-ms", default_values_t = vec![300])]
        shadow_latency_ms: Vec<u64>,
        #[arg(long = "shadow-tick-offset", default_values_t = vec![0, 1, 2, 5])]
        shadow_tick_offsets: Vec<i32>,
        #[arg(long, default_value_t = 50)]
        book_cache_depth_levels: usize,
    },
    Status {
        #[arg(long)]
        state_root: PathBuf,
    },
    VerifyHftrec4 {
        #[arg(long)]
        root: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::InitConfig {
            path,
            raw_root,
            state_root,
            poll_interval_ms,
            max_assets_per_cycle,
            discovery_limit,
            discovery_interval_cycles,
            http_timeout_ms,
            http_max_retries,
            http_retry_backoff_ms,
            pm5m_symbols,
            pm5m_intervals,
            past_window_count,
            future_window_count,
            question_or_slug_contains_any,
        } => {
            let mut config = RecorderConfig::default_for_roots(raw_root, state_root);
            config.poll_interval_ms = poll_interval_ms;
            config.max_assets_per_cycle = max_assets_per_cycle;
            config.discovery_interval_cycles = discovery_interval_cycles;
            config.http_timeout_ms = http_timeout_ms;
            config.http_max_retries = http_max_retries;
            config.http_retry_backoff_ms = http_retry_backoff_ms;
            config.source.discovery.limit = discovery_limit;
            if !pm5m_symbols.is_empty() {
                config.source.discovery.pm5m_symbols = pm5m_symbols;
            }
            if !pm5m_intervals.is_empty() {
                config.source.discovery.pm5m_intervals = pm5m_intervals;
            }
            config.source.discovery.pm5m_past_window_count = past_window_count;
            config.source.discovery.pm5m_future_window_count = future_window_count;
            config.source.discovery.question_or_slug_contains_any = question_or_slug_contains_any;
            validate_config(&config)?;
            write_json_atomic(&path, &config)?;
            println!("{}", path.display());
        }
        Command::RunWs {
            config,
            raw_root,
            state_root,
            typed_root,
            book_state_cache_root,
            raw_format,
            endpoint,
            channel_capacity,
            flush_interval_ms,
            flush_bytes,
            flush_rows,
            connect_timeout_ms,
            ping_interval_ms,
            rediscovery_interval_ms,
            pm5m_symbols,
            pm5m_intervals,
        } => {
            let mut config = load_config(&config)?;
            config.raw_root = raw_root.clone();
            config.state_root = state_root.clone();
            if !pm5m_symbols.is_empty() {
                config.source.discovery.pm5m_symbols = pm5m_symbols;
            }
            if !pm5m_intervals.is_empty() {
                config.source.discovery.pm5m_intervals = pm5m_intervals;
            }
            validate_config(&config)?;
            let options = WsRecorderOptions {
                endpoint,
                raw_root,
                state_root,
                typed_root,
                book_state_cache_root,
                audit_profile_hash: None,
                raw_format: raw_format.into(),
                channel_capacity,
                flush_policy: WsFlushPolicy {
                    interval: Duration::from_millis(flush_interval_ms),
                    max_payload_bytes: flush_bytes,
                    max_rows: flush_rows,
                },
                connect_timeout: Duration::from_millis(connect_timeout_ms),
                ping_interval: Duration::from_millis(ping_interval_ms),
                rediscovery_interval: Duration::from_millis(rediscovery_interval_ms),
            };
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(run_ws_forever(config, options))?;
        }
        Command::RunReferenceWs {
            raw_root,
            state_root,
            reference_venues,
            reference_symbols,
            binance_ws_url,
            okx_ws_url,
            channel_capacity,
            flush_interval_ms,
            flush_bytes,
            flush_rows,
            shadow_latency_ms,
            shadow_tick_offsets,
            book_cache_depth_levels,
        } => {
            let venues = reference_venues
                .iter()
                .map(|venue| venue.as_str().to_string())
                .collect::<Vec<_>>();
            let symbols = reference_symbols
                .into_iter()
                .map(|symbol| symbol.to_ascii_uppercase())
                .collect::<Vec<_>>();
            let audit_profile = build_recorder_audit_profile(
                shadow_latency_ms,
                shadow_tick_offsets,
                venues.clone(),
                symbols.clone(),
                book_cache_depth_levels,
            )?;
            write_audit_profile(&state_root, &audit_profile)?;
            let options = ReferenceWsRecorderOptions {
                raw_root,
                state_root,
                audit_profile_hash: Some(audit_profile.audit_profile_hash),
                venues,
                symbols,
                binance_ws_url,
                okx_ws_url,
                channel_capacity,
                flush_policy: ReferenceWsFlushPolicy {
                    interval: Duration::from_millis(flush_interval_ms),
                    max_payload_bytes: flush_bytes,
                    max_rows: flush_rows,
                },
            };
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(run_reference_ws_forever(options))?;
        }
        Command::RunDual {
            config,
            raw_root,
            state_root,
            typed_root,
            book_state_cache_root,
            raw_format,
            endpoint,
            channel_capacity,
            flush_interval_ms,
            flush_bytes,
            flush_rows,
            connect_timeout_ms,
            ping_interval_ms,
            rediscovery_interval_ms,
            pm5m_symbols,
            pm5m_intervals,
            reference_venues,
            reference_symbols,
            binance_ws_url,
            okx_ws_url,
            shadow_latency_ms,
            shadow_tick_offsets,
            book_cache_depth_levels,
        } => {
            let mut config = load_config(&config)?;
            config.raw_root = raw_root.clone();
            config.state_root = state_root.clone();
            if !pm5m_symbols.is_empty() {
                config.source.discovery.pm5m_symbols = pm5m_symbols;
            }
            if !pm5m_intervals.is_empty() {
                config.source.discovery.pm5m_intervals = pm5m_intervals;
            }
            validate_config(&config)?;
            let venues = reference_venues
                .iter()
                .map(|venue| venue.as_str().to_string())
                .collect::<Vec<_>>();
            let symbols = reference_symbols
                .into_iter()
                .map(|symbol| symbol.to_ascii_uppercase())
                .collect::<Vec<_>>();
            let audit_profile = build_recorder_audit_profile(
                shadow_latency_ms,
                shadow_tick_offsets,
                venues.clone(),
                symbols.clone(),
                book_cache_depth_levels,
            )?;
            write_audit_profile(&state_root, &audit_profile)?;
            let audit_profile_hash = Some(audit_profile.audit_profile_hash);
            let ws_options = WsRecorderOptions {
                endpoint,
                raw_root: raw_root.clone(),
                state_root: state_root.clone(),
                typed_root,
                book_state_cache_root,
                audit_profile_hash: audit_profile_hash.clone(),
                raw_format: raw_format.into(),
                channel_capacity,
                flush_policy: WsFlushPolicy {
                    interval: Duration::from_millis(flush_interval_ms),
                    max_payload_bytes: flush_bytes,
                    max_rows: flush_rows,
                },
                connect_timeout: Duration::from_millis(connect_timeout_ms),
                ping_interval: Duration::from_millis(ping_interval_ms),
                rediscovery_interval: Duration::from_millis(rediscovery_interval_ms),
            };
            let reference_options = ReferenceWsRecorderOptions {
                raw_root,
                state_root,
                audit_profile_hash,
                venues,
                symbols,
                binance_ws_url,
                okx_ws_url,
                channel_capacity: DEFAULT_REFERENCE_WS_CHANNEL_CAPACITY,
                flush_policy: ReferenceWsFlushPolicy::default_live(),
            };
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(async move {
                    tokio::try_join!(
                        run_ws_forever(config, ws_options),
                        run_reference_ws_forever(reference_options)
                    )?;
                    Ok::<(), anyhow::Error>(())
                })?;
        }
        Command::Status { state_root } => {
            let state = state_root.join("recorder_state.json");
            let manifest = state_root.join("recorder_manifest.json");
            if state.exists() {
                println!("{}", fs::read_to_string(&state)?);
            } else {
                println!("state missing: {}", state.display());
            }
            if manifest.exists() {
                println!("{}", fs::read_to_string(&manifest)?);
            }
        }
        Command::VerifyHftrec4 { root } => {
            let manifests = discover_hftrec4_manifests(&root)?;
            let mut rows = 0u64;
            for manifest_path in &manifests {
                let manifest = verify_hftrec4_manifest(manifest_path)?;
                rows += manifest.record_count;
            }
            println!(
                "verified_hftrec4_manifests={} rows={} root={}",
                manifests.len(),
                rows,
                root.display()
            );
        }
    }
    Ok(())
}

fn load_config(path: &PathBuf) -> Result<RecorderConfig> {
    let file = fs::File::open(path)?;
    let config = serde_json::from_reader::<_, RecorderConfig>(file)?;
    anyhow::ensure!(
        config.dataset_format == RECORDER_CONFIG_FORMAT,
        "unsupported recorder config format {}",
        config.dataset_format
    );
    validate_config(&config)?;
    Ok(config)
}

fn build_recorder_audit_profile(
    mut shadow_order_latencies_ms: Vec<u64>,
    mut shadow_tick_offsets: Vec<i32>,
    mut reference_venues: Vec<String>,
    mut reference_symbols: Vec<String>,
    book_cache_depth_levels: usize,
) -> Result<RecorderAuditProfile> {
    shadow_order_latencies_ms.sort_unstable();
    shadow_order_latencies_ms.dedup();
    shadow_tick_offsets.sort_unstable();
    shadow_tick_offsets.dedup();
    reference_venues.sort();
    reference_venues.dedup();
    reference_symbols.sort();
    reference_symbols.dedup();
    let price_rounding =
        "floor_to_tick_for_base_buy_limit_then_add_shadow_ticks_clamped_to_[tick,1-tick]"
            .to_string();
    let seed = serde_json::json!({
        "schema_version": 1,
        "dataset_format": RECORDER_AUDIT_PROFILE_FORMAT,
        "shadow_order_latencies_ms": shadow_order_latencies_ms,
        "shadow_tick_offsets": shadow_tick_offsets,
        "reference_venues": reference_venues,
        "reference_symbols": reference_symbols,
        "book_cache_depth_levels": book_cache_depth_levels,
        "price_rounding": &price_rounding,
    });
    let audit_profile_hash = sha256_bytes(&serde_json::to_vec(&seed)?);
    Ok(RecorderAuditProfile {
        schema_version: 1,
        dataset_format: RECORDER_AUDIT_PROFILE_FORMAT.to_string(),
        audit_profile_hash,
        shadow_order_latencies_ms: seed["shadow_order_latencies_ms"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_u64())
            .collect(),
        shadow_tick_offsets: seed["shadow_tick_offsets"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_i64().and_then(|raw| i32::try_from(raw).ok()))
            .collect(),
        reference_venues: seed["reference_venues"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str().map(ToString::to_string))
            .collect(),
        reference_symbols: seed["reference_symbols"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str().map(ToString::to_string))
            .collect(),
        book_cache_depth_levels,
        price_rounding,
    })
}

fn write_audit_profile(state_root: &PathBuf, profile: &RecorderAuditProfile) -> Result<()> {
    fs::create_dir_all(state_root)?;
    write_json_atomic(&state_root.join("audit_profile.json"), profile)
}

fn write_json_atomic<T: Serialize>(path: &PathBuf, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    market_data_etl_core::atomic_write_verified(path, &bytes, |tmp| {
        let file = fs::File::open(tmp)?;
        let _: serde_json::Value = serde_json::from_reader(file)?;
        Ok(())
    })
}
