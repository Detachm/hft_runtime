use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use rayon::prelude::*;
use serde::Serialize;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const CONDITION_MAGIC: &[u8; 8] = b"PM5MCB1\n";
const NANOS_PER_MS: i64 = 1_000_000;
const DEFAULT_RUN_ROOT: &str =
    "/mnt/data/hft/hft_runtime/analysis_runs/gansinimen_strategy_20260629";
const DEFAULT_QUOTE_CSV: &str = "/mnt/data/hft/hft_runtime/analysis_runs/gansinimen_strategy_20260629/behavior_reconstruction_exp131/queue_position_execution_stress_v0/queue_enriched_selected_orders.csv";
const DEFAULT_OUTPUT_DIR: &str = "/mnt/data/hft/hft_runtime/analysis_runs/gansinimen_strategy_20260629/behavior_reconstruction_active_lifecycle_raw_labels/rust_day_20260628_profile_v3_target_state_vec_agg";
const DEFAULT_START_TS_NS: i64 = 1_782_608_400_000_000_000;
const DEFAULT_END_TS_NS: i64 = DEFAULT_START_TS_NS + 86_400 * 1_000_000_000;

#[derive(Debug, Parser)]
#[command(name = "active-lifecycle-raw-labels")]
#[command(about = "Build active quote fill-time labels from condition replay buckets")]
struct Args {
    #[arg(long, default_value = DEFAULT_QUOTE_CSV)]
    quote_csv: PathBuf,
    #[arg(long = "bucket-root")]
    bucket_roots: Vec<PathBuf>,
    #[arg(long, default_value = DEFAULT_OUTPUT_DIR)]
    output_dir: PathBuf,
    #[arg(long, default_value_t = DEFAULT_START_TS_NS)]
    start_ts_ns: i64,
    #[arg(long, default_value_t = DEFAULT_END_TS_NS)]
    end_ts_ns: i64,
    #[arg(long, value_enum, default_value_t = TerminalMode::FixedTtl)]
    terminal_mode: TerminalMode,
    #[arg(long, default_value_t = 300_000)]
    ttl_ms: i64,
    #[arg(long, default_value = "1000,5000,30000,300000")]
    horizons_ms: String,
    #[arg(long, default_value_t = 0)]
    max_quotes: usize,
    #[arg(long, value_enum, default_value_t = SampleMode::Earliest)]
    sample_mode: SampleMode,
    #[arg(long, default_value_t = 8)]
    workers: usize,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum, PartialEq, Eq, Serialize)]
enum TerminalMode {
    ActualLife,
    FixedTtl,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum, PartialEq, Eq, Serialize)]
enum SampleMode {
    Earliest,
    Balanced,
}

#[derive(Debug, Clone)]
struct BucketWindow {
    window_idx: usize,
    bucket_root: PathBuf,
    start_ts_ns: i64,
    end_ts_ns: i64,
    bucket_count: usize,
    buckets: HashMap<usize, PathBuf>,
}

#[derive(Debug, Clone)]
struct Quote {
    query_id: usize,
    order_rank: Option<i64>,
    candidate_id: Option<i64>,
    condition_id: String,
    asset_id: String,
    outcome: String,
    birth_ts_ns: i64,
    terminal_ts_ns: i64,
    market_start_ts_ns: Option<i64>,
    birth_phase: String,
    scheduler_action: String,
    status: String,
    price_micros: i64,
    order_qty_micros: f64,
    queue_ahead_micros: f64,
    cost_micros: f64,
    pnl_micros: f64,
    fill_label: i32,
    old_full_label: i32,
    old_queue_label: i32,
    old_direct_label: i32,
    old_first_fill_delay_ms: Option<f64>,
    old_effective_delay_ms: Option<f64>,
    old_max_depletion_micros: Option<f64>,
    window_idx: usize,
    bucket_idx: usize,
}

#[derive(Debug, Clone)]
struct Task {
    task_id: usize,
    window_idx: usize,
    window_start_ts_ns: i64,
    window_end_ts_ns: i64,
    bucket_idx: usize,
    bucket_path: PathBuf,
    quotes: Vec<Quote>,
    horizons_ms: Vec<i64>,
}

#[derive(Debug, Default)]
struct LevelPath {
    ts_ns: Vec<i64>,
    strict_cum: Vec<f64>,
    base_cum: Vec<f64>,
    optimistic_cum: Vec<f64>,
    strict_total: f64,
    base_total: f64,
    optimistic_total: f64,
    last_qty: Option<f64>,
    last_best: Option<i64>,
}

impl LevelPath {
    fn append(
        &mut self,
        ts_ns: i64,
        qty: f64,
        best_bid: Option<i64>,
        strict_drop: f64,
        base_drop: f64,
        optimistic_drop: f64,
    ) {
        self.strict_total += strict_drop.max(0.0);
        self.base_total += base_drop.max(0.0);
        self.optimistic_total += optimistic_drop.max(0.0);
        self.ts_ns.push(ts_ns);
        self.strict_cum.push(self.strict_total);
        self.base_cum.push(self.base_total);
        self.optimistic_cum.push(self.optimistic_total);
        self.last_qty = Some(qty);
        self.last_best = best_bid;
    }
}

#[derive(Debug)]
struct TargetLevelState {
    price_micros: i64,
    qty_micros: i64,
    path: LevelPath,
}

#[derive(Debug)]
struct AssetPathState {
    best_bid: Option<i64>,
    price_to_level_idx: HashMap<i64, usize>,
    levels: Vec<TargetLevelState>,
}

impl AssetPathState {
    fn new(target_prices: &HashSet<i64>) -> Self {
        let mut prices: Vec<i64> = target_prices.iter().copied().collect();
        prices.sort_unstable();
        prices.dedup();
        let mut price_to_level_idx = HashMap::with_capacity(prices.len());
        let mut levels = Vec::with_capacity(prices.len());
        for price in prices {
            price_to_level_idx.insert(price, levels.len());
            levels.push(TargetLevelState {
                price_micros: price,
                qty_micros: 0,
                path: LevelPath::default(),
            });
        }
        Self {
            best_bid: None,
            price_to_level_idx,
            levels,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Bid,
    Ask,
}

#[derive(Debug)]
enum Body {
    Other,
    Book {
        asset_id: String,
        bids: Vec<(i64, i64)>,
    },
    PriceChanges {
        changes: Vec<LevelChange>,
    },
}

#[derive(Debug)]
struct LevelChange {
    asset_id: String,
    side: Side,
    price_micros: i64,
    qty_micros: i64,
    best_bid_price_micros: Option<i64>,
}

#[derive(Debug)]
struct Update {
    visible_ts_ns: i64,
    body: Body,
}

#[derive(Debug)]
enum ConditionEvent {
    Reference,
    Update {
        update: Update,
        emit_assets: Vec<String>,
    },
}

#[derive(Debug, Default, Clone)]
struct BucketCounters {
    event_count: u64,
    reference_event_count: u64,
    target_asset_event_count: u64,
    target_price_update_count: u64,
    first_event_ts_ns: Option<i64>,
    last_event_ts_ns: Option<i64>,
    read_ns: u128,
    decode_ns: u128,
    apply_ns: u128,
}

#[derive(Debug, Serialize)]
struct BucketProfile {
    task_id: usize,
    window_idx: usize,
    window_start_ts_ns: i64,
    window_end_ts_ns: i64,
    bucket_idx: usize,
    bucket_path: String,
    quote_count: usize,
    target_asset_count: usize,
    target_price_count: usize,
    path_count: usize,
    label_count: usize,
    elapsed_sec: f64,
    replay_sec: f64,
    label_sec: f64,
    event_count: u64,
    reference_event_count: u64,
    target_asset_event_count: u64,
    target_price_update_count: u64,
    first_event_ts_ns: Option<i64>,
    last_event_ts_ns: Option<i64>,
    read_sec: f64,
    decode_sec: f64,
    apply_sec: f64,
}

#[derive(Debug, Serialize)]
struct LabelRow {
    query_id: usize,
    order_rank: Option<i64>,
    candidate_id: Option<i64>,
    condition_id: String,
    asset_id: String,
    outcome: String,
    birth_ts_ns: i64,
    terminal_ts_ns: i64,
    terminal_age_ms: f64,
    market_start_ts_ns: Option<i64>,
    birth_phase: String,
    scheduler_action: String,
    status: String,
    price_micros: i64,
    order_qty_micros: f64,
    queue_ahead_micros: f64,
    full_threshold_micros: f64,
    cost_micros: f64,
    candidate_single_leg_pnl_micros: f64,
    old_fill_label: i32,
    old_full_label: i32,
    old_queue_label: i32,
    old_direct_label: i32,
    old_first_fill_delay_ms: Option<f64>,
    old_effective_delay_ms: Option<f64>,
    old_max_depletion_micros: Option<f64>,
    path_event_count: usize,
    raw_strict_window_depletion_micros: f64,
    raw_strict_full_label: i32,
    raw_strict_full_age_ms: Option<f64>,
    raw_strict_full_1s: i32,
    raw_strict_full_5s: i32,
    raw_strict_full_30s: i32,
    raw_strict_full_300s: i32,
    raw_base_window_depletion_micros: f64,
    raw_base_full_label: i32,
    raw_base_full_age_ms: Option<f64>,
    raw_base_full_1s: i32,
    raw_base_full_5s: i32,
    raw_base_full_30s: i32,
    raw_base_full_300s: i32,
    raw_optimistic_window_depletion_micros: f64,
    raw_optimistic_full_label: i32,
    raw_optimistic_full_age_ms: Option<f64>,
    raw_optimistic_full_1s: i32,
    raw_optimistic_full_5s: i32,
    raw_optimistic_full_30s: i32,
    raw_optimistic_full_300s: i32,
    old_full_1s: i32,
    old_full_5s: i32,
    old_full_30s: i32,
    old_full_300s: i32,
}

#[derive(Debug, Serialize)]
struct LabelSummaryRow {
    mode: String,
    horizon: String,
    row_count: usize,
    raw_positive_rate: f64,
    old_positive_rate: f64,
    agreement_rate: f64,
    raw_pos_old_neg: usize,
    raw_neg_old_pos: usize,
    both_pos: usize,
    both_neg: usize,
}

#[derive(Debug, Serialize)]
struct PhaseSummaryRow {
    birth_phase: String,
    row_count: usize,
    old_full_label_rate: f64,
    raw_base_full_label_rate: f64,
    raw_base_full_5s_rate: f64,
    raw_base_full_30s_rate: f64,
    raw_base_full_300s_rate: f64,
}

#[derive(Debug, Serialize)]
struct Headline {
    input_quote_count: usize,
    time_filtered_quote_count: usize,
    covered_quote_count: usize,
    outside_ready_bucket_count: usize,
    sampled_quote_count: usize,
    start_ts_ns: i64,
    end_ts_ns: i64,
    requested_window_hours: f64,
    ready_window_count: usize,
    ready_window_hours: f64,
    task_count: usize,
    label_count: usize,
    elapsed_sec: f64,
    task_elapsed_sec: f64,
    worker_count: usize,
    event_count: u64,
    target_asset_event_count: u64,
    target_price_update_count: u64,
    path_observed_share: f64,
    old_full_label_rate: f64,
    raw_strict_full_label_rate: f64,
    raw_base_full_label_rate: f64,
    raw_base_full_5s_rate: f64,
    raw_base_full_30s_rate: f64,
    raw_base_full_300s_rate: f64,
    median_raw_base_full_age_ms: Option<f64>,
    p90_raw_base_full_age_ms: Option<f64>,
    read_sec: f64,
    decode_sec: f64,
    apply_sec: f64,
    label_sec: f64,
}

#[derive(Debug, Serialize)]
struct RunSummary<'a> {
    schema_version: u32,
    generated_unix_ms: u128,
    inputs: SummaryInputs,
    outputs: SummaryOutputs,
    headline: &'a Headline,
    windows: Vec<SummaryWindow>,
    notes: Vec<&'a str>,
}

#[derive(Debug, Serialize)]
struct SummaryInputs {
    quote_csv: String,
    bucket_roots: Vec<String>,
    terminal_mode: TerminalMode,
    ttl_ms: i64,
    horizons_ms: Vec<i64>,
    max_quotes: usize,
    sample_mode: SampleMode,
    workers: usize,
}

#[derive(Debug, Serialize)]
struct SummaryOutputs {
    labels: String,
    bucket_profile: String,
    label_summary: String,
    phase_summary: String,
    summary: String,
}

#[derive(Debug, Serialize)]
struct SummaryWindow {
    window_idx: usize,
    bucket_root: String,
    start_ts_ns: i64,
    end_ts_ns: i64,
    bucket_count: usize,
}

#[derive(Debug)]
struct TaskResult {
    profile: BucketProfile,
    labels: Vec<LabelRow>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    run(args)
}

fn run(args: Args) -> Result<()> {
    let started = Instant::now();
    fs::create_dir_all(&args.output_dir)
        .with_context(|| format!("create output dir {}", args.output_dir.display()))?;
    if args.workers > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(args.workers)
            .build_global()
            .ok();
    }
    let horizons = parse_horizons(&args.horizons_ms)?;
    let bucket_roots = if args.bucket_roots.is_empty() {
        default_bucket_roots()
    } else {
        args.bucket_roots.clone()
    };
    let windows = read_ready_windows(&bucket_roots, args.start_ts_ns, args.end_ts_ns)?;
    let quote_read_started = Instant::now();
    let (quotes, filter_summary) = read_and_prepare_quotes(&args, &windows)?;
    let quote_read_sec = quote_read_started.elapsed().as_secs_f64();
    let tasks = build_tasks(&windows, &quotes, &horizons)?;
    let task_started = Instant::now();
    let results: Vec<TaskResult> = tasks
        .par_iter()
        .map(process_task)
        .collect::<Result<Vec<_>>>()?;
    let task_elapsed_sec = task_started.elapsed().as_secs_f64();

    let mut profiles = Vec::with_capacity(results.len());
    let mut labels = Vec::new();
    for result in results {
        profiles.push(result.profile);
        labels.extend(result.labels);
    }
    profiles.sort_by_key(|profile| profile.task_id);
    labels.sort_by_key(|row| row.query_id);

    let label_summary = build_label_summary(&labels);
    let phase_summary = build_phase_summary(&labels);

    let labels_path = args.output_dir.join("active_lifecycle_raw_labels.csv");
    let bucket_profile_path = args.output_dir.join("bucket_profile.csv");
    let label_summary_path = args.output_dir.join("label_summary.csv");
    let phase_summary_path = args.output_dir.join("phase_summary.csv");
    let summary_path = args.output_dir.join("run_summary.json");

    write_csv(&labels_path, &labels)?;
    write_csv(&bucket_profile_path, &profiles)?;
    write_csv(&label_summary_path, &label_summary)?;
    write_csv(&phase_summary_path, &phase_summary)?;

    let elapsed_sec = started.elapsed().as_secs_f64();
    let headline = build_headline(
        &args,
        &windows,
        &labels,
        &profiles,
        &filter_summary,
        elapsed_sec,
        task_elapsed_sec,
    );
    let output_strings = (
        labels_path.to_string_lossy().to_string(),
        bucket_profile_path.to_string_lossy().to_string(),
        label_summary_path.to_string_lossy().to_string(),
        phase_summary_path.to_string_lossy().to_string(),
        summary_path.to_string_lossy().to_string(),
    );
    let summary = RunSummary {
        schema_version: 1,
        generated_unix_ms: unix_ms(),
        inputs: SummaryInputs {
            quote_csv: args.quote_csv.to_string_lossy().to_string(),
            bucket_roots: bucket_roots
                .iter()
                .map(|path| path.to_string_lossy().to_string())
                .collect(),
            terminal_mode: args.terminal_mode,
            ttl_ms: args.ttl_ms,
            horizons_ms: horizons,
            max_quotes: args.max_quotes,
            sample_mode: args.sample_mode,
            workers: args.workers,
        },
        outputs: SummaryOutputs {
            labels: output_strings.0.clone(),
            bucket_profile: output_strings.1.clone(),
            label_summary: output_strings.2.clone(),
            phase_summary: output_strings.3.clone(),
            summary: output_strings.4.clone(),
        },
        headline: &headline,
        windows: windows
            .iter()
            .map(|window| SummaryWindow {
                window_idx: window.window_idx,
                bucket_root: window.bucket_root.to_string_lossy().to_string(),
                start_ts_ns: window.start_ts_ns,
                end_ts_ns: window.end_ts_ns,
                bucket_count: window.bucket_count,
            })
            .collect(),
        notes: vec![
            "Existing condition replay bucket ETL is unchanged.",
            "This Rust binary only builds derived active quote lifecycle labels from READY buckets.",
            "base counts same-price bid quantity drops; strict requires the quote price to be top bid before or after the drop.",
            "If ready_window_hours is less than requested_window_hours, the local bucket inventory has gaps.",
        ],
    };
    fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?)
        .with_context(|| format!("write {}", summary_path.display()))?;
    println!("{}", serde_json::to_string_pretty(&headline)?);
    eprintln!(
        "quote_read_sec={:.3} labels={} tasks={}",
        quote_read_sec,
        labels.len(),
        profiles.len()
    );
    Ok(())
}

fn default_bucket_roots() -> Vec<PathBuf> {
    vec![
        PathBuf::from(
            "/mnt/data/hft/hft_runtime/analysis_runs/maker_oos_20260630/buckets_btc5m_rebuild_20260628_0100_1000",
        ),
        PathBuf::from(
            "/mnt/data/hft/hft_runtime/analysis_runs/maker_oos_20260630/buckets_btc5m_current_20260628_1000_20260629_0100",
        ),
        PathBuf::from(DEFAULT_RUN_ROOT).join("leader_v3_neighbor_btc_only_buckets_pm5m"),
    ]
}

fn read_ready_windows(
    roots: &[PathBuf],
    start_ts_ns: i64,
    end_ts_ns: i64,
) -> Result<Vec<BucketWindow>> {
    let mut by_key: BTreeMap<(i64, i64, usize), BucketWindow> = BTreeMap::new();
    for root in roots {
        if !root.exists() {
            continue;
        }
        let ready_paths = ready_paths_for_root(root)?;
        for ready_path in ready_paths {
            let ready: Value = serde_json::from_slice(
                &fs::read(&ready_path).with_context(|| format!("read {}", ready_path.display()))?,
            )?;
            let manifest_path = ready
                .get("manifest")
                .and_then(Value::as_str)
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    ready_path
                        .parent()
                        .unwrap()
                        .join("manifest.condition_replay.json")
                });
            if !manifest_path.exists() {
                continue;
            }
            let manifest: Value = serde_json::from_slice(
                &fs::read(&manifest_path)
                    .with_context(|| format!("read {}", manifest_path.display()))?,
            )?;
            let window = ready
                .get("window")
                .ok_or_else(|| anyhow!("READY missing window"))?;
            let window_start = required_i64(window, "start_ts_ns")?;
            let window_end = required_i64(window, "end_ts_ns")?;
            if window_end <= start_ts_ns || window_start >= end_ts_ns {
                continue;
            }
            let buckets_arr = manifest
                .get("buckets")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("manifest missing buckets: {}", manifest_path.display()))?;
            let bucket_count = manifest
                .get("bucket_count")
                .and_then(Value::as_u64)
                .map(|value| value as usize)
                .unwrap_or(buckets_arr.len());
            let mut buckets = HashMap::new();
            for bucket in buckets_arr {
                let bucket_idx = required_u64(bucket, "bucket_idx")? as usize;
                let path = bucket
                    .get("path")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("bucket missing path"))?;
                buckets.insert(bucket_idx, PathBuf::from(path));
            }
            by_key
                .entry((window_start, window_end, bucket_count))
                .or_insert(BucketWindow {
                    window_idx: 0,
                    bucket_root: ready_path.parent().unwrap().to_path_buf(),
                    start_ts_ns: window_start,
                    end_ts_ns: window_end,
                    bucket_count,
                    buckets,
                });
        }
    }
    let mut windows: Vec<_> = by_key.into_values().collect();
    windows.sort_by_key(|window| (window.start_ts_ns, window.end_ts_ns));
    for (idx, window) in windows.iter_mut().enumerate() {
        window.window_idx = idx;
    }
    Ok(windows)
}

fn ready_paths_for_root(root: &Path) -> Result<Vec<PathBuf>> {
    if root.is_file()
        && root.file_name().and_then(|name| name.to_str()) == Some("READY.condition_replay.json")
    {
        return Ok(vec![root.to_path_buf()]);
    }
    let direct = root.join("READY.condition_replay.json");
    if direct.exists() {
        return Ok(vec![direct]);
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(root).with_context(|| format!("read dir {}", root.display()))? {
        let path = entry?.path().join("READY.condition_replay.json");
        if path.exists() {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

#[derive(Debug, Default)]
struct FilterSummary {
    input_quote_count: usize,
    time_filtered_quote_count: usize,
    covered_quote_count: usize,
    outside_ready_bucket_count: usize,
    sampled_quote_count: usize,
}

fn read_and_prepare_quotes(
    args: &Args,
    windows: &[BucketWindow],
) -> Result<(Vec<Quote>, FilterSummary)> {
    let mut reader = csv::Reader::from_path(&args.quote_csv)
        .with_context(|| format!("open {}", args.quote_csv.display()))?;
    let headers = reader.headers()?.clone();
    let idx = HeaderIndex::new(&headers);
    let mut rows = Vec::new();
    let mut summary = FilterSummary::default();
    for record in reader.records() {
        let record = record?;
        summary.input_quote_count += 1;
        let Some(birth_ts_ns) = idx.i64(&record, "birth_ts_ns") else {
            continue;
        };
        if birth_ts_ns < args.start_ts_ns || birth_ts_ns >= args.end_ts_ns {
            continue;
        }
        summary.time_filtered_quote_count += 1;
        let terminal_ts_ns = terminal_ts_ns(&idx, &record, birth_ts_ns, args)
            .min(args.end_ts_ns)
            .max(birth_ts_ns);
        let Some(window_idx) = find_covering_window_idx(windows, birth_ts_ns, terminal_ts_ns)
        else {
            summary.outside_ready_bucket_count += 1;
            continue;
        };
        let condition_id = idx.string(&record, "condition_id");
        let asset_id = idx.string(&record, "asset_id");
        if condition_id.is_empty() || asset_id.is_empty() {
            summary.outside_ready_bucket_count += 1;
            continue;
        }
        let price_micros = idx.i64(&record, "price_micros").unwrap_or_default();
        let bucket_idx = condition_bucket_idx(&condition_id, windows[window_idx].bucket_count);
        let quote = Quote {
            query_id: 0,
            order_rank: idx.i64(&record, "order_rank"),
            candidate_id: idx.i64(&record, "candidate_id"),
            condition_id,
            asset_id,
            outcome: idx.string(&record, "outcome"),
            birth_ts_ns,
            terminal_ts_ns,
            market_start_ts_ns: idx.i64(&record, "market_start_ts_ns"),
            birth_phase: idx.string(&record, "birth_phase"),
            scheduler_action: idx.string(&record, "scheduler_action"),
            status: idx.string(&record, "status"),
            price_micros,
            order_qty_micros: first_f64(
                &idx,
                &record,
                &["queue_model_order_qty_micros", "order_qty_micros"],
            )
            .unwrap_or(0.0),
            queue_ahead_micros: first_f64(
                &idx,
                &record,
                &[
                    "queue_model_queue_ahead_micros",
                    "visible_queue_ahead_micros",
                ],
            )
            .unwrap_or(0.0),
            cost_micros: idx.f64(&record, "cost_micros").unwrap_or(0.0),
            pnl_micros: idx
                .f64(&record, "candidate_single_leg_pnl_micros")
                .unwrap_or(0.0),
            fill_label: idx.f64(&record, "fill_label").unwrap_or(0.0) as i32,
            old_full_label: idx.bool_int(&record, "full_queue_depletion_fill_label"),
            old_queue_label: idx.bool_int(&record, "queue_depletion_fill_label"),
            old_direct_label: idx.bool_int(&record, "direct_depletion_label"),
            old_first_fill_delay_ms: first_f64(
                &idx,
                &record,
                &[
                    "queue_model_first_fill_delay_ms",
                    "universe_first_fill_delay_ms",
                    "first_fill_delay_ms",
                ],
            ),
            old_effective_delay_ms: first_f64(
                &idx,
                &record,
                &[
                    "queue_model_effective_delay_ms",
                    "effective_fill_delay_ms",
                    "first_fill_delay_ms",
                ],
            ),
            old_max_depletion_micros: first_f64(
                &idx,
                &record,
                &["queue_model_max_depletion_micros", "max_depletion_micros"],
            ),
            window_idx,
            bucket_idx,
        };
        summary.covered_quote_count += 1;
        rows.push(quote);
    }
    rows.sort_by_key(|quote| {
        (
            quote.birth_ts_ns,
            quote.order_rank.unwrap_or(i64::MAX),
            quote.candidate_id.unwrap_or(i64::MAX),
        )
    });
    if args.max_quotes > 0 && rows.len() > args.max_quotes {
        match args.sample_mode {
            SampleMode::Earliest => rows.truncate(args.max_quotes),
            SampleMode::Balanced => rows = balanced_sample(rows, args.max_quotes),
        }
    }
    for (idx, quote) in rows.iter_mut().enumerate() {
        quote.query_id = idx + 1;
    }
    summary.sampled_quote_count = rows.len();
    Ok((rows, summary))
}

fn terminal_ts_ns(
    idx: &HeaderIndex,
    record: &csv::StringRecord,
    birth_ts_ns: i64,
    args: &Args,
) -> i64 {
    let fixed = birth_ts_ns.saturating_add(args.ttl_ms.saturating_mul(NANOS_PER_MS));
    if args.terminal_mode == TerminalMode::FixedTtl {
        return fixed;
    }
    idx.i64(record, "fill_ts_ns")
        .or_else(|| idx.i64(record, "cancel_ts_ns"))
        .or_else(|| {
            idx.f64(record, "life_ms")
                .map(|life_ms| birth_ts_ns + (life_ms * NANOS_PER_MS as f64).round() as i64)
        })
        .unwrap_or(fixed)
}

fn balanced_sample(rows: Vec<Quote>, max_quotes: usize) -> Vec<Quote> {
    let mut groups: BTreeMap<(String, String), Vec<Quote>> = BTreeMap::new();
    for row in rows {
        groups
            .entry((row.birth_phase.clone(), row.scheduler_action.clone()))
            .or_default()
            .push(row);
    }
    let per_group = (max_quotes / groups.len().max(1)).max(1);
    let mut out = Vec::new();
    let mut rest = Vec::new();
    for (_, mut group) in groups {
        group.sort_by_key(|quote| (quote.birth_ts_ns, quote.order_rank.unwrap_or(i64::MAX)));
        let split = group.len().min(per_group);
        out.extend(group.drain(..split));
        rest.extend(group);
    }
    rest.sort_by_key(|quote| (quote.birth_ts_ns, quote.order_rank.unwrap_or(i64::MAX)));
    out.extend(rest.into_iter().take(max_quotes.saturating_sub(out.len())));
    out.sort_by_key(|quote| (quote.birth_ts_ns, quote.order_rank.unwrap_or(i64::MAX)));
    out.truncate(max_quotes);
    out
}

struct HeaderIndex {
    positions: HashMap<String, usize>,
}

impl HeaderIndex {
    fn new(headers: &csv::StringRecord) -> Self {
        Self {
            positions: headers
                .iter()
                .enumerate()
                .map(|(idx, name)| (name.to_string(), idx))
                .collect(),
        }
    }

    fn get<'a>(&self, record: &'a csv::StringRecord, name: &str) -> Option<&'a str> {
        self.positions
            .get(name)
            .and_then(|idx| record.get(*idx))
            .filter(|value| !value.trim().is_empty())
    }

    fn string(&self, record: &csv::StringRecord, name: &str) -> String {
        self.get(record, name).unwrap_or("").to_string()
    }

    fn i64(&self, record: &csv::StringRecord, name: &str) -> Option<i64> {
        self.get(record, name)?
            .parse::<f64>()
            .ok()
            .map(|value| value as i64)
    }

    fn f64(&self, record: &csv::StringRecord, name: &str) -> Option<f64> {
        let value = self.get(record, name)?.parse::<f64>().ok()?;
        value.is_finite().then_some(value)
    }

    fn bool_int(&self, record: &csv::StringRecord, name: &str) -> i32 {
        let Some(value) = self.get(record, name) else {
            return 0;
        };
        match value.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "yes" | "1" => 1,
            _ => value
                .parse::<f64>()
                .ok()
                .filter(|number| *number != 0.0)
                .map_or(0, |_| 1),
        }
    }
}

fn first_f64(idx: &HeaderIndex, record: &csv::StringRecord, names: &[&str]) -> Option<f64> {
    names.iter().find_map(|name| idx.f64(record, name))
}

fn find_covering_window_idx(
    windows: &[BucketWindow],
    birth_ts_ns: i64,
    terminal_ts_ns: i64,
) -> Option<usize> {
    windows
        .iter()
        .find(|window| {
            window.start_ts_ns <= birth_ts_ns
                && birth_ts_ns <= terminal_ts_ns
                && terminal_ts_ns <= window.end_ts_ns
        })
        .map(|window| window.window_idx)
}

fn build_tasks(windows: &[BucketWindow], quotes: &[Quote], horizons: &[i64]) -> Result<Vec<Task>> {
    let window_by_idx: HashMap<usize, &BucketWindow> = windows
        .iter()
        .map(|window| (window.window_idx, window))
        .collect();
    let mut grouped: BTreeMap<(usize, usize), Vec<Quote>> = BTreeMap::new();
    for quote in quotes {
        grouped
            .entry((quote.window_idx, quote.bucket_idx))
            .or_default()
            .push(quote.clone());
    }
    let mut out = Vec::new();
    for ((window_idx, bucket_idx), group) in grouped {
        let window = window_by_idx
            .get(&window_idx)
            .ok_or_else(|| anyhow!("missing window {window_idx}"))?;
        let bucket_path = window
            .buckets
            .get(&bucket_idx)
            .ok_or_else(|| anyhow!("missing bucket {bucket_idx} in window {window_idx}"))?;
        out.push(Task {
            task_id: out.len() + 1,
            window_idx,
            window_start_ts_ns: window.start_ts_ns,
            window_end_ts_ns: window.end_ts_ns,
            bucket_idx,
            bucket_path: bucket_path.clone(),
            quotes: group,
            horizons_ms: horizons.to_vec(),
        });
    }
    Ok(out)
}

fn process_task(task: &Task) -> Result<TaskResult> {
    let started = Instant::now();
    let mut target_prices_by_asset: HashMap<String, HashSet<i64>> = HashMap::new();
    for quote in &task.quotes {
        target_prices_by_asset
            .entry(quote.asset_id.clone())
            .or_default()
            .insert(quote.price_micros);
    }
    let replay_started = Instant::now();
    let (paths, counters) = replay_bucket_to_paths(&task.bucket_path, &target_prices_by_asset)?;
    let replay_sec = replay_started.elapsed().as_secs_f64();
    let label_started = Instant::now();
    let labels = build_label_rows(&task.quotes, &paths, &task.horizons_ms);
    let label_sec = label_started.elapsed().as_secs_f64();
    let elapsed_sec = started.elapsed().as_secs_f64();
    let profile = BucketProfile {
        task_id: task.task_id,
        window_idx: task.window_idx,
        window_start_ts_ns: task.window_start_ts_ns,
        window_end_ts_ns: task.window_end_ts_ns,
        bucket_idx: task.bucket_idx,
        bucket_path: task.bucket_path.to_string_lossy().to_string(),
        quote_count: task.quotes.len(),
        target_asset_count: target_prices_by_asset.len(),
        target_price_count: target_prices_by_asset.values().map(HashSet::len).sum(),
        path_count: paths.len(),
        label_count: labels.len(),
        elapsed_sec,
        replay_sec,
        label_sec,
        event_count: counters.event_count,
        reference_event_count: counters.reference_event_count,
        target_asset_event_count: counters.target_asset_event_count,
        target_price_update_count: counters.target_price_update_count,
        first_event_ts_ns: counters.first_event_ts_ns,
        last_event_ts_ns: counters.last_event_ts_ns,
        read_sec: counters.read_ns as f64 / 1e9,
        decode_sec: counters.decode_ns as f64 / 1e9,
        apply_sec: counters.apply_ns as f64 / 1e9,
    };
    eprintln!(
        "{{\"progress\":\"bucket_done\",\"task_id\":{},\"bucket_idx\":{},\"quote_count\":{},\"event_count\":{},\"elapsed_sec\":{:.3}}}",
        profile.task_id, profile.bucket_idx, profile.quote_count, profile.event_count, profile.elapsed_sec
    );
    Ok(TaskResult { profile, labels })
}

fn replay_bucket_to_paths(
    bucket_path: &Path,
    target_prices_by_asset: &HashMap<String, HashSet<i64>>,
) -> Result<(HashMap<(String, i64), LevelPath>, BucketCounters)> {
    let target_assets: HashSet<String> = target_prices_by_asset.keys().cloned().collect();
    let mut asset_states: HashMap<String, AssetPathState> = target_prices_by_asset
        .iter()
        .map(|(asset_id, prices)| (asset_id.clone(), AssetPathState::new(prices)))
        .collect();
    let mut counters = BucketCounters::default();
    let raw = File::open(bucket_path).with_context(|| format!("open {}", bucket_path.display()))?;
    let mut raw = BufReader::new(raw);
    let mut magic = [0u8; 8];
    raw.read_exact(&mut magic)?;
    if &magic != CONDITION_MAGIC {
        bail!("bad condition bucket magic: {}", bucket_path.display());
    }
    let mut reader = zstd::stream::read::Decoder::new(raw)?;
    loop {
        let read_started = Instant::now();
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("read record length {}", bucket_path.display()))
            }
        }
        let payload_len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; payload_len];
        reader
            .read_exact(&mut payload)
            .with_context(|| format!("read record payload {}", bucket_path.display()))?;
        counters.read_ns += read_started.elapsed().as_nanos();

        let decode_started = Instant::now();
        let event = decode_condition_event(&payload)?;
        counters.decode_ns += decode_started.elapsed().as_nanos();
        counters.event_count += 1;
        let ConditionEvent::Update {
            update,
            emit_assets,
        } = event
        else {
            counters.reference_event_count += 1;
            continue;
        };
        counters.first_event_ts_ns = Some(
            counters
                .first_event_ts_ns
                .map_or(update.visible_ts_ns, |ts| ts.min(update.visible_ts_ns)),
        );
        counters.last_event_ts_ns = Some(
            counters
                .last_event_ts_ns
                .map_or(update.visible_ts_ns, |ts| ts.max(update.visible_ts_ns)),
        );
        let affected = affected_assets(&update, &emit_assets);
        let affected_target: Vec<String> = affected
            .into_iter()
            .filter(|asset| target_assets.contains(asset))
            .collect();
        if affected_target.is_empty() {
            continue;
        }
        counters.target_asset_event_count += 1;

        let apply_started = Instant::now();
        apply_update_to_target_paths(&update, &target_assets, &mut asset_states, &mut counters);
        counters.apply_ns += apply_started.elapsed().as_nanos();
    }
    let mut paths: HashMap<(String, i64), LevelPath> = HashMap::new();
    for (asset_id, state) in asset_states {
        for level in state.levels {
            if !level.path.ts_ns.is_empty() {
                paths.insert((asset_id.clone(), level.price_micros), level.path);
            }
        }
    }
    Ok((paths, counters))
}

fn apply_update_to_target_paths(
    update: &Update,
    target_assets: &HashSet<String>,
    asset_states: &mut HashMap<String, AssetPathState>,
    counters: &mut BucketCounters,
) {
    match &update.body {
        Body::Book { asset_id, bids } => {
            if !target_assets.contains(asset_id) {
                return;
            }
            let Some(state) = asset_states.get_mut(asset_id) else {
                return;
            };
            let prev_best = state.best_bid;
            let mut best_bid = None;
            let mut next_qtys = vec![0_i64; state.levels.len()];
            for &(price, qty) in bids {
                if qty <= 0 {
                    continue;
                }
                if best_bid.map_or(true, |best| price > best) {
                    best_bid = Some(price);
                }
                if let Some(idx) = state.price_to_level_idx.get(&price).copied() {
                    next_qtys[idx] = qty;
                }
            }
            state.best_bid = best_bid;
            for (idx, next_qty) in next_qtys.into_iter().enumerate() {
                append_target_level_update(
                    update.visible_ts_ns,
                    &mut state.levels[idx],
                    prev_best,
                    state.best_bid,
                    next_qty,
                    counters,
                );
            }
        }
        Body::PriceChanges { changes } => {
            let mut prev_best_by_asset: Vec<(&str, Option<i64>)> = Vec::new();
            let mut changed_target_prices: Vec<(&str, i64, i64)> = Vec::new();
            for change in changes {
                if !target_assets.contains(&change.asset_id) {
                    continue;
                }
                let Some(state) = asset_states.get_mut(&change.asset_id) else {
                    continue;
                };
                let asset_id = change.asset_id.as_str();
                if !prev_best_by_asset
                    .iter()
                    .any(|(seen_asset_id, _)| *seen_asset_id == asset_id)
                {
                    prev_best_by_asset.push((asset_id, state.best_bid));
                }
                if let Some(best_bid) = change.best_bid_price_micros {
                    state.best_bid = (best_bid > 0).then_some(best_bid);
                } else if change.side == Side::Bid {
                    update_best_bid_fallback(state, change.price_micros, change.qty_micros);
                }
                if change.side == Side::Bid
                    && state.price_to_level_idx.contains_key(&change.price_micros)
                {
                    changed_target_prices.push((
                        asset_id,
                        change.price_micros,
                        change.qty_micros.max(0),
                    ));
                }
            }
            for idx in 0..changed_target_prices.len() {
                let (asset_id, price, next_qty) = changed_target_prices[idx];
                if changed_target_prices[idx + 1..].iter().any(
                    |(other_asset_id, other_price, _)| {
                        *other_asset_id == asset_id && *other_price == price
                    },
                ) {
                    continue;
                }
                let Some(state) = asset_states.get_mut(asset_id) else {
                    continue;
                };
                let prev_best = prev_best_by_asset
                    .iter()
                    .find(|(seen_asset_id, _)| *seen_asset_id == asset_id)
                    .and_then(|(_, best)| *best);
                let Some(level_idx) = state.price_to_level_idx.get(&price).copied() else {
                    continue;
                };
                append_target_level_update(
                    update.visible_ts_ns,
                    &mut state.levels[level_idx],
                    prev_best,
                    state.best_bid,
                    next_qty,
                    counters,
                );
            }
        }
        Body::Other => {}
    }
}

fn update_best_bid_fallback(state: &mut AssetPathState, price: i64, qty: i64) {
    if qty > 0 {
        if state.best_bid.map_or(true, |best| price > best) {
            state.best_bid = Some(price);
        }
    } else if state.best_bid == Some(price) {
        state.best_bid = state
            .levels
            .iter()
            .filter(|level| level.qty_micros > 0 && level.price_micros != price)
            .map(|level| level.price_micros)
            .max();
    }
}

fn append_target_level_update(
    ts_ns: i64,
    level: &mut TargetLevelState,
    prev_best: Option<i64>,
    best_bid: Option<i64>,
    next_qty_micros: i64,
    counters: &mut BucketCounters,
) {
    let next_qty_micros = next_qty_micros.max(0);
    let before_qty = level.qty_micros.max(0) as f64;
    let after_qty = next_qty_micros as f64;
    let drop = (before_qty - after_qty).max(0.0);
    let strict_drop =
        if prev_best == Some(level.price_micros) || best_bid == Some(level.price_micros) {
            drop
        } else {
            0.0
        };
    if drop > 0.0 || level.path.last_qty != Some(after_qty) || level.path.last_best != best_bid {
        level
            .path
            .append(ts_ns, after_qty, best_bid, strict_drop, drop, drop);
        counters.target_price_update_count += 1;
    }
    level.qty_micros = next_qty_micros;
}

fn decode_condition_event(payload: &[u8]) -> Result<ConditionEvent> {
    let mut reader = PayloadReader::new(payload);
    let code = reader.u8()?;
    if code == 2 {
        let _global_event_seq = reader.u64()?;
        let _reference_row_idx = reader.u64()?;
        let _ts_ns = reader.i64()?;
        return Ok(ConditionEvent::Reference);
    }
    let update = match code {
        5 => decode_full_update(&mut reader)?,
        6 => decode_slim_update(&mut reader)?,
        other => bail!("unknown condition event code {other}"),
    };
    let emit_count = reader.u32()? as usize;
    let mut emit_assets = Vec::with_capacity(emit_count);
    for _ in 0..emit_count {
        emit_assets.push(reader.string()?);
        let _seq = reader.u64()?;
    }
    Ok(ConditionEvent::Update {
        update,
        emit_assets,
    })
}

fn decode_slim_update(reader: &mut PayloadReader<'_>) -> Result<Update> {
    let _global_event_seq = reader.u64()?;
    let _symbol = reader.opt_string()?;
    let _horizon = reader.opt_i64()?;
    let _condition = reader.opt_string()?;
    let _original_local_recv_ts_ns = reader.i64()?;
    let visible_ts_ns = reader.i64()?;
    let _ingest_seq = reader.u64()?;
    let _market_start_ts_ns = reader.opt_i64()?;
    let _market_end_ts_ns = reader.opt_i64()?;
    let body = decode_body(reader)?;
    Ok(Update {
        visible_ts_ns,
        body,
    })
}

fn decode_full_update(reader: &mut PayloadReader<'_>) -> Result<Update> {
    let _schema_version = reader.u32()?;
    let _dataset_format = reader.string()?;
    let _global_event_seq = reader.u64()?;
    let _symbol = reader.opt_string()?;
    let _horizon = reader.opt_i64()?;
    let _condition = reader.opt_string()?;
    let _event_type = reader.string()?;
    let _original_local_recv_ts_ns = reader.i64()?;
    let visible_ts_ns = reader.i64()?;
    let _ingest_seq = reader.u64()?;
    let _source_row_idx = reader.u64()?;
    let _payload_hash = reader.string()?;
    let _market_start_ts_ns = reader.opt_i64()?;
    let _market_end_ts_ns = reader.opt_i64()?;
    let body = decode_body(reader)?;
    Ok(Update {
        visible_ts_ns,
        body,
    })
}

fn decode_body(reader: &mut PayloadReader<'_>) -> Result<Body> {
    match reader.u8()? {
        0 => Ok(Body::Other),
        1 => {
            let asset_id = reader.string()?;
            let bids = reader.levels()?;
            reader.skip_levels()?;
            Ok(Body::Book { asset_id, bids })
        }
        2 => {
            let count = reader.u32()? as usize;
            let mut changes = Vec::with_capacity(count);
            for _ in 0..count {
                let asset_id = reader.string()?;
                let side = match reader.u8()? {
                    1 => Side::Bid,
                    2 => Side::Ask,
                    other => bail!("bad side code {other}"),
                };
                let price_micros = reader.i64()?;
                let qty_micros = reader.i64()?;
                let best_bid_price_micros = reader.opt_i64()?;
                let _best_ask = reader.opt_i64()?;
                changes.push(LevelChange {
                    asset_id,
                    side,
                    price_micros,
                    qty_micros,
                    best_bid_price_micros,
                });
            }
            Ok(Body::PriceChanges { changes })
        }
        other => bail!("bad update body code {other}"),
    }
}

struct PayloadReader<'a> {
    payload: &'a [u8],
    cursor: usize,
}

impl<'a> PayloadReader<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self { payload, cursor: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.payload.len().saturating_sub(self.cursor) < n {
            bail!("unexpected end of condition payload");
        }
        let out = &self.payload[self.cursor..self.cursor + n];
        self.cursor += n;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn opt_i64(&mut self) -> Result<Option<i64>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.i64()?)),
            other => bail!("bad optional i64 flag {other}"),
        }
    }

    fn string(&mut self) -> Result<String> {
        let len = self.u32()? as usize;
        Ok(std::str::from_utf8(self.take(len)?)?.to_string())
    }

    fn opt_string(&mut self) -> Result<Option<String>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.string()?)),
            other => bail!("bad optional string flag {other}"),
        }
    }

    fn levels(&mut self) -> Result<Vec<(i64, i64)>> {
        let count = self.u32()? as usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let price = self.i64()?;
            let qty = self.i64()?;
            if qty > 0 {
                out.push((price, qty));
            }
        }
        Ok(out)
    }

    fn skip_levels(&mut self) -> Result<()> {
        let count = self.u32()? as usize;
        for _ in 0..count {
            let _price = self.i64()?;
            let _qty = self.i64()?;
        }
        Ok(())
    }
}

fn affected_assets(update: &Update, emit_assets: &[String]) -> HashSet<String> {
    if !emit_assets.is_empty() {
        return emit_assets.iter().cloned().collect();
    }
    match &update.body {
        Body::Book { asset_id, .. } => HashSet::from([asset_id.clone()]),
        Body::PriceChanges { changes } => changes
            .iter()
            .map(|change| change.asset_id.clone())
            .collect(),
        Body::Other => HashSet::new(),
    }
}

fn build_label_rows(
    quotes: &[Quote],
    paths: &HashMap<(String, i64), LevelPath>,
    _horizons: &[i64],
) -> Vec<LabelRow> {
    quotes
        .iter()
        .map(|quote| {
            let path = paths.get(&(quote.asset_id.clone(), quote.price_micros));
            let full_threshold =
                quote.queue_ahead_micros.max(0.0) + quote.order_qty_micros.max(0.0);
            let mut row = empty_label_row(quote, path.map_or(0, |p| p.ts_ns.len()), full_threshold);
            if let Some(path) = path {
                fill_mode_labels(
                    quote,
                    &mut row,
                    "strict",
                    &path.ts_ns,
                    &path.strict_cum,
                    full_threshold,
                );
                fill_mode_labels(
                    quote,
                    &mut row,
                    "base",
                    &path.ts_ns,
                    &path.base_cum,
                    full_threshold,
                );
                fill_mode_labels(
                    quote,
                    &mut row,
                    "optimistic",
                    &path.ts_ns,
                    &path.optimistic_cum,
                    full_threshold,
                );
            }
            row.old_full_1s = old_horizon_label(quote, 1_000);
            row.old_full_5s = old_horizon_label(quote, 5_000);
            row.old_full_30s = old_horizon_label(quote, 30_000);
            row.old_full_300s = old_horizon_label(quote, 300_000);
            row
        })
        .collect()
}

fn empty_label_row(quote: &Quote, path_event_count: usize, full_threshold: f64) -> LabelRow {
    LabelRow {
        query_id: quote.query_id,
        order_rank: quote.order_rank,
        candidate_id: quote.candidate_id,
        condition_id: quote.condition_id.clone(),
        asset_id: quote.asset_id.clone(),
        outcome: quote.outcome.clone(),
        birth_ts_ns: quote.birth_ts_ns,
        terminal_ts_ns: quote.terminal_ts_ns,
        terminal_age_ms: (quote.terminal_ts_ns - quote.birth_ts_ns) as f64 / NANOS_PER_MS as f64,
        market_start_ts_ns: quote.market_start_ts_ns,
        birth_phase: quote.birth_phase.clone(),
        scheduler_action: quote.scheduler_action.clone(),
        status: quote.status.clone(),
        price_micros: quote.price_micros,
        order_qty_micros: quote.order_qty_micros,
        queue_ahead_micros: quote.queue_ahead_micros,
        full_threshold_micros: full_threshold,
        cost_micros: quote.cost_micros,
        candidate_single_leg_pnl_micros: quote.pnl_micros,
        old_fill_label: quote.fill_label,
        old_full_label: quote.old_full_label,
        old_queue_label: quote.old_queue_label,
        old_direct_label: quote.old_direct_label,
        old_first_fill_delay_ms: quote.old_first_fill_delay_ms,
        old_effective_delay_ms: quote.old_effective_delay_ms,
        old_max_depletion_micros: quote.old_max_depletion_micros,
        path_event_count,
        raw_strict_window_depletion_micros: 0.0,
        raw_strict_full_label: 0,
        raw_strict_full_age_ms: None,
        raw_strict_full_1s: 0,
        raw_strict_full_5s: 0,
        raw_strict_full_30s: 0,
        raw_strict_full_300s: 0,
        raw_base_window_depletion_micros: 0.0,
        raw_base_full_label: 0,
        raw_base_full_age_ms: None,
        raw_base_full_1s: 0,
        raw_base_full_5s: 0,
        raw_base_full_30s: 0,
        raw_base_full_300s: 0,
        raw_optimistic_window_depletion_micros: 0.0,
        raw_optimistic_full_label: 0,
        raw_optimistic_full_age_ms: None,
        raw_optimistic_full_1s: 0,
        raw_optimistic_full_5s: 0,
        raw_optimistic_full_30s: 0,
        raw_optimistic_full_300s: 0,
        old_full_1s: 0,
        old_full_5s: 0,
        old_full_30s: 0,
        old_full_300s: 0,
    }
}

fn fill_mode_labels(
    quote: &Quote,
    row: &mut LabelRow,
    mode: &str,
    ts_ns: &[i64],
    cum: &[f64],
    full_threshold: f64,
) {
    let birth_cum = cum_at(ts_ns, cum, quote.birth_ts_ns);
    let terminal_cum = cum_at(ts_ns, cum, quote.terminal_ts_ns);
    let full_ts = first_cross_ts(
        ts_ns,
        cum,
        quote.birth_ts_ns,
        quote.terminal_ts_ns,
        birth_cum + full_threshold,
        full_threshold,
    );
    let window_depletion = (terminal_cum - birth_cum).max(0.0);
    let full_label = i32::from(full_ts.is_some());
    let full_age_ms = full_ts.map(|ts| (ts - quote.birth_ts_ns) as f64 / NANOS_PER_MS as f64);
    let h1 = horizon_label(full_ts, quote.birth_ts_ns, 1_000);
    let h5 = horizon_label(full_ts, quote.birth_ts_ns, 5_000);
    let h30 = horizon_label(full_ts, quote.birth_ts_ns, 30_000);
    let h300 = horizon_label(full_ts, quote.birth_ts_ns, 300_000);
    match mode {
        "strict" => {
            row.raw_strict_window_depletion_micros = window_depletion;
            row.raw_strict_full_label = full_label;
            row.raw_strict_full_age_ms = full_age_ms;
            row.raw_strict_full_1s = h1;
            row.raw_strict_full_5s = h5;
            row.raw_strict_full_30s = h30;
            row.raw_strict_full_300s = h300;
        }
        "base" => {
            row.raw_base_window_depletion_micros = window_depletion;
            row.raw_base_full_label = full_label;
            row.raw_base_full_age_ms = full_age_ms;
            row.raw_base_full_1s = h1;
            row.raw_base_full_5s = h5;
            row.raw_base_full_30s = h30;
            row.raw_base_full_300s = h300;
        }
        "optimistic" => {
            row.raw_optimistic_window_depletion_micros = window_depletion;
            row.raw_optimistic_full_label = full_label;
            row.raw_optimistic_full_age_ms = full_age_ms;
            row.raw_optimistic_full_1s = h1;
            row.raw_optimistic_full_5s = h5;
            row.raw_optimistic_full_30s = h30;
            row.raw_optimistic_full_300s = h300;
        }
        _ => {}
    }
}

fn cum_at(ts_ns: &[i64], cum: &[f64], ts: i64) -> f64 {
    let idx = ts_ns.partition_point(|value| *value <= ts);
    if idx == 0 {
        0.0
    } else {
        cum[idx - 1]
    }
}

fn first_cross_ts(
    ts_ns: &[i64],
    cum: &[f64],
    birth_ts_ns: i64,
    terminal_ts_ns: i64,
    target_cum: f64,
    threshold: f64,
) -> Option<i64> {
    if threshold <= 0.0 {
        return Some(birth_ts_ns);
    }
    let birth_left = ts_ns.partition_point(|value| *value < birth_ts_ns);
    let mut idx = cum.partition_point(|value| *value < target_cum);
    if idx < birth_left {
        idx = birth_left;
        while idx < cum.len() && cum[idx] < target_cum {
            idx += 1;
        }
    }
    let out = *ts_ns.get(idx)?;
    (out >= birth_ts_ns && out <= terminal_ts_ns).then_some(out)
}

fn horizon_label(full_ts: Option<i64>, birth_ts_ns: i64, horizon_ms: i64) -> i32 {
    i32::from(full_ts.is_some_and(|ts| ts <= birth_ts_ns + horizon_ms * NANOS_PER_MS))
}

fn old_horizon_label(quote: &Quote, horizon_ms: i64) -> i32 {
    i32::from(
        quote.old_full_label == 1
            && quote
                .old_first_fill_delay_ms
                .is_some_and(|delay| delay <= horizon_ms as f64),
    )
}

fn build_label_summary(labels: &[LabelRow]) -> Vec<LabelSummaryRow> {
    let specs: [(&str, &str, fn(&LabelRow) -> i32, fn(&LabelRow) -> i32); 15] = [
        (
            "strict",
            "ttl",
            |r| r.raw_strict_full_label,
            |r| r.old_full_label,
        ),
        ("strict", "1s", |r| r.raw_strict_full_1s, |r| r.old_full_1s),
        ("strict", "5s", |r| r.raw_strict_full_5s, |r| r.old_full_5s),
        (
            "strict",
            "30s",
            |r| r.raw_strict_full_30s,
            |r| r.old_full_30s,
        ),
        (
            "strict",
            "300s",
            |r| r.raw_strict_full_300s,
            |r| r.old_full_300s,
        ),
        (
            "base",
            "ttl",
            |r| r.raw_base_full_label,
            |r| r.old_full_label,
        ),
        ("base", "1s", |r| r.raw_base_full_1s, |r| r.old_full_1s),
        ("base", "5s", |r| r.raw_base_full_5s, |r| r.old_full_5s),
        ("base", "30s", |r| r.raw_base_full_30s, |r| r.old_full_30s),
        (
            "base",
            "300s",
            |r| r.raw_base_full_300s,
            |r| r.old_full_300s,
        ),
        (
            "optimistic",
            "ttl",
            |r| r.raw_optimistic_full_label,
            |r| r.old_full_label,
        ),
        (
            "optimistic",
            "1s",
            |r| r.raw_optimistic_full_1s,
            |r| r.old_full_1s,
        ),
        (
            "optimistic",
            "5s",
            |r| r.raw_optimistic_full_5s,
            |r| r.old_full_5s,
        ),
        (
            "optimistic",
            "30s",
            |r| r.raw_optimistic_full_30s,
            |r| r.old_full_30s,
        ),
        (
            "optimistic",
            "300s",
            |r| r.raw_optimistic_full_300s,
            |r| r.old_full_300s,
        ),
    ];
    specs
        .iter()
        .map(|(mode, horizon, raw_fn, old_fn)| {
            let mut raw_pos = 0usize;
            let mut old_pos = 0usize;
            let mut agree = 0usize;
            let mut raw_pos_old_neg = 0usize;
            let mut raw_neg_old_pos = 0usize;
            let mut both_pos = 0usize;
            let mut both_neg = 0usize;
            for label in labels {
                let raw = raw_fn(label);
                let old = old_fn(label);
                raw_pos += usize::from(raw == 1);
                old_pos += usize::from(old == 1);
                agree += usize::from(raw == old);
                raw_pos_old_neg += usize::from(raw == 1 && old == 0);
                raw_neg_old_pos += usize::from(raw == 0 && old == 1);
                both_pos += usize::from(raw == 1 && old == 1);
                both_neg += usize::from(raw == 0 && old == 0);
            }
            let n = labels.len().max(1) as f64;
            LabelSummaryRow {
                mode: (*mode).to_string(),
                horizon: (*horizon).to_string(),
                row_count: labels.len(),
                raw_positive_rate: raw_pos as f64 / n,
                old_positive_rate: old_pos as f64 / n,
                agreement_rate: agree as f64 / n,
                raw_pos_old_neg,
                raw_neg_old_pos,
                both_pos,
                both_neg,
            }
        })
        .collect()
}

fn build_phase_summary(labels: &[LabelRow]) -> Vec<PhaseSummaryRow> {
    let mut groups: BTreeMap<String, Vec<&LabelRow>> = BTreeMap::new();
    for label in labels {
        groups
            .entry(label.birth_phase.clone())
            .or_default()
            .push(label);
    }
    let mut rows: Vec<_> = groups
        .into_iter()
        .map(|(birth_phase, group)| {
            let n = group.len().max(1) as f64;
            PhaseSummaryRow {
                birth_phase,
                row_count: group.len(),
                old_full_label_rate: group.iter().filter(|row| row.old_full_label == 1).count()
                    as f64
                    / n,
                raw_base_full_label_rate: group
                    .iter()
                    .filter(|row| row.raw_base_full_label == 1)
                    .count() as f64
                    / n,
                raw_base_full_5s_rate: group.iter().filter(|row| row.raw_base_full_5s == 1).count()
                    as f64
                    / n,
                raw_base_full_30s_rate: group
                    .iter()
                    .filter(|row| row.raw_base_full_30s == 1)
                    .count() as f64
                    / n,
                raw_base_full_300s_rate: group
                    .iter()
                    .filter(|row| row.raw_base_full_300s == 1)
                    .count() as f64
                    / n,
            }
        })
        .collect();
    rows.sort_by_key(|row| std::cmp::Reverse(row.row_count));
    rows
}

fn build_headline(
    args: &Args,
    windows: &[BucketWindow],
    labels: &[LabelRow],
    profiles: &[BucketProfile],
    filter_summary: &FilterSummary,
    elapsed_sec: f64,
    task_elapsed_sec: f64,
) -> Headline {
    let n = labels.len().max(1) as f64;
    let mut base_ages: Vec<f64> = labels
        .iter()
        .filter_map(|row| row.raw_base_full_age_ms)
        .collect();
    base_ages.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    Headline {
        input_quote_count: filter_summary.input_quote_count,
        time_filtered_quote_count: filter_summary.time_filtered_quote_count,
        covered_quote_count: filter_summary.covered_quote_count,
        outside_ready_bucket_count: filter_summary.outside_ready_bucket_count,
        sampled_quote_count: filter_summary.sampled_quote_count,
        start_ts_ns: args.start_ts_ns,
        end_ts_ns: args.end_ts_ns,
        requested_window_hours: (args.end_ts_ns - args.start_ts_ns) as f64 / 3_600_000_000_000.0,
        ready_window_count: windows.len(),
        ready_window_hours: windows
            .iter()
            .map(|window| {
                (window.end_ts_ns.min(args.end_ts_ns) - window.start_ts_ns.max(args.start_ts_ns))
                    .max(0)
            })
            .sum::<i64>() as f64
            / 3_600_000_000_000.0,
        task_count: profiles.len(),
        label_count: labels.len(),
        elapsed_sec,
        task_elapsed_sec,
        worker_count: args.workers,
        event_count: profiles.iter().map(|profile| profile.event_count).sum(),
        target_asset_event_count: profiles
            .iter()
            .map(|profile| profile.target_asset_event_count)
            .sum(),
        target_price_update_count: profiles
            .iter()
            .map(|profile| profile.target_price_update_count)
            .sum(),
        path_observed_share: labels.iter().filter(|row| row.path_event_count > 0).count() as f64
            / n,
        old_full_label_rate: labels.iter().filter(|row| row.old_full_label == 1).count() as f64 / n,
        raw_strict_full_label_rate: labels
            .iter()
            .filter(|row| row.raw_strict_full_label == 1)
            .count() as f64
            / n,
        raw_base_full_label_rate: labels
            .iter()
            .filter(|row| row.raw_base_full_label == 1)
            .count() as f64
            / n,
        raw_base_full_5s_rate: labels
            .iter()
            .filter(|row| row.raw_base_full_5s == 1)
            .count() as f64
            / n,
        raw_base_full_30s_rate: labels
            .iter()
            .filter(|row| row.raw_base_full_30s == 1)
            .count() as f64
            / n,
        raw_base_full_300s_rate: labels
            .iter()
            .filter(|row| row.raw_base_full_300s == 1)
            .count() as f64
            / n,
        median_raw_base_full_age_ms: quantile_sorted(&base_ages, 0.5),
        p90_raw_base_full_age_ms: quantile_sorted(&base_ages, 0.9),
        read_sec: profiles.iter().map(|profile| profile.read_sec).sum(),
        decode_sec: profiles.iter().map(|profile| profile.decode_sec).sum(),
        apply_sec: profiles.iter().map(|profile| profile.apply_sec).sum(),
        label_sec: profiles.iter().map(|profile| profile.label_sec).sum(),
    }
}

fn parse_horizons(raw: &str) -> Result<Vec<i64>> {
    let mut out = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        out.push(
            part.parse::<i64>()
                .with_context(|| format!("parse horizon {part}"))?,
        );
    }
    out.sort_unstable();
    out.dedup();
    if out.is_empty() {
        bail!("empty horizons");
    }
    Ok(out)
}

fn condition_bucket_idx(condition_id: &str, bucket_count: usize) -> usize {
    let mut h: u64 = 0xCBF29CE484222325;
    for byte in condition_id.as_bytes() {
        h ^= u64::from(*byte);
        h = h.wrapping_mul(0x100000001B3);
    }
    (h % bucket_count as u64) as usize
}

fn required_i64(value: &Value, key: &str) -> Result<i64> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("missing i64 field {key}"))
}

fn required_u64(value: &Value, key: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing u64 field {key}"))
}

fn quantile_sorted(values: &[f64], q: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let pos = ((values.len() - 1) as f64 * q).round() as usize;
    values.get(pos).copied()
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn write_csv<T: Serialize>(path: &Path, rows: &[T]) -> Result<()> {
    let mut writer =
        csv::Writer::from_path(path).with_context(|| format!("write {}", path.display()))?;
    for row in rows {
        writer.serialize(row)?;
    }
    writer.flush()?;
    Ok(())
}
