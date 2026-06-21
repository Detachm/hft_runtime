use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use pm5m_data_etl::{
    accept, build_book_state_cache, build_depth_feature, build_event_index, build_facts,
    export_dataset, load_plan, prepare_caches_with_options, sync_inputs, write_plan,
    BuildBookStateCacheOptions, CachePreparationOptions, CacheSourceSpec,
    DefaultCachePreparationHttp, DefaultFetcher, PipelinePlan,
};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "pm5m-data-etl")]
#[command(about = "Jupiter-side PM5M data factory")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Plan {
        #[arg(long = "raw-root")]
        raw_roots: Vec<PathBuf>,
        #[arg(long)]
        plan_root: PathBuf,
        #[arg(long)]
        cache_root: PathBuf,
        #[arg(long)]
        dataset_root: PathBuf,
        #[arg(long)]
        book_state_cache_root: Option<PathBuf>,
        #[arg(long)]
        raw_start_ts_ns: Option<i64>,
        #[arg(long)]
        raw_end_ts_ns: Option<i64>,
        #[arg(long, default_value_t = 1_000)]
        reference_latency_ms: i64,
        #[arg(long)]
        fail_closed_on_missing_reference: bool,
        #[arg(long)]
        allow_unsettled_settlement: bool,
        #[arg(long = "market-symbol")]
        market_symbols: Vec<String>,
        #[arg(long = "binance-source", value_parser = parse_source_arg)]
        binance_sources: Vec<CacheSourceSpec>,
        #[arg(long = "settlement-source", value_parser = parse_source_arg)]
        settlement_sources: Vec<CacheSourceSpec>,
    },
    SyncInputs {
        #[arg(long)]
        plan: PathBuf,
    },
    PrepareCaches {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        refresh_reference: bool,
        #[arg(long)]
        refresh_settlement: bool,
        #[arg(long)]
        refresh_unsettled: bool,
    },
    BuildBookStateCache {
        #[arg(long = "raw-root", required = true)]
        raw_roots: Vec<PathBuf>,
        #[arg(long)]
        cache_root: PathBuf,
        #[arg(long)]
        raw_start_ts_ns: Option<i64>,
        #[arg(long)]
        raw_end_ts_ns: Option<i64>,
        #[arg(long = "market-symbol")]
        market_symbols: Vec<String>,
        #[arg(long)]
        overwrite: bool,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    BuildFacts {
        #[arg(long)]
        plan: PathBuf,
    },
    BuildDepthFeature {
        #[arg(long)]
        plan: PathBuf,
    },
    BuildEventIndex {
        #[arg(long)]
        plan: PathBuf,
    },
    Accept {
        #[arg(long)]
        plan: PathBuf,
    },
    Export {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        export_root: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Plan {
            raw_roots,
            plan_root,
            cache_root,
            dataset_root,
            book_state_cache_root,
            raw_start_ts_ns,
            raw_end_ts_ns,
            reference_latency_ms,
            fail_closed_on_missing_reference,
            allow_unsettled_settlement,
            market_symbols,
            binance_sources,
            settlement_sources,
        } => {
            if raw_roots.is_empty() && book_state_cache_root.is_none() {
                return Err(anyhow!(
                    "plan requires --raw-root unless --book-state-cache-root is set"
                ));
            }
            let mut plan = PipelinePlan::new(raw_roots, plan_root, cache_root, dataset_root);
            plan.book_state_cache_root = book_state_cache_root;
            plan.raw_start_ts_ns = raw_start_ts_ns;
            plan.raw_end_ts_ns = raw_end_ts_ns;
            plan.reference_latency_ms = reference_latency_ms;
            plan.accept_fail_closed_on_missing_reference = fail_closed_on_missing_reference;
            plan.accept_fail_closed_on_unsettled_settlement = !allow_unsettled_settlement;
            plan.market_symbol_allowlist = market_symbols;
            plan.binance_sources = binance_sources;
            plan.settlement_sources = settlement_sources;
            let path = write_plan(&plan)?;
            println!("{}", path.display());
        }
        Command::SyncInputs { plan } => {
            let plan = load_plan(&plan)?;
            let manifest = sync_inputs(&plan, &DefaultFetcher)?;
            println!("synced {} cache source(s)", manifest.records.len());
        }
        Command::PrepareCaches {
            plan,
            refresh_reference,
            refresh_settlement,
            refresh_unsettled,
        } => {
            let plan = load_plan(&plan)?;
            let http = DefaultCachePreparationHttp::new()?;
            let manifest = prepare_caches_with_options(
                &plan,
                &http,
                CachePreparationOptions {
                    refresh_reference,
                    refresh_settlement,
                    refresh_unsettled,
                },
            )?;
            println!("prepared {} cache source(s)", manifest.records.len());
        }
        Command::BuildBookStateCache {
            raw_roots,
            cache_root,
            raw_start_ts_ns,
            raw_end_ts_ns,
            market_symbols,
            overwrite,
            output,
        } => {
            let report = build_book_state_cache(&BuildBookStateCacheOptions {
                raw_roots,
                cache_root,
                raw_start_ts_ns,
                raw_end_ts_ns,
                market_symbol_allowlist: market_symbols,
                overwrite,
            })?;
            print_or_write_json(output.as_deref(), &report)?;
        }
        Command::BuildFacts { plan } => {
            let plan = load_plan(&plan)?;
            let report = build_facts(&plan)?;
            println!("materialized {} fact table(s)", report.table_hashes.len());
        }
        Command::BuildDepthFeature { plan } => {
            let plan = load_plan(&plan)?;
            let rows = build_depth_feature(&plan)?;
            println!("built {} depth feature row(s)", rows.len());
        }
        Command::BuildEventIndex { plan } => {
            let plan = load_plan(&plan)?;
            let row_count = build_event_index(&plan)?;
            println!("built {row_count} event index row(s)");
        }
        Command::Accept { plan } => {
            let plan = load_plan(&plan)?;
            accept(&plan)?;
            println!("accepted");
        }
        Command::Export { plan, export_root } => {
            let plan = load_plan(&plan)?;
            export_dataset(&plan, &export_root)?;
            println!("{}", export_root.display());
        }
    }
    Ok(())
}

fn print_or_write_json<T: serde::Serialize>(
    output: Option<&std::path::Path>,
    value: &T,
) -> Result<()> {
    if let Some(path) = output {
        market_data_etl_core::write_json_file_pretty(path, value)?;
        println!("{}", path.display());
    } else {
        println!("{}", serde_json::to_string_pretty(value)?);
    }
    Ok(())
}

fn parse_source_arg(input: &str) -> Result<CacheSourceSpec> {
    let mut map = BTreeMap::new();
    for pair in input.split(',') {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| anyhow!("source arg pair '{}' must be key=value", pair))?;
        map.insert(key.trim().to_string(), value.trim().to_string());
    }

    let name = take_required(&mut map, "name")?;
    let source_url = take_required(&mut map, "url")?;
    let symbol = map.remove("symbol").filter(|value| !value.is_empty());
    let start_ts_ns = map
        .remove("start")
        .unwrap_or_else(|| "0".to_string())
        .parse::<i64>()
        .context("parse source start")?;
    let end_ts_ns = map
        .remove("end")
        .unwrap_or_else(|| "0".to_string())
        .parse::<i64>()
        .context("parse source end")?;

    if !map.is_empty() {
        return Err(anyhow!(
            "unknown source arg keys: {}",
            map.keys().cloned().collect::<Vec<_>>().join(",")
        ));
    }

    Ok(CacheSourceSpec {
        name,
        source_url,
        symbol,
        start_ts_ns,
        end_ts_ns,
    })
}

fn take_required(map: &mut BTreeMap<String, String>, key: &str) -> Result<String> {
    map.remove(key)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("source arg missing required key '{}'", key))
}
