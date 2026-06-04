use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use pm5m_data_etl::{
    accept, build_depth_feature, build_event_index, build_facts, export_dataset, load_plan,
    sync_inputs, write_plan, CacheSourceSpec, DefaultFetcher, PipelinePlan,
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
        #[arg(long = "raw-root", required = true)]
        raw_roots: Vec<PathBuf>,
        #[arg(long)]
        plan_root: PathBuf,
        #[arg(long)]
        cache_root: PathBuf,
        #[arg(long)]
        dataset_root: PathBuf,
        #[arg(long, default_value_t = 1_000)]
        reference_latency_ms: i64,
        #[arg(long)]
        fail_closed_on_missing_reference: bool,
        #[arg(long = "binance-source", value_parser = parse_source_arg)]
        binance_sources: Vec<CacheSourceSpec>,
        #[arg(long = "settlement-source", value_parser = parse_source_arg)]
        settlement_sources: Vec<CacheSourceSpec>,
    },
    SyncInputs {
        #[arg(long)]
        plan: PathBuf,
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
            reference_latency_ms,
            fail_closed_on_missing_reference,
            binance_sources,
            settlement_sources,
        } => {
            let mut plan = PipelinePlan::new(raw_roots, plan_root, cache_root, dataset_root);
            plan.reference_latency_ms = reference_latency_ms;
            plan.accept_fail_closed_on_missing_reference = fail_closed_on_missing_reference;
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
            let rows = build_event_index(&plan)?;
            println!("built {} event index row(s)", rows.len());
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
