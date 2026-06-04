use anyhow::Result;
use clap::{Parser, Subcommand};
use market_data_etl_core::write_json_file_pretty;
use pm5m_recorder::{
    run_forever, run_once, BlockingHttpFetcher, RecorderConfig, RECORDER_CONFIG_FORMAT,
};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "pm5m-recorder")]
#[command(about = "Jupiter-side continuous raw market recorder")]
struct Cli {
    #[command(subcommand)]
    command: Command,
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
        #[arg(long = "filter")]
        question_or_slug_contains_any: Vec<String>,
    },
    Run {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        once: bool,
        #[arg(long)]
        max_cycles: Option<u64>,
    },
    Status {
        #[arg(long)]
        state_root: PathBuf,
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
            question_or_slug_contains_any,
        } => {
            let mut config = RecorderConfig::default_for_roots(raw_root, state_root);
            config.poll_interval_ms = poll_interval_ms;
            config.max_assets_per_cycle = max_assets_per_cycle;
            config.source.discovery.question_or_slug_contains_any = question_or_slug_contains_any;
            write_json_file_pretty(&path, &config)?;
            println!("{}", path.display());
        }
        Command::Run {
            config,
            once,
            max_cycles,
        } => {
            let config = load_config(&config)?;
            if once {
                let report = run_once(&config, &BlockingHttpFetcher)?;
                println!(
                    "rows={} assets={} errors={}",
                    report.rows_written,
                    report.assets_recorded,
                    report.errors.len()
                );
            } else if let Some(max_cycles) = max_cycles {
                for _ in 0..max_cycles {
                    let report = run_once(&config, &BlockingHttpFetcher)?;
                    println!(
                        "rows={} assets={} errors={}",
                        report.rows_written,
                        report.assets_recorded,
                        report.errors.len()
                    );
                    std::thread::sleep(std::time::Duration::from_millis(config.poll_interval_ms));
                }
            } else {
                run_forever(&config, &BlockingHttpFetcher)?;
            }
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
    Ok(config)
}
