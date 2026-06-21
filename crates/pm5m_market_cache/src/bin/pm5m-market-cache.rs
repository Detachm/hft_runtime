use anyhow::Result;
use clap::{Parser, Subcommand};
use pm5m_market_cache::{
    bench_book_cache2, bench_book_state_index, build_book_cache, build_book_state_index,
    build_reference_cache, build_reference_cache_from_binance, build_settlement_cache,
    build_settlement_cache_from_clob, build_settlement_cache_from_condition_metadata,
    inspect_book_cache2_coverage, validate_book_cache2, validate_book_state_index,
    validate_reference_cache, validate_settlement_cache, BuildBookCacheOptions,
    BuildBookStateIndexOptions, BuildReferenceCacheFromBinanceOptions, BuildReferenceCacheOptions,
    BuildSettlementCacheFromClobOptions, BuildSettlementCacheFromConditionMetadataOptions,
    BuildSettlementCacheOptions,
};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "pm5m-market-cache")]
#[command(about = "PM5M HFTBOOK2 market cache tools")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    BuildBookCache {
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
        replay_workers: Option<usize>,
        #[arg(long)]
        enrich_missing_clob_metadata: bool,
        #[arg(long)]
        clob_metadata_cache_root: Option<PathBuf>,
        #[arg(long)]
        condition_allowlist: Option<PathBuf>,
        #[arg(long)]
        poly_server_visible_time: bool,
        #[arg(long, default_value_t = 20)]
        poly_incremental_latency_ms: i64,
        #[arg(long, default_value_t = 500)]
        poly_incremental_freshness_guard_ms: i64,
        #[arg(long)]
        overwrite: bool,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    ValidateBookCache {
        #[arg(long)]
        cache_root: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    BenchCache {
        #[arg(long)]
        book_cache_root: PathBuf,
        #[arg(long)]
        start_ts_ns: Option<i64>,
        #[arg(long)]
        end_ts_ns: Option<i64>,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    InspectCoverage {
        #[arg(long)]
        book_cache_root: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    BuildBookIndex {
        #[arg(long)]
        book_cache_root: PathBuf,
        #[arg(long)]
        index_root: PathBuf,
        #[arg(long)]
        start_ts_ns: Option<i64>,
        #[arg(long)]
        end_ts_ns: Option<i64>,
        #[arg(long)]
        overwrite: bool,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    ValidateBookIndex {
        #[arg(long)]
        index_root: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    BenchBookIndex {
        #[arg(long)]
        index_root: PathBuf,
        #[arg(long)]
        start_ts_ns: Option<i64>,
        #[arg(long)]
        end_ts_ns: Option<i64>,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    BuildReferenceCache {
        #[arg(long)]
        input_table: PathBuf,
        #[arg(long)]
        cache_root: PathBuf,
        #[arg(long)]
        overwrite: bool,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    BuildReferenceCacheFromBinance {
        #[arg(long)]
        cache_root: PathBuf,
        #[arg(long = "symbol", required = true)]
        symbols: Vec<String>,
        #[arg(long)]
        start_ts_ns: i64,
        #[arg(long)]
        end_ts_ns: i64,
        #[arg(long, default_value_t = 1000)]
        reference_latency_ms: i64,
        #[arg(long)]
        overwrite: bool,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    BuildSettlementCache {
        #[arg(long)]
        input_table: PathBuf,
        #[arg(long)]
        cache_root: PathBuf,
        #[arg(long)]
        book_cache_root: Option<PathBuf>,
        #[arg(long)]
        overwrite: bool,
        #[arg(long)]
        allow_unsettled: bool,
        #[arg(long)]
        filter_to_book_cache: bool,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    BuildSettlementCacheFromClob {
        #[arg(long)]
        input_table: Option<PathBuf>,
        #[arg(long)]
        cache_root: PathBuf,
        #[arg(long)]
        book_cache_root: PathBuf,
        #[arg(long)]
        overwrite: bool,
        #[arg(long, default_value_t = 16)]
        refresh_workers: usize,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    BuildSettlementCacheFromConditionMetadata {
        #[arg(long)]
        condition_metadata_root: PathBuf,
        #[arg(long)]
        cache_root: PathBuf,
        #[arg(long)]
        overwrite: bool,
        #[arg(long, default_value_t = 16)]
        refresh_workers: usize,
        #[arg(long)]
        allow_unresolved: bool,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    ValidateReferenceCache {
        #[arg(long)]
        cache_root: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    ValidateSettlementCache {
        #[arg(long)]
        cache_root: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::BuildBookCache {
            raw_roots,
            cache_root,
            raw_start_ts_ns,
            raw_end_ts_ns,
            market_symbols,
            replay_workers,
            enrich_missing_clob_metadata,
            clob_metadata_cache_root,
            condition_allowlist,
            poly_server_visible_time,
            poly_incremental_latency_ms,
            poly_incremental_freshness_guard_ms,
            overwrite,
            output,
        } => {
            let report = build_book_cache(&BuildBookCacheOptions {
                raw_roots,
                cache_root,
                raw_start_ts_ns,
                raw_end_ts_ns,
                market_symbol_allowlist: market_symbols,
                overwrite,
                replay_workers,
                enrich_missing_clob_metadata,
                clob_metadata_cache_root,
                condition_allowlist_path: condition_allowlist,
                poly_server_visible_time,
                poly_incremental_latency_ms,
                poly_incremental_freshness_guard_ms,
            })?;
            print_or_write_json(output.as_deref(), &report)?;
        }
        Command::ValidateBookCache { cache_root, output } => {
            let report = validate_book_cache2(&cache_root)?;
            print_or_write_json(output.as_deref(), &report)?;
        }
        Command::BenchCache {
            book_cache_root,
            start_ts_ns,
            end_ts_ns,
            output,
        } => {
            let report = bench_book_cache2(&book_cache_root, start_ts_ns, end_ts_ns)?;
            print_or_write_json(output.as_deref(), &report)?;
        }
        Command::InspectCoverage {
            book_cache_root,
            output,
        } => {
            let report = inspect_book_cache2_coverage(&book_cache_root)?;
            print_or_write_json(output.as_deref(), &report)?;
        }
        Command::BuildBookIndex {
            book_cache_root,
            index_root,
            start_ts_ns,
            end_ts_ns,
            overwrite,
            output,
        } => {
            let report = build_book_state_index(&BuildBookStateIndexOptions {
                book_cache_root,
                index_root,
                start_ts_ns,
                end_ts_ns,
                overwrite,
            })?;
            print_or_write_json(output.as_deref(), &report)?;
        }
        Command::ValidateBookIndex { index_root, output } => {
            let report = validate_book_state_index(&index_root)?;
            print_or_write_json(output.as_deref(), &report)?;
        }
        Command::BenchBookIndex {
            index_root,
            start_ts_ns,
            end_ts_ns,
            output,
        } => {
            let report = bench_book_state_index(&index_root, start_ts_ns, end_ts_ns)?;
            print_or_write_json(output.as_deref(), &report)?;
        }
        Command::BuildReferenceCache {
            input_table,
            cache_root,
            overwrite,
            output,
        } => {
            let report = build_reference_cache(&BuildReferenceCacheOptions {
                input_table,
                cache_root,
                overwrite,
            })?;
            print_or_write_json(output.as_deref(), &report)?;
        }
        Command::BuildReferenceCacheFromBinance {
            cache_root,
            symbols,
            start_ts_ns,
            end_ts_ns,
            reference_latency_ms,
            overwrite,
            output,
        } => {
            let report =
                build_reference_cache_from_binance(&BuildReferenceCacheFromBinanceOptions {
                    cache_root,
                    overwrite,
                    symbols,
                    start_ts_ns,
                    end_ts_ns,
                    reference_latency_ms,
                })?;
            print_or_write_json(output.as_deref(), &report)?;
        }
        Command::BuildSettlementCache {
            input_table,
            cache_root,
            book_cache_root,
            overwrite,
            allow_unsettled,
            filter_to_book_cache,
            output,
        } => {
            let report = build_settlement_cache(&BuildSettlementCacheOptions {
                input_table,
                cache_root,
                overwrite,
                allow_unsettled,
                book_cache_root,
                filter_to_book_cache,
            })?;
            print_or_write_json(output.as_deref(), &report)?;
        }
        Command::BuildSettlementCacheFromClob {
            input_table,
            cache_root,
            book_cache_root,
            overwrite,
            refresh_workers,
            output,
        } => {
            let report = build_settlement_cache_from_clob(&BuildSettlementCacheFromClobOptions {
                input_table,
                cache_root,
                overwrite,
                book_cache_root,
                refresh_workers,
            })?;
            print_or_write_json(output.as_deref(), &report)?;
        }
        Command::BuildSettlementCacheFromConditionMetadata {
            condition_metadata_root,
            cache_root,
            overwrite,
            refresh_workers,
            allow_unresolved,
            output,
        } => {
            let report = build_settlement_cache_from_condition_metadata(
                &BuildSettlementCacheFromConditionMetadataOptions {
                    condition_metadata_root,
                    cache_root,
                    overwrite,
                    refresh_workers,
                    allow_unresolved,
                },
            )?;
            print_or_write_json(output.as_deref(), &report)?;
        }
        Command::ValidateReferenceCache { cache_root, output } => {
            let report = validate_reference_cache(&cache_root)?;
            print_or_write_json(output.as_deref(), &report)?;
        }
        Command::ValidateSettlementCache { cache_root, output } => {
            let report = validate_settlement_cache(&cache_root)?;
            print_or_write_json(output.as_deref(), &report)?;
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
