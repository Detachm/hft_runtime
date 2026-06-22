# PM5M Runtime

This repository is the lean PM5M data and backtest stack for Polymarket up/down markets.

Start here:

- [docs/START_HERE_PM5M.md](docs/START_HERE_PM5M.md)
- [docs/MODULE_OWNERSHIP.md](docs/MODULE_OWNERSHIP.md)
- [docs/DATA_ROOTS.md](docs/DATA_ROOTS.md)
- [docs/pm5m_new_chain_retirement_and_acceptance.md](docs/pm5m_new_chain_retirement_and_acceptance.md)
- [docs/pm5m_backtest_results_0609_now.md](docs/pm5m_backtest_results_0609_now.md)
- [docs/poly_live_multi_strategy_architecture.md](docs/poly_live_multi_strategy_architecture.md)

## Current Production Chain

There is one active PM5M chain:

```text
Polymarket CLOB websocket
Binance 1s reference websocket
  -> HFTREC4 raw WS audit
  -> HFTBOOK2 strategy-neutral book_state cache, built offline from raw
  -> HFTIDX1 file-backed book_state index
  -> HFTREF1 reference cache
  -> HFTSETTLE1 official settlement cache
  -> pm5m-backtest run-fast
```

The raw WS audit is the source of truth. Repeated research and backtests should read
HFTBOOK2/HFTIDX1 and compact reference/settlement caches, not replay raw WS payloads.

Retired PM5M paths must not be reintroduced:

- HFTREC3 raw segments
- HFTBOOK1 book cache
- JSONL raw import/replay
- old `pm5m_research_engine`
- raw-window backtest scripts
- `pm5m-recorder run --config`

## Crates

Workspace crates:

- `market_data_etl_core`: shared IO, hashing, Parquet/ZSTD, HFTREC4, atomic writes.
- `pm5m_recorder`: live Polymarket discovery, sharded CLOB WS recording, and Binance reference WS audit profile recording.
- `pm5m_market_cache`: HFTBOOK2/HFTIDX1/HFTREF1/HFTSETTLE1 builders, validators, benches.
- `pm5m_data_etl`: Jupiter typed Parquet ETL/export contract. Not the fast backtest hot path.

Private crate:

- `hft_private`: local-only strategy and backtest code. It is intentionally ignored by git and
  outside the root workspace.

## Common Commands

Run the production recorders. These keep Poly and Binance recording independent, reduce subscription
surface, and write explicit coverage rows for live-vs-local alignment:

```sh
ROLE=poly_btc scripts/run_pm5m_recorder_supervised.sh
ROLE=poly_eth scripts/run_pm5m_recorder_supervised.sh
ROLE=poly_sol scripts/run_pm5m_recorder_supervised.sh
ROLE=reference_binance scripts/run_pm5m_recorder_supervised.sh
```

Build and validate the fast cache/index:

```sh
cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- build-book-cache \
  --raw-root "${PM5M_RAW_ROOT}" \
  --cache-root "${PM5M_BOOK_CACHE_ROOT}" \
  --overwrite

cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- validate-book-cache \
  --cache-root "${PM5M_BOOK_CACHE_ROOT}"

cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- build-book-index \
  --book-cache-root "${PM5M_BOOK_CACHE_ROOT}" \
  --index-root "${PM5M_BOOK_INDEX_ROOT}" \
  --overwrite

cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- validate-book-index \
  --index-root "${PM5M_BOOK_INDEX_ROOT}"
```

Run the fast backtest with the current ETH gray live-aligned config, when the local private strategy
checkout exists:

```sh
cargo run --release --manifest-path hft_private/Cargo.toml --bin pm5m_backtest -- run-fast \
  --book-cache-root "${PM5M_BOOK_CACHE_ROOT}" \
  --book-index-root "${PM5M_BOOK_INDEX_ROOT}" \
  --reference-cache-root "${PM5M_REFERENCE_CACHE_ROOT}" \
  --settlement-cache-root "${PM5M_SETTLEMENT_CACHE_ROOT}" \
  --config hft_private/configs/position_v4_edge_live_eth_gray_20260620.json \
  --output-dir "${PM5M_OUTPUT_ROOT}/run"
```

Static safety gate:

```sh
scripts/private/perf_gate_pm5m.sh --static-only
```

Full perf gate with real paths:

```sh
export PM5M_RAW_ROOT=/path/to/hftrec4/raw
export PM5M_BOOK_CACHE_ROOT=/path/to/hftbook2
export PM5M_BOOK_INDEX_ROOT=/path/to/hftidx1
export PM5M_REFERENCE_CACHE_ROOT=/path/to/hftref1
export PM5M_SETTLEMENT_CACHE_ROOT=/path/to/hftsettle1
export PM5M_CONFIG=/home/hliu/hft_runtime/hft_private/configs/position_v4_edge_live_eth_gray_20260620.json
export PM5M_OUTPUT_ROOT=/path/to/reports
scripts/private/perf_gate_pm5m.sh --required
```

## Verification

Before handing off changes:

```sh
cargo fmt --check
cargo test --workspace --no-fail-fast
scripts/private/perf_gate_pm5m.sh --static-only
```

Local private strategy checks are optional and require an ignored `hft_private/` checkout:

```sh
cargo test --manifest-path hft_private/Cargo.toml --all-targets --no-fail-fast
```
