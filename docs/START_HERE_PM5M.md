# PM5M Start Here

This is the first document a new agent should read.

## One Sentence

PM5M records Polymarket CLOB WS raw data for audit, converts it once into strategy-neutral compact
book state, indexes that book state, and runs private strategies against the index plus local
reference and settlement caches.

## Active Flow

```text
pm5m-recorder run-ws
  runs one Polymarket CLOB HFTREC4 raw recorder per active symbol
  records BTC/ETH/SOL 5m/15m with structured coverage control rows

pm5m-recorder run-reference-ws
  records Binance BTC/ETH/SOL 1s reference events and audit_profile.json

pm5m-market-cache
  builds/validates HFTBOOK2 from HFTREC4 offline
  builds/validates HFTIDX1 from HFTBOOK2
  builds/validates HFTREF1 and HFTSETTLE1

pm5m-backtest run-fast
  reads HFTIDX1 + HFTREF1 + HFTSETTLE1
  writes deterministic run reports and hashes
```

## What Not To Use

Do not use or recreate these for PM5M production or research:

- HFTREC3 raw segments
- HFTBOOK1 cache
- JSONL raw replay/import
- `pm5m_research_engine`
- old raw-window backtest scripts
- `pm5m-recorder run --config`
- depth-feature event index as a fast-backtest input

## Minimum Research Loop

1. Make sure the four recorder roles are writing HFTREC4 raw: `poly_btc`, `poly_eth`,
   `poly_sol`, and `reference_binance`.
2. For live-like research, apply `docs/pm5m_poly_server_time_alignment.md` before building
   HFTBOOK2/HFTREF1.
3. Build or refresh HFTIDX1 from the HFTBOOK2 root.
4. Build or validate HFTREF1 and HFTSETTLE1 for the same time window.
5. Run `pm5m-backtest run-fast` with the config snapshot that matches the live experiment.
6. Compare `summary.json`, `metrics_summary.json`, `daily_metrics.json`, `market_metrics.json`,
   `condition_metrics.json`, `run_manifest.json`, and hash anchors.
7. Append the experiment to the daily research log under `docs/research_logs/YYYY-MM-DD.md`.

## Canonical Commands

Production recorders:

```sh
ROLE=poly_btc scripts/run_pm5m_recorder_supervised.sh
ROLE=poly_eth scripts/run_pm5m_recorder_supervised.sh
ROLE=poly_sol scripts/run_pm5m_recorder_supervised.sh
ROLE=reference_binance scripts/run_pm5m_recorder_supervised.sh
```

Backtest, current ETH gray live-aligned config, when the local private `hft_private/` checkout
exists:

```sh
cargo run --release --manifest-path hft_private/Cargo.toml --bin pm5m_backtest -- run-fast \
  --book-cache-root "${PM5M_BOOK_CACHE_ROOT}" \
  --book-index-root "${PM5M_BOOK_INDEX_ROOT}" \
  --reference-cache-root "${PM5M_REFERENCE_CACHE_ROOT}" \
  --settlement-cache-root "${PM5M_SETTLEMENT_CACHE_ROOT}" \
  --config hft_private/configs/position_v4_edge_live_eth_gray_20260620.json \
  --output-dir "${PM5M_OUTPUT_ROOT}/run"
```

Static gate:

```sh
scripts/private/perf_gate_pm5m.sh --static-only
```

## Where To Work

- Recorder/data capture work: `crates/pm5m_recorder`
- Compact format and performance work: `crates/pm5m_market_cache`
- Typed Parquet export/acceptance work: `crates/pm5m_data_etl`
- Strategy/backtest work: local-only ignored `hft_private`
- Shared IO/hashing/HFTREC4 work: `crates/market_data_etl_core`

## Current Result Docs

- `docs/RESEARCH_LOG_POLICY.md`: append-only daily research log rules. Each experiment uses an ID
  like `YYYY-MM-DD-EXP-NNN`; old entries must not be edited.
- `docs/research_logs/`: daily append-only research logs.
- `docs/pm5m_poly_server_time_alignment.md`: canonical PM5M timing model for simulating what the
  AWS `poly` server could see: Poly incremental visible time, Binance reference latency, and the
  stale-snapshot guard.
- `docs/pm5m_backtest_results_0609_now.md`: 2026-06-09 through current local-data full backtest
  result, data coverage gaps, daily PnL, market PnL, and counterfactual reference.
- `docs/pm5m_live_vs_backtest_0609_0611_live_only_analysis.md`: 2026-06-09 to 2026-06-11
  live-vs-backtest mismatch analysis.
- `docs/poly_live_multi_strategy_architecture.md`: canonical Poly live multi-strategy upgrade plan,
  current implementation state, strategy slots, risk parameters, hot-path/side-path separation,
  audit records, and local replay/reconciliation requirements.

Local runtime logs, state, and generated data belong under `runtime/` or `/mnt/data/...`; they are
not source-of-truth code.
