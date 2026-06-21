# PM5M Module Ownership

This file defines the active ownership boundaries. Use it before changing code.

## `crates/market_data_etl_core`

Shared, strategy-neutral primitives:

- atomic verified writes
- filesystem helpers
- hashing
- Parquet/ZSTD typed table helpers
- HFTREC4 raw segment writer/reader/verifier
- small JSONL helpers for fixtures and diagnostics only

It must not contain PM5M strategy logic or retired raw formats.

## `crates/pm5m_recorder`

Live recording only:

- discovers Polymarket PM5M markets through Gamma/CLOB metadata
- subscribes to Polymarket CLOB market websocket
- writes HFTREC4 WS raw audit
- optionally double-writes HFTBOOK2 book-state cache
- records Binance reference WS events and recorder audit profile when run through `run-dual`
- maintains recorder state and evidence

It must not depend on `pm5m_data_etl`, run backtests, build research features, or write old raw
formats. HTTP code here is discovery/metadata support, not the old HTTP book recorder.

## `crates/pm5m_market_cache`

High-performance strategy-neutral data layer:

- builds HFTBOOK2 from HFTREC4
- validates and benchmarks HFTBOOK2
- builds HFTIDX1 file-backed index
- validates and benchmarks HFTIDX1
- builds HFTREF1 and HFTSETTLE1 compact caches

Backtest performance work should start here before touching strategy code.

## `crates/pm5m_data_etl`

Jupiter typed Parquet contract:

- builds fact tables from HFTBOOK2/book-state cache
- prepares reference/settlement inputs when needed
- exports/accepts typed Parquet datasets
- keeps diagnostic depth-feature support out of the fast-backtest hot path

It is not the normal input path for `pm5m-backtest run-fast`.

## `hft_private`

Local-only private research and strategy backtest crate:

- strategy config and model logic
- fast dataset loading from HFTBOOK2/HFTIDX1/HFTREF1/HFTSETTLE1
- execution simulation
- ledger and report generation
- deterministic hashes and benchmark output

It is intentionally outside the root workspace, ignored by git, and must not enter production
artifacts.

## Scripts

- `scripts/run_pm5m_recorder_supervised.sh`: long-running dual CLOB/reference recorder wrapper.
- `scripts/private/perf_gate_pm5m.sh`: static and real-path performance gate.
- `scripts/check_production_artifact_boundary.sh`: package boundary guard.

Scripts must call current CLIs only. In particular, recorder scripts must use `pm5m-recorder run-dual`.
