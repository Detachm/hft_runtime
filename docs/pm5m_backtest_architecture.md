# PM5M Backtest Architecture

Status note, 2026-06-24 CST: this document describes the compact-cache architecture that existed
before the replay ETL refactor decision. For the current target architecture and implementation
order, use `docs/pm5m_replay_etl_refactor_plan.md`. In that plan, `HFTBOOK2/HFTIDX1` are retained as
the optional `Depth State Store / Replay State Index` path, not the default live-comparison or
large-window research path.

This document describes the current backtest architecture after the compact-cache cutover.

## Goal

The active research path must rerun full-window PM5M strategies in minutes or less by reading
strategy-neutral compact data:

```text
HFTBOOK2 + HFTIDX1 + HFTREF1 + HFTSETTLE1
  -> pm5m-backtest run-fast
  -> deterministic reports and hashes
```

The backtest does not replay raw WS payloads during normal iteration. HFTREC4 raw is kept for audit
and one-time cache rebuilds.

## Inputs

Required `run-fast` inputs:

- `--book-cache-root`: HFTBOOK2 cache root.
- `--book-index-root`: HFTIDX1 file-backed index root.
- `--reference-cache-root`: HFTREF1 reference cache root.
- `--settlement-cache-root`: HFTSETTLE1 official settlement cache root.
- `--config`: strategy config snapshot.

`run-fast` must fail closed on missing book, reference, or settlement data. It must not silently
fill missing settlement or use future book state.

## Time Semantics

Execution lookup must use visible state at arrival time:

```text
valid_from_ts_ns <= arrival_ts_ns < valid_until_ts_ns
```

Using the first book update after arrival is lookahead and is not allowed.

For live-like PM5M research, "visible state" means state visible to the AWS `poly` process, not
plain exchange time and not an arbitrary recorder's local receive time. The canonical model is in
`docs/pm5m_poly_server_time_alignment.md`: Binance 1s reference bars use close time + 200 ms;
Polymarket healthy `price_change` events may use exchange timestamp + 20 ms with a 500 ms freshness
guard; `book` snapshots stay on recorded receive time to avoid stale-snapshot lookahead.

## Strategy Boundary

The current strategy implementation lives in local-only ignored `hft_private` and is private by
design. Strategy fields, edge columns, PnL labels, and candidate subsets must not be written into
Jupiter production artifacts.

The active frozen strategy is buy-only and holds to settlement. There is no sell/exit path in the
normal model. Pair strategy support should build on the same cache/index interfaces but must keep
single-leg reporting separate from pair residual reporting.

## Hot Path Rules

The hot path must not:

- parse raw WS JSON;
- scan HFTREC4 raw;
- deserialize row-json compatibility columns;
- call ETL `build_facts` or `build_event_index`;
- materialize the whole book index in memory;
- use retired raw/cache formats.

The hot path should:

- use fixed-point price/size/notional/PnL;
- use `i128` for internal cash/notional/PnL accumulation where overflow matters;
- intern repeated condition, asset, and symbol strings before replay;
- use file-backed HFTIDX1 for book lookup;
- write deterministic run manifests and hash anchors.

## Outputs

Each run directory should contain:

- `summary.json`
- `run_manifest.json`
- `intent_groups.parquet`
- `fills.parquet`
- `group_ledger.parquet`
- `condition_ledger.parquet`
- `reject_reasons.json`
- `daily_pnl.json`
- `metrics_summary.json`
- `daily_metrics.json`
- `market_metrics.json`
- `daily_market_metrics.json`
- `condition_metrics.json`

The exact output set may grow, but summaries, manifests, configs, and stable hashes are the durable
research evidence.

`metrics_summary.json` is the canonical quick-read report. It includes condition count, fill count,
condition win rate, fill win rate, profit factor when defined, and final settlement PnL. Daily and
market metrics use the same schema. Condition PnL is attributed to the first fill day.

## Live Alignment

Before interpreting any live-aligned result, confirm whether the HFTBOOK2/HFTREF1 inputs used the
poly-server time model from `docs/pm5m_poly_server_time_alignment.md`.

Current live-aligned backtests should use
`hft_private/configs/position_v4_edge_live_eth_gray_20260620.json` unless the experiment explicitly
targets an older live run. This config mirrors the June 20 ETH gray run on the strategy-facing fields:
ETH only, 10 USDC per entry, max 3 entries per asset, 300 ms submit latency, conservative policy, and
Polymarket taker fee model.

This PM5M config is the current local ETH gray replay baseline. It is not the five-strategy Poly
cutover config; the live multi-strategy runtime state and reconciliation requirements are tracked in
`docs/poly_live_multi_strategy_architecture.md`.

Live runs write the audit streams needed to explain the three core mismatch classes:

- live filled, local did not: compare `order_audit`, `shadow_execution_checks`, local HFTIDX1 book
  state, and settlement.
- local filled, live did not: compare local intent rows against `shadow_intents` evaluations and
  `live_strategy_bar_coverage` records.
- both filled but PnL differs: compare `decision_key`, execution price/cash, fill ratio, settlement,
  and first-fill-day attribution.

`live_strategy_bar_coverage` is intentionally a compact per-reference-bar diagnostic. It answers
whether the live engine had any current market for the closed Binance bar, evaluated it, selected a
candidate, and emitted an intent, without adding a high-volume audit stream.

## Retired Compatibility

Older dataset/event-index loading code may exist only as explicit diagnostic compatibility. It is
not the current fast backtest path and must not be used for production research results.

If old accepted Parquet datasets are needed for comparison, label the run diagnostic and do not mix
those results with HFTIDX1 `run-fast` results.
