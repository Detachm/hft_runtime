# Poly Live Multi-Strategy Architecture Upgrade Plan

Date: 2026-06-21 CST

Status: implementation target plus current-state snapshot. This document is the canonical plan for
upgrading the Poly live runtime from a single ETH PM5M trader into a multi-strategy runtime that is
auditable, replayable, and locally reconcilable without putting heavy work on the order path.

Current state as of 2026-06-21:

- Poly production is still the old single ETH PM5M `pm5m-live-trader` config with no
  `strategy_instances`.
- The multi-strategy implementation exists on branch `codex/poly-multi-strategy-runtime`, but has
  not been switched to real multi-strategy live orders.
- The branch has strategy-scoped config, PM5M/PM15M strategy instances, PM15M live/gray order
  emission enabled at code level, centralized risk/execution, strategy-scoped audit keys, and a
  generic side-path run comparator.
- A live cutover still requires an explicit config promotion and operator confirmation.

## First Principles

Live trading PnL is produced by four functions:

```text
PnL = Decision(visible market state)
    + Risk(current portfolio state)
    + Execution(actual order arrival and matching)
    + Settlement(final market outcome)
```

The architecture only needs to preserve and replay those four functions. Anything that does not
improve live ordering, auditability, replayability, or local reconciliation is out of scope.

Non-negotiable rules:

- The live runtime trades against what the Poly server can see, not against a later local view.
- Backtest/replay must use the same strategy kernel as live.
- Portfolio risk and execution must be centralized across strategies.
- The hot path does no disk IO, compression, database writes, large snapshot serialization, or local
  replay work.
- Critical audit is part of risk control. If critical audit cannot be enqueued, new orders stop.
- Full reconstruction uses event journals and source pointers; the order path only emits compact
  trace records.

## Strategy Slots

The runtime must reserve explicit strategy instances before adding the four new live candidates.
Each strategy instance has an immutable `strategy_instance_id` in every decision, risk, execution,
shadow, settlement, and replay record.

| strategy_instance_id | Family | Symbol | Initial mode | Order size | Max entries | Active cap | Notes |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| `eth_5m_main` | PM5M | ETH | live | 10 USDC | 3 | 30 USDC | Existing live strategy. |
| `btc_5m_probe` | PM5M | BTC | shadow -> gray | 5 USDC | 3 | 15 USDC | Promote only after replay comparator is clean. |
| `eth_15m_cap5` | PM15M | ETH | shadow -> gray | 5 USDC | 5 | 25 USDC | Best 15m production candidate. |
| `sol_15m_cap5` | PM15M | SOL | shadow -> gray | 3 USDC | 5 | 15 USDC | Can later test 5 USDC x 5 after stable gray. |
| `btc_15m_cap2` | PM15M | BTC | shadow/probe | 1 USDC | 2 | 2 USDC | Probe only; acceptable to keep disabled. |

Initial target gray active risk with all five enabled is about 87 USDC:

```text
ETH 5m  30
BTC 5m  15
ETH 15m 25
SOL 15m 15
BTC 15m  2
Total    87 USDC
```

This is active market risk, not total configured capital. Current capital assumption is 500 USDC.

## Strategy Logic To Preserve

The new live strategies must preserve the signal and position logic from the accepted backtests.
Live gray sizing can be smaller than the research sizing, but signal generation, direction locking,
edge checks, add rules, and settlement behavior must not drift.

### Shared Signal Primitive

Both PM5M and PM15M use the same reference primitive:

- anchor price: latest closed primary reference bar at or before `window_start`;
- current price: latest closed primary reference bar visible at decision time;
- volatility: realized sigma from the latest 60 closed 1s bars, falling back to 180 bars;
- model probability: existing time-aware probability model using anchor, current, sigma, and time
  left to `window_end`;
- side probability: `YES = p_up_model`, `NO = 1 - p_up_model`;
- raw edge: `side_probability - average_buy_price`;
- decision limit: `floor_to_tick(side_probability - raw_edge_threshold)`.

The replay truth for real Poly runs is `local_recv_ts_ns`. Fixed synthetic delays are only for
historical research when Poly-side receive timestamps do not exist.

### PM5M Strategy Instances

`eth_5m_main` must keep the current live ETH 5m behavior:

- market duration: 300 seconds;
- primary reference: Binance 1s;
- raw edge threshold: 0.20;
- order type: FAK taker buy;
- live gray size: 10 USDC;
- max entries: 3 per condition/outcome;
- active cap: 30 USDC per market;
- min entry seconds to end: 60;
- max book age: 1000 ms;
- max reference age: 2000 ms;
- policy: ETH selected side must have positive 60s side momentum;
- hold to settlement, no live exit sell.

`btc_5m_probe` must match the accepted BTC 5m backtest logic before it is enabled:

- market duration: 300 seconds;
- raw edge threshold: 0.20;
- max entries: 3 per condition/outcome;
- no accidental inherited `btc_disabled` gate in the shared strategy core;
- live gray size: 5 USDC, active cap 15 USDC;
- start shadow-only, then gray only after comparator output is clean.

The existing hardcoded BTC rejection in the PM5M live shadow strategy must become an explicit
per-strategy-instance enable/disable setting. Policy should be config-driven, not symbol-hardcoded
in a way that prevents BTC replay from matching BTC live.

### PM15M Strategy Instances

The PM15M live strategy must match the `cheap_chip_sweep_15m` accepted backtest logic:

- market duration: 900 seconds;
- raw edge threshold: 0.20;
- policy variant: conservative;
- first valid signal locks one direction per condition;
- after lock, only buy the locked outcome;
- never buy the opposite side as a hedge;
- every add must re-confirm current edge and current book depth;
- dynamic edge-only limit: effective limit is `p_model - raw_edge_threshold`;
- fixed price caps such as 0.30/0.40/0.50 are not production logic;
- max entries per condition is enforced by the centralized risk gate and should count successful
  filled execution groups, matching the accepted backtest definition;
- min entry seconds to end: 60;
- max book age: 1000 ms;
- max reference age: 2000 ms;
- replay submit latency assumption: 300 ms;
- live execution: FAK taker buy;
- hold to settlement, no exit sell.

PM15M symbol-specific gates:

- BTC: allow the backtested BTC direction logic, but keep `btc_15m_cap2` shadow/probe unless daily
  distribution materially improves.
- ETH: selected side must have positive 60s side momentum.
- SOL: only YES is eligible; conservative mode also requires positive BTC 60s lead momentum.

PM15M live gray risk settings:

| strategy_instance_id | Logic cap | Research sizing | Initial live gray sizing |
| --- | ---: | ---: | ---: |
| `eth_15m_cap5` | 5 entries | 10 USDC per entry in the count-cap backtest | 5 USDC per entry |
| `sol_15m_cap5` | 5 entries | 10 USDC per entry in the count-cap backtest | 3 USDC per entry |
| `btc_15m_cap2` | 2 entries | 10 USDC per entry in the count-cap backtest | 1 USDC per entry |

The smaller live sizing is a risk choice. It must not change the signal, lock, add, or settlement
rules.

## Risk Parameters

Risk controls are split into operational stops and strategy PnL stops.

Operational stops are strict and should block new orders immediately:

- `audit_degraded == true`
- market data/reference feed stale beyond configured freshness
- execution queue wait beyond limit, unless explicitly observe-only
- too many uncertain orders
- too many consecutive submit failures
- local portfolio ledger cannot reconcile with exchange state
- kill switch file present, if configured

Strategy PnL stops should not be too tight. One wrong prediction can lose the full active cap for
that market, and 3-4 consecutive misses are normal for this strategy family.

Portfolio-level stress-loss controls for the initial 500 USDC capital plan:

| Control | Threshold | Action |
| --- | ---: | --- |
| Soft | `0.6 * catastrophe_drawdown_usdc` = 150 USDC | Pause trial strategies for 3 hours; primary strategy can continue. |
| Hard | `0.8 * catastrophe_drawdown_usdc` = 200 USDC | Block trial strategies; primary strategy can continue below catastrophe. |
| Catastrophe | `catastrophe_drawdown_usdc` = 250 USDC | Stop all live trading and reconcile account state. |

Initial per-strategy sizing and exposure caps:

| strategy_instance_id | Order size | Active cap | Role |
| --- | ---: | ---: | --- |
| `eth_5m_main` | 10 USDC | 30 USDC | Primary |
| `btc_5m_probe` | 5 USDC | 15 USDC | Trial |
| `eth_15m_cap5` | 5 USDC | 25 USDC | Trial |
| `sol_15m_cap5` | 3 USDC | 15 USDC | Trial |
| `btc_15m_cap2` | 1 USDC | 2 USDC | Trial |

Promotion rule:

- New strategies start shadow-only for at least one full trading day.
- Move to gray only after local replay comparator classifies all material differences.
- BTC 15m is not promoted unless its daily distribution improves materially.

## Poly Runtime Target

Use one live runtime with multiple strategy instances sharing the same market state, risk ledger, and
execution adapter.

```text
Feed tasks -> In-memory Book/Reference State
                         |
                         v
              Strategy Instances
                         |
                         v
                 Central Risk Gate
                         |
                         v
                 Execution Adapter
                         |
                         v
              In-memory Portfolio Ledger

Side path: event journal, decision trace, risk trace, execution trace, shadow checks, settlement,
latency summaries, and local sync.
```

If separate processes are used temporarily, they must still share a central risk/condition lock.
Independent live processes with independent risk books are not acceptable for production.

## Hot Path

The synchronous trading path is intentionally small:

1. Read the latest in-memory book/reference state.
2. Run the deterministic strategy kernel for enabled strategy instances.
3. Build compact `OrderIntent`s.
4. Run central in-memory risk checks.
5. Submit FAK order through the execution adapter.
6. Update in-memory pending/filled/uncertain ledger from the execution result.
7. Enqueue compact audit events with non-blocking `try_send`.

The hot path must not:

- write files;
- compress logs;
- query local databases;
- build full order book snapshots;
- run local replay/comparison;
- block on shadow execution checks;
- block on settlement polling;
- make non-exchange network calls.

The only network call on the order path is the exchange order request.

## Side Path

All non-ordering work runs outside the hot path:

- raw Poly event journal writer;
- reference event journal writer;
- compact decision/risk/execution audit writer;
- shadow execution check scheduler;
- latency summary writer;
- settlement poller;
- account reconciliation;
- run artifact sync to local;
- local replay and comparator.

Side-path queues are bounded. Critical audit queues use this rule:

```text
try_send fails -> audit_degraded -> block future orders
```

Non-critical metrics can be dropped and counted.

## Poly Implementation Details

The Poly runtime should evolve by adding strategy instances and shared core components around the
current live trader, not by adding four independent trader scripts.

### Config

Add a top-level `strategy_instances` array. Keep the current single-strategy fields temporarily for
backward compatibility by normalizing them into `eth_5m_main`.

Target shape:

```json
{
  "strategy_instances": [
    {
      "strategy_instance_id": "eth_5m_main",
      "family": "pm5m",
      "symbol": "ETH",
      "mode": "live",
      "buy_amount_usdc": 10.0,
      "max_entries_per_condition_outcome": 3,
      "active_cap_usdc": 30.0,
      "raw_edge_threshold": 0.2,
      "min_entry_seconds_to_end": 60.0
    }
  ],
  "portfolio_risk": {
    "capital_usdc": 500.0,
    "catastrophe_drawdown_usdc": 250.0
  }
}
```

Implementation notes:

- `symbols` becomes derived from enabled strategy instances.
- `reference_venues`, feed URLs, logger settings, and execution settings remain process-level.
- Strategy-family defaults live in code; instance config only overrides live risk and gates.
- Every config normalization step must be hash-stable so `config_hash` remains replayable.

### Intent and Keys

Upgrade `OrderIntent` from PM5M-only schema to strategy-scoped schema.

Add fields:

- `strategy_instance_id`
- `strategy_family`
- `market_duration_seconds`
- `decision_key_v2`
- `decision_trace_id`
- `strategy_config_hash`

Use:

```text
decision_key_v2 =
  {family_key}:{strategy_instance_id}:{symbol}:{bar_open_time_ms}:{condition_id}:{outcome}:{repeat_index}
```

Keep the old `decision_key` for one migration window, but new code and local comparator must join
on `strategy_instance_id + decision_key_v2`. Current code uses `family_key=pm5m_v2` for PM5M and
`family_key=pm15m_v1` for PM15M.

### Shared Strategy Core

Create a strategy core interface used by both live and replay:

```rust
trait StrategyKernel {
    fn evaluate(
        &self,
        instance: &StrategyInstanceConfig,
        market: &MarketView,
        reference: &ReferenceView,
        book: &BookView,
        risk_view: &RiskReadView,
        now_ns: i64,
    ) -> DecisionTrace;
}
```

`DecisionTrace` contains all selected and rejected candidates. `OrderIntent`s are derived from
selected trace candidates after repeat/cap assignment.

Required kernels:

- `Pm5mEdgeKernel`
- `Pm15mDirectionLockKernel`

The PM15M kernel owns the per-condition direction lock state. The lock state is deterministic:

- key: `strategy_instance_id + condition_id`;
- value: locked outcome, asset id, lock timestamp, lock p-model, lock raw edge;
- first eligible candidate by highest raw edge wins;
- tie-breaker must be stable and documented.

### Runner

One reference/book update can fan out to multiple strategy instances:

1. Determine active markets for each instance by family, symbol, and market duration.
2. Evaluate only instances whose market window is active and whose mode is not disabled.
3. Write compact `decision_trace` records to the audit queue.
4. Emit `OrderIntent`s only for live/gray instances.
5. Send all intents to the central risk gate.

Shadow-only instances must produce traces and shadow checks but no exchange orders.

### Central Risk

Replace per-strategy risk assumptions with one portfolio ledger keyed by:

- account;
- strategy instance;
- symbol;
- condition;
- condition/outcome;
- uncertain order id.

Risk check order:

1. operational stops;
2. strategy mode and enable flag;
3. strategy active cap;
4. condition/outcome cap;
5. symbol cap, if configured;
6. portfolio active exposure cap;
7. portfolio stress-loss soft/hard/catastrophe tiers;
8. uncertain order limit;
9. repeat/count caps.

Soft and hard tiers restrict trial/probe strategies first; the primary strategy can continue below
catastrophe. Catastrophe stops all new live orders.

### Execution

Use one execution adapter for all strategy instances:

- accepted intents enter one bounded execution queue;
- live order concurrency remains process-level;
- request signing, FAK submission, response parsing, and timeout reconciliation stay centralized;
- execution trace always includes `strategy_instance_id` and `decision_key_v2`;
- portfolio ledger updates from actual execution reports, not from optimistic submitted intents.

### Audit Logger

Keep the current asynchronous logger pattern. Add or rename streams only where necessary:

- `decision_traces.jsonl.zst` or existing `shadow_intents` with `event_type=decision_trace`;
- `order_intents.jsonl.zst`, if intents are separated from decision traces;
- `risk_events.jsonl.zst`;
- `order_audit.jsonl.zst`;
- `shadow_execution_checks.jsonl.zst`;
- `settlement_trace.jsonl.zst`;
- market/reference event journals.

Critical audit streams:

- run manifest;
- decision trace for selected intents;
- order intent;
- risk trace;
- execution trace;
- settlement trace.

If a selected intent's critical audit cannot be enqueued, new live orders must be blocked through
`audit_degraded`.

## Required Record Types

The goal is complete replay and reconciliation with minimal records. Do not log full book snapshots
on every decision unless diagnosing a specific incident.

### run_manifest.json

Required fields:

- `run_id`
- `start_time_ns`
- `hostname`
- `region`
- `git_sha`
- `binary_sha256`
- `config_hash`
- `audit_profile_hash`
- `strategy_instances`
- `risk_profile`
- `execution_profile`
- `schema_version`

### market_event_journal

Purpose: reconstruct what the Poly server could see.

Required fields:

- `local_recv_ts_ns`
- `source_id`
- `connection_id`
- `subscription_epoch`
- `ingest_seq`
- `asset_id`
- `condition_id`
- `event_type`
- `exchange_ts_ms` when present
- `raw_payload_sha256`
- `raw_record_hash`

### reference_event_journal

Purpose: reconstruct Binance/OKX reference visibility.

Required fields:

- `local_recv_ts_ns`
- `venue`
- `symbol`
- `exchange_event_ts_ms`
- `bar_open_time_ms`
- `bar_close_time_ms`
- `is_closed`
- OHLC fields used by strategy
- `ingest_seq`
- `raw_payload_sha256`
- `raw_record_hash`

### decision_trace

Purpose: explain why a strategy did or did not generate an order.

Required fields:

- `strategy_instance_id`
- `decision_trace_id`
- `decision_key_v2`
- `config_hash`
- `condition_id`
- `market_id`
- `symbol`
- `window_start_ms`
- `window_end_ms`
- reference source pointer: `source_id`, `ingest_seq`, `local_recv_ts_ns`, `exchange_ts_ns`
- book source pointer: `source_id`, `ingest_seq`, `local_recv_ts_ns`, `exchange_ts_ms`
- candidate outcome
- model probability
- buy average price
- raw edge
- selected flag
- reject reasons
- compact top-of-book/depth summary
- `trace_hash`

For performance, full reconstruction uses the event journals plus source pointers.

### order_intent

Purpose: immutable order request before risk.

Required fields:

- `strategy_instance_id`
- `decision_key_v2`
- `intent_id`
- `decision_trace_id`
- `condition_id`
- `outcome`
- `token_id`
- `repeat_index`
- `cash_limit_usdc`
- `limit_price`
- `order_type`
- `intent_ns`
- `signal_recv_ts_ns`
- `book_recv_ts_ns`

### risk_trace

Purpose: explain why an intent was accepted or rejected.

Required fields:

- `strategy_instance_id`
- `decision_key_v2`
- `intent_id`
- accepted flag
- rejection reasons
- projected account exposure
- projected symbol exposure
- projected condition exposure
- projected condition/outcome exposure
- strategy active cap
- realized PnL plus open worst-case stress-loss state
- uncertain order count
- `risk_start_ns`
- `risk_end_ns`

### execution_trace

Purpose: reconcile actual live order behavior.

Required fields:

- `strategy_instance_id`
- `decision_key_v2`
- `intent_id`
- execution status
- requested cash
- filled cash
- filled shares
- fill ratio
- average price
- worst price when available
- order id
- request payload hash
- response body hash
- post start/end ns
- HTTP status
- TTFB
- error stage
- exchange error body hash or compact message

### shadow_execution_check

Purpose: calibrate fillability without blocking live orders.

Only run for selected intents. Required fields:

- `strategy_instance_id`
- `decision_key_v2`
- `intent_id`
- latency variant
- tick offset
- scheduled ns
- actual ns
- timer slip
- would_fill
- simulated filled cash
- simulated shares
- simulated average/worst price
- book source pointer

### settlement_trace

Purpose: convert fills into final realized PnL.

Required fields:

- `strategy_instance_id`
- `condition_id`
- market closed flag
- winning outcome
- settlement source
- filled cash by outcome
- payout
- realized PnL
- settlement timestamp

## Decision Key V2

Use a strategy-scoped key:

```text
{family_key}:{strategy_instance_id}:{symbol}:{bar_open_time_ms}:{condition_id}:{outcome}:{repeat_index}
```

`family_key` is currently `pm5m_v2` or `pm15m_v1`. Do not reuse the current PM5M-only key for
multi-strategy reconciliation.

## Local Runtime Target

Local code is responsible for replay, comparison, and explanation. It should not be required for
live ordering.

Required local tools:

1. `sync_poly_run`: copy Poly run artifacts into a local run catalog.
2. `normal_replay`: rebuild visible state from event journals using `local_recv_ts_ns`, then run the
   same strategy kernel to regenerate decisions.
3. `live_intent_replay`: read live `order_intent`s and replay execution fillability at the recorded
   intent/arrival times.
4. `compare_live_backtest`: join live and replay by `strategy_instance_id + decision_key_v2`.
5. `explain_decision`: print the first point where live and replay diverged for one decision.

Implemented today: `scripts/compare_multi_strategy_runs.py` can compare two existing run
directories by `strategy_instance_id + decision_key_v2` across risk, execution, and shadow
fillability records. The fuller `sync_poly_run`, `normal_replay`, `live_intent_replay`, and
single-decision explanation tools remain the target local toolchain.

Comparison classes:

- live intent/fill exists, replay intent missing: data visibility, strategy gate, or risk state
  mismatch.
- replay intent/fill exists, live missing: live did not see the same state, risk blocked, queue
  blocked, or operational stop triggered.
- both exist but execution differs materially: execution latency, FAK matching, book depth, price
  rounding, or response reconciliation mismatch.

## Local Implementation Details

Local tooling should be append-only and artifact-driven. It should never be required for Poly order
submission.

### Run Catalog

Store synced Poly runs under a stable catalog:

```text
runtime/poly_runs/YYYY-MM-DD/{run_id}/
  run_manifest.json
  market_event_journal/
  reference_event_journal/
  decision_traces.jsonl.zst
  order_intents.jsonl.zst
  risk_events.jsonl.zst
  order_audit.jsonl.zst
  shadow_execution_checks.jsonl.zst
  settlement_trace.jsonl.zst
  local_reports/
```

The sync tool writes a manifest containing file sizes, sha256 hashes, and sync time.

### Normal Replay

`normal_replay` rebuilds the live-visible state using server-side `local_recv_ts_ns`:

1. Load run manifest and strategy configs.
2. Replay Poly market events in receive order into a book store.
3. Replay reference events in receive order into a reference store.
4. At each strategy evaluation time, call the same strategy kernel as live.
5. Apply the same central risk rules using replayed ledger state.
6. Produce replay decision traces, intents, risk traces, and simulated fills.

This mode answers: "Would the same code, seeing the same server-visible data, make the same
decision?"

### Live Intent Replay

`live_intent_replay` skips signal generation:

1. Read live order intents.
2. Rebuild the book at `intent_ns + latency_variant` or actual recorded arrival time.
3. Simulate FAK taker buy at the recorded limit price and cash limit.
4. Compare simulated fill with actual execution trace.

This mode answers: "Was the execution model faithful after live already decided to trade?"

### Comparator Output

The target `compare_live_backtest` writes machine-readable summaries:

```text
local_reports/compare_summary.json
local_reports/decision_diff.jsonl.zst
local_reports/execution_diff.jsonl.zst
local_reports/daily_strategy_pnl.csv
```

Required aggregate dimensions:

- date;
- strategy instance;
- symbol;
- market duration;
- condition;
- outcome;
- divergence class.

Material divergence thresholds:

- missing live/replay decision for an orderable selected intent;
- fill status differs;
- filled cash differs by more than 0.50 USDC or 10% of order size;
- average price differs by more than 1 tick;
- realized PnL differs after settlement.

### Explain Tool

`explain_decision` prints one decision's first divergence:

```text
explain_decision \
  --run-id RUN \
  --strategy eth_15m_cap5 \
  --condition CONDITION \
  --outcome YES \
  --repeat-index 2
```

Output order:

1. live decision trace summary;
2. replay decision trace summary;
3. reference pointer comparison;
4. book pointer comparison;
5. candidate edge and reject reason comparison;
6. risk trace comparison;
7. execution trace or simulated fill comparison;
8. settlement comparison when available.

## Implementation Plan

Current branch status: phases 1, 3, and the comparator part of phase 4 are implemented in
`codex/poly-multi-strategy-runtime`; phase 2 is implemented through the current live trader strategy
paths rather than a fully abstract trait; phase 5 has not been promoted to live. Poly production has
not been switched from the existing single ETH PM5M process.

### Phase 1 - Schema and Config

- Add `strategy_instance_id` to live intent, risk, execution, shadow, settlement, and manifest
  schemas.
- Add `decision_key_v2`.
- Add multi-strategy config with the five reserved strategy slots.
- Keep only `eth_5m_main` live-enabled.

Acceptance:

- Current ETH 5m live behavior is unchanged except for extra fields.
- Every live order can be joined through `strategy_instance_id + decision_key_v2`.

### Phase 2 - Strategy Core

- Extract PM5M strategy evaluation into a shared deterministic kernel.
- Extract PM15M cap strategy into the same interface.
- The interface returns `DecisionTrace` plus zero or more `OrderIntent`s.
- Research scripts and live runtime both call this shared core.

Acceptance:

- Local replay of current ETH 5m can reproduce live `DecisionTrace` records using Poly run data.
- PM15M strategies can run shadow-only in the live runtime.

### Phase 3 - Central Risk and Execution

- Replace per-strategy risk with one portfolio risk ledger.
- Enforce strategy caps, symbol caps, condition caps, condition/outcome caps, stress-loss tiers, and
  uncertain-order caps in one place.
- Route all accepted intents to one execution adapter.

Acceptance:

- Two strategies cannot unknowingly exceed shared exposure on the same condition/outcome.
- Every rejected intent has a compact `risk_trace`.

### Phase 4 - Side-Path Audit and Replay

- Ensure market/reference event journals contain the source pointers needed by `DecisionTrace`.
- Add local `sync_poly_run`, `normal_replay`, `live_intent_replay`, and comparator outputs.
- Keep full replay local; do not put replay work on Poly hot path.

Acceptance:

- Daily local report classifies live-only, replay-only, and execution-divergent decisions.
- Material differences are explainable without manually reading raw logs.

### Phase 5 - Shadow, Gray, Promotion

- Run four new strategies shadow-only for at least one full day.
- Enable gray size only after comparator is clean.
- Promote size only after daily PnL distribution and replay reconciliation remain healthy.

Acceptance:

- No unexplained material live/replay divergence.
- No `audit_degraded` during gray.
- Operational stops and PnL stops behave as configured.

## What Not To Build Yet

- No Kafka/Redis/database dependency on the order path.
- No full book snapshot on every decision.
- No separate live process per strategy unless a central risk/condition lock already exists.
- No second copy of strategy logic for backtest.
- No fixed-delay replay for Poly run reconciliation when server-side `local_recv_ts_ns` is available.

Fixed-delay timing models are still acceptable for historical research without Poly-side recordings,
but they are not the reconciliation truth for actual live runs.
