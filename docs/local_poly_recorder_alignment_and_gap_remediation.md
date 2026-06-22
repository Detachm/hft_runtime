# Local Poly Recorder and Replay Lean Final Contract

Date: 2026-06-22

This document defines the lean final state for local Polymarket recording, book alignment, replay,
and live/local reconciliation. It is not a temporary remediation checklist. The local side must
produce affordable but honest research data and must be able to explain live/backtest divergence.

## Final Model

The local side must align with the live fact model:

```text
decision facts
decision-book facts
risk facts
execution facts
settlement facts
data-coverage facts
```

The shared reconciliation key is:

```text
strategy_instance_id + decision_key_v2
```

Local raw data is not automatically authoritative for a live order. Local replay is authoritative
only when its coverage and alignment facts prove that the needed local data was visible and complete.

## Current Evidence From 2026-06-21

Analysis window:

```text
UTC: 2026-06-21 00:00:00 to 2026-06-21 18:13:10
local raw root: /mnt/data/hft/hft_runtime/live_polymarket_all_current_hftrec4_ws_raw/raw
local recorder log: /mnt/data/hft/hft_runtime/live_polymarket_all_current_hftrec4_ws_raw/recorder.log
```

Observed local recorder health:

```text
disconnect        278
gap_suspected     278
reconnect         277
subscribe         351
```

Merged gap episodes:

```text
episodes          278
total gap         17,220s, about 4.78h
window duration   65,590s, about 18.22h
gap percentage    26.25%
median gap        69.97s
p90 gap           89.99s
max gap           286.98s
```

Disconnect payload breakdown:

```text
send WS PING                       115
read WS message                     93
connect timeout                     42
send WS dynamic subscribe           15
send WS dynamic unsubscribe         11
connect wss://...                    2
```

Conclusion: local data gaps were systematic during the live-loss window. Missing local books inside
those intervals are data-coverage failures, not strategy no-fill evidence.

## Alignment Rules

### price_change

For `price_change` events:

```text
visible_ts = exchange_ts_ms + calibrated_poly_latency_ms
```

Default:

```text
calibrated_poly_latency_ms = 20
```

This fixes cases where local receive time is delayed relative to the Poly server's visible state.

### book snapshots

For `book` snapshots:

```text
initialize book state at local receive time
```

Do not blindly rewrite snapshots to `exchange_ts_ms + 20ms`. A snapshot may be:

```text
initial subscription state
reconnect state
backlog / replayed state
delayed state-version snapshot
```

Moving such snapshots backward can create lookahead.

### price_change before snapshot

A `price_change` can update an existing book state. It cannot safely initialize a missing book.

If local replay sees price changes before a valid local book snapshot for that asset, it must mark
the interval as incomplete, not invent a book.

## Recorder Final State

The local recorder should be narrow and reliable, not globally exhaustive.

Record the active research universe:

```text
BTC/ETH/SOL
5m/15m
YES/NO
```

Shard by symbol:

```text
poly_recorder_btc_5m_15m
poly_recorder_eth_5m_15m
poly_recorder_sol_5m_15m
```

Each shard owns:

```text
raw event root
state/checkpoint root
gap/control log
recorder health log
```

Merge identity:

```text
condition_id + asset_id + exchange_ts_ms + payload_hash
```

The final design does not require AWS full raw storage.

Implemented production roles:

```text
ROLE=poly_btc            -> BTC 5m/15m Polymarket CLOB raw
ROLE=poly_eth            -> ETH 5m/15m Polymarket CLOB raw
ROLE=poly_sol            -> SOL 5m/15m Polymarket CLOB raw
ROLE=reference_binance   -> BTC/ETH/SOL Binance 1s raw reference
```

Production Poly configs:

```text
configs/pm5m-recorder-poly-btc-5m-15m.json
configs/pm5m-recorder-poly-eth-5m-15m.json
configs/pm5m-recorder-poly-sol-5m-15m.json
```

Online book-cache writing is disabled by default in the production supervisor. HFTBOOK2/HFTIDX1
should be built offline from HFTREC4 so raw recording remains the first-priority path.

## Recorder Hot Path

The WebSocket reader path must stay hot:

```text
parse compact event
stamp local receive time
enqueue
return to reading
```

It must not do:

```text
book replay
heavy JSON transforms
compression
blocking disk writes
network upload
large synchronous diagnostics
```

The writer may batch and compress off-path.

Required recorder telemetry:

```text
connection_id
subscription_epoch
asset_count
last_recv_age_ms
last_successful_event_type
enqueue_wait_ms
queue_depth
local_overrun
writer_error
```

In the production recorder, `queue_depth`, writer buffer rows, and cumulative `local_overrun_count`
are written to `recorder_health.jsonl`. `local_overrun` is also written as a raw control row when
the reader had to drop events because the writer queue was full.

`local_overrun` is a first-class coverage event. It means the recorder may have stopped draining the
WebSocket fast enough.

## Disconnect and Gap Facts

The recorder must preserve control rows:

```text
disconnect
gap_suspected
reconnect
subscribe
first_valid_post_reconnect_event
local_overrun
```

Disconnect rows must include:

```text
full error chain
WebSocket close frame code and reason when available
IO error kind
TLS/protocol category when available
operation: read / ping write / subscribe / unsubscribe / connect
connection_id
subscription_epoch
asset_count
last_recv_age_ms
```

Gap derivation:

```text
gap_start = disconnect or gap_suspected or local_overrun timestamp
gap_end   = first valid post-reconnect book/price_change for that condition/asset
```

Backtest/replay must classify gap-period `missing_book` and `stale_book` as:

```text
local_data_gap
```

not as real strategy rejects.

## Coverage Outputs

Every local replay/backtest report must include:

```text
data_gap_intervals
orders_inside_data_gap
orders_after_gap_before_snapshot
fills_clean_data
fills_gap_affected
pnl_clean_data
pnl_gap_affected
unknown_coverage_pnl
coverage_percentage_by_symbol_interval
```

Strategy quality should be judged primarily from `clean_data_pnl`. Gap-affected PnL is diagnostic.

## Live Reconciliation Inputs

The live side is defined by:

```text
run_manifest.json
order_audit.jsonl.zst
shadow_intents.jsonl.zst
shadow_execution_checks.jsonl.zst
risk_events.jsonl.zst
logger_health.jsonl.zst
health_events.jsonl.zst
```

Local reconciliation must not depend on nonexistent files such as:

```text
audit_events.jsonl.zst
health_1s.jsonl.zst
settlement.jsonl.zst
```

`logger_health` must be checked first. If the live run has critical audit drops or writer failure,
the run is not fully replayable.

## Local Canonical Replay Rows

Local replay must emit canonical rows keyed by:

```text
strategy_instance_id + decision_key_v2
```

Each row should include:

```text
strategy_instance_id
decision_key_v2
condition_id
token_id / asset_id
symbol
window_start_ms
window_end_ms
outcome
signal_exchange_ts_ns
signal_recv_ts_ns
model_probability
raw_edge
side_momentum_60s_bps
btc_lead_side_momentum_60s_bps
decision_book_local_recv_ts_ns
decision_book_exchange_ts_ms
decision_book_age_ms
best_bid
best_ask
buy_avg_price
buy_worst_price
decision_limit_price
cash_limit_usdc
condition portfolio risk fields
risk_decision
simulated_execution
settlement_result
local_data_gap_status
```

These rows are the local counterpart to live `order_audit_risk` and `order_audit_execution`.

## Risk Replay Final State

Local replay must reproduce live risk semantics, not only strategy signals.

Required risk semantics:

```text
condition worst_case_loss
pending accepted orders
current intent
strategy active_cap_usdc
portfolio soft / hard / catastrophe
adaptive strategy multiplier
settlement release
bootstrap/external exposure handling
```

Condition portfolio risk uses the same live formula:

```text
gross_cash = yes_cash + no_cash
worst_case_payout = min(yes_shares, no_shares)
worst_case_loss = gross_cash - worst_case_payout
```

Reverse-side buying is allowed. Local replay must not reject it just because the other side exists.

Unknown/external bootstrap fills:

```text
count in portfolio risk
do not pollute a strategy's own condition cap
```

Without risk replay, local cannot correctly explain:

```text
live traded but local did not
local traded but live did not
both traded but size differed
```

## Comparator Final State

Comparator categories are fixed:

```text
live_only
local_only
both_same
signal_mismatch
decision_book_mismatch
execution_book_mismatch
risk_mismatch
execution_mismatch
local_data_gap
```

Each mismatch must identify the first failing layer:

```text
signal
decision book
risk state
execution book
execution response
settlement
local coverage
```

The comparator must preserve enough context to inspect one representative case without rescanning
all raw files.

## Validation Targets

Required validation:

- Re-run the 2026-06-21 BTC missing-event case and classify it as `local_data_gap`.
- Re-run the ETH delayed-local-receive case and verify `price_change` exchange-time alignment.
- Confirm book snapshots remain receive-time anchored.
- Confirm local replay reproduces live condition portfolio risk decisions.
- Confirm comparator can explain `live_only`, `local_only`, and `risk_mismatch` cases.
- Reduce active-market local recorder gap percentage below `1%` before using local data as strong
  live-like fill evidence.

## Non-Goals

Do not implement as part of the lean final state:

```text
AWS full raw journal
Kafka / database / service architecture
binary live log format
global all-symbol recorder
blind snapshot exchange-time rewrites
strategy-only replay without risk state
```

## Final Principle

Local raw is the affordable research backbone, but only if it is coverage-aware.

`exchange_ts` alignment can fix delayed local receive timestamps. It cannot recover missing events.

Missing events must become explicit `local_data_gap` intervals, not silent `missing_book` or
`stale_book` strategy outcomes.
