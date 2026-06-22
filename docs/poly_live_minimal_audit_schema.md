# Poly Live Lean Final Audit and Risk Contract

Date: 2026-06-22

This document defines the lean final state for the Poly live runtime. It is not a temporary
bootstrap schema. The goal is to keep the live order path small while making every real decision
replayable and auditable.

Current code baseline:

```text
/home/hliu/hft_probe_multi_strategy_impl
branch: codex/poly-multi-strategy-runtime
HEAD: 395d7fc Log adaptive cutover follow-up
code-equivalent live commit: 949324d00611eac28dc6076bf4aa6e73be31ae65
```

Commits after `949324d` only update research logs, so executable behavior is the same as the last
remote live binary.

## Final Model

Live and local systems must converge on the same fact model:

```text
decision facts
decision-book facts
risk facts
execution facts
settlement facts
data-coverage facts
```

The primary reconciliation key is:

```text
strategy_instance_id + decision_key_v2
```

`decision_key` is legacy-compatible only. It does not include `strategy_instance_id`, so it is not a
safe multi-strategy key. `intent_id` links one order attempt. `decision_trace_id` is for debugging
nearby repeated evaluations.

## Live Files

Do not add a parallel full audit stream. The final live contract uses the existing files:

```text
runs/{run}/
  run_manifest.json
  shadow_intents.jsonl.zst
  order_audit.jsonl.zst
  shadow_execution_checks.jsonl.zst
  risk_events.jsonl.zst
  logger_health.jsonl.zst
  health_events.jsonl.zst
  order_chains.jsonl.zst
```

Do not add:

```text
audit_events.jsonl.zst
health_1s.jsonl.zst
settlement.jsonl.zst
```

Settlement already belongs in `risk_events` and `shadow_intents`. Runtime health belongs in
`logger_health` and, if needed, `health_events`.

## Artifact Reproducibility

Live must run from a reproducible artifact:

```text
git_sha is retrievable
binary_sha256 matches the running binary
config_hash is recorded
audit_profile_hash is recorded
strategy_instances are recorded
outputs list every emitted file
live directory is not a dirty ad hoc checkout
```

The 2026-06-21 live binary was reproducible on the remote host:

```text
git_sha: 949324d00611eac28dc6076bf4aa6e73be31ae65
binary_sha256: 8690179c563f171be6fe9f68ee7dfa2bc52c5a92f5f883f3aad85fb090fa53b1
```

## OrderIntent

`OrderIntent` is the canonical live decision object. It already carries:

```text
schema_version
mode
decision_id
decision_key
decision_key_v2
decision_trace_id
strategy_instance_id
strategy_family
market_duration_seconds
strategy_config_hash
market_id
symbol
bar_open_time_ms
condition_id
outcome
repeat_index
token_id
side
sizing_mode
target_shares
cash_limit_usdc
buy_amount_usdc
price
order_type
tick_size
neg_risk
window_start_ms
window_end_ms
model_probability
raw_edge
signal_exchange_ts_ns
signal_recv_ts_ns
eval_start_ns
eval_end_ns
intent_ns
risk_start_ns
risk_end_ns
queue_enter_ns
queue_exit_ns
book_recv_ts_ns
```

Final-state upgrade: copy selected-candidate decision-book evidence into `OrderIntent`:

```text
book_local_recv_ts_ns
book_exchange_ts_ms
book_age_ms
book_source_id
book_ingest_seq
best_bid
best_ask
book_crossed
buy_avg_price
buy_worst_price
buy_cash
buy_filled
full_depth
cash_limit_at_decision
cash_spent_at_decision
shares_filled_at_decision
full_cash_spent_at_decision
side_momentum_60s_bps
btc_lead_side_momentum_60s_bps
entry_limit_price
limit_avg_price_le_limit_at_decision
limit_full_depth_at_decision
```

This removes the need to scan every `shadow_market_evaluation` row when reconciling one live order.

## Main Audit Rows

### order_audit_risk

`order_audit_risk` is the main decision and risk audit row. It must answer:

```text
why the strategy triggered
what signal was used
what decision-book state was visible
why risk allowed or blocked
what order would be submitted
what condition-level and portfolio risk would become
```

Existing important fields:

```text
event_type = "order_audit_risk"
local_ts_ns
accepted
reasons
observed_reasons
intent_id
decision_key
decision_key_v2
decision_trace_id
strategy_instance_id
strategy_family
market_duration_seconds
strategy_config_hash
market_id
symbol
condition_id
outcome
repeat_index
side
sizing_mode
cash_limit_usdc
target_shares
base_limit_price
price
tick_size
order_type
risk_start_ns
risk_end_ns
realized_loss_usdc
realized_pnl_usdc
peak_realized_pnl_usdc
projected_total_cash
projected_pair_payout_floor_usdc
projected_worst_case_loss_usdc
projected_daily_pnl_usdc
effective_position_cap_usdc
portfolio_risk_tier
portfolio_risk_used_usdc
portfolio_open_risk_usdc
portfolio_soft_loss_usdc
portfolio_hard_loss_usdc
portfolio_catastrophe_loss_usdc
strategy_risk
```

Final-state upgrade: also emit the `OrderIntent` decision-book fields and the condition portfolio
risk fields below.

### order_audit_execution

`order_audit_execution` is the main execution audit row. It already carries enough execution facts:

```text
event_type = "order_audit_execution"
intent_id
decision_key_v2
decision_trace_id
strategy_instance_id
condition_id
outcome
execution_status
order_id
filled_cash_usdc
filled_shares
avg_price
request_payload_sha256
response_body_sha256
post_start_ns
post_end_ns
post_latency_ms
http_status_code
http_error_stage
ack_ns
error
```

`post_latency_ms` is observe-only. It should be recorded and analyzed, not used as a direct order
block in this final state.

## Condition Portfolio Risk

Reverse-side buying is allowed. It is part of the strategy surface and can reduce portfolio risk.
The final risk definition is therefore condition-level portfolio risk, not one-sided cash.

For every `strategy_instance_id + condition_id`, maintain filled plus pending exposure:

```text
yes_cash
yes_shares
no_cash
no_shares
gross_cash = yes_cash + no_cash
worst_case_payout = min(yes_shares, no_shares)
worst_case_loss = gross_cash - worst_case_payout
```

For a new intent, compute projected values after adding the current intent:

```text
projected_condition_yes_cash
projected_condition_yes_shares
projected_condition_no_cash
projected_condition_no_shares
projected_condition_gross_cash
projected_condition_worst_case_loss
```

The hard condition-level cap is:

```text
projected_condition_worst_case_loss <= strategy active_cap_usdc
```

`active_cap_usdc` is the maximum condition-level worst-case loss for that strategy instance.

`gross_cash` is observe-only by default. A wide gross-cash sanity cap is acceptable as a bug guard,
but it must not be the primary risk definition because it can penalize valid hedging.

`max_entries_per_condition_outcome` is an execution throttle. It is not the primary risk boundary.

## Risk Layers

The final live risk model has three explicit layers:

```text
condition worst_case_loss cap
strategy active risk and adaptive multiplier
portfolio drawdown / hard stop / catastrophe
```

The condition calculation must include:

```text
filled positions
pending accepted orders
current intent
```

This prevents queue/concurrency penetration.

`strategy_risk` and adaptive multipliers remain sizing controls. They do not replace condition or
portfolio risk.

## Bootstrap and Settlement

Bootstrap:

```text
recognized strategy_instance_id -> count in that strategy's condition exposure
unknown or external fills -> count in portfolio risk only
```

Unknown/external fills must not pollute a strategy's own condition cap.

Settlement:

```text
release condition exposure
update strategy adaptive state
write risk_events
preserve settlement source and outcome
```

## Required Risk Audit Fields

Add these fields to `order_audit_risk`:

```text
condition_yes_cash
condition_yes_shares
condition_no_cash
condition_no_shares
condition_gross_cash
condition_worst_case_loss
projected_condition_yes_cash
projected_condition_yes_shares
projected_condition_no_cash
projected_condition_no_shares
projected_condition_gross_cash
projected_condition_worst_case_loss
condition_risk_cap_usdc
condition_risk_reasons
condition_risk_decision
```

These fields are required for local replay to explain live `allow` versus `block` decisions without
reconstructing private in-memory risk state from logs.

## Supporting Files

### shadow_intents.jsonl.zst

Keep this as the research-heavy stream:

```text
shadow_market_evaluation
selected and rejected candidates
order_intent after risk acceptance
settlement preview/report rows
```

It can stay verbose. It is not the order-level canonical audit table.

### shadow_execution_checks.jsonl.zst

Use this for execution-book mismatch analysis. It carries:

```text
decision_key_v2
strategy_instance_id
token_id
limit_price
latency_ms
tick_offset
actual_check_ns
would_fill
cash_spent
shares_filled
buy_avg_price
buy_worst_price
best_bid
best_ask
book_age_ms
book_local_recv_ts_ns
book_exchange_ts_ms
```

### risk_events.jsonl.zst

Use this for portfolio, settlement, and adaptive state:

```text
risk_check
position_update
polymarket_settlement_risk_update
live_equity_snapshot
risk_halt
strategy_adaptive_risk_state_loaded
strategy_adaptive_risk_state_persisted
live_execution_uncertainty_started
live_execution_uncertainty_resolved
```

### logger_health.jsonl.zst and health_events.jsonl.zst

`logger_health` must show:

```text
critical_dropped_count == 0
audit_degraded == false
writer_failure == null
```

If runtime health is added, write it into existing `health_events.jsonl.zst` as
`runtime_health_1s`. It should be side-path telemetry and must not perform synchronous IO or heavy
aggregation in the order path.

## Local Replay Contract

Local replay must emit comparable rows keyed by:

```text
strategy_instance_id + decision_key_v2
```

Comparable rows include:

```text
signal fields
decision-book fields
condition portfolio risk fields
risk decision
simulated execution
settlement result if known
local_data_gap_status
```

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

## Performance Contract

The live order path may do:

```text
copy compact candidate evidence into OrderIntent
compute condition portfolio risk from in-memory state
serialize existing audit records
enqueue to the existing non-blocking logger channel
```

The live order path must not do:

```text
zstd compression
fsync
full raw payload dump
network upload
blocking disk writes
large no-signal audit logging
```

JSONL+zstd remains the live audit format. If offline analytics need faster scans, compact runs to
Parquet locally after the run. Do not replace the live audit format with binary logs.

## Non-Goals

Do not implement:

```text
reverse-side order ban
new full audit_events stream
new health_1s file
new settlement file
AWS full raw book journal
Kafka / database / service architecture
binary live logging
complex predictive drawdown model
```

Final state in one sentence:

```text
Live writes compact, replayable facts for every real decision, risk check, execution, settlement,
and logger-health state; local replay uses the same semantics to explain every mismatch.
```
