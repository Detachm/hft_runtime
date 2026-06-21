# PM5M Backtest Results From 2026-06-09

## Scope

Canonical run:

- `/mnt/data/hft/hft_runtime/backtest_runs/full_0609_now_live_like_300ms_20260620T0116Z`

This run used the live-like 300 ms delayed FAK assumption and hold-to-settlement accounting. The
standard config for new live-aligned ETH gray runs is now:

- `hft_private/configs/position_v4_edge_live_eth_gray_20260620.json`

## Data Coverage

Available local book coverage in the run:

- start: `2026-06-09T12:39:42.041965+08:00`
- end: `2026-06-20T00:01:16.698673+08:00`

Known missing windows:

- `2026-06-09 00:00:00` to `2026-06-09 12:39:42.041965` CST
- `2026-06-13 15:00:00` to `2026-06-15 15:59:59.999999` CST
- `2026-06-17 23:00:00` to `2026-06-18 19:59:59.999999` CST

Do not treat those missing windows as strategy no-trade evidence.

## Full-Window Result

- final PnL: `+4540.568827` USDC
- fills: `1534`
- filled conditions: `676`
- condition win rate: `63.76%`
- fill win rate: `66.43%`

By market bucket:

| market | PnL USDC | conditions | fills | condition win rate | fill win rate |
| --- | ---: | ---: | ---: | ---: | ---: |
| ETH-5M | +4540.568827 | 676 | 1534 | 63.76% | 66.43% |
| SOL-5M | 0.000000 | 0 | 0 | n/a | n/a |

## Daily Result

| CST day | PnL USDC | conditions | fills | condition win rate | fill win rate |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2026-06-09 | +290.741547 | 34 | 69 | 64.71% | 68.12% |
| 2026-06-10 | +137.857614 | 61 | 116 | 63.93% | 66.38% |
| 2026-06-11 | +844.145954 | 78 | 180 | 67.95% | 71.67% |
| 2026-06-12 | -236.302267 | 89 | 188 | 57.30% | 62.77% |
| 2026-06-13 | +569.208818 | 97 | 259 | 61.86% | 66.80% |
| 2026-06-14 | 0.000000 | 0 | 0 | n/a | n/a |
| 2026-06-15 | +125.388598 | 14 | 33 | 64.29% | 63.64% |
| 2026-06-16 | +288.191797 | 89 | 197 | 58.43% | 60.91% |
| 2026-06-17 | +523.339335 | 95 | 214 | 66.32% | 66.36% |
| 2026-06-18 | +230.603923 | 9 | 23 | 77.78% | 82.61% |
| 2026-06-19 | +1767.393508 | 110 | 255 | 68.18% | 67.84% |
| 2026-06-20 | 0.000000 | 0 | 0 | n/a | n/a |

## Live-Vs-Backtest Follow-Up

The 2026-06-09 to 2026-06-11 live-only comparison is recorded in:

- `docs/pm5m_live_vs_backtest_0609_0611_live_only_analysis.md`
- `/mnt/data/hft/hft_runtime/analysis_runs/compare_live_vs_backtest_0609_0611/comparison_outputs`

The main conclusion remains: the largest mismatch was not explained by a simple fixed 300 ms latency
or FAK assumption alone. The core issue was inconsistent intent formation and visible local/live book
state. New live runs therefore must use `decision_key`, `order_audit`, `shadow_execution_checks`, and
`live_strategy_bar_coverage` to separate:

- live filled, local did not;
- local filled, live did not;
- both filled but price/cash/settlement/PnL differed.

## Counterfactual Reference

The 2026-06-09 to 2026-06-12 live order counterfactual artifacts are in:

- `/mnt/data/hft/hft_runtime/backtest_runs/live_counterfactual_0609_0612_20260620T_try_ticks`

Summary:

| variant | filled orders | win rate | PnL USDC |
| --- | ---: | ---: | ---: |
| live actual FAK | 296 | 62.84% | -58.646382 |
| same latency, original price | 179 | 69.27% | +237.521795 |
| same latency, +1 tick | 182 | 69.23% | +286.401217 |
| same latency, +2 ticks | 185 | 68.65% | +256.334514 |
| dynamic +1/+2 | 185 | 69.19% | +321.338354 |

This is evidence for controlled follow-up, not a guarantee that chasing every missed order improves
live PnL. It supports testing small tick offsets with strict audit, not unbounded persistent buying.
