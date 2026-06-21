# PM5M 2026-06-09 至 2026-06-11 实盘/回测成交差异备查

生成时间：2026-06-20

## 口径

- 对齐窗口：2026-06-09 12:34:41 CST 至 2026-06-12 00:00:00 CST。本地 HFTBOOK/HFTIDX 可回放数据从 6/9 中午后才完整可比，早于该时间的实盘订单不纳入公平比较。
- 关注分类：`live_filled_backtest_not`，即实盘有成交、当前 300ms run-fast 回测没有成交。
- PnL：按持有到结算计算，live 侧用回测同一手续费公式估算。
- 主要输入：
  - `/mnt/data/hft/hft_runtime/analysis_runs/compare_live_vs_backtest_0609_0611/comparison_outputs/market_details.csv`
  - `/mnt/data/hft/hft_runtime/analysis_runs/compare_live_vs_backtest_0609_0611/live_logs/poly_0609_0611_combined.txt`
  - `/mnt/data/hft/hft_runtime/analysis_runs/compare_live_vs_backtest_0609_0611/fillability_16_sources_300ms/live_fillability_compare.csv`
  - `/mnt/data/hft/hft_runtime/analysis_runs/compare_live_vs_backtest_0609_0611/backtest_300ms`

## 总览

6/11 的 `live_filled_backtest_not` 一共 16 个市场、41 笔 live 订单、40 笔有实际 fill，live cash 945.22 USDC，live PnL -347.58 USDC。

当前回测输出里，这 16 个 condition_id 在 `intent_groups`、`fills`、`group_ledger`、`condition_ledger` 都是 0 行。所以这里的“回测无”不是成交模型拒单后的无成交，而是回测策略根本没有为这些市场生成 intent。

## 为什么会出现实盘有、回测无

结论：主因不是单纯 FAK/300ms 成交假设，而是当前本地回测脚本没有 100% 复原当时实盘的决策入口、运行参数和本地盘口状态。

已确认的差异有三类：

1. 策略 intent 流不一致。
   - live 日志里这些市场都有 `order_audit_risk accepted=true`，说明当时实盘策略确实生成了订单意图。
   - 当前回测对同一批 condition_id 没有任何 intent。
   - 这说明问题发生在执行前：市场入口、reference-clock 驱动、active market 选择、信号门槛、动量过滤、状态机或参数至少有一处没有复原。

2. 执行层盘口也不完全一致。
   - 对这 41 个 live intent，用 live shadow 的同一 300ms 延迟对比本地 HFTIDX1：
     - `both_fillable`: 13
     - `live_only_fillable`: 15
     - `local_only_fillable`: 6
     - `neither_fillable`: 7
   - 本地不可成交的原因主要是 `local_missing_book`、`local_stale_book`、`local_no_ask_depth_at_limit`。
   - 也就是说，即使强行把 live intent 喂给本地执行模型，仍有一部分会因为本地盘口状态不同而不能复现。

3. 实盘 run 参数并非全程等于当前回测配置。
   - 6/11 凌晨这些订单落在 `unknown-live-trader-20260610T071747Z` 跨天 run，manifest 显示 ETH 单笔 40、ETH cap 120。
   - 当前回测配置是 ETH 30、SOL 20、最多 3 次。
   - 6/11 白天之后的 run 才回到 ETH 30、SOL 20。参数差异不会单独解释“0 intent”，但会放大 PnL 和仓位差异。

一句话：这 16 个市场证明当前回测不是当时 live 的逐笔 replay；它是“当前策略 + 当前本地数据”的理论回测。要解释实盘亏损，必须先把 live intent 生成链路复原，再谈追价或成交优化。

## 16 个市场明细

`本地300ms对照` 来自 live shadow intent 与本地 HFTIDX1 的 300ms fillability 对比。

| 市场 | run | 首单CST | 标的 | live买入 | 结算赢方 | 订单 | cash | 均价 | limit | 模型P | PnL | 本地300ms对照 | 判断 |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| `0x7100daac...` | 20260610T071747 | 03:51:04 | ETH | YES | NO | 3 | 117.58 | 0.423 | 0.42-0.43 | 0.629-0.635 | -122.34 | live_only_fillable:3 | 本地同价无深度；执行盘口不一致 |
| `0x761fe66b...` | 20260611T053343 | 17:46:27 | ETH | YES | NO | 3 | 90.00 | 0.482 | 0.61-0.63 | 0.818-0.831 | -93.26 | both_fillable:3 | 本地300ms也可成交；主要是回测没生成intent |
| `0x6bbc9472...` | 20260610T071747 | 03:32:16 | ETH | NO | YES | 3 | 86.30 | 0.330 | 0.32-0.39 | 0.524-0.592 | -90.32 | both_fillable:1/local_only_fillable:2 | 本地300ms也可成交；主要是回测没生成intent |
| `0xd95b2c7c...` | 20260611T125056 | 21:31:01 | SOL | YES | NO | 3 | 60.00 | 0.344 | 0.39-0.39 | 0.597-0.600 | -62.76 | both_fillable:3 | 本地300ms也可成交；主要是回测没生成intent |
| `0x1e06e623...` | 20260610T071747 | 03:56:30 | SOL | YES | NO | 3 | 46.27 | 0.544 | 0.53-0.57 | 0.735-0.773 | -47.77 | neither_fillable:1/live_only_fillable:2 | 本地盘口过期；策略入口+数据时效都不一致 |
| `0x7da71afb...` | 20260610T071747 | 03:50:06 | SOL | YES | NO | 3 | 44.27 | 0.208 | 0.28-0.28 | 0.489-0.489 | -46.75 | live_only_fillable:3 | 本地缺盘口；策略入口+数据覆盖都不一致 |
| `0xe750256b...` | 20260610T071747 | 07:05:29 | ETH | YES | NO | 2 | 44.75 | 0.600 | 0.60-0.60 | 0.800-0.801 | -46.01 | neither_fillable:2 | shadow/live也多为不可成交；但实盘实际有部分成交 |
| `0x3b1c24bc...` | 20260611T125056 | 21:32:55 | ETH | YES | NO | 4 | 38.89 | 0.200 | 0.15-0.29 | 0.352-0.497 | -41.01 | local_only_fillable:1/neither_fillable:2/live_only_fillable:1 | 本地盘口过期；策略入口+数据时效都不一致 |
| `0xa8b8a338...` | 20260611T125056 | 21:21:58 | ETH | YES | NO | 1 | 19.00 | 0.700 | 0.70-0.70 | 0.901-0.901 | -19.40 | neither_fillable:1 | shadow/live也多为不可成交；但实盘实际有部分成交 |
| `0x6d9ff398...` | 20260611T053343 | 14:58:43 | SOL | YES | NO | 1 | 1.80 | 0.300 | 0.30-0.30 | 0.500-0.500 | -1.89 | both_fillable:1 | 本地300ms也可成交；主要是回测没生成intent |
| `0xe1ef7955...` | 20260611T125056 | 20:57:15 | ETH | YES | YES | 2 | 33.85 | 0.763 | 0.76-0.77 | 0.967-0.976 | +10.24 | both_fillable:1/local_only_fillable:1 | 本地300ms也可成交；主要是回测没生成intent |
| `0x414bb5b7...` | 20260610T071747 | 03:56:49 | ETH | YES | YES | 2 | 62.44 | 0.655 | 0.66-0.69 | 0.868-0.899 | +31.58 | neither_fillable:1/both_fillable:1 | 混合；需逐笔看盘口 |
| `0x72a9c8d2...` | 20260610T071747 | 05:47:28 | ETH | NO | NO | 3 | 106.67 | 0.711 | 0.70-0.72 | 0.907-0.929 | +41.23 | local_only_fillable:2/both_fillable:1 | 本地300ms也可成交；主要是回测没生成intent |
| `0x52291dbf...` | 20260611T053343 | 16:35:55 | ETH | YES | YES | 3 | 65.30 | 0.573 | 0.54-0.63 | 0.745-0.835 | +43.73 | live_only_fillable:3 | 本地缺盘口；策略入口+数据覆盖都不一致 |
| `0xbb7d6612...` | 20260611T125056 | 22:28:53 | ETH | YES | YES | 2 | 60.00 | 0.545 | 0.53-0.58 | 0.731-0.789 | +48.26 | both_fillable:2 | 本地300ms也可成交；主要是回测没生成intent |
| `0x6e08183c...` | 20260611T053343 | 17:51:00 | ETH | YES | YES | 3 | 68.10 | 0.576 | 0.59-0.59 | 0.791-0.797 | +48.88 | live_only_fillable:3 | 本地缺盘口；策略入口+数据覆盖都不一致 |

## 当前判断

优先级最高的问题不是追价，而是复原 live intent 流。

如果同一时间、同一市场、同一盘口、同一 reference 下，live 生成 intent 而回测不生成 intent，那么任何基于当前回测的策略胜率/PnL 都不能直接解释实盘亏损。

下一步应先做两件事：

1. 将 live 的 intent 输入字段旁路记录完整：condition_id、asset_id、window_start/end、reference anchor/current、sigma、momentum、p_model、edge、best ask/top depth、strategy gate 结果、risk gate 结果。
2. 在本地回测增加同口径 diagnostic：对指定 condition_id/time 重放时输出每个 gate 的 pass/fail。先把这 16 个市场逐笔复现到“同一条 intent 是否应该生成”，再讨论 FAK/FOK/追价。
