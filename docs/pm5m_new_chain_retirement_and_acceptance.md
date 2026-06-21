# PM5M 新链路清退与验收备查

本文档记录 PM5M 高性能数据链路落地后的清退和验收规则。目标是只保留一份审计源、一层策略无关 book state cache、一层回测查询 index，以及一条 fast backtest 主路径。

## 目标链路

最终接受的主链路是：

```text
HFTREC4 raw WS audit
  -> HFTBOOK2 book_state cache
  -> HFTIDX1 book_state index
  -> HFTREF1 reference cache
  -> HFTSETTLE1 settlement cache
  -> pm5m-backtest run-fast --book-index-root
```

常态目录形态：

```text
raw_audit/HFTREC4
market_cache/HFTBOOK2
book_index/HFTIDX1
reference/HFTREF1
settlement/HFTSETTLE1
backtest_runs/
```

`HFTBOOK2`、`HFTIDX1`、`HFTREF1`、`HFTSETTLE1` 都必须保持策略无关，不能包含 signal、PnL、execution 输出或任何策略字段。

## 清退原则

新链路验收通过后，旧路径不能继续留在主流程里。两条可运行链路并存会制造静默正确性风险：旧 loader 也能跑出看似合理、实际不可信的回测结果。

| 对象 | 替代验证通过后的处理 |
| --- | --- |
| JSONL raw | 记录转换证据后删除；PM5M 代码不再保留 importer 入口。 |
| HFTREC3 raw | WS raw 已转换到 HFTREC4；PM5M 代码不再保留 HFTREC3 可执行入口。 |
| HFTBOOK1 | 已删除 builder/reader/CLI/API；HFTBOOK2 是唯一 book cache 格式。 |
| 旧 accepted Parquet dataset | 保留 manifest/hash 证据；表 payload 删除或冷归档。 |
| depth feature stream | 从生产输出移除；只允许诊断生成。 |
| 指向 depth feature 的 event index | 标为 legacy/invalid；生产回测永不接受。 |
| HFTIDX1 | 为速度保留；存储紧张时可从 HFTBOOK2 重建。 |
| 回测大输出 | 保留 summary、run manifest、config snapshot、hash；大 reject dump 压缩或删除。 |

## 代码清退

旧路径不再只做 freeze/gate，而是从 PM5M 可执行代码中剥离：

- 删除 JSONL importer、HFTREC3 raw backfill/filter/summarize、legacy raw backtest 脚本。
- 删除 HFTBOOK1 builder/reader/CLI/API。
- `pm5m-recorder` 只保留 WS HFTREC4 raw audit 写入，不再提供 HFTREC3 raw-format 或 HTTP book snapshot recorder。
- `pm5m_data_etl build_facts` 必须从 HFTBOOK2/book-state cache 读取，不再直接 replay raw。
- `pm5m-backtest run-fast` 必须使用 HFTIDX1 file-backed 查询。
- static gate 拒绝旧文件重新出现，拒绝 PM5M 新链路源码出现 HFTREC3/HFTBOOK1 调用。

## 验收 Gates

一次新链路验收完成，必须同时满足：

1. 静态 hot-path gate 通过。
2. HFTBOOK2 能从选定 raw root 构建。
3. HFTIDX1 能从 HFTBOOK2 构建。
4. HFTBOOK2/HFTIDX1/HFTREF1/HFTSETTLE1 validation 全部通过。
5. HFTBOOK2 和 HFTIDX1 benchmark report 必须产出；HFTIDX1 必须达到回放吞吐门槛。HFTBOOK2 全扫吞吐默认只做诊断，除非显式配置硬门槛。
6. `pm5m-backtest run-fast --book-index-root` 必须在配置的 wall-clock 目标内完成。
7. run 目录必须写出 `summary.json`、`run_manifest.json` 和稳定 hash anchors。
8. settlement/reference/book 缺失必须显式报错或 reject，不能静默补值，不能伪造 settlement。

生产目标是全量回测分钟级。未达标时，后续优化必须继续沿新链路推进，不能重新打开旧路径。

## 标准验收命令

设置环境变量：

```sh
export PM5M_RAW_ROOT=/path/to/raw_audit_or_legacy_source
export PM5M_BOOK_CACHE_ROOT=/path/to/market_cache/hftbook2
export PM5M_BOOK_INDEX_ROOT=/path/to/book_index/hftidx1
export PM5M_REFERENCE_CACHE_ROOT=/path/to/reference/hftref1
export PM5M_SETTLEMENT_CACHE_ROOT=/path/to/settlement/hftsettle1
export PM5M_CONFIG=/path/to/backtest_config.json
export PM5M_OUTPUT_ROOT=/path/to/acceptance_report_dir
```

可选：

```sh
export PM5M_START_TS_NS=...
export PM5M_END_TS_NS=...
export PM5M_MAX_BUILD_MS=3600000
export PM5M_MIN_SCAN_ROWS_PER_SEC=0
export PM5M_MIN_INDEX_ROWS_PER_SEC=1000000
export PM5M_MAX_BACKTEST_MS=300000
```

执行：

```sh
scripts/private/perf_gate_pm5m.sh --required
```

## 手工构建命令

构建 HFTBOOK2：

```sh
cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- build-book-cache \
  --raw-root "${PM5M_RAW_ROOT}" \
  --cache-root "${PM5M_BOOK_CACHE_ROOT}" \
  --overwrite
```

构建 HFTIDX1：

```sh
cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- build-book-index \
  --book-cache-root "${PM5M_BOOK_CACHE_ROOT}" \
  --index-root "${PM5M_BOOK_INDEX_ROOT}" \
  --overwrite
```

从 accepted dataset 构建 reference：

```sh
cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- build-reference-cache \
  --input-table /path/to/dataset/tables/okx_kline_1s_reference \
  --cache-root "${PM5M_REFERENCE_CACHE_ROOT}" \
  --overwrite
```

如果 accepted dataset 没有覆盖新 HFTBOOK2 窗口，可以一次性从 Binance 1s kline 生成 HFTREF1。生成后回测只读本地 HFTREF1，不再重复下载：

```sh
cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- build-reference-cache-from-binance \
  --cache-root "${PM5M_REFERENCE_CACHE_ROOT}" \
  --symbol BTCUSDT \
  --symbol ETHUSDT \
  --symbol SOLUSDT \
  --start-ts-ns "${PM5M_REFERENCE_START_TS_NS}" \
  --end-ts-ns "${PM5M_REFERENCE_END_TS_NS}" \
  --reference-latency-ms 1000 \
  --overwrite
```

从 accepted dataset 离线构建 settlement：

```sh
cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- build-settlement-cache \
  --input-table /path/to/dataset/tables/polymarket_settlement \
  --book-cache-root "${PM5M_BOOK_CACHE_ROOT}" \
  --filter-to-book-cache \
  --cache-root "${PM5M_SETTLEMENT_CACHE_ROOT}" \
  --overwrite
```

如果 settlement 表有缺失或 stale winner，用 HFTBOOK2 作为 condition/asset 权威来源，只对缺失 condition 从 CLOB 补 official result，并直接输出 HFTSETTLE1：

```sh
cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- build-settlement-cache-from-clob \
  --input-table /path/to/dataset/tables/polymarket_settlement \
  --book-cache-root "${PM5M_BOOK_CACHE_ROOT}" \
  --cache-root "${PM5M_SETTLEMENT_CACHE_ROOT}" \
  --refresh-workers 2 \
  --overwrite
```

运行 fast backtest：

```sh
cargo run --release --manifest-path hft_private/Cargo.toml --bin pm5m_backtest -- run-fast \
  --book-cache-root "${PM5M_BOOK_CACHE_ROOT}" \
  --book-index-root "${PM5M_BOOK_INDEX_ROOT}" \
  --reference-cache-root "${PM5M_REFERENCE_CACHE_ROOT}" \
  --settlement-cache-root "${PM5M_SETTLEMENT_CACHE_ROOT}" \
  --config "${PM5M_CONFIG}" \
  --output-dir "${PM5M_OUTPUT_ROOT}/run"
```

## 删除前 Manifest

删除或隔离旧数据前，必须写一份 retirement manifest，包含：

- 删除或隔离路径
- 格式
- 字节大小
- 时间范围
- 替代路径
- 替代 catalog hash
- 删除/隔离时间
- 操作人和执行命令

旧 HFTREC3 payload 在 HFTREC4 转换和新链路验收通过后删除，只保留 freeze/convert/delete manifest。

## 2026-06-19 新链路验收记录

- 验收根目录：`/mnt/data/hft/hft_runtime/new_chain_acceptance/20260619T082351Z_full_ws_hftrec4`
- 原始 HFTREC3 freeze manifest：`/mnt/data/hft/hft_runtime/retirement_manifests/20260619T074043Z_hftrec3_ws_freeze/freeze_manifest.json`
- HFTREC3 -> HFTREC4 转换输出：`/mnt/data/hft/hft_runtime/converted_hftrec4_ws_raw/live_polymarket_all_current_ws_raw_20260619`
- HFTREC3 转换校验：90,768 manifests，90,768 `.hfr4` segments，90,768 HFTREC4 manifests，652,031,074 converted rows
- HFTBOOK2：107,607,709 rows，validation 通过，bench 24.198s，4.45M rows/s
- HFTIDX1：107,607,709 rows，validation 通过，bench 56.617s，1.90M rows/s
- HFTREF1：164,946 rows，从 Binance 1s 当前窗口生成，validation 通过
- HFTSETTLE1：1,824/1,824 conditions，从 CLOB official result 补齐，validation 通过
- `run-fast --book-index-root`：164,934 events，159 intents/fills，0 rejects，total 37.982s，peak RSS 94MB
- 本次 summary：`/mnt/data/hft/hft_runtime/new_chain_acceptance/20260619T082351Z_full_ws_hftrec4/run_fast_position_v4_edge_current/summary.json`
- 本次 run manifest：`/mnt/data/hft/hft_runtime/new_chain_acceptance/20260619T082351Z_full_ws_hftrec4/run_fast_position_v4_edge_current/run_manifest.json`

验收结论：新链路已经达到单窗口分钟级回测；旧 ETL/event-index/HFTREC3/HFTBOOK1 入口已从 PM5M 可执行代码中剥离，`pm5m-backtest run-fast` 必须使用 HFTIDX1 file-backed 查询。
