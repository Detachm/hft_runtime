# PM5M Replay ETL Refactor Plan

Date: 2026-06-24 CST

Status: architecture decision and implementation plan. Phase 0-2 are implemented as of
2026-06-24 CST. This document supersedes the assumption that daily live-vs-backtest comparison must
use `HFTBOOK2 + HFTIDX1` as the primary hot path. The depth state index remains useful for
forensic/debug work, but it must not be the default path for live comparison or large-window
strategy iteration.

Implementation update, 2026-06-25 CST:

- `pm5m_backtest market-replay-strategy-compare` now accepts compact typed binary replay input via
  `--compact-typed-file` / `--compact-typed-root`. Existing `--typed-update-root` discovery also
  recognizes `.pm5mtb` compact typed files.
- The compact typed stream is validated against the raw typed stream on the PM5M BTC+ETH+SOL 3-hour
  golden window `1782203400000000000..1782214200000000000`: 14,823,060 book updates match exactly,
  with identical stream hash `7c341aef2b002e6f5095cc2e28f53772abcf7089a11f8e9e8f25555faf815c58`.
- On the same 3-hour window, current release results using
  `chunks/chunk_01/settlement_hftsettle1` are:
  - raw serial: 136.8s;
  - compact typed serial: 43.0s;
  - compact typed condition-direct, 16 workers: 50.8s.
- Raw serial, compact typed serial, and compact typed condition-direct now produce identical
  current/V1 order counts, PnL, intent hashes, and ledger hashes. The older saved 181s raw summary
  is not a current-code baseline.
- Default exact replay path for this workload is now compact typed serial unless a specific benchmark
  shows condition-direct is faster. Condition-direct remains available, but routes by
  `hash(condition_id)` for correctness and should not be assumed faster.
- Fixed condition-count shards and market-hour condition caches are not the default implementation
  path. They remain design options only after they beat compact typed serial and match golden hashes.
- 2026-06-25 follow-up experiments rejected three narrower "zero-copy" reader tweaks on the same
  3-hour window:
  - mmap `.pm5mtb` segment reader was exact but slower: 46.3s and 50.9s versus the 43.0s baseline;
  - sequential row visitor that avoided `Vec<MarketReplayCompactTypedRecord>` was exact but slower:
    44.9s versus the 43.0s baseline;
  - borrowed affected-asset refs preserved hashes after sorting and cut the local
    `affected_assets_ns` bucket from 1.81s to about 1.36s, but end-to-end runs were slower
    (46.9s and 49.2s), so it was not kept.
- Current conclusion: do not optimize by making the existing `.pm5mtb` reader more "zero-copy".
  The next real target is the per-update state/strategy path: replay-state apply, `StreamBook`
  construction, market-state apply, and V1 book-event evaluation.

Implementation status, 2026-06-24 CST:

- Phase 0 is complete. Replay semantics are frozen in `MarketReplaySemantics` and the Phase 0
  golden window is recorded in
  `docs/replay_golden_windows/live_1u_compare_20260624_1038_btc5m.json`.
- Versioned strategy snapshots now include the live runtime config, current
  `position_v4_edge` configs, and `hft_private/configs/frozen_edge_hold_v1_default_20260624.json`.
- Phase 1 is complete in `crates/pm5m_market_cache/src/market_replay.rs`.
- The CLI entry `pm5m-market-cache build-market-replay-dataset` emits normalized Polymarket WS
  market events into an `events` Parquet table plus `catalog.market_replay.json`.
- The raw reader has deterministic ordering, source pointers, visible-window filtering, fail-closed
  hour-bucket coverage validation, and duplicate raw-root de-duplication.
- Phase 2 is complete with `StreamingMarketReplayState`, ordered Market Replay event output, raw
  payload-level updates, and `pm5m-market-cache bench-market-replay-dataset`.
- The Market Replay Dataset accepts `reference_ws_raw` and emits closed 1s `reference_bar` events
  with configurable reference latency.
- `StreamingMarketReplayState` applies book, reference, condition metadata, settlement state,
  residual visible ask depth, and pending delayed FAK buy orders.
- Tests cover timestamp ordering, missing coverage, duplicate raw roots, raw-update/event-state
  equivalence, settlement winner state, reference visibility, and sampled exact equality against
  `Replay State Index` for best bid/ask plus 1u and 5u sweeps.
- A 1-minute real BTC+reference smoke produced 196,627 ordered events, 7.0 MiB output, and
  state-apply bench throughput around 200k events/sec on the local release binary.
- Phase 3 live-comparison wiring is implemented through two private CLIs:
  `pm5m_backtest market-replay-live-compare` for live-order fillability, and
  `pm5m_backtest market-replay-strategy-compare` for actual/current/V1 strategy comparison.
- The private CLI now has `pm5m_backtest market-replay-live-compare`, which reads live
  `order_audit`, maps condition/outcome to asset ids through `HFTSETTLE1`, streams a
  Market Replay Dataset, maintains residual ask depth between book updates, and compares
  live FAK fills with same-latency local fillability.
- A real BTC-5M smoke over `1782193100000000000..1782193380000000000` produced 1,360,635 replay
  events in 27.3s and replayed them for live comparison in 8.0s; the four in-window live orders
  matched exactly: 3 fills and 1 unfilled on both live and Market Replay.
- The same smoke can now run without materializing Parquet by using direct raw streaming:
  `pm5m_backtest market-replay-live-compare --raw-root ...`. It matched the same 4 orders and took
  9.3s end-to-end, avoiding the 27.3s dataset build. This proves the hot path should stream raw
  updates directly for live comparison.
- The raw direct path now has a payload-level fast path:
  `stream_market_replay_raw_updates_from_raw` plus `StreamingMarketReplayState::apply_raw_update`.
  It applies a WS `book` or `price_change` payload as one update instead of expanding every
  level/change into independent replay rows.
- Live compare also filters raw updates to the target asset ids present in the live orders and
  trims the raw scan end to `last_order_arrival + poly_freshness_guard`.
- On the latest BTC-5M smoke, the optimized raw path matched the same 4 orders and took 4.56s
  end-to-end. It scanned 363,648 raw updates, applied 257,985 target-relevant updates, and kept only
  the 2 target books in memory.
- The new live-compare reader tolerates truncated `.zst` order-audit files copied from active live
  runs and records the affected paths in the summary.
- HFTBOOK2/HFTIDX1 behavior is unchanged while the new path is introduced.
- There are no remaining Phase 0-3 functional implementation items. Remaining performance work for
  15-day/month research starts at Phase 4: first make the exact raw-direct path use hardware
  efficiently, then add optional accelerated replay datasets for repeated parameter sweeps.
- Phase 4E condition-bucket parallel replay is implemented in the private strategy compare CLI.
  It keeps exact per-condition market updates, replays independent condition buckets on multiple
  workers, and merges standard current/V1 artifacts.
- BTC-5M 3-hour exact replay from prepared condition buckets now runs in 7.2s on 16 workers
  versus 31.9s for the typed-shard serial path. Current/V1 orders, fills, PnL, intent hashes, and
  ledger hashes match the serial typed-shard baseline exactly.
- Phase 4H added direct condition-parallel replay with compact per-worker asset metadata. It avoids
  writing condition bucket files on first run and runs the same BTC-5M 3-hour exact comparison in
  28.3s on 16 workers, with the same current/V1 hashes as the bucket and serial baselines.
- Phase 3 raw-direct performance smoke:
  - BTC-5M 4.7-minute window: 384,633 replay events, 7.17s, actual/current/V1 sections written.
  - PM5M BTC+ETH+SOL 3-hour window: 14,856,900 replay events, 181s, actual/current/V1 sections
    written.
  - PM5M BTC+ETH+SOL 3-hour window with `--parallel-by-symbol --parallel-workers 3`:
    14,879,460 replay events, 129.7s wall time, aggregate actual/current/V1 metrics match the
    serial run. The long pole is BTC-5M at 129.7s; ETH-5M took 31.7s and SOL-5M took 22.9s.
  - This is valid for daily evidence, but a single-process raw scan is not the final 21-hour
    all-symbol performance path. For 21h all-symbol runs, use symbol/horizon parallelism first,
    then BTC-root internal scan/decode parallelism. Accelerated datasets are a later research
    convenience, not the required correctness path.

## Executive Decision

The ETL and replay stack should be split into three explicit layers, aligned with the terminology
used by classic market-replay frameworks such as `hftbacktest`:

```text
Raw Feed
  -> Market Replay Dataset
      -> Streaming Market Replay Engine
      -> Accelerated Replay Dataset
      -> Depth State Store / Replay State Index
```

The primary daily path for live comparison is:

```text
live order logs
  -> actual PnL

Raw Feed / Market Replay Dataset iterator
  -> Streaming Market Replay Engine
      -> current strategy replay
      -> V1 strategy replay
      -> current vs V1 vs actual comparison
```

The primary large-window exact replay path is:

```text
Raw Feed
  -> parallel raw scan/decode/filter
  -> ordered merge
  -> Streaming Market Replay Engine
```

The primary repeated parameter-sweep path is:

```text
Accelerated Replay Dataset
  -> current/V1/parameter replay
  -> metrics and comparison reports
```

`Depth State Store / Replay State Index` is no longer the default research path. It is an optional,
heavier path for point-in-time full-book inspection, forensic replay, and validating accelerated
outputs. `Accelerated Replay Dataset` must also not become a replacement default for exact live
comparison; it is for repeated runs over a known strategy-family field set.

## Why Change

The previous compact-cache path made backtests faster than raw replay, but profiling showed it is
not the right default for live comparison:

- A 3-hour chunk expanded from roughly 10-13 GiB raw input to 34-41 GiB of book/index state.
- Cold `HFTBOOK2 + HFTIDX1` construction took roughly 6-7 minutes per 3-hour chunk.
- Strategy replay itself was not the bottleneck; repeated load/index work and full depth-state
  materialization were.
- A 15-day or 30-day window would turn the depth-state path into multi-TiB intermediate data and
  hours of cold build time.

The live-vs-backtest problem is sequential by nature: from live start to cutoff, state is consumed in
time order. It does not require random access to every historical book state. A streaming replay loop
can maintain the same visible book/reference state in memory and evaluate decisions as time advances.

## Terminology

Use these names in code, CLIs, docs, and run manifests going forward.

| New name | Current/local equivalent | Purpose |
| --- | --- | --- |
| `Raw Feed` | HFTREC4 raw payloads | Append-only truth source for audit and rebuilds. |
| `Market Replay Dataset` | Normalized event stream, not yet fully implemented | Strategy-neutral tick/event replay input. |
| `Streaming Market Replay Engine` | New live comparison engine | Sequential replay that maintains current state in memory. |
| `Accelerated Replay Dataset` | Previously discussed as "tape" | Precomputed strategy-family sufficient state for fast repeated backtests. |
| `Depth State Store` | HFTBOOK2 | Materialized full depth state by time/asset. |
| `Replay State Index` | HFTIDX1 | Point-in-time lookup index over depth state. |
| `Reference Replay Dataset` | HFTREF1 | Normalized reference-market state with latency semantics. |
| `Settlement Dataset` | HFTSETTLE1 | Outcome/winner data used for realized PnL. |

Do not use `tape` as a durable name. It is an implementation analogy, not a product or schema name.

## Layer Contracts

### Raw Feed

`Raw Feed` is the immutable truth source.

Responsibilities:

- store raw WS/REST payload bytes;
- preserve local receive timestamps, source identifiers, and payload hashes;
- maintain segment manifests and coverage metadata;
- support rebuilds of all downstream datasets.

Non-goals:

- no strategy fields;
- no model features;
- no hot-path backtesting;
- no retroactive mutation except explicit quarantine/repair records.

Existing HFTREC4 files can continue to be the physical format for this layer.

### Market Replay Dataset

`Market Replay Dataset` is the canonical normalized event stream for deterministic replay.

It should be strategy-neutral and contain typed events:

```text
venue
stream
symbol
horizon_seconds
condition_id
asset_id
event_type          # book_snapshot, depth_delta, trade, reference_bar, market_metadata, settlement
exchange_ts_ns
local_recv_ts_ns
visible_ts_ns
side
price_micros
qty_micros
order_id_or_seq
raw_record_hash
payload_hash
flags
```

Responsibilities:

- parse raw payloads once into typed events;
- normalize timestamp semantics;
- compute `visible_ts_ns` according to the Poly server time model;
- bind condition/asset metadata;
- provide settlement/outcome joins;
- expose a sequential iterator ordered by `(visible_ts_ns, source_priority, sequence)`;
- fail closed on missing coverage for required windows.

Non-goals:

- no strategy-specific edge/model columns;
- no full materialized book state by default;
- no random `state_at(ts)` requirement in the primary API.

### Streaming Market Replay Engine

`Streaming Market Replay Engine` is the correct primary path for live-vs-backtest comparison.

It consumes `Market Replay Dataset` in time order and maintains in-memory state:

```text
latest book by asset_id
latest reference bars by symbol
active market metadata by condition_id
pending simulated orders ordered by arrival_ts
strategy/risk state by strategy_instance_id
settlement state by condition_id
```

The engine evaluates all requested strategy instances in one pass:

```text
for event in replay_events:
    apply event to in-memory state
    settle pending simulated orders whose arrival_ts <= event.visible_ts
    on reference decision ticks:
        evaluate current strategy
        evaluate V1 strategy
        enqueue simulated FAK orders at decision_ts + configured_latency
```

This is equivalent to `state_at(ts)` for sequential replay because the current in-memory state after
applying all events with `visible_ts_ns <= ts` is exactly the visible market state at `ts`.

300 ms submit latency does not require a random-access index. The engine should enqueue a pending
order at decision time and fill it when replay time reaches `decision_ts + 300 ms`, using the
then-current in-memory book.

Primary outputs:

- actual live summary from order/audit logs;
- local replay summary for the same strategy/config as live;
- V1 replay summary for the same window;
- per-strategy fills, rejects, and condition ledgers;
- mismatch classification between actual and replay.

### Accelerated Replay Dataset

`Accelerated Replay Dataset` is the fast research input for repeated parameter sweeps over the same
strategy family.

It stores precomputed sufficient state, not full depth history. It is an optional derived cache, not
the default evidence path for exact live comparison. A change in required fields, latency model,
order size, or strategy feature can require adding fields or rebuilding it.

Example fields:

```text
ts_ns
symbol
horizon_seconds
condition_id
window_start_ts_ns
window_end_ts_ns
reference_price_micros
reference_age_ms
anchor_price_micros
sigma_60_micros
sigma_180_micros
momentum_60s_bps
btc_lead_momentum_60s_bps
yes_best_bid_micros
yes_best_ask_micros
yes_sweep_1u_avg_micros
yes_sweep_5u_avg_micros
yes_book_age_ms
no_best_bid_micros
no_best_ask_micros
no_sweep_1u_avg_micros
no_sweep_5u_avg_micros
no_book_age_ms
arrival_ts_ns
arrival_yes_sweep_1u_avg_micros
arrival_no_sweep_1u_avg_micros
winner
```

Changes that should not require rebuilding this dataset:

- edge threshold;
- allowed side;
- max entries;
- order size up to precomputed sweep sizes;
- active caps;
- trial/live portfolio composition;
- current vs V1 strategy selection;
- basic risk throttles that use already stored fields.

Changes that do require adding fields or rebuilding:

- new depth features not stored in the dataset;
- queue-position modeling;
- cancellation/order-flow features;
- new latency model not representable by stored arrival snapshots;
- new symbols/horizons;
- larger order sizes than precomputed sweep depth.

The initial accelerated dataset should be deliberately narrow: only BTC/ETH/SOL, PM5M/PM15M, and
the fields needed by current live strategy plus `OPTION_V1_GRAY`.

### Depth State Store / Replay State Index

`Depth State Store` and `Replay State Index` are retained as optional forensic infrastructure.

Use them for:

- inspecting full order book state at a specific time;
- debugging a disputed fill or mismatch;
- validating streaming/accelerated replay on sampled windows;
- researching features that genuinely need full depth state;
- rebuilding an accelerated dataset when raw parsing is not needed.

Do not use them for:

- daily live-vs-backtest comparison;
- latest-window current vs V1 reports;
- routine parameter sweeps;
- month-level strategy iteration.

## Required ETL Changes

### 1. Introduce MarketEvent Schema

Add a durable `MarketEvent` schema in the shared ETL/replay crate. It should be versioned and
independent of strategy code.

Minimum fields:

```text
schema_version
source
venue
stream
symbol
horizon_seconds
condition_id
asset_id
event_type
exchange_ts_ns
local_recv_ts_ns
visible_ts_ns
sequence
side
price_micros
qty_micros
order_id_or_seq
raw_record_hash
payload_hash
flags
```

Implementation notes:

- Keep fixed-point numeric fields.
- Keep repeated identifiers internable/dictionary-friendly.
- Ensure deterministic ordering for same-timestamp events.
- Include source pointers back to raw segment and row/hash.

### 2. Split Raw Parsing From Book Materialization

Current raw-to-book builders should be refactored so the parser can emit normalized `MarketEvent`
records before any book state is materialized.

Target split:

```text
Raw Feed reader
  -> MarketEvent parser
  -> Market Replay Dataset writer

Market Replay Dataset reader
  -> Depth State Store builder
  -> Replay State Index builder
```

The parser must be reusable by both streaming replay and optional depth-state builds.

### 3. Build Streaming Replay State

Add an in-memory state engine that applies `MarketEvent` records:

```text
BookStateByAsset
ReferenceStateBySymbol
MarketMetadataByCondition
SettlementStateByCondition
PendingOrderQueue
StrategyRuntimeState
```

The state engine should expose strategy-facing methods:

```text
book_for(asset_id)
reference_for(symbol, ts)
active_markets(symbol, horizon, ts)
sweep_buy(asset_id, notional, ts)
enqueue_order(intent, arrival_ts)
settle_due_orders(ts)
```

The strategy kernel should not care whether these values come from streaming replay, live runtime,
or a depth-state index.

### 4. Add Live Comparison CLI

Add a dedicated CLI for the original business need:

```text
pm5m-replay compare-live-window \
  --start-ts-ns ... \
  --end-ts-ns ... \
  --live-log-root ... \
  --market-replay-root ... \
  --settlement-root ... \
  --live-config ... \
  --v1-config ... \
  --out-dir ...
```

It must produce three sections:

```text
actual_live
local_replay_same_strategy
local_replay_v1
```

It should also emit:

- per-strategy fill summary;
- per-condition ledger;
- realized/MTM PnL;
- reject/miss reasons;
- mismatch rows for live filled vs replay not filled, replay filled vs live not filled, and price/PnL
  differences.

### 5. Add Accelerated Replay Builder

Add an incremental builder:

```text
pm5m-replay build-accelerated-dataset \
  --market-replay-root ... \
  --settlement-root ... \
  --symbols BTC,ETH,SOL \
  --horizons 300,900 \
  --start-ts-ns ... \
  --end-ts-ns ... \
  --out-root ...
```

The builder should use the same streaming state engine and write rows only at strategy decision and
arrival points. It should not store full book state.

### 6. Add Accelerated Replay Runner

Add a runner for repeated research:

```text
pm5m-replay run-accelerated \
  --accelerated-root ... \
  --strategy-config ... \
  --start-ts-ns ... \
  --end-ts-ns ... \
  --out-dir ...
```

This runner should handle current strategy, V1, and simple sweeps without touching raw feed or depth
state index.

### 7. Keep Depth State Builders, But Move Them Out Of The Default Path

Rename documentation and CLI help around existing cache/index tools:

```text
HFTBOOK2 -> Depth State Store
HFTIDX1  -> Replay State Index
```

The underlying file format names can remain for compatibility, but user-facing docs and run
manifests should make clear that these are forensic/index products, not the default live comparison
path.

## Implementation Order

### Phase 0: Freeze Semantics Before Refactor

Goal: prevent hidden logic drift.

Status: complete.

Completed tasks:

- Documented current live strategy instances and V1 rules in versioned config snapshots:
  `hft_private/configs/position_v4_edge_live.json`,
  `hft_private/configs/position_v4_edge_live_eth_gray_20260620.json`, and
  `hft_private/configs/frozen_edge_hold_v1_default_20260624.json`.
- Wrote Poly visible-time rules into the shared Market Replay implementation through
  `MarketReplaySemantics` and the raw/event stream options.
- Froze fee, fill, settlement, reference-latency, Poly-latency, freshness, and submit-latency
  assumptions for replay through stable model ids.
- Added the BTC-5M golden window with known live order/fill outcomes in
  `docs/replay_golden_windows/live_1u_compare_20260624_1038_btc5m.json`.

Acceptance evidence:

- Existing live actual summary is captured by stable metrics and the
  `live_actual_summary.txt` SHA-256 in the golden manifest.
- Current HFTIDX-based replay still works: `cargo test` in `hft_private` passes the existing
  HFTIDX fast-path and delayed FAK tests.
- Golden output is checked by stable metrics and `market_replay_live_compare_orders.jsonl`
  SHA-256; elapsed wall-clock time is intentionally recorded only as a performance observation,
  not as a hash-stable assertion.

### Phase 1: MarketEvent Schema And Reader

Goal: create the common replay input without changing strategy behavior.

Status: complete.

Completed tasks:

- Added `MarketEvent` and `pm5m.market_replay_dataset.v1` schema.
- Added raw `HFTREC4` -> `MarketEvent` streaming and dataset writers.
- Included deterministic ordering and source pointers:
  visible timestamp, local receive timestamp, ingest sequence, source segment, row index, sequence,
  asset id, and event type.
- Added fail-closed coverage validation for explicit start/end windows.
- Added tests for timestamp ordering, visible-window filtering, missing coverage, duplicate raw
  roots, and raw-vs-dataset event counts.

Acceptance evidence:

- A known raw window converts to Market Replay Dataset and can also be streamed directly.
- Event counts and coverage are asserted in `crates/pm5m_market_cache/tests/market_replay.rs`.
- No existing strategy replay path was changed; HFTBOOK2/HFTIDX1 tests and private backtest tests
  still pass.

### Phase 2: Streaming State Engine

Goal: replay market state sequentially without building a depth index.

Status: complete.

Completed tasks:

- Implemented book, reference, condition metadata, settlement, residual depth, and pending-order
  state application in `StreamingMarketReplayState`.
- Implemented non-consuming and residual-consuming buy sweeps from in-memory book state.
- Implemented pending delayed FAK buy orders ordered by arrival timestamp.
- Implemented fail-closed freshness checks for delayed order execution.
- Added sampled equivalence tests against existing `Replay State Index` for:
  - same best bid/ask at sampled decision timestamps;
  - same sweep result for 1u and 5u;
  - same reference bar visibility by visible timestamp.
- Added settlement winner state tests in the streaming state contract. Settlement is not stored in
  `Replay State Index`, so the assertion is contract-level rather than index-level.

Acceptance evidence:

- Streaming state matches indexed state on sampled windows within exact fixed-point equality for
  stored book fields and sweep results.
- Delayed FAK simulation uses the same arrival-time visible state and residual-depth consumption as
  the live comparison smoke: 4 comparable BTC-5M live orders, 3 filled and 1 unfilled, with zero
  fillability mismatches.

### Phase 3: Live Comparison CLI

Goal: satisfy the original actual/current/V1 comparison without depth index.

Status: complete for the functional raw-direct path.

Completed tasks:

- Parse live logs and produce `actual_live`.
- Run same-strategy local replay through the streaming engine.
- Run V1 replay in the same raw/reference stream.
- Write unified comparison report:
  `market_replay_strategy_compare_summary.json`.
- Write standard per-variant backtest artifacts under `current_replay/` and `v1_replay/`.
- Include aggregate mismatch classification for `actual_vs_current` and `current_vs_v1`.
- Avoid `Depth State Store` and `Replay State Index`; the command consumes raw HFTREC4,
  `HFTREF1`, and `HFTSETTLE1`.
- Load enough reference warmup to cover both sigma lookback and market-start anchor lookup.
- Add `--parallel-by-symbol --parallel-workers N`, which runs independent symbol shards in
  parallel and merges aggregate summaries while preserving each shard's standard artifacts.
- Preserve BTC reference in every shard because SOL policy needs BTC 60s lead momentum.

Acceptance evidence:

- Latest live windows can be compared without building `Depth State Store` or `Replay State Index`.
- Output contains the three required sections:
  - actual live performance from 1u start;
  - same-strategy local replay over same time;
  - V1 replay over same time.
- BTC-5M smoke over `1782193100000000000..1782193380000000000`:
  - elapsed 7.17s;
  - actual live: 4 orders, 3 fills;
  - current replay: 1 order, 1 fill;
  - V1 replay: 12 orders, 9 fills.
- PM5M BTC+ETH+SOL 3-hour smoke over `1782203400000000000..1782214200000000000`:
  - elapsed 181s;
  - actual live: 144 orders, 128 fills;
  - current replay: 102 orders, 102 fills;
  - V1 replay: 299 orders, 299 fills.
- PM5M BTC+ETH+SOL 3-hour parallel-by-symbol smoke over the same window:
  - elapsed 129.7s;
  - aggregate actual/current/V1 metrics match the serial run;
  - shard timings: BTC-5M 129.7s, ETH-5M 31.7s, SOL-5M 22.9s.
- Runtime note: symbol parallelism uses hardware safely without time-boundary artifacts, but the
  BTC shard remains the long pole. The stricter 21-hour all-PM5M/PM15M "few minutes" target needs
  Phase 4A root-internal parallel scan/decode work before accelerated research caches.

### Phase 4A: Parallel Raw Scan/Decode Hot Path

Goal: make exact live comparison and large-window current/V1 replay use available CPU and IO without
introducing another mandatory precomputed dataset.

Rationale:

- Exact replay is sequential at the state-transition layer, but raw segment scan, zstd decode,
  payload parse, timestamp normalization, and symbol filtering are independent per segment.
- The safe parallelization boundary is before ordered replay: workers decode/filter candidate raw
  segments, then the main replay loop consumes a deterministic ordered merge by
  `(visible_ts_ns, source_priority, ingest_seq, source_row_idx)`.
- This keeps the same semantic model as the current raw-direct Phase 3 path and does not drop any
  book update, reference event, timestamp, or payload hash.
- Time-slice strategy replay is not the first target because pending orders, residual depth,
  reference anchors, and open lots make chunk boundaries stateful. Segment-level decode parallelism
  avoids those correctness risks.

Tasks:

- Add profiling counters around raw scan, zstd decode, raw payload parse, symbol filtering,
  ordered-merge heap time, state apply, and strategy evaluation.
- Add a bounded worker pool for HFTREC4 segment scan/decode/filter.
- Keep per-worker output sorted by the existing raw update ordering key.
- Merge worker streams with the existing deterministic ordering and assign `global_event_seq` only
  after merge.
- Preserve fail-closed coverage validation before dispatching workers.
- Add CLI knobs for worker count and bounded queue size.
- Add a serial-vs-parallel equivalence test on a golden window comparing event counts, ordering,
  fill decisions, and aggregate PnL.

Acceptance:

- Serial and parallel raw-direct runs produce identical current/V1 aggregate metrics on the golden
  BTC-5M window and the PM5M BTC+ETH+SOL 3-hour window.
- Parallel raw-direct speedup is reported with per-stage timing.
- No `Depth State Store`, `Replay State Index`, or accelerated dataset build is required for exact
  live comparison.

Profiling baseline, 2026-06-24 CST:

- BTC-5M 3-hour raw-direct profile:
  - elapsed 130.2s;
  - raw stream excluding callback 46.4s;
  - callback total 79.1s;
  - `apply_raw_update` 75.7s;
  - strategy book-event evaluation 0.75s;
  - report writing 4.2s.
- Event counts:
  - raw update callbacks: 10,464,099;
  - `price_change`: 10,305,370;
  - `book`: 158,729;
  - changed strategy book rows: 158,395;
  - zero-changed callbacks: 10,305,704;
  - updates missing market window metadata: 10,305,704;
  - horizon counts: `300` = 158,395, `none` = 10,305,704.

Implication:

- The first Phase 4A optimization is not raw reader parallelism. The immediate bottleneck is that
  `price_change` updates are parsed and applied millions of times but currently do not become
  strategy-visible book rows because the adapter builds `BookRow` metadata from the current raw
  update, and these incremental updates often lack market window fields.
- Next implementation order:
  1. carry market window/symbol/horizon metadata in `ReplayBookState` from the latest full book
     snapshot and build strategy `BookRow` from replay state, not from the incremental update;
  2. parse each raw payload once and share the parsed result between affected-asset extraction and
     state application;
  3. add exact observable-state gating: keep replay book state current for every relevant update,
     but only emit a strategy `BookRow` when the strategy/execution-visible top-10 view changes;
  4. re-profile the same BTC-5M 3-hour window;
  5. only then add segment/root internal raw scan/decode parallelism if raw reader time remains a
     long pole.

Implementation update:

- `ReplayBookState` now retains market window metadata from the full book snapshot, and private
  strategy replay builds `BookRow` from replay state metadata when an incremental `price_change`
  lacks those fields.
- Private strategy replay now parses each raw update payload once and reuses it for affected-asset
  extraction and state application.
- A BTC-5M 4.7-minute smoke after the metadata fix produced 549,555 changed strategy book rows from
  383,873 raw updates. This confirms incremental price changes now reach the strategy state.
- The same smoke took 23.1s when V1 `record_reject_rows=true`, mostly due to a 212 MiB
  `reject_rows.json`. With reject rows disabled for the V1 profiling config, the same run took
  8.35s; fills/PnL were unchanged. Large-window comparison should keep reject-row diagnostics off
  unless a specific disputed condition is being inspected.
- `hft_private/configs/frozen_edge_hold_v1_default_20260624.json` now keeps
  `record_reject_rows=false`; this is an artifact-size setting, not a strategy decision rule.

Re-profile after metadata propagation and parse-once:

- BTC-5M 3-hour raw-direct profile:
  - elapsed 162.7s;
  - raw stream excluding callback 40.6s;
  - callback total 121.7s;
  - `apply_raw_update` 77.3s;
  - strategy book-event evaluation 41.9s;
  - report writing 0.018s.
- Event counts:
  - raw update callbacks: 10,464,099;
  - changed strategy book rows: 15,831,665;
  - zero-changed callbacks: 2,469,069;
  - `price_change`: 10,305,370;
  - `book`: 158,729.

Updated implication:

- Correct metadata propagation makes incremental price changes strategy-visible; replay results also
  changed, which confirms the previous profile was not a correct final execution path.
- The new long poles are state application and excessive strategy `BookRow` emission:
  `apply_raw_update` is 47.5% of wall time, strategy book-event evaluation is 25.8%, and raw reader
  work is 25.0%.
- Next optimization should reduce emitted strategy rows before adding reader parallelism:
  maintain exact latest execution state for every raw update, but emit strategy decision events only
  on decision-relevant observable changes or reference ticks. This must be guarded by golden
  serial-vs-optimized equivalence for orders, fills, PnL, and ledger hashes.

Hot-path slimming update:

- Avoided cloning the full replay book state when converting state into a private strategy
  `BookRow`.
- Avoided repeated settlement asset lookups when market metadata for a condition already exists.
- Added a streaming-state `apply_stream_book` path that moves the latest `BookRow` into
  `MarketState` instead of cloning it.
- BTC-5M 3-hour profile results:
  - metadata propagation + parse-once: 162.7s total, `apply_raw_update` 77.3s;
  - no full replay-book clone: 148.5s total, `apply_raw_update` 61.6s;
  - move latest `BookRow` into state: 145.0s total, `apply_raw_update` 57.6s.
- Orders, fills, PnL, and ledger hashes were unchanged across these hot-path slimming runs:
  current replay 70 orders / 70 fills / 34,890,416 micros PnL, V1 replay 147 orders / 147 fills /
  2,428,305 micros PnL.

Deep profiling breakdown, same BTC-5M 3-hour window:

- raw reader excluding callback: 43.2s;
- payload JSON parse: 22.6s;
- affected asset extraction: 1.3s;
- replay book state apply: 10.6s;
- strategy `BookRow` build: 15.5s;
- market metadata upsert: 0.6s;
- latest `MarketState` apply: 3.2s;
- latest book lookup: 0.5s;
- current book event path: 3.0s;
- V1 book event path: 39.9s;
- due-order execution: 1.1s;
- reference processing: 1.0s.

This shows the concrete remaining bottlenecks:

- V1 book-event strategy evaluation is the largest single callback-side cost.
- JSON payload parsing is the largest raw-update parsing cost.
- `BookRow` construction is still a material cost even after removing full-book clones.
- Raw reader work is now comparable to the largest callback-side components, so reader
  parallelism is useful after the single-update hot path is simplified.

V1 book-event hot-path update:

- The runner now skips book-event dispatch for `position_v4_edge` variants because that strategy
  acts on reference ticks in this comparison path.
- Frozen V1 no longer constructs reject rows when `record_reject_rows=false`.
- Frozen V1 caches model/reference calculations by `(condition, reference symbol, current
  reference row)`; every book update is still evaluated, but repeated model work inside the same
  reference row is reused. Per-event reference age and book age remain event-time based.
- ASCII symbol-prefix checks no longer allocate uppercase strings on the hot path.
- BTC-5M 3-hour detailed profile after this update:
  - total elapsed: 112.1s, down from 154.5s;
  - V1 book-event path: 6.4s, down from 39.9s;
  - current book-event path: 0.0s, down from 3.0s;
  - orders, fills, PnL, intent hashes, and ledger hashes were unchanged.
- New dominant costs:
  - raw reader excluding callback: 41.0s;
  - raw payload JSON parse: 22.9s;
  - replay state apply: 9.5s;
  - strategy `BookRow` build: 14.3s;
  - V1 book-event path: 6.4s.

Implication:

- The V1 strategy itself is no longer the main obstacle for minute-level 3-hour comparisons.
- Large-window exact replay now needs data-supply optimization: typed binary replay input or
  parallel raw scan/decode, then fewer transient `BookRow` allocations where semantics permit.

Phase 4B implementation update:

- Added a minimal typed update layer:
  `Raw JSON -> MarketReplayTypedUpdate -> StreamingMarketReplayState`.
- Strategy compare can now read a typed update file via `--typed-update-file`; raw input remains the
  default path.
- Added `pm5m-market-cache build-market-replay-typed-updates` to build a sequential typed update
  file from raw HFTREC4.
- Typed update files are zstd-compressed binary streams. BTC-5M 3-hour output is 688 MiB versus
  4.2 GiB uncompressed.
- Added shard output for typed updates:
  - single-file mode remains available with `--output-file`;
  - production mode uses `--output-root`, writing
    `symbol=<SYMBOL>/hour_bucket=<HOUR>/part-00000.pm5mtu.zst`;
  - a root manifest is written to `manifest.market_replay_typed_updates.json`.
- `pm5m_backtest market-replay-strategy-compare` now accepts multiple `--typed-update-file`
  arguments and `--typed-update-root`. Root discovery filters shard files by requested
  `raw_start_ts_ns/raw_end_ts_ns` hour buckets before replay.
- Multi-file typed replay uses a k-way merge on preserved `global_event_seq` and fails closed if
  the input files are not one strictly monotonic build stream. This keeps the same book-update
  order as the original raw replay and prevents accidental concatenation of independently built
  files.
- `BookRow` hot path no longer builds per-update `primary_key` / `row_hash` strings. Decision event
  keys are materialized only when an intent is actually emitted.
- BTC-5M 3-hour results:
  - raw path after typed update + lazy key: 97.7s;
  - typed binary path, zstd: 30.6s;
  - typed shard root path, zstd: 34.2s;
  - orders, fills, PnL, intent hashes, and ledger hashes were unchanged.
- BTC-5M 3-hour typed shard build wrote four hourly shards under `symbol=BTC-5M`, with
  10,464,099 updates in 83.4s.
- Phase 4D replaced hot-path `BookRow` materialization in streaming replay with a lightweight
  `StreamBook` plus per-asset metadata cache. Strategy and execution code now consume a shared
  `BookSnapshot` interface, while old dataset/index paths still accept `BookRow`.
- BTC-5M 3-hour typed shard replay after `StreamBook`:
  - total elapsed: 31.9s, down from 34.2s;
  - book materialization profile bucket: 3.6s, down from 8.0s;
  - current/V1 orders, fills, PnL, intent hashes, and ledger hashes were unchanged.
- Phase 4E added exact condition-bucket parallel replay:
  - CLI flags:
    `--parallel-by-condition --condition-workers N --condition-replay-root <root>`;
  - input must be typed replay data (`--typed-update-file` or `--typed-update-root`);
  - build phase streams the typed updates once, routes each top-10 book update to a deterministic
    bucket by `condition_id`, and writes reference ticks to the buckets that have active books for
    that reference symbol;
  - worker phase replays each bucket independently and writes merged standard artifacts under
    `current_replay/` and `v1_replay/`;
  - cache manifest records the typed files, reference cache root, settlement root, Poly visible-time
    parameters, reference latency, time window, symbol allowlist, and bucket paths; if any of these
    change, the bucket cache is rebuilt rather than reused.
- BTC-5M 3-hour condition-bucket results:
  - 8-worker reuse run from prepared buckets: 11.1s wall time, 11.0s reported elapsed;
  - 16-worker first run including bucket build after writer hot-path optimization: 35.3s wall time;
  - 16-worker bucket build itself after writer hot-path optimization: 27.5s, 16 bucket files, 188
    MiB total;
  - 16-worker reuse run from prepared buckets after writer hot-path optimization: 7.7s wall time,
    7.6s reported elapsed;
  - replay events: 10,475,379 total, 10,464,099 book events, 11,280 reference events;
  - current replay: 70 orders / 70 fills / 34,890,416 micros PnL;
  - V1 replay: 147 orders / 147 fills / 2,428,305 micros PnL;
  - current intent hash:
    `800a7bf4ae8881c9a65ebc2c6b6e0dd8a41fbeb3198352746c9a19f4738c97ca`;
  - current ledger hash:
    `40f84c4ea30f5a74203b967aec57046a31fb0b55ec2c3aa4ae9d5be502b098f6`;
  - V1 intent hash:
    `833487f8d519726ef7de1b5a5f61eacdcacedd0fff2c6550fbac92e52ce1119c`;
  - V1 ledger hash:
    `7a0d958b177dfd6091176cf807f18d0f79ac1adc75a6bedbfbc23b07e49dd004`;
  - all four hashes match the serial typed-shard baseline.
- Phase 4F profiled condition-bucket build with `--condition-build-profile`. Profiling itself adds
  overhead, so production runs keep it off by default.
- BTC-5M 3-hour build-profile result after the writer hot-path optimization:
  - profiled bucket-build elapsed: 31.1s;
  - typed read/decompress/merge outside callback: about 7.6s;
  - callback total: 23.4s;
  - `apply_typed_update`: 12.0s;
  - bucket book write: 8.3s, down from 11.2s before reusing writer buffers and avoiding per-book
    owned event/string allocation;
  - reference routing/write: 0.8s;
  - update accounting/observe: 0.9s.
- Build-time implication for 15-day/30-day windows:
  - the current serial bucket builder still scales linearly with market updates;
  - extrapolating 27.5s per 3-hour BTC-5M window is still too slow for 30 days;
  - further micro-optimizing the writer cannot change that order of magnitude because
    `apply_typed_update` plus typed read/decode remain the long poles.
- Next exact optimization should parallelize the bucket build itself by condition bucket:
  - one serial dispatcher reads typed updates in order and routes per-asset updates to the target
    condition bucket;
  - each bucket worker owns its local replay book state and zstd writer;
  - book snapshots route directly by `condition_id`;
  - incremental price changes route by the asset-to-condition mapping learned from prior snapshots;
  - reference ticks are sent to bucket workers and written only when that worker has active books;
  - event sequence numbers are assigned by the dispatcher in original update order, preserving
    deterministic merge order.
- Expected effect: this keeps exactness but moves the current single-thread
  `apply_typed_update + bucket write` work onto available cores. The remaining serial floor becomes
  typed shard read/decompress/merge plus dispatch overhead.
- Phase 4G implemented this dispatcher/worker prototype and tested it on the BTC-5M 3-hour window.
  The exactness bug found in the first attempt was metadata reset handling: when a full book
  snapshot lacks market metadata, the serial builder clears the replay book metadata and later
  metadata-less price changes stop emitting book events; the dispatcher must also clear the
  asset-to-bucket route. After fixing that, current/V1 orders, fills, PnL, intent hashes, and
  ledger hashes matched the serial bucket baseline.
- Phase 4G performance result:
  - default serial bucket build plus 16-worker replay: 34.5s first-run wall time, 27.6s bucket
    build time;
  - dispatcher/worker parallel bucket build plus 16-worker replay: 59.4s first-run wall time,
    52.0s bucket build time;
  - user CPU rose from roughly 76s to 141s and sys time from roughly 2s to 23s.
- Decision: do not enable the dispatcher/worker bucket builder by default. It is exact after the
  metadata-reset fix, but it moves too much data through channels and duplicates per-worker replay
  state. More CPU is used, but useful work per CPU cycle is worse.
- The next hypothesis before Phase 4H was:
  - avoid building typed shards and then scanning them again into buckets;
  - build condition buckets directly during raw/typed construction in one pass, or make typed
    update build emit the bucket-ready top-10 stream as a side output;
  - keep the single ordered decoder as the semantic authority, but parallelize at coarser segment or
    symbol roots where data copying is amortized, not per update over channels.
- Phase 4H implemented the better first-run path: direct condition-parallel replay without physical
  bucket files.
  - CLI flag: `--parallel-by-condition-direct --condition-workers N`.
  - The main thread streams typed updates once, applies the canonical replay state once, assigns the
    same global event sequence numbers, and routes top-10 book events/reference ticks to condition
    workers.
  - Workers maintain independent per-condition strategy/execution state and return standard
    current/V1 artifacts for merge.
  - The first direct attempt sent typed updates to workers and let each worker replay its own book
    state. It was exact but slow: 63.8s wall time, 96s user CPU, 26s sys CPU on the BTC-5M 3-hour
    window. Root cause: per-update routing/cloning/channel overhead plus duplicated replay state.
  - The second attempt centralized book materialization but sent full string metadata on every book
    event. It was still slow: 60.8s wall time. Root cause: repeated symbol/condition/asset/outcome
    copies and repeated worker-side interning.
  - The accepted compact path sends metadata once per asset slot per worker; subsequent book events
    carry only the slot id, event sequence, timestamps, top-10 levels, and best bid/ask. This does
    not drop price/depth precision.
- BTC-5M 3-hour Phase 4H compact-direct result:
  - 16-worker first run, no condition bucket files: 28.3s wall time, 28.2s reported elapsed;
  - user/sys CPU: 43.5s / 3.2s;
  - current replay: 70 orders / 70 fills / 34,890,416 micros PnL;
  - V1 replay: 147 orders / 147 fills / 2,428,305 micros PnL;
  - current intent hash:
    `800a7bf4ae8881c9a65ebc2c6b6e0dd8a41fbeb3198352746c9a19f4738c97ca`;
  - current ledger hash:
    `40f84c4ea30f5a74203b967aec57046a31fb0b55ec2c3aa4ae9d5be502b098f6`;
  - V1 intent hash:
    `833487f8d519726ef7de1b5a5f61eacdcacedd0fff2c6550fbac92e52ce1119c`;
  - V1 ledger hash:
    `7a0d958b177dfd6091176cf807f18d0f79ac1adc75a6bedbfbc23b07e49dd004`;
  - all four hashes match the serial typed-shard and condition-bucket baselines.
- Phase 4H decision:
  - use compact direct condition-parallel replay for cold first-run exact comparisons;
  - keep condition bucket files for repeated replay of the exact same typed window/config roots,
    where prepared-bucket reuse still runs in about 7-8s;
  - do not enable dispatcher/worker typed-update replay or physical parallel bucket build by
    default.
- Phase 4I fused raw-root cold start into the direct condition-parallel path:
  - `pm5m_market_cache` now exposes
    `stream_market_replay_typed_updates_from_raw_parallel`;
  - raw HFTREC4 segments are scanned/decoded in parallel into typed updates, then merged by the same
    replay ordering key and streamed directly into compact condition workers;
  - `market-replay-strategy-compare --parallel-by-condition-direct` now accepts either
    `--typed-update-root/--typed-update-file` or `--raw-root`;
  - raw-root mode no longer writes typed shards or rereads them before strategy replay.
- BTC-5M 3-hour Phase 4I raw-root cold-start result:
  - serial raw fused direct before parallel raw decode: 112.0s wall time;
  - raw parallel typed stream + compact direct condition replay: 33.2s best run, 37.4s final
    release verification run;
  - final execution path:
    `parallel_by_condition_direct:16x/raw_parallel_typed_logical_condition_workers_plus_hftref1`;
  - current replay: 70 orders / 70 fills / 34,890,416 micros PnL;
  - V1 replay: 147 orders / 147 fills / 2,428,305 micros PnL;
  - current intent hash:
    `800a7bf4ae8881c9a65ebc2c6b6e0dd8a41fbeb3198352746c9a19f4738c97ca`;
  - current ledger hash:
    `40f84c4ea30f5a74203b967aec57046a31fb0b55ec2c3aa4ae9d5be502b098f6`;
  - V1 intent hash:
    `833487f8d519726ef7de1b5a5f61eacdcacedd0fff2c6550fbac92e52ce1119c`;
  - V1 ledger hash:
    `7a0d958b177dfd6091176cf807f18d0f79ac1adc75a6bedbfbc23b07e49dd004`.
- Phase 4I decision:
  - raw-root first-run exact comparison should now use fused raw parallel typed stream plus compact
    direct condition replay;
  - prepared typed shards remain useful only when they already exist or when a later workflow needs
    a durable intermediate;
  - prepared condition buckets remain the fastest repeated same-window cache.
- Phase 4J reduced unnecessary raw-parallel typed stream copying:
  - the merge stage now moves `MarketReplayTypedUpdate` ownership into the ordered heap and then
    into the replay callback instead of cloning each update;
  - sort keys are derived from `MarketReplayRawUpdate` without cloning the full raw payload;
  - a bounded-channel, process-as-ready prototype was tested and rejected: it preserved correctness
    but slowed the BTC-5M 3-hour window to 72.3s because workers were back-pressured behind
    manifest-order flushing;
  - the accepted version keeps unbounded segment result collection for throughput, but removes the
    largest avoidable clones.
- BTC-5M 3-hour Phase 4J result:
  - raw parallel typed stream + compact direct condition replay: 32.4s wall time, 31.9s reported
    elapsed;
  - user/sys CPU: 126.8s / 9.6s;
  - current replay: 70 orders / 70 fills / 34,890,416 micros PnL;
  - V1 replay: 147 orders / 147 fills / 2,428,305 micros PnL;
  - all four current/V1 intent and ledger hashes still match the serial typed-shard baseline.
- Phase 4K added internal raw-decode profiling to the raw-root direct path:
  - `MarketReplayStreamProfile` now breaks out metadata filter, HFTREC4 segment scan, raw update
    construction, JSON payload parse, typed-update decode, per-segment sort, ordered merge, and
    replay callback time;
  - `market-replay-strategy-compare` surfaces these counters in the final summary under
    `raw_parallel_*`.
- BTC-5M 3-hour Phase 4K raw-decode profile:
  - wall time: 35.1s real, 34.4s reported elapsed;
  - selected/emitted updates: 10,464,099 / 10,464,099;
  - selected records: 10,464,629; selected payload bytes: 6.72GB; average selected payload:
    642 bytes;
  - cumulative raw worker wall: 109.7s; cumulative segment scan: 107.6s;
  - metadata filtering: 0.97s over 10.8M rows, so filtering is not the bottleneck;
  - raw update construction: 39.1s; JSON payload parse: 31.5s; typed-update decode: 9.0s;
  - residual HFTREC4 scan/read/copy cost after subtracting measured filter/build/parse/decode:
    27.0s;
  - per-segment sort: 2.1s; ordered merge excluding callback: 5.0s; replay callback from the
    main raw stream: 20.1s;
  - current/V1 order counts, PnL, intent hashes, and ledger hashes still match the Phase 4I/4J
    baselines exactly.
- Phase 4K decision:
  - do not spend effort on the metadata filter path; it is already sub-second cumulative;
  - the raw-root cold-start bottleneck is now mostly three things: building `MarketReplayRawUpdate`
    objects, parsing JSON payloads, and HFTREC4 payload read/copy;
  - any next raw-decode optimization should reduce allocations/copies and JSON materialization in
    those three places before changing scheduling again.
- Phase 4L implemented the direct typed raw-decode fast path:
  - the raw-root condition-direct path now decodes HFTREC4 book/price-change payloads directly into
    `MarketReplayTypedUpdate`;
  - it bypasses the hot-path construction of full `MarketReplayRawUpdate` objects and avoids the
    generic `serde_json::Value` payload pass for normal book/price-change payloads;
  - the previous slow path remains as a correctness fallback and is counted in the profile.
- BTC-5M 3-hour Phase 4L result:
  - final fast-only wall time: 32.9s real, 32.3s reported elapsed;
  - user/sys CPU: 82.8s / 11.2s, down from the Phase 4K 142.5s / 11.5s profile run;
  - all 10,464,099 emitted updates used the fast path; slow fallback count was 0;
  - raw worker wall fell from 109.7s to 49.2s;
  - raw update construction fell from 39.1s to 0.7s;
  - payload parse fell from 31.5s to 17.9s;
  - typed decode fell from 9.0s to 6.4s;
  - current/V1 order counts, PnL, intent hashes, and ledger hashes still match the baseline exactly.
- Phase 4L rejected the borrowed HFTREC4 selected-payload scan change:
  - it preserved correctness, but two runs were slower than the fast-only path: 34.0s and 36.0s
    real time versus about 32-33s for fast-only;
  - the change was not kept because it added API surface without a measured wall-time win on this
    workload.
- Market-hour condition event cache design:
  - `docs/market_hour_condition_event_cache_design.md` is kept as an archived design, not the next
    default implementation pass;
  - strict market-hour shards add boundary and cache-key complexity, and current compact typed replay
    already removes the raw JSON/HFTREC cold-scan cost for normal exact replay;
  - revisit only if a strategy-observable event cache can beat compact typed serial and match golden
    hashes on PM5M BTC+ETH+SOL windows.
- The remaining prepared-replay cost is now mostly unavoidable per-update work at this layer:
  typed shard read/decompress/merge, top-10 level extraction, replay state apply, execution due
  checks, and V1 decision evaluation.
- Tried a worker-pool JSON decode pipeline for typed-file build. It was slower on this window
  (120.2s versus 79.1s single path), because the remaining cost is dominated by ordered raw scan,
  channel/reorder overhead, and single zstd write. The slow option was not kept.

Current implication:

- Historical raw files should be converted once into compact typed binary, and new recorder output
  should continue writing compact typed directly.
- Default exact workflow when compact typed exists:
  compact typed binary -> streaming market replay -> current/V1 comparison.
- Raw feed remains the audit/rebuild source and the validation oracle, not the normal replay input.
- Condition-direct parallel replay is a correctness-preserving option, but it is not the default for
  this workload because the 16-worker 3-hour PM5M run was slower than compact typed serial.
- Prepared condition buckets remain a possible repeated same-window cache, but the next optimization
  should first reduce the compact typed serial hot path: per-update book apply, top-10 row
  construction, due-order checks, and V1 decision evaluation.
- If 7-day/30-day replay still needs another order of magnitude, cache strategy-observable events
  only after proving exact hash equality against compact typed serial. Do not introduce a new shard
  boundary until it has a measured wall-time win.

### Phase 4B: Accelerated Replay Dataset V1

Goal: make 15-day and 30-day repeated research practical.

Tasks:

- Build accelerated rows from the streaming state engine.
- Partition by date/symbol/horizon.
- Store only current/V1 sufficient fields.
- Include schema version, source coverage, and build hash.
- Add rebuild detection when config requests unavailable fields or order sizes.

Acceptance:

- Parameter sweeps over current/V1 fields do not touch raw feed.
- 15-day and 30-day windows run by scanning accelerated partitions.
- The runner fails closed if a strategy asks for a field not present in the accelerated schema.
- Accelerated results are not accepted as final evidence until sampled equivalence against the
  exact streaming raw path passes for the relevant schema version.

### Phase 5: Depth State Path Repositioning

Goal: keep full-state tools available without letting them dominate normal research.

Tasks:

- Update docs and CLI help to call HFTBOOK/HFTIDX the `Depth State Store` and `Replay State Index`.
- Mark builders as optional forensic/index tools.
- Add a sampled validation command comparing accelerated rows with depth-index state.
- Remove depth-state build from daily live-comparison scripts.

Acceptance:

- Default live comparison scripts do not invoke depth-state builders.
- Forensic commands still support point-in-time full book inspection.
- Validation can explain differences between streaming, accelerated, and indexed state.

## Performance Model

Cold raw replay lower bound:

```text
time >= bytes_to_read / effective_read_and_decode_throughput
```

Therefore a 30-day raw cold scan cannot be guaranteed to finish in a few minutes if the raw input is
multi-TiB. The correct design is:

- latest-window/live comparison: streaming replay from raw/direct Market Replay events;
- large-window exact current/V1 comparison: parallel raw scan/decode plus ordered streaming replay;
- repeated 15-day/month parameter sweeps: Accelerated Replay Dataset;
- forensic point lookup: Replay State Index.

The goal is not one universal physical format. The goal is one semantic replay model with execution
paths chosen by query shape:

| Query shape | Correct path |
| --- | --- |
| Latest live actual vs same strategy vs V1 | Parallel raw scan/decode + Streaming Market Replay Engine |
| 15-day/month exact current vs V1 replay | Parallel raw scan/decode + Streaming Market Replay Engine |
| Repeated threshold/side/cap sweeps | Accelerated Replay Dataset |
| One disputed fill or full book inspection | Depth State Store + Replay State Index |
| New strategy feature not in accelerated fields | Market Replay Dataset or Raw Feed rebuild |

## Guardrails

- Live and replay must share the same strategy kernel where practical.
- Strategy code should consume a state provider interface, not `HFTIDX1` directly.
- Accelerated datasets must be schema-versioned and fail closed on missing fields.
- Raw feed remains the only truth source.
- Depth-state products must include source hashes and coverage manifests.
- Reports must always record which execution path produced them.
- Do not use accelerated results as final evidence until sampled equivalence against streaming or
  depth-index replay has passed for the relevant schema version.

## First Deliverable

The first implementation deliverable should be intentionally narrow:

```text
compare-live-window for 2026-06-23 13:26:58 CST -> 2026-06-24 10:38:00 CST
symbols: BTC, ETH, SOL
horizons: PM5M, PM15M
strategies: current live config and OPTION_V1_GRAY
execution: 300 ms delayed FAK buy, hold to settlement
output: actual_live, local_replay_same_strategy, local_replay_v1
```

This deliverable proves the key architectural point: live comparison does not need a full depth-state
index. After that is correct, first parallelize exact raw scan/decode; then build the accelerated
dataset only for repeated 15-day and 30-day parameter iteration.
