# Market-Hour Condition Event Cache Design

Status: archived design, superseded on 2026-06-25 CST.

This design is retained for reference, but it is not the current implementation target. The current
validated path is compact typed binary replay. On the PM5M BTC+ETH+SOL 3-hour golden window, current
raw serial, compact typed serial, and compact typed condition-direct produce identical hashes, while
compact typed serial is faster than 16-worker condition-direct. A market-hour cache should be
revisited only if it matches compact typed serial hashes and delivers a measured wall-time win.

This document previously proposed replacing the long-term "raw direct versus prepared bucket" split
with one primary replay path:

```text
raw HFTREC4
  -> market-hour condition event cache
  -> hour-parallel strategy replay
```

In that archived design, the raw direct path remained only as the internal cache-miss builder. The
current implementation target is the compact typed binary replay path described in
`docs/pm5m_replay_etl_refactor_plan.md`.

## Verified Facts

Local metadata scan on the BTC-5M/BTC-15M recorder around the 2026-06-24 comparison window:

- Full market hours contain 12 BTC-5M conditions and 4 BTC-15M conditions.
- `condition_id` market windows did not cross market-hour boundaries in the sampled raw data.
- Raw book/price messages for a condition can appear in adjacent raw ingest-hour buckets.
- Therefore cache shards must be keyed by `market_start_ts_ns // 1h`, not by raw ingest hour.

Recent performance baseline on the BTC-5M 3-hour window:

- Raw cold direct, after Phase 4L fast decode: about 32-33s wall time.
- Old prepared condition-bucket reuse: about 7-8s wall time.
- Window-level bucket build is not the right long-term cache because a different window forces a
  rebuild.

## First Principles

The cache boundary should sit after irreversible market-data interpretation and before strategy
decisions:

```text
raw payloads
  -> visible-time model
  -> order book state reconstruction
  -> strategy-observable book/reference events
  -> cache boundary
  -> strategy replay
```

Do not cache too early:

- Raw HFTREC4 still requires decode and book-state reconstruction on every replay.
- Typed updates still require applying the incremental book stream and building top-10 observable
  rows on every replay.

Do not cache too late:

- Strategy results are invalidated by every strategy/config change.
- Existing condition buckets are window-level replay materializations, so they are not reusable
  across nearby windows.

The reusable unit is:

```text
market_hour -> complete strategy-observable events for all conditions whose market_start is in that hour
```

## Target User Flow

One command path:

```text
market-replay-strategy-compare
  -> ensure market-hour condition event cache for requested window
  -> replay cache in parallel by market_hour
  -> merge artifacts
```

Cache miss is internal:

```text
missing market_hour
  -> scan raw ingest hours around that market_hour
  -> build cache shard
  -> replay
```

Existing raw-direct and window-bucket flags can stay during rollout, but the final default should be
the cache-backed path.

## Physical Layout

Root:

```text
<cache_root>/
  semantics=<semantics_hash>/
    market_hour=495056/
      manifest.condition_events.v1.json
      events.pm5mce.zst
```

First implementation should store BTC-5M and BTC-15M together in the same `market_hour` shard. The
event rows already carry symbol/condition metadata. Splitting by symbol is only worth doing if one
hour shard becomes too large.

Do not use `hash(condition_id) % P` as the primary layout. It is only an optional second-level split
if a single market_hour becomes too large. For the current PM5M 5m/15m shape, hour-level tasks are
enough:

- 7 days: 168 market-hour tasks.
- 30 days: 720 market-hour tasks.
- Each full hour has about 16 conditions for BTC 5m/15m.

## Cache Semantics Key

`semantics_hash` must include only inputs that change observable events:

- Cache schema version.
- Raw source identity and segment manifest hashes used by the shard.
- Market replay semantics id.
- Poly visible-time setting.
- Poly incremental latency and freshness guard.
- Top-10 book materialization rules.
- Reference cache root/hash and reference latency, if reference events are embedded.
- Symbol allowlist / market family covered by the shard.
- Raw context policy, such as `scan_before_hours=1`, `scan_after_hours=1`.

Do not include:

- Strategy config.
- Current/V1 strategy id.
- Settlement cache root.
- Output directory.
- Worker count.

Those affect replay or final settlement, not the observable market event cache.

## Shard Manifest

`manifest.condition_events.v1.json` should contain:

```json
{
  "schema_version": 1,
  "cache_format": "pm5m.condition_event_cache.v1",
  "semantics_hash": "...",
  "market_hour": 495056,
  "market_start_ts_ns": 1782201600000000000,
  "market_end_ts_ns": 1782205200000000000,
  "raw_scan_start_hour": 495055,
  "raw_scan_end_hour": 495057,
  "symbols": ["BTC-5M", "BTC-15M"],
  "condition_count": 16,
  "book_event_count": 0,
  "reference_event_count": 0,
  "event_count": 0,
  "min_event_ts_ns": null,
  "max_event_ts_ns": null,
  "raw_manifest_hashes": [],
  "event_file": "events.pm5mce.zst",
  "event_file_sha256": "...",
  "build_elapsed_ms": 0,
  "profile": {}
}
```

Exact field names can be adjusted during implementation, but the manifest must be sufficient to
decide whether a shard is reusable without reading the event file.

## Event File Format

Use typed binary plus zstd, not JSON.

Recommended structure:

```text
magic: "PM5MCE1\n"
zstd stream:
  header dictionaries
  repeated length-prefixed event records
```

Header dictionaries:

- symbols
- condition ids
- asset ids
- outcomes
- reference symbols

Book event record:

```text
type = book
local_event_seq
condition_slot
asset_slot
symbol_slot
outcome_code
market_start_ts_ns
market_end_ts_ns
local_recv_ts_ns
ingest_seq
best_bid_price_micros
best_ask_price_micros
bid_levels[10]
ask_levels[10]
sort key fields needed for deterministic replay
```

Reference event record:

```text
type = reference
local_event_seq
reference_symbol_slot
reference_row_idx
ts_ns
price_micros
sort key fields needed for deterministic replay
```

Do not store raw payload bytes. Do not store full typed updates. The cache is an observable event
stream, not another raw/typed archive.

## Builder Algorithm

Build one `market_hour=H`:

1. Compute market-hour interval:

   ```text
   hour_start = H * 1h
   hour_end = hour_start + 1h
   ```

2. Scan raw ingest-hour buckets around the market hour:

   ```text
   raw hours [H - 1, H, H + 1]
   ```

   The padding is part of the cache semantics. Keep it configurable, but default to one hour on each
   side based on observed data.

3. Use the Phase 4L fast raw decode path.

4. Keep only book/price updates whose condition has:

   ```text
   market_start_ts_ns // 1h == H
   ```

5. Apply the same visible-time model and book-state reconstruction as the current exact replay path.

6. Emit only strategy-observable events:

   - top-10 bid/ask rows
   - best bid/ask
   - condition/asset metadata
   - deterministic sort key

7. Add reference events for the same market hour.

   First implementation should embed reference events into the shard because current strategy
   compare needs them and reference volume is small. If reference latency/root changes often, split
   reference into a separate small stream later.

8. Write `events.pm5mce.zst` and atomically write the manifest.

Open/current hour:

- Build as provisional into a temp location.
- Do not mark as finalized until the raw recorder has finalized all required raw context hours.
- It is acceptable to rebuild the open hour on every run.

Historical/finalized hour:

- Build once.
- Reuse until a manifest semantics or raw source hash changes.

## Replay Algorithm

For a requested window:

1. Determine covered market hours:

   ```text
   first_hour = floor(window_start / 1h)
   last_hour = floor((window_end - 1) / 1h)
   ```

2. Ensure cache shards exist for all covered hours.

3. Pre-read manifests and event counts.

4. Assign deterministic global event offsets if existing artifacts require `global_event_seq`.

   The cache file stores `local_event_seq`. Replay can compute:

   ```text
   global_event_seq = hour_offset + local_event_seq_inside_filtered_window
   ```

   This keeps artifact hashes stable while allowing hour-parallel execution.

5. Dispatch market-hour shards to a worker pool.

6. Each worker replays all conditions inside its assigned hour shard.

   This is valid because local validation showed condition market windows do not cross market hours.
   No strategy state needs to move between hours for the current 5m/15m strategy model.

7. Merge per-hour results deterministically:

   - sort by market_hour
   - then condition id
   - then event/order sequence

8. Apply settlement after merging, using the requested settlement cache.

Settlement is intentionally not part of the condition event cache key.

## Code Changes

### Shared cache module

Add a module in `pm5m_market_cache`, for example:

```text
crates/pm5m_market_cache/src/condition_events.rs
```

Initial public API:

```rust
pub struct BuildConditionEventCacheOptions { ... }
pub struct EnsureConditionEventCacheOptions { ... }
pub struct ConditionEventCacheManifest { ... }
pub struct ConditionEventCacheBuildReport { ... }

pub fn build_condition_event_cache_hour(
    options: &BuildConditionEventCacheOptions,
    market_hour: i64,
) -> Result<ConditionEventCacheBuildReport>;

pub fn ensure_condition_event_cache(
    options: &EnsureConditionEventCacheOptions,
) -> Result<Vec<ConditionEventCacheManifest>>;

pub fn stream_condition_event_cache_hour<F>(
    manifest: &ConditionEventCacheManifest,
    visit: F,
) -> Result<()>
where
    F: FnMut(ConditionEvent) -> Result<()>;
```

Keep the first implementation narrow. It only needs to support the exact strategy compare path.

### Event encoding

Reuse the current typed-update binary pattern:

- magic bytes
- zstd level 1
- length-prefixed binary records

Do not introduce a generic serialization framework unless the manual encoding becomes a real
maintenance problem.

### Strategy compare integration

In `hft_private/src/market_replay_strategy_compare.rs`:

- Add cache options:

  ```text
  --condition-event-cache-root <path>
  --condition-event-cache-build-missing
  --condition-event-cache-workers <N>
  ```

- Add a new execution path:

  ```text
  compare_strategies_on_condition_event_cache_parallel_by_hour
  ```

- The final default should be:

  ```text
  if condition_event_cache_root is configured:
      ensure missing market hours
      replay cache
  else:
      raw direct path for compatibility
  ```

- Once validated, deprecate manual `--parallel-by-condition` window-bucket use for normal research.

### Existing paths

Keep temporarily:

- raw direct: cache miss builder and fallback.
- typed shard path: compatibility until cache path covers all needed workflows.
- old condition bucket path: validation baseline and rollback.

Remove or hide later:

- user-facing choice between raw direct and prepared condition buckets.

## Implementation Order

1. Add cache manifest and binary reader/writer.

   Tests:

   - roundtrip one book event
   - roundtrip one reference event
   - manifest hash validation

2. Build one market-hour shard from raw.

   Reuse Phase 4L fast decode and existing streaming book-state logic.

   Tests:

   - sampled BTC hour has 12 BTC-5M and 4 BTC-15M conditions when both symbols are enabled
   - no emitted event has `market_start_hour != shard_hour`

3. Replay one market-hour shard.

   Tests:

   - compare per-hour replay artifacts against raw direct for the same hour
   - current/V1 order/fill/PnL match

4. Add multi-hour replay and deterministic merge.

   Tests:

   - BTC-5M 3-hour window matches current raw-direct hashes
   - BTC-5M/BTC-15M mixed window matches current raw-direct hashes

5. Add `ensure` behavior.

   Tests:

   - missing hour is built
   - existing matching hour is reused
   - changing visible-time semantics rebuilds
   - changing strategy config does not rebuild

6. Add performance benchmarks.

   Required benchmark windows:

   - BTC-5M 3-hour
   - BTC-5M/BTC-15M 24-hour
   - BTC-5M/BTC-15M 7-day if data is available

7. Update default workflow.

   After exactness and performance are proven, make cache-backed replay the normal strategy compare
   path when a cache root is configured.

## Acceptance Criteria

Correctness:

- BTC-5M 3-hour current/V1 orders, fills, PnL, intent hash, and ledger hash match the current
  raw-direct baseline.
- Cache replay does not miss book updates at raw-hour boundaries.
- Cache replay does not require cross-hour strategy state for current 5m/15m markets.
- Settlement output stays controlled by the requested settlement cache.

Performance:

- Cache replay for the BTC-5M 3-hour window should be in the same class as old prepared bucket reuse
  rather than raw direct.
- 7-day replay target: under 10 minutes with cache ready.
- 30-day replay target: 20-40 minutes with cache ready.
- Historical cache backfill should use a market-hour worker pool and saturate available cores until
  storage bandwidth becomes the limit.

Usability:

- Users should run one strategy compare command.
- The system should build missing shards automatically.
- Users should not manually choose raw direct versus prepared bucket for normal workflows.

## Risks

- Raw context padding may be too small for unusual late messages.
  - Mitigation: manifest records raw scan hours; validation should check boundary misses. Increase
    padding if needed.

- Artifact hashes may depend on old global event sequence assignment.
  - Mitigation: store local event sequence and compute deterministic global offsets during replay.

- Reference semantics may change.
  - Mitigation: reference root/hash and latency are part of the cache semantics. If this becomes too
    invalidating, split reference into a separate stream.

- Current strategy code may contain hidden global state across conditions.
  - Mitigation: hour-cache replay must first match raw-direct golden hashes before replacing the
    default path.

## Design Decision

Use `market_hour` as the primary shard.

Do not use raw ingest hour as the replay shard.

Do not use `hash(condition_id) % P` as the primary shard.

The only durable cache boundary should be the market-hour condition event cache. Raw direct and old
prepared buckets become implementation details or compatibility paths, not separate user workflows.
