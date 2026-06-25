# PM5M Replay Performance Iterations

Goal: exact 7-day full replay cold start under 5 minutes, without hiding large one-time preprocessing
costs outside the measured path.

## Ground Rules

- Preserve replay correctness first: compare current/V1 intent and ledger hashes against the known
  3h golden window after every material change.
- Profile after each iteration and optimize the largest remaining end-to-end bucket.
- Keep iteration changes separable so they can be reverted independently.
- Do not count heavy ahead-of-time dataset rebuilds as a solved cold-start replay.
- `hft_private/` is currently ignored by this repo, so private-runner-only changes must be called
  out explicitly until that ownership boundary is changed.

## Golden Window

- Window: `1782203400000000000..1782214200000000000` (3h)
- Inputs:
  - `/mnt/data/hft/hft_runtime/pm5m_recorder_prod/poly_btc_5m_15m/typed`
  - `/mnt/data/hft/hft_runtime/pm5m_recorder_prod/poly_eth_5m_15m/typed`
  - `/mnt/data/hft/hft_runtime/pm5m_recorder_prod/poly_sol_5m_15m/typed`
- Expected hashes:
  - current intent: `e18e6aec77c16c3dd8015579fe42aff19fdfc64ffd59aab5a79b47a98b81e8e8`
  - current ledger: `20fd3d2b6e5c23ec88c0a3555734c05791992569fb6bb643bfe0b6553472254b`
  - v1 intent: `ad3eee273f4d8d5f010801d3c24eb7b37b10be166c50b2bd3fbbe0c124779b32`
  - v1 ledger: `acf53e9e7c37107fc95de9f59f1f0adab19a05e9a8a8d669c38a5f1cb0a481e8`

## Baseline, 2026-06-26

Default compact typed serial run:

- elapsed: 46.793s / 3h
- replay events: 14,856,900
- book updates: 14,823,060
- changed book rows: 21,316,909
- linear 7d estimate: 43.7 min
- linear 30d estimate: 3.12 h
- hashes: matched golden window

Deep profile run (`PM5M_MARKET_REPLAY_DEEP_PROFILE=1`):

- elapsed: 57.064s / 3h
- top buckets:
  - callback total: 36.007s
  - apply update / state maintenance: 23.293s
  - BookRow build: 8.365s
  - on-book strategy event: 7.409s
  - typed stream merge/flush: 7.047s
  - compact segment read: 6.138s
  - replay state apply: 5.705s
  - V1 on-book event: 5.472s
  - compact record decode: 5.244s
  - stream book levels build: 2.983s

Conclusion: current path is not JSON-bound and not disk-bound. The limiting shape is single-threaded
per-update replay across 14.8M book updates and 21.3M changed books. The next useful iteration should
either remove expensive per-update work that does not affect decisions, or split independent markets
so decode, state update, and strategy execution run in parallel end-to-end.

## Iteration 1 Plan

Target the largest controllable cost without moving work into a large preprocessing phase:

1. Remove avoidable full BookRow/level construction when no strategy needs full visible depth.
2. Preserve exact hashes on the 3h golden window.
3. Re-profile default and deep mode.
4. If improvement is material, commit as a standalone iteration.

## Iteration 1 Result, 2026-06-26

Change: in the private market replay strategy comparator, skip constructing bid top10 levels for
stream books when both replay configs have `enable_exit_sell=false`. Best bid is still computed, so
crossed-book checks remain unchanged; ask levels are still built because buy sizing/execution needs
them. If either config enables sell exits, the runner falls back to full bid+ask level construction.

Status:

- This code path currently lives under ignored `hft_private/`; the public repo records the iteration
  result here, but the code change is not in the public commit unless the private boundary changes.
- Correctness: 3h golden hashes matched all four expected hashes.
- Private tests: `cargo test --manifest-path hft_private/Cargo.toml` passed.

Default compact typed serial run:

- baseline: 46.793s / 3h
- iteration 1: 44.932s / 3h
- improvement: 1.861s / 3h, 4.0%
- linear 7d estimate: 41.9 min
- linear 30d estimate: 3.00 h

Deep profile:

- elapsed: 52.649s / 3h
- `stream_book_levels_ns`: 2.983s -> 1.619s
- `book_row_build_ns`: 8.365s -> 6.697s
- largest remaining buckets:
  - callback total: 33.122s
  - apply update / state maintenance: 21.018s
  - on-book strategy event: 7.035s
  - BookRow build: 6.697s
  - typed stream merge/flush: 6.455s
  - compact segment read: 5.614s
  - replay state apply: 5.501s
  - V1 on-book event: 5.177s
  - compact record decode: 4.799s

Next target: reduce `affected_assets`/`replay_state_apply`/BookRow construction by avoiding owned
asset-id sets and repeated string-key lookups in the hot path, while preserving exact update order
and hash output.

## Iteration 2 Result, 2026-06-26

Change: replace the private runner's per-update `BTreeSet<String>` affected-asset collection with
borrowed asset-id references. Single-asset updates take the zero-allocation path. Multi-asset updates
still sort and deduplicate borrowed `&str` values, preserving the old `BTreeSet` iteration order and
therefore preserving event/hash ordering.

Status:

- Correctness: 3h golden hashes matched all four expected hashes.
- Private tests: `cargo test --manifest-path hft_private/Cargo.toml` passed.
- Code path is still in ignored `hft_private/`.

Default compact typed serial run:

- iteration 1: 44.932s / 3h
- iteration 2: 40.782s / 3h
- improvement vs iteration 1: 4.150s / 3h, 9.2%
- improvement vs baseline: 6.011s / 3h, 12.8%
- linear 7d estimate: 38.1 min
- linear 30d estimate: 2.72 h

Deep profile:

- elapsed: 47.210s / 3h
- `affected_assets_ns`: 2.093s -> 1.166s
- `apply_raw_update_ns`: 21.018s -> 18.151s
- largest remaining buckets:
  - callback total: 29.289s
  - apply update / state maintenance: 18.151s
  - on-book strategy event: 6.430s
  - BookRow build: 6.321s
  - typed stream merge/flush: 5.928s
  - compact segment read: 5.200s
  - replay state apply: 4.926s
  - V1 on-book event: 4.736s
  - compact record decode: 4.455s

## Iteration 3 Result, 2026-06-26

Change: add `StreamingMarketReplayState::new_without_condition_tracking()` in public
`pm5m_market_cache`. Default state construction still tracks conditions. The private strategy
comparator uses the lighter state because exact current/V1 replay only needs latest books by asset;
condition summaries are already derived from intents/fills/settlements outside the streaming state.

Status:

- Correctness: 3h golden hashes matched all four expected hashes.
- Public tests: `cargo test --workspace` passed.
- Private tests: `cargo test --manifest-path hft_private/Cargo.toml` passed.
- Public code is commit-ready; private runner call remains under ignored `hft_private/`.

Default compact typed serial run:

- iteration 2: 40.782s / 3h
- iteration 3: 38.597s / 3h
- improvement vs iteration 2: 2.185s / 3h, 5.4%
- improvement vs baseline: 8.196s / 3h, 17.5%
- linear 7d estimate: 36.0 min
- linear 30d estimate: 2.57 h

Deep profile:

- elapsed: 43.562s / 3h
- `replay_state_apply_ns`: 4.926s -> 2.612s
- `apply_raw_update_ns`: 18.151s -> 15.452s
- largest remaining buckets:
  - callback total: 26.216s
  - apply update / state maintenance: 15.452s
  - on-book strategy event: 6.205s
  - BookRow build: 6.130s
  - typed stream merge/flush: 5.750s
  - compact segment read: 5.062s
  - V1 on-book event: 4.543s
  - compact record decode: 4.329s

Next target: remove the single-threaded global heap/merge tax by partitioning independent market
streams, or reduce V1/on-book work by avoiding repeated string/interner lookups in the strategy hot
path. The current single-thread path still linearly extrapolates far above the 5-minute 7d target.

## Iteration 4 Result, 2026-06-26

Change: enable `parallel_by_symbol` for compact typed input in the private comparator and filter
typed roots/files per symbol shard so BTC/ETH/SOL workers do not each scan all roots.

Status:

- Correctness: aggregate actual/current/V1 orders, fills, cash, qty, fees, settlement payout, and
  final PnL matched the serial iteration-3 run exactly.
- Caveat: the merged symbol summary does not produce the same single serial intent-stream hash,
  because artifacts remain per symbol shard. Book event counts match serial; reference event counts
  are counted per shard.
- Private tests: `cargo test --manifest-path hft_private/Cargo.toml` passed.
- Code path is still in ignored `hft_private/`.

Default compact typed symbol-parallel run:

- serial iteration 3: 38.597s / 3h
- symbol parallel 3 workers: 26.828s / 3h
- improvement vs iteration 3 serial: 11.769s / 3h, 30.5%
- improvement vs baseline serial: 19.965s / 3h, 42.7%
- linear 7d estimate: 25.0 min
- linear 30d estimate: 1.79 h

Shard wall times:

- BTC-5M: 26.298s, 10,464,099 book events
- ETH-5M: 6.484s, 2,528,138 book events
- SOL-5M: 4.471s, 1,830,823 book events

Conclusion: symbol parallelism is worthwhile but BTC dominates, so three workers is not enough for
the 7d 5-minute target. Further speedup must split BTC internally without reintroducing a single
dispatcher bottleneck.

## Iteration 5 Result, 2026-06-26

Change: stream compact typed segments directly into the existing ordered pending heap instead of
first materializing a `Vec<MarketReplayCompactTypedRecord>`. The new path still uses the same
segment ordering, order holdback, `MarketEventSortKey`, and callback sequence; it only removes the
intermediate record vector, per-record `PathBuf` clone, and one redundant `dataset_format`
allocation before the final emitted update is assigned its canonical format.

Status:

- Correctness: serial 3h golden hashes matched all four expected hashes.
- Public tests: `cargo test -p pm5m_market_cache` passed.
- Private tests: `cargo test --manifest-path hft_private/Cargo.toml` passed.
- Public code path is commit-ready; private runner still supplies the exact replay workload.

Default compact typed serial run:

- iteration 3 serial: 38.597s / 3h
- iteration 5 serial: 35.608s / 3h
- improvement vs iteration 3 serial: 2.989s / 3h, 7.7%
- linear 7d estimate: 33.2 min
- linear 30d estimate: 2.37 h

Default compact typed symbol-parallel run:

- iteration 4 symbol-parallel: 26.828s / 3h
- iteration 5 symbol-parallel: 24.968s / 3h
- improvement vs iteration 4: 1.860s / 3h, 6.9%
- linear 7d estimate: 23.3 min
- linear 30d estimate: 1.66 h

Deep profile, symbol-parallel:

- elapsed: 32.038s -> 30.519s / 3h
- `raw_stream_excluding_callback_ns`: 18.422s -> 15.921s
- `compact_record_decode_ns`: 5.252s -> 4.012s
- `compact_pending_push_ns`: 2.804s -> 1.993s
- `merge_flush_ns`: 5.226s -> 4.925s
- `callback_total_ns`: 25.408s -> 24.465s
- `apply_raw_update_ns`: 14.745s -> 14.207s
- `book_row_build_ns`: 6.135s -> 6.059s
- `v1_on_book_event_ns`: 4.132s -> 3.960s

Shard wall times in deep mode:

- BTC-5M: 29.734s, 10,464,099 book events
- ETH-5M: 7.077s, 2,528,138 book events
- SOL-5M: 4.945s, 1,830,823 book events

Conclusion: this was a valid constant-factor improvement in the compact stream path, but it does
not change the first-order bottleneck. BTC remains one mostly single-threaded replay stream. The
largest remaining exact buckets are callback/apply state maintenance, BookRow construction, and
ordered heap merge. To reach 7d under 5 minutes, the next material step still has to split BTC's
independent market work without a central per-update dispatcher and without resetting strategy
state incorrectly.

## Rejected Attempt: Residual Fast Path, 2026-06-26

Attempt: skip `StreamingMarketReplayState::residual_asks.remove()` when the residual map is empty,
and avoid the condition observer call when condition tracking is disabled.

Result:

- previous symbol-parallel default: 24.968s / 3h
- residual fast-path symbol-parallel default: 25.335s / 3h
- `replay_state_apply_ns`: 2.212s -> 2.176s, but total wall and surrounding callback buckets got
  worse

Reason: this did reduce a tiny part of replay-state apply, but not enough to survive normal run
variance; it also did not address the larger BookRow/callback/merge costs.

Status: reverted, not committed.

## Iteration 6 Result, 2026-06-26

Change: in the ignored private comparator, replace `levels_from_book`'s per-call
`Box<dyn Iterator>` with static bid/ask iterator branches. The generated top-10 level arrays are
unchanged: bids still iterate descending, asks ascending, and only positive quantities are emitted.

Status:

- Correctness: serial 3h golden hashes matched all four expected hashes.
- Private tests: `cargo test --manifest-path hft_private/Cargo.toml` passed.
- Code path is private-only under ignored `hft_private/`; the public repo can only record this
  iteration unless that ownership boundary changes.

Default compact typed symbol-parallel run:

- iteration 5 symbol-parallel: 24.968s / 3h
- iteration 6 symbol-parallel: 24.748s / 3h
- improvement vs iteration 5: 0.220s / 3h, 0.9%
- linear 7d estimate: 23.1 min
- linear 30d estimate: 1.65 h

Deep profile, symbol-parallel:

- elapsed: 30.519s -> 29.797s / 3h
- `stream_book_levels_ns`: 1.499s -> 1.062s
- `book_row_build_ns`: 6.059s -> 5.720s
- `apply_raw_update_ns`: 14.207s -> 13.958s

Conclusion: the change is correct and cheap, but it is only a small constant-factor win. The profile
now says further micro-optimizing level extraction will not close the gap. The dominant unsolved
problem remains BTC being a single exact replay stream with callback/apply/on-book work running
mostly serially.

## Iteration 7 Result, 2026-06-26

Change: give the compact typed stream its own lighter heap sort key. The old compact path reused
`MarketEventSortKey`, which cloned `source_segment` and the asset tie-breaker into a `String` for
every emitted update. The new compact key keeps the same ordering fields and comparison semantics,
but shares per-segment `source_segment` and per-segment asset dictionary values via `Arc<str>`, and
uses the compact event type code for the final event-type tie-breaker. Raw replay sorting is
unchanged.

Status:

- Correctness: serial 3h golden hashes matched all four expected hashes.
- Public tests: `cargo test -p pm5m_market_cache` passed.
- Private tests: `cargo test --manifest-path hft_private/Cargo.toml` passed.
- Public code committed; the measured runner also includes the private iteration-6 static-levels
  change.

Default compact typed symbol-parallel run:

- iteration 6 symbol-parallel: 24.748s / 3h
- iteration 7 symbol-parallel: 24.603s / 3h
- improvement vs iteration 6: 0.145s / 3h, 0.6%
- linear 7d estimate: 23.0 min
- linear 30d estimate: 1.64 h

Deep profile, symbol-parallel:

- elapsed: 29.797s -> 28.751s / 3h
- `compact_pending_push_ns`: 2.008s -> 0.961s
- `merge_flush_ns`: 4.999s -> 4.587s
- `raw_stream_excluding_callback_ns`: 16.069s -> 15.397s

Conclusion: the targeted heap-key allocation cost was real and is now much smaller, but total wall
only moved slightly because callback/apply/on-book work expanded with run variance and BTC still
dominates the wall clock. The remaining path to a 5-minute 7d replay is not more string/key
micro-optimization; it requires exact BTC-internal parallelism or a materially different callback
state layout.

## Rejected Attempt: Compact Heap Key `Rc<str>`, 2026-06-26

Attempt: replace the compact heap key's `Arc<str>` fields with `Rc<str>`, since the pending heap is
thread-local and does not need atomic reference counts.

Result:

- previous `Arc<str>` symbol-parallel default: 24.603s / 3h
- `Rc<str>` symbol-parallel default: 25.138s / 3h

Reason: the theoretical atomic-count saving did not appear in end-to-end runtime; total callback and
wall time got worse. This is below the noise floor at best and negative in this run.

Status: reverted, not committed.

## Iteration 8 Result, 2026-06-26

Change: in the ignored private comparator's compact typed main path, call strategy book handlers
with the freshly constructed `StreamBook` after applying it to `MarketState`, instead of inserting
the book into `MarketState` and immediately looking it up again by asset key. State visibility is
unchanged: `MarketState::apply_stream_book` still runs before `on_book_event`.

Status:

- Correctness: serial 3h golden hashes matched all four expected hashes.
- Private tests: `cargo test --manifest-path hft_private/Cargo.toml` passed.
- Code path is private-only under ignored `hft_private/`; the public repo records the result here
  but cannot track the private diff.

Default compact typed serial run:

- iteration 7 serial: 36.677s / 3h
- iteration 8 serial: 34.260s / 3h
- linear 7d estimate: 32.0 min
- linear 30d estimate: 2.28 h

Default compact typed symbol-parallel run:

- iteration 7 symbol-parallel: 24.603s / 3h
- iteration 8 symbol-parallel: 23.954s / 3h
- improvement vs iteration 7: 0.649s / 3h, 2.6%
- linear 7d estimate: 22.4 min
- linear 30d estimate: 1.60 h

Deep profile, symbol-parallel:

- elapsed: 28.751s -> 28.171s / 3h
- `latest_book_lookup_ns`: 0.490s -> 0.000s
- `callback_total_ns`: 25.732s -> 23.502s
- `apply_raw_update_ns`: 14.979s -> 11.350s
- `on_book_event_ns`: 5.821s -> 4.737s
- `v1_on_book_event_ns`: 4.012s -> 3.905s

Conclusion: avoiding the write-then-readback pattern is a real exact-path improvement. It is still
a constant-factor win, not the required order-of-magnitude jump. BTC remains the wall-clock limiter:
after this change BTC deep-mode wall is 27.366s for 3h, while ETH/SOL finish much earlier.

## Rejected Attempt: Stream Metadata Hot-Cache Fast Path, 2026-06-26

Attempt: on `stream_meta_by_asset_id` cache hits, skip re-reading condition/symbol/window metadata
from the replay book state and use the cached metadata directly. The cache hit count is much larger
than misses, so this targeted `stream_book_metadata_ns`.

Result:

- previous symbol-parallel default: 23.954s / 3h
- metadata fast-path symbol-parallel default: 24.185s / 3h
- current replay orders changed from 107 to 109

Reason: this is not just a defensive consistency check. For some updates, requiring the current
book/update metadata before emitting the stream book affects whether the event is observable to the
strategy. Skipping it changes replay semantics.

Status: reverted, not committed.

## Rejected Attempt: Condition Update Dispatch, 2026-06-26

Attempt: change condition-direct compact typed mode so the dispatcher sends typed updates to
condition workers and workers maintain replay state locally.

Result:

- condition-direct 16 workers: 64.101s / 3h
- after optimizing the update router to avoid the common single-asset clone path: 55.210s / 3h
- aggregate strategy results still matched serial
- worse than previous condition-direct and much worse than serial/symbol parallel

Reason: sending typed updates through channels moved too much data and increased worker-side on-book
work. Avoiding the common clone path helped but not enough. This is not the right route unless
compact updates are partitioned before decode or sent as very small borrowed/encoded records with
strict per-condition ordering.

Status: reverted.

## Rejected Attempt: V1 Lookup Cache, 2026-06-26

Attempt: cache `symbol`/`outcome` lookups in `FrozenSingleLegStrategy::on_book_snapshot_event`.

Result:

- serial run: 38.672s / 3h
- hashes matched, but no material improvement vs 38.597s iteration 3

Reason: repeated symbol/outcome lookup is not the limiting cost; the remaining V1 bucket is mostly
actual decision/sweep/cache work and surrounding event flow.

Status: reverted.

## Iteration 9 Result: Disable Default Per-Event Timers, 2026-06-26

Change: in the ignored private comparator's compact typed path, keep detailed stage timers behind
`PM5M_MARKET_REPLAY_DEEP_PROFILE=1`. Default production-speed runs still report wall time and
high-level counters, but no longer call `Instant::now()` around every update/book/strategy stage.

Status:

- Correctness: serial 3h golden hashes matched all four expected hashes.
- Private tests: `cargo test --manifest-path hft_private/Cargo.toml` passed.
- Code path is private-only under ignored `hft_private/`; the public repo records the result here
  but cannot track the private diff.

Default compact typed symbol-parallel run:

- iteration 8 symbol-parallel: 23.954s / 3h
- iteration 9 symbol-parallel: 18.763s / 3h
- improvement vs iteration 8: 5.191s / 3h, 21.7%
- linear 7d estimate: 17.5 min
- linear 30d estimate: 1.25 h

Default compact typed serial run:

- iteration 8 serial: 34.260s / 3h
- iteration 9 serial: 26.497s / 3h
- linear 7d estimate: 24.7 min
- linear 30d estimate: 1.77 h

Deep profile, symbol-parallel:

- elapsed: 28.171s -> 27.806s / 3h
- BTC shard wall: 27.039s; ETH: 6.315s; SOL: 4.735s
- total callbacks: 14.823M typed updates; 21.317M changed book rows
- top BTC buckets:
  - raw stream excluding callback: 9.961s
  - apply update / state maintenance: 8.100s
  - BookRow build: 4.595s
  - on-book strategy event: 3.471s
  - typed stream merge/flush: 3.191s
  - compact record decode: 2.836s
  - V1 on-book event: 2.871s
  - replay state apply: 1.548s
  - process references: 1.014s
  - execute due: 0.980s

Conclusion: earlier default timings were polluted by profiling overhead. The real production path is
now faster, but still not close to the 7-day under-5-minute target. The wall clock is dominated by
the BTC shard, so symbol-level parallelism has largely hit its ceiling for the current 3-symbol
workload. The remaining first-order problem is exact parallelism inside BTC, not more timers or small
lookup caches.
