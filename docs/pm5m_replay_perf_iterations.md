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
