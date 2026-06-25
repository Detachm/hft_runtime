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
