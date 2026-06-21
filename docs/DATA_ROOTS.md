# PM5M Data Roots

Generated data is not code. Keep it out of git and keep directory names explicit.

## Canonical Layout

```text
/mnt/data/hft/hft_runtime/
  live_polymarket_all_current_hftrec4_ws_raw/
    raw/              # HFTREC4 raw WS audit
    state/            # recorder state
    book_hftbook2/    # live HFTBOOK2 append, when enabled

  market_cache/
    hftbook2/         # rebuilt or consolidated HFTBOOK2 roots
    hftidx1/          # HFTIDX1 indexes derived from HFTBOOK2
    hftref1/          # local reference caches
    hftsettle1/       # local official settlement caches

  backtest_runs/
    ...               # run-fast output directories

  retirement_manifests/
    ...               # freeze/convert/delete evidence for retired data
```

The exact production root may vary by machine, but the role of each directory should not.

## Retention Rules

- Keep HFTREC4 raw WS audit as the durable audit source.
- Keep HFTBOOK2 if it is expensive to rebuild for the current research window.
- Keep HFTIDX1 while actively iterating; it can be rebuilt from HFTBOOK2.
- Keep HFTREF1 and HFTSETTLE1 local so backtests do not repeatedly download the same data.
- Keep run manifests, summaries, configs, and hashes; compress or delete large intermediate run
  payloads when they are no longer needed.
- Delete or archive old raw formats only after replacement cache/index validation and a retirement
  manifest exist.

## Repository Runtime Directory

`runtime/` is ignored by git. Use it only for local logs, local state, and scratch runs. Do not put
canonical examples or long-term research inputs there.

Versioned examples belong in `configs/` or `docs/`.
