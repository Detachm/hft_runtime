# PM5M Poly-Server Time Alignment

This is the canonical timing model for PM5M live-like backtests that try to simulate what the AWS
`poly` server could see in real time. Use this document before interpreting BTC/ETH/SOL PM5M
strategy results.

## Objective

Backtests should align inputs by the time data becomes visible to the strategy process on the
`poly` server, not by an arbitrary recorder's local receive timestamp and not by exchange timestamps
alone.

The timing model has three separate clocks:

- Polymarket CLOB event timestamp: the `timestamp`/`ts`/`time` field carried in WS payloads.
- Polymarket visible time on `poly`: the time the strategy can safely act on CLOB state.
- Binance visible time on `poly`: the time the strategy can safely act on the closed 1s reference
  bar.

## Current Default

For PM5M live-like research runs, use:

- Binance reference visible time: `1s kline close time + 200 ms`.
- Polymarket `price_change` visible time: `payload exchange timestamp + 20 ms`, only when the raw
  recorded receive time is within a 500 ms freshness guard of that modeled time.
- Polymarket `book` snapshot visible time: keep the raw recorded receive time.
- Execution submit latency: keep `submit_latency_ns = 300_000_000`.

The corresponding cache-builder flags are:

```sh
--poly-server-visible-time \
--poly-incremental-latency-ms 20 \
--poly-incremental-freshness-guard-ms 500
```

## Why This Model

Empirical checks on the AWS `poly` host showed current live receive latency around these levels:

- Polymarket WS book/event receive latency is usually tens of milliseconds, with observed medians
  around 9-20 ms depending on stream and run.
- BTC Binance 1s reference receive after bar close is materially slower, with observed BTC median
  around 130 ms and p99 around 216 ms. Using 200 ms is the conservative default; sweep 150/200/250 ms
  when validating sensitivity.
- Polymarket CLOB order POST latency is around 300 ms p50 and roughly 325-340 ms p95/p99 in the
  current live audit, so the backtest submit latency should stay at 300 ms unless the live route
  changes.

Official Polymarket docs describe market-channel events and payload timestamp fields, but they do
not provide a strong guarantee that every WS `timestamp` is exactly client-visible matching-engine
time. In local raw data we also observed stale/replayed snapshots and backlog-like events where
`local_recv - payload_timestamp` was many seconds. Therefore payload timestamps are usable only with
event-type restrictions and a freshness guard.

Reference URLs:

- https://docs.polymarket.com/market-data/websocket/market-channel
- https://docs.polymarket.com/api-reference/wss/market
- https://docs.polymarket.com/api-reference/market-data/get-order-book

## Event Rules

`price_change`:

- Treat as the healthy incremental stream.
- If `exchange_ts_ms + 20 ms <= recorded_local_recv_ts` and the gap is `<= 500 ms`, rewrite
  `local_recv_ts_ns` to `exchange_ts_ms + 20 ms`.
- Otherwise keep recorded local receive time.

`book`:

- Do not blindly rewrite to `exchange_ts + 20 ms`.
- A `book` event can be an initial subscribe snapshot, reconnect snapshot, or stale state snapshot.
  Its payload timestamp can refer to the state version rather than the moment our strategy first
  received that snapshot.
- Keeping recorded receive time prevents a future snapshot from initializing past state.

Replay safety:

- A price-change update must not make an asset's derived book state visible before that asset's
  current book snapshot state was visible.
- The replayer clamps a rewritten price-change context to at least the prior asset-state visible
  time, preventing snapshot lookahead.

## Interpretation

Results built with Binance `+200 ms` but ordinary HFTBOOK2 recorded Polymarket receive time are not
strict poly-server-aligned results. They answer a different question: "What if Binance is modeled
with 200 ms latency, while Polymarket uses the recorder's historical local receive time?"

Strict PM5M live-like BTC/ETH/SOL results should use both:

- HFTREF1 built with `--reference-latency-ms 200`.
- HFTBOOK2 built with `--poly-server-visible-time --poly-incremental-latency-ms 20
  --poly-incremental-freshness-guard-ms 500`.

If the live host, region, provider route, or recorder architecture changes, remeasure the latency
distribution and update this document before comparing new research runs with old ones.
