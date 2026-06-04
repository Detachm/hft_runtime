# Jupiter PM5M Data Pipeline

Standalone Rust workspace for the Jupiter-side PM5M data factory.

Jupiter responsibilities:

- discover/record markets and raw book data
- sync external caches
- materialize fact tables
- derive `depth_feature`
- build a standard replay event index
- run quality acceptance
- export accepted datasets

Local-only research responsibilities live in `pm5m_research_engine` and include fair value, edge,
trigger logic, replay policies, parameter search, and PnL.

The Jupiter CLI is `pm5m-data-etl`:

```sh
cargo run -p pm5m_data_etl --bin pm5m-data-etl -- plan --help
cargo run -p pm5m_data_etl --bin pm5m-data-etl -- sync-inputs --plan ./plan/pipeline_plan.json
cargo run -p pm5m_data_etl --bin pm5m-data-etl -- build-facts --plan ./plan/pipeline_plan.json
cargo run -p pm5m_data_etl --bin pm5m-data-etl -- build-depth-feature --plan ./plan/pipeline_plan.json
cargo run -p pm5m_data_etl --bin pm5m-data-etl -- build-event-index --plan ./plan/pipeline_plan.json
cargo run -p pm5m_data_etl --bin pm5m-data-etl -- accept --plan ./plan/pipeline_plan.json
cargo run -p pm5m_data_etl --bin pm5m-data-etl -- export --plan ./plan/pipeline_plan.json --export-root ./export
```

The continuous Jupiter recorder CLI is `pm5m-recorder`:

```sh
cargo run -p pm5m_recorder --bin pm5m-recorder -- init-config \
  --path ./runtime/pm5m-recorder.json \
  --raw-root ./runtime/raw \
  --state-root ./runtime/state \
  --poll-interval-ms 1000 \
  --max-assets-per-cycle 24

cargo run -p pm5m_recorder --bin pm5m-recorder -- run --config ./runtime/pm5m-recorder.json
```

For user-mode supervision on Jupiter:

```sh
tmux new-session -d -s hft-runtime-recorder \
  'ROOT_DIR=/home/hliu/hft_runtime /home/hliu/hft_runtime/scripts/run_pm5m_recorder_supervised.sh'
```

The recorder discovers active Polymarket Gamma markets with CLOB order books, polls
`clob.polymarket.com/book`, and appends ETL-compatible raw rows to recursive
`polymarket_book_top10.jsonl` partitions. It writes `recorder_state.json` and
`recorder_manifest.json` under the configured state root.

Current table parts are deterministic JSONL. The shared `market_data_etl_core` crate isolates table
writing, hashing, manifest, and schema guard behavior so the physical writer can be swapped for
Parquet without changing the Jupiter/local ownership boundary.
