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

Current table parts are deterministic JSONL. The shared `market_data_etl_core` crate isolates table
writing, hashing, manifest, and schema guard behavior so the physical writer can be swapped for
Parquet without changing the Jupiter/local ownership boundary.
