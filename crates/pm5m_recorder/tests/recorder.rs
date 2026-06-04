use anyhow::{anyhow, Result};
use market_data_etl_core::{read_jsonl_file, scan_json_fields};
use pm5m_data_etl::RawPolymarketBookTop10;
use pm5m_recorder::{run_once, HttpFetcher, RecorderConfig};
use std::collections::BTreeMap;
use tempfile::TempDir;

#[test]
fn run_once_discovers_gamma_assets_and_writes_raw_book_rows() {
    let temp = TempDir::new().unwrap();
    let mut config =
        RecorderConfig::default_for_roots(temp.path().join("raw"), temp.path().join("state"));
    config.source.gamma_markets_url = "https://gamma.example/markets".to_string();
    config.source.clob_base_url = "https://clob.example".to_string();
    config.max_assets_per_cycle = 2;
    config.top_n = 2;

    let mut fetcher = FakeFetcher::default();
    fetcher.insert(
        "https://gamma.example/markets?active=true&closed=false&limit=24&order=volume24hr&ascending=false&accepting_orders=true",
        r#"[{
            "conditionId": "cond-1",
            "slug": "bitcoin-up-or-down",
            "question": "Bitcoin up or down?",
            "active": true,
            "closed": false,
            "acceptingOrders": true,
            "enableOrderBook": true,
            "outcomes": "[\"Yes\", \"No\"]",
            "clobTokenIds": "[\"asset-yes\", \"asset-no\"]"
        }]"#,
    );
    fetcher.insert(
        "https://clob.example/book?token_id=asset-yes",
        r#"{
            "asset_id": "asset-yes",
            "bids": [{"price":"0.49","size":"5"}, {"price":"0.50","size":"3"}, {"price":"0.45","size":"9"}],
            "asks": [{"price":"0.53","size":"1"}, {"price":"0.52","size":"4"}, {"price":"0.55","size":"2"}]
        }"#,
    );
    fetcher.insert(
        "https://clob.example/book?token_id=asset-no",
        r#"{
            "asset_id": "asset-no",
            "bids": [{"price":"0.48","size":"2"}],
            "asks": [{"price":"0.51","size":"2"}]
        }"#,
    );

    let report = run_once(&config, &fetcher).unwrap();
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.errors.len(), 0);

    let output_path = report.output_path.unwrap();
    let rows = read_jsonl_file::<RawPolymarketBookTop10>(&output_path).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].condition_id, "cond-1");
    assert_eq!(rows[0].asset_id, "asset-yes");
    assert_eq!(rows[0].outcome, "Yes");
    assert_eq!(rows[0].bids[0].price, 0.50);
    assert_eq!(rows[0].asks[0].price, 0.52);
    assert_eq!(rows[0].bids.len(), 2);
    assert_eq!(rows[0].asks.len(), 2);

    assert!(config.state_root.join("recorder_state.json").exists());
    assert!(config.state_root.join("recorder_manifest.json").exists());
}

#[test]
fn recorder_output_does_not_contain_strategy_fields() {
    let temp = TempDir::new().unwrap();
    let mut config =
        RecorderConfig::default_for_roots(temp.path().join("raw"), temp.path().join("state"));
    config.source.discovery.enabled = false;
    config.source.clob_base_url = "https://clob.example".to_string();
    config.source.explicit_assets = vec![pm5m_recorder::AssetSpec {
        symbol: "manual".to_string(),
        condition_id: "cond".to_string(),
        asset_id: "asset".to_string(),
        outcome: "YES".to_string(),
    }];

    let mut fetcher = FakeFetcher::default();
    fetcher.insert(
        "https://clob.example/book?token_id=asset",
        r#"{"bids":[{"price":"0.4","size":"1"}],"asks":[{"price":"0.6","size":"1"}]}"#,
    );

    run_once(&config, &fetcher).unwrap();
    let violations = scan_json_fields(
        &config.raw_root,
        &["model_probability", "edge", "trigger", "pnl"],
    )
    .unwrap();
    assert_eq!(violations, Vec::new());
}

#[derive(Default)]
struct FakeFetcher {
    responses: BTreeMap<String, Vec<u8>>,
}

impl FakeFetcher {
    fn insert(&mut self, url: &str, body: &str) {
        self.responses
            .insert(url.to_string(), body.as_bytes().to_vec());
    }
}

impl HttpFetcher for FakeFetcher {
    fn get(&self, url: &str) -> Result<Vec<u8>> {
        self.responses
            .get(url)
            .cloned()
            .ok_or_else(|| anyhow!("missing fake response for {url}"))
    }
}
