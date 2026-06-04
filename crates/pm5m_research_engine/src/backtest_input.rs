use anyhow::Result;
use market_data_etl_core::read_jsonl_table;
use pm5m_data_etl::{
    BinanceKline1sReferenceRow, DepthFeatureRow, PolymarketSettlementRow, StandardEventIndexRow,
};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct BacktestInputs {
    pub events: Vec<StandardEventIndexRow>,
    pub depth_features: Vec<DepthFeatureRow>,
    pub binance_reference: Vec<BinanceKline1sReferenceRow>,
    pub settlement_labels: Vec<PolymarketSettlementRow>,
}

pub fn load_backtest_inputs(dataset_root: &Path) -> Result<BacktestInputs> {
    Ok(BacktestInputs {
        events: read_jsonl_table(&dataset_root.join("streams/pm5m_standard_event_index_v1"))?,
        depth_features: read_jsonl_table(&dataset_root.join("derived/depth_feature_stream_v1"))?,
        binance_reference: read_jsonl_table(
            &dataset_root.join("tables/binance_kline_1s_reference"),
        )?,
        settlement_labels: read_jsonl_table(&dataset_root.join("tables/polymarket_settlement"))?,
    })
}
