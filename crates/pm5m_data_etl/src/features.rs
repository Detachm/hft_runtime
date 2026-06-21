use crate::constants::{TABLE_BOOK_TOP10, TABLE_DEPTH_FEATURE};
use crate::facts::{from_micros, row_ask_levels, row_bid_levels};
use crate::manifest::{
    dataset_path, read_or_base_dataset_manifest, row_hash, write_dataset_manifest,
};
use crate::types::*;
use anyhow::Result;
use market_data_etl_core::{hash_path, read_parquet_table, write_parquet_table, BookLevel};
use std::cmp::Ordering;

pub fn build_depth_feature(plan: &PipelinePlan) -> Result<Vec<DepthFeatureRow>> {
    let book_rows =
        read_parquet_table::<PolymarketBookTop10Row>(&dataset_path(plan, TABLE_BOOK_TOP10))?;
    let mut depth_rows = book_rows
        .iter()
        .map(depth_feature_from_book_row)
        .collect::<Result<Vec<_>>>()?;
    depth_rows.sort_by(|a, b| {
        (a.local_recv_ts_ns, a.ingest_seq, &a.primary_key).cmp(&(
            b.local_recv_ts_ns,
            b.ingest_seq,
            &b.primary_key,
        ))
    });
    write_parquet_table(
        &dataset_path(plan, TABLE_DEPTH_FEATURE),
        &depth_rows,
        Some("local_recv_ts_ns"),
    )?;

    let mut manifest = read_or_base_dataset_manifest(plan)?;
    manifest.depth_feature_hash = Some(hash_path(&dataset_path(plan, TABLE_DEPTH_FEATURE))?);
    write_dataset_manifest(plan, &manifest)?;

    Ok(depth_rows)
}

fn depth_feature_from_book_row(row: &PolymarketBookTop10Row) -> Result<DepthFeatureRow> {
    let bids = row_bid_levels(row);
    let asks = row_ask_levels(row);
    let buy_cost_1 = cost_to_buy(1.0, &asks);
    let buy_cost_5 = cost_to_buy(5.0, &asks);
    let buy_cost_10 = cost_to_buy(10.0, &asks);
    let sell_proceeds_1 = proceeds_to_sell(1.0, &bids);
    let sell_proceeds_5 = proceeds_to_sell(5.0, &bids);
    let sell_proceeds_10 = proceeds_to_sell(10.0, &bids);
    let mut feature = DepthFeatureRow {
        schema_version: 1,
        dataset_format: DEPTH_FEATURE_FORMAT.to_string(),
        primary_key: format!("pm_depth_feature:{}", row.primary_key),
        source_book_primary_key: row.primary_key.clone(),
        symbol: row.symbol.clone(),
        condition_id: row.condition_id.clone(),
        asset_id: row.asset_id.clone(),
        outcome: row.outcome.clone(),
        local_recv_ts_ns: row.local_recv_ts_ns,
        ingest_seq: row.ingest_seq,
        best_bid_price: row.best_bid_price_micros.map(from_micros),
        best_ask_price: row.best_ask_price_micros.map(from_micros),
        spread: row
            .best_bid_price_micros
            .zip(row.best_ask_price_micros)
            .map(|(bid, ask)| from_micros(ask - bid)),
        bid_depth_top10: from_micros(row.bid_depth_top10_micros),
        ask_depth_top10: from_micros(row.ask_depth_top10_micros),
        buy_cost_1,
        buy_cost_5,
        buy_cost_10,
        sell_proceeds_1,
        sell_proceeds_5,
        sell_proceeds_10,
        fillable_buy_1: buy_cost_1.is_some(),
        fillable_buy_5: buy_cost_5.is_some(),
        fillable_buy_10: buy_cost_10.is_some(),
        fillable_sell_1: sell_proceeds_1.is_some(),
        fillable_sell_5: sell_proceeds_5.is_some(),
        fillable_sell_10: sell_proceeds_10.is_some(),
        book_age_ms: 0,
        row_hash: String::new(),
    };
    feature.row_hash = row_hash(&feature)?;
    Ok(feature)
}

fn cost_to_buy(quantity: f64, asks: &[BookLevel]) -> Option<f64> {
    fill_cost(quantity, asks, true)
}

fn proceeds_to_sell(quantity: f64, bids: &[BookLevel]) -> Option<f64> {
    fill_cost(quantity, bids, false)
}

fn fill_cost(quantity: f64, levels: &[BookLevel], ascending_price: bool) -> Option<f64> {
    if quantity <= 0.0 {
        return Some(0.0);
    }
    let mut sorted = levels.to_vec();
    if ascending_price {
        sorted.sort_by(|a, b| float_cmp(&a.price, &b.price));
    } else {
        sorted.sort_by(|a, b| float_cmp(&b.price, &a.price));
    }
    let mut remaining = quantity;
    let mut total = 0.0;
    for level in sorted {
        if level.size <= 0.0 {
            continue;
        }
        let fill = remaining.min(level.size);
        total += fill * level.price;
        remaining -= fill;
        if remaining <= f64::EPSILON {
            return Some(total);
        }
    }
    None
}

fn float_cmp(a: &f64, b: &f64) -> Ordering {
    a.partial_cmp(b).unwrap_or(Ordering::Equal)
}
