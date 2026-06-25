#!/usr/bin/env python3
import argparse
import glob
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression


USECOLS = [
    "symbol",
    "horizon_seconds",
    "condition_id",
    "asset_id",
    "asset_key",
    "outcome",
    "ts_ns",
    "window_start_ts_ns",
    "window_end_ts_ns",
    "seconds_to_end",
    "p_model_micros",
    "market_mid_micros",
    "spread_micros",
    "best_ask_price_micros",
    "target_notional_micros",
    "target_fillable",
    "target_avg_fill_price_micros",
    "target_worst_price_micros",
    "target_cash_micros",
    "target_shares_micros",
    "avg_fill_1u_micros",
    "avg_fill_5u_micros",
    "avg_fill_10u_micros",
    "fee_micros_per_share",
    "raw_edge_micros",
    "net_raw_edge_after_fee_micros",
    "winner",
    "realized_pnl_per_share_micros",
    "strategy_gate_passed",
    "locked_side_match",
]

DAY_NS = 86_400 * 1_000_000_000
CST_OFFSET_NS = 8 * 3_600 * 1_000_000_000
THRESHOLDS_5M = [0.02, 0.04, 0.06, 0.08]
SPREAD_CAPS_5M = [0.03, 0.05, 0.08]
MAX_ORDERS_5M = [1, 2, 3]
THRESHOLDS_15M = [0.04, 0.06, 0.08, 0.10]
MAX_ADDS_15M = [0, 1, 2]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pm5m-snapshot-glob", action="append", required=True)
    parser.add_argument("--pm15m-snapshot-glob", action="append", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-train-days", type=int, default=3)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    pm5 = prepare_base(load_snapshots(expand_globs(args.pm5m_snapshot_glob)))
    pm15 = prepare_base(load_snapshots(expand_globs(args.pm15m_snapshot_glob)))

    eth5 = pm5[(pm5["coin"] == "ETH") & (pm5["horizon_seconds"] == 300)].copy()
    eth15_all = pm15[(pm15["coin"] == "ETH") & (pm15["horizon_seconds"] == 900)].copy()
    eth15_gate = derive_entry_phase(eth15_all[eth15_all["strategy_gate_passed"]].copy())

    eth5_gate = eth5[eth5["strategy_gate_passed"]].copy()
    eth5_gate = add_walk_forward_prob(
        eth5_gate,
        group_cols=["outcome"],
        feature_col="market_mid",
        output_col="q_gate",
        min_train_days=args.min_train_days,
    )
    eth5_gate = add_walk_forward_prob(
        eth5_gate,
        group_cols=["outcome"],
        feature_col="p_raw",
        output_col="q_raw_cal",
        min_train_days=args.min_train_days,
    )
    eth5_gate = add_eth5_bucket_aware_prob(eth5_gate, args.min_train_days)

    eth15_gate = add_walk_forward_prob(
        eth15_gate,
        group_cols=["outcome", "entry_phase"],
        feature_col="market_mid",
        output_col="q_gate",
        min_train_days=args.min_train_days,
    )

    eth5_grid = build_eth5m_shadow_grid(eth5_gate)
    eth15_grid = build_eth15m_phase_shadow_grid(eth15_gate, eth15_all)
    eth5_fill_bucket = build_eth5m_qedge_fill_bucket(eth5_gate, eth5)
    eth15_order_index = build_eth15m_current_control_order_index(eth15_gate, eth15_all)
    eth5_low_fill_forensic = build_eth5m_low_fill_forensic(eth5_gate, eth5)
    eth5_bucket_aware = build_eth5m_bucket_aware_qcal_netedge(eth5_gate, eth5)
    eth15_side_order_quality = build_eth15m_side_order_index_add_quality(
        eth15_gate, eth15_all
    )

    eth5_grid.to_csv(output_dir / "eth5m_shadow_grid_gate_qmarket.csv", index=False)
    eth15_grid.to_csv(output_dir / "eth15m_phase_shadow_grid.csv", index=False)
    eth5_fill_bucket.to_csv(output_dir / "eth5m_qedge_fill_bucket.csv", index=False)
    eth15_order_index.to_csv(output_dir / "eth15m_current_control_order_index.csv", index=False)
    eth5_low_fill_forensic.to_csv(output_dir / "eth5m_low_fill_forensic.csv", index=False)
    eth5_bucket_aware.to_csv(output_dir / "eth5m_bucket_aware_qcal_netedge.csv", index=False)
    eth15_side_order_quality.to_csv(
        output_dir / "eth15m_side_order_index_add_quality.csv", index=False
    )
    write_manifest(output_dir, args, eth5, eth5_gate, eth15_all, eth15_gate)


def expand_globs(patterns: list[str]) -> list[str]:
    paths: list[str] = []
    for pattern in patterns:
        paths.extend(path for path in sorted(glob.glob(pattern)) if Path(path).is_file())
    return sorted(set(paths))


def load_snapshots(paths: list[str]) -> pd.DataFrame:
    if not paths:
        raise SystemExit("no input snapshots matched")
    frames = [pd.read_csv(path, usecols=USECOLS, low_memory=False) for path in paths]
    return pd.concat(frames, ignore_index=True)


def prepare_base(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["symbol"] = work["symbol"].str.upper()
    work["coin"] = work["symbol"].str.extract(r"^(BTC|ETH|SOL)", expand=False).fillna(work["symbol"])
    work["outcome"] = work["outcome"].str.upper()
    work["horizon_seconds"] = work["horizon_seconds"].astype(int)
    for col in ["target_fillable", "winner", "strategy_gate_passed", "locked_side_match"]:
        work[col] = parse_bool(work[col])
    work["p_raw"] = clip_prob(work["p_model_micros"] / 1_000_000.0)
    work["market_mid"] = clip_prob(work["market_mid_micros"] / 1_000_000.0)
    work["spread"] = work["spread_micros"] / 1_000_000.0
    work["best_ask"] = work["best_ask_price_micros"] / 1_000_000.0
    work["avg_fill"] = work["target_avg_fill_price_micros"] / 1_000_000.0
    work["worst_fill"] = work["target_worst_price_micros"] / 1_000_000.0
    work["target_cash"] = work["target_cash_micros"] / 1_000_000.0
    work["target_notional"] = work["target_notional_micros"] / 1_000_000.0
    work["avg_fill_1u"] = work["avg_fill_1u_micros"] / 1_000_000.0
    work["avg_fill_5u"] = work["avg_fill_5u_micros"] / 1_000_000.0
    work["avg_fill_10u"] = work["avg_fill_10u_micros"] / 1_000_000.0
    work["fee"] = work["fee_micros_per_share"] / 1_000_000.0
    work["raw_edge"] = work["raw_edge_micros"] / 1_000_000.0
    work["net_raw_edge_after_fee"] = work["net_raw_edge_after_fee_micros"] / 1_000_000.0
    work["shares"] = work["target_shares_micros"] / 1_000_000.0
    work["pnl_share"] = work["realized_pnl_per_share_micros"] / 1_000_000.0
    work["pnl_usdc"] = work["pnl_share"] * work["shares"]
    work["day_cst"] = ((work["ts_ns"] + CST_OFFSET_NS) // DAY_NS).astype(np.int64)
    work["tau_bucket"] = pd.cut(
        work["seconds_to_end"],
        bins=[30, 90, 180, np.inf],
        labels=["30-90s", "90-180s", "180s+"],
        right=False,
    )
    work["avg_fill_bucket"] = pd.cut(
        work["avg_fill"],
        bins=[-np.inf, 0.55, 0.65, 0.75, np.inf],
        labels=["<55c", "55-65c", "65-75c", ">75c"],
        right=False,
    )
    return work


def parse_bool(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def clip_prob(values):
    return np.clip(values, 1e-6, 1 - 1e-6)


def logit(values) -> np.ndarray:
    values = clip_prob(np.asarray(values, dtype=float))
    return np.log(values / (1 - values))


def add_walk_forward_prob(
    df: pd.DataFrame,
    group_cols: list[str],
    feature_col: str,
    output_col: str,
    min_train_days: int,
) -> pd.DataFrame:
    work = df.copy()
    work[output_col] = np.nan
    for _, idx in work.groupby(group_cols, sort=True).groups.items():
        group = work.loc[idx].sort_values("ts_ns")
        for day in sorted(group["day_cst"].unique()):
            train = group[group["day_cst"] < day].dropna(subset=[feature_col])
            test_idx = group[(group["day_cst"] == day) & group[feature_col].notna()].index
            if train["day_cst"].nunique() < min_train_days or len(test_idx) == 0:
                continue
            y_train = train["winner"].astype(int).to_numpy()
            if len(np.unique(y_train)) < 2:
                continue
            clf = LogisticRegression(C=1e6, solver="lbfgs", max_iter=300)
            clf.fit(logit(train[feature_col]).reshape(-1, 1), y_train)
            work.loc[test_idx, output_col] = clf.predict_proba(
                logit(work.loc[test_idx, feature_col]).reshape(-1, 1)
            )[:, 1]
    return work


def add_eth5_bucket_aware_prob(df: pd.DataFrame, min_train_days: int) -> pd.DataFrame:
    work = df.copy()
    work["q_bucket_cal"] = np.nan
    valid = work[
        work["target_fillable"]
        & work["market_mid"].notna()
        & work["tau_bucket"].notna()
        & work["avg_fill_bucket"].notna()
    ].copy()
    if valid.empty:
        return work
    for day in sorted(valid["day_cst"].unique()):
        train = valid[valid["day_cst"] < day]
        test = valid[valid["day_cst"] == day]
        if train["day_cst"].nunique() < min_train_days or test.empty:
            continue
        y_train = train["winner"].astype(int).to_numpy()
        if len(np.unique(y_train)) < 2:
            continue
        weights = market_balanced_weights(train)
        clf = LogisticRegression(C=1.0, solver="lbfgs", max_iter=500)
        clf.fit(bucket_aware_features(train), y_train, sample_weight=weights)
        work.loc[test.index, "q_bucket_cal"] = clf.predict_proba(
            bucket_aware_features(test)
        )[:, 1]
    return work


def market_balanced_weights(df: pd.DataFrame) -> np.ndarray:
    counts = df.groupby("condition_id", observed=True)["condition_id"].transform("size")
    weights = 1.0 / counts.astype(float)
    return weights.to_numpy()


def bucket_aware_features(df: pd.DataFrame) -> np.ndarray:
    tau = df["tau_bucket"].astype(str)
    fill = df["avg_fill_bucket"].astype(str)
    features = pd.DataFrame(
        {
            "logit_market_mid": logit(df["market_mid"]),
            "side_yes": (df["outcome"] == "YES").astype(float).to_numpy(),
            "tau_90_180": (tau == "90-180s").astype(float).to_numpy(),
            "tau_180_plus": (tau == "180s+").astype(float).to_numpy(),
            "fill_55_65": (fill == "55-65c").astype(float).to_numpy(),
            "fill_65_75": (fill == "65-75c").astype(float).to_numpy(),
            "fill_75_plus": (fill == ">75c").astype(float).to_numpy(),
        },
        index=df.index,
    )
    return features.to_numpy(dtype=float)


def derive_entry_phase(gate: pd.DataFrame) -> pd.DataFrame:
    work = gate.copy()
    work["entry_phase"] = "cheap_reentry"
    first_locked_ts = (
        work[work["locked_side_match"]].groupby("condition_id", observed=True)["ts_ns"].min()
    )
    mapped = work["condition_id"].map(first_locked_ts)
    has_first = mapped.notna()
    is_first = work["locked_side_match"] & has_first & (work["ts_ns"] == mapped)
    is_add = work["locked_side_match"] & has_first & (work["ts_ns"] > mapped)
    work.loc[is_first, "entry_phase"] = "first_entry"
    work.loc[is_add, "entry_phase"] = "locked_add"
    return work


def build_eth5m_shadow_grid(gate: pd.DataFrame) -> pd.DataFrame:
    base = gate[
        gate["target_fillable"]
        & gate["q_gate"].notna()
        & gate["q_raw_cal"].notna()
        & gate["tau_bucket"].notna()
        & gate["avg_fill_bucket"].notna()
    ].copy()
    base["net_edge_qmarket"] = base["q_gate"] - base["avg_fill"] - base["fee"]
    base["raw_veto_edge"] = base["q_raw_cal"] - base["avg_fill"] - base["fee"]
    rows = []
    for threshold in THRESHOLDS_5M:
        for max_orders in MAX_ORDERS_5M:
            for spread_cap in SPREAD_CAPS_5M:
                for raw_veto in [False, True]:
                    eligible = base[
                        (base["net_edge_qmarket"] > threshold)
                        & (base["spread"] <= spread_cap)
                    ].copy()
                    if raw_veto:
                        eligible = eligible[eligible["raw_veto_edge"] > 0].copy()
                    selected = cap_orders_per_market(eligible, max_orders)
                    rows.extend(
                        summarize_selected(
                            selected,
                            {
                                "threshold_c": int(round(threshold * 100)),
                                "max_orders_per_market": max_orders,
                                "spread_cap_c": int(round(spread_cap * 100)),
                                "raw_veto": raw_veto,
                            },
                            group_cols=["outcome", "tau_bucket", "avg_fill_bucket"],
                        )
                    )
    return pd.DataFrame(rows).sort_values(
        [
            "threshold_c",
            "max_orders_per_market",
            "spread_cap_c",
            "raw_veto",
            "outcome",
            "tau_bucket",
            "avg_fill_bucket",
        ]
    )


def build_eth5m_qedge_fill_bucket(gate: pd.DataFrame, all_rows: pd.DataFrame) -> pd.DataFrame:
    base = gate[
        gate["target_fillable"]
        & gate["q_gate"].notna()
        & gate["q_raw_cal"].notna()
        & gate["avg_fill_bucket"].notna()
        & gate["tau_bucket"].notna()
    ].copy()
    base["net_edge_qmarket"] = base["q_gate"] - base["avg_fill"] - base["fee"]
    base["raw_veto_edge"] = base["q_raw_cal"] - base["avg_fill"] - base["fee"]
    rows = []
    drift_cols = ["mid_drift_1s", "mid_drift_3s", "mid_drift_5s"]
    for threshold in [0.04, 0.06]:
        eligible = base[
            (base["net_edge_qmarket"] > threshold)
            & (base["spread"] <= 0.03)
            & (base["raw_veto_edge"] > 0)
        ].copy()
        selected = cap_orders_per_market(eligible, 1)
        selected = add_mid_drifts(selected, all_rows)
        config = {
            "threshold_c": int(round(threshold * 100)),
            "max_orders_per_market": 1,
            "spread_cap_c": 3,
            "raw_veto": True,
        }
        rows.extend(
            summarize_selected(
                selected.assign(tau_bucket="ALL"),
                {**config, "group_level": "fill_bucket"},
                group_cols=["outcome", "tau_bucket", "avg_fill_bucket"],
                drift_cols=drift_cols,
            )
        )
        rows.extend(
            summarize_selected(
                selected.assign(outcome="ALL", tau_bucket="ALL"),
                {**config, "group_level": "fill_bucket"},
                group_cols=["outcome", "tau_bucket", "avg_fill_bucket"],
                drift_cols=drift_cols,
            )
        )
        rows.extend(
            summarize_selected(
                selected,
                {**config, "group_level": "tau_x_fill_bucket"},
                group_cols=["outcome", "tau_bucket", "avg_fill_bucket"],
                drift_cols=drift_cols,
            )
        )
        rows.extend(
            summarize_selected(
                selected.assign(outcome="ALL"),
                {**config, "group_level": "tau_x_fill_bucket"},
                group_cols=["outcome", "tau_bucket", "avg_fill_bucket"],
                drift_cols=drift_cols,
            )
        )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(
        ["threshold_c", "group_level", "outcome", "tau_bucket", "avg_fill_bucket"]
    )


def build_eth5m_low_fill_forensic(gate: pd.DataFrame, all_rows: pd.DataFrame) -> pd.DataFrame:
    base = eth5_qmarket_candidate_base(gate)
    pre_cap = base[
        (base["net_edge_qmarket"] > 0.04)
        & (base["spread"] <= 0.03)
        & (base["raw_veto_edge"] > 0)
    ].copy()
    selected = cap_orders_per_market(pre_cap, 1)
    selected = add_mid_drifts(selected, all_rows)
    config = {
        "candidate_rule": "qgate4c_max1_spread3_rawveto",
        "threshold_c": 4,
        "max_orders_per_market": 1,
        "spread_cap_c": 3,
        "raw_veto": True,
    }
    rows = []
    for group_level, group_cols in [
        ("fill_bucket", ["avg_fill_bucket"]),
        ("side_x_fill_bucket", ["outcome", "avg_fill_bucket"]),
        ("side_x_tau_x_fill_bucket", ["outcome", "tau_bucket", "avg_fill_bucket"]),
    ]:
        rows.extend(
            summarize_forensic(
                selected,
                pre_cap,
                all_rows,
                {**config, "group_level": group_level},
                group_cols=group_cols,
            )
        )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(
        ["group_level", "outcome", "tau_bucket", "avg_fill_bucket"],
        na_position="first",
    )


def build_eth5m_bucket_aware_qcal_netedge(
    gate: pd.DataFrame, all_rows: pd.DataFrame
) -> pd.DataFrame:
    base = gate[
        gate["target_fillable"]
        & gate["q_bucket_cal"].notna()
        & gate["q_raw_cal"].notna()
        & gate["tau_bucket"].notna()
        & gate["avg_fill_bucket"].notna()
    ].copy()
    base["net_edge_bucket_qcal"] = base["q_bucket_cal"] - base["avg_fill"] - base["fee"]
    base["raw_veto_edge"] = base["q_raw_cal"] - base["avg_fill"] - base["fee"]
    base["net_edge_bucket"] = net_edge_bucket(base["net_edge_bucket_qcal"])
    base = add_mid_drifts(base, all_rows)

    rows = []
    rows.extend(
        summarize_edge_groups(
            base.assign(outcome="ALL", tau_bucket="ALL", avg_fill_bucket="ALL"),
            {
                "scope": "all_oos_rows",
                "group_level": "net_edge_bucket",
                "threshold_c": math.nan,
                "max_orders_per_market": math.nan,
                "spread_cap_c": math.nan,
                "raw_veto": math.nan,
                "fill_filter": "none",
            },
            group_cols=["net_edge_bucket", "outcome", "tau_bucket", "avg_fill_bucket"],
        )
    )
    rows.extend(
        summarize_edge_groups(
            base.assign(outcome="ALL", tau_bucket="ALL"),
            {
                "scope": "all_oos_rows",
                "group_level": "net_edge_x_fill_bucket",
                "threshold_c": math.nan,
                "max_orders_per_market": math.nan,
                "spread_cap_c": math.nan,
                "raw_veto": math.nan,
                "fill_filter": "none",
            },
            group_cols=["net_edge_bucket", "outcome", "tau_bucket", "avg_fill_bucket"],
        )
    )

    first_rows = base.sort_values(["condition_id", "ts_ns", "outcome"]).groupby(
        "condition_id", observed=True
    ).head(1)
    rows.extend(
        summarize_edge_groups(
            first_rows.assign(outcome="ALL", tau_bucket="ALL", avg_fill_bucket="ALL"),
            {
                "scope": "first_oos_row_per_market",
                "group_level": "net_edge_bucket",
                "threshold_c": math.nan,
                "max_orders_per_market": 1,
                "spread_cap_c": math.nan,
                "raw_veto": math.nan,
                "fill_filter": "none",
            },
            group_cols=["net_edge_bucket", "outcome", "tau_bucket", "avg_fill_bucket"],
        )
    )

    for threshold in [0.02, 0.04, 0.06, 0.08]:
        eligible = base[
            (base["net_edge_bucket_qcal"] > threshold)
            & (base["spread"] <= 0.03)
            & (base["raw_veto_edge"] > 0)
            & (base["avg_fill"] < 0.65)
        ].copy()
        selected = cap_orders_per_market(eligible, 1)
        config = {
            "scope": "trade_candidate_lt65",
            "threshold_c": int(round(threshold * 100)),
            "max_orders_per_market": 1,
            "spread_cap_c": 3,
            "raw_veto": True,
            "fill_filter": "avg_fill<65c",
        }
        rows.extend(
            summarize_edge_groups(
                selected.assign(
                    net_edge_bucket="ALL",
                    outcome="ALL",
                    tau_bucket="ALL",
                    avg_fill_bucket="ALL",
                ),
                {**config, "group_level": "threshold_summary"},
                group_cols=["net_edge_bucket", "outcome", "tau_bucket", "avg_fill_bucket"],
            )
        )
        rows.extend(
            summarize_edge_groups(
                selected.assign(outcome="ALL", tau_bucket="ALL"),
                {**config, "group_level": "threshold_x_fill_bucket"},
                group_cols=["net_edge_bucket", "outcome", "tau_bucket", "avg_fill_bucket"],
            )
        )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(
        [
            "scope",
            "group_level",
            "threshold_c",
            "net_edge_bucket",
            "outcome",
            "tau_bucket",
            "avg_fill_bucket",
        ],
        na_position="first",
    )


def eth5_qmarket_candidate_base(gate: pd.DataFrame) -> pd.DataFrame:
    base = gate[
        gate["target_fillable"]
        & gate["q_gate"].notna()
        & gate["q_raw_cal"].notna()
        & gate["tau_bucket"].notna()
        & gate["avg_fill_bucket"].notna()
    ].copy()
    base["net_edge_qmarket"] = base["q_gate"] - base["avg_fill"] - base["fee"]
    base["raw_veto_edge"] = base["q_raw_cal"] - base["avg_fill"] - base["fee"]
    return base


def net_edge_bucket(series: pd.Series) -> pd.Series:
    return pd.cut(
        series,
        bins=[-np.inf, 0.0, 0.02, 0.04, 0.06, 0.08, 0.12, np.inf],
        labels=["<=0c", "0-2c", "2-4c", "4-6c", "6-8c", "8-12c", ">12c"],
        right=False,
    )


def summarize_edge_groups(
    selected: pd.DataFrame,
    config: dict,
    group_cols: list[str],
) -> list[dict]:
    if selected.empty:
        return []
    rows = []
    drift_cols = ["mid_drift_1s", "mid_drift_3s", "mid_drift_5s"]
    for keys, group in selected.groupby(group_cols, observed=True, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(config)
        row.update(dict(zip(group_cols, keys)))
        row.update(performance_metrics(group, drift_cols))
        row.update(distribution_metrics_plus(group))
        row["spearman_net_edge_vs_pnl_share"] = safe_spearman(
            group["net_edge_bucket_qcal"], group["pnl_share"]
        )
        rows.append(row)
    return rows


def summarize_forensic(
    selected: pd.DataFrame,
    pre_cap: pd.DataFrame,
    all_rows: pd.DataFrame,
    config: dict,
    group_cols: list[str],
) -> list[dict]:
    if selected.empty:
        return []
    rows = []
    drift_cols = ["mid_drift_1s", "mid_drift_3s", "mid_drift_5s"]
    original_selected = selected.copy()
    selected_work = selected.copy()
    pre_cap_work = pre_cap.copy()
    default_values = {"outcome": "ALL", "tau_bucket": "ALL", "avg_fill_bucket": "ALL"}
    for col, value in default_values.items():
        if col not in group_cols:
            selected_work[col] = value
            pre_cap_work[col] = value
    for keys, group in selected_work.groupby(group_cols, observed=True, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        filters = dict(zip(group_cols, keys))
        pre_group = pre_cap_work
        for col, value in filters.items():
            pre_group = pre_group[pre_group[col] == value]
        row = dict(config)
        row.update({"outcome": "ALL", "tau_bucket": "ALL", "avg_fill_bucket": "ALL"})
        row.update(filters)
        row.update(performance_metrics(group, drift_cols))
        row.update(distribution_metrics_plus(group))
        row.update(pre_cap_metrics(pre_group))
        original_group = original_selected.loc[group.index]
        row.update(fill_sanity_metrics(original_group))
        row.update(mapping_sanity_metrics(original_group, all_rows))
        rows.append(row)
    return rows


def pre_cap_metrics(group: pd.DataFrame) -> dict:
    rows_per_market = group.groupby("condition_id", observed=True).size()
    return {
        "pre_cap_row_count": int(len(group)),
        "pre_cap_unique_market_count": int(group["condition_id"].nunique()),
        "pre_cap_avg_rows_per_market": float(rows_per_market.mean())
        if not rows_per_market.empty
        else math.nan,
        "pre_cap_max_rows_per_market": int(rows_per_market.max())
        if not rows_per_market.empty
        else 0,
    }


def fill_sanity_metrics(group: pd.DataFrame) -> dict:
    eps = 1e-9
    return {
        "avg_seconds_to_end": float(group["seconds_to_end"].mean()),
        "min_seconds_to_end": float(group["seconds_to_end"].min()),
        "max_seconds_to_end": float(group["seconds_to_end"].max()),
        "avg_best_ask": float(group["best_ask"].mean()),
        "avg_fill_minus_best_ask": float((group["avg_fill"] - group["best_ask"]).mean()),
        "avg_worst_fill": float(group["worst_fill"].mean()),
        "avg_target_cash": float(group["target_cash"].mean()),
        "avg_target_notional": float(group["target_notional"].mean()),
        "avg_fill_equals_1u_fill_frac": float(
            ((group["avg_fill"] - group["avg_fill_1u"]).abs() <= eps).mean()
        ),
        "avg_fill_minus_5u_fill": float((group["avg_fill"] - group["avg_fill_5u"]).mean()),
        "avg_fill_minus_10u_fill": float((group["avg_fill"] - group["avg_fill_10u"]).mean()),
        "selected_duplicate_condition_count": int(len(group) - group["condition_id"].nunique()),
    }


def mapping_sanity_metrics(group: pd.DataFrame, all_rows: pd.DataFrame) -> dict:
    if group.empty:
        return {
            "window_mapping_conflict_count": 0,
            "asset_mapping_conflict_count": 0,
            "asset_id_missing_frac": math.nan,
        }
    source = all_rows[all_rows["condition_id"].isin(group["condition_id"].unique())]
    window_counts = source.groupby("condition_id", observed=True)[
        ["window_start_ts_ns", "window_end_ts_ns"]
    ].nunique(dropna=False)
    window_conflicts = (
        (window_counts["window_start_ts_ns"] > 1)
        | (window_counts["window_end_ts_ns"] > 1)
    )
    pairs = group[["condition_id", "outcome"]].drop_duplicates()
    source_pairs = source.merge(pairs, on=["condition_id", "outcome"], how="inner")
    asset_counts = source_pairs.groupby(["condition_id", "outcome"], observed=True)[
        "asset_id"
    ].nunique(dropna=False)
    return {
        "window_mapping_conflict_count": int(window_conflicts.sum()),
        "asset_mapping_conflict_count": int((asset_counts > 1).sum()),
        "asset_id_missing_frac": float(group["asset_id"].isna().mean()),
    }


def cap_orders_per_market(df: pd.DataFrame, max_orders: int) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    work = df.sort_values(["condition_id", "ts_ns", "outcome"]).copy()
    work["order_rank_in_market"] = work.groupby("condition_id", observed=True).cumcount() + 1
    return work[work["order_rank_in_market"] <= max_orders].copy()


def summarize_selected(
    selected: pd.DataFrame,
    config: dict,
    group_cols: list[str],
    drift_cols: list[str] | None = None,
) -> list[dict]:
    if selected.empty:
        return []
    rows = []
    drift_cols = drift_cols or []
    for keys, group in selected.groupby(group_cols, observed=True, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(config)
        row.update(dict(zip(group_cols, keys)))
        row.update(performance_metrics(group, drift_cols))
        rows.append(row)
    return rows


def performance_metrics(group: pd.DataFrame, drift_cols: list[str]) -> dict:
    out = {
        "order_count": int(len(group)),
        "unique_market_count": int(group["condition_id"].nunique()),
        "win_rate": float(group["winner"].mean()),
        "total_pnl_usdc": float(group["pnl_usdc"].sum()),
        "avg_pnl_usdc_per_order": float(group["pnl_usdc"].mean()),
        "avg_pnl_per_share": float(group["pnl_share"].mean()),
        "avg_market_mid": float(group["market_mid"].mean()),
        "avg_fill": float(group["avg_fill"].mean()),
        "avg_fee": float(group["fee"].mean()),
        "avg_spread": float(group["spread"].mean()),
    }
    out["realized_ev_win_minus_fill_fee"] = (
        out["win_rate"] - out["avg_fill"] - out["avg_fee"]
    )
    out["avg_q_gate"] = float(group["q_gate"].mean()) if "q_gate" in group else math.nan
    out["avg_net_edge_qmarket"] = (
        float(group["net_edge_qmarket"].mean()) if "net_edge_qmarket" in group else math.nan
    )
    out["avg_q_bucket_cal"] = (
        float(group["q_bucket_cal"].mean()) if "q_bucket_cal" in group else math.nan
    )
    out["avg_net_edge_bucket_qcal"] = (
        float(group["net_edge_bucket_qcal"].mean())
        if "net_edge_bucket_qcal" in group
        else math.nan
    )
    if "q_raw_cal" in group:
        out["avg_q_raw_cal"] = float(group["q_raw_cal"].mean())
        out["avg_raw_veto_edge"] = float(group["raw_veto_edge"].mean())
    for col in drift_cols:
        out[f"avg_{col}"] = float(group[col].mean(skipna=True))
    return out


def build_eth15m_phase_shadow_grid(gate: pd.DataFrame, all_rows: pd.DataFrame) -> pd.DataFrame:
    base = gate[
        gate["target_fillable"]
        & gate["q_gate"].notna()
        & gate["avg_fill"].notna()
    ].copy()
    base["net_edge_qmarket"] = base["q_gate"] - base["avg_fill"] - base["fee"]
    base = add_mid_drifts(base, all_rows)

    rows = []
    drift_cols = ["mid_drift_1s", "mid_drift_3s", "mid_drift_5s"]
    for threshold in THRESHOLDS_15M:
        for max_add in MAX_ADDS_15M:
            selected = select_eth15_first_plus_fresh_add(base, threshold, max_add)
            rows.extend(
                summarize_selected(
                    selected,
                    {
                        "strategy_variant": "first_plus_fresh_add_qedge",
                        "threshold_c": int(round(threshold * 100)),
                        "max_add": max_add,
                    },
                    group_cols=["outcome", "entry_phase"],
                    drift_cols=drift_cols,
                )
            )
            if max_add == 0:
                first_only = select_eth15_first_only(base, threshold)
                rows.extend(
                    summarize_selected(
                        first_only,
                        {
                            "strategy_variant": "first_entry_only_qedge",
                            "threshold_c": int(round(threshold * 100)),
                            "max_add": 0,
                        },
                        group_cols=["outcome", "entry_phase"],
                        drift_cols=drift_cols,
                    )
                )

    control_base = gate[gate["target_fillable"] & gate["q_gate"].notna()].copy()
    control_base["net_edge_qmarket"] = np.nan
    control_base = add_mid_drifts(control_base, all_rows)
    for max_add in MAX_ADDS_15M:
        control = select_eth15_current_direction_lock_control(control_base, max_add)
        rows.extend(
            summarize_selected(
                control,
                {
                    "strategy_variant": "current_direction_lock_control",
                    "threshold_c": math.nan,
                    "max_add": max_add,
                },
                group_cols=["outcome", "entry_phase"],
                drift_cols=drift_cols,
            )
        )
    return pd.DataFrame(rows).sort_values(
        ["strategy_variant", "threshold_c", "max_add", "outcome", "entry_phase"],
        na_position="first",
    )


def build_eth15m_current_control_order_index(
    oos_gate: pd.DataFrame, all_rows: pd.DataFrame
) -> pd.DataFrame:
    full_gate = derive_entry_phase(all_rows[all_rows["strategy_gate_passed"]].copy())
    full_gate = full_gate[full_gate["target_fillable"]].copy()
    oos_gate = oos_gate[oos_gate["target_fillable"] & oos_gate["q_gate"].notna()].copy()
    scopes = [
        ("full_gate_passed", full_gate),
        ("oos_q_available", oos_gate),
    ]
    rows = []
    for scope, frame in scopes:
        selected = select_eth15_current_control_with_order_index(frame, max_add=2)
        selected = add_mid_drifts(selected, all_rows)
        for outcome in ["ALL", "NO", "YES"]:
            subset = selected if outcome == "ALL" else selected[selected["outcome"] == outcome]
            if subset.empty:
                continue
            for order_index, group in subset.groupby("order_index", sort=True):
                row = {
                    "sample_scope": scope,
                    "outcome": outcome,
                    "order_index": order_index,
                    **performance_metrics(
                        group, ["mid_drift_1s", "mid_drift_3s", "mid_drift_5s"]
                    ),
                    **distribution_metrics(group),
                }
                rows.append(row)
            rows.append(
                {
                    "sample_scope": scope,
                    "outcome": outcome,
                    "order_index": "ALL",
                    **performance_metrics(
                        subset, ["mid_drift_1s", "mid_drift_3s", "mid_drift_5s"]
                    ),
                    **distribution_metrics(subset),
                }
            )
    if not rows:
        return pd.DataFrame()
    order_rank = {"first": 0, "add1": 1, "add2": 2, "ALL": 3}
    out = pd.DataFrame(rows)
    out["_order_rank"] = out["order_index"].map(order_rank).fillna(99)
    return out.sort_values(["sample_scope", "outcome", "_order_rank"]).drop(
        columns=["_order_rank"]
    )


def build_eth15m_side_order_index_add_quality(
    oos_gate: pd.DataFrame, all_rows: pd.DataFrame
) -> pd.DataFrame:
    full_gate = derive_entry_phase(all_rows[all_rows["strategy_gate_passed"]].copy())
    full_gate = full_gate[full_gate["target_fillable"]].copy()
    oos_gate = oos_gate[oos_gate["target_fillable"] & oos_gate["q_gate"].notna()].copy()
    rows = []
    for scope, frame in [
        ("full_gate_passed", full_gate),
        ("oos_q_available", oos_gate),
    ]:
        selected = select_eth15_current_control_with_order_index(frame, max_add=2)
        selected = add_mid_drifts(selected, all_rows)
        for group_level, group_cols in [
            ("side_order_index", ["outcome", "order_index"]),
            ("side_order_index_tau", ["outcome", "order_index", "tau_bucket"]),
            ("side_order_index_fill", ["outcome", "order_index", "avg_fill_bucket"]),
            (
                "side_order_index_tau_fill",
                ["outcome", "order_index", "tau_bucket", "avg_fill_bucket"],
            ),
        ]:
            rows.extend(
                summarize_eth15_add_quality(
                    selected,
                    {
                        "sample_scope": scope,
                        "group_level": group_level,
                        "max_add": 2,
                    },
                    group_cols,
                )
            )
    if not rows:
        return pd.DataFrame()
    order_rank = {"first": 0, "add1": 1, "add2": 2, "ALL": 3}
    out = pd.DataFrame(rows)
    out["_order_rank"] = out["order_index"].map(order_rank).fillna(99)
    return out.sort_values(
        [
            "sample_scope",
            "group_level",
            "outcome",
            "_order_rank",
            "tau_bucket",
            "avg_fill_bucket",
        ],
        na_position="first",
    ).drop(columns=["_order_rank"])


def summarize_eth15_add_quality(
    selected: pd.DataFrame, config: dict, group_cols: list[str]
) -> list[dict]:
    if selected.empty:
        return []
    rows = []
    drift_cols = ["mid_drift_1s", "mid_drift_3s", "mid_drift_5s"]
    work = selected.copy()
    for col, value in {"tau_bucket": "ALL", "avg_fill_bucket": "ALL"}.items():
        if col not in group_cols:
            work[col] = value
    for keys, group in work.groupby(group_cols, observed=True, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(config)
        row.update({"tau_bucket": "ALL", "avg_fill_bucket": "ALL"})
        row.update(dict(zip(group_cols, keys)))
        row.update(performance_metrics(group, drift_cols))
        row.update(distribution_metrics_plus(group))
        rows.append(row)
    return rows


def select_eth15_first_only(base: pd.DataFrame, threshold: float) -> pd.DataFrame:
    first = base[
        (base["entry_phase"] == "first_entry")
        & (base["net_edge_qmarket"] > threshold)
    ].copy()
    return first.sort_values(["condition_id", "ts_ns"]).groupby("condition_id", observed=True).head(1)


def select_eth15_first_plus_fresh_add(
    base: pd.DataFrame, threshold: float, max_add: int
) -> pd.DataFrame:
    selected = []
    eligible = base[base["net_edge_qmarket"] > threshold].sort_values(
        ["condition_id", "ts_ns", "entry_phase"]
    )
    for _, group in eligible.groupby("condition_id", observed=True):
        first = group[group["entry_phase"] == "first_entry"].head(1)
        if first.empty:
            continue
        selected.append(first)
        if max_add > 0:
            adds = group[
                (group["entry_phase"] == "locked_add")
                & (group["outcome"].iloc[0] == group["outcome"])
            ].head(max_add)
            if not adds.empty:
                selected.append(adds)
    if not selected:
        return base.iloc[0:0].copy()
    return pd.concat(selected, ignore_index=False).sort_values(["condition_id", "ts_ns"])


def select_eth15_current_direction_lock_control(base: pd.DataFrame, max_add: int) -> pd.DataFrame:
    selected = []
    eligible = base[base["entry_phase"].isin(["first_entry", "locked_add"])].sort_values(
        ["condition_id", "ts_ns", "entry_phase"]
    )
    for _, group in eligible.groupby("condition_id", observed=True):
        first = group[group["entry_phase"] == "first_entry"].head(1)
        if first.empty:
            continue
        selected.append(first)
        if max_add > 0:
            adds = group[
                (group["entry_phase"] == "locked_add")
                & (group["outcome"] == first["outcome"].iloc[0])
            ].head(max_add)
            if not adds.empty:
                selected.append(adds)
    if not selected:
        return base.iloc[0:0].copy()
    return pd.concat(selected, ignore_index=False).sort_values(["condition_id", "ts_ns"])


def select_eth15_current_control_with_order_index(
    base: pd.DataFrame, max_add: int
) -> pd.DataFrame:
    selected = []
    eligible = base[base["entry_phase"].isin(["first_entry", "locked_add"])].sort_values(
        ["condition_id", "ts_ns", "entry_phase"]
    )
    for _, group in eligible.groupby("condition_id", observed=True):
        first = group[group["entry_phase"] == "first_entry"].head(1).copy()
        if first.empty:
            continue
        first["order_index"] = "first"
        selected.append(first)
        if max_add > 0:
            adds = group[
                (group["entry_phase"] == "locked_add")
                & (group["outcome"] == first["outcome"].iloc[0])
            ].head(max_add).copy()
            if not adds.empty:
                adds["order_index"] = [f"add{idx}" for idx in range(1, len(adds) + 1)]
                selected.append(adds)
    if not selected:
        return base.iloc[0:0].copy()
    return pd.concat(selected, ignore_index=False).sort_values(["condition_id", "ts_ns"])


def add_mid_drifts(selected: pd.DataFrame, all_rows: pd.DataFrame) -> pd.DataFrame:
    out = selected.copy()
    lookup = all_rows[
        all_rows["market_mid"].notna()
    ][["condition_id", "outcome", "ts_ns", "market_mid"]].sort_values(
        ["condition_id", "outcome", "ts_ns"]
    )
    left_base = out[["condition_id", "outcome", "ts_ns", "market_mid"]].copy()
    left_base["ts_ns"] = left_base["ts_ns"].astype(np.int64)
    lookup["ts_ns"] = lookup["ts_ns"].astype(np.int64)
    for seconds in [1, 3, 5]:
        left = left_base.copy()
        left["target_ts_ns"] = left["ts_ns"] + seconds * 1_000_000_000
        left["target_ts_ns"] = left["target_ts_ns"].astype(np.int64)
        right = lookup.rename(columns={"ts_ns": "future_ts_ns", "market_mid": "future_mid"}).copy()
        right["future_ts_ns"] = right["future_ts_ns"].astype(np.int64)
        merged = pd.merge_asof(
            left.sort_values("target_ts_ns"),
            right.sort_values("future_ts_ns"),
            left_on="target_ts_ns",
            right_on="future_ts_ns",
            by=["condition_id", "outcome"],
            direction="forward",
            tolerance=2_000_000_000,
        )
        drift = merged["future_mid"].to_numpy() - merged["market_mid"].to_numpy()
        out.loc[left.sort_values("target_ts_ns").index, f"mid_drift_{seconds}s"] = drift
    return out


def distribution_metrics(group: pd.DataFrame) -> dict:
    daily = group.groupby("day_cst", observed=True)["pnl_usdc"].sum()
    market = group.groupby("condition_id", observed=True)["pnl_usdc"].sum()
    total_pnl = float(group["pnl_usdc"].sum())
    total_abs_market = float(market.abs().sum())
    top5_abs = float(market.abs().sort_values(ascending=False).head(5).sum())
    top10_abs = float(market.abs().sort_values(ascending=False).head(10).sum())
    return {
        "day_count": int(daily.size),
        "positive_day_count": int((daily > 0).sum()),
        "min_daily_pnl_usdc": float(daily.min()) if daily.size else math.nan,
        "max_daily_pnl_usdc": float(daily.max()) if daily.size else math.nan,
        "avg_daily_pnl_usdc": float(daily.mean()) if daily.size else math.nan,
        "market_count": int(market.size),
        "positive_market_frac": float((market > 0).mean()) if market.size else math.nan,
        "max_market_pnl_usdc": float(market.max()) if market.size else math.nan,
        "min_market_pnl_usdc": float(market.min()) if market.size else math.nan,
        "top5_abs_market_pnl_share": top5_abs / total_abs_market
        if total_abs_market > 0
        else math.nan,
        "top10_abs_market_pnl_share": top10_abs / total_abs_market
        if total_abs_market > 0
        else math.nan,
        "total_pnl_usdc_check": total_pnl,
    }


def distribution_metrics_plus(group: pd.DataFrame) -> dict:
    out = distribution_metrics(group)
    daily = group.groupby("day_cst", observed=True)["pnl_usdc"].sum().sort_index()
    out["unique_day_count"] = out["day_count"]
    out["max_drawdown_usdc"] = max_drawdown(daily)
    out["daily_pnl_series"] = format_daily_pnl_series(daily)
    return out


def max_drawdown(daily: pd.Series) -> float:
    if daily.empty:
        return math.nan
    cumulative = daily.cumsum()
    running_peak = cumulative.cummax()
    drawdown = running_peak - cumulative
    return float(drawdown.max())


def format_daily_pnl_series(daily: pd.Series) -> str:
    if daily.empty:
        return ""
    parts = []
    for day, pnl in daily.items():
        date = pd.to_datetime(int(day), unit="D").strftime("%Y-%m-%d")
        parts.append(f"{date}:{float(pnl):.4f}")
    return ";".join(parts)


def safe_spearman(left: pd.Series, right: pd.Series) -> float:
    if len(left) < 3 or left.nunique(dropna=True) < 2 or right.nunique(dropna=True) < 2:
        return math.nan
    return float(left.corr(right, method="spearman"))


def write_manifest(
    output_dir: Path,
    args: argparse.Namespace,
    eth5: pd.DataFrame,
    eth5_gate: pd.DataFrame,
    eth15_all: pd.DataFrame,
    eth15_gate: pd.DataFrame,
) -> None:
    manifest = {
        "schema_version": 1,
        "objective": "ETH gate-passed shadow grids using market-calibrated executable edge",
        "min_train_days": args.min_train_days,
        "pm5m_snapshot_glob": args.pm5m_snapshot_glob,
        "pm15m_snapshot_glob": args.pm15m_snapshot_glob,
        "rows": {
            "eth5_all": int(len(eth5)),
            "eth5_gate_passed": int(len(eth5_gate)),
            "eth15_all": int(len(eth15_all)),
            "eth15_gate_passed": int(len(eth15_gate)),
        },
        "eth5m": {
            "q_gate": "walk-forward sigmoid(a + b*logit(market_mid)) by ETH 5m side, trained on gate-passed rows",
            "q_raw_cal": "walk-forward sigmoid(a + b*logit(p_raw)) by ETH 5m side, used only as optional veto",
            "q_bucket_cal": (
                "walk-forward logistic regression using logit(market_mid), side, tau_bucket, "
                "and avg_fill_bucket; trained with market-balanced sample weights"
            ),
            "thresholds_c": [2, 4, 6, 8],
            "max_orders_per_market": MAX_ORDERS_5M,
            "spread_caps_c": [3, 5, 8],
            "tau_buckets": ["30-90s", "90-180s", "180s+"],
            "avg_fill_buckets": ["<55c", "55-65c", "65-75c", ">75c"],
        },
        "eth15m": {
            "q_gate": "walk-forward sigmoid(a + b*logit(market_mid)) by ETH 15m side and derived entry_phase",
            "entry_phase": {
                "first_entry": "first locked-side gate-passed row per condition",
                "locked_add": "later locked-side gate-passed rows",
                "cheap_reentry": "gate-passed rows not matching locked_side; not a native fixed-price execution reason",
            },
            "thresholds_c": [4, 6, 8, 10],
            "max_add": MAX_ADDS_15M,
            "drift": "future same-side market_mid minus current market_mid at 1s/3s/5s, forward asof with 2s tolerance",
        },
        "outputs": [
            "eth5m_shadow_grid_gate_qmarket.csv",
            "eth15m_phase_shadow_grid.csv",
            "eth5m_qedge_fill_bucket.csv",
            "eth15m_current_control_order_index.csv",
            "eth5m_low_fill_forensic.csv",
            "eth5m_bucket_aware_qcal_netedge.csv",
            "eth15m_side_order_index_add_quality.csv",
            "manifest.json",
        ],
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
