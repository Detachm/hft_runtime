#!/usr/bin/env python3
import argparse
import glob
import json
import math
from pathlib import Path
from statistics import NormalDist

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss


USECOLS = [
    "symbol",
    "horizon_seconds",
    "condition_id",
    "asset_id",
    "outcome",
    "ts_ns",
    "window_start_ts_ns",
    "window_end_ts_ns",
    "seconds_to_end",
    "anchor_price_micros",
    "current_price_micros",
    "sigma_micros",
    "sigma_60_micros",
    "sigma_180_micros",
    "p_model_micros",
    "momentum_60s_bps",
    "side_momentum_60s_bps",
    "btc_lead_momentum_60s_bps",
    "best_bid_price_micros",
    "best_ask_price_micros",
    "market_mid_micros",
    "spread_micros",
    "target_fillable",
    "target_avg_fill_price_micros",
    "target_worst_price_micros",
    "target_cash_micros",
    "target_shares_micros",
    "avg_fill_1u_micros",
    "avg_fill_5u_micros",
    "avg_fill_10u_micros",
    "fee_micros_per_share",
    "winner",
    "realized_pnl_per_share_micros",
    "strategy_gate_passed",
    "locked_side",
    "locked_side_match",
    "current_priced_side_edge_micros",
]

NUMERIC_COLS = [
    "horizon_seconds",
    "ts_ns",
    "window_start_ts_ns",
    "window_end_ts_ns",
    "seconds_to_end",
    "anchor_price_micros",
    "current_price_micros",
    "sigma_micros",
    "sigma_60_micros",
    "sigma_180_micros",
    "p_model_micros",
    "momentum_60s_bps",
    "side_momentum_60s_bps",
    "btc_lead_momentum_60s_bps",
    "best_bid_price_micros",
    "best_ask_price_micros",
    "market_mid_micros",
    "spread_micros",
    "target_avg_fill_price_micros",
    "target_worst_price_micros",
    "target_cash_micros",
    "target_shares_micros",
    "avg_fill_1u_micros",
    "avg_fill_5u_micros",
    "avg_fill_10u_micros",
    "fee_micros_per_share",
    "realized_pnl_per_share_micros",
    "current_priced_side_edge_micros",
]

DAY_NS = 86_400 * 1_000_000_000
CST_OFFSET_NS = 8 * 3_600 * 1_000_000_000
NORM = NormalDist()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pm5m-snapshot-glob", action="append", required=True)
    parser.add_argument("--pm15m-snapshot-glob", action="append", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-train-days", type=int, default=3)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = prepare_base(
        load_snapshots(
            expand_globs(args.pm5m_snapshot_glob)
            + expand_globs(args.pm15m_snapshot_glob)
        )
    )
    df = add_entry_phase(df)

    btc = df[df["coin"] == "BTC"].copy()
    sol = df[df["coin"] == "SOL"].copy()

    btc_residual = build_btc_residual_buckets(btc)
    btc_tau_vol = build_btc_tau_distance_vol_grid(btc)
    btc_sigma = build_btc_sigma_model_comparison(btc)
    btc_oos, btc_q = build_btc_market_residual_logistic_oos(
        btc, min_train_days=args.min_train_days
    )
    btc_net_edge = build_btc_calibrated_net_edge_buckets(btc_q)
    btc_yes_residual_grid = build_btc_yes_residual_shadow_grid(btc_q, btc)

    sol_fill = build_sol_fill_bucket_ev(sol)
    sol_toxicity = build_sol_post_fill_toxicity(sol)
    sol_lead = build_sol_yes_btc_lead_buckets(sol)
    sol_15m = build_sol_15m_phase_side_quality(sol)
    sol_relaxation = build_sol_relaxation_ladder(sol)

    btc_residual.to_csv(output_dir / "btc_residual_buckets.csv", index=False)
    btc_tau_vol.to_csv(output_dir / "btc_tau_distance_vol_grid.csv", index=False)
    btc_sigma.to_csv(output_dir / "btc_sigma_model_comparison.csv", index=False)
    btc_oos.to_csv(output_dir / "btc_market_residual_logistic_oos.csv", index=False)
    btc_net_edge.to_csv(output_dir / "btc_calibrated_net_edge_buckets.csv", index=False)
    btc_yes_residual_grid.to_csv(output_dir / "btc_yes_residual_shadow_grid.csv", index=False)

    sol_fill.to_csv(output_dir / "sol_fill_bucket_ev.csv", index=False)
    sol_toxicity.to_csv(output_dir / "sol_post_fill_toxicity.csv", index=False)
    sol_lead.to_csv(output_dir / "sol_yes_btc_lead_buckets.csv", index=False)
    sol_15m.to_csv(output_dir / "sol_15m_phase_side_quality.csv", index=False)
    sol_relaxation.to_csv(output_dir / "sol_relaxation_ladder.csv", index=False)

    write_manifest(output_dir, args, btc, sol)


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
    for col in NUMERIC_COLS:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work["symbol"] = work["symbol"].str.upper()
    work["coin"] = work["symbol"].str.extract(r"^(BTC|ETH|SOL)", expand=False).fillna(work["symbol"])
    work["outcome"] = work["outcome"].str.upper()
    work["horizon_seconds"] = work["horizon_seconds"].astype(int)
    for col in ["target_fillable", "winner", "strategy_gate_passed", "locked_side_match"]:
        work[col] = parse_bool(work[col])

    work["p_raw"] = clip_prob(work["p_model_micros"] / 1_000_000.0)
    work["market_mid"] = clip_prob(work["market_mid_micros"] / 1_000_000.0)
    work["residual_logit_raw_minus_market"] = logit(work["p_raw"]) - logit(work["market_mid"])
    work["best_bid"] = work["best_bid_price_micros"] / 1_000_000.0
    work["best_ask"] = work["best_ask_price_micros"] / 1_000_000.0
    work["spread"] = work["spread_micros"] / 1_000_000.0
    work["avg_fill"] = work["target_avg_fill_price_micros"] / 1_000_000.0
    work["worst_fill"] = work["target_worst_price_micros"] / 1_000_000.0
    work["target_cash"] = work["target_cash_micros"] / 1_000_000.0
    work["shares"] = work["target_shares_micros"] / 1_000_000.0
    work["avg_fill_1u"] = work["avg_fill_1u_micros"] / 1_000_000.0
    work["avg_fill_5u"] = work["avg_fill_5u_micros"] / 1_000_000.0
    work["avg_fill_10u"] = work["avg_fill_10u_micros"] / 1_000_000.0
    work["fee"] = work["fee_micros_per_share"] / 1_000_000.0
    work["pnl_share"] = work["realized_pnl_per_share_micros"] / 1_000_000.0
    work["pnl_usdc"] = work["pnl_share"] * work["shares"]

    work["anchor_price"] = work["anchor_price_micros"] / 1_000_000.0
    work["current_price"] = work["current_price_micros"] / 1_000_000.0
    work["distance_log"] = np.log(work["current_price"] / work["anchor_price"])
    work["distance_bps"] = work["distance_log"] * 10_000.0
    work["side_sign"] = np.where(work["outcome"] == "YES", 1.0, -1.0)
    work["side_distance_bps"] = work["side_sign"] * work["distance_bps"]
    work["abs_distance_bps"] = work["distance_bps"].abs()

    for src, dst in [
        ("sigma_micros", "sigma"),
        ("sigma_60_micros", "sigma_60"),
        ("sigma_180_micros", "sigma_180"),
    ]:
        work[dst] = work[src] / 1_000_000.0
    work["sigma_max_60_180"] = work[["sigma_60", "sigma_180"]].max(axis=1)
    sigma_floor = work.groupby(["coin", "horizon_seconds"], observed=True)["sigma_180"].transform(
        lambda s: s.quantile(0.25)
    )
    work["sigma_eff_floor"] = np.maximum(work["sigma_max_60_180"], sigma_floor)
    work["vol_ratio_60_180"] = safe_divide(work["sigma_60"], work["sigma_180"])
    work["z_60_side"] = safe_divide(
        work["side_sign"] * work["distance_log"],
        work["sigma_60"] * np.sqrt(work["seconds_to_end"].clip(lower=1)),
    )
    work["z_180_side"] = safe_divide(
        work["side_sign"] * work["distance_log"],
        work["sigma_180"] * np.sqrt(work["seconds_to_end"].clip(lower=1)),
    )

    work["day_cst"] = ((work["ts_ns"] + CST_OFFSET_NS) // DAY_NS).astype(np.int64)
    work["tau_bucket"] = pd.cut(
        work["seconds_to_end"],
        bins=[30, 90, 180, 360, 720, np.inf],
        labels=["30-90s", "90-180s", "180-360s", "360-720s", "720s+"],
        right=False,
    )
    work["avg_fill_bucket"] = pd.cut(
        work["avg_fill"],
        bins=[-np.inf, 0.55, 0.65, 0.75, np.inf],
        labels=["<55c", "55-65c", "65-75c", ">75c"],
        right=False,
    )
    work["spread_bucket"] = pd.cut(
        work["spread"],
        bins=[-np.inf, 0.01, 0.03, 0.05, np.inf],
        labels=["<=1c", "1-3c", "3-5c", ">5c"],
        right=False,
    )
    work["side_distance_bucket"] = pd.cut(
        work["side_distance_bps"],
        bins=[-np.inf, -20, -10, -5, 0, 5, 10, 20, np.inf],
        labels=["<-20", "-20--10", "-10--5", "-5-0", "0-5", "5-10", "10-20", ">20"],
        right=False,
    )
    work["abs_distance_bucket"] = pd.cut(
        work["abs_distance_bps"],
        bins=[-np.inf, 5, 10, 20, 40, np.inf],
        labels=["<5", "5-10", "10-20", "20-40", ">40"],
        right=False,
    )
    work["vol_ratio_bucket"] = pd.cut(
        work["vol_ratio_60_180"],
        bins=[-np.inf, 0.75, 1.0, 1.25, 1.5, np.inf],
        labels=["<0.75", "0.75-1", "1-1.25", "1.25-1.5", ">1.5"],
        right=False,
    )
    work["btc_lead_bucket"] = pd.cut(
        work["btc_lead_momentum_60s_bps"],
        bins=[-np.inf, -10, -3, 0, 3, 10, np.inf],
        labels=["<-10", "-10--3", "-3-0", "0-3", "3-10", ">10"],
        right=False,
    )
    work["impact_5u"] = work["avg_fill_5u"] - work["avg_fill_1u"]
    work["depth_bucket"] = pd.cut(
        work["impact_5u"],
        bins=[-np.inf, 0.005, 0.01, 0.03, np.inf],
        labels=["<=0.5c", "0.5-1c", "1-3c", ">3c"],
        right=False,
    )
    return work


def parse_bool(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def clip_prob(values):
    values = np.nan_to_num(np.asarray(values, dtype=float), nan=0.5, posinf=1 - 1e-6, neginf=1e-6)
    return np.clip(values, 1e-6, 1 - 1e-6)


def logit(values) -> np.ndarray:
    values = clip_prob(values)
    return np.log(values / (1 - values))


def safe_divide(numerator, denominator):
    den = np.asarray(denominator, dtype=float)
    num = np.asarray(numerator, dtype=float)
    out = np.full_like(num, np.nan, dtype=float)
    mask = np.isfinite(num) & np.isfinite(den) & (np.abs(den) > 1e-12)
    out[mask] = num[mask] / den[mask]
    return out


def add_entry_phase(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["entry_phase"] = "all"
    work["order_index"] = "all"
    is_15m = work["horizon_seconds"] == 900
    if not is_15m.any():
        return work
    fifteen = work[is_15m & work["strategy_gate_passed"]].copy()
    if fifteen.empty:
        return work
    fifteen["entry_phase"] = "cheap_reentry"
    first_locked_ts = (
        fifteen[fifteen["locked_side_match"]].groupby("condition_id", observed=True)["ts_ns"].min()
    )
    mapped = fifteen["condition_id"].map(first_locked_ts)
    has_first = mapped.notna()
    is_first = fifteen["locked_side_match"] & has_first & (fifteen["ts_ns"] == mapped)
    is_add = fifteen["locked_side_match"] & has_first & (fifteen["ts_ns"] > mapped)
    fifteen.loc[is_first, "entry_phase"] = "first_entry"
    fifteen.loc[is_add, "entry_phase"] = "locked_add"
    fifteen = assign_order_index(fifteen)
    work.loc[fifteen.index, "entry_phase"] = fifteen["entry_phase"]
    work.loc[fifteen.index, "order_index"] = fifteen["order_index"]
    return work


def assign_order_index(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    work["order_index"] = "other"
    eligible = work[work["entry_phase"].isin(["first_entry", "locked_add"])].sort_values(
        ["condition_id", "ts_ns", "entry_phase"]
    )
    for _, group in eligible.groupby("condition_id", observed=True):
        first = group[group["entry_phase"] == "first_entry"].head(1)
        if first.empty:
            continue
        work.loc[first.index, "order_index"] = "first"
        adds = group[
            (group["entry_phase"] == "locked_add")
            & (group["outcome"] == first["outcome"].iloc[0])
        ].head(3)
        for idx, row_index in enumerate(adds.index, start=1):
            work.loc[row_index, "order_index"] = f"add{idx}"
    return work


def build_btc_residual_buckets(btc: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for scope, data in [
        ("all_fillable", btc[btc["target_fillable"]]),
        ("gate_passed_fillable", btc[btc["target_fillable"] & btc["strategy_gate_passed"]]),
    ]:
        work = data.copy()
        work["residual_bucket"] = pd.cut(
            work["residual_logit_raw_minus_market"],
            bins=[-np.inf, -2, -1, -0.5, -0.25, 0, 0.25, 0.5, 1, 2, np.inf],
            labels=["<-2", "-2--1", "-1--0.5", "-0.5--0.25", "-0.25-0", "0-0.25", "0.25-0.5", "0.5-1", "1-2", ">2"],
            right=False,
        )
        frames.append(
            summarize_groups(
                work,
                {"scope": scope},
                ["horizon_seconds", "outcome", "residual_bucket"],
            )
        )
    return concat_rows(frames).sort_values(["scope", "horizon_seconds", "outcome", "residual_bucket"])


def build_btc_tau_distance_vol_grid(btc: pd.DataFrame) -> pd.DataFrame:
    data = btc[btc["target_fillable"]].copy()
    return summarize_groups(
        data,
        {"scope": "all_fillable"},
        [
            "horizon_seconds",
            "outcome",
            "tau_bucket",
            "side_distance_bucket",
            "vol_ratio_bucket",
        ],
    ).sort_values(
        ["horizon_seconds", "outcome", "tau_bucket", "side_distance_bucket", "vol_ratio_bucket"]
    )


def build_btc_sigma_model_comparison(btc: pd.DataFrame) -> pd.DataFrame:
    data = btc.copy()
    models = {
        "p_raw_recorded": pd.Series(data["p_raw"].to_numpy(), index=data.index),
        "market_mid": pd.Series(data["market_mid"].to_numpy(), index=data.index),
        "sigma_60": pd.Series(prob_from_sigma(data, "sigma_60"), index=data.index),
        "sigma_180": pd.Series(prob_from_sigma(data, "sigma_180"), index=data.index),
        "sigma_max_60_180": pd.Series(prob_from_sigma(data, "sigma_max_60_180"), index=data.index),
        "sigma_eff_floor": pd.Series(prob_from_sigma(data, "sigma_eff_floor"), index=data.index),
    }
    rows = []
    for keys, group_idx in data.groupby(["horizon_seconds", "outcome"], sort=True).groups.items():
        horizon, outcome = keys
        y = data.loc[group_idx, "winner"].astype(int).to_numpy()
        for model_name, probs in models.items():
            prob = clip_prob(probs.loc[group_idx].to_numpy())
            rows.append(
                {
                    "horizon_seconds": horizon,
                    "outcome": outcome,
                    "model": model_name,
                    "sample_count": int(len(prob)),
                    "unique_market_count": int(data.loc[group_idx, "condition_id"].nunique()),
                    "brier": brier_score_loss(y, prob),
                    "log_loss": log_loss(y, prob, labels=[0, 1]),
                    "avg_prob": float(np.mean(prob)),
                    "realized_win_rate": float(np.mean(y)),
                    "avg_pnl_share": float(data.loc[group_idx, "pnl_share"].mean()),
                }
            )
    return pd.DataFrame(rows).sort_values(["horizon_seconds", "outcome", "log_loss"])


def prob_from_sigma(df: pd.DataFrame, sigma_col: str) -> np.ndarray:
    sigma = df[sigma_col].to_numpy(dtype=float)
    tau = df["seconds_to_end"].clip(lower=1).to_numpy(dtype=float)
    z_yes = safe_divide(df["distance_log"].to_numpy(dtype=float), sigma * np.sqrt(tau))
    p_yes = normal_cdf(z_yes)
    return np.where(df["outcome"].to_numpy() == "YES", p_yes, 1.0 - p_yes)


def normal_cdf(values: np.ndarray) -> np.ndarray:
    vals = np.asarray(values, dtype=float)
    out = np.full_like(vals, np.nan, dtype=float)
    mask = np.isfinite(vals)
    out[mask] = [NORM.cdf(float(v)) for v in vals[mask]]
    return clip_prob(np.nan_to_num(out, nan=0.5))


def build_btc_market_residual_logistic_oos(
    btc: pd.DataFrame, min_train_days: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    data = btc[btc["target_fillable"]].copy()
    data["q_btc_features"] = np.nan
    data["q_market_only"] = np.nan
    rows = []
    for keys, group in data.groupby(["horizon_seconds", "outcome"], sort=True):
        horizon, outcome = keys
        group = group.sort_values("ts_ns")
        for day in sorted(group["day_cst"].unique()):
            train = group[group["day_cst"] < day]
            test = group[group["day_cst"] == day]
            if train["day_cst"].nunique() < min_train_days or test.empty:
                continue
            y_train = train["winner"].astype(int).to_numpy()
            y_test = test["winner"].astype(int).to_numpy()
            if len(np.unique(y_train)) < 2 or len(np.unique(y_test)) < 2:
                continue

            weights = market_balanced_weights(train)
            model_specs = [
                ("market_only", btc_features(train, ["market"]), btc_features(test, ["market"])),
                (
                    "market_plus_residual",
                    btc_features(train, ["market", "residual"]),
                    btc_features(test, ["market", "residual"]),
                ),
                (
                    "market_plus_btc_features",
                    btc_features(train, ["market", "residual", "state"]),
                    btc_features(test, ["market", "residual", "state"]),
                ),
            ]
            for model_name, x_train, x_test in model_specs:
                clf = LogisticRegression(C=1.0, solver="lbfgs", max_iter=500)
                clf.fit(x_train, y_train, sample_weight=weights)
                prob = clip_prob(clf.predict_proba(x_test)[:, 1])
                rows.append(
                    {
                        "horizon_seconds": horizon,
                        "outcome": outcome,
                        "test_day_cst_ord": int(day),
                        "model": model_name,
                        "sample_count": int(len(test)),
                        "unique_market_count": int(test["condition_id"].nunique()),
                        "brier": brier_score_loss(y_test, prob),
                        "log_loss": log_loss(y_test, prob, labels=[0, 1]),
                        "avg_prob": float(np.mean(prob)),
                        "realized_win_rate": float(np.mean(y_test)),
                    }
                )
                if model_name == "market_only":
                    data.loc[test.index, "q_market_only"] = prob
                if model_name == "market_plus_btc_features":
                    data.loc[test.index, "q_btc_features"] = prob
    folds = pd.DataFrame(rows)
    if folds.empty:
        return folds, data
    summary = (
        folds.groupby(["horizon_seconds", "outcome", "model"], observed=True)
        .apply(weighted_oos_summary, include_groups=False)
        .reset_index()
    )
    return summary.sort_values(["horizon_seconds", "outcome", "log_loss"]), data


def btc_features(df: pd.DataFrame, groups: list[str]) -> np.ndarray:
    cols = []
    if "market" in groups:
        cols.append(logit(df["market_mid"]))
    if "residual" in groups:
        cols.append(df["residual_logit_raw_minus_market"].to_numpy(dtype=float))
    if "state" in groups:
        cols.extend(
            [
                df["side_distance_bps"].to_numpy(dtype=float) / 20.0,
                df["abs_distance_bps"].to_numpy(dtype=float) / 20.0,
                np.log1p(df["seconds_to_end"].to_numpy(dtype=float)) / 7.0,
                np.nan_to_num(df["vol_ratio_60_180"].to_numpy(dtype=float), nan=1.0),
                df["momentum_60s_bps"].to_numpy(dtype=float) / 20.0,
                df["side_momentum_60s_bps"].to_numpy(dtype=float) / 20.0,
                df["spread"].to_numpy(dtype=float) / 0.05,
            ]
        )
    return np.nan_to_num(np.column_stack(cols), nan=0.0, posinf=0.0, neginf=0.0)


def market_balanced_weights(df: pd.DataFrame) -> np.ndarray:
    counts = df.groupby("condition_id", observed=True)["condition_id"].transform("size")
    return (1.0 / counts.astype(float)).to_numpy()


def weighted_oos_summary(group: pd.DataFrame) -> pd.Series:
    weights = group["sample_count"].to_numpy(dtype=float)
    total = weights.sum() if weights.sum() > 0 else 1.0
    return pd.Series(
        {
            "sample_count": int(group["sample_count"].sum()),
            "unique_market_count_sum": int(group["unique_market_count"].sum()),
            "fold_count": int(len(group)),
            "brier": float(np.sum(group["brier"] * weights) / total),
            "log_loss": float(np.sum(group["log_loss"] * weights) / total),
            "avg_prob": float(np.sum(group["avg_prob"] * weights) / total),
            "realized_win_rate": float(np.sum(group["realized_win_rate"] * weights) / total),
        }
    )


def build_btc_calibrated_net_edge_buckets(btc_q: pd.DataFrame) -> pd.DataFrame:
    data = btc_q[btc_q["q_btc_features"].notna() & btc_q["target_fillable"]].copy()
    if data.empty:
        return pd.DataFrame()
    data["net_edge_btc_features"] = data["q_btc_features"] - data["avg_fill"] - data["fee"]
    data["net_edge_bucket"] = pd.cut(
        data["net_edge_btc_features"],
        bins=[-np.inf, 0, 0.02, 0.04, 0.06, 0.08, 0.12, np.inf],
        labels=["<=0c", "0-2c", "2-4c", "4-6c", "6-8c", "8-12c", ">12c"],
        right=False,
    )
    return summarize_groups(
        data,
        {"model": "market_plus_btc_features"},
        ["horizon_seconds", "outcome", "net_edge_bucket"],
    ).sort_values(["horizon_seconds", "outcome", "net_edge_bucket"])


def build_btc_yes_residual_shadow_grid(
    btc_q: pd.DataFrame, btc_all: pd.DataFrame
) -> pd.DataFrame:
    data = btc_q[
        btc_q["target_fillable"]
        & btc_q["q_btc_features"].notna()
        & (btc_q["outcome"] == "YES")
        & btc_q["tau_bucket"].notna()
        & btc_q["avg_fill_bucket"].notna()
        & btc_q["spread_bucket"].notna()
    ].copy()
    if data.empty:
        return pd.DataFrame()
    data["model_residual"] = logit(data["q_btc_features"]) - logit(data["market_mid"])
    data["residual_bucket"] = residual_bucket(data["model_residual"])
    data = add_mid_drifts(data, btc_all)

    rows = []
    group_cols = [
        "horizon_seconds",
        "outcome",
        "residual_bucket",
        "tau_bucket",
        "avg_fill_bucket",
        "spread_bucket",
    ]
    rows.append(
        summarize_groups(
            data,
            {
                "scope": "all_oos_yes_fillable",
                "residual_threshold": math.nan,
                "max_orders_per_market": math.nan,
            },
            group_cols,
            drift_cols=["mid_drift_1s", "mid_drift_3s", "mid_drift_5s", "mid_drift_10s"],
        )
    )

    for threshold in [0.5, 1.0, 1.5, 2.0]:
        eligible = data[data["model_residual"] >= threshold].copy()
        selected = cap_orders_per_market(eligible, max_orders=1)
        rows.append(
            summarize_groups(
                selected,
                {
                    "scope": "threshold_max1",
                    "residual_threshold": threshold,
                    "max_orders_per_market": 1,
                },
                group_cols,
                drift_cols=["mid_drift_1s", "mid_drift_3s", "mid_drift_5s", "mid_drift_10s"],
            )
        )
    out = concat_rows(rows)
    if out.empty:
        return out
    return out.sort_values(
        [
            "scope",
            "residual_threshold",
            "horizon_seconds",
            "outcome",
            "residual_bucket",
            "tau_bucket",
            "avg_fill_bucket",
            "spread_bucket",
        ],
        na_position="first",
    )


def residual_bucket(series: pd.Series) -> pd.Series:
    return pd.cut(
        series,
        bins=[-np.inf, -2, -1, -0.5, 0, 0.5, 1, 1.5, 2, np.inf],
        labels=["<-2", "-2--1", "-1--0.5", "-0.5-0", "0-0.5", "0.5-1", "1-1.5", "1.5-2", ">2"],
        right=False,
    )


def cap_orders_per_market(df: pd.DataFrame, max_orders: int) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    work = df.sort_values(["condition_id", "ts_ns", "outcome"]).copy()
    work["order_rank_in_market"] = work.groupby("condition_id", observed=True).cumcount() + 1
    return work[work["order_rank_in_market"] <= max_orders].copy()


def build_sol_fill_bucket_ev(sol: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for scope, data in [
        ("all_fillable", sol[sol["target_fillable"]]),
        ("gate_passed_fillable", sol[sol["target_fillable"] & sol["strategy_gate_passed"]]),
        (
            "yes_btc_lead_positive",
            sol[
                sol["target_fillable"]
                & (sol["outcome"] == "YES")
                & (sol["btc_lead_momentum_60s_bps"] > 0)
            ],
        ),
    ]:
        with_drift = add_mid_drifts(data, sol)
        frames.append(
            summarize_groups(
                with_drift,
                {"scope": scope},
                [
                    "horizon_seconds",
                    "outcome",
                    "avg_fill_bucket",
                    "spread_bucket",
                    "tau_bucket",
                ],
                drift_cols=["mid_drift_1s", "mid_drift_3s", "mid_drift_5s", "mid_drift_10s"],
            )
        )
    return concat_rows(frames).sort_values(
        ["scope", "horizon_seconds", "outcome", "avg_fill_bucket", "spread_bucket", "tau_bucket"]
    )


def build_sol_post_fill_toxicity(sol: pd.DataFrame) -> pd.DataFrame:
    data = sol[sol["target_fillable"] & sol["strategy_gate_passed"]].copy()
    data = add_mid_drifts(data, sol)
    return summarize_groups(
        data,
        {"scope": "gate_passed_fillable"},
        ["horizon_seconds", "outcome", "avg_fill_bucket", "tau_bucket"],
        drift_cols=["mid_drift_1s", "mid_drift_3s", "mid_drift_5s", "mid_drift_10s"],
    ).sort_values(["horizon_seconds", "outcome", "avg_fill_bucket", "tau_bucket"])


def build_sol_yes_btc_lead_buckets(sol: pd.DataFrame) -> pd.DataFrame:
    data = sol[sol["target_fillable"] & (sol["outcome"] == "YES")].copy()
    data = add_mid_drifts(data, sol)
    return summarize_groups(
        data,
        {"scope": "yes_fillable"},
        ["horizon_seconds", "btc_lead_bucket", "avg_fill_bucket", "tau_bucket"],
        drift_cols=["mid_drift_1s", "mid_drift_3s", "mid_drift_5s", "mid_drift_10s"],
    ).sort_values(["horizon_seconds", "btc_lead_bucket", "avg_fill_bucket", "tau_bucket"])


def build_sol_15m_phase_side_quality(sol: pd.DataFrame) -> pd.DataFrame:
    data = sol[
        (sol["horizon_seconds"] == 900)
        & sol["target_fillable"]
        & sol["strategy_gate_passed"]
    ].copy()
    data = add_mid_drifts(data, sol[sol["horizon_seconds"] == 900])
    frames = [
        summarize_groups(
            data,
            {"scope": "gate_passed_fillable"},
            ["outcome", "entry_phase", "order_index"],
            drift_cols=["mid_drift_1s", "mid_drift_3s", "mid_drift_5s", "mid_drift_10s"],
        ),
        summarize_groups(
            data,
            {"scope": "gate_passed_fillable_tau_fill"},
            ["outcome", "entry_phase", "order_index", "tau_bucket", "avg_fill_bucket"],
            drift_cols=["mid_drift_1s", "mid_drift_3s", "mid_drift_5s", "mid_drift_10s"],
        ),
    ]
    return concat_rows(frames).sort_values(
        ["scope", "outcome", "entry_phase", "order_index", "tau_bucket", "avg_fill_bucket"],
        na_position="first",
    )


def build_sol_relaxation_ladder(sol: pd.DataFrame) -> pd.DataFrame:
    base = sol[sol["target_fillable"]].copy()
    base = add_mid_drifts(base, sol)
    rows = []

    rows.append(
        summarize_sol_ladder_rule(
            base[
                (base["horizon_seconds"] == 300)
                & (base["outcome"] == "YES")
                & base["strategy_gate_passed"]
                & (base["avg_fill"] < 0.55)
                & (base["seconds_to_end"] >= 90)
            ],
            tier="A",
            rule_name="sol5_yes_gate_fill_lt55_tau_ge90",
            relaxed_dimension="none",
        )
    )
    rows.append(
        summarize_sol_ladder_rule(
            base[
                (base["horizon_seconds"] == 300)
                & (base["outcome"] == "YES")
                & base["strategy_gate_passed"]
                & (base["avg_fill"] >= 0.55)
                & (base["avg_fill"] < 0.65)
                & (base["seconds_to_end"] >= 90)
            ],
            tier="B",
            rule_name="sol5_yes_gate_fill_55_65_tau_ge90",
            relaxed_dimension="fill",
        )
    )
    for threshold in [0.0, 3.0, 10.0]:
        rows.append(
            summarize_sol_ladder_rule(
                base[
                    (base["horizon_seconds"] == 300)
                    & (base["outcome"] == "YES")
                    & (base["avg_fill"] < 0.55)
                    & (base["seconds_to_end"] >= 90)
                    & (base["btc_lead_momentum_60s_bps"] > threshold)
                ],
                tier="B",
                rule_name=f"sol5_yes_fill_lt55_tau_ge90_btc_lead_gt_{int(threshold)}",
                relaxed_dimension="btc_lead",
            )
        )
    rows.append(
        summarize_sol_ladder_rule(
            base[
                (base["horizon_seconds"] == 300)
                & (base["outcome"] == "YES")
                & base["strategy_gate_passed"]
                & (base["avg_fill"] < 0.55)
                & (base["seconds_to_end"] >= 30)
                & (base["seconds_to_end"] < 90)
            ],
            tier="B",
            rule_name="sol5_yes_gate_fill_lt55_tau_30_90",
            relaxed_dimension="tau_negative_control",
        )
    )

    rows.append(
        summarize_sol_ladder_rule(
            base[
                (base["horizon_seconds"] == 900)
                & (base["outcome"] == "YES")
                & base["strategy_gate_passed"]
                & (base["avg_fill"] < 0.55)
                & (base["btc_lead_momentum_60s_bps"] > 10)
            ],
            tier="A",
            rule_name="sol15_yes_gate_fill_lt55_btc_lead_gt10",
            relaxed_dimension="none",
        )
    )
    for threshold in [0.0, 3.0]:
        rows.append(
            summarize_sol_ladder_rule(
                base[
                    (base["horizon_seconds"] == 900)
                    & (base["outcome"] == "YES")
                    & base["strategy_gate_passed"]
                    & (base["avg_fill"] < 0.55)
                    & (base["btc_lead_momentum_60s_bps"] > threshold)
                ],
                tier="B",
                rule_name=f"sol15_yes_gate_fill_lt55_btc_lead_gt_{int(threshold)}",
                relaxed_dimension="btc_lead",
            )
        )
    rows.append(
        summarize_sol_ladder_rule(
            base[
                (base["horizon_seconds"] == 900)
                & (base["outcome"] == "YES")
                & base["strategy_gate_passed"]
                & (base["avg_fill"] >= 0.55)
                & (base["avg_fill"] < 0.65)
            ],
            tier="B",
            rule_name="sol15_yes_gate_fill_55_65",
            relaxed_dimension="fill",
        )
    )

    rows.append(
        summarize_sol_ladder_rule(
            base[base["outcome"] == "YES"],
            tier="C",
            rule_name="sol_yes_all_fillable_diagnostic",
            relaxed_dimension="full_yes_diagnostic",
        )
    )
    rows.append(
        summarize_sol_ladder_rule(
            base[base["outcome"] == "NO"],
            tier="C",
            rule_name="sol_no_all_fillable_baseline",
            relaxed_dimension="no_baseline",
        )
    )

    out = concat_rows(rows)
    if out.empty:
        return out
    return out.sort_values(
        [
            "tier",
            "rule_name",
            "horizon_seconds",
            "outcome",
            "tau_bucket",
            "avg_fill_bucket",
            "btc_lead_bucket",
            "spread_bucket",
            "depth_bucket",
            "entry_phase",
            "order_index",
        ],
        na_position="first",
    )


def summarize_sol_ladder_rule(
    data: pd.DataFrame, tier: str, rule_name: str, relaxed_dimension: str
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame()
    return summarize_groups(
        data,
        {
            "tier": tier,
            "rule_name": rule_name,
            "relaxed_dimension": relaxed_dimension,
        },
        [
            "horizon_seconds",
            "outcome",
            "tau_bucket",
            "avg_fill_bucket",
            "btc_lead_bucket",
            "spread_bucket",
            "depth_bucket",
            "entry_phase",
            "order_index",
        ],
        drift_cols=["mid_drift_1s", "mid_drift_3s", "mid_drift_5s", "mid_drift_10s"],
    )


def add_mid_drifts(selected: pd.DataFrame, all_rows: pd.DataFrame) -> pd.DataFrame:
    out = selected.copy()
    if out.empty:
        return out
    lookup = all_rows[
        all_rows["market_mid"].notna()
    ][["condition_id", "outcome", "ts_ns", "market_mid"]].sort_values(
        ["condition_id", "outcome", "ts_ns"]
    )
    left_base = out[["condition_id", "outcome", "ts_ns", "market_mid"]].copy()
    left_base["ts_ns"] = left_base["ts_ns"].astype(np.int64)
    lookup["ts_ns"] = lookup["ts_ns"].astype(np.int64)
    for seconds in [1, 3, 5, 10]:
        left = left_base.copy()
        left["target_ts_ns"] = (left["ts_ns"] + seconds * 1_000_000_000).astype(np.int64)
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


def summarize_groups(
    data: pd.DataFrame,
    config: dict,
    group_cols: list[str],
    drift_cols: list[str] | None = None,
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame()
    drift_cols = drift_cols or []
    rows = []
    for keys, group in data.groupby(group_cols, observed=True, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(config)
        row.update(dict(zip(group_cols, keys)))
        row.update(performance_metrics(group, drift_cols))
        rows.append(row)
    return pd.DataFrame(rows)


def performance_metrics(group: pd.DataFrame, drift_cols: list[str]) -> dict:
    daily = group.groupby("day_cst", observed=True)["pnl_usdc"].sum()
    market = group.groupby("condition_id", observed=True)["pnl_usdc"].sum()
    total_abs_market = float(market.abs().sum())
    out = {
        "order_count": int(len(group)),
        "unique_market_count": int(group["condition_id"].nunique()),
        "unique_day_count": int(group["day_cst"].nunique()),
        "win_rate": float(group["winner"].mean()),
        "avg_fill": float(group["avg_fill"].mean()),
        "avg_fee": float(group["fee"].mean()),
        "realized_ev_win_minus_fill_fee": float(
            group["winner"].mean() - group["avg_fill"].mean() - group["fee"].mean()
        ),
        "total_pnl_usdc": float(group["pnl_usdc"].sum()),
        "avg_pnl_usdc_per_order": float(group["pnl_usdc"].mean()),
        "avg_pnl_per_share": float(group["pnl_share"].mean()),
        "avg_market_mid": float(group["market_mid"].mean()),
        "avg_spread": float(group["spread"].mean()),
        "positive_day_count": int((daily > 0).sum()),
        "min_daily_pnl_usdc": float(daily.min()) if not daily.empty else math.nan,
        "max_daily_pnl_usdc": float(daily.max()) if not daily.empty else math.nan,
        "max_drawdown_usdc": max_drawdown(daily),
        "top5_abs_market_pnl_share": float(market.abs().sort_values(ascending=False).head(5).sum() / total_abs_market)
        if total_abs_market > 0
        else math.nan,
    }
    for col in drift_cols:
        out[f"avg_{col}"] = float(group[col].mean(skipna=True))
    if "q_btc_features" in group:
        out["avg_q_btc_features"] = float(group["q_btc_features"].mean(skipna=True))
        if "net_edge_btc_features" in group:
            out["avg_net_edge_btc_features"] = float(
                group["net_edge_btc_features"].mean(skipna=True)
            )
    return out


def max_drawdown(daily: pd.Series) -> float:
    if daily.empty:
        return math.nan
    cumulative = daily.sort_index().cumsum()
    running_peak = cumulative.cummax()
    return float((running_peak - cumulative).max())


def concat_rows(frames: list[pd.DataFrame]) -> pd.DataFrame:
    frames = [frame for frame in frames if frame is not None and not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def write_manifest(output_dir: Path, args: argparse.Namespace, btc: pd.DataFrame, sol: pd.DataFrame) -> None:
    manifest = {
        "schema_version": 1,
        "objective": "BTC residual/sigma repair and SOL execution attribution",
        "min_train_days": args.min_train_days,
        "pm5m_snapshot_glob": args.pm5m_snapshot_glob,
        "pm15m_snapshot_glob": args.pm15m_snapshot_glob,
        "rows": {
            "btc": int(len(btc)),
            "btc_fillable": int(btc["target_fillable"].sum()),
            "btc_gate_passed_fillable": int((btc["target_fillable"] & btc["strategy_gate_passed"]).sum()),
            "sol": int(len(sol)),
            "sol_fillable": int(sol["target_fillable"].sum()),
            "sol_gate_passed_fillable": int((sol["target_fillable"] & sol["strategy_gate_passed"]).sum()),
        },
        "outputs": [
            "btc_residual_buckets.csv",
            "btc_tau_distance_vol_grid.csv",
            "btc_sigma_model_comparison.csv",
            "btc_market_residual_logistic_oos.csv",
            "btc_calibrated_net_edge_buckets.csv",
            "btc_yes_residual_shadow_grid.csv",
            "sol_fill_bucket_ev.csv",
            "sol_post_fill_toxicity.csv",
            "sol_yes_btc_lead_buckets.csv",
            "sol_15m_phase_side_quality.csv",
            "sol_relaxation_ladder.csv",
            "manifest.json",
        ],
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
