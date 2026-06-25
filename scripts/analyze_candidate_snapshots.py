#!/usr/bin/env python3
import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss


USECOLS = [
    "symbol",
    "horizon_seconds",
    "condition_id",
    "outcome",
    "ts_ns",
    "p_model_micros",
    "market_mid_micros",
    "target_fillable",
    "target_avg_fill_price_micros",
    "fee_micros_per_share",
    "raw_edge_micros",
    "net_raw_edge_after_fee_micros",
    "winner",
    "realized_pnl_per_share_micros",
    "strategy_gate_passed",
    "locked_side",
    "locked_side_match",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", action="append", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-train-days", type=int, default=3)
    parser.add_argument("--subset", choices=["all", "gate_passed"], default="all")
    parser.add_argument("--edge-buffer-micros", type=int, default=0)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = load_inputs(args.input_csv)
    if args.subset == "gate_passed":
        df = prepare_gate_passed_subset(df)
        write_gate_passed_market_vs_signal(df, output_dir, args.min_train_days)
        qcal = write_gate_passed_qcal_calibration(df, output_dir, args.min_train_days)
        write_gate_passed_net_edge_buckets(
            qcal, output_dir, args.edge_buffer_micros / 1_000_000.0
        )
        write_gate_passed_entry_phase_15m(df, output_dir)
    else:
        write_raw_calibration(df, output_dir)
        write_market_signal_oos(df, output_dir, args.min_train_days)
        write_net_edge_buckets(df, output_dir, args.min_train_days)
    write_manifest(df, output_dir, args)


def load_inputs(paths: list[str]) -> pd.DataFrame:
    frames = []
    for path in paths:
        frame = pd.read_csv(path, usecols=USECOLS, low_memory=False)
        frames.append(frame)
    df = pd.concat(frames, ignore_index=True)
    df["symbol"] = df["symbol"].str.upper()
    df["coin"] = df["symbol"].str.extract(r"^(BTC|ETH|SOL)", expand=False).fillna(df["symbol"])
    df["outcome"] = df["outcome"].str.upper()
    df["horizon_seconds"] = df["horizon_seconds"].astype(int)
    df["winner"] = parse_bool(df["winner"])
    df["target_fillable"] = parse_bool(df["target_fillable"])
    df["strategy_gate_passed"] = parse_bool(df["strategy_gate_passed"])
    df["locked_side_match"] = parse_bool(df["locked_side_match"])
    df["p_raw"] = clip_prob(df["p_model_micros"] / 1_000_000.0)
    df["market_mid"] = clip_prob(df["market_mid_micros"] / 1_000_000.0)
    df["target_avg_fill"] = clip_prob(df["target_avg_fill_price_micros"] / 1_000_000.0)
    df["fee_per_share"] = df["fee_micros_per_share"] / 1_000_000.0
    df["raw_edge"] = df["raw_edge_micros"] / 1_000_000.0
    df["net_raw_edge_after_fee"] = df["net_raw_edge_after_fee_micros"] / 1_000_000.0
    df["realized_pnl_per_share"] = df["realized_pnl_per_share_micros"] / 1_000_000.0
    day_ns = 86_400 * 1_000_000_000
    cst_offset_ns = 8 * 3_600 * 1_000_000_000
    df["day_cst"] = ((df["ts_ns"] + cst_offset_ns) // day_ns).astype(np.int64)
    return df


def prepare_gate_passed_subset(df: pd.DataFrame) -> pd.DataFrame:
    work = df[df["strategy_gate_passed"]].copy()
    work["strategy_instance"] = (
        work["coin"]
        + "_"
        + (work["horizon_seconds"] // 60).astype(str)
        + "m_current_gate"
    )
    work["entry_phase"] = "all"

    is_15m = work["horizon_seconds"] == 900
    if is_15m.any():
        fifteen = work[is_15m].copy()
        first_locked_ts = (
            fifteen[fifteen["locked_side_match"]]
            .groupby("condition_id", observed=True)["ts_ns"]
            .min()
        )
        mapped_first_ts = fifteen["condition_id"].map(first_locked_ts)
        phase = np.full(len(fifteen), "cheap_reentry", dtype=object)
        locked_match = fifteen["locked_side_match"].to_numpy(dtype=bool)
        ts = fifteen["ts_ns"].to_numpy()
        first_ts = mapped_first_ts.to_numpy()
        has_first = ~pd.isna(mapped_first_ts).to_numpy()
        phase[locked_match & has_first & (ts == first_ts)] = "first_entry"
        phase[locked_match & has_first & (ts > first_ts)] = "locked_add"
        phase[locked_match & ~has_first] = "locked_add"
        work.loc[fifteen.index, "entry_phase"] = phase
    return work


def parse_bool(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def clip_prob(series: pd.Series | np.ndarray) -> pd.Series | np.ndarray:
    return np.clip(series, 1e-6, 1 - 1e-6)


def logit(values: pd.Series | np.ndarray) -> np.ndarray:
    values = clip_prob(np.asarray(values, dtype=float))
    return np.log(values / (1.0 - values))


def write_raw_calibration(df: pd.DataFrame, output_dir: Path) -> None:
    bins = np.arange(0.0, 1.00001, 0.05)
    labels = [f"{bins[i]:.2f}-{bins[i + 1]:.2f}" for i in range(len(bins) - 1)]
    work = df.copy()
    work["p_raw_bucket"] = pd.cut(work["p_raw"], bins=bins, labels=labels, include_lowest=True)
    grouped = (
        work.groupby(["coin", "horizon_seconds", "outcome", "p_raw_bucket"], observed=True)
        .agg(
            sample_count=("winner", "size"),
            avg_p_raw=("p_raw", "mean"),
            realized_win_rate=("winner", "mean"),
            fillable_count=("target_fillable", "sum"),
            avg_pnl_per_share=("realized_pnl_per_share", "mean"),
        )
        .reset_index()
    )
    grouped.to_csv(output_dir / "table1_p_raw_calibration.csv", index=False)


def write_market_signal_oos(df: pd.DataFrame, output_dir: Path, min_train_days: int) -> None:
    work = df.dropna(subset=["market_mid"]).copy()
    rows = []
    for keys, group in work.groupby(["coin", "horizon_seconds", "outcome"], sort=True):
        coin, horizon, outcome = keys
        group = group.sort_values("ts_ns")
        days = sorted(group["day_cst"].unique())
        for day in days:
            train = group[group["day_cst"] < day]
            test = group[group["day_cst"] == day]
            if train["day_cst"].nunique() < min_train_days or len(test) == 0:
                continue
            y_train = train["winner"].astype(int).to_numpy()
            y_test = test["winner"].astype(int).to_numpy()
            if len(np.unique(y_train)) < 2 or len(np.unique(y_test)) < 2:
                continue

            add_metric_row(
                rows,
                coin,
                horizon,
                outcome,
                day,
                "market_mid_direct",
                y_test,
                test["market_mid"].to_numpy(),
            )
            add_metric_row(
                rows,
                coin,
                horizon,
                outcome,
                day,
                "p_raw_direct",
                y_test,
                test["p_raw"].to_numpy(),
            )

            fit_and_score(
                rows,
                coin,
                horizon,
                outcome,
                day,
                "logit_market_mid",
                logit(train["market_mid"]).reshape(-1, 1),
                y_train,
                logit(test["market_mid"]).reshape(-1, 1),
                y_test,
            )
            fit_and_score(
                rows,
                coin,
                horizon,
                outcome,
                day,
                "logit_market_plus_p_raw",
                np.column_stack([logit(train["market_mid"]), logit(train["p_raw"])]),
                y_train,
                np.column_stack([logit(test["market_mid"]), logit(test["p_raw"])]),
                y_test,
            )

    folds = pd.DataFrame(rows)
    folds.to_csv(output_dir / "table2_market_signal_oos_folds.csv", index=False)
    if folds.empty:
        pd.DataFrame().to_csv(output_dir / "table2_market_signal_oos_summary.csv", index=False)
        return
    summary = (
        folds.groupby(["coin", "horizon_seconds", "outcome", "model"], observed=True)
        .apply(weighted_metric_summary, include_groups=False)
        .reset_index()
    )
    summary.to_csv(output_dir / "table2_market_signal_oos_summary.csv", index=False)


def add_metric_row(rows, coin, horizon, outcome, day, model, y_test, prob) -> None:
    prob = clip_prob(np.asarray(prob))
    rows.append(
        {
            "coin": coin,
            "horizon_seconds": horizon,
            "outcome": outcome,
            "test_day_cst_ord": int(day),
            "model": model,
            "sample_count": int(len(y_test)),
            "brier": brier_score_loss(y_test, prob),
            "log_loss": log_loss(y_test, prob, labels=[0, 1]),
            "avg_prob": float(np.mean(prob)),
            "realized_win_rate": float(np.mean(y_test)),
        }
    )


def fit_and_score(rows, coin, horizon, outcome, day, model, x_train, y_train, x_test, y_test) -> None:
    clf = LogisticRegression(C=1e6, solver="lbfgs", max_iter=300)
    clf.fit(x_train, y_train)
    prob = clf.predict_proba(x_test)[:, 1]
    add_metric_row(rows, coin, horizon, outcome, day, model, y_test, prob)


def weighted_metric_summary(group: pd.DataFrame) -> pd.Series:
    weights = group["sample_count"].to_numpy(dtype=float)
    total = weights.sum()
    if total <= 0:
        total = 1.0
    return pd.Series(
        {
            "fold_count": len(group),
            "sample_count": int(group["sample_count"].sum()),
            "weighted_brier": float(np.average(group["brier"], weights=weights)),
            "weighted_log_loss": float(np.average(group["log_loss"], weights=weights)),
            "avg_prob": float(np.average(group["avg_prob"], weights=weights)),
            "realized_win_rate": float(np.average(group["realized_win_rate"], weights=weights)),
        }
    )


def write_net_edge_buckets(df: pd.DataFrame, output_dir: Path, min_train_days: int) -> None:
    work = df[df["target_fillable"]].dropna(
        subset=["target_avg_fill", "fee_per_share", "realized_pnl_per_share"]
    ).copy()
    work["q_cal"] = np.nan
    calibration_rows = []
    for keys, group_idx in work.groupby(["coin", "horizon_seconds", "outcome"], sort=True).groups.items():
        group = work.loc[group_idx].sort_values("ts_ns")
        days = sorted(group["day_cst"].unique())
        for day in days:
            train = group[group["day_cst"] < day]
            test_idx = group[group["day_cst"] == day].index
            if train["day_cst"].nunique() < min_train_days or len(test_idx) == 0:
                continue
            y_train = train["winner"].astype(int).to_numpy()
            if len(np.unique(y_train)) < 2:
                continue
            clf = LogisticRegression(C=1e6, solver="lbfgs", max_iter=300)
            x_train = logit(train["p_raw"]).reshape(-1, 1)
            clf.fit(x_train, y_train)
            x_test = logit(work.loc[test_idx, "p_raw"]).reshape(-1, 1)
            work.loc[test_idx, "q_cal"] = clf.predict_proba(x_test)[:, 1]
            calibration_rows.append(
                {
                    "coin": keys[0],
                    "horizon_seconds": keys[1],
                    "outcome": keys[2],
                    "test_day_cst_ord": int(day),
                    "train_sample_count": int(len(train)),
                    "intercept": float(clf.intercept_[0]),
                    "logit_p_raw_coef": float(clf.coef_[0][0]),
                }
            )

    pd.DataFrame(calibration_rows).to_csv(output_dir / "q_cal_walk_forward_coefficients.csv", index=False)
    valid = work.dropna(subset=["q_cal"]).copy()
    valid["net_edge"] = valid["q_cal"] - valid["target_avg_fill"] - valid["fee_per_share"]
    bins = np.arange(-1.0, 1.00001, 0.05)
    labels = [f"{bins[i]:.2f}-{bins[i + 1]:.2f}" for i in range(len(bins) - 1)]
    valid["net_edge_bucket"] = pd.cut(valid["net_edge"], bins=bins, labels=labels, include_lowest=True)
    buckets = (
        valid.groupby(["coin", "horizon_seconds", "outcome", "net_edge_bucket"], observed=True)
        .agg(
            sample_count=("winner", "size"),
            avg_q_cal=("q_cal", "mean"),
            avg_target_fill=("target_avg_fill", "mean"),
            avg_fee_per_share=("fee_per_share", "mean"),
            avg_net_edge=("net_edge", "mean"),
            realized_win_rate=("winner", "mean"),
            avg_realized_pnl_per_share=("realized_pnl_per_share", "mean"),
        )
        .reset_index()
    )
    buckets.to_csv(output_dir / "table3_net_edge_bucket_pnl.csv", index=False)

    monotonic = []
    for keys, group in buckets.groupby(["coin", "horizon_seconds", "outcome"], sort=True):
        group = group[group["sample_count"] > 0].copy()
        if len(group) < 3:
            continue
        monotonic.append(
            {
                "coin": keys[0],
                "horizon_seconds": keys[1],
                "outcome": keys[2],
                "bucket_count": len(group),
                "spearman_net_edge_vs_pnl": float(
                    group["avg_net_edge"].corr(group["avg_realized_pnl_per_share"], method="spearman")
                ),
                "pearson_net_edge_vs_pnl": float(
                    group["avg_net_edge"].corr(group["avg_realized_pnl_per_share"], method="pearson")
                ),
            }
        )
    pd.DataFrame(monotonic).to_csv(output_dir / "table3_net_edge_monotonicity.csv", index=False)


GATE_GROUP_COLS = ["horizon_seconds", "coin", "outcome", "strategy_instance", "entry_phase"]


def write_gate_passed_market_vs_signal(
    df: pd.DataFrame, output_dir: Path, min_train_days: int
) -> None:
    rows = []
    work = df.dropna(subset=["market_mid", "p_raw"]).copy()
    for keys, group in work.groupby(GATE_GROUP_COLS, sort=True):
        group = group.sort_values("ts_ns")
        days = sorted(group["day_cst"].unique())
        for day in days:
            train = group[group["day_cst"] < day]
            test = group[group["day_cst"] == day]
            if train["day_cst"].nunique() < min_train_days or len(test) == 0:
                continue
            y_train = train["winner"].astype(int).to_numpy()
            y_test = test["winner"].astype(int).to_numpy()
            if len(np.unique(y_train)) < 2 or len(np.unique(y_test)) < 2:
                continue

            market = fit_logistic(
                logit(train["market_mid"]).reshape(-1, 1),
                y_train,
                logit(test["market_mid"]).reshape(-1, 1),
            )
            signal = fit_logistic(
                np.column_stack([logit(train["market_mid"]), logit(train["p_raw"])]),
                y_train,
                np.column_stack([logit(test["market_mid"]), logit(test["p_raw"])]),
            )
            if market is None or signal is None:
                continue
            market_prob, market_clf = market
            signal_prob, signal_clf = signal
            rows.append(
                {
                    **group_key_dict(keys),
                    "test_day_cst_ord": int(day),
                    "sample_count": int(len(test)),
                    "market_brier": brier_score_loss(y_test, market_prob),
                    "signal_brier": brier_score_loss(y_test, signal_prob),
                    "market_log_loss": log_loss(y_test, market_prob, labels=[0, 1]),
                    "signal_log_loss": log_loss(y_test, signal_prob, labels=[0, 1]),
                    "market_coef": float(market_clf.coef_[0][0]),
                    "signal_market_coef": float(signal_clf.coef_[0][0]),
                    "signal_p_raw_coef": float(signal_clf.coef_[0][1]),
                    "market_avg_prob": float(np.mean(market_prob)),
                    "signal_avg_prob": float(np.mean(signal_prob)),
                    "realized_win_rate": float(np.mean(y_test)),
                }
            )

    folds = pd.DataFrame(rows)
    if folds.empty:
        empty_gate_market_vs_signal().to_csv(
            output_dir / "gate_passed_market_vs_signal.csv", index=False
        )
        return
    grouped_rows = []
    for keys, group in folds.groupby(GATE_GROUP_COLS, sort=True):
        weights = group["sample_count"].to_numpy(dtype=float)
        c_values = group["signal_p_raw_coef"].to_numpy(dtype=float)
        grouped_rows.append(
            {
                **group_key_dict(keys),
                "fold_count": int(len(group)),
                "sample_count": int(group["sample_count"].sum()),
                "market_brier": weighted_average(group["market_brier"], weights),
                "signal_brier": weighted_average(group["signal_brier"], weights),
                "delta_brier_signal_minus_market": weighted_average(
                    group["signal_brier"] - group["market_brier"], weights
                ),
                "market_log_loss": weighted_average(group["market_log_loss"], weights),
                "signal_log_loss": weighted_average(group["signal_log_loss"], weights),
                "delta_log_loss_signal_minus_market": weighted_average(
                    group["signal_log_loss"] - group["market_log_loss"], weights
                ),
                "mean_signal_p_raw_coef": float(np.mean(c_values)),
                "median_signal_p_raw_coef": float(np.median(c_values)),
                "p_raw_coef_positive_frac": float(np.mean(c_values > 0)),
                "p_raw_coef_negative_frac": float(np.mean(c_values < 0)),
                "p_raw_coef_sign_stability": float(
                    max(np.mean(c_values > 0), np.mean(c_values < 0))
                ),
                "realized_win_rate": weighted_average(group["realized_win_rate"], weights),
                "market_avg_prob": weighted_average(group["market_avg_prob"], weights),
                "signal_avg_prob": weighted_average(group["signal_avg_prob"], weights),
            }
        )
    pd.DataFrame(grouped_rows).to_csv(
        output_dir / "gate_passed_market_vs_signal.csv", index=False
    )


def write_gate_passed_qcal_calibration(
    df: pd.DataFrame, output_dir: Path, min_train_days: int
) -> pd.DataFrame:
    work = df.dropna(subset=["p_raw"]).copy()
    work["q_cal"] = np.nan
    rows = []
    for keys, group_idx in work.groupby(GATE_GROUP_COLS, sort=True).groups.items():
        group = work.loc[group_idx].sort_values("ts_ns")
        days = sorted(group["day_cst"].unique())
        for day in days:
            train = group[group["day_cst"] < day]
            test_idx = group[group["day_cst"] == day].index
            if train["day_cst"].nunique() < min_train_days or len(test_idx) == 0:
                continue
            y_train = train["winner"].astype(int).to_numpy()
            y_test = work.loc[test_idx, "winner"].astype(int).to_numpy()
            if len(np.unique(y_train)) < 2 or len(np.unique(y_test)) < 2:
                continue
            clf = LogisticRegression(C=1e6, solver="lbfgs", max_iter=300)
            clf.fit(logit(train["p_raw"]).reshape(-1, 1), y_train)
            q_cal = clf.predict_proba(logit(work.loc[test_idx, "p_raw"]).reshape(-1, 1))[:, 1]
            work.loc[test_idx, "q_cal"] = q_cal
            rows.append(
                {
                    **group_key_dict(keys),
                    "test_day_cst_ord": int(day),
                    "train_sample_count": int(len(train)),
                    "sample_count": int(len(test_idx)),
                    "intercept": float(clf.intercept_[0]),
                    "logit_p_raw_coef": float(clf.coef_[0][0]),
                    "qcal_brier": brier_score_loss(y_test, q_cal),
                    "qcal_log_loss": log_loss(y_test, q_cal, labels=[0, 1]),
                    "avg_q_cal": float(np.mean(q_cal)),
                    "realized_win_rate": float(np.mean(y_test)),
                }
            )
    folds = pd.DataFrame(rows)
    if folds.empty:
        empty_gate_qcal().to_csv(output_dir / "gate_passed_qcal_calibration.csv", index=False)
        return work

    grouped_rows = []
    for keys, group in folds.groupby(GATE_GROUP_COLS, sort=True):
        weights = group["sample_count"].to_numpy(dtype=float)
        slopes = group["logit_p_raw_coef"].to_numpy(dtype=float)
        grouped_rows.append(
            {
                **group_key_dict(keys),
                "fold_count": int(len(group)),
                "sample_count": int(group["sample_count"].sum()),
                "avg_train_sample_count": float(np.mean(group["train_sample_count"])),
                "avg_intercept": float(np.mean(group["intercept"])),
                "avg_logit_p_raw_coef": float(np.mean(slopes)),
                "p_raw_coef_positive_frac": float(np.mean(slopes > 0)),
                "qcal_brier": weighted_average(group["qcal_brier"], weights),
                "qcal_log_loss": weighted_average(group["qcal_log_loss"], weights),
                "avg_q_cal": weighted_average(group["avg_q_cal"], weights),
                "realized_win_rate": weighted_average(group["realized_win_rate"], weights),
            }
        )
    pd.DataFrame(grouped_rows).to_csv(
        output_dir / "gate_passed_qcal_calibration.csv", index=False
    )
    return work


def write_gate_passed_net_edge_buckets(
    df: pd.DataFrame, output_dir: Path, edge_buffer: float
) -> None:
    valid = df[df["target_fillable"]].dropna(
        subset=["q_cal", "target_avg_fill", "fee_per_share", "realized_pnl_per_share"]
    ).copy()
    valid["net_edge"] = valid["q_cal"] - valid["target_avg_fill"] - valid["fee_per_share"] - edge_buffer
    bins = np.arange(-1.0, 1.00001, 0.05)
    labels = [f"{bins[i]:.2f}-{bins[i + 1]:.2f}" for i in range(len(bins) - 1)]
    valid["net_edge_bucket"] = pd.cut(valid["net_edge"], bins=bins, labels=labels, include_lowest=True)
    if valid.empty:
        empty_gate_buckets().to_csv(output_dir / "gate_passed_net_edge_buckets.csv", index=False)
        return
    buckets = (
        valid.groupby(GATE_GROUP_COLS + ["net_edge_bucket"], observed=True)
        .agg(
            sample_count=("winner", "size"),
            avg_q_cal=("q_cal", "mean"),
            avg_target_fill=("target_avg_fill", "mean"),
            avg_fee_per_share=("fee_per_share", "mean"),
            avg_net_edge=("net_edge", "mean"),
            realized_win_rate=("winner", "mean"),
            avg_realized_pnl_per_share=("realized_pnl_per_share", "mean"),
        )
        .reset_index()
    )
    monotonic_rows = []
    for keys, group in buckets.groupby(GATE_GROUP_COLS, sort=True):
        nonempty = group[group["sample_count"] > 0]
        if len(nonempty) < 3:
            spearman = math.nan
            pearson = math.nan
        else:
            spearman = float(
                nonempty["avg_net_edge"].corr(
                    nonempty["avg_realized_pnl_per_share"], method="spearman"
                )
            )
            pearson = float(
                nonempty["avg_net_edge"].corr(
                    nonempty["avg_realized_pnl_per_share"], method="pearson"
                )
            )
        monotonic_rows.append(
            {
                **group_key_dict(keys),
                "bucket_count": int(len(nonempty)),
                "spearman_net_edge_vs_pnl": spearman,
                "pearson_net_edge_vs_pnl": pearson,
            }
        )
    monotonic = pd.DataFrame(monotonic_rows)
    out = buckets.merge(monotonic, on=GATE_GROUP_COLS, how="left")
    out.to_csv(output_dir / "gate_passed_net_edge_buckets.csv", index=False)


def write_gate_passed_entry_phase_15m(df: pd.DataFrame, output_dir: Path) -> None:
    work = df[df["horizon_seconds"] == 900].copy()
    if work.empty:
        empty_gate_entry_phase().to_csv(output_dir / "gate_passed_entry_phase_15m.csv", index=False)
        return
    grouped = (
        work.groupby(GATE_GROUP_COLS, observed=True)
        .agg(
            sample_count=("winner", "size"),
            fillable_count=("target_fillable", "sum"),
            condition_count=("condition_id", "nunique"),
            realized_win_rate=("winner", "mean"),
            avg_p_raw=("p_raw", "mean"),
            avg_market_mid=("market_mid", "mean"),
            avg_target_fill=("target_avg_fill", "mean"),
            avg_raw_edge=("raw_edge", "mean"),
            avg_net_raw_edge_after_fee=("net_raw_edge_after_fee", "mean"),
            avg_realized_pnl_per_share=("realized_pnl_per_share", "mean"),
        )
        .reset_index()
    )
    grouped.to_csv(output_dir / "gate_passed_entry_phase_15m.csv", index=False)


def fit_logistic(x_train, y_train, x_test):
    clf = LogisticRegression(C=1e6, solver="lbfgs", max_iter=300)
    try:
        clf.fit(x_train, y_train)
    except ValueError:
        return None
    return clip_prob(clf.predict_proba(x_test)[:, 1]), clf


def group_key_dict(keys) -> dict:
    return dict(zip(GATE_GROUP_COLS, keys))


def weighted_average(values: pd.Series | np.ndarray, weights: np.ndarray) -> float:
    values = np.asarray(values, dtype=float)
    if len(values) == 0:
        return math.nan
    total = weights.sum()
    if total <= 0:
        return float(np.mean(values))
    return float(np.average(values, weights=weights))


def empty_gate_market_vs_signal() -> pd.DataFrame:
    return pd.DataFrame(
        columns=GATE_GROUP_COLS
        + [
            "fold_count",
            "sample_count",
            "market_brier",
            "signal_brier",
            "delta_brier_signal_minus_market",
            "market_log_loss",
            "signal_log_loss",
            "delta_log_loss_signal_minus_market",
            "mean_signal_p_raw_coef",
            "median_signal_p_raw_coef",
            "p_raw_coef_positive_frac",
            "p_raw_coef_negative_frac",
            "p_raw_coef_sign_stability",
            "realized_win_rate",
            "market_avg_prob",
            "signal_avg_prob",
        ]
    )


def empty_gate_qcal() -> pd.DataFrame:
    return pd.DataFrame(
        columns=GATE_GROUP_COLS
        + [
            "fold_count",
            "sample_count",
            "avg_train_sample_count",
            "avg_intercept",
            "avg_logit_p_raw_coef",
            "p_raw_coef_positive_frac",
            "qcal_brier",
            "qcal_log_loss",
            "avg_q_cal",
            "realized_win_rate",
        ]
    )


def empty_gate_buckets() -> pd.DataFrame:
    return pd.DataFrame(
        columns=GATE_GROUP_COLS
        + [
            "net_edge_bucket",
            "sample_count",
            "avg_q_cal",
            "avg_target_fill",
            "avg_fee_per_share",
            "avg_net_edge",
            "realized_win_rate",
            "avg_realized_pnl_per_share",
            "bucket_count",
            "spearman_net_edge_vs_pnl",
            "pearson_net_edge_vs_pnl",
        ]
    )


def empty_gate_entry_phase() -> pd.DataFrame:
    return pd.DataFrame(
        columns=GATE_GROUP_COLS
        + [
            "sample_count",
            "fillable_count",
            "condition_count",
            "realized_win_rate",
            "avg_p_raw",
            "avg_market_mid",
            "avg_target_fill",
            "avg_raw_edge",
            "avg_net_raw_edge_after_fee",
            "avg_realized_pnl_per_share",
        ]
    )


def write_manifest(df: pd.DataFrame, output_dir: Path, args: argparse.Namespace) -> None:
    if args.subset == "gate_passed":
        outputs = [
            "gate_passed_market_vs_signal.csv",
            "gate_passed_qcal_calibration.csv",
            "gate_passed_net_edge_buckets.csv",
            "gate_passed_entry_phase_15m.csv",
        ]
        phase_counts = (
            df["entry_phase"].value_counts(dropna=False).sort_index().to_dict()
            if "entry_phase" in df
            else {}
        )
    else:
        outputs = [
            "table1_p_raw_calibration.csv",
            "table2_market_signal_oos_folds.csv",
            "table2_market_signal_oos_summary.csv",
            "q_cal_walk_forward_coefficients.csv",
            "table3_net_edge_bucket_pnl.csv",
            "table3_net_edge_monotonicity.csv",
        ]
        phase_counts = {}
    manifest = {
        "schema_version": 1,
        "input_csv": args.input_csv,
        "subset": args.subset,
        "row_count": int(len(df)),
        "symbols": sorted(df["symbol"].dropna().unique().tolist()),
        "coins": sorted(df["coin"].dropna().unique().tolist()),
        "horizons": sorted(int(v) for v in df["horizon_seconds"].dropna().unique().tolist()),
        "min_train_days": args.min_train_days,
        "edge_buffer_micros": args.edge_buffer_micros,
        "entry_phase_definition": (
            "For 15m gate-passed rows: first_entry is the first locked-side gate-passed row per "
            "condition; locked_add is later locked-side gate-passed rows; cheap_reentry is "
            "gate-passed rows that do not match locked_side. Non-15m rows use entry_phase=all."
        ),
        "entry_phase_counts": phase_counts,
        "outputs": outputs,
    }
    pd.Series(manifest).to_json(output_dir / "analysis_manifest.json", indent=2)


if __name__ == "__main__":
    main()
