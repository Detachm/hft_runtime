#!/usr/bin/env python3
import argparse
import glob
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.special import ndtr
from sklearn.linear_model import LogisticRegression


USECOLS = [
    "symbol",
    "horizon_seconds",
    "condition_id",
    "asset_id",
    "outcome",
    "ts_ns",
    "seconds_to_end",
    "anchor_price_micros",
    "current_price_micros",
    "sigma_60_micros",
    "sigma_180_micros",
    "p_model_micros",
    "momentum_60s_bps",
    "side_momentum_60s_bps",
    "btc_lead_momentum_60s_bps",
    "market_mid_micros",
    "target_fillable",
    "target_avg_fill_price_micros",
    "target_shares_micros",
    "fee_micros_per_share",
    "winner",
    "realized_pnl_per_share_micros",
    "strategy_gate_passed",
]

NUMERIC_COLS = [
    "horizon_seconds",
    "ts_ns",
    "seconds_to_end",
    "anchor_price_micros",
    "current_price_micros",
    "sigma_60_micros",
    "sigma_180_micros",
    "p_model_micros",
    "momentum_60s_bps",
    "side_momentum_60s_bps",
    "btc_lead_momentum_60s_bps",
    "market_mid_micros",
    "target_avg_fill_price_micros",
    "target_shares_micros",
    "fee_micros_per_share",
    "realized_pnl_per_share_micros",
]

DAY_NS = 86_400 * 1_000_000_000
CST_OFFSET_NS = 8 * 3_600 * 1_000_000_000
SQRT_2PI = math.sqrt(2.0 * math.pi)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pm5m-snapshot-glob", action="append", required=True)
    parser.add_argument("--pm15m-snapshot-glob", action="append", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-train-days", type=int, default=3)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    paths = expand_globs(args.pm5m_snapshot_glob) + expand_globs(args.pm15m_snapshot_glob)
    df = prepare_base(load_snapshots(paths))
    df = build_oos_joint_predictions(df, min_train_days=args.min_train_days)

    model_oos = build_model_oos_comparison(df)
    joint_edge = build_option_joint_edge_max1(df)
    threshold_grid = build_sensitivity_adjusted_threshold_grid(df)

    model_oos.to_csv(output_dir / "option_model_oos_comparison.csv", index=False)
    joint_edge.to_csv(output_dir / "option_joint_edge_max1.csv", index=False)
    threshold_grid.to_csv(output_dir / "sensitivity_adjusted_threshold_grid.csv", index=False)
    write_manifest(output_dir, args, df, paths)


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
    work["side"] = work["outcome"]
    for col in ["target_fillable", "winner", "strategy_gate_passed"]:
        work[col] = parse_bool(work[col])
    work["market_id"] = work["condition_id"]
    work["day_cst"] = ((work["ts_ns"] + CST_OFFSET_NS) // DAY_NS).astype("Int64")
    work["tau"] = work["seconds_to_end"].astype(float)
    work["S"] = work["current_price_micros"] / 1_000_000.0
    work["K"] = work["anchor_price_micros"] / 1_000_000.0
    work["moneyness"] = np.log(work["S"] / work["K"])
    work["market_q_mid"] = clip_prob(work["market_mid_micros"] / 1_000_000.0)
    work["p_raw"] = clip_prob(work["p_model_micros"] / 1_000_000.0)
    work["executable_ask"] = work["target_avg_fill_price_micros"] / 1_000_000.0
    work["shares_1u"] = work["target_shares_micros"] / 1_000_000.0
    work["fee"] = work["fee_micros_per_share"] / 1_000_000.0
    work["pnl_share"] = work["realized_pnl_per_share_micros"] / 1_000_000.0
    work["pnl_if_buy_1u"] = work["pnl_share"] * work["shares_1u"]
    work["final_outcome"] = work["winner"].astype(int)
    work["side_sign"] = np.where(work["side"] == "YES", 1.0, -1.0)

    work["sigma_60"] = work["sigma_60_micros"] / 1_000_000.0
    work["sigma_180"] = work["sigma_180_micros"] / 1_000_000.0
    work["sigma_floor"] = work.groupby(["coin", "horizon_seconds"], observed=True)[
        "sigma_180"
    ].transform(lambda s: s.quantile(0.25))
    work["sigma_eff"] = work[["sigma_60", "sigma_180", "sigma_floor"]].max(axis=1)

    tau = work["tau"].clip(lower=1).to_numpy(dtype=float)
    denom = work["sigma_eff"].to_numpy(dtype=float) * np.sqrt(tau)
    d_yes = safe_divide(work["moneyness"].to_numpy(dtype=float), denom)
    q_yes = clip_prob(ndtr(d_yes))
    work["q_yes_sigma_eff"] = q_yes
    work["q_side_sigma_eff"] = np.where(work["side"] == "YES", q_yes, 1.0 - q_yes)
    work["thin_logit_residual"] = logit(work["q_side_sigma_eff"]) - logit(work["market_q_mid"])
    work["residual_q"] = work["q_side_sigma_eff"] - work["market_q_mid"]
    pdf = np.exp(-0.5 * np.square(np.nan_to_num(d_yes, nan=0.0))) / SQRT_2PI
    work["sensitivity"] = safe_divide(pdf, denom)
    work.loc[~np.isfinite(d_yes), "sensitivity"] = np.nan
    work["log_sensitivity"] = np.log1p(work["sensitivity"])
    work["drift_side_momentum"] = work["side_momentum_60s_bps"] / 20.0
    work["drift_signed_momentum"] = work["side_sign"] * work["momentum_60s_bps"] / 20.0
    work["drift_signed_btc_lead"] = work["side_sign"] * work["btc_lead_momentum_60s_bps"] / 20.0
    work["option_valid"] = (
        work["target_fillable"]
        & work["market_q_mid"].notna()
        & work["q_side_sigma_eff"].notna()
        & work["executable_ask"].notna()
        & work["fee"].notna()
        & work["pnl_if_buy_1u"].notna()
        & work["sensitivity"].notna()
        & np.isfinite(work["moneyness"])
        & np.isfinite(work["sigma_eff"])
        & (work["sigma_eff"] > 0)
        & (work["tau"] > 0)
    )
    add_buckets(work)
    return work


def parse_bool(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.fillna(False)
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def clip_prob(values) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    return np.clip(arr, 1e-6, 1 - 1e-6)


def logit(values) -> np.ndarray:
    prob = clip_prob(values)
    return np.log(prob / (1.0 - prob))


def safe_divide(numerator, denominator) -> np.ndarray:
    num = np.asarray(numerator, dtype=float)
    den = np.asarray(denominator, dtype=float)
    out = np.full_like(num, np.nan, dtype=float)
    mask = np.isfinite(num) & np.isfinite(den) & (np.abs(den) > 1e-12)
    out[mask] = num[mask] / den[mask]
    return out


def add_buckets(work: pd.DataFrame) -> None:
    work["sensitivity_bucket"] = pd.cut(
        work["sensitivity"],
        bins=[-np.inf, 50, 100, 200, 400, 800, 1600, np.inf],
        labels=["<=50", "50-100", "100-200", "200-400", "400-800", "800-1600", ">1600"],
        right=False,
    )


def build_oos_joint_predictions(df: pd.DataFrame, min_train_days: int) -> pd.DataFrame:
    data = df.copy()
    data["q_market_option_oos"] = np.nan
    data["q_market_option_drift_oos"] = np.nan
    valid = data[data["option_valid"]].copy()
    for _, group_idx in valid.groupby(["coin", "horizon_seconds", "side"], sort=True).groups.items():
        group = valid.loc[group_idx].sort_values("ts_ns")
        for day in sorted(group["day_cst"].dropna().unique()):
            train = group[group["day_cst"] < day]
            test = group[group["day_cst"] == day]
            if train["day_cst"].nunique() < min_train_days or test.empty:
                continue
            y_train = train["final_outcome"].astype(int).to_numpy()
            if len(np.unique(y_train)) < 2:
                continue
            weights = market_balanced_weights(train)
            specs = [
                (
                    "q_market_option_oos",
                    option_features(train, include_drift=False),
                    option_features(test, include_drift=False),
                ),
                (
                    "q_market_option_drift_oos",
                    option_features(train, include_drift=True),
                    option_features(test, include_drift=True),
                ),
            ]
            for col, x_train, x_test in specs:
                data.loc[test.index, col] = fit_predict_logit(x_train, y_train, x_test, weights)
    for col in ["q_market_option_oos", "q_market_option_drift_oos"]:
        edge_col = col.replace("q_", "joint_edge_").replace("_oos", "")
        data[edge_col] = data[col] - data["executable_ask"] - data["fee"]
    return data


def option_features(df: pd.DataFrame, include_drift: bool) -> np.ndarray:
    cols = [
        logit(df["market_q_mid"]),
        df["thin_logit_residual"].to_numpy(dtype=float),
        df["log_sensitivity"].to_numpy(dtype=float),
    ]
    if include_drift:
        cols.extend(
            [
                df["drift_side_momentum"].to_numpy(dtype=float),
                df["drift_signed_momentum"].to_numpy(dtype=float),
                df["drift_signed_btc_lead"].to_numpy(dtype=float),
            ]
        )
    return np.nan_to_num(np.column_stack(cols), nan=0.0, posinf=0.0, neginf=0.0)


def fit_predict_logit(
    x_train: np.ndarray, y_train: np.ndarray, x_test: np.ndarray, weights: np.ndarray
) -> np.ndarray:
    mean = x_train.mean(axis=0)
    std = x_train.std(axis=0)
    std[std < 1e-9] = 1.0
    model = LogisticRegression(C=1.0, solver="lbfgs", max_iter=300)
    model.fit((x_train - mean) / std, y_train, sample_weight=weights)
    return clip_prob(model.predict_proba((x_test - mean) / std)[:, 1])


def market_balanced_weights(df: pd.DataFrame) -> np.ndarray:
    counts = df.groupby("market_id", observed=True)["market_id"].transform("size")
    return (1.0 / counts.astype(float)).to_numpy()


def build_model_oos_comparison(df: pd.DataFrame) -> pd.DataFrame:
    specs = [
        ("all_fillable", "market_mid_direct", "market_q_mid", df["option_valid"]),
        ("all_fillable", "q_sigma_eff_direct", "q_side_sigma_eff", df["option_valid"]),
        (
            "all_fillable_oos_aligned",
            "market_mid_direct",
            "market_q_mid",
            df["option_valid"] & df["q_market_option_oos"].notna(),
        ),
        (
            "all_fillable_oos_aligned",
            "q_sigma_eff_direct",
            "q_side_sigma_eff",
            df["option_valid"] & df["q_market_option_oos"].notna(),
        ),
        (
            "all_fillable_oos_aligned",
            "market_plus_option_sensitivity_oos",
            "q_market_option_oos",
            df["option_valid"] & df["q_market_option_oos"].notna(),
        ),
        (
            "all_fillable_oos_aligned",
            "market_plus_option_sensitivity_drift_oos",
            "q_market_option_drift_oos",
            df["option_valid"] & df["q_market_option_drift_oos"].notna(),
        ),
        (
            "gate_passed_fillable",
            "current_gate_market_mid_direct",
            "market_q_mid",
            df["option_valid"] & df["strategy_gate_passed"],
        ),
        (
            "gate_passed_fillable_oos_aligned",
            "current_gate_market_mid_direct",
            "market_q_mid",
            df["option_valid"] & df["strategy_gate_passed"] & df["q_market_option_oos"].notna(),
        ),
        (
            "gate_passed_fillable_oos_aligned",
            "current_gate_joint_drift_oos",
            "q_market_option_drift_oos",
            df["option_valid"] & df["strategy_gate_passed"] & df["q_market_option_drift_oos"].notna(),
        ),
    ]
    rows = []
    for scope, model, prob_col, mask in specs:
        data = df[mask].copy()
        for keys, group in data.groupby(["coin", "horizon_seconds", "side"], observed=True, sort=True):
            y = group["final_outcome"].to_numpy(dtype=float)
            prob = clip_prob(group[prob_col].to_numpy(dtype=float))
            row = {
                "scope": scope,
                "coin": keys[0],
                "horizon_seconds": int(keys[1]),
                "side": keys[2],
                "model": model,
                "rows": int(len(group)),
                "unique_markets": int(group["market_id"].nunique()),
                "unique_days": int(group["day_cst"].nunique()),
                "oos_brier": float(np.mean(np.square(prob - y))),
                "oos_logloss": mean_log_loss(y, prob),
                "calibration_slope": calibration_slope(y, prob),
                "avg_prob": float(np.mean(prob)),
                "realized_win_rate": float(np.mean(y)),
            }
            rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["scope", "coin", "horizon_seconds", "side", "oos_logloss"])


def calibration_slope(y: np.ndarray, prob: np.ndarray) -> float:
    if len(prob) < 20 or len(np.unique(y)) < 2:
        return math.nan
    x = logit(prob).reshape(-1, 1)
    try:
        model = LogisticRegression(C=1e6, solver="lbfgs", max_iter=300)
        model.fit(x, y.astype(int))
        return float(model.coef_[0][0])
    except Exception:
        return math.nan


def build_option_joint_edge_max1(df: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for model_name, q_col, edge_col in [
        (
            "market_plus_option_sensitivity_oos",
            "q_market_option_oos",
            "joint_edge_market_option",
        ),
        (
            "market_plus_option_sensitivity_drift_oos",
            "q_market_option_drift_oos",
            "joint_edge_market_option_drift",
        ),
    ]:
        data = df[df["option_valid"] & df[q_col].notna()].copy()
        data["model"] = model_name
        data["joint_edge"] = data[edge_col]
        data["edge_bucket"] = edge_bucket(data["joint_edge"])
        group_cols = ["coin", "horizon_seconds", "side", "model", "edge_bucket", "sensitivity_bucket"]
        frames.append(max1_group_summary(data, group_cols))
    out = concat_rows(frames)
    if out.empty:
        return out
    return out.sort_values(["coin", "horizon_seconds", "side", "model", "edge_bucket", "sensitivity_bucket"])


def build_sensitivity_adjusted_threshold_grid(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    thresholds = [0.0, 0.02, 0.04, 0.06, 0.08, 0.12, 0.16, 0.20]
    for model_name, q_col, edge_col in [
        (
            "market_plus_option_sensitivity_oos",
            "q_market_option_oos",
            "joint_edge_market_option",
        ),
        (
            "market_plus_option_sensitivity_drift_oos",
            "q_market_option_drift_oos",
            "joint_edge_market_option_drift",
        ),
    ]:
        base = df[df["option_valid"] & df[q_col].notna()].copy()
        base["model"] = model_name
        base["joint_edge"] = base[edge_col]
        for keys, group in base.groupby(["coin", "horizon_seconds", "side"], observed=True, sort=True):
            for sensitivity_name, sens_group in [("ALL", group)] + [
                (str(bucket), part)
                for bucket, part in group.groupby("sensitivity_bucket", observed=True, sort=True)
            ]:
                for threshold in thresholds:
                    selected = cap_orders_per_market(sens_group[sens_group["joint_edge"] >= threshold])
                    if selected.empty:
                        continue
                    row = {
                        "coin": keys[0],
                        "horizon_seconds": int(keys[1]),
                        "side": keys[2],
                        "model": model_name,
                        "sensitivity_bucket": sensitivity_name,
                        "threshold": threshold,
                    }
                    row.update(performance_metrics(selected))
                    rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["coin", "horizon_seconds", "side", "model", "sensitivity_bucket", "threshold"])


def max1_group_summary(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows = []
    for keys, group in df.groupby(group_cols, observed=True, sort=True):
        selected = cap_orders_per_market(group)
        if selected.empty:
            continue
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_cols, keys))
        row.update(performance_metrics(selected))
        rows.append(row)
    return pd.DataFrame(rows)


def cap_orders_per_market(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    work = df.sort_values(["market_id", "ts_ns", "side"]).copy()
    rank = work.groupby("market_id", observed=True).cumcount()
    return work[rank == 0].copy()


def edge_bucket(series: pd.Series) -> pd.Series:
    return pd.cut(
        series,
        bins=[-np.inf, -0.12, -0.08, -0.04, -0.02, 0, 0.02, 0.04, 0.06, 0.08, 0.12, np.inf],
        labels=["<-12c", "-12--8c", "-8--4c", "-4--2c", "-2-0c", "0-2c", "2-4c", "4-6c", "6-8c", "8-12c", ">12c"],
        right=False,
    )


def performance_metrics(group: pd.DataFrame) -> dict:
    daily = group.groupby("day_cst", observed=True)["pnl_if_buy_1u"].sum()
    market = group.groupby("market_id", observed=True)["pnl_if_buy_1u"].sum()
    total_abs_market = float(market.abs().sum())
    return {
        "orders": int(len(group)),
        "unique_markets": int(group["market_id"].nunique()),
        "unique_days": int(group["day_cst"].nunique()),
        "win": float(group["final_outcome"].mean()),
        "avg_fill": float(group["executable_ask"].mean()),
        "avg_fee": float(group["fee"].mean()),
        "avg_joint_edge": float(group["joint_edge"].mean()),
        "avg_sensitivity": float(group["sensitivity"].mean()),
        "pnl": float(group["pnl_if_buy_1u"].sum()),
        "pnl_per_order": float(group["pnl_if_buy_1u"].mean()),
        "pos_days": int((daily > 0).sum()),
        "avg_daily_pnl": float(daily.mean()) if not daily.empty else math.nan,
        "min_daily_pnl": float(daily.min()) if not daily.empty else math.nan,
        "max_daily_pnl": float(daily.max()) if not daily.empty else math.nan,
        "max_drawdown": max_drawdown(daily),
        "top5_abs_share": float(
            market.abs().sort_values(ascending=False).head(5).sum() / total_abs_market
        )
        if total_abs_market > 0
        else math.nan,
    }


def mean_log_loss(y: np.ndarray, prob: np.ndarray) -> float:
    prob = clip_prob(prob)
    return float(np.mean(-(y * np.log(prob) + (1.0 - y) * np.log1p(-prob))))


def max_drawdown(daily: pd.Series) -> float:
    if daily.empty:
        return math.nan
    cumulative = daily.sort_index().cumsum()
    peak = cumulative.cummax()
    return float((peak - cumulative).max())


def concat_rows(frames: list[pd.DataFrame]) -> pd.DataFrame:
    frames = [frame for frame in frames if frame is not None and not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def write_manifest(output_dir: Path, args: argparse.Namespace, df: pd.DataFrame, paths: list[str]) -> None:
    manifest = {
        "schema_version": 1,
        "objective": "EXP-OPTION-002 market-anchored thin option model",
        "snapshot_count": len(paths),
        "min_train_days": args.min_train_days,
        "pm5m_snapshot_glob": args.pm5m_snapshot_glob,
        "pm15m_snapshot_glob": args.pm15m_snapshot_glob,
        "rows": {
            "total": int(len(df)),
            "option_valid": int(df["option_valid"].sum()),
            "gate_passed_valid": int((df["option_valid"] & df["strategy_gate_passed"]).sum()),
            "joint_oos": int((df["option_valid"] & df["q_market_option_oos"].notna()).sum()),
            "joint_drift_oos": int((df["option_valid"] & df["q_market_option_drift_oos"].notna()).sum()),
        },
        "models": {
            "market_mid_direct": "direct market_mid probability",
            "q_sigma_eff_direct": "direct thin option sigma_eff probability",
            "market_plus_option_sensitivity_oos": "walk-forward logistic: logit(market_mid), logit(q_sigma_eff)-logit(market_mid), log1p(sensitivity)",
            "market_plus_option_sensitivity_drift_oos": "same plus side momentum, signed underlying momentum, signed BTC lead",
        },
        "outputs": [
            "option_model_oos_comparison.csv",
            "option_joint_edge_max1.csv",
            "sensitivity_adjusted_threshold_grid.csv",
            "manifest.json",
        ],
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
