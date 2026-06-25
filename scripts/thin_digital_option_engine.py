#!/usr/bin/env python3
import argparse
import glob
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.special import ndtr


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
    "sigma_60_micros",
    "sigma_180_micros",
    "p_model_micros",
    "best_bid_price_micros",
    "best_ask_price_micros",
    "market_mid_micros",
    "spread_micros",
    "target_fillable",
    "target_avg_fill_price_micros",
    "target_shares_micros",
    "avg_fill_1u_micros",
    "fee_micros_per_share",
    "winner",
    "realized_pnl_per_share_micros",
    "strategy_gate_passed",
]

NUMERIC_COLS = [
    "horizon_seconds",
    "ts_ns",
    "window_start_ts_ns",
    "window_end_ts_ns",
    "seconds_to_end",
    "anchor_price_micros",
    "current_price_micros",
    "sigma_60_micros",
    "sigma_180_micros",
    "p_model_micros",
    "best_bid_price_micros",
    "best_ask_price_micros",
    "market_mid_micros",
    "spread_micros",
    "target_avg_fill_price_micros",
    "target_shares_micros",
    "avg_fill_1u_micros",
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
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    paths = expand_globs(args.pm5m_snapshot_glob) + expand_globs(args.pm15m_snapshot_glob)
    df = prepare_base(load_snapshots(paths))

    snapshot_path = output_dir / "thin_option_snapshots.parquet"
    write_snapshot_layer(df, snapshot_path)

    calibration = build_q_sigma_eff_calibration(df)
    residual = build_market_residual_buckets(df)
    executable_edge = build_executable_edge_buckets(df)
    sensitivity = build_sensitivity_buckets(df)

    calibration.to_csv(output_dir / "q_sigma_eff_calibration.csv", index=False)
    residual.to_csv(output_dir / "market_residual_buckets.csv", index=False)
    executable_edge.to_csv(output_dir / "executable_edge_buckets.csv", index=False)
    sensitivity.to_csv(output_dir / "sensitivity_buckets.csv", index=False)
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
    for col in ["target_fillable", "winner", "strategy_gate_passed"]:
        work[col] = parse_bool(work[col])

    work["market_id"] = work["condition_id"]
    work["side"] = work["outcome"]
    work["strategy_instance"] = work["symbol"]
    work["tau"] = work["seconds_to_end"].astype(float)
    work["day_cst"] = ((work["ts_ns"] + CST_OFFSET_NS) // DAY_NS).astype("Int64")

    work["S"] = work["current_price_micros"] / 1_000_000.0
    work["K"] = work["anchor_price_micros"] / 1_000_000.0
    work["moneyness"] = np.log(work["S"] / work["K"])

    work["market_q_mid"] = clip_prob(work["market_mid_micros"] / 1_000_000.0)
    work["p_raw"] = clip_prob(work["p_model_micros"] / 1_000_000.0)
    work["best_bid"] = work["best_bid_price_micros"] / 1_000_000.0
    work["best_ask"] = work["best_ask_price_micros"] / 1_000_000.0
    work["spread"] = work["spread_micros"] / 1_000_000.0
    work["avg_fill_1u"] = work["avg_fill_1u_micros"] / 1_000_000.0
    work["executable_ask"] = work["target_avg_fill_price_micros"] / 1_000_000.0
    work["fee"] = work["fee_micros_per_share"] / 1_000_000.0
    work["shares_1u"] = work["target_shares_micros"] / 1_000_000.0
    work["pnl_share"] = work["realized_pnl_per_share_micros"] / 1_000_000.0
    work["pnl_if_buy_1u"] = work["pnl_share"] * work["shares_1u"]

    work["sigma_60"] = work["sigma_60_micros"] / 1_000_000.0
    work["sigma_180"] = work["sigma_180_micros"] / 1_000_000.0
    work["sigma_floor"] = work.groupby(["coin", "horizon_seconds"], observed=True)[
        "sigma_180"
    ].transform(lambda s: s.quantile(0.25))
    work["sigma_eff"] = work[["sigma_60", "sigma_180", "sigma_floor"]].max(axis=1)

    tau_for_calc = work["tau"].clip(lower=1).to_numpy(dtype=float)
    sigma_eff = work["sigma_eff"].to_numpy(dtype=float)
    denom = sigma_eff * np.sqrt(tau_for_calc)
    d_yes = safe_divide(work["moneyness"].to_numpy(dtype=float), denom)
    q_yes = clip_prob(ndtr(d_yes))
    work["q_yes_sigma_eff"] = q_yes
    work["q_side_sigma_eff"] = np.where(work["side"] == "YES", q_yes, 1.0 - q_yes)
    work["residual_q"] = work["q_side_sigma_eff"] - work["market_q_mid"]
    work["executable_edge"] = work["q_side_sigma_eff"] - work["executable_ask"] - work["fee"]
    pdf = np.exp(-0.5 * np.square(np.nan_to_num(d_yes, nan=0.0))) / SQRT_2PI
    work["sensitivity"] = safe_divide(pdf, denom)
    work.loc[~np.isfinite(d_yes), "sensitivity"] = np.nan
    work["final_outcome"] = work["winner"].astype(int)

    work["option_valid"] = (
        np.isfinite(work["moneyness"])
        & np.isfinite(work["sigma_eff"])
        & (work["sigma_eff"] > 0)
        & np.isfinite(work["tau"])
        & (work["tau"] > 0)
        & work["market_q_mid"].notna()
        & work["q_side_sigma_eff"].notna()
    )
    add_buckets(work)
    return work


def parse_bool(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.fillna(False)
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def clip_prob(values, fill_nan: bool = False) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    if fill_nan:
        arr = np.nan_to_num(arr, nan=0.5, posinf=1 - 1e-6, neginf=1e-6)
    return np.clip(arr, 1e-6, 1 - 1e-6)


def safe_divide(numerator, denominator) -> np.ndarray:
    num = np.asarray(numerator, dtype=float)
    den = np.asarray(denominator, dtype=float)
    out = np.full_like(num, np.nan, dtype=float)
    mask = np.isfinite(num) & np.isfinite(den) & (np.abs(den) > 1e-12)
    out[mask] = num[mask] / den[mask]
    return out


def add_buckets(work: pd.DataFrame) -> None:
    q_bins = [0.0, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0]
    q_labels = ["0-5", "5-10", "10-20", "20-30", "30-40", "40-50", "50-60", "60-70", "70-80", "80-90", "90-95", "95-100"]
    work["q_sigma_eff_bucket"] = pd.cut(
        work["q_side_sigma_eff"], bins=q_bins, labels=q_labels, include_lowest=True, right=False
    )
    work["p_raw_bucket"] = pd.cut(
        work["p_raw"], bins=q_bins, labels=q_labels, include_lowest=True, right=False
    )
    work["market_q_bucket"] = pd.cut(
        work["market_q_mid"], bins=q_bins, labels=q_labels, include_lowest=True, right=False
    )
    work["residual_q_bucket"] = pd.cut(
        work["residual_q"],
        bins=[-np.inf, -0.2, -0.12, -0.08, -0.04, -0.02, 0, 0.02, 0.04, 0.08, 0.12, 0.2, np.inf],
        labels=["<-20c", "-20--12c", "-12--8c", "-8--4c", "-4--2c", "-2-0c", "0-2c", "2-4c", "4-8c", "8-12c", "12-20c", ">20c"],
        right=False,
    )
    work["executable_edge_bucket"] = pd.cut(
        work["executable_edge"],
        bins=[-np.inf, -0.12, -0.08, -0.04, -0.02, 0, 0.02, 0.04, 0.06, 0.08, 0.12, np.inf],
        labels=["<-12c", "-12--8c", "-8--4c", "-4--2c", "-2-0c", "0-2c", "2-4c", "4-6c", "6-8c", "8-12c", ">12c"],
        right=False,
    )
    work["sensitivity_bucket"] = pd.cut(
        work["sensitivity"],
        bins=[-np.inf, 50, 100, 200, 400, 800, 1600, np.inf],
        labels=["<=50", "50-100", "100-200", "200-400", "400-800", "800-1600", ">1600"],
        right=False,
    )


def write_snapshot_layer(df: pd.DataFrame, path: Path) -> None:
    cols = [
        "coin",
        "horizon_seconds",
        "market_id",
        "asset_id",
        "side",
        "ts_ns",
        "tau",
        "S",
        "K",
        "moneyness",
        "market_q_mid",
        "executable_ask",
        "avg_fill_1u",
        "fee",
        "sigma_60",
        "sigma_180",
        "sigma_floor",
        "sigma_eff",
        "q_yes_sigma_eff",
        "q_side_sigma_eff",
        "residual_q",
        "executable_edge",
        "sensitivity",
        "target_fillable",
        "strategy_gate_passed",
        "strategy_instance",
        "final_outcome",
        "pnl_if_buy_1u",
        "p_raw",
        "spread",
        "best_bid",
        "best_ask",
    ]
    df.loc[df["option_valid"], cols].to_parquet(path, index=False, compression="zstd")


def build_q_sigma_eff_calibration(df: pd.DataFrame) -> pd.DataFrame:
    data = df[df["option_valid"]].copy()
    rows = []
    for model_name, prob_col, bucket_col in [
        ("q_sigma_eff", "q_side_sigma_eff", "q_sigma_eff_bucket"),
        ("p_raw_recorded", "p_raw", "p_raw_bucket"),
        ("market_mid", "market_q_mid", "market_q_bucket"),
    ]:
        work = data.copy()
        work["model"] = model_name
        work["model_prob"] = work[prob_col]
        work["prob_bucket"] = work[bucket_col].astype(str)
        rows.append(calibration_summary(work, ["coin", "horizon_seconds", "side", "model", "prob_bucket"]))
        all_bucket = calibration_summary(work, ["coin", "horizon_seconds", "side", "model"])
        if not all_bucket.empty:
            all_bucket["prob_bucket"] = "ALL"
            rows.append(all_bucket)
    out = concat_rows(rows)
    if out.empty:
        return out
    return out.sort_values(["coin", "horizon_seconds", "side", "model", "prob_bucket"])


def build_market_residual_buckets(df: pd.DataFrame) -> pd.DataFrame:
    return scoped_bucket_summary(
        df,
        bucket_cols=["residual_q_bucket"],
        sort_cols=["scope", "coin", "horizon_seconds", "side", "residual_q_bucket"],
    )


def build_executable_edge_buckets(df: pd.DataFrame) -> pd.DataFrame:
    return scoped_bucket_summary(
        df,
        bucket_cols=["executable_edge_bucket"],
        sort_cols=["scope", "coin", "horizon_seconds", "side", "executable_edge_bucket"],
    )


def build_sensitivity_buckets(df: pd.DataFrame) -> pd.DataFrame:
    return scoped_bucket_summary(
        df,
        bucket_cols=["sensitivity_bucket"],
        sort_cols=["scope", "coin", "horizon_seconds", "side", "sensitivity_bucket"],
    )


def scoped_bucket_summary(df: pd.DataFrame, bucket_cols: list[str], sort_cols: list[str]) -> pd.DataFrame:
    data = df[
        df["option_valid"]
        & df["target_fillable"]
        & df["executable_ask"].notna()
        & df["fee"].notna()
        & df["pnl_if_buy_1u"].notna()
    ].copy()
    frames = []
    for scope, scoped in [
        ("all_fillable", data),
        ("gate_passed_fillable", data[data["strategy_gate_passed"]]),
    ]:
        frames.append(
            performance_summary(
                scoped,
                {"scope": scope},
                ["coin", "horizon_seconds", "side"] + bucket_cols,
            )
        )
    out = concat_rows(frames)
    if out.empty:
        return out
    return out.sort_values(sort_cols)


def calibration_summary(data: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows = []
    for keys, group in data.groupby(group_cols, observed=True, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        y = group["final_outcome"].to_numpy(dtype=float)
        p = clip_prob(group["model_prob"].to_numpy(dtype=float))
        row = dict(zip(group_cols, keys))
        row.update(
            {
                "sample_count": int(len(group)),
                "unique_market_count": int(group["market_id"].nunique()),
                "unique_day_count": int(group["day_cst"].nunique()),
                "avg_prob": float(np.mean(p)),
                "realized_win_rate": float(np.mean(y)),
                "calibration_error_win_minus_prob": float(np.mean(y) - np.mean(p)),
                "brier": float(np.mean(np.square(p - y))),
                "log_loss": mean_log_loss(y, p),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def performance_summary(data: pd.DataFrame, config: dict, group_cols: list[str]) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame()
    rows = []
    for keys, group in data.groupby(group_cols, observed=True, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(config)
        row.update(dict(zip(group_cols, keys)))
        row.update(performance_metrics(group))
        rows.append(row)
    return pd.DataFrame(rows)


def performance_metrics(group: pd.DataFrame) -> dict:
    daily = group.groupby("day_cst", observed=True)["pnl_if_buy_1u"].sum()
    market = group.groupby("market_id", observed=True)["pnl_if_buy_1u"].sum()
    total_abs_market = float(market.abs().sum())
    return {
        "order_count": int(len(group)),
        "unique_market_count": int(group["market_id"].nunique()),
        "unique_day_count": int(group["day_cst"].nunique()),
        "win_rate": float(group["final_outcome"].mean()),
        "avg_market_q_mid": float(group["market_q_mid"].mean()),
        "avg_q_side_sigma_eff": float(group["q_side_sigma_eff"].mean()),
        "avg_residual_q": float(group["residual_q"].mean()),
        "avg_executable_ask": float(group["executable_ask"].mean()),
        "avg_fee": float(group["fee"].mean()),
        "avg_executable_edge": float(group["executable_edge"].mean()),
        "avg_sensitivity": float(group["sensitivity"].mean()),
        "realized_ev_win_minus_ask_fee": float(
            group["final_outcome"].mean() - group["executable_ask"].mean() - group["fee"].mean()
        ),
        "total_pnl_usdc": float(group["pnl_if_buy_1u"].sum()),
        "avg_pnl_usdc_per_order": float(group["pnl_if_buy_1u"].mean()),
        "positive_day_count": int((daily > 0).sum()),
        "min_daily_pnl_usdc": float(daily.min()) if not daily.empty else math.nan,
        "max_daily_pnl_usdc": float(daily.max()) if not daily.empty else math.nan,
        "max_drawdown_usdc": max_drawdown(daily),
        "top5_abs_market_pnl_share": float(
            market.abs().sort_values(ascending=False).head(5).sum() / total_abs_market
        )
        if total_abs_market > 0
        else math.nan,
    }


def mean_log_loss(y: np.ndarray, p: np.ndarray) -> float:
    p = clip_prob(p)
    return float(np.mean(-(y * np.log(p) + (1.0 - y) * np.log1p(-p))))


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


def write_manifest(output_dir: Path, args: argparse.Namespace, df: pd.DataFrame, paths: list[str]) -> None:
    manifest = {
        "schema_version": 1,
        "objective": "EXP-OPTION-001 thin digital option layer",
        "snapshot_count": len(paths),
        "pm5m_snapshot_glob": args.pm5m_snapshot_glob,
        "pm15m_snapshot_glob": args.pm15m_snapshot_glob,
        "rows": {
            "total": int(len(df)),
            "option_valid": int(df["option_valid"].sum()),
            "fillable": int((df["option_valid"] & df["target_fillable"]).sum()),
            "gate_passed_fillable": int(
                (df["option_valid"] & df["target_fillable"] & df["strategy_gate_passed"]).sum()
            ),
        },
        "sigma_eff": "max(sigma_60, sigma_180, per coin/horizon 25th percentile sigma_180 floor)",
        "tau_unit": "seconds",
        "outputs": [
            "thin_option_snapshots.parquet",
            "q_sigma_eff_calibration.csv",
            "market_residual_buckets.csv",
            "executable_edge_buckets.csv",
            "sensitivity_buckets.csv",
            "manifest.json",
        ],
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
