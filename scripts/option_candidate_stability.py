#!/usr/bin/env python3
import argparse
import glob
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

import option_market_anchored_model as option_model


RNG_SEED = 20260623
BOOTSTRAP_ITERS = 5000


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
    df = option_model.prepare_base(option_model.load_snapshots(paths))
    df = option_model.build_oos_joint_predictions(df, min_train_days=args.min_train_days)
    df["date_cst"] = pd.to_datetime(df["ts_ns"] + option_model.CST_OFFSET_NS).dt.date.astype(str)
    df["joint_edge"] = df["joint_edge_market_option"]

    selected = select_candidates(df)
    summary = build_candidate_summary(selected)
    daily = build_daily_pnl(selected)
    market = build_market_pnl(selected)
    bucket = build_sensitivity_edge_bucket(selected)
    stress = build_buffer_stress(selected)

    summary.to_csv(output_dir / "option003_candidate_summary.csv", index=False)
    daily.to_csv(output_dir / "option003_daily_pnl.csv", index=False)
    market.to_csv(output_dir / "option003_market_pnl.csv", index=False)
    bucket.to_csv(output_dir / "option003_sensitivity_edge_bucket.csv", index=False)
    stress.to_csv(output_dir / "option003_buffer_stress.csv", index=False)
    write_manifest(output_dir, args, paths, df, selected)


def expand_globs(patterns: list[str]) -> list[str]:
    paths: list[str] = []
    for pattern in patterns:
        paths.extend(path for path in sorted(glob.glob(pattern)) if Path(path).is_file())
    return sorted(set(paths))


def select_candidates(df: pd.DataFrame) -> pd.DataFrame:
    specs = [
        {
            "candidate_id": "BTC5_YES_OPTION_SENS_4C",
            "status": "frozen_candidate",
            "rule_family": "option_sensitivity",
            "coin": "BTC",
            "horizon_seconds": 300,
            "side": "YES",
            "threshold": 0.04,
        },
        {
            "candidate_id": "BTC15_NO_OPTION_SENS_4C",
            "status": "frozen_candidate",
            "rule_family": "option_sensitivity",
            "coin": "BTC",
            "horizon_seconds": 900,
            "side": "NO",
            "threshold": 0.04,
        },
        {
            "candidate_id": "ETH5_YES_OPTION_SENS_4C",
            "status": "frozen_candidate",
            "rule_family": "option_sensitivity",
            "coin": "ETH",
            "horizon_seconds": 300,
            "side": "YES",
            "threshold": 0.04,
        },
        {
            "candidate_id": "ETH5_NO_OPTION_SENS_4C",
            "status": "frozen_candidate",
            "rule_family": "option_sensitivity",
            "coin": "ETH",
            "horizon_seconds": 300,
            "side": "NO",
            "threshold": 0.04,
        },
        {
            "candidate_id": "ETH15_NO_OPTION_SENS_4C",
            "status": "frozen_candidate",
            "rule_family": "option_sensitivity",
            "coin": "ETH",
            "horizon_seconds": 900,
            "side": "NO",
            "threshold": 0.04,
        },
        {
            "candidate_id": "SOL5_YES_OPTION_SENS_4C",
            "status": "frozen_candidate",
            "rule_family": "option_sensitivity",
            "coin": "SOL",
            "horizon_seconds": 300,
            "side": "YES",
            "threshold": 0.04,
        },
        {
            "candidate_id": "SOL5_NO_OPTION_SENS_4C",
            "status": "frozen_candidate",
            "rule_family": "option_sensitivity",
            "coin": "SOL",
            "horizon_seconds": 300,
            "side": "NO",
            "threshold": 0.04,
        },
        {
            "candidate_id": "SOL15_YES_LOWFILL_BTCLEAD_GT10",
            "status": "reference_only",
            "rule_family": "lowfill_btc_lead",
            "coin": "SOL",
            "horizon_seconds": 900,
            "side": "YES",
            "threshold": math.nan,
        },
    ]

    frames = []
    for spec in specs:
        if spec["rule_family"] == "option_sensitivity":
            mask = (
                df["option_valid"]
                & df["q_market_option_oos"].notna()
                & (df["coin"] == spec["coin"])
                & (df["horizon_seconds"] == spec["horizon_seconds"])
                & (df["side"] == spec["side"])
                & (df["joint_edge_market_option"] >= spec["threshold"])
            )
        else:
            mask = (
                df["option_valid"]
                & (df["coin"] == "SOL")
                & (df["horizon_seconds"] == 900)
                & (df["side"] == "YES")
                & df["strategy_gate_passed"]
                & (df["executable_ask"] < 0.55)
                & (df["btc_lead_momentum_60s_bps"] > 10)
            )
        selected = cap_orders_per_market(df[mask].copy())
        if selected.empty:
            continue
        for key, value in spec.items():
            selected[key] = value
        frames.append(selected)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["edge_bucket"] = edge_bucket(out["joint_edge"])
    return out


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


def build_candidate_summary(selected: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for candidate_id, group in selected.groupby("candidate_id", sort=True):
        row = candidate_config_fields(group)
        row.update(performance_metrics(group))
        row.update(bootstrap_metrics(group))
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["status", "coin", "horizon_seconds", "side", "candidate_id"])


def build_daily_pnl(selected: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for keys, group in selected.groupby(["candidate_id", "date_cst"], sort=True):
        candidate_id, date_cst = keys
        row = candidate_config_fields(group)
        row.update(
            {
                "date_cst": date_cst,
                "orders": int(len(group)),
                "unique_markets": int(group["market_id"].nunique()),
                "win": float(group["final_outcome"].mean()),
                "avg_fill": float(group["executable_ask"].mean()),
                "avg_joint_edge": float(group["joint_edge"].mean()),
                "pnl": float(group["pnl_if_buy_1u"].sum()),
                "pnl_per_order": float(group["pnl_if_buy_1u"].mean()),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["candidate_id", "date_cst"])


def build_market_pnl(selected: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "candidate_id",
        "status",
        "rule_family",
        "coin",
        "horizon_seconds",
        "side",
        "market_id",
        "date_cst",
        "ts_ns",
        "executable_ask",
        "fee",
        "joint_edge",
        "sensitivity",
        "sensitivity_bucket",
        "edge_bucket",
        "final_outcome",
        "pnl_if_buy_1u",
    ]
    return selected[cols].sort_values(["candidate_id", "date_cst", "ts_ns"])


def build_sensitivity_edge_bucket(selected: pd.DataFrame) -> pd.DataFrame:
    rows = []
    group_cols = ["candidate_id", "sensitivity_bucket", "edge_bucket"]
    for keys, group in selected.groupby(group_cols, observed=True, sort=True):
        if group.empty:
            continue
        row = candidate_config_fields(group)
        row.update(dict(zip(group_cols, keys)))
        row.update(performance_metrics(group))
        rows.append(row)
    return pd.DataFrame(rows).sort_values(group_cols)


def build_buffer_stress(selected: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for candidate_id, group in selected.groupby("candidate_id", sort=True):
        for buffer_c in [0.0, 0.01, 0.02, 0.04]:
            stressed = group.copy()
            stressed["pnl_if_buy_1u"] = stressed["pnl_if_buy_1u"] - buffer_c * stressed["shares_1u"]
            row = candidate_config_fields(group)
            row["buffer_c_per_share"] = buffer_c
            row.update(performance_metrics(stressed))
            rows.append(row)
    return pd.DataFrame(rows).sort_values(["candidate_id", "buffer_c_per_share"])


def candidate_config_fields(group: pd.DataFrame) -> dict:
    first = group.iloc[0]
    return {
        "candidate_id": first["candidate_id"],
        "status": first["status"],
        "rule_family": first["rule_family"],
        "coin": first["coin"],
        "horizon_seconds": int(first["horizon_seconds"]),
        "side": first["side"],
        "threshold": first["threshold"],
    }


def performance_metrics(group: pd.DataFrame) -> dict:
    daily = group.groupby("date_cst", observed=True)["pnl_if_buy_1u"].sum()
    market = group.groupby("market_id", observed=True)["pnl_if_buy_1u"].sum()
    total_abs_market = float(market.abs().sum())
    return {
        "orders": int(len(group)),
        "unique_markets": int(group["market_id"].nunique()),
        "unique_days": int(group["date_cst"].nunique()),
        "win": float(group["final_outcome"].mean()),
        "avg_fill": float(group["executable_ask"].mean()),
        "avg_fee": float(group["fee"].mean()),
        "avg_joint_edge": float(group["joint_edge"].mean()),
        "avg_sensitivity": float(group["sensitivity"].mean()),
        "pnl": float(group["pnl_if_buy_1u"].sum()),
        "pnl_per_order": float(group["pnl_if_buy_1u"].mean()),
        "pos_days": int((daily > 0).sum()),
        "neg_days": int((daily < 0).sum()),
        "avg_daily_pnl": float(daily.mean()) if not daily.empty else math.nan,
        "median_daily_pnl": float(daily.median()) if not daily.empty else math.nan,
        "min_daily_pnl": float(daily.min()) if not daily.empty else math.nan,
        "max_daily_pnl": float(daily.max()) if not daily.empty else math.nan,
        "max_drawdown": max_drawdown(daily),
        "top5_abs_share": float(
            market.abs().sort_values(ascending=False).head(5).sum() / total_abs_market
        )
        if total_abs_market > 0
        else math.nan,
    }


def bootstrap_metrics(group: pd.DataFrame) -> dict:
    market_pnl = group.groupby("market_id", observed=True)["pnl_if_buy_1u"].sum().to_numpy(dtype=float)
    if len(market_pnl) == 0:
        return {
            "bootstrap_iters": 0,
            "bootstrap_mean_pnl": math.nan,
            "bootstrap_p05_pnl": math.nan,
            "bootstrap_p50_pnl": math.nan,
            "bootstrap_p95_pnl": math.nan,
            "bootstrap_positive_frac": math.nan,
        }
    rng = np.random.default_rng(RNG_SEED)
    samples = rng.choice(market_pnl, size=(BOOTSTRAP_ITERS, len(market_pnl)), replace=True).sum(axis=1)
    return {
        "bootstrap_iters": BOOTSTRAP_ITERS,
        "bootstrap_mean_pnl": float(np.mean(samples)),
        "bootstrap_p05_pnl": float(np.quantile(samples, 0.05)),
        "bootstrap_p50_pnl": float(np.quantile(samples, 0.50)),
        "bootstrap_p95_pnl": float(np.quantile(samples, 0.95)),
        "bootstrap_positive_frac": float(np.mean(samples > 0)),
    }


def max_drawdown(daily: pd.Series) -> float:
    if daily.empty:
        return math.nan
    cumulative = daily.sort_index().cumsum()
    peak = cumulative.cummax()
    return float((peak - cumulative).max())


def write_manifest(
    output_dir: Path,
    args: argparse.Namespace,
    paths: list[str],
    df: pd.DataFrame,
    selected: pd.DataFrame,
) -> None:
    manifest = {
        "schema_version": 1,
        "objective": "EXP-OPTION-003 frozen candidate stability validation",
        "snapshot_count": len(paths),
        "min_train_days": args.min_train_days,
        "bootstrap_iters": BOOTSTRAP_ITERS,
        "bootstrap_seed": RNG_SEED,
        "rows": {
            "total": int(len(df)),
            "option_valid": int(df["option_valid"].sum()),
            "joint_oos": int((df["option_valid"] & df["q_market_option_oos"].notna()).sum()),
            "selected": int(len(selected)),
            "candidate_count": int(selected["candidate_id"].nunique()) if not selected.empty else 0,
        },
        "rules": {
            "option_sensitivity": "q_market_option_oos - executable_ask - fee >= 4c, max1 per market",
            "sol15_reference": "SOL 15m YES, current gate, executable_ask < 55c, BTC lead > 10bps, max1 per market",
        },
        "outputs": [
            "option003_candidate_summary.csv",
            "option003_daily_pnl.csv",
            "option003_market_pnl.csv",
            "option003_sensitivity_edge_bucket.csv",
            "option003_buffer_stress.csv",
            "manifest.json",
        ],
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
