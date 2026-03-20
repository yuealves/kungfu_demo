#!/usr/bin/env python3
"""
用华泰 tick 合成 1min K 线并与聚宽 price_1m 对比。

用法：
    python compare_ht_1m.py --date 2026-03-17
    python compare_ht_1m.py --date 2026-03-17 --code 000001.XSHE 600000.XSHG
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from generate_1m_kbar_ht import (
    load_ht_tick,
    load_prev_day_close,
    load_universe_codes,
    generate_1m_bars,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="用华泰 tick 合成 1min K 线，与聚宽 price_1m 对比。"
    )
    parser.add_argument("--date", default="2026-03-17", help="交易日。")
    parser.add_argument(
        "--tick-dir",
        default="/data/tickl2_data_ht2_raw",
        help="华泰 tick 数据根目录。",
    )
    parser.add_argument(
        "--price-1m-dir",
        default="/nkd_shared/assets/stock/price/price_1m",
        help="聚宽 price_1m 目录。",
    )
    parser.add_argument(
        "--code",
        nargs="*",
        default=None,
        help="可选，只对比指定股票。",
    )
    parser.add_argument(
        "--output-dir",
        default="output_ht",
        help="输出目录。",
    )
    parser.add_argument(
        "--diff-limit",
        type=int,
        default=200,
        help="最多保存多少行不一致的详细记录。",
    )
    return parser.parse_args()


def compare_bars(synth: pd.DataFrame, official: pd.DataFrame) -> pd.DataFrame:
    merged = official.merge(
        synth,
        on=["datetime", "code"],
        how="outer",
        suffixes=("_jq", "_synth"),
        indicator=True,
    )
    for col in ["open", "close", "high", "low", "money", "volume"]:
        merged[f"{col}_diff"] = merged[f"{col}_synth"] - merged[f"{col}_jq"]
        merged[f"{col}_match"] = np.isclose(
            merged[f"{col}_synth"],
            merged[f"{col}_jq"],
            rtol=1e-9,
            atol=1e-6,
            equal_nan=True,
        )
    merged["all_match"] = merged[
        [f"{c}_match" for c in ["open", "close", "high", "low", "money", "volume"]]
    ].all(axis=1)
    return merged


def summarize_diff(diff: pd.DataFrame) -> pd.DataFrame:
    summary = {
        "rows_total": int(len(diff)),
        "rows_exact_match": int(diff["all_match"].sum()),
        "rows_mismatch": int((~diff["all_match"]).sum()),
        "only_in_jq": int((diff["_merge"] == "left_only").sum()),
        "only_in_synth": int((diff["_merge"] == "right_only").sum()),
    }
    for col in ["open", "close", "high", "low", "money", "volume"]:
        summary[f"{col}_mismatch_rows"] = int((~diff[f"{col}_match"]).sum())
        summary[f"{col}_max_abs_diff"] = float(diff[f"{col}_diff"].abs().max(skipna=True) or 0.0)
    return pd.DataFrame([summary])


def main() -> None:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent
    tick_dir = Path(args.tick_dir)
    price_1m_dir = Path(args.price_1m_dir)
    output_dir = (base_dir / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # 加载聚宽 price_1m 作为 ground truth
    price_1m_path = price_1m_dir / f"{args.date}.parquet"
    if not price_1m_path.exists():
        raise FileNotFoundError(f"找不到聚宽 1m 数据: {price_1m_path}")
    price_1m = pd.read_parquet(price_1m_path)
    price_1m["datetime"] = pd.to_datetime(price_1m["datetime"])
    if args.code:
        price_1m = price_1m[price_1m["code"].isin(args.code)].copy()

    # 加载华泰 tick
    print(f"Loading Huatai tick data for {args.date} ...")
    tick = load_ht_tick(tick_dir, args.date, args.code)
    print(f"  tick rows: {len(tick)}, codes: {tick['code'].nunique()}")

    # 前一交易日 close
    prev_day_close = load_prev_day_close(price_1m_dir, args.date, args.code)

    # 股票池 = 聚宽 price_1m 的 code
    codes = sorted(set(price_1m["code"]))
    print(f"  JQ price_1m codes: {len(codes)}, bars: {len(price_1m)}")

    # 合成
    print("Synthesizing 1m bars ...")
    synth = generate_1m_bars(tick, prev_day_close, codes, args.date)
    print(f"  synth bars: {len(synth)}")

    # 对比
    diff = compare_bars(synth, price_1m)
    summary = summarize_diff(diff)

    # 输出
    file_stub = f"ht.{args.date}"
    synth_path = output_dir / f"synth_1m.{file_stub}.parquet"
    diff_path = output_dir / f"diff_1m.{file_stub}.parquet"
    mismatch_csv = output_dir / f"diff_1m.{file_stub}.mismatch_top{args.diff_limit}.csv"
    summary_path = output_dir / f"summary_1m.{file_stub}.csv"

    synth.to_parquet(synth_path, index=False)
    diff.to_parquet(diff_path, index=False)
    summary.to_csv(summary_path, index=False)

    mismatches = diff.loc[~diff["all_match"]].copy()
    if not mismatches.empty:
        sort_cols = [f"{c}_diff" for c in ["open", "close", "high", "low", "money", "volume"]]
        mismatches["_sort_key"] = mismatches[sort_cols].abs().max(axis=1)
        mismatches = mismatches.sort_values(
            ["_sort_key", "code", "datetime"], ascending=[False, True, True]
        ).drop(columns="_sort_key")
        mismatches.head(args.diff_limit).to_csv(mismatch_csv, index=False)
    else:
        pd.DataFrame().to_csv(mismatch_csv, index=False)

    print(f"\nsynth_path={synth_path}")
    print(f"diff_path={diff_path}")
    print(f"summary_path={summary_path}")
    print(f"mismatch_csv={mismatch_csv}")
    print(f"\n{summary.to_string(index=False)}")


if __name__ == "__main__":
    main()
