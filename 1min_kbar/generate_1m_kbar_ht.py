#!/usr/bin/env python3
"""
根据华泰 Insight L2 tick 快照生成聚宽口径的 1 分钟 K 线。

华泰 tick 数据格式与聚宽 tick_l1 本质相同（来自同一交易所数据源），
只是列名、单位和文件组织方式不同：

    华泰                        聚宽 tick_l1
    ─────────────────────────  ─────────────────
    Price (float32, ×10000)    current (float64, 元)
    High  (float32, ×10000)    high (float64, 元)
    Low   (float32, ×10000)    low (float64, 元)
    AccVolume (int64, 股)      volume (float64, 股)
    AccTurnover (int64, 元)    money (float64, 元)
    Symbol (int32) + exchange  code (str, e.g. '000001.XSHE')
    Time (int32, HHMMSSmmm)    datetime (timestamp)

合成规则与 generate_1m_kbar.py 完全一致（聚宽口径）。
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


MORNING_START = "09:30:00"
MORNING_END = "11:30:00"
AFTERNOON_START = "13:00:00"
AFTERNOON_END = "15:00:00"
CALL_AUCTION_START = "09:25:00"
CLOSE_CARRY_END = "15:05:00"


# ─────────────────── 华泰数据加载与转换 ───────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="读取华泰 Insight L2 tick 数据，按聚宽口径生成 1 分钟 K 线。"
    )
    parser.add_argument("--date", default="2026-03-17", help="交易日，格式 YYYY-MM-DD。")
    parser.add_argument(
        "--tick-dir",
        default="/data/tickl2_data_ht2_raw",
        help="华泰 tick 数据根目录，默认 /data/tickl2_data_ht2_raw",
    )
    parser.add_argument(
        "--prev-1m-dir",
        default="/nkd_shared/assets/stock/price/price_1m",
        help="聚宽 price_1m 目录，用于读取前一交易日 close。",
    )
    parser.add_argument(
        "--universe-path",
        default=None,
        help=(
            "可选的股票全集 parquet。若提供，会用其中的 code 作为输出股票池。"
            "默认优先使用 price_1m/{date}.parquet；若不存在，则退化为 tick 中出现过的股票。"
        ),
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="输出 parquet 路径，默认使用 ./generated_1m_ht_{date}.parquet",
    )
    parser.add_argument(
        "--code",
        nargs="*",
        default=None,
        help="可选，只生成指定股票，例如 000001.XSHE 600000.XSHG",
    )
    return parser.parse_args()


def _ht_time_to_datetime(time_col: pd.Series, date_str: str) -> pd.Series:
    """
    将华泰 Time (int32, HHMMSSmmm) 转为 pandas Timestamp。

    格式：HH * 10_000_000 + MM * 100_000 + SS * 1_000 + ms
    例如：93015000 → 09:30:15.000
    """
    t = time_col.astype(np.int64)
    hh = t // 10_000_000
    mm = (t % 10_000_000) // 100_000
    ss = (t % 100_000) // 1_000
    ms = t % 1_000
    base = pd.Timestamp(date_str)
    delta = pd.to_timedelta(hh * 3600 + mm * 60 + ss, unit="s") + pd.to_timedelta(ms, unit="ms")
    return base + delta


def _ht_symbol_to_code(symbol: pd.Series, exchange: pd.Series) -> pd.Series:
    """
    将华泰 Symbol (int) + exchange ('SZ'/'SH') 转为聚宽风格代码。

    例如：Symbol=1, exchange='SZ' → '000001.XSHE'
          Symbol=600000, exchange='SH' → '600000.XSHG'
    """
    sym_str = symbol.astype(str).str.zfill(6)
    suffix = exchange.map({"SZ": ".XSHE", "SH": ".XSHG"})
    return sym_str + suffix


def load_ht_tick(tick_dir: Path, date_str: str, codes: list[str] | None) -> pd.DataFrame:
    """
    读取华泰某日全部 tick parquet 文件，转换为聚宽 tick_l1 等价格式。

    输出列：datetime, code, current, high, low, volume, money
    """
    date_dir = tick_dir / date_str
    date_nodash = date_str.replace("-", "")
    files = sorted(date_dir.glob(f"{date_nodash}_tick_data_*.parquet"))
    if not files:
        raise FileNotFoundError(f"找不到华泰 tick 文件: {date_dir}/{date_nodash}_tick_data_*.parquet")

    cols = ["Symbol", "exchange", "Time", "Price", "High", "Low", "AccVolume", "AccTurnover"]
    frames = []
    for f in files:
        df = pd.read_parquet(f, columns=cols)
        frames.append(df)
    raw = pd.concat(frames, ignore_index=True)

    tick = pd.DataFrame()
    tick["datetime"] = _ht_time_to_datetime(raw["Time"], date_str)
    tick["code"] = _ht_symbol_to_code(raw["Symbol"], raw["exchange"])
    tick["current"] = raw["Price"].astype(np.float64) / 10000.0
    tick["high"] = raw["High"].astype(np.float64) / 10000.0
    tick["low"] = raw["Low"].astype(np.float64) / 10000.0
    tick["volume"] = raw["AccVolume"].astype(np.float64)
    tick["money"] = raw["AccTurnover"].astype(np.float64)

    if codes:
        tick = tick[tick["code"].isin(codes)].copy()
    tick = tick.sort_values(["code", "datetime"], kind="stable").reset_index(drop=True)
    return tick


# ─────────────────── 以下为合成逻辑（与 generate_1m_kbar.py 完全一致） ───────────────────


def trading_minutes(date_str: str) -> pd.DatetimeIndex:
    morning = pd.date_range(f"{date_str} 09:31:00", f"{date_str} 11:30:00", freq="1min")
    afternoon = pd.date_range(f"{date_str} 13:01:00", f"{date_str} 15:00:00", freq="1min")
    return morning.append(afternoon)


def load_prev_day_close(prev_1m_dir: Path, date_str: str, codes: list[str] | None) -> pd.DataFrame:
    date_files = sorted(p for p in prev_1m_dir.glob("*.parquet") if p.stem < date_str)
    if not date_files:
        return pd.DataFrame(columns=["code", "prev_day_close"])

    prev_price = pd.read_parquet(date_files[-1], columns=["datetime", "code", "close"])
    prev_price["datetime"] = pd.to_datetime(prev_price["datetime"])
    if codes:
        prev_price = prev_price[prev_price["code"].isin(codes)].copy()
    prev_close = (
        prev_price.sort_values(["code", "datetime"], kind="stable")
        .groupby("code", sort=False)
        .tail(1)[["code", "close"]]
        .rename(columns={"close": "prev_day_close"})
    )
    return prev_close


def load_universe_codes(
    tick: pd.DataFrame,
    universe_path: Path | None,
    prev_day_close: pd.DataFrame,
    codes: list[str] | None,
) -> list[str]:
    code_set = set(tick["code"])
    if universe_path is not None and universe_path.exists():
        universe = pd.read_parquet(universe_path, columns=["code"])
        code_set |= set(universe["code"])
    code_set |= set(prev_day_close["code"])
    if codes:
        code_set &= set(codes)
    return sorted(code_set)


def is_exact_minute(ts: pd.Series) -> pd.Series:
    return (
        ts.dt.second.eq(0)
        & ts.dt.microsecond.eq(0)
        & ts.dt.nanosecond.eq(0)
    )


def assign_bar_end(tick: pd.DataFrame) -> pd.Series:
    ts = tick["datetime"]
    exact_minute = is_exact_minute(ts)
    session_end = ts.dt.strftime("%H:%M:%S").isin([MORNING_END, AFTERNOON_END]) & exact_minute
    bar_end = ts.dt.floor("min") + pd.Timedelta(minutes=1)
    bar_end = bar_end.where(~session_end, ts)
    return bar_end


def build_baseline(tick: pd.DataFrame, date_str: str) -> pd.DataFrame:
    cutoff = pd.Timestamp(f"{date_str} {CALL_AUCTION_START}")
    pre_open = tick[tick["datetime"] < cutoff]
    baseline = (
        pre_open.groupby("code", sort=False)[["current", "volume", "money"]]
        .last()
        .rename(
            columns={
                "current": "baseline_close",
                "volume": "baseline_volume",
                "money": "baseline_money",
            }
        )
    )
    return baseline


def filter_trading_ticks(tick: pd.DataFrame, date_str: str) -> pd.DataFrame:
    call_auction = tick["datetime"].between(
        pd.Timestamp(f"{date_str} {CALL_AUCTION_START}"),
        pd.Timestamp(f"{date_str} {MORNING_START}") - pd.Timedelta(microseconds=1),
        inclusive="both",
    )
    morning = tick["datetime"].between(
        pd.Timestamp(f"{date_str} {MORNING_START}"),
        pd.Timestamp(f"{date_str} {MORNING_END}"),
        inclusive="both",
    )
    afternoon = tick["datetime"].between(
        pd.Timestamp(f"{date_str} {AFTERNOON_START}"),
        pd.Timestamp(f"{date_str} {AFTERNOON_END}"),
        inclusive="both",
    )
    return tick[call_auction | morning | afternoon].copy()


def add_changed_flags(tick: pd.DataFrame) -> pd.DataFrame:
    tick = tick.copy()
    tick["prev_high"] = tick.groupby("code", sort=False)["high"].shift()
    tick["prev_low"] = tick.groupby("code", sort=False)["low"].shift()
    tick["high_changed"] = tick["high"] != tick["prev_high"]
    tick["low_changed"] = tick["low"] != tick["prev_low"]
    return tick


def add_bar_end_and_auction_bucket(tick: pd.DataFrame, date_str: str) -> pd.DataFrame:
    tick = tick.copy()
    tick["bar_end"] = assign_bar_end(tick)
    call_auction_mask = tick["datetime"].between(
        pd.Timestamp(f"{date_str} {CALL_AUCTION_START}"),
        pd.Timestamp(f"{date_str} {MORNING_START}") - pd.Timedelta(microseconds=1),
        inclusive="both",
    )
    tick.loc[call_auction_mask, "bar_end"] = pd.Timestamp(f"{date_str} 09:31:00")
    return tick


def carry_session_end_ticks(raw_tick: pd.DataFrame, date_str: str) -> pd.DataFrame:
    morning_end = pd.Timestamp(f"{date_str} {MORNING_END}")
    afternoon_start = pd.Timestamp(f"{date_str} {AFTERNOON_START}")
    afternoon_end = pd.Timestamp(f"{date_str} {AFTERNOON_END}")
    close_carry_end = pd.Timestamp(f"{date_str} {CLOSE_CARRY_END}")

    midday = raw_tick[(raw_tick["datetime"] > morning_end) & (raw_tick["datetime"] < afternoon_start)].copy()
    close = raw_tick[(raw_tick["datetime"] > afternoon_end) & (raw_tick["datetime"] <= close_carry_end)].copy()

    carry_frames: list[pd.DataFrame] = []
    if not midday.empty:
        midday = midday.sort_values(["code", "datetime"], kind="stable").groupby("code", sort=False).head(1).copy()
        midday["bar_end"] = morning_end
        carry_frames.append(midday)

    if not close.empty:
        close = close.sort_values(["code", "datetime"], kind="stable").groupby("code", sort=False).head(1).copy()
        close["bar_end"] = afternoon_end
        carry_frames.append(close)

    if not carry_frames:
        return raw_tick.iloc[0:0].copy()

    carry = pd.concat(carry_frames, ignore_index=True)
    carry["is_carry_tick"] = True
    return carry


def select_price_ticks(tick: pd.DataFrame) -> pd.DataFrame:
    price_ticks = tick.copy()
    prev_volume = price_ticks.groupby("code", sort=False)["volume"].shift()
    prev_money = price_ticks.groupby("code", sort=False)["money"].shift()
    changed = (price_ticks["volume"] != prev_volume) | (price_ticks["money"] != prev_money)
    price_ticks = price_ticks[changed.fillna(True)].copy()
    price_ticks = price_ticks[price_ticks["current"] > 0].copy()

    if "is_carry_tick" in tick.columns:
        base_valid = tick[tick["current"] > 0].copy()
        price_ticks["is_pre_end_tick"] = price_ticks["datetime"] <= price_ticks["bar_end"]
        carry_only = (
            price_ticks.groupby(["code", "bar_end"], sort=False)
            .agg(has_pre_end_tick=("is_pre_end_tick", "any"), has_carry_tick=("is_carry_tick", "any"))
            .reset_index()
        )
        carry_only = carry_only[carry_only["has_carry_tick"] & ~carry_only["has_pre_end_tick"]]
        if not carry_only.empty:
            anchors = base_valid.merge(carry_only[["code", "bar_end"]], on=["code", "bar_end"], how="inner")
            anchors = anchors[anchors["datetime"] <= anchors["bar_end"]].copy()
            if not anchors.empty:
                anchors = (
                    anchors.sort_values(["code", "bar_end", "datetime"], kind="stable")
                    .groupby(["code", "bar_end"], sort=False)
                    .tail(1)
                )
                price_ticks = pd.concat([price_ticks, anchors], ignore_index=True)
                price_ticks = price_ticks.sort_values(["code", "bar_end", "datetime"], kind="stable")
        price_ticks = price_ticks.drop(columns="is_pre_end_tick", errors="ignore")

    return price_ticks


def build_empty_frame(codes: list[str], minutes: pd.DatetimeIndex) -> pd.DataFrame:
    full_index = pd.MultiIndex.from_product([codes, minutes], names=["code", "datetime"])
    return full_index.to_frame(index=False)


def aggregate_intraday_bars(
    tick: pd.DataFrame,
    baseline: pd.DataFrame,
    codes: list[str],
    minutes: pd.DatetimeIndex,
) -> pd.DataFrame:
    agg_vm = (
        tick.groupby(["code", "bar_end"], sort=False)
        .agg(
            cum_volume=("volume", "last"),
            cum_money=("money", "last"),
            close_all_ticks=("current", "last"),
        )
        .reset_index()
    )

    price_ticks = select_price_ticks(tick)
    agg_price = (
        price_ticks.groupby(["code", "bar_end"], sort=False)
        .agg(
            open=("current", "first"),
            high=("current", "max"),
            low=("current", "min"),
            close=("current", "last"),
        )
        .reset_index()
    )

    high_updates = (
        tick[tick["high_changed"] & (tick["high"] > 0)]
        .groupby(["code", "bar_end"], sort=False)["high"]
        .max()
        .reset_index()
        .rename(columns={"high": "high_update"})
    )
    low_updates = (
        tick[tick["low_changed"] & (tick["low"] > 0)]
        .groupby(["code", "bar_end"], sort=False)["low"]
        .min()
        .reset_index()
        .rename(columns={"low": "low_update"})
    )

    agg = agg_vm.merge(agg_price, on=["code", "bar_end"], how="left")
    agg = agg.merge(high_updates, on=["code", "bar_end"], how="left")
    agg = agg.merge(low_updates, on=["code", "bar_end"], how="left")
    agg["high"] = agg[["high", "high_update"]].max(axis=1, skipna=True)
    agg["low"] = agg[["low", "low_update"]].min(axis=1, skipna=True)

    bars = build_empty_frame(codes, minutes)
    bars = bars.merge(
        agg.rename(columns={"bar_end": "datetime"}),
        on=["code", "datetime"],
        how="left",
    )
    bars = bars.merge(baseline, on="code", how="left")

    bars["cum_volume"] = bars.groupby("code", sort=False)["cum_volume"].ffill()
    bars["cum_money"] = bars.groupby("code", sort=False)["cum_money"].ffill()
    bars["close_all_ticks"] = bars.groupby("code", sort=False)["close_all_ticks"].ffill()
    bars["close_all_ticks"] = bars["close_all_ticks"].fillna(bars["baseline_close"])

    bars["close"] = bars.groupby("code", sort=False)["close"].ffill()
    bars["close"] = bars["close"].fillna(bars["close_all_ticks"])

    missing_ohlc = bars["open"].isna()
    bars.loc[missing_ohlc, "open"] = bars.loc[missing_ohlc, "close"]
    bars.loc[missing_ohlc, "high"] = bars.loc[missing_ohlc, "close"]
    bars.loc[missing_ohlc, "low"] = bars.loc[missing_ohlc, "close"]

    prev_cum_volume = bars.groupby("code", sort=False)["cum_volume"].shift()
    prev_cum_money = bars.groupby("code", sort=False)["cum_money"].shift()
    prev_cum_volume = prev_cum_volume.fillna(bars["baseline_volume"]).fillna(0)
    prev_cum_money = prev_cum_money.fillna(bars["baseline_money"]).fillna(0)

    bars["cum_volume"] = bars["cum_volume"].fillna(prev_cum_volume)
    bars["cum_money"] = bars["cum_money"].fillna(prev_cum_money)

    bars["volume"] = (bars["cum_volume"] - prev_cum_volume).clip(lower=0)
    bars["money"] = (bars["cum_money"] - prev_cum_money).clip(lower=0)

    return bars[["datetime", "code", "open", "close", "high", "low", "money", "volume"]].copy()


def apply_morning_end_rule(result: pd.DataFrame, raw_tick: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """
    11:30 边界特殊处理。

    当 11:30 bar 内 11:30:00 之前没有成交（bar 内所有快照 volume 不变），
    聚宽会用 11:30:00 之前的静态快照价格作为 open，而不是 11:30:00 的成交价。
    类似 15:00 的收盘撮合 bar 逻辑。
    """
    morning_end_dt = pd.Timestamp(f"{date_str} {MORNING_END}")
    bar_start = morning_end_dt - pd.Timedelta(minutes=1)  # 11:29:00

    raw = raw_tick.sort_values(["code", "datetime"], kind="stable").copy()

    # 11:30 bar 内、11:30:00 之前的有效快照
    pre_end = raw[
        (raw["datetime"] >= bar_start)
        & (raw["datetime"] < morning_end_dt)
        & (raw["current"] > 0)
    ]
    if pre_end.empty:
        return result

    # 只处理窗口内 volume 不变的股票（= 没有 intra-minute trade）
    vol_agg = pre_end.groupby("code", sort=False)["volume"].agg(["first", "last"])
    no_trade_codes = set(vol_agg[vol_agg["first"] == vol_agg["last"]].index)
    if not no_trade_codes:
        return result

    first_snapshot = (
        pre_end[pre_end["code"].isin(no_trade_codes)]
        .groupby("code", sort=False)["current"]
        .first()
        .reset_index()
        .rename(columns={"current": "snapshot_open"})
    )

    bar_11_30 = result[result["datetime"] == morning_end_dt].copy()
    bar_11_30 = bar_11_30.merge(first_snapshot, on="code", how="inner")

    need_fix = bar_11_30[
        ~np.isclose(
            bar_11_30["open"].astype(np.float64),
            bar_11_30["snapshot_open"].astype(np.float64),
            atol=1e-6,
        )
    ]
    if need_fix.empty:
        return result

    override = need_fix[["datetime", "code", "snapshot_open", "close"]].copy()
    close_f64 = override["close"].astype(np.float64)
    snap_f64 = override["snapshot_open"].astype(np.float64)
    override["open"] = snap_f64
    override["high"] = np.maximum(snap_f64, close_f64)
    override["low"] = np.minimum(snap_f64, close_f64)
    override = override[["datetime", "code", "open", "high", "low"]]

    result = result.set_index(["datetime", "code"])
    result.update(override.set_index(["datetime", "code"]))
    return result.reset_index()


def apply_close_auction_rule(result: pd.DataFrame, raw_tick: pd.DataFrame, date_str: str) -> pd.DataFrame:
    close_bar_dt = pd.Timestamp(f"{date_str} {AFTERNOON_END}")
    raw = raw_tick.sort_values(["code", "datetime"], kind="stable").copy()

    preclose = raw[(raw["datetime"] < close_bar_dt) & (raw["current"] > 0)].copy()
    preclose = (
        preclose.groupby("code", sort=False)
        .tail(1)[["code", "current", "volume", "money"]]
        .rename(
            columns={
                "current": "preclose_price",
                "volume": "preclose_volume",
                "money": "preclose_money",
            }
        )
    )

    postclose = raw[
        (raw["datetime"] >= close_bar_dt)
        & (raw["datetime"] <= pd.Timestamp(f"{date_str} {CLOSE_CARRY_END}"))
        & (raw["current"] > 0)
    ].copy()
    if preclose.empty or postclose.empty:
        return result

    postclose = postclose.merge(
        preclose[["code", "preclose_price", "preclose_volume", "preclose_money"]],
        on="code",
        how="inner",
    )
    postclose["trade_changed_from_preclose"] = (
        (postclose["volume"] > postclose["preclose_volume"])
        | (postclose["money"] > postclose["preclose_money"])
    )

    changed_postclose = (
        postclose[postclose["trade_changed_from_preclose"]]
        .sort_values(["code", "datetime"], kind="stable")
        .groupby("code", sort=False)
        .head(1)
    )
    exact_close_price_change = (
        postclose[
            (~postclose["trade_changed_from_preclose"])
            & (postclose["datetime"] == close_bar_dt)
            & (postclose["current"] != postclose["preclose_price"])
        ]
        .sort_values(["code", "datetime"], kind="stable")
        .groupby("code", sort=False)
        .head(1)
    )

    auction = changed_postclose.combine_first(exact_close_price_change).reset_index(drop=True)
    if auction.empty:
        return result

    auction = (
        auction[["code", "current", "volume", "money"]]
        .drop_duplicates(subset=["code"], keep="first")
        .rename(
            columns={
                "current": "auction_price",
                "volume": "auction_volume",
                "money": "auction_money",
            }
        )
    )

    override = preclose.merge(auction, on="code", how="inner")
    override["datetime"] = close_bar_dt
    override["open"] = override["preclose_price"]
    override["close"] = override["auction_price"]
    override["high"] = override[["preclose_price", "auction_price"]].max(axis=1)
    override["low"] = override[["preclose_price", "auction_price"]].min(axis=1)
    override["volume"] = (override["auction_volume"] - override["preclose_volume"]).clip(lower=0)
    override["money"] = (override["auction_money"] - override["preclose_money"]).clip(lower=0)
    override = override[["datetime", "code", "open", "close", "high", "low", "money", "volume"]]

    result = result.set_index(["datetime", "code"])
    result.update(override.set_index(["datetime", "code"]))
    return result.reset_index()


def apply_prev_close_fill(result: pd.DataFrame, raw_tick: pd.DataFrame, prev_day_close: pd.DataFrame) -> pd.DataFrame:
    if prev_day_close.empty:
        return result

    valid_raw = raw_tick[(raw_tick["current"] > 0) | (raw_tick["volume"] > 0) | (raw_tick["money"] > 0)].copy()
    first_valid = (
        valid_raw.groupby("code", sort=False)["datetime"]
        .min()
        .reset_index()
        .rename(columns={"datetime": "first_valid_dt"})
    )
    if not first_valid.empty:
        first_valid["first_valid_bar_end"] = first_valid["first_valid_dt"].dt.floor("min") + pd.Timedelta(minutes=1)

    merged = result.merge(
        first_valid[["code", "first_valid_bar_end"]] if not first_valid.empty else pd.DataFrame(columns=["code", "first_valid_bar_end"]),
        on="code", how="left",
    )
    merged = merged.merge(prev_day_close, on="code", how="left")

    missing_prices = (
        (merged["open"].isna() | merged["open"].eq(0))
        & (merged["close"].isna() | merged["close"].eq(0))
        & (merged["high"].isna() | merged["high"].eq(0))
        & (merged["low"].isna() | merged["low"].eq(0))
    )

    fill_mask = (
        merged["prev_day_close"].notna()
        & merged["volume"].fillna(0).eq(0)
        & merged["money"].fillna(0).eq(0)
        & missing_prices
        & (
            (
                merged["first_valid_bar_end"].notna()
                & (merged["datetime"] < merged["first_valid_bar_end"])
            )
            | merged["first_valid_bar_end"].isna()
        )
    )
    if not fill_mask.any():
        return result

    override = merged.loc[fill_mask, ["datetime", "code", "prev_day_close"]].copy()
    for col in ["open", "close", "high", "low"]:
        override[col] = override["prev_day_close"]
    override["money"] = 0.0
    override["volume"] = 0.0
    override = override[["datetime", "code", "open", "close", "high", "low", "money", "volume"]]

    result = result.set_index(["datetime", "code"])
    result.update(override.set_index(["datetime", "code"]))
    return result.reset_index()


def normalize_output_types(result: pd.DataFrame) -> pd.DataFrame:
    result = result.copy()
    for col in ["open", "close", "high", "low"]:
        result[col] = pd.to_numeric(result[col], errors="coerce").astype(np.float32)
    result["money"] = np.floor(pd.to_numeric(result["money"], errors="coerce").fillna(0) + 1e-6).astype(np.float64)
    result["volume"] = pd.to_numeric(result["volume"], errors="coerce").fillna(0).astype(np.float64)
    return result


def generate_1m_bars(
    tick: pd.DataFrame,
    prev_day_close: pd.DataFrame,
    codes: list[str],
    date_str: str,
) -> pd.DataFrame:
    minutes = trading_minutes(date_str)

    tick = tick[tick["code"].isin(codes)].copy()
    tick = add_changed_flags(tick)
    baseline = build_baseline(tick, date_str)

    intraday_tick = filter_trading_ticks(tick, date_str)
    intraday_tick = add_bar_end_and_auction_bucket(intraday_tick, date_str)

    carry_tick = carry_session_end_ticks(tick, date_str)
    if not carry_tick.empty:
        carry_tick = carry_tick[carry_tick["code"].isin(codes)].copy()
        intraday_tick = pd.concat([intraday_tick, carry_tick], ignore_index=True)
        intraday_tick = intraday_tick.sort_values(["code", "datetime", "bar_end"], kind="stable").reset_index(drop=True)

    if "is_carry_tick" not in intraday_tick.columns:
        intraday_tick["is_carry_tick"] = False
    else:
        intraday_tick["is_carry_tick"] = intraday_tick["is_carry_tick"].astype("boolean").fillna(False).astype(bool)

    intraday_tick = intraday_tick[intraday_tick["bar_end"].isin(minutes)].copy()
    result = aggregate_intraday_bars(intraday_tick, baseline, codes, minutes)
    result = apply_morning_end_rule(result, tick, date_str)
    result = apply_close_auction_rule(result, tick, date_str)
    result = apply_prev_close_fill(result, tick, prev_day_close)
    result = normalize_output_types(result)
    return result.sort_values(["code", "datetime"], kind="stable").reset_index(drop=True)


def main() -> None:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent
    tick_dir = Path(args.tick_dir)
    prev_1m_dir = Path(args.prev_1m_dir)

    if args.output_path:
        output_path = Path(args.output_path)
    else:
        output_path = base_dir / f"generated_1m_ht_{args.date}.parquet"

    if args.universe_path:
        universe_path = Path(args.universe_path)
    else:
        default_universe = prev_1m_dir / f"{args.date}.parquet"
        universe_path = default_universe if default_universe.exists() else None

    print(f"Loading Huatai tick data from {tick_dir / args.date} ...")
    tick = load_ht_tick(tick_dir, args.date, args.code)
    print(f"  tick rows: {len(tick)}, codes: {tick['code'].nunique()}")

    prev_day_close = load_prev_day_close(prev_1m_dir, args.date, args.code)
    codes = load_universe_codes(tick, universe_path, prev_day_close, args.code)
    if not codes:
        raise ValueError("没有可生成的股票代码。请检查 tick 文件、universe_path 或 code 参数。")
    print(f"  universe codes: {len(codes)}")

    result = generate_1m_bars(tick, prev_day_close, codes, args.date)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output_path, index=False)

    print(f"output_path={output_path}")
    print(f"rows={len(result)}")
    print(result.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
