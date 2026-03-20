#!/usr/bin/env python3

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Use JQ tick_l1 snapshots to synthesize 1-minute bars and compare them with JQ price_1m."
    )
    parser.add_argument(
        "--date",
        default="2026-03-17",
        help="Trading date, matching parquet file names under jq_data.",
    )
    parser.add_argument(
        "--tick-path",
        default=None,
        help="Explicit tick parquet path. Defaults to ./jq_data/tick_l1/{date}.parquet",
    )
    parser.add_argument(
        "--price-path",
        default=None,
        help="Explicit 1m parquet path. Defaults to ./jq_data/price_1m/{date}.parquet",
    )
    parser.add_argument(
        "--code",
        nargs="*",
        default=None,
        help="Optional stock codes to limit processing, e.g. 000001.XSHE 600000.XSHG",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory for synthesized bars and diff reports.",
    )
    parser.add_argument(
        "--boundary-mode",
        choices=("jq_doc", "prev"),
        default="jq_doc",
        help=(
            "How to treat ticks exactly on minute boundaries. "
            "'jq_doc' puts HH:MM:00 into the next bar except session-end ticks at 11:30:00/15:00:00; "
            "'prev' puts HH:MM:00 into the previous bar."
        ),
    )
    parser.add_argument(
        "--diff-limit",
        type=int,
        default=200,
        help="Maximum number of mismatched rows to save into CSV reports.",
    )
    parser.add_argument(
        "--include-call-auction-first-bar",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include 09:25:00-09:29:59 ticks into the first 09:31 bar.",
    )
    parser.add_argument(
        "--price-tick-mode",
        choices=("all", "trade"),
        default="trade",
        help="Which ticks participate in minute price synthesis: all snapshots or only ticks with changed volume/money.",
    )
    parser.add_argument(
        "--ignore-nonpositive-current",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Ignore ticks with current <= 0 when synthesizing OHLC prices.",
    )
    parser.add_argument(
        "--carry-session-end-tick",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Carry the first tick after 11:30:00 and 15:00:00 back into the ending bar.",
    )
    parser.add_argument(
        "--jq-like",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Convenience preset for a more JQ-like synthesis: "
            "include call auction in first bar, price_tick_mode=trade, ignore nonpositive current, carry session-end tick."
        ),
    )
    parser.add_argument(
        "--extrema-mode",
        choices=("current", "hybrid_updates"),
        default="hybrid_updates",
        help=(
            "How to synthesize minute high/low. 'current' uses tick.current only; "
            "'hybrid_updates' starts from current-based high/low and absorbs intra-minute updates of tick.high/tick.low."
        ),
    )
    return parser.parse_args()


def build_paths(base_dir: Path, date_str: str, tick_path: str | None, price_path: str | None) -> tuple[Path, Path]:
    tick = Path(tick_path) if tick_path else base_dir / "jq_data" / "tick_l1" / f"{date_str}.parquet"
    price = Path(price_path) if price_path else base_dir / "jq_data" / "price_1m" / f"{date_str}.parquet"
    return tick, price


def load_prev_day_close(price_path: Path, date_str: str) -> pd.DataFrame:
    price_dir = price_path.parent
    date_files = sorted(p for p in price_dir.glob("*.parquet") if p.stem < date_str)
    if not date_files:
        return pd.DataFrame(columns=["code", "prev_day_close"])

    prev_path = date_files[-1]
    prev_price = pd.read_parquet(prev_path, columns=["datetime", "code", "close"])
    prev_price["datetime"] = pd.to_datetime(prev_price["datetime"])
    prev_close = (
        prev_price.sort_values(["code", "datetime"], kind="stable")
        .groupby("code", sort=False)
        .tail(1)[["code", "close"]]
        .rename(columns={"close": "prev_day_close"})
    )
    return prev_close


def load_data(tick_path: Path, price_path: Path, codes: list[str] | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    tick = pd.read_parquet(tick_path, columns=["datetime", "code", "current", "high", "low", "volume", "money"])
    price = pd.read_parquet(price_path)

    tick["datetime"] = pd.to_datetime(tick["datetime"])
    price["datetime"] = pd.to_datetime(price["datetime"])

    if codes:
        tick = tick[tick["code"].isin(codes)].copy()
        price = price[price["code"].isin(codes)].copy()

    tick = tick.sort_values(["code", "datetime"], kind="stable").reset_index(drop=True)
    price = price.sort_values(["code", "datetime"], kind="stable").reset_index(drop=True)
    return tick, price


def trading_minutes(date_str: str) -> pd.DatetimeIndex:
    morning = pd.date_range(f"{date_str} 09:31:00", f"{date_str} 11:30:00", freq="1min")
    afternoon = pd.date_range(f"{date_str} 13:01:00", f"{date_str} 15:00:00", freq="1min")
    return morning.append(afternoon)


def _is_exact_minute(ts: pd.Series) -> pd.Series:
    return (
        ts.dt.second.eq(0)
        & ts.dt.microsecond.eq(0)
        & ts.dt.nanosecond.eq(0)
    )


def assign_bar_end(tick: pd.DataFrame, boundary_mode: str) -> pd.Series:
    ts = tick["datetime"]
    exact_minute = _is_exact_minute(ts)
    session_end = ts.dt.strftime("%H:%M:%S").isin([MORNING_END, AFTERNOON_END]) & exact_minute

    if boundary_mode == "jq_doc":
        bar_end = ts.dt.floor("min") + pd.Timedelta(minutes=1)
        bar_end = bar_end.where(~session_end, ts)
        return bar_end

    bar_end = ts.dt.floor("min")
    not_session_start = ~ts.dt.strftime("%H:%M:%S").isin([MORNING_START, AFTERNOON_START])
    bar_end = bar_end.where(~(exact_minute & not_session_start), bar_end + pd.Timedelta(minutes=1))
    return bar_end


def build_baseline(tick: pd.DataFrame, date_str: str, include_call_auction_first_bar: bool) -> pd.DataFrame:
    baseline_cutoff = CALL_AUCTION_START if include_call_auction_first_bar else MORNING_START
    pre_open = tick[tick["datetime"] < pd.Timestamp(f"{date_str} {baseline_cutoff}")]
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


def filter_ticks(tick: pd.DataFrame, date_str: str, include_call_auction_first_bar: bool) -> pd.DataFrame:
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
    mask = morning | afternoon
    if include_call_auction_first_bar:
        mask = mask | call_auction
    return tick[mask].copy()


def carry_session_end_ticks(raw_tick: pd.DataFrame, date_str: str) -> pd.DataFrame:
    morning_end = pd.Timestamp(f"{date_str} {MORNING_END}")
    afternoon_start = pd.Timestamp(f"{date_str} {AFTERNOON_START}")
    afternoon_end = pd.Timestamp(f"{date_str} {AFTERNOON_END}")
    close_carry_end = pd.Timestamp(f"{date_str} {CLOSE_CARRY_END}")

    midday = raw_tick[
        (raw_tick["datetime"] > morning_end)
        & (raw_tick["datetime"] < afternoon_start)
    ].copy()
    close = raw_tick[
        (raw_tick["datetime"] > afternoon_end)
        & (raw_tick["datetime"] <= close_carry_end)
    ].copy()

    carry_frames: list[pd.DataFrame] = []
    if not midday.empty:
        midday = midday.sort_values(["code", "datetime"], kind="stable")
        midday = midday.groupby("code", sort=False).head(1).copy()
        midday["bar_end"] = morning_end
        carry_frames.append(midday)
    if not close.empty:
        close = close.sort_values(["code", "datetime"], kind="stable")
        close = close.groupby("code", sort=False).head(1).copy()
        close["bar_end"] = afternoon_end
        carry_frames.append(close)

    if not carry_frames:
        return raw_tick.iloc[0:0].copy()
    return pd.concat(carry_frames, ignore_index=True)


def select_price_ticks(
    tick: pd.DataFrame,
    price_tick_mode: str,
    ignore_nonpositive_current: bool,
) -> pd.DataFrame:
    price_ticks = tick.copy()
    if price_tick_mode == "trade":
        prev_vol = price_ticks.groupby("code", sort=False)["volume"].shift()
        prev_money = price_ticks.groupby("code", sort=False)["money"].shift()
        changed = (price_ticks["volume"] != prev_vol) | (price_ticks["money"] != prev_money)
        price_ticks = price_ticks[changed.fillna(True)].copy()
    if ignore_nonpositive_current:
        price_ticks = price_ticks[price_ticks["current"] > 0].copy()

    # If a bar only gets a carried post-session trade tick, preserve the last
    # valid in-session quote as the price anchor for open/high before the close auction.
    if price_tick_mode == "trade" and "is_carry_tick" in tick.columns:
        base_valid = tick.copy()
        if ignore_nonpositive_current:
            base_valid = base_valid[base_valid["current"] > 0].copy()

        price_ticks["is_pre_end_tick"] = price_ticks["datetime"] <= price_ticks["bar_end"]
        carry_only_groups = (
            price_ticks.groupby(["code", "bar_end"], sort=False)
            .agg(
                has_pre_end_tick=("is_pre_end_tick", "any"),
                has_carry_tick=("is_carry_tick", "any"),
            )
            .reset_index()
        )
        carry_only_groups = carry_only_groups[
            carry_only_groups["has_carry_tick"] & ~carry_only_groups["has_pre_end_tick"]
        ]
        if not carry_only_groups.empty:
            anchors = base_valid.merge(
                carry_only_groups[["code", "bar_end"]],
                on=["code", "bar_end"],
                how="inner",
            )
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


def apply_morning_end_rule(
    result: pd.DataFrame,
    raw_tick: pd.DataFrame,
    date_str: str,
    ignore_nonpositive_current: bool,
) -> pd.DataFrame:
    """
    11:30 边界特殊处理。

    当 11:30 bar 内 11:30:00 之前没有成交（bar 内所有快照 volume 不变），
    聚宽会用 11:30:00 之前的静态快照价格作为 open，而不是 11:30:00 的成交价。
    """
    morning_end_dt = pd.Timestamp(f"{date_str} {MORNING_END}")
    bar_start = morning_end_dt - pd.Timedelta(minutes=1)

    raw = raw_tick.sort_values(["code", "datetime"], kind="stable").copy()

    pre_end = raw[
        (raw["datetime"] >= bar_start)
        & (raw["datetime"] < morning_end_dt)
    ]
    if ignore_nonpositive_current:
        pre_end = pre_end[pre_end["current"] > 0]
    if pre_end.empty:
        return result

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


def apply_close_auction_bar(
    result: pd.DataFrame,
    raw_tick: pd.DataFrame,
    date_str: str,
    ignore_nonpositive_current: bool,
) -> pd.DataFrame:
    close_bar_dt = pd.Timestamp(f"{date_str} {AFTERNOON_END}")
    raw = raw_tick.sort_values(["code", "datetime"], kind="stable").copy()

    preclose = raw[raw["datetime"] < close_bar_dt].copy()
    if ignore_nonpositive_current:
        preclose = preclose[preclose["current"] > 0].copy()
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
    if ignore_nonpositive_current:
        postclose = postclose[postclose["current"] > 0].copy()
    changed_postclose = (
        postclose[postclose["trade_changed_from_preclose"]]
        .sort_values(["code", "datetime"], kind="stable")
        .groupby("code", sort=False)
        .head(1)
    )
    unchanged_exact_close = (
        postclose[
            (~postclose["trade_changed_from_preclose"])
            & (postclose["datetime"] == close_bar_dt)
            & (postclose["current"] != postclose["preclose_price"])
        ]
        .sort_values(["code", "datetime"], kind="stable")
        .groupby("code", sort=False)
        .head(1)
    )
    postclose = changed_postclose.combine_first(unchanged_exact_close).reset_index(drop=True)
    postclose = postclose[["code", "current", "volume", "money"]].drop_duplicates(subset=["code"], keep="first")
    postclose = (
        postclose
        .rename(
            columns={
                "current": "auction_price",
                "volume": "auction_volume",
                "money": "auction_money",
            }
        )
    )

    if postclose.empty:
        return result

    close_override = preclose.merge(postclose, on="code", how="inner")
    close_override["datetime"] = close_bar_dt
    close_override["open"] = close_override["preclose_price"]
    close_override["close"] = close_override["auction_price"]
    close_override["high"] = close_override[["preclose_price", "auction_price"]].max(axis=1)
    close_override["low"] = close_override[["preclose_price", "auction_price"]].min(axis=1)
    close_override["volume"] = (close_override["auction_volume"] - close_override["preclose_volume"]).clip(lower=0)
    close_override["money"] = (close_override["auction_money"] - close_override["preclose_money"]).clip(lower=0)

    close_override = close_override[["datetime", "code", "open", "close", "high", "low", "money", "volume"]]
    result = result.set_index(["datetime", "code"])
    close_override = close_override.set_index(["datetime", "code"])
    result.update(close_override)
    return result.reset_index()


def apply_prev_day_close_fill(
    result: pd.DataFrame,
    raw_tick: pd.DataFrame,
    prev_day_close: pd.DataFrame,
    date_str: str,
) -> pd.DataFrame:
    if prev_day_close.empty:
        return result

    raw = raw_tick.sort_values(["code", "datetime"], kind="stable").copy()
    valid_raw = raw[
        (raw["current"] > 0) | (raw["volume"] > 0) | (raw["money"] > 0)
    ].copy()
    if valid_raw.empty:
        return result

    first_valid = (
        valid_raw.groupby("code", sort=False)["datetime"]
        .min()
        .reset_index()
        .rename(columns={"datetime": "first_valid_dt"})
    )
    first_valid["first_valid_bar_end"] = first_valid["first_valid_dt"].dt.floor("min") + pd.Timedelta(minutes=1)

    early = result.merge(first_valid[["code", "first_valid_bar_end"]], on="code", how="left")
    early = early.merge(prev_day_close, on="code", how="left")
    missing_prices = (
        early["open"].isna() | early["open"].eq(0)
    ) & (
        early["close"].isna() | early["close"].eq(0)
    ) & (
        early["high"].isna() | early["high"].eq(0)
    ) & (
        early["low"].isna() | early["low"].eq(0)
    )
    fill_mask = (
        early["prev_day_close"].notna()
        & early["volume"].fillna(0).eq(0)
        & early["money"].fillna(0).eq(0)
        & missing_prices
        & (
            (
                early["first_valid_bar_end"].notna()
                & (early["datetime"] < early["first_valid_bar_end"])
            )
            | early["first_valid_bar_end"].isna()
        )
    )
    if not fill_mask.any():
        return result

    override = early.loc[fill_mask, ["datetime", "code", "prev_day_close"]].copy()
    for col in ["open", "close", "high", "low"]:
        override[col] = override["prev_day_close"]
    override["money"] = 0.0
    override["volume"] = 0.0
    override = override[["datetime", "code", "open", "close", "high", "low", "money", "volume"]]

    result = result.set_index(["datetime", "code"])
    override = override.set_index(["datetime", "code"])
    result.update(override)
    return result.reset_index()


def synthesize_bars(
    raw_tick: pd.DataFrame,
    price_1m: pd.DataFrame,
    prev_day_close: pd.DataFrame,
    date_str: str,
    boundary_mode: str,
    include_call_auction_first_bar: bool,
    price_tick_mode: str,
    ignore_nonpositive_current: bool,
    carry_session_end_tick: bool,
    extrema_mode: str,
) -> pd.DataFrame:
    minutes = trading_minutes(date_str)
    codes = sorted(set(price_1m["code"]))
    if not codes:
        raise ValueError("No codes found in price_1m.")

    raw_tick = raw_tick[raw_tick["code"].isin(codes)].copy()
    raw_tick["prev_high"] = raw_tick.groupby("code", sort=False)["high"].shift()
    raw_tick["prev_low"] = raw_tick.groupby("code", sort=False)["low"].shift()
    raw_tick["high_changed"] = raw_tick["high"] != raw_tick["prev_high"]
    raw_tick["low_changed"] = raw_tick["low"] != raw_tick["prev_low"]
    baseline = build_baseline(raw_tick, date_str, include_call_auction_first_bar)
    tick = filter_ticks(raw_tick, date_str, include_call_auction_first_bar)
    tick["bar_end"] = assign_bar_end(tick, boundary_mode)
    if include_call_auction_first_bar:
        call_auction_mask = tick["datetime"].between(
            pd.Timestamp(f"{date_str} {CALL_AUCTION_START}"),
            pd.Timestamp(f"{date_str} {MORNING_START}") - pd.Timedelta(microseconds=1),
            inclusive="both",
        )
        tick.loc[call_auction_mask, "bar_end"] = pd.Timestamp(f"{date_str} 09:31:00")
    if carry_session_end_tick:
        carry = carry_session_end_ticks(raw_tick, date_str)
        if not carry.empty:
            carry = carry[carry["code"].isin(codes)].copy()
            carry["is_carry_tick"] = True
            tick = pd.concat([tick, carry], ignore_index=True)
            tick = tick.sort_values(["code", "datetime", "bar_end"], kind="stable").reset_index(drop=True)
    if "is_carry_tick" not in tick.columns:
        tick["is_carry_tick"] = False
    else:
        tick["is_carry_tick"] = tick["is_carry_tick"].astype("boolean").fillna(False).astype(bool)
    tick = tick[tick["bar_end"].isin(minutes)].copy()

    agg_volume_money = (
        tick.groupby(["code", "bar_end"], sort=False)
        .agg(
            cum_volume=("volume", "last"),
            cum_money=("money", "last"),
            close_all_ticks=("current", "last"),
        )
        .reset_index()
    )
    price_ticks = select_price_ticks(tick, price_tick_mode, ignore_nonpositive_current)
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
    agg = agg_volume_money.merge(agg_price, on=["code", "bar_end"], how="left")
    if extrema_mode == "hybrid_updates":
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
        agg = agg.merge(high_updates, on=["code", "bar_end"], how="left")
        agg = agg.merge(low_updates, on=["code", "bar_end"], how="left")
        agg["high"] = agg[["high", "high_update"]].max(axis=1, skipna=True)
        agg["low"] = agg[["low", "low_update"]].min(axis=1, skipna=True)

    full_index = pd.MultiIndex.from_product([codes, minutes], names=["code", "datetime"])
    bars = agg.rename(columns={"bar_end": "datetime"}).set_index(["code", "datetime"]).reindex(full_index)
    bars = bars.reset_index()
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

    result = bars[["datetime", "code", "open", "close", "high", "low", "money", "volume"]].copy()
    for col in ["open", "close", "high", "low", "money", "volume"]:
        result[col] = pd.to_numeric(result[col], errors="coerce")
    if carry_session_end_tick:
        result = apply_morning_end_rule(result, raw_tick, date_str, ignore_nonpositive_current)
        result = apply_close_auction_bar(result, raw_tick, date_str, ignore_nonpositive_current)
    result = apply_prev_day_close_fill(result, raw_tick, prev_day_close, date_str)
    for col in ["open", "close", "high", "low"]:
        result[col] = result[col].astype(np.float32)
    result["money"] = np.floor(result["money"].fillna(0) + 1e-6).astype(np.float64)
    result["volume"] = result["volume"].fillna(0).astype(np.float64)
    return result


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
    merged["all_match"] = merged[[f"{c}_match" for c in ["open", "close", "high", "low", "money", "volume"]]].all(axis=1)
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
    if args.jq_like:
        args.include_call_auction_first_bar = True
        args.price_tick_mode = "trade"
        args.ignore_nonpositive_current = True
        args.carry_session_end_tick = True
        args.extrema_mode = "hybrid_updates"

    base_dir = Path(__file__).resolve().parent
    tick_path, price_path = build_paths(base_dir, args.date, args.tick_path, args.price_path)
    output_dir = (base_dir / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    tick, price_1m = load_data(tick_path, price_path, args.code)
    prev_day_close = load_prev_day_close(price_path, args.date)
    if args.code:
        prev_day_close = prev_day_close[prev_day_close["code"].isin(args.code)].copy()
    synth = synthesize_bars(
        tick,
        price_1m,
        prev_day_close,
        args.date,
        args.boundary_mode,
        args.include_call_auction_first_bar,
        args.price_tick_mode,
        args.ignore_nonpositive_current,
        args.carry_session_end_tick,
        args.extrema_mode,
    )
    diff = compare_bars(synth, price_1m)
    summary = summarize_diff(diff)

    auction_stub = "with_auction" if args.include_call_auction_first_bar else "no_auction"
    current_stub = "pos_current" if args.ignore_nonpositive_current else "all_current"
    carry_stub = "carry_end" if args.carry_session_end_tick else "no_carry_end"
    file_stub = f"{args.date}.{args.boundary_mode}.{auction_stub}.{args.price_tick_mode}.{current_stub}.{carry_stub}.{args.extrema_mode}"
    synth_path = output_dir / f"synth_1m.{file_stub}.parquet"
    diff_path = output_dir / f"diff_1m.{file_stub}.parquet"
    mismatch_csv_path = output_dir / f"diff_1m.{file_stub}.mismatch_top{args.diff_limit}.csv"
    summary_path = output_dir / f"summary_1m.{file_stub}.csv"

    synth.to_parquet(synth_path, index=False)
    diff.to_parquet(diff_path, index=False)
    summary.to_csv(summary_path, index=False)

    mismatches = diff.loc[~diff["all_match"]].copy()
    sort_cols = [f"{c}_diff" for c in ["open", "close", "high", "low", "money", "volume"]]
    mismatches["_sort_key"] = mismatches[sort_cols].abs().max(axis=1)
    mismatches = mismatches.sort_values(["_sort_key", "code", "datetime"], ascending=[False, True, True]).drop(columns="_sort_key")
    mismatches.head(args.diff_limit).to_csv(mismatch_csv_path, index=False)

    print(f"tick_path={tick_path}")
    print(f"price_path={price_path}")
    print(f"synth_path={synth_path}")
    print(f"diff_path={diff_path}")
    print(f"summary_path={summary_path}")
    print(f"mismatch_csv_path={mismatch_csv_path}")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
