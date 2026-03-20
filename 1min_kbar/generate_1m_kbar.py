#!/usr/bin/env python3
"""
根据聚宽 tick_l1 快照生成聚宽口径的 1 分钟 K 线。

这个脚本是“纯生成版”脚本，只做一件事:
- 读取 tick parquet
- 按当前已验证通过的聚宽规则生成 1m bar
- 输出 parquet

适用场景:
- 你已经有某交易日的聚宽 tick_l1 数据
- 你希望直接产出与聚宽 price_1m 一致的分钟线
- 你不需要 compare_jq_1m.py 里的 diff / report / 实验逻辑

脚本依赖的额外信息:
1. 前一交易日最后一根 1m close
   用于处理:
   - 开盘初段 tick 全 0 的股票
   - 当天完全没有任何 tick，但聚宽仍给出整天静态分钟线的股票
2. 当天股票池
   如果你想完整复现“当天全市场”的聚宽 1m，必须知道当天应该输出哪些股票。
   默认脚本会优先使用当天 price_1m 文件中的 code 作为股票池；
   如果没有当天 price_1m，就需要你通过 --universe-path 提供一个至少包含 code 列的文件。

当前脚本实现的关键规则:
- 集合竞价 09:25-09:29:59 并入 09:31
- 普通 HH:MM:00 tick 归到下一根 bar，11:30:00 / 15:00:00 保留在本分钟
- open/close 只使用“有成交变化”的有效 tick.current
- high/low 以 tick.current 为主，同时吸收分钟内 tick.high/tick.low 的更新值
- 11:30 和 15:00 边界会回补 session 结束后的第一条 tick
- 15:00 单独按“收盘撮合 bar”处理
- 对早盘全 0 或全天无 tick 的股票，用前一交易日最后一根 1m close 静态回填
- 输出时把 OHLC / money / volume 规范到与聚宽 parquet 一致的数值表现
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="读取聚宽 tick_l1，按聚宽口径生成 1 分钟 K 线。"
    )
    parser.add_argument("--date", default="2026-03-17", help="交易日，格式 YYYY-MM-DD。")
    parser.add_argument(
        "--tick-path",
        default=None,
        help="tick parquet 路径，默认使用 ./jq_data/tick_l1/{date}.parquet",
    )
    parser.add_argument(
        "--prev-1m-dir",
        default=None,
        help="用于读取前一交易日 1m 数据目录，默认使用 ./jq_data/price_1m",
    )
    parser.add_argument(
        "--universe-path",
        default=None,
        help=(
            "可选的股票全集 parquet。若提供，会用其中的 code 作为输出股票池。"
            "默认优先使用 ./jq_data/price_1m/{date}.parquet；若不存在，则退化为 tick 中出现过的股票。"
        ),
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="输出 parquet 路径，默认使用 ./generated_1m_{date}.parquet",
    )
    parser.add_argument(
        "--code",
        nargs="*",
        default=None,
        help="可选，只生成指定股票，例如 000001.XSHE 600000.XSHG",
    )
    return parser.parse_args()


def trading_minutes(date_str: str) -> pd.DatetimeIndex:
    morning = pd.date_range(f"{date_str} 09:31:00", f"{date_str} 11:30:00", freq="1min")
    afternoon = pd.date_range(f"{date_str} 13:01:00", f"{date_str} 15:00:00", freq="1min")
    return morning.append(afternoon)


def load_tick(tick_path: Path, codes: list[str] | None) -> pd.DataFrame:
    tick = pd.read_parquet(
        tick_path,
        columns=["datetime", "code", "current", "high", "low", "volume", "money"],
    )
    tick["datetime"] = pd.to_datetime(tick["datetime"])
    if codes:
        tick = tick[tick["code"].isin(codes)].copy()
    tick = tick.sort_values(["code", "datetime"], kind="stable").reset_index(drop=True)
    return tick


def resolve_paths(base_dir: Path, args: argparse.Namespace) -> tuple[Path, Path | None, Path]:
    tick_path = Path(args.tick_path) if args.tick_path else base_dir / "jq_data" / "tick_l1" / f"{args.date}.parquet"
    prev_1m_dir = Path(args.prev_1m_dir) if args.prev_1m_dir else base_dir / "jq_data" / "price_1m"

    if args.output_path:
        output_path = Path(args.output_path)
    else:
        output_path = base_dir / f"generated_1m_{args.date}.parquet"

    if args.universe_path:
        universe_path = Path(args.universe_path)
    else:
        default_universe = prev_1m_dir / f"{args.date}.parquet"
        universe_path = default_universe if default_universe.exists() else None

    return tick_path, universe_path, output_path


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
    """
    生成输出股票池。

    说明:
    1. 如果用户给了 universe_path，就优先使用它的 code 集合作为输出股票池。
    2. 如果没给 universe_path，就只能退化为 tick 中出现过的股票。
    3. 为了兼容“全天没有任何 tick，但聚宽仍输出静态 bar”的情况，会把 prev_day_close 中的股票并进来。
    """
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
    """
    聚宽风格分钟切片:

    - 普通的 HH:MM:00 归到下一根分钟线
    - 但 11:30:00 和 15:00:00 保留在本分钟
    """
    ts = tick["datetime"]
    exact_minute = is_exact_minute(ts)
    session_end = ts.dt.strftime("%H:%M:%S").isin([MORNING_END, AFTERNOON_END]) & exact_minute
    bar_end = ts.dt.floor("min") + pd.Timedelta(minutes=1)
    bar_end = bar_end.where(~session_end, ts)
    return bar_end


def build_baseline(tick: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """
    baseline 用于处理开盘前累计量额，以及开盘前最后一个参考价。

    这里把 09:25 之前最后一条快照作为 baseline。
    因为 09:25-09:29 的集合竞价会并入 09:31，所以 baseline 要停在 09:25 之前。
    """
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
    """
    只保留聚宽 1m 需要参与合成的时段:

    - 集合竞价 09:25-09:29:59.xxx
    - 上午连续竞价 09:30-11:30
    - 下午连续竞价 13:00-15:00
    """
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

    # 集合竞价统一并入 09:31 这根 bar。
    call_auction_mask = tick["datetime"].between(
        pd.Timestamp(f"{date_str} {CALL_AUCTION_START}"),
        pd.Timestamp(f"{date_str} {MORNING_START}") - pd.Timedelta(microseconds=1),
        inclusive="both",
    )
    tick.loc[call_auction_mask, "bar_end"] = pd.Timestamp(f"{date_str} 09:31:00")
    return tick


def carry_session_end_ticks(raw_tick: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """
    聚宽在 11:30 和 15:00 边界附近不是简单截断。

    这里把:
    - 午盘结束后第一条 tick 回补到 11:30
    - 收盘后第一条 tick 回补到 15:00

    这一步主要用于对齐午盘边界和收盘撮合边界。
    """
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
    """
    聚宽分钟价格更像是只使用“有成交变化”的有效 tick。

    这里的过滤规则是:
    - 相对上一条 tick，volume 或 money 发生变化
    - current > 0
    """
    price_ticks = tick.copy()
    prev_volume = price_ticks.groupby("code", sort=False)["volume"].shift()
    prev_money = price_ticks.groupby("code", sort=False)["money"].shift()
    changed = (price_ticks["volume"] != prev_volume) | (price_ticks["money"] != prev_money)
    price_ticks = price_ticks[changed.fillna(True)].copy()
    price_ticks = price_ticks[price_ticks["current"] > 0].copy()

    # 如果某根 bar 只有回补的收盘后 tick，没有 bar 内有效价格 tick，
    # 则把 bar_end 之前最后一个有效价也并进来，保证 open/high 能挂住收盘前价格。
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
    """
    这是日内普通分钟 bar 的主合成流程。

    核心思路:
    - volume/money 取每分钟最后一条快照的累计值，再做差分
    - open/close/high/low 只用“有成交变化”的 tick.current 合成
    - high/low 额外吸收分钟内 tick.high/tick.low 的更新值
    - 空分钟沿用上一有效价格，量额为 0
    """
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


def apply_close_auction_rule(result: pd.DataFrame, raw_tick: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """
    对 15:00 单独套收盘撮合规则。

    这一步不能沿用普通分钟逻辑，因为聚宽的 15:00 更像:
    - open = 收盘撮合前最后有效价
    - close = 收盘撮合后第一条有效价
    - high/low = open 和 close 的极值
    - volume/money = 收盘撮合后的累计值减去撮合前累计值
    """
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
    """
    处理两类异常:

    1. 开盘初段 tick 全 0，直到后面才出现第一条有效 tick
       这时前面的 0 成交 bar 用前一交易日最后 1m close 填价。
    2. 当天完全没有任何 tick 的股票
       这时整天 240 根 bar 都用前一交易日最后 1m close 静态填充。
    """
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

    merged = result.merge(first_valid[["code", "first_valid_bar_end"]] if not first_valid.empty else pd.DataFrame(columns=["code", "first_valid_bar_end"]), on="code", how="left")
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
    """
    最终统一输出类型，尽量贴近聚宽 parquet 的数值表现。
    """
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
    result = apply_close_auction_rule(result, tick, date_str)
    result = apply_prev_close_fill(result, tick, prev_day_close)
    result = normalize_output_types(result)
    return result.sort_values(["code", "datetime"], kind="stable").reset_index(drop=True)


def main() -> None:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent
    tick_path, universe_path, output_path = resolve_paths(base_dir, args)
    prev_1m_dir = Path(args.prev_1m_dir) if args.prev_1m_dir else base_dir / "jq_data" / "price_1m"

    tick = load_tick(tick_path, args.code)
    prev_day_close = load_prev_day_close(prev_1m_dir, args.date, args.code)
    codes = load_universe_codes(tick, universe_path, prev_day_close, args.code)
    if not codes:
        raise ValueError("没有可生成的股票代码。请检查 tick 文件、universe_path 或 code 参数。")

    result = generate_1m_bars(tick, prev_day_close, codes, args.date)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output_path, index=False)

    print(f"tick_path={tick_path}")
    print(f"universe_path={universe_path}")
    print(f"output_path={output_path}")
    print(f"rows={len(result)}")
    print(result.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
