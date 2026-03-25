"""
Minimal demo: backfill incremental factors from HT2 raw data.

Reuses the IncrementalPipeline class structure (copied, not imported) with only
the data loading changed to read from /data/tickl2_data_ht2_raw/. Only computes
cat1 (history-accumulated moments -> factors at 30-min TTS) as a demo.

No imports from tickl2_factors — all needed code is copied inline to avoid
the config._config env-var chain.

Usage:
    python demo_backfill_ht2.py --date 2026-03-10 --output-dir /tmp/demo_factors
"""

import datetime as dt
import logging
import pathlib
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import polars as pl
import polars.selectors as cs
from qdata.core.dt import is_td, shift_n_td
from qdata.stock.fundamental import Valuation
from qdata.stock.price import PriceDaily
from qdata.stock.universe import Universe as StockUniverse
from qdata.unified_loader import get_universe

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HT2_RAW_DIR = pathlib.Path("/data/tickl2_data_ht2_raw")

# ============================================================================
# Constants (from constants.py, factor_config.toml, incremental_pipeline.py)
# ============================================================================

DATA_START_TIME = dt.time(9, 14, 55)
DATA_TIME = pl.concat(
    [
        pl.time_range(start=dt.time(9, 25), end=dt.time(9, 25), interval="1m", eager=True),
        pl.time_range(start=dt.time(9, 31), end=dt.time(11, 30), interval="1m", eager=True),
        pl.time_range(start=dt.time(13, 1), end=dt.time(15, 0), interval="1m", eager=True),
    ]
).alias("time")

CAT1_TTS = [
    dt.time(9, 25),
    dt.time(10, 0),
    dt.time(10, 30),
    dt.time(11, 0),
    dt.time(11, 30),
    dt.time(13, 30),
    dt.time(14, 0),
    dt.time(14, 30),
    dt.time(15, 0),
]

# Factor config thresholds (from factor_config.toml)
LARGE_VOL_INTERVAL = (100_000, float("inf"))
LARGE_MONEY_INTERVAL = (1_000_000, float("inf"))
SMALL_VOL_INTERVAL_1 = (0, 10_000)
SMALL_MONEY_INTERVAL_1 = (0, 100_000)
SMALL_VOL_INTERVAL_2 = (0, 1_000)
SMALL_MONEY_INTERVAL_2 = (0, 500_000)
INSTANT_INTERVAL_MS = (0, 3_000)

ORDER_BASE_COLS = ["channelid", "orderid", "symbol", "ordertm", "orderpx", "ordervol", "sideflag", "ordertype"]
BUY_ORDER_FOR_TRADE_EXPRS = [
    pl.col("channelid"),
    pl.col("orderid"),
    pl.col("ordertm").alias("ordertm_buy"),
    pl.col("orderpx").alias("orderpx_buy"),
    pl.col("ordervol").alias("ordervol_buy"),
    pl.col("ordermoney").alias("ordermoney_buy"),
    pl.col("orderturnover").alias("orderturnover_buy"),
    pl.col("orderret").alias("orderret_buy"),
]
SELL_ORDER_FOR_TRADE_EXPRS = [
    pl.col("channelid"),
    pl.col("orderid"),
    pl.col("ordertm").alias("ordertm_sell"),
    pl.col("orderpx").alias("orderpx_sell"),
    pl.col("ordervol").alias("ordervol_sell"),
    pl.col("ordermoney").alias("ordermoney_sell"),
    pl.col("orderturnover").alias("orderturnover_sell"),
    pl.col("orderret").alias("orderret_sell"),
]
ORDER_FOR_WITHDRAW_COLS = ["channelid", "orderid", "ordertm", "ordervol"]
ORDER_FOR_WITHDRAW_WITH_PX_COLS = ["channelid", "orderid", "ordertm", "orderpx", "ordervol"]

# ============================================================================
# Expression helpers (from exprs.py)
# ============================================================================

OPEN_MS = (9 * 3600 + 29 * 60 + 57) * 1000
CUTOFF_MS = (12 * 3600 + 59 * 60 + 57) * 1000
ADD_5MIN = 5 * 60 * 1000
SUB_1H30 = 90 * 60 * 1000


def tm_int_to_ms(expr: pl.Expr) -> pl.Expr:
    x = expr.cast(pl.Int64)
    hh = x // 10_000_000
    rem = x % 10_000_000
    mm = rem // 100_000
    rem2 = rem % 100_000
    ss = rem2 // 1_000
    mmm = rem2 % 1_000
    return ((hh * 3600 + mm * 60 + ss) * 1000 + mmm).cast(pl.Int64)


def apply_continuous_session(ms_expr: pl.Expr) -> pl.Expr:
    return (
        pl.when(ms_expr < OPEN_MS)
        .then(ms_expr + ADD_5MIN)
        .when(ms_expr > CUTOFF_MS)
        .then(ms_expr - SUB_1H30)
        .otherwise(ms_expr)
    )


def tm_to_timestamp(expr: pl.Expr) -> pl.Expr:
    return apply_continuous_session(tm_int_to_ms(expr))


def filter_large_expr(vol_col: str, money_col: str, large_vol_interval, large_money_interval) -> pl.Expr:
    return pl.col(vol_col).is_between(*large_vol_interval) | pl.col(money_col).is_between(*large_money_interval)


def filter_small_expr(vol_col, money_col, small_vol_1, small_money_1, small_vol_2, small_money_2) -> pl.Expr:
    return (pl.col(vol_col).is_between(*small_vol_1) & pl.col(money_col).is_between(*small_money_1)) | (
        pl.col(vol_col).is_between(*small_vol_2) & pl.col(money_col).is_between(*small_money_2)
    )


def add_vwapx(factors: pl.DataFrame) -> pl.DataFrame:
    _vol_mean_cols = factors.select(cs.contains("vol_mean")).columns
    return factors.with_columns(
        (pl.col(_c.replace("vol", "money")) / pl.col(_c)).alias(_c.replace("vol_mean", "vwapx"))
        for _c in _vol_mean_cols
    ).drop(_vol_mean_cols)


def moment_expr(vol_col: str, money_col: str, fields: str | Iterable[str]):
    return (
        pl.len().alias("count"),
        cs.by_name(vol_col, money_col, fields).count().name.suffix("_count"),
        cs.by_name(vol_col, money_col, fields).cast(pl.Float64).sum().name.suffix("_s1"),
        cs.by_name(money_col, fields).cast(pl.Float64).pow(2).sum().name.suffix("_s2"),
        cs.by_name(money_col, fields).cast(pl.Float64).pow(3).sum().name.suffix("_s3"),
        cs.by_name(money_col, fields).cast(pl.Float64).pow(4).sum().name.suffix("_s4"),
    )


def moment_expr_with_conditions(vol_col, money_col, fields, conditions=None):
    if isinstance(fields, str):
        fields = [fields]
    expressions = []
    if conditions is None:
        expressions.extend(moment_expr(vol_col, money_col, fields))
    else:
        for cname, cexpr in conditions.items():
            expressions.append(pl.col(vol_col).filter(cexpr).len().alias(f"count_{cname}"))
            for col in [vol_col, money_col] + list(fields):
                expressions.append(pl.col(col).filter(cexpr).count().alias(f"{col}_count_{cname}"))
                expressions.append(pl.col(col).filter(cexpr).cast(pl.Float64).sum().alias(f"{col}_s1_{cname}"))
            for col in [money_col] + list(fields):
                expressions.append(pl.col(col).filter(cexpr).cast(pl.Float64).pow(2).sum().alias(f"{col}_s2_{cname}"))
                expressions.append(pl.col(col).filter(cexpr).cast(pl.Float64).pow(3).sum().alias(f"{col}_s3_{cname}"))
                expressions.append(pl.col(col).filter(cexpr).cast(pl.Float64).pow(4).sum().alias(f"{col}_s4_{cname}"))
    return expressions


def unique_count_expr_with_conditions(id_cols, conditions=None):
    expressions = []
    if conditions is None:
        for col in id_cols:
            expressions.append(pl.col(col).drop_nulls().unique().alias(f"{col}_unique_list"))
    else:
        for cname, cexpr in conditions.items():
            for col in id_cols:
                expressions.append(pl.col(col).filter(cexpr).drop_nulls().unique().alias(f"{col}_unique_list_{cname}"))
    return expressions


def stats_from_moments_expr(vol_col, money_col, fields):
    if isinstance(fields, str):
        fields = [fields]
    eps = float(np.finfo(np.float16).eps)
    all_cols = [vol_col, money_col] + list(fields)
    stat_cols = [money_col] + list(fields)
    exprs = []
    for col in all_cols:
        exprs.append((pl.col(f"{col}_s1") / pl.col(f"{col}_count")).alias(f"{col}_mean"))
    for col in stat_cols:
        n, s1, s2 = pl.col(f"{col}_count"), pl.col(f"{col}_s1"), pl.col(f"{col}_s2")
        var = (s2 - s1.pow(2) / n) / (n - 1)
        exprs.append(pl.when(var < 0).then(0.0).otherwise(var).sqrt().alias(f"{col}_std"))
    for col in stat_cols:
        n, s1, s2, s3 = pl.col(f"{col}_count"), pl.col(f"{col}_s1"), pl.col(f"{col}_s2"), pl.col(f"{col}_s3")
        mu = s1 / n
        vp = s2 / n - mu.pow(2)
        sp = vp.sqrt()
        num = s3 / n - 3 * mu * (s2 / n) + 2 * mu.pow(3)
        exprs.append(
            pl.when((vp <= eps * mu.pow(2)) | (n < 3)).then(None).otherwise(num / sp.pow(3)).alias(f"{col}_skew")
        )
    for col in stat_cols:
        n, s1, s2, s3, s4 = (
            pl.col(f"{col}_count"),
            pl.col(f"{col}_s1"),
            pl.col(f"{col}_s2"),
            pl.col(f"{col}_s3"),
            pl.col(f"{col}_s4"),
        )
        mu = s1 / n
        vp = s2 / n - mu.pow(2)
        sp = vp.sqrt()
        num = s4 / n - 4 * mu * (s3 / n) + 6 * mu.pow(2) * (s2 / n) - 3 * mu.pow(4)
        exprs.append(
            pl.when((vp <= eps * mu.pow(2)) | (n < 4)).then(None).otherwise(num / sp.pow(4) - 3).alias(f"{col}_kurt")
        )
    return exprs


def stats_from_moments_expr_with_conditions(vol_col, money_col, fields, condition_names):
    if isinstance(fields, str):
        fields = [fields]
    eps = float(np.finfo(np.float16).eps)
    exprs = []
    for cn in condition_names:
        sfx = f"_{cn}"
        all_cols = [vol_col, money_col] + list(fields)
        stat_cols = [money_col] + list(fields)
        for col in all_cols:
            exprs.append((pl.col(f"{col}_s1{sfx}") / pl.col(f"{col}_count{sfx}")).alias(f"{col}_mean{sfx}"))
        for col in stat_cols:
            n, s1, s2 = pl.col(f"{col}_count{sfx}"), pl.col(f"{col}_s1{sfx}"), pl.col(f"{col}_s2{sfx}")
            var = (s2 - s1.pow(2) / n) / (n - 1)
            exprs.append(pl.when(var < 0).then(0.0).otherwise(var).sqrt().alias(f"{col}_std{sfx}"))
        for col in stat_cols:
            n, s1, s2, s3 = (
                pl.col(f"{col}_count{sfx}"),
                pl.col(f"{col}_s1{sfx}"),
                pl.col(f"{col}_s2{sfx}"),
                pl.col(f"{col}_s3{sfx}"),
            )
            mu = s1 / n
            vp = s2 / n - mu.pow(2)
            sp = vp.sqrt()
            num = s3 / n - 3 * mu * (s2 / n) + 2 * mu.pow(3)
            exprs.append(
                pl.when((vp <= eps * mu.pow(2)) | (n < 3))
                .then(None)
                .otherwise(num / sp.pow(3))
                .alias(f"{col}_skew{sfx}")
            )
        for col in stat_cols:
            n, s1, s2, s3, s4 = (
                pl.col(f"{col}_count{sfx}"),
                pl.col(f"{col}_s1{sfx}"),
                pl.col(f"{col}_s2{sfx}"),
                pl.col(f"{col}_s3{sfx}"),
                pl.col(f"{col}_s4{sfx}"),
            )
            mu = s1 / n
            vp = s2 / n - mu.pow(2)
            sp = vp.sqrt()
            num = s4 / n - 4 * mu * (s3 / n) + 6 * mu.pow(2) * (s2 / n) - 3 * mu.pow(4)
            exprs.append(
                pl.when((vp <= eps * mu.pow(2)) | (n < 4))
                .then(None)
                .otherwise(num / sp.pow(4) - 3)
                .alias(f"{col}_kurt{sfx}")
            )
    return exprs


# ============================================================================
# Helpers (from helpers.py) — simplified for offline stock only
# ============================================================================


def time_to_int(t: dt.time) -> int:
    return t.hour * 10_000_000 + t.minute * 100_000 + t.second * 1_000


def get_pre_close(T0_date: dt.date) -> pl.DataFrame:
    return PriceDaily.get_recorded(
        start_date=T0_date,
        end_date=T0_date,
        columns=("date", "code", "pre_close"),
    ).select("pre_close", pl.col.code.str.slice(0, 6).cast(pl.UInt32).alias("symbol"))


def px_to_ret(df: pl.DataFrame, T0_date: dt.date, px_cols) -> pl.DataFrame:
    px_cols = (px_cols,) if isinstance(px_cols, str) else tuple(px_cols)
    _pre_close = get_pre_close(T0_date)
    return (
        df.join(_pre_close, how="left", on="symbol")
        .with_columns(
            (cs.by_name(px_cols) / pl.col.pre_close - 1).clip(-0.2, 0.2).name.map(lambda x: x.replace("px", "ret"))
        )
        .drop("pre_close")
    )


def add_turnover(df: pl.DataFrame, T0_date: dt.date, money_cols) -> pl.DataFrame:
    money_cols = (money_cols,) if isinstance(money_cols, str) else tuple(money_cols)
    _L1_date = shift_n_td(T0_date, 1)
    valuation_df = Valuation.get_recorded(
        start_date=_L1_date,
        end_date=_L1_date,
        columns=("date", "code", "circulating_market_cap"),
    ).select(pl.col.code.str.slice(0, 6).cast(pl.UInt32).alias("symbol"), "circulating_market_cap")
    return (
        df.join(valuation_df, how="left", on="symbol")
        .with_columns(
            (cs.by_name(money_cols) / pl.col.circulating_market_cap).name.map(lambda s: s.replace("money", "turnover"))
        )
        .drop("circulating_market_cap")
    )


# ============================================================================
# Incremental moment functions (from incremental_factors.py)
# ============================================================================


def _make_large_filter(vol_col, money_col):
    return filter_large_expr(vol_col, money_col, LARGE_VOL_INTERVAL, LARGE_MONEY_INTERVAL)


def _make_small_filter(vol_col, money_col):
    return filter_small_expr(
        vol_col, money_col, SMALL_VOL_INTERVAL_1, SMALL_MONEY_INTERVAL_1, SMALL_VOL_INTERVAL_2, SMALL_MONEY_INTERVAL_2
    )


def cal_order_moments(order_data: pl.DataFrame) -> dict[str, pl.DataFrame]:
    _vol, _money, _fields = "ordervol", "ordermoney", ("orderret", "orderturnover")
    conditions = {"large": _make_large_filter(_vol, _money), "small": _make_small_filter(_vol, _money)}
    return {
        "base": order_data.group_by("symbol").agg(*moment_expr(_vol, _money, _fields)),
        "conditions": order_data.group_by("symbol").agg(
            *moment_expr_with_conditions(_vol, _money, _fields, conditions)
        ),
    }


def cal_trade_moments(trade_data: pl.DataFrame) -> dict[str, pl.DataFrame]:
    _vol, _money = "tradevol", "trademoney"
    _fields = ("traderet", "ordertmlag", "orderretdiff", "tradeturnover", "orderturnoverdiff")
    conditions = {
        "large": _make_large_filter(_vol, _money),
        "small": _make_small_filter(_vol, _money),
        "bolarge": _make_large_filter("ordervol_buy", "ordermoney_buy"),
        "bosmall": _make_small_filter("ordervol_buy", "ordermoney_buy"),
        "solarge": _make_large_filter("ordervol_sell", "ordermoney_sell"),
        "sosmall": _make_small_filter("ordervol_sell", "ordermoney_sell"),
    }
    return {
        "base": trade_data.group_by("symbol").agg(
            *moment_expr(_vol, _money, _fields),
            *unique_count_expr_with_conditions(["tradebuyid", "tradesellid"]),
        ),
        "conditions": trade_data.group_by("symbol").agg(
            *moment_expr_with_conditions(_vol, _money, _fields, conditions),
            *unique_count_expr_with_conditions(["tradebuyid", "tradesellid"], conditions),
        ),
    }


def cal_withdraw_moments(withdraw_data: pl.DataFrame) -> dict[str, pl.DataFrame]:
    _vol, _money = "withdrawvol", "withdrawmoney"
    _fields = ("withdrawret", "withdrawtmlag", "withdrawturnover")
    conditions = {
        "large": _make_large_filter(_vol, _money),
        "small": _make_small_filter(_vol, _money),
        "complete": pl.col.withdrawvol == pl.col.ordervol,
        "partial": pl.col.withdrawvol < pl.col.ordervol,
        "instant": pl.col.withdrawtmlag.is_between(*INSTANT_INTERVAL_MS),
    }
    return {
        "base": withdraw_data.group_by("symbol").agg(*moment_expr(_vol, _money, _fields)),
        "conditions": withdraw_data.group_by("symbol").agg(
            *moment_expr_with_conditions(_vol, _money, _fields, conditions)
        ),
    }


def _merge_moments_generic(hist, delta):
    result = {}
    for key in ("base", "conditions"):
        concat_df = pl.concat([hist[key], delta[key]])
        num_cols = [c for c in concat_df.columns if c != "symbol" and "_unique_list" not in c]
        ul_cols = [c for c in concat_df.columns if "_unique_list" in c]
        result[key] = concat_df.group_by("symbol").agg(
            [pl.col(c).sum() for c in num_cols]
            + [pl.col(c).list.explode(keep_nulls=False, empty_as_null=False).drop_nulls().unique() for c in ul_cols]
        )
    return result


merge_order_moments = _merge_moments_generic
merge_trade_moments = _merge_moments_generic
merge_withdraw_moments = _merge_moments_generic


def cal_order_factors_from_moments(moments: dict[str, pl.DataFrame]) -> pl.DataFrame:
    _vol, _money, _fields = "ordervol", "ordermoney", ("orderret", "orderturnover")
    base_stats = moments["base"].select("symbol", "count", *stats_from_moments_expr(_vol, _money, _fields))
    cond_stats = moments["conditions"].select(
        "symbol",
        *stats_from_moments_expr_with_conditions(_vol, _money, _fields, ["large", "small"]),
        cs.starts_with("count_"),
    )
    result = base_stats.join(cond_stats, on="symbol", how="full", coalesce=True).pipe(add_vwapx)
    return result.select(cs.all() - cs.starts_with("count"), cs.starts_with("count").name.prefix("order_"))


def cal_trade_factors_from_moments(moments: dict[str, pl.DataFrame]) -> pl.DataFrame:
    _vol, _money = "tradevol", "trademoney"
    _fields = ("traderet", "ordertmlag", "orderretdiff", "tradeturnover", "orderturnoverdiff")
    cond_names = ["large", "small", "bolarge", "bosmall", "solarge", "sosmall"]

    def process_uc(df, cn=None):
        exprs = []
        prefixes = cn or [None]
        for p in prefixes if cn else [None]:
            for col in ["tradebuyid", "tradesellid"]:
                sfx = f"_{p}" if p else ""
                ul = f"{col}_unique_list{sfx}"
                alias = f"trade_{'buyorder' if col == 'tradebuyid' else 'sellorder'}_count{sfx}"
                if ul in df.columns:
                    exprs.append(pl.col(ul).list.len().alias(alias))
        return df.with_columns(exprs) if exprs else df

    base = process_uc(moments["base"])
    conds = process_uc(moments["conditions"], cond_names)
    base_stats = base.select(
        "symbol",
        "count",
        *stats_from_moments_expr(_vol, _money, _fields),
        cs.contains("trade_") & cs.contains("_count") & ~cs.starts_with("count") & ~cs.contains("_unique_list"),
    )
    cond_stats = conds.select(
        "symbol",
        *stats_from_moments_expr_with_conditions(_vol, _money, _fields, cond_names),
        cs.contains("trade_") & cs.contains("_count") & ~cs.starts_with("count") & ~cs.contains("_unique_list"),
        cs.starts_with("count_"),
    )
    result = base_stats.join(cond_stats, on="symbol", how="full", coalesce=True).pipe(add_vwapx)
    rename = {}
    for c in result.columns:
        if c.startswith("count_") and not c.startswith("trade_"):
            rename[c] = f"trade_{c}"
        elif c == "count":
            rename[c] = "trade_count"
    return result.rename(rename) if rename else result


def cal_withdraw_factors_from_moments(moments: dict[str, pl.DataFrame]) -> pl.DataFrame:
    _vol, _money = "withdrawvol", "withdrawmoney"
    _fields = ("withdrawret", "withdrawtmlag", "withdrawturnover")
    cond_names = ["large", "small", "complete", "partial", "instant"]
    base_stats = moments["base"].select("symbol", "count", *stats_from_moments_expr(_vol, _money, _fields))
    cond_stats = moments["conditions"].select(
        "symbol",
        *stats_from_moments_expr_with_conditions(_vol, _money, _fields, cond_names),
        cs.starts_with("count_"),
    )
    result = base_stats.join(cond_stats, on="symbol", how="full", coalesce=True).pipe(add_vwapx)
    return result.select(cs.all() - cs.starts_with("count"), cs.starts_with("count").name.prefix("withdraw_"))


# ============================================================================
# HT2 raw data loading & preprocessing
# ============================================================================


def _load_ht2_raw(T0_date: dt.date, data_type: str) -> pl.DataFrame:
    date_dir = HT2_RAW_DIR / T0_date.strftime("%Y-%m-%d")
    date_str = T0_date.strftime("%Y%m%d")
    pattern = f"{date_str}_{data_type}_data_*.parquet"
    shard_files = sorted(date_dir.glob(pattern))
    if not shard_files:
        raise FileNotFoundError(f"No {data_type} files in {date_dir} matching {pattern}")
    logger.info(f"Loading {len(shard_files)} {data_type} shards from {date_dir}")
    return pl.concat([pl.read_parquet(f) for f in shard_files])


def _preprocess_order_ht2(raw: pl.DataFrame, exchange: str) -> pl.DataFrame:
    """HT2 order -> canonical schema (channelid, orderid, ordertm, symbol, orderpx, ordervol, ordertype, sideflag)."""
    df = raw.filter(pl.col.exchange == exchange)
    if exchange == "SH":
        return df.select(
            pl.col.Channel.cast(pl.UInt32).alias("channelid"),
            pl.col.OrderNumber.cast(pl.UInt32).alias("orderid"),
            pl.col.Time.cast(pl.UInt32).alias("ordertm"),
            pl.col.Symbol.cast(pl.UInt32).alias("symbol"),
            (pl.col.Price.cast(pl.Float64) / 10_000).alias("orderpx"),
            pl.col.Volume.cast(pl.UInt32).alias("ordervol"),
            pl.col.OrderKind.replace_strict({2: 65, 10: 68, 11: 83}, default=None, return_dtype=pl.UInt8).alias(
                "ordertype"
            ),
            pl.col.FunctionCode.replace_strict({1: 66, 2: 83}, default=None, return_dtype=pl.UInt8).alias("sideflag"),
        ).filter(pl.col.ordertype != 83)
    else:  # SZ
        return df.select(
            pl.col.Channel.cast(pl.UInt32).alias("channelid"),
            pl.col.OrderNumber.cast(pl.UInt32).alias("orderid"),
            pl.col.Time.cast(pl.UInt32).alias("ordertm"),
            pl.col.Symbol.cast(pl.UInt32).alias("symbol"),
            (pl.col.Price.cast(pl.Float64) / 10_000).alias("orderpx"),
            pl.col.Volume.cast(pl.UInt32).alias("ordervol"),
            pl.col.FunctionCode.replace_strict({1: 66, 2: 83}, default=None, return_dtype=pl.UInt8).alias("sideflag"),
            pl.col.OrderKind.replace_strict({1: 50, 2: 49, 3: 85}, default=None, return_dtype=pl.UInt8).alias(
                "ordertype"
            ),
        )


def _preprocess_trade_ht2(raw: pl.DataFrame, exchange: str) -> pl.DataFrame:
    """HT2 trade -> canonical schema."""
    df = raw.filter(pl.col.exchange == exchange)
    if exchange == "SH":
        return df.select(
            pl.col.Channel.cast(pl.UInt32).alias("channelid"),
            pl.col.BidOrder.cast(pl.UInt32).alias("tradebuyid"),
            pl.col.AskOrder.cast(pl.UInt32).alias("tradesellid"),
            pl.col.Time.cast(pl.UInt32).alias("tradetm"),
            pl.col.Symbol.cast(pl.UInt32).alias("symbol"),
            (pl.col.Price.cast(pl.Float64) / 10_000).alias("tradepx"),
            pl.col.Volume.cast(pl.UInt32).alias("tradevol"),
            pl.col.BSFlag.replace_strict({0: 78, 1: 66, 2: 83}, default=None, return_dtype=pl.UInt8).alias("sideflag"),
        )
    else:  # SZ
        return df.select(
            pl.col.Channel.cast(pl.UInt32).alias("channelid"),
            pl.col.BidOrder.cast(pl.UInt32).alias("tradebuyid"),
            pl.col.AskOrder.cast(pl.UInt32).alias("tradesellid"),
            pl.col.Time.cast(pl.UInt32).alias("tradetm"),
            pl.col.Symbol.cast(pl.UInt32).alias("symbol"),
            (pl.col.Price.cast(pl.Float64) / 10_000).alias("tradepx"),
            pl.col.Volume.cast(pl.UInt32).alias("tradevol"),
            pl.col.FunctionCode.replace_strict({0: 70, 1: 52}, default=None, return_dtype=pl.UInt8).alias("tradetype"),
        )


# ============================================================================
# IncrementalPipeline (copied from incremental_pipeline.py, cat1-only)
# ============================================================================


class HT2IncrementalPipeline:
    """Incremental pipeline for HT2 raw data, cat1 factors only."""

    def __init__(self, T0_date: dt.date, factor_save_dir: str) -> None:
        self.T0_date = T0_date
        if not is_td(self.T0_date):
            raise ValueError(f"{T0_date} is not a trading day")

        self._load_backfill_data()
        self._setup_universe()
        self._init_state_storage()
        self._setup_processing_schedule()
        self.factor_save_dir = pathlib.Path(factor_save_dir)
        self.factor_save_dir.mkdir(parents=True, exist_ok=True)

    def _load_backfill_data(self) -> None:
        """Load HT2 raw data and preprocess into canonical schema."""
        order_raw = _load_ht2_raw(self.T0_date, "order")
        trade_raw = _load_ht2_raw(self.T0_date, "trade")
        self.sh_order = _preprocess_order_ht2(order_raw, "SH")
        self.sz_order = _preprocess_order_ht2(order_raw, "SZ")
        self.sh_trade = _preprocess_trade_ht2(trade_raw, "SH")
        self.sz_trade = _preprocess_trade_ht2(trade_raw, "SZ")
        logger.info(
            f"Data loaded: sh_order={self.sh_order.shape}, sz_order={self.sz_order.shape}, "
            f"sh_trade={self.sh_trade.shape}, sz_trade={self.sz_trade.shape}"
        )
        del order_raw, trade_raw

    def _setup_universe(self) -> None:
        self.universe = get_universe(sdate=self.T0_date, edate=self.T0_date, universe=StockUniverse.FULL).select(
            "date", "code", pl.col.code.str.slice(0, 6).cast(pl.UInt32).alias("symbol")
        )
        logger.info(f"Universe: {self.universe.height} stocks")

    def _init_state_storage(self) -> None:
        """Initialize state storage (from IncrementalPipeline._init_state_storage)."""
        self.processed_buy_order_sh = pl.DataFrame()
        self.processed_buy_order_sz = pl.DataFrame()
        self.processed_sell_order_sh = pl.DataFrame()
        self.processed_sell_order_sz = pl.DataFrame()
        self.latest_trade_px_sz = pl.DataFrame(
            schema={"symbol": pl.UInt32, "tradetm": pl.UInt32, "tradepx": pl.Float64, "sideflag": pl.Int8}
        )
        # Moment accumulators for cat1 (history)
        self.moments = {
            k: None
            for k in [
                "buy_order",
                "sell_order",
                "buy_withdraw",
                "sell_withdraw",
                "both_trade",
                "buy_trade",
                "sell_trade",
            ]
        }

    def _setup_processing_schedule(self) -> None:
        self.process_dt = (
            DATA_TIME.to_frame()
            .select(pl.lit(self.T0_date).dt.combine(pl.col.time).alias("datetime"))
            .get_column("datetime")
        )

    # --- Time range (from IncrementalPipeline._get_time_range) ---

    def _get_time_range(self, T0_datetime: dt.datetime) -> tuple[int, int]:
        t = T0_datetime.time()
        if t == dt.time(9, 25):
            start = DATA_START_TIME
        elif t in (dt.time(9, 31), dt.time(13, 1)):
            start = (T0_datetime - dt.timedelta(minutes=1, seconds=5)).time()
        else:
            start = (T0_datetime - dt.timedelta(minutes=1)).time()
        end = (
            (T0_datetime + dt.timedelta(seconds=5)).time()
            if t in (dt.time(9, 25), dt.time(11, 30), dt.time(15, 0))
            else t
        )
        return time_to_int(start), time_to_int(end)

    # --- Data loading per minute (from IncrementalPipeline._load_incr_tickl2_data_per_minute_from_csmar_recorded) ---

    def _load_incr_data_per_minute(self, T0_datetime: dt.datetime):
        start_int, end_int = self._get_time_range(T0_datetime)
        ord_filt = pl.col.ordertm.is_between(start_int, end_int, closed="right")
        trd_filt = pl.col.tradetm.is_between(start_int, end_int, closed="right")
        return (
            self.sh_order.filter(ord_filt),
            self.sh_trade.filter(trd_filt),
            self.sz_order.filter(ord_filt),
            self.sz_trade.filter(trd_filt),
        )

    # --- Order processing (from IncrementalPipeline._process_order_data_sh/sz) ---

    def _process_order_data_sh(self, new_order_df):
        ord_lf = new_order_df.lazy().select(ORDER_BASE_COLS).filter(pl.col.orderid > 0)
        buy_lf = ord_lf.filter(pl.col.sideflag == 66).drop("sideflag")
        sell_lf = ord_lf.filter(pl.col.sideflag == 83).drop("sideflag")
        buy_wd = (
            buy_lf.filter(pl.col.ordertype == 68)
            .drop("ordertype")
            .rename({"orderid": "id", "ordertm": "withdrawtm", "orderpx": "withdrawpx", "ordervol": "withdrawvol"})
        )
        sell_wd = (
            sell_lf.filter(pl.col.ordertype == 68)
            .drop("ordertype")
            .rename({"orderid": "id", "ordertm": "withdrawtm", "orderpx": "withdrawpx", "ordervol": "withdrawvol"})
        )
        buy_lf = buy_lf.filter(pl.col.ordertype == 65).drop("ordertype")
        sell_lf = sell_lf.filter(pl.col.ordertype == 65).drop("ordertype")
        return buy_lf, sell_lf, buy_wd, sell_wd

    def _process_trade_data_sh(self, new_trade_df):
        return (
            new_trade_df.lazy()
            .select("channelid", "symbol", "tradetm", "tradebuyid", "tradesellid", "tradepx", "tradevol", "sideflag")
            .filter((pl.col.tradebuyid != 0) & (pl.col.tradesellid != 0))
        )

    def _process_order_data_sz(self, new_order_df):
        lf = (
            new_order_df.lazy()
            .with_columns(pl.when(pl.col.ordertype == 50).then(pl.col.orderpx).otherwise(None).alias("orderpx"))
            .select(ORDER_BASE_COLS)
            .filter(pl.col.orderid > 0)
        )
        return lf.filter(pl.col.sideflag == 66).drop("sideflag"), lf.filter(pl.col.sideflag == 83).drop("sideflag")

    def _process_trade_data_sz(self, new_trade_df):
        lf = new_trade_df.lazy().select(
            "channelid", "symbol", "tradetm", "tradebuyid", "tradesellid", "tradepx", "tradevol", "tradetype"
        )
        buy_wd = (
            lf.filter(pl.col.tradesellid == 0)
            .drop("tradesellid", "tradetype")
            .rename({"tradebuyid": "id", "tradetm": "withdrawtm", "tradepx": "withdrawpx", "tradevol": "withdrawvol"})
            .with_columns(pl.lit(None).alias("withdrawpx"))
            .filter(pl.col.id != 0)
        )
        sell_wd = (
            lf.filter(pl.col.tradebuyid == 0)
            .drop("tradebuyid", "tradetype")
            .rename({"tradesellid": "id", "tradetm": "withdrawtm", "tradepx": "withdrawpx", "tradevol": "withdrawvol"})
            .with_columns(pl.lit(None).alias("withdrawpx"))
            .filter(pl.col.id != 0)
        )
        trade_lf = (
            lf.filter(pl.col.tradetype == 70)
            .drop("tradetype")
            .with_columns(
                pl.when(pl.col.tradebuyid > pl.col.tradesellid).then(66).otherwise(83).alias("sideflag").cast(pl.Int8)
            )
            .filter((pl.col.tradebuyid != 0) & (pl.col.tradesellid != 0))
        )
        return buy_wd, sell_wd, trade_lf

    # --- Final processing (from IncrementalPipeline._apply_final_processing) ---

    def _apply_final_processing(self, df, T0_date: dt.date, data_type: str):
        lf = df.lazy() if isinstance(df, pl.DataFrame) else df
        if data_type == "order":
            return (
                lf.with_columns(ordermoney=pl.col.ordervol * pl.col.orderpx)
                .collect()
                .pipe(px_to_ret, T0_date=T0_date, px_cols="orderpx")
                .pipe(add_turnover, T0_date=T0_date, money_cols="ordermoney")
            )
        elif data_type == "withdraw":
            return (
                lf.with_columns(
                    withdrawmoney=pl.col.withdrawvol * pl.col.withdrawpx,
                    withdrawtmlag=pl.col.withdrawtm.pipe(tm_to_timestamp) - pl.col.ordertm.pipe(tm_to_timestamp),
                )
                .collect()
                .pipe(px_to_ret, T0_date=T0_date, px_cols="withdrawpx")
                .pipe(add_turnover, T0_date=T0_date, money_cols="withdrawmoney")
                .select(
                    "symbol",
                    "withdrawtm",
                    "withdrawpx",
                    "withdrawvol",
                    "ordertm",
                    "ordervol",
                    "withdrawmoney",
                    "withdrawtmlag",
                    "withdrawret",
                    "withdrawturnover",
                )
            )
        elif data_type == "trade":
            return (
                lf.with_columns(
                    trademoney=pl.col.tradevol * pl.col.tradepx,
                    ordertmlag=(
                        pl.col.ordertm_buy.pipe(tm_to_timestamp) - pl.col.ordertm_sell.pipe(tm_to_timestamp)
                    ).abs(),
                )
                .collect()
                .pipe(px_to_ret, T0_date=T0_date, px_cols="tradepx")
                .with_columns(
                    ordermoneydiff=(pl.col.ordermoney_buy - pl.col.ordermoney_sell).abs(),
                    orderturnoverdiff=(pl.col.orderturnover_buy - pl.col.orderturnover_sell).abs(),
                    orderretdiff=(pl.col.orderret_buy - pl.col.orderret_sell).abs(),
                )
                .pipe(add_turnover, T0_date=T0_date, money_cols="trademoney")
            )
        raise ValueError(f"Unknown data_type: {data_type}")

    # --- SH parallel processing (from IncrementalPipeline._process_sh_data_parallel) ---

    def _process_sh_data_parallel(self, new_order_sh, new_trade_sh, prev_buy, prev_sell, T0_date):
        buy_lf, sell_lf, buy_wd_lf, sell_wd_lf = self._process_order_data_sh(new_order_sh)
        trade_lf = self._process_trade_data_sh(new_trade_sh)
        new_buy = buy_lf.collect()
        new_sell = sell_lf.collect()
        new_trade = trade_lf.collect()

        # Complement aggressive orders with trade data
        agg_buy = (
            new_trade.lazy()
            .filter(pl.col.sideflag == 66)
            .group_by("channelid", "tradebuyid")
            .agg(
                pl.col.symbol.first().alias("tradesymbol"),
                pl.col.tradevol.sum(),
                pl.col.tradetm.min(),
                pl.col.tradepx.max(),
            )
        )
        comp_buy = (
            new_buy.lazy()
            .join(agg_buy, how="full", left_on=["channelid", "orderid"], right_on=["channelid", "tradebuyid"])
            .select(
                pl.col.channelid.fill_null(pl.col.channelid_right),
                pl.col.orderid.fill_null(pl.col.tradebuyid),
                pl.col.symbol.fill_null(pl.col.tradesymbol),
                pl.col.ordertm.fill_null(pl.col.tradetm),
                pl.col.orderpx.fill_null(pl.col.tradepx),
                pl.col.ordervol.fill_null(0).add(pl.col.tradevol.fill_null(0)),
            )
        )
        agg_sell = (
            new_trade.lazy()
            .filter(pl.col.sideflag == 83)
            .group_by("channelid", "tradesellid")
            .agg(
                pl.col.symbol.first().alias("tradesymbol"),
                pl.col.tradevol.sum(),
                pl.col.tradetm.min(),
                pl.col.tradepx.min(),
            )
        )
        comp_sell = (
            new_sell.lazy()
            .join(agg_sell, how="full", left_on=["channelid", "orderid"], right_on=["channelid", "tradesellid"])
            .select(
                pl.col.channelid.fill_null(pl.col.channelid_right),
                pl.col.orderid.fill_null(pl.col.tradesellid),
                pl.col.symbol.fill_null(pl.col.tradesymbol),
                pl.col.ordertm.fill_null(pl.col.tradetm),
                pl.col.orderpx.fill_null(pl.col.tradepx),
                pl.col.ordervol.fill_null(0).add(pl.col.tradevol.fill_null(0)),
            )
        )

        fin_buy = self._apply_final_processing(comp_buy, T0_date, "order")
        fin_sell = self._apply_final_processing(comp_sell, T0_date, "order")
        full_buy = pl.concat([prev_buy, fin_buy], how="diagonal_relaxed")
        full_sell = pl.concat([prev_sell, fin_sell], how="diagonal_relaxed")

        buy_ft = full_buy.select(BUY_ORDER_FOR_TRADE_EXPRS)
        sell_ft = full_sell.select(SELL_ORDER_FOR_TRADE_EXPRS)
        ptrade = (
            new_trade.lazy()
            .join(
                buy_ft.lazy(),
                how="left",
                left_on=("channelid", "tradebuyid"),
                right_on=("channelid", "orderid"),
                coalesce=True,
            )
            .join(
                sell_ft.lazy(),
                how="left",
                left_on=("channelid", "tradesellid"),
                right_on=("channelid", "orderid"),
                coalesce=True,
            )
            .collect()
        )
        buy_wf = full_buy.select(ORDER_FOR_WITHDRAW_COLS)
        sell_wf = full_sell.select(ORDER_FOR_WITHDRAW_COLS)
        pbuy_wd = buy_wd_lf.collect().join(
            buy_wf, how="left", left_on=("channelid", "id"), right_on=("channelid", "orderid"), coalesce=True
        )
        psell_wd = sell_wd_lf.collect().join(
            sell_wf, how="left", left_on=("channelid", "id"), right_on=("channelid", "orderid"), coalesce=True
        )

        return {
            "final_buy_ord": fin_buy,
            "final_sell_ord": fin_sell,
            "full_buy_ord": full_buy,
            "full_sell_ord": full_sell,
            "trade": ptrade,
            "buy_withdraw": pbuy_wd,
            "sell_withdraw": psell_wd,
        }

    # --- SZ parallel processing (from IncrementalPipeline._process_sz_data_parallel) ---

    def _process_sz_data_parallel(self, new_order_sz, new_trade_sz, prev_buy, prev_sell, T0_date):
        buy_lf, sell_lf = self._process_order_data_sz(new_order_sz)
        buy_wd_lf, sell_wd_lf, trade_lf = self._process_trade_data_sz(new_trade_sz)
        new_buy = buy_lf.collect()
        new_sell = sell_lf.collect()
        new_trade = trade_lf.collect()

        # Fill market order prices from recent trades
        tpx = pl.concat([self.latest_trade_px_sz, new_trade.select("symbol", "tradetm", "tradepx", "sideflag")])
        new_buy = (
            new_buy.join_asof(
                tpx.filter(pl.col.sideflag == 83).drop("sideflag"),
                left_on="ordertm",
                right_on="tradetm",
                by="symbol",
                strategy="backward",
            )
            .with_columns(
                pl.col.orderpx.fill_null(
                    pl.when(pl.col.ordertype == 49).then(pl.col.tradepx * 1.001).otherwise(pl.col.tradepx)
                )
            )
            .drop("tradetm", "tradepx", "ordertype")
        )
        new_sell = (
            new_sell.join_asof(
                tpx.filter(pl.col.sideflag == 66).drop("sideflag"),
                left_on="ordertm",
                right_on="tradetm",
                by="symbol",
                strategy="backward",
            )
            .with_columns(
                pl.col.orderpx.fill_null(
                    pl.when(pl.col.ordertype == 49).then(pl.col.tradepx * 0.999).otherwise(pl.col.tradepx)
                )
            )
            .drop("tradetm", "tradepx", "ordertype")
        )
        self.latest_trade_px_sz = (
            tpx.group_by("symbol", "sideflag")
            .agg(pl.col("tradepx").last(), pl.col("tradetm").last())
            .select("symbol", "tradetm", "tradepx", "sideflag")
        )

        fin_buy = self._apply_final_processing(new_buy, T0_date, "order")
        fin_sell = self._apply_final_processing(new_sell, T0_date, "order")
        full_buy = pl.concat([prev_buy, fin_buy], how="diagonal_relaxed")
        full_sell = pl.concat([prev_sell, fin_sell], how="diagonal_relaxed")

        buy_ft = full_buy.select(BUY_ORDER_FOR_TRADE_EXPRS)
        sell_ft = full_sell.select(SELL_ORDER_FOR_TRADE_EXPRS)
        ptrade = (
            new_trade.lazy()
            .join(
                buy_ft.lazy(),
                how="left",
                left_on=("channelid", "tradebuyid"),
                right_on=("channelid", "orderid"),
                coalesce=True,
            )
            .join(
                sell_ft.lazy(),
                how="left",
                left_on=("channelid", "tradesellid"),
                right_on=("channelid", "orderid"),
                coalesce=True,
            )
            .collect()
        )
        buy_wf = full_buy.select(ORDER_FOR_WITHDRAW_WITH_PX_COLS)
        sell_wf = full_sell.select(ORDER_FOR_WITHDRAW_WITH_PX_COLS)
        pbuy_wd = (
            buy_wd_lf.collect()
            .join(buy_wf, how="left", left_on=("channelid", "id"), right_on=("channelid", "orderid"), coalesce=True)
            .drop("withdrawpx")
            .rename({"orderpx": "withdrawpx"})
        )
        psell_wd = (
            sell_wd_lf.collect()
            .join(sell_wf, how="left", left_on=("channelid", "id"), right_on=("channelid", "orderid"), coalesce=True)
            .drop("withdrawpx")
            .rename({"orderpx": "withdrawpx"})
        )

        return {
            "final_buy_ord": fin_buy,
            "final_sell_ord": fin_sell,
            "full_buy_ord": full_buy,
            "full_sell_ord": full_sell,
            "trade": ptrade,
            "buy_withdraw": pbuy_wd,
            "sell_withdraw": psell_wd,
        }

    # --- Remove matched orders (from IncrementalPipeline._remove_matched_orders) ---

    def _remove_matched_orders(self, full_buy, full_sell, trade_df, buy_wd, sell_wd):
        buy_excl = (
            pl.concat(
                [
                    trade_df.lazy()
                    .group_by("channelid", "tradebuyid")
                    .agg(pl.col.tradevol.sum().alias("tv"), pl.col.ordervol_buy.first().alias("ov"))
                    .filter(pl.col.ov <= pl.col.tv)
                    .select("channelid", pl.col("tradebuyid").alias("orderid")),
                    buy_wd.lazy()
                    .filter(pl.col.ordervol <= pl.col.withdrawvol)
                    .select("channelid", pl.col("id").alias("orderid")),
                ]
            )
            .unique()
            .collect()
        )
        sell_excl = (
            pl.concat(
                [
                    trade_df.lazy()
                    .group_by("channelid", "tradesellid")
                    .agg(pl.col.tradevol.sum().alias("tv"), pl.col.ordervol_sell.first().alias("ov"))
                    .filter(pl.col.ov <= pl.col.tv)
                    .select("channelid", pl.col("tradesellid").alias("orderid")),
                    sell_wd.lazy()
                    .filter(pl.col.ordervol <= pl.col.withdrawvol)
                    .select("channelid", pl.col("id").alias("orderid")),
                ]
            )
            .unique()
            .collect()
        )
        return (
            full_buy.join(buy_excl, on=["channelid", "orderid"], how="anti"),
            full_sell.join(sell_excl, on=["channelid", "orderid"], how="anti"),
        )

    # --- Incremental data processing (from IncrementalPipeline._incremental_process_data) ---

    def _incremental_process_data(self, T0_datetime: dt.datetime):
        prev_buy_sh = self.processed_buy_order_sh
        prev_sell_sh = self.processed_sell_order_sh
        prev_buy_sz = self.processed_buy_order_sz
        prev_sell_sz = self.processed_sell_order_sz

        new_order_sh, new_trade_sh, new_order_sz, new_trade_sz = self._load_incr_data_per_minute(T0_datetime)
        T0_date = T0_datetime.date()

        with ThreadPoolExecutor(max_workers=2) as executor:
            sh_future = executor.submit(
                self._process_sh_data_parallel, new_order_sh, new_trade_sh, prev_buy_sh, prev_sell_sh, T0_date
            )
            sz_future = executor.submit(
                self._process_sz_data_parallel, new_order_sz, new_trade_sz, prev_buy_sz, prev_sell_sz, T0_date
            )
            sh = sh_future.result()
            sz = sz_future.result()

        trade = pl.concat([sh["trade"], sz["trade"]], how="diagonal_relaxed")
        buy_wd = pl.concat([sh["buy_withdraw"], sz["buy_withdraw"]], how="diagonal_relaxed")
        sell_wd = pl.concat([sh["sell_withdraw"], sz["sell_withdraw"]], how="diagonal_relaxed")

        full_buy_sh, full_sell_sh = self._remove_matched_orders(
            sh["full_buy_ord"], sh["full_sell_ord"], sh["trade"], sh["buy_withdraw"], sh["sell_withdraw"]
        )
        full_buy_sz, full_sell_sz = self._remove_matched_orders(
            sz["full_buy_ord"], sz["full_sell_ord"], sz["trade"], sz["buy_withdraw"], sz["sell_withdraw"]
        )

        buy_ord = pl.concat([sh["final_buy_ord"], sz["final_buy_ord"]], how="diagonal_relaxed")
        sell_ord = pl.concat([sh["final_sell_ord"], sz["final_sell_ord"]], how="diagonal_relaxed")
        fin_buy_wd = self._apply_final_processing(buy_wd, T0_date, "withdraw")
        fin_sell_wd = self._apply_final_processing(sell_wd, T0_date, "withdraw")
        fin_trade = self._apply_final_processing(trade, T0_date, "trade")

        self.processed_buy_order_sh = full_buy_sh
        self.processed_sell_order_sh = full_sell_sh
        self.processed_buy_order_sz = full_buy_sz
        self.processed_sell_order_sz = full_sell_sz

        return buy_ord, sell_ord, fin_buy_wd, fin_sell_wd, fin_trade

    # --- Factor computation from moments (from IncrementalPipeline._cal_factors_from_moments) ---

    def _cal_factors_from_moments(self, rack, moments):
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {
                "buy_order": ex.submit(cal_order_factors_from_moments, moments["buy_order"]),
                "sell_order": ex.submit(cal_order_factors_from_moments, moments["sell_order"]),
                "buy_withdraw": ex.submit(cal_withdraw_factors_from_moments, moments["buy_withdraw"]),
                "sell_withdraw": ex.submit(cal_withdraw_factors_from_moments, moments["sell_withdraw"]),
                "both_trade": ex.submit(cal_trade_factors_from_moments, moments["both_trade"]),
                "buy_trade": ex.submit(cal_trade_factors_from_moments, moments["buy_trade"]),
                "sell_trade": ex.submit(cal_trade_factors_from_moments, moments["sell_trade"]),
            }
            results = {k: f.result() for k, f in futs.items()}

        factors = rack.lazy()
        for key, prefix in [
            ("buy_order", "buy_"),
            ("sell_order", "sell_"),
            ("buy_withdraw", "buy_"),
            ("sell_withdraw", "sell_"),
            ("both_trade", "both_"),
            ("buy_trade", "buy_"),
            ("sell_trade", "sell_"),
        ]:
            factors = factors.join(
                results[key].lazy().select("symbol", pl.all().exclude("symbol").name.prefix(prefix)),
                how="left",
                on="symbol",
            )
        return factors.collect()

    # --- Incremental factor generation (from IncrementalPipeline._gen_incremental_factors) ---

    def _gen_incremental_factors(self, T0_datetime, buy_ord, sell_ord, buy_wd, sell_wd, trade):
        rack = self.universe.select(pl.col.date.dt.combine(T0_datetime.time()).alias("datetime"), "code", "symbol")

        with ThreadPoolExecutor(max_workers=4) as executor:
            futs = {
                "buy_order": executor.submit(cal_order_moments, buy_ord),
                "sell_order": executor.submit(cal_order_moments, sell_ord),
                "buy_withdraw": executor.submit(cal_withdraw_moments, buy_wd),
                "sell_withdraw": executor.submit(cal_withdraw_moments, sell_wd),
                "both_trade": executor.submit(cal_trade_moments, trade),
                "buy_trade": executor.submit(cal_trade_moments, trade.filter(pl.col.sideflag == 66)),
                "sell_trade": executor.submit(cal_trade_moments, trade.filter(pl.col.sideflag == 83)),
            }
            inc = {k: f.result() for k, f in futs.items()}

        # Merge into history moments
        for key in self.moments:
            if self.moments[key] is None:
                self.moments[key] = inc[key]
            else:
                merge_fn = {
                    "buy_order": merge_order_moments,
                    "sell_order": merge_order_moments,
                    "buy_withdraw": merge_withdraw_moments,
                    "sell_withdraw": merge_withdraw_moments,
                    "both_trade": merge_trade_moments,
                    "buy_trade": merge_trade_moments,
                    "sell_trade": merge_trade_moments,
                }
                self.moments[key] = merge_fn[key](self.moments[key], inc[key])

        # At TTS, compute & save factors
        if T0_datetime.time() in CAT1_TTS:
            factors = self._cal_factors_from_moments(rack, self.moments)

            # px_to_ret for vwapx columns
            vwapx_cols = factors.select(pl.col("^.*_vwapx$")).columns
            if vwapx_cols:
                factors = factors.pipe(px_to_ret, T0_date=T0_datetime.date(), px_cols=vwapx_cols)

            factors = (
                factors.drop("symbol")
                .drop_nulls("code")
                .with_columns(
                    cs.float().map_batches(
                        lambda s: (
                            s.to_frame()
                            .select(
                                pl.when(pl.first().is_nan() | pl.first().is_infinite()).then(None).otherwise(pl.first())
                            )
                            .to_series()
                        ),
                        return_dtype=pl.Float64,
                    )
                )
                .select("datetime", "code", cs.numeric().name.suffix("_cat1"))
            )
            return factors
        return None

    # --- Main backfill loop (from IncrementalPipeline.backfill_factors) ---

    def backfill_factors(self):
        logger.info("Starting backfill factors computation")
        for _dt in self.process_dt:
            t0 = time.time()
            buy_ord, sell_ord, buy_wd, sell_wd, trade = self._incremental_process_data(_dt)
            factors = self._gen_incremental_factors(_dt, buy_ord, sell_ord, buy_wd, sell_wd, trade)
            if factors is not None:
                _sub_dir = self.factor_save_dir / "cat1"
                _sub_dir.mkdir(parents=True, exist_ok=True)
                fpath = _sub_dir / f"{_dt.isoformat()}.parquet"
                factors.write_parquet(fpath)
                logger.info(f"Saved cat1 factors at {_dt}: {factors.shape} -> {fpath}")
            logger.info(f"Processed {_dt.time()} in {time.time() - t0:.1f}s")


# ============================================================================
# Entry point
# ============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Demo: backfill cat1 factors from HT2 raw data")
    parser.add_argument("--date", required=True, help="Trading date (YYYY-MM-DD)")
    parser.add_argument("--output-dir", default="/tmp/demo_factors", help="Output directory")
    args = parser.parse_args()

    pipeline = HT2IncrementalPipeline(
        T0_date=dt.date.fromisoformat(args.date),
        factor_save_dir=args.output_dir,
    )
    pipeline.backfill_factors()
