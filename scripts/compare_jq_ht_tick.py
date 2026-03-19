#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
对比聚宽（JQ）tick 数据与华泰（HT）tick 数据的完整脚本。

对比字段：
  - Price:  current (JQ)  vs Price     (HT)  [当日最新价]
  - High:   high  (JQ)   vs High      (HT)  [当日最高价]
  - Low:    low   (JQ)   vs Low       (HT)  [当日最低价]
  - Volume: volume (JQ)  vs AccVolume  (HT) [累计成交量]
  - Money:  money  (JQ)  vs AccTurnover (HT)[累计成交额]
  - Ask1-5: a1_p/a1_v ... a5_p/a5_v (JQ) vs AskPx1/AskVol1 ... AskPx5/AskVol5 (HT)
  - Bid1-5: b1_p/b1_v ... b5_p/b5_v (JQ) vs BidPx1/BidVol1 ... BidPx5/BidVol5 (HT)

注意：
  - HT 的 Price/High/Low/AskPx/BidPx 为 x10000 格式（如 147800.0 = 14.78元），
    JQ 的对应字段已是实际价格。脚本会自动处理单位转换。

用法：
    python3 scripts/compare_jq_ht_tick.py [日期]
    默认日期：2026-03-17
"""

import polars as pl
import glob
import sys

JQ_TICK_DIR = "/nkd_shared/assets/stock/price/tick_l1"
HT_TICK_DIR = "/data/tickl2_data_ht2_raw"

PRICE_THR = 0.0001
VOL_THR = 1
MONEY_THR = 100.0


def load_jq_tick(date_str):
    path = "{}/{}.parquet".format(JQ_TICK_DIR, date_str)
    df = pl.read_parquet(path)
    df = df.filter(pl.col("current") > 0)
    df = df.with_columns([
        (pl.col("datetime").dt.hour() * 10000000
         + pl.col("datetime").dt.minute() * 100000
         + pl.col("datetime").dt.second() * 1000).cast(pl.Int64).alias("time"),
        pl.col("code"),
        pl.col("current").alias("jq_price"),
        pl.col("high").alias("jq_high"),
        pl.col("low").alias("jq_low"),
        pl.col("volume").cast(pl.Int64).alias("jq_volume"),
        pl.col("money").alias("jq_money"),
        pl.col("a1_p").alias("jq_a1_p"),
        pl.col("a1_v").alias("jq_a1_v"),
        pl.col("a2_p").alias("jq_a2_p"),
        pl.col("a2_v").alias("jq_a2_v"),
        pl.col("a3_p").alias("jq_a3_p"),
        pl.col("a3_v").alias("jq_a3_v"),
        pl.col("a4_p").alias("jq_a4_p"),
        pl.col("a4_v").alias("jq_a4_v"),
        pl.col("a5_p").alias("jq_a5_p"),
        pl.col("a5_v").alias("jq_a5_v"),
        pl.col("b1_p").alias("jq_b1_p"),
        pl.col("b1_v").alias("jq_b1_v"),
        pl.col("b2_p").alias("jq_b2_p"),
        pl.col("b2_v").alias("jq_b2_v"),
        pl.col("b3_p").alias("jq_b3_p"),
        pl.col("b3_v").alias("jq_b3_v"),
        pl.col("b4_p").alias("jq_b4_p"),
        pl.col("b4_v").alias("jq_b4_v"),
        pl.col("b5_p").alias("jq_b5_p"),
        pl.col("b5_v").alias("jq_b5_v"),
    ])
    return df


def load_ht_tick(date_str):
    date_compact = date_str.replace("-", "")
    files = sorted(glob.glob(
        "{}/{}/{}_tick_data_*.parquet".format(HT_TICK_DIR, date_str, date_compact)))
    frames = [pl.read_parquet(f) for f in files]
    df = pl.concat(frames)
    df = df.filter(df["Price"] > 0)
    df = df.with_columns([
        pl.col("Time").cast(pl.Int64).alias("time"),
        pl.when(pl.col("Symbol") < 400000)
          .then(pl.col("Symbol").cast(pl.Utf8).str.zfill(6) + ".XSHE")
          .otherwise(pl.col("Symbol").cast(pl.Utf8).str.zfill(6) + ".XSHG")
          .alias("code"),
        # HT Price/High/Low/AskPx/BidPx 为 x10000 格式，转为实际价格
        (pl.col("Price").cast(pl.Float64) / 10000.0).alias("ht_price"),
        (pl.col("High").cast(pl.Float64) / 10000.0).alias("ht_high"),
        (pl.col("Low").cast(pl.Float64) / 10000.0).alias("ht_low"),
        pl.col("AccVolume").cast(pl.Int64).alias("ht_volume"),
        pl.col("AccTurnover").alias("ht_money"),
        (pl.col("AskPx1").cast(pl.Float64) / 10000.0).alias("ht_a1_p"),
        pl.col("AskVol1").cast(pl.Int64).alias("ht_a1_v"),
        (pl.col("AskPx2").cast(pl.Float64) / 10000.0).alias("ht_a2_p"),
        pl.col("AskVol2").cast(pl.Int64).alias("ht_a2_v"),
        (pl.col("AskPx3").cast(pl.Float64) / 10000.0).alias("ht_a3_p"),
        pl.col("AskVol3").cast(pl.Int64).alias("ht_a3_v"),
        (pl.col("AskPx4").cast(pl.Float64) / 10000.0).alias("ht_a4_p"),
        pl.col("AskVol4").cast(pl.Int64).alias("ht_a4_v"),
        (pl.col("AskPx5").cast(pl.Float64) / 10000.0).alias("ht_a5_p"),
        pl.col("AskVol5").cast(pl.Int64).alias("ht_a5_v"),
        (pl.col("BidPx1").cast(pl.Float64) / 10000.0).alias("ht_b1_p"),
        pl.col("BidVol1").cast(pl.Int64).alias("ht_b1_v"),
        (pl.col("BidPx2").cast(pl.Float64) / 10000.0).alias("ht_b2_p"),
        pl.col("BidVol2").cast(pl.Int64).alias("ht_b2_v"),
        (pl.col("BidPx3").cast(pl.Float64) / 10000.0).alias("ht_b3_p"),
        pl.col("BidVol3").cast(pl.Int64).alias("ht_b3_v"),
        (pl.col("BidPx4").cast(pl.Float64) / 10000.0).alias("ht_b4_p"),
        pl.col("BidVol4").cast(pl.Int64).alias("ht_b4_v"),
        (pl.col("BidPx5").cast(pl.Float64) / 10000.0).alias("ht_b5_p"),
        pl.col("BidVol5").cast(pl.Int64).alias("ht_b5_v"),
    ])
    return df


def cmp(merged, jq_col, ht_col, thr, label):
    diff = (merged[jq_col] - merged[ht_col]).abs()
    match = int((diff < thr).sum())
    total = len(merged)
    print("  {:>8}: {:>12,} / {:>12,} ({:6.2f}%)".format(
        label, match, total, 100.0 * match / total))
    return match, total


def main():
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-17"

    print("=" * 70)
    print("  JQ tick vs HT tick 全字段对比  date={}".format(date))
    print("=" * 70)

    print("\n[1] Loading JQ tick...")
    jq = load_jq_tick(date)
    print("    JQ rows: {:,}".format(jq.height))

    print("\n[2] Loading HT tick...")
    ht = load_ht_tick(date)
    print("    HT rows: {:,}".format(ht.height))

    # ================================================================
    # 时间覆盖分析：统计 JQ 独有 / HT 独有 / 双方共有 的 (code, time) 数量
    # ================================================================
    print("\n" + "=" * 70)
    print("  时间覆盖分析（按交易时段）")
    print("=" * 70)

    # time = HHMMSSmmm (milliseconds)
    # h = time // 10000000, m = (time // 100000) % 100, s = (time // 1000) % 100
    def add_period(df):
        h = (df["time"] // 10000000).cast(pl.Int32)
        m = ((df["time"] // 100000) % 100).cast(pl.Int32)
        return df.with_columns([
            pl.concat_str(
                pl.when((h < 9) | ((h == 9) & (m < 25))).then(pl.lit("08_盘前"))
                  .when((h == 9) & (m < 30)).then(pl.lit("09_竞价"))
                  .when((h == 9) & (m >= 30)).then(pl.lit("09_早盘"))
                  .when((h >= 10) & (h < 11)).then(pl.lit("10_早盘"))
                  .when((h == 11) & (m < 30)).then(pl.lit("11_早盘"))
                  .when((h == 11) & (m >= 30)).then(pl.lit("11_竞价"))
                  .when((h >= 12) & (h < 13)).then(pl.lit("12_午间"))
                  .when((h == 13)).then(pl.lit("13_下午"))
                  .when((h == 14) & (m < 57)).then(pl.lit("14_尾盘"))
                  .when((h == 14) & (m >= 57)).then(pl.lit("14_竞价"))
                  .when(h >= 15).then(pl.lit("15_收盘"))
                  .otherwise(pl.lit("other")),
                pl.lit("_")
            ).alias("period")
        ])

    jq_period = add_period(jq.select(["time", "code"]))
    ht_period = add_period(ht.select(["time", "code"]))

    periods = ["08_盘前_", "09_竞价_", "09_早盘_", "10_早盘_", "11_早盘_",
               "11_竞价_", "12_午间_", "13_下午_", "14_尾盘_", "14_竞价_", "15_收盘_"]

    print("  {:12s} {:>12s} {:>12s} {:>12s} {:>11s}".format(
        "时段", "JQ总行数", "HT总行数", "共有行数", "一致率"))
    print("  " + "-" * 65)

    for period in periods:
        jq_p = jq_period.filter(pl.col("period") == period)
        ht_p = ht_period.filter(pl.col("period") == period)

        jq_keys = set(zip(jq_p["time"].to_list(), jq_p["code"].to_list()))
        ht_keys = set(zip(ht_p["time"].to_list(), ht_p["code"].to_list()))

        both = jq_keys & ht_keys
        jq_only_n = len(jq_keys - ht_keys)
        ht_only_n = len(ht_keys - jq_keys)
        total_both = len(both)
        jqs = len(jq_keys)
        hts = len(ht_keys)

        cov = 100.0 * total_both / jqs if jqs > 0 else 0
        print("  {:12s} {:>12,} {:>12,} {:>12,} {:>10.2f}%".format(
            period.replace("_", ""), jqs, hts, total_both, cov))
        if jq_only_n > 0 or ht_only_n > 0:
            print("    JQ独有: {:,}, HT独有: {:,}".format(jq_only_n, ht_only_n))

    # Core fields
    jq_core = jq.select(["time", "code", "jq_price", "jq_high", "jq_low",
                          "jq_volume", "jq_money"])
    ht_core = ht.select(["time", "code", "ht_price", "ht_high", "ht_low",
                          "ht_volume", "ht_money"])

    print("\n[3] Merging on (code, time)...")
    merged = jq_core.join(ht_core, left_on=["time", "code"],
                          right_on=["time", "code"], how="inner")
    n = merged.height
    print("    Merged rows: {:,} (JQ: {:,} HT: {:,})".format(n, jq.height, ht.height))

    print("\n" + "=" * 70)
    print("  核心字段（Price / High / Low / Volume / Money）")
    print("=" * 70)
    cmp(merged, "jq_price", "ht_price", PRICE_THR, "price")
    cmp(merged, "jq_high", "ht_high", PRICE_THR, "high")
    cmp(merged, "jq_low", "ht_low", PRICE_THR, "low")
    cmp(merged, "jq_volume", "ht_volume", VOL_THR, "volume")
    cmp(merged, "jq_money", "ht_money", MONEY_THR, "money")

    # Ask/Bid fields
    print("\n" + "=" * 70)
    print("  盘口字段 Ask1-5 / Bid1-5（Price & Volume）")
    print("=" * 70)
    jq_ab = jq.select(["time", "code",
                        "jq_a1_p", "jq_a1_v", "jq_a2_p", "jq_a2_v",
                        "jq_a3_p", "jq_a3_v", "jq_a4_p", "jq_a4_v",
                        "jq_a5_p", "jq_a5_v",
                        "jq_b1_p", "jq_b1_v", "jq_b2_p", "jq_b2_v",
                        "jq_b3_p", "jq_b3_v", "jq_b4_p", "jq_b4_v",
                        "jq_b5_p", "jq_b5_v"])
    ht_ab = ht.select(["time", "code",
                        "ht_a1_p", "ht_a1_v", "ht_a2_p", "ht_a2_v",
                        "ht_a3_p", "ht_a3_v", "ht_a4_p", "ht_a4_v",
                        "ht_a5_p", "ht_a5_v",
                        "ht_b1_p", "ht_b1_v", "ht_b2_p", "ht_b2_v",
                        "ht_b3_p", "ht_b3_v", "ht_b4_p", "ht_b4_v",
                        "ht_b5_p", "ht_b5_v"])
    m_ab = jq_ab.join(ht_ab, left_on=["time", "code"],
                       right_on=["time", "code"], how="inner")
    print("    Merged rows: {:,}".format(m_ab.height))

    for i in range(1, 6):
        cmp(m_ab, "jq_a{}_p".format(i), "ht_a{}_p".format(i), PRICE_THR, "a{}_p".format(i))
        cmp(m_ab, "jq_a{}_v".format(i), "ht_a{}_v".format(i), VOL_THR, "a{}_v".format(i))
    for i in range(1, 6):
        cmp(m_ab, "jq_b{}_p".format(i), "ht_b{}_p".format(i), PRICE_THR, "b{}_p".format(i))
        cmp(m_ab, "jq_b{}_v".format(i), "ht_b{}_v".format(i), VOL_THR, "b{}_v".format(i))

    # Mismatch analysis
    print("\n" + "=" * 70)
    print("  不一致行分析")
    print("=" * 70)
    merged = merged.with_columns([
        (merged["jq_price"] - merged["ht_price"]).abs().alias("price_diff"),
        (merged["jq_volume"] - merged["ht_volume"]).abs().alias("vol_diff"),
        (merged["jq_money"] - merged["ht_money"]).abs().alias("money_diff"),
    ])

    vol_mm = merged.filter(pl.col("vol_diff") > VOL_THR)
    money_mm = merged.filter(pl.col("money_diff") > MONEY_THR)

    print("  Price 不一致: {:,} 行".format(
        int((merged["price_diff"] >= PRICE_THR).sum())))
    print("  Volume 不一致: {:,} 行".format(vol_mm.height))
    if vol_mm.height > 0:
        times = sorted(vol_mm["time"].unique().to_list())
        after_1500 = [t for t in times if int(str(t)[:2]) >= 15]
        print("    时间戳总数: {}, 15:00之后: {} (全部为收盘后)".format(
            len(times), len(after_1500)))
        print("    Sample:")
        for row in vol_mm.head(3).iter_rows(named=True):
            print("      code={} time={} JQ_vol={} HT_vol={}".format(
                row["code"], row["time"], row["jq_volume"], row["ht_volume"]))

    print("  Money 不一致: {:,} 行".format(money_mm.height))
    if money_mm.height > 0:
        print("    Sample:")
        for row in money_mm.head(3).iter_rows(named=True):
            print("      code={} time={} JQ_money={:.2f} HT_money={:.2f}".format(
                row["code"], row["time"], row["jq_money"], row["ht_money"]))

    # Summary
    total = len(merged)
    price_ok = int((merged["price_diff"] < PRICE_THR).sum())
    vol_ok = int((merged["vol_diff"] <= VOL_THR).sum())
    money_ok = int((merged["money_diff"] <= MONEY_THR).sum())

    print("\n" + "=" * 70)
    print("  结论")
    print("=" * 70)
    print("  总行数: {:,}".format(total))
    print("  Price  一致率: {:,.2f}%".format(100.0 * price_ok / total))
    print("  Volume 一致率: {:,.2f}%".format(100.0 * vol_ok / total))
    print("  Money  一致率: {:,.2f}%".format(100.0 * money_ok / total))


if __name__ == "__main__":
    main()
