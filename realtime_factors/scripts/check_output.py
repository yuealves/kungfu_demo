#!/usr/bin/env python3
"""Quick sanity check on realtime_factors CSV output."""
import sys
import csv
import math

def main():
    fname = sys.argv[1] if len(sys.argv) > 1 else "/tmp/rtf_test/factors_final.csv"
    with open(fname) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Total symbols: {len(rows)}")
    print(f"Total columns: {len(rows[0]) if rows else 0}")

    errors = 0
    warn = 0
    for row in rows:
        sym = row["symbol"]
        for k, v in row.items():
            if not v or k == "symbol":
                continue
            try:
                val = float(v)
            except ValueError:
                continue
            # count 应该 >= 0
            if "count" in k and val < 0:
                print(f"ERROR: {sym}.{k} = {val} < 0")
                errors += 1
            # vwapx 应该是合理的股票价格范围
            if "vwapx" in k and not math.isnan(val):
                if val <= 0 or val > 5000:
                    print(f"WARN: {sym}.{k} = {val} looks unusual")
                    warn += 1

    # 汇总第一只股票
    if rows:
        first = rows[0]
        non_empty = sum(1 for k, v in first.items() if v and k != "symbol")
        print(f"\nFirst symbol ({first['symbol']}): {non_empty}/{len(first)-1} non-empty factors")

    # 抽样打印几个因子
    if rows:
        sample = rows[len(rows)//2]  # 中间的股票
        print(f"\nSample symbol ({sample['symbol']}):")
        for k in ["buy_order_count", "buy_ordervwapx", "buy_ordermoney_mean",
                   "sell_order_count", "sell_ordervwapx",
                   "both_trade_count", "both_tradevwapx",
                   "buy_withdraw_count", "buy_withdrawvwapx"]:
            print(f"  {k} = {sample.get(k, 'N/A')}")

    print(f"\nErrors: {errors}, Warnings: {warn}")
    return 1 if errors > 0 else 0

if __name__ == "__main__":
    sys.exit(main())
