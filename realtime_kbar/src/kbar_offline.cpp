/**
 * kbar_offline — 离线 tick parquet → 1min KBar 合成 + JQ 对比
 *
 * 读取华泰 tick parquet，合成聚宽口径 1min K 线，并与 JQ price_1m 对比。
 *
 * 用法: ./kbar_offline --date 2026-03-17 [--code 000001] [--tick-dir ...] [--jq-dir ...]
 */

#include "tick_bar_builder.h"
#include "parquet_reader.h"
#include "jq_comparator.h"

#include <iostream>
#include <iomanip>
#include <chrono>
#include <string>
#include <map>
#include <set>
#include <cmath>
#include <cstdio>

static void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " --date YYYY-MM-DD [options]\n"
              << "  --date      Trading date (required)\n"
              << "  --code      Filter single stock (e.g. 000001)\n"
              << "  --tick-dir  HT tick root (default: /data/tickl2_data_ht2_raw)\n"
              << "  --jq-dir    JQ price_1m dir (default: /nkd_shared/assets/stock/price/price_1m)\n"
              << "  --no-compare  Skip JQ comparison\n";
}

int main(int argc, const char* argv[]) {
    std::string date_str;
    std::string tick_dir = "/data/tickl2_data_ht2_raw";
    std::string jq_dir = "/nkd_shared/assets/stock/price/price_1m";
    int32_t filter_symbol = 0;
    bool do_compare = true;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--date" && i + 1 < argc) {
            date_str = argv[++i];
        } else if (arg == "--code" && i + 1 < argc) {
            filter_symbol = std::stoi(argv[++i]);
        } else if (arg == "--tick-dir" && i + 1 < argc) {
            tick_dir = argv[++i];
        } else if (arg == "--jq-dir" && i + 1 < argc) {
            jq_dir = argv[++i];
        } else if (arg == "--no-compare") {
            do_compare = false;
        } else if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        }
    }

    if (date_str.empty()) {
        print_usage(argv[0]);
        return 1;
    }

    auto t0 = std::chrono::steady_clock::now();

    // 1. Read prev day close
    std::cout << "[1/5] Reading prev day close from " << jq_dir << " ..." << std::endl;
    std::unordered_map<int32_t, float> prev_close;
    try {
        prev_close = read_prev_day_close(jq_dir, date_str);
    } catch (const std::exception& e) {
        std::cerr << "  [warn] Cannot read prev_day_close: " << e.what() << std::endl;
    }
    std::cout << "  prev_day_close: " << prev_close.size() << " stocks" << std::endl;

    // 2. Read tick data
    std::cout << "[2/5] Reading HT tick data from " << tick_dir << "/" << date_str << " ..." << std::endl;
    auto ticks = read_ht_ticks(tick_dir, date_str, filter_symbol);
    // Count unique symbols
    std::set<int32_t> tick_symbols;
    for (auto& t : ticks) tick_symbols.insert(t.symbol);
    std::cout << "  ticks: " << ticks.size() << ", stocks: " << tick_symbols.size() << std::endl;

    auto t1 = std::chrono::steady_clock::now();
    double read_sec = std::chrono::duration<double>(t1 - t0).count();
    std::cout << "  read time: " << std::fixed << std::setprecision(1) << read_sec << "s" << std::endl;

    // 3. Build universe (tick symbols + prev_close symbols)
    std::set<int32_t> universe = tick_symbols;
    for (auto& [sym, _] : prev_close) universe.insert(sym);

    // If JQ file exists for this date, add those symbols too
    std::string jq_path = jq_dir + "/" + date_str + ".parquet";
    std::vector<JqBar> jq_bars;
    if (do_compare) {
        try {
            jq_bars = read_jq_bars(jq_path);
            for (auto& jb : jq_bars) {
                int32_t sym = code_to_symbol(jb.code);
                if (sym > 0) universe.insert(sym);
            }
            std::cout << "  JQ bars: " << jq_bars.size() << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "  [warn] Cannot read JQ: " << e.what() << std::endl;
            do_compare = false;
        }
    }

    if (filter_symbol > 0)
        universe = {filter_symbol};

    std::cout << "  universe: " << universe.size() << " stocks" << std::endl;

    // 4. Build kbars
    std::cout << "[3/5] Building kbars ..." << std::endl;
    TickBarBuilder builder;

    // Set prev day close + ensure all universe symbols have accumulators
    for (int32_t sym : universe) {
        builder.add_symbol(sym);
        auto it = prev_close.find(sym);
        if (it != prev_close.end())
            builder.set_prev_day_close(sym, it->second);
    }

    // Feed ticks
    for (auto& t : ticks) {
        builder.push_tick(t.symbol, t.time, t.price, t.high, t.low,
                          t.acc_volume, t.acc_turnover);
    }

    builder.finalize_all();

    auto t2 = std::chrono::steady_clock::now();
    double build_sec = std::chrono::duration<double>(t2 - t1).count();
    std::cout << "  build time: " << std::fixed << std::setprecision(1) << build_sec << "s" << std::endl;

    // 5. Collect all bars into comparison map
    std::map<std::pair<int32_t, int>, MinuteBar> all_bars;
    int total_bars = 0;
    for (int32_t sym : universe) {
        auto bars = builder.get_bars(sym);
        for (auto& bar : bars) {
            all_bars[{sym, bar.bar_minute}] = bar;
            total_bars++;
        }
    }
    std::cout << "  generated bars: " << total_bars << std::endl;

    // 6. Compare with JQ
    if (do_compare) {
        std::cout << "[4/5] Comparing with JQ ..." << std::endl;
        auto stats = compare_with_jq(all_bars, jq_bars);
        print_compare_report(stats);
    }

    // 7. Print sample bars for a specific stock
    if (filter_symbol > 0) {
        std::cout << "[5/5] Sample bars for " << format_symbol(filter_symbol) << ":\n";
        printf("%-20s %10s %10s %10s %10s %12s %14s\n",
               "time", "open", "close", "high", "low", "volume", "money");
        int shown = 0;
        for (auto& [key, bar] : all_bars) {
            if (key.first != filter_symbol) continue;
            if (bar.volume == 0 && bar.open == 0) continue;
            double money_out = std::floor(bar.money + 1e-6);
            printf("%-20s %10.4f %10.4f %10.4f %10.4f %12.0f %14.0f\n",
                   format_bar_time(date_str, bar.bar_minute).c_str(),
                   bar.open, bar.close, bar.high, bar.low,
                   bar.volume, money_out);
            if (++shown >= 30) { printf("  ...\n"); break; }
        }
    }

    auto t3 = std::chrono::steady_clock::now();
    double total_sec = std::chrono::duration<double>(t3 - t0).count();
    std::cout << "\n[done] total time: " << std::fixed << std::setprecision(1) << total_sec << "s" << std::endl;

    return 0;
}
