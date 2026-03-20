#pragma once

#include "tick_bar_builder.h"
#include "parquet_reader.h"

#include <string>
#include <vector>
#include <unordered_map>
#include <map>

struct CompareStats {
    int total = 0;
    int open_match = 0, close_match = 0, high_match = 0, low_match = 0;
    int volume_match = 0;
    int money_exact = 0, money_close = 0;  // exact and ±1 tolerance
    int both_zero = 0;   // bars where both sides have 0 OHLC
    int jq_only = 0;     // bars only in JQ (no generated bar)
    int gen_only = 0;    // bars only in generated (no JQ bar)
};

// Compare generated bars with JQ bars.
// generated: map from (symbol, bar_minute) → MinuteBar
// jq_bars: vector of JqBar from read_jq_bars()
CompareStats compare_with_jq(
    const std::map<std::pair<int32_t, int>, MinuteBar>& generated,
    const std::vector<JqBar>& jq_bars);

// Print comparison report to stdout
void print_compare_report(const CompareStats& stats);
