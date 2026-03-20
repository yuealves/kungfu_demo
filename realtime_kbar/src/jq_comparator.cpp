#include "jq_comparator.h"
#include <cmath>
#include <cstdio>
#include <iostream>
#include <iomanip>

static bool float_eq(float a, float b) {
    return std::abs(a - b) < 1e-4f;
}

CompareStats compare_with_jq(
    const std::map<std::pair<int32_t, int>, MinuteBar>& generated,
    const std::vector<JqBar>& jq_bars)
{
    CompareStats stats;

    // Build JQ lookup: (symbol, bar_minute) → JqBar
    std::map<std::pair<int32_t, int>, const JqBar*> jq_map;
    for (auto& jb : jq_bars) {
        int32_t sym = code_to_symbol(jb.code);
        if (sym > 0)
            jq_map[{sym, jb.bar_minute}] = &jb;
    }

    // Compare each JQ bar against generated
    for (auto& [key, jq_ptr] : jq_map) {
        auto it = generated.find(key);
        if (it == generated.end()) {
            stats.jq_only++;
            stats.total++;
            continue;
        }

        stats.total++;
        const auto& gen = it->second;
        const auto& jq = *jq_ptr;

        // Skip bars where both sides have all-zero OHLC
        if (float_eq(jq.open, 0) && float_eq(gen.open, 0)) {
            stats.both_zero++;
            stats.open_match++;
            stats.close_match++;
            stats.high_match++;
            stats.low_match++;
            stats.volume_match++;
            stats.money_exact++;
            stats.money_close++;
            continue;
        }

        if (float_eq(gen.open, jq.open))   stats.open_match++;
        if (float_eq(gen.close, jq.close)) stats.close_match++;
        if (float_eq(gen.high, jq.high))   stats.high_match++;
        if (float_eq(gen.low, jq.low))     stats.low_match++;

        // Volume comparison
        if ((int64_t)gen.volume == jq.volume)
            stats.volume_match++;

        // Money: floor(money + 1e-6) for our side
        double our_money = std::floor(gen.money + 1e-6);
        double jq_money  = jq.money;
        if (std::abs(our_money - jq_money) < 0.5)
            stats.money_exact++;
        if (std::abs(our_money - jq_money) <= 1.5)
            stats.money_close++;
    }

    // Count bars only in generated (not in JQ)
    for (auto& [key, bar] : generated) {
        if (jq_map.find(key) == jq_map.end())
            stats.gen_only++;
    }

    return stats;
}

void print_compare_report(const CompareStats& stats) {
    auto pct = [](int n, int total) -> double {
        return total > 0 ? 100.0 * n / total : 0;
    };

    int compared = stats.total - stats.jq_only;
    printf("\n=== KBar Comparison Report ===\n");
    printf("Total JQ bars:     %d\n", stats.total);
    printf("  matched:         %d\n", compared);
    printf("  jq_only:         %d\n", stats.jq_only);
    printf("  gen_only:        %d\n", stats.gen_only);
    printf("  both_zero:       %d\n", stats.both_zero);
    printf("\n");
    printf("Metric        Match / Compared     Rate\n");
    printf("─────────────────────────────────────────\n");
    printf("open      %8d / %8d    %6.2f%%\n", stats.open_match, compared, pct(stats.open_match, compared));
    printf("close     %8d / %8d    %6.2f%%\n", stats.close_match, compared, pct(stats.close_match, compared));
    printf("high      %8d / %8d    %6.2f%%\n", stats.high_match, compared, pct(stats.high_match, compared));
    printf("low       %8d / %8d    %6.2f%%\n", stats.low_match, compared, pct(stats.low_match, compared));
    printf("volume    %8d / %8d    %6.2f%%\n", stats.volume_match, compared, pct(stats.volume_match, compared));
    printf("money(=)  %8d / %8d    %6.2f%%\n", stats.money_exact, compared, pct(stats.money_exact, compared));
    printf("money(±1) %8d / %8d    %6.2f%%\n", stats.money_close, compared, pct(stats.money_close, compared));
    printf("\n");
}
