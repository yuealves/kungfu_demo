#pragma once
#include <string>
#include <vector>

namespace rtf {

// ---- 桶名枚举 ----
enum class OrderBucket    { ALL, LARGE, SMALL, COUNT };
enum class WithdrawBucket { ALL, LARGE, SMALL, COMPLETE, PARTIAL, INSTANT, COUNT };
enum class TradeBucket    { ALL, LARGE, SMALL, BOLARGE, BOSMALL, SOLARGE, SOSMALL, COUNT };

static constexpr int ORDER_BUCKET_N    = static_cast<int>(OrderBucket::COUNT);      // 3
static constexpr int WITHDRAW_BUCKET_N = static_cast<int>(WithdrawBucket::COUNT);   // 6
static constexpr int TRADE_BUCKET_N    = static_cast<int>(TradeBucket::COUNT);      // 7

// ---- 每种桶内的因子数量 ----
// Order:    count, vwapx, money_{mean,std,skew,kurt} = 6
// Withdraw: count, vwapx, money_{mean,std,skew,kurt}, tmlag_{mean,std,skew,kurt} = 10
// Trade:    count, vwapx, money_{mean,std,skew,kurt}, tmlag_{mean,std,skew,kurt},
//           moneydiff_{mean,std,skew,kurt}, buyorder_count, sellorder_count = 16
static constexpr int ORDER_FACTORS_PER_BUCKET    = 6;
static constexpr int WITHDRAW_FACTORS_PER_BUCKET = 10;
static constexpr int TRADE_FACTORS_PER_BUCKET    = 16;

// ---- 每个事件族的总因子数 ----
static constexpr int ORDER_FAMILY_FACTORS    = ORDER_FACTORS_PER_BUCKET * ORDER_BUCKET_N;       // 6*3=18
static constexpr int WITHDRAW_FAMILY_FACTORS = WITHDRAW_FACTORS_PER_BUCKET * WITHDRAW_BUCKET_N; // 10*6=60
static constexpr int TRADE_FAMILY_FACTORS    = TRADE_FACTORS_PER_BUCKET * TRADE_BUCKET_N;       // 16*7=112

// ---- 七个组件 → 总因子数 ----
// buy_order(18) + sell_order(18) + buy_withdraw(60) + sell_withdraw(60)
// + both_trade(112) + buy_trade(112) + sell_trade(112) = 492
static constexpr int TOTAL_TS_FACTORS =
    2 * ORDER_FAMILY_FACTORS + 2 * WITHDRAW_FAMILY_FACTORS + 3 * TRADE_FAMILY_FACTORS;

// 返回所有因子列名（带前缀），供 CSV header 使用
inline std::vector<std::string> get_factor_names() {
    std::vector<std::string> names;
    names.reserve(TOTAL_TS_FACTORS);

    auto add_order = [&](const std::string& prefix) {
        const char* buckets[] = {"", "_large", "_small"};
        for (auto bk : buckets) {
            std::string sfx(bk);
            names.push_back(prefix + "order_count" + sfx);
            names.push_back(prefix + "ordervwapx" + sfx);
            names.push_back(prefix + "ordermoney_mean" + sfx);
            names.push_back(prefix + "ordermoney_std" + sfx);
            names.push_back(prefix + "ordermoney_skew" + sfx);
            names.push_back(prefix + "ordermoney_kurt" + sfx);
        }
    };

    auto add_withdraw = [&](const std::string& prefix) {
        const char* buckets[] = {"", "_large", "_small", "_complete", "_partial", "_instant"};
        for (auto bk : buckets) {
            std::string sfx(bk);
            names.push_back(prefix + "withdraw_count" + sfx);
            names.push_back(prefix + "withdrawvwapx" + sfx);
            names.push_back(prefix + "withdrawmoney_mean" + sfx);
            names.push_back(prefix + "withdrawmoney_std" + sfx);
            names.push_back(prefix + "withdrawmoney_skew" + sfx);
            names.push_back(prefix + "withdrawmoney_kurt" + sfx);
            names.push_back(prefix + "withdrawtmlag_mean" + sfx);
            names.push_back(prefix + "withdrawtmlag_std" + sfx);
            names.push_back(prefix + "withdrawtmlag_skew" + sfx);
            names.push_back(prefix + "withdrawtmlag_kurt" + sfx);
        }
    };

    auto add_trade = [&](const std::string& prefix) {
        const char* buckets[] = {"", "_large", "_small", "_bolarge", "_bosmall", "_solarge", "_sosmall"};
        for (auto bk : buckets) {
            std::string sfx(bk);
            names.push_back(prefix + "trade_count" + sfx);
            names.push_back(prefix + "tradevwapx" + sfx);
            names.push_back(prefix + "trademoney_mean" + sfx);
            names.push_back(prefix + "trademoney_std" + sfx);
            names.push_back(prefix + "trademoney_skew" + sfx);
            names.push_back(prefix + "trademoney_kurt" + sfx);
            names.push_back(prefix + "ordertmlag_mean" + sfx);
            names.push_back(prefix + "ordertmlag_std" + sfx);
            names.push_back(prefix + "ordertmlag_skew" + sfx);
            names.push_back(prefix + "ordertmlag_kurt" + sfx);
            names.push_back(prefix + "ordermoneydiff_mean" + sfx);
            names.push_back(prefix + "ordermoneydiff_std" + sfx);
            names.push_back(prefix + "ordermoneydiff_skew" + sfx);
            names.push_back(prefix + "ordermoneydiff_kurt" + sfx);
            names.push_back(prefix + "trade_buyorder_count" + sfx);
            names.push_back(prefix + "trade_sellorder_count" + sfx);
        }
    };

    add_order("buy_");
    add_order("sell_");
    add_withdraw("buy_");
    add_withdraw("sell_");
    add_trade("both_");
    add_trade("buy_");
    add_trade("sell_");

    return names;
}

} // namespace rtf
