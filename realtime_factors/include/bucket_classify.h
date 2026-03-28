#pragma once
#include <cstdint>

namespace rtf {

// 阈值常量（与 Python demo factor_config.toml 对齐）
static constexpr int32_t LARGE_VOL    = 100000;
static constexpr double  LARGE_MONEY  = 1000000.0;
static constexpr int32_t SMALL_VOL_1  = 10000;
static constexpr double  SMALL_MONEY_1 = 100000.0;
static constexpr int32_t SMALL_VOL_2  = 1000;
static constexpr double  SMALL_MONEY_2 = 500000.0;
static constexpr int64_t INSTANT_MS   = 3000;  // 3秒

// 大单: vol >= LARGE_VOL OR money >= LARGE_MONEY
inline bool is_large(int32_t vol, double money) {
    return vol >= LARGE_VOL || money >= LARGE_MONEY;
}

// 小单: (vol <= SMALL_VOL_1 AND money <= SMALL_MONEY_1) OR (vol <= SMALL_VOL_2 AND money <= SMALL_MONEY_2)
inline bool is_small(int32_t vol, double money) {
    return (vol <= SMALL_VOL_1 && money <= SMALL_MONEY_1)
        || (vol <= SMALL_VOL_2 && money <= SMALL_MONEY_2);
}

// 撤单分桶
inline bool is_complete(int32_t withdraw_vol, int32_t order_vol) {
    return withdraw_vol >= order_vol;
}

inline bool is_partial(int32_t withdraw_vol, int32_t order_vol) {
    return withdraw_vol < order_vol;
}

inline bool is_instant(int64_t withdraw_tmlag_ms) {
    return withdraw_tmlag_ms >= 0 && withdraw_tmlag_ms <= INSTANT_MS;
}

} // namespace rtf
