#pragma once
#include <cstdint>

namespace rtf {

// 常量: Python demo 中的 OPEN_MS, CUTOFF_MS
static constexpr int64_t OPEN_MS   = (9LL*3600 + 29*60 + 57) * 1000;  // 09:29:57
static constexpr int64_t CUTOFF_MS = (12LL*3600 + 59*60 + 57) * 1000; // 12:59:57
static constexpr int64_t ADD_5MIN  = 5LL * 60 * 1000;
static constexpr int64_t SUB_1H30  = 90LL * 60 * 1000;

// HHMMSSmmm (int32) → 毫秒 (从午夜起)
inline int64_t tm_int_to_ms(int32_t tm) {
    int64_t x = static_cast<int64_t>(tm);
    int64_t hh  = x / 10000000LL;
    int64_t rem = x % 10000000LL;
    int64_t mm  = rem / 100000LL;
    int64_t rem2 = rem % 100000LL;
    int64_t ss  = rem2 / 1000LL;
    int64_t mmm = rem2 % 1000LL;
    return (hh * 3600 + mm * 60 + ss) * 1000 + mmm;
}

// 连续竞价时间修正
inline int64_t apply_continuous_session(int64_t ms) {
    if (ms < OPEN_MS)   return ms + ADD_5MIN;
    if (ms > CUTOFF_MS) return ms - SUB_1H30;
    return ms;
}

// 组合: int32 → 连续竞价毫秒
inline int64_t tm_to_continuous_ms(int32_t tm) {
    return apply_continuous_session(tm_int_to_ms(tm));
}

} // namespace rtf
