# Realtime Factors V1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single-threaded TS-Engine that reads L2 Order/Trade from KungFu Journal, manages order state (SH/SZ rules), computes streaming cumulative moments per symbol, and periodically dumps factor snapshots to CSV for validation against the Python demo.

**Architecture:** Single-process, single-thread event loop. Reads journal via Paged (`JournalReader`), dispatches Order (msg_type=62) and Trade (msg_type=63) events. Per-symbol state includes an `OrderStateStore` (active orders hash map) and `MomentAccumulators` for 7 event families × multiple buckets. At configurable intervals, dumps all symbols' factors to stdout/CSV.

**Tech Stack:** C++17, KungFu v1.0.0 (JournalReader/Paged), Boost 1.62.0 (unit_test_framework), CMake 3.18+

---

## V1 Factor Scope

跳过依赖外部数据（pre_close, circulating_market_cap）的字段。V1 只计算基于 vol/money 的 moments，加上纯时间差字段 (ordertmlag, withdrawtmlag) 和 ordermoneydiff。

### 每个事件族的字段

| 事件族 | vol_col | money_col | extra fields | 分桶 |
|---|---|---|---|---|
| buy_order / sell_order | ordervol | ordermoney | (无) | ALL, large, small |
| buy_withdraw / sell_withdraw | withdrawvol | withdrawmoney | withdrawtmlag | ALL, large, small, complete, partial, instant |
| both_trade / buy_trade / sell_trade | tradevol | trademoney | ordertmlag, ordermoneydiff | ALL, large, small, bolarge, bosmall, solarge, sosmall |

### 每个桶产出的统计量

对 `all_cols = [vol_col, money_col] + fields`:
- `count`: 事件计数
- `{col}_mean`: s1/count（vol_mean 最终变为 vwapx = money_s1/vol_s1）

对 `stat_cols = [money_col] + fields`:
- `{col}_std`: 样本标准差 sqrt(max((s2 - s1²/n)/(n-1), 0))
- `{col}_skew`: (s3/n - 3·μ·s2/n + 2·μ³) / σₚ³，NULL if σₚ² ≤ ε·μ² or n<3
- `{col}_kurt`: (s4/n - 4·μ·s3/n + 6·μ²·s2/n - 3·μ⁴) / σₚ⁴ - 3，NULL if σₚ² ≤ ε·μ² or n<4

其中 ε = 0.0009765625 (float16 eps)，μ = s1/n，σₚ² = s2/n - μ²（population variance）。

对 trade 还有 `unique_count`: tradebuyid/tradesellid 的去重计数。

### 预估因子列数（V1 精简版）

| 组件 | base列 | 条件列 | 小计 | ×份数 | 合计 |
|---|---|---|---|---|---|
| order (vol+money) | 7 | 8×2 | 23 | ×2(buy/sell) | 46 |
| withdraw (vol+money+tmlag) | 12 | 13×5 | 77 | ×2(buy/sell) | 154 |
| trade (vol+money+tmlag+moneydiff) | 21 | 22×6+uc | ~155 | ×3(both/buy/sell) | ~465 |
| **合计** | | | | | **~665** |

---

## File Structure

```
realtime_factors/
├── include/
│   ├── moments.h              # Moments 累加器
│   ├── order_state.h          # OrderStateStore: 订单状态管理
│   ├── event_types.h          # 内部事件结构体定义
│   ├── bucket_classify.h      # 分桶条件判断
│   ├── time_utils.h           # HHMMSSmmm → ms 连续竞价时间转换
│   ├── factor_schema.h        # 因子列名/索引注册表
│   └── ts_engine.h            # TS-Engine 主循环声明
├── src/
│   ├── ts_engine.cpp          # TS-Engine 实现
│   ├── order_state.cpp        # OrderStateStore 实现
│   └── main.cpp               # 入口：参数解析、reader 创建、启动引擎
├── tests/
│   ├── test_moments.cpp       # Moments 单元测试
│   ├── test_order_state.cpp   # OrderStateStore 单元测试
│   ├── test_time_utils.cpp    # 时间转换单元测试
│   └── test_bucket.cpp        # 分桶逻辑单元测试
└── CMakeLists.txt             # 独立构建
```

---

## Task 1: Moments 累加器

**Files:**
- Create: `realtime_factors/include/moments.h`
- Create: `realtime_factors/tests/test_moments.cpp`

这是最核心的数值计算单元。header-only，零依赖。

- [ ] **Step 1: 编写 test_moments.cpp 测试框架**

```cpp
// realtime_factors/tests/test_moments.cpp
#define BOOST_TEST_MODULE MomentsTest
#include <boost/test/unit_test.hpp>
#include "moments.h"
#include <cmath>
#include <vector>

static constexpr double EPS = 1e-9;

BOOST_AUTO_TEST_CASE(test_empty_moments) {
    rtf::Moments m;
    BOOST_CHECK_EQUAL(m.count(), 0);
    BOOST_CHECK(std::isnan(m.mean()));
    BOOST_CHECK(std::isnan(m.std_sample()));
    BOOST_CHECK(std::isnan(m.skew()));
    BOOST_CHECK(std::isnan(m.kurt()));
}

BOOST_AUTO_TEST_CASE(test_single_value) {
    rtf::Moments m;
    m.update(5.0);
    BOOST_CHECK_EQUAL(m.count(), 1);
    BOOST_CHECK_CLOSE(m.mean(), 5.0, EPS);
    BOOST_CHECK(std::isnan(m.std_sample())); // n=1, 样本std无定义
    BOOST_CHECK(std::isnan(m.skew()));       // n<3
    BOOST_CHECK(std::isnan(m.kurt()));       // n<4
}

BOOST_AUTO_TEST_CASE(test_known_sequence) {
    // 值: 1,2,3,4,5 → mean=3, sample_std=sqrt(2.5)≈1.5811
    // s1=15, s2=55, s3=225, s4=979
    // population var = 55/5 - 9 = 2.0
    // skew_num = 225/5 - 3*3*11 + 2*27 = 45-99+54 = 0 → skew=0
    // kurt_num = 979/5 - 4*3*45 + 6*9*11 - 3*81 = 195.8-540+594-243 = 6.8
    // kurt = 6.8/4 - 3 = 1.7 - 3 = -1.3
    rtf::Moments m;
    for (double v : {1.0, 2.0, 3.0, 4.0, 5.0}) m.update(v);
    BOOST_CHECK_EQUAL(m.count(), 5);
    BOOST_CHECK_CLOSE(m.mean(), 3.0, EPS);
    BOOST_CHECK_CLOSE(m.std_sample(), std::sqrt(2.5), 1e-6);
    BOOST_CHECK_SMALL(m.skew(), 1e-6);  // 对称分布 skew=0
    BOOST_CHECK_CLOSE(m.kurt(), -1.3, 1e-4);
}

BOOST_AUTO_TEST_CASE(test_merge_two_moments) {
    rtf::Moments a, b;
    a.update(1.0); a.update(2.0); a.update(3.0);
    b.update(4.0); b.update(5.0);

    rtf::Moments merged = rtf::Moments::merge(a, b);
    BOOST_CHECK_EQUAL(merged.count(), 5);
    BOOST_CHECK_CLOSE(merged.mean(), 3.0, EPS);

    // 应该和一次性输入等价
    rtf::Moments full;
    for (double v : {1.0, 2.0, 3.0, 4.0, 5.0}) full.update(v);
    BOOST_CHECK_CLOSE(merged.std_sample(), full.std_sample(), 1e-9);
    BOOST_CHECK_SMALL(merged.skew() - full.skew(), 1e-9);
    BOOST_CHECK_CLOSE(merged.kurt(), full.kurt(), 1e-6);
}

BOOST_AUTO_TEST_CASE(test_vwapx) {
    // vwapx = money_s1 / vol_s1
    rtf::Moments vol_m, money_m;
    vol_m.update(100); vol_m.update(200);
    money_m.update(1000); money_m.update(2200);
    // vwapx = (1000+2200) / (100+200) = 3200/300 ≈ 10.6667
    double vwapx = money_m.s1() / vol_m.s1();
    BOOST_CHECK_CLOSE(vwapx, 3200.0/300.0, 1e-9);
}

BOOST_AUTO_TEST_CASE(test_constant_values_skew_null) {
    // 所有值相同 → variance=0 → skew/kurt 应返回 NaN
    rtf::Moments m;
    for (int i = 0; i < 10; ++i) m.update(42.0);
    BOOST_CHECK(std::isnan(m.skew()));
    BOOST_CHECK(std::isnan(m.kurt()));
}
```

- [ ] **Step 2: 编写 moments.h**

```cpp
// realtime_factors/include/moments.h
#pragma once
#include <cstdint>
#include <cmath>
#include <limits>

namespace rtf {

// Python demo 中 ε = float(np.finfo(np.float16).eps) = 0.0009765625
static constexpr double MOMENTS_EPS = 0.0009765625;

class Moments {
public:
    Moments() = default;

    void update(double x) {
        n_++;
        s1_ += x;
        double x2 = x * x;
        s2_ += x2;
        s3_ += x2 * x;
        s4_ += x2 * x2;
    }

    // 直接累加合并（等价于 Python 中 pl.col(c).sum()）
    static Moments merge(const Moments& a, const Moments& b) {
        Moments r;
        r.n_  = a.n_  + b.n_;
        r.s1_ = a.s1_ + b.s1_;
        r.s2_ = a.s2_ + b.s2_;
        r.s3_ = a.s3_ + b.s3_;
        r.s4_ = a.s4_ + b.s4_;
        return r;
    }

    int64_t count() const { return n_; }
    double s1() const { return s1_; }
    double s2() const { return s2_; }
    double s3() const { return s3_; }
    double s4() const { return s4_; }

    double mean() const {
        if (n_ == 0) return NAN;
        return s1_ / n_;
    }

    // 样本标准差: sqrt(max((s2 - s1²/n) / (n-1), 0))
    double std_sample() const {
        if (n_ < 2) return NAN;
        double var = (s2_ - s1_ * s1_ / n_) / (n_ - 1);
        return std::sqrt(std::max(var, 0.0));
    }

    // skew: population moments 计算
    double skew() const {
        if (n_ < 3) return NAN;
        double mu = s1_ / n_;
        double vp = s2_ / n_ - mu * mu;  // population variance
        if (vp <= MOMENTS_EPS * mu * mu) return NAN;
        double sp = std::sqrt(vp);
        double num = s3_ / n_ - 3.0 * mu * (s2_ / n_) + 2.0 * mu * mu * mu;
        return num / (sp * sp * sp);
    }

    // kurt: excess kurtosis (减3)
    double kurt() const {
        if (n_ < 4) return NAN;
        double mu = s1_ / n_;
        double vp = s2_ / n_ - mu * mu;
        if (vp <= MOMENTS_EPS * mu * mu) return NAN;
        double sp2 = vp;  // σ²
        double num = s4_ / n_ - 4.0 * mu * (s3_ / n_)
                   + 6.0 * mu * mu * (s2_ / n_) - 3.0 * mu * mu * mu * mu;
        return num / (sp2 * sp2) - 3.0;
    }

private:
    int64_t n_ = 0;
    double s1_ = 0, s2_ = 0, s3_ = 0, s4_ = 0;
};

} // namespace rtf
```

- [ ] **Step 3: 创建 CMakeLists.txt 支持测试**

```cmake
# realtime_factors/CMakeLists.txt
cmake_minimum_required(VERSION 3.18)
project(realtime_factors)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

SET(TOOLCHAIN_DIR "/opt/kungfu/toolchain")
SET(KUNGFU_INCLUDE_DIR "/opt/kungfu/master/include")
SET(KUNGFU_LIB_DIR "/opt/kungfu/master/lib/yijinjing")
SET(BOOST_ROOT "${TOOLCHAIN_DIR}/boost-1.62.0")
SET(BOOST_INCLUDEDIR "${BOOST_ROOT}/include")
SET(BOOST_LIBRARYDIR "${BOOST_ROOT}/lib")

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -O2 -Wall -fPIC")

INCLUDE_DIRECTORIES(
    ${KUNGFU_INCLUDE_DIR}
    ${BOOST_INCLUDEDIR}
    "${PROJECT_SOURCE_DIR}/include"
    "${PROJECT_SOURCE_DIR}/../src"   # 父项目的 data_struct.hpp, sys_messages.h
)

LINK_DIRECTORIES(${KUNGFU_LIB_DIR} ${BOOST_LIBRARYDIR})

# Boost 库列表
SET(Boost_LIBRARIES
    ${BOOST_LIBRARYDIR}/libboost_filesystem.so
    ${BOOST_LIBRARYDIR}/libboost_system.so
    ${BOOST_LIBRARYDIR}/libboost_thread.so
    ${BOOST_LIBRARYDIR}/libboost_chrono.so
    ${BOOST_LIBRARYDIR}/libboost_date_time.so
    ${BOOST_LIBRARYDIR}/libboost_locale.so
    ${BOOST_LIBRARYDIR}/libboost_serialization.so
    ${BOOST_LIBRARYDIR}/libboost_math_tr1.so
    ${BOOST_LIBRARYDIR}/libboost_regex.so
    ${BOOST_LIBRARYDIR}/libboost_program_options.so
    ${BOOST_LIBRARYDIR}/libboost_atomic.so
)

FIND_PACKAGE(PythonInterp 2)
FIND_PACKAGE(PythonLibs 2)
INCLUDE_DIRECTORIES(${PYTHON_INCLUDE_PATH})

# ---- 单元测试 ----
enable_testing()

add_executable(test_moments tests/test_moments.cpp)
target_link_libraries(test_moments ${BOOST_LIBRARYDIR}/libboost_unit_test_framework.so)
add_test(NAME moments COMMAND test_moments)

# ---- 主程序（后续 Task 添加） ----
# add_executable(realtime_factors src/main.cpp src/ts_engine.cpp src/order_state.cpp)
# target_link_libraries(realtime_factors journal -lpthread python2.7 ${Boost_LIBRARIES})
```

- [ ] **Step 4: 编译运行测试**

```bash
cd realtime_factors && mkdir -p build && cd build && cmake .. && make test_moments
./test_moments --log_level=test_suite
```

Expected: 所有 6 个测试用例 PASS。

- [ ] **Step 5: 提交**

```bash
git add realtime_factors/include/moments.h realtime_factors/tests/test_moments.cpp realtime_factors/CMakeLists.txt
git commit -m "feat(realtime_factors): add Moments accumulator with unit tests"
```

---

## Task 2: 时间转换工具

**Files:**
- Create: `realtime_factors/include/time_utils.h`
- Create: `realtime_factors/tests/test_time_utils.cpp`

实现 `HHMMSSmmm → ms` 和连续竞价时间修正，用于 withdrawtmlag 和 ordertmlag 计算。

- [ ] **Step 1: 编写测试**

```cpp
// realtime_factors/tests/test_time_utils.cpp
#define BOOST_TEST_MODULE TimeUtilsTest
#include <boost/test/unit_test.hpp>
#include "time_utils.h"

BOOST_AUTO_TEST_CASE(test_tm_int_to_ms) {
    // 93000000 = 09:30:00.000 → (9*3600+30*60)*1000 = 34200000
    BOOST_CHECK_EQUAL(rtf::tm_int_to_ms(93000000), 34200000LL);
    // 145959999 = 14:59:59.999
    BOOST_CHECK_EQUAL(rtf::tm_int_to_ms(145959999),
        (14*3600+59*60+59)*1000LL + 999);
    // 92500000 = 09:25:00.000
    BOOST_CHECK_EQUAL(rtf::tm_int_to_ms(92500000), (9*3600+25*60)*1000LL);
}

BOOST_AUTO_TEST_CASE(test_continuous_session) {
    // 集合竞价: 09:25:00.000 < OPEN_MS(09:29:57) → +5min
    int64_t ms_0925 = rtf::tm_int_to_ms(92500000);
    int64_t cont_0925 = rtf::apply_continuous_session(ms_0925);
    BOOST_CHECK_EQUAL(cont_0925, ms_0925 + 5*60*1000);

    // 午盘: 13:00:00.000 > CUTOFF_MS(12:59:57) → -90min
    int64_t ms_1300 = rtf::tm_int_to_ms(130000000);
    int64_t cont_1300 = rtf::apply_continuous_session(ms_1300);
    BOOST_CHECK_EQUAL(cont_1300, ms_1300 - 90*60*1000);

    // 正常盘中: 10:00:00.000 → 不变
    int64_t ms_1000 = rtf::tm_int_to_ms(100000000);
    BOOST_CHECK_EQUAL(rtf::apply_continuous_session(ms_1000), ms_1000);
}

BOOST_AUTO_TEST_CASE(test_tm_to_continuous_ms) {
    // 便捷函数: 组合 tm_int_to_ms + apply_continuous_session
    int64_t result = rtf::tm_to_continuous_ms(93000000);
    BOOST_CHECK_EQUAL(result, rtf::tm_int_to_ms(93000000));
}
```

- [ ] **Step 2: 编写 time_utils.h**

```cpp
// realtime_factors/include/time_utils.h
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
```

- [ ] **Step 3: 添加到 CMakeLists.txt 并测试**

在 CMakeLists.txt 中添加:
```cmake
add_executable(test_time_utils tests/test_time_utils.cpp)
target_link_libraries(test_time_utils ${BOOST_LIBRARYDIR}/libboost_unit_test_framework.so)
add_test(NAME time_utils COMMAND test_time_utils)
```

```bash
cd realtime_factors/build && cmake .. && make test_time_utils && ./test_time_utils --log_level=test_suite
```

- [ ] **Step 4: 提交**

```bash
git add realtime_factors/include/time_utils.h realtime_factors/tests/test_time_utils.cpp realtime_factors/CMakeLists.txt
git commit -m "feat(realtime_factors): add time utils (HHMMSSmmm→ms, continuous session)"
```

---

## Task 3: 内部事件类型 + 分桶逻辑

**Files:**
- Create: `realtime_factors/include/event_types.h`
- Create: `realtime_factors/include/bucket_classify.h`
- Create: `realtime_factors/tests/test_bucket.cpp`

定义统一的内部事件结构体，以及分桶条件判断。

- [ ] **Step 1: 编写 event_types.h**

```cpp
// realtime_factors/include/event_types.h
#pragma once
#include <cstdint>
#include <cmath>

namespace rtf {

// 交易所判定
inline bool is_sh(int32_t symbol) { return symbol >= 400000; }
inline bool is_sz(int32_t symbol) { return symbol < 400000; }

// 内部订单事件（从 KyStdOrderType 转换而来）
struct OrderEvent {
    int32_t symbol;
    int32_t channel_id;     // channelid
    int32_t order_id;       // orderid
    int32_t time;           // HHMMSSmmm
    double  price;          // orderpx (已除10000 in journal; 实际 KyStdOrderType.Price 是 float)
    int32_t volume;         // ordervol
    int8_t  side;           // 66='B', 83='S'
    int8_t  order_type;     // SH: 65='A'(新单), 68='D'(撤单); SZ: 50='2'(限价), 49='1'(对手方最优), 85='U'(本方最优)
};

// 内部成交事件
struct TradeEvent {
    int32_t symbol;
    int32_t channel_id;
    int32_t buy_order_id;   // tradebuyid
    int32_t sell_order_id;  // tradesellid
    int32_t time;           // HHMMSSmmm
    double  price;          // tradepx
    int32_t volume;         // tradevol
    int8_t  side;           // 66='B', 83='S' (SH从BSFlag; SZ从buyid>sellid推断)
    int8_t  trade_type;     // SZ: 70='F'(成交), 52='4'(撤单); SH: 不用
};

// 处理后的委托事件（用于因子计算）
struct ProcessedOrder {
    int32_t symbol;
    int32_t channel_id;
    int32_t order_id;
    int32_t time;
    double  price;
    int32_t volume;
    double  money;          // volume * price
};

// 处理后的撤单事件
struct ProcessedWithdraw {
    int32_t symbol;
    int32_t channel_id;
    int32_t withdraw_id;    // 对应原订单 id
    int32_t withdraw_time;
    double  withdraw_price;
    int32_t withdraw_vol;
    double  withdraw_money;
    int32_t order_time;     // 原订单时间
    int32_t order_vol;      // 原订单量
    int64_t withdraw_tmlag; // 连续竞价时间差 (ms)
};

// 处理后的成交事件
struct ProcessedTrade {
    int32_t symbol;
    int32_t channel_id;
    int32_t buy_order_id;
    int32_t sell_order_id;
    int32_t time;
    double  price;
    int32_t volume;
    double  money;          // volume * price
    int8_t  side;           // 66='B', 83='S'
    // 买方委托信息
    int32_t ordertm_buy;
    double  orderpx_buy;
    int32_t ordervol_buy;
    double  ordermoney_buy;
    // 卖方委托信息
    int32_t ordertm_sell;
    double  orderpx_sell;
    int32_t ordervol_sell;
    double  ordermoney_sell;
    // 衍生
    int64_t order_tmlag;    // |continuous_ms(ordertm_buy) - continuous_ms(ordertm_sell)|
    double  order_money_diff; // |ordermoney_buy - ordermoney_sell|
};

} // namespace rtf
```

- [ ] **Step 2: 编写 bucket_classify.h**

```cpp
// realtime_factors/include/bucket_classify.h
#pragma once
#include <cstdint>

namespace rtf {

// 阈值常量（与 Python demo factor_config.toml 对齐）
static constexpr int32_t LARGE_VOL  = 100000;
static constexpr double  LARGE_MONEY = 1000000.0;
static constexpr int32_t SMALL_VOL_1  = 10000;
static constexpr double  SMALL_MONEY_1 = 100000.0;
static constexpr int32_t SMALL_VOL_2  = 1000;
static constexpr double  SMALL_MONEY_2 = 500000.0;
static constexpr int64_t INSTANT_MS = 3000;  // 3秒

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
```

- [ ] **Step 3: 编写 test_bucket.cpp**

```cpp
// realtime_factors/tests/test_bucket.cpp
#define BOOST_TEST_MODULE BucketTest
#include <boost/test/unit_test.hpp>
#include "bucket_classify.h"

BOOST_AUTO_TEST_CASE(test_large) {
    BOOST_CHECK(rtf::is_large(100000, 0));      // vol >= 100k
    BOOST_CHECK(rtf::is_large(0, 1000000));      // money >= 1M
    BOOST_CHECK(rtf::is_large(200000, 2000000)); // both
    BOOST_CHECK(!rtf::is_large(99999, 999999));  // neither
}

BOOST_AUTO_TEST_CASE(test_small) {
    BOOST_CHECK(rtf::is_small(10000, 100000));   // cond1
    BOOST_CHECK(rtf::is_small(1000, 500000));    // cond2
    BOOST_CHECK(rtf::is_small(500, 50000));      // both
    BOOST_CHECK(!rtf::is_small(10001, 100001));  // neither
    BOOST_CHECK(!rtf::is_small(50000, 50000));   // vol too high for cond1, money too low for cond2 but vol too high
}

BOOST_AUTO_TEST_CASE(test_withdraw_buckets) {
    BOOST_CHECK(rtf::is_complete(100, 100));
    BOOST_CHECK(rtf::is_complete(150, 100));
    BOOST_CHECK(!rtf::is_complete(50, 100));
    BOOST_CHECK(rtf::is_partial(50, 100));
    BOOST_CHECK(!rtf::is_partial(100, 100));
    BOOST_CHECK(rtf::is_instant(0));
    BOOST_CHECK(rtf::is_instant(3000));
    BOOST_CHECK(!rtf::is_instant(3001));
    BOOST_CHECK(!rtf::is_instant(-1));
}
```

- [ ] **Step 4: 添加到 CMakeLists.txt 并测试**

```cmake
add_executable(test_bucket tests/test_bucket.cpp)
target_link_libraries(test_bucket ${BOOST_LIBRARYDIR}/libboost_unit_test_framework.so)
add_test(NAME bucket COMMAND test_bucket)
```

```bash
cd realtime_factors/build && cmake .. && make test_bucket && ./test_bucket --log_level=test_suite
```

- [ ] **Step 5: 提交**

```bash
git add realtime_factors/include/event_types.h realtime_factors/include/bucket_classify.h realtime_factors/tests/test_bucket.cpp realtime_factors/CMakeLists.txt
git commit -m "feat(realtime_factors): add event types and bucket classification"
```

---

## Task 4: 因子 Schema 注册表

**Files:**
- Create: `realtime_factors/include/factor_schema.h`

定义所有因子列的枚举/索引，以及按事件族输出因子向量的布局。这是保证 C++ 输出列名与 Python 对齐的关键。

- [ ] **Step 1: 编写 factor_schema.h**

```cpp
// realtime_factors/include/factor_schema.h
#pragma once
#include <string>
#include <vector>
#include <array>

namespace rtf {

// ---- 每个 Moments 实例产出的统计量索引 ----
// 对 all_cols: count, mean (vol_mean 变 vwapx)
// 对 stat_cols: std, skew, kurt
// 实际上 count 只有事件级别一个，不是每个 col 一个

// 单个分桶内的因子: 我们对每个 (family, bucket) 维护多组 Moments
// Order: vol, money → 因子: count, vwapx, money_mean, money_std, money_skew, money_kurt = 6
// Withdraw: vol, money, tmlag → 因子: count, vwapx, money_mean/std/skew/kurt, tmlag_mean/std/skew/kurt = 10
// Trade: vol, money, tmlag, moneydiff → 因子: count, vwapx, money_mean/std/skew/kurt, tmlag_mean/std/skew/kurt, moneydiff_mean/std/skew/kurt + buyorder_count + sellorder_count = 16

// ---- 桶名枚举 ----
enum class OrderBucket { ALL, LARGE, SMALL, COUNT };
enum class WithdrawBucket { ALL, LARGE, SMALL, COMPLETE, PARTIAL, INSTANT, COUNT };
enum class TradeBucket { ALL, LARGE, SMALL, BOLARGE, BOSMALL, SOLARGE, SOSMALL, COUNT };

static constexpr int ORDER_BUCKET_N    = static_cast<int>(OrderBucket::COUNT);      // 3
static constexpr int WITHDRAW_BUCKET_N = static_cast<int>(WithdrawBucket::COUNT);   // 6
static constexpr int TRADE_BUCKET_N    = static_cast<int>(TradeBucket::COUNT);      // 7

// ---- 每种桶内的因子数量 ----
static constexpr int ORDER_FACTORS_PER_BUCKET    = 6;   // count, vwapx, money_{mean,std,skew,kurt}
static constexpr int WITHDRAW_FACTORS_PER_BUCKET = 10;  // count, vwapx, money_{...4}, tmlag_{...4}
static constexpr int TRADE_FACTORS_PER_BUCKET    = 16;  // count, vwapx, money_{...4}, tmlag_{...4}, moneydiff_{...4}, buyorder_count, sellorder_count

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
```

- [ ] **Step 2: 提交**

```bash
git add realtime_factors/include/factor_schema.h
git commit -m "feat(realtime_factors): add factor schema registry (492 factors)"
```

---

## Task 5: SymbolState — 每只股票的因子累加器

**Files:**
- Create: `realtime_factors/include/symbol_state.h`

每只股票维护 7 个事件族 × 各自的分桶 × 每个桶内多组 Moments。这是因子计算的核心状态容器。

- [ ] **Step 1: 编写 symbol_state.h**

```cpp
// realtime_factors/include/symbol_state.h
#pragma once
#include "moments.h"
#include "factor_schema.h"
#include "bucket_classify.h"
#include "event_types.h"
#include "time_utils.h"
#include <array>
#include <cmath>
#include <unordered_set>

namespace rtf {

// ---- 单个事件族的分桶 Moments 容器 ----

// Order: vol, money × 3 buckets
struct OrderMoments {
    // [bucket][0=vol, 1=money]
    Moments data[ORDER_BUCKET_N][2];

    void update(const ProcessedOrder& o, OrderBucket bk) {
        int b = static_cast<int>(bk);
        data[b][0].update(o.volume);
        data[b][1].update(o.money);
    }

    void feed(const ProcessedOrder& o) {
        update(o, OrderBucket::ALL);
        if (is_large(o.volume, o.money)) update(o, OrderBucket::LARGE);
        if (is_small(o.volume, o.money)) update(o, OrderBucket::SMALL);
    }

    // 输出 ORDER_FAMILY_FACTORS 个 double 到 out
    void write_factors(double* out) const {
        int idx = 0;
        for (int b = 0; b < ORDER_BUCKET_N; ++b) {
            const auto& vol = data[b][0];
            const auto& money = data[b][1];
            out[idx++] = static_cast<double>(vol.count()); // count (用 vol.count 或 money.count 均可)
            // vwapx = money_s1 / vol_s1
            out[idx++] = (vol.s1() > 0) ? money.s1() / vol.s1() : NAN;
            out[idx++] = money.mean();
            out[idx++] = money.std_sample();
            out[idx++] = money.skew();
            out[idx++] = money.kurt();
        }
    }
};

// Withdraw: vol, money, tmlag × 6 buckets
struct WithdrawMoments {
    // [bucket][0=vol, 1=money, 2=tmlag]
    Moments data[WITHDRAW_BUCKET_N][3];

    void update(const ProcessedWithdraw& w, WithdrawBucket bk) {
        int b = static_cast<int>(bk);
        data[b][0].update(w.withdraw_vol);
        data[b][1].update(w.withdraw_money);
        data[b][2].update(static_cast<double>(w.withdraw_tmlag));
    }

    void feed(const ProcessedWithdraw& w) {
        update(w, WithdrawBucket::ALL);
        if (is_large(w.withdraw_vol, w.withdraw_money)) update(w, WithdrawBucket::LARGE);
        if (is_small(w.withdraw_vol, w.withdraw_money)) update(w, WithdrawBucket::SMALL);
        if (is_complete(w.withdraw_vol, w.order_vol))   update(w, WithdrawBucket::COMPLETE);
        if (is_partial(w.withdraw_vol, w.order_vol))    update(w, WithdrawBucket::PARTIAL);
        if (is_instant(w.withdraw_tmlag))               update(w, WithdrawBucket::INSTANT);
    }

    void write_factors(double* out) const {
        int idx = 0;
        for (int b = 0; b < WITHDRAW_BUCKET_N; ++b) {
            const auto& vol = data[b][0];
            const auto& money = data[b][1];
            const auto& tmlag = data[b][2];
            out[idx++] = static_cast<double>(vol.count());
            out[idx++] = (vol.s1() > 0) ? money.s1() / vol.s1() : NAN;
            out[idx++] = money.mean();
            out[idx++] = money.std_sample();
            out[idx++] = money.skew();
            out[idx++] = money.kurt();
            out[idx++] = tmlag.mean();
            out[idx++] = tmlag.std_sample();
            out[idx++] = tmlag.skew();
            out[idx++] = tmlag.kurt();
        }
    }
};

// Trade: vol, money, tmlag, moneydiff × 7 buckets + unique counts
struct TradeMoments {
    // [bucket][0=vol, 1=money, 2=tmlag, 3=moneydiff]
    Moments data[TRADE_BUCKET_N][4];
    // unique order id sets per bucket
    std::unordered_set<int64_t> buy_order_ids[TRADE_BUCKET_N];
    std::unordered_set<int64_t> sell_order_ids[TRADE_BUCKET_N];

    // 合成 (channel_id, order_id) 为唯一 key
    static int64_t make_key(int32_t channel, int32_t order_id) {
        return (static_cast<int64_t>(channel) << 32) | static_cast<int64_t>(static_cast<uint32_t>(order_id));
    }

    void update(const ProcessedTrade& t, TradeBucket bk) {
        int b = static_cast<int>(bk);
        data[b][0].update(t.volume);
        data[b][1].update(t.money);
        data[b][2].update(static_cast<double>(t.order_tmlag));
        data[b][3].update(t.order_money_diff);
        if (t.buy_order_id != 0)
            buy_order_ids[b].insert(make_key(t.channel_id, t.buy_order_id));
        if (t.sell_order_id != 0)
            sell_order_ids[b].insert(make_key(t.channel_id, t.sell_order_id));
    }

    void feed(const ProcessedTrade& t) {
        update(t, TradeBucket::ALL);
        if (is_large(t.volume, t.money))                update(t, TradeBucket::LARGE);
        if (is_small(t.volume, t.money))                update(t, TradeBucket::SMALL);
        if (is_large(t.ordervol_buy, t.ordermoney_buy)) update(t, TradeBucket::BOLARGE);
        if (is_small(t.ordervol_buy, t.ordermoney_buy)) update(t, TradeBucket::BOSMALL);
        if (is_large(t.ordervol_sell, t.ordermoney_sell)) update(t, TradeBucket::SOLARGE);
        if (is_small(t.ordervol_sell, t.ordermoney_sell)) update(t, TradeBucket::SOSMALL);
    }

    void write_factors(double* out) const {
        int idx = 0;
        for (int b = 0; b < TRADE_BUCKET_N; ++b) {
            const auto& vol = data[b][0];
            const auto& money = data[b][1];
            const auto& tmlag = data[b][2];
            const auto& mdiff = data[b][3];
            out[idx++] = static_cast<double>(vol.count());
            out[idx++] = (vol.s1() > 0) ? money.s1() / vol.s1() : NAN;
            out[idx++] = money.mean();
            out[idx++] = money.std_sample();
            out[idx++] = money.skew();
            out[idx++] = money.kurt();
            out[idx++] = tmlag.mean();
            out[idx++] = tmlag.std_sample();
            out[idx++] = tmlag.skew();
            out[idx++] = tmlag.kurt();
            out[idx++] = mdiff.mean();
            out[idx++] = mdiff.std_sample();
            out[idx++] = mdiff.skew();
            out[idx++] = mdiff.kurt();
            out[idx++] = static_cast<double>(buy_order_ids[b].size());
            out[idx++] = static_cast<double>(sell_order_ids[b].size());
        }
    }
};

// ---- 单只股票的完整因子状态 ----
struct SymbolState {
    OrderMoments    buy_order;
    OrderMoments    sell_order;
    WithdrawMoments buy_withdraw;
    WithdrawMoments sell_withdraw;
    TradeMoments    both_trade;
    TradeMoments    buy_trade;
    TradeMoments    sell_trade;

    // 输出全量因子向量 (TOTAL_TS_FACTORS 个 double)
    void write_all_factors(double* out) const {
        double* p = out;
        buy_order.write_factors(p);    p += ORDER_FAMILY_FACTORS;
        sell_order.write_factors(p);   p += ORDER_FAMILY_FACTORS;
        buy_withdraw.write_factors(p); p += WITHDRAW_FAMILY_FACTORS;
        sell_withdraw.write_factors(p); p += WITHDRAW_FAMILY_FACTORS;
        both_trade.write_factors(p);   p += TRADE_FAMILY_FACTORS;
        buy_trade.write_factors(p);    p += TRADE_FAMILY_FACTORS;
        sell_trade.write_factors(p);   // p += TRADE_FAMILY_FACTORS;
    }
};

} // namespace rtf
```

- [ ] **Step 2: 提交**

```bash
git add realtime_factors/include/symbol_state.h
git commit -m "feat(realtime_factors): add SymbolState with 7 event families and bucketed moments"
```

---

## Task 6: OrderStateStore — 订单状态管理

**Files:**
- Create: `realtime_factors/include/order_state.h`
- Create: `realtime_factors/src/order_state.cpp`
- Create: `realtime_factors/tests/test_order_state.cpp`

这是最复杂的组件。需要完整复刻 Python demo 的 SH/SZ 订单处理规则。

- [ ] **Step 1: 编写 order_state.h**

```cpp
// realtime_factors/include/order_state.h
#pragma once
#include "event_types.h"
#include "symbol_state.h"
#include "time_utils.h"
#include <unordered_map>
#include <functional>

namespace rtf {

// 活跃订单记录
struct ActiveOrder {
    int32_t symbol;
    int32_t channel_id;
    int32_t order_id;
    int32_t time;       // ordertm
    double  price;      // orderpx
    int32_t volume;     // ordervol
    double  money;      // ordervol * orderpx
    int8_t  side;       // 66='B', 83='S'
    int32_t traded_vol; // 已成交量（用于判断订单是否完成）
};

// 复合 key: (channel_id, order_id)
struct OrderKey {
    int32_t channel_id;
    int32_t order_id;
    bool operator==(const OrderKey& o) const {
        return channel_id == o.channel_id && order_id == o.order_id;
    }
};

struct OrderKeyHash {
    size_t operator()(const OrderKey& k) const {
        return std::hash<int64_t>{}(
            (static_cast<int64_t>(k.channel_id) << 32) |
            static_cast<int64_t>(static_cast<uint32_t>(k.order_id)));
    }
};

// 深圳最新成交价记录（按 symbol+side）
struct LatestTradePrice {
    double price;
    int32_t time;
};

class OrderStateStore {
public:
    // 处理一条原始委托事件，返回产出的 ProcessedOrder 或 ProcessedWithdraw
    // 通过回调发送给 SymbolState
    using OrderCallback = std::function<void(int8_t side, const ProcessedOrder&)>;
    using WithdrawCallback = std::function<void(int8_t side, const ProcessedWithdraw&)>;
    using TradeCallback = std::function<void(const ProcessedTrade&)>;

    void on_order_sh(const OrderEvent& evt,
                     OrderCallback on_order,
                     WithdrawCallback on_withdraw);

    void on_trade_sh(const TradeEvent& evt,
                     OrderCallback on_order,
                     TradeCallback on_trade);

    void on_order_sz(const OrderEvent& evt,
                     OrderCallback on_order);

    void on_trade_sz(const TradeEvent& evt,
                     WithdrawCallback on_withdraw,
                     TradeCallback on_trade);

    // 获取活跃订单（用于测试验证）
    const ActiveOrder* find_order(int32_t channel_id, int32_t order_id) const;

private:
    // 活跃订单表
    std::unordered_map<OrderKey, ActiveOrder, OrderKeyHash> orders_;

    // 深圳最新成交价: key = (symbol << 8) | side
    std::unordered_map<int64_t, LatestTradePrice> sz_latest_trade_px_;

    int64_t sz_trade_px_key(int32_t symbol, int8_t side) const {
        return (static_cast<int64_t>(symbol) << 8) | static_cast<int64_t>(static_cast<uint8_t>(side));
    }

    void insert_order(const OrderEvent& evt, double price);
    void remove_order(int32_t channel_id, int32_t order_id);

    // SH: 从成交数据补全激进单
    void complement_aggressive_order_sh(const TradeEvent& evt,
                                        int32_t order_id, int8_t side,
                                        OrderCallback on_order);

    // 构建 ProcessedWithdraw
    ProcessedWithdraw make_withdraw(const ActiveOrder& orig,
                                    int32_t withdraw_time,
                                    double withdraw_price,
                                    int32_t withdraw_vol) const;

    // 构建 ProcessedTrade
    ProcessedTrade make_trade(const TradeEvent& evt,
                              const ActiveOrder* buy_order,
                              const ActiveOrder* sell_order) const;
};

} // namespace rtf
```

- [ ] **Step 2: 编写 order_state.cpp**

```cpp
// realtime_factors/src/order_state.cpp
#include "order_state.h"
#include <cmath>

namespace rtf {

const ActiveOrder* OrderStateStore::find_order(int32_t channel_id, int32_t order_id) const {
    auto it = orders_.find({channel_id, order_id});
    return (it != orders_.end()) ? &it->second : nullptr;
}

void OrderStateStore::insert_order(const OrderEvent& evt, double price) {
    OrderKey key{static_cast<int32_t>(evt.channel_id), evt.order_id};
    ActiveOrder ao;
    ao.symbol = evt.symbol;
    ao.channel_id = evt.channel_id;
    ao.order_id = evt.order_id;
    ao.time = evt.time;
    ao.price = price;
    ao.volume = evt.volume;
    ao.money = evt.volume * price;
    ao.side = evt.side;
    ao.traded_vol = 0;
    orders_[key] = ao;
}

void OrderStateStore::remove_order(int32_t channel_id, int32_t order_id) {
    orders_.erase({channel_id, order_id});
}

ProcessedWithdraw OrderStateStore::make_withdraw(const ActiveOrder& orig,
                                                  int32_t withdraw_time,
                                                  double withdraw_price,
                                                  int32_t withdraw_vol) const {
    ProcessedWithdraw pw;
    pw.symbol = orig.symbol;
    pw.channel_id = orig.channel_id;
    pw.withdraw_id = orig.order_id;
    pw.withdraw_time = withdraw_time;
    pw.withdraw_price = withdraw_price;
    pw.withdraw_vol = withdraw_vol;
    pw.withdraw_money = withdraw_vol * withdraw_price;
    pw.order_time = orig.time;
    pw.order_vol = orig.volume;
    pw.withdraw_tmlag = tm_to_continuous_ms(withdraw_time) - tm_to_continuous_ms(orig.time);
    return pw;
}

ProcessedTrade OrderStateStore::make_trade(const TradeEvent& evt,
                                           const ActiveOrder* buy_order,
                                           const ActiveOrder* sell_order) const {
    ProcessedTrade pt;
    pt.symbol = evt.symbol;
    pt.channel_id = evt.channel_id;
    pt.buy_order_id = evt.buy_order_id;
    pt.sell_order_id = evt.sell_order_id;
    pt.time = evt.time;
    pt.price = evt.price;
    pt.volume = evt.volume;
    pt.money = evt.volume * evt.price;
    pt.side = evt.side;

    if (buy_order) {
        pt.ordertm_buy = buy_order->time;
        pt.orderpx_buy = buy_order->price;
        pt.ordervol_buy = buy_order->volume;
        pt.ordermoney_buy = buy_order->money;
    } else {
        pt.ordertm_buy = 0;
        pt.orderpx_buy = NAN;
        pt.ordervol_buy = 0;
        pt.ordermoney_buy = NAN;
    }

    if (sell_order) {
        pt.ordertm_sell = sell_order->time;
        pt.orderpx_sell = sell_order->price;
        pt.ordervol_sell = sell_order->volume;
        pt.ordermoney_sell = sell_order->money;
    } else {
        pt.ordertm_sell = 0;
        pt.orderpx_sell = NAN;
        pt.ordervol_sell = 0;
        pt.ordermoney_sell = NAN;
    }

    // ordertmlag = |continuous_ms(ordertm_buy) - continuous_ms(ordertm_sell)|
    if (buy_order && sell_order) {
        pt.order_tmlag = std::abs(
            tm_to_continuous_ms(buy_order->time) - tm_to_continuous_ms(sell_order->time));
        pt.order_money_diff = std::abs(buy_order->money - sell_order->money);
    } else {
        pt.order_tmlag = 0;
        pt.order_money_diff = NAN;
    }

    return pt;
}

// ============ SH 处理 ============

void OrderStateStore::on_order_sh(const OrderEvent& evt,
                                   OrderCallback on_order,
                                   WithdrawCallback on_withdraw) {
    if (evt.order_id <= 0) return;

    if (evt.order_type == 68) {
        // 撤单 (D)
        auto* orig = find_order(evt.channel_id, evt.order_id);
        if (orig) {
            auto pw = make_withdraw(*orig, evt.time, evt.price, evt.volume);
            on_withdraw(orig->side, pw);
            // 如果撤单量 >= 订单量，移除订单
            if (evt.volume >= orig->volume) {
                remove_order(evt.channel_id, evt.order_id);
            }
        }
    } else if (evt.order_type == 65) {
        // 新单 (A) — 不过可能是激进单不在此出现
        insert_order(evt, evt.price);
        ProcessedOrder po;
        po.symbol = evt.symbol;
        po.channel_id = evt.channel_id;
        po.order_id = evt.order_id;
        po.time = evt.time;
        po.price = evt.price;
        po.volume = evt.volume;
        po.money = evt.volume * evt.price;
        on_order(evt.side, po);
    }
}

void OrderStateStore::complement_aggressive_order_sh(const TradeEvent& evt,
                                                      int32_t order_id, int8_t side,
                                                      OrderCallback on_order) {
    auto* existing = find_order(evt.channel_id, order_id);
    if (!existing) {
        // 激进单不在 order stream 中，从 trade 数据补全
        OrderEvent synth;
        synth.symbol = evt.symbol;
        synth.channel_id = evt.channel_id;
        synth.order_id = order_id;
        synth.time = evt.time;
        synth.price = evt.price;
        synth.volume = evt.volume;
        synth.side = side;
        synth.order_type = 65;
        insert_order(synth, evt.price);

        ProcessedOrder po;
        po.symbol = evt.symbol;
        po.channel_id = evt.channel_id;
        po.order_id = order_id;
        po.time = evt.time;
        po.price = evt.price;
        po.volume = evt.volume;
        po.money = evt.volume * evt.price;
        on_order(side, po);
    } else {
        // 已存在，累加成交量到 volume（Python demo 中 ordervol = ordervol + tradevol）
        existing->volume += evt.volume;
        existing->money = existing->volume * existing->price;
    }
}

void OrderStateStore::on_trade_sh(const TradeEvent& evt,
                                   OrderCallback on_order,
                                   TradeCallback on_trade) {
    if (evt.buy_order_id == 0 || evt.sell_order_id == 0) return;

    // 补全激进单
    if (evt.side == 66) {
        // 买方主动 → 买方可能是激进单
        complement_aggressive_order_sh(evt, evt.buy_order_id, 66, on_order);
    } else if (evt.side == 83) {
        // 卖方主动 → 卖方可能是激进单
        complement_aggressive_order_sh(evt, evt.sell_order_id, 83, on_order);
    }

    // 查找双方订单
    auto* buy_ord = find_order(evt.channel_id, evt.buy_order_id);
    auto* sell_ord = find_order(evt.channel_id, evt.sell_order_id);

    auto pt = make_trade(evt, buy_ord, sell_ord);
    on_trade(pt);

    // 更新已成交量
    if (buy_ord) {
        buy_ord->traded_vol += evt.volume;
        if (buy_ord->traded_vol >= buy_ord->volume) {
            remove_order(evt.channel_id, evt.buy_order_id);
        }
    }
    if (sell_ord) {
        sell_ord->traded_vol += evt.volume;
        if (sell_ord->traded_vol >= sell_ord->volume) {
            remove_order(evt.channel_id, evt.sell_order_id);
        }
    }
}

// ============ SZ 处理 ============

void OrderStateStore::on_order_sz(const OrderEvent& evt,
                                   OrderCallback on_order) {
    if (evt.order_id <= 0) return;

    // SZ 市价单: ordertype != 50('2') → 价格为 null → 需要用最新成交价回填
    double price = evt.price;
    if (evt.order_type != 50) {
        // 市价单价格回填
        int8_t lookup_side = (evt.side == 66) ? 83 : 66; // 买单→查卖成交价, 卖单→查买成交价
        auto it = sz_latest_trade_px_.find(sz_trade_px_key(evt.symbol, lookup_side));
        if (it != sz_latest_trade_px_.end()) {
            double factor = (evt.order_type == 49) ?
                ((evt.side == 66) ? 1.001 : 0.999) : 1.0;
            price = it->second.price * factor;
        }
        // 如果查不到最新成交价，price 保持原值（可能为 0）
    }

    insert_order(evt, price);

    ProcessedOrder po;
    po.symbol = evt.symbol;
    po.channel_id = evt.channel_id;
    po.order_id = evt.order_id;
    po.time = evt.time;
    po.price = price;
    po.volume = evt.volume;
    po.money = evt.volume * price;
    on_order(evt.side, po);
}

void OrderStateStore::on_trade_sz(const TradeEvent& evt,
                                   WithdrawCallback on_withdraw,
                                   TradeCallback on_trade) {
    // 更新最新成交价
    if (evt.trade_type == 70 && evt.buy_order_id != 0 && evt.sell_order_id != 0) {
        // 推断方向: buyid > sellid → buy side, else sell side
        int8_t inferred_side = (evt.buy_order_id > evt.sell_order_id) ? 66 : 83;
        sz_latest_trade_px_[sz_trade_px_key(evt.symbol, inferred_side)] =
            {evt.price, evt.time};
    }

    // 撤单: trade stream 中 tradebuyid==0 或 tradesellid==0
    if (evt.sell_order_id == 0 && evt.buy_order_id != 0) {
        // 买方撤单
        auto* orig = find_order(evt.channel_id, evt.buy_order_id);
        if (orig) {
            auto pw = make_withdraw(*orig, evt.time, orig->price, evt.volume);
            on_withdraw(66, pw);
            if (evt.volume >= orig->volume) {
                remove_order(evt.channel_id, evt.buy_order_id);
            }
        }
        return;
    }
    if (evt.buy_order_id == 0 && evt.sell_order_id != 0) {
        // 卖方撤单
        auto* orig = find_order(evt.channel_id, evt.sell_order_id);
        if (orig) {
            auto pw = make_withdraw(*orig, evt.time, orig->price, evt.volume);
            on_withdraw(83, pw);
            if (evt.volume >= orig->volume) {
                remove_order(evt.channel_id, evt.sell_order_id);
            }
        }
        return;
    }

    // 正常成交: tradetype == 70 ('F')
    if (evt.trade_type != 70) return;
    if (evt.buy_order_id == 0 || evt.sell_order_id == 0) return;

    auto* buy_ord = find_order(evt.channel_id, evt.buy_order_id);
    auto* sell_ord = find_order(evt.channel_id, evt.sell_order_id);

    // 推断方向
    TradeEvent adjusted = evt;
    adjusted.side = (evt.buy_order_id > evt.sell_order_id) ? 66 : 83;

    auto pt = make_trade(adjusted, buy_ord, sell_ord);
    on_trade(pt);

    // 更新已成交量
    if (buy_ord) {
        buy_ord->traded_vol += evt.volume;
        if (buy_ord->traded_vol >= buy_ord->volume) {
            remove_order(evt.channel_id, evt.buy_order_id);
        }
    }
    if (sell_ord) {
        sell_ord->traded_vol += evt.volume;
        if (sell_ord->traded_vol >= sell_ord->volume) {
            remove_order(evt.channel_id, evt.sell_order_id);
        }
    }
}

} // namespace rtf
```

- [ ] **Step 3: 编写 test_order_state.cpp**

```cpp
// realtime_factors/tests/test_order_state.cpp
#define BOOST_TEST_MODULE OrderStateTest
#include <boost/test/unit_test.hpp>
#include "order_state.h"
#include <vector>

using namespace rtf;

// Helper: 创建 SH 新单
static OrderEvent make_sh_order(int32_t symbol, int32_t ch, int32_t id,
                                 int32_t tm, double px, int32_t vol,
                                 int8_t side, int8_t type = 65) {
    return {symbol, ch, id, tm, px, vol, side, type};
}

// Helper: 创建 SH 撤单
static OrderEvent make_sh_cancel(int32_t symbol, int32_t ch, int32_t id,
                                  int32_t tm, double px, int32_t vol, int8_t side) {
    return {symbol, ch, id, tm, px, vol, side, 68};
}

// Helper: 创建 SH 成交
static TradeEvent make_sh_trade(int32_t symbol, int32_t ch, int32_t buyid, int32_t sellid,
                                 int32_t tm, double px, int32_t vol, int8_t side) {
    return {symbol, ch, buyid, sellid, tm, px, vol, side, 0};
}

// Helper: 创建 SZ 委托
static OrderEvent make_sz_order(int32_t symbol, int32_t ch, int32_t id,
                                 int32_t tm, double px, int32_t vol,
                                 int8_t side, int8_t type = 50) {
    return {symbol, ch, id, tm, px, vol, side, type};
}

// Helper: 创建 SZ 成交
static TradeEvent make_sz_trade(int32_t symbol, int32_t ch, int32_t buyid, int32_t sellid,
                                 int32_t tm, double px, int32_t vol, int8_t trade_type = 70) {
    return {symbol, ch, buyid, sellid, tm, px, vol, 0, trade_type};
}

BOOST_AUTO_TEST_CASE(test_sh_new_order_and_cancel) {
    OrderStateStore store;
    std::vector<ProcessedOrder> orders;
    std::vector<ProcessedWithdraw> withdraws;

    auto on_order = [&](int8_t side, const ProcessedOrder& o) { orders.push_back(o); };
    auto on_withdraw = [&](int8_t side, const ProcessedWithdraw& w) { withdraws.push_back(w); };

    // 新买单
    auto evt = make_sh_order(600000, 1, 100, 93000000, 10.5, 1000, 66);
    store.on_order_sh(evt, on_order, on_withdraw);
    BOOST_CHECK_EQUAL(orders.size(), 1u);
    BOOST_CHECK(store.find_order(1, 100) != nullptr);

    // 撤单
    auto cancel = make_sh_cancel(600000, 1, 100, 93001000, 10.5, 1000, 66);
    store.on_order_sh(cancel, on_order, on_withdraw);
    BOOST_CHECK_EQUAL(withdraws.size(), 1u);
    BOOST_CHECK_EQUAL(withdraws[0].withdraw_vol, 1000);
    // 全撤 → 订单移除
    BOOST_CHECK(store.find_order(1, 100) == nullptr);
}

BOOST_AUTO_TEST_CASE(test_sh_aggressive_order_from_trade) {
    OrderStateStore store;
    std::vector<ProcessedOrder> orders;
    std::vector<ProcessedTrade> trades;

    auto on_order = [&](int8_t side, const ProcessedOrder& o) { orders.push_back(o); };
    auto on_trade = [&](const ProcessedTrade& t) { trades.push_back(t); };

    // 先注册一个卖单
    auto sell = make_sh_order(600000, 1, 200, 93000000, 10.5, 500, 83);
    store.on_order_sh(sell, on_order, [](int8_t, const ProcessedWithdraw&){});
    BOOST_CHECK_EQUAL(orders.size(), 1u);

    // 买方主动成交，但买单 (id=300) 不在 order stream 中 → 激进单补全
    auto trade = make_sh_trade(600000, 1, 300, 200, 93001000, 10.5, 200, 66);
    store.on_trade_sh(trade, on_order, on_trade);

    BOOST_CHECK_EQUAL(orders.size(), 2u);  // 补全了激进买单
    BOOST_CHECK_EQUAL(orders[1].order_id, 300);
    BOOST_CHECK_EQUAL(trades.size(), 1u);
    BOOST_CHECK_EQUAL(trades[0].buy_order_id, 300);
}

BOOST_AUTO_TEST_CASE(test_sz_cancel_from_trade_stream) {
    OrderStateStore store;
    std::vector<ProcessedOrder> orders;
    std::vector<ProcessedWithdraw> withdraws;
    std::vector<ProcessedTrade> trades;

    auto on_order = [&](int8_t side, const ProcessedOrder& o) { orders.push_back(o); };
    auto on_withdraw = [&](int8_t side, const ProcessedWithdraw& w) { withdraws.push_back(w); };
    auto on_trade = [&](const ProcessedTrade& t) { trades.push_back(t); };

    // 注册一个深圳买单
    auto buy = make_sz_order(1000, 1, 100, 93000000, 10.0, 500, 66);
    store.on_order_sz(buy, on_order);
    BOOST_CHECK_EQUAL(orders.size(), 1u);

    // 深圳撤单: tradesellid==0, tradebuyid==100
    auto cancel = make_sz_trade(1000, 1, 100, 0, 93001000, 0, 500, 52);
    store.on_trade_sz(cancel, on_withdraw, on_trade);

    BOOST_CHECK_EQUAL(withdraws.size(), 1u);
    BOOST_CHECK_EQUAL(withdraws[0].withdraw_id, 100);
    BOOST_CHECK_EQUAL(trades.size(), 0u);
}

BOOST_AUTO_TEST_CASE(test_sz_market_order_price_backfill) {
    OrderStateStore store;
    std::vector<ProcessedOrder> orders;

    auto on_order = [&](int8_t side, const ProcessedOrder& o) { orders.push_back(o); };
    auto on_withdraw = [&](int8_t side, const ProcessedWithdraw& w) {};
    auto on_trade = [&](const ProcessedTrade& t) {};

    // 先有一笔卖方主动成交，建立最新价
    auto trade = make_sz_trade(1000, 1, 50, 60, 93000000, 10.0, 100);
    // buyid(50) < sellid(60) → side=83(sell), 记录 sell side 价格
    store.on_trade_sz(trade, on_withdraw, on_trade);

    // 买方市价单 (ordertype=49, 对手方最优)
    auto buy_mkt = make_sz_order(1000, 1, 200, 93001000, 0.0, 100, 66, 49);
    store.on_order_sz(buy_mkt, on_order);

    // 买单应该用卖方最新成交价 * 1.001 回填
    // 但注意上面的 trade 被推断为 sell side (buyid<sellid)
    // 买单查 sell side 的最新价 → 应该找到 10.0
    // 但实际上 Python demo 中:
    //   - SZ 成交推断方向: buyid > sellid → buy(66), else sell(83)
    //   - 上面的 trade buyid=50 < sellid=60 → inferred_side=83
    //   - 更新 sz_latest_trade_px_[symbol=1000, side=83] = 10.0
    //   - 买单市价回填: 查 lookup_side = 83 (对面) → 找到 10.0 → price = 10.0 * 1.001
    BOOST_CHECK_EQUAL(orders.size(), 1u);
    BOOST_CHECK_CLOSE(orders[0].price, 10.0 * 1.001, 1e-6);
}
```

- [ ] **Step 4: 更新 CMakeLists.txt**

```cmake
# 在 CMakeLists.txt 中添加:
add_executable(test_order_state tests/test_order_state.cpp src/order_state.cpp)
target_link_libraries(test_order_state ${BOOST_LIBRARYDIR}/libboost_unit_test_framework.so)
add_test(NAME order_state COMMAND test_order_state)
```

- [ ] **Step 5: 编译运行测试**

```bash
cd realtime_factors/build && cmake .. && make test_order_state && ./test_order_state --log_level=test_suite
```

- [ ] **Step 6: 提交**

```bash
git add realtime_factors/include/order_state.h realtime_factors/src/order_state.cpp realtime_factors/tests/test_order_state.cpp realtime_factors/CMakeLists.txt
git commit -m "feat(realtime_factors): add OrderStateStore with SH/SZ rules and unit tests"
```

---

## Task 7: TS-Engine 主循环

**Files:**
- Create: `realtime_factors/include/ts_engine.h`
- Create: `realtime_factors/src/ts_engine.cpp`

从 Journal 读取事件，转换为内部类型，交给 OrderStateStore 处理，回调更新 SymbolState。

- [ ] **Step 1: 编写 ts_engine.h**

```cpp
// realtime_factors/include/ts_engine.h
#pragma once
#include "symbol_state.h"
#include "order_state.h"
#include "factor_schema.h"
#include "data_struct.hpp"
#include "sys_messages.h"

#include <unordered_map>
#include <string>
#include <vector>
#include <functional>
#include <atomic>

// Forward declare KungFu types
namespace kungfu { namespace yijinjing {
    class JournalReader;
    typedef std::shared_ptr<JournalReader> JournalReaderPtr;
    class Frame;
    typedef std::shared_ptr<Frame> FramePtr;
}}

namespace rtf {

struct EngineConfig {
    std::string journal_dir = "/shared/kungfu/journal/user/";
    std::string reader_name = "rtf_engine";
    int dump_interval_seconds = 60;  // 每 N 秒 dump 一次
    bool dump_csv = true;
    std::string csv_output_dir = "/tmp/rtf_output/";
};

class TSEngine {
public:
    explicit TSEngine(const EngineConfig& cfg);

    // 主循环（阻塞，直到 stop() 被调用）
    void run();
    void stop();

    // 手动灌入事件（用于测试，不经过 Journal）
    void feed_order(const KyStdOrderType& order);
    void feed_trade(const KyStdTradeType& trade);

    // 获取某只股票的因子状态（用于测试）
    const SymbolState* get_symbol_state(int32_t symbol) const;

    // 导出全市场因子快照到 CSV
    void dump_csv(const std::string& filename) const;

    // 获取已处理事件计数
    int64_t order_count() const { return order_count_; }
    int64_t trade_count() const { return trade_count_; }

private:
    EngineConfig cfg_;
    std::atomic<bool> running_{false};

    OrderStateStore order_store_;
    std::unordered_map<int32_t, SymbolState> symbol_states_;

    int64_t order_count_ = 0;
    int64_t trade_count_ = 0;

    // Journal → 内部事件转换
    OrderEvent convert_order(const KyStdOrderType& raw) const;
    TradeEvent convert_trade(const KyStdTradeType& raw) const;

    // 处理单条事件
    void process_order(const KyStdOrderType& raw);
    void process_trade(const KyStdTradeType& raw);
};

} // namespace rtf
```

- [ ] **Step 2: 编写 ts_engine.cpp**

```cpp
// realtime_factors/src/ts_engine.cpp
#include "ts_engine.h"
#include "JournalReader.h"
#include "Timer.h"

#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <csignal>
#include <cmath>
#include <sys/stat.h>

namespace rtf {

TSEngine::TSEngine(const EngineConfig& cfg) : cfg_(cfg) {}

OrderEvent TSEngine::convert_order(const KyStdOrderType& raw) const {
    OrderEvent evt;
    evt.symbol = raw.Symbol;
    evt.channel_id = static_cast<int32_t>(raw.Channel);
    evt.order_id = raw.OrderNumber;
    evt.time = raw.Time;
    evt.price = static_cast<double>(raw.Price);
    evt.volume = raw.Volume;

    if (is_sh(raw.Symbol)) {
        // SH: FunctionCode 1→B(66), 2→S(83)
        evt.side = (raw.FunctionCode == 1) ? 66 : 83;
        // SH: OrderKind 2→A(65), 10→D(68), 11→S(83,过滤掉)
        if (raw.OrderKind == 2) evt.order_type = 65;
        else if (raw.OrderKind == 10) evt.order_type = 68;
        else evt.order_type = 83; // 将被过滤
    } else {
        // SZ: FunctionCode 1→B(66), 2→S(83)
        evt.side = (raw.FunctionCode == 1) ? 66 : 83;
        // SZ: OrderKind 1→50('2'限价), 2→49('1'对手方), 3→85('U'本方最优)
        if (raw.OrderKind == 1) evt.order_type = 50;
        else if (raw.OrderKind == 2) evt.order_type = 49;
        else if (raw.OrderKind == 3) evt.order_type = 85;
        else evt.order_type = 0;
    }
    return evt;
}

TradeEvent TSEngine::convert_trade(const KyStdTradeType& raw) const {
    TradeEvent evt;
    evt.symbol = raw.Symbol;
    evt.channel_id = raw.Channel;
    evt.buy_order_id = raw.BidOrder;
    evt.sell_order_id = raw.AskOrder;
    evt.time = raw.Time;
    evt.price = static_cast<double>(raw.Price);
    evt.volume = raw.Volume;

    if (is_sh(raw.Symbol)) {
        // SH: BSFlag 0→N(78), 1→B(66), 2→S(83)
        if (raw.BSFlag == 1) evt.side = 66;
        else if (raw.BSFlag == 2) evt.side = 83;
        else evt.side = 78; // neutral
        evt.trade_type = 0; // SH 不用 trade_type
    } else {
        // SZ: FunctionCode 0→F(70成交), 1→4(52撤单)
        if (raw.FunctionCode == 0) evt.trade_type = 70;
        else evt.trade_type = 52;
        evt.side = 0; // SZ 方向由 on_trade_sz 内部推断
    }
    return evt;
}

void TSEngine::process_order(const KyStdOrderType& raw) {
    OrderEvent evt = convert_order(raw);
    order_count_++;

    auto on_order = [this](int8_t side, const ProcessedOrder& po) {
        auto& ss = symbol_states_[po.symbol];
        if (side == 66) ss.buy_order.feed(po);
        else            ss.sell_order.feed(po);
    };

    auto on_withdraw = [this](int8_t side, const ProcessedWithdraw& pw) {
        auto& ss = symbol_states_[pw.symbol];
        if (side == 66) ss.buy_withdraw.feed(pw);
        else            ss.sell_withdraw.feed(pw);
    };

    if (is_sh(raw.Symbol)) {
        if (evt.order_type == 83) return; // 过滤 SH 特殊委托
        order_store_.on_order_sh(evt, on_order, on_withdraw);
    } else {
        order_store_.on_order_sz(evt, on_order);
    }
}

void TSEngine::process_trade(const KyStdTradeType& raw) {
    TradeEvent evt = convert_trade(raw);
    trade_count_++;

    auto on_order = [this](int8_t side, const ProcessedOrder& po) {
        auto& ss = symbol_states_[po.symbol];
        if (side == 66) ss.buy_order.feed(po);
        else            ss.sell_order.feed(po);
    };

    auto on_withdraw = [this](int8_t side, const ProcessedWithdraw& pw) {
        auto& ss = symbol_states_[pw.symbol];
        if (side == 66) ss.buy_withdraw.feed(pw);
        else            ss.sell_withdraw.feed(pw);
    };

    auto on_trade = [this](const ProcessedTrade& pt) {
        auto& ss = symbol_states_[pt.symbol];
        ss.both_trade.feed(pt);
        if (pt.side == 66)      ss.buy_trade.feed(pt);
        else if (pt.side == 83) ss.sell_trade.feed(pt);
    };

    if (is_sh(raw.Symbol)) {
        order_store_.on_trade_sh(evt, on_order, on_trade);
    } else {
        order_store_.on_trade_sz(evt, on_withdraw, on_trade);
    }
}

void TSEngine::feed_order(const KyStdOrderType& order) {
    process_order(order);
}

void TSEngine::feed_trade(const KyStdTradeType& trade) {
    process_trade(trade);
}

const SymbolState* TSEngine::get_symbol_state(int32_t symbol) const {
    auto it = symbol_states_.find(symbol);
    return (it != symbol_states_.end()) ? &it->second : nullptr;
}

void TSEngine::dump_csv(const std::string& filename) const {
    std::ofstream ofs(filename);
    if (!ofs.is_open()) {
        std::cerr << "[dump_csv] failed to open " << filename << std::endl;
        return;
    }

    auto names = get_factor_names();

    // header
    ofs << "symbol";
    for (const auto& n : names) ofs << "," << n;
    ofs << "\n";

    // data
    std::vector<double> buf(TOTAL_TS_FACTORS);
    for (const auto& [sym, ss] : symbol_states_) {
        ss.write_all_factors(buf.data());
        ofs << sym;
        for (int i = 0; i < TOTAL_TS_FACTORS; ++i) {
            ofs << ",";
            if (std::isnan(buf[i])) ofs << "";
            else ofs << std::setprecision(10) << buf[i];
        }
        ofs << "\n";
    }

    std::cerr << "[dump_csv] wrote " << symbol_states_.size()
              << " symbols to " << filename << std::endl;
}

void TSEngine::stop() {
    running_ = false;
}

void TSEngine::run() {
    using namespace kungfu::yijinjing;

    std::vector<std::string> dirs = {cfg_.journal_dir, cfg_.journal_dir, cfg_.journal_dir};
    std::vector<std::string> jnames = {
        "insight_stock_tick_data",
        "insight_stock_order_data",
        "insight_stock_trade_data",
    };

    std::string reader_name = cfg_.reader_name + "_" + parseNano(getNanoTime(), "%H%M%S");
    JournalReaderPtr reader = JournalReader::createReaderWithSys(
        dirs, jnames, 0, reader_name);

    std::cerr << "[TSEngine] reader=" << reader_name
              << " connected to Paged, waiting for data..." << std::endl;

    running_ = true;
    auto last_dump = std::chrono::steady_clock::now();
    int dump_seq = 0;

    while (running_) {
        FramePtr frame = reader->getNextFrame();
        if (frame.get() == nullptr) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));

            // 检查是否该 dump
            if (cfg_.dump_csv) {
                auto now = std::chrono::steady_clock::now();
                if (std::chrono::duration_cast<std::chrono::seconds>(now - last_dump).count()
                    >= cfg_.dump_interval_seconds) {
                    mkdir(cfg_.csv_output_dir.c_str(), 0755);
                    std::string fname = cfg_.csv_output_dir + "/factors_"
                        + std::to_string(dump_seq++) + ".csv";
                    dump_csv(fname);
                    last_dump = now;
                }
            }
            continue;
        }

        short msg_type = frame->getMsgType();
        void* data = frame->getData();

        if (msg_type == MSG_TYPE_L2_ORDER) {
            process_order(*static_cast<KyStdOrderType*>(data));
        } else if (msg_type == MSG_TYPE_L2_TRADE) {
            process_trade(*static_cast<KyStdTradeType*>(data));
        }
        // 忽略 tick (msg_type=61)
    }

    // 最终 dump
    if (cfg_.dump_csv && !symbol_states_.empty()) {
        mkdir(cfg_.csv_output_dir.c_str(), 0755);
        dump_csv(cfg_.csv_output_dir + "/factors_final.csv");
    }

    std::cerr << "[TSEngine] done. orders=" << order_count_
              << " trades=" << trade_count_
              << " symbols=" << symbol_states_.size() << std::endl;
}

} // namespace rtf
```

- [ ] **Step 3: 提交**

```bash
git add realtime_factors/include/ts_engine.h realtime_factors/src/ts_engine.cpp
git commit -m "feat(realtime_factors): add TS-Engine with journal reading and CSV dump"
```

---

## Task 8: main.cpp 入口

**Files:**
- Create: `realtime_factors/src/main.cpp`

- [ ] **Step 1: 编写 main.cpp**

```cpp
// realtime_factors/src/main.cpp
#include "ts_engine.h"
#include <iostream>
#include <csignal>
#include <string>

static rtf::TSEngine* g_engine = nullptr;
static void signal_handler(int) {
    if (g_engine) g_engine->stop();
}

int main(int argc, char* argv[]) {
    rtf::EngineConfig cfg;

    // 简单命令行参数解析
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--journal-dir" && i + 1 < argc)
            cfg.journal_dir = argv[++i];
        else if (arg == "--output-dir" && i + 1 < argc)
            cfg.csv_output_dir = argv[++i];
        else if (arg == "--dump-interval" && i + 1 < argc)
            cfg.dump_interval_seconds = std::stoi(argv[++i]);
        else if (arg == "--reader-name" && i + 1 < argc)
            cfg.reader_name = argv[++i];
        else if (arg == "--help") {
            std::cout << "Usage: realtime_factors [options]\n"
                      << "  --journal-dir DIR    Journal directory (default: /shared/kungfu/journal/user/)\n"
                      << "  --output-dir DIR     CSV output directory (default: /tmp/rtf_output/)\n"
                      << "  --dump-interval N    Dump interval in seconds (default: 60)\n"
                      << "  --reader-name NAME   Paged reader name (default: rtf_engine)\n"
                      << std::endl;
            return 0;
        }
    }

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    rtf::TSEngine engine(cfg);
    g_engine = &engine;

    std::cerr << "[main] starting TSEngine..."
              << " journal=" << cfg.journal_dir
              << " output=" << cfg.csv_output_dir
              << " interval=" << cfg.dump_interval_seconds << "s"
              << std::endl;

    engine.run();
    return 0;
}
```

- [ ] **Step 2: 更新 CMakeLists.txt 启用主程序**

在 CMakeLists.txt 末尾添加主程序 target:
```cmake
# ---- 主程序 ----
add_executable(realtime_factors src/main.cpp src/ts_engine.cpp src/order_state.cpp)
target_link_libraries(realtime_factors journal paged -lpthread python2.7 ${Boost_LIBRARIES})
```

- [ ] **Step 3: 编译**

```bash
cd realtime_factors/build && cmake .. && make -j4
```

Expected: 编译通过，产出 `realtime_factors` 可执行文件。

- [ ] **Step 4: 提交**

```bash
git add realtime_factors/src/main.cpp realtime_factors/CMakeLists.txt
git commit -m "feat(realtime_factors): add main entry point and complete build"
```

---

## Task 9: 集成测试 — Journal 回放验证

**Files:**
- 使用 `scripts/replay` 启动回放
- 运行 `realtime_factors` 读取回放数据
- 检查输出 CSV

- [ ] **Step 1: 启动回放**

```bash
cd /workspace/Code/quant/stream_feature/kungfu_demo
scripts/replay start --reset 100  # 100 倍速从头回放
```

- [ ] **Step 2: 运行 realtime_factors**

```bash
cd /workspace/Code/quant/stream_feature/kungfu_demo/realtime_factors/build
./realtime_factors --dump-interval 10 --output-dir /tmp/rtf_test/
```

等待约 10-20 秒（100 倍速回放），然后 Ctrl+C 停止。

- [ ] **Step 3: 检查输出**

```bash
ls -la /tmp/rtf_test/
head -5 /tmp/rtf_test/factors_final.csv
wc -l /tmp/rtf_test/factors_final.csv
```

Expected:
- CSV 文件存在
- 有 symbol 列 + 492 个因子列
- 行数 > 0（至少几百只股票有数据）

- [ ] **Step 4: 停止回放**

```bash
scripts/replay stop
```

- [ ] **Step 5: 提交（如有修复）**

```bash
git add -A && git commit -m "fix(realtime_factors): integration test fixes"
```

---

## Task 10: 基础正确性检查

验证几个关键因子值的合理性。

- [ ] **Step 1: 写一个简单的验证脚本**

创建 `realtime_factors/scripts/check_output.py`:

```python
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

    # 检查一些基本约束
    errors = 0
    for row in rows:
        sym = row["symbol"]
        # count 应该 >= 0
        for k, v in row.items():
            if "count" in k and v:
                try:
                    cnt = float(v)
                    if cnt < 0:
                        print(f"ERROR: {sym}.{k} = {cnt} < 0")
                        errors += 1
                except:
                    pass
            # vwapx 应该 > 0 (如果有数据)
            if "vwapx" in k and v:
                try:
                    vw = float(v)
                    if vw <= 0 or vw > 10000:
                        print(f"WARN: {sym}.{k} = {vw} looks unusual")
                except:
                    pass

    # 汇总
    if rows:
        first = rows[0]
        non_empty = sum(1 for v in first.values() if v and v != "symbol")
        print(f"First symbol ({first['symbol']}): {non_empty}/{len(first)-1} non-empty factors")

    print(f"Errors: {errors}")
    return 1 if errors > 0 else 0

if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: 运行检查**

```bash
python3 realtime_factors/scripts/check_output.py /tmp/rtf_test/factors_final.csv
```

- [ ] **Step 3: 提交**

```bash
git add realtime_factors/scripts/check_output.py
git commit -m "feat(realtime_factors): add output sanity check script"
```
