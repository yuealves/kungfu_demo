# KungFu getNanoTime() 时钟偏移 Bug 定位与分析

## 问题现象

2026-03-10 上线 `monitor_latency` 监控进程后，发现 journal 中 frame 的时间戳（`journal_time`，即 `frame->getNano()`）比华泰 Insight 行情数据中的交易所时间（`exchange_time`，即 `MDTime`）**早约 500ms**，且 tick/order/trade 三个频道均一致复现。

```csv
write_time,monitor_item,journal_time,exchange_time
2026-03-10 09:30:15.123,tick,2026-03-10 09:30:14.456,09:30:15.230
2026-03-10 09:30:15.123,order,2026-03-10 09:30:14.457,09:30:15.231
2026-03-10 09:30:15.123,trade,2026-03-10 09:30:14.458,09:30:15.232
```

正常预期：market_gateway 从 Insight SDK 收到数据后写入 journal，journal_time 应**晚于** exchange_time（因为有网络传输 + 处理延迟）。但实际 journal_time 反而更早，不合逻辑。

## 排查过程

### 1. 排除托管机 NTP 时钟同步问题

在托管机上执行 `ntpq -p`，确认与上游 NTP 服务器（10.6.143.251）的时钟偏差 < 1ms，排除托管机系统时钟不准的可能性。

### 2. 排除 tick 数据精度问题

华泰文档说明 tick 数据的 MDTime 格式为 `HHMMSSsss`，但实际 tick 的 MDTime 只精确到秒（末三位为 000）。这能解释 tick 的取整误差，但 order/trade 数据有完整毫秒精度，同样出现 journal_time 更早的现象，因此精度问题不是根因。

### 3. 排除上游 NTP 服务器偏差

托管机不联网，无法直接校验上游 NTP 服务器。通过 SSH 从外部 Mac 对比时钟（SSH ControlMaster 复用连接消除握手延迟），得出托管机与外部 Mac 的时钟偏差不超过几十毫秒，远小于观测到的 ~500ms 偏差。

### 4. 排除 exchange_time 不准

将 journal 数据 dump 为 Parquet 存档后，与 CSMAR（国泰安）数据逐条比对，exchange_time 完全一致，排除华泰数据源的问题。

### 5. 分析 market_gateway 写入流程

阅读 `market_gateway/src/brokers/insight_tcp/insight_gateway.h` 源码，发现 `AsyncBatchWriter` 架构：

```
Insight SDK callback (OnMarketData)
  → save_to_queue()  [入队到 moodycamel::ConcurrentQueue]
  → background writer thread process_queue()  [批量出队]
  → JournalWriter::write_frame()  [内部调用 getNanoTime() 打时间戳]
```

队列引入的延迟只会让 journal_time **更晚**，无法解释"更早"的现象。问题指向 `getNanoTime()` 本身。

### 6. 编写测试程序验证 getNanoTime()

在托管机上编写如下测试程序，对比 KungFu `getNanoTime()` 和 C++ 标准库 `system_clock`（底层为 `clock_gettime(CLOCK_REALTIME)`）：

```cpp
// test_clock.cpp
#include "Timer.h"
#include <chrono>
#include <cstdio>
#include <unistd.h>

int main() {
    using namespace kungfu::yijinjing;
    for (int i = 0; i < 5; i++) {
        long kf = getNanoTime();
        long sc = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        printf("kf=%ld  sys=%ld  diff=%ldms\n", kf, sc, (sc - kf) / 1000000);
        usleep(500000);
    }
}
```

编译命令：

```bash
g++ -std=c++17 -O2 -w \
  -I/opt/kungfu/master/include \
  -I/opt/kungfu/toolchain/boost-1.62.0/include \
  -I/usr/include/python2.7 \
  -L/opt/kungfu/master/lib/yijinjing \
  -L/opt/kungfu/toolchain/boost-1.62.0/lib \
  -o test_clock test_clock.cpp \
  -ljournal -lpython2.7 -lpthread \
  -lboost_filesystem -lboost_system -lboost_thread \
  -lboost_python -lboost_chrono -lboost_regex \
  -lboost_date_time -lboost_locale -lboost_serialization \
  -lboost_math_tr1 -lboost_program_options -lboost_atomic
```

运行命令：

```bash
LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/toolchain/boost-1.62.0/lib ./test_clock
```

**托管机执行结果**：

```
kf=1773134370736985893  sys=1773134371488746781  diff=751ms
kf=1773134371237264754  sys=1773134371989023490  diff=751ms
kf=1773134371737346520  sys=1773134372489105203  diff=751ms
kf=1773134372237565270  sys=1773134372989324223  diff=751ms
kf=1773134372737811983  sys=1773134373489570696  diff=751ms
```

**结论：`getNanoTime()` 比系统时钟稳定慢 751ms。**

## 根因分析

### getNano() 的工作原理

KungFu 的 `getNanoTime()` 不直接使用 `CLOCK_REALTIME`，而是：

1. 读取 `CLOCK_MONOTONIC`（系统启动以来的单调递增时钟）
2. 加上一个预计算的偏移量 `secDiff`，将单调时钟转换为 Unix 时间戳

源码位于 [yijinjing/utils/Timer.cpp](https://github.com/kungfu-origin/kungfu/blob/1.0.0/yijinjing/utils/Timer.cpp)：

```cpp
// Timer.cpp L30-38: 底层使用 CLOCK_MONOTONIC
inline std::chrono::steady_clock::time_point get_time_now()
{
    timespec tp;
    clock_gettime(CLOCK_MONOTONIC, &tp);
    return std::chrono::steady_clock::time_point(
        std::chrono::steady_clock::duration(
            std::chrono::seconds(tp.tv_sec) + std::chrono::nanoseconds(tp.tv_nsec)));
}

// Timer.cpp L72-78: getNano() = CLOCK_MONOTONIC + secDiff
long NanoTimer::getNano() const
{
    long _nano = std::chrono::duration_cast<std::chrono::nanoseconds>(
            get_time_now().time_since_epoch()
    ).count();
    return _nano + secDiff;
}
```

### Bug 所在：`get_local_diff()` 整数截断

`secDiff` 的计算在 [Timer.cpp L56-62](https://github.com/kungfu-origin/kungfu/blob/1.0.0/yijinjing/utils/Timer.cpp#L56-L62)：

```cpp
inline long get_local_diff()
{
    int unix_second_num = std::chrono::seconds(std::time(NULL)).count();       // CLOCK_REALTIME 截断到整秒
    int tick_second_num = std::chrono::duration_cast<std::chrono::seconds>(
            get_time_now().time_since_epoch()                                  // CLOCK_MONOTONIC 截断到整秒
    ).count();
    return (unix_second_num - tick_second_num) * NANOSECONDS_PER_SECOND;       // 整秒差 × 1e9
}
```

**问题**：`time(NULL)` 返回 `CLOCK_REALTIME` 截断到整秒，`duration_cast<seconds>` 将 `CLOCK_MONOTONIC` 截断到整秒。两者分别丢弃了各自的亚秒部分后再做差，导致 `secDiff` 的精度只有 ±1 秒。

实际误差 = `CLOCK_MONOTONIC 的亚秒分量 - CLOCK_REALTIME 的亚秒分量`，取值范围 **-999ms ~ +999ms**，取决于进程启动那一刻两个时钟的亚秒部分之差。该误差在进程生命周期内固定不变。

### 误差传播链

1. **Paged 进程**最先启动，它自己的 `NanoTimer` 通过 `get_local_diff()` 初始化，引入截断误差（本次为 -751ms）
2. **market_gateway** 启动时，`NanoTimer` 通过 Unix socket 连接 Paged 获取 `secDiff`（[Timer.cpp L40-53](https://github.com/kungfu-origin/kungfu/blob/1.0.0/yijinjing/utils/Timer.cpp#L40-L53) 的 `get_socket_diff()`），但 Paged 返回的就是它自己带误差的值
3. 所有下游进程（market_gateway、monitor_latency 等）**继承同一个偏差**

### 数值验证

- `getNanoTime()` 偏差：-751ms
- 行情数据从交易所经网络到 gateway 写入 journal 的处理延迟：约 +200ms~+300ms（网络 + 队列）
- 最终观测到的 `journal_time - exchange_time`：约 -500ms
- 计算：-751ms + ~250ms ≈ -500ms，与观测吻合

## 影响范围

- **不影响数据正确性**：所有 journal frame 使用同一时钟源，相对顺序完全正确
- **不影响回放**：replayer 基于 frame 之间的时间差回放，不受绝对偏移影响
- **影响绝对时间比较**：journal_time 与外部时钟（exchange_time、wall clock）的绝对值比较会有 ±999ms 的随机偏差，具体值由 Paged 启动时刻决定

## 为什么 KungFu 不直接用 CLOCK_REALTIME

KungFu 选择 `CLOCK_MONOTONIC + 偏移量` 的设计并非随意，而是为了解决 `CLOCK_REALTIME` 的一个致命问题：**会跳变**。

`CLOCK_REALTIME` 受 NTP 校时、管理员手动调时、闰秒等影响，可能突然前跳或后跳。对于高频行情系统，时钟跳变会导致：

- frame 的时间戳出现**倒序**（后写入的 nano 比先写入的小），破坏 journal 的有序性
- 依赖时间差做回放、排序的下游逻辑崩溃
- 基于时间的 page 文件切换可能异常

`CLOCK_MONOTONIC` 保证**单调递增、永不回退**，不受任何外部调时影响。因此 KungFu 选择以 `CLOCK_MONOTONIC` 为基准来保证 frame 之间的时间顺序绝对正确，再加一个一次性的偏移量 `secDiff` 把它映射到 Unix 时间戳。

**设计思路本身是合理的，bug 只在于 `get_local_diff()` 计算偏移量时把两个时钟都截断到整秒再做差，丢掉了亚秒精度。** 这大概率是原作者没仔细想，觉得"秒级差值乘以 1e9 就是纳秒级差值"，忽略了两个时钟的亚秒部分不同。

### 更合理的做法

如果不需要防跳变，最简单的方案是直接用 `CLOCK_REALTIME`：

```cpp
// 方案 A：直接用 CLOCK_REALTIME，简单直接
long getNano() const {
    timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    return tp.tv_sec * 1000000000L + tp.tv_nsec;
}
```

对于绝大多数场景这就够了。NTP 默认的 slew 模式是渐进式调整，不会跳变，每次微调幅度极小。

如果确实需要防跳变（比如担心管理员手动调时），保留 KungFu 的 `CLOCK_MONOTONIC + 偏移量` 思路即可，只需把偏移量算对：

```cpp
// 方案 B：CLOCK_MONOTONIC + 精确偏移量（两行代码修复原 bug）
NanoTimer::NanoTimer() {
    timespec tp_real, tp_mono;
    clock_gettime(CLOCK_REALTIME, &tp_real);
    clock_gettime(CLOCK_MONOTONIC, &tp_mono);
    secDiff = (tp_real.tv_sec * 1000000000L + tp_real.tv_nsec)
            - (tp_mono.tv_sec * 1000000000L + tp_mono.tv_nsec);
}
```

## 修复方案

### 方案 1：修改 KungFu 源码重编译 libjournal.so（推荐）

将 `get_local_diff()` 替换为纳秒精度版本：

```cpp
inline long get_local_diff()
{
    timespec tp_real, tp_mono;
    clock_gettime(CLOCK_REALTIME, &tp_real);
    clock_gettime(CLOCK_MONOTONIC, &tp_mono);
    long real_ns = tp_real.tv_sec * 1000000000L + tp_real.tv_nsec;
    long mono_ns = tp_mono.tv_sec * 1000000000L + tp_mono.tv_nsec;
    return real_ns - mono_ns;
}
```

需要搭建 KungFu 编译环境，重新编译 `libjournal.so` 后替换线上 `/opt/kungfu/master/lib/yijinjing/libjournal.so`。

### 方案 2：monitor_latency 侧补偿

在 monitor_latency 启动时测量一次 `getNanoTime()` 与 `system_clock` 的差值，对读到的 journal_time 做补偿。不修改 journal 数据本身，仅在展示时修正。

### 方案 3：重启 Paged 碰运气（不推荐）

每次重启 Paged 时截断误差随机变化，理论上有概率接近 0。但不可控、不稳定，仅作为临时应急手段。

## 相关文件

| 文件 | 说明 |
|------|------|
| [Timer.cpp](https://github.com/kungfu-origin/kungfu/blob/1.0.0/yijinjing/utils/Timer.cpp) | `NanoTimer` 实现，bug 所在 |
| [Timer.h](https://github.com/kungfu-origin/kungfu/blob/1.0.0/yijinjing/utils/Timer.h) | `getNanoTime()` 声明 |
| [JournalWriter.h](https://github.com/kungfu-origin/kungfu/blob/1.0.0/yijinjing/journal/JournalWriter.h) | `write_frame()` 内部调用 `getNanoTime()` |
| [FrameHeader.h](https://github.com/kungfu-origin/kungfu/blob/1.0.0/yijinjing/journal/FrameHeader.h) | Frame 头部结构，`nano` 字段定义 |
| `monitor_latency/src/main.cpp` | 监控进程，发现问题的起点 |
| `market_gateway/src/brokers/insight_tcp/insight_gateway.h` | AsyncBatchWriter 写入流程 |
