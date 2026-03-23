# KBar Builder — 实时 1 分钟 K 线聚合模块（已废弃）

> **本文档已过时。** 此处描述的是早期基于 trade 数据、嵌入 `realtime_parquet` 进程的 kbar 实现。该实现已从 `realtime_parquet` 中移除（`realtime_parquet` 现在只负责 dump parquet）。
>
> 当前使用的 kbar 实现基于 **tick 快照**合成聚宽口径 1min K 线，作为独立子项目运行。文档见 **[`realtime_kbar/README.md`](../realtime_kbar/README.md)**。
>
> 以下内容仅作历史参考，记录早期基于 trade + watermark 的设计思路。

## 背景

`realtime_parquet` 已在线上实时读取 journal 并输出 tick/order/trade 分钟 Parquet 文件。KBar Builder 在此基础上新增 **从逐笔成交（trade）实时聚合每分钟每只股票的 OHLCV K 线** 的能力，输出为 CSV 文件（`kbar_HHMM.csv`），与 tick/order/trade Parquet 文件放在同一日期目录下。

选择 CSV 而非 Parquet：每分钟约 5000 行（全市场活跃股票数），Parquet 的列式压缩优势在此量级下可忽略，CSV 代码更简单、可直接 `cat` 查看。

~~源码：`realtime_parquet/src/kbar_builder.h`、`realtime_parquet/src/kbar_builder.cpp`。~~（已删除）

---

## 1. 整体架构

```
Main Thread (已有)                       KBar Thread (新增)
────────────────────────                 ────────────────────────
read journal frame
  ├─ tick  → append_tick()
  ├─ order → append_order()
  └─ trade → append_trade()
              │
              ├─ push TradeEvent ──→ SPSC Queue ──→ pop TradeEvent
              │  (~48 bytes copy)                    on_trade():
              │                                        按 exchange_time 分拣
              │                                        → minute_bars_[HHMM][symbol]
              │                                      check watermark → flush 已完成的分钟
              │                                      write kbar CSV
              │
              └─ (主循环延迟不受 kbar 影响)
```

主线程只做一次 `TradeEvent` 的值拷贝 + 入队（~50ns），所有聚合、watermark 判断、文件写入都在 kbar 线程完成，**不阻塞主循环的 journal 读取和 Parquet 写入**。

**核心数据结构**：`std::map<int, BarMap> minute_bars_`——trade 到达后立即按 exchange_time 的 HHMM 分拣到对应分钟的 bar 桶中，然后原始 trade 数据即可丢弃。每只股票的 bar 仅 ~60 bytes（OHLCV + 时间），全市场 5000 只 × 几个分钟 = 不到 2MB，内存开销可忽略。Watermark 只控制 **何时 flush**，不影响数据归属——从根本上避免了跨 channel 乱序导致不同分钟数据混入同一 bar 的问题。

---

## 2. exchange_time 处理

### 2.1 为什么用 exchange_time 而非 nano_timestamp

K 线归属应以 **交易所时间（exchange_time）** 为准，而非 journal 写入时间（nano_timestamp）。原因：

- nano_timestamp 反映的是 gateway 将数据写入 journal 的时刻，受 gateway 处理延迟、多线程调度等因素影响
- exchange_time 是交易所撮合引擎打的时间戳，真实反映成交发生的时刻
- 同一分钟内的成交应归入同一根 K 线，即使它们因 channel 交叉到达而在 journal 中乱序

### 2.2 HHMMSSmmm 格式

`KyStdTradeType::Time` 字段是 `int32_t`，编码为 `HHMMSSmmm`（时分秒毫秒压缩为一个整数）。例如：

| exchange_time | 含义 |
|--------------|------|
| `93000120` | 09:30:00.120 |
| `113000000` | 11:30:00.000 |
| `145959880` | 14:59:59.880 |

提取分钟的方法（`exch_time_to_hhmm`）：

```cpp
int hh = exch_time / 10000000;        // 93000120 → 9
int mm = (exch_time / 100000) % 100;  // 93000120 → 30
int hhmm = hh * 100 + mm;             // → 930
```

K 线文件以 **结束分钟** 命名：`[09:30, 09:31)` 区间的数据写入 `kbar_0931.csv`，与 tick/order/trade Parquet 的命名规则一致。

---

## 3. 独立线程设计

### 3.1 为什么需要独立线程

主循环是一个紧密的 `read frame → dispatch → append` 循环，任何阻塞都会导致 journal 读取堆积。K 线聚合涉及：

- 哈希表查找与更新（`unordered_map`）
- 分钟切换时遍历全部活跃股票生成 carry-forward 条目
- 文件 I/O（`fopen` / `fprintf` / `fclose` / `rename`）

虽然单次操作很快（<1ms），但在分钟切换时的突发写入可能叠加到几毫秒，对低延迟场景不可接受。独立线程将这些开销完全隔离。

### 3.2 SPSC Lock-Free Queue

主线程和 kbar 线程之间通过一个 **Single-Producer Single-Consumer (SPSC)** 无锁环形缓冲区通信：

```cpp
template<size_t Cap>  // Cap = 65536，必须是 2 的幂
class SPSCQueue {
    TradeEvent buf_[Cap];
    alignas(64) std::atomic<size_t> head_{0};  // 生产者写
    alignas(64) std::atomic<size_t> tail_{0};  // 消费者写
};
```

关键设计点：

| 设计 | 说明 |
|------|------|
| 容量 65536 | 单分钟全市场 trade 约 20 万条，65536 足够缓冲（消费端持续 drain） |
| `alignas(64)` | head 和 tail 分别占独立 cache line，避免 false sharing |
| `memory_order_acquire/release` | 最小同步开销，保证 producer 写入的数据对 consumer 可见 |
| Power-of-2 mask | `(idx + 1) & (Cap - 1)` 替代取模，编译为单条 AND 指令 |
| 值拷贝 | `TradeEvent` ~48 bytes，拷贝成本 ~50ns，远低于指针间接引用的 cache miss |

### 3.3 线程生命周期

```
构造函数 KBarBuilder()
  └─ 启动 kbar thread

thread_loop():
  while (running_):
    1. 检查 date_changed_ flag → apply_date_change()
    2. 循环 pop queue → on_trade()
    3. 若队列为空 → check_timeout() + sleep 100μs

stop():  // 主线程调用
  1. running_ = false
  2. thread_.join()  ← 阻塞等待 kbar 线程退出
     └─ kbar 线程退出前: drain 剩余 queue + flush pending bars
```

### 3.4 主线程与 kbar 线程的交互

只有 **三个接触点**，均由主线程发起：

| 方法 | 调用时机 | 机制 |
|------|---------|------|
| `push_trade(md, nano)` | 每条 trade 数据 | 值拷贝 + SPSC push |
| `set_date(date_str)` | 日期切换时 | 写 `pending_date_` + atomic flag |
| `stop()` | 程序退出 | atomic flag + `join()` |

`set_date` 的同步保证：主线程先写 `pending_date_`，再 `store(release)` 设置 flag；kbar 线程 `load(acquire)` 读到 flag 后再读 `pending_date_`，acquire-release 配对保证可见性。

---

## 4. Watermark 机制

### 4.1 问题：跨 channel 乱序

交易所按 channel 分发数据（深圳 5 个 channel、上海 7 个 channel），不同 channel 间无全局时序保证（详见 `docs/行情数据链路与乱序分析.md`）。如果收到一条 09:31 的 trade 就立即 flush 09:30 的 K 线，可能遗漏其他 channel 中尚未到达的 09:30 数据。

### 4.2 方案：per-channel watermark

为每个 channel 维护一个 watermark（该 channel 已观察到的最大 exchange_time）：

```cpp
std::unordered_map<int32_t, int32_t> channel_watermarks_;
// key = Channel ID, value = max exchange_time (HHMMSSmmm)
```

**Flush 条件**：所有已出现的 channel 的 watermark 都越过了当前分钟边界。

```cpp
int compute_watermark_minute() {
    // 取所有 channel watermark 中的最小 HHMM
    int min_hhmm = 9999;
    for (auto& [ch, exch_time] : channel_watermarks_)
        min_hhmm = min(min_hhmm, exch_time_to_hhmm(exch_time));
    return min_hhmm;
}
```

Watermark 只控制 **何时 flush**，不控制数据归属。当 `watermark_minute > M` 时，说明所有 channel 都已经进入了 M 之后的分钟，`minute_bars_[M]` 不会再收到新数据，可以安全 flush。

**关键区别**：即使 watermark 尚未推进，新到达的 trade 也会按自身的 exchange_time 分拣到正确的分钟桶中（比如 930 的 trade 进入 `minute_bars_[930]`），不会污染尚未 flush 的旧分钟（如 925）。

### 4.3 触发流程

```
收到 trade (channel=2013, time=09:31:00.050)
  │
  ├─ 分拣: minute_bars_[931][symbol] 累加 OHLCV
  ├─ 更新 channel_watermarks_[2013] = max(current, 93100050)
  │
  └─ try_flush(): 计算 watermark:
       channel 2011: 93100200 → 931   ← 已过
       channel 2013: 93100050 → 931   ← 已过
       channel 2015: 93059800 → 930   ← 未过，min = 930
       →  flush 所有 < 930 的分钟（已无待 flush）
       →  minute_bars_[930] 保留，等待 channel 2015

收到 trade (channel=2015, time=09:31:00.010)
  │
  ├─ 分拣: minute_bars_[931][symbol] 累加
  └─ try_flush():
       全部 channel watermark → 931
       →  flush 所有 < 931 的分钟 → 输出 kbar_0931.csv (minute_bars_[930])
```

### 4.4 超时兜底

午休（11:30-13:00）和收盘（15:00）后不会有新 trade，watermark 永远不会推进。采用 30 秒墙上时间超时强制 flush 所有 `minute_bars_` 中的剩余分钟：

```cpp
void check_timeout() {
    if (steady_now_ns() - last_trade_steady_ns_ > 30s)
        flush_remaining();  // 输出所有未 flush 的分钟
}
```

使用 `std::chrono::steady_clock` 而非 journal 时间戳，因为 replay 模式下 journal 时间可能是历史时间。

---

## 5. Carry-Forward（无成交填充）

Flush 时，对 `active_stocks_` 中存在但本分钟 bar 中不存在的股票（即本分钟无成交），用上一次成交价填充：

```
open = close = high = low = last_close_price
volume = 0, money = 0
first_trade_time = 0, last_trade_time = 0
```

`active_stocks_` 是一个 `map<symbol, last_close_price>`，**仅在 flush 时更新**（从已 flush 的 bar 中取 close price）。这保证了 carry-forward 使用的价格来自已确认输出的分钟，不会被尚未 flush 的未来分钟的 trade 污染。

**停牌股**不会出现在 `active_stocks_` 中（因为从未有 trade），因此不会被输出。

日期切换时 `active_stocks_` 被清空，新交易日从空白开始。

---

## 6. CSV 输出格式

### 6.1 文件命名

```
output/20260312/kbar_0931.csv    ← [09:30, 09:31) 的 K 线
output/20260312/kbar_0932.csv    ← [09:31, 09:32) 的 K 线
...
output/20260312/kbar_1500.csv    ← [14:59, 15:00) 的 K 线
```

### 6.2 原子写入

先写到 `.kbar_0931.csv`（dot 前缀临时文件），完成后 `rename` 为 `kbar_0931.csv`。`rename` 在同一文件系统上是原子操作，下游读者不会看到写了一半的文件。

### 6.3 列定义

```csv
time,symbol,open,close,high,low,volume,money,first_trade_time,last_trade_time
2026-03-12 09:31:00,000001.XSHE,10.87,10.89,10.90,10.87,1508335,16408670.50,93000120,93059880
2026-03-12 09:31:00,600519.XSHG,1810.00,1812.50,1813.00,1809.00,23456,42498750.00,93000050,93059920
```

| 列 | 类型 | 说明 |
|----|------|------|
| time | string | Bar 结束时间，`YYYY-MM-DD HH:MM:00` |
| symbol | string | 股票代码，`%06d.XSHE`（深圳）或 `%06d.XSHG`（上海） |
| open | float | 开盘价（本分钟第一笔成交价） |
| close | float | 收盘价（本分钟最后一笔成交价） |
| high | float | 最高价 |
| low | float | 最低价 |
| volume | int | 成交量（股） |
| money | float | 成交额（元） |
| first_trade_time | int | 本分钟第一笔成交的 exchange_time（HHMMSSmmm），无成交为 0 |
| last_trade_time | int | 本分钟最后一笔成交的 exchange_time（HHMMSSmmm），无成交为 0 |

CSV 行按 symbol 数值升序排列，保证输出确定性。

---

## 7. 数据过滤

| 条件 | 处理 |
|------|------|
| `Price <= 0` | 丢弃，不参与 OHLCV 计算 |
| `Volume <= 0` | 丢弃，不参与 OHLCV 计算 |
| 停牌股（全天无 trade） | 不出现在 `active_stocks_` 中，不输出 |

---

## 8. 边界场景

| 场景 | 处理方式 |
|------|---------|
| 开盘热身（部分 channel 未出现） | watermark 只计算已出现的 channel |
| 午休 11:30-13:00 | 30s timeout flush 所有 `minute_bars_` 剩余分钟 |
| 收盘 15:00 | 30s timeout + `stop()` drain 剩余 + `flush_remaining()` |
| 跨 channel 乱序 | trade 按 exchange_time 分拣到正确分钟桶，watermark 控制 flush 时机 |
| 跳分钟（某分钟无 trade） | `active_stocks_` 全部 carry-forward |
| 队列满 | `push` 返回 false，打 WARNING 日志（理论上不会满） |
| 日期切换 | flush 旧日期 pending bars → 清空全部状态 → 建新目录 |

---

## 9. 日志示例

```
[kbar] date=20260312 dir=output/20260312
[kbar] 0931 stocks=4856 traded=4102 carried=754
[kbar] 0932 stocks=4856 traded=4843 carried=13
...
[kbar] 1130 stocks=4856 traded=4200 carried=656
(30 秒无数据后)
[kbar] 1131 stocks=4856 traded=0 carried=4856    ← timeout flush
```

---

## 10. ~~与主循环的集成~~（已移除）

kbar 功能已从 `realtime_parquet` 中移除。现在 kbar 由独立的 `realtime_kbar` 进程负责，两者可同时通过 Paged 读取 journal，互不影响。
