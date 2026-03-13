# L2 行情数据与 KungFu Journal 系统

本文档说明 L2 行情数据的含义、格式，以及 KungFu yijinjing journal 系统中数据的写入和读取机制。

## 0. L2 行情数据：Order / Trade / Tick

L2 行情由交易所实时推送，包含三种数据，从底层到聚合依次为：

### Order（逐笔委托）

每一笔委托挂单。某个投资者在某个时刻下了一笔买/卖单：

```
09:30:01.234  股票 000001  买入  价格 10.50  数量 500 股
09:30:01.235  股票 600519  卖出  价格 1800.00  数量 100 股
```

关键字段（`KyStdOrderType`）：
- `Symbol` — 股票代码
- `Time` — 交易所时间（HHMMSSmmm）
- `Price` — 委托价格
- `Volume` — 委托数量
- `OrderKind` — 买/卖方向
- `FunctionCode` — 委托类型（限价、市价、撤单等）
- `Channel` — 交易所频道号
- `BizIndex` — 业务序号（同 Channel 内唯一递增）

这是**最细粒度**的数据，量最大。集合竞价时 0915 上海交易所一次性下发 120 万笔委托（整段竞价 order book），0925 竞价结束再下发 247 万笔。

### Trade（逐笔成交）

每一笔撮合成交。交易所撮合引擎将一笔买单和一笔卖单配对后产生：

```
09:30:01.236  股票 000001  成交价 10.50  数量 500 股  买方委托#12345  卖方委托#67890
```

关键字段（`KyStdTradeType`）：
- `Symbol`、`Time`、`Price`、`Volume` — 同上
- `AskOrder` — 卖方委托编号
- `BidOrder` — 买方委托编号
- `BSFlag` — 主买/主卖标志（谁主动发起的成交）

每笔 trade 可以通过 `AskOrder`/`BidOrder` 追溯到对应的两笔 order。一笔大委托可能拆成多笔小成交（如卖 1 万股分别和 5 个买家成交，产生 5 条 trade 记录）。

### Tick（快照行情）

整个盘口的定时快照。交易所每隔一段时间（沪深约 3 秒）对每只股票拍一张"照片"：

```
09:30:03.000  股票 000001
  最新价 10.50  开盘 10.30  最高 10.55  最低 10.28
  买一 10.49 × 2000股   卖一 10.50 × 1500股
  买二 10.48 × 3000股   卖二 10.51 × 800股
  ...（十档买卖盘）
  累计成交量 125000股  累计成交额 1312500元
```

关键字段（`KyStdSnpType`，64 列）：
- `Price`、`Open`、`High`、`Low`、`PreClose` — 价格信息
- `AskPx1`~`AskPx10`、`AskVol1`~`AskVol10` — 卖方十档挂单
- `BidPx1`~`BidPx10`、`BidVol1`~`BidVol10` — 买方十档挂单
- `AccVolume`、`AccTurnover` — 累计成交量/额

Tick 是**聚合数据**，单条信息量最大（64 个字段），但条数最少（每只股票每 3 秒一条，~5000 只股票 × 每分钟 20 条 ≈ 10 万条/分钟）。

### 三者关系

```
Order（委托）          Trade（成交）           Tick（快照）
逐笔，最细粒度         逐笔，两两配对           定时聚合
─────────────────────────────────────────────────────────
投资者下单 ──→ 交易所撮合 ──→ 配对成交 ──→ 汇总成快照
                                              ↓
买入 10.50×500 ─┐                      最新价 10.50
                ├─→ 成交 10.50×500     买一 10.49×2000
卖出 10.50×800 ─┘                      卖一 10.51×800
                                       累计成交量 125000
```

量化研究中：
- **tick** 用得最多 — 做因子计算、盘口分析，数据量适中
- **order + trade** 用于更精细的微观结构研究 — 比如追踪大单、分析主力行为、重建完整 order book

### 数据量对比（典型全天）

| 数据类型 | 每分钟量级 | 说明 |
|----------|-----------|------|
| tick | ~10 万条 | 5000 只股票 × 每 3 秒一条 |
| order | ~10-100 万条 | 每笔委托挂单都记录，集合竞价时爆发 |
| trade | ~10-100 万条 | 只记成交的，一笔大单可拆多笔 |

这也是为什么同样 5 × 128MB 的 journal 文件，tick 能装到 0941 而 trade 只到 0930 — trade/order 数据密度远高于 tick。

## 1. 写入与读取的协作原理

Journal 文件通过 mmap 映射到内存，writer 和 reader 进程映射同一个文件，共享同一块物理内存。数据的写入和新数据发现完全通过这块共享内存完成，不依赖任何 IPC 通知机制。

### 写入过程

实盘中，`insight_gateway` 通过 `JournalWriter::write_frame()` 写入数据。每次写入在 mmap 内存上执行以下操作：

1. **定位空闲帧**：`Page::locateWritableFrame()` 从当前位置向后扫描，跳过 `status==WRITTEN` 的已写帧，找到第一个 `status==RAW`（值为 0）的位置。
2. **填写帧头 + 拷贝数据**：在该位置写入 40 字节 FrameHeader（时间戳、msg_type、长度等），然后 `memcpy` 将 packed 结构体（如 `KyStdSnpType`）复制到帧头之后。
3. **标记完成**：`Frame::setStatusWritten()` 先将**下一帧**的 status 设为 RAW（边界哨兵，防止 reader 越界读取），再将**当前帧**的 status 从 RAW 改为 WRITTEN（值为 1）。

当页面剩余空间不足 2MB 时，writer 向 Paged 申请下一个 128MB 页面。

### reader 如何发现新数据

**没有显式通知。** reader 轮询 mmap 共享内存中帧头的 `status` 字段：

- `status == WRITTEN (1)` → 这一帧有数据，可以读取
- `status == RAW (0)` → 这一帧还没有被写入，暂无新数据
- `status == PAGE_END (2)` → 当前页结束，加载下一页

`status` 字段声明为 `volatile`，保证编译器不会缓存它的值，每次都从内存重新读取。因为 writer 和 reader 的 mmap 映射到同一块物理内存页，writer 写入 `status = WRITTEN` 后 reader 立即可见，无需 socket、管道或信号量。

Paged 进程**不参与数据通知**，它只负责 mmap 映射管理和换页分配。

### 为什么用 busy-wait：用 CPU 换延迟

这种"while 循环不停检查 status"的做法是 busy-wait（忙等），没有新数据时线程会空转，CPU 占用率会飙到接近 100%（实盘 `insight_gateway` 就是 99% CPU）。这是低延迟系统的经典设计取舍：`sleep` 或 `epoll` 等休眠/通知机制的唤醒延迟在微秒级，而 busy-wait 轮询内存的延迟在纳秒级，对高频行情场景差了几个数量级。KungFu 框架不提供"有数据通知、无数据休眠"的模式，它就是为实盘托管机设计的——CPU 本来就是拿来干这个的。

## 2. 概述

Journal 是 KungFu 框架的持久化存储组件，用于记录和回放行情数据。读取方式有两种：

| 读取方式 | 用途 | 核心类 |
|---------|------|--------|
| `DataConsumer::run()` | 遍历全部历史数据 | `DataConsumer` (src/data_consumer.h:9-76, src/data_consumer.cpp:140-185) |
| `DataFetcher::get_*_data()` | 按时间段提取数据 | `DataFetcher` (src/data_consumer.h:78-89, src/data_consumer.cpp:187-354) |

### 输入

| 参数 | 类型 | 说明 |
|------|------|------|
| journal 目录路径 | `std::string` | 数据文件所在目录 |
| 时间范围 | `long start_nano, end_nano` | 纳秒级时间戳 |
| 频道名称 | `std::string` | 见下表 |

### 频道配置

定义于 `src/data_consumer.h:71-75`

```cpp
std::string tick_channel = "insight_stock_tick_data";      // 快照数据
std::string order_channel = "insight_stock_order_data";   // 逐笔委托
std::string trade_channel = "insight_stock_trade_data";   // 逐笔成交
```

## 3. 数据流与调用链

```
外部调用（main.cpp）
        ↓
DataConsumer::run()                         ← 本模块入口  § 3
        │
        ├─→ getJournalFiles()               § 3.1  查找 journal 文件 (src/data_consumer.cpp:107-125)
        │       └─→ 遍历目录，匹配 yjj.<channel>.*.journal
        │
        ├─→ LocalJournalPage::load()        § 3.2  mmap 加载页面 (src/data_consumer.cpp:62-84)
        │       └─→ open() + mmap() 系统调用
        │
        └─→ 循环读取帧
                │
                ├─→ nextFrame()             § 3.3  读取下一帧 (src/data_consumer.cpp:86-99)
                │       └─→ 解析 LocalFrameHeader
                │
                ├─→ getFrameData()          § 3.4  获取帧数据 (src/data_consumer.cpp:101-103)
                │       └─→ 跳过 header，取 payload
                │
                └─→ 消息分发（按 msg_type）
                        │
                        ├─→ MSG_TYPE_L2_TICK (61)
                        │       └─→ on_market_data()     § 3.5 (src/data_consumer.h:21-31)
                        │               └─→ trans_tick()  § 4.1 (src/insight_types.cpp:3-74)
                        │
                        ├─→ MSG_TYPE_L2_ORDER (62)
                        │       └─→ on_order_data()      § 3.6 (src/data_consumer.h:32-46)
                        │               └─→ trans_order() § 4.2 (src/insight_types.cpp:76-114)
                        │
                        └─→ MSG_TYPE_L2_TRADE (63)
                                └─→ on_trade_data()      § 3.7 (src/data_consumer.h:48-62)
                                        └─→ trans_trade() § 4.3 (src/insight_types.cpp:116-161)
```

## 4. 消息类型与数据结构

### 4.1 消息类型常量

定义于 `src/sys_messages.h:32-34`

| 常量 | 值 | 数据类型 |
|------|-----|----------|
| `MSG_TYPE_L2_TICK` | 61 | `KyStdSnpType` |
| `MSG_TYPE_L2_ORDER` | 62 | `KyStdOrderType` |
| `MSG_TYPE_L2_TRADE` | 63 | `KyStdTradeType` |

### 4.2 KungFu 原始结构

定义于 `src/data_struct.hpp`

**KyStdTradeType（逐笔成交）** - `src/data_struct.hpp:25-93`
```cpp
struct KyStdTradeType {
    int32_t AskOrder;         // 卖方订单号
    int8_t BSFlag;            // 内外盘标识（B=外盘, S=内盘）
    int32_t BidOrder;         // 买方订单号
    int32_t BizIndex;         // 序号
    int32_t Channel;          // 频道代码
    int8_t FunctionCode;      // 成交类别
    int32_t Index;            // 成交序号
    int8_t OrderKind;         // 委托类别
    float_t Price;            // 成交价格
    int32_t Time;             // 时间
    int32_t Volume;           // 成交数量
    int32_t Symbol;           // 股票代码
    int32_t Date;             // 交易日期
    int32_t Multiple;         // 数量倍数
    int32_t Money;            // 成交金额
};
```

**KyStdSnpType（快照数据）** - `src/data_struct.hpp:95-245`
```cpp
struct KyStdSnpType {
    int32_t Symbol;           // 股票代码（6位数字）
    int32_t Time;             // 时间（HHMMSSmmm）
    float_t Price;            // 最新价
    float_t Open;             // 开盘价
    float_t High;             // 最高价
    float_t Low;              // 最低价
    float PreClose;           // 昨收价
    int32_t Volume;           // 成交量
    int64_t AccVolume;        // 累计成交量
    int64_t AccTurnover;      // 累计成交额
    int64_t Turnover;         // 成交额
    float BidPx1~10;          // 买一~买十价
    float AskPx1~10;          // 卖一~卖十价
    float64_t BidVol1~10;     // 买一~买十量
    float64_t AskVol1~10;     // 卖一~卖十量
};
```

**KyStdOrderType（逐笔委托）** - `src/data_struct.hpp:247-329`
```cpp
struct KyStdOrderType {
    int32_t Symbol;           // 股票代码
    int32_t Date;             // 交易日期
    int32_t Time;             // 时间
    int32_t OrderNumber;      // 委托编号
    int8_t OrderKind;         // 委托类别（1=买入, 2=卖出）
    int8_t FunctionCode;      // 委托方向
    float_t Price;            // 委托价格
    int32_t Volume;           // 委托数量
    int32_t BizIndex;         // 序号
    int64_t Channel;          // 频道代码
    int32_t OrderOriNo;       // 原始订单号（仅上海）
    int32_t TradedQty;        // 已成交数量（仅上海）
};
```

### 4.3 转换后结构

定义于 `src/insight_types.h`

转换函数将 KungFu 原始结构转换为统一的 L2 结构体：

| 原始类型 | 转换函数 | 输出类型 |
|----------|----------|----------|
| `KyStdSnpType` | `trans_tick()` (src/insight_types.cpp:3) | `L2StockTickDataField` (src/insight_types.h:870) |
| `KyStdOrderType` | `trans_order()` (src/insight_types.cpp:76) | `SZ_StockStepOrderField` / `SH_StockStepOrderField` (src/insight_types.h:1119/1162) |
| `KyStdTradeType` | `trans_trade()` (src/insight_types.cpp:116) | `SZ_StockStepTradeField` / `SH_StockStepTradeField` (src/insight_types.h:1204/1247) |

**L2StockTickDataField** (`src/insight_types.h:870-1117`) 包含 10 档买卖盘（BidPx1~10, AskPx1~10）及成交量、成交额等统计信息。

**SH_/SZ_StockStepOrderField** (`src/insight_types.h:1119-1160` / `1162-1202`) 字段基本相同，上海市场多了 `iOrderNo`（原始订单号）和 `nTradedQty`（已成交数量）。

**SH_/SZ_StockStepTradeField** (`src/insight_types.h:1204-1245` / `1247-1287`) 包含买卖双方订单号、成交价格、成交量、成交金额、内外盘标识等。

### 4.4 交易所判断

通过股票代码判断交易所，定义于 `src/utils.cpp:41-50`：
```cpp
std::string decode_exchange(int symbol) {
    if (symbol < 400000) return "SZ";   // 深市
    else if (symbol < 700000) return "SH"; // 沪市
    return "";
}
```

## 5. Journal 文件格式

### 5.1 文件命名规则

```
yjj.<channel_name>.<page_num>.journal
```

示例：`yjj.insight_stock_tick_data.0.journal`

### 5.2 本地页面结构

定义于 `src/data_consumer.cpp:19-43`

**LocalPageHeader**（页头，64 字节）
```cpp
struct LocalPageHeader {
    unsigned char status;                    // 状态
    char journal_name[30];                  // Journal 名称
    short page_num;                         // 页号
    long start_nano;                        // 开始时间（纳秒）
    long close_nano;                        // 关闭时间
    int frame_num;                          // 帧数量
    int last_pos;                           // 最后位置
    // ... 保留字段
};
```

**LocalFrameHeader**（帧头，32 字节）
```cpp
struct LocalFrameHeader {
    volatile unsigned char status;           // 帧状态（1=已写入）
    short source;                           // 数据源
    long nano;                              // 时间戳（纳秒）
    int length;                             // 数据长度
    unsigned int hash;                      // 哈希
    short msg_type;                         // 消息类型（61/62/63）
    unsigned char last_flag;                // 结束标志
    int req_id;                             // 请求 ID
    long extra_nano;                        // 扩展时间
    int err_id;                             // 错误 ID
};
```

### 5.3 读取流程

1. **mmap 映射文件**：`LocalJournalPage::load()` (`src/data_consumer.cpp:62-84`) 使用 `mmap()` 将整个 journal 文件映射到内存
2. **跳过页头**：从 `sizeof(LocalPageHeader)` 开始读取
3. **遍历帧**：循环调用 `nextFrame()` (`src/data_consumer.cpp:86-99`)，根据 `header->length` 移动位置
4. **检查有效性**：`status == JOURNAL_FRAME_STATUS_WRITTEN (1)` 表示帧已完整写入
5. **解析数据**：`getFrameData()` (`src/data_consumer.cpp:101-103`) 返回帧头后的实际数据

## 6. 使用示例

### 6.1 遍历全部数据

```cpp
#include "data_consumer.h"

using namespace kungfu::yijinjing;

int main() {
    // 创建消费者
    DataConsumer consumer;

    // 设置时间范围（0 表示从最早开始）
    long start_nano = 0;
    long end_nano = parseTime("20260101-00:00:00", "%Y%m%d-%H:%M:%S");

    consumer.init(start_nano, end_nano);
    consumer.run();  // 遍历所有 journal 文件

    // 打印统计
    // Total tick data: xxx
    // Total order data: xxx
    // Total trade data: xxx
}
```

### 6.2 按时间段提取数据

```cpp
DataFetcher fetcher;

// 提取 2024-08-09 10:50:00 ~ 2025-01-01 的数据
long start_nano = parseTime("20240809-10:50:00", "%Y%m%d-%H:%M:%S");
long end_nano = parseTime("20250101-00:00:00", "%Y%m%d-%H:%M:%S");

// 提取 Tick 数据
auto ticks = fetcher.get_tick_data(start_nano, end_nano);

// 提取上海逐笔委托
auto sh_orders = fetcher.get_sh_order_data(start_nano, end_nano);

// 提取深圳逐笔委托
auto sz_orders = fetcher.get_sz_order_data(start_nano, end_nano);

// 提取上海逐笔成交
auto sh_trades = fetcher.get_sh_trade_data(start_nano, end_nano);

// 提取深圳逐笔成交
auto sz_trades = fetcher.get_sz_trade_data(start_nano, end_nano);
```

## 7. 编译与运行

**编译**
```bash
cd kungfu_demo/build && cmake .. && make
```

**运行**
```bash
./build/data_consumer
```

输出示例：
```
Found 1 journal files for insight_stock_tick_data
Processing: xxx/yjj.insight_stock_tick_data.0.journal
get 100 tick data
L2StockTickDataField { nTime: 935000, ... }
get 200 tick data
...
```

## 8. 实盘 data_fetcher：定时读 journal 写 CSV/Parquet

实盘托管机上运行着三个 `data_fetcher` 实例（源码位于 `market_gateway/src/brokers/insight_tcp/data_fetcher.h`），它们定时从 journal 读取数据并导出为 CSV 或 Parquet 文件。

### 8.1 启动方式

```bash
./data_fetcher --tick-only  --interval 1 &   # 每 1 分钟 dump tick
./data_fetcher --order-only --interval 1 &   # 每 1 分钟 dump order
./data_fetcher --trade-only --interval 1 &   # 每 1 分钟 dump trade
```

`--interval 1` 表示 1 分钟为一个时间窗口。每个实例只处理一种数据类型，互不干扰。

### 8.2 类继承关系

```
DataConsumer          — journal 读取 + msg_type 分发（createReaderWithSys）
  └── DataFetcher     — 按时间段提取数据到 vector（get_tick_data 等）
        └── DumpHandler — 定时调度 + 写 CSV/Parquet
```

### 8.3 定时 dump 流程

`DumpHandler` 启动后，在独立线程中执行 `dump_to_file()`：

1. **生成时间点序列**：根据 `interval`、`start_time`、`end_time` 生成目标时间列表。例如 interval=1 分钟时生成 `[09:26, 09:27, ..., 11:31, 13:00, ..., 15:01]`，自动跳过午休（11:31-12:59）。

2. **等待到达目标时间**：对每个目标时间，`sleep_until` 等到该时刻。

3. **读取该时间窗口的 journal 数据**：调用 `get_tick_data(start_nano, end_nano)` 等方法。内部创建 `JournalReader::create()`（LocalPageProvider，不需要 Paged），遍历该时间段内的所有帧，将 `KyStdSnpType` 通过 `trans_tick()` 转为 `L2StockTickDataField`，收集到 vector 中。对于 order 和 trade 数据，还会按 `decode_exchange()` 拆分为沪市/深市两个 vector。

4. **写入文件**：根据配置写 CSV 或 Parquet：
   - **CSV**：调用 L2 结构体的 `to_csv_header()` 写表头（仅第一次），`to_csv_row()` 逐行写入
   - **Parquet**：调用 `ParquetUtils::write_tick_data_to_parquet()` 等，通过 Apache Arrow 构建列式数据后写入

输出文件路径格式：`{dump_path}/{channel_name}/{date}_{HH:MM:00}.{csv|parquet}`

### 8.4 设计缺陷

这个 data_fetcher 的实现方式相当愚蠢——KungFu journal 系统本身就支持通过 mmap 共享内存 + volatile status 轮询来**实时发现新数据**（见 § 1），`JournalReader::createReaderWithSys()` 可以做到帧级别的实时流式读取。但 data_fetcher 完全没有利用这个能力，而是用 `sleep_until` 傻等到整分钟，再用 `JournalReader::create()`（LocalPageProvider）做一次离线批量提取。

本质上就是把一个天然支持实时流的系统，硬生生用成了**每分钟跑一次的定时批处理任务**。延迟白白增加到分钟级，还要维护时间点序列生成、午休跳过等一堆不必要的调度逻辑。

### 8.5 整体时序

```
09:10  insight_gateway 启动，开始写 journal
09:20  3 个 data_fetcher 启动
09:26  tick fetcher: 读 journal [09:10, 09:26]，写 tick csv/parquet
09:26  order fetcher: 读 journal [09:10, 09:26]，拆分 SH/SZ，各写一个文件
09:27  tick fetcher: 读 journal [09:26, 09:27]，追加写入
09:27  order fetcher: 读 journal [09:26, 09:27]，追加写入
...    每分钟重复
11:31  上午收盘，dump 11:30-11:31 的数据
13:00  下午开盘，跳过午休，继续 dump
...
15:01  收盘后最后一次 dump
15:30  stopAtTime 触发，进程退出
```

## 9. 注意事项

1. **数据目录**：默认路径为 `kungfu_demo/deps/data/`，可通过修改 `DataConsumer::path` (`src/data_consumer.h:74`) 变量更改
2. **时间格式**：`parseTime()` 使用 `%Y%m%d-%H:%M:%S` 格式
3. **纳秒时间**：`parse_nano()` (`src/utils.cpp:53-69`) 将纳秒转换为 `HHMMSSmmm` 格式（`int32_t`）
4. **内存映射**：使用 mmap 读取大文件性能较好，但需确保文件完整写入
