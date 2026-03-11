# insight_gateway 写入流程详解

基于 `market_gateway/src/brokers/insight_tcp/` 源码的完整分析。

## 1. 整体架构

```
insight_gateway 进程
│
├── main() [insight_gateway_run.cpp]
│   ├── InsightHandle handle(export_folder)
│   ├── handle.init()              ← 创建 3 个 AsyncBatchWriter
│   ├── handle.login()             ← 连接 Insight 服务器
│   ├── handle.subscribe_by_source_type()  ← 订阅沪深股票行情
│   ├── stopAtTime("15:30")        ← 阻塞等待到 15:30
│   └── handle.logout()            ← 停止 writer，断开连接
│
├── Insight SDK 回调线程（5 个线程池）
│   └── OnMarketData(data)         ← SDK 收到行情时自动调用
│       ├── MD_TICK → StockTickData(data) → save_to_queue()
│       ├── MD_ORDER → StockOrderData(data) → save_to_queue()
│       └── MD_TRANSACTION → StockTransaction(data) → save_to_queue()
│
└── 3 个 AsyncBatchWriter 线程
    ├── tick_writer: 从队列取 StockTickData → ParseFrom() → KyStdSnpType → write_frame(type=61)
    ├── order_writer: 从队列取 StockOrderData → ParseFrom() → KyStdOrderType → write_frame(type=62)
    └── trade_writer: 从队列取 StockTransaction → ParseFrom() → KyStdTradeType → write_frame(type=63)
```

## 2. 启动流程（main）

**文件**: `insight_gateway_run.cpp`

```cpp
int main(int argc, char *argv[]) {
    std::string stop_time_str = "15:30";    // 默认 15:30 停止
    if (argc >= 2) stop_time_str = argv[1]; // 可通过参数指定停止时间

    InsightHandle handle("./export_folder");
    handle.init();                           // 步骤 1: 初始化 writer
    handle.login();                          // 步骤 2: 连接 Insight
    handle.subscribe_by_source_type();       // 步骤 3: 订阅行情

    setupSignalHandler();                    // SIGINT/SIGTERM 优雅退出
    std::thread stop_thread(stopAtTime, stop_time_str);
    stop_thread.join();                      // 阻塞直到到达停止时间

    handle.logout();                         // 步骤 4: 停止并清理
}
```

## 3. init() — 创建 3 个 AsyncBatchWriter

**文件**: `insight_gateway.cpp:130-146`

```cpp
void InsightHandle::init() {
    open_trace();
    open_heartbeat_trace();
    open_response_callback();
    open_file_log();
    init_env();  // Insight SDK 环境初始化

    // ���建 3 个异步批量写入器，每个对应一个 journal 频道
    stock_tick_writer = make_unique<AsyncBatchWriter<StockTickData, KyStdSnpType>>(
        path, tick_channel, MSG_TYPE_L2_TICK);           // path="/shared/kungfu/journal/user/", channel="insight_stock_tick_data", type=61
    stock_order_writer = make_unique<AsyncBatchWriter<StockOrderData, KyStdOrderType>>(
        path, stock_order_channel, MSG_TYPE_L2_ORDER);   // channel="insight_stock_order_data", type=62
    stock_trade_writer = make_unique<AsyncBatchWriter<StockTransaction, KyStdTradeType>>(
        path, stock_trade_channel, MSG_TYPE_L2_TRADE);   // channel="insight_stock_trade_data", type=63
}
```

关键硬编码值：
- `path = "/shared/kungfu/journal/user/"`
- `tick_channel = "insight_stock_tick_data"`
- `stock_order_channel = "insight_stock_order_data"`
- `stock_trade_channel = "insight_stock_trade_data"`

## 4. login() — 连接华泰 Insight 服务器

**文件**: `insight_gateway.cpp:148-206`

```cpp
bool InsightHandle::login() {
    // 读取 config.json
    json config = json::parse(file);
    user = config["insight_config"]["user"];
    password = config["insight_config"]["password"];
    ip = config["insight_config"]["ip"];
    port = config["insight_config"]["port"];
    cert_folder = config["insight_config"]["cert_folder"];

    // 创建 Insight 客户端（TLS 加密）
    client = ClientFactory::Instance()->CreateClient(true, cert_folder.c_str());

    // 设置 5 个回调处理线程
    client->set_handle_pool_thread_count(5);

    // 注册当前对象为回调处理器
    client->RegistHandle(this);  // this 实现了 MessageHandle 接口

    // 备用服务器列表
    std::vector<std::string> backup_list = {
        "168.71.72.6:9362", "168.71.72.7:9373", "221.6.24.39:8262"
    };

    // 通过服务发现登录
    client->LoginByServiceDiscovery(ip, port, user, password, false, backup_list);
}
```

## 5. subscribe_by_source_type() — 订阅沪深股票全市场

**文件**: `insight_gateway.cpp:208-241`

订阅了两个交易所的三种数据类型：

| 交易所 | 证券类型 | 数据类型 |
|--------|---------|---------|
| XSHG（上海） | StockType | MD_TICK, MD_TRANSACTION, MD_ORDER |
| XSHE（深圳） | StockType | MD_TICK, MD_TRANSACTION, MD_ORDER |

这是全市场订阅，不限定具体股票代码。

## 6. OnMarketData() — SDK 回调入口（核心）

**文件**: `insight_gateway.cpp:15-84`

这是整个写入链路的核心。当 Insight SDK 收到交易所推送的行情数据时，会调用此回调：

```cpp
void InsightHandle::OnMarketData(const MarketData &data) {
    switch (data.marketdatatype()) {
    case MD_TICK:
        if (data.has_mdstock()) {
            auto stock_data = StockTickData(data);       // protobuf → StockTickData
            stock_tick_writer->save_to_queue(stock_data); // 入队（无锁）
        }
        // 也处理了 bond/index/fund/option/future，但只转换不入队（未写入 journal）
        break;

    case MD_TRANSACTION:
        if (data.has_mdtransaction()) {
            auto trans_data = StockTransaction(data);         // protobuf → StockTransaction
            stock_trade_writer->save_to_queue(trans_data);    // 入队
        }
        break;

    case MD_ORDER:
        if (data.has_mdorder()) {
            auto order_data = StockOrderData(data);           // protobuf → StockOrderData
            stock_order_writer->save_to_queue(order_data);    // 入队
        }
        break;
    }
}
```

**注意**：只有 `mdstock()`（股票 Tick）会写入 journal。`mdbond()`、`mdindex()`、`mdfund()` 等虽然也会进入 `MD_TICK` 分支，但没有入队操作，不会写入 journal。

## 7. AsyncBatchWriter — 异步批量写入器（核心组件）

**文件**: `insight_gateway.h:24-87`

这是生产者-消费者模式的关键组件：

```
OnMarketData 回调线程(5个)          AsyncBatchWriter 写入线程(1个)
         │                                    │
         │  save_to_queue(data)               │  process_queue()
         │  ──────────────────→                │
         │     ConcurrentQueue                 │  ←── 从队列批量取出
         │     (无锁，100万容量)                │  ──→ ParseFrom() 转换
         │                                    │  ──→ write_frame() 写入 journal
```

### 7.1 入队（生产者端）

```cpp
void save_to_queue(const T1 &data) {
    queue.enqueue(data);  // moodycamel::ConcurrentQueue，无锁入队
}
```

### 7.2 出队 + 写入（消费者端）

```cpp
void process_queue() {
    while (running) {
        auto queue_size = queue.size_approx();
        if (queue_size > 0) {
            std::vector<T1> batch(queue_size);
            size_t num_dequeued = queue.try_dequeue_bulk(batch.begin(), queue_size);
            if (num_dequeued > 0) {
                for (const auto &stock_data : batch) {
                    T2 packed_data = stock_data.ParseFrom();  // StockTickData → KyStdSnpType
                    data_writer->write_frame(
                        &packed_data,        // 数据指针
                        sizeof(T2),          // 数据长度
                        1,                   // source = 1
                        data_type,           // msg_type (61/62/63)
                        1,                   // lastFlag = 1
                        -1                   // requestId = -1
                    );
                }
            }
        }
        // 注意：队列为空时没有 sleep，是 busy-wait（这就是 99% CPU 的原因）
    }
}
```

### 7.3 JournalWriter 创建

```cpp
AsyncBatchWriter(const string &journal_path, const string &channel, int data_type)
    : channel(channel), data_type(data_type), running(true), path(journal_path)
{
    data_writer = JournalWriter::create(path, channel);  // 需要 Paged 运行
    writer_thread = std::thread(&AsyncBatchWriter::process_queue, this);
}
```

`JournalWriter::create(path, channel)` 使用 `ClientPageProvider`，需要 Paged 进程运行。这是默认的工厂方法（不像 Reader 有 `create` 和 `createReaderWithSys` 两种）。

### 7.4 队列实现

使用 `moodycamel::ConcurrentQueue`（[concurrentqueue.h](https://github.com/cameron314/concurrentqueue)），初始容量 100 万：

```cpp
moodycamel::ConcurrentQueue<T1> queue{1000000};
```

这是一个高性能无锁并发队列，支持多生产者单消费者（MPSC）模式。5 个 SDK 回调线程作为生产者，1 个 writer 线程作为消费者。

## 8. 数据转换链路

### 8.1 Tick 数据（MD_TICK → msg_type=61）

```
Insight protobuf (MarketData)
  ↓ StockTickData(data)  [构造函数，insight_types.h:90-202]
  ↓ 从 data.mdstock() 提取 56+ 个字段
StockTickData（中间结构，含 string/vector 等动态类型）
  ↓ ParseFrom()  [insight_types.h:258-340]
  ↓ 将动态字段转为固定大小的 float/int
KyStdSnpType（packed 结构，固定大小）
  ↓ write_frame(&packed_data, sizeof(KyStdSnpType), 1, 61, 1, -1)
journal 文件
```

**StockTickData 构造函数**从 protobuf 提取字段：
- `stock.htscsecurityid()` → 证券代码（如 "600460.SH"）
- `stock.mddate()`, `stock.mdtime()` → 日期时间
- `stock.lastpx()`, `stock.openpx()`, `stock.highpx()`, `stock.lowpx()` → OHLC
- `stock.buypricequeue(i)`, `stock.sellpricequeue(i)` → 10 档买卖价格
- `stock.buyorderqtyqueue(i)`, `stock.sellorderqtyqueue(i)` → 10 档委托量

**ParseFrom()** 关键转换：
- `htscsecurityid` "600460.SH" → 截掉后 3 位 → `symbol = 600460`（int32）
- `totalsellqty / 1000`, `totalbuyqty / 1000`（单位转换）
- 价格 `long` → `float`
- 委托量 `long` → `float64_t`

### 8.2 逐笔委托（MD_ORDER → msg_type=62）

```
Insight protobuf → StockOrderData(data) → ParseFrom() → KyStdOrderType → journal
```

**ParseFrom()** 关键转换：
- `orderbsflag`: 1→'B'(66), 2→'S'(83), else→0
- `ordertype`: 1→'1'(49), 2→'2'(50), 3→'U'(85), 10→'D'(68), else→'A'(65)
- `htscsecurityid` → 截掉后 3 位 → `symbol`

### 8.3 逐笔成交（MD_TRANSACTION → msg_type=63）

```
Insight protobuf → StockTransaction(data) → ParseFrom() → KyStdTradeType → journal
```

**ParseFrom()** 关键转换：
- `tradebsflag`: 1→'B'(66), 2→'S'(83), else→0
- `tradetype`: 0 或 9 → 'F'(70, 成交), else → '4'(52, 撤单)

## 9. write_frame 参数对照

| 参数 | Tick (61) | Order (62) | Trade (63) |
|------|-----------|------------|------------|
| data | `&KyStdSnpType` | `&KyStdOrderType` | `&KyStdTradeType` |
| length | `sizeof(KyStdSnpType)` | `sizeof(KyStdOrderType)` | `sizeof(KyStdTradeType)` |
| source | 1 | 1 | 1 |
| msg_type | 61 | 62 | 63 |
| lastFlag | 1 | 1 | 1 |
| requestId | -1 | -1 | -1 |

**与读取端 `data_consumer` 的对应关系**：
- `source=1` 在读取端被 `frame->getSource()` 读出，可用于区分数据来源
- `lastFlag=1` 表示该帧是独立的（不是分片消息的一部分）
- `requestId=-1` 表示这不是请求-响应模式的消息

## 10. 线程模型总结

```
线程 1: main 线程
  └── stopAtTime() 阻塞等待

线程 2-6: Insight SDK 回调线程池（5个）
  └── OnMarketData() 被 SDK 调用
      └── save_to_queue() 无锁入队

线程 7: tick AsyncBatchWriter::process_queue()
  └── 从队列取 StockTickData → ParseFrom() → write_frame(type=61)

线程 8: order AsyncBatchWriter::process_queue()
  └── 从队列取 StockOrderData → ParseFrom() → write_frame(type=62)

线程 9: trade AsyncBatchWriter::process_queue()
  └── 从队列取 StockTransaction → ParseFrom() → write_frame(type=63)

总计: 至少 9 个线程
```

## 11. 性能特征

- **99% CPU 的原因**：3 个 `process_queue()` 线程在队列为空时没有 `sleep`，是纯 busy-wait 轮询。这保证了最低延迟但牺牲了 CPU。
- **无锁队列**：`moodycamel::ConcurrentQueue` 避免了生产者-消费者之间的锁竞争。
- **批量出队**：`try_dequeue_bulk` 一次性取出所有可用数据，减少队列操作开销。
- **写入瓶颈**：每个 writer 是单线程写入 journal，如果行情速度超过写入速度，队列会持续增长（最大 100 万条）。

## 12. 与 kungfu_demo 的关系

| 组件 | market_gateway（实盘） | kungfu_demo（本地） |
|------|----------------------|-------------------|
| 写入端 | `insight_gateway` | 无（只有历史 journal） |
| 读取端 | `data_fetcher` | `data_consumer` |
| 数据结构 | `data_struct.hpp` (相同) | `src/data_struct.hpp` (相同) |
| L2 输出类型 | `insight_types.h` (相同) | `src/insight_types.h` (相同) |
| 转换函数 | `trans_tick/order/trade` (相同) | `trans_tick/order/trade` (相同) |
| 额外功能 | Parquet 导出、ConfigManager、SymbolSubscriber | 无 |

`kungfu_demo` 本质上是 `market_gateway` 读取端（`data_consumer` + `data_fetcher`）的简化版本，去掉了 ConfigManager、Parquet 支持等，只保留核心的 journal 读取和数据转换逻辑。
