# Journal 写入架构与本地模拟方案

## 1. 实盘环境：谁在写 journal？

### 1.1 结论

实盘机器上写入 journal 的是一个**独立的行情采集进程**，它使用华泰证券 Insight 行情系统的 SDK（`mdc_gateway_client`）从交易所接收 L2 行情数据，然后通过 KungFu 的 `JournalWriter` API 写入 journal 文件。

这个进程**不是** KungFu 自带的行情引擎（`wingchun md ctp/xtp`），而是公司自研的 insight 行情网关。

### 1.2 推断依据

**依据一：journal 频道名不是 KungFu 标准的**

KungFu 自带的行情引擎写入路径和频道名是硬编码的（来自 `LFUtils.h`）：

| 柜台 | 目录 | 频道名 |
|------|------|--------|
| CTP | `/shared/kungfu/journal/MD/CTP/` | `MD_CTP` |
| XTP | `/shared/kungfu/journal/MD/XTP/` | `MD_XTP` |

而我们的 journal 文件是：

| 目录 | 频道名 |
|------|--------|
| `/shared/kungfu/journal/user/` | `insight_stock_tick_data` |
| `/shared/kungfu/journal/user/` | `insight_stock_order_data` |
| `/shared/kungfu/journal/user/` | `insight_stock_trade_data` |

频道名以 `insight_` 开��，目录是 `user/` 而非 `MD/`，说明这不是 KungFu 内建的 MD 引擎写的。

**依据二：项目依赖了华泰 Insight SDK**

`include/mdc_gateway_client/` 目录下包含华泰证券 MDC Gateway Client 的头文件：
- `client_interface.h` — 定义了 `HTSC_MDC_GATEWAY` 命名空间
- `mdc_client_factory.h` — 客户端工厂类
- `message_handle.h` — 消息回调接口（`OnMarketData`、`OnPlaybackPayload` 等）
- `protobuf/` — 大量 protobuf 定义（`MDQuote.pb.h`、`MDQBTransaction.pb.h` 等）

命名空间是 `com::htsc::mdc::insight::model`，HTSC = 华泰证券（Huatai Securities Co.）。

**依据三：数据结构对应关系**

项目中的 `KyStdSnpType`、`KyStdOrderType`、`KyStdTradeType`（`data_struct.hpp`）是 L2 Level-2 行情的 packed 结构，包含逐笔委托、逐笔成交等深度数据，这正是 Insight SDK 提供的数据类型。

**依据四：KungFu 自带的 MD 引擎没有启用**

supervisor 配置中 `md_ctp` 和 `md_xtp` 都是 `autostart=false`：

```ini
# /opt/kungfu/master/etc/supervisor/conf.d/md_ctp.conf
[program:md_ctp]
command= /usr/bin/wingchun md ctp
autostart=false    ← 没有启用
```

### 1.3 实盘数据流全貌

```
交易所（沪深 L2 行情）
    ↓ 网络推送
华泰 Insight 行情服务器
    ↓ protobuf over TCP
insight 行情网关进程（公司自研）
    ├─ 使用 mdc_gateway_client SDK 接收数据
    ├─ 将 Insight protobuf 数据转换为 KyStdSnpType / KyStdOrderType / KyStdTradeType
    └─ 调用 JournalWriter::write_frame() 写入 journal
         ├─ msg_type=61 → insight_stock_tick_data    (Tick 快照)
         ├─ msg_type=62 → insight_stock_order_data   (逐笔委托)
         └─ msg_type=63 → insight_stock_trade_data   (逐笔成交)
              ↓
journal 文件 (/shared/kungfu/journal/user/yjj.insight_stock_*.journal)
              ↓ Paged 做 mmap 协��
策略进程 / 回放程序 (JournalReader::createReaderWithSys)
```

> insight 行情网关进程可能运行在同一个 Docker 容器内（交易时段运行，非交易时段退出，所以 `ps -ef` 没看到），也可能在宿主机或另一个容器中，通过共享 `/shared/kungfu/journal/user/` 目录写入。

### 1.4 JournalWriter 写入原理

写入端的代码模式大致如下（推测）：

```cpp
#include "JournalWriter.h"

// 创建 writer（需要 Paged 运行）
// 参数: 目录, 频道名, writer名称
auto tick_writer = JournalWriter::create(
    "/shared/kungfu/journal/user/",
    "insight_stock_tick_data",
    "insight_gateway"
);

// 写入一帧数据
KyStdSnpType tick_data = convert_from_insight(raw_data);
tick_writer->write_frame(
    &tick_data,                    // 数据指针
    sizeof(KyStdSnpType),          // 数据长度
    0,                             // source
    MSG_TYPE_L2_TICK,              // msg_type = 61
    0,                             // lastFlag
    0                              // requestId
);
// write_frame 返回值是写入时的 nano 时间戳
```

每个 `write_frame` 调用会在 journal page 中追加一个帧：
- 40 字节 FrameHeader（nano 时间戳、msg_type、数据长度等）
- N 字节 payload（即 `KyStdSnpType` 等结构的二进制内容）

当一个 page（128MB）写满后，Paged 自动分配下一个 page。

## 2. 本地模拟方案

### 方案 A：直接使用历史 journal 文件（当前方案，最简单）

把实盘机器上的 journal 文件拷贝到本地，直接读取。

```
实盘 /shared/kungfu/journal/user/yjj.insight_stock_tick_data.*.journal
    ↓ scp / rsync 拷贝
本地 /shared/kungfu/journal/user/ 或 deps/data/
    ↓
本地程序读取（直接 mmap 或通过 Paged）
```

**优点**：零开发成本，数据完全真实
**缺点**：不能模拟实时写入的场景，只能做离线回放

**详见** `paged_quick_start.md`。

### 方案 B：写一个 journal replayer，模拟实时写入

开发一个程序，读取历史 journal 文件，按照原始时间戳间隔重新写入新的 journal，模拟实时行情流。

```
历史 journal 文件 (LocalPageProvider 直接读)
    ↓ JournalReader::create() 读取
replayer 进程
    ↓ 按时间间隔 sleep + JournalWriter::write_frame()
新 journal 文件 (/shared/kungfu/journal/user/)
    ↓ Paged 协调
策略进程 (JournalReader::createReaderWithSys 实时读)
```

实现思路：

```cpp
#include "JournalReader.h"
#include "JournalWriter.h"

// 用 LocalPageProvider 读取历史数据（不需要 Paged）
auto reader = JournalReader::create(
    "/path/to/historical/data",
    "insight_stock_tick_data",
    0  // startTime
);

// 用 ClientPageProvider 写入新 journal（需要 Paged）
auto writer = JournalWriter::create(
    "/shared/kungfu/journal/user/",
    "insight_stock_tick_data",
    "replayer"
);

long last_nano = 0;
while (true) {
    auto frame = reader->getNextFrame();
    if (!frame) break;

    // 按原始时间间隔 sleep（可选加速倍率）
    if (last_nano > 0) {
        long delay_ns = frame->getNano() - last_nano;
        delay_ns /= speed_multiplier;  // 如 10x 加速
        std::this_thread::sleep_for(std::chrono::nanoseconds(delay_ns));
    }
    last_nano = frame->getNano();

    // 写入新 journal
    writer->write_frame(
        frame->getData(),
        frame->getDataLength(),
        frame->getSource(),
        frame->getMsgType(),
        frame->getLastFlag(),
        frame->getRequestId()
    );
}
```

**优点**：模拟真实的实时数据流，可以测试策略的实时响应
**缺点**：需要开发，需要处理时间同步和多频道协调

### 方案 C：用 Python 快速原型

KungFu 的 `libjournal` 提供了 Python 绑定，可以快速写一个 replayer 原型：

```python
import libjournal
import time

# 读取历史数据
reader = libjournal.createReader(
    ['/path/to/historical/data'],
    ['insight_stock_tick_data'],
    'replay_reader',
    long(0)
)

# 创建 writer（需要 Paged 运行）
writer = libjournal.createWriter(
    '/shared/kungfu/journal/user/',
    'insight_stock_tick_data',
    'replayer'
)

last_nano = 0
speed = 10.0  # 10x 加速

while True:
    frame = reader.next()
    if frame is None:
        break

    nano = frame.nano()
    if last_nano > 0:
        delay = (nano - last_nano) / 1e9 / speed
        if delay > 0:
            time.sleep(delay)
    last_nano = nano

    # 注意：Python 绑定的 write 接口需要根据实际 API 调整
    # writer.writePyData(...)
```

运行前设置环境：
```bash
PYTHONPATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
python2 replayer.py
```

**优点**：开发速度快，方便调试
**缺点**：Python 2.7 的 GIL 限制，高频场景性能不够

### 方案对比

| | 方案 A：直接读历史文件 | 方案 B：C++ replayer | 方案 C：Python replayer |
|--|--|--|--|
| 开发成本 | 零 | 中等 | 低 |
| 模拟实时 | 不能 | 能 | 能 |
| 性能 | 最快（直接 mmap） | 高 | 受 GIL 限制 |
| 需要 Paged | 可选 | 需要 | 需要 |
| 适用场景 | 离线分析、回测 | 策略实盘模拟测试 | 快速验证逻辑 |

### 建议

1. **日常回测分析**：用方案 A，直接读历史文件，不需要 Paged
2. **想模拟完整实盘环境**：先用方案 C (Python) 快速验证可行性，确认没问题后再用方案 B (C++) 写正式版本
3. **多天数据切换**：按日期组织 journal 文件目录，replayer 支持指定日期参数

## 3. KungFu 自带 vs Insight 行情引擎对比

| | KungFu 自带 MD 引擎 | Insight 行情网关（公司自研） |
|--|--|--|
| 启动命令 | `wingchun md ctp` / `wingchun md xtp` | 自研进程 |
| Journal 路径 | `/shared/kungfu/journal/MD/CTP/` | `/shared/kungfu/journal/user/` |
| Journal 频道 | `MD_CTP` / `MD_XTP` | `insight_stock_tick_data` 等 |
| 数据结构 | `LFMarketDataField`（5 档行情） | `KyStdSnpType`（10 档行情 + 逐笔） |
| 柜台 | CTP（期货）、XTP（股票） | 华泰 Insight（沪深 L2 全市场） |
| msg_type | KungFu 标准值 | 61 (tick) / 62 (order) / 63 (trade) |
| supervisor | `md_ctp.conf` / `md_xtp.conf` | 不在 supervisor 中 |
