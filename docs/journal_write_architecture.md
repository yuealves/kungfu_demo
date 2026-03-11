# Journal 写入架构与本地模拟方案

## 1. 实盘环境：谁在写 journal？

### 1.1 结论

实盘机器上写入 journal 的是 **`insight_gateway`** 进程，位于 `/opt/insight_test/` 目录。它使用华泰证券 Insight 行情系统的 SDK（`mdc_gateway_client`）从交易所接收 L2 行情数据，然后通过 KungFu 的 `JournalWriter` API 写入 journal 文件。

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

项目中的 `KyStdSnpType`、`KyStdOrderType`、`KyStdTradeType`（`src/data_struct.hpp`）是 L2 Level-2 行情的 packed 结构，包含逐笔委托、逐笔成交等深度数据，这正是 Insight SDK 提供的数据类型。

**依据四：KungFu 自带的 MD 引擎没有启用**

supervisor 配置中 `md_ctp` 和 `md_xtp` 都是 `autostart=false`：

```ini
# /opt/kungfu/master/etc/supervisor/conf.d/md_ctp.conf
[program:md_ctp]
command= /usr/bin/wingchun md ctp
autostart=false    ← 没有启用
```

### 1.3 实盘进程实录（2026-03-05 开盘时段 ps -ef）

以下是开盘时间（约 10:39）在实盘托管机 Docker 环境内抓取的进程列表（仅保留关键进程）：

```
# 常驻服务
bruce       3229       0    supervisord -c /opt/kungfu/master/etc/supervisor/supervisord.conf
bruce       3230    3229    python -u /opt/kungfu/master/bin/yjj server    # Paged 父进程
bruce       3231    3230    python -u /opt/kungfu/master/bin/yjj server    # Paged 实际进程 (99% CPU)

# 行情写入端 — insight_gateway（09:10 由 cron 启动）
root       24417      29    CROND -n
root       24419   24417    /bin/sh -c bash /opt/insight_test/start_insight.sh >> /opt/insight_test/insight.log 2>&1
root       24420   24419    bash /opt/insight_test/start_insight.sh
root       24430   24420    ./insight_gateway                               # 99% CPU，行情网关

# 行情消费端 — data_fetcher（09:20 启动，三个实例）
root       24463       1    ./data_fetcher --tick-only  --interval 1        # 21% CPU
root       24464       1    ./data_fetcher --order-only --interval 1        # 10% CPU
root       24465       1    ./data_fetcher --trade-only --interval 1        #  0% CPU

# 其他：多个 bash 终端、yjj shell python 会话等（省略）
```

#### 关键观察

**`insight_gateway`（PID 24430）— 行情写入端（writer）**：
- 路径：`/opt/insight_test/insight_gateway`
- 启动方式：cron 定时任务，09:10 触发 `start_insight.sh` 脚本
- CPU 占用：99%（全速处理交易所推送的 L2 行情流）
- 启动时间：09:10，早于 09:15 集合竞价，提前连接 Insight 服务器做准备
- 职责：连接华泰 Insight → 接收 protobuf 行情 → 转换为 KyStd* 结构 → JournalWriter 写入 journal

**`data_fetcher`（PID 24463/24464/24465）— 行情消费端（reader）**：
- 三个独立实例，分别处理不同数据类型：
  - `--tick-only`：只读 tick 数据（21% CPU，数据量最大）
  - `--order-only`：只读逐笔委托（10% CPU）
  - `--trade-only`：只读逐笔成交（~0% CPU，数据量最小）
- `--interval 1`：可能是 1 秒轮询间隔或批处理间隔
- 启动时间：09:20，比 gateway 晚 10 分钟，确保 journal 中已有数据
- PPID 为 1：说明是由某个脚本启动后脱离了父进程（daemon 化）
- 这就是本项目 `DataFetcher` 类的生产版本

**Paged（PID 3231）— 常驻服务**：
- 从 1 月 30 日开始运行，累计 CPU 时间 33 天+
- 由 supervisord 管理，开机自启

#### 时序关系

```
09:10  cron 触发 start_insight.sh → insight_gateway 启动，连接 Insight 服务器
09:15  集合竞价开始，insight_gateway 开始接收数据并写入 journal
09:20  data_fetcher 三个实例启动，开始从 journal 消费数据
09:25  集合竞价结束
09:30  连续竞价开始
...
15:00  收盘（具体停止时间未知，可能由 cron 或脚本控制）
```

### 1.4 实盘目录结构与启动脚本（已确认）

#### `/opt/insight_test/` 目录内容

```
drwxrwxrwx 5 root root      221 Feb 12 20:00 .
drwx------ 2 1103 1103       68 Feb 12  2025 cert/            # TLS 证书（���接 Insight 服务器用）
-rwx------ 1 1103 1103      254 Jan 30 22:19 config.json      # 配置文件
-rw-r--r-- 1 root root      527 Feb  4 15:23 delete_old_data.sh
-rw-r--r-- 1 root root   196926 Mar  5 10:37 fetcher.log      # data_fetcher 日志
-rw-r--r-- 1 root root 55545887 Mar  5 10:46 insight.log      # insight_gateway 日志（55MB）
drwx------ 7 1103 1103      188 Jan 30 21:09 market_gateway/  # ★ 主项目目录
-rw------- 1 1103 1103 64845480 Jan 28 21:28 market_gateway.zip
drwx------ 7 1103 1103     4096 Feb 25 21:13 market_gateway_2/ # 可能是新版本或备份
-rw-r--r-- 1 root root      731 Feb  2 11:51 start_fetcher.sh
-rw------- 1 1103 1103      553 Feb  2 11:47 start_insight.sh
```

#### 关键发现：insight_gateway 和 data_fetcher 来自同一个项目

两个二进制文件都位于 `market_gateway` 项目的同一构建目录：

```
/opt/insight_test/market_gateway/build/src/brokers/insight_tcp/
├── insight_gateway    # 行情写入端（journal writer）
├── data_fetcher       # 行情消费端（journal reader）— 即 orig_insight_demo 的生产版本
└── lib/               # 依赖库目录
```

项目结构为 `market_gateway/src/brokers/insight_tcp/`，说明这是一个通用的行情网关框架，`insight_tcp` 是其中华泰 Insight TCP 行情的 broker 实现。

#### `start_insight.sh`（insight_gateway 启动脚本）

```bash
cd /opt/insight_test/market_gateway/build/src/brokers/insight_tcp

export LD_LIBRARY_PATH=/opt/insight_test/market_gateway/build/src/brokers/insight_tcp:\
/opt/insight_test/market_gateway/src/brokers/insight_tcp/lib:\
/opt/kungfu/toolchain/boost-1.62.0/lib:\
/opt/kungfu/master/lib/yijinjing:$LD_LIBRARY_PATH

# 先 kill 旧进程
echo "existing insight_gateway job"
echo `ps -ef | grep insight_gateway | grep -v grep`
for pid in $(ps -ef | grep insight_gateway | grep -v grep | awk '{print $2}')
    do kill -9 $pid;
done

echo "starting insight_gateway job"
./insight_gateway
```

**LD_LIBRARY_PATH 分析**：
- 自身构建目录（`.so` 依赖）
- `src/brokers/insight_tcp/lib/`（源码目录下的预编译依赖，如 Insight SDK 的 `.so`）
- Boost 1.62.0（与 KungFu 一致）
- KungFu yijinjing 库（`libjournal.so`，用于 `JournalWriter`）

这证实了 insight_gateway **直接链接 KungFu 的 yijinjing 库来写 journal**。

#### `start_fetcher.sh`（data_fetcher 启动脚本）

```bash
cd /opt/insight_test/market_gateway/build/src/brokers/insight_tcp

export LD_LIBRARY_PATH=...  # 同上

# 先 kill 旧进程
for pid in $(ps -ef | grep data_fetcher | grep -v grep | awk '{print $2}')
    do kill -9 $pid;
done

# 启动三个实例，后台运行
./data_fetcher --tick-only --interval 1 &
./data_fetcher --order-only --interval 1 &
./data_fetcher --trade-only --interval 1 &
```

**观察**：
- 与 insight_gateway 使用完全相同的 LD_LIBRARY_PATH
- 三个实例分别处理 tick/order/trade，以 `&` 后台运行
- PPID 为 1 的原因：`start_fetcher.sh` 脚本退出后，三个子进程被 init 收养

#### 与本地项目的关系

```
market_gateway/src/brokers/insight_tcp/   （实盘源码，不可直接访问）
    │
    ├── insight_gateway  ← 行情写入端（SDK 回调 → JournalWriter）
    │                       本地没有对应源码
    │
    └── data_fetcher     ← 行情消费端（JournalReader → 数据处理）
                            ↕ 对应关系
                         orig_insight_demo/  （本地项目）
                         kungfu_demo/        （本地改良版）
```

`data_fetcher` 和 `orig_insight_demo` 的 `data_consumer` 本质上是同一个程序的不同版本。

### 1.5 待进一步确认

以下信息如有机会可在实盘机器上进一步确认：

```bash
# 查看 config.json（可能包含 Insight 服务器地址、订阅的股票列表等）
cat /opt/insight_test/config.json

# 查看 market_gateway 项目的源码结构
ls -la /opt/insight_test/market_gateway/src/brokers/insight_tcp/
find /opt/insight_test/market_gateway/src -name "*.cpp" -o -name "*.h" | head -30

# 查看 market_gateway_2 是什么
ls -la /opt/insight_test/market_gateway_2/

# 查看 cron 定时任务配置
crontab -l
cat /etc/cron.d/*

# 查看 delete_old_data.sh（数据清理策略）
cat /opt/insight_test/delete_old_data.sh

# 查看 insight_gateway 运行日志的最后部分
tail -100 /opt/insight_test/insight.log

# 确认 journal 文件在开盘期间的增长
ls -la /shared/kungfu/journal/user/
```

### 1.6 insight_gateway 写入流程（源码已确认）

以下基于 `market_gateway/src/brokers/insight_tcp/` 源码分析，不再是推测。

#### 数据来源

insight_gateway 通过华泰 Insight SDK（`mdc_gateway_client`）获取行情。连接方式是 **TCP 长连接 + TLS 加密**（非 HTTP/WebSocket），使用 Insight SDK 自有的二进制协议（protobuf 序列化）。登录后 SDK 自动维护连接，交易所有新行情时 SDK 回调 `OnMarketData()`，是**推送模式**。

#### 写入架构

insight_gateway 内部用**生产者-消费者**模式解耦 SDK 回调和 journal 写入：

```
SDK 回调线程(5个)                          3 个 AsyncBatchWriter 线程
OnMarketData(protobuf)                            │
  ├─ MD_TICK  → StockTickData(data)  ──enqueue──→  tick_writer  ──→ ParseFrom() → KyStdSnpType  → write_frame(61)
  ├─ MD_ORDER → StockOrderData(data) ──enqueue──→  order_writer ──→ ParseFrom() → KyStdOrderType → write_frame(62)
  └─ MD_TRANSACTION → StockTransaction(data) ──→  trade_writer ──→ ParseFrom() → KyStdTradeType → write_frame(63)
                                                                                                        │
                                                                                               journal 文件 (mmap)
```

中间的 `ConcurrentQueue`（moodycamel 无锁队列，容量 100 万）让 SDK 回调线程只做轻量的 protobuf 字段提取 + 入队，写 journal 的重活由专用 writer 线程完成。

#### 实际的 write_frame 调用

```cpp
// insight_gateway.h — AsyncBatchWriter::process_queue()
for (const auto &stock_data : batch) {
    T2 packed_data = stock_data.ParseFrom();  // 如 StockTickData → KyStdSnpType
    data_writer->write_frame(
        &packed_data,   // packed 结构的指针
        sizeof(T2),     // 结构大小
        1,              // source = 1
        data_type,      // msg_type: 61/62/63
        1,              // lastFlag = 1
        -1              // requestId = -1
    );
}
```

#### write_frame 内部做了什么

`write_frame()` 最终调用 `write_frame_full()`，整个过程发生在 **mmap 的共享内存**上：

```
journal 文件 (128MB, mmap 到内存)
┌──────────────────────────────────────────────────────┐
│ Page Header (56 bytes)                                │
├──────────────────────────────────────────────────────┤
│ Frame 0: [FrameHeader 40B][payload]  status=WRITTEN   │  ← 已写入
│ Frame 1: [FrameHeader 40B][payload]  status=WRITTEN   │  ← 已写入
│ Frame 2: [FrameHeader 40B][payload]  status=WRITTEN   │  ← 已写入
│ Frame 3: [FrameHeader 40B]           status=RAW       │  ← writer 下次写这里
│          ... (剩余空间)                                │
└──────────────────────────────────────────────────────┘
```

写入一帧的 4 个步骤：

1. **定位写入位置**：`Page::locateWritableFrame()` 跳过所有 `status==WRITTEN` 的帧，找到第一个 `status==RAW` 的位置。如果剩余空间不足 2MB，返回 nullptr 触发换页。

2. **填写 FrameHeader**：设置 source、nano（当前纳秒时间戳）、msg_type、length 等字段。

3. **拷贝 payload**：`memcpy(header地址+40, &KyStdSnpType, sizeof(KyStdSnpType))`，把 packed 结构的二进制内容直接复制到 mmap 内存。

4. **标记写入完成**：`Frame::setStatusWritten()` — **先**把下一帧的 status 设为 RAW（边界哨兵），**再**把当前帧的 status 设为 `WRITTEN`。

FrameHeader 结构（40 字节，`FrameHeader.h`）：

```c
struct FrameHeader {
    volatile byte status;     // ★ 0=RAW, 1=WRITTEN, 2=PAGE_END
    short         source;     // 数据来源 ID
    long          nano;       // 纳秒时间戳
    int           length;     // 整帧长度（header + payload）
    uint          hash;       // payload 哈希
    short         msg_type;   // 消息类型 (61/62/63)
    byte          last_flag;  // 是否最后一帧
    int           req_id;     // 请求 ID
    long          extra_nano; // 额外时间戳
    int           err_id;     // 错误 ID
} __attribute__((packed));
```

`status` 字段声明了 `volatile`，保证编译器不会优化掉对它的重复读取——这是 reader 发现新数据的关键。

### 1.7 reader 怎么知道有新数据写入

**没有任何显式通知机制。** reader 靠轮询 mmap 共享内存中的 `status` 字段来发现新数据。

#### 原理

Writer 和 reader 对同一个 journal 文件做 mmap，操作系统保证它们映射到**同一块物理内存**。writer 设置 `status = WRITTEN` 后，reader 在同一块内存上立刻能看到，不需要 socket、管道或信号量。

```
insight_gateway 进程                         data_fetcher 进程
       │                                            │
  mmap("yjj.insight_stock_                    mmap(同一个文件)
        tick_data.1.journal")                        │
       │                                            │
       ▼                                            ▼
  ┌──────────┐                                ┌──────────┐
  │ 虚拟地址   │──→ 同一块物理内存页 ←──│ 虚拟地址   │
  └──────────┘                                └──────────┘
       │                                            │
  写入:                                         读取:
  1. 填 header + payload                       Page::locateReadableFrame()
  2. status = WRITTEN ←─── 立即可见 ───→         检查 status == WRITTEN ?
                                                   是 → 返回帧地址，读到新数据
                                                   否 → 返回 nullptr，继续轮询
```

#### reader 读取流程

`JournalReader::getNextFrame()`（`JournalReader.h`）：

```
遍历所有订阅的 journal:
  对每个 journal 调用 locateFrame()
    → Page::locateReadableFrame()
      → status == WRITTEN → 返回帧地址（有数据）
      → status == RAW     → 返回 nullptr（没有新数据）
      → status == PAGE_END → 加载下一页

如果多个 journal 同时有新帧，取 nano 最小的（按时间顺序合并多个频道）
如果都没有新帧，返回空指针（调用方自行决定是否重试）
```

#### Paged 在读写中的角色

Paged **不参与数据通知**，它的作用仅限于：
- **启动时**：帮 writer/reader 完成 journal 文件的 mmap 映射分配
- **换页时**：当 128MB page 写满，分配并 mmap 下一个 page 文件

数据写入和读取完全是 writer/reader 两个进程之间通过 **mmap 共享内存 + volatile status 轮询**实现的，数据不经过 Paged。

### 1.8 实盘数据流全貌（已确认）

```
交易所（沪深 L2 行情）
    ↓ 网络推送
华泰 Insight 行情服务器
    ↓ TCP + TLS + protobuf（cert/ 下的 TLS 证书）
    ↓
insight_gateway（market_gateway 项目编译产物）
    二进制: /opt/insight_test/market_gateway/build/src/brokers/insight_tcp/insight_gateway
    配置: /opt/insight_test/config.json
    09:10 cron 启动, 99% CPU
    │
    │  SDK 回调 OnMarketData() → protobuf 字段提取 → 无锁队列
    │  AsyncBatchWriter 线程 → ParseFrom() → packed 结构 → write_frame()
    │
    ├─ write_frame(type=61) → insight_stock_tick_data    (Tick 快照)
    ├─ write_frame(type=62) → insight_stock_order_data   (逐笔委托)
    └─ write_frame(type=63) → insight_stock_trade_data   (逐笔成交)
         │
         │  写入 mmap 共享内存，设置 status=WRITTEN
         │
         ▼
journal 文件 (/shared/kungfu/journal/user/yjj.insight_stock_*.journal)
         │
         │  reader 轮询 status 字段发现新数据（纯共享内存，不经过 Paged）
         │  Paged 只负责 mmap 映射管理和换页
         │
         ▼
data_fetcher（同一项目编译产物，即 orig_insight_demo 的生产版本）
    09:20 由 start_fetcher.sh 启动, 3 个实例:
    ├─ --tick-only  --interval 1  (21% CPU)
    ├─ --order-only --interval 1  (10% CPU)
    └─ --trade-only --interval 1  (~0% CPU)
```

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
