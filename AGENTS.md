# AGENTS.md

本文件为 OpenCode / 通用代码代理提供项目上下文与工作约定。

## 沟通约定

- 始终使用中文回复用户。

## 项目背景

本项目属于盖亚青柯量化私募的流式因子计算系统。

线上架构中，托管机运行 `market_gateway`（代码位于 `ht_insight_market_gateway/insight_market_gateway/`），负责：
1. 读取华泰 Insight 实时 L2 行情数据
2. 转换为 KungFu journal 格式写入 journal 文件
3. 通过 KungFu yjj 启动 Paged 服务，允许其他进程以 mmap 方式实时读取 journal

下游多个独立进程通过 Paged 同时读取 journal：
- `realtime_parquet`：实时落盘 tick/order/trade 分钟 Parquet
- `realtime_kbar`：实时合成聚宽口径 1min K 线 CSV
- `monitor_latency`：监控行情链路延迟
- `realtime_factors`：规划中的实时因子计算模块

长期目标：将量化研究员所需因子逐步迁移为 C++ 流式实时计算，直接基于 L2 journal 行情数据产出。

KungFu journal 系统说明见 `docs/L2行情数据与Journal系统.md`。

## 编译与运行

主项目（工具库 + replayer + live_reader）：

```bash
cd build && cmake .. && make
```

产物：
- `libinsight_handle.so`：工具函数 + 数据类型
- `journal_replayer`：回放工具
- `live_reader`：实时读取工具

构建要求：
- C++17
- `-O3 -Wall -fPIC`
- CMake >= 3.18

各子项目独立编译，细节见对应目录说明。

### 本地模拟回放

优先使用 `scripts/replay`：

```bash
scripts/replay start 10
scripts/replay stop
scripts/replay start 10
scripts/replay start --reset 10
scripts/replay status
scripts/replay clean
```

说明：
- `start 10`：10 倍速启动 Paged + replayer
- `stop`：停止 replayer，保留 Paged，reader 可重连
- `start --reset 10`：从头开始回放
- 回放数据源目录：`deps/data/`

## 核心架构

### KungFu 原始结构体

定义于 `src/data_struct.hpp`：
- `KyStdSnpType`
- `KyStdOrderType`
- `KyStdTradeType`

这些结构体使用 `__attribute__((packed))`，二进制布局直接对应 journal frame payload。

### Journal 读取

`LocalJournalPage` 使用 `mmap()` 读取 `yjj.<channel>.<page>.journal` 文件。

Frame header 固定为 40 字节，按 `msg_type` 分发：
- 61 (`MSG_TYPE_L2_TICK`) -> tick 快照
- 62 (`MSG_TYPE_L2_ORDER`) -> 逐笔委托
- 63 (`MSG_TYPE_L2_TRADE`) -> 逐笔成交

### Journal 回放

`tools/journal_replayer.cpp`：
- mmap 读取 `deps/data/` 下历史 journal
- 跨频道按时间排序
- 通过 Paged 的 `JournalWriter` 按时间回放写入
- 支持断点续写，通过 `extra_nano` 记录原始时间戳

### 实时读取

`tools/live_reader.cpp` 通过 `JournalReader::createReaderWithSys()` 经 Paged 实时读取，并按 `msg_type` 分发处理。

### 交易所判断

- Symbol < 400000 -> SZ（深圳）
- Symbol >= 400000 -> SH（上海）

## 关键约定

- `src/data_struct.hpp` 中 packed 结构体不可添加 padding，不可调整字段顺序
- Journal page header 固定 56 字节，frame header 固定 40 字节，在 `LocalJournalPage` 中硬编码
- `JournalWriter` name 不可超过 30 个字符
- Paged 服务支持多个 reader 同时连接，各自使用不同 name 注册

## KungFu 版本

项目使用 KungFu `v1.0.0`（taurus.ai）：
- Git Tag: `v1.0.0`
- Commit: `ebbc9e21f3b2efad743c402f453d7487d8d3f961`
- RPM: `kungfu-0.0.5-20240912141305.x86_64`
- 技术栈：C++ / Python 2.7 / Boost.Python
- 验证命令：`rpm -qi kungfu`

注意：本项目是 KungFu v1.x 体系，不能套用 v2.x 文档或接口假设。

## 依赖环境（当前 CMake 配置）

- KungFu v1.0.0：`/opt/kungfu/master/`
- Boost 1.62.0：`/opt/kungfu/toolchain/boost-1.62.0/`
- Python 2.7：系统自带
- Apache Arrow 21.0.0 + Parquet：`/usr/local/`
- 本地第三方库：`lib/` 下的 ACE 6.4.3、Protobuf 3.1.0、MDC Gateway Client

环境切换说明见上层 `streaming_factor/CLAUDE.md`。

## 子项目说明

### realtime_parquet/

通过 Paged 实时读取 journal，按分钟输出 tick/order/trade Parquet 文件；每分钟一组文件，原子写入；分钟切换由 `nano_timestamp` 的 HHMM 变化触发。已在线上运行。

### realtime_kbar/

通过 Paged 实时读取 tick journal，合成聚宽口径 1min K 线，输出 CSV；采用 sec01_flush + patch 模式发布。详见 `realtime_kbar/README.md`。

离线验证工具 `kbar_offline` 可与 JQ `price_1m` 逐 bar 对比，验证结果 OHLCV 全部 100% match。

### monitor_latency/

监控 gateway 行情链路延迟。

### archive_journal/

收盘后读取当天 journal 并转成 Parquet 存档；多线程并行读取，按 page number 数字排序保证输出有序。已在线上运行。

### realtime_factors/

规划中的实时因子计算模块，从 L2 journal 实时计算量化因子。

### demo_project/

自包含的实习生面试项目，包含精简源码、回放工具、示例因子代码和 30 个因子题目；独立编译，不依赖父项目。

## 代理工作建议

- 优先遵守本文件与仓库内现有实现约定
- 涉及二进制结构体、journal header、msg_type 分发时，先核对现有代码再改动
- 涉及回放、实时读取、paged 多进程协作时，避免基于通用消息队列模型做错误抽象
- 修改构建或依赖路径前，先确认当前环境是否为 Docker 路径配置
