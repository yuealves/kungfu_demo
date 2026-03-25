# CLAUDE.md

本文件为 Claude Code 提供项目上下文。

## 沟通语言

始终使用中文回复用户。

## 项目背景

本项目属于盖亚青柯量化私募的流式因子计算系统。

**线上架构**：托管机上运行 `market_gateway`（代码位于 `ht_insight_market_gateway/insight_market_gateway/`），负责：
1. 读取华泰 Insight 实时 L2 行情数据
2. 转换为 KungFu journal 格式写入 journal 文件
3. 通过 KungFu yjj 启动 Paged 服务，允许其他进程以 mmap 方式实时读取 journal

下游多个独立进程通过 Paged 同时读取 journal，各自处理：
- `realtime_parquet`：实时 dump tick/order/trade 分钟 Parquet 文件
- `realtime_kbar`：实时合成聚宽口径 1min K 线 CSV
- `monitor_latency`：监控行情链路延迟
- `realtime_factors`（规划中）：实时计算量化因子

**长期目标**：将公司量化研究员所需的因子全部改为 C++ 流式实时计算，直接从 L2 journal 行情数据中计算得到。

关于 KungFu journal 系统的详细说明见 `docs/L2行情数据与Journal系统.md`。

## 编译

主项目（工具库 + replayer + live_reader）：

```bash
cd build && cmake .. && make
```

产物：`libinsight_handle.so`（工具函数 + 数据类型）、`journal_replayer`（回放工具）、`live_reader`（实时读取工具）。

各子项目独立编译，详见各自目录的说明。

C++17，`-O3 -Wall -fPIC`，CMake >= 3.18。

### 本地模拟回放

推荐使用 `scripts/replay` 管理脚本（详见 `docs/replay_guide.md`）：

```bash
scripts/replay start 10          # 启动 Paged + replayer 后台运行（10 倍速）
scripts/replay stop              # 停止 replayer（Paged 保留，reader 可重连）
scripts/replay start 10          # 断点续播
scripts/replay start --reset 10  # 从头开始
scripts/replay status            # 查看运行状态
scripts/replay clean             # 停止所有服务 + 清理 journal
```

回放数据源目录：`deps/data/`。

## 架构

**KungFu 原始结构体**（`src/data_struct.hpp`）：`KyStdSnpType`、`KyStdOrderType`、`KyStdTradeType`，`__attribute__((packed))` 二进制布局，直接对应 journal frame payload。

**Journal 读取**：`LocalJournalPage` 使用 `mmap()` 读取 `yjj.<channel>.<page>.journal` 文件。Frame 有 40 字节 `LocalFrameHeader`，按 `msg_type` 分发：
- 61 (`MSG_TYPE_L2_TICK`) → tick 快照
- 62 (`MSG_TYPE_L2_ORDER`) → 逐笔委托
- 63 (`MSG_TYPE_L2_TRADE`) → 逐笔成交

**Journal 回放**（`tools/journal_replayer.cpp`）：mmap 读取 `deps/data/` 下的历史 journal → 跨频道排序 → 通过 Paged 的 JournalWriter 按时间回放写入。支持断点续写（通过 `extra_nano` 记录原始时间戳）。

**实时读取**（`tools/live_reader.cpp`）：通过 `JournalReader::createReaderWithSys()` 经 Paged 实时读取，按 msg_type 分发处理。

**交易所判断**：Symbol < 400000 → SZ（深圳），>= 400000 → SH（上海）。

## 关键约定

- `src/data_struct.hpp` 中所有结构体使用 `__attribute__((packed))`，不可添加 padding 或调整字段顺序
- Journal page header 56 字节，frame header 40 字节，在 `LocalJournalPage` 中硬编��
- JournalWriter name 不可超过 30 个字符
- Paged 服务支持多个 reader 同时连接，各自用不同 name 注册

## KungFu 版本

本项目使用 KungFu **v1.0.0**（taurus.ai）：
- **Git Tag**: `v1.0.0` — commit `ebbc9e21f3b2efad743c402f453d7487d8d3f961` (2018-06-15)
- **RPM**: `kungfu-0.0.5-20240912141305.x86_64` (built 2024-09-12)
- **GitHub**: https://github.com/kungfu-origin/kungfu/releases/tag/1.0.0
- **技术栈**: C++ / Python 2.7 / Boost.Python（v2.x 文档不适用）
- 验证命令: `rpm -qi kungfu`

## 依赖（Docker 环境）

CMakeLists.txt 当前配置为 Docker 路径：
- KungFu v1.0.0：`/opt/kungfu/master/`（头文件 `include/`，库 `lib/yijinjing/`）
- Boost 1.62.0：`/opt/kungfu/toolchain/boost-1.62.0/`
- Python 2.7（系统自带）
- Apache Arrow 21.0.0 + Parquet：`/usr/local/`（通过 `archive_journal/setup_parquet.sh` 离线编译安装）
- 本地第三方库在 `lib/` 目录：ACE 6.4.3、Protobuf 3.1.0、MDC Gateway Client

环境切换说明见上层 `streaming_factor/CLAUDE.md`。

## 子项目

### realtime_parquet/（实时行情 Parquet 落盘）

通过 Paged 实时读取 journal，按分钟输出 tick/order/trade Parquet 文件。每分钟一组文件，原子写入。分钟切换由 nano_timestamp 的 HHMM 变化触发。已在线上运行。

### realtime_kbar/（实时 1min K 线合成）

通过 Paged 实时读取 tick journal，合成聚宽口径 1min K 线，输出 CSV 文件。采用 sec01_flush + patch 模式发布，详见 `realtime_kbar/README.md`。

离线验证工具 `kbar_offline` 可与 JQ price_1m 逐 bar 对比，验证结果：OHLCV 全部 100% match。

### monitor_latency/（行情延迟监控）

监控 gateway 行情链路延迟。

### archive_journal/（Journal 转 Parquet 归档工具）

线上每日收盘后使用，读取当天的 journal 文件并转成 Parquet 格式存档。多线程并行读取，按 page number 数字排序保证输出有序。已在线上运行。

### realtime_factors/（实时因子计算，规划中）

从 L2 journal 实时计算量化因子。

### demo_project/（实习生面试项目）

自包含的面试考核项目，包含精简后的源码（无第三方 mdc_gateway 依赖）、回放工具、示例因子代码和 30 个因子题目。独立编译，不依赖父项目。
