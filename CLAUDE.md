# CLAUDE.md

本文件为 Claude Code 提供项目上下文。

## 项目背景

本项目属于盖亚青柯量化私募的流式因子计算系统。

**线上架构**：托管机上运行 `market_gateway`（代码位于 `ht_insight_market_gateway/insight_market_gateway/`），负责：
1. 读取华泰 Insight 实时 L2 行情数据
2. 转换为 KungFu journal 格式写入 journal 文件
3. 通过 KungFu yjj 启动 Paged 服务，允许其他进程以 mmap 方式实时读取 journal
4. 目前还运行了一个 `data_fetcher` 从 journal 中导出 CSV（该模块有 bug，后续会替换）

**长期目标**：将公司量化研究员所需的因子全部改为 C++ 流式实时计算，直接从 L2 journal 行情数据中计算得到。

**本地模拟环境**：`journal_replayer` 读取历史 journal 数据，通过 Paged 回放，模拟线上 `market_gateway` 的写入行为；`live_reader` 通过 Paged 实时读取，模拟下游消费者。

关于 KungFu journal 系统的详细说明见 `docs/journal_data_write_read.md`。

## 编译

```bash
cd build && cmake .. && make
```

产物：`libinsight_handle.so`（工具函数 + 数据类型）、`libconsumer.so`（journal 读取器）、`data_consumer`（可执行文件）、`journal_replayer`（回放工具）、`live_reader`（实时读取工具）。

C++17，`-O3 -Wall -fPIC`，CMake >= 3.18。

## 运行

```bash
cd build && ./data_consumer
```

journal 文件路径由 CMake 宏 `PROJECT_ROOT_DIR` 设定，在 `src/data_consumer.h` 中引用。如需更换数据，修改 `DataConsumer` 的 `path` 成员。

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

底层脚本（`replay` 内部调用，一般不需要直接使用）：

```bash
scripts/start_paged.sh      # 单独启动 Paged
scripts/stop_paged.sh       # 停止 Paged
scripts/start_replayer.sh   # 前台启动 replayer
scripts/start_reader.sh     # 前台启动 reader
scripts/run_all.sh          # 一键前台启动 Paged → reader → replayer
```

## 架构

两层数据模型 + journal 读取器按消息类型分发：

**第一层 — KungFu 原始结构体**（`src/data_struct.hpp`）：`KyStdSnpType`、`KyStdOrderType`、`KyStdTradeType`，`__attribute__((packed))` 二进制布局，直接对应 journal frame payload。

**第二层 — L2 输出结构体**（`src/insight_types.h`）：`L2StockTickDataField`、`SH/SZ_StockStepOrderField`、`SH/SZ_StockStepTradeField`，提供 `to_string()` 和 `to_csv_row()` 方法。

**转换函数**（`src/insight_types.cpp`）：`trans_tick()`、`trans_order()`、`trans_trade()` 将第一层转为第二层。Order 和 Trade 根据交易所返回 `std::variant`（SZ/SH）。

**Journal 读取**（`src/data_consumer.cpp`）：`LocalJournalPage` 使用 `mmap()` 读取 `yjj.<channel>.<page>.journal` 文件。Frame 有 40 字节 `LocalFrameHeader`，按 `msg_type` 分发：
- 61 (`MSG_TYPE_L2_TICK`) → `on_market_data()`
- 62 (`MSG_TYPE_L2_ORDER`) → `on_order_data()`
- 63 (`MSG_TYPE_L2_TRADE`) → `on_trade_data()`

**两个消费者类**（`src/data_consumer.h`）：
- `DataConsumer`：遍历所有 frame，调用虚函数 handler，入口为 `run()`
- `DataFetcher`（继承 DataConsumer）：提供 `get_tick_data()`、`get_sh/sz_order_data()`、`get_sh/sz_trade_data()`，按时间范围过滤返回 vector，使用 KungFu 的 `JournalReader` API

**Journal 回放**（`tools/journal_replayer.cpp`）：mmap 读取历史 journal → 跨频道排序 → 通过 Paged 的 JournalWriter 按时间回放写入。支持断点续写（通过 `extra_nano` 记录原始时间戳）。

**实时读取**（`tools/live_reader.cpp`）：通过 `JournalReader::createReaderWithSys()` 经 Paged 实时读取，按 msg_type 分发处理。

**交易所判断**（`src/utils.cpp`）：symbol < 400000 → SZ（深圳），>= 400000 → SH（上海）。

## 关键约定

- `src/data_struct.hpp` 中所有结构体使用 `__attribute__((packed))`，不可添加 padding 或调整字段顺序
- `main()` 位于 `src/data_consumer.cpp` 末尾
- Journal page header 56 字节，frame header 40 字节，在 `LocalJournalPage` 中硬编码
- `parse_nano()` 将纳秒时间戳转换为 `HHmmSSmm` 整数格式
- JournalWriter name 不可超过 30 个字符

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
- 本地第三方库在 `lib/` 目录：ACE 6.4.3、Protobuf 3.1.0、MDC Gateway Client

环境切换说明见上层 `streaming_factor/CLAUDE.md`。

## 子项目

### archive_journal_demo/（Journal 转 Parquet 归档工具）

线上每日收盘后使用，读取当天的 journal 文件并转成 Parquet 格式存档。当前已在线上运行。

### demo_project/（实习生面试项目）

自包含的面试考核项目，包含精简后的源码（无第三方 mdc_gateway 依赖）、回放工具、示例因子代码和 30 个因子题目。独立编译，不依赖父项目。
