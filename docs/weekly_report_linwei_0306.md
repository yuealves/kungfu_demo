# 周报 - linwei 2026.03.06

## 本周工作

### 1. L2 行情数据 Journal 写入/读取流程调研

线上环境没有任何文档，通过以下方式还原了完整的数据链路：

- **进程分析**：通过 `ps -ef` 梳理出线上 `insight_gateway`、`paged`（PageEngine）、`data_fetcher` 三个核心进程的启动参数和依赖关系
- **源码逆向**：从 `market_gateway` 历史代码中还原出 `insight_gateway` 的写入流程，以及 `data_fetcher` 的定时 dump 机制
- **KungFu Journal读写流程解析**：确认线上使用 KungFu v1.0.0（taurus.ai，2018 年），与 v2.x（pybind11/Electron）架构完全不同，网上大部分文档不适用。通过 `rpm -qi kungfu` 确认 RPM 版本和 git commit，根据对应版本的kungfu代码完成了journal读写流程的关键路径解析。

#### 核心发现

1. **数据通路**：`insight_gateway` 通过 `JournalWriter::write_frame()` 将 L2 数据（Tick 快照、逐笔委托、逐笔成交）写入 Paged 管理的 journal 文件（mmap 共享内存），三个频道分别为 `insight_stock_tick_data`、`insight_stock_order_data`、`insight_stock_trade_data`

2. **实时通知机制**：Journal 系统通过 mmap 共享内存 + volatile status 轮询实现纳秒级延迟的跨进程数据传递，不依赖任何 IPC 通知机制（socket/管道/信号量）。writer 写入 `status=WRITTEN` 后 reader 立即可见

3. **现有 data_fetcher 的设计问题**：线上 `data_fetcher` 用 `sleep_until` 每分钟批量提取一次数据写 CSV/Parquet，完全没有利用 journal 系统的实时流式读取能力（`createReaderWithSys`），把天然支持实时流的系统用成了分钟级定时批处理

4. **数据结构**：梳理了两层数据模型——KungFu 原始 packed 结构（`KyStdSnpType`/`KyStdOrderType`/`KyStdTradeType`）和转换后的 L2 标准结构（`L2StockTickDataField`/`SH_SZ_StockStepOrderField`/`SH_SZ_StockStepTradeField`），以及 `trans_tick()`/`trans_order()`/`trans_trade()` 转换函数

详细文档：`docs/L2行情数据与Journal系统.md`

### 2. 本地模拟环境搭建

基于调研结果，搭建了完整的本地模拟环境，可以在不依赖线上 `insight_gateway` 的情况下模拟实时行情写入和读取：

#### journal_replayer（回放写入端）

- 从线上下载了几十分钟的历史 journal 文件（`deps/new_journal_data/`，约 1.9GB，覆盖某日 09:10~09:42 约 1900 万帧数据）读取数据
- 用 mmap 加载所有 page 文件，3 个频道帧按 nano 时间戳全局排序合并
- 通过 `JournalWriter::create()` 经 Paged 按原始时间间隔写入，支持倍速回放（如 10x、100x）
- 支持断点续传：启动时扫描目标 journal 已写入的最后时间点（通过 `extra_nano` 字段记录源数据原始时间戳），跳过已写入部分继续回放，利用 `PageHeader.close_nano` 实现整页跳过优化

#### live_reader（实时读取端）

- 通过 `JournalReader::createReaderWithSys()` 连接 Paged，实时读取 replayer 写入的数据
- 按 `msg_type` 分发处理 Tick/Order/Trade，调用 `trans_tick()`/`trans_order()`/`trans_trade()` 转换为 L2 结构体
- 定期打印样本数据和统计信息（每 N 条打印详细数据，每 3 秒输出吞吐量统计）

#### 配套脚本

| 脚本 | 功能 |
|------|------|
| `scripts/start_paged.sh` | 启动 Paged 服务（���动清理残留、等待 socket 就绪） |
| `scripts/stop_paged.sh` | 停止 Paged |
| `scripts/start_replayer.sh` | 启动 replayer（支持倍速参数） |
| `scripts/start_reader.sh` | 启动 reader（支持打印间隔参数） |
| `scripts/run_all.sh` | 一键启动：编译 → Paged → Reader → Replayer，Ctrl+C 后自动清理 |

## 下周计划

- 基于 replayer/reader 模拟环境，开始实现 streaming 流式因子计算框架，先跑通最简单的count计数改为流式生成
- 韩老师提供的线上docker环境，目前解决丢包问题的新代码会实时写入的journal文件，但没有生成对应的parquet文件留档，需结合新代码完成该功能，提供给shenrun，验证新代码丢包问题是否解决。