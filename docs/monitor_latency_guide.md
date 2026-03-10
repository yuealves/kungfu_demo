# monitor_latency 使用指南

## 简介

`monitor_latency` 是一个行情延迟监控进程，持续读取 tick/order/trade 三个频道的最新 journal 时间戳和交易所时间戳，追加写入 CSV 监控文件，供公司 IT 从外网定期检查行情延迟。

## 编译

```bash
cd monitor_latency/build && cmake .. && make
```

产物：`monitor_latency` 可执行文件。

## 命令行参数

```
./monitor_latency -o <output_dir> [-i <seconds>] [-d <journal_dir>]
```

| 参数 | 必选 | 说明 |
|------|------|------|
| `-o <dir>` | 是 | 输出目录，自动生成 `latency_YYYYMMDD.csv` 文件名（东八区日期），每天一个文件 |
| `-i <seconds>` | 否 | 写文件间隔，默认 1 秒 |
| `-d <journal_dir>` | 否 | journal 目录，默认 `/shared/kungfu/journal/user/`，通常无需指定 |

## 输出格式

CSV 追加写入，有新数据时每个周期追加 3 行（tick/order/trade 各一行），无新数据不写入：

```csv
write_time,monitor_item,journal_time,exchange_time
2026-03-10 09:30:15.123,tick,2026-03-10 09:30:14.456,09:30:15.230
2026-03-10 09:30:15.123,order,2026-03-10 09:30:14.457,09:30:15.231
2026-03-10 09:30:15.123,trade,2026-03-10 09:30:14.458,09:30:15.232
```

| 列 | 说明 |
|----|------|
| `write_time` | 写入时刻，东八区可读格式 `YYYY-MM-DD HH:MM:SS.mmm` |
| `monitor_item` | 频道类型：tick / order / trade |
| `journal_time` | 该频道最新 frame 的写入时间（同上格式） |
| `exchange_time` | 该频道最新数据的交易所时间 `HH:MM:SS.mmm` |

## 线上部署

Paged 和 market_gateway 已在运行的情况下，直接启动即可：

```bash
cd monitor_latency
./scripts/start_monitor.sh -o /path/to/output/
```

会自动生成 `/path/to/output/latency_20260310.csv` 这样的文件。

## 本地模拟测试

本地没有线上行情，需要用 Paged + replayer 模拟。开 3 个终端：

### 终端 1：启动 Paged 服务

```bash
cd /workspace/valerie/streaming_factor/kungfu_demo
./scripts/start_paged.sh
```

看到 `started (pid=xxx)` 即可。Paged 在后台运行。

### 终端 2：启动 Replayer（模拟 market_gateway 写行情）

```bash
cd /workspace/valerie/streaming_factor/kungfu_demo
./scripts/start_replayer.sh 10    # 10 倍速回放，前台运行
```

从本地历史 journal 数据读取，通过 Paged 按时间顺序写入，模拟线上行情写入。

### 终端 3：启动 monitor_latency

```bash
cd /workspace/valerie/streaming_factor/kungfu_demo/monitor_latency
./scripts/start_monitor.sh -o /tmp -i 2
```

### 验证

```bash
head -20 /tmp/latency_$(date +%Y%m%d).csv
```

### 停止

1. 终端 3：`Ctrl+C` 停 monitor_latency
2. 终端 2：`Ctrl+C` 停 replayer
3. 停 Paged：`./scripts/stop_paged.sh`
