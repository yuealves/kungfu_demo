# replay 管理脚本使用指南

`scripts/replay` 是 replayer + Paged 的生命周期管理工具，简化本地流式因子开发调试的工作流。

## 前置条件

- Docker 容器环境（KungFu v1.0.0 已安装）
- `build/journal_replayer` 已编译（`cd build && cmake .. && make journal_replayer`）
- `deps/data/` 下有历史 journal 数据，或某个交易日目录下有 archive 输出的 parquet 分片

## 命令一览

| 命令 | 说明 |
|------|------|
| `./scripts/replay start [--reset] [--source journal|parquet] [--date-dir PATH] [speed]` | 启动 Paged + replayer 后台运行 |
| `./scripts/replay stop` | 停止 replayer（保留 Paged） |
| `./scripts/replay status` | 查看 Paged 和 replayer 运行状态 |
| `./scripts/replay clean` | 停止所有服务 + 清理 journal 文件 |

## 典型工作流

### 日常开发调试

```bash
# 1. 启动回放环境（10 倍速）
./scripts/replay start 10

# 2. 运行你的因子代码
./build/my_factor

# 3. Ctrl+C 退出，修改代码，重新编译
cd build && make my_factor

# 4. 再次运行，不需要重启 replayer
./build/my_factor

# 5. 收工，停掉 replayer
./scripts/replay stop
```

整个过程中 replayer 在后台持续运行，不受因子进程启停影响。Paged 也保持运行，reader 随时可重连。

### 断点续播

```bash
# 第一次跑��一部分
./scripts/replay start 10
# ... 过了一会儿 ...
./scripts/replay stop          # 停掉 replayer，进度保留

# 下次启动，自动从断点继续
./scripts/replay start 10      # 日志中会显示 skipped N frames
```

replayer 通过扫描目标 journal 的 `extra_nano` 字段自动跳过已写入的帧，脚本无需额外处理。

### 从头开始

```bash
./scripts/replay start --reset 10
```

`--reset` 会在启动前删除 `/shared/kungfu/journal/user/yjj.insight_stock_*`，使 replayer 从第一帧开始写入。

### 从 parquet 日期目录回放

```bash
./scripts/replay start --source parquet --date-dir /data/dump_parquet/2026-03-26 10
./scripts/replay start --reset --source parquet --date-dir /data/dump_parquet/2026-03-26 10
```

说明：

- `--source parquet`：切换为 parquet 数据源模式
- `--date-dir`：指定 parquet 所在目录。既可以是某个交易日目录，也可以直接是放置这批 parquet 文件的目录；目录内应包含 `*_tick_data_*.parquet`、`*_order_data_*.parquet`、`*_trade_data_*.parquet`
- parquet 回放时使用 `nano_timestamp` 作为原始时间轴，并写入目标 journal 的 `extra_nano`
- 因此下游仍应优先读取 `extra_nano`，其语义与现有 journal replay 完全一致
- parquet 模式会先同步打开每个频道的首个可用文件，然后后台线程继续预取后续文件，因此通常会比一次性全量加载更早看到 `[replay] starting`
- 回放期间会持续打印文件进度日志，例如 `[parquet] tick files: N`、`[parquet] open tick file 1/N: ...`、`[parquet] finish tick file 1/N`

### 完全清理

```bash
./scripts/replay clean
```

停止 replayer + 停止 Paged + 清理目标 journal 文件。适用于切换数据源或排查问题时需要干净环境的场景。

## 各命令详解

### `replay start [--reset] [--source journal|parquet] [--date-dir PATH] [speed]`

1. 检查 replayer 是否已在运行，是则报错退出（避免重复启动）
2. 检查 Paged 是否运行，未运行则调用 `start_paged.sh` 启动
3. 若指定 `--reset`，删除目标 journal 文件
4. 按参数选择 journal 或 parquet 数据源，以 `nohup` 后台启动 `journal_replayer`，PID 写入 `build/replayer.pid`，日志输出到 `build/replayer.log`

其中 parquet 模式的启动行为是：
- 先统计 tick/order/trade 三类 parquet 文件数量
- 每个频道同步加载首个可回放文件，满足后立即进入 replay 主循环
- 后续 parquet 分片由后台预取，当前文件读完时继续打印 `finish/open` 日志并切换到下一文件

参数：
- `--reset`：可选，从头开始回放
- `--source`：可选，`journal` 或 `parquet`，默认 `journal`
- `--date-dir`：`--source parquet` 时必填，指定 parquet 日期目录
- `speed`：可选，回放倍速，默认 1（实时速度）

输出示例：
```
[replay] Paged already running (pid=12345)
[replay] resuming from last position (use --reset to start over)
[replay] replayer started (pid=12350, source=parquet, speed=10x)
[replay] parquet dir: /data/dump_parquet/2026-03-26
[replay] log: /workspace/.../build/replayer.log
[parquet] tick files: 120
[parquet] open tick file 1/120: /data/dump_parquet/2026-03-26/20260326_tick_data_001.parquet
[replay] starting (09:30:00)...
[parquet] finish tick file 1/120
[parquet] open tick file 2/120: /data/dump_parquet/2026-03-26/20260326_tick_data_002.parquet
```

### `replay stop`

停止 replayer 进程（先 `SIGTERM`，3 秒后仍存活则 `SIGKILL`）。**不停止 Paged**，reader 仍可重连。

### `replay status`

显示 Paged 和 replayer 的运行状态，并输出 `replayer.log` 最后 10 行。

输出示例：
```
=== Paged ===
  running (pid=12345)

=== Replayer ===
  running (pid=12350)

=== Last 10 lines of replayer.log ===
[replayer] writing tick frame ...
```

### `replay clean`

依次执行：
1. 停��� replayer
2. 调用 `stop_paged.sh` 停止 Paged
3. 删除 `/shared/kungfu/journal/user/yjj.insight_stock_*`

## 回放 journal 的时间戳说明

replayer 写入目标 journal 时，frame 中有三个时间相关字段，含义各不相同：

| 字段 | 位置 | 回放后的值 | 说明 |
|------|------|-----------|------|
| exchange time（如 `nTime`） | payload 内部 | **与原始一致** | 交易所行情时间，payload 原封不动 memcpy |
| `extra_nano` | frame header | **与原始一致** | replayer 将源 frame 的 `nano` 写入此字段 |
| `nano` | frame header | **回放时墙钟时间** | `JournalWriter` 内部调用 `getNanoTime()` 设置 |

源码参考：`JournalWriter::write_frame_full()`（`JournalWriter.cpp:70-72`）

```cpp
frame.setExtraNano(extraNano);   // extraNano = 源 frame 的原始 nano
long nano = getNanoTime();        // 当前墙钟时间
frame.setNano(nano);
```

**因子代码应使用 `extra_nano` 或 payload 内的交易所时间，不要使用 frame header 的 `nano`。** `nano` 是回放时刻的本机时间，与原始行情时间无关。

## 文件位置

| 文件 | 说明 |
|------|------|
| `build/replayer.pid` | replayer 进程 PID |
| `build/replayer.log` | replayer 标准输出和错误输出 |
| `/shared/kungfu/pid/paged.pid` | Paged 进程 PID |
| `/shared/kungfu/log/paged_stdout.log` | Paged 日志 |

## 常见问题

**Q: `replay start` 报 "replayer already running"？**

说明上次的 replayer 还在跑。先 `replay stop` 再重新 `start`。

**Q: `replay start` 后因子程序报连不上 Paged？**

运行 `replay status` 检查 Paged 是否正常。若 Paged 未运行，用 `replay clean` 清理后重新 `start`。

**Q: 续播时 replayer 立刻退出？**

说明所有帧都已写入完毕。用 `--reset` 重头开始，或换一份新的历史数据。

**Q: 想换一份历史数据？**

```bash
./scripts/replay clean
# 替换 deps/data/ 下的 journal 文件
./scripts/replay start 10
```

如果是 parquet：

```bash
./scripts/replay clean
./scripts/replay start --source parquet --date-dir /new/date/dir 10
```
