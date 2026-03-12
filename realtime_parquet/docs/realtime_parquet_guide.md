# realtime_parquet 使用指南

实时行情数据按分钟写 Parquet。从 Paged 服务实时读取 L2 行情 journal，每分钟写一组 Parquet 文件（tick/order/trade 各一个）。

## 架构

```
Paged (mmap)
   │
   ▼
┌─────────────────────────────────────────────┐
│  realtime_parquet (单线程主循环)              │
│                                              │
│  reader->getNextFrame()                      │
│      │                                       │
│      ├─ msg_type=61 → tick_writer.append()   │
│      ├─ msg_type=62 → order_writer.append()  │
│      └─ msg_type=63 → trade_writer.append()  │
│                                              │
│  每个 writer 内部:                            │
│    buffer → 满 ROW_GROUP_SIZE → WriteTable    │
│    (Row Group 预压缩，不等分钟边界)            │
│                                              │
│  检测分钟切换 (exchange_time 变化):            │
│    flush 残余 buffer → Close → 打印延迟       │
│    Open 新文件 → 继续                         │
└─────────────────────────────────────────────┘
        │
        ▼
output_dir/YYYYMMDD/
  ├── tick_0930.parquet
  ├── order_0930.parquet
  ├── trade_0930.parquet
  ├── tick_0931.parquet
  └── ...
```

## 核心设计

### Row Group 预压缩

数据到达时立即 append 到 Arrow builders。当累积行数达到 `ROW_GROUP_SIZE`（50000）时自动触发 Row Group 写入（Snappy 压缩在此发生），然后重建 builders 继续接收数据。

到分钟边界时只需 flush 最后一批不完整的 Row Group + 写 footer + close 文件，延迟极低（通常 < 20ms）。

### 独立通道 + 只向前推进

tick/order/trade 三个通道各自独立追踪分钟。从 Paged 读取时三种消息交织到达，exchange_time 可能跨分钟乱序（尤其是 order 数据，集合竞价 0915 的委托散布在整段开盘前数据中）。

每个通道只在 exchange_time 的分钟**向前推进**时才触发文件切换，回退的旧时间数据直接写入当前文件。这避免了频繁 close/open 导致的性能问题和大量碎片文件。

## 首次部署

### 1. 安装 Arrow + Parquet

```bash
# 使用自包含的 setup 脚本（从 third_party/ 离线编译 Arrow 21.0.0）
cd /path/to/realtime_parquet
bash setup_parquet.sh
```

> setup_parquet.sh 和 third_party/ 是指向 archive_journal_demo 的符号链接。
> 在 tar.gz 包中它们是实际文件（打包时解引用了符号链接）。

### 2. 编译

```bash
cd /path/to/realtime_parquet
mkdir -p build && cd build
cmake .. && make -j$(nproc)
```

产物：`build/realtime_parquet`

### 3. 运行

```bash
# 线上（Paged 已在运行，journal 在默认路径）
./build/realtime_parquet -o /data/rt_parquet

# 指定 journal 目录
./build/realtime_parquet -o /data/rt_parquet -d /shared/kungfu/journal/user/
```

参数：
- `-o output_dir`：输出目录（默认 `./output`）
- `-d journal_dir`：journal 目录（默认 `/shared/kungfu/journal/user/`）
- `-h`：显示帮助

## 配合 replay 脚本测试

```bash
# 1. 编译
cd kungfu_demo/realtime_parquet
mkdir -p build && cd build && cmake .. && make -j$(nproc)

# 2. 启动 replay 环境（在 kungfu_demo/ 目录下）
cd kungfu_demo
./scripts/replay start --reset 10

# 3. 运行 realtime_parquet
./realtime_parquet/build/realtime_parquet -o /tmp/rt_parquet

# 4. 观察输出，关注 flush 延迟
# [flush] 0930 latency=12ms tick=92341 order=958123 trade=769045
# [flush] 0931 latency=8ms  tick=91200 order=945000 trade=760000

# 5. Ctrl+C 停止 realtime_parquet

# 6. 检查输出文件
ls /tmp/rt_parquet/$(date +%Y%m%d)/

# 7. 用 Python 验证 parquet 可读
python3 -c "
import pyarrow.parquet as pq
t = pq.read_table('/tmp/rt_parquet/$(date +%Y%m%d)/tick_0930.parquet')
print(t.schema)
print(f'rows: {len(t)}')
"

# 8. 停止 replay
./scripts/replay stop
```

## 打包部署

本项���设计为自包含，可以打包为 tar.gz 直接上传到线上编译。

### 打包命令

```bash
# 在 kungfu_demo/ 目录下执行
# -h 参数解引用符号链接，将实际文件打包进去
tar -czhf realtime_parquet.tar.gz \
    -C kungfu_demo \
    realtime_parquet/
```

> `-h`（`--dereference`）是关键：它将符号链接替换为实际文件，使得 tar.gz 包完全自包含。

包内包含：
- `src/` — 源代码
- `CMakeLists.txt` — 构建配置
- `setup_parquet.sh` — Arrow/Parquet 离线编译脚本（原为符号链接 → archive_journal_demo）
- `third_party/` — Arrow 21.0.0 源码 + 依赖（原为符号链接 → archive_journal_demo）
- `deps/kungfu_include/` — KungFu v1.0.0 头文件（原为符号链接 → 父项目 deps/）
- `deps/kungfu_yijinjing_lib/` — KungFu yijinjing 库（原为符号链接 → 父项目 deps/）
- `docs/` — 文档

### 线上部署流程

```bash
# 1. 上传 tar.gz 到线上机器
scp realtime_parquet.tar.gz user@server:/path/to/

# 2. 解压
cd /path/to/
tar xzf realtime_parquet.tar.gz
cd realtime_parquet

# 3. 首次：安装 Arrow（只需一次）
bash setup_parquet.sh

# 4. 编译
mkdir -p build && cd build
cmake .. && make -j$(nproc)

# 5. 运行
./realtime_parquet -o /data/rt_parquet
```

## 输出文件格式

### 目录结构

```
output_dir/
└── YYYYMMDD/
    ├── tick_HHMM.parquet
    ├── order_HHMM.parquet
    └── trade_HHMM.parquet
```

- YYYYMMDD：程序运行时的日期
- HHMM：从 exchange_time 提取的分钟（如 0930 = 09:30）

### Parquet schema

与 archive_journal_demo 生成的 Parquet 文件格式完全一致：

**Tick (64 列)**
- `nano_timestamp` (int64) — 纳秒时间戳（replay 时取 extra_nano，线上取 frame nano）
- `exchange` (utf8) — 交易所（"SZ"/"SH"）
- `Symbol` (int32), `Time` (int32), `AccTurnover` (int64), `AccVolume` (int64), ...
- `AskPx1`..`AskPx10` (float32), `AskVol1`..`AskVol10` (float64)
- `BidPx1`..`BidPx10` (float32), `BidVol1`..`BidVol10` (float64)
- `High`, `Low`, `Open`, `PreClose`, `Price` (float32)
- `Volume` (int32), `BizIndex` (int32), ...

**Order (12 列)**
- `nano_timestamp`, `exchange`, `Symbol`, `BizIndex`, `Channel` (int64)
- `FunctionCode` (int8), `OrderKind` (int8)
- `OrderNumber`, `OrderOriNo`, `Time`, `Volume` (int32), `Price` (float32)

**Trade (14 列)**
- `nano_timestamp`, `exchange`, `Symbol`, `AskOrder`, `BidOrder` (int32)
- `BSFlag`, `FunctionCode`, `OrderKind` (int8)
- `BizIndex`, `Channel`, `Index`, `Time`, `Volume` (int32), `Price` (float32)

### 压缩
- 算法：Snappy
- Row Group 大小：50000 行
- Arrow writer 开启多线程列写入 (`use_threads=true`)

## 延迟指标

每次分钟切换时打印 flush 延迟：

```
[flush] 0930 latency=12ms tick=92341 order=958123 trade=769045
```

- `latency`：close 3 个文件的总耗时（包含 flush 残余 + 写 footer + fsync）
- 由于 Row Group 预压缩，大部分数据在分钟内已经写出，flush 时只需处理最后不完整的 batch
- 典型延迟：< 20ms

## 依赖汇总

| 依赖 | 来源 | 说明 |
|------|------|------|
| Apache Arrow 21.0.0 | third_party/ | 通过 setup_parquet.sh 编译安装到 /usr/local |
| Snappy, Thrift, zlib, etc. | third_party/ | Arrow 的 bundled 依赖 |
| KungFu v1.0.0 headers | deps/kungfu_include/ | JournalReader API |
| KungFu yijinjing libs | deps/kungfu_yijinjing_lib/ | libjournal.so |
| Boost 1.62.0 | /opt/kungfu/toolchain/ | Docker 环境已有 |
| Python 2.7 | 系统 | KungFu 依赖 |
