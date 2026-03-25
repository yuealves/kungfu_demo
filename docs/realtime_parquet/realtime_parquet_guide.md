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
│  检测分钟切换 (nano_timestamp 变化):            │
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

### 基于 nano_timestamp 切分钟

分钟切换依据 **nano_timestamp**（journal 帧时间戳）而非 exchange_time：

- **线上**：`nano = frame->getNano()`（写入 journal 的墙上时间）
- **replay**：`nano = frame->getExtraNano()`（原始历史时间戳）

nano_timestamp 严格递增（单线程写入 journal 保证），不存在乱序问题。exchange_time 因交易所多 channel 并行分发 + gateway 多线程入队，跨 channel 会乱序（详见 `docs/行情数据链路与乱序分析.md`）。

文件名和目录日期均从 nano 提取。文件内容可能包含不同 exchange_time 的数据（如 SH 集合竞价 order 在 09:25 一次性到达，exchTime 横跨 09:15~09:24）。

### 基于特殊时间切分与系统强制落盘机制（2026.03 更新）

文件的生成时间与命名规则不再是简单的“每分钟切分一次”，而是基于量化研究需求的特殊业务规则进行分段，并且引入了**系统时间强制落盘机制**。

**特殊切分规则（纯 Nanotime 路由）：**
1. **盘前 (09:26 之前)**：所有 `nanotime < 09:26:00` 的早盘数据，落入 `0926.parquet`。
2. **集合竞价 (09:26 - 09:31)**：`09:26:00 <= nanotime < 09:31:00` 的数据，落入 `0931.parquet`。
3. **上午盘中 (09:31 - 11:31)**：正常每分钟切分（生成 `0932`, `0933` ... `1131.parquet`）。
4. **中午时段 (11:31 - 13:01)**：`11:31:00 <= nanotime < 13:01:00` 的数据（包含午休期间偶发的测试数据或延迟数据），统一合并落入 `1301.parquet`。
5. **下午盘中 (13:01 - 15:01)**：正常每分钟切分（生成 `1302` ... `1501.parquet`）。
6. **收盘后 (15:01 之后)**：恢复每分钟正常切分。

*稀疏特性：如果某一个时间段内没有任何数据到来，则该时间段对应的 parquet 文件不会被生成。这完美兼容了下游（如 Pandas/DolphinDB）按目录批量加载的使用习惯。*

**系统时间强制落盘 (System Time Crossing)：**
为了防止在业务边界时刻（如 `11:31:00`）市场行情出现绝对静默（没有任何数据到来）而导致当前文件一直卡在内存中无法生成，系统引入了兜底防线。
- 在 `09:26`, `09:31`, `11:31`, `13:01`, `15:01` 这五个关键节点，主程序会检测所在机器的**东八区真实系统时间**。
- 一旦系统时间**跨越**了上述时间点，不管此时有没有新数据到来，程序都会被强制唤醒，并把当前内存中积压的落后文件（如 `1131` 或更早的文件）强行 `flush` 到磁盘并关闭，准备好接收下一阶段的数据。
- 由于采用的是“跨越时检测”逻辑，在半夜跑历史回放测试时，由于初始系统时间（如 22:00）一开始就大于所有业务时间点，系统时钟强制落盘逻辑将处于完全静默状态，从而保证**离线回放 100% 依赖数据自身 nanotime 驱动而不受系统时钟干扰**。

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
# 线上测试时推荐：只处理新数据（跳过 journal 中已有的历史存量）
./build/realtime_parquet -o /data/rt_parquet -s now

# 从 journal 开头处理所有数据（线上部署 & 本地 replay 测试用）
./build/realtime_parquet -o /data/rt_parquet

# 指定 journal 目录
./build/realtime_parquet -o /data/rt_parquet -d /shared/kungfu/journal/user/
```

参数：
- `-o output_dir`：输出目录（默认 `./output`）
- `-d journal_dir`：journal 目录（默认 `/shared/kungfu/journal/user/`）
- `-s now`：从当前时间开始，跳过 journal 中的历史数据（线上测试时推荐）
- `-h`：显示帮助

> **线上测试时建议使用 `-s now`**：如果 gateway 已经运行了一段时间（journal 中有大量存量数据），不加 `-s now` 会全速追赶所有历史数据，瞬间占满 CPU 和磁盘 I/O。`-s now` 让 reader 的 `jumpStart` 跳到当前时间，只处理之后的新数据。

## 配合 replay 脚本测试

```bash
# 1. 编译（在 realtime_parquet/ 目录下）
cd kungfu_demo/realtime_parquet
mkdir -p build && cd build && cmake .. && make -j$(nproc)

# 2. 启动 replay（在 kungfu_demo/ 目录下）
cd kungfu_demo
./scripts/replay start 5              # 5 倍速，断点续播
# 或
./scripts/replay start --reset 5      # 从头开始

# 3. 等几秒让 replay 开始写入数据，再启动 realtime_parquet
sleep 3
./realtime_parquet/build/realtime_parquet -o /tmp/rt_parquet

# 4. 观察输出，关注延迟指标
# [dir] /tmp/rt_parquet/20260304                                          ← 目录名是数据的实际日期
# [flush] tick 0930 rows=89157 last=300118 nano=09:30:59.999 exch=09:30:51.000 gw=8999ms disk=35ms
# [flush] tick 0931 rows=95445 last=2757   nano=09:31:59.979 exch=09:31:45.000 gw=14979ms disk=28ms

# 5. Ctrl+C 停止 realtime_parquet

# 6. 检查输出文件（注意目录名是数据日期，不是今天日期）
ls /tmp/rt_parquet/

# 7. 用 Python 验证 parquet 可读
python3 -c "
import pyarrow.parquet as pq, os
data_dir = os.listdir('/tmp/rt_parquet/')[0]
t = pq.read_table(f'/tmp/rt_parquet/{data_dir}/tick_0930.parquet')
print(t.schema)
print(f'rows: {len(t)}')
"

# 8. 停止 replay
./scripts/replay stop
```

> **注意**：replay 模式下目录名是历史数据的日期（如 `20260304`），不是本机当前日期。
> 这是因为分钟切换和日期都从 `extra_nano`（原始 journal 时间戳）提取。

## 打包部署

本项目设计为自包含，可以打包为 tar.gz 直接上传到线上编译。

### 打包命令

```bash
# 在 kungfu_demo/ 目录下执行
# -h 参数解引用符号链接，将实际文件打包进去
tar -czhf realtime_parquet.tar.gz \
    --exclude='realtime_parquet/build' \
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

# 5. 运行（线上推荐 -s now 跳过存量数据）
./realtime_parquet -o /data/rt_parquet -s now
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

- YYYYMMDD：从 nano_timestamp 提取的日期（线上=当天，replay=历史数据日期）
- HHMM：从 nano_timestamp 提取的分钟（如 0930 = 数据在 09:30 到达）

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

### 压缩与线程

- 算法：Snappy
- Row Group 大小：50000 行
- Arrow writer 开启多线程列写入 (`use_threads=true`)

**线程模型**：主循环是单线程（读 journal → 分发 → append → 分钟切换）。唯一的多线程来自 Arrow 内部——`use_threads=true` 让 `WriteTable` 时 Arrow 用全局线程池并行编码/压缩不同列。**默认线程池大小 = 4**（启动时通过 `arrow::SetCpuThreadPoolCapacity` 设置），仅在写 Row Group 的瞬间（几毫秒）占用，不是持续占用。如需调整，可通过环境变量覆盖：

```bash
OMP_NUM_THREADS=2 ./realtime_parquet -o /data/rt_parquet -s now  # 更保守
OMP_NUM_THREADS=8 ./realtime_parquet -o /data/rt_parquet -s now  # 更激进
```

## 实时读取 Parquet 文件

### 文件完整性标记

写入中的文件使用 **dot 前缀**标记未完成状态：

```
output_dir/20260304/
  ├── tick_0930.parquet       ← 已完成，可安全读取
  ├── order_0930.parquet      ← 已完成
  ├── .tick_0931.parquet      ← 正在写入，不要读取
  └── .order_0931.parquet     ← 正在写入
```

写入流程：`open(.tick_0931.parquet)` → 写 Row Group → ... → `close()` 写 footer → `rename(.tick_0931.parquet, tick_0931.parquet)`。

`rename()` 在同一文件系统上是原子操作（inode 指针更新），耗时微秒级，不引入额外延迟。

### Python 端使用

消费者只需 glob 无 dot 前缀的文件，天然跳过未完成文件：

```python
import glob, pyarrow.parquet as pq

# 只匹配已完成的文件（dot 前缀的不会被 glob 匹配）
for f in sorted(glob.glob('/data/rt_parquet/20260304/tick_*.parquet')):
    t = pq.read_table(f)
    ...
```

### 异常退出

如果进程被 `kill -9` 强杀，当前分钟的 `.tick_HHMM.parquet` 会残留在磁盘上（无 footer，不完整）。正常 Ctrl+C（SIGINT/SIGTERM）会触发 `close()` + `rename()`，不会残留。

## 延迟指标

每次分钟切换时打印两层延迟和最后一条数据的详情：

```
[flush] tick 0930 rows=89157 last=300118 nano=09:30:59.999 exch=09:30:51.000 gw=8999ms disk=35ms
```

各字段含义：

| 字段 | 含义 |
|------|------|
| `rows` | 该分钟写入的总行数 |
| `last` | 最后一条数据的股票代码 |
| `nano` | 最后一条数据的 nano_timestamp（HH:MM:SS.mmm） |
| `exch` | 最后一条数据的 exchange_time（HH:MM:SS.mmm） |
| `gw` | gateway 延迟 = nano_timestamp - exchange_time |
| `disk` | 落盘延迟 = parquet close 完成时的墙上时间 - 读到最后一条数据的墙上时间 |

### 两层延迟的含义

**gw（gateway 延迟）**：从交易所产生数据到 gateway 写�� journal 的延迟。包含 Insight SDK 接收 → protobuf 解析 → ConcurrentQueue → writer 线程 → write_frame 的全链路。线上和 replay 模式下都有意义（replay 保留了原始的 nano 和 exchange_time 关系）。

典型观察：
- 连续竞价时 tick 的 gw ��常 < 100ms（负值正常，说明 nano 在 exch_time 之前）
- 集合竞价时 order 的 gw 可达数分钟（SH 在 09:25 一次性下发 09:15~09:24 的 order）
- gw 为负值表示 nano < exch_time：数据在交易所标记时间之前就已到达 gateway

**disk（落盘延迟）**：从 realtime_parquet 读到数据到 parquet 文件 close 完成的延迟。

| 模式 | disk 的含义 |
|------|------------|
| 实盘 | journal 写入 → parquet 可读的延迟（因为读到数据 ≈ 数据写入时刻） |
| replay | realtime_parquet 实际处理延迟（读到数据 → parquet close） |

典型值：< 50ms（Row Group 预压缩的功劳，close 时只需 flush 最后一批 + 写 footer）。

## 依赖汇总

| 依赖 | 来源 | 说明 |
|------|------|------|
| Apache Arrow 21.0.0 | third_party/ | 通过 setup_parquet.sh 编译安装到 /usr/local |
| Snappy, Thrift, zlib, etc. | third_party/ | Arrow 的 bundled 依赖 |
| KungFu v1.0.0 headers | deps/kungfu_include/ | JournalReader API |
| KungFu yijinjing libs | deps/kungfu_yijinjing_lib/ | libjournal.so |
| Boost 1.62.0 | /opt/kungfu/toolchain/ | Docker 环境已有 |
| Python 2.7 | 系统 | KungFu 依赖 |
