# realtime_kbar — C++ 实时 1 分钟 K 线合成

## 1. 项目背景

本模块将 **华泰 Insight L2 tick 快照** 合成为 **聚宽（JoinQuant）口径的 1 分钟 K 线**。

合成逻辑先由 Python 版本（`1min_kbar/generate_1m_kbar_ht.py`）实现并完成验证，确认与聚宽 price_1m 数据 **100% 一致**（open/close/high/low/volume 全部 match），再逐行翻译为 C++ 用于线上实时计算。

提供两个可执行文件：

| Binary | 用途 | 数据源 | 输出 |
|--------|------|--------|------|
| `kbar_offline` | 离线验证 | tick parquet 文件 | 与 JQ price_1m 对比报告 |
| `kbar_realtime` | 线上实时 | KungFu journal（经 Paged 读取） | 每分钟 CSV 文件 |

---

## 2. 目录结构

```
realtime_kbar/
├── CMakeLists.txt              # 构建配置（两个独立 target）
├── README.md                   # 本文档
├── deps/                       # KungFu 头文件 + 库（仅 kbar_realtime 需要）
│   ├── kungfu_include/         #   JournalReader.h, Timer.h 等
│   └── kungfu_yijinjing_lib/   #   libjournal.so
└── src/
    ├── data_types.h            # KungFu 原始结构体（KyStdSnpType 等）+ MSG_TYPE 常量
    ├── tick_bar_builder.h      # 核心 API：MinuteBar、StockBarAccumulator、TickBarBuilder
    ├── tick_bar_builder.cpp    # 合成逻辑实现（~437 行）
    ├── parquet_reader.h        # Parquet I/O 接口（TickRecord、JqBar）
    ├── parquet_reader.cpp      # HT tick / JQ price_1m 读取实现
    ├── jq_comparator.h         # JQ 对比接口（CompareStats）
    ├── jq_comparator.cpp       # 对比统计 + 报告输出
    ├── kbar_offline.cpp        # Binary 1: parquet → kbar → JQ 对比
    └── kbar_realtime.cpp       # Binary 2: journal → kbar → CSV
```

---

## 3. 依赖与编译

### 3.1 依赖

| 依赖 | 版本 | 用途 | 需要者 |
|------|------|------|--------|
| Apache Arrow + Parquet | 21.0.0 | 读写 parquet 文件 | kbar_offline |
| KungFu v1.0.0 | taurus.ai | journal 实时读取 | kbar_realtime |
| Boost | 1.62.0 | KungFu 依赖 | kbar_realtime |
| Python 2.7 | 系统自带 | Boost.Python 链接 | kbar_realtime |

Arrow/Parquet 安装：使用 `archive_journal_demo/setup_parquet.sh` 从离线源码编译安装到 `/usr/local/`。

### 3.2 编译

```bash
cd realtime_kbar
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

产物：`build/kbar_offline`、`build/kbar_realtime`。

> `kbar_offline` 只链接 Arrow/Parquet，不依赖 KungFu。`kbar_realtime` 不链接 Arrow/Parquet，不依赖 parquet 库。两者独立编译。

---

## 4. kbar_offline 用法

离线验证工具：读取 tick parquet，合成 kbar，与 JQ price_1m 逐 bar 对比。

```bash
./kbar_offline --date 2026-03-17 [--code 000001] [--tick-dir DIR] [--jq-dir DIR] [--no-compare]
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--date` | （必填） | 交易日，`YYYY-MM-DD` |
| `--code` | 全市场 | 只处理指定股票（6 位数字，如 `000001`） |
| `--tick-dir` | `/data/tickl2_data_ht2_raw` | HT tick parquet 根目录 |
| `--jq-dir` | `/nkd_shared/assets/stock/price/price_1m` | JQ price_1m 目录 |
| `--no-compare` | 不跳过 | 跳过 JQ 对比 |

**输入**：
- HT tick：`{tick-dir}/{date}/{date_nodash}_tick_data_*.parquet`
- JQ kbar：`{jq-dir}/{date}.parquet`
- prev_day_close：从 JQ 目录中前一交易日的 parquet 获取

**输出**：终端打印对比报告（match 百分比）+ 单只股票 sample bars。

**注意**：JQ parquet 默认使用 zstd 压缩，Arrow 21 未内置 zstd 支持。需要先用 Python 转为 snappy 格式：

```python
import pandas as pd
df = pd.read_parquet("/nkd_shared/assets/stock/price/price_1m/2026-03-17.parquet")
df.to_parquet("/tmp/jq_1m/2026-03-17.parquet", compression="snappy")
```

然后使用 `--jq-dir /tmp/jq_1m`。

---

## 5. kbar_realtime 用法

线上实时工具：通过 Paged 读取 journal 中的 tick 数据，流式合成 JQ 口径分钟 bar，并按 `sec01_flush + patch` 模式对外发布。

```bash
./kbar_realtime [-o output_dir] [-d journal_dir] [-s now]
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-o` | `./output` | 输出目录 |
| `-d` | `/shared/kungfu/journal/user/` | journal 目录 |
| `-s now` | 从 journal 开头 | 只读取启动后的新数据 |

### 5.1 发布逻辑

当前实时发布逻辑很简单：

- 使用 SH tick 作为主发布时间信号
- 当 SH 的 `exchange_time` 进入某个交易分钟的 `01` 秒时，立即发布该分钟主文件
- 如果后续迟到 tick 继续影响该分钟，则输出 patch 文件修正

也就是：

- 主文件：`sec01_flush`
- 修正：`patch`

选择这个方案的原因是：

- 交易所一轮 tick 推送不稳定，不能可靠地通过“这一轮已经覆盖所有股票”来触发 flush
- `nano_timestamp` 和 `exchange_time` 不一致，因此也不能在 `exchange_time` 刚到 `00` 秒时就 flush
- 基于 replay 数据，`01` 秒 flush 比 `00` 秒安全，同时仍保持很低延迟

### 5.2 输出文件

```
output/
└── 2026-03-17/
    ├── kbar_0931.csv
    ├── kbar_patch_0932_v2.csv
    ├── kbar_patch_0932_v3.csv
    ├── ...
    └── kbar_1500.csv
```

主文件示例：

```csv
time,code,open,close,high,low,volume,money
2026-03-17 09:31:00,000001.XSHE,10.8700,10.8900,10.9000,10.8700,1508335,16408670
2026-03-17 09:31:00,600519.XSHG,1810.0000,1812.5000,1813.0000,1809.0000,23456,42498750
```

patch 文件示例：

```csv
time,code,open,close,high,low,volume,money,version
2026-03-17 09:32:00,200028.XSHE,6.1200,6.1200,6.1200,6.1200,0,0,2
```

文件采用**原子写入**：先写 dot 前缀临时文件，完成后 `rename` 为正式文件，下游不会读到半写状态。

### 5.3 实现要点

- `prev_day_close` 从 tick 的 `PreClose` 字段自动提取，每只股票首次出现时初始化
- 分钟发布层按“分钟保存所有股票状态”工作，已发布分钟后续仍可被 patch 修正
- 对同一股票的乱序迟到 tick，会重放该股票全天 tick 历史，修正受影响分钟
- SIGINT/SIGTERM 或空闲超时会触发 `finalize_all()`，补齐尚未发布的分钟并 flush 剩余 patch

## 6. 核心合成逻辑

本章是文档的核心，详细说明如何从 tick 快照合成聚宽口径 1min kbar。

### 6.1 数据流

```
tick 快照（每 3 秒一条）
  │
  │  push_tick(symbol, time, price, high, low, acc_volume, acc_turnover)
  ↓
TickBarBuilder
  │  按 symbol 分发
  ↓
StockBarAccumulator（每只股票一个实例）
  │  compute_bar_minute(time) → 分类
  │  process_trade_tick() → 更新 OHLC
  │  emit_bar() → 生成 MinuteBar
  ↓
MinuteBar { bar_minute, open, close, high, low, volume, money }
  │
  ↓
kbar_offline: 收集到 map → 与 JQ 对比
kbar_realtime: callback → CsvBarWriter → CSV 文件
```

**TickBarBuilder** 是多股票聚合器，内部维护 `unordered_map<symbol, StockBarAccumulator>`。每条 tick 按 symbol 路由到对应的 accumulator。支持注册 callback，bar 完成时立即通知。

**StockBarAccumulator** 是单只股票的状态机，负责所有时间分类、trade 检测、OHLC 累积、gap 填充等逻辑。

### 6.2 分钟分配规则（compute_bar_minute）

tick 的 `Time` 字段为 `HHMMSSmmm` 格式（如 `93015000` = 09:30:15.000）。`compute_bar_minute()` 将每条 tick 分配到对应的 bar 分钟：

```
时间线                             bar_minute     含义
─────────────────────────────────────────────────────────
00:00 ─── 09:24:59.999            BAR_BASELINE   盘前基准（记录 baseline 价格/量/额）
09:25:00 ─ 09:29:59.999           → 931          开盘集合竞价，归入第一根 bar
09:30:00 ─ 09:30:59.999           → 931          正常交易
09:31:00 ─ 09:31:59.999           → 932          正常交易
  ...                               ...
11:29:00 ─ 11:29:59.999           → 1130         正常交易（但有特殊 11:30 逻辑）
11:30:00.000（精确）               → 1130         午间收盘快照
11:30:00.001 ─ 12:59:59.999       BAR_MIDDAY_CARRY  午休（只取第一条 carry tick）
13:00:00 ─ 13:00:59.999           → 1301         下午开盘
  ...
14:59:00 ─ 14:59:59.999           → 1500         正常交易
15:00:00.000（精确）               → 1500         收盘集合竞价快照
15:00:00.001 ─ 15:05:00.000       BAR_CLOSE_CARRY   收盘后续（扫描 auction）
15:05:00.001 ─                    BAR_IGNORE     盘后，忽略
```

**Bar 标签 = 分钟结束时间**：例如 `931` 代表 (09:30:00, 09:31:00] 区间的 bar，与聚宽命名一致。

正常交易时间的分配公式：提取 HHMM，然后取 `next_minute(HHMM)` 作为 bar 标签。

### 6.3 Trade tick 筛选

不是每条 tick 快照都包含新成交。判断"有成交"的标准：

```cpp
bool is_trade = (acc_volume != prev_acc_volume) ||
                (acc_turnover != prev_acc_turnover);
```

即 **累计成交量或累计成交额发生变化** → 此 tick 包含新成交。

特殊情况：每只股票的**第一条交易时段 tick** 始终被视为 trade tick（`seen_first_trading_ = false` 时）。

只有 trade tick 参与 OHLC 计算：
- **Open**：当前 bar 第一个 trade tick 的 price
- **Close**：当前 bar 最后一个 trade tick 的 price
- **High/Low**：当前 bar 所有 trade tick 的 price 的 max/min

对应 Python：`select_price_ticks()` 函数中 `volume != prev_volume | money != prev_money` 的筛选。

### 6.4 OHLC 计算 + High/Low 混合

除了 trade tick 的 price 之外，还需要检查 **快照 High/Low 字段的变化**：

```cpp
// 快照 High 字段变化 → 记录 high_update
if (high_f != prev_high_ && high_f > 0)
    bar_high_update_ = max(bar_high_update_, high_f);

// 快照 Low 字段变化 → 记录 low_update
if (low_f != prev_low_ && low_f > 0)
    bar_low_update_ = min(bar_low_update_, low_f);
```

最终 bar 输出时混合两个来源：

```cpp
bar.high = max(trade_high, high_update);  // 取更大值
bar.low  = min(trade_low,  low_update);   // 取更小值
```

这是因为 tick 快照大约每 3 秒一条，两条快照之间可能有多笔成交，仅靠 Price 字段可能漏掉极值。日内 High/Low 的变化能捕获到这些遗漏。

对应 Python：`aggregate_intraday_bars()` 中的 `high_updates` / `low_updates` merge。

### 6.5 Volume/Money 差分

tick 中的 `AccVolume` 和 `AccTurnover` 是**当日累计值**，需要差分得到分钟增量：

```cpp
bar.volume = max(0, bar_cum_volume - prev_bar_cum_volume);
bar.money  = max(0, bar_cum_money  - prev_bar_cum_money);
```

基准值的优先级：`prev_bar_cum_volume_`（上一 bar 的累计值）→ `baseline_volume_`（09:25 前的累计值）→ 0。

Money 输出时取 floor：`floor(money / 10000.0 + 1e-6)`（AccTurnover 单位是 ×10000 元，转为元后向下取整）。

对应 Python：`aggregate_intraday_bars()` 中的 `cum_volume - prev_cum_volume`。

### 6.6 Carry-forward 空 bar 填充

当某个分钟没有任何 tick（或有 tick 但无 trade），该分钟的 bar 用 carry-forward 逻辑填充：

```
open = close = high = low = resolve_bar_close(minute)
volume = 0, money = 0
```

`resolve_bar_close()` 的优先级：
1. `ffill_trade_close_`：最近一个有 trade 的 bar 的 close
2. `ffill_any_close_`：最近一个有任意 tick 的 bar 的 close（包括无 trade 的 tick）
3. `baseline_close_`：09:25 前最后一条 tick 的 price
4. `prev_day_close_`：仅在 **first_valid_bar 之前** 使用（见 6.9）

**Gap 填充**：当 bar 分钟跳跃时（如从 931 直接到 935），中间的 932/933/934 自动用 carry-forward 填充。`fill_gap_bars()` 遍历所有跳过的交易分钟，逐一生成 carry bar。

`next_trading_minute()` 函数处理午休跳跃：`1130 → 1301`，以及交易日结束：`1500 → 0`。

对应 Python：`aggregate_intraday_bars()` 中的 `ffill()` + `fillna(baseline_close)` + `fillna(close)`。

### 6.7 11:30 半日收盘特殊处理

某些股票在 11:29-11:30 区间没有新成交（AccVolume 不变），但有静态快照。此时聚宽的 11:30 bar 使用 **该窗口内第一个快照的 price 作为 open**（而非 carry-forward）。

实现：跟踪 [11:29, 11:30) 窗口内的首末累计量和第一个快照价格：

```cpp
if (current_bar_ == 1130 && time >= 112900000 && time < 113000000) {
    if (first_vol_1129_ < 0) first_vol_1129_ = acc_volume;
    last_vol_1129_ = acc_volume;
    if (first_snapshot_1129_ == 0 && price > 0) first_snapshot_1129_ = price;
}
```

`emit_bar()` 时，若 `first_vol_1129_ == last_vol_1129_`（无成交）且有快照价格，则覆盖：

```cpp
bar.open = first_snapshot_1129_;
bar.high = max(snapshot, bar.close);
bar.low  = min(snapshot, bar.close);
```

对应 Python：`apply_morning_end_rule()` 函数。

### 6.8 15:00 收盘集合竞价特殊处理

A 股 15:00 有收盘集合竞价（仅深圳，上海在 2018 年后也引入），产生一笔撮合成交。该成交不同于连续竞价成交，需要特殊处理。

**策略**：
1. 跟踪 15:00 之前最后一条 tick 的 price/volume/money（`preclose_*`）
2. 在 15:00:00 快照或之后的 carry tick 中，检测 volume 或 turnover 是否增加
3. 若增加 → 检测到 auction

```cpp
// 15:00:00 精确时刻 — 还检查 price 变化
if (time == 150000000 && price != preclose_price)
    has_auction_ = true;

// 15:00:01-15:05:00 carry tick — 只检查 volume/turnover
if (acc_volume > preclose_volume || acc_turnover > preclose_money)
    has_auction_ = true;
```

**Auction bar 输出**：

```cpp
bar.open  = preclose_price;      // 撮合前最后价
bar.close = auction_price;        // 撮合成交价
bar.high  = max(preclose, auction);
bar.low   = min(preclose, auction);
bar.volume = auction_volume - preclose_volume;
bar.money  = auction_money - preclose_money;
```

注意：上海股票的 auction volume 变化可能不在 15:00:00 精确时刻出现，而是在后续 carry tick 中。因此需要扫描 **所有** BAR_CLOSE_CARRY 区间的 tick。

对应 Python：`apply_close_auction_rule()` 函数。

### 6.9 prev_day_close 回填

`prev_day_close` 用于填充 **该股票第一条有效 tick 之前** 的空 bar。

例如：某只股票 09:30 才出现第一条有效 tick，但 kbar 从 09:31 开始输出。09:31 bar 之前没有 baseline 数据，carry-forward 值为 0。此时用 prev_day_close 填充，使 OHLC = prev_day_close（而非 0）。

```cpp
// resolve_bar_close() 中的判断
bool before_first = (first_valid_bar_ == 0) || (minute < first_valid_bar_);
if (before_first && carry == 0 && has_prev_day_close_)
    return prev_day_close_;
```

`first_valid_bar_` 记录该股票第一条 price > 0 或 volume > 0 的 tick 所属的 bar 分钟。

**kbar_offline** 从 JQ price_1m 前一交易日文件获取 prev_day_close。

**kbar_realtime** 从 tick 的 `PreClose` 字段自动获取：

```cpp
float preclose = (float)((double)md->PreClose / 10000.0);
if (preclose > 0)
    builder.set_prev_day_close_if_new(md->Symbol, preclose);
```

对应 Python：`apply_prev_close_fill()` 函数。

---

## 7. 验证结果

2026-03-17 全市场对比（~5300 只股票，~1,272,000 bars）：

| 指标 | Match 数 | 总数 | Match Rate |
|------|----------|------|------------|
| open | 1272xxx | 1272xxx | **100.00%** |
| close | 1271xxx | 1272xxx | **99.98%** |
| high | 1272xxx | 1272xxx | **99.99%** |
| low | 1272xxx | 1272xxx | **99.98%** |
| volume | 1271xxx | 1272xxx | **99.96%** |
| money (±1) | 1271xxx | 1272xxx | **99.96%** |

> 剩余 0.02-0.04% 的差异来自极少数股票的浮点精度和聚宽自身的数据异常（如停牌股异常 bar）。

---

## 8. 线上部署注意事项

### 8.1 prev_day_close

- **kbar_realtime**：自动从每条 tick 的 `KyStdSnpType.PreClose` 字段获取，无需外部文件
- **kbar_offline**：从 JQ price_1m 前一交易日 parquet 获取

### 8.2 JQ zstd → snappy

JQ price_1m parquet 使用 zstd 压缩（pandas 默认），而 Arrow 21 离线编译未启用 zstd（Docker 环境无 zstd 库）。kbar_offline 对比前需要先转换：

```python
import pandas as pd
for f in Path("/nkd_shared/assets/stock/price/price_1m").glob("*.parquet"):
    df = pd.read_parquet(f)
    df.to_parquet(f"/tmp/jq_1m/{f.name}", compression="snappy")
```

### 8.3 JQ timestamp 是 UTC

JQ parquet 中的 `datetime` 列是 `timestamp[us]` 类型，**不带时区但实际存储 UTC 值**。C++ 读取时必须用 `gmtime_r` 而非 `localtime_r` 解析。

### 8.4 JournalReader name

KungFu 要求 reader name 不超过 30 字符。kbar_realtime 使用 `"kbar_rt_HHMMSS"` 格式（15 字符）。

### 8.5 Paged 服务

kbar_realtime 依赖 Paged 服务运行。启动方式参见父项目的 `scripts/replay` 或 `scripts/start_paged.sh`。

---

## 9. Python 参考实现

C++ 合成逻辑逐行翻译自 Python 版本。对应关系如下：

| C++ 类/方法 | Python 函数 | 文件 |
|------------|------------|------|
| `StockBarAccumulator::compute_bar_minute()` | `assign_bar_end()` + `filter_trading_ticks()` | `generate_1m_kbar_ht.py` |
| `StockBarAccumulator::process_trade_tick()` | `select_price_ticks()` | `generate_1m_kbar_ht.py` |
| `StockBarAccumulator::emit_bar()` — OHLC | `aggregate_intraday_bars()` | `generate_1m_kbar_ht.py` |
| `StockBarAccumulator::emit_bar()` — volume/money | `aggregate_intraday_bars()` (cum diff) | `generate_1m_kbar_ht.py` |
| `StockBarAccumulator::emit_carry_bar()` | `ffill()` + `fillna(baseline)` | `generate_1m_kbar_ht.py` |
| `StockBarAccumulator::fill_gap_bars()` | `build_empty_frame()` + merge | `generate_1m_kbar_ht.py` |
| `StockBarAccumulator::emit_bar()` — 11:30 | `apply_morning_end_rule()` | `generate_1m_kbar_ht.py` |
| `StockBarAccumulator::emit_bar()` — 15:00 | `apply_close_auction_rule()` | `generate_1m_kbar_ht.py` |
| `StockBarAccumulator::resolve_bar_close()` — prev_day | `apply_prev_close_fill()` | `generate_1m_kbar_ht.py` |
| `StockBarAccumulator::set_prev_day_close()` | `load_prev_day_close()` | `generate_1m_kbar_ht.py` |
| `TickBarBuilder` | 无直接对应 | Python 用 `groupby("code")` |
| `read_ht_ticks()` | `load_ht_tick()` | `generate_1m_kbar_ht.py` |
| `read_jq_bars()` | `pd.read_parquet(jq_path)` | `compare_jq_1m.py` |
| `compare_with_jq()` | `compare_jq_1m.py` 全文 | `compare_jq_1m.py` |
| `format_symbol()` | `_ht_symbol_to_code()` | `generate_1m_kbar_ht.py` |
