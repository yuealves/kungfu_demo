# Realtime Factors V1 实现文档

## 一句话概述

V1 实现了一个 **单线程 C++ 流式因子引擎**，从 KungFu Journal 实时读取 L2 逐笔委托和逐笔成交数据，维护全市场订单状态，逐事件更新累积统计矩（count / mean / std / skew / kurt），最终产出 **5000+ 只股票 × 492 个因子** 的全量快照并落盘到 CSV。

---

## 目录

1. [做了什么](#1-做了什么)
2. [没做什么](#2-没做什么)
3. [代码结构一览](#3-代码结构一览)
4. [数据流全景](#4-数据流全景)
5. [核心模块逐个说明](#5-核心模块逐个说明)
   - 5.1 Moments 累加器
   - 5.2 时间转换
   - 5.3 分桶条件
   - 5.4 事件类型
   - 5.5 因子 Schema
   - 5.6 SymbolState
   - 5.7 OrderStateStore
   - 5.8 TS-Engine
6. [492 个因子的完整构成](#6-492-个因子的完整构成)
7. [沪深两市的差异处理](#7-沪深两市的差异处理)
8. [Journal 读取与价格编码](#8-journal-读取与价格编码)
9. [编译与运行](#9-编译与运行)
10. [已知局限](#10-已知局限)

---

## 1. 做了什么

| 能力 | 说明 |
|------|------|
| **从 Journal 实时读取** | 通过 KungFu Paged 服务订阅 3 个频道（tick/order/trade），与 `realtime_parquet`、`realtime_kbar` 等进程共享同一数据源 |
| **沪深订单状态管理** | 完整实现了 SH 撤单识别、激进单从成交流补全、SZ 撤单从成交流检测、SZ 市价单价格回填 |
| **7 个事件族 × 多分桶** | buy_order / sell_order / buy_withdraw / sell_withdraw / both_trade / buy_trade / sell_trade，每族按大小单/全撤/部分撤/闪撤等条件分桶 |
| **累积统计矩** | 对每个 (事件族, 分桶) 的 vol/money/tmlag/moneydiff 维护 O(1) 更新的 s1~s4，直接计算 mean/std/skew/kurt |
| **定时 CSV 快照** | 可配置间隔（默认 60s）将全市场因子 dump 到 CSV |
| **单元测试** | 17 个 Boost.Test 用例覆盖 Moments、时间转换、分桶逻辑、订单状态管理 |
| **集成测试通过** | journal 回放 1000 倍速下处理约 1300 万条委托 + 1000 万条成交，5258 只股票，因子值经健全性检查无异常 |

## 2. 没做什么

V1 是精简版，以下内容留给后续版本：

| 未实现 | 原因 |
|--------|------|
| **ret / turnover 类因子** | 需要外部数据（pre_close 昨收价、circulating_market_cap 流通市值），V1 没有加载这些数据的机制 |
| **Delta Ring Buffer / Snapshot-Composer / SHM** | README 描述的四角色架构，V1 只实现了 TS-Engine 角色 |
| **截面因子（zscore / srank）** | 依赖 Snapshot-Composer 的截面计算阶段 |
| **Python 读取 SDK** | 等 SHM 层实现后才有意义 |
| **与 Python demo 的精确对拍** | Python demo 的因子列包含 ret/turnover，V1 的因子列是精简的 vol/money 子集，列不对齐无法直接对比 |

---

## 3. 代码结构一览

```
realtime_factors/
├── include/                    # 头文件（大部分是 header-only）
│   ├── moments.h               # Moments 累加器（81行）
│   ├── time_utils.h            # HHMMSSmmm → 连续竞价毫秒（36行）
│   ├── bucket_classify.h       # 大小单/撤单分桶条件（39行）
│   ├── event_types.h           # 内部事件结构体（87行）
│   ├── factor_schema.h         # 因子列名注册 + 常量（105行）
│   ├── symbol_state.h          # 单只股票的全部累加器（173行）
│   ├── order_state.h           # OrderStateStore 声明（99行）
│   └── ts_engine.h             # TSEngine 声明（67行）
├── src/
│   ├── order_state.cpp         # 沪深订单状态管理实现（300行）
│   ├── ts_engine.cpp           # Journal 读取 + 事件分发 + CSV 输出（253行）
│   └── main.cpp                # 命令行入口（50行）
├── tests/
│   ├── test_moments.cpp        # 6 个测试
│   ├── test_time_utils.cpp     # 3 个测试
│   ├── test_bucket.cpp         # 3 个测试
│   └── test_order_state.cpp    # 5 个测试
├── scripts/
│   └── check_output.py         # CSV 输出健全性检查
├── docs/
│   └── impl_v1.md              # 本文档
└── CMakeLists.txt              # 独立构建配置
```

总计约 1400 行 C++ 代码（不含测试），300 行测试代码。

---

## 4. 数据流全景

下面是一条委托/成交事件从 Journal 到因子输出的完整路径：

```
Journal (mmap)
    │
    ▼
┌─────────────────────────────────────────────────┐
│  TSEngine::run()                                │
│  JournalReader::getNextFrame()                  │
│    │                                            │
│    ├─ msg_type=62 ──► convert_order()           │
│    │                     │                      │
│    │                     ▼                      │
│    │               ┌─────────────┐              │
│    │               │ OrderEvent  │              │
│    │               └──────┬──────┘              │
│    │                      │                     │
│    │         ┌────────────┼────────────┐        │
│    │         │ SH         │ SZ         │        │
│    │         ▼            ▼            │        │
│    │   on_order_sh   on_order_sz       │        │
│    │     │     │          │            │        │
│    │     │  ┌──┘          │            │        │
│    │     ▼  ▼             ▼            │        │
│    │  回调: ProcessedOrder              │        │
│    │  回调: ProcessedWithdraw (SH撤单)  │        │
│    │         │                         │        │
│    │         ▼                         │        │
│    │  symbol_states_[sym]              │        │
│    │    .buy_order.feed(po)            │        │
│    │    .sell_order.feed(po)           │        │
│    │    .buy_withdraw.feed(pw)         │        │
│    │    .sell_withdraw.feed(pw)        │        │
│    │                                   │        │
│    ├─ msg_type=63 ──► convert_trade()  │        │
│    │                     │             │        │
│    │                     ▼             │        │
│    │               ┌─────────────┐    │        │
│    │               │ TradeEvent  │    │        │
│    │               └──────┬──────┘    │        │
│    │         ┌────────────┼─────────┐ │        │
│    │         │ SH         │ SZ      │ │        │
│    │         ▼            ▼         │ │        │
│    │   on_trade_sh   on_trade_sz    │ │        │
│    │     │     │      │    │        │ │        │
│    │     ▼     ▼      ▼    ▼        │ │        │
│    │  回调: ProcessedOrder (激进单补全)│ │        │
│    │  回调: ProcessedTrade            │ │        │
│    │  回调: ProcessedWithdraw (SZ撤单)│ │        │
│    │         │                       │ │        │
│    │         ▼                       │ │        │
│    │  symbol_states_[sym]            │ │        │
│    │    .both_trade.feed(pt)         │ │        │
│    │    .buy_trade.feed(pt)          │ │        │
│    │    .sell_trade.feed(pt)         │ │        │
│    │                                   │        │
│    └─ 定时 ──► dump_csv()              │        │
│                  │                     │        │
│                  ▼                     │        │
│         factors_N.csv                  │        │
└─────────────────────────────────────────────────┘
```

关键点：**OrderStateStore 是事件路由的核心**。它接收原始的 Order/Trade 事件，维护活跃订单表，并通过回调将处理后的 ProcessedOrder / ProcessedWithdraw / ProcessedTrade 事件分发给 SymbolState 进行累积统计。

---

## 5. 核心模块逐个说明

### 5.1 Moments 累加器

**文件**: `include/moments.h`

每个 Moments 实例跟踪一个数值序列的 5 个累积量：

```
n   — 样本数
s1  — Σx      （一阶矩）
s2  — Σx²     （二阶矩）
s3  — Σx³     （三阶矩）
s4  — Σx⁴     （四阶矩）
```

从这 5 个量可以 O(1) 计算所有统计量：

| 统计量 | 公式 | 边界处理 |
|--------|------|----------|
| **mean** | s1 / n | n=0 → NaN |
| **std** (样本) | √(max((s2 − s1²/n) / (n−1), 0)) | n<2 → NaN |
| **skew** | (s3/n − 3μ·s2/n + 2μ³) / σₚ³ | n<3 或 σₚ² ≤ ε·μ² → NaN |
| **kurt** (超额) | (s4/n − 4μ·s3/n + 6μ²·s2/n − 3μ⁴) / σₚ⁴ − 3 | n<4 或 σₚ² ≤ ε·μ² → NaN |
| **vwapx** | money.s1 / vol.s1 | vol.s1=0 → NaN |

几个关键设计决策：

- **std 用样本标准差**（除以 n−1），**skew/kurt 用总体矩**（除以 n）。这与 Python demo 的 Polars 计算口径一致。
- 零方差保护阈值 `ε = 0.0009765625`（float16 的 machine epsilon），使用**相对判据** `σₚ² ≤ ε·μ²` 而非绝对阈值，避免大均值序列被误判为零方差。
- `merge()` 方法直接对应字段相加，因为 s1~s4 都是可加的原始矩和。这使得增量合并 trivial。

### 5.2 时间转换

**文件**: `include/time_utils.h`

Journal 中的时间字段是 `int32_t`，格式为 `HHMMSSmmm`（如 `93015230` = 09:30:15.230）。

`tm_int_to_ms()` 将其解码为从午夜起的毫秒数：
```
93015230  →  hh=9, mm=30, ss=15, mmm=230  →  34215230 ms
```

`apply_continuous_session()` 将 A 股的三段不连续交易时间映射到单一连续时间轴上：

```
原始时段:        09:15 ─── 09:30 │ 09:30 ───── 11:30 │ 13:00 ───── 15:00
                 集合竞价         │     上午连续       │     下午连续

映射规则:        < 09:29:57  →  +5min
                 09:29:57 ~ 12:59:57  →  不变
                 > 12:59:57  →  −90min

映射后:          09:20 ─── 09:35 │ 09:30 ───── 11:30 │ 11:30 ───── 13:30
                                 │                    │
                               无间断                无间断
```

这样做的好处是：计算 `withdrawtmlag`（撤单与下单的时间差）和 `ordertmlag`（买卖双方下单的时间差）时，不会因为午休间隔或集合竞价切换产生不合理的大值。

### 5.3 分桶条件

**文件**: `include/bucket_classify.h`

每条事件根据 vol（量）和 money（金额）被分入一个或多个桶。桶之间**不互斥**——同一条事件可以同时进入 ALL + LARGE + COMPLETE 三个桶。

**委托/撤单的大小单判定**（与 Python demo `factor_config.toml` 对齐）：

```
大单:  vol ≥ 100,000  或  money ≥ 1,000,000
小单:  (vol ≤ 10,000 且 money ≤ 100,000) 或 (vol ≤ 1,000 且 money ≤ 500,000)
```

注意大单用**或**逻辑（量大或金额大即为大单），小单用**与**逻辑的两个条件再取**或**（两种小单定义满足其一即可）。中间地带既不是大单也不是小单。

**撤单额外分桶**：
- 全撤（complete）：撤单量 ≥ 原订单量
- 部分撤（partial）：撤单量 < 原订单量
- 闪撤（instant）：撤单时间差 ≤ 3 秒

**成交额外分桶**：
- BOLARGE / BOSMALL：按**买方原始委托**的 vol/money 判定
- SOLARGE / SOSMALL：按**卖方原始委托**的 vol/money 判定

### 5.4 事件类型

**文件**: `include/event_types.h`

定义了 5 个结构体，代表从 Journal 原始 packed 结构体到因子计算之间的中间表示：

| 结构体 | 角色 | 来源 |
|--------|------|------|
| `OrderEvent` | TS-Engine 从 `KyStdOrderType` 转换而来的内部委托 | `convert_order()` |
| `TradeEvent` | TS-Engine 从 `KyStdTradeType` 转换而来的内部成交 | `convert_trade()` |
| `ProcessedOrder` | OrderStateStore 处理后的委托（含 money） | `on_order_sh/sz` 回调 |
| `ProcessedWithdraw` | OrderStateStore 处理后的撤单（含 tmlag） | 撤单检测回调 |
| `ProcessedTrade` | OrderStateStore 处理后的成交（含双方委托信息、tmlag、moneydiff） | 成交处理回调 |

为什么需要两层结构体？因为 `OrderEvent/TradeEvent` 只是原始字段的语义映射，而 `ProcessedXxx` 包含了 OrderStateStore 查找活跃订单后**丰富过的**信息（如买方的原始委托时间、金额等），这些信息是因子计算所需的。

### 5.5 因子 Schema

**文件**: `include/factor_schema.h`

定义了所有因子列的常量和列名生成逻辑。核心常量：

```
ORDER_FACTORS_PER_BUCKET    = 6    // count, vwapx, money 的 mean/std/skew/kurt
WITHDRAW_FACTORS_PER_BUCKET = 10   // 上面 + tmlag 的 mean/std/skew/kurt
TRADE_FACTORS_PER_BUCKET    = 16   // 上面 + moneydiff 的 mean/std/skew/kurt + buyorder_count + sellorder_count

ORDER_BUCKET_N    = 3   (ALL, LARGE, SMALL)
WITHDRAW_BUCKET_N = 6   (ALL, LARGE, SMALL, COMPLETE, PARTIAL, INSTANT)
TRADE_BUCKET_N    = 7   (ALL, LARGE, SMALL, BOLARGE, BOSMALL, SOLARGE, SOSMALL)
```

`get_factor_names()` 按固定顺序生成 492 个列名字符串，顺序与 `SymbolState::write_all_factors()` 输出的 double 数组严格对应。命名规则：

```
{方向前缀}{事件族}{统计量}{桶后缀}

示例:
  buy_order_count           — 买方委托总数 (ALL 桶)
  buy_ordervwapx_large      — 买方大单 VWAP
  sell_withdrawtmlag_skew_instant — 卖方闪撤时间差偏度
  both_trade_buyorder_count_bolarge — 全部成交中买方大单的去重委托数
```

### 5.6 SymbolState

**文件**: `include/symbol_state.h`

每只股票一个 `SymbolState` 实例，内部组织为 **7 个事件族容器**：

```
SymbolState
├── buy_order       : OrderMoments     (18 因子)
├── sell_order      : OrderMoments     (18 因子)
├── buy_withdraw    : WithdrawMoments  (60 因子)
├── sell_withdraw   : WithdrawMoments  (60 因子)
├── both_trade      : TradeMoments     (112 因子)  ← 不区分方向
├── buy_trade       : TradeMoments     (112 因子)  ← 仅买方主动
└── sell_trade      : TradeMoments     (112 因子)  ← 仅卖方主动
                                        ─────────
                                        合计 492
```

每个容器的内部结构都是 `Moments data[桶数][维度数]` 的二维数组。以 `TradeMoments` 为例：

```
TradeMoments
├── data[7][4]                  7 个桶 × 4 个维度(vol/money/tmlag/moneydiff)
│   └── 每个 Moments 存 {n, s1, s2, s3, s4}
├── buy_order_ids[7]            7 个桶各自的 unordered_set<int64>
└── sell_order_ids[7]           用于 unique order count 去重
```

`feed()` 方法接收一条 ProcessedXxx 事件，判断它属于哪些桶，然后更新对应桶的每个 Moments 实例。`write_factors()` 方法将所有桶的统计量按固定顺序写入 double 数组。

### 5.7 OrderStateStore

**文件**: `include/order_state.h` + `src/order_state.cpp`

这是 V1 中最复杂的模块（约 400 行），负责维护**全市场活跃订单哈希表**并实现沪深两市不同的订单处理规则。

**活跃订单表**: `unordered_map<OrderKey, ActiveOrder>`，key 为 (channel_id, order_id) 的复合键。每条活跃订单记录包含 symbol、time、price、volume、money、side、traded_vol（已成交量）。

**核心接口是 4 个处理函数**，它们通过回调将处理结果分发出去：

```cpp
on_order_sh(evt, on_order_callback, on_withdraw_callback)
on_trade_sh(evt, on_order_callback, on_trade_callback)
on_order_sz(evt, on_order_callback)
on_trade_sz(evt, on_withdraw_callback, on_trade_callback)
```

为什么用回调而不是返回值？因为一条原始事件可能触发**多种**处理结果——比如一条 SH 成交事件可能同时触发一次激进单补全（on_order）和一次正常成交（on_trade）。

**订单回收**: 当 `traded_vol >= volume`（成交量已达到或超过委托量），或者全撤（撤单量 >= 委托量）时，该订单从哈希表中移除。

详细的沪深差异处理见[第 7 节](#7-沪深两市的差异处理)。

### 5.8 TS-Engine

**文件**: `include/ts_engine.h` + `src/ts_engine.cpp` + `src/main.cpp`

TS-Engine 是整个系统的入口和主循环。它做三件事：

1. **读取**: 通过 KungFu `JournalReader::createReaderWithSys()` 连接到 Paged 服务，订阅 tick/order/trade 三个 journal 频道
2. **处理**: 按 `msg_type` 分发，order(62) 和 trade(63) 进入 `process_order/process_trade`，tick(61) 忽略
3. **输出**: 定时将 `symbol_states_` 的全量因子写成 CSV

转换逻辑 (`convert_order` / `convert_trade`) 将 Journal 的 packed 结构体字段映射为内部事件类型，最关键的一步是 **Price / 10000.0**——因为 Journal 中存的 Price 是原始的 ×10000 整数格式（如 `468800` 表示 46.88 元），而非实际价格。

`process_order` 和 `process_trade` 定义了闭包回调，将 OrderStateStore 产出的 ProcessedXxx 事件路由到对应的 `SymbolState` 子组件。

---

## 6. 492 个因子的完整构成

### 按事件族展开

| 事件族 | 方向前缀 | 桶 | 维度 | 每桶因子 | 桶数 | 小计 |
|--------|----------|-----|------|----------|------|------|
| buy_order | `buy_` | ALL/LARGE/SMALL | vol,money | 6 | 3 | 18 |
| sell_order | `sell_` | ALL/LARGE/SMALL | vol,money | 6 | 3 | 18 |
| buy_withdraw | `buy_` | ALL/LARGE/SMALL/COMPLETE/PARTIAL/INSTANT | vol,money,tmlag | 10 | 6 | 60 |
| sell_withdraw | `sell_` | ALL/LARGE/SMALL/COMPLETE/PARTIAL/INSTANT | vol,money,tmlag | 10 | 6 | 60 |
| both_trade | `both_` | ALL/LARGE/SMALL/BOLARGE/BOSMALL/SOLARGE/SOSMALL | vol,money,tmlag,moneydiff + unique_count | 16 | 7 | 112 |
| buy_trade | `buy_` | 同上 | 同上 | 16 | 7 | 112 |
| sell_trade | `sell_` | 同上 | 同上 | 16 | 7 | 112 |
| | | | | | **合计** | **492** |

### 每个桶内的因子列表

**Order 桶** (6 个因子):

| # | 列名模板 | 含义 |
|---|----------|------|
| 1 | `{p}order_count{s}` | 委托条数 |
| 2 | `{p}ordervwapx{s}` | VWAP = 总金额 / 总量 |
| 3 | `{p}ordermoney_mean{s}` | 委托金额均值 |
| 4 | `{p}ordermoney_std{s}` | 委托金额样本标准差 |
| 5 | `{p}ordermoney_skew{s}` | 委托金额偏度 |
| 6 | `{p}ordermoney_kurt{s}` | 委托金额超额峰度 |

`{p}` = 方向前缀（buy_/sell_），`{s}` = 桶后缀（空/\_large/\_small）。

**Withdraw 桶** (10 个因子): Order 桶的 6 个 + tmlag 的 4 个统计量：

| # | 列名模板 | 含义 |
|---|----------|------|
| 7 | `{p}withdrawtmlag_mean{s}` | 撤单时间差均值(ms) |
| 8 | `{p}withdrawtmlag_std{s}` | 撤单时间差标准差 |
| 9 | `{p}withdrawtmlag_skew{s}` | 撤单时间差偏度 |
| 10 | `{p}withdrawtmlag_kurt{s}` | 撤单时间差峰度 |

**Trade 桶** (16 个因子): Withdraw 桶的 10 个（把 withdraw 换成 trade）+ moneydiff 的 4 个 + 去重计数 2 个：

| # | 列名模板 | 含义 |
|---|----------|------|
| 11 | `{p}ordermoneydiff_mean{s}` | 买卖方委托金额差绝对值的均值 |
| 12~14 | `..._std/skew/kurt{s}` | 对应的标准差/偏度/峰度 |
| 15 | `{p}trade_buyorder_count{s}` | 该桶涉及的买方委托去重数 |
| 16 | `{p}trade_sellorder_count{s}` | 该桶涉及的卖方委托去重数 |

---

## 7. 沪深两市的差异处理

这是理解 OrderStateStore 的关键。A 股的逐笔委托和逐笔成交数据在沪深两市有完全不同的编码规则。

### 上海 (SH, symbol ≥ 400000)

**委托流** (`on_order_sh`):

| OrderKind | 含义 | 处理 |
|-----------|------|------|
| 2 (→65 'A') | 新增限价单 | 插入活跃订单池，触发 ProcessedOrder 回调 |
| 10 (→68 'D') | 撤单 | 查找原单，计算 withdraw_tmlag，触发 ProcessedWithdraw 回调；全撤时移除原单 |
| 11 (→83 'S') | 特殊委托 | 直接过滤丢弃 |

**成交流** (`on_trade_sh`):

上海的关键特点是**激进单（市价单）可能不在委托流中出现**。买方主动成交时，如果买单 ID 不在活跃订单池中，说明这是一笔激进单，需要从成交数据中**补全**：

```
如果 trade.buy_order_id 不在活跃订单池中:
    创建虚拟买单: price=trade.price, volume=trade.volume
    插入活跃订单池
    触发 ProcessedOrder 回调
否则:
    将 trade.volume 累加到现有订单的 volume 上
```

这个"累加 volume"的逻辑对应 Python demo 中 `ordervol = ordervol + tradevol` 的行为——激进单的总量是通过多笔成交逐步累加出来的。

**成交方向**: 直接使用 `BSFlag`（1=B, 2=S）。

### 深圳 (SZ, symbol < 400000)

**委托流** (`on_order_sz`):

| OrderKind | 含义 | 处理 |
|-----------|------|------|
| 1 (→50 '2') | 限价单 | 使用原始价格 |
| 2 (→49 '1') | 对手方最优 | **市价单**：买单用卖方最新成交价 × 1.001 回填，卖单用买方最新成交价 × 0.999 回填 |
| 3 (→85 'U') | 本方最优 | **市价单**：用对方最新成交价直接回填 |

深圳的市价单在委托流中 Price=0，需要用**最新成交价**回填。回填逻辑：
- 买单查卖方主动成交的最新价格（买单吃卖盘挂单）
- 卖单查买方主动成交的最新价格
- "对手方最优"额外乘以 1.001（买）或 0.999（卖）以近似实际成交价

**成交流** (`on_trade_sz`):

深圳的撤单信息**不在委托流中**，而是通过成交流特殊编码传递：

| 条件 | 含义 | 处理 |
|------|------|------|
| tradesellid == 0 且 tradebuyid ≠ 0 | 买方撤单 | 查找原买单，构造 ProcessedWithdraw |
| tradebuyid == 0 且 tradesellid ≠ 0 | 卖方撤单 | 查找原卖单，构造 ProcessedWithdraw |
| tradetype == 70 ('F') 且双方 ID 均非 0 | 正常成交 | 构造 ProcessedTrade |

**成交方向推断**: 深圳成交流没有 BSFlag，而是通过 `tradebuyid > tradesellid → 买方主动(B)` 推断。

**最新成交价维护**: 每笔正常成交都更新 `sz_latest_trade_px_[symbol, inferred_side]`，用于后续市价单的价格回填。

---

## 8. Journal 读取与价格编码

### Journal 连接

通过 `JournalReader::createReaderWithSys()` 连接到 Paged 服务，订阅 3 个频道：

```cpp
dirs   = {journal_dir, journal_dir, journal_dir}
jnames = {"insight_stock_tick_data", "insight_stock_order_data", "insight_stock_trade_data"}
```

Reader 通过 Paged 的 Unix socket 注册，获得 journal 文件的 mmap 读取权限。多个 reader 可以同时读取同一组 journal。

### 价格编码

**这是一个重要的坑**：Journal 中 `KyStdOrderType.Price` 和 `KyStdTradeType.Price` 字段存储的是**原始 ×10000 格式的整数值**（以 float32 存储）。例如 46.88 元存储为 `468800.0f`。

V1 在 `convert_order()` 和 `convert_trade()` 中执行 `/ 10000.0` 转换：

```cpp
evt.price = static_cast<double>(raw.Price) / 10000.0;
```

注意 Python demo `demo_backfill_ht2.py` 读取的是另一份原始 Parquet 数据（同样是 ×10000 格式），也做了相同的除法。

### 时序竞态

在回放场景下，reader 必须在 replayer 开始写入用户 journal **之后**才能读到行情数据。如果 reader 在 replayer 启动之前就连接，它只会读到 Paged 的系统消息（msg_type 20/30/31/32/33/34）。实际使用时：

```bash
scripts/replay start --reset 1000      # 先启动回放
sleep 8                                 # 等待数据写入
./realtime_factors --dump-interval 10   # 再启动 reader
```

线上环境（gateway 持续写入）没有这个问题。

---

## 9. 编译与运行

### 编译

```bash
cd realtime_factors
mkdir -p build && cd build
cmake ..
make -j4              # 编译主程序 + 4 个测试
```

产物：
- `realtime_factors` — 主程序
- `test_moments` / `test_time_utils` / `test_bucket` / `test_order_state` — 单元测试

### 运行测试

```bash
./test_moments --log_level=test_suite        # 6 个测试
./test_time_utils --log_level=test_suite     # 3 个测试
./test_bucket --log_level=test_suite         # 3 个测试
./test_order_state --log_level=test_suite    # 5 个测试
```

### 运行主程序

```bash
# 先启动 Paged 和 replayer（或者线上 gateway）
cd /workspace/Code/quant/stream_feature/kungfu_demo
scripts/replay start --reset 1000
sleep 8

# 启动因子引擎
./realtime_factors/build/realtime_factors \
    --journal-dir /shared/kungfu/journal/user/ \
    --output-dir /tmp/rtf_output/ \
    --dump-interval 10

# Ctrl+C 停止后查看输出
python3 realtime_factors/scripts/check_output.py /tmp/rtf_output/factors_final.csv
```

### 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--journal-dir` | `/shared/kungfu/journal/user/` | Journal 文件目录 |
| `--output-dir` | `/tmp/rtf_output/` | CSV 输出目录 |
| `--dump-interval` | 60 | 快照间隔（秒） |
| `--reader-name` | `rtf_engine` | Paged reader 注册名 |

### CSV 输出格式

```
symbol,buy_order_count,buy_ordervwapx,buy_ordermoney_mean,...(492列)
688757,62,26.53505198,19677.025,...
688583,210,132.0402129,79144.27486,...
...
```

- 第一列 `symbol` 为整数股票代码
- 后续 492 列为因子值，NaN 输出为空
- 精度 10 位有效数字

---

## 10. 已知局限

| 问题 | 影响 | 计划 |
|------|------|------|
| **不处理 tick (msg_type=61)** | tick 级别因子（如买卖价差、订单簿不平衡）无法计算 | 后续按需添加 |
| **活跃订单表无上限** | 极端情况下内存增长无限制。集成测试中观察到峰值约 700 万条活跃订单 | 可加定期清理过期订单的逻辑 |
| **unique_count 用 unordered_set** | 内存占用大（每个 trade 桶 2 个 set × 7 桶 × 3 方向 = 42 个 set），且不可增量合并 | 可考虑 HyperLogLog 近似 |
| **CSV 输出是同步阻塞的** | dump 期间主循环暂停处理事件。5000 只股票 × 492 列约需 ~100ms | 改为异步写或在 Snapshot-Composer 中处理 |
| **单线程** | 无法利用多核。目前单核处理能力已足够（百万事件/秒级） | README 架构支持按 channel 拆分多线程 |
| **SH 激进单补全不完美** | 首笔成交的虚拟订单 price 取自成交价而非真实委托价，可能引入微小偏差 | 这与 Python demo 行为一致 |
