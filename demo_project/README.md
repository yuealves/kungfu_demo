# 实习生项目：L2 行情数据流式因子计算

## 1. 背景

我们的量化交易系统通过 KungFu yijinjing journal 系统实时接收 A 股 Level-2 行情数据（Tick 快照、逐笔委托、逐笔成交）。

你的任务是实现一套 **C++ 流式因子计算引擎**，随着 L2 数据实时到达，逐帧增量计算因子。

## 2. 考核标准

本项目重点考察以下两个方面：

### 2.1 计算延迟

**从收到一帧行情数据到该帧所有因子计算完成的平均延迟**。你需要在代码中测量并输出这个延迟。参考指标：数据中约 5000 只股票，30 个因子全部算完，输出延迟指标。

### 2.2 框架可扩展性

后续会持续新增大量因子（数百个），因此框架设计至关重要：

- **新增因子的便捷程度**：理想情况下，新增一个因子只需实现一个函数/类，注册即用，不需要改动框架代码
- **Python 集成友好性**：框架输出的因子是否方便为 Python 进程实时访问（pybind11 / ctypes / cython/ numpy共享内存等处理方式 均可），让研究员能用 Python 快速原型验证（即，C++实时生成的因子需要在python中可实时访问，供python中后续继续做复杂因子处理）
- **状态管理的统一性**：滑动窗口、在线统计量等公共基础设施是否易于复用

## 3. 环境

Docker 容器内已安装：

| 组件 | 路径 | 版本 |
|------|------|------|
| KungFu | `/opt/kungfu/master/` | **v1.0.0** |
| Boost | `/opt/kungfu/toolchain/boost-1.62.0/` | 1.62.0 |
| GCC | 系统 | 8.x (C++17) |
| Python | 系统 | 2.7 |

### KungFu 版本信息

如需了解 journal 文件格式的底层细节（PageHeader、FrameHeader 等），可参考 KungFu 源码：

| 项目 | 值 |
|------|---|
| GitHub 仓库 | https://github.com/kungfu-origin/kungfu |
| Git Tag | `v1.0.0` |
| Git Commit | `ebbc9e21f3b2efad743c402f453d7487d8d3f961` |
| 对应 Release | https://github.com/kungfu-origin/kungfu/releases/tag/1.0.0 |

> 注意：这是 v1.0.0 版本（纯 C++ / Boost.Python），v2.x 架构完全不同，其文档不适用。

### 数据

`data/` 目录包含约 32 分钟的真实 L2 历史数据（15 个 journal 文件，约 1900 万帧，覆盖 ~5000 只股票）。

## 4. 快速启动

```bash
# 编译
mkdir -p build && cd build && cmake .. && make -j

# 一键启动模拟环境（10 倍速回放）
./scripts/run_all.sh 10

# 或者分步启动（三个终端）：
./scripts/start_paged.sh          # 终端 1：Paged 服务
./scripts/start_replayer.sh 10    # 终端 2：回放写入（10 倍速）
# 终端 3：你的程序（先跑 example_factor 验证环境）
cd build && LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost:../lib:. ./example_factor
```

### 已有程序

| 程序 | 说明 |
|------|------|
| `journal_replayer` | 从历史 journal 按时间间隔写入 Paged，模拟实时行情到达 |
| `live_reader` | 通过 Paged 实时读取数据，验证数据链路 |
| `example_factor` | **入门示例**：展示完整的读取→状态维护→因子计算→输出流程（从这里开始读） |

## 5. 数据格式

通过 `JournalReader::createReaderWithSys()` 实时读取帧，每帧通过 `msg_type` 区分数据类型。数据体是 `__attribute__((packed))` 的 C 结构体（定义见 `data_struct.hpp`）。

### 5.1 Tick 快照 (msg_type = 61)

每只股票约每 3 秒一个快照。结构体 `KyStdSnpType`：

| 字段 | 类型 | 说明 |
|------|------|------|
| `Symbol` | int32 | 股票代码（如 600519） |
| `Time` | int32 | 时间，格式 HHMMSSmmm（如 93500000 = 09:35:00.000） |
| `Price` | float | 最新成交价 |
| `Open, High, Low` | float | 当日开高低 |
| `PreClose` | float | 昨收价 |
| `Volume` | int32 | 当笔成交量（本快照周期内） |
| `AccVolume` | int64 | 累计成交量 |
| `AccTurnover` | int64 | 累计成交额 |
| `BidPx1~10` | float | 买一~买十价 |
| `AskPx1~10` | float | 卖一~卖十价 |
| `BidVol1~10` | double | 买一~买十量 |
| `AskVol1~10` | double | 卖一~卖十量 |

### 5.2 逐笔成交 (msg_type = 63)

每笔成交一条。结构体 `KyStdTradeType`：

| 字段 | 类型 | 说明 |
|------|------|------|
| `Symbol` | int32 | 股票代码 |
| `Time` | int32 | 时间 |
| `Price` | float | 成交价格 |
| `Volume` | int32 | 成交数量 |
| `Money` | int32 | 成交金额 |
| `BSFlag` | int8 | 内外盘标识：`'B'`(66)=外盘/主买，`'S'`(83)=内盘/主卖 |
| `AskOrder` | int32 | 卖方订单号 |
| `BidOrder` | int32 | 买方订单号 |

### 5.3 逐笔委托 (msg_type = 62)

每笔委托一条。结构体 `KyStdOrderType`：

| 字段 | 类型 | 说明 |
|------|------|------|
| `Symbol` | int32 | 股票代码 |
| `Time` | int32 | 时间 |
| `Price` | float | 委托价格 |
| `Volume` | int32 | 委托数量 |
| `FunctionCode` | int8 | 委托类型：`'B'`=买入, `'S'`=卖出, `'C'`=撤单 |
| `OrderKind` | int8 | 订单类型 |

### 5.4 交易所判断

```
Symbol < 400000  → 深圳（SZ）
Symbol >= 400000 → 上海（SH）
```

## 6. 因子列表（30 个）

以下因子分 4 个级别。所有公式中，下标 `i` 表示当前帧，`N` 为窗口大小（建议 N=100 个 tick 或 200 笔成交，可自行调整）。

### 符号约定

| 符号 | 含义 |
|------|------|
| `p_i` | 第 i 个 tick 的 `Price` |
| `v_i` | 第 i 个 tick 的 `Volume` |
| `BidPx1_i, AskPx1_i` | 第 i 个 tick 的买一/卖一价 |
| `BidVol_k_i, AskVol_k_i` | 第 i 个 tick 第 k 档的买/卖量 |
| `tp_j, tv_j, tm_j` | 第 j 笔成交的价格/数量/金额 |
| `bs_j` | 第 j 笔成交的内外盘标识 |
| `op_j, ov_j, of_j` | 第 j 笔委托的价格/数量/FunctionCode |

---

### Level 1：基础统计量（8 个）

**1. `vwap`** — 成交量加权平均价

```
vwap = Σ(p_k × v_k, k=i-N+1..i) / Σ(v_k, k=i-N+1..i)
```

**2. `return_N`** — N-tick 收益率

```
return_N = (p_i - p_{i-N}) / p_{i-N}
```

**3. `volatility_N`** — N-tick 波动率

```
r_k = (p_k - p_{k-1}) / p_{k-1}    对 k = i-N+1 .. i
volatility_N = sqrt( Σ(r_k - r̄)² / (N-1) )
其中 r̄ = Σ(r_k) / N
```

**4. `volume_ma_N`** — 成交量 N-tick 滑动均值

```
volume_ma_N = Σ(v_k, k=i-N+1..i) / N
```

**5. `price_range_N`** — N-tick 价格振幅

```
price_range_N = (max(p_k) - min(p_k)) / mean(p_k)    k = i-N+1..i
```

**6. `acc_turnover_ratio`** — 累计换手率（相对当日开盘后总量）

```
acc_turnover_ratio = AccVolume_i / AccVolume_at_first_tick
```

即当前累计成交量相对于当日第一个 tick 时的累计量的增长倍数。不需要流通股本数据。

**7. `bid_ask_spread`** — 买卖价差（相对值）

```
mid = (BidPx1_i + AskPx1_i) / 2
bid_ask_spread = (AskPx1_i - BidPx1_i) / mid
```

**8. `mid_price_return`** — 中间价收益率

```
mid_i = (BidPx1_i + AskPx1_i) / 2
mid_price_return = (mid_i - mid_{i-1}) / mid_{i-1}
```

---

### Level 2：订单簿因子（8 个）

**9. `order_imbalance`** — 前 5 档委托不平衡

```
B5 = Σ(BidVol_k_i, k=1..5)
A5 = Σ(AskVol_k_i, k=1..5)
order_imbalance = (B5 - A5) / (B5 + A5)
```

范围 [-1, 1]。正值表示买方力量更强。

**10. `weighted_order_imbalance`** — 按价格距离加权的委托不平衡

```
w_k = 1 / k    (第 k 档权重，近档权重大)
WB = Σ(w_k × BidVol_k_i, k=1..5)
WA = Σ(w_k × AskVol_k_i, k=1..5)
weighted_order_imbalance = (WB - WA) / (WB + WA)
```

**11. `depth_ratio`** — 全 10 档深度比率

```
depth_ratio = Σ(BidVol_k_i, k=1..10) / Σ(AskVol_k_i, k=1..10)
```

> 1 表示买方挂单量更大。

**12. `order_book_slope_bid`** — 买方订单簿斜率

```
对 k=1..10，令 x_k = BidPx1_i - BidPx_k_i（价格距离），y_k = Σ(BidVol_j_i, j=1..k)（累积量）
order_book_slope_bid = Cov(x, y) / Var(x)
```

即用 OLS 回归拟合"累积挂单量 vs 价格距离"的斜率。斜率大 → 挂单密集。

**13. `price_impact`** — 价格冲击

```
r_k = |p_k - p_{k-1}| / p_{k-1}
price_impact = mean(r_k / (v_k + 1), k=i-N+1..i)
```

单位成交量导致的价格变动。

**14. `kyle_lambda`** — Kyle Lambda（N-tick 滑动均值）

```
kyle_lambda = mean(|r_k| / (v_k + 1), k=i-N+1..i)
```

与 price_impact 计算方式相同，名字不同仅为了对应经典文献命名。

**15. `top_heavy`** — 首档集中度

```
top_heavy = (BidVol1_i + AskVol1_i) / (Σ(BidVol_k_i, k=1..5) + Σ(AskVol_k_i, k=1..5))
```

值越大说明挂单集中在最优价位。

**16. `spread_volatility`** — 价差波动率（N-tick 窗口）

```
s_k = (AskPx1_k - BidPx1_k) / ((AskPx1_k + BidPx1_k) / 2)
spread_volatility = std(s_k, k=i-N+1..i)
```

---

### Level 3：逐笔数据因子（8 个）

以下因子基于逐笔成交（trade）和逐笔委托（order）数据。窗口 M 表示最近 M 笔成交/委托。

**17. `trade_flow_imbalance`** — 成交资金流向不平衡

```
buy_amount = Σ(tp_j × tv_j, 对所有 bs_j == 'B' 的 j, j=最近M笔)
sell_amount = Σ(tp_j × tv_j, 对所有 bs_j == 'S' 的 j, j=最近M笔)
trade_flow_imbalance = (buy_amount - sell_amount) / (buy_amount + sell_amount)
```

**18. `active_buy_ratio`** — 主买成交量占比

```
buy_vol = Σ(tv_j, 对所有 bs_j == 'B', j=最近M笔)
total_vol = Σ(tv_j, j=最近M笔)
active_buy_ratio = buy_vol / total_vol
```

**19. `large_order_ratio`** — 大单成交量占比

```
大单阈值 T = mean(tv_j, j=最近M笔) × 5
large_vol = Σ(tv_j, 对所有 tv_j >= T, j=最近M笔)
large_order_ratio = large_vol / Σ(tv_j, j=最近M笔)
```

"大单"定义为成交量 >= 窗口内平均成交量的 5 倍。

**20. `trade_arrival_rate`** — 成交到达速率

```
trade_arrival_rate = 最近 T 秒内的成交笔数 / T
```

T 建议取 10 秒（基于 `KyStdTradeType::Time` 字段判断时间差）。

**21. `order_cancel_ratio`** — 撤单比例

```
cancel_count = 最近 M 笔委托中 FunctionCode == 'C' 的数量
order_cancel_ratio = cancel_count / M
```

**22. `avg_trade_size`** — 平均成交手数

```
avg_trade_size = mean(tv_j, j=最近M笔)
```

**23. `trade_price_trend`** — 成交价格趋势（线性回归斜率）

```
对最近 M 笔成交 (j=1..M)，令 x_j = j, y_j = tp_j
trade_price_trend = Cov(x, y) / Var(x)
```

正值 → 成交价格在上涨，负值 → 在下跌。

**24. `order_aggressiveness`** — 委托攻击性

```
aggressive_count = 最近 M 笔委托中满足以下条件的数量:
  - 买入委托(FunctionCode=='B') 且 op_j >= AskPx1（穿过卖一价）
  - 或 卖出委托(FunctionCode=='S') 且 op_j <= BidPx1（穿过买一价）
order_aggressiveness = aggressive_count / M
```

需要保存最新的 BidPx1/AskPx1（来自最近的 tick 快照）用于判断。

---

### Level 4：复合因子（6 个）

**25. `amihud_illiquidity`** — Amihud 非流动性指标

```
r_k = |p_k - p_{k-1}| / p_{k-1}
amihud_illiquidity = mean(r_k / (v_k × p_k + 1), k=i-N+1..i)
```

相比 kyle_lambda 多除了 `p_k`，使不同价格水平的股票可比。值越大 → 流动性越差。

**26. `volume_price_divergence`** — 量价背离度

```
price_chg = (p_i - p_{i-N}) / p_{i-N}
volume_chg = (volume_ma_current - volume_ma_prev) / volume_ma_prev

其中:
  volume_ma_current = mean(v_k, k=i-N+1..i)
  volume_ma_prev = mean(v_k, k=i-2N+1..i-N)

volume_price_divergence = price_chg × volume_chg
```

正值 → 量价同向（趋势延续），负值 → 量价背离（趋势可能反转）。

**27. `smart_money_flow`** — 聪明资金净流入（大单主买 - 大单主卖）

```
大单阈值 T = mean(tv_j, j=最近M笔成交) × 5

smart_buy = Σ(tp_j × tv_j, 对所有 bs_j=='B' 且 tv_j >= T)
smart_sell = Σ(tp_j × tv_j, 对所有 bs_j=='S' 且 tv_j >= T)
total_amount = Σ(tp_j × tv_j, j=最近M笔)

smart_money_flow = (smart_buy - smart_sell) / (total_amount + 1)
```

**28. `momentum_reversal`** — 短长期动量对比

```
ret_short = (p_i - p_{i-N}) / p_{i-N}        (短期收益率，N tick)
ret_long = (p_i - p_{i-3N}) / p_{i-3N}       (长期收益率，3N tick)

momentum_reversal = ret_short - ret_long
```

正值 → 短期加速（动量），负值 → 短期减速（可能反转）。

**29. `order_book_pressure_change`** — 订单簿压力变化

```
imb_i = order_imbalance 当前值 (因子 #9)
imb_{i-1} = order_imbalance 上一个 tick 的值
vol_ratio = v_i / volume_ma_N

order_book_pressure_change = (imb_i - imb_{i-1}) × vol_ratio
```

委托不平衡在恶化且放量 → 大正值/大负值，表示买/卖方发力。

**30. `microstructure_alpha`** — 综合微观结构信号

```
imb = order_imbalance (因子 #9)
spread_norm = bid_ask_spread_i / mean(bid_ask_spread_k, k=i-N+1..i)
flow = trade_flow_imbalance (因子 #17)

microstructure_alpha = imb × (2 - spread_norm) × sign(flow)

其中 sign(x) = 1 if x>0, -1 if x<0, 0 if x==0
```

三重确认信号：买方挂单强（imb > 0）+ 价差收窄（spread_norm < 1 → 系数 > 1）+ 资金净流入（flow > 0）→ 看涨。

---

## 7. 实现要求

1. **流式计算**：每收到一帧数据增量更新
2. **延迟测量**：在代码中用 `std::chrono::high_resolution_clock` 测量每帧的因子计算耗时，输出平均/P99 延迟
4. **输出格式**：每个 tick 到达时，输出该股票当前的 30 个因子值以及生成完所有因子的latency。（需落盘为文件以供验证）
5. **代码结构**：因子应可插拔——通过注册/配置新增因子，不需要修改主循环

### 加分项

- Python binding（pybind11 / ctypes/ cython），让 Python 能访问 C++ 引擎实时生成的因子
- 性能优化使全市场 5000 股不掉帧
- 增量统计量（如 Welford 在线方差算法）替代朴素 O(N) 窗口遍历
- 单元测试

## 8. 交付物

1. 源代码
2. 编译和运行说明
3. 简要设计文档：数据结构设计、窗口管理方案、因子注册机制、延迟测量结果
4. 运行输出样本（证明因子在模拟环境中正确计算）

## 9. 时间

**一周**

## 10. 参考文件

| 文件 | 说明 |
|------|------|
| `example_factor.cpp` | **从这里开始**：完整的读取→状态维护→因子计算示例 |
| `data_struct.hpp` | KungFu 原始数据结构定义（packed 结构体） |
| `insight_types.h/.cpp` | 转换后的 L2 数据结构和转换函数 |
| `sys_messages.h` | 消息类型常量（61=TICK, 62=ORDER, 63=TRADE） |
| `live_reader.cpp` | 实时读取的完整示例 |
| `journal_replayer.cpp` | 回放写入的实现 |
