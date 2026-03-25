# realtime_factors 设计稿

本文档描述一个基于 KungFu journal 的实时因子计算框架第一版设计。目标是复刻并实时化 `realtime_factors/demo_backfill_ht2.py` 中的 `cat1` 因子逻辑：

- 输入：journal 中的 L2 `order` / `trade` 数据
- 处理：边读边维护订单状态、成交关联、撤单事件、统计矩
- 输出：在固定时点产出每只股票的因子快照

文档重点分两部分：

1. demo 里到底算了哪些因子
2. 这些因子如果改成实时框架，应该怎样组织

---

## 1. 先说结论：demo 在做什么

这个 demo 的核心思想不是“直接写一批因子公式”，而是：

1. 先把逐笔委托、逐笔成交、逐笔撤单整理成统一事件流
2. 对每类事件提取若干基础字段，比如量、金额、收益率、时间差
3. 按股票、按条件分桶持续累计统计矩（1 到 4 阶矩）
4. 到固定时点，把统计矩转换成均值、波动、偏度、峰度、计数类因子

换句话说，demo 更像一个“日内事件统计因子引擎”。

它当前只实现了 `cat1`：

- 从开盘后开始累计历史
- 在多个固定时点输出一版快照
- 每次输出都是“从早上累到当前时刻”的历史累计值，不是滚动窗口值

输出时点为：

- `09:25`
- `10:00`
- `10:30`
- `11:00`
- `11:30`
- `13:30`
- `14:00`
- `14:30`
- `15:00`

---

## 2. demo 里的因子族

demo 将事件分成 7 个大类，每个大类各算一套因子。

### 2.1 买卖委托因子

- `buy_order_*`
- `sell_order_*`

含义：统计新增买单、新增卖单本身的特征。

### 2.2 买卖撤单因子

- `buy_withdraw_*`
- `sell_withdraw_*`

含义：统计撤掉的买单、撤掉的卖单的特征。

### 2.3 成交因子

- `both_trade_*`：所有成交
- `buy_trade_*`：主动买方向成交
- `sell_trade_*`：主动卖方向成交

含义：统计成交本身，以及成交所对应的买卖原始订单属性。

这里的“buy/sell trade”不是简单按成交记录分组，而是按成交的方向字段分组。

---

## 3. 每类事件到底提取了什么基础特征

为了好理解，可以把 demo 因子拆成两层：

- 第一层：基础事件字段
- 第二层：对这些字段做统计

### 3.1 订单事件 order 的基础字段

对每一条委托，demo 最终使用这些字段：

- `ordervol`：委托量
- `ordermoney = orderpx * ordervol`：委托金额
- `orderret = orderpx / pre_close - 1`：委托价相对昨收收益率
- `orderturnover = ordermoney / circulating_market_cap`：委托金额占流通市值比例

所以 order 因子本质上是在问：

- 今天股票被挂出了多大的单
- 这些单挂在离昨收多远的位置
- 这些单相对于股票流通盘有多大

### 3.2 撤单事件 withdraw 的基础字段

对每一条撤单，demo 使用：

- `withdrawvol`：撤单量
- `withdrawmoney = withdrawpx * withdrawvol`：撤单金额
- `withdrawret = withdrawpx / pre_close - 1`：撤单价相对昨收收益率
- `withdrawturnover = withdrawmoney / circulating_market_cap`
- `withdrawtmlag = withdrawtm - ordertm`：从原始下单到撤单的时间差（按连续交易时钟修正后计算）

这个事件更偏“行为刻画”：

- 有多少单被撤
- 撤掉的是大单还是小单
- 是挂了很久才撤，还是刚挂出去就撤

### 3.3 成交事件 trade 的基础字段

对每一条成交，demo 使用：

- `tradevol`：成交量
- `trademoney = tradepx * tradevol`：成交金额
- `traderet = tradepx / pre_close - 1`：成交价相对昨收收益率
- `tradeturnover = trademoney / circulating_market_cap`
- `ordertmlag = |ordertm_buy - ordertm_sell|`：撮合买卖两侧原始订单的时间差
- `orderretdiff = |orderret_buy - orderret_sell|`：成交双方原始委托收益率差
- `orderturnoverdiff = |orderturnover_buy - orderturnover_sell|`：成交双方原始委托规模差
- `tradebuyid` / `tradesellid` 的唯一数目：参与成交的独立买单/卖单个数

这些字段很有代表性：

- `traderet` 看成交价格位置
- `ordertmlag` 看一笔成交是否由“时间上接近的订单”撮合出来
- `orderretdiff` 看买卖双方挂价分歧
- `orderturnoverdiff` 看买卖双方订单规模是否匹配
- 唯一订单数看成交是集中在少数大单，还是分散在许多小单

---

## 4. demo 对这些基础字段做了哪些统计

这是 demo 最核心的地方。

它不是保存所有历史事件再临时聚合，而是对每个字段累计以下统计量：

- `count`：样本数
- `s1 = Σx`
- `s2 = Σx^2`
- `s3 = Σx^3`
- `s4 = Σx^4`

然后在输出时再还原成常见统计特征。

### 4.1 均值

对任意字段 `x`：

```text
mean(x) = s1 / count
```

### 4.2 标准差

```text
var(x) = (s2 - s1^2 / n) / (n - 1)
std(x) = sqrt(max(var(x), 0))
```

其中 `n = count`。

### 4.3 偏度

记：

```text
mu = s1 / n
vp = s2 / n - mu^2
sp = sqrt(vp)
```

则偏度近似为：

```text
skew(x) = (s3 / n - 3 * mu * (s2 / n) + 2 * mu^3) / sp^3
```

直观理解：

- 偏度 > 0：右尾更长，大值偶尔出现
- 偏度 < 0：左尾更长，小值偶尔出现

### 4.4 峰度

```text
kurt(x) = (s4 / n - 4 * mu * (s3 / n) + 6 * mu^2 * (s2 / n) - 3 * mu^4) / sp^4 - 3
```

直观理解：

- 峰度高：尖峰厚尾，更容易出现极端值

### 4.5 VWAPX

demo 对所有 `money_mean / vol_mean` 再额外构造：

```text
vwapx = mean(money) / mean(vol)
```

因为：

```text
mean(money) / mean(vol)
= (Σmoney / n) / (Σvol / n)
= Σmoney / Σvol
```

所以它本质上是这组事件的成交/委托/撤单加权均价。

最后再进一步转成相对昨收收益率。

---

## 5. demo 里有哪些分桶条件

为了让同一类事件能区分不同风格，demo 不只算全样本统计，还会做条件分桶。

### 5.1 大单 / 小单

阈值如下：

```text
large: vol >= 100,000  或 money >= 1,000,000
small: (vol <= 10,000 且 money <= 100,000)
    或 (vol <= 1,000 且 money <= 500,000)
```

注意这里不是严格互补，存在中间区域不属于 large/small 的情况。

### 5.2 撤单专属分桶

撤单额外分成：

- `complete`：完全撤单，`withdrawvol == ordervol`
- `partial`：部分撤单，`withdrawvol < ordervol`
- `instant`：瞬时撤单，`withdrawtmlag` 在 0 到 3000ms 内

这些分桶很重要，因为它们对应不同交易行为：

- `complete`：整笔单都撤了
- `partial`：只撤掉一部分，可能是边挂边撤
- `instant`：刚挂就撤，更接近试探性下单或高频行为

### 5.3 成交专属分桶

成交除了对自己按大/小单分桶，还按“对应的买方原单、卖方原单”分桶：

- `large` / `small`：按 `tradevol`、`trademoney`
- `bolarge` / `bosmall`：按买方原始订单 `ordervol_buy`、`ordermoney_buy`
- `solarge` / `sosmall`：按卖方原始订单 `ordervol_sell`、`ordermoney_sell`

这相当于把成交拆成三种观察角度：

- 成交本身大不大
- 这笔成交来自大买单还是小买单
- 这笔成交来自大卖单还是小卖单

---

## 6. 因子名字怎么理解

虽然 demo 没有单独列出一份因子字典，但名字基本可以按下面的方法读。

### 6.1 例子一：`buy_order_ordervol_mean_large_cat1`

拆开看：

- `buy_order`：买单事件
- `ordervol`：统计字段是委托量
- `mean`：统计量是均值
- `large`：只统计大单
- `cat1`：日内历史累计型因子

含义：截至当前 TTS 时点，股票今天所有大买单的平均委托量。

### 6.2 例子二：`both_trade_orderretdiff_std_cat1`

- `both_trade`：所有成交
- `orderretdiff`：成交两侧原始订单价格偏离差
- `std`：标准差

含义：截至当前时点，这只股票成交中“买卖双方报价分歧”的离散程度。

### 6.3 例子三：`sell_withdraw_withdrawtmlag_mean_instant_cat1`

- `sell_withdraw`：卖撤单
- `withdrawtmlag`：撤单等待时间
- `mean`：均值
- `instant`：瞬时撤单样本

这个名字稍绕，因为 `instant` 自己已经是小于 3 秒的样本，所以这个因子通常更像是在描述“瞬时撤单内部的平均等待时长”。

### 6.4 例子四：`buy_trade_trade_buyorder_count_cat1`

含义：截至当前时点，主动买成交中，参与成交的独立买单数量。

这是计数类而不是矩类。

---

## 7. 如果用实时 journal 做，这些因子该怎么构建

这里给出一个面向第一版可落地实现的框架。

## 7.1 总体原则

第一版不要追求“通用因子 DSL”，先把 demo 逻辑一比一实时化：

- 先能稳定实时跑
- 先能和 demo 回放结果对拍
- 先把订单关联和事件构建做正确

因为这个项目里最难的不是公式，而是：

- 上交所 / 深交所逐笔格式不同
- 成交和订单需要关联
- 撤单在不同交易所表达方式不同
- 订单需要跨分钟持续保存在内存里

---

## 7.2 推荐模块划分

建议把 `realtime_factors` 拆成下面几层。

### 模块一：JournalReader

职责：

- 复用现有 KungFu journal 读取逻辑
- 订阅 `tick/order/trade` 中需要的频道
- 这里只关注 `order` 与 `trade`
- 保证按 frame 到达顺序处理

建议复用：

- 主项目里的 `JournalReader::createReaderWithSys()` 模式
- `live_reader` / `realtime_parquet` 里已有的消息分发框架

### 模块二：Normalizer

职责：把 SH/SZ 原始逐笔结构体转成统一内部事件格式。

统一后的订单事件建议至少包含：

```text
exchange
channel_id
order_id
symbol
order_tm
side
order_type
order_px
order_vol
```

统一后的成交事件建议至少包含：

```text
exchange
channel_id
symbol
trade_tm
trade_px
trade_vol
trade_buy_id
trade_sell_id
side_flag
```

这里要注意 demo 里的两个交易所差异：

- SH：撤单来自 order 流中的撤单记录
- SZ：撤单主要从 trade 流里由 `买号为 0 / 卖号为 0` 识别出来

所以 `Normalizer` 不只是字段改名，还负责把交易所特有编码映射成统一语义。

### 模块三：StaticDataProvider

职责：在开盘前或启动时一次性加载日级静态数据。

至少需要：

- `pre_close[symbol]`
- `circulating_market_cap[symbol]`
- `universe`

原因：

- 没有 `pre_close`，算不出 `*_ret`
- 没有流通市值，算不出 `*_turnover`

### 模块四：OrderStateStore

这是整个实时框架最核心的状态层。

建议维护：

```text
key = (channel_id, order_id)
value = {
    symbol,
    side,
    order_type,
    order_tm,
    order_px,
    original_vol,
    remaining_vol,
    order_money,
    order_ret,
    order_turnover,
}
```

职责：

- 收到新订单时建档
- 收到成交时找到买卖两侧原始订单
- 收到撤单时找到被撤的原始订单
- 更新 `remaining_vol`
- 完全成交或完全撤销后将订单移出活动状态

这是实时替代 demo 中这几块 DataFrame 状态的版本：

- `processed_buy_order_sh`
- `processed_sell_order_sh`
- `processed_buy_order_sz`
- `processed_sell_order_sz`
- `latest_trade_px_sz`

### 模块五：DerivedEventBuilder

职责：由原始 `order/trade` 构造 3 类标准化派生事件：

- `OrderFeatureEvent`
- `TradeFeatureEvent`
- `WithdrawFeatureEvent`

#### 新订单到来时

生成 `OrderFeatureEvent`，字段包括：

- `ordervol`
- `ordermoney`
- `orderret`
- `orderturnover`

并根据买卖方向进入 `buy_order` 或 `sell_order` 因子簇。

#### 成交到来时

通过 `tradebuyid` 和 `tradesellid` 到 `OrderStateStore` 查两侧原始订单，构造：

- `tradevol`
- `trademoney`
- `traderet`
- `tradeturnover`
- `ordertmlag`
- `orderretdiff`
- `orderturnoverdiff`

并更新参与成交订单的剩余量。

#### 撤单到来时

通过被撤订单 id 回查原单，构造：

- `withdrawvol`
- `withdrawmoney`
- `withdrawret`
- `withdrawturnover`
- `withdrawtmlag`

并根据 `withdrawvol == ordervol` / `< ordervol` 标记 `complete` / `partial`。

### 模块六：MomentAccumulator

职责：对每只股票、每个因子簇、每个条件桶，维护累计统计矩。

建议键结构：

```text
[symbol][factor_group][bucket][field] -> Moments
```

其中 `Moments` 为：

```text
count
sum1
sum2
sum3
sum4
```

每来一条事件 `x`，直接做：

```text
count += 1
sum1  += x
sum2  += x^2
sum3  += x^3
sum4  += x^4
```

优点：

- 单条事件更新是 O(1)
- 不需要保留所有历史样本
- 很适合日内累计型因子

### 模块七：UniqueCounter

成交因子里有“独立买单数 / 独立卖单数”。

第一版建议直接维护精确集合：

```text
[symbol][factor_group][bucket].buy_order_ids
[symbol][factor_group][bucket].sell_order_ids
```

到输出时取集合大小。

理由：

- 第一版先保证正确性
- A 股单日逐笔量虽然大，但按股票拆分后通常可控

如果后续内存压力大，再评估 HyperLogLog 一类近似计数。

### 模块八：SnapshotScheduler

职责：在固定时点触发快照输出。

建议第一版完全复刻 demo 的 TTS：

- `09:25`
- `10:00`
- `10:30`
- `11:00`
- `11:30`
- `13:30`
- `14:00`
- `14:30`
- `15:00`

触发时：

1. 遍历所有股票的累计矩
2. 转成 `mean/std/skew/kurt/vwapx/count`
3. 拼成一张因子截面表
4. 输出到文件或 downstream

---

## 8. 实时框架中的关键难点

## 8.1 不是所有 feature 都能直接从单条成交看出来

比如：

- `ordertmlag`
- `orderretdiff`
- `orderturnoverdiff`
- `withdrawtmlag`

都依赖“回查原始订单”。

所以实时因子框架的核心不是简单的流式 group-by，而是：

`逐笔成交/撤单 -> 回查订单状态 -> 生成派生特征事件 -> 更新统计量`

## 8.2 SZ 市价单价格补全

demo 中深市订单会出现无法直接得到价格的情况，因此使用最近成交价来回填：

- 买单补价时参考最近卖向成交
- 卖单补价时参考最近买向成交

实时版同样需要一个：

```text
latest_trade_px_sz[symbol][side]
```

否则很多 `orderret`、`ordermoney`、`orderturnover` 都算不出来。

## 8.3 SH aggressive order 补全

demo 对上交所会用成交记录去补 aggressive order 的量价。这说明：

- 原始订单流并不总能完整表达最终可用于统计的订单画像
- 有些订单要结合成交信息才能补齐

实时版建议：

- 收到 trade 时若对应 order state 缺失，允许建立临时占位记录
- 后续若 order 再到达，则做 reconcile
- 第一版可以先记录告警计数，确保问题可观测

## 8.4 活跃订单状态要及时清理

如果订单完全成交或完全撤销后仍保留在活动状态，内存会持续膨胀。

所以必须维护：

- `remaining_vol`
- 完成后删除或移入已结束状态

这相当于实时版的 `_remove_matched_orders`。

---

## 9. 第一版建议实现范围

建议先做一个最小可用版本，不要一开始就泛化过度。

### V1 范围

- 只做 `cat1`
- 只做和 demo 一致的 7 组因子簇
- 只支持日内历史累计，不做滚动窗口
- 只输出固定 TTS 快照
- 输出格式先用 Parquet 或 CSV 均可
- 先和 `demo_backfill_ht2.py` 单日单时点对拍

### V1 不急着做

- 多日跨日状态续跑
- 通用公式配置化
- 动态注册任意新因子
- 近似去重计数
- 跨窗口增量衰减统计

---

## 10. 推荐代码结构

下面给一个偏 C++ 工程化的目录建议，仅作为第一版参考。

```text
realtime_factors/
  README.md
  include/
    factor_types.hpp          # 统一事件、状态、输出结构
    static_data.hpp           # pre_close / market_cap / universe
    order_state_store.hpp     # 订单状态表
    event_normalizer.hpp      # SH/SZ 原始结构转内部事件
    event_builder.hpp         # order/trade/withdraw 派生事件生成
    moment_accumulator.hpp    # 统计矩维护
    factor_snapshot.hpp       # 矩 -> 因子快照
    scheduler.hpp             # TTS 触发
  src/
    main.cpp
    static_data.cpp
    order_state_store.cpp
    event_normalizer.cpp
    event_builder.cpp
    moment_accumulator.cpp
    factor_snapshot.cpp
    scheduler.cpp
```

如果第一版想更快落地，也可以先做成：

- 一个 `main.cpp`
- 几个 `.hpp` 模块

先把状态和处理链跑通，再逐步拆分。

---

## 11. 输出接口建议

建议快照输出至少包含：

- `datetime`
- `code`
- 各数值因子列

命名上建议继续沿用 demo 的风格：

```text
<group>_<field>_<stat>[_<bucket>]_cat1
```

例如：

- `buy_order_ordervol_mean_cat1`
- `buy_order_orderret_std_large_cat1`
- `both_trade_orderturnoverdiff_mean_cat1`
- `sell_withdraw_withdrawtmlag_mean_instant_cat1`

这样好处是：

- 能直接和 demo 对拍
- 研究员也容易从名字反推定义

---

## 12. 验证方案

第一版最重要的是“结果能否对齐 demo”。

建议分 3 层验证。

### 12.1 单事件级验证

抽一只股票、几分钟数据，检查：

- order 是否正确入状态表
- trade 是否正确关联到买卖原单
- withdraw 是否正确识别 complete / partial / instant

### 12.2 单股票统计级验证

对单只股票、单个 TTS：

- 对比 `count`
- 对比 `mean`
- 对比 `std`
- 对比 `vwapx`

先对这些低风险统计量，再看 `skew/kurt`。

### 12.3 全市场截面对拍

用 `scripts/replay` 回放某天 journal：

1. 运行实时框架产出 TTS 快照
2. 运行 `demo_backfill_ht2.py` 产出同日同 TTS 快照
3. 按列比对误差分布

重点检查：

- 缺失率
- 完全相等比例
- 浮点误差范围
- SH/SZ 分市场差异

---

## 13. 为什么这个设计适合实时化

因为 demo 本身已经非常接近流式框架了，只是实现上用了 DataFrame 做离线回放。

它适合实时化的原因有三点：

- 因子是累计统计矩，天然支持增量更新
- 输出是固定 TTS 快照，不要求每笔都全量重算
- 大部分复杂度来自订单关联，而不是复杂模型

所以第一版最合理的路线不是“重新发明一套因子体系”，而是：

`把 demo 的事件定义 + 分桶规则 + 统计矩逻辑，搬到 C++ 实时状态机里`

---

## 14. 一页版总结

如果只看最简版，可以记住下面这几句话：

- demo 不是直接算若干公式，而是在算订单、撤单、成交三类事件的统计分布
- 每类事件按股票累计 `count/sum/sum2/sum3/sum4`
- 输出时还原成 `mean/std/skew/kurt/vwapx/count`
- 成交和撤单因子依赖“回查原始订单”，这是实时框架核心
- 第一版只要复刻 `cat1`，并和 demo 对拍成功，就已经是很好的起点

---

## 15. 下一步建议

建议按下面顺序推进实现：

1. 先画出内部统一事件结构与 `OrderState` 结构
2. 先完成 SH/SZ 的订单、成交、撤单标准化与关联
3. 先只实现 `count/mean/vwapx`，跑通一版实时快照
4. 再补 `std/skew/kurt`
5. 最后做和 demo 的逐时点对拍

如果后续继续推进，下一份文档建议写成《realtime_factors V1 模块接口草案》，把每个类的输入输出字段直接定死，便于开始写 C++ 代码。
