# realtime_kbar 实时发布设计

## 1. 目标

本文档描述当前 `realtime_kbar` 的实时发布逻辑。

设计目标有两个：

- 主文件尽可能低延迟地产出，方便盘中实时消费
- 最终结果允许通过 patch 收敛，尽量贴近 JQ 口径

当前实现不再试图通过“判断交易所一轮 push 已经完整结束”来触发主发布，而是采用更直接的秒级触发方案：

- SH `exchange_time` 进入目标分钟的 `01` 秒时，立刻发布该分钟的主文件
- 之后若迟到 tick 继续影响该分钟，则输出 patch 文件修正

也就是：

- 主发布：`sec01_flush`
- 最终修正：`patch`

## 2. 为什么选择这个方案

### 2.1 不能依赖“交易所一轮 push 完整”来 flush

一开始考虑过这样的思路：

- 跟踪 SH / SZ 的一轮 push
- 当某轮 push 的股票覆盖数达到预期值时，再 flush 该分钟

这个方向最终放弃了，原因是实际数据不支持稳定实现。

根据实盘 replay 和离线分析：

- 交易所一轮 tick push 本身不稳定
- 某一轮 push 覆盖到的股票数不是常数
- 有些股票在某一轮里本来就不会出现
- 固定按 3 秒或某个 round 去做“全股票到齐”判断，不可靠

因此，没法通过“比对所有股票已经推送了”来安全地决定 flush 时刻。

### 2.2 不能在 `exchange_time` 到 `00` 秒就 flush

另一个看起来更直接的方案是：

- 只要看到下一分钟 `00` 秒的数据，就 flush 上一分钟

这个也不够安全。

原因是 `arrival time` 和 `event time` 不一致：

- `arrival time`：journal 的真实到达顺序，由 `nano_timestamp` 决定
- `event time`：tick 的 `exchange_time`，也就是 `Time`

实盘数据里会出现：

- 按 `nano_timestamp` 排序时，已经收到下一分钟 `00` 秒的数据
- 后面仍然会夹杂少量上一分钟 `59` 秒的 tick

所以如果 `exchange_time` 一到 `00` 秒就 flush，就会偏早。

### 2.3 为什么选择 `01` 秒 flush

当前方案使用的经验事实是：

- 虽然会出现“见到 `00` 秒后，后面仍有上一分钟 `59` 秒 tick”
- 但在当前 replay 样本里，没有出现“已经见到下一分钟 `01` 秒后，后面还继续出现上一分钟 `59` 秒 tick”

因此选择：

- 以 SH 的 `exchange_time` 到达目标分钟 `01` 秒，作为主发布触发

这个规则的优点是：

- 延迟足够低，盘中大约 1 秒出头即可发布
- 不依赖不稳定的 round 覆盖数判断
- 比 `00` 秒更安全
- 即使后面仍有少量异常晚到 tick，也可以由 patch 机制修正

## 3. 当前实现概览

当前实现可以概括成两层：

1. `TickBarBuilder / StockBarAccumulator`
   负责按 JQ 口径正确生成单股票分钟 bar

2. `MinutePublishCoordinator`
   负责按分钟发布主文件和 patch 文件

职责分离如下：

- builder 负责“bar 算得对”
- coordinator 负责“什么时候发、怎么修正”

## 4. 发布模型

### 4.1 主文件

每个分钟生成一个主文件：

- `kbar_HHMM.csv`

例如：

- `kbar_0931.csv`
- `kbar_0940.csv`

主文件内容是该分钟当前的全市场快照。

### 4.2 Patch 文件

如果主文件发布后，后续 tick 继续影响该分钟，则输出 patch：

- `kbar_patch_HHMM_vN.csv`

例如：

- `kbar_patch_0932_v2.csv`
- `kbar_patch_0940_v3.csv`

patch 文件只包含被更新的股票，不重写整分钟全量文件。

### 4.3 版本语义

- 主文件固定视为 `version=1`
- 后续 patch 从 `version=2` 开始递增

## 5. sec01_flush 规则

### 5.1 触发条件

对于 SH tick：

- 取其 `exchange_time` 对应的 `hhmm`
- 取其 `exchange_time` 对应的秒 `ss`
- 若 `hhmm` 属于有效交易分钟，且 `ss == 1`
- 则发布该 `hhmm` 对应分钟的主文件

当前代码中的有效区间为：

- `09:31 ~ 11:30`
- `13:01 ~ 15:00`

触发函数名：

- `sec01_flush_second_for_bar()`

触发日志中的标记：

- `trigger=sec01_flush`

### 5.2 为什么只看 SH

主触发信号只看 SH，是因为：

- 这条经验规律是先在 SH 数据上验证出来的
- flush 的动作虽然是发布全市场主文件，但触发锚点使用 SH

也就是说：

- SH 负责给出发布时间信号
- 主文件内容包含 SH + SZ 全市场当前快照

## 6. Patch 机制

### 6.1 为什么仍然需要 patch

即使已经用 `sec01_flush`，仍然会有少量迟到数据：

- 某些股票在主文件发布时，该分钟 bar 尚未生成
- 或该分钟 bar 后续又被迟到 tick 更新

这在 opening 和盘中都可能发生，只是 opening 相对更多一点。

所以 `sec01_flush` 不是“最终版发布”，而是“低延迟主版本发布”。

### 6.2 Patch 里会出现什么

patch 只写被更新的股票，常见情况有两类：

- `insert`
  主文件里没有这只股票该分钟的 bar，后面才补出来
- `update`
  主文件里已有 bar，但数值又被迟到 tick 更新

当前 CSV patch 文件里没有额外写 `change_type` 字段，而是直接输出更新后的最新 bar 行。

### 6.3 Patch flush 节流

当前实现不是每来一条迟到 tick 就立刻写一个 patch 文件，而是：

- 对已发布分钟维护 `dirty_symbols`
- 首次变脏时记录 `dirty_since_ns`
- 当脏数据累计超过 1 秒后，再统一 flush 一个 patch 文件

这样可以避免过于碎片化的小文件。

## 7. 为什么采用“按分钟保存所有股票状态”

当前实现不再只保留“每只股票上一个 bar 的状态”，而是保留：

- 每只股票全天 tick 历史
- 每只股票已生成的分钟 bar 状态
- 每个分钟的已发布快照

这样做的原因是：

- 一天内分钟数有限
- 股票数有限
- 内存完全可接受
- 这样能自然支持迟到 tick 对历史分钟的修正

这带来两个直接收益：

1. 某条 tick 即使迟到较久，只要还落在当天交易时段，系统仍然可以重放该股票并修正受影响分钟
2. patch 输出不依赖“分钟状态已经丢弃”，而是始终可以对历史分钟做 upsert

## 8. 当前状态机

每个分钟维护一个 `MinutePublishState`，状态很简单：

- `BUILDING`
  还没发布主文件
- `PRELIM_PUBLISHED`
  已发布主文件，后续允许 patch

当前实现没有再引入单独的 `FINAL_CLOSED` 状态。

原因是当前策略就是：

- 只处理单日数据
- 分钟状态全天保留
- 当天结束或进程退出时统一 finalize

这和“按分钟保存所有股票状态”的总体方向是一致的。

## 9. 日志语义

当前实现的核心日志有三类。

### 9.1 主文件发布日志

示例：

```text
[minute_publish] minute=0931 status=preliminary version=1 trigger=sec01_flush stocks_published=5229
```

含义：

- `minute`：分钟标签
- `version=1`：主文件版本
- `trigger=sec01_flush`：由 SH 的 `01` 秒触发
- `stocks_published`：主文件行数

### 9.2 Patch 日志

示例：

```text
[minute_patch] minute=0932 version=3 dirty_symbols=1
```

含义：

- `version`：patch 版本号
- `dirty_symbols`：本次 patch 覆盖的股票数量

### 9.3 Finalize 日志

如果某分钟直到进程结束都还没被 `sec01_flush` 触发，则在退出时会看到：

```text
[minute_publish] minute=0957 status=preliminary version=1 trigger=finalize stocks_published=5207
```

这表示：

- 该分钟没有等到正常的 `sec01_flush`
- 最终由 `finalize_all()` 输出

## 10. 当前已验证结论

基于本地 replay，当前代码已经验证出：

- `09:31 ~ 11:30`、`13:01 ~ 15:00` 统一使用 `sec01_flush` 是可行的
- opening 阶段也可以直接走 `sec01_flush`
- opening 会有少量 patch，但量级可接受
- 盘中主文件规模稳定在接近全市场水平
- patch 多为少量迟到股票的补发或更新

因此，当前结论是：

- `sec01_flush + patch`
  是目前这份数据上最简单、最稳妥、最贴近实际的方案

而不是：

- 复杂的 `expected_sh_count`
- `round_count_complete`
- 或固定 round 覆盖数判断

## 11. 与旧设计的差异

旧设计曾尝试：

- 学习 `expected_sh_count`
- 跟踪 SH round
- 以 round 覆盖数达到阈值触发主发布

当前实现已经去掉这些逻辑，原因是：

- round 覆盖数不稳定
- 不适合作为生产触发器
- 会引入不必要复杂度

当前版本只保留一个触发原则：

- `sec01_flush`

所有剩余误差统一由 patch 吸收。
