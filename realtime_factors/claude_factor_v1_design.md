# Realtime Factors V1 设计方案 (基于 README 架构)

## 1. 架构总览

本方案严格遵循 `README.md` 中的「进程内增量同步 + 共享内存快照池」架构，不使用 `fork()`。整个系统由多个紧密协作的组件构成：

1. **TS-Engine (时序引擎)**：
   - **单线程主循环**：负责通过 Journal 读取全市场 L2 Order/Trade 事件。
   - **订单状态管理 (OrderStateStore)**：完整复刻 Python demo 逻辑，处理沪深两市不同规则（撤单识别、市价单补价、激进单补全）。
   - **时序更新**：每处理完一个 symbol 的事件，更新其统计矩（Moments），计算最新的时序因子。
   - **Delta 产出**：将该 symbol 更新后的全量时序因子行，作为一条 Delta `(seq_id, symbol, ts_feature_vector)` 写入私有的 **Delta Ring Buffer**。

2. **Snapshot-Composer (快照编排器线程)**：
   - **回放 Delta**：独立线程，从 Delta Ring Buffer 读取增量，更新本地的一致性快照镜像。
   - **时序与截面合成**：在确定需要发布快照时，将镜像写入 Snapshot Ring Buffer 槽位的 **TS Region**。同时，基于这些时序数据，计算截面因子（zscore, srank），写入 **CS Region**。
   - **原子发布**：采用无锁探测逻辑 (`ProbeSeq`) 和原子序号更新，供下游零拷贝读取。

3. **Delta Ring Buffer**：固定容量的分段环形缓冲区，用于 TS-Engine 和 Snapshot-Composer 的无锁增量传递。

4. **Snapshot Ring Buffer (共享内存)**：包含 N 个 Slot，采用槽位引用计数 (Pin/Unpin) 保证读写安全。

## 2. 因子计算范围

### 2.1 时序因子 (与 `demo_backfill_ht2.py` 对齐)
包含 7 个事件族及其对应的分桶逻辑：
- `buy_order`, `sell_order`: ALL, LARGE, SMALL
- `buy_withdraw`, `sell_withdraw`: ALL, LARGE, SMALL, COMPLETE, PARTIAL, INSTANT
- `both_trade`, `buy_trade`, `sell_trade`: ALL, LARGE, SMALL, BOLARGE, BOSMALL, SOLARGE, SOSMALL

每个族/桶计算基础的统计矩：`count`, `mean`, `std`, `skew`, `kurt`, 并衍生 `vwapx`。
为了极致性能，使用 `double` 单趟累加算法。允许与 Python (Float64) 对比存在 `1e-9` 内的浮点误差。

### 2.2 截面因子 (新增)
在 Snapshot-Composer 线程中，基于写入 TS Region 前的全市场切片，计算简单的截面特征，例如：
- `srank`：因子值在全市场的横截面排序 (0~1)。
- `zscore`：截面去极值和标准化 ( $(x - \mu) / \sigma$ )。
**V1 示例因子**：选几个最具代表性的时序因子（如 `both_trade_traderet_mean_large_cat1`）做截面衍生。

## 3. 核心状态与数据结构设计

### 3.1 OrderStateStore (严格复刻)
维持一个哈希表跟踪活动订单：
- **SH 规则**：撤单从 order 流显式读取 (`FunctionCode == 'C'`/68)，激进单(Aggressive Order)可能在委托流缺价缺量，需在成交流中通过 `tradebuyid/tradesellid` 追溯补全。
- **SZ 规则**：撤单从 trade 流识别 (`tradebuyid == 0` 或 `tradesellid == 0`)；市价单需拿最新成交价回填。

### 3.2 纯累加统计矩 (Moments)
使用 O(1) 更新的统计矩维护器，不保留历史数据流，只需：
```cpp
struct Moments {
    int64_t count = 0;
    double s1 = 0, s2 = 0, s3 = 0, s4 = 0;
    void update(double x);
    // mean, std, skew, kurt 按数学公式直接得出
};
```

## 4. 共享内存布局 (Snapshot Slot)
每个 Slot 结构如下：
- `Header`: 逻辑版本号 `seq`，输入数据截止时间戳，引用计数等。
- `TS Region (行优先)`: `double ts_factors[MAX_SYMBOLS][NUM_TS_FACTORS];`，便于按股票查全量。
- `CS Region (列优先)`: `double cs_factors[NUM_CS_FACTORS][MAX_SYMBOLS];`，便于 Python 取整列。

## 5. 验证策略
采用**单元测试优先**策略：
1. 先构造明确的 mock 数据流（包含 SH 撤单、SZ 撤单、缺价单等 edge case）。
2. 将数据送入 C++ 的 TS-Engine 和 Python 脚本，对比 O(1) Moment 和 OrderStateStore 的中间状态。
3. 单元测试覆盖 `Snapshot-Composer` 模拟并发回放与截面计算逻辑。
4. 跑通后，再接真实 journal (利用 `scripts/replay`) 和 `demo_backfill_ht2.py` 的落盘文件做全面对拍验证。