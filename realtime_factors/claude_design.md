# realtime_factors 系统设计

本文档描述一个低延迟、流式实时因子计算框架的完整设计。目标是将 `demo_backfill_ht2.py` 中的因子逻辑实时化，同时为时序因子和截面因子提供统一的架构基础。

---

## 1. 设计目标

| 目标 | 说明 |
|------|------|
| **实时流式输出** | 因子值在每条 L2 事件处理后立即更新到共享内存，非定时快照 |
| **双因子类型** | 支持时序因子（per-symbol 累计统计）和截面因子（cross-symbol 排名/标准化） |
| **低延迟** | 时序因子：事件到达 → 因子更新 < 10µs；截面因子：持续 fork 刷新，新鲜度 < 5ms |
| **共享内存接口** | 因子通过 POSIX shared memory 暴露，Python 下游可零拷贝读取 |
| **定时落盘** | 每 N 分钟将当前因子快照持久化为 Parquet，供盘后复盘 |
| **与 demo 对拍** | 第一版必须能和 `demo_backfill_ht2.py` 逐时点对齐 |

---

## 2. 架构总览

```
┌────────────────────────────────────────────────────────────────────┐
│                        C++ 主进程 (单线程事件循环)                    │
│                                                                    │
│  ┌─────────────┐    ┌──────────────┐    ┌───────────────────────┐  │
│  │ JournalReader│───→│  Normalizer  │───→│  OrderStateStore      │  │
│  │ (order/trade)│    │  (SH/SZ统一)  │    │  (订单建档/关联/清理)  │  │
│  └─────────────┘    └──────┬───────┘    └──────────┬────────────┘  │
│                            │                       │               │
│                            ▼                       ▼               │
│                   ┌────────────────────────────────────┐           │
│                   │      DerivedEventBuilder            │           │
│                   │  (OrderEvent/TradeEvent/Withdraw)   │           │
│                   └───────────────┬────────────────────┘           │
│                                   │                                │
│                                   ▼                                │
│                   ┌────────────────────────────────────┐           │
│                   │      MomentAccumulator              │           │
│                   │  per-symbol × family × bucket       │           │
│                   │  + UniqueCounter (set<order_id>)    │           │
│                   └───────────────┬────────────────────┘           │
│                                   │                                │
│                    ┌──────────────┴──────────────┐                 │
│                    ▼                             ▼                 │
│           ┌───────────────┐            ┌─────────────────┐        │
│           │ 时序因子即时计算 │            │ 截面因子持续fork  │        │
│           │ (每事件更新)    │            │ (fork snapshot)  │        │
│           └───────┬───────┘            └────────┬────────┘        │
│                   │                             │                  │
│                   ▼                             ▼                  │
│         ┌──────────────────────────────────────────────┐          │
│         │          POSIX Shared Memory                  │          │
│         │  ┌────────────┐    ┌───────────────────────┐ │          │
│         │  │ Live Region │    │  Snapshot Region      │ │          ���
│         │  │ (行优先,     │    │  (列优先, 双缓冲,     │ │          │
│         │  │  per-symbol │    │   fork子进程写入)      │ │          │
│         │  │  seqlock)   │    │                       │ │          │
│         │  └────────────┘    └───────────────────────┘ │          │
│         └──────────────────────────────────────────────┘          │
│                                                                    │
│         ┌──────────────────┐                                      │
│         │ PeriodicPersister │  每 N 分钟 fork → Parquet 落盘       │
│         └──────────────────┘                                      │
└────────────────────────────────────────────────────────────────────┘
                    │                             │
                    ▼                             ▼
          ┌──────────────────┐          ┌──────────────────┐
          │ Python 下游进程    │          │ Python 下游进程    │
          │ (逐symbol监控)    │          │ (全截面模型推理)   │
          │ 读 Live Region    │          │ 读 Snapshot Region│
          └──────────────────┘          └──────────────────┘
```

### 2.1 关键设计决策

**单线程事件循环**：主进程的事件处理路径是严格单线程的。从 journal 读取 → 事件标准化 → 订单关联 → 统计矩更新 → 时序因子计算 → 写共享内存，全部在一个线程内完成。这消除了锁竞争，保证因子更新的确定性顺序。

**双区域共享内存**：共享内存分为两个独立区域，各自针对不同消费模式优化：
- **Live Region**：行优先布局，每条事件后即时更新对应 symbol 的因子值。适合 Python 逐 symbol 实时读取。
- **Snapshot Region**：列优先布局（按 factor 分列），双缓冲。由 fork 子进程在一致性快照上计算截面因子后原子切换。适合 Python 全截面模型推理。

**fork 一致性快照**：截面因子需要跨 symbol 数据一致性。通过 Linux `fork()` 的 COW（Copy-on-Write）语义，子进程瞬时获得父���程全部内存状态的冻结副本，在此之上计算截面因子并写入 Snapshot Region。

---

## 3. 因子分类

### 3.1 时序因子 (Time-Series Factors)

定义：只依赖单个 symbol 自身历史事件的因子。

特点：
- 每条事件到来后，只需更新该 symbol 的统计矩，然后重算该 symbol 的因子值
- 更新是 O(1) 的——累加统计矩，不保留历史事件
- demo 中全部 7 族因子（buy_order, sell_order, buy_withdraw, sell_withdraw, both_trade, buy_trade, sell_trade）的 mean/std/skew/kurt/vwapx/count 都属于此类

示例：
```
buy_order_ordervol_mean_large_cat1     ← 某symbol大买单平均委托量
both_trade_orderretdiff_std_cat1       ← 某symbol成交双方报价分歧的波动
sell_withdraw_withdrawtmlag_mean_instant_cat1  ← 某symbol瞬时撤单平均等待时长
```

### 3.2 截面因子 (Cross-Sectional Factors)

定义：依赖同一时刻多个 symbol 数据的因子。

特点：
- 需要在一个一致的时间快照上，读取全部 symbol 的时序因子值，再做跨 symbol 计算
- 典型操作：排名（rank）、标准化（z-score）、分位数、行业中性化
- 不能在单条事件处理中完成，需要独立的计算路径

示例：
```
buy_order_ordervol_mean_large_cat1_rank   ← 大买单均量的全市场排名
both_trade_count_cat1_zscore              ← 成交笔数的截面标准化
```

### 3.3 因子命名规范

```
<group>_<field>_<stat>[_<bucket>]_cat1[_<xs_op>]
```

- `group`：buy_order / sell_order / buy_withdraw / sell_withdraw / both_trade / buy_trade / sell_trade
- `field`：ordervol / ordermoney / orderret / orderturnover / tradevol / trademoney / traderet / tradeturnover / ordertmlag / orderretdiff / orderturnoverdiff / withdrawvol / withdrawmoney / withdrawret / withdrawturnover / withdrawtmlag
- `stat`：mean / std / skew / kurt / vwapx / count
- `bucket`（可选）：large / small / complete / partial / instant / bolarge / bosmall / solarge / sosmall
- `xs_op`（可选，截面因子）：rank / zscore / quantile

---

## 4. 内部状态数据结构

### 4.1 统计矩

```cpp
struct Moments {
    int64_t count = 0;
    double s1 = 0;   // Σx
    double s2 = 0;   // Σx²
    double s3 = 0;   // Σx³
    double s4 = 0;   // Σx⁴

    void update(double x) {
        count++;
        s1 += x;
        double x2 = x * x;
        s2 += x2;
        s3 += x2 * x;
        s4 += x2 * x2;
    }

    double mean() const { return count > 0 ? s1 / count : NAN; }

    double std() const {
        if (count < 2) return NAN;
        double var = (s2 - s1 * s1 / count) / (count - 1);
        return var > 0 ? std::sqrt(var) : 0.0;
    }

    double skew() const {
        if (count < 3) return NAN;
        double mu = s1 / count;
        double vp = s2 / count - mu * mu;
        if (vp < EPS * mu * mu) return NAN;
        double sp = std::sqrt(vp);
        return (s3 / count - 3 * mu * s2 / count + 2 * mu * mu * mu) / (sp * sp * sp);
    }

    double kurt() const {
        if (count < 4) return NAN;
        double mu = s1 / count;
        double vp = s2 / count - mu * mu;
        if (vp < EPS * mu * mu) return NAN;
        double vp2 = vp * vp;
        double num = s4 / count - 4 * mu * s3 / count
                     + 6 * mu * mu * s2 / count - 3 * mu * mu * mu * mu;
        return num / vp2 - 3;
    }
};
```

### 4.2 分桶矩集合

每个因子族在每个分桶条件下维护一组 Moments：

```cpp
// 订单因子族的矩集合
struct OrderBucketMoments {
    Moments vol;        // ordervol
    Moments money;      // ordermoney
    Moments ret;        // orderret
    Moments turnover;   // orderturnover
};

// 撤单因子族的矩集合
struct WithdrawBucketMoments {
    Moments vol;        // withdrawvol
    Moments money;      // withdrawmoney
    Moments ret;        // withdrawret
    Moments turnover;   // withdrawturnover
    Moments tmlag;      // withdrawtmlag
};

// 成交因子族的矩集合
struct TradeBucketMoments {
    Moments vol;            // tradevol
    Moments money;          // trademoney
    Moments ret;            // traderet
    Moments turnover;       // tradeturnover
    Moments ordertmlag;     // |ordertm_buy - ordertm_sell|
    Moments orderretdiff;   // |orderret_buy - orderret_sell|
    Moments orderturnoverdiff;
};
```

### 4.3 per-symbol 因子状态

```cpp
// 分桶索引枚举
enum class OrderBucket : uint8_t { ALL = 0, LARGE, SMALL, COUNT };
enum class WithdrawBucket : uint8_t { ALL = 0, LARGE, SMALL, COMPLETE, PARTIAL, INSTANT, COUNT };
enum class TradeBucket : uint8_t { ALL = 0, LARGE, SMALL, BOLARGE, BOSMALL, SOLARGE, SOSMALL, COUNT };

struct SymbolFactorState {
    // ---- 7 个因子族 × 各自的分桶 ----
    OrderBucketMoments    buy_order[static_cast<int>(OrderBucket::COUNT)];
    OrderBucketMoments    sell_order[static_cast<int>(OrderBucket::COUNT)];

    WithdrawBucketMoments buy_withdraw[static_cast<int>(WithdrawBucket::COUNT)];
    WithdrawBucketMoments sell_withdraw[static_cast<int>(WithdrawBucket::COUNT)];

    TradeBucketMoments    both_trade[static_cast<int>(TradeBucket::COUNT)];
    TradeBucketMoments    buy_trade[static_cast<int>(TradeBucket::COUNT)];
    TradeBucketMoments    sell_trade[static_cast<int>(TradeBucket::COUNT)];

    // ---- 成交族的独立订单计数 ----
    // key: bucket index, value: {buy_order_ids, sell_order_ids}
    struct UniqueCounters {
        std::unordered_set<uint64_t> buy_ids;
        std::unordered_set<uint64_t> sell_ids;
    };
    UniqueCounters both_trade_uc[static_cast<int>(TradeBucket::COUNT)];
    UniqueCounters buy_trade_uc[static_cast<int>(TradeBucket::COUNT)];
    UniqueCounters sell_trade_uc[static_cast<int>(TradeBucket::COUNT)];

    // ---- 元数据 ----
    int64_t last_event_nano = 0;
    uint32_t total_events = 0;
};
```

### 4.4 订单状态表

```cpp
struct OrderEntry {
    int32_t  symbol;
    int8_t   side;           // 'B' or 'S'
    int32_t  order_tm;       // HHMMSSmmm
    float    order_px;
    int32_t  original_vol;
    int32_t  remaining_vol;
    double   order_money;
    double   order_ret;
    double   order_turnover;
};

// key: (channel_id, order_id) → 直接用 uint64_t 编码
// channel_id << 32 | order_id
using OrderKey = uint64_t;

class OrderStateStore {
    std::unordered_map<OrderKey, OrderEntry> active_orders_;
    // SZ 深市市价单补价用
    std::unordered_map<int32_t, float> latest_buy_trade_px_;   // symbol → px
    std::unordered_map<int32_t, float> latest_sell_trade_px_;  // symbol → px

public:
    void insert(OrderKey key, OrderEntry entry);
    OrderEntry* lookup(OrderKey key);
    void reduce_volume(OrderKey key, int32_t filled_vol);
    void remove_if_empty(OrderKey key);
};
```

### 4.5 全局状态

```cpp
struct GlobalState {
    // symbol slot 映射（开盘前初始化）
    static constexpr int MAX_SYMBOLS = 6000;
    int32_t symbol_codes[MAX_SYMBOLS];     // slot → symbol code
    std::unordered_map<int32_t, uint16_t> symbol_to_slot;
    int num_symbols = 0;

    // 静态数据
    float pre_close[MAX_SYMBOLS];
    float circ_market_cap[MAX_SYMBOLS];

    // per-symbol 因子状态
    SymbolFactorState factor_state[MAX_SYMBOLS];

    // 订单状态表
    OrderStateStore order_store;

    // 输出：per-symbol 时序因子值（从 moments 计算得到，写入共享内存）
    // 因子数量在编译时确定
    static constexpr int NUM_TS_FACTORS = /* 由因子注册表决定 */;
    double ts_factor_values[MAX_SYMBOLS][NUM_TS_FACTORS];
};
```

---

## 5. 事件处理流水线

### 5.1 主循环

```cpp
while (g_running) {
    FramePtr frame = reader->getNextFrame();

    if (!frame) {
        // 无新数据：尝试 fork 截面计算 + 检查落盘
        try_fork_xs_child();
        check_persist_timer();
        std::this_thread::sleep_for(std::chrono::microseconds(50));
        continue;
    }

    int64_t nano = frame->getNano();
    short msg_type = frame->getMsgType();
    void* data = frame->getData();

    switch (msg_type) {
        case MSG_TYPE_L2_ORDER:
            process_order(static_cast<KyStdOrderType*>(data), nano);
            break;
        case MSG_TYPE_L2_TRADE:
            process_trade(static_cast<KyStdTradeType*>(data), nano);
            break;
    }

    // 每处理一批事件后尝试 fork 截面计算��开销极低：一次 waitpid + 一次 fork）
    try_fork_xs_child();
}
```

### 5.2 Order 处理

```
收到 KyStdOrderType:
  1. 判断交易所 (SH/SZ)
  2. 判断事件类型：
     - SH: FunctionCode=='B'买 / 'S'卖 / 'C'撤
     - SZ: 纯订单（撤单在 trade 流中识别）
  3. 如果是新订单：
     a. 查 pre_close → 计算 orderret, ordermoney, orderturnover
     b. SZ 市价单无价格 → 用 latest_trade_px 补全
     c. 存入 OrderStateStore
     d. 构造 OrderFeatureEvent
     e. 判断大/小单条件
     f. 更新 buy_order 或 sell_order 的 ALL + 条件桶 moments
     g. 重算该 symbol 的时序因子 → 写入 Live Region
  4. 如果是 SH 撤单 (FunctionCode == 'C'):
     a. 查 OrderStateStore 找到原单
     b. 构造 WithdrawFeatureEvent
     c. 判断 complete/partial/instant + 大/小单
     d. 更新 buy_withdraw 或 sell_withdraw 的对应桶 moments
     e. 更新 OrderStateStore (减量/移除)
     f. 重算该 symbol 时序因子 → 写入 Live Region
```

### 5.3 Trade 处理

```
收到 KyStdTradeType:
  1. 判断交易所 (SH/SZ)
  2. SZ 特殊：
     - tradebuyid==0 → 买方撤单事件（同上撤单处理）
     - tradesellid==0 → 卖方撤单事件
     - 正常成交: tradebuyid>0 && tradesellid>0
  3. 更新 latest_trade_px（SZ 市价单补价用）
  4. 从 OrderStateStore 查买方/卖方原单
  5. SH aggressive order 补全：
     - 若原单不存在，根据成交信息建立临时记录
  6. 构造 TradeFeatureEvent:
     - tradevol, trademoney, traderet, tradeturnover
     - ordertmlag = |ordertm_buy - ordertm_sell|
     - orderretdiff = |orderret_buy - orderret_sell|
     - orderturnoverdiff
  7. 判断分桶条件:
     - 成交自身大/小单
     - 买方原单大/小单 (bolarge/bosmall)
     - 卖方原单大/小单 (solarge/sosmall)
  8. 根据 sideflag 确定方向，更新三个成交族:
     - both_trade (ALL + 条件桶)
     - buy_trade 或 sell_trade (ALL + 条件桶)
     - 同时更新 UniqueCounter (buy_ids, sell_ids)
  9. 更新 OrderStateStore (减 remaining_vol，完全成交则移除)
  10. 重算该 symbol 时序因子 → 写入 Live Region
```

### 5.4 时序因子即时重算

每次更新完 moments 后，对该 symbol 调用：

```cpp
void recompute_ts_factors(uint16_t slot) {
    auto& state = g_state.factor_state[slot];
    double* out = g_state.ts_factor_values[slot];
    int idx = 0;

    // 遍历所有因子��� × 桶 × 字段 × 统计量
    // buy_order
    for (int b = 0; b < (int)OrderBucket::COUNT; b++) {
        auto& m = state.buy_order[b];
        out[idx++] = m.vol.mean();
        out[idx++] = m.money.mean();
        out[idx++] = m.ret.mean();
        out[idx++] = m.ret.std();
        out[idx++] = m.ret.skew();
        out[idx++] = m.ret.kurt();
        // ... turnover 同理
        // vwapx = money.mean() / vol.mean()
        double vwapx = (m.vol.count > 0 && m.vol.mean() != 0)
            ? m.money.mean() / m.vol.mean() : NAN;
        out[idx++] = vwapx / g_state.pre_close[slot] - 1;  // vwapx → ret
        out[idx++] = static_cast<double>(m.vol.count);
    }
    // ... sell_order, withdraws, trades 同理

    // 写入共享内存 Live Region (seqlock)
    publish_ts_to_live(slot, out, idx);
}
```

---

## 6. 共享内存设计

### 6.1 总体布局

```
/dev/shm/realtime_factors

偏移量                 内容
─────────────────────────────────────────────────────────
0x0000                 GlobalHeader (128 bytes, cache-line aligned)
0x0080                 SymbolTable  (MAX_SYMBOLS × 4 bytes)
                         int32_t symbol_code[MAX_SYMBOLS]
0x5DC0 (approx)        FactorNameTable
                         因子名字符串（用于 Python 端自描述）
                         以 '\0' 分隔的连续字符串
─────────────────────────────────────────────────────────
live_offset            Live Region (行优先)
                         PerSymbolLiveBlock[MAX_SYMBOLS]
                           uint64_t seqlock
                           int64_t  last_update_nano
                           double   factors[NUM_TS_FACTORS]
                           padding to 64-byte boundary
─────────────────────────────────────────────────────────
snap_offset            Snapshot Region (列优先, AB双缓冲)
                         ControlBlock (64 bytes)
                           atomic<uint64_t> version  (偶数=稳定)
                           atomic<int32_t>  latest   (0 or 1)
                           atomic<int64_t>  compute_nano
                         Buffer[2]:
                           ts_columns:
                             double factor_0[num_symbols]
                             double factor_1[num_symbols]
                             ...
                           xs_columns:
                             double xs_factor_0[num_symbols]
                             ...
─────────────────────────────────────────────────────────
```

### 6.2 GlobalHeader

```cpp
struct ShmGlobalHeader {
    uint64_t magic;               // 0x525446310000000ULL ("RTF1")
    uint32_t version;             // 协议版本号
    uint32_t num_symbols;         // 当前活跃 symbol 数
    uint32_t num_ts_factors;      // 时序因子列数
    uint32_t num_xs_factors;      // 截面因子列数
    uint64_t symbol_table_offset;
    uint64_t factor_name_offset;
    uint64_t live_offset;
    uint64_t snap_offset;
    uint64_t total_size;
    uint8_t  reserved[64];
} __attribute__((packed));
```

### 6.3 Live Region 详细设计

**布局：行优先 (row-major)**

Live Region 的设计目标是让主线程能以最低延迟写入单个 symbol 的因子值。

```cpp
struct alignas(64) PerSymbolLiveBlock {
    std::atomic<uint64_t> seqlock;   // 偶数=稳定, 奇数=写入中
    int64_t last_update_nano;        // 最后更新时间
    double factors[NUM_TS_FACTORS];  // 因子值
    // 尾部自动 padding 到 64 字节对齐
};
```

**写入协议 (C++ 主线程)：**
```cpp
void publish_ts_to_live(uint16_t slot, const double* values, int count) {
    auto& block = live_region[slot];
    // 1. seqlock 设为奇数（表示写入中）
    uint64_t seq = block.seqlock.load(std::memory_order_relaxed);
    block.seqlock.store(seq + 1, std::memory_order_release);
    // 2. 写入因子值
    block.last_update_nano = current_nano;
    std::memcpy(block.factors, values, count * sizeof(double));
    // 3. seqlock 设为偶数（表示写入完成）
    block.seqlock.store(seq + 2, std::memory_order_release);
}
```

**读取协议 (Python 单symbol)：**
```python
def read_symbol_factors(mm, slot):
    block_offset = live_offset + slot * BLOCK_SIZE
    while True:
        seq1 = struct.unpack_from('<Q', mm, block_offset)[0]
        if seq1 & 1:  # 奇数 → 写入中，自旋
            continue
        nano = struct.unpack_from('<q', mm, block_offset + 8)[0]
        values = np.frombuffer(mm, dtype=np.float64,
                               offset=block_offset + 16,
                               count=NUM_TS_FACTORS).copy()  # 必须copy
        seq2 = struct.unpack_from('<Q', mm, block_offset)[0]
        if seq1 == seq2:
            return nano, values  # 一致读取成功
        # seq 变了 → 被写入覆盖，重试
```

**为什么选行优先**：主线程更新一个 symbol 时写入的是连续内存（同一个 `PerSymbolLiveBlock`），cache line 友好。单次更新全部因子值的开销约 ~200ns（对比列优先需要跳跃到数百个不同 cache line，开销 ~100µs）。

### 6.4 Snapshot Region 详细设计

**布局：列优先 (column-major) + AB 双缓冲 + 乐观校验**

Snapshot Region 的设计目标是让 Python 能高效读取整列因子数据用于模型推理。

```
ControlBlock (64 bytes, cache-line aligned):
  atomic<uint64_t> version;        ← 单调递增的截面版本号（偶数=稳定, 奇数=写入中）
  atomic<int32_t>  latest;         ← 最新可读的缓冲区 (0 or 1)
  atomic<int64_t>  compute_nano;   ← 最新快照的数据时间戳
  padding

Buffer[0]:
  num_valid_symbols: int32
  reserved[60]
  ts_col_0: double[MAX_SYMBOLS]    ← 第 0 个时序因子的全市场值
  ts_col_1: double[MAX_SYMBOLS]
  ...
  ts_col_N: double[MAX_SYMBOLS]
  xs_col_0: double[MAX_SYMBOLS]    ← 第 0 个截面因子的全市场值
  xs_col_1: double[MAX_SYMBOLS]
  ...

Buffer[1]: (同上)
```

**为什么选列优先**：Python 读取某个因子的全市场值时，`np.frombuffer(mm, dtype=np.float64, offset=col_offset, count=num_symbols)` 直接得到连续内存的 numpy array，无需 copy 即可使用。做 ranking、z-score 时都在连续内存上操作，极度 cache 友好。

**为什么双缓冲就够了**：

关键观察：连续两次 fork 之间有 **2-5ms 的天然间隔**（fork 开销 + 截面计算时间）。Writer 永远只写 `buf[1-latest]`，绝不碰 `buf[latest]`。翻转 `latest` 后，要等到下一个 fork 子进程启动并执行到写入阶段（又要 2-5ms），旧 `latest` 的 buffer 才会被覆盖。Reader 只要在这个窗口内完成 copy 就安全。

```
Writer(fork子1): ──[写 buf 1]──[flip latest→1]──[exit]
                                                      ├── 2-5ms gap ──┤
Writer(fork子2):                                      └──[写 buf 0]──...

Reader:                       [读latest=1]──[copy buf 1 ~1ms]──[verify latest=1 ✓]
                              ↑ 安全：buf[1] 在 2-5ms 内不会被碰
```

Reader 用乐观校验：copy 后验证 `latest` 没变。如果变了（极罕见：Reader 被 GC 暂停 >2ms），重试即可。**无需引用计数、无需 CAS、无需三缓冲。**

### 6.5 AB 双缓冲读写协议

**写者侧 (fork 子进程)：**

```cpp
void write_and_publish(SnapshotControl& ctrl, int64_t nano) {
    // 1. 选 inactive buffer
    int32_t active = ctrl.latest.load(std::memory_order_acquire);
    int32_t target = 1 - active;

    // 2. version 设为奇数（写入中）
    uint64_t ver = ctrl.version.load(std::memory_order_relaxed);
    ctrl.version.store(ver + 1, std::memory_order_release);

    // 3. 写入截面因子到 buffer[target]
    write_xs_factors_to_buffer(target);

    // 4. 翻转 latest + version 设为偶数（完成）
    ctrl.compute_nano.store(nano, std::memory_order_relaxed);
    ctrl.latest.store(target, std::memory_order_release);
    ctrl.version.store(ver + 2, std::memory_order_release);
}
```

**读者侧 (Python)：**

```python
def read_snapshot(mm, ctrl_offset):
    """乐观读：copy 数据后验证 version 没变"""
    while True:
        ver1 = struct.unpack_from('<Q', mm, ctrl_offset)[0]
        if ver1 & 1:
            continue  # 写入中，自旋（极少命中）

        latest = struct.unpack_from('<i', mm, ctrl_offset + 8)[0]
        nano = struct.unpack_from('<q', mm, ctrl_offset + 12)[0]

        # copy 需要的列（~1-2ms，远小于写周期 2-5ms）
        buf_off = snapshot_buffer_offset(latest)
        data = {}
        for name, col_idx in needed_columns.items():
            col_off = buf_off + 64 + col_idx * MAX_SYMBOLS * 8
            data[name] = np.frombuffer(mm, np.float64,
                                        offset=col_off,
                                        count=num_symbols).copy()

        ver2 = struct.unpack_from('<Q', mm, ctrl_offset)[0]
        if ver1 == ver2:
            return nano, data  # 一致！

        # ver 变了 → 期间有新写入，重试（极罕见）
```

**无需额外依赖**：只用了 `struct.unpack_from` 读一个 `uint64`，不需要 CAS、不需要 C 扩展、不需要引用计数。

**安全保证**：
- Writer 写 `buf[1-latest]`，翻转后不会被碰，直到下一个 fork 子进程写入（2-5ms 后）
- Reader copy 所需列通常 <2ms（5000 symbols × 200 列 × 8B = 8MB ≈ 1ms）
- 即使 GC 暂停导致 copy 超时，`version` 校验会检测到不一致并重试
- 最坏情况只是多重试一次，不会读到脏数据

---

## 7. 一致性快照与截面因子计算

### 7.1 问题

截面因子需要读取全部 ~5000 个 symbol 的时序因子值作为输入。但主线程在持续处理事件并更新这些值。如果在读取过程中某些 symbol 被更新了，得到的数据不一致。

### 7.2 方案：fork() + COW

**原理**：Linux `fork()` 创建子进程时，父子进程共享相同的物理内存页。当任一方修改某页时，内核才会复制该页（Copy-on-Write）。这意味着：

1. `fork()` 本身几乎瞬时完成（只复制页���，不复制数据）
2. 子进程看到的是 fork 瞬间父进程的完整内存快照
3. 父进程继续处理事件，修改的页被 COW，不影响子进程
4. 子进程只读状态（不触发 COW），计算截面因子后写入共享内存，退出

**时序图**：

```
主线程:     ──[处理事件]──[处理事件]──[fork()]──[继续处理事件]──[waitpid]──
                                         │
子进程:                                  └──[读state]──[算截面]──[写SHM]──[exit]
                                              ↑
                                        COW快照，数据一致
```

**开销分析**：

| 操作 | 开销 | 说明 |
|------|------|------|
| `fork()` | ~0.5ms | 复制页表。200MB RSS ≈ 50K 页 ≈ 0.5ms |
| COW 开销 | ~0.01ms/page | 父进程修改页时触发。事件处理期间，通常只有少数页被修改 |
| 子进程计算 | 1-3ms | 5000 symbols × N 因子的 rank/zscore |
| 写入 SHM | ~0.5ms | 写入 Snapshot Region inactive buffer |
| **总延迟** | **~2-5ms** | 从请求到截面因子可用 |

对比 epoch snapshot ring（100ms/500ms 间隔）：
- fork 方案延迟 2-5ms，是 epoch 100ms 的 1/20 到 1/50
- fork 不需要预分配多个完整副本，COW 只在修改时才复制单页
- fork 可以在任意时刻触发，不受固定周期限制

### 7.3 持续 fork 模式（推荐）

截面因子不应由下游 Python 按需触发（那样 Python 需要空等计算完成），而是由主进程**持续不断地 fork 计算**。Python 来了直接读上一个 ready 版本，零等待。


**时序图：**

```
主线程:  ──[事件]──[事件]──[fork]──[事件]──[事件]──[waitpid OK]──[fork]──...
                              │                                      │
子进程1:                     └──[算截面]──[写buf1+flip]──[exit]     │
子进程2:                                                            └──[算截面]──[写buf0+flip]─��[exit]

Python:  ──────────[read_snapshot: copy buf1, verify ✓]──[推理]──[read_snapshot: copy buf0]──...
         直接读 latest 版本，零等待，无需触发
```

**实现：**

```cpp
static pid_t xs_child_pid = 0;

void try_fork_xs_child() {
    // 1. 回收上一个子进程
    if (xs_child_pid > 0) {
        int status;
        if (waitpid(xs_child_pid, &status, WNOHANG) > 0)
            xs_child_pid = 0;
        else
            return;  // 上一个还在跑
    }

    // 2. 立即 fork 下一轮
    xs_child_pid = fork();
    if (xs_child_pid == 0) {
        // ---- 子进程 ----
        signal(SIGINT, SIG_DFL);
        signal(SIGTERM, SIG_DFL);

        auto& ctrl = *snap_control_block;
        int64_t snap_nano = get_latest_event_nano();

        // 调用 6.5 节的 write_and_publish
        write_and_publish(ctrl, snap_nano);
        _exit(0);
    }
}
```

**效果**：截面因子以背靠背方式持续刷新，周期 ≈ fork + 计算 ≈ 2-5ms。Python 永远有 ready 版本可读。

### 7.4 为什么不用 MVCC

| 维度 | fork() + COW | MVCC (乐观读 + 重试) |
|------|-------------|---------------------|
| 一致性 | 完美（时间冻结） | 近似（µs 级时间差） |
| 实现复杂度 | 低（fork 一行代码） | 中高（原子版本号 + 重试） |
| 内存开销 | 低（COW 只复制修改页） | 低（只需版本号） |
| 高频事件下 | 稳定 | 可能活锁 |

**结论**：Linux-only 项目，fork() 是更优选择。

### 7.5 fork 注意事项

- **`_exit(0)`**：子进程必须用 `_exit()`，避免触发 atexit handlers
- **信号**：fork 后立即 reset SIGINT/SIGTERM 为 SIG_DFL
- **僵尸回收**：`try_fork_xs_child()` 每次先 `waitpid(WNOHANG)`
- **fd 泄漏**：子进程继承 journal reader 的 mmap fd。生命周期很短（<10ms），通常无需显式关闭
- **fork 频率**：持续 fork ≈ 每 2-5ms 一次。200MB RSS 进程 fork 开销 ~0.5ms，可接受

### 7.6 为什么 Python fork 读 SHM 不行

注意：fork 的 COW 只作用于**进程私有内存**（heap/stack），不作用于 `MAP_SHARED` 共享内存。`/dev/shm/` 的 mmap 页在 fork 后父子进程仍映射同一物理页——C++ 写者的修改对 Python 子进程立即可见，不存在「冻结快照」���果。

因此 Snapshot Region 的一致性靠 AB 双缓冲 + version 乐观校验（见 6.5 节），而非 Python 侧 fork。
---

## 8. Python 读取接口

### 8.1 推荐提供 Python 包装库

```python
# realtime_factors_reader.py

import mmap
import struct
import numpy as np
import os

class FactorReader:
    SHM_NAME = '/dev/shm/realtime_factors'
    MAGIC = 0x5254463100000000

    def __init__(self):
        fd = os.open(self.SHM_NAME, os.O_RDONLY)
        size = os.fstat(fd).st_size
        self._mm = mmap.mmap(fd, size, access=mmap.ACCESS_READ)
        os.close(fd)
        self._parse_header()

    def _parse_header(self):
        hdr = struct.unpack_from('<QIIIIQQQQ', self._mm, 0)
        assert hdr[0] == self.MAGIC, "Magic mismatch"
        self.num_symbols = hdr[2]
        self.num_ts_factors = hdr[3]
        self.num_xs_factors = hdr[4]
        self._sym_off = hdr[5]
        self._name_off = hdr[6]
        self._live_off = hdr[7]
        self._snap_off = hdr[8]
        # 解析 symbol table
        self.symbols = np.frombuffer(
            self._mm, dtype=np.int32,
            offset=self._sym_off, count=self.num_symbols)
        # 解析 factor name table
        self.ts_factor_names = self._parse_names(self.num_ts_factors)
        self.xs_factor_names = self._parse_names(self.num_xs_factors,
                                                  offset=self.num_ts_factors)

    # ---- Live Region: 逐 symbol 读取 ----

    def read_symbol(self, slot: int) -> tuple[int, np.ndarray]:
        """读取单个 symbol 的时序因子值 (seqlock 一致读)"""
        block_size = self._live_block_size
        base = self._live_off + slot * block_size
        while True:
            seq1 = struct.unpack_from('<Q', self._mm, base)[0]
            if seq1 & 1:
                continue  # 写入中
            nano = struct.unpack_from('<q', self._mm, base + 8)[0]
            vals = np.frombuffer(self._mm, dtype=np.float64,
                                 offset=base + 16,
                                 count=self.num_ts_factors).copy()
            seq2 = struct.unpack_from('<Q', self._mm, base)[0]
            if seq1 == seq2:
                return nano, vals

    # ---- Snapshot Region: 全截面读取 ----

    def read_snapshot(self, columns=None) -> dict:
        """读取全截面快照（乐观校验，自动重试）"""
        ctrl_base = self._snap_off
        while True:
            ver1 = struct.unpack_from('<Q', self._mm, ctrl_base)[0]
            if ver1 & 1:
                continue  # 写入中

            latest = struct.unpack_from('<i', self._mm, ctrl_base + 8)[0]
            nano = struct.unpack_from('<q', self._mm, ctrl_base + 12)[0]
            buf_off = self._snap_buffer_offset(latest)

            # copy 需要的列
            data = {'compute_nano': nano}
            data_off = buf_off + 64
            target_cols = columns or range(self.num_ts_factors)
            for i in target_cols:
                col_off = data_off + i * self.num_symbols * 8
                name = self.ts_factor_names[i]
                data[name] = np.frombuffer(
                    self._mm, dtype=np.float64,
                    offset=col_off, count=self.num_symbols).copy()

            ver2 = struct.unpack_from('<Q', self._mm, ctrl_base)[0]
            if ver1 == ver2:
                return data  # 一致！
            # 期间有新写入，重试
```

### 8.2 注意事项

- **Live Region**：`np.frombuffer` 必须 `.copy()`，因为主线程随时可能覆写
- **Snapshot Region**：`read_snapshot()` 内部已经 copy，返回的数据可放心使用
- **无需额外依赖**：只用 `struct.unpack_from` 读 version，不需要 CAS 或 C 扩展

---

## 9. 定时落盘

### 9.1 机制

每 N 分钟（可配置，默认 10 分钟）将当前因子快照持久化到磁盘。

**实现方式**：复用 fork 机制。主线程在定时器触发时 fork 一个子进程，子进程将 COW 状态快照序列化为 Parquet 文件。

```cpp
void check_persist_timer() {
    if (!persist_timer_expired()) return;
    if (persist_child_pid > 0) return;  // 上一个还在写

    persist_child_pid = fork();
    if (persist_child_pid == 0) {
        // 子进程：序列化当前状态为 Parquet
        persist_snapshot_to_parquet();
        _exit(0);
    }
}
```

### 9.2 输出格式

```
output/
  realtime_factors/
    2026-03-10/
      0931.parquet     ← 09:31 的因子快照
      0940.parquet
      0950.parquet
      1000.parquet
      ...
      1500.parquet     ← 收盘
```

每个 Parquet 文件的 schema：
```
datetime: timestamp
code: string          ← "000001"
<factor_name>_cat1: float64
...
```

### 9.3 与 demo TTS 的关系

为了对拍方便，建议在 demo 的 TTS 时点（09:25, 10:00, ...）也触发一次额外的落盘。这样可以直接和 `demo_backfill_ht2.py` 的输出逐列对比。

---

## 10. 交易所差异处理

### 10.1 SH (上海) 特殊逻辑

1. **撤单来自 order 流**：`FunctionCode == 'C'` (68 decimal) 表示撤单
2. **aggressive order 补全**：上交所的 aggressive order（主动方订单）在 order 流中可能缺失价格/量信息。需要在收到对应 trade 时，用 trade 信息补全：
   - 买方 aggressive：取该 `tradebuyid` 的所有成交，`Σtradevol` 补充为 ordervol，`max(tradepx)` 补充为 orderpx
   - 卖方 aggressive：取该 `tradesellid` 的所有成交，`Σtradevol` 补充为 ordervol，`min(tradepx)` 补充为 orderpx
3. **OrderKind 映射**：`2→A(65), 10→D(68), 11→S(83)`

### 10.2 SZ (深圳) 特殊逻辑

1. **撤单来自 trade 流**：`tradebuyid == 0` 表示买方撤单，`tradesellid == 0` 表示卖方撤单
2. **市价单无价格**：`OrderKind != 50 (限价)` 时 orderpx 可能为 0，需用最近成交价补全：
   - 买单用最近的卖向成交价 × 1.001
   - 卖单用最近的买向成交价 × 0.999
3. **成交方向推断**：`tradebuyid > tradesellid` → 主动买 (B=66)，否则主动卖 (S=83)
4. **OrderKind 映射**：`1→'2'(50 限价), 2→'1'(49 对手方最优), 3→'U'(85 本方最优)`

### 10.3 交易所判断

```cpp
inline bool is_sz(int32_t symbol) { return symbol < 400000; }
inline bool is_sh(int32_t symbol) { return symbol >= 400000; }
```

---

## 11. 分桶��件定义

### 11.1 大/小单

```cpp
bool is_large(int32_t vol, double money) {
    return vol >= 100000 || money >= 1000000.0;
}
bool is_small(int32_t vol, double money) {
    return (vol <= 10000 && money <= 100000.0)
        || (vol <= 1000  && money <= 500000.0);
}
```

注意：large 和 small 不互补，中间区域的订单只进入 ALL 桶，不进入大/小单桶。

### 11.2 撤单专属分桶

```cpp
bool is_complete_withdraw(int32_t withdraw_vol, int32_t original_vol) {
    return withdraw_vol >= original_vol;
}
bool is_partial_withdraw(int32_t withdraw_vol, int32_t original_vol) {
    return withdraw_vol < original_vol;
}
bool is_instant_withdraw(int32_t tmlag_ms) {
    return tmlag_ms >= 0 && tmlag_ms < 3000;
}
```

### 11.3 成交专属分桶

成交有三个维度的大/小单判定：
- 按成交自身：`is_large(tradevol, trademoney)` / `is_small(tradevol, trademoney)`
- 按买方原单：`is_large(ordervol_buy, ordermoney_buy)` → bolarge / bosmall
- 按卖方原单：`is_large(ordervol_sell, ordermoney_sell)` → solarge / sosmall

---

## 12. 连续交易时钟

撤单等待时间 `withdrawtmlag = withdrawtm - ordertm` 需要在连续交易时钟下计算，避免午休时间被算入：

```cpp
// 将 HHMMSSmmm 转为连续交易毫秒
int64_t to_continuous_ms(int32_t tm) {
    int hh = tm / 10000000;
    int mm = (tm % 10000000) / 100000;
    int ss = (tm % 100000) / 1000;
    int ms = tm % 1000;
    int64_t raw_ms = (hh * 3600 + mm * 60 + ss) * 1000 + ms;

    // 09:29:57 之前：右移 5 分钟
    if (raw_ms < 33597000LL) return raw_ms + 300000;
    // 12:59:57 之后：左移 90 分钟
    if (raw_ms > 46797000LL) return raw_ms - 5400000;
    return raw_ms;
}
```

---

## 13. 实现路线

### Phase 1: 最小可用 (约 2 周)

**目标**：单线程事件循环 + 时序因子 + Live Region + 和 demo 对拍

1. 定义内部数据结构（Moments, SymbolFactorState, OrderStateStore）
2. 实现 SH/SZ 事件标准化
3. 实现订单状态管理（建档、关联、清理）
4. 实现 MomentAccumulator 增量更新
5. 实现时序因子从 moments 计算
6. 实现共享内存 Live Region (seqlock)
7. 用 `scripts/replay` 回放 → 在 demo TTS 时点 dump → 和 demo 输出对拍

### Phase 2: 截面因子 + Snapshot Region (约 1 周)

1. 实现 fork-based 一致性快照
2. 实现 Snapshot Region (列优先, 双缓冲)
3. 实现截面因子计算（rank, zscore）
4. 实现 Python IPC 请求协议
5. Python reader 库

### Phase 3: 定时落盘 + 工程化 (约 1 周)

1. 定时 fork → Parquet 落盘
2. Python reader 包装库完善
3. demo TTS 时点自动对拍工具
4. 监控指标（事件处理延迟、fork 耗时、因子更新频率）
5. 线上部署脚本

### Phase 4: 扩展 (后续)

- 滚动窗口因子 (cat2, cat3)
- 更多截面因子类型
- HyperLogLog 替代精确集合
- 因子热加载
- 多日跨日状态续跑

---

## 14. 因子数量估算

### 14.1 时序因子

| 因子族 | 桶数 | 基础字段数 | 统计量/字段 | 额外(vwapx+count) | 小计 |
|--------|------|-----------|------------|-------------------|------|
| buy_order | 3 (all/large/small) | 4 (vol,money,ret,turnover) | ~4 (mean,std,skew,kurt) | vwapx+count | ~3×(4×4+2) = 54 |
| sell_order | 3 | 4 | ~4 | 同上 | 54 |
| buy_withdraw | 6 (all/large/small/complete/partial/instant) | 5 (+tmlag) | ~4 | 同上 | ~6×(5×4+2) = 132 |
| sell_withdraw | 6 | 5 | ~4 | 同上 | 132 |
| both_trade | 7 (all/large/small/bo×2/so×2) | 7 (+tmlag,retdiff,turnoverdiff) | ~4 | +2 unique counts | ~7×(7×4+4) = 224 |
| buy_trade | 7 | 7 | ~4 | 同上 | 224 |
| sell_trade | 7 | 7 | ~4 | 同上 | 224 |

**时序因子总数估算：~1044**

注：vol 只算 mean（不算 std/skew/kurt），所以实际数量会少一些。精确数量在实现因子注册表时确定。

### 14.2 共享内存大小估算

- Live Region：6000 symbols × (16 + 1044 × 8) bytes ≈ **50 MB**
- Snapshot Region：2 buffers × (64 + (1044 + N_xs) × 6000 × 8) ≈ **100-120 MB**
- 总计：**~170-200 MB**

这对于服务器来说完全可控。

---

## 15. 一页版总结

- 主进程单线程事件循环处理 journal order/trade，维护订单状态表和统计矩
- 时序因子（per-symbol 累计统计）每条事件后即时更新，通过 seqlock 写入共享内存 Live Region（行优先）
- 截面因子（cross-symbol rank/zscore）持续 fork 刷新：主进程背靠背 fork → 子进程在 COW 一致快照上计算 → 写入 Snapshot Region（列优先，AB 双缓冲 + 乐观校验）→ Python 直接读 latest 版本
- fork 方案延迟 2-5ms，远优于 epoch snapshot 100ms
- 每 N 分钟 fork 落盘 Parquet，供盘后复盘
- Python 通过 mmap 零拷贝读取共享内存，单 symbol 用 Live Region，全截面用 Snapshot Region
