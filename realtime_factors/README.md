# Realtime Factors 实时因子计算框架

## 1. 框架简介
`realtime_factors` 是一个基于 KungFu Journal 的极低延迟实时因子计算框架。它通过“进程内增量同步 + 共享内存快照池”架构，实现了在主进程 **绝对零抖动** 的前提下，为下游提供物理时间对齐的全量因子快照。

---

## 2. 系统中的四个关键角色

### 角色 1：时序引擎 (TS-Engine) —— 极速更新主线程
*   **职责**：负责逐笔行情处理（Order/Trade），维护订单状态并计算基础时序因子。
*   **机制**：每处理一条事件后，将该 symbol 的最新时序因子向量以增量（Delta）形式写入私有的 **Delta Ring Buffer**。每条 Delta 为 `(seq_id, symbol, ts_feature_vector)`，即该 symbol 完整时序因子行的全量覆盖。TS-Engine **不直接写共享内存**，所有对外输出均通过 Delta Ring Buffer 交由 Snapshot-Composer 发布。
*   **Delta Ring Buffer 容量控制**：Delta 的产生频率与 trade/order 事件一致，而每条 Delta 携带完整的时序因子向量（可能数百列），若不回收则数据量将达到 TB 级别。因此 Delta Ring Buffer 采用**固定容量的分段环形缓冲区**（详见 Appendix D），其大小只需容纳两次 Snapshot 之间的增量（通常为毫秒级窗口的事件量）。Snapshot-Composer 每完成一次快照发布即推进 **Checkpoint**，Checkpoint 之前的 Delta 全部可丢弃，Delta Ring Buffer tail 随之前移回收空间。
*   **性能**：主循环仅执行纳秒级原子写入（append to Delta Ring Buffer），单次更新开销 < 20ns (若时序因子计算流程复杂，则单次更新开销会显著增加)。不过由于交易所数据分channel推送，不同channel对应不用的股票池，所以若出现性能瓶颈， TS-Engine 可按 channel （甚至按 Symbol）做多线程并发以提效。

### 角色 2：快照编排器 (Snapshot-Composer) —— 特征一致性核心
*   **职责**：
    1.  **日志重放**：从上一次 Checkpoint 状态出发，回放 Delta Ring Buffer 中 Checkpoint 之后的增量记录。对每条 Delta，用其 `ts_feature_vector` 覆盖对应 symbol 在私有 Checkpoint 镜像中的行。回放至当前最新 seq 后，即获得一个全市场一致的时序因子截面。
    2.  **写入 TS Region**：将所有 symbol 的最新时序因子向量写入快照槽位的 **TS Region**（行优先布局，便于按 symbol 定位）。
    3.  **截面合成 + 写入 CS Region**：基于同一时间戳的 TS 因子值，计算衍生截面因子（rank、zscore 等），写入同一槽位的 **CS Region**（列优先布局，便于全市场向量化计算）。
    4.  **原子发布**：TS Region 与 CS Region 在同一个槽位内完成写入后，原子更新 `LatestSeq`，保证下游读到的时序值与截面值在逻辑序号和时间戳上完全对齐。
    5.  **推进 Checkpoint**：发布完成后，将当前 Checkpoint 镜像和对应的 `seq_id` 记为新 Checkpoint。通知 Delta Ring Buffer 回收该 `seq_id` 之前的全部条目（推进 Delta Ring Buffer tail），防止 Delta Ring Buffer 被撑满。
*   **发布机制 (Sequence-based Search)**——Snapshot-Composer 通过以下机制选择 **Snapshot Ring Buffer** 中的目标槽位：
    *   **逻辑序号**：`LatestSeq` 表示最新已发布快照的逻辑序号，用于对外标识版本。
    *   **确定性映射**：候选物理槽位由 `ProbeSeq % N` 确定，其中 `ProbeSeq` 从 `LatestSeq + 1` 起步，仅用于槽位探测。
    *   **非阻塞跳转**：若 `ProbeSeq % N` 对应的槽位被读者引用（Pin），则**持续自增 `ProbeSeq`** 跳过该槽位，确保写者永不阻塞。
    *   **流量保证**：由于下游 Reader 数量有限（通常 < 10，可通过注册机制强制要求 Reader 数目小于槽位数 $N$），Snapshot-Composer 永远能找到空闲槽位进行写入，实现了**逻辑上的绝对无锁写**。

### 角色 3：下游策略进程 (Consumer) —— 共享内存读取者
*   **职责**：执行交易逻辑与模型推理。
*   **机制：槽位锁定 + 双模式读取**：
    1.  **原子引用 (Pin)**：从 Snapshot Ring Buffer 获取最新快照序号并增加其引用计数（Pin）。
    2.  **View Mode**：直接在共享内存上零拷贝映射 NumPy 视图，适用于严格受控的短持有、极高频计算场景，使用零拷贝实现更高性能。
    3.  **Copy Mode**：读取后立即 `memcpy` 到私有 Buffer（此时即可 Unpin 释放引用计数），再执行后续推理；这是 Python 策略侧的默认推荐模式（反正都用python写策略了，也不会很高性能，memcpy开销这种场景下微不足道）。
    4.  **计算释放**：推理完成后或 memcpy 后即可减少引用计数（Unpin），释放槽位供写者回收。

### 角色 4：持久化器 (Persister) —— 后台归档线程
*   **职责**：将实时因子快照定期落盘。（频率较低，一分钟一次，供离线复盘研究使用）
*   **机制：异步写盘**：
    1.  采用与角色 3 相同的“槽位锁定”机制。
    2.  为隔离 I/O 耗时，先执行 `memcpy` 至私有 Buffer 后立即解锁，随后在后台独立线程执行 Parquet 写入。

---

## 3. 核心设计深度解析

### 3.1 为什么放弃 fork()？
*   **页表开销**：在 10GB+ 的大内存环境下，`fork()` 拷贝页表会造成数百微秒的确定性抖动。
*   **全线程优势**：通过“序号跳转 + 槽位引用计数”实现了比 `fork()` 更轻量、新鲜度更高的一致性快照方案。

### 3.2 绝对的一致性 (Atomicity)
*   **消除特征撕裂**：每个 Slot 存储的是同一时刻的全量切片。`Snapshot-Composer` 确保了原始值与衍生值在逻辑序号上完全对齐。

### 3.3 NumPy 友好的 SHM 布局
**Snapshot Ring Buffer** 中的每个槽位内部包含两个子区域，由 Snapshot-Composer 在同一个发布周期内写入，共享同一逻辑序号和时间戳：
*   **TS Region（时序因子区）**：行优先 (Row-Major)，存储每个 symbol 的完整时序因子向量。适合按 symbol 快速定位和单股票监控。
*   **CS Region（截面因子区）**：列优先 (Column-Major)，存储由 TS 值衍生的截面因子（rank、zscore 等）。适合按因子列做全市场向量化计算与模型推理。

下游通过 **View Mode**（共享内存零拷贝映射）或 **Copy Mode**（memcpy 到私有 Buffer 后释放槽位）读取任一子区域。

---

## 4. 架构权衡：无抖动与稳定性

| 风险点 | 应对方案 | 预期延迟 |
| :--- | :--- | :--- |
| **主进程 Jitter** | Delta Ring Buffer (Atomic Append) | < 20ns |
| **Snapshot Ring Buffer 读写冲突** | 槽位引用计数 (Pinning) | < 100ns |
| **Snapshot Ring Buffer 槽位套圈** | **序号自增跳转 (ProbeSeq++)** 机制 | 零影响 |
| **读取开销** | **共享内存零拷贝 (Zero-Copy)** | 0μs |

---

## 5. 接口与运行约束

*   **序号语义分离**：`LatestSeq` 仅表示最新已发布快照的逻辑版本；快照实际覆盖到的输入边界由独立的 `InputSeqEnd` 记录；物理槽位选择由 `ProbeSeq % N` 完成。
*   **Reader 受控运行**：下游 Reader 属于受控进程，设计目标不是容忍 Reader 任意失联，而是在异常出现时明确告警、可定位、可回收，避免单个故障永久阻塞发布链路。
*   **默认使用 Copy Mode**：`View Mode` 仅用于严格受控的短持有场景；常规 Python 模型推理应优先采用 `Copy Mode`，尽快释放共享槽位。

---

## 6. 目录结构

```text
realtime_factors/
├── include/
│   ├── ts_engine.h            # 角色 1: 时序引擎接口
│   ├── snapshot_composer.h    # 角色 2: 快照编排器接口
│   ├── shm_layout.h           # 共享内存与 Slot 状态机定义
│   ├── moments.h              # 统计矩实现
│   └── order_state_store.h    # 订单状态管理
├── src/
│   ├── main.cpp               # 程序入口与线程调度
│   ├── ts_engine.cpp          # 时序处理实现
│   ├── snapshot_composer.cpp  # 快照编排实现
│   ├── persister.cpp          # 角色 4: 持久化实现
│   └── moments.cpp            # 统计矩算法实现
├── python/
│   └── rtf_reader/            # 角色 3: Python 读取 SDK
└── README.md
```

---

## Appendix A. Snapshot Ring Buffer 序号与槽位语义

为避免 Snapshot Ring Buffer 中”逻辑快照版本”和”物理槽位编号”混淆，框架内部对几个概念做明确区分：

*   `LatestSeq`：最新已成功发布的快照逻辑序号，对外可见，用于 Reader 获取“当前最新版本”。
*   `InputSeqEnd`：该快照消费到的增量流尾序号，用于表达“这版快照覆盖到了哪里”。
*   `ProbeSeq`：发布时用于探测可用槽位的临时序号，从 `LatestSeq + 1` 起步；若当前候选槽位被 Pin，则持续自增。
*   `SlotIdx`：实际物理槽位编号，通常由 `ProbeSeq % N` 得到。

推荐的发布过程：

1. 先在私有内存中完成一版候选快照。
2. 令 `ProbeSeq = LatestSeq + 1`。
3. 检查 `ProbeSeq % N` 对应槽位是否可写；若不可写，则递增 `ProbeSeq` 继续探测。
4. 一旦找到空闲槽位，写入快照并原子发布。
5. 发布完成后更新 `LatestSeq = ProbeSeq`，并在槽位元数据中单独记录本次 `InputSeqEnd`。

这样做的好处是：

*   `LatestSeq` 始终保持“发布版本号”语义稳定。
*   快照覆盖的数据边界不依赖物理槽位编号。
*   槽位探测策略可以演进，而不影响外部 Reader 对版本号的理解。

---

## Appendix B. Reader 异常与回收策略

本系统假设 Reader 由内部统一维护，正常情况下不允许失联。Reader 异常属于严重故障，应明确暴露，而不是静默吞掉。

但从系统健壮性角度，`realtime_factors` 仍需要在异常情况下防止共享槽位被永久占用。建议采用“注册 + 心跳 + 分级处理”机制：

*   Reader 启动时注册，获得固定 `reader_id`。
*   Reader 在 acquire 快照后登记当前 `pinned_seq` / `pinned_slot`。
*   Reader 持有期间持续更新 heartbeat。
*   Reader release 时清空 pin 状态。

当 heartbeat 超时后，系统可按两阶段处理：

*   **STALE**：先标记为异常，触发高优先级告警，并进入短暂宽限期。
*   **DEAD**：若宽限期后仍未恢复，或已确认进程不存在，则强制回收其 pin，并将系统标记为 `degraded` 或直接触发 `fatal`。

建议支持两种运行策略：

*   `strict`：发现 Reader 异常即停止发布或直接退出，适合严格生产环境。
*   `degraded`：确认 Reader 已死亡后强制回收其 pin，系统继续运行并持续报警，适合调试或回放场景。

该机制的目标不是放宽对 Reader 的要求，而是避免单个 Reader 故障把整个 Snapshot Ring Buffer 发布链路永久拖死。

---

## Appendix C. Python 读取模式边界

为兼顾低延迟与槽位回收效率，Python 读取接口建议显式区分两种模式：

### C.1 View Mode

特点：

*   返回共享内存上的只读视图，不执行数据拷贝。
*   Reader 持有期间持续 Pin 槽位。
*   仅适用于短持有、轻量计算、严格受控的场景。

适用场景：

*   在线监控
*   超短路径诊断
*   极轻量的向量化打分

不适用场景：

*   长时间模型推理
*   可能触发大量分配或 GC 的 Python 逻辑
*   需要将数组引用跨上下文长期保存的逻辑

### C.2 Copy Mode

特点：

*   acquire 后立即将所需列复制到私有内存。
*   复制完成后立刻 release，对发布器最友好。
*   应作为常规 Python 策略的默认模式。

适用场景：

*   横截面模型推理
*   研究侧特征变换
*   任意耗时不可严格上界化的 Python 逻辑

推荐接口形态：

```python
with client.acquire_latest(mode="view") as snap:
    x = snap.column_view("factor_a")
    y = snap.column_view("factor_b")
    signal = x - y

snap = client.copy_latest(columns=["factor_a", "factor_b", "factor_c"])
run_model(snap)
```

默认建议：

*   **生产策略默认使用 `Copy Mode`**
*   **`View Mode` 仅作为专家接口暴露**

---

## Appendix D. Delta Ring Buffer 内存设计

### D.1 问题

每条 Delta 携带完整时序因子向量（数百列 double），单条约 6-8 KB。产生频率与 trade/order 事件一致（峰值 ~100K 条/秒），若不回收则日内数据量达 TB 级。但 Checkpoint 机制保证了活跃窗口极短（两次 Snapshot 间隔，通常仅毫秒级），因此**实际需要保留的数据量很小，关键是回收要快、扩容不能阻塞主线程**。

### D.2 设计：Segmented Ring Buffer

将 Delta Ring Buffer 实现为**一组等大的 Segment 构成的逻辑环**。每个 Segment 内部是连续定长数组，正常写入路径上无分支、无分配，cache locality 与单块 ring buffer 一致。

```text
┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐
│ Segment 0 │───→│ Segment 1 │───→│ Segment 2 │───→│ Segment 3 │───→ ...
│ [Delta 0 ]│    │ [Delta K ]│    │ [Delta 2K]│    │ [Delta 3K]│
│ [Delta 1 ]│    │ [Delta K+1]│   │ [  ...   ]│    │ [  ...   ]│
│ [  ...   ]│    │ [  ...   ]│    │ [Delta   ]│    │ [        ]│ ← write_pos
│ [Delta K-1]│   │ [Delta 2K-1]│  │ [Delta   ]│    │ [        ]│
└───────────┘    └───────────┘    └───────────┘    └───────────┘
       ↑                                ↑
  checkpoint_pos                   read_pos (Snapshot-Composer 回放到这里)
  (此段及之前可回收)
```

**核心结构：**

```cpp
struct Segment {
    Delta entries[SEGMENT_CAPACITY];   // 连续定长数组，例如 1024 条
    uint32_t count = 0;                // 当前已写入条目数
    Segment* next = nullptr;           // 逻辑链表指针
};

struct SegmentedRingBuffer {
    // ---- 写入端 (TS-Engine 独占) ----
    Segment* write_seg;                // 当前正在写入的 Segment
    uint32_t write_offset;             // 段内偏移

    // ---- 读取端 (Snapshot-Composer 独占) ----
    Segment* read_seg;                 // 当前正在回放的 Segment
    uint32_t read_offset;              // 段内偏移

    // ---- 回收边界 ----
    Segment* checkpoint_seg;           // 最近一次 Checkpoint 所在 Segment
    uint32_t checkpoint_offset;        // 段内偏移

    // ---- 空闲池 ----
    Segment* free_head;                // 回收后的 Segment 链表（LIFO，热缓存友好）
    uint32_t free_count;               // 空闲段数
    uint32_t total_segments;           // 总分配段数（用于监控）
};
```

### D.3 三条关键路径

**写入 (TS-Engine，热路径)：**

```text
1. write_seg->entries[write_offset] = delta    // 连续内存写入
2. write_offset++
3. 若 write_offset == SEGMENT_CAPACITY：
   a. 从 free pool 取一个 Segment（O(1) 链表��头）
   b. 若 free pool 为空 → malloc 新 Segment，total_segments++
   c. write_seg->next = new_seg
   d. write_seg = new_seg，write_offset = 0
```

正常情况下步骤 3 不会执行（一个 Segment 容纳 1024 条，峰值 100K/s 下约 10ms 才切一次段）。绝大部分写入只执行步骤 1-2，等价于普通 ring buffer 的 append。

**回放 (Snapshot-Composer)：**

```text
loop:
    若 read_seg == write_seg && read_offset == write_offset → 已追平，退出
    delta = read_seg->entries[read_offset]
    checkpoint_mirror[delta.symbol] = delta.ts_feature_vector   // 覆盖该 symbol
    read_offset++
    若 read_offset == read_seg->count：
        read_seg = read_seg->next
        read_offset = 0
```

段内顺序遍历，完美的线性预取。

**Checkpoint 回收 (Snapshot-Composer，Snapshot 发布后)：**

```text
1. checkpoint_seg = read_seg，checkpoint_offset = read_offset
2. 从链表头部开始，将所有 checkpoint_seg 之前的 Segment 整段归还 free pool：
   while (head != checkpoint_seg):
       next = head->next
       head->count = 0
       head->next = free_head    // 归还 free pool
       free_head = head
       free_count++
       head = next
```

回收粒度是整段，O(回收段数) 且通常只有 1-2 段。

### D.4 容量特性

| 场景 | Segment 活跃数 | 总内存 |
|:---|:---|:---|
| 稳态（Snapshot 间隔 ~5ms，100K events/s） | 1-2 段 | ~8-16 MB |
| 短暂积压（Snapshot-Composer 偶尔慢 50ms） | ~10 段 | ~80 MB |
| 极端积压（Snapshot-Composer 卡死 1s） | ~100 段 | ~800 MB |

极端积压场景下 malloc 新段的开销（~µs 级）相对积压本身可以忽略。正常运行时 free pool 循环利用，**不会产生任何 malloc/free**。

### D.5 Segment 容量选择

建议 `SEGMENT_CAPACITY` 使单个 Segment 大小落在 **4-8 MB**（对齐 huge page 2MB 的整数倍更佳）：

```text
单条 Delta ≈ 8 + 4 + 1000 × 8 = 8012 Bytes ≈ 8 KB
SEGMENT_CAPACITY = 512 → Segment ≈ 4 MB
SEGMENT_CAPACITY = 1024 → Segment ≈ 8 MB
```

取 512 或 1024 均可。段越大切段越少（更少的分支），但 Checkpoint 回收粒度也更粗。推荐 V1 先取 **1024**。

### D.6 为什么不用单块 realloc ring buffer

| 维度 | 分段环形缓冲区 | 单块 realloc |
|:---|:---|:---|
| 扩容开销 | O(1) 挂新段 | O(active_data) 拷贝 + 重映射 |
| 主线程抖动 | 无（malloc 新段 ~µs，且极少触发） | 扩容时有确定性大拷贝 |
| 回收粒度 | 整段归还 free pool，O(1) | tail 推进，但无法缩容释放内存 |
| cache locality | 段内连续，与单块等效 | 全局连续 |
| 实现复杂度 | 略高（需管理段链表） | 低 |

唯一劣势是跨段边界时有一次指针跳转，但频率极低（每写满一段才发生一次），对整体性能影响可忽略。
