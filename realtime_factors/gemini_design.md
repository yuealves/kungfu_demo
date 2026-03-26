# realtime_factors 极低延迟流式计算架构设计方案

本文档描述了一个基于 C++ 共享内存 (SHM) 和操作系统写时复制 (CoW) 机制的极低延迟实时因子计算框架设计。该方案彻底抛弃了定时汇聚的慢节奏模型，转向**真正极速、事件驱动且读写完全解耦**的架构，完美适配下游 Python 进程的高频交易信号生成需求。

---

## 1. 核心设计目标

1. **流式低延迟输出**：因子不依赖定时任务生成，而是随着 L2 行情的到来实时流式计算并输出。
2. **C++ 与 Python 读写解耦**：C++ 侧永远不等待、不加锁，下游 Python 进程的卡顿（如 GC、模型推理耗时）绝对不能影响 C++ 主进程处理行情的延迟。
3. **时序与截面状态隔离**：针对时序因子（单股票历史累积）和截面因子（全市场同时点横截面）采用不同的触发机制和内存布局。
4. **绝对的数据一致性（防撕裂）**：下游 Python 读取全市场截面因子时，必须保证是在同一个微秒级物理时间的完美切片，不能出现“部分旧、部分新”的数据撕裂。
5. **零性能损耗落盘**：因子持久化落盘任务不能占用 C++ 主线程的时间片，不能带来长尾延迟抖动。

---

## 2. 全局架构全景图

系统被划分为四个极度精简的协同模块：

1. **主事件循环 (Main Thread - C++)**
   - 极速轮询读取 KungFu Journal (`order` / `trade` / `tick`)。
   - 维护 Orderbook 状态和**时序因子状态 (TS State)**。
   - 每次状态变更，单点更新时序共享内存（TS SHM）。
   - 标记全市场状态为“脏 (Dirty)”。

2. **截面计算模块 (CS Compute Thread - C++)**
   - 独立线程（或在主线程空闲间隙运行）。
   - 持续检查“脏标志”和**频率上限控制（Throttle，例如最小间隔 0.5ms）**。
   - 满足条件时，捕获当前 TS 状态快照，计算全市场截面因子（如排名、标准化等）。
   - 将计算结果写入 **无锁环形共享内存 (CS Ring Buffer SHM)**。

3. **零损耗持久化模块 (Persister - Fork CoW)**
   - 由主进程内部的定时器触发（例如每 1 分钟或 10 分钟）。
   - 主进程执行 `fork()`。
   - 派生的子进程利用操作系统**写时复制 (Copy-on-Write)** 获得完美的全局一致性内存快照。
   - 子进程在后台将快照数据转换为 Parquet 格式写入磁盘，写完后自行 `exit(0)`。
   - 主进程完全不阻塞，继续处理行情。

4. **下游消费者 (Python Consumer)**
   - 通过轮询共享内存中的 `ready_idx` 或监听轻量级事件唤醒。
   - 采用乐观并发控制（Seqlock）：读取前记录 Version，**0.1ms 极速内存拷贝 (ctypes/numpy)**，读取后校验 Version。
   - 校验通过后，使用本地纯净的 NumPy 数组副本进行策略推理。

---

## 3. 内存与并发控制设计 (核心)

为了彻底解决“慢读者”和“读写撕裂”问题，采用 **多槽位环形缓冲区 (Ring Buffer) + 顺序锁 (Seqlock)** 方案。

### 3.1 共享内存 (SHM) 布局

假设全市场 5,000 只股票，每只提取 100 个双精度浮点数 (double) 作为截面因子，单次截面快照大小约为 `5000 * 100 * 8 Bytes ≈ 4MB`。

我们在 SHM 中开辟 **N 个槽位（例如 N = 64）**。总内存占用约为 `64 * 4MB = 256MB`，对现代服务器毫无压力。

```cpp
#pragma pack(push, 1)

// 单个截面快照槽位
struct CSSnapshotSlot {
    std::atomic<uint64_t> version;    // 顺序锁版本号 (奇数表示正在写入，偶数表示写入完成)
    uint64_t timestamp;               // 截面生成的纳秒时间戳
    double factors[5000][100];        // [SymbolIndex][FactorIndex] 的连续内存块
};

// 截面因子共享内存头部
struct CS_SHM_Region {
    std::atomic<uint32_t> current_ready_idx; // 最新已完成写入的槽位索引 (0 ~ N-1)
    CSSnapshotSlot slots[64];                // 64 槽环形缓冲区
};

#pragma pack(pop)
```

### 3.2 C++ 写入端：无锁推进与微节流防空转

截面计算进程（或线程）**永远不等待下游**，遵循以下规则：

1. **防空转与节流 (Throttle)**：
   - 如果距离上一次生成截面因子**不足 0.5ms**，则跳过计算。
   - 如果期间没有任何 TS 状态发生变化（没有新行情驱动），跳过计算。
   - 这避免了 C++ 在毫无意义的情况下疯狂消耗 CPU 并快速写穿 Ring Buffer 套圈 Python 进程。
2. **写槽位操作**：
   - 获取下一个写入槽位：`next_idx = (current_ready_idx + 1) % 64`。
   - 修改版本号：`slots[next_idx].version.fetch_add(1)` （版本号变奇数，标志开始写入）。
   - 将最新计算好的截面数据 memcpy 写入 `slots[next_idx].factors`。
   - 写入时间戳。
   - 修改版本号：`slots[next_idx].version.fetch_add(1)` （版本号变偶数，标志写入完成）。
   - 发布最新索引：`current_ready_idx.store(next_idx)`。

### 3.3 Python 读取端：极速 Copy 逃逸

Python 进程必须将自己在共享内存上的停留时间压缩到极致，绝对不能直接在 SHM 指针上跑策略模型。

```python
import ctypes
import numpy as np

def fetch_latest_cross_section(shm_region_ptr):
    while True: # 乐观锁重试循环
        # 1. 获取最新槽位索引
        idx = get_current_ready_idx(shm_region_ptr)
        slot_ptr = get_slot_ptr(shm_region_ptr, idx)
        
        # 2. 读取前置 Version
        v1 = get_version(slot_ptr)
        
        # 如果是奇数，说明 C++ 正在写这个槽（极其罕见，说明赶巧了），自旋等待
        if v1 % 2 != 0:
            continue
            
        # 3. 极速内存拷贝逃逸 (4MB 内存拷贝仅需 ~0.1ms)
        # 通过 ctypes 和 numpy.copy() 将 C 连续内存直接强转并复制到 Python 私有内存空间
        local_buffer = (ctypes.c_byte * 4000000).from_address(slot_ptr + offset)
        local_copy = np.ctypeslib.as_array(local_buffer).copy() 
        
        # 4. 读取后置 Version 校验
        v2 = get_version(slot_ptr)
        
        # 5. 校验防撕裂
        if v1 == v2:
            # 数据完美一致，退出重试循环
            return local_copy.view(np.float64).reshape((5000, 100))
        
        # 发生撕裂（说明 Python 被 OS 卡顿调度，C++ 已经跑了 64 圈覆盖了这条数据），重试
```

**防活锁/饥饿分析：**
- C++ 最小写入间隔受 Throttle 保护（`0.5ms`）。跑完一整圈 (64个槽) 最快需要 `64 * 0.5ms = 32ms`。
- Python 内存拷贝仅需 `0.1ms`。
- 只有当 Python 进程在拷贝前被操作系统强行挂起（Suspend）超过 `32ms`，才会发生一次撕裂重试。这在系统级保证了读取方绝对不会饿死。

---

## 4. 零损耗持久化 (Disk Persister)

实时系统最大的性能杀手是磁盘 I/O。本架构利用 Linux `fork()` 进行无锁持久化。

1. 主进程设置一个每分钟（或 5 分钟）触发一次的 Timer。
2. Timer 触发时，主进程执行 `pid = fork()`。
   - `fork()` 耗时极短（几十微秒），仅拷贝页表。
3. **主进程 (pid > 0)**：继续极速处理 L2 行情，延迟几乎无波动。
4. **子进程 (pid == 0)**：
   - 获得了 Fork 瞬间的完美全量 TS 与 CS 内存状态（归功于 OS 的写时复制）。
   - 从容地将内存中的因子矩阵转换为列式存储的 Apache Parquet 格式。
   - 写入磁盘：`/data/factors/YYYYMMDD/factor_HHMM.parquet`。
   - 执行完毕后安全退出：`exit(0)`。
5. **回收机制**：主进程通过 `SIGCHLD` 信号处理函数或在 Event Loop 中定期调用非阻塞的 `waitpid(WNOHANG)` 来回收僵尸子进程。

---

## 5. 总结

本方案在物理层面上分离了三个速度截然不同的流：
- **微秒级 (C++ 核心处理)**：处理 L2 Tick，维护内存状态。
- **亚毫秒级 (C++ 到 Python)**：通过 SHM Seqlock + Throttle，以 0.5ms 为极限间隔向 Python 投递极度安全的无锁截面切片。
- **分钟级 (持久化)**：通过 `fork()` 分离出对延迟极不敏感的磁盘写操作。

最终实现了高频交易系统中梦寐以求的：**写入不阻塞、读取零撕裂、持久化零损耗。**