# mmap 读取 Journal 与并发控制

本文档整理了关于 journal 使用 mmap 读取的原理，以及多进程并发控制的机制。

## 1. 为什么用 mmap 读取 Journal

`LocalJournalPage::load()` (`src/data_consumer.cpp:62-84`) 使用 `mmap` 将 journal 文件映射到进程虚拟地址空间：

```cpp
buffer = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
close(fd);
```

| 参数 | 含义 |
|------|------|
| `nullptr` | 让内核自动选择映射地址 |
| `size` | 映射整个文件 |
| `PROT_READ` | 只读权限 |
| `MAP_SHARED` | 共享映射，多进程可见同一物理页 |
| `fd` | 文件描述符 |
| `0` | 从文件头开始映射 |

mmap 成功后 `close(fd)` 不影响映射，`buffer` 指针可以直接当内存数组使用：

```cpp
// src/data_consumer.cpp:89
LocalFrameHeader* header = (LocalFrameHeader*)((char*)buffer + current_pos);
```

析构时由 `munmap(buffer, size)` (`src/data_consumer.cpp:58`) 释放映射。

### mmap vs read() 对比

| | `mmap` | `read()` |
|---|---|---|
| 内存拷贝次数 | 1次（磁盘 → page cache，直接访问）| 2次（磁盘 → page cache → 用户 buffer）|
| 访问方式 | 指针随机访问，零系统调用 | 需要 lseek + read，每次都是系统调用 |
| 多进程共享 | 共享同一物理页，节省内存 | 各自独立 buffer |
| 适用场景 | 大文件、随机访问、多次读同一��域 | 小文件、流式一次性读取 |

Journal 文件可能很大，且同一页数据会被多次访问（解析不同 frame），适合用 mmap。

## 2. mmap 底层原理：按需加载而非提前载入

调用 `mmap` 时，内核**只建立页表映射关系**，并不实际读取任何数据：

```
进程虚拟地址空间         物理内存（page cache）
[0x7f000000          ]  ←—— 页表条目：尚未映射
[0x7f001000          ]  ←—— 页表条目：尚未映射
[0x7f002000          ]  ←—— 页表条目：尚未映射
```

首次访问某个地址时触发**缺页中断（Page Fault）**：

```
CPU 访问 *((char*)buffer + 4096)
    ↓
MMU 查页表 → 该页无物理内存映射
    ↓
触发 Page Fault，陷入内核
    ↓
内核从磁盘读入对应 4KB 页到 page cache
    ↓
更新页表，建立虚拟地址 → 物理页的映射
    ↓
返回用户态，CPU 重新执行指令（命中）
```

之后访问同一页直接命中，不再触发 Page Fault。

**单次随机访问的磁盘 IO 开销**与 `lseek+read` 相当，mmap 的优势在于：
- 省去一次内存拷贝（不需要从 page cache 再拷到用户 buffer）
- 多次访问同一区域时完全在用户态完成，无系统调用开销

## 3. 多进程并发控制

### MAP_SHARED 的可见性

`MAP_SHARED` 下，多个进程映射同一文件时共享同一物理页：

```
进程A（Writer）              物理页（page cache）         进程B（Reader）
虚拟地址 0x7f000000  ←——→  [同一块物理内存]  ←——→  虚拟地址 0x6e000000
```

Writer 写入修改的是这块物理内存本身，CPU 通过 **MESI 协议**保证多核 cache line 一致性，Reader 下次访问时能看到修改后的值。

但这**不保证原子性，不防止并发写导致数据撕裂**，并发控制需要程序员自己处理。

### 常见并发控制手段

**1. 把锁放在共享内存里**
```c
pthread_mutex_t* mtx = (pthread_mutex_t*)buffer;
pthread_mutexattr_t attr;
pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED); // 关键
pthread_mutex_init(mtx, &attr);
```

**2. 文件锁（flock / fcntl）**：粒度是整个文件或字节范围，适合简单场景，开销较大。

**3. 原子操作 + 内存屏障**：适合单写多读的无锁设计，这正是 journal 采用的方案。

## 4. Journal 的无锁并发设计

### status 字段作为发布屏障

`LocalFrameHeader` (`src/data_consumer.cpp:32-43`) 中：

```cpp
struct LocalFrameHeader {
    volatile unsigned char status;  // ← volatile，防止编译器缓存到寄存器
    short source;
    long nano;
    int length;
    // ...
};
```

Writer 的写入顺序：
```
1. 写入 frame 数据（payload）
2. 写入 length、msg_type 等字段
3. 最后写 status = 1（JOURNAL_FRAME_STATUS_WRITTEN）← 发布操作
```

Reader 在 `nextFrame()` (`src/data_consumer.cpp:86-99`) 中检查：

```cpp
LocalFrameHeader* nextFrame() {
    if (!buffer || current_pos >= size) return nullptr;

    LocalFrameHeader* header = (LocalFrameHeader*)((char*)buffer + current_pos);

    // status != 1 → 数据尚未就绪，停止读取
    if (header->status != JOURNAL_FRAME_STATUS_WRITTEN) {
        return nullptr;
    }

    current_pos += header->length;
    return header;
}
```

上层调用（`src/data_consumer.cpp:160-177`）：

```cpp
LocalFrameHeader* header;
while ((header = page.nextFrame()) != nullptr) {
    void* data = page.getFrameData(header);
    // 到这里时 status == 1，数据字段已全部就绪
    short msg_type = header->msg_type;
    // ...
}
```

`status` 最后写入，作为"数据已就绪"的信号。Reader 看到 `status == 1` 时，其他字段已经全部写完。

### volatile 的作用与局限

`volatile` 防止编译器将 `status` 的读取优化掉（缓存到寄存器里），确保每次都从内存读取。

但严格来说，`volatile` **不提供 CPU 级别的内存序保证**，正确的写法应使用原子操作和 `memory_order_acquire/release`。x86 的强内存模型（TSO）在实践中让这里的 `volatile` 凑合够用。

### 本代码的实际情况

本代码中 mmap 使用 `PROT_READ`（只读），消费者进程不会写页，并发写入由 KungFu Writer 进程负责。消费者只需通过 `status` 检查确认数据就绪后再读取即可。
