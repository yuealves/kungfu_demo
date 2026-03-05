# insight_gateway 行情写入概览

## 数据来源

insight_gateway 通过华泰 Insight SDK（`mdc_gateway_client`）获取行情数据。

**连接方式**：TCP 长连接 + TLS 加密（需要证书，存放在 `cert/` 目录）。不是 HTTP，不是 WebSocket，是 Insight SDK 自有的 TCP 二进制协议（protobuf 序列化）。

**连接过程**：
```
client = ClientFactory::Instance()->CreateClient(true, cert_folder);  // TLS
client->LoginByServiceDiscovery(ip, port, user, password, false, backup_list);  // 主+备服务器
client->SubscribeBySourceType(...);  // 订阅沪深全市场股票的 tick/order/trade
```

登录后 SDK 自动维护连接，交易所有新行情时 SDK 回调 `OnMarketData()`，是**推送模式**，不是轮询。

## 写入流程

```
交易所 ──TCP──→ Insight 服务器 ──TCP/TLS──→ SDK 回调 OnMarketData()
                                                    │
                                          ┌─────────┼─────────┐
                                          ▼         ▼         ▼
                                       MD_TICK   MD_ORDER  MD_TRANSACTION
                                          │         │         │
                                 StockTickData  StockOrderData StockTransaction
                                  (从 protobuf    (从 protobuf   (从 protobuf
                                   提取字段)       提取字段)      提取字段)
                                          │         │         │
                                          ▼         ▼         ▼
                                    ┌─────────────────────────────┐
                                    │  ConcurrentQueue (无锁队列)  │
                                    └─────────────────────────────┘
                                          │         │         │
                                      ParseFrom() ParseFrom() ParseFrom()
                                          │         │         │
                                          ▼         ▼         ▼
                                     KyStdSnpType KyStdOrderType KyStdTradeType
                                     (packed 结构) (packed 结构)  (packed 结构)
                                          │         │         │
                                   write_frame  write_frame  write_frame
                                    (type=61)    (type=62)    (type=63)
                                          │         │         │
                                          ▼         ▼         ▼
                                    journal 文件 (/shared/kungfu/journal/user/)
                                    insight_stock_ insight_stock_ insight_stock_
                                    tick_data      order_data     trade_data
```

中间有个**无锁队列**做缓冲，SDK 回调线程只负责把 protobuf 数据转成中间结构扔进队列（快），另外 3 个 writer 线程各自从队列取数据、转成 packed 二进制结构、调用 KungFu 的 `JournalWriter::write_frame()` 写入 journal（稍慢）。

这就是为什么 insight_gateway 99% CPU——3 个 writer 线程空转等数据（busy-wait，不 sleep）。

## write_frame 写入细节

`write_frame()` 最终调用的是 `write_frame_full()`。整个写入过程发生在 **mmap 的共享内存**上，不经过任何 socket 或管道。

### journal 文件的内存结构

```
journal 文件 (128MB, mmap 映射到内存)
┌──────────────────────────────────────────────────────────────┐
│ Page Header (56 bytes)                                       │
├──────────────────────────────────────────────────────────────┤
│ Frame 0: [FrameHeader 40B][payload N bytes]                  │  status=WRITTEN ✓ 已写
│ Frame 1: [FrameHeader 40B][payload N bytes]                  │  status=WRITTEN ✓ 已写
│ Frame 2: [FrameHeader 40B][payload N bytes]                  │  status=WRITTEN ✓ 已写
│ Frame 3: [FrameHeader 40B]                                   │  status=RAW     ← writer 下次写这里
│          ...                                                 │
│ (剩余空间)                                                    │  全是 0
└──────────────────────────────────────────────────────────────┘
```

FrameHeader 结构（40 字节，`FrameHeader.h`）：

```c
struct FrameHeader {
    volatile byte   status;      // 0=RAW, 1=WRITTEN, 2=PAGE_END  ← 关键字段
    short           source;      // 数据来源 ID
    long            nano;        // 纳秒时间戳
    int             length;      // 整帧长度（header + payload）
    uint            hash;        // payload 的哈希
    short           msg_type;    // 消息类型 (61/62/63)
    byte            last_flag;   // 是否最后一帧
    int             req_id;      // 请求 ID
    long            extra_nano;  // 额外时间戳
    int             err_id;      // 错误 ID
} __attribute__((packed));
```

### 写入一帧的步骤

`write_frame_full()` 做的事情：

```
1. 定位写入位置
   Journal::locateFrame()
     → Page::locateWritableFrame()
       → 跳过所有 status==WRITTEN 的帧
       → 找到第一个 status==RAW 的帧地址
       → 如果剩余空间不足 2MB，返回 nullptr → 触发换页

2. 填写 FrameHeader
   frame.setSource(1)
   frame.setNano(当前纳秒时间戳)
   frame.setMsgType(61/62/63)
   frame.setLastFlag(1)
   frame.setRequestId(-1)

3. 拷贝 payload
   frame.setData(data, length)
     → memcpy(header地址 + 40, &KyStdSnpType, sizeof(KyStdSnpType))
     → 设置 frame.length = 40 + sizeof(KyStdSnpType)

4. 标记写入完成（原子性关键步骤）
   frame.setStatusWritten()
     → 先把【下一帧】的 status 设为 RAW (防止 reader 误读)
     → 再把【当前帧】的 status 设为 WRITTEN
```

**第 4 步是整个机制的关键**——先确保下一帧是 RAW（边界哨兵），再把当前帧标记为 WRITTEN。因为 `status` 字段声明了 `volatile`，这个写操作对其他进程（reader）立即可见。

### 换页

当一个 page 剩余空间不足 `PAGE_MIN_HEADROOM`（2MB）时，`locateWritableFrame()` 返回 nullptr，触发 `Journal::loadNextPage()`：
- 如果是 ClientPageProvider（需要 Paged）：向 Paged 请求分配下一个 page（page_num + 1），Paged 负责 mmap 新文件
- 如果是 LocalPageProvider：直接本地 mmap 下一个 journal 文件

## reader 怎么知道有新数据？

**没有任何通知机制。reader 靠轮询 mmap 共享内存中的 `status` 字段来发现新数据。**

### 原理

Writer 和 reader 进程 mmap 了同一个 journal 文件，操作系统保证它们看到的是**同一块物理内存**。不需要 socket、管道、信号量——对内存的写入对所有 mmap 了该文件的进程**立即可见**。

```
insight_gateway 进程                    data_fetcher 进程
        │                                      │
        │  mmap("yjj.insight_stock_            │  mmap(同一个文件)
        │        tick_data.1.journal")          │
        │         │                            │         │
        │         ▼                            │         ▼
        │   ┌───────────┐                      │   ┌───────────┐
        │   │ 物理内存页  │◄─── 同一块物理内存 ───►│   │ 物理内存页  │
        │   └───────────┘                      │   └───────────┘
        │                                      │
        │  写入:                                │  读取:
        │  1. 填 header + payload              │  while (true) {
        │  2. status = WRITTEN                 │    status = frame->status
        │     ↓ 立即可见                        │    if (status == WRITTEN)
        │                                      │      → 读到新数据！
        │                                      │    else (status == RAW)
        │                                      │      → 没有新数据，继续轮询
        │                                      │  }
```

### reader 读取流程

`JournalReader::getNextFrame()`（`JournalReader.h:119-146`）：

```
遍历所有订阅的 journal:
  对每个 journal 调用 locateFrame()
    → Page::locateReadableFrame()
      → 检查当前帧的 status
      → status == WRITTEN → 返回帧地址（有新数据）
      → status == RAW     → 返回 nullptr（没有新数据）
      → status == PAGE_END → 加载下一页

如果多个 journal 同时有新帧，取 nano 时间戳最小的那个（按时间顺序合并）
如果所有 journal 都没有新帧，返回空指针
```

### Paged 在读写通知中的角色

**Paged 不参与数据通知。** Paged 的角色仅限于：
1. **启动时**：帮 writer/reader 完成 journal 文件的 mmap 映射
2. **换页时**：当一个 128MB 的 page 写满，Paged 分配下一个 page 文件并 mmap

数据写入和读取发现完全是 writer 和 reader 两个进程之间通过 **mmap 共享内存 + volatile status 字段轮询**实现的，Paged 进程不在数据通路上。

```
Paged 的作用范围:
  ✓ mmap 文件映射管理
  ✓ 换页时分配新 page
  ✗ 不参与"通知 reader 有新数据"
  ✗ 不转发数据
  ✗ 数据不经过 Paged
```

## 关键代码位置

| 内容 | 文件 | 关键点 |
|------|------|--------|
| main() 入口 | `insight_gateway_run.cpp` | 全文 |
| 创建 3 个 writer | `insight_gateway.cpp` | `init()` |
| SDK 回调分发 | `insight_gateway.cpp` | `OnMarketData()` |
| 异步写入器 | `insight_gateway.h` | `AsyncBatchWriter` 模板类 |
| protobuf → 中间结构 | `insight_types.h` | `StockTickData(data)` 等构造函数 |
| 中间结构 → packed 结构 | `insight_types.h` | `ParseFrom()` 方法 |
| FrameHeader 定义 | `FrameHeader.h` | `volatile status` 字段 |
| 写入标记 | `Frame.hpp` | `setStatusWritten()` |
| 读取检查 | `Page.h` | `locateReadableFrame()` |
| 多 journal 合并读取 | `JournalReader.h` | `getNextFrame()` |
