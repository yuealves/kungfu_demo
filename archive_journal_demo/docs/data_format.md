# 数据格式说明：Insight SDK → Kungfu Journal → Parquet

本文档描述 L2 行情数据从华泰 Insight SDK 到 Kungfu Journal 文件、再到 Parquet 归档文件的完整字段流转过程。

---

## 1. 整体数据流

```
华泰 Insight SDK (Protobuf)
        │
        │  insight_gateway 回调 OnMarketData()
        │  ▸ insight_gateway.cpp:15
        ▼
中间结构体 (StockTickData / StockOrderData / StockTransaction)
        │  ▸ insight_types.h:36 / :714 / :578
        │
        │  ParseFrom() 转换
        │  ▸ insight_types.h:258 / :811 / :677
        ▼
Packed 结构体 (KyStdSnpType / KyStdOrderType / KyStdTradeType)
        │  ▸ data_struct.hpp:86 / :258 / :25
        │
        │  AsyncBatchWriter::process_queue() → write_frame()
        │  ▸ insight_gateway.h:71
        ▼
Kungfu Journal 文件 (mmap 二进制, 按 channel 分文件)
        │  ▸ journal_reader.h:10-34 (Page/Frame Header 定义)
        │
        │  archive_journal 读取 → convert_*() → write_*_parquet()
        │  ▸ main.cpp:52-107 / conversion.cpp:11-94 / parquet_writer.cpp:101-413
        ▼
Parquet 归档文件 (tick_data / order_data / trade_data)
```

> **路径约定**：下文中未标注完整路径的文件，gateway 侧位于
> `ht_insight_market_gateway/insight_market_gateway/src/`，
> archive 侧位于 `archive_journal_demo/src/`。

三类数据对应三个独立的 journal channel（初始化见 `insight_gateway.cpp:140-142`）：

| 数据类型 | Journal Channel 名 | MSG_TYPE | Journal 文件名模式 |
|---------|-------------------|----------|-------------------|
| Tick 快照 | `insight_stock_tick_data` | 61 | `yjj.insight_stock_tick_data.*.journal` |
| 逐笔委托 | `insight_stock_order_data` | 62 | `yjj.insight_stock_order_data.*.journal` |
| 逐笔成交 | `insight_stock_trade_data` | 63 | `yjj.insight_stock_trade_data.*.journal` |

---

## 2. Kungfu Journal 二进制格式

每个 journal 文件由一个 **Page Header** + 若干 **Frame** 组成，通过 mmap 映射到内存后顺序遍历。

> 定义位于 `archive_journal_demo/src/journal_reader.h:10-34`，
> 读取逻辑位于 `journal_reader.cpp:20-58`。

### 2.1 Page Header（文件头）

```c
// journal_reader.h:10-21
struct LocalPageHeader {
    unsigned char status;                    // 1B   页面状态
    char journal_name[30];                   // 30B  journal 名称
    short page_num;                          // 2B   页码
    long start_nano;                         // 8B   页面起始纳秒时间戳
    long close_nano;                         // 8B   页面关闭纳秒时间戳
    int frame_num;                           // 4B   帧总数
    int last_pos;                            // 4B   最后一帧位置
    short frame_version;                     // 2B   帧格式版本
    short reserve_short[3];                  // 6B   保留
    long reserve_long[9];                    // 72B  保留
} __attribute__((packed));                   // 合计 137 字节
```

### 2.2 Frame Header（帧头，38 字节）

```c
// journal_reader.h:23-34
struct LocalFrameHeader {
    volatile unsigned char status;           // 1B   帧状态（1 = 已写入）
    short source;                            // 2B   数据源标识
    long nano;                               // 8B   帧纳秒时间戳 ★
    int length;                              // 4B   帧总长度（含帧头 + 数据）
    unsigned int hash;                       // 4B   数据校验
    short msg_type;                          // 2B   消息类型（61/62/63）★
    unsigned char last_flag;                 // 1B   末帧标记
    int req_id;                              // 4B   请求 ID
    long extra_nano;                         // 8B   附加时间戳
    int err_id;                              // 4B   错误 ID
} __attribute__((packed));                   // 合计 38 字节
```

关键字段：
- **`nano`**：帧写入时的纳秒时间戳，最终写入 parquet 的 `nano_timestamp` 列
- **`msg_type`**：决定帧数据应解析为哪种 packed 结构体（61=Tick，62=Order，63=Trade）
- **`length`**：帧总长度，用于跳转到下一帧（`current_pos += header->length`）

### 2.3 帧数据（Frame Data）

帧数据紧跟在 Frame Header 之后（偏移 `sizeof(LocalFrameHeader)` 字节），内容为对应的 packed 结构体的二进制表示，通过指针强转直接解析（`journal_reader.cpp:56-58`）。

### 2.4 遍历流程

```
// journal_reader.cpp:43-54
1. mmap 打开 journal 文件
2. current_pos = sizeof(LocalPageHeader)     // 跳过文件头
3. while (current_pos < file_size):
     header = (LocalFrameHeader*)(buffer + current_pos)
     if header->status != 1: break           // 未写入帧，结束
     data = (char*)header + sizeof(LocalFrameHeader)
     根据 header->msg_type 解析 data          // main.cpp:62-66
     current_pos += header->length           // 跳到下一帧
```

---

## 3. Tick 快照（MSG_TYPE = 61）

### 3.1 Insight SDK → KyStdSnpType

Gateway 从 Insight SDK 接收 Protobuf `MarketData.mdstock()` 消息（`insight_gateway.cpp:29-35`），经 `StockTickData` 中间结构体（`insight_types.h:36-325`）转换为 `KyStdSnpType` packed 结构体写入 journal。

**Insight SDK 原始字段（Protobuf `MDStock`，接收于 `insight_types.h:90-197`）**：

| SDK 字段 | 类型 | 说明 |
|----------|------|------|
| `htscsecurityid` | string | 证券代码（如 "000001.SZ"） |
| `mddate` | int | 日期（YYYYMMDD） |
| `mdtime` | int | 时间（HHMMSSmmm） |
| `securityidsource` | string | 交易所标识 |
| `preclosepx` | long | 昨收价 |
| `openpx` | long | 开盘价 |
| `highpx` | long | 最高价 |
| `lowpx` | long | 最低价 |
| `lastpx` | long | 最新价 |
| `totalvolumetrade` | long | 总成交量 |
| `totalvaluetrade` | long | 总成交额 |
| `totalbuyqty` | long | 总买量 |
| `totalsellqty` | long | 总卖量 |
| `weightedavgbuypx` | long | 加权平均买入价 |
| `weightedavgsellpx` | long | 加权平均卖出价 |
| `numtrades` | long | 成交笔数 |
| `buypricequeue[0..9]` | long[] | 买一到买十价（`insight_types.h:120-131`） |
| `buyorderqtyqueue[0..9]` | long[] | 买一到买十量（`insight_types.h:133-144`） |
| `sellpricequeue[0..9]` | long[] | 卖一到卖十价（`insight_types.h:146-157`） |
| `sellorderqtyqueue[0..9]` | long[] | 卖一到卖十量（`insight_types.h:159-170`） |
| `afterhourslastpx` | long | 盘后最新价 |
| `afterhourstotalvaluetrade` | long | 盘后成交额 |
| `afterhourstotalvolumetrade` | long | 盘后成交量 |
| `afterhoursnumtrades` | long | 盘后成交笔数 |
| `diffpx1` | long | 涨跌（**未写入 journal**） |
| `tradingphasecode` | string | 交易阶段代码（**未写入 journal**） |
| `buyorderqueue[0..9]` | long[] | 买方委托队列（**未写入 journal**） |
| `sellorderqueue[0..9]` | long[] | 卖方委托队列（**未写入 journal**） |

**`StockTickData.ParseFrom()` 转换逻辑**（`insight_types.h:258-324`）：

```
htscsecurityid  →  截取数字部分 → Symbol (int32)        // :260
                   例: "000001.SZ" → 1, "600000.SH" → 600000
mdtime          →  Time (int32，保持 HHMMSSmmm 格式)    // :277
totalvaluetrade →  AccTurnover (int64)                   // :262
totalvolumetrade → AccVolume (int64)                     // :263
afterhoursnumtrades → AfterMatchItem (int32)             // :264
afterhourslastpx    → AfterPrice (float，long→float)    // :265
afterhourstotalvaluetrade → AfterTurnover (int64)        // :266
afterhourstotalvolumetrade → AfterVolume (int32)         // :267
weightedavgsellpx → AskAvgPrice (float)                  // :268
sellpricequeue[0..9] → AskPx1..AskPx10 (float)          // :303-312
sellorderqtyqueue[0..9] → AskVol1..AskVol10 (float64)   // :313-322
0 (硬编码)       →  BSFlag (int8，Tick 无买卖方向)       // :269
weightedavgbuypx → BidAvgPrice (float)                   // :270
buypricequeue[0..9] → BidPx1..BidPx10 (float)           // :283-292
buyorderqtyqueue[0..9] → BidVol1..BidVol10 (float64)    // :293-302
highpx          →  High (float)                          // :271
lowpx           →  Low (float)                           // :272
numtrades       →  MatchItem (int32)                     // :273
openpx          →  Open (float)                          // :274
preclosepx      →  PreClose (float)                      // :275
lastpx          →  Price (float)                         // :276
totalsellqty/1000 → TotalAskVolume (int64)               // :278
totalbuyqty/1000  → TotalBidVolume (int64)               // :279
0 (硬编码)       →  Turnover (int64)                     // :280
0 (硬编码)       →  Volume (int32)                       // :281
symbol          →  Symbol (int32)                        // :282
0 (构造函数体内) → BizIndex (int32)                      // data_struct.hpp:218
```

> **注意**：Insight SDK 的价格/金额字段为 long 整数（含精度倍率），gateway 转换时直接 `static_cast<float>` 截断，**未做除法还原小数**。上游使用时需注意价格单位。`TotalAskVolume` 和 `TotalBidVolume` 除以了 1000。`Turnover`、`Volume` 被硬编码为 0（gateway 构造函数参数位置问题）。`BizIndex` 在构造函数体内被设为 0。

### 3.2 KyStdSnpType 结构体（Journal 中的二进制布局）

```c
// data_struct.hpp:86-256 (gateway)
// archive_journal_demo/src/data_types.h:32-97 (reader, 与 gateway 一致)
struct KyStdSnpType {           // 总计 349 字节, __attribute__((packed))
    int64_t   AccTurnover;      // 8B   累计成交额
    int64_t   AccVolume;        // 8B   累计成交量
    int32_t   AfterMatchItem;   // 4B   盘后成交笔数
    float     AfterPrice;       // 4B   盘后价格
    int64_t   AfterTurnover;    // 8B   盘后成交额
    int32_t   AfterVolume;      // 4B   盘后成交量
    float     AskAvgPrice;      // 4B   卖方加权均价
    float     AskPx1..AskPx10;  // 40B  卖一到卖十价（10 × float）
    float64_t AskVol1..AskVol10;// 80B  卖一到卖十量（10 × double）
    int8_t    BSFlag;           // 1B   买卖方向标记
    float     BidAvgPrice;      // 4B   买方加权均价
    float     BidPx1..BidPx10;  // 40B  买一到买十价（10 × float）
    float64_t BidVol1..BidVol10;// 80B  买一到买十量（10 × double）
    float     High;             // 4B   最高价
    float     Low;              // 4B   最低价
    int32_t   MatchItem;        // 4B   成交笔数
    float     Open;             // 4B   开盘价
    float     PreClose;         // 4B   昨收价
    float     Price;            // 4B   最新价
    int32_t   Time;             // 4B   时间
    int64_t   TotalAskVolume;   // 8B   总卖量
    int64_t   TotalBidVolume;   // 8B   总买量
    int64_t   Turnover;         // 8B   成交额
    int32_t   Volume;           // 4B   成交量
    int32_t   Symbol;           // 4B   证券代码（数字）
    int32_t   BizIndex;         // 4B   业务序号
};
```

### 3.3 KyStdSnpType → Parquet

读取 journal 后（`main.cpp:62-66`），将 KyStdSnpType 的全部字段 **1:1 直接拷贝** 到 TickRecord（`conversion.cpp:11-57`），外加两个派生字段。

Parquet schema 定义于 `parquet_writer.cpp:32-98`，写入逻辑于 `parquet_writer.cpp:101-222`：

| Parquet 列名 | Arrow 类型 | 来源 | 说明 |
|-------------|-----------|------|------|
| **nano_timestamp** | int64 | **Frame Header `.nano`** | 帧纳秒时间戳（**派生字段**） |
| **exchange** | utf8 | **Symbol 推导**（`conversion.cpp:3-8`） | Symbol < 400000 → "SZ"，< 700000 → "SH" |
| Symbol | int32 | KyStdSnpType.Symbol | 证券代码 |
| Time | int32 | KyStdSnpType.Time | 时间 |
| AccTurnover | int64 | KyStdSnpType.AccTurnover | 累计成交额 |
| AccVolume | int64 | KyStdSnpType.AccVolume | 累计成交量 |
| AfterMatchItem | int32 | KyStdSnpType.AfterMatchItem | 盘后成交笔数 |
| AfterPrice | float32 | KyStdSnpType.AfterPrice | 盘后价格 |
| AfterTurnover | int64 | KyStdSnpType.AfterTurnover | 盘后成交额 |
| AfterVolume | int32 | KyStdSnpType.AfterVolume | 盘后成交量 |
| AskAvgPrice | float32 | KyStdSnpType.AskAvgPrice | 卖方加权均价 |
| AskPx1..AskPx10 | float32 | KyStdSnpType.AskPx1..10 | 卖一到卖十价 |
| AskVol1..AskVol10 | float64 | KyStdSnpType.AskVol1..10 | 卖一到卖十量 |
| BSFlag | int8 | KyStdSnpType.BSFlag | 买卖标记 |
| BidAvgPrice | float32 | KyStdSnpType.BidAvgPrice | 买方加权均价 |
| BidPx1..BidPx10 | float32 | KyStdSnpType.BidPx1..10 | 买一到买十价 |
| BidVol1..BidVol10 | float64 | KyStdSnpType.BidVol1..10 | 买一到买十量 |
| High | float32 | KyStdSnpType.High | 最高价 |
| Low | float32 | KyStdSnpType.Low | 最低价 |
| MatchItem | int32 | KyStdSnpType.MatchItem | 成交笔数 |
| Open | float32 | KyStdSnpType.Open | 开盘价 |
| PreClose | float32 | KyStdSnpType.PreClose | 昨收价 |
| Price | float32 | KyStdSnpType.Price | 最新价 |
| TotalAskVolume | int64 | KyStdSnpType.TotalAskVolume | 总卖量 |
| TotalBidVolume | int64 | KyStdSnpType.TotalBidVolume | 总买量 |
| Turnover | int64 | KyStdSnpType.Turnover | 成交额 |
| Volume | int32 | KyStdSnpType.Volume | 成交量 |
| BizIndex | int32 | KyStdSnpType.BizIndex | 业务序号 |

**合计 64 列**（2 个派生 + 62 个原始字段，其中 AskPx/AskVol/BidPx/BidVol 各展开为 10 列）。

---

## 4. 逐笔委托（MSG_TYPE = 62）

### 4.1 Insight SDK → KyStdOrderType

Gateway 从 Insight SDK 接收 Protobuf `MarketData.mdorder()` 消息（`insight_gateway.cpp:74-80`），经 `StockOrderData` 中间结构体（`insight_types.h:714-850`）转换。

**Insight SDK 原始字段（Protobuf `MDOrder`，接收于 `insight_types.h:741-765`）**：

| SDK 字段 | 类型 | 说明 |
|----------|------|------|
| `htscsecurityid` | string | 证券代码 |
| `mddate` | int | 日期（**未写入 journal**） |
| `mdtime` | int | 时间 |
| `securityidsource` | string | 交易所标识（**未写入 journal**） |
| `securitytype` | string | 证券类型（**未写入 journal**） |
| `orderindex` | long | 委托序号 |
| `ordertype` | int | 委托类型（枚举值） |
| `orderprice` | long | 委托价格 |
| `orderqty` | long | 委托数量 |
| `orderbsflag` | int | 买卖方向（枚举值） |
| `orderno` | long | 原始委托号 |
| `applseqnum` | long | 应用序列号 |
| `channelno` | int | 频道号 |

**`StockOrderData.ParseFrom()` 转换逻辑**（`insight_types.h:811-848`）：

```
htscsecurityid  →  截取数字部分 → Symbol (int32)       // :834
applseqnum      →  BizIndex (int32)                     // :837
channelno       →  Channel (int64)                      // :838
orderbsflag     →  FunctionCode (int8)，枚举映射：      // :813-819
                     1 → 66 ('B', Buy)
                     2 → 83 ('S', Sell)
                     else → 0
ordertype       →  OrderKind (int8)，枚举映射：         // :821-832
                     1 → 49 ('1', 限价委托)
                     2 → 50 ('2', 最优委托)
                     3 → 85 ('U', 未知)
                     10 → 68 ('D', 撤单)
                     else → 65 ('A', 默认)
orderindex      →  OrderNumber (int32)                  // :841
orderno         →  OrderOriNo (int32)                   // :842
orderprice      →  Price (float，long→float 截断)      // :843
mdtime          →  Time (int32)                         // :844
orderqty        →  Volume (int32)                       // :845
```

> **丢弃的字段**：`mddate`（日期）、`securityidsource`（交易所）、`securitytype`（证券类型）未写入 journal。日期信息可通过 `nano_timestamp` 恢复。

### 4.2 KyStdOrderType 结构体（Journal 中的二进制布局）

```c
// data_struct.hpp:258-327 (gateway)
// archive_journal_demo/src/data_types.h:99-112 (reader, 与 gateway 一致)
struct KyStdOrderType {         // 总计 38 字节, __attribute__((packed))
    int32_t BizIndex;           // 4B   业务序号（来自 applseqnum）
    int64_t Channel;            // 8B   频道号
    int8_t  FunctionCode;       // 1B   买卖方向（'B'=66/'S'=83）
    int8_t  OrderKind;          // 1B   委托类型（'1'/'2'/'U'/'D'/'A'）
    int32_t OrderNumber;        // 4B   委托序号（来自 orderindex）
    int32_t OrderOriNo;         // 4B   原始委托号（来自 orderno）
    float   Price;              // 4B   委托价格
    int32_t Time;               // 4B   时间
    int32_t Volume;             // 4B   委托数量
    int32_t Symbol;             // 4B   证券代码
};
```

### 4.3 KyStdOrderType → Parquet

全部字段 1:1 直接拷贝（`conversion.cpp:60-74`），外加两个派生字段。

Parquet schema 定义于 `parquet_writer.cpp:227-242`，写入逻辑于 `parquet_writer.cpp:244-314`：

| Parquet 列名 | Arrow 类型 | 来源 | 说明 |
|-------------|-----------|------|------|
| **nano_timestamp** | int64 | **Frame Header `.nano`** | 帧纳秒时间戳（**派生字段**） |
| **exchange** | utf8 | **Symbol 推导** | "SZ" 或 "SH"（**派生字段**） |
| Symbol | int32 | KyStdOrderType.Symbol | 证券代码 |
| BizIndex | int32 | KyStdOrderType.BizIndex | 业务序号 |
| Channel | int64 | KyStdOrderType.Channel | 频道号 |
| FunctionCode | int8 | KyStdOrderType.FunctionCode | 买卖方向 |
| OrderKind | int8 | KyStdOrderType.OrderKind | 委托类型 |
| OrderNumber | int32 | KyStdOrderType.OrderNumber | 委托序号 |
| OrderOriNo | int32 | KyStdOrderType.OrderOriNo | 原始委托号 |
| Price | float32 | KyStdOrderType.Price | 委托价格 |
| Time | int32 | KyStdOrderType.Time | 时间 |
| Volume | int32 | KyStdOrderType.Volume | 委托数量 |

**合计 12 列**（2 个派生 + 10 个原始字段）。

---

## 5. 逐笔成交（MSG_TYPE = 63）

### 5.1 Insight SDK → KyStdTradeType

Gateway 从 Insight SDK 接收 Protobuf `MarketData.mdtransaction()` 消息（`insight_gateway.cpp:64-70`），经 `StockTransaction` 中间结构体（`insight_types.h:578-712`）转换。

**Insight SDK 原始字段（Protobuf `MDTransaction`，接收于 `insight_types.h:608-633`）**：

| SDK 字段 | 类型 | 说明 |
|----------|------|------|
| `htscsecurityid` | string | 证券代码 |
| `mddate` | int | 日期（**未写入 journal**） |
| `mdtime` | int | 时间 |
| `securityidsource` | string | 交易所标识（**未写入 journal**） |
| `tradeindex` | long | 成交序号 |
| `tradebuyno` | long | 买方委托号 |
| `tradesellno` | long | 卖方委托号 |
| `tradebsflag` | int | 主动买卖标记（枚举值） |
| `tradeprice` | long | 成交价格 |
| `tradeqty` | long | 成交数量 |
| `trademoney` | long | 成交金额（**未写入 journal**） |
| `applseqnum` | long | 应用序列号 |
| `channelno` | long | 频道号 |
| `tradetype` | int | 成交类型（枚举值） |

**`StockTransaction.ParseFrom()` 转换逻辑**（`insight_types.h:677-711`）：

```
htscsecurityid  →  截取数字部分 → Symbol (int32)       // :694
tradesellno     →  AskOrder (int32)                     // :696
tradebsflag     →  BSFlag (int8)，枚举映射：            // :679-685
                     1 → 66 ('B', Buy)
                     2 → 83 ('S', Sell)
                     else → 0
tradebuyno      →  BidOrder (int32)                     // :698
applseqnum      →  BizIndex (int32)                     // :699
channelno       →  Channel (int32)                      // :700
tradetype       →  FunctionCode (int8)，枚举映射：      // :687-691
                     0 或 9 → 70 ('F', 正常成交)
                     else → 52 ('4', 其他)
tradeindex      →  Index (int32)                        // :702
0 (硬编码)       →  OrderKind (int8，始终为 0)          // :703
tradeprice      →  Price (float，long→float 截断)      // :704
mdtime          →  Time (int32)                         // :705
tradeqty        →  Volume (int32)                       // :706
```

> **丢弃的字段**：`mddate`（日期）、`securityidsource`（交易所）、`trademoney`（成交金额）未写入 journal。`OrderKind` 在 Trade 中始终硬编码为 0。

### 5.2 KyStdTradeType 结构体（Journal 中的二进制布局）

```c
// data_struct.hpp:25-84 (gateway)
// archive_journal_demo/src/data_types.h:15-30 (reader, 与 gateway 一致)
struct KyStdTradeType {         // 总计 39 字节, __attribute__((packed))
    int32_t AskOrder;           // 4B   卖方委托号（来自 tradesellno）
    int8_t  BSFlag;             // 1B   主动买卖（'B'=66/'S'=83）
    int32_t BidOrder;           // 4B   买方委托号（来自 tradebuyno）
    int32_t BizIndex;           // 4B   业务序号（来自 applseqnum）
    int32_t Channel;            // 4B   频道号
    int8_t  FunctionCode;       // 1B   成交类型（'F'=70/'4'=52）
    int32_t Index;              // 4B   成交序号
    int8_t  OrderKind;          // 1B   委托类型（始终 0）
    float   Price;              // 4B   成交价格
    int32_t Time;               // 4B   时间
    int32_t Volume;             // 4B   成交数量
    int32_t Symbol;             // 4B   证券代码
};
```

### 5.3 KyStdTradeType → Parquet

全部字段 1:1 直接拷贝（`conversion.cpp:77-93`），外加两个派生字段。

Parquet schema 定义于 `parquet_writer.cpp:319-336`，写入逻辑于 `parquet_writer.cpp:338-413`：

| Parquet 列名 | Arrow 类型 | 来源 | 说明 |
|-------------|-----------|------|------|
| **nano_timestamp** | int64 | **Frame Header `.nano`** | 帧纳秒时间戳（**派生字段**） |
| **exchange** | utf8 | **Symbol 推导** | "SZ" 或 "SH"（**派生字段**） |
| Symbol | int32 | KyStdTradeType.Symbol | 证券代码 |
| AskOrder | int32 | KyStdTradeType.AskOrder | 卖方委托号 |
| BSFlag | int8 | KyStdTradeType.BSFlag | 主动买卖标记 |
| BidOrder | int32 | KyStdTradeType.BidOrder | 买方委托号 |
| BizIndex | int32 | KyStdTradeType.BizIndex | 业务序号 |
| Channel | int32 | KyStdTradeType.Channel | 频道号 |
| FunctionCode | int8 | KyStdTradeType.FunctionCode | 成交类型 |
| Index | int32 | KyStdTradeType.Index | 成交序号 |
| OrderKind | int8 | KyStdTradeType.OrderKind | 委托类型 |
| Price | float32 | KyStdTradeType.Price | 成交价格 |
| Time | int32 | KyStdTradeType.Time | 时间 |
| Volume | int32 | KyStdTradeType.Volume | 成交数量 |

**合计 14 列**（2 个派生 + 12 个原始字段）。

---

## 6. 枚举值速查

### 6.1 FunctionCode（Order 中的买卖方向，`insight_types.h:813-819`）

| 原始值 (orderbsflag) | FunctionCode | 字符 | 含义 |
|---------------------|-------------|------|------|
| 1 | 66 | 'B' | 买入 |
| 2 | 83 | 'S' | 卖出 |
| 其他 | 0 | — | 未知 |

### 6.2 OrderKind（Order 中的委托类型，`insight_types.h:821-832`）

| 原始值 (ordertype) | OrderKind | 字符 | 含义 |
|-------------------|----------|------|------|
| 1 | 49 | '1' | 限价委托 |
| 2 | 50 | '2' | 最优委托 |
| 3 | 85 | 'U' | 未知类型 |
| 10 | 68 | 'D' | 撤单 |
| 其他 | 65 | 'A' | 默认/普通 |

### 6.3 BSFlag（Trade 中的主动方向，`insight_types.h:679-685`）

| 原始值 (tradebsflag) | BSFlag | 字符 | 含义 |
|---------------------|--------|------|------|
| 1 | 66 | 'B' | 主动买入 |
| 2 | 83 | 'S' | 主动卖出 |
| 其他 | 0 | — | 未知 |

### 6.4 FunctionCode（Trade 中的成交类型，`insight_types.h:687-691`）

| 原始值 (tradetype) | FunctionCode | 字符 | 含义 |
|-------------------|-------------|------|------|
| 0 或 9 | 70 | 'F' | 正常成交 |
| 其他 | 52 | '4' | 其他类型 |

### 6.5 Exchange（Symbol → 交易所，`conversion.cpp:3-8`）

| Symbol 范围 | Exchange | 含义 |
|------------|----------|------|
| < 400000 | "SZ" | 深圳 |
| 400000 ~ 699999 | "SH" | 上海 |

---

## 7. 转换过程中丢失的 Insight SDK 字段

以下字段存在于 Insight SDK Protobuf 消息中，但在 gateway 的 `ParseFrom()` 转换时被丢弃，**不存在于 journal 和 parquet 中**：

### Tick

| SDK 字段 | 说明 | 备注 |
|----------|------|------|
| `mddate` | 日期 | 可从 `nano_timestamp` 恢复 |
| `securityidsource` | 交易所标识 | 可从 `exchange` 字段获取 |
| `diffpx1` | 涨跌额 | 可由 Price - PreClose 计算 |
| `tradingphasecode` | 交易阶段代码 | 如 "T111" |
| `buyorderqueue` | 买方委托队列 | 前 50 笔委托数量 |
| `sellorderqueue` | 卖方委托队列 | 前 50 笔委托数量 |

### Order

| SDK 字段 | 说明 | 备注 |
|----------|------|------|
| `mddate` | 日期 | 可从 `nano_timestamp` 恢复 |
| `securityidsource` | 交易所标识 | 可从 `exchange` 字段获取 |
| `securitytype` | 证券类型 | 如 "EQA" |

### Trade

| SDK 字段 | 说明 | 备注 |
|----------|------|------|
| `mddate` | 日期 | 可从 `nano_timestamp` 恢复 |
| `securityidsource` | 交易所标识 | 可从 `exchange` 字段获取 |
| `trademoney` | 成交金额 | 可由 Price × Volume 近似计算 |

---

## 8. 转换过程中的精度/语义变化

| 阶段 | 变化 | 影响 | 代码位置 |
|------|------|------|---------|
| SDK→Gateway | 价格字段 `long→float` 截断 | SDK 价格为整数（含精度因子），gateway 直接 `static_cast<float>` | `insight_types.h:265,275` 等 |
| SDK→Gateway | 量字段 `long→int32` 截断 | `orderqty`、`tradeqty` 等从 long 截断为 int32 | `insight_types.h:845,706` |
| SDK→Gateway | `TotalAskVolume`/`TotalBidVolume` 除以 1000 | `totalsellqty/1000` → `TotalAskVolume` | `insight_types.h:278-279` |
| SDK→Gateway | Tick 的 `Turnover`/`Volume` 硬编码为 0 | 构造函数参数对位问题 | `insight_types.h:280-281` |
| SDK→Gateway | Tick 的 `BizIndex` 硬编码为 0 | 在构造函数体内设置 | `data_struct.hpp:218` |
| SDK→Gateway | Trade 的 `OrderKind` 硬编码为 0 | 始终为 0 | `insight_types.h:703` |
| Journal→Parquet | **无精度损失** | 直接 memcpy 读取 packed struct，1:1 写入 parquet | `conversion.cpp` 全文 |

---

## 9. 并行读取的有序性保证

归档程序使用多线程并行读取 journal 文件以提高速度，同时**不做任何排序**，完全保持 journal 中 `nano_timestamp` 的原始写入顺序。有序性通过以下机制保证：

```
Journal 文件（已按 page number 数字排序）:
  page 0    page 1    page 2    page 3    page 4    page 5
  [t0─t1]   [t1─t2]   [t2─t3]   [t3─t4]   [t4─t5]   [t5─t6]
  ╰── thread 0 ──────╯  ╰── thread 1 ──────╯  ╰── thread 2 ──────╯

各线程局部 vector:
  thread 0: [t0 ... t2]    ← 内部有序（按文件顺序逐帧读取）
  thread 1: [t2 ... t4]    ← 内部有序
  thread 2: [t4 ... t6]    ← 内部有序

拼接（按 thread 0, 1, 2 顺序）:
  [t0 ... t2 | t2 ... t4 | t4 ... t6]  → 全局有序 ✓
```

**四个关键环节**：

1. **文件排序**（`journal_reader.cpp: getJournalFiles()`）：按 page number **数字排序**，确保 `page 1, 2, ..., 9, 10, 11` 而非字典序的 `1, 10, 11, 2, ...`。这是有序性的基础。

2. **连续分块**（`main.cpp: read_journal_parallel()`）：文件列表按 thread id 均分为连续块——thread 0 读 page 0~N，thread 1 读 page N+1~2N...。每个线程的时间范围不重叠。

3. **顺序读取**：每个线程内按文件顺序逐页逐帧读取到局部 vector，保持 journal 原始顺序。

4. **有序拼接**：合并时按 thread 0, 1, 2... 顺序拼接各局部 vector，等价于单线程从头到尾顺序读取。

**设计原则**：程序不对数据做任何排序。如果输出 parquet 中出现 `nano_timestamp` 乱序，说明上游 journal 写入有问题，应排查线上 gateway，而非在归档侧掩盖问题。

---

## 10. Parquet 文件规格

| 属性 | 值 | 代码位置 |
|------|-----|---------|
| 压缩算法 | Snappy | `parquet_writer.cpp:108` |
| Row Group 大小 | 100,000 行 | `parquet_writer.h:8` |
| 单文件行数上限 | 按 `-s` 参数自动计算（默认 ~128MB/文件） | `main.cpp` |
| Arrow 版本 | 21.0.0 | |
| 存储 schema | 是（`store_schema()` 启用） | `parquet_writer.cpp:110` |

### 输出文件

数据量超出单文件上限时自动分片，命名规则：`{type}_data_001.parquet`、`{type}_data_002.parquet`...

| 文件名 | 数据类型 | 列数 | 典型日行数 |
|--------|---------|------|-----------|
| `tick_data[_NNN].parquet` | L2 快照 | 64 | ~170 万 |
| `order_data[_NNN].parquet` | 逐笔委托 | 12 | ~870 万 |
| `trade_data[_NNN].parquet` | 逐笔成交 | 14 | ~860 万 |

---

## 11. 源码文件对照

### Gateway 侧（写入 journal）

| 文件 | 关键行号 | 说明 |
|------|---------|------|
| `brokers/insight_tcp/insight_gateway.cpp` | :15-87 | SDK 回调 `OnMarketData()`，按类型分发 |
| `brokers/insight_tcp/insight_gateway.cpp` | :140-142 | 初始化三个 AsyncBatchWriter |
| `brokers/insight_tcp/insight_gateway.h` | :24-87 | `AsyncBatchWriter` 模板，`:71` 调用 `write_frame()` |
| `brokers/insight_tcp/insight_types.h` | :36-325 | `StockTickData`，`:258-324` ParseFrom() |
| `brokers/insight_tcp/insight_types.h` | :578-712 | `StockTransaction`，`:677-711` ParseFrom() |
| `brokers/insight_tcp/insight_types.h` | :714-850 | `StockOrderData`，`:811-848` ParseFrom() |
| `brokers/insight_tcp/insight_types.h` | :852-898 | `RawTickData` / `RawOrderData` / `RawTransactionData` 包装 |
| `data/data_struct.hpp` | :25-84 | `KyStdTradeType` packed 结构体 |
| `data/data_struct.hpp` | :86-256 | `KyStdSnpType` packed 结构体 |
| `data/data_struct.hpp` | :258-327 | `KyStdOrderType` packed 结构体 |
| `data/sys_messages.h` | | `MSG_TYPE_L2_TICK=61`, `ORDER=62`, `TRADE=63` |

### Archive 侧（读取 journal → 写入 parquet）

| 文件 | 关键行号 | 说明 |
|------|---------|------|
| `journal_reader.h` | :10-21 | `LocalPageHeader` 定义 |
| `journal_reader.h` | :23-34 | `LocalFrameHeader` 定义 |
| `journal_reader.cpp` | :20-40 | mmap 加载 journal 文件 |
| `journal_reader.cpp` | :43-58 | `nextFrame()` / `getFrameData()` 帧遍历 |
| `data_types.h` | :15-30 | `KyStdTradeType`（与 gateway 一致） |
| `data_types.h` | :32-97 | `KyStdSnpType`（与 gateway 一致） |
| `data_types.h` | :99-112 | `KyStdOrderType`（与 gateway 一致） |
| `l2_types.h` | :7-36 | `TickRecord` — parquet 输出结构体 |
| `l2_types.h` | :39-52 | `OrderRecord` — parquet 输出结构体 |
| `l2_types.h` | :55-70 | `TradeRecord` — parquet 输出结构体 |
| `conversion.cpp` | :3-8 | `decode_exchange()` — Symbol → 交易所 |
| `conversion.cpp` | :11-57 | `convert_tick()` — 1:1 字段拷贝 |
| `conversion.cpp` | :60-74 | `convert_order()` — 1:1 字段拷贝 |
| `conversion.cpp` | :77-93 | `convert_trade()` — 1:1 字段拷贝 |
| `parquet_writer.cpp` | :32-98 | `tick_schema()` — 64 列 Arrow schema |
| `parquet_writer.cpp` | :227-242 | `order_schema()` — 12 列 Arrow schema |
| `parquet_writer.cpp` | :319-336 | `trade_schema()` — 14 列 Arrow schema |
| `main.cpp` | :52-68 | 读取 tick journal 并转换 |
| `main.cpp` | :71-87 | 读取 order journal 并转换 |
| `main.cpp` | :90-107 | 读取 trade journal 并转换 |
