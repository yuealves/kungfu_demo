#pragma once

#include <cstdint>
#include <string>

// Tick 输出记录：KyStdSnpType 全部字段 + nano_timestamp + exchange
struct TickRecord {
    int64_t  nano_timestamp;
    std::string exchange;
    int32_t  Symbol;
    int32_t  Time;
    int64_t  AccTurnover;
    int64_t  AccVolume;
    int32_t  AfterMatchItem;
    float    AfterPrice;
    int64_t  AfterTurnover;
    int32_t  AfterVolume;
    float    AskAvgPrice;
    float    AskPx[10];
    double   AskVol[10];
    int8_t   BSFlag;
    float    BidAvgPrice;
    float    BidPx[10];
    double   BidVol[10];
    float    High;
    float    Low;
    int32_t  MatchItem;
    float    Open;
    float    PreClose;
    float    Price;
    int64_t  TotalAskVolume;
    int64_t  TotalBidVolume;
    int64_t  Turnover;
    int32_t  Volume;
    int32_t  BizIndex;
};

// Order 输出记录：KyStdOrderType 全部字段 + nano_timestamp + exchange
struct OrderRecord {
    int64_t     nano_timestamp;
    std::string exchange;
    int32_t     Symbol;
    int32_t     BizIndex;
    int64_t     Channel;
    int8_t      FunctionCode;
    int8_t      OrderKind;
    int32_t     OrderNumber;
    int32_t     OrderOriNo;
    float       Price;
    int32_t     Time;
    int32_t     Volume;
};

// Trade 输出记录：KyStdTradeType 全部字段 + nano_timestamp + exchange
struct TradeRecord {
    int64_t     nano_timestamp;
    std::string exchange;
    int32_t     Symbol;
    int32_t     AskOrder;
    int8_t      BSFlag;
    int32_t     BidOrder;
    int32_t     BizIndex;
    int32_t     Channel;
    int8_t      FunctionCode;
    int32_t     Index;
    int8_t      OrderKind;
    float       Price;
    int32_t     Time;
    int32_t     Volume;
};
