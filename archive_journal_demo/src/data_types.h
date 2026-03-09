#pragma once

#include <cstdint>
#include <cmath>

// MSG_TYPE 常量
const short MSG_TYPE_L2_TICK  = 61;
const short MSG_TYPE_L2_ORDER = 62;
const short MSG_TYPE_L2_TRADE = 63;

typedef int32_t  T_I32;
typedef uint32_t T_U32;
typedef long long T_I64;
typedef double   float64_t;

struct KyStdTradeType {
    int32_t AskOrder;
    int8_t  BSFlag;
    int32_t BidOrder;
    int32_t BizIndex;
    int32_t Channel;
    int8_t  FunctionCode;
    int32_t Index;
    int8_t  OrderKind;
    float_t Price;
    int32_t Time;
    int32_t Volume;
    int32_t Symbol;
    int32_t Date;
    int32_t Multiple;
    int32_t Money;

    static constexpr short type_id = MSG_TYPE_L2_TRADE;
} __attribute__((packed));

struct KyStdSnpType {
    int64_t   AccTurnover;
    int64_t   AccVolume;
    int32_t   AfterMatchItem;
    float     AfterPrice;
    int64_t   AfterTurnover;
    int32_t   AfterVolume;
    float     AskAvgPrice;
    float     AskPx1;
    float     AskPx2;
    float     AskPx3;
    float     AskPx4;
    float     AskPx5;
    float     AskPx6;
    float     AskPx7;
    float     AskPx8;
    float     AskPx9;
    float     AskPx10;
    float64_t AskVol1;
    float64_t AskVol2;
    float64_t AskVol3;
    float64_t AskVol4;
    float64_t AskVol5;
    float64_t AskVol6;
    float64_t AskVol7;
    float64_t AskVol8;
    float64_t AskVol9;
    float64_t AskVol10;
    int8_t    BSFlag;
    float     BidAvgPrice;
    float     BidPx1;
    float     BidPx2;
    float     BidPx3;
    float     BidPx4;
    float     BidPx5;
    float     BidPx6;
    float     BidPx7;
    float     BidPx8;
    float     BidPx9;
    float     BidPx10;
    float64_t BidVol1;
    float64_t BidVol2;
    float64_t BidVol3;
    float64_t BidVol4;
    float64_t BidVol5;
    float64_t BidVol6;
    float64_t BidVol7;
    float64_t BidVol8;
    float64_t BidVol9;
    float64_t BidVol10;
    float_t   High;
    float_t   Low;
    int32_t   MatchItem;
    float_t   Open;
    float_t   PreClose;
    float_t   Price;
    int32_t   Time;
    int64_t   TotalAskVolume;
    int64_t   TotalBidVolume;
    int64_t   Turnover;
    int32_t   Volume;
    int32_t   Symbol;
    int32_t   BizIndex;

    static constexpr short type_id = MSG_TYPE_L2_TICK;
} __attribute__((packed));

struct KyStdOrderType {
    int32_t BizIndex;
    int64_t Channel;
    int8_t  FunctionCode;
    int8_t  OrderKind;
    int32_t OrderNumber;
    int32_t OrderOriNo;
    float_t Price;
    int32_t Time;
    int32_t Volume;
    int32_t Symbol;
    int32_t TradedQty;
    int32_t Date;
    int32_t Multiple;

    static constexpr short type_id = MSG_TYPE_L2_ORDER;
} __attribute__((packed));
