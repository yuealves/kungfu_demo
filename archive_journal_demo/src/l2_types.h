#pragma once

#include <cstdint>
#include <string>

// 统一 Tick 输出记录（与 L2StockTickDataField 字段一致，新增 nano_timestamp）
struct TickRecord {
    int64_t  nano_timestamp;
    int32_t  nTime;
    int32_t  hostTime;
    int32_t  nStatus;
    uint32_t uPreClose;
    uint32_t uOpen;
    uint32_t uHigh;
    uint32_t uLow;
    uint32_t uMatch;
    uint32_t uAskPrice1;
    uint32_t uAskPrice2;
    uint32_t uAskPrice3;
    uint32_t uAskPrice4;
    uint32_t uAskPrice5;
    uint32_t uAskPrice6;
    uint32_t uAskPrice7;
    uint32_t uAskPrice8;
    uint32_t uAskPrice9;
    uint32_t uAskPrice10;
    uint32_t uAskVol1;
    uint32_t uAskVol2;
    uint32_t uAskVol3;
    uint32_t uAskVol4;
    uint32_t uAskVol5;
    uint32_t uAskVol6;
    uint32_t uAskVol7;
    uint32_t uAskVol8;
    uint32_t uAskVol9;
    uint32_t uAskVol10;
    uint32_t uBidPrice1;
    uint32_t uBidPrice2;
    uint32_t uBidPrice3;
    uint32_t uBidPrice4;
    uint32_t uBidPrice5;
    uint32_t uBidPrice6;
    uint32_t uBidPrice7;
    uint32_t uBidPrice8;
    uint32_t uBidPrice9;
    uint32_t uBidPrice10;
    uint32_t uBidVol1;
    uint32_t uBidVol2;
    uint32_t uBidVol3;
    uint32_t uBidVol4;
    uint32_t uBidVol5;
    uint32_t uBidVol6;
    uint32_t uBidVol7;
    uint32_t uBidVol8;
    uint32_t uBidVol9;
    uint32_t uBidVol10;
    uint32_t uNumTrades;
    int64_t  iVolume;
    int64_t  iTurnover;
    int64_t  iTotalBidVol;
    int64_t  iTotalAskVol;
    uint32_t uWeightedAvgBidPrice;
    uint32_t uWeightedAvgAskPrice;
    int32_t  nIOPV;
    int32_t  nYieldToMaturity;
    uint32_t uHighLimited;
    uint32_t uLowLimited;
    std::string sPrefix;        // "SH" or "SZ"
    int32_t  nSyl1;
    int32_t  nSyl2;
    int32_t  nSD2;
    std::string sTradingPhraseCode;
    std::string code;           // 6-digit security code
    int32_t  nPreIOPV;
};

// 统一 Order 输出记录（SH+SZ 合一，SH 独有字段在 SZ 记录中为 0）
struct OrderRecord {
    int64_t     nano_timestamp;
    std::string exchange;       // "SH" or "SZ"
    int32_t     nHostTime;
    std::string sSecurityID;    // 6-digit code
    int32_t     nMDDate;
    int32_t     nMDTime;
    int32_t     nOrderIndex;
    int32_t     nOrderType;
    int64_t     iOrderPrice;
    int64_t     iOrderQty;
    int32_t     nOrderBSFlag;
    int32_t     nChannelNo;
    int64_t     iOrderNo;       // SH only, SZ = 0
    int64_t     iApplSeqNum;
    int32_t     nDataMultiplePowerOf10;
    int32_t     nTradedQty;     // SH only, SZ = 0
};

// 统一 Trade 输出记录（SH+SZ 合一）
struct TradeRecord {
    int64_t     nano_timestamp;
    std::string exchange;       // "SH" or "SZ"
    int32_t     nHostTime;
    std::string sSecurityID;    // 6-digit code
    int32_t     nMDDate;
    int32_t     nMDTime;
    int32_t     nTradeIndex;
    int64_t     iTradeBuyNo;
    int64_t     iTradeSellNo;
    int32_t     nTradeBSflag;
    int64_t     iTradePrice;
    int64_t     iTradeQty;
    int64_t     iTradeMoney;
    int32_t     nTradeType;
    int64_t     iApplSeqNum;
    int32_t     nChannelNo;
    int32_t     nDataMultiplePowerOf10;
};
