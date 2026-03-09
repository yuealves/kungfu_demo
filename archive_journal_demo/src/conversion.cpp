#include "conversion.h"
#include <ctime>
#include <cstring>

static const long NANOSECONDS_PER_SECOND = 1000000000L;

int32_t parse_nano(long nano) {
    if (nano <= 0) return 0;
    long seconds = nano / NANOSECONDS_PER_SECOND;
    int millisecond = (nano % NANOSECONDS_PER_SECOND) / 1000000;
    struct tm* dt = localtime(&seconds);
    int hour   = dt->tm_hour;
    int minute = dt->tm_min;
    int second = dt->tm_sec;
    return millisecond + second * 1000 + minute * 100000 + hour * 10000000;
}

std::string decode_exchange(int symbol) {
    if (symbol < 400000)
        return "SZ";
    else if (symbol < 700000)
        return "SH";
    return "";
}

TickRecord convert_tick(const KyStdSnpType& src, long nano) {
    TickRecord r{};
    r.nano_timestamp = nano;
    int32_t host_time = parse_nano(nano);

    r.nTime    = src.Time;
    r.hostTime = host_time;
    r.nStatus  = 0;
    r.uPreClose = src.PreClose;
    r.uOpen     = src.Open;
    r.uHigh     = src.High;
    r.uLow      = src.Low;
    r.uMatch    = src.Price;

    r.uAskPrice1  = src.AskPx1;
    r.uAskPrice2  = src.AskPx2;
    r.uAskPrice3  = src.AskPx3;
    r.uAskPrice4  = src.AskPx4;
    r.uAskPrice5  = src.AskPx5;
    r.uAskPrice6  = src.AskPx6;
    r.uAskPrice7  = src.AskPx7;
    r.uAskPrice8  = src.AskPx8;
    r.uAskPrice9  = src.AskPx9;
    r.uAskPrice10 = src.AskPx10;

    r.uAskVol1  = src.AskVol1;
    r.uAskVol2  = src.AskVol2;
    r.uAskVol3  = src.AskVol3;
    r.uAskVol4  = src.AskVol4;
    r.uAskVol5  = src.AskVol5;
    r.uAskVol6  = src.AskVol6;
    r.uAskVol7  = src.AskVol7;
    r.uAskVol8  = src.AskVol8;
    r.uAskVol9  = src.AskVol9;
    r.uAskVol10 = src.AskVol10;

    r.uBidPrice1  = src.BidPx1;
    r.uBidPrice2  = src.BidPx2;
    r.uBidPrice3  = src.BidPx3;
    r.uBidPrice4  = src.BidPx4;
    r.uBidPrice5  = src.BidPx5;
    r.uBidPrice6  = src.BidPx6;
    r.uBidPrice7  = src.BidPx7;
    r.uBidPrice8  = src.BidPx8;
    r.uBidPrice9  = src.BidPx9;
    r.uBidPrice10 = src.BidPx10;

    r.uBidVol1  = src.BidVol1;
    r.uBidVol2  = src.BidVol2;
    r.uBidVol3  = src.BidVol3;
    r.uBidVol4  = src.BidVol4;
    r.uBidVol5  = src.BidVol5;
    r.uBidVol6  = src.BidVol6;
    r.uBidVol7  = src.BidVol7;
    r.uBidVol8  = src.BidVol8;
    r.uBidVol9  = src.BidVol9;
    r.uBidVol10 = src.BidVol10;

    r.uNumTrades = src.MatchItem;
    r.iVolume    = src.AccVolume;
    r.iTurnover  = src.AccTurnover;
    r.iTotalBidVol = src.TotalBidVolume;
    r.iTotalAskVol = src.TotalAskVolume;
    r.uWeightedAvgBidPrice = src.BidAvgPrice;
    r.uWeightedAvgAskPrice = src.AskAvgPrice;

    r.nIOPV = 0;
    r.nYieldToMaturity = 0;
    r.uHighLimited = 0;
    r.uLowLimited = 0;
    r.sPrefix = decode_exchange(src.Symbol);
    r.nSyl1 = 0;
    r.nSyl2 = 0;
    r.nSD2  = 0;
    r.sTradingPhraseCode = "";
    r.code = std::to_string(src.Symbol);
    // 左侧补零到 6 位
    while (r.code.size() < 6) r.code = "0" + r.code;
    r.code = r.code.substr(0, 6);
    r.nPreIOPV = 0;

    return r;
}

OrderRecord convert_order(const KyStdOrderType& src, long nano) {
    OrderRecord r{};
    r.nano_timestamp = nano;
    r.exchange  = decode_exchange(src.Symbol);
    r.nHostTime = parse_nano(nano);

    r.sSecurityID = std::to_string(src.Symbol);
    while (r.sSecurityID.size() < 6) r.sSecurityID = "0" + r.sSecurityID;
    r.sSecurityID = r.sSecurityID.substr(0, 6);

    r.nMDDate    = src.Date;
    r.nMDTime    = src.Time;
    r.nOrderIndex = src.OrderNumber;
    r.nOrderType  = src.OrderKind;
    r.iOrderPrice = src.Price;
    r.iOrderQty   = src.Volume;
    r.nOrderBSFlag = src.FunctionCode;
    r.nChannelNo   = src.Channel;
    r.iApplSeqNum  = src.BizIndex;
    r.nDataMultiplePowerOf10 = src.Multiple;

    if (r.exchange == "SH") {
        r.iOrderNo  = src.OrderOriNo;
        r.nTradedQty = src.TradedQty;
    } else {
        r.iOrderNo   = 0;
        r.nTradedQty = 0;
    }

    return r;
}

TradeRecord convert_trade(const KyStdTradeType& src, long nano) {
    TradeRecord r{};
    r.nano_timestamp = nano;
    r.exchange  = decode_exchange(src.Symbol);
    r.nHostTime = parse_nano(nano);

    r.sSecurityID = std::to_string(src.Symbol);
    while (r.sSecurityID.size() < 6) r.sSecurityID = "0" + r.sSecurityID;
    r.sSecurityID = r.sSecurityID.substr(0, 6);

    r.nMDDate      = src.Date;
    r.nMDTime      = src.Time;
    r.nTradeIndex  = src.Index;
    r.iTradeBuyNo  = src.BidOrder;
    r.iTradeSellNo = src.AskOrder;
    r.nTradeBSflag = src.BSFlag;
    r.iTradePrice  = src.Price;
    r.iTradeQty    = src.Volume;
    r.iTradeMoney  = src.Money;
    r.nTradeType   = src.FunctionCode;
    r.iApplSeqNum  = src.BizIndex;
    r.nChannelNo   = src.Channel;
    r.nDataMultiplePowerOf10 = src.Multiple;

    return r;
}
