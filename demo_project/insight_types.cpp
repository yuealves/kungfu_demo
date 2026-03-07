#include "insight_types.h"
#include <cstring>

L2StockTickDataField trans_tick(const KyStdSnpType& ky_snp_data, int32_t host_time) {
    L2StockTickDataField l2_data;
    l2_data.nTime = ky_snp_data.Time;
    l2_data.hostTime = host_time;
    l2_data.nStatus = 0;
    l2_data.uPreClose = ky_snp_data.PreClose;
    l2_data.uOpen = ky_snp_data.Open;
    l2_data.uHigh = ky_snp_data.High;
    l2_data.uLow = ky_snp_data.Low;
    l2_data.uMatch = ky_snp_data.Price;
    l2_data.uAskPrice1 = ky_snp_data.AskPx1;
    l2_data.uAskPrice2 = ky_snp_data.AskPx2;
    l2_data.uAskPrice3 = ky_snp_data.AskPx3;
    l2_data.uAskPrice4 = ky_snp_data.AskPx4;
    l2_data.uAskPrice5 = ky_snp_data.AskPx5;
    l2_data.uAskPrice6 = ky_snp_data.AskPx6;
    l2_data.uAskPrice7 = ky_snp_data.AskPx7;
    l2_data.uAskPrice8 = ky_snp_data.AskPx8;
    l2_data.uAskPrice9 = ky_snp_data.AskPx9;
    l2_data.uAskPrice10 = ky_snp_data.AskPx10;
    l2_data.uAskVol1 = ky_snp_data.AskVol1;
    l2_data.uAskVol2 = ky_snp_data.AskVol2;
    l2_data.uAskVol3 = ky_snp_data.AskVol3;
    l2_data.uAskVol4 = ky_snp_data.AskVol4;
    l2_data.uAskVol5 = ky_snp_data.AskVol5;
    l2_data.uAskVol6 = ky_snp_data.AskVol6;
    l2_data.uAskVol7 = ky_snp_data.AskVol7;
    l2_data.uAskVol8 = ky_snp_data.AskVol8;
    l2_data.uAskVol9 = ky_snp_data.AskVol9;
    l2_data.uAskVol10 = ky_snp_data.AskVol10;
    l2_data.uBidPrice1 = ky_snp_data.BidPx1;
    l2_data.uBidPrice2 = ky_snp_data.BidPx2;
    l2_data.uBidPrice3 = ky_snp_data.BidPx3;
    l2_data.uBidPrice4 = ky_snp_data.BidPx4;
    l2_data.uBidPrice5 = ky_snp_data.BidPx5;
    l2_data.uBidPrice6 = ky_snp_data.BidPx6;
    l2_data.uBidPrice7 = ky_snp_data.BidPx7;
    l2_data.uBidPrice8 = ky_snp_data.BidPx8;
    l2_data.uBidPrice9 = ky_snp_data.BidPx9;
    l2_data.uBidPrice10 = ky_snp_data.BidPx10;
    l2_data.uBidVol1 = ky_snp_data.BidVol1;
    l2_data.uBidVol2 = ky_snp_data.BidVol2;
    l2_data.uBidVol3 = ky_snp_data.BidVol3;
    l2_data.uBidVol4 = ky_snp_data.BidVol4;
    l2_data.uBidVol5 = ky_snp_data.BidVol5;
    l2_data.uBidVol6 = ky_snp_data.BidVol6;
    l2_data.uBidVol7 = ky_snp_data.BidVol7;
    l2_data.uBidVol8 = ky_snp_data.BidVol8;
    l2_data.uBidVol9 = ky_snp_data.BidVol9;
    l2_data.uBidVol10 = ky_snp_data.BidVol10;
    l2_data.uNumTrades = ky_snp_data.MatchItem;
    l2_data.iVolume = ky_snp_data.AccVolume;
    l2_data.iTurnover = ky_snp_data.AccTurnover;
    l2_data.iTotalBidVol = ky_snp_data.TotalBidVolume;
    l2_data.iTotalAskVol = ky_snp_data.TotalAskVolume;
    l2_data.uWeightedAvgBidPrice = ky_snp_data.BidAvgPrice;
    l2_data.uWeightedAvgAskPrice = ky_snp_data.AskAvgPrice;
    l2_data.nIOPV = 0;
    l2_data.nYieldToMaturity = 0;
    l2_data.uHighLimited = 0;
    l2_data.uLowLimited = 0;
    l2_data.nSyl1 = 0;
    l2_data.nSyl2 = 0;
    l2_data.nSD2 = 0;
    std::strncpy(l2_data.sPrefix, decode_exchange(ky_snp_data.Symbol).c_str(), 4);
    std::strncpy(l2_data.sTradingPhraseCode, "", 8);
    std::strncpy(l2_data.code, std::to_string(ky_snp_data.Symbol).c_str(), 6);
    l2_data.code[6] = '\0';
    l2_data.nPreIOPV = 0;
    return l2_data;
}

std::variant<SZ_StockStepOrderField, SH_StockStepOrderField> trans_order(const KyStdOrderType& ky_order_data, int32_t host_time, const std::string& exchange) {
    if (exchange == "SZ") {
        SZ_StockStepOrderField sz_order;
        sz_order.nHostTime = host_time;
        sz_order.nMDDate = ky_order_data.Date;
        sz_order.nMDTime = ky_order_data.Time;
        sz_order.nOrderIndex = ky_order_data.OrderNumber;
        sz_order.nOrderType = ky_order_data.OrderKind;
        sz_order.iOrderPrice = ky_order_data.Price;
        sz_order.iOrderQty = ky_order_data.Volume;
        sz_order.nOrderBSFlag = ky_order_data.FunctionCode;
        sz_order.nChannelNo = ky_order_data.Channel;
        sz_order.iApplSeqNum = ky_order_data.BizIndex;
        sz_order.nDataMultiplePowerOf10 = ky_order_data.Multiple;
        std::strncpy(sz_order.sSecurityID, std::to_string(ky_order_data.Symbol).c_str(), 6);
        sz_order.sSecurityID[6] = '\0';
        sz_order.sSecurityID[7] = '\0';
        return sz_order;
    } else {
        SH_StockStepOrderField sh_order;
        sh_order.nHostTime = host_time;
        sh_order.nMDDate = ky_order_data.Date;
        sh_order.nMDTime = ky_order_data.Time;
        sh_order.nOrderIndex = ky_order_data.OrderNumber;
        sh_order.nOrderType = ky_order_data.OrderKind;
        sh_order.iOrderPrice = ky_order_data.Price;
        sh_order.iOrderQty = ky_order_data.Volume;
        sh_order.nOrderBSFlag = ky_order_data.FunctionCode;
        sh_order.nChannelNo = ky_order_data.Channel;
        sh_order.iOrderNo = ky_order_data.OrderOriNo;
        sh_order.iApplSeqNum = ky_order_data.BizIndex;
        sh_order.nDataMultiplePowerOf10 = ky_order_data.Multiple;
        std::strncpy(sh_order.sSecurityID, std::to_string(ky_order_data.Symbol).c_str(), 6);
        sh_order.sSecurityID[6] = '\0';
        sh_order.sSecurityID[7] = '\0';
        sh_order.nTradedQty = ky_order_data.TradedQty;
        return sh_order;
    }
}

std::variant<SZ_StockStepTradeField, SH_StockStepTradeField> trans_trade(const KyStdTradeType& ky_trade_data, int32_t host_time, const std::string& exchange) {
    if (exchange == "SZ") {
        SZ_StockStepTradeField sz_trade;
        sz_trade.nHostTime = host_time;
        sz_trade.nMDDate = ky_trade_data.Date;
        sz_trade.nMDTime = ky_trade_data.Time;
        sz_trade.nTradeIndex = ky_trade_data.Index;
        sz_trade.iTradeBuyNo = ky_trade_data.BidOrder;
        sz_trade.iTradeSellNo = ky_trade_data.AskOrder;
        sz_trade.nTradeBSflag = ky_trade_data.BSFlag;
        sz_trade.iTradePrice = ky_trade_data.Price;
        sz_trade.iTradeQty = ky_trade_data.Volume;
        sz_trade.iTradeMoney = ky_trade_data.Money;
        sz_trade.nTradeType = ky_trade_data.FunctionCode;
        sz_trade.iApplSeqNum = ky_trade_data.BizIndex;
        sz_trade.nChannelNo = ky_trade_data.Channel;
        sz_trade.nDataMultiplePowerOf10 = ky_trade_data.Multiple;
        std::strncpy(sz_trade.sSecurityID, std::to_string(ky_trade_data.Symbol).c_str(), 6);
        sz_trade.sSecurityID[6] = '\0';
        sz_trade.sSecurityID[7] = '\0';
        return sz_trade;
    }
    else {
        SH_StockStepTradeField sh_trade;
        sh_trade.nHostTime = host_time;
        sh_trade.nMDDate = ky_trade_data.Date;
        sh_trade.nMDTime = ky_trade_data.Time;
        sh_trade.nTradeIndex = ky_trade_data.Index;
        sh_trade.iTradeBuyNo = ky_trade_data.BidOrder;
        sh_trade.iTradeSellNo = ky_trade_data.AskOrder;
        sh_trade.nTradeBSflag = ky_trade_data.BSFlag;
        sh_trade.iTradePrice = ky_trade_data.Price;
        sh_trade.iTradeQty = ky_trade_data.Volume;
        sh_trade.iTradeMoney = ky_trade_data.Money;
        sh_trade.nTradeType = ky_trade_data.FunctionCode;
        sh_trade.iApplSeqNum = ky_trade_data.BizIndex;
        sh_trade.nChannelNo = ky_trade_data.Channel;
        sh_trade.nDataMultiplePowerOf10 = ky_trade_data.Multiple;
        std::strncpy(sh_trade.sSecurityID, std::to_string(ky_trade_data.Symbol).c_str(), 6);
        sh_trade.sSecurityID[6] = '\0';
        sh_trade.sSecurityID[7] = '\0';
        return sh_trade;
    }
}
