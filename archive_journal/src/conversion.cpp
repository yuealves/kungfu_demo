#include "conversion.h"

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
    r.exchange = decode_exchange(src.Symbol);
    r.Symbol = src.Symbol;
    r.Time = src.Time;
    r.AccTurnover = src.AccTurnover;
    r.AccVolume = src.AccVolume;
    r.AfterMatchItem = src.AfterMatchItem;
    r.AfterPrice = src.AfterPrice;
    r.AfterTurnover = src.AfterTurnover;
    r.AfterVolume = src.AfterVolume;
    r.AskAvgPrice = src.AskAvgPrice;
    r.AskPx[0] = src.AskPx1;  r.AskPx[1] = src.AskPx2;
    r.AskPx[2] = src.AskPx3;  r.AskPx[3] = src.AskPx4;
    r.AskPx[4] = src.AskPx5;  r.AskPx[5] = src.AskPx6;
    r.AskPx[6] = src.AskPx7;  r.AskPx[7] = src.AskPx8;
    r.AskPx[8] = src.AskPx9;  r.AskPx[9] = src.AskPx10;
    r.AskVol[0] = src.AskVol1;  r.AskVol[1] = src.AskVol2;
    r.AskVol[2] = src.AskVol3;  r.AskVol[3] = src.AskVol4;
    r.AskVol[4] = src.AskVol5;  r.AskVol[5] = src.AskVol6;
    r.AskVol[6] = src.AskVol7;  r.AskVol[7] = src.AskVol8;
    r.AskVol[8] = src.AskVol9;  r.AskVol[9] = src.AskVol10;
    r.BSFlag = src.BSFlag;
    r.BidAvgPrice = src.BidAvgPrice;
    r.BidPx[0] = src.BidPx1;  r.BidPx[1] = src.BidPx2;
    r.BidPx[2] = src.BidPx3;  r.BidPx[3] = src.BidPx4;
    r.BidPx[4] = src.BidPx5;  r.BidPx[5] = src.BidPx6;
    r.BidPx[6] = src.BidPx7;  r.BidPx[7] = src.BidPx8;
    r.BidPx[8] = src.BidPx9;  r.BidPx[9] = src.BidPx10;
    r.BidVol[0] = src.BidVol1;  r.BidVol[1] = src.BidVol2;
    r.BidVol[2] = src.BidVol3;  r.BidVol[3] = src.BidVol4;
    r.BidVol[4] = src.BidVol5;  r.BidVol[5] = src.BidVol6;
    r.BidVol[6] = src.BidVol7;  r.BidVol[7] = src.BidVol8;
    r.BidVol[8] = src.BidVol9;  r.BidVol[9] = src.BidVol10;
    r.High = src.High;
    r.Low = src.Low;
    r.MatchItem = src.MatchItem;
    r.Open = src.Open;
    r.PreClose = src.PreClose;
    r.Price = src.Price;
    r.TotalAskVolume = src.TotalAskVolume;
    r.TotalBidVolume = src.TotalBidVolume;
    r.Turnover = src.Turnover;
    r.Volume = src.Volume;
    r.BizIndex = src.BizIndex;
    return r;
}

OrderRecord convert_order(const KyStdOrderType& src, long nano) {
    OrderRecord r{};
    r.nano_timestamp = nano;
    r.exchange = decode_exchange(src.Symbol);
    r.Symbol = src.Symbol;
    r.BizIndex = src.BizIndex;
    r.Channel = src.Channel;
    r.FunctionCode = src.FunctionCode;
    r.OrderKind = src.OrderKind;
    r.OrderNumber = src.OrderNumber;
    r.OrderOriNo = src.OrderOriNo;
    r.Price = src.Price;
    r.Time = src.Time;
    r.Volume = src.Volume;
    return r;
}

TradeRecord convert_trade(const KyStdTradeType& src, long nano) {
    TradeRecord r{};
    r.nano_timestamp = nano;
    r.exchange = decode_exchange(src.Symbol);
    r.Symbol = src.Symbol;
    r.AskOrder = src.AskOrder;
    r.BSFlag = src.BSFlag;
    r.BidOrder = src.BidOrder;
    r.BizIndex = src.BizIndex;
    r.Channel = src.Channel;
    r.FunctionCode = src.FunctionCode;
    r.Index = src.Index;
    r.OrderKind = src.OrderKind;
    r.Price = src.Price;
    r.Time = src.Time;
    r.Volume = src.Volume;
    return r;
}
