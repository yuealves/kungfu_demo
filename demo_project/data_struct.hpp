#pragma once
#include "constants.h"
#include "sys_messages.h"
#include <iostream>
#include <vector>
#include <cstring>
#include <sstream>
#include <csignal>
#include <fstream>
#include <cstdlib>
#include <stdio.h>
#include <cstdint>
#include <iostream>
#include <memory>
#include <vector>
#include <math.h>
#include <iomanip>


typedef int32_t T_I32;
typedef uint32_t T_U32;
typedef long long T_I64;
typedef double float64_t;

struct KyStdTradeType
{
    int32_t AskOrder;
    int8_t BSFlag;
    int32_t BidOrder;
    int32_t BizIndex;
    int32_t Channel;
    int8_t FunctionCode;
    int32_t Index;
    int8_t OrderKind;
    float_t Price;
    int32_t Time;
    int32_t Volume;
    int32_t Symbol;
    int32_t Date;
    int32_t Multiple;
    int32_t Money;


    static constexpr short type_id = MSG_TYPE_L2_TRADE;

    KyStdTradeType() = default;
    KyStdTradeType(int32_t AskOrder, int8_t BSFlag, int32_t BidOrder, int32_t BizIndex, int32_t Channel, int8_t FunctionCode, int32_t Index, int8_t OrderKind, float_t Price, int32_t Time, int32_t Volume, int32_t Symbol, int32_t Date, int32_t Multiple, int32_t Money)
    {
        this->AskOrder = AskOrder;
        this->BSFlag = BSFlag;
        this->BidOrder = BidOrder;
        this->BizIndex = BizIndex;
        this->Channel = Channel;
        this->FunctionCode = FunctionCode;
        this->Index = Index;
        this->OrderKind = OrderKind;
        this->Price = Price;
        this->Time = Time;
        this->Volume = Volume;
        this->Symbol = Symbol;
        this->Date = Date;
        this->Multiple = Multiple;
        this->Money = Money;
    }
    std::string type_name() const { return "KyStdTrade"; }
    int tm() const { return Time; }
    std::string to_string() const
    {
        std::ostringstream oss;
        oss << AskOrder << ",";
        oss << BSFlag << ",";
        oss << BidOrder << ",";
        oss << BizIndex << ",";
        oss << Channel << ",";
        oss << FunctionCode << ",";
        oss << Index << ",";
        oss << OrderKind << ",";
        oss << Price << ",";
        oss << Time << ",";
        oss << Volume << ",";
        oss << Symbol << ",";
        oss << Date << ",";
        oss << Multiple;
        return oss.str();
    }


    friend std::ostream &operator<<(std::ostream &os, KyStdTradeType const &s)
    {
        os << s.to_string();
        return os;
    }
} __attribute__((packed));

struct KyStdSnpType
{
    int64_t AccTurnover;
    int64_t AccVolume;
    int32_t AfterMatchItem;
    float AfterPrice;
    int64_t AfterTurnover;
    int32_t AfterVolume;
    float AskAvgPrice;
    float AskPx1;
    float AskPx2;
    float AskPx3;
    float AskPx4;
    float AskPx5;
    float AskPx6;
    float AskPx7;
    float AskPx8;
    float AskPx9;
    float AskPx10;
    float64_t AskVol1;
    float64_t AskVol2;
    float64_t AskVol3;
    float64_t AskVol4;
    float64_t AskVol5;
    float64_t AskVol6;
    float64_t AskVol7;
    float64_t AskVol8;
    float64_t AskVol9;
    float64_t  AskVol10;
    int8_t BSFlag;
    float BidAvgPrice;
    float BidPx1;
    float BidPx2;
    float BidPx3;
    float BidPx4;
    float BidPx5;
    float BidPx6;
    float BidPx7;
    float BidPx8;
    float BidPx9;
    float BidPx10;
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
    float_t High;
    float_t Low;
    int32_t MatchItem;
    float_t Open;
    float_t PreClose;
    float_t Price;
    int32_t Time;
    int64_t TotalAskVolume;
    int64_t TotalBidVolume;
    int64_t Turnover;
    int32_t Volume;
    int32_t Symbol;
    int32_t BizIndex;

    static constexpr short type_id = MSG_TYPE_L2_TICK;

    KyStdSnpType() = default;
    KyStdSnpType(int64_t accTurnover, int64_t accVolume, int32_t afterMatchItem, float afterPrice, int64_t afterTurnover, int32_t afterVolume, float askAvgPrice, int8_t bsFlag, float_t bidAvgPrice, float_t high, float_t low, int32_t matchItem, float_t open, float_t preClose, float_t price, int64_t time, int64_t totalAskVolume, int64_t totalBidVolume, int64_t turnover, int32_t volume, int32_t symbol, float_t bidPx1, float_t bidPx2, float_t bidPx3, float_t bidPx4, float_t bidPx5, float_t bidPx6, float_t bidPx7, float_t bidPx8, float_t bidPx9, float_t bidPx10, float64_t bidVol1, float64_t bidVol2, float64_t bidVol3, float64_t bidVol4, float64_t bidVol5, float64_t bidVol6, float64_t bidVol7, float64_t bidVol8, float64_t bidVol9, float64_t bidVol10, float_t askPx1, float_t askPx2, float_t askPx3, float_t askPx4, float_t askPx5, float_t askPx6, float_t askPx7, float_t askPx8, float_t askPx9, float_t askPx10, float64_t askVol1, float64_t askVol2, float64_t askVol3, float64_t askVol4, float64_t askVol5, float64_t askVol6, float64_t askVol7, float64_t askVol8, float64_t askVol9, float64_t askVol10)
        :
            AccTurnover(accTurnover),
            AccVolume(accVolume),
            AfterMatchItem(afterMatchItem),
            AfterPrice(afterPrice),
            AfterTurnover(afterTurnover),
            AfterVolume(afterVolume),
            AskAvgPrice(askAvgPrice),
            BSFlag(bsFlag),
            BidAvgPrice(bidAvgPrice),
            High(high),
            Low(low),
            MatchItem(matchItem),
            Open(open),
            PreClose(preClose),
            Price(price),
            Time(time),
            TotalAskVolume(totalAskVolume),
            TotalBidVolume(totalBidVolume),
            Turnover(turnover),
            Volume(volume),
            Symbol(symbol),
            BidPx1(bidPx1),
            BidPx2(bidPx2),
            BidPx3(bidPx3),
            BidPx4(bidPx4),
            BidPx5(bidPx5),
            BidPx6(bidPx6),
            BidPx7(bidPx7),
            BidPx8(bidPx8),
            BidPx9(bidPx9),
            BidPx10(bidPx10),
            BidVol1(bidVol1),
            BidVol2(bidVol2),
            BidVol3(bidVol3),
            BidVol4(bidVol4),
            BidVol5(bidVol5),
            BidVol6(bidVol6),
            BidVol7(bidVol7),
            BidVol8(bidVol8),
            BidVol9(bidVol9),
            BidVol10(bidVol10),
            AskPx1(askPx1),
            AskPx2(askPx2),
            AskPx3(askPx3),
            AskPx4(askPx4),
            AskPx5(askPx5),
            AskPx6(askPx6),
            AskPx7(askPx7),
            AskPx8(askPx8),
            AskPx9(askPx9),
            AskPx10(askPx10),
            AskVol1(askVol1),
            AskVol2(askVol2),
            AskVol3(askVol3),
            AskVol4(askVol4),
            AskVol5(askVol5),
            AskVol6(askVol6),
            AskVol7(askVol7),
            AskVol8(askVol8),
            AskVol9(askVol9),
            AskVol10(askVol10)
    {
        BizIndex = 0;
    }
    ~KyStdSnpType() = default;
    std::string type_name() const { return "KyStdSnp"; }
    int tm() const { return Time; }
    std::string to_string()
    {
        std::stringstream oss;
        oss << AccTurnover << "," << AccVolume << "," << AfterMatchItem << "," << AfterPrice << "," << AfterTurnover << "," << AfterVolume << "," << AskAvgPrice << "," << BSFlag \
        << "," << BidAvgPrice << "," << High << "," << Low << "," << MatchItem << "," << Open << "," << PreClose << "," << Price << "," << Time << "," << TotalAskVolume << "," \
        << TotalBidVolume << "," << Turnover << "," << Volume << "," << Symbol << "," << BidPx1 << "," << BidPx2 << "," << BidPx3 << "," << BidPx4 << "," << BidPx5 << "," << BidPx6 << "," \
        << BidPx7 << "," << BidPx8 << "," << BidPx9 << "," << BidPx10 << "," << BidVol1 << "," << BidVol2 << "," << BidVol3 << "," << BidVol4 << "," << BidVol5 << "," << BidVol6 << "," << BidVol7 \
        << "," << BidVol8 << "," << BidVol9 << "," << BidVol10 << "," << AskPx1 << "," << AskPx2 << "," << AskPx3 << "," << AskPx4 << "," << AskPx5 << "," << AskPx6 << "," << AskPx7 << "," \
        << AskPx8 << "," << AskPx9 << "," << AskPx10 << "," << AskVol1 << "," << AskVol2 << "," << AskVol3 << "," << AskVol4 << "," << AskVol5 << "," << AskVol6 << "," << AskVol7 << "," << AskVol8 \
        << "," << AskVol9 << "," << AskVol10;
        return oss.str();
    }

} __attribute__((packed));

struct KyStdOrderType
{
    int32_t BizIndex;
    int64_t Channel;
    int8_t FunctionCode;
    int8_t OrderKind;
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

    KyStdOrderType() = default;
    KyStdOrderType(int32_t BizIndex_, int64_t Channel_, int8_t FunctionCode_, int8_t OrderKind_, int32_t OrderNumber_, int32_t OrderOriNo_, float_t Price_, int32_t Time_, int32_t Volume_, int32_t Symbol_,int32_t TradedQty_,int32_t Date_, int32_t Multiple_)
    {
        this->BizIndex = BizIndex_;
        this->Channel = Channel_;
        this->FunctionCode = FunctionCode_;
        this->OrderKind = OrderKind_;
        this->OrderNumber = OrderNumber_;
        this->OrderOriNo = OrderOriNo_;
        this->Price = Price_;
        this->Time = Time_;
        this->Volume = Volume_;
        this->Symbol = Symbol_;
        this->TradedQty = TradedQty_;
        this->Date = Date_;
        this->Multiple = Multiple_;
    }

    KyStdOrderType(int32_t BizIndex_, int8_t FunctionCode_, int8_t OrderKind_, int32_t OrderNumber_, int32_t OrderOriNo_, float_t Price_, int32_t Time_, int32_t Volume_, int32_t Symbol_)
    {
        this->BizIndex = BizIndex_;
        this->FunctionCode = FunctionCode_;
        this->OrderKind = OrderKind_;
        this->OrderNumber = OrderNumber_;
        this->OrderOriNo = OrderOriNo_;
        this->Price = Price_;
        this->Time = Time_;
        this->Volume = Volume_;
        this->Symbol = Symbol_;
        this->Channel = 0;
        this->TradedQty = 0;
        this->Date = 0;
        this->Multiple = 0;
    }

    ~KyStdOrderType() = default;
    int32_t index() const { return BizIndex; }
    std::string type_name() const { return "KyStdOrder"; }
    int tm() const { return Time; }
    std::string to_string() const
    {
        std::ostringstream oss;
        oss << BizIndex << ",";
        oss << Channel << ",";
        oss << FunctionCode << ",";
        oss << OrderKind << ",";
        oss << OrderNumber << ",";
        oss << OrderOriNo << ",";
        oss << Price << ",";
        oss << Time << ",";
        oss << Volume << ",";
        oss << Symbol<<",";
        oss << TradedQty << ",";
        oss << Date << ",";
        oss << Multiple;
        return oss.str();
    }

    friend std::ostream &operator<<(std::ostream &os, KyStdOrderType const &s)
    {
        os << s.to_string();
        return os;
    }
} __attribute__((packed));

