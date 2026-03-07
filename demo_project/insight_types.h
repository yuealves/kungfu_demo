#pragma once

#include "utils.h"
#include <string>
#include <vector>
#include <sstream>
#include <cstring>
#include <cstdint>
#include <variant>

#include "data_struct.hpp"

typedef int32_t T_I32;
typedef uint32_t T_U32;
typedef long long T_I64;

struct L2StockTickDataField
{
    T_I32 nTime;
    T_I32 hostTime;
    T_I32 nStatus;
    T_U32 uPreClose;
    T_U32 uOpen;
    T_U32 uHigh;
    T_U32 uLow;
    T_U32 uMatch;
    T_U32 uAskPrice1;
    T_U32 uAskPrice2;
    T_U32 uAskPrice3;
    T_U32 uAskPrice4;
    T_U32 uAskPrice5;
    T_U32 uAskPrice6;
    T_U32 uAskPrice7;
    T_U32 uAskPrice8;
    T_U32 uAskPrice9;
    T_U32 uAskPrice10;
    T_U32 uAskVol1;
    T_U32 uAskVol2;
    T_U32 uAskVol3;
    T_U32 uAskVol4;
    T_U32 uAskVol5;
    T_U32 uAskVol6;
    T_U32 uAskVol7;
    T_U32 uAskVol8;
    T_U32 uAskVol9;
    T_U32 uAskVol10;
    T_U32 uBidPrice1;
    T_U32 uBidPrice2;
    T_U32 uBidPrice3;
    T_U32 uBidPrice4;
    T_U32 uBidPrice5;
    T_U32 uBidPrice6;
    T_U32 uBidPrice7;
    T_U32 uBidPrice8;
    T_U32 uBidPrice9;
    T_U32 uBidPrice10;
    T_U32 uBidVol1;
    T_U32 uBidVol2;
    T_U32 uBidVol3;
    T_U32 uBidVol4;
    T_U32 uBidVol5;
    T_U32 uBidVol6;
    T_U32 uBidVol7;
    T_U32 uBidVol8;
    T_U32 uBidVol9;
    T_U32 uBidVol10;
    T_U32 uNumTrades;
    T_I64 iVolume;
    T_I64 iTurnover;
    T_I64 iTotalBidVol;
    T_I64 iTotalAskVol;
    T_U32 uWeightedAvgBidPrice;
    T_U32 uWeightedAvgAskPrice;
    T_I32 nIOPV;
    T_I32 nYieldToMaturity;
    T_U32 uHighLimited;
    T_U32 uLowLimited;
    char sPrefix[4];
    T_I32 nSyl1;
    T_I32 nSyl2;
    T_I32 nSD2;
    char sTradingPhraseCode[8];
    char code[7];
    T_I32 nPreIOPV;

    L2StockTickDataField() = default;

    std::string to_string() const {
        std::ostringstream oss;
        oss << "L2StockTickDataField { "
            << "nTime: " << nTime
            << ", hostTime: " << hostTime
            << ", nStatus: " << nStatus
            << ", uPreClose: " << uPreClose
            << ", uOpen: " << uOpen
            << ", uHigh: " << uHigh
            << ", uLow: " << uLow
            << ", uMatch: " << uMatch
            << ", uAskPrice1: " << uAskPrice1
            << ", uBidPrice1: " << uBidPrice1
            << ", iVolume: " << iVolume
            << ", iTurnover: " << iTurnover
            << ", code: " << std::string(code, 6)
            << " }";
        return oss.str();
    }

    std::string to_csv_header() const {
        return "nTime,hostTime,nStatus,uPreClose,uOpen,uHigh,uLow,uMatch,uAskPrice1,uAskPrice2,uAskPrice3,uAskPrice4,uAskPrice5,"
               "uAskPrice6,uAskPrice7,uAskPrice8,uAskPrice9,uAskPrice10,uAskVol1,uAskVol2,uAskVol3,uAskVol4,uAskVol5,"
               "uAskVol6,uAskVol7,uAskVol8,uAskVol9,uAskVol10,uBidPrice1,uBidPrice2,uBidPrice3,uBidPrice4,uBidPrice5,"
               "uBidPrice6,uBidPrice7,uBidPrice8,uBidPrice9,uBidPrice10,uBidVol1,uBidVol2,uBidVol3,uBidVol4,uBidVol5,"
               "uBidVol6,uBidVol7,uBidVol8,uBidVol9,uBidVol10,uNumTrades,iVolume,iTurnover,iTotalBidVol,iTotalAskVol,"
               "uWeightedAvgBidPrice,uWeightedAvgAskPrice,nIOPV,nYieldToMaturity,uHighLimited,uLowLimited,sPrefix,nSyl1,"
               "nSyl2,nSD2,sTradingPhraseCode,code";
    }

    std::string to_csv_row() const {
        std::ostringstream oss;
        oss << nTime << ',' << hostTime << ',' << nStatus << ',' << uPreClose << ',' << uOpen << ',' << uHigh << ',' << uLow << ',' << uMatch << ','
            << uAskPrice1 << ',' << uAskPrice2 << ',' << uAskPrice3 << ',' << uAskPrice4 << ',' << uAskPrice5 << ',' << uAskPrice6 << ',' << uAskPrice7 << ','
            << uAskPrice8 << ',' << uAskPrice9 << ',' << uAskPrice10 << ',' << uAskVol1 << ',' << uAskVol2 << ',' << uAskVol3 << ',' << uAskVol4 << ','
            << uAskVol5 << ',' << uAskVol6 << ',' << uAskVol7 << ',' << uAskVol8 << ',' << uAskVol9 << ',' << uAskVol10 << ',' << uBidPrice1 << ','
            << uBidPrice2 << ',' << uBidPrice3 << ',' << uBidPrice4 << ',' << uBidPrice5 << ',' << uBidPrice6 << ',' << uBidPrice7 << ',' << uBidPrice8 << ','
            << uBidPrice9 << ',' << uBidPrice10 << ',' << uBidVol1 << ',' << uBidVol2 << ',' << uBidVol3 << ',' << uBidVol4 << ',' << uBidVol5 << ','
            << uBidVol6 << ',' << uBidVol7 << ',' << uBidVol8 << ',' << uBidVol9 << ',' << uBidVol10 << ',' << uNumTrades << ',' << iVolume << ','
            << iTurnover << ',' << iTotalBidVol << ',' << iTotalAskVol << ',' << uWeightedAvgBidPrice << ',' << uWeightedAvgAskPrice << ','
            << nIOPV << ',' << nYieldToMaturity << ',' << uHighLimited << ',' << uLowLimited << ',' << std::string(sPrefix, 4) << ',' << nSyl1 << ','
            << nSyl2 << ',' << nSD2 << ',' << std::string(sTradingPhraseCode, 8) << ',' << std::string(code, 6);
        return oss.str();
    }
};

/*上海市场逐笔订单*/
struct SH_StockStepOrderField {
public:
    int32_t nHostTime;          //本地时间
    char    sSecurityID[8];     //证券代码
    int32_t nMDDate;            //交易日期
    int32_t nMDTime;            //交易时间
    int32_t nOrderIndex;        //订单编号
    int32_t nOrderType;         //委托类别
    int64_t iOrderPrice;        //委托价格
    int64_t iOrderQty;          //委托数量
    int32_t nOrderBSFlag;       //委托方向
    int32_t nChannelNo;         //频道代码
    int64_t iOrderNo;           //原始订单号
    int64_t iApplSeqNum;        //序号
    int32_t nDataMultiplePowerOf10;
    int32_t nTradedQty;         //已成交数量

    std::string to_string() const {
        std::ostringstream oss;
        oss << "nHostTime: " << nHostTime << ", sSecurityID: " << sSecurityID << ", nMDDate: " << nMDDate << ", nMDTime: " << nMDTime
            << ", nOrderIndex: " << nOrderIndex << ", nOrderType: " << nOrderType << ", iOrderPrice: " << iOrderPrice
            << ", iOrderQty: " << iOrderQty << ", nOrderBSFlag: " << nOrderBSFlag << ", nChannelNo: " << nChannelNo << ", iOrderNo: " << iOrderNo
            << ", iApplSeqNum: " << iApplSeqNum << ", nDataMultiplePowerOf10: " << nDataMultiplePowerOf10 << ", nTradedQty: " << nTradedQty;
        return oss.str();
    }

    std::string to_csv_header() const {
        return "nHostTime,sSecurityID,nMDDate,nMDTime,nOrderIndex,nOrderType,iOrderPrice,iOrderQty,nOrderBSFlag,nChannelNo,iOrderNo,iApplSeqNum,nDataMultiplePowerOf10,nTradedQty";
    }

    std::string to_csv_row() const {
        std::ostringstream oss;
        oss << nHostTime << ',' << sSecurityID << ',' << nMDDate << ',' << nMDTime << ',' << nOrderIndex << ',' << nOrderType << ',' << iOrderPrice << ',' << iOrderQty << ',' << nOrderBSFlag << ',' << nChannelNo << ',' << iOrderNo << "," << iApplSeqNum << "," << nDataMultiplePowerOf10 <<","<<nTradedQty;
        return oss.str();
    }
};

struct SZ_StockStepOrderField {
public:
    int32_t nHostTime;          //本地时间
    char    sSecurityID[8];     //证券代码
    int32_t nMDDate;            //交易日期
    int32_t nMDTime;            //交易时间
    int32_t nOrderIndex;        //订单编号
    int32_t nOrderType;         //委托类别
    int64_t iOrderPrice;        //委托价格
    int64_t iOrderQty;          //委托数量
    int32_t nOrderBSFlag;       //委托方向
    int32_t nChannelNo;         //频道代码
    int64_t iApplSeqNum;        //序号
    int32_t nDataMultiplePowerOf10;

    SZ_StockStepOrderField() = default;

    std::string to_string() const {
        std::ostringstream oss;
        oss << "nHostTime: " << nHostTime << ", sSecurityID: " << sSecurityID << ", nMDDate: " << nMDDate
            << ", nMDTime: " << nMDTime << ", nOrderIndex: " << nOrderIndex << ", nOrderType: " << nOrderType
            << ", nOrderPrice: " << iOrderPrice << ", iOrderQty: " << iOrderQty << ", nOrderBSFlag: " << nOrderBSFlag
            << ", nChannelNo: " << nChannelNo << ", iApplSeqNum: " << iApplSeqNum << ", nDataMultiplePowerOf10: " << nDataMultiplePowerOf10;
        return oss.str();
    }

    std::string to_csv_header() const {
        return "nHostTime,sSecurityID,nMDDate,nMDTime,nOrderIndex,nOrderType,iOrderPrice,iOrderQty,nOrderBSFlag,nChannelNo,iApplSeqNum,nDataMultiplePowerOf10";
    }

    std::string to_csv_row() const {
        std::ostringstream oss;
        oss << nHostTime << ',' << sSecurityID << ',' << nMDDate << ',' << nMDTime << ',' << nOrderIndex << ',' << nOrderType << ',' << iOrderPrice << ',' << iOrderQty << ',' << nOrderBSFlag << ',' << nChannelNo << ',' << iApplSeqNum << ',' << nDataMultiplePowerOf10;
        return oss.str();
    }

} __attribute__((packed));

struct SH_StockStepTradeField {
public:
    int32_t nHostTime;          //本地时间
    char    sSecurityID[8];     //证券代码
    int32_t nMDDate;            //交易日期
    int32_t nMDTime;            //交易时间
    int32_t nTradeIndex;        // 成交序号
    int64_t iTradeBuyNo;        // 买方订单号
    int64_t iTradeSellNo;       // 卖方订单号
    int32_t nTradeBSflag;       // 内外盘标识 B -外盘，主动买  S-内盘,主动卖 N 未知
    int64_t iTradePrice;        // 成交价格 扩大10000倍
    int64_t iTradeQty;          // 成交数量 ��票：股 权证：份 债券：手(千元面额)
    int64_t iTradeMoney;        // 成交金额(元)
    int32_t nTradeType;         //成交类别
    int64_t iApplSeqNum;        //序号
    int32_t nChannelNo;         //频道代码
    int32_t nDataMultiplePowerOf10;

    std::string to_string() const {
        std::ostringstream oss;
        oss << "nHostTime: " << nHostTime << ", sSecurityID: " << sSecurityID << ", nMDDate: " << nMDDate << ", nMDTime: " << nMDTime
            << ", nTradeIndex: " << nTradeIndex << ", iTradeBuyNo: " << iTradeBuyNo << ", iTradeSellNo: " << iTradeSellNo
            << ", nTradeBSflag: " << nTradeBSflag << ", iTradePrice: " << iTradePrice << ", iTradeQty: " << iTradeQty
            << ", iTradeMoney: " << iTradeMoney << ", nTradeType" << nTradeType << ", iApplSeqNum" << iApplSeqNum
            << ", nChannelNo: " << nChannelNo << ", nDataMultiplePowerOf10" << nDataMultiplePowerOf10;
        return oss.str();
    }

    std::string to_csv_header() const {
        return "nHostTime,sSecurityID,nMDDate,nMDTime,nTradeIndex,iTradeBuyNo,iTradeSellNo,nTradeBSflag,iTradePrice,iTradeQty,iTradeMoney,nTradeType,iApplSeqNum,nChannelNo,nDataMultiplePowerOf10";
    }

    std::string to_csv_row() const {
        std::ostringstream oss;
        oss << nHostTime << ',' << sSecurityID << ',' << nMDDate << ',' << nMDTime << ',' << nTradeIndex << ',' << iTradeBuyNo
            << ',' << iTradeSellNo << ',' << nTradeBSflag << ',' << iTradePrice << ',' << iTradeQty << ',' << iTradeMoney
            << ',' << nTradeType << ',' << iApplSeqNum << ',' << nChannelNo << ',' << nDataMultiplePowerOf10;
        return oss.str();
    }

} __attribute__((packed));

struct SZ_StockStepTradeField {
public:
    int32_t nHostTime;          //本地时间
    char    sSecurityID[8];     //证券代码
    int32_t nMDDate;            //交易日期
    int32_t nMDTime;            //交易时间
    int32_t nTradeIndex;        // 成交序号
    int64_t iTradeBuyNo;        // 买方订单号
    int64_t iTradeSellNo;       // 卖方订单号
    int32_t nTradeBSflag;       // 内外盘标识 B -外盘，主动买  S-内盘,主动卖 N 未知
    int64_t iTradePrice;        // 成交价格 扩大10000倍
    int64_t iTradeQty;          // 成交数量 股票：股 权证：份 债券：手(千元面额)
    int64_t iTradeMoney;        // 成交金额(元)
    int32_t nTradeType;         //成交类别
    int64_t iApplSeqNum;        //序号
    int32_t nChannelNo;         //频道代码
    int32_t nDataMultiplePowerOf10;

    std::string to_string() const {
        std::ostringstream oss;
        oss << "nHostTime: " << nHostTime << ", sSecurityID: " << sSecurityID << ", nMDDate: " << nMDDate << ", nMDTime: " << nMDTime
            << ", nTradeIndex: " << nTradeIndex << ", iTradeBuyNo: " << iTradeBuyNo << ", iTradeSellNo: " << iTradeSellNo
            << ", nTradeBSflag: " << nTradeBSflag << ", iTradePrice: " << iTradePrice << ", iTradeQty: " << iTradeQty
            << ", iTradeMoney: " << iTradeMoney << ", nTradeType" << nTradeType << ", iApplSeqNum" << iApplSeqNum
            << ", nChannelNo: " << nChannelNo << ", nDataMultiplePowerOf10" << nDataMultiplePowerOf10;
        return oss.str();
    }

    std::string to_csv_header() const {
        return "nHostTime,sSecurityID,nMDDate,nMDTime,nTradeIndex,iTradeBuyNo,iTradeSellNo,nTradeBSflag,iTradePrice,iTradeQty,iTradeMoney,nTradeType,iApplSeqNum,nChannelNo,nDataMultiplePowerOf10";
    }

    std::string to_csv_row() const {
        std::ostringstream oss;
        oss << nHostTime << ',' << sSecurityID << ',' << nMDDate << ',' << nMDTime << ',' << nTradeIndex << ',' << iTradeBuyNo
            << ',' << iTradeSellNo << ',' << nTradeBSflag << ',' << iTradePrice << ',' << iTradeQty << ',' << iTradeMoney
            << ',' << nTradeType << ',' << iApplSeqNum << ',' << nChannelNo << ',' << nDataMultiplePowerOf10;
        return oss.str();
    }

} __attribute__((packed));

L2StockTickDataField trans_tick(const KyStdSnpType& ky_snp_data, int32_t host_time);

std::variant<SZ_StockStepOrderField, SH_StockStepOrderField> trans_order(const KyStdOrderType& ky_order_data, int32_t host_time, const std::string& exchange);

std::variant<SZ_StockStepTradeField, SH_StockStepTradeField> trans_trade(const KyStdTradeType& ky_trade_data, int32_t host_time, const std::string& exchange);
