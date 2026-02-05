#pragma once

#include "utils.h"
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <vector>
// #include <map>
#include <sstream>
#include <iostream>
#include <fstream>
#include <thread>
#include "data_struct.hpp"

#include <vector>
#include <cstring>
#include <sstream>
#include <csignal>
#include <fstream>
#include <memory.h>
#include <cstdlib>
#include <stdio.h>
#include <cstdint>
#include <variant>

typedef int32_t T_I32;
typedef uint32_t T_U32;
typedef long long T_I64;

using namespace std;
using namespace com::htsc::mdc::gateway;
using namespace com::htsc::mdc::insight::model;
using namespace com::htsc::mdc::model;

struct StockTickData
{
public:
    std::string htscsecurityid;
    int mddate;
    int mdtime;
    std::string securityidsource;
    long preclosepx;
    long totalvolumetrade;
    long totalvaluetrade;
    long lastpx;
    long openpx;
    long highpx;
    long lowpx;
    long diffpx1;
    long totalbuyqty;
    long totalsellqty;
    long weightedavgbuypx;
    long weightedavgsellpx;
    std::vector<long> buypricequeue;
    std::vector<long> buyorderqtyqueue;
    std::vector<long> sellpricequeue;
    std::vector<long> sellorderqtyqueue;
    std::vector<long> buyorderqueue;
    std::vector<long> sellorderqueue;
    std::string tradingphasecode;
    long receiveTime;
    long numtrades;
    long afterprice;
    long afterturnover;
    long aftervolume;
    long afternumtrades;

    // 默认构造函数
    StockTickData() = default;

    // 构造函数
    StockTickData(const std::string &htscsecurityid, int mddate, int mdtime, const std::string &securityidsource,
                  long preclosepx, long totalvolumetrade, long totalvaluetrade, long lastpx, long openpx, long highpx, long lowpx,
                  long diffpx1, long totalbuyqty, long totalsellqty, long weightedavgbuypx, long weightedavgsellpx,
                  const std::vector<long> &buypricequeue, const std::vector<long> &buyorderqtyqueue,
                  const std::vector<long> &sellpricequeue, const std::vector<long> &sellorderqtyqueue,
                  const std::vector<long> &buyorderqueue, const std::vector<long> &sellorderqueue,
                  const std::vector<long> &buynumordersqueue, const std::vector<long> &sellnumordersqueue,
                  const std::string &tradingphasecode, long receiveTime, long numtrades, long afterprice, long afterturnover, long aftervolume, long afternumtrades)
        : htscsecurityid(htscsecurityid), mddate(mddate), mdtime(mdtime), securityidsource(securityidsource),
          preclosepx(preclosepx), totalvolumetrade(totalvolumetrade), totalvaluetrade(totalvaluetrade), lastpx(lastpx),
          openpx(openpx), highpx(highpx), lowpx(lowpx), diffpx1(diffpx1), totalbuyqty(totalbuyqty), totalsellqty(totalsellqty),
          weightedavgbuypx(weightedavgbuypx), weightedavgsellpx(weightedavgsellpx), buypricequeue(buypricequeue),
          buyorderqtyqueue(buyorderqtyqueue), sellpricequeue(sellpricequeue), sellorderqtyqueue(sellorderqtyqueue),
          buyorderqueue(buyorderqueue), sellorderqueue(sellorderqueue), tradingphasecode(tradingphasecode), receiveTime(receiveTime),
          numtrades(numtrades), afterprice(afterprice), afterturnover(afterturnover), aftervolume(aftervolume), afternumtrades(afternumtrades)
    {
    }
    StockTickData(const com::htsc::mdc::insight::model::MarketData &data)
    {
        if (data.has_mdstock())
        {
            const auto &stock = data.mdstock();
            htscsecurityid = stock.htscsecurityid();
            mddate = stock.mddate();
            mdtime = stock.mdtime();
            securityidsource = stock.securityidsource();
            preclosepx = stock.preclosepx();
            totalvolumetrade = stock.totalvolumetrade();
            totalvaluetrade = stock.totalvaluetrade();
            lastpx = stock.lastpx();
            openpx = stock.openpx();
            highpx = stock.highpx();
            lowpx = stock.lowpx();
            diffpx1 = stock.diffpx1();
            totalbuyqty = stock.totalbuyqty();
            totalsellqty = stock.totalsellqty();
            weightedavgbuypx = stock.weightedavgbuypx();
            weightedavgsellpx = stock.weightedavgsellpx();
            tradingphasecode = stock.tradingphasecode();
            numtrades = stock.numtrades();
            afterprice = stock.afterhourslastpx();
            afterturnover = stock.afterhourstotalvaluetrade();
            aftervolume = stock.afterhourstotalvolumetrade();
            afternumtrades = stock.afterhoursnumtrades();

            receiveTime = 0;

            buypricequeue.reserve(10);
            for (int i = 0; i < 10; ++i)
            {
                if (i >= stock.buypricequeue_size())
                {
                    buypricequeue.push_back(0);
                }
                else
                {
                    buypricequeue.push_back(stock.buypricequeue(i));
                }
            }

            buyorderqtyqueue.reserve(10);
            for (int i = 0; i < 10; ++i)
            {
                if (i >= stock.buyorderqtyqueue_size())
                {
                    buyorderqtyqueue.push_back(0);
                }
                else
                {
                    buyorderqtyqueue.push_back(stock.buyorderqtyqueue(i));
                }
            }

            sellpricequeue.reserve(10);
            for (int i = 0; i < 10; ++i)
            {
                if (i >= stock.sellpricequeue_size())
                {
                    sellpricequeue.push_back(0);
                }
                else
                {
                    sellpricequeue.push_back(stock.sellpricequeue(i));
                }
            }

            sellorderqtyqueue.reserve(10);
            for (int i = 0; i < 10; ++i)
            {
                if (i >= stock.sellorderqtyqueue_size())
                {
                    sellorderqtyqueue.push_back(0);
                }
                else
                {
                    sellorderqtyqueue.push_back(stock.sellorderqtyqueue(i));
                }
            }

            buyorderqueue.reserve(10);
            for (int i = 0; i < 10; ++i)
            {
                if (i >= stock.buyorderqueue_size())
                {
                    buyorderqueue.push_back(0);
                }
                else
                {
                    buyorderqueue.push_back(stock.buyorderqueue(i));
                }
            }

            sellorderqueue.reserve(10);
            for (int i = 0; i < 10; ++i)
            {
                if (i >= stock.sellorderqueue_size())
                {
                    sellorderqueue.push_back(0);
                }
                else
                {
                    sellorderqueue.push_back(stock.sellorderqueue(i));
                }
            }
        }
        else
        {
            throw std::invalid_argument("Invalid MarketData type");
        }
    }

    // to_string方法
    std::string to_string() const
    {
        std::ostringstream oss;
        //oss << htscsecurityid << ","
        oss << mddate << ","
            << mdtime << ","
            //<< securityidsource << ","
            << preclosepx << ","
            << totalvolumetrade << ","
            << totalvaluetrade << ","
            << lastpx << ","
            << openpx << ","
            << highpx << ","
            << lowpx << ","
            << diffpx1 << ","
            << totalbuyqty << ","
            << totalsellqty << ","
            << weightedavgbuypx << ","
            << weightedavgsellpx << ",";
        for (const auto &val : buypricequeue)
        {
            oss << val << ",";
        }
        for (const auto &val : buyorderqtyqueue)
        {
            oss << val << ",";
        }
        for (const auto &val : sellpricequeue)
        {
            oss << val << ",";
        }
        for (const auto &val : sellorderqtyqueue)
        {
            oss << val << ",";
        }
        for (const auto &val : buyorderqueue)
        {
            oss << val << ",";
        }
        for (const auto &val : sellorderqueue)
        {
            oss << val << ",";
        }
        //oss << tradingphasecode << ","
        oss << numtrades << ","
            << afterprice << ","
            << afterturnover << ","
            << aftervolume << ","
            << afternumtrades << ","
            << receiveTime;
        return oss.str();
    }

    KyStdSnpType ParseFrom() const
    {
        int32_t symbol = std::stoi(this->htscsecurityid.substr(0, this->htscsecurityid.length() - 3));
        return KyStdSnpType(
            this->totalvaluetrade,
            this->totalvolumetrade,
            this->afternumtrades,
            static_cast<float>(this->afterprice),
            this->afterturnover,
            this->aftervolume,
            static_cast<float>(this->weightedavgsellpx),
            0,
            static_cast<float>(this->weightedavgbuypx),
            static_cast<float>(this->highpx),
            static_cast<float>(this->lowpx),
            this->numtrades,
            static_cast<float>(this->openpx),
            static_cast<float>(this->preclosepx),
            static_cast<float>(this->lastpx),
            this->mdtime,
            this->totalsellqty / 1000,
            this->totalbuyqty / 1000,
            0,
            0,
            symbol,
            static_cast<float>(this->buypricequeue[0]),
            static_cast<float>(this->buypricequeue[1]),
            static_cast<float>(this->buypricequeue[2]),
            static_cast<float>(this->buypricequeue[3]),
            static_cast<float>(this->buypricequeue[4]),
            static_cast<float>(this->buypricequeue[5]),
            static_cast<float>(this->buypricequeue[6]),
            static_cast<float>(this->buypricequeue[7]),
            static_cast<float>(this->buypricequeue[8]),
            static_cast<float>(this->buypricequeue[9]),
            static_cast<float64_t>(this->buyorderqtyqueue[0]),
            static_cast<float64_t>(this->buyorderqtyqueue[1]),
            static_cast<float64_t>(this->buyorderqtyqueue[2]),
            static_cast<float64_t>(this->buyorderqtyqueue[3]),
            static_cast<float64_t>(this->buyorderqtyqueue[4]),
            static_cast<float64_t>(this->buyorderqtyqueue[5]),
            static_cast<float64_t>(this->buyorderqtyqueue[6]),
            static_cast<float64_t>(this->buyorderqtyqueue[7]),
            static_cast<float64_t>(this->buyorderqtyqueue[8]),
            static_cast<float64_t>(this->buyorderqtyqueue[9]),
            static_cast<float>(this->sellpricequeue[0]),
            static_cast<float>(this->sellpricequeue[1]),
            static_cast<float>(this->sellpricequeue[2]),
            static_cast<float>(this->sellpricequeue[3]),
            static_cast<float>(this->sellpricequeue[4]),
            static_cast<float>(this->sellpricequeue[5]),
            static_cast<float>(this->sellpricequeue[6]),
            static_cast<float>(this->sellpricequeue[7]),
            static_cast<float>(this->sellpricequeue[8]),
            static_cast<float>(this->sellpricequeue[9]),
            static_cast<float64_t>(this->sellorderqtyqueue[0]),
            static_cast<float64_t>(this->sellorderqtyqueue[1]),
            static_cast<float64_t>(this->sellorderqtyqueue[2]),
            static_cast<float64_t>(this->sellorderqtyqueue[3]),
            static_cast<float64_t>(this->sellorderqtyqueue[4]),
            static_cast<float64_t>(this->sellorderqtyqueue[5]),
            static_cast<float64_t>(this->sellorderqtyqueue[6]),
            static_cast<float64_t>(this->sellorderqtyqueue[7]),
            static_cast<float64_t>(this->sellorderqtyqueue[8]),
            static_cast<float64_t>(this->sellorderqtyqueue[9])
        );
    }
};

struct IndexTickData
{
public:
    int mddate;
    int mdtime;
    std::string htscsecurityid;
    long lastpx;
    long highpx;
    long lowpx;
    long totalvolumetrade;
    long totalvaluetrade;
    std::string tradingphasecode;
    long receiveTime;

    // 构造函数
    IndexTickData(int mddate, int mdtime, const std::string &htscsecurityid, long lastpx, long highpx, long lowpx,
                  long totalvolumetrade, long totalvaluetrade, const std::string &tradingphasecode, long receiveTime)
        : mddate(mddate), mdtime(mdtime), htscsecurityid(htscsecurityid), lastpx(lastpx), highpx(highpx), lowpx(lowpx),
          totalvolumetrade(totalvolumetrade), totalvaluetrade(totalvaluetrade), tradingphasecode(tradingphasecode), receiveTime(receiveTime) {}

    // 构造函数，通过 MarketData 初始化
    IndexTickData(const com::htsc::mdc::insight::model::MarketData &data)
    {
        if (data.has_mdindex())
        {
            const auto &index = data.mdindex();
            htscsecurityid = index.htscsecurityid();
            mddate = index.mddate();
            mdtime = index.mdtime();
            lastpx = index.lastpx();
            highpx = index.highpx();
            lowpx = index.lowpx();
            totalvolumetrade = index.totalvolumetrade();
            totalvaluetrade = index.totalvaluetrade();
            tradingphasecode = index.tradingphasecode();
            receiveTime = 0;
        }
        else
        {
            throw std::invalid_argument("Invalid MarketData type:Index");
        }
    }
    // to_string方法
    std::string to_string() const
    {
        std::ostringstream oss;
        oss << mddate << ","
            << mdtime << ","
            << htscsecurityid << ","
            << lastpx << ","
            << highpx << ","
            << lowpx << ","
            << totalvolumetrade << ","
            << totalvaluetrade << ","
            << tradingphasecode << ","
            << receiveTime;
        return oss.str();
    }
} __attribute__((packed));

struct FutureTickData
{
public:
    std::string htscsecurityid;
    int mddate;
    int mdtime;
    std::string securityidsource;
    long preclosepx;
    long totalvolumetrade;
    long totalvaluetrade;
    long lastpx;
    long openpx;
    long highpx;
    long lowpx;
    long preopeninterest;
    long presettleprice;
    long openinterest;
    std::vector<long> buypricequeue;
    std::vector<long> buyorderqtyqueue;
    std::vector<long> sellpricequeue;
    std::vector<long> sellorderqtyqueue;
    std::string tradingphasecode;
    long receiveTime;

    // 构造函数
    FutureTickData(const std::string &htscsecurityid, int mddate, int mdtime, const std::string &securityidsource,
                   long preclosepx, long totalvolumetrade, long totalvaluetrade, long lastpx, long openpx, long highpx, long lowpx,
                   long preopeninterest, long presettleprice, long openinterest,
                   const std::vector<long> &buypricequeue, const std::vector<long> &buyorderqtyqueue,
                   const std::vector<long> &sellpricequeue, const std::vector<long> &sellorderqtyqueue,
                   const std::string &tradingphasecode, long receiveTime)
        : htscsecurityid(htscsecurityid), mddate(mddate), mdtime(mdtime), securityidsource(securityidsource),
          preclosepx(preclosepx), totalvolumetrade(totalvolumetrade), totalvaluetrade(totalvaluetrade), lastpx(lastpx),
          openpx(openpx), highpx(highpx), lowpx(lowpx), preopeninterest(preopeninterest), presettleprice(presettleprice),
          openinterest(openinterest), buypricequeue(buypricequeue), buyorderqtyqueue(buyorderqtyqueue),
          sellpricequeue(sellpricequeue), sellorderqtyqueue(sellorderqtyqueue), tradingphasecode(tradingphasecode),
          receiveTime(receiveTime) {}

    FutureTickData(const com::htsc::mdc::insight::model::MarketData &data)
    {
        {
            htscsecurityid = data.mdfuture().htscsecurityid();
            mddate = data.mdfuture().mddate();
            mdtime = realTime(data.mdfuture().mdtime());
            securityidsource = data.mdfuture().securityidsource();
            preclosepx = data.mdfuture().preclosepx();
            totalvolumetrade = data.mdfuture().totalvolumetrade();
            totalvaluetrade = data.mdfuture().totalvaluetrade();
            lastpx = data.mdfuture().lastpx();
            openpx = data.mdfuture().openpx();
            highpx = data.mdfuture().highpx();
            lowpx = data.mdfuture().lowpx();
            preopeninterest = data.mdfuture().preopeninterest();
            presettleprice = data.mdfuture().presettleprice();
            openinterest = data.mdfuture().openinterest();
            tradingphasecode = data.mdfuture().tradingphasecode();
            receiveTime = 0;

            // 构建队列数据
            buypricequeue.reserve(data.mdfuture().buypricequeue_size());
            for (int i = 0; i < data.mdfuture().buypricequeue_size(); ++i)
            {
                buypricequeue.push_back(data.mdfuture().buypricequeue(i));
            }

            buyorderqtyqueue.reserve(data.mdfuture().buyorderqtyqueue_size());
            for (int i = 0; i < data.mdfuture().buyorderqtyqueue_size(); ++i)
            {
                buyorderqtyqueue.push_back(data.mdfuture().buyorderqtyqueue(i));
            }

            sellpricequeue.reserve(data.mdfuture().sellpricequeue_size());
            for (int i = 0; i < data.mdfuture().sellpricequeue_size(); ++i)
            {
                sellpricequeue.push_back(data.mdfuture().sellpricequeue(i));
            }

            sellorderqtyqueue.reserve(data.mdfuture().sellorderqtyqueue_size());
            for (int i = 0; i < data.mdfuture().sellorderqtyqueue_size(); ++i)
            {
                sellorderqtyqueue.push_back(data.mdfuture().sellorderqtyqueue(i));
            }
        }
    }

    // to_string方法
    std::string to_string() const
    {
        std::ostringstream oss;
        //oss << htscsecurityid << ","
        oss << mddate << ","
            << mdtime << ","
            //<< securityidsource << ","
            << preclosepx << ","
            << totalvolumetrade << ","
            << totalvaluetrade << ","
            << lastpx << ","
            << openpx << ","
            << highpx << ","
            << lowpx << ","
            << preopeninterest << ","
            << presettleprice << ","
            << openinterest << ",";
        /*
                for (const auto &val : buypricequeue)
        {
            oss << val << ",";
        }
        for (const auto &val : buyorderqtyqueue)
        {
            oss << val << ",";
        }
        for (const auto &val : sellpricequeue)
        {
            oss << val << ",";
        }
        for (const auto &val : sellorderqtyqueue)
        {
            oss << val << ",";
        }
        */

        //oss << tradingphasecode << ","
        oss << receiveTime;
        return oss.str();
    }
} __attribute__((packed));

struct KlineData
{
public:
    std::string htscsecurityid;
    int mddate;
    int mdtime;
    int openpx;
    int closepx;
    int highpx;
    int lowpx;
    int numtrades;
    long totalvolumetrade;
    long totalvaluetrade;
    long long receiveTime;

    // 构造函数
    KlineData(const std::string &htscsecurityid, int mddate, int mdtime, int openpx, int closepx,
              int highpx, int lowpx, int numtrades, long totalvolumetrade, long totalvaluetrade, long long receiveTime)
        : htscsecurityid(htscsecurityid), mddate(mddate), mdtime(mdtime), openpx(openpx), closepx(closepx),
          highpx(highpx), lowpx(lowpx), numtrades(numtrades), totalvolumetrade(totalvolumetrade),
          totalvaluetrade(totalvaluetrade), receiveTime(receiveTime) {}
    KlineData(const com::htsc::mdc::insight::model::MarketData &data)
    {
        ADKLine ad_kline = data.mdkline();
        if (data.mdkline().klinecategory() == 11)
        {
            htscsecurityid = data.mdkline().htscsecurityid();
            mddate = data.mdkline().mddate();
            mdtime = realTime(data.mdkline().mdtime());
            openpx = data.mdkline().openpx();
            closepx = data.mdkline().closepx();
            highpx = data.mdkline().highpx();
            lowpx = data.mdkline().lowpx();
            numtrades = data.mdkline().numtrades();
            totalvolumetrade = data.mdkline().totalvolumetrade();
            totalvaluetrade = data.mdkline().totalvaluetrade();
            receiveTime = 0;
        }
        else
        {
            throw std::invalid_argument("Invalid klinecategory");
        }
    }

    // to_string方法
    std::string to_string() const
    {
        std::ostringstream oss;
        oss << htscsecurityid << ","
            << mddate << ","
            << mdtime << ","
            << openpx << ","
            << closepx << ","
            << highpx << ","
            << lowpx << ","
            << numtrades << ","
            << totalvolumetrade << ","
            << totalvaluetrade << ","
            << receiveTime;
        return oss.str();
    }
};

struct StockTransaction
{
public:
    std::string htscsecurityid;
    int mddate;
    int mdtime;
    std::string securityidsource;
    long tradeindex;
    long tradebuyno;
    long tradesellno;
    int tradebsflag;
    long tradeprice;
    long tradeqty;
    long trademoney;
    long channelno;
    long applseqnum;
    long receiveTime;
    int tradetype;
    int multiple;


    // 构造函数
    StockTransaction() = default;
    StockTransaction(const std::string &htscsecurityid, int mddate, int mdtime, const std::string &securityidsource,
                     long tradeindex, long tradebuyno, long tradesellno, int tradebsflag, long tradeprice,
                     long tradeqty, long trademoney, long channelno, long applseqnum, long receiveTime, int TradeType)
        : htscsecurityid(htscsecurityid), mddate(mddate), mdtime(mdtime), securityidsource(securityidsource),
          tradeindex(tradeindex), tradebuyno(tradebuyno), tradesellno(tradesellno), tradebsflag(tradebsflag),
          tradeprice(tradeprice), tradeqty(tradeqty), trademoney(trademoney), applseqnum(applseqnum), channelno(channelno),
          receiveTime(receiveTime), tradetype(TradeType) {}

    // 构造函数，通过 MarketData 初始化
    StockTransaction(const com::htsc::mdc::insight::model::MarketData &data)
    {
        if (data.has_mdtransaction())
        {
            const auto &record = data.mdtransaction();
            htscsecurityid = record.htscsecurityid();
            mddate = record.mddate();
            mdtime = record.mdtime();
            securityidsource = record.securityidsource();
            tradeindex = record.tradeindex();
            tradebuyno = record.tradebuyno();
            tradesellno = record.tradesellno();
            tradebsflag = record.tradebsflag();
            tradeprice = record.tradeprice();
            tradeqty = record.tradeqty();
            trademoney = record.trademoney();
            applseqnum = record.applseqnum();
            channelno = record.channelno();
            receiveTime = 0;
            tradetype = record.tradetype();
            multiple = record.datamultiplepowerof10();
        }
        else
        {
            throw std::invalid_argument("Invalid MarketData type");
        }
    }

    // to_string方法
    std::string to_string() const
    {
        std::ostringstream oss;
        //oss << htscsecurityid << ","
        oss << mddate << ","
            << mdtime << ","
            //<< securityidsource << ","
            << tradeindex << ","
            << tradebuyno << ","
            << tradesellno << ","
            << tradebsflag << ","
            << tradeprice << ","
            << tradeqty << ","
            << trademoney << ","
            << applseqnum << ","
            << tradetype << ","
            << channelno << ","
            << receiveTime;
        return oss.str();
    }

    // 将StockTransaction写入CSV文件
    void to_csv(const std::string &filePath) const
    {
        std::ofstream file(filePath);
        if (!file.is_open())
        {
            std::cerr << "Failed to open file: " << filePath << std::endl;
            return;
        }

        // 写入CSV头
        if (file.tellp() == 0)
        {
            file << "htscsecurityid,mddate,mdtime,securityidsource,tradeindex,tradebuyno,tradesellno,tradebsflag,tradeprice,tradeqty,trademoney,applseqnum,tradetype,channelno,receiveTime\n";
        }
        // 写入数据
        file << to_string() << "\n";

        file.close();
    }
    KyStdTradeType ParseFrom() const
    {
        /*
        int8_t bsFlag;
        if (this->tradebsflag == 1)
            bsFlag = 66; // 'S'
        else if (this->tradebsflag == 2)
            bsFlag = 83;
        else
            bsFlag = 0;

        int8_t function_code;
        if(this->tradetype == 0 || this->tradetype == 9)
            function_code = 70; // 'F'
        else
            function_code = 52; // '4'
        */

        int32_t symbol = 0;
        symbol = std::stoi(this->htscsecurityid.substr(0, this->htscsecurityid.length() - 3));
        return KyStdTradeType(
            this->tradesellno,
            (int8_t) this->tradebsflag,
            this->tradebuyno,
            this->applseqnum,
            this->channelno,
            (int8_t) this->tradetype,  // FunctionCode
            this->tradeindex,
            0,  // OrderKind
            static_cast<float>(this->tradeprice),
            this->mdtime,
            this->tradeqty,
            symbol,
            this->mddate,
            this->multiple,
            this->trademoney
        );


    }
} __attribute__((packed));

struct StockOrderData
{
public:
    std::string htscsecurityid;
    int mddate;
    int mdtime;
    std::string securityidsource;
    std::string securitytype;
    long orderindex;
    int ordertype;
    long orderprice;
    long orderqty;
    int orderbsflag;
    long orderno;
    long applseqnum;
    int channelno;
    long receiveTime;
    int tradedqty;
    int multiple;

    // 构造函数
    StockOrderData(const std::string &htscsecurityid, int mddate, int mdtime, const std::string &securityidsource,
                   const std::string &securitytype, long orderindex, int ordertype, long orderprice, long orderqty,
                   int orderbsflag, long orderno, long applseqnum, int channelno, long receiveTime)
        : htscsecurityid(htscsecurityid), mddate(mddate), mdtime(mdtime), securityidsource(securityidsource),
          securitytype(securitytype), orderindex(orderindex), ordertype(ordertype), orderprice(orderprice),
          orderqty(orderqty), orderbsflag(orderbsflag), orderno(orderno), applseqnum(applseqnum),
          channelno(channelno), receiveTime(receiveTime) {}

    StockOrderData(const com::htsc::mdc::insight::model::MarketData &data)
    {
        if (data.has_mdorder())
        {
            const auto &record = data.mdorder();
            htscsecurityid = record.htscsecurityid();
            mddate = record.mddate();
            mdtime = record.mdtime();
            securityidsource = record.securityidsource();
            securitytype = record.securitytype();
            orderindex = record.orderindex();
            ordertype = record.ordertype();
            orderprice = record.orderprice();
            orderqty = record.orderqty();
            orderbsflag = record.orderbsflag();
            orderno = record.orderno();
            applseqnum = record.applseqnum();
            channelno = record.channelno();
            receiveTime = 0;
            tradedqty = record.tradedqty();
            multiple = record.datamultiplepowerof10();
        }
        else
        {
            throw std::invalid_argument("Invalid MarketData type");
        }
    }

    StockOrderData() = default;

    // to_string方法
    std::string to_string() const
    {
        std::ostringstream oss;
        //oss << htscsecurityid << ","
        oss << mddate << ","
            << mdtime << ","
            //<< securityidsource << ","
            //<< securitytype << ","
            << orderindex << ","
            << ordertype << ","
            << orderprice << ","
            << orderqty << ","
            << orderbsflag << ","
            << orderno << ","
            << applseqnum << ","
            << channelno << ","
            << receiveTime<< ","
            << tradedqty;
        return oss.str();
    }

    // 将StockOrderData写入CSV文件
    void to_csv(const std::string &filePath) const
    {
        std::ofstream file(filePath);
        if (!file.is_open())
        {
            std::cerr << "Failed to open file: " << filePath << std::endl;
            return;
        }

        // 写入CSV头
        if (file.tellp() == 0)
        {
            file << "htscsecurityid,mddate,mdtime,securityidsource,securitytype,orderindex,ordertype,orderprice,orderqty,orderbsflag,orderno,applseqnum,channelno,receiveTime,tradedqty\n";
        }
        // 写入数据
        file << to_string() << "\n";

        file.close();
    }

    KyStdOrderType ParseFrom() const
    {
        /*
        int8_t functionCode;
        if (this->orderbsflag == 1)
            functionCode = 66; // 'S'
        else if (this->orderbsflag == 2)
            functionCode = 83;
        else
            functionCode = 0;

        int8_t orderKind = 65;
        if (this->ordertype == 1)
            orderKind = 49;// '1'
        else if (this->ordertype == 2)
            orderKind = 50; // '2'
        else if (this->ordertype == 3)
            orderKind = 85; // 'U'
        else if (this->ordertype == 10)
            orderKind = 68; // 'D'
        else {
            orderKind = 65;
        }
        */

        int32_t symbol = std::stoi(this->htscsecurityid.substr(0, this->htscsecurityid.length() - 3));

        return KyStdOrderType(
            this->applseqnum,
            this->channelno,
            (int8_t) this->orderbsflag,
            (int8_t) this->ordertype,
            this->orderindex,
            this->orderno,
            this->orderprice,
            this->mdtime,
            this->orderqty,
            symbol,
            this->tradedqty,
            this->mddate,
            this->multiple
        );
    }

} __attribute__((packed));

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
            << ", uAskPrice2: " << uAskPrice2
            << ", uAskPrice3: " << uAskPrice3
            << ", uAskPrice4: " << uAskPrice4
            << ", uAskPrice5: " << uAskPrice5
            << ", uAskPrice6: " << uAskPrice6
            << ", uAskPrice7: " << uAskPrice7
            << ", uAskPrice8: " << uAskPrice8
            << ", uAskPrice9: " << uAskPrice9
            << ", uAskPrice10: " << uAskPrice10
            << ", uAskVol1: " << uAskVol1
            << ", uAskVol2: " << uAskVol2
            << ", uAskVol3: " << uAskVol3
            << ", uAskVol4: " << uAskVol4
            << ", uAskVol5: " << uAskVol5
            << ", uAskVol6: " << uAskVol6
            << ", uAskVol7: " << uAskVol7
            << ", uAskVol8: " << uAskVol8
            << ", uAskVol9: " << uAskVol9
            << ", uAskVol10: " << uAskVol10
            << ", uBidPrice1: " << uBidPrice1
            << ", uBidPrice2: " << uBidPrice2
            << ", uBidPrice3: " << uBidPrice3
            << ", uBidPrice4: " << uBidPrice4
            << ", uBidPrice5: " << uBidPrice5
            << ", uBidPrice6: " << uBidPrice6
            << ", uBidPrice7: " << uBidPrice7
            << ", uBidPrice8: " << uBidPrice8
            << ", uBidPrice9: " << uBidPrice9
            << ", uBidPrice10: " << uBidPrice10
            << ", uBidVol1: " << uBidVol1
            << ", uBidVol2: " << uBidVol2
            << ", uBidVol3: " << uBidVol3
            << ", uBidVol4: " << uBidVol4
            << ", uBidVol5: " << uBidVol5
            << ", uBidVol6: " << uBidVol6
            << ", uBidVol7: " << uBidVol7
            << ", uBidVol8: " << uBidVol8
            << ", uBidVol9: " << uBidVol9
            << ", uBidVol10: " << uBidVol10
            << ", uNumTrades: " << uNumTrades
            << ", iVolume: " << iVolume
            << ", iTurnover: " << iTurnover
            << ", iTotalBidVol: " << iTotalBidVol
            << ", iTotalAskVol: " << iTotalAskVol
            << ", uWeightedAvgBidPrice: " << uWeightedAvgBidPrice
            << ", uWeightedAvgAskPrice: " << uWeightedAvgAskPrice
            << ", nIOPV: " << nIOPV
            << ", nYieldToMaturity: " << nYieldToMaturity
            << ", uHighLimited: " << uHighLimited
            << ", uLowLimited: " << uLowLimited
            << ", sPrefix: " << std::string(sPrefix, 4)
            << ", nSyl1: " << nSyl1
            << ", nSyl2: " << nSyl2
            << ", nSD2: " << nSD2
            << ", sTradingPhraseCode: " << std::string(sTradingPhraseCode, 8)
            << ", code: " << std::string(code, 6)
            << ", nPreIOPV: " << nPreIOPV
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

    // 用 StockTickData 初始化的函数
    L2StockTickDataField(const StockTickData& stock_tick_data) {
        nTime = stock_tick_data.mdtime;
        hostTime = stock_tick_data.receiveTime;
        nStatus = 0;

        uPreClose = stock_tick_data.preclosepx;  
        uOpen = stock_tick_data.openpx;  
        uHigh = stock_tick_data.highpx;  
        uLow = stock_tick_data.lowpx;  
        uMatch = stock_tick_data.lastpx;  

        uAskPrice1 = stock_tick_data.sellpricequeue[0];
        uAskPrice2 = stock_tick_data.sellpricequeue[1];
        uAskPrice3 = stock_tick_data.sellpricequeue[2];
        uAskPrice4 = stock_tick_data.sellpricequeue[3];
        uAskPrice5 = stock_tick_data.sellpricequeue[4];
        uAskPrice6 = stock_tick_data.sellpricequeue[5];
        uAskPrice7 = stock_tick_data.sellpricequeue[6];
        uAskPrice8 = stock_tick_data.sellpricequeue[7];
        uAskPrice9 = stock_tick_data.sellpricequeue[8];
        uAskPrice10 = stock_tick_data.sellpricequeue[9];

        uAskVol1 = stock_tick_data.sellorderqtyqueue[0];
        uAskVol2 = stock_tick_data.sellorderqtyqueue[1];
        uAskVol3 = stock_tick_data.sellorderqtyqueue[2];
        uAskVol4 = stock_tick_data.sellorderqtyqueue[3];
        uAskVol5 = stock_tick_data.sellorderqtyqueue[4];
        uAskVol6 = stock_tick_data.sellorderqtyqueue[5];
        uAskVol7 = stock_tick_data.sellorderqtyqueue[6];
        uAskVol8 = stock_tick_data.sellorderqtyqueue[7];
        uAskVol9 = stock_tick_data.sellorderqtyqueue[8];
        uAskVol10 = stock_tick_data.sellorderqtyqueue[9];

        uBidPrice1 = stock_tick_data.buypricequeue[0];
        uBidPrice2 = stock_tick_data.buypricequeue[1];
        uBidPrice3 = stock_tick_data.buypricequeue[2];
        uBidPrice4 = stock_tick_data.buypricequeue[3];
        uBidPrice5 = stock_tick_data.buypricequeue[4];
        uBidPrice6 = stock_tick_data.buypricequeue[5];
        uBidPrice7 = stock_tick_data.buypricequeue[6];
        uBidPrice8 = stock_tick_data.buypricequeue[7];
        uBidPrice9 = stock_tick_data.buypricequeue[8];
        uBidPrice10 = stock_tick_data.buypricequeue[9];

        uBidVol1 = stock_tick_data.buyorderqtyqueue[0];
        uBidVol2 = stock_tick_data.buyorderqtyqueue[1];
        uBidVol3 = stock_tick_data.buyorderqtyqueue[2];
        uBidVol4 = stock_tick_data.buyorderqtyqueue[3];
        uBidVol5 = stock_tick_data.buyorderqtyqueue[4];
        uBidVol6 = stock_tick_data.buyorderqtyqueue[5];
        uBidVol7 = stock_tick_data.buyorderqtyqueue[6];
        uBidVol8 = stock_tick_data.buyorderqtyqueue[7];
        uBidVol9 = stock_tick_data.buyorderqtyqueue[8];
        uBidVol10 = stock_tick_data.buyorderqtyqueue[9];

        uNumTrades = stock_tick_data.numtrades;
        iVolume = stock_tick_data.totalvolumetrade;
        iTurnover = stock_tick_data.totalvaluetrade;
        iTotalBidVol = stock_tick_data.totalbuyqty / 1000;
        iTotalAskVol = stock_tick_data.totalsellqty / 1000;

        uWeightedAvgBidPrice = stock_tick_data.weightedavgbuypx;
        uWeightedAvgAskPrice = stock_tick_data.weightedavgsellpx;

        nIOPV = 0;
        nYieldToMaturity = 0;
        uHighLimited = 0;
        uLowLimited = 0;

        std::copy(stock_tick_data.htscsecurityid.begin(), stock_tick_data.htscsecurityid.end(), code);
        std::copy(stock_tick_data.tradingphasecode.begin(), stock_tick_data.tradingphasecode.end(), sTradingPhraseCode);

        nSyl1 = 0;
        nSyl2 = 0;
        nSD2 = 0;
        nPreIOPV = 0;
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

void WriteIndexTickDataToCSV(const std::vector<IndexTickData> &indexDataList, const std::string &filePath);
void WriteKlineDataToCSV(const std::vector<KlineData> &klineDataList, const std::string &filePath);
void WriteFutureTickDataToCSV(const std::vector<FutureTickData> &futureDataList, const std::string &filePath);
void WriteStockTickDataToCSV(const ::vector<StockTickData> &marketDataList, const std::string &filePath);
void WriteStockOrderDataToCSV(const std::vector<StockOrderData> &stock_order_list, const std::string &filePath);
void WriteStockTransactionDataToCSV(const std::vector<StockTransaction> &transaction_list, const std::string &filePath);
