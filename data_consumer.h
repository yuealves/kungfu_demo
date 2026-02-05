#pragma once

#include "JournalReader.h"
#include "insight_types.h"
#include "utils.h"

YJJ_NAMESPACE_START

class DataConsumer
{
protected:
    long start_nano;
    long end_nano;
    /** reader */
    JournalReaderPtr reader;
    /** current nano time */
    long cur_time;
    bool is_running;

protected:
    virtual void on_market_data(KyStdSnpType* md)
    {
        tick_offset++;
        if(tick_offset % 100000 == 100){
            std::cout << "get " << tick_offset << " tick data" << std::endl;
            int host_time = parse_nano(this->cur_time);
            L2StockTickDataField l2_tick = trans_tick(*md, host_time);
            std::cout << l2_tick.to_string() << std::endl;
        }

    };
    virtual void on_order_data(KyStdOrderType* md)
    {
        order_offset++;
        if(order_offset % 100000 == 100){
            std::cout << "get " << order_offset << " order data" << std::endl;
            int host_time = parse_nano(this->cur_time);
            std::string exchange = decode_exchange(md->Symbol);
            auto l2_order = trans_order(*md, host_time, exchange);
            auto res = std::visit([](const auto& field) {
                return field.to_string();
            }, l2_order);
            std::cout << res << std::endl;
            //std::cout << md->htscsecurityid << " ," << md->securityidsource << std::endl;
        }
    };

    virtual void on_trade_data(KyStdTradeType* md)
    {
        trade_offset++;
        if(trade_offset % 100000 == 100){
            std::cout << "get " << trade_offset << " trade data" << std::endl;
            int host_time = parse_nano(this->cur_time);
            std::string exchange = decode_exchange(md->Symbol);
            auto l2_trade = trans_trade(*md, host_time, exchange);
            auto res = std::visit([](const auto& field) {
                return field.to_string();
            }, l2_trade);
            std::cout << res << std::endl;
            
        }
    };
public:
    DataConsumer();
    void init(long start_nano, long end_nano);

    void run();
    int tick_offset = 0;
    int order_offset = 0;
    int trade_offset = 0;
    std::string tick_channel = "insight_stock_tick_data";
    std::string order_channel = "insight_stock_order_data";
    std::string trade_channel = "insight_stock_trade_data";
    std::string path = "/shared/kungfu/journal/user/";
    std::vector<std::pair<std::string, std::string>> journal_pair = {{path, tick_channel}, {path, order_channel},  { path, trade_channel}};
};

class DataFetcher : public DataConsumer
{
public:
    std::vector<L2StockTickDataField> get_tick_data(long start_nano, long end_nano);
    //std::vector<L2StockTickDataField> get_gj_tick_data(long start_nano, long end_nano);
    //std::vector<StockOrderData> get_order_data(long start_nano, long end_nano);
    std::vector<SH_StockStepOrderField> get_sh_order_data(long start_nano, long end_nano);
    std::vector<SZ_StockStepOrderField> get_sz_order_data(long start_nano, long end_nano);
    std::vector<SH_StockStepTradeField> get_sh_trade_data(long start_nano, long end_nano);
    std::vector<SZ_StockStepTradeField> get_sz_trade_data(long start_nano, long end_nano);
    //sstd::vector<StockTransaction> get_trade_data(long start_nano, long end_nano);
};

YJJ_NAMESPACE_END