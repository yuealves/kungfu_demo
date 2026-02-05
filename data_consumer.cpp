#include "data_consumer.h"
#include "Timer.h"

#include <iostream>
#include <sstream>
#include <math.h>

YJJ_NAMESPACE_START

DataConsumer::DataConsumer(): start_nano(-1), end_nano(-1), cur_time(-1), is_running(true)
{
    std::cout << "start!" << std::endl;
}

void DataConsumer::init(long start_nano, long end_nano)
{
    this->start_nano = start_nano;
    this->end_nano = end_nano;
    std::string reader_name = "Consumer_" + parseNano(getNanoTime(), "%H%M%S");
    vector<std::string> empty;
    reader = JournalReader::createReaderWithSys(empty, empty, start_nano, reader_name);
    cur_time = start_nano;
    for(const auto& writer_pair: journal_pair){
        int idx = reader->addJournal(writer_pair.first, writer_pair.second);
        reader->seekTimeJournal(idx, cur_time);
    }

}

void DataConsumer::run()
{
    FramePtr frame;

    while (is_running)
    {
        {
            frame = reader->getNextFrame();
            if (frame.get() != nullptr)
            {
                void* data = frame->getData();
                short msg_type = frame->getMsgType();
                cur_time = frame->getNano();
                if(msg_type == MSG_TYPE_L2_TICK){
                    KyStdSnpType *md = (KyStdSnpType*)data;
                    on_market_data(md);
                }
                else if(msg_type == MSG_TYPE_L2_ORDER){
                    KyStdOrderType* md = (KyStdOrderType*)data;
                    on_order_data(md);
                }
                else if(msg_type == MSG_TYPE_L2_TRADE){
                    KyStdTradeType* md = (KyStdTradeType*)data;
                    on_trade_data(md);
                }
                else{
                    std::cout << "can't recognize type: " << msg_type << std::endl;
                }
            }

        }
    }
}

std::vector<L2StockTickDataField> DataFetcher::get_tick_data(long start_nano, long end_nano)
{
    string reader_name = "Consumer_" + parseNano(getNanoTime(), "%H%M%S");
    vector<string> empty;
    JournalReaderPtr tick_reader = JournalReader::create(path, tick_channel, start_nano, reader_name);
    cur_time = start_nano;

    FramePtr frame;
    std::vector<L2StockTickDataField> res;
    while (end_nano < 0 || cur_time < end_nano){
        frame = tick_reader->getNextFrame();
        if (frame.get() != nullptr)
        {
            void* data = frame->getData();
            short msg_type = frame->getMsgType();
            cur_time = frame->getNano();
            if(msg_type == MSG_TYPE_L2_TICK){
                KyStdSnpType *md = (KyStdSnpType*)data;
                int host_time = parse_nano(cur_time);
                auto l2_tick = trans_tick(*md, host_time);
                res.push_back(l2_tick);
            }
        }
        else
        {
            return res;
        }
    }

    return res;
}

std::vector<SH_StockStepOrderField> DataFetcher::get_sh_order_data(long start_nano, long end_nano)
{
    string reader_name = "Consumer_" + parseNano(getNanoTime(), "%H%M%S");
    JournalReaderPtr sh_order_reader = JournalReader::create(path, order_channel, start_nano, reader_name);
    cur_time = start_nano;
    FramePtr frame;
    std::vector<SH_StockStepOrderField> res;
    while (end_nano < 0 || cur_time < end_nano){
        frame = sh_order_reader->getNextFrame();
        if (frame.get() != nullptr)
        {
            void* data = frame->getData();
            short msg_type = frame->getMsgType();
            cur_time = frame->getNano();
            if(msg_type == MSG_TYPE_L2_ORDER){
                KyStdOrderType *md = (KyStdOrderType*)data;
                int host_time = parse_nano(this->cur_time);
                std::string exchange = decode_exchange(md->Symbol);
                if(exchange == "SH")
                {
                    auto l2_order = trans_order(*md, host_time, exchange);
                    res.push_back(std::get<SH_StockStepOrderField>(l2_order));
                }
            }
        }
        else
        {
            return res;
        }
    }

    return res;
}

std::vector<SZ_StockStepOrderField> DataFetcher::get_sz_order_data(long start_nano, long end_nano)
{
    string reader_name = "Consumer_" + parseNano(getNanoTime(), "%H%M%S");
    JournalReaderPtr sh_order_reader = JournalReader::create(path, order_channel, start_nano, reader_name);
    cur_time = start_nano;
    FramePtr frame;
    std::vector<SZ_StockStepOrderField> res;
    while (end_nano < 0 || cur_time < end_nano){
        frame = sh_order_reader->getNextFrame();
        if (frame.get() != nullptr)
        {
            void* data = frame->getData();
            short msg_type = frame->getMsgType();
            cur_time = frame->getNano();
            if(msg_type == MSG_TYPE_L2_ORDER){
                KyStdOrderType *md = (KyStdOrderType*)data;
                int host_time = parse_nano(this->cur_time);
                std::string exchange = decode_exchange(md->Symbol);
                if(exchange == "SZ")
                {
                    auto l2_order = trans_order(*md, host_time, exchange);
                    res.push_back(std::get<SZ_StockStepOrderField>(l2_order));
                }
            }
        }
        else
        {
            return res;
        }
    }

    return res;
}


std::vector<SH_StockStepTradeField> DataFetcher::get_sh_trade_data(long start_nano, long end_nano)
{
    string reader_name = "Consumer_" + parseNano(getNanoTime(), "%H%M%S");
    JournalReaderPtr sh_trade_reader = JournalReader::create(path, trade_channel, start_nano, reader_name);
    cur_time = start_nano;
    FramePtr frame;
    std::vector<SH_StockStepTradeField> res;
    while (end_nano < 0 || cur_time < end_nano){
        frame = sh_trade_reader->getNextFrame();
        if (frame.get() != nullptr)
        {
            void* data = frame->getData();
            short msg_type = frame->getMsgType();
            cur_time = frame->getNano();
            if(msg_type == MSG_TYPE_L2_TRADE){
                KyStdTradeType *md = (KyStdTradeType*)data;
                int host_time = parse_nano(this->cur_time);
                std::string exchange = decode_exchange(md->Symbol);
                if(exchange == "SH")
                {
                    auto l2_trade = trans_trade(*md, host_time, exchange);
                    res.push_back(std::get<SH_StockStepTradeField>(l2_trade));
                }
            }
        }
        else
        {
            return res;
        }
    }

    return res;
}

std::vector<SZ_StockStepTradeField> DataFetcher::get_sz_trade_data(long start_nano, long end_nano)
{
    string reader_name = "Consumer_" + parseNano(getNanoTime(), "%H%M%S");
    JournalReaderPtr sz_trade_reader = JournalReader::create(path, trade_channel, start_nano, reader_name);
    cur_time = start_nano;
    FramePtr frame;
    std::vector<SZ_StockStepTradeField> res;
    while (end_nano < 0 || cur_time < end_nano){
        frame = sz_trade_reader->getNextFrame();
        if (frame.get() != nullptr)
        {
            void* data = frame->getData();
            short msg_type = frame->getMsgType();
            cur_time = frame->getNano();
            if(msg_type == MSG_TYPE_L2_TRADE){
                KyStdTradeType *md = (KyStdTradeType*)data;
                int host_time = parse_nano(this->cur_time);
                std::string exchange = decode_exchange(md->Symbol);
                if(exchange == "SZ")
                {
                    auto l2_trade = trans_trade(*md, host_time, exchange);
                    res.push_back(std::get<SZ_StockStepTradeField>(l2_trade));
                }
            }
        }
        else
        {
            return res;
        }
    }

    return res;
}




YJJ_NAMESPACE_END


int main(int argc, const char* argv[])
{
    long start_nano = 0;
    string format("%Y%m%d-%H:%M:%S");
    string name("test_consumer");
    string end_time = "20260101-00:00:00";
    long end_nano = kungfu::yijinjing::parseTime(end_time.c_str(), format.c_str());
    kungfu::yijinjing::DataConsumer consumer;
    consumer.init(start_nano, end_nano);
    consumer.run();

    //获取指定时间段的数据
    /*
    {
    string format("%Y%m%d-%H:%M:%S");
    string name("test_consumer");
    string end_time = "20250101-00:00:00";
    string start_time = "20240809-10:50:00";
    long end_nano = kungfu::yijinjing::parseTime(end_time.c_str(), format.c_str());
    long start_nano = kungfu::yijinjing::parseTime(start_time.c_str(), format.c_str());
    kungfu::yijinjing::DataFetcher data_fetcher;

    {
        auto tick_data = data_fetcher.get_tick_data(start_nano, end_nano);
        for(int i = 0 ; i < tick_data.size(); i++){
            if(i % 100000 == 100)
                std::cout << tick_data[i].to_string() << std::endl;
        }
    }

    {
        auto sh_order_data = data_fetcher.get_sh_order_data(start_nano, end_nano);
        for(int i = 0 ; i < sh_order_data.size(); i++){
            if(i % 100000 == 100)
                std::cout << sh_order_data[i].to_string() << std::endl;
        }
    }

    {
        auto sz_order_data = data_fetcher.get_sz_order_data(start_nano, end_nano);
        for(int i = 0 ; i < sz_order_data.size(); i++){
            if(i % 100000 == 100)
                std::cout << sz_order_data[i].to_string() << std::endl;
        }
    }

    {
        auto sh_trade_data = data_fetcher.get_sh_trade_data(start_nano, end_nano);
        for(int i = 0 ; i < sh_trade_data.size(); i++){
            if(i % 100000 == 100)
                std::cout << sh_trade_data[i].to_string() << std::endl;
        }
    }

    {
        auto sz_trade_data = data_fetcher.get_sz_trade_data(start_nano, end_nano);
        for(int i = 0 ; i < sz_trade_data.size(); i++){
            if(i % 100000 == 100)
                std::cout << sz_trade_data[i].to_string() << std::endl;
        }
    }
}
    */
}


