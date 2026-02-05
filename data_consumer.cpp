#include "data_consumer.h"
#include "Timer.h"

#include <iostream>
#include <sstream>
#include <math.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <algorithm>
#include <cstring>

// 本地定义 Journal 文件格式结构（避免头文件 byte 类型冲突）
#define JOURNAL_SHORT_NAME_MAX_LENGTH 30
#define JOURNAL_FRAME_STATUS_WRITTEN 1

struct LocalPageHeader {
    unsigned char status;
    char journal_name[JOURNAL_SHORT_NAME_MAX_LENGTH];
    short page_num;
    long start_nano;
    long close_nano;
    int frame_num;
    int last_pos;
    short frame_version;
    short reserve_short[3];
    long reserve_long[9];
} __attribute__((packed));

struct LocalFrameHeader {
    volatile unsigned char status;
    short source;
    long nano;
    int length;
    unsigned int hash;
    short msg_type;
    unsigned char last_flag;
    int req_id;
    long extra_nano;
    int err_id;
} __attribute__((packed));

YJJ_NAMESPACE_START

// 简单的本地 Journal 页面读取器
class LocalJournalPage {
public:
    void* buffer;
    int size;
    int current_pos;

    LocalJournalPage() : buffer(nullptr), size(0), current_pos(0) {}

    ~LocalJournalPage() {
        if (buffer) {
            munmap(buffer, size);
        }
    }

    bool load(const std::string& filepath) {
        int fd = open(filepath.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Cannot open file: " << filepath << std::endl;
            return false;
        }

        struct stat st;
        fstat(fd, &st);
        size = st.st_size;

        buffer = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        close(fd);

        if (buffer == MAP_FAILED) {
            buffer = nullptr;
            return false;
        }

        // 跳过 PageHeader
        current_pos = sizeof(LocalPageHeader);
        return true;
    }

    LocalFrameHeader* nextFrame() {
        if (!buffer || current_pos >= size) return nullptr;

        LocalFrameHeader* header = (LocalFrameHeader*)((char*)buffer + current_pos);

        // 检查 frame 是否有效
        if (header->status != JOURNAL_FRAME_STATUS_WRITTEN) {
            return nullptr;
        }

        // 移动到下一个 frame
        current_pos += header->length;
        return header;
    }

    void* getFrameData(LocalFrameHeader* header) {
        return (void*)((char*)header + sizeof(LocalFrameHeader));
    }
};

// 获取目录下所有匹配的 journal 文件
std::vector<std::string> getJournalFiles(const std::string& dir, const std::string& jname) {
    std::vector<std::string> files;
    DIR* d = opendir(dir.c_str());
    if (!d) return files;

    std::string pattern = "yjj." + jname + ".";
    struct dirent* entry;
    while ((entry = readdir(d)) != nullptr) {
        std::string filename = entry->d_name;
        if (filename.find(pattern) == 0 && filename.find(".journal") != std::string::npos) {
            files.push_back(dir + filename);
        }
    }
    closedir(d);

    // 按文件名排序（按页号）
    std::sort(files.begin(), files.end());
    return files;
}

DataConsumer::DataConsumer(): start_nano(-1), end_nano(-1), reader(nullptr), cur_time(-1), is_running(true)
{
    std::cout << "start!" << std::endl;
}

void DataConsumer::init(long start_nano, long end_nano)
{
    this->start_nano = start_nano;
    this->end_nano = end_nano;
    cur_time = start_nano;
    // 不再使用 JournalReader，改用本地文件直接读取
}

void DataConsumer::run()
{
    // 使用本地文件读取方式
    for (const auto& writer_pair : journal_pair) {
        std::string dir = writer_pair.first;
        std::string jname = writer_pair.second;

        std::vector<std::string> files = getJournalFiles(dir, jname);
        std::cout << "Found " << files.size() << " journal files for " << jname << std::endl;

        for (const auto& filepath : files) {
            std::cout << "Processing: " << filepath << std::endl;

            LocalJournalPage page;
            if (!page.load(filepath)) {
                std::cerr << "Failed to load: " << filepath << std::endl;
                continue;
            }

            LocalFrameHeader* header;
            while ((header = page.nextFrame()) != nullptr) {
                void* data = page.getFrameData(header);
                short msg_type = header->msg_type;
                cur_time = header->nano;

                if (msg_type == MSG_TYPE_L2_TICK) {
                    KyStdSnpType* md = (KyStdSnpType*)data;
                    on_market_data(md);
                }
                else if (msg_type == MSG_TYPE_L2_ORDER) {
                    KyStdOrderType* md = (KyStdOrderType*)data;
                    on_order_data(md);
                }
                else if (msg_type == MSG_TYPE_L2_TRADE) {
                    KyStdTradeType* md = (KyStdTradeType*)data;
                    on_trade_data(md);
                }
            }
        }
    }

    std::cout << "\n=== Summary ===" << std::endl;
    std::cout << "Total tick data: " << tick_offset << std::endl;
    std::cout << "Total order data: " << order_offset << std::endl;
    std::cout << "Total trade data: " << trade_offset << std::endl;
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


