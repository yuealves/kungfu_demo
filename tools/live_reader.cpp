/**
 * Live Reader - 通过 Paged 实时读取 journal 数据
 *
 * 每秒打印最新一帧的信息，包含三个时间：
 *   - exchange_time:  payload 中的交易所行情时间
 *   - original_time:  原始 journal 的 nano（存于 extra_nano）
 *   - replay_time:    回放写入时的 nano（frame header nano）
 *
 * 用法: ./live_reader
 */

#include "JournalReader.h"
#include "Timer.h"
#include "data_struct.hpp"
#include "sys_messages.h"
#include "utils.h"

#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <csignal>
#include <atomic>
#include <ctime>

static std::atomic<bool> g_running{true};
static void signal_handler(int) { g_running = false; }

// 纳秒时间戳 → "HH:MM:SS.mmm" (UTC+8)
static std::string nano_to_str(long nano) {
    if (nano <= 0) return "N/A";
    time_t sec = nano / 1000000000L;
    int ms = (nano % 1000000000L) / 1000000;
    struct tm t;
    localtime_r(&sec, &t);
    char buf[32];
    snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03d", t.tm_hour, t.tm_min, t.tm_sec, ms);
    return buf;
}

// 交易所时间 int (HHMMSSmmm) → "HH:MM:SS.mmm"
static std::string exchange_time_to_str(int t) {
    if (t <= 0) return "N/A";
    int ms  = t % 1000;    t /= 1000;
    int sec = t % 100;     t /= 100;
    int min = t % 100;     t /= 100;
    int hour = t;
    char buf[32];
    snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03d", hour, min, sec, ms);
    return buf;
}

static const char* msg_type_name(short msg_type) {
    switch (msg_type) {
        case MSG_TYPE_L2_TICK:  return "TICK";
        case MSG_TYPE_L2_ORDER: return "ORDER";
        case MSG_TYPE_L2_TRADE: return "TRADE";
        default:                return "OTHER";
    }
}

int main(int argc, const char* argv[])
{
    using namespace kungfu::yijinjing;

    std::string journal_dir = "/shared/kungfu/journal/user/";
    std::string reader_name = "live_reader_" + parseNano(getNanoTime(), "%H%M%S");

    std::vector<std::string> dirs = {journal_dir, journal_dir, journal_dir};
    std::vector<std::string> jnames = {
        "insight_stock_tick_data",
        "insight_stock_order_data",
        "insight_stock_trade_data",
    };

    std::cout << "[init] reader: " << reader_name << std::endl;

    long start_time = 0;
    JournalReaderPtr reader = JournalReader::createReaderWithSys(
        dirs, jnames, start_time, reader_name);

    std::cout << "[init] connected to Paged, waiting for data..." << std::endl;

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // 每种消息类型分别记录最新帧信息和计数
    struct ChannelState {
        const char* name;
        const char* count_label;
        long  count = 0;
        long  nano = 0;          // replay time (frame header nano)
        long  extra_nano = 0;    // original time
        int   exchange_time = 0;
        int   symbol = 0;
    };
    ChannelState ch[3] = {{"TICK", "tick_count"}, {"ORDER", "order_count"}, {"TRADE", "trade_count"}};

    auto last_print = std::chrono::steady_clock::now();
    long last_total = 0;

    while (g_running) {
        FramePtr frame = reader->getNextFrame();

        if (frame.get() == nullptr) {
            // 没有新数据时，检查是否该打印
            auto now = std::chrono::steady_clock::now();
            long total = ch[0].count + ch[1].count + ch[2].count;
            if (total > last_total &&
                std::chrono::duration_cast<std::chrono::seconds>(now - last_print).count() >= 1) {
                for (auto& c : ch) {
                    if (c.count == 0) continue;
                    std::cout << "[" << std::setw(5) << std::left << c.name << "]"
                              << " symbol=" << std::setw(6) << std::setfill('0') << std::right << c.symbol << std::setfill(' ')
                              << " | exchange=" << exchange_time_to_str(c.exchange_time)
                              << " | original=" << nano_to_str(c.extra_nano)
                              << " | replay=" << nano_to_str(c.nano)
                              << " | " << c.count_label << "=" << c.count
                              << std::endl;
                }
                std::cout << std::endl;
                last_print = now;
                last_total = total;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            continue;
        }

        short msg_type = frame->getMsgType();
        void* data = frame->getData();

        int idx = -1;
        if (msg_type == MSG_TYPE_L2_TICK)       idx = 0;
        else if (msg_type == MSG_TYPE_L2_ORDER) idx = 1;
        else if (msg_type == MSG_TYPE_L2_TRADE) idx = 2;
        if (idx < 0) continue;

        auto& c = ch[idx];
        c.count++;
        c.nano = frame->getNano();
        c.extra_nano = frame->getExtraNano();

        if (idx == 0) {
            auto* md = (KyStdSnpType*)data;
            c.exchange_time = md->Time;
            c.symbol = md->Symbol;
        } else if (idx == 1) {
            auto* md = (KyStdOrderType*)data;
            c.exchange_time = md->Time;
            c.symbol = md->Symbol;
        } else {
            auto* md = (KyStdTradeType*)data;
            c.exchange_time = md->Time;
            c.symbol = md->Symbol;
        }

        // 每秒打印一次
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_print).count() >= 1) {
            long total = ch[0].count + ch[1].count + ch[2].count;
            for (auto& c : ch) {
                if (c.count == 0) continue;
                std::cout << "[" << std::setw(5) << std::left << c.name << "]"
                          << " symbol=" << std::setw(6) << std::setfill('0') << std::right << c.symbol << std::setfill(' ')
                          << " | exchange=" << exchange_time_to_str(c.exchange_time)
                          << " | original=" << nano_to_str(c.extra_nano)
                          << " | replay=" << nano_to_str(c.nano)
                          << " | " << c.count_label << "=" << c.count
                          << std::endl;
            }
            std::cout << std::endl;
            last_print = now;
            last_total = total;
        }
    }

    std::cout << "\n[done] tick=" << ch[0].count
              << " order=" << ch[1].count
              << " trade=" << ch[2].count
              << std::endl;

    return 0;
}
