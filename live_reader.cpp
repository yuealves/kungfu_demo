/**
 * Live Reader - 通过 Paged 的 createReaderWithSys 实时读取 journal 数据，
 * 模拟线上策略程序接收行情的方式。
 *
 * 用法: ./live_reader [print_interval]
 *   print_interval: 每隔多少条打印一次详细数据，默认 10000
 *
 * 需要 Paged 服务已启动，且有 writer 正在往对应频道写数据。
 */

#include "JournalReader.h"
#include "Timer.h"
#include "data_struct.hpp"
#include "insight_types.h"
#include "sys_messages.h"
#include "utils.h"

#include <iostream>
#include <chrono>
#include <thread>
#include <csignal>
#include <atomic>

static std::atomic<bool> g_running{true};
static void signal_handler(int) { g_running = false; }

int main(int argc, const char* argv[])
{
    int print_interval = 10000;
    if (argc > 1) print_interval = std::atoi(argv[1]);
    if (print_interval <= 0) print_interval = 10000;

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
    std::cout << "[init] journal dir: " << journal_dir << std::endl;
    std::cout << "[init] print interval: " << print_interval << std::endl;

    // 通过 Paged 创建 reader（ClientPageProvider），从当前时间开始读
    long start_time = 0;  // TIME_FROM_FIRST: 从头开始读
    JournalReaderPtr reader = JournalReader::createReaderWithSys(
        dirs, jnames, start_time, reader_name);

    std::cout << "[init] connected to Paged, waiting for data..." << std::endl;

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    long tick_count = 0;
    long order_count = 0;
    long trade_count = 0;
    long sys_count = 0;
    long other_count = 0;

    auto last_report = std::chrono::steady_clock::now();
    long last_total = 0;

    while (g_running) {
        FramePtr frame = reader->getNextFrame();

        if (frame.get() == nullptr) {
            // 没有新数据，短暂休眠避免空转
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            continue;
        }

        short msg_type = frame->getMsgType();
        long nano = frame->getNano();
        void* data = frame->getData();

        if (msg_type == MSG_TYPE_L2_TICK) {
            tick_count++;
            if (tick_count % print_interval == 1) {
                KyStdSnpType* md = (KyStdSnpType*)data;
                int host_time = parse_nano(nano);
                L2StockTickDataField l2_tick = trans_tick(*md, host_time);
                std::cout << "[TICK #" << tick_count << "] " << l2_tick.to_string() << std::endl;
            }
        }
        else if (msg_type == MSG_TYPE_L2_ORDER) {
            order_count++;
            if (order_count % print_interval == 1) {
                KyStdOrderType* md = (KyStdOrderType*)data;
                int host_time = parse_nano(nano);
                std::string exchange = decode_exchange(md->Symbol);
                auto l2_order = trans_order(*md, host_time, exchange);
                auto res = std::visit([](const auto& field) {
                    return field.to_string();
                }, l2_order);
                std::cout << "[ORDER #" << order_count << "] " << res << std::endl;
            }
        }
        else if (msg_type == MSG_TYPE_L2_TRADE) {
            trade_count++;
            if (trade_count % print_interval == 1) {
                KyStdTradeType* md = (KyStdTradeType*)data;
                int host_time = parse_nano(nano);
                std::string exchange = decode_exchange(md->Symbol);
                auto l2_trade = trans_trade(*md, host_time, exchange);
                auto res = std::visit([](const auto& field) {
                    return field.to_string();
                }, l2_trade);
                std::cout << "[TRADE #" << trade_count << "] " << res << std::endl;
            }
        }
        else if (msg_type == MSG_TYPE_PAGED_START || msg_type == MSG_TYPE_PAGED_END) {
            sys_count++;
            std::cout << "[SYS] msg_type=" << msg_type
                      << (msg_type == MSG_TYPE_PAGED_START ? " (PAGED_START)" : " (PAGED_END)")
                      << std::endl;
        }
        else {
            other_count++;
        }

        // 每 3 秒打印一次统计
        auto now = std::chrono::steady_clock::now();
        long total = tick_count + order_count + trade_count;
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_report).count() >= 3 && total > last_total) {
            long delta = total - last_total;
            std::cout << "[stats] tick=" << tick_count
                      << " order=" << order_count
                      << " trade=" << trade_count
                      << " | +" << delta << " frames in 3s"
                      << " | time=" << parseNano(nano, "%H:%M:%S")
                      << std::endl;
            last_report = now;
            last_total = total;
        }
    }

    std::cout << "\n[done] === Summary ===" << std::endl;
    std::cout << "  tick:  " << tick_count << std::endl;
    std::cout << "  order: " << order_count << std::endl;
    std::cout << "  trade: " << trade_count << std::endl;
    std::cout << "  sys:   " << sys_count << std::endl;
    if (other_count > 0)
        std::cout << "  other: " << other_count << std::endl;

    return 0;
}
