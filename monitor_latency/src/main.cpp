/**
 * Monitor Latency - 持续监控 tick/order/trade 三个频道的最新时间戳，
 * 写入监控文件供外部检查行情延迟。
 *
 * 用法: ./monitor_latency -o <output_dir> [-i <seconds>] [-d <journal_dir>]
 *   -o: 输出目录（必选），自动生成 latency_YYYYMMDD.csv 文件名（东八区日期）
 *   -i: 写文件间隔秒数，默认 1
 *   -d: journal 目录，默认 /shared/kungfu/journal/user/
 */

#include "JournalReader.h"
#include "Timer.h"
#include "data_struct.hpp"
#include "sys_messages.h"

#include <iostream>
#include <fstream>
#include <chrono>
#include <thread>
#include <csignal>
#include <atomic>
#include <ctime>
#include <unistd.h>

static std::atomic<bool> g_running{true};
static void signal_handler(int) { g_running = false; }

struct ChannelState {
    long journal_nano = 0;
    int exchange_time = 0;
};

// 纳秒时间戳 → 东八区可读字符串 "YYYY-MM-DD HH:MM:SS.mmm"
// 依赖 main() 中 setenv("TZ", "Asia/Shanghai") 设置时区
static std::string format_nano_cst(long nano)
{
    if (nano <= 0) return "NULL";
    time_t sec = nano / 1000000000L;
    int ms = (nano % 1000000000L) / 1000000L;
    struct tm tm_buf;
    localtime_r(&sec, &tm_buf);
    char buf[32];
    int n = strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_buf);
    snprintf(buf + n, sizeof(buf) - n, ".%03d", ms);
    return std::string(buf);
}

// exchange_time (HH*10000000 + MM*100000 + SS*1000 + mmm) → "HH:MM:SS.mmm"
static std::string format_exchange_time(int t)
{
    if (t == 0) return "NULL";
    int hh = t / 10000000;
    int mm = (t / 100000) % 100;
    int ss = (t / 1000) % 100;
    int ms = t % 1000;
    char buf[16];
    snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03d", hh, mm, ss, ms);
    return std::string(buf);
}

static void write_csv_header(const std::string& path)
{
    std::ofstream ofs(path, std::ios::app);
    if (!ofs.is_open()) {
        std::cerr << "[monitor] failed to open " << path << std::endl;
        return;
    }
    ofs << "write_time,monitor_item,journal_time,exchange_time\n";
}

// 用系统时钟获取当前东八区时间（不依赖 KungFu getNanoTime）
static std::string get_current_cst_time()
{
    auto now = std::chrono::system_clock::now();
    time_t sec = std::chrono::system_clock::to_time_t(now);
    int ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count() % 1000;
    struct tm tm_buf;
    localtime_r(&sec, &tm_buf);
    char buf[32];
    int n = strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_buf);
    snprintf(buf + n, sizeof(buf) - n, ".%03d", ms);
    return std::string(buf);
}

static void append_monitor_data(const std::string& path,
                                const ChannelState& tick,
                                const ChannelState& order,
                                const ChannelState& trade)
{
    std::ofstream ofs(path, std::ios::app);
    if (!ofs.is_open()) {
        std::cerr << "[monitor] failed to open " << path << std::endl;
        return;
    }

    std::string wt = get_current_cst_time();
    ofs << wt << ",tick," << format_nano_cst(tick.journal_nano) << "," << format_exchange_time(tick.exchange_time) << "\n";
    ofs << wt << ",order," << format_nano_cst(order.journal_nano) << "," << format_exchange_time(order.exchange_time) << "\n";
    ofs << wt << ",trade," << format_nano_cst(trade.journal_nano) << "," << format_exchange_time(trade.exchange_time) << "\n";
}

// 获取东八区当天日期，格式 YYYYMMDD
// 依赖 main() 中 setenv("TZ", "Asia/Shanghai") 设置时区
static std::string get_cst_date()
{
    time_t now = time(nullptr);
    struct tm tm_buf;
    localtime_r(&now, &tm_buf);
    char buf[16];
    strftime(buf, sizeof(buf), "%Y%m%d", &tm_buf);
    return std::string(buf);
}

static void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " -o <output_dir> [-i <seconds>] [-d <journal_dir>]\n"
              << "  -o  输出目录（必选），自动生成 latency_YYYYMMDD.csv（东八区日期）\n"
              << "  -i  写文件间隔秒数，默认 1\n"
              << "  -d  journal 目录，默认 /shared/kungfu/journal/user/（通常无需指定）\n";
}

int main(int argc, char* argv[])
{
    // 强制使用东八区，不依赖容器时区配置
    setenv("TZ", "Asia/Shanghai", 1);
    tzset();

    std::string output_dir;
    int interval_sec = 1;
    std::string journal_dir = "/shared/kungfu/journal/user/";

    int opt;
    while ((opt = getopt(argc, argv, "o:i:d:h")) != -1) {
        switch (opt) {
            case 'o': output_dir = optarg; break;
            case 'i': interval_sec = std::atoi(optarg); break;
            case 'd': journal_dir = optarg; break;
            case 'h':
            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    if (output_dir.empty()) {
        std::cerr << "[monitor] ERROR: -o <output_dir> is required\n";
        print_usage(argv[0]);
        return 1;
    }

    // 拼接输出文件路径：<output_dir>/latency_YYYYMMDD.csv
    std::string today = get_cst_date();
    if (output_dir.back() != '/') output_dir += '/';
    std::string output_path = output_dir + "latency_" + today + ".csv";
    if (interval_sec <= 0) interval_sec = 1;

    using namespace kungfu::yijinjing;

    std::string reader_name = "latency_mon_" + parseNano(getNanoTime(), "%H%M%S");

    std::vector<std::string> dirs = {journal_dir, journal_dir, journal_dir};
    std::vector<std::string> jnames = {
        "insight_stock_tick_data",
        "insight_stock_order_data",
        "insight_stock_trade_data",
    };

    std::cout << "[monitor] reader: " << reader_name << std::endl;
    std::cout << "[monitor] journal dir: " << journal_dir << std::endl;
    std::cout << "[monitor] output: " << output_path << " (date=" << today << ")" << std::endl;
    std::cout << "[monitor] interval: " << interval_sec << "s" << std::endl;

    // 从当前时间开始读，跳过历史数据，直接定位到最新位置
    auto now_tp = std::chrono::system_clock::now();
    long start_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
        now_tp.time_since_epoch()).count();
    JournalReaderPtr reader = JournalReader::createReaderWithSys(
        dirs, jnames, start_time, reader_name);

    std::cout << "[monitor] connected to Paged, monitoring..." << std::endl;

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // 如果文件不存在或为空，写入 CSV header
    {
        std::ifstream check(output_path);
        if (!check.good() || check.peek() == std::ifstream::traits_type::eof()) {
            write_csv_header(output_path);
        }
    }

    ChannelState tick_state, order_state, trade_state;
    bool has_new_data = false;
    auto last_write = std::chrono::steady_clock::now();

    while (g_running) {
        FramePtr frame = reader->getNextFrame();

        if (frame.get() == nullptr) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));

            // 有新数据且到达写入间隔才写
            auto now = std::chrono::steady_clock::now();
            if (has_new_data && std::chrono::duration_cast<std::chrono::seconds>(now - last_write).count() >= interval_sec) {
                append_monitor_data(output_path, tick_state, order_state, trade_state);
                has_new_data = false;
                last_write = now;
            }
            continue;
        }

        short msg_type = frame->getMsgType();
        long nano = frame->getNano();
        void* data = frame->getData();

        if (msg_type == MSG_TYPE_L2_TICK) {
            KyStdSnpType* md = (KyStdSnpType*)data;
            tick_state.journal_nano = nano;
            tick_state.exchange_time = md->Time;
            has_new_data = true;
        }
        else if (msg_type == MSG_TYPE_L2_ORDER) {
            KyStdOrderType* md = (KyStdOrderType*)data;
            order_state.journal_nano = nano;
            order_state.exchange_time = md->Time;
            has_new_data = true;
        }
        else if (msg_type == MSG_TYPE_L2_TRADE) {
            KyStdTradeType* md = (KyStdTradeType*)data;
            trade_state.journal_nano = nano;
            trade_state.exchange_time = md->Time;
            has_new_data = true;
        }

        // 有新数据且到达写入间隔才写
        auto now = std::chrono::steady_clock::now();
        if (has_new_data && std::chrono::duration_cast<std::chrono::seconds>(now - last_write).count() >= interval_sec) {
            append_monitor_data(output_path, tick_state, order_state, trade_state);
            has_new_data = false;
            last_write = now;
        }
    }

    // 退出前如有未写的数据，追加一次
    if (has_new_data) {
        append_monitor_data(output_path, tick_state, order_state, trade_state);
    }
    std::cout << "\n[monitor] stopped." << std::endl;

    return 0;
}
