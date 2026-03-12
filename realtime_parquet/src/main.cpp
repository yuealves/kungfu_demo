/**
 * realtime_parquet — 实时行情数据按分钟写 Parquet
 *
 * 从 Paged 服务实时读取 L2 行情 journal，每分钟写一组 Parquet 文件
 * （tick/order/trade 各一个）。Row Group 预压缩：数据到达时增量构建
 * Arrow RecordBatch，满 ROW_GROUP_SIZE 即写出，分钟边界只需 flush 残余。
 *
 * 用法: ./realtime_parquet [-o output_dir] [-d journal_dir]
 */

#include "minute_writer.h"
#include "data_types.h"

#include "JournalReader.h"
#include "Timer.h"

#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <csignal>
#include <atomic>
#include <ctime>
#include <string>
#include <sys/stat.h>

static std::atomic<bool> g_running{true};
static void signal_handler(int) { g_running = false; }

// 从纳秒时间戳提取 HHMM 分钟数（带缓存，避免每帧都调 localtime_r）
static int64_t g_cached_minute_end_nano = 0;
static int g_cached_hhmm = -1;

static int nano_to_hhmm(int64_t nano) {
    if (nano < g_cached_minute_end_nano) {
        return g_cached_hhmm;
    }
    time_t sec = nano / 1000000000LL;
    struct tm t;
    localtime_r(&sec, &t);
    g_cached_hhmm = t.tm_hour * 100 + t.tm_min;
    // 计算下一分钟起点的 nano，作为缓存失效边界
    t.tm_sec = 0;
    t.tm_min += 1;
    g_cached_minute_end_nano = static_cast<int64_t>(mktime(&t)) * 1000000000LL;
    return g_cached_hhmm;
}

// 从纳秒时间戳提取 YYYYMMDD 日期字符串
static std::string nano_to_date(int64_t nano) {
    time_t sec = nano / 1000000000LL;
    struct tm t;
    localtime_r(&sec, &t);
    char buf[16];
    snprintf(buf, sizeof(buf), "%04d%02d%02d", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
    return buf;
}

// 递归创建目录
static void mkdir_p(const std::string& path) {
    std::string tmp;
    for (char c : path) {
        tmp += c;
        if (c == '/') {
            mkdir(tmp.c_str(), 0755);
        }
    }
    mkdir(tmp.c_str(), 0755);
}

static void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [-o output_dir] [-d journal_dir]" << std::endl;
    std::cerr << "  -o  Output directory (default: ./output)" << std::endl;
    std::cerr << "  -d  Journal directory (default: /shared/kungfu/journal/user/)" << std::endl;
}

// 每种消息类型独立追踪分钟，避免 tick/order/trade 交织导致反复切换
struct ChannelCtx {
    const char* name;
    MinuteWriter writer;
    int current_minute = -1;
    long total_rows = 0;

    ChannelCtx(const char* n, MinuteWriter::Type t) : name(n), writer(t) {}
};

int main(int argc, const char* argv[])
{
    using namespace kungfu::yijinjing;

    std::string output_dir = "./output";
    std::string journal_dir = "/shared/kungfu/journal/user/";

    // 简单参数解析
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-o" && i + 1 < argc) {
            output_dir = argv[++i];
        } else if (arg == "-d" && i + 1 < argc) {
            journal_dir = argv[++i];
        } else if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        }
    }

    // 初始化 JournalReader
    std::string reader_name = "rt_parquet_" + parseNano(getNanoTime(), "%H%M%S");

    std::vector<std::string> dirs = {journal_dir, journal_dir, journal_dir};
    std::vector<std::string> jnames = {
        "insight_stock_tick_data",
        "insight_stock_order_data",
        "insight_stock_trade_data",
    };

    std::cout << "[init] reader: " << reader_name << std::endl;
    std::cout << "[init] journal: " << journal_dir << std::endl;
    std::cout << "[init] output:  " << output_dir << std::endl;

    long start_time = 0;
    JournalReaderPtr reader = JournalReader::createReaderWithSys(
        dirs, jnames, start_time, reader_name);

    std::cout << "[init] connected to Paged, waiting for data..." << std::endl;

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // 3 个独立通道，各自追踪分钟
    ChannelCtx channels[3] = {
        {"tick",  MinuteWriter::TICK},
        {"order", MinuteWriter::ORDER},
        {"trade", MinuteWriter::TRADE},
    };

    std::string current_date;
    std::string current_dir;

    auto ensure_dir = [&](int64_t nano) {
        std::string date = nano_to_date(nano);
        if (date != current_date) {
            current_date = date;
            current_dir = output_dir + "/" + current_date;
            mkdir_p(current_dir);
            std::cout << "[dir] " << current_dir << std::endl;
        }
    };

    auto rotate_channel = [&](ChannelCtx& ch, int minute, int64_t nano) {
        // 关闭当前文件
        if (ch.current_minute >= 0) {
            auto t0 = std::chrono::steady_clock::now();
            ch.writer.close();
            auto t1 = std::chrono::steady_clock::now();
            auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

            char ms[8];
            snprintf(ms, sizeof(ms), "%04d", ch.current_minute);
            std::cout << "[flush] " << ch.name << " " << ms
                      << " latency=" << latency_ms << "ms"
                      << " rows=" << ch.writer.row_count()
                      << std::endl;

            ch.total_rows += ch.writer.row_count();
        }

        // 打开新文件
        ensure_dir(nano);
        char minute_str[8];
        snprintf(minute_str, sizeof(minute_str), "%04d", minute);
        ch.writer.open(current_dir + "/" + ch.name + "_" + minute_str + ".parquet");
        ch.current_minute = minute;
    };

    while (g_running) {
        FramePtr frame = reader->getNextFrame();

        if (frame.get() == nullptr) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            continue;
        }

        short msg_type = frame->getMsgType();
        void* data = frame->getData();
        long extra_nano = frame->getExtraNano();
        int64_t nano = (extra_nano > 0) ? extra_nano : frame->getNano();

        // 分发到对应通道
        int idx;
        if (msg_type == MSG_TYPE_L2_TICK) {
            idx = 0;
        } else if (msg_type == MSG_TYPE_L2_ORDER) {
            idx = 1;
        } else if (msg_type == MSG_TYPE_L2_TRADE) {
            idx = 2;
        } else {
            continue;
        }

        // 用 nano_timestamp 判断分钟切换（nano 严格递增，不受 exchange_time 乱序影响）
        // nano 在线上 = frame->getNano()（写入时刻），replay = extra_nano（原始时刻）
        int minute = nano_to_hhmm(nano);
        auto& ch = channels[idx];

        if (minute != ch.current_minute) {
            rotate_channel(ch, minute, nano);
        }

        // 写入数据
        if (idx == 0) {
            ch.writer.append_tick((KyStdSnpType*)data, nano);
        } else if (idx == 1) {
            ch.writer.append_order((KyStdOrderType*)data, nano);
        } else {
            ch.writer.append_trade((KyStdTradeType*)data, nano);
        }
    }

    // 退出时 flush 所有通道
    for (auto& ch : channels) {
        if (ch.current_minute >= 0) {
            ch.writer.close();
            ch.total_rows += ch.writer.row_count();
        }
    }

    std::cout << "\n[done] total tick=" << channels[0].total_rows
              << " order=" << channels[1].total_rows
              << " trade=" << channels[2].total_rows
              << std::endl;

    return 0;
}
