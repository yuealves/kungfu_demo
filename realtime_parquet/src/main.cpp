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

#include <arrow/util/thread_pool.h>

#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <csignal>
#include <atomic>
#include <cstdlib>
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
    std::cerr << "Usage: " << prog << " [-o output_dir] [-d journal_dir] [-s now]" << std::endl;
    std::cerr << "  -o  Output directory (default: ./output)" << std::endl;
    std::cerr << "  -d  Journal directory (default: /shared/kungfu/journal/user/)" << std::endl;
    std::cerr << "  -s  Start mode: 'now' = only new data, default = from journal start" << std::endl;
}

// 将 nano 时间戳中的时间部分转换为当天零点以来的毫秒数
static int64_t nano_to_ms_of_day(int64_t nano) {
    time_t sec = nano / 1000000000LL;
    struct tm t;
    localtime_r(&sec, &t);
    int64_t ms = t.tm_hour * 3600000LL + t.tm_min * 60000LL + t.tm_sec * 1000LL
               + (nano % 1000000000LL) / 1000000LL;
    return ms;
}

// 将 exchange_time (HHMMSSmmm) 转换为当天零点以来的毫秒数
static int64_t exch_time_to_ms_of_day(int32_t exch_time) {
    int mmm = exch_time % 1000;
    int ss  = (exch_time / 1000) % 100;
    int mm  = (exch_time / 100000) % 100;
    int hh  = exch_time / 10000000;
    return hh * 3600000LL + mm * 60000LL + ss * 1000LL + mmm;
}

// 每种消息类型独立追踪分钟，避免 tick/order/trade 交织导致反复切换
struct ChannelCtx {
    const char* name;
    MinuteWriter writer;
    int current_minute = -1;
    long total_rows = 0;
    int64_t last_nano = 0;        // 最后一条数据的 nano_timestamp（线上=getNano, replay=extra_nano）
    int64_t last_read_wall = 0;   // 读到最后一条数据时的墙上时间
    int32_t last_exch_time = 0;   // 最后一条数据的 exchange_time (HHMMSSmmm)
    int32_t last_symbol = 0;      // 最后一条数据的股票代码

    ChannelCtx(const char* n, MinuteWriter::Type t) : name(n), writer(t) {}
};

int main(int argc, const char* argv[])
{
    using namespace kungfu::yijinjing;

    std::string output_dir = "./output";
    std::string journal_dir = "/shared/kungfu/journal/user/";
    bool start_from_now = false;

    // 简单参数解析
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-o" && i + 1 < argc) {
            output_dir = argv[++i];
        } else if (arg == "-d" && i + 1 < argc) {
            journal_dir = argv[++i];
        } else if (arg == "-s" && i + 1 < argc) {
            std::string mode = argv[++i];
            if (mode == "now") {
                start_from_now = true;
            } else {
                std::cerr << "Unknown start mode: " << mode << " (use 'now')" << std::endl;
                return 1;
            }
        } else if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        }
    }

    // 限制 Arrow 全局线程池（默认 4，可通过 OMP_NUM_THREADS 覆盖）
    {
        int nthreads = 4;
        const char* env = std::getenv("OMP_NUM_THREADS");
        if (env) {
            int n = std::atoi(env);
            if (n > 0) nthreads = n;
        }
        auto st = arrow::SetCpuThreadPoolCapacity(nthreads);
        if (!st.ok()) {
            std::cerr << "[warn] Arrow thread pool: " << st.ToString() << std::endl;
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

    long start_time = start_from_now ? getNanoTime() : 0;

    std::cout << "[init] reader:  " << reader_name << std::endl;
    std::cout << "[init] journal: " << journal_dir << std::endl;
    std::cout << "[init] output:  " << output_dir << std::endl;
    std::cout << "[init] start:   " << (start_from_now ? "now (skip history)" : "beginning") << std::endl;
    std::cout << "[init] threads: " << arrow::GetCpuThreadPoolCapacity() << std::endl;

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
            ch.writer.close();
            // 落盘完成的墙上时间
            int64_t now_ns = getNanoTime();

            // L1: gateway 延迟 = nano_timestamp - exchange_time（journal 写入相对交易所时间的延迟）
            int64_t gateway_ms = nano_to_ms_of_day(ch.last_nano)
                               - exch_time_to_ms_of_day(ch.last_exch_time);

            // L2: 落盘延迟 = wall_clock_now - 读到最后一条数据的墙上时间
            //   实盘: 读到时刻 ≈ journal 写入时刻（mmap 轮询延迟可忽略）
            //   replay: 读到时刻 = realtime_parquet 实际处理的时间点
            int64_t disk_ms = (now_ns - ch.last_read_wall) / 1000000LL;

            // 格式化 nano 为 HH:MM:SS.mmm
            time_t last_sec = ch.last_nano / 1000000000LL;
            struct tm last_tm;
            localtime_r(&last_sec, &last_tm);
            int last_ms = (ch.last_nano % 1000000000LL) / 1000000;

            // 格式化 exchange_time (HHMMSSmmm) 为 HH:MM:SS.mmm
            int et = ch.last_exch_time;
            int et_h = et / 10000000, et_m = (et / 100000) % 100;
            int et_s = (et / 1000) % 100, et_ms = et % 1000;

            char ms[8];
            snprintf(ms, sizeof(ms), "%04d", ch.current_minute);
            char nano_str[16], exch_str[16];
            snprintf(nano_str, sizeof(nano_str), "%02d:%02d:%02d.%03d",
                     last_tm.tm_hour, last_tm.tm_min, last_tm.tm_sec, last_ms);
            snprintf(exch_str, sizeof(exch_str), "%02d:%02d:%02d.%03d",
                     et_h, et_m, et_s, et_ms);

            std::cout << "[flush] " << ch.name << " " << ms
                      << " rows=" << ch.writer.row_count()
                      << " last=" << ch.last_symbol
                      << " nano=" << nano_str
                      << " exch=" << exch_str
                      << " gw=" << gateway_ms << "ms"
                      << " disk=" << disk_ms << "ms"
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

        // 写入数据，同时追踪最后一条数据的时间戳
        ch.last_nano = nano;
        ch.last_read_wall = getNanoTime();
        if (idx == 0) {
            auto* md = (KyStdSnpType*)data;
            ch.last_exch_time = md->Time;
            ch.last_symbol = md->Symbol;
            ch.writer.append_tick(md, nano);
        } else if (idx == 1) {
            auto* md = (KyStdOrderType*)data;
            ch.last_exch_time = md->Time;
            ch.last_symbol = md->Symbol;
            ch.writer.append_order(md, nano);
        } else {
            auto* md = (KyStdTradeType*)data;
            ch.last_exch_time = md->Time;
            ch.last_symbol = md->Symbol;
            ch.writer.append_trade(md, nano);
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
