/**
 * kbar_realtime — 实时 journal tick → 1min KBar CSV
 *
 * 通过 JournalReader 读取 tick journal（msg_type=61），
 * 实时合成 1min K 线，每分钟输出一个 CSV 文件。
 *
 * 用法: ./kbar_realtime [-o output_dir] [-d journal_dir] [-s now]
 */

#include "tick_bar_builder.h"
#include "data_types.h"

#include "JournalReader.h"
#include "Timer.h"

#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <csignal>
#include <atomic>
#include <cstdlib>
#include <ctime>
#include <cstdio>
#include <string>
#include <map>
#include <sys/stat.h>

static std::atomic<bool> g_running{true};
static void signal_handler(int) { g_running = false; }

static void mkdir_p(const std::string& path) {
    std::string tmp;
    for (char c : path) {
        tmp += c;
        if (c == '/') mkdir(tmp.c_str(), 0755);
    }
    mkdir(tmp.c_str(), 0755);
}

static std::string nano_to_date(int64_t nano) {
    time_t sec = nano / 1000000000LL;
    struct tm t;
    localtime_r(&sec, &t);
    char buf[32];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
    return buf;
}

static void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [-o output_dir] [-d journal_dir] [-s now]\n"
              << "  -o  Output directory (default: ./output)\n"
              << "  -d  Journal directory (default: /shared/kungfu/journal/user/)\n"
              << "  -s  Start mode: 'now' = only new data, default = from journal start\n";
}

// Per-minute CSV writer for completed bars
class CsvBarWriter {
public:
    explicit CsvBarWriter(const std::string& output_dir) : output_dir_(output_dir) {}

    void set_date(const std::string& date) {
        current_date_ = date;
        current_dir_ = output_dir_ + "/" + date;
        mkdir_p(current_dir_);
    }

    void write_bar(int32_t symbol, const MinuteBar& bar) {
        bars_[bar.bar_minute][symbol] = bar;
    }

    void flush_minute(int minute) {
        if (current_date_.empty()) return;
        auto it = bars_.find(minute);
        if (it == bars_.end()) return;
        write_csv(minute, it->second);
        bars_.erase(it);
    }

    // Flush all buffered minutes with bar_minute < before_minute
    void flush_up_to(int before_minute) {
        if (current_date_.empty()) return;
        auto it = bars_.begin();
        while (it != bars_.end() && it->first < before_minute) {
            write_csv(it->first, it->second);
            it = bars_.erase(it);
        }
    }

    // Flush all buffered minutes (for finalize)
    void flush_all() {
        if (current_date_.empty()) return;
        for (auto& [minute, sym_bars] : bars_) {
            write_csv(minute, sym_bars);
        }
        bars_.clear();
    }

private:
    std::string output_dir_;
    std::string current_date_;
    std::string current_dir_;
    std::map<int, std::map<int32_t, MinuteBar>> bars_;

    void write_csv(int minute, const std::map<int32_t, MinuteBar>& sym_bars) {
        char minute_str[8];
        snprintf(minute_str, sizeof(minute_str), "%04d", minute);

        std::string final_path = current_dir_ + "/kbar_" + minute_str + ".csv";
        std::string tmp_path = current_dir_ + "/.kbar_" + minute_str + ".csv";

        FILE* fp = fopen(tmp_path.c_str(), "w");
        if (!fp) {
            std::cerr << "[kbar] ERROR: cannot open " << tmp_path << std::endl;
            return;
        }

        fprintf(fp, "time,code,open,close,high,low,volume,money\n");
        std::string bar_time = format_bar_time(current_date_, minute);

        // Sort by symbol
        std::vector<int32_t> syms;
        for (auto& [sym, _] : sym_bars) syms.push_back(sym);
        std::sort(syms.begin(), syms.end());

        for (int32_t sym : syms) {
            auto& b = sym_bars.at(sym);
            double money_out = std::floor(b.money + 1e-6);
            fprintf(fp, "%s,%s,%.4f,%.4f,%.4f,%.4f,%.0f,%.0f\n",
                    bar_time.c_str(), format_symbol(sym).c_str(),
                    b.open, b.close, b.high, b.low,
                    b.volume, money_out);
        }

        fclose(fp);
        std::rename(tmp_path.c_str(), final_path.c_str());

        std::cout << "[kbar] " << minute_str
                  << " stocks=" << (int)sym_bars.size()
                  << " → " << final_path << std::endl;
    }

};

int main(int argc, const char* argv[]) {
    using namespace kungfu::yijinjing;

    std::string output_dir = "./output";
    std::string journal_dir = "/shared/kungfu/journal/user/";
    bool start_from_now = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-o" && i + 1 < argc) {
            output_dir = argv[++i];
        } else if (arg == "-d" && i + 1 < argc) {
            journal_dir = argv[++i];
        } else if (arg == "-s" && i + 1 < argc) {
            std::string mode = argv[++i];
            if (mode == "now") start_from_now = true;
        } else if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        }
    }

    mkdir_p(output_dir);

    // Initialize JournalReader (subscribe all 3 channels, filter tick in processing)
    std::string reader_name = "kbar_rt_" + parseNano(getNanoTime(), "%H%M%S");
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
    std::cout << "[init] start:   " << (start_from_now ? "now" : "beginning") << std::endl;

    JournalReaderPtr reader = JournalReader::createReaderWithSys(
        dirs, jnames, start_time, reader_name);

    std::cout << "[init] connected to Paged, waiting for data..." << std::endl;

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    CsvBarWriter csv_writer(output_dir);
    TickBarBuilder builder;
    bool has_data = false;  // 是否收到过有效 tick

    // callback 只负责缓冲 bar，不做 flush
    builder.set_bar_callback([&](int32_t symbol, const MinuteBar& bar) {
        csv_writer.write_bar(symbol, bar);
    });

    // Flush 策略：按秒级 exchange_time 变化触发。
    // HHMM 跨分钟时记下 pending_flush_bar，下一次 HHMMSS 变化时执行 flush
    // （此时跨分钟那一秒的全部 tick 已处理完，bar 已通过 callback 缓冲，延迟 ~3 秒���
    int32_t last_exch_sec = -1;   // 上一条 tick 的 HHMMSS
    int pending_flush_bar = -1;

    // finalize：关闭 callback → finalize（不填充 gap）→ 收集 bar → 写出
    auto do_finalize = [&]() {
        builder.set_bar_callback(nullptr);
        builder.finalize_all_current_only();
        for (int32_t sym : builder.get_symbols()) {
            for (auto& bar : builder.get_bars(sym)) {
                csv_writer.write_bar(sym, bar);
            }
        }
        csv_writer.flush_all();
    };

    std::string current_date;
    long tick_count = 0;

    // 空闲超时：15:05 后不再有有效 bar 产出，60 秒无新 tick 则 finalize + 退出
    using SteadyClock = std::chrono::steady_clock;
    auto last_tick_time = SteadyClock::now();
    constexpr int IDLE_TIMEOUT_SEC = 60;

    while (g_running) {
        FramePtr frame = reader->getNextFrame();
        if (frame.get() == nullptr) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            // 空闲超时检查：有数据被处理过且超过 60 秒无新 tick → finalize 并退出
            if (has_data) {
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                    SteadyClock::now() - last_tick_time).count();
                if (elapsed >= IDLE_TIMEOUT_SEC) {
                    std::cout << "[idle] " << elapsed << "s no data, finalizing and exiting..." << std::endl;
                    do_finalize();
                    std::cout << "\n[done] total ticks: " << tick_count << std::endl;
                    return 0;
                }
            }
            continue;
        }

        short msg_type = frame->getMsgType();
        if (msg_type != MSG_TYPE_L2_TICK) continue;

        auto* md = (KyStdSnpType*)frame->getData();
        int64_t extra_nano = frame->getExtraNano();
        int64_t nano = (extra_nano > 0) ? extra_nano : frame->getNano();

        // Date tracking
        std::string date = nano_to_date(nano);
        if (date != current_date) {
            current_date = date;
            csv_writer.set_date(current_date);
            std::cout << "[date] " << current_date << std::endl;
        }

        last_tick_time = SteadyClock::now();
        has_data = true;

        // Extract prev_day_close from tick PreClose field (only first time per symbol)
        float preclose = (float)((double)md->PreClose / 10000.0);
        if (preclose > 0)
            builder.set_prev_day_close_if_new(md->Symbol, preclose);

        builder.push_tick(md->Symbol, md->Time, md->Price, md->High, md->Low,
                          md->AccVolume, md->AccTurnover);
        tick_count++;

        if (tick_count % 1000000 == 0) {
            std::cout << "[progress] " << tick_count << " ticks processed" << std::endl;
        }

        // 秒级 exchange_time 变化 → 上一秒的全部 tick 已处理完
        int32_t exch_sec = md->Time / 1000;  // HHMMSSmmm → HHMMSS
        if (exch_sec != last_exch_sec) {
            // flush 上次 HHMM 跨分钟时记下的 bar
            if (pending_flush_bar > 0) {
                csv_writer.flush_up_to(pending_flush_bar + 1);
                pending_flush_bar = -1;
            }
            // 检查 HHMM 是否跨分钟
            if (last_exch_sec >= 0) {
                int old_hhmm = last_exch_sec / 10000;
                int new_hhmm = exch_sec / 10000;
                if (new_hhmm > old_hhmm) {
                    pending_flush_bar = StockBarAccumulator::next_minute(old_hhmm);
                }
            }
            last_exch_sec = exch_sec;
        }

        // 收到 exchange_time > 15:05:00 的 tick，说明有效数据已全部到齐，finalize 并退出
        if (md->Time > 150500000) {
            std::cout << "[eod] received tick with Time=" << md->Time
                      << " (past 15:05), finalizing and exiting..." << std::endl;
            do_finalize();
            std::cout << "\n[done] total ticks: " << tick_count << std::endl;
            return 0;
        }
    }

    // Finalize on exit (SIGINT/SIGTERM)
    do_finalize();

    std::cout << "\n[done] total ticks: " << tick_count << std::endl;
    return 0;
}
