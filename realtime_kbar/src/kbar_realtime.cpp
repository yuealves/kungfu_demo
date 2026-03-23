/**
 * kbar_realtime — 实时 journal tick → 1min KBar CSV / patch CSV
 *
 * 本版本采用“按分钟保存所有股票最新 bar 快照”的发布层设计：
 * - preliminary 低延迟发布
 * - 后续迟到数据可对任意已发布分钟持续输出 patch
 * - 对同一股票的乱序 tick，保留全天 tick 历史并重放该股票，修正所有受影响分钟
 */

#include "tick_bar_builder.h"
#include "data_types.h"

#include "JournalReader.h"
#include "Timer.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

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

static bool is_sh_symbol(int32_t symbol) {
    return symbol >= 400000 && symbol < 700000;
}

static bool is_excluded_non_target_symbol(int32_t symbol) {
    return symbol >= 200000 && symbol < 202000;
}

static bool is_supported_symbol(int32_t symbol) {
    return symbol >= 0 && symbol < 700000 && !is_excluded_non_target_symbol(symbol);
}

static int hhmm_from_time(int32_t time_int) {
    int hh = time_int / 10000000;
    int mm = (time_int / 100000) % 100;
    return hh * 100 + mm;
}

static int hhmmss_from_time(int32_t time_int) {
    return time_int / 1000;
}




static int sec01_flush_second_for_bar(int hhmm) {
    if (hhmm >= 931 && hhmm <= 1130) return 1;
    if (hhmm >= 1301 && hhmm <= 1500) return 1;
    return -1;
}


static bool bar_equal(const MinuteBar& a, const MinuteBar& b) {
    return a.bar_minute == b.bar_minute &&
           std::fabs(a.open - b.open) < 1e-6f &&
           std::fabs(a.close - b.close) < 1e-6f &&
           std::fabs(a.high - b.high) < 1e-6f &&
           std::fabs(a.low - b.low) < 1e-6f &&
           std::fabs(a.volume - b.volume) < 1e-6 &&
           std::fabs(a.money - b.money) < 1e-6;
}

struct RawTick {
    int32_t time = 0;
    float price_raw = 0;
    float high_raw = 0;
    float low_raw = 0;
    int64_t acc_volume = 0;
    int64_t acc_turnover = 0;
    uint64_t seq = 0;
};

enum class PublishStatus {
    BUILDING,
    PRELIM_PUBLISHED,
};

struct MinutePublishState {
    PublishStatus status = PublishStatus::BUILDING;
    int version = 0;
    int patch_count = 0;
    std::string trigger;
    int64_t prelim_publish_ns = 0;
    std::map<int32_t, MinuteBar> published_snapshot;
    std::unordered_set<int32_t> dirty_symbols;
    int64_t dirty_since_ns = 0;
};

class CsvPublishWriter {
public:
    explicit CsvPublishWriter(const std::string& output_dir) : output_dir_(output_dir) {}

    void set_date(const std::string& date) {
        current_date_ = date;
        current_dir_ = output_dir_ + "/" + date;
        mkdir_p(current_dir_);
    }

    void write_preliminary(int minute, const std::map<int32_t, MinuteBar>& sym_bars) {
        write_csv(minute, sym_bars, /*version=*/1, /*is_patch=*/false);
    }

    void write_patch(int minute, int version, const std::map<int32_t, MinuteBar>& sym_bars) {
        write_csv(minute, sym_bars, version, /*is_patch=*/true);
    }

private:
    std::string output_dir_;
    std::string current_date_;
    std::string current_dir_;

    void write_csv(int minute, const std::map<int32_t, MinuteBar>& sym_bars,
                   int version, bool is_patch)
    {
        if (current_date_.empty()) return;

        char minute_str[8];
        snprintf(minute_str, sizeof(minute_str), "%04d", minute);

        std::string final_path;
        std::string tmp_path;
        if (is_patch) {
            final_path = current_dir_ + "/kbar_patch_" + minute_str + "_v" + std::to_string(version) + ".csv";
            tmp_path = current_dir_ + "/.kbar_patch_" + minute_str + "_v" + std::to_string(version) + ".csv";
        } else {
            final_path = current_dir_ + "/kbar_" + minute_str + ".csv";
            tmp_path = current_dir_ + "/.kbar_" + minute_str + ".csv";
        }

        FILE* fp = fopen(tmp_path.c_str(), "w");
        if (!fp) {
            std::cerr << "[kbar] ERROR: cannot open " << tmp_path << std::endl;
            return;
        }

        if (is_patch)
            fprintf(fp, "time,code,open,close,high,low,volume,money,version\n");
        else
            fprintf(fp, "time,code,open,close,high,low,volume,money\n");

        std::string bar_time = format_bar_time(current_date_, minute);
        for (const auto& [sym, b] : sym_bars) {
            double money_out = std::floor(b.money + 1e-6);
            if (is_patch) {
                fprintf(fp, "%s,%s,%.4f,%.4f,%.4f,%.4f,%.0f,%.0f,%d\n",
                        bar_time.c_str(), format_symbol(sym).c_str(),
                        b.open, b.close, b.high, b.low,
                        b.volume, money_out, version);
            } else {
                fprintf(fp, "%s,%s,%.4f,%.4f,%.4f,%.4f,%.0f,%.0f\n",
                        bar_time.c_str(), format_symbol(sym).c_str(),
                        b.open, b.close, b.high, b.low,
                        b.volume, money_out);
            }
        }

        fclose(fp);
        std::rename(tmp_path.c_str(), final_path.c_str());

        if (is_patch) {
            std::cout << "[minute_patch] minute=" << minute_str
                      << " version=" << version
                      << " dirty_symbols=" << (int)sym_bars.size()
                      << " path=" << final_path << std::endl;
        } else {
            std::cout << "[minute_publish] minute=" << minute_str
                      << " version=1"
                      << " stocks=" << (int)sym_bars.size()
                      << " path=" << final_path << std::endl;
        }
    }
};

class MinutePublishCoordinator {
public:
    void log_summary() const {
        std::cout << "[summary] preliminary_minutes=" << preliminary_publish_count_
                  << " patch_files=" << patch_file_count_
                  << " patched_symbols_total=" << patched_symbol_total_
                  << std::endl;
    }

    int preliminary_publish_count() const { return preliminary_publish_count_; }
    int patch_file_count() const { return patch_file_count_; }
    int patched_symbol_total() const { return patched_symbol_total_; }

    using SnapshotProvider = std::function<std::map<int32_t, MinuteBar>(int)>;

    explicit MinutePublishCoordinator(const std::string& output_dir)
        : writer_(output_dir) {}

    void set_date(const std::string& date) {
        current_date_ = date;
        writer_.set_date(date);
    }

    void set_snapshot_provider(SnapshotProvider provider) {
        snapshot_provider_ = std::move(provider);
    }

    void on_bar_upsert(int32_t symbol, const MinuteBar& bar, int64_t nano) {
        auto& latest = latest_by_minute_[bar.bar_minute];
        latest[symbol] = bar;

        auto& st = minute_states_[bar.bar_minute];
        if (st.status == PublishStatus::PRELIM_PUBLISHED) {
            auto it = st.published_snapshot.find(symbol);
            bool changed = (it == st.published_snapshot.end()) || !bar_equal(it->second, bar);
            if (changed) {
                st.dirty_symbols.insert(symbol);
                if (st.dirty_since_ns == 0) st.dirty_since_ns = nano;
            }
        }
    }

    void on_sh_tick(int32_t symbol, int32_t time_int, int64_t nano) {
        flush_due_patches(nano);
        if (!is_sh_symbol(symbol)) return;

        int hhmm = hhmm_from_time(time_int);
        int ss = hhmmss_from_time(time_int) % 100;
        int flush_second = sec01_flush_second_for_bar(hhmm);
        if (flush_second >= 0 && ss == flush_second) {
            publish_preliminary(hhmm, nano, "sec01_flush");
        }
    }

    void finalize_all() {
        for (auto& [minute, bars] : latest_by_minute_) {
            publish_preliminary(minute, 0, "finalize");
        }

        for (auto& [minute, st] : minute_states_) {
            if (st.status == PublishStatus::PRELIM_PUBLISHED && !st.dirty_symbols.empty()) {
                flush_patch(minute, st);
            }
        }
    }

private:
    static constexpr int64_t PATCH_FLUSH_NS = 1000000000LL;

    CsvPublishWriter writer_;
    SnapshotProvider snapshot_provider_;
    std::string current_date_;
    std::map<int, std::map<int32_t, MinuteBar>> latest_by_minute_;
    std::map<int, MinutePublishState> minute_states_;
    int preliminary_publish_count_ = 0;
    int patch_file_count_ = 0;
    int patched_symbol_total_ = 0;

    void flush_due_patches(int64_t nano) {
        for (auto& [minute, st] : minute_states_) {
            if (st.status != PublishStatus::PRELIM_PUBLISHED) continue;
            if (st.dirty_symbols.empty()) continue;
            if (st.dirty_since_ns == 0) continue;
            if (nano - st.dirty_since_ns < PATCH_FLUSH_NS) continue;
            flush_patch(minute, st);
        }
    }

    void publish_preliminary(int minute, int64_t nano, const std::string& trigger) {
        auto& st = minute_states_[minute];
        if (st.status == PublishStatus::PRELIM_PUBLISHED) return;

        std::map<int32_t, MinuteBar> snapshot;
        if (snapshot_provider_) snapshot = snapshot_provider_(minute);
        if (snapshot.empty()) {
            auto it = latest_by_minute_.find(minute);
            if (it != latest_by_minute_.end()) snapshot = it->second;
        }
        if (snapshot.empty()) return;

        latest_by_minute_[minute] = snapshot;
        writer_.write_preliminary(minute, snapshot);
        st.status = PublishStatus::PRELIM_PUBLISHED;
        st.version = 1;
        st.prelim_publish_ns = nano;
        st.trigger = trigger;
        st.published_snapshot = snapshot;
        preliminary_publish_count_ += 1;

        char minute_str[8];
        snprintf(minute_str, sizeof(minute_str), "%04d", minute);
        std::cout << "[minute_publish] minute=" << minute_str
                  << " status=preliminary"
                  << " version=1"
                  << " trigger=" << trigger
                  << " stocks_published=" << (int)snapshot.size()
                  << " preliminary_minutes_total=" << preliminary_publish_count_
                  << std::endl;
    }

    void flush_patch(int minute, MinutePublishState& st) {
        std::map<int32_t, MinuteBar> patch_bars;
        auto latest_it = latest_by_minute_.find(minute);
        if (latest_it == latest_by_minute_.end()) return;

        for (int32_t sym : st.dirty_symbols) {
            auto bar_it = latest_it->second.find(sym);
            if (bar_it == latest_it->second.end()) continue;
            patch_bars[sym] = bar_it->second;
            st.published_snapshot[sym] = bar_it->second;
        }

        if (patch_bars.empty()) {
            st.dirty_symbols.clear();
            st.dirty_since_ns = 0;
            return;
        }

        st.version += 1;
        st.patch_count += 1;
        patch_file_count_ += 1;
        patched_symbol_total_ += (int)patch_bars.size();
        writer_.write_patch(minute, st.version, patch_bars);
        std::cout << "[minute_patch] minute=" << minute
                  << " version=" << st.version
                  << " dirty_symbols=" << (int)patch_bars.size()
                  << " patch_files_total=" << patch_file_count_
                  << std::endl;
        st.dirty_symbols.clear();
        st.dirty_since_ns = 0;
    }
};

struct SymbolRealtimeState {
    StockBarAccumulator acc;
    bool has_prev_day_close = false;
    float prev_day_close = 0;
    std::vector<RawTick> history;
    std::map<int, MinuteBar> bars_by_minute;
    int32_t max_time_seen = -1;
};

static void drain_accumulator(StockBarAccumulator& acc,
                              std::vector<MinuteBar>& out_bars) {
    MinuteBar bar;
    while (acc.pop_bar(bar)) {
        out_bars.push_back(bar);
    }
}

template <typename Callback>
static void rebuild_symbol_state(SymbolRealtimeState& state, Callback&& on_bar) {
    std::sort(state.history.begin(), state.history.end(), [](const RawTick& a, const RawTick& b) {
        if (a.time != b.time) return a.time < b.time;
        return a.seq < b.seq;
    });

    StockBarAccumulator rebuilt;
    if (state.has_prev_day_close) rebuilt.set_prev_day_close(state.prev_day_close);

    std::map<int, MinuteBar> new_bars;
    for (const auto& tick : state.history) {
        rebuilt.push_tick(tick.time, tick.price_raw, tick.high_raw, tick.low_raw,
                          tick.acc_volume, tick.acc_turnover);
        std::vector<MinuteBar> emitted;
        drain_accumulator(rebuilt, emitted);
        for (const auto& bar : emitted) {
            new_bars[bar.bar_minute] = bar;
        }
    }

    std::set<int> all_minutes;
    for (const auto& [minute, _] : state.bars_by_minute) all_minutes.insert(minute);
    for (const auto& [minute, _] : new_bars) all_minutes.insert(minute);

    for (int minute : all_minutes) {
        auto old_it = state.bars_by_minute.find(minute);
        auto new_it = new_bars.find(minute);
        if (new_it == new_bars.end()) continue;
        if (old_it == state.bars_by_minute.end() || !bar_equal(old_it->second, new_it->second)) {
            on_bar(new_it->second);
        }
    }

    state.acc = std::move(rebuilt);
    state.bars_by_minute = std::move(new_bars);
    state.max_time_seen = state.history.empty() ? -1 : state.history.back().time;
}

template <typename Callback>
static void process_symbol_tick(SymbolRealtimeState& state,
                                const RawTick& tick,
                                Callback&& on_bar)
{
    state.history.push_back(tick);

    bool out_of_order = (state.max_time_seen >= 0 && tick.time < state.max_time_seen);
    if (out_of_order) {
        rebuild_symbol_state(state, std::forward<Callback>(on_bar));
        return;
    }

    state.acc.push_tick(tick.time, tick.price_raw, tick.high_raw, tick.low_raw,
                        tick.acc_volume, tick.acc_turnover);
    state.max_time_seen = std::max(state.max_time_seen, tick.time);

    std::vector<MinuteBar> emitted;
    drain_accumulator(state.acc, emitted);
    for (const auto& bar : emitted) {
        auto it = state.bars_by_minute.find(bar.bar_minute);
        if (it == state.bars_by_minute.end() || !bar_equal(it->second, bar)) {
            state.bars_by_minute[bar.bar_minute] = bar;
            on_bar(bar);
        }
    }
}

template <typename Callback>
static void finalize_symbol_state(SymbolRealtimeState& state, Callback&& on_bar) {
    state.acc.finalize_current_only();
    std::vector<MinuteBar> emitted;
    drain_accumulator(state.acc, emitted);
    for (const auto& bar : emitted) {
        auto it = state.bars_by_minute.find(bar.bar_minute);
        if (it == state.bars_by_minute.end() || !bar_equal(it->second, bar)) {
            state.bars_by_minute[bar.bar_minute] = bar;
            on_bar(bar);
        }
    }
}

template <typename SymbolMap>
static std::map<int32_t, MinuteBar> build_minute_snapshot(const SymbolMap& symbols, int minute) {
    std::map<int32_t, MinuteBar> snapshot;
    for (const auto& [symbol, state] : symbols) {
        auto it = state.bars_by_minute.find(minute);
        if (it != state.bars_by_minute.end()) {
            snapshot[symbol] = it->second;
            continue;
        }

        StockBarAccumulator acc_copy = state.acc;
        acc_copy.finalize_current_only();
        std::vector<MinuteBar> emitted;
        drain_accumulator(acc_copy, emitted);
        for (const auto& bar : emitted) {
            if (bar.bar_minute == minute) {
                snapshot[symbol] = bar;
                break;
            }
        }
    }
    return snapshot;
}

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

    MinutePublishCoordinator coordinator(output_dir);
    std::unordered_map<int32_t, SymbolRealtimeState> symbols;
    coordinator.set_snapshot_provider([&](int minute) {
        return build_minute_snapshot(symbols, minute);
    });
    bool has_data = false;
    std::string current_date;
    long tick_count = 0;
    uint64_t tick_seq = 0;
    long out_of_order_rebuild_count = 0;
    std::unordered_set<int32_t> out_of_order_symbols;

    auto do_finalize = [&]() {
        for (auto& [symbol, state] : symbols) {
            finalize_symbol_state(state, [&](const MinuteBar& bar) {
                coordinator.on_bar_upsert(symbol, bar, /*nano=*/0);
            });
        }
        coordinator.finalize_all();
        coordinator.log_summary();
        std::cout << "[summary] symbols_seen=" << symbols.size()
                  << " out_of_order_rebuilds=" << out_of_order_rebuild_count
                  << " out_of_order_symbols=" << out_of_order_symbols.size()
                  << std::endl;
    };

    using SteadyClock = std::chrono::steady_clock;
    auto last_tick_time = SteadyClock::now();
    constexpr int IDLE_TIMEOUT_SEC = 60;

    while (g_running) {
        FramePtr frame = reader->getNextFrame();
        if (frame.get() == nullptr) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
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
        if (!is_supported_symbol(md->Symbol)) continue;
        int64_t extra_nano = frame->getExtraNano();
        int64_t nano = (extra_nano > 0) ? extra_nano : frame->getNano();

        std::string date = nano_to_date(nano);
        if (date != current_date) {
            current_date = date;
            coordinator.set_date(current_date);
            std::cout << "[date] " << current_date << std::endl;
        }

        last_tick_time = SteadyClock::now();
        has_data = true;

        auto& state = symbols[md->Symbol];
        float preclose = (float)((double)md->PreClose / 10000.0);
        if (preclose > 0 && !state.has_prev_day_close) {
            state.has_prev_day_close = true;
            state.prev_day_close = preclose;
            state.acc.set_prev_day_close(preclose);
        }

        RawTick tick;
        tick.time = md->Time;
        tick.price_raw = md->Price;
        tick.high_raw = md->High;
        tick.low_raw = md->Low;
        tick.acc_volume = md->AccVolume;
        tick.acc_turnover = md->AccTurnover;
        tick.seq = ++tick_seq;

        bool out_of_order = (state.max_time_seen >= 0 && tick.time < state.max_time_seen);
        if (out_of_order) {
            out_of_order_rebuild_count += 1;
            out_of_order_symbols.insert(md->Symbol);
            if (out_of_order_rebuild_count <= 20 || out_of_order_rebuild_count % 100 == 0) {
                std::cout << "[rebuild] symbol=" << format_symbol(md->Symbol)
                          << " tick_time=" << tick.time
                          << " max_time_seen=" << state.max_time_seen
                          << " history_size=" << state.history.size() + 1
                          << " rebuild_count=" << out_of_order_rebuild_count
                          << std::endl;
            }
        }

        process_symbol_tick(state, tick, [&](const MinuteBar& bar) {
            coordinator.on_bar_upsert(md->Symbol, bar, nano);
        });
        coordinator.on_sh_tick(md->Symbol, md->Time, nano);

        tick_count++;
        if (tick_count % 1000000 == 0) {
            std::cout << "[progress] ticks=" << tick_count
                      << " symbols_seen=" << symbols.size()
                      << " preliminary_minutes=" << coordinator.preliminary_publish_count()
                      << " patch_files=" << coordinator.patch_file_count()
                      << " out_of_order_rebuilds=" << out_of_order_rebuild_count
                      << std::endl;
        }

        if (md->Time > 150500000) {
            std::cout << "[eod] received tick with Time=" << md->Time
                      << " (past 15:05), finalizing and exiting..." << std::endl;
            do_finalize();
            std::cout << "\n[done] total ticks: " << tick_count << std::endl;
            return 0;
        }
    }

    do_finalize();

    std::cout << "\n[done] total ticks: " << tick_count << std::endl;
    return 0;
}
