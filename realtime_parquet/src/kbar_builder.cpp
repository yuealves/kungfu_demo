#include "kbar_builder.h"

#include <iostream>
#include <iomanip>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <sys/stat.h>

// ---------------------------------------------------------------------------
// Static helpers
// ---------------------------------------------------------------------------

int KBarBuilder::exch_time_to_hhmm(int32_t exch_time) {
    int hh = exch_time / 10000000;
    int mm = (exch_time / 100000) % 100;
    return hh * 100 + mm;
}

int KBarBuilder::next_minute(int hhmm) {
    int h = hhmm / 100;
    int m = hhmm % 100 + 1;
    if (m >= 60) { m = 0; h++; }
    return h * 100 + m;
}

void KBarBuilder::mkdir_p(const std::string& path) {
    std::string tmp;
    for (char c : path) {
        tmp += c;
        if (c == '/') mkdir(tmp.c_str(), 0755);
    }
    mkdir(tmp.c_str(), 0755);
}

std::string KBarBuilder::format_symbol(int32_t symbol) {
    char buf[24];
    if (symbol < 400000)
        snprintf(buf, sizeof(buf), "%06d.XSHE", symbol);
    else
        snprintf(buf, sizeof(buf), "%06d.XSHG", symbol);
    return buf;
}

std::string KBarBuilder::format_bar_time(const std::string& date, int hhmm) {
    // date is "YYYYMMDD", returns "YYYY-MM-DD HH:MM:00"
    char buf[32];
    snprintf(buf, sizeof(buf), "%.4s-%.2s-%.2s %02d:%02d:00",
             date.c_str(), date.c_str() + 4, date.c_str() + 6,
             hhmm / 100, hhmm % 100);
    return buf;
}

int64_t KBarBuilder::steady_now_ns() {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        now.time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// Public interface (main thread)
// ---------------------------------------------------------------------------

KBarBuilder::KBarBuilder(const std::string& output_dir)
    : output_dir_(output_dir)
{
    thread_ = std::thread(&KBarBuilder::thread_loop, this);
}

KBarBuilder::~KBarBuilder() {
    stop();
}

void KBarBuilder::push_trade(const KyStdTradeType* md, int64_t nano) {
    TradeEvent evt;
    evt.data = *md;
    evt.nano = nano;
    if (!queue_.push(evt)) {
        std::cerr << "[kbar] WARNING: queue full, dropping trade" << std::endl;
    }
}

void KBarBuilder::set_date(const std::string& date_str) {
    pending_date_ = date_str;
    date_changed_.store(true, std::memory_order_release);
}

void KBarBuilder::stop() {
    running_.store(false, std::memory_order_release);
    if (thread_.joinable()) thread_.join();
}

// ---------------------------------------------------------------------------
// Thread loop
// ---------------------------------------------------------------------------

void KBarBuilder::thread_loop() {
    TradeEvent evt;

    while (running_.load(std::memory_order_acquire)) {
        // Check date change first (before draining queue)
        if (date_changed_.load(std::memory_order_acquire)) {
            apply_date_change();
            date_changed_.store(false, std::memory_order_release);
        }

        // Drain queue
        bool got_any = false;
        while (queue_.pop(evt)) {
            on_trade(evt);
            got_any = true;
        }

        if (!got_any) {
            check_timeout();
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }

    // Drain remaining events after stop
    int drain_count = 0;
    while (queue_.pop(evt)) {
        on_trade(evt);
        drain_count++;
    }

    std::cout << "[kbar] stopping: drained=" << drain_count
              << " pending=" << minute_bars_.size() << " minutes"
              << " active=" << active_stocks_.size() << " stocks"
              << std::endl;

    // Flush all pending bars
    flush_remaining();
}

// ---------------------------------------------------------------------------
// on_trade — 按 exchange_time 分拣到对应分钟的 bar
// ---------------------------------------------------------------------------

void KBarBuilder::on_trade(const TradeEvent& evt) {
    const auto* md = &evt.data;

    // Filter invalid trades
    if (md->Price <= 0 || md->Volume <= 0) return;

    int trade_minute = exch_time_to_hhmm(md->Time);

    // Update channel watermark (max exchange_time per channel)
    auto& wm = channel_watermarks_[md->Channel];
    if (md->Time > wm) wm = md->Time;

    // Accumulate into the correct minute's bar bucket
    double price = md->Price;
    double volume = md->Volume;

    auto& bar = minute_bars_[trade_minute][md->Symbol];
    if (bar.trade_count == 0) {
        bar.open = price;
        bar.high = price;
        bar.low = price;
    } else {
        if (price > bar.high) bar.high = price;
        if (price < bar.low) bar.low = price;
    }
    bar.close = price;
    bar.volume += volume;
    bar.money += price * volume;
    bar.trade_count++;
    if (bar.first_trade_time == 0) bar.first_trade_time = md->Time;
    bar.last_trade_time = md->Time;

    // Update wall clock for timeout detection
    last_trade_steady_ns_ = steady_now_ns();

    // Try to flush completed minutes
    try_flush();
}

// ---------------------------------------------------------------------------
// Watermark & flush logic
// ---------------------------------------------------------------------------

int KBarBuilder::compute_watermark_minute() {
    if (channel_watermarks_.empty()) return -1;
    int min_hhmm = 9999;
    for (auto& kv : channel_watermarks_) {
        int hhmm = exch_time_to_hhmm(kv.second);
        if (hhmm < min_hhmm) min_hhmm = hhmm;
    }
    return min_hhmm;
}

void KBarBuilder::try_flush() {
    int wm = compute_watermark_minute();
    if (wm < 0) return;

    // Determine the first minute to consider flushing
    int start;
    if (last_flushed_minute_ < 0) {
        if (minute_bars_.empty()) return;
        start = minute_bars_.begin()->first;
    } else {
        start = next_minute(last_flushed_minute_);
    }

    // Flush all minutes strictly before the watermark
    // (watermark minute itself may still receive late trades from slower channels)
    for (int m = start; m < wm; m = next_minute(m)) {
        auto it = minute_bars_.find(m);
        if (it != minute_bars_.end()) {
            flush_bars(m, it->second);
            minute_bars_.erase(it);
        } else if (!active_stocks_.empty()) {
            // No trades this minute — pure carry-forward
            BarMap empty;
            flush_bars(m, empty);
        }
        last_flushed_minute_ = m;
    }
}

void KBarBuilder::flush_remaining() {
    // Flush all minutes still in minute_bars_, in order
    for (auto it = minute_bars_.begin(); it != minute_bars_.end(); ) {
        // Fill carry-forward gaps between last_flushed and this minute
        int target = it->first;
        int m = (last_flushed_minute_ < 0) ? target : next_minute(last_flushed_minute_);
        for (; m < target; m = next_minute(m)) {
            if (!active_stocks_.empty()) {
                BarMap empty;
                flush_bars(m, empty);
            }
            last_flushed_minute_ = m;
        }

        flush_bars(it->first, it->second);
        last_flushed_minute_ = it->first;
        it = minute_bars_.erase(it);
    }
}

void KBarBuilder::flush_bars(int minute, BarMap& bars) {
    if (current_date_.empty()) return;

    int traded = (int)bars.size();
    int carried = 0;
    double total_volume = 0;
    double total_money = 0;
    int total_trades = 0;

    // Compute stats from real (traded) bars
    for (auto& kv : bars) {
        total_volume += kv.second.volume;
        total_money += kv.second.money;
        total_trades += kv.second.trade_count;
    }

    // Add carry-forward bars for active stocks with no trade this minute
    for (auto& kv : active_stocks_) {
        if (bars.find(kv.first) == bars.end()) {
            StockBar bar;
            bar.open = kv.second;
            bar.close = kv.second;
            bar.high = kv.second;
            bar.low = kv.second;
            // volume, money, first/last_trade_time, trade_count = 0 by default
            bars[kv.first] = bar;
            carried++;
        }
    }

    write_csv(minute, bars);

    // Update active_stocks_ from flushed bars (for next minute's carry-forward)
    for (auto& kv : bars) {
        active_stocks_[kv.first] = kv.second.close;
    }

    // Watermark snapshot for logging
    char wm_str[64] = "n/a";
    if (!channel_watermarks_.empty()) {
        int wm_min = 9999;
        int wm_max = 0;
        for (auto& kv : channel_watermarks_) {
            int hhmm = exch_time_to_hhmm(kv.second);
            if (hhmm < wm_min) wm_min = hhmm;
            if (hhmm > wm_max) wm_max = hhmm;
        }
        snprintf(wm_str, sizeof(wm_str), "%04d..%04d/%dc",
                 wm_min, wm_max, (int)channel_watermarks_.size());
    }

    int end_min = next_minute(minute);
    char ms[8];
    snprintf(ms, sizeof(ms), "%04d", end_min);
    // pending = how many minutes still buffered in minute_bars_
    std::cout << "[kbar] " << ms
              << " stocks=" << (traded + carried)
              << " traded=" << traded
              << " carried=" << carried
              << " trades=" << total_trades
              << " vol=" << (long long)total_volume
              << " money=" << std::fixed << std::setprecision(0) << total_money
              << " wm=" << wm_str
              << " pending=" << minute_bars_.size()
              << std::endl;
}

void KBarBuilder::check_timeout() {
    if (minute_bars_.empty() && active_stocks_.empty()) return;
    if (last_trade_steady_ns_ == 0) return;

    int64_t now = steady_now_ns();
    if (now - last_trade_steady_ns_ > TIMEOUT_NS) {
        int64_t idle_sec = (now - last_trade_steady_ns_) / 1000000000LL;
        std::cout << "[kbar] timeout after " << idle_sec << "s idle"
                  << " pending=" << minute_bars_.size() << " minutes"
                  << std::endl;
        flush_remaining();
        last_trade_steady_ns_ = 0;
    }
}

// ---------------------------------------------------------------------------
// CSV output
// ---------------------------------------------------------------------------

void KBarBuilder::write_csv(int minute, const BarMap& bars) {
    int end_min = next_minute(minute);

    char minute_str[8];
    snprintf(minute_str, sizeof(minute_str), "%04d", end_min);

    std::string final_path = current_dir_ + "/kbar_" + minute_str + ".csv";
    std::string tmp_path = current_dir_ + "/.kbar_" + minute_str + ".csv";

    FILE* fp = fopen(tmp_path.c_str(), "w");
    if (!fp) {
        std::cerr << "[kbar] ERROR: cannot open " << tmp_path << std::endl;
        return;
    }

    fprintf(fp, "time,symbol,open,close,high,low,volume,money,first_trade_time,last_trade_time\n");

    std::string bar_time = format_bar_time(current_date_, end_min);

    // Sort by symbol for deterministic output
    std::vector<int32_t> symbols;
    symbols.reserve(bars.size());
    for (auto& kv : bars) {
        symbols.push_back(kv.first);
    }
    std::sort(symbols.begin(), symbols.end());

    for (int32_t sym : symbols) {
        auto& bar = bars.at(sym);
        fprintf(fp, "%s,%s,%.2f,%.2f,%.2f,%.2f,%.0f,%.2f,%d,%d\n",
                bar_time.c_str(),
                format_symbol(sym).c_str(),
                bar.open, bar.close, bar.high, bar.low,
                bar.volume, bar.money,
                bar.first_trade_time, bar.last_trade_time);
    }

    fclose(fp);

    // Atomic rename
    if (std::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
        std::cerr << "[kbar] WARN: rename failed: " << tmp_path
                  << " -> " << final_path << std::endl;
    }
}

// ---------------------------------------------------------------------------
// Date change handling
// ---------------------------------------------------------------------------

void KBarBuilder::apply_date_change() {
    // Flush pending bars for previous day
    flush_remaining();

    // Clear all state
    minute_bars_.clear();
    channel_watermarks_.clear();
    active_stocks_.clear();
    last_flushed_minute_ = -1;
    last_trade_steady_ns_ = 0;

    // Update date and directory
    current_date_ = pending_date_;
    current_dir_ = output_dir_ + "/" + current_date_;
    mkdir_p(current_dir_);

    std::cout << "[kbar] date=" << current_date_ << " dir=" << current_dir_ << std::endl;
}
