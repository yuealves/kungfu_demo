#pragma once

#include "data_types.h"

#include <string>
#include <map>
#include <unordered_map>
#include <vector>
#include <thread>
#include <atomic>
#include <cstdint>

struct TradeEvent {
    KyStdTradeType data;
    int64_t nano;
};

// Lock-free SPSC ring buffer (power-of-2 capacity)
template<size_t Cap>
class SPSCQueue {
    static_assert((Cap & (Cap - 1)) == 0, "Cap must be power of 2");
    TradeEvent buf_[Cap];
    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};
public:
    bool push(const TradeEvent& e) {
        size_t h = head_.load(std::memory_order_relaxed);
        size_t next = (h + 1) & (Cap - 1);
        if (next == tail_.load(std::memory_order_acquire)) return false;
        buf_[h] = e;
        head_.store(next, std::memory_order_release);
        return true;
    }
    bool pop(TradeEvent& e) {
        size_t t = tail_.load(std::memory_order_relaxed);
        if (t == head_.load(std::memory_order_acquire)) return false;
        e = buf_[t];
        tail_.store((t + 1) & (Cap - 1), std::memory_order_release);
        return true;
    }
};

struct StockBar {
    double open = 0, close = 0, high = 0, low = 0;
    double volume = 0;
    double money = 0;
    int32_t first_trade_time = 0;  // HHMMSSmmm, 0 if no trade
    int32_t last_trade_time = 0;
    int trade_count = 0;
};

using BarMap = std::unordered_map<int32_t, StockBar>;

class KBarBuilder {
public:
    explicit KBarBuilder(const std::string& output_dir);
    ~KBarBuilder();

    void push_trade(const KyStdTradeType* md, int64_t nano);  // main thread
    void set_date(const std::string& date_str);                // main thread
    void stop();                                                // main thread, blocks until done

private:
    SPSCQueue<65536> queue_;
    std::thread thread_;
    std::atomic<bool> running_{true};
    std::atomic<bool> date_changed_{false};
    std::string pending_date_;  // written by main thread, read by kbar thread when flag set

    // --- kbar thread exclusive state ---
    std::string output_dir_, current_date_, current_dir_;

    // Per-minute bar buckets: trade 按 exchange_time 直接分拣到对应分钟
    // std::map 保证按分钟顺序迭代
    std::map<int, BarMap> minute_bars_;
    int last_flushed_minute_ = -1;  // 最后一个已 flush 的分钟 (HHMM), -1 = 尚无

    std::unordered_map<int32_t, int32_t> channel_watermarks_;
    std::unordered_map<int32_t, double> active_stocks_;  // flush 时更新，用于 carry-forward
    int64_t last_trade_steady_ns_ = 0;
    static constexpr int64_t TIMEOUT_NS = 30LL * 1000000000LL;

    void thread_loop();
    void on_trade(const TradeEvent& evt);
    int compute_watermark_minute();
    void try_flush();                              // watermark 驱动的常规 flush
    void flush_remaining();                        // 强制 flush 所有未输出的分钟
    void flush_bars(int minute, BarMap& bars);     // 输出单个分钟（含 carry-forward）
    void write_csv(int minute, const BarMap& bars);
    void apply_date_change();
    void check_timeout();

    static int exch_time_to_hhmm(int32_t exch_time);
    static int next_minute(int hhmm);
    static void mkdir_p(const std::string& path);
    static std::string format_symbol(int32_t symbol);
    static std::string format_bar_time(const std::string& date, int hhmm);
    static int64_t steady_now_ns();
};
