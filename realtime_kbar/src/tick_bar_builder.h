#pragma once

#include <cstdint>
#include <vector>
#include <unordered_map>
#include <string>
#include <cmath>
#include <functional>
#include <algorithm>

// Bar minute special values
constexpr int BAR_BASELINE    = -1;
constexpr int BAR_MIDDAY_CARRY = -2;
constexpr int BAR_CLOSE_CARRY  = -3;
constexpr int BAR_IGNORE       = -4;

struct MinuteBar {
    int bar_minute = 0;       // HHMM format (e.g. 931, 1130, 1500)
    float open = 0, close = 0, high = 0, low = 0;
    double volume = 0, money = 0;
};

class StockBarAccumulator {
public:
    void set_prev_day_close(float price);
    bool has_prev_day_close() const { return has_prev_day_close_; }

    // Push a tick snapshot. price/high/low are raw (×10000).
    // Returns true if one or more bars were completed.
    bool push_tick(int32_t time, float price_raw, float high_raw, float low_raw,
                   int64_t acc_volume, int64_t acc_turnover);

    // Finalize: emit current bar and fill all remaining trading minutes.
    void finalize();

    // Pop a completed bar. Returns false if queue is empty.
    bool pop_bar(MinuteBar& out);

    bool has_any_data() const { return has_any_tick_ever_; }

    // Helpers (public for testing / reuse)
    static int compute_bar_minute(int32_t time);
    static int next_minute(int hhmm);
    static int next_trading_minute(int hhmm);

private:
    // Baseline state (pre-09:25)
    float baseline_close_ = 0;
    int64_t baseline_volume_ = 0;
    int64_t baseline_money_ = 0;

    // Previous tick state (for trade detection + high/low change)
    int64_t prev_acc_volume_ = -1;
    int64_t prev_acc_turnover_ = -1;
    float prev_high_ = 0;
    float prev_low_ = 0;
    bool seen_first_trading_ = false;

    // Current bar accumulation
    int current_bar_ = 0;
    float bar_open_ = 0, bar_close_ = 0, bar_high_ = 0, bar_low_ = 0;
    bool bar_has_trade_ = false;
    int64_t bar_cum_volume_ = 0;
    int64_t bar_cum_money_ = 0;
    bool bar_has_cum_data_ = false;
    float close_all_ticks_ = 0;
    bool bar_has_any_tick_ = false;

    // High/low updates from snapshot changes
    float bar_high_update_ = 0;
    float bar_low_update_ = 0;
    bool has_high_update_ = false;
    bool has_low_update_ = false;

    // Forward-fill state across bars
    float ffill_trade_close_ = -1;
    float ffill_any_close_ = -1;
    int64_t prev_bar_cum_volume_ = -1;
    int64_t prev_bar_cum_money_ = -1;

    // 11:30 morning end tracking ([11:29, 11:30) window)
    int64_t first_vol_1129_ = -1;
    int64_t last_vol_1129_ = -1;
    float first_snapshot_1129_ = 0;
    bool midday_carry_done_ = false;

    // 15:00 close auction tracking
    float preclose_price_ = 0;
    int64_t preclose_volume_ = 0;
    int64_t preclose_money_ = 0;
    bool has_preclose_ = false;
    float auction_price_ = 0;
    int64_t auction_volume_ = 0;
    int64_t auction_money_ = 0;
    bool has_auction_ = false;
    bool close_carry_done_ = false;

    // Prev day close
    float prev_day_close_ = 0;
    bool has_prev_day_close_ = false;

    // First valid tick bar (for prev_day_close fill)
    int first_valid_bar_ = 0;

    bool has_any_tick_ever_ = false;
    bool finalized_ = false;

    std::vector<MinuteBar> completed_bars_;

    void emit_bar();
    void emit_carry_bar(int minute);
    void fill_gap_bars(int from_bar, int to_bar);
    void reset_bar_state();
    float resolve_bar_close(int minute) const;

    void process_trade_tick(float price_f, float high_f, float low_f,
                            int64_t acc_volume, int64_t acc_turnover,
                            int32_t time_int);
};

class TickBarBuilder {
public:
    using BarCallback = std::function<void(int32_t symbol, const MinuteBar& bar)>;

    void set_bar_callback(BarCallback cb) { callback_ = std::move(cb); }

    void push_tick(int32_t symbol, int32_t time, float price_raw,
                   float high_raw, float low_raw,
                   int64_t acc_volume, int64_t acc_turnover);

    void set_prev_day_close(int32_t symbol, float price);

    // Set prev_day_close only if not already set for this symbol.
    // Useful for extracting from tick PreClose field (called every tick, only first takes effect).
    void set_prev_day_close_if_new(int32_t symbol, float price);

    // Ensure accumulator exists for a symbol (even without ticks)
    void add_symbol(int32_t symbol);

    void finalize_all();

    // Get all completed bars for a symbol (after finalize)
    std::vector<MinuteBar> get_bars(int32_t symbol);

    std::vector<int32_t> get_symbols() const;

private:
    std::unordered_map<int32_t, StockBarAccumulator> accumulators_;
    BarCallback callback_;

    void drain_bars(int32_t symbol, StockBarAccumulator& acc);
};

// Utility: format symbol int to JQ-style code string
std::string format_symbol(int32_t symbol);

// Utility: format bar time string "YYYY-MM-DD HH:MM:00"
std::string format_bar_time(const std::string& date, int hhmm);
