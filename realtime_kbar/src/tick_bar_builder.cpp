#include "tick_bar_builder.h"
#include <cstdio>
#include <cstring>

// ─────────────────── StockBarAccumulator ───────────────────

void StockBarAccumulator::set_prev_day_close(float price) {
    prev_day_close_ = price;
    has_prev_day_close_ = true;
}

int StockBarAccumulator::next_minute(int hhmm) {
    int h = hhmm / 100;
    int m = hhmm % 100 + 1;
    if (m >= 60) { m = 0; h++; }
    return h * 100 + m;
}

int StockBarAccumulator::next_trading_minute(int hhmm) {
    if (hhmm < 931)  return 931;
    if (hhmm < 1130) return next_minute(hhmm);
    if (hhmm == 1130) return 1301;
    if (hhmm < 1500) return next_minute(hhmm);
    return 0;
}

int StockBarAccumulator::compute_bar_minute(int32_t time) {
    if (time < 92500000)  return BAR_BASELINE;
    if (time < 93000000)  return 931;
    if (time == 113000000) return 1130;
    if (time == 150000000) return 1500;
    if (time > 113000000 && time < 130000000) return BAR_MIDDAY_CARRY;
    if (time > 150000000 && time <= 150500000) return BAR_CLOSE_CARRY;
    if (time > 150500000) return BAR_IGNORE;
    // Normal trading: next_minute(HHMM)
    int hh = time / 10000000;
    int mm = (time / 100000) % 100;
    return next_minute(hh * 100 + mm);
}

void StockBarAccumulator::reset_bar_state() {
    bar_open_ = bar_close_ = bar_high_ = bar_low_ = 0;
    bar_has_trade_ = false;
    bar_cum_volume_ = bar_cum_money_ = 0;
    bar_has_cum_data_ = false;
    close_all_ticks_ = 0;
    bar_has_any_tick_ = false;
    bar_high_update_ = 0;
    bar_low_update_ = 0;
    has_high_update_ = false;
    has_low_update_ = false;
}

float StockBarAccumulator::resolve_bar_close(int minute) const {
    float carry;
    if (ffill_trade_close_ >= 0)
        carry = ffill_trade_close_;
    else if (ffill_any_close_ >= 0)
        carry = ffill_any_close_;
    else
        carry = baseline_close_;

    bool before_first = (first_valid_bar_ == 0) || (minute < first_valid_bar_);
    if (before_first && carry == 0 && has_prev_day_close_)
        return prev_day_close_;

    return carry;
}

// Shared logic for processing a tick within a trading bar
void StockBarAccumulator::process_trade_tick(
    float price_f, float high_f, float low_f,
    int64_t acc_volume, int64_t acc_turnover, int32_t time_int)
{
    // Track all ticks
    bar_has_any_tick_ = true;
    if (price_f > 0) close_all_ticks_ = price_f;
    bar_cum_volume_ = acc_volume;
    bar_cum_money_ = acc_turnover;
    bar_has_cum_data_ = true;

    // Detect trade tick
    bool is_trade;
    if (!seen_first_trading_) {
        is_trade = true;
        seen_first_trading_ = true;
    } else {
        is_trade = (acc_volume != prev_acc_volume_) ||
                   (acc_turnover != prev_acc_turnover_);
    }

    // OHLC from trade ticks
    if (is_trade && price_f > 0) {
        if (!bar_has_trade_) {
            bar_open_ = price_f;
            bar_high_ = price_f;
            bar_low_ = price_f;
        } else {
            if (price_f > bar_high_) bar_high_ = price_f;
            if (price_f < bar_low_)  bar_low_ = price_f;
        }
        bar_close_ = price_f;
        bar_has_trade_ = true;
    }

    // High/low updates from snapshot changes
    if (high_f != prev_high_ && high_f > 0) {
        if (!has_high_update_ || high_f > bar_high_update_) {
            bar_high_update_ = high_f;
            has_high_update_ = true;
        }
    }
    if (low_f != prev_low_ && low_f > 0) {
        if (!has_low_update_ || low_f < bar_low_update_) {
            bar_low_update_ = low_f;
            has_low_update_ = true;
        }
    }
}

bool StockBarAccumulator::push_tick(
    int32_t time_int, float price_raw, float high_raw, float low_raw,
    int64_t acc_volume, int64_t acc_turnover)
{
    float price_f = (float)((double)price_raw / 10000.0);
    float high_f  = (float)((double)high_raw / 10000.0);
    float low_f   = (float)((double)low_raw / 10000.0);

    has_any_tick_ever_ = true;

    // Track first valid tick (for prev_day_close fill)
    if (first_valid_bar_ == 0 &&
        (price_f > 0 || acc_volume > 0 || acc_turnover > 0)) {
        int hh = time_int / 10000000;
        int mm = (time_int / 100000) % 100;
        first_valid_bar_ = next_minute(hh * 100 + mm);
    }

    // Track preclose (for close auction) — every tick with price > 0 before 15:00
    if (price_f > 0 && time_int < 150000000) {
        preclose_price_ = price_f;
        preclose_volume_ = acc_volume;
        preclose_money_ = acc_turnover;
        has_preclose_ = true;
    }

    int bar_min = compute_bar_minute(time_int);
    bool bar_completed = false;

    if (bar_min == BAR_BASELINE) {
        // Update baseline (pre-09:25)
        baseline_close_ = price_f;
        baseline_volume_ = acc_volume;
        baseline_money_ = acc_turnover;

    } else if (bar_min == BAR_MIDDAY_CARRY) {
        // First midday carry tick → contributes to bar 1130
        if (!midday_carry_done_) {
            midday_carry_done_ = true;
            process_trade_tick(price_f, high_f, low_f, acc_volume, acc_turnover, time_int);
        }

    } else if (bar_min == BAR_CLOSE_CARRY) {
        // Scan ALL carry ticks for auction detection (volume increase may
        // not appear on the first carry tick — common for SH stocks)
        if (has_preclose_ && price_f > 0 && !has_auction_) {
            bool vol_increased = (acc_volume > preclose_volume_) ||
                                 (acc_turnover > preclose_money_);
            if (vol_increased) {
                auction_price_ = price_f;
                auction_volume_ = acc_volume;
                auction_money_ = acc_turnover;
                has_auction_ = true;
            }
        }
        // Only update bar 1500 cum data from the FIRST carry tick
        if (!close_carry_done_) {
            close_carry_done_ = true;
            bar_cum_volume_ = acc_volume;
            bar_cum_money_ = acc_turnover;
            bar_has_cum_data_ = true;
            if (price_f > 0) {
                close_all_ticks_ = price_f;
                bar_has_any_tick_ = true;
            }
        }

    } else if (bar_min == BAR_IGNORE) {
        // Do nothing

    } else {
        // Normal trading bar
        if (bar_min != current_bar_) {
            if (current_bar_ > 0) {
                emit_bar();
                bar_completed = true;
            }
            fill_gap_bars(current_bar_, bar_min);
            current_bar_ = bar_min;
            reset_bar_state();
        }

        process_trade_tick(price_f, high_f, low_f, acc_volume, acc_turnover, time_int);

        // Track 11:30 window [11:29, 11:30)
        if (current_bar_ == 1130 && time_int >= 112900000 && time_int < 113000000) {
            if (first_vol_1129_ < 0) first_vol_1129_ = acc_volume;
            last_vol_1129_ = acc_volume;
            if (first_snapshot_1129_ == 0 && price_f > 0)
                first_snapshot_1129_ = price_f;
        }

        // Detect auction at exactly 15:00:00.000
        if (time_int == 150000000 && has_preclose_ && price_f > 0 && !has_auction_) {
            bool vol_increased = (acc_volume > preclose_volume_) ||
                                 (acc_turnover > preclose_money_);
            bool price_changed = (price_f != preclose_price_);
            if (vol_increased || price_changed) {
                auction_price_ = price_f;
                auction_volume_ = acc_volume;
                auction_money_ = acc_turnover;
                has_auction_ = true;
            }
        }
    }

    // Update prev state (always, for all phases)
    prev_acc_volume_ = acc_volume;
    prev_acc_turnover_ = acc_turnover;
    prev_high_ = high_f;
    prev_low_ = low_f;

    return bar_completed;
}

void StockBarAccumulator::emit_bar() {
    if (current_bar_ == 0) return;

    MinuteBar bar;
    bar.bar_minute = current_bar_;

    // --- Resolve close_all_ticks forward-fill ---
    float this_cat = bar_has_any_tick_ ? close_all_ticks_ : -1.0f;
    if (this_cat >= 0) ffill_any_close_ = this_cat;
    float resolved_cat = (ffill_any_close_ >= 0) ? ffill_any_close_ : baseline_close_;

    // --- Resolve trade close forward-fill ---
    float this_trade_close = bar_has_trade_ ? bar_close_ : -1.0f;
    if (this_trade_close >= 0) ffill_trade_close_ = this_trade_close;
    float resolved_close = (ffill_trade_close_ >= 0) ? ffill_trade_close_ : resolved_cat;

    // Fallback to prev_day_close if needed
    if (resolved_close == 0) {
        bool before_first = (first_valid_bar_ == 0) || (current_bar_ < first_valid_bar_);
        if (before_first && has_prev_day_close_)
            resolved_close = prev_day_close_;
    }

    // --- OHLC ---
    if (bar_has_trade_) {
        bar.open = bar_open_;
        bar.close = bar_close_;
        bar.high = bar_high_;
        bar.low = bar_low_;
        // Apply high/low updates from snapshot changes
        if (has_high_update_ && bar_high_update_ > bar.high) bar.high = bar_high_update_;
        if (has_low_update_ && bar_low_update_ < bar.low)    bar.low = bar_low_update_;
    } else {
        bar.open = bar.close = bar.high = bar.low = resolved_close;
    }

    // --- Volume / Money ---
    int64_t base_vol   = (prev_bar_cum_volume_ >= 0) ? prev_bar_cum_volume_ : baseline_volume_;
    int64_t base_money = (prev_bar_cum_money_ >= 0)  ? prev_bar_cum_money_  : baseline_money_;

    if (bar_has_cum_data_) {
        bar.volume = std::max(0.0, (double)(bar_cum_volume_ - base_vol));
        bar.money  = std::max(0.0, (double)(bar_cum_money_ - base_money));
        prev_bar_cum_volume_ = bar_cum_volume_;
        prev_bar_cum_money_  = bar_cum_money_;
    } else {
        bar.volume = 0;
        bar.money  = 0;
        // Forward-fill: if never had cum data, init from baseline
        if (prev_bar_cum_volume_ < 0) {
            prev_bar_cum_volume_ = baseline_volume_;
            prev_bar_cum_money_  = baseline_money_;
        }
    }

    // --- 15:00 close auction override ---
    if (current_bar_ == 1500 && has_preclose_ && has_auction_) {
        bar.open  = preclose_price_;
        bar.close = auction_price_;
        bar.high  = std::max(preclose_price_, auction_price_);
        bar.low   = std::min(preclose_price_, auction_price_);
        bar.volume = std::max(0.0, (double)(auction_volume_ - preclose_volume_));
        bar.money  = std::max(0.0, (double)(auction_money_ - preclose_money_));
        ffill_trade_close_ = bar.close;
        // Update cum state with auction values
        prev_bar_cum_volume_ = auction_volume_;
        prev_bar_cum_money_  = auction_money_;
    }

    // --- 11:30 morning end override ---
    if (current_bar_ == 1130) {
        bool no_trade_in_window = (first_vol_1129_ >= 0) &&
                                  (first_vol_1129_ == last_vol_1129_);
        if (no_trade_in_window && first_snapshot_1129_ > 0) {
            float snap = first_snapshot_1129_;
            if (std::abs(bar.open - snap) > 1e-6f) {
                bar.open = snap;
                bar.high = std::max(snap, bar.close);
                bar.low  = std::min(snap, bar.close);
            }
        }
    }

    completed_bars_.push_back(bar);
}

void StockBarAccumulator::emit_carry_bar(int minute) {
    MinuteBar bar;
    bar.bar_minute = minute;
    float close = resolve_bar_close(minute);
    bar.open = bar.close = bar.high = bar.low = close;
    bar.volume = 0;
    bar.money  = 0;
    completed_bars_.push_back(bar);
}

void StockBarAccumulator::fill_gap_bars(int from_bar, int to_bar) {
    int m = (from_bar > 0) ? next_trading_minute(from_bar) : 931;
    while (m > 0 && m < to_bar) {
        emit_carry_bar(m);
        m = next_trading_minute(m);
    }
}

void StockBarAccumulator::finalize() {
    if (finalized_) return;
    finalized_ = true;

    // Emit current bar
    if (current_bar_ > 0) {
        emit_bar();
    }

    // Fill remaining trading minutes
    int last = current_bar_;
    int m = (last > 0) ? next_trading_minute(last) : 931;
    while (m > 0) {
        emit_carry_bar(m);
        m = next_trading_minute(m);
    }
}

void StockBarAccumulator::finalize_current_only() {
    if (finalized_) return;
    finalized_ = true;

    if (current_bar_ > 0) {
        emit_bar();
    }
}

bool StockBarAccumulator::pop_bar(MinuteBar& out) {
    if (completed_bars_.empty()) return false;
    out = completed_bars_.front();
    completed_bars_.erase(completed_bars_.begin());
    return true;
}

// ─────────────────── TickBarBuilder ───────────────────

void TickBarBuilder::push_tick(
    int32_t symbol, int32_t time, float price_raw,
    float high_raw, float low_raw,
    int64_t acc_volume, int64_t acc_turnover)
{
    auto& acc = accumulators_[symbol];
    acc.push_tick(time, price_raw, high_raw, low_raw, acc_volume, acc_turnover);
    if (callback_) drain_bars(symbol, acc);
}

void TickBarBuilder::set_prev_day_close(int32_t symbol, float price) {
    accumulators_[symbol].set_prev_day_close(price);
}

void TickBarBuilder::set_prev_day_close_if_new(int32_t symbol, float price) {
    auto& acc = accumulators_[symbol];
    if (!acc.has_prev_day_close())
        acc.set_prev_day_close(price);
}

void TickBarBuilder::add_symbol(int32_t symbol) {
    accumulators_[symbol];  // default-construct if not present
}

void TickBarBuilder::finalize_all() {
    for (auto& [sym, acc] : accumulators_) {
        acc.finalize();
        if (callback_) drain_bars(sym, acc);
    }
}

void TickBarBuilder::finalize_all_current_only() {
    for (auto& [sym, acc] : accumulators_) {
        acc.finalize_current_only();
        if (callback_) drain_bars(sym, acc);
    }
}

std::vector<MinuteBar> TickBarBuilder::get_bars(int32_t symbol) {
    std::vector<MinuteBar> result;
    auto it = accumulators_.find(symbol);
    if (it == accumulators_.end()) return result;
    MinuteBar bar;
    while (it->second.pop_bar(bar)) {
        result.push_back(bar);
    }
    return result;
}

std::vector<int32_t> TickBarBuilder::get_symbols() const {
    std::vector<int32_t> syms;
    syms.reserve(accumulators_.size());
    for (auto& [sym, acc] : accumulators_) {
        syms.push_back(sym);
    }
    return syms;
}

void TickBarBuilder::drain_bars(int32_t symbol, StockBarAccumulator& acc) {
    MinuteBar bar;
    while (acc.pop_bar(bar)) {
        callback_(symbol, bar);
    }
}

// ─────────────────── Utilities ───────────────────

std::string format_symbol(int32_t symbol) {
    char buf[24];
    if (symbol < 400000)
        snprintf(buf, sizeof(buf), "%06d.XSHE", symbol);
    else if (symbol < 700000)
        snprintf(buf, sizeof(buf), "%06d.XSHG", symbol);
    else
        snprintf(buf, sizeof(buf), "%06d.OTHER", symbol);
    return buf;
}

std::string format_bar_time(const std::string& date, int hhmm) {
    // date is "YYYY-MM-DD", returns "YYYY-MM-DD HH:MM:00"
    char buf[32];
    snprintf(buf, sizeof(buf), "%s %02d:%02d:00",
             date.c_str(), hhmm / 100, hhmm % 100);
    return buf;
}
