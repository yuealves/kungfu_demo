#pragma once
#include "moments.h"
#include "factor_schema.h"
#include "bucket_classify.h"
#include "event_types.h"
#include "time_utils.h"
#include <cmath>
#include <unordered_set>

namespace rtf {

// ---- 单个事件族的分桶 Moments 容器 ----

// Order: vol, money × 3 buckets
struct OrderMoments {
    // [bucket][0=vol, 1=money]
    Moments data[ORDER_BUCKET_N][2];

    void update(const ProcessedOrder& o, OrderBucket bk) {
        int b = static_cast<int>(bk);
        data[b][0].update(o.volume);
        data[b][1].update(o.money);
    }

    void feed(const ProcessedOrder& o) {
        update(o, OrderBucket::ALL);
        if (is_large(o.volume, o.money)) update(o, OrderBucket::LARGE);
        if (is_small(o.volume, o.money)) update(o, OrderBucket::SMALL);
    }

    // 输出 ORDER_FAMILY_FACTORS 个 double 到 out
    void write_factors(double* out) const {
        int idx = 0;
        for (int b = 0; b < ORDER_BUCKET_N; ++b) {
            const auto& vol = data[b][0];
            const auto& money = data[b][1];
            out[idx++] = static_cast<double>(vol.count());
            out[idx++] = (vol.s1() > 0) ? money.s1() / vol.s1() : NAN; // vwapx
            out[idx++] = money.mean();
            out[idx++] = money.std_sample();
            out[idx++] = money.skew();
            out[idx++] = money.kurt();
        }
    }
};

// Withdraw: vol, money, tmlag × 6 buckets
struct WithdrawMoments {
    // [bucket][0=vol, 1=money, 2=tmlag]
    Moments data[WITHDRAW_BUCKET_N][3];

    void update(const ProcessedWithdraw& w, WithdrawBucket bk) {
        int b = static_cast<int>(bk);
        data[b][0].update(w.withdraw_vol);
        data[b][1].update(w.withdraw_money);
        data[b][2].update(static_cast<double>(w.withdraw_tmlag));
    }

    void feed(const ProcessedWithdraw& w) {
        update(w, WithdrawBucket::ALL);
        if (is_large(w.withdraw_vol, w.withdraw_money)) update(w, WithdrawBucket::LARGE);
        if (is_small(w.withdraw_vol, w.withdraw_money)) update(w, WithdrawBucket::SMALL);
        if (is_complete(w.withdraw_vol, w.order_vol))    update(w, WithdrawBucket::COMPLETE);
        if (is_partial(w.withdraw_vol, w.order_vol))     update(w, WithdrawBucket::PARTIAL);
        if (is_instant(w.withdraw_tmlag))                update(w, WithdrawBucket::INSTANT);
    }

    void write_factors(double* out) const {
        int idx = 0;
        for (int b = 0; b < WITHDRAW_BUCKET_N; ++b) {
            const auto& vol = data[b][0];
            const auto& money = data[b][1];
            const auto& tmlag = data[b][2];
            out[idx++] = static_cast<double>(vol.count());
            out[idx++] = (vol.s1() > 0) ? money.s1() / vol.s1() : NAN; // vwapx
            out[idx++] = money.mean();
            out[idx++] = money.std_sample();
            out[idx++] = money.skew();
            out[idx++] = money.kurt();
            out[idx++] = tmlag.mean();
            out[idx++] = tmlag.std_sample();
            out[idx++] = tmlag.skew();
            out[idx++] = tmlag.kurt();
        }
    }
};

// Trade: vol, money, tmlag, moneydiff × 7 buckets + unique counts
struct TradeMoments {
    // [bucket][0=vol, 1=money, 2=tmlag, 3=moneydiff]
    Moments data[TRADE_BUCKET_N][4];
    // unique order id sets per bucket
    std::unordered_set<int64_t> buy_order_ids[TRADE_BUCKET_N];
    std::unordered_set<int64_t> sell_order_ids[TRADE_BUCKET_N];

    // 合成 (channel_id, order_id) 为唯一 key
    static int64_t make_key(int32_t channel, int32_t order_id) {
        return (static_cast<int64_t>(channel) << 32) | static_cast<int64_t>(static_cast<uint32_t>(order_id));
    }

    void update(const ProcessedTrade& t, TradeBucket bk) {
        int b = static_cast<int>(bk);
        data[b][0].update(t.volume);
        data[b][1].update(t.money);
        data[b][2].update(static_cast<double>(t.order_tmlag));
        data[b][3].update(t.order_money_diff);
        if (t.buy_order_id != 0)
            buy_order_ids[b].insert(make_key(t.channel_id, t.buy_order_id));
        if (t.sell_order_id != 0)
            sell_order_ids[b].insert(make_key(t.channel_id, t.sell_order_id));
    }

    void feed(const ProcessedTrade& t) {
        update(t, TradeBucket::ALL);
        if (is_large(t.volume, t.money))                  update(t, TradeBucket::LARGE);
        if (is_small(t.volume, t.money))                  update(t, TradeBucket::SMALL);
        if (is_large(t.ordervol_buy, t.ordermoney_buy))   update(t, TradeBucket::BOLARGE);
        if (is_small(t.ordervol_buy, t.ordermoney_buy))   update(t, TradeBucket::BOSMALL);
        if (is_large(t.ordervol_sell, t.ordermoney_sell))  update(t, TradeBucket::SOLARGE);
        if (is_small(t.ordervol_sell, t.ordermoney_sell))  update(t, TradeBucket::SOSMALL);
    }

    void write_factors(double* out) const {
        int idx = 0;
        for (int b = 0; b < TRADE_BUCKET_N; ++b) {
            const auto& vol = data[b][0];
            const auto& money = data[b][1];
            const auto& tmlag = data[b][2];
            const auto& mdiff = data[b][3];
            out[idx++] = static_cast<double>(vol.count());
            out[idx++] = (vol.s1() > 0) ? money.s1() / vol.s1() : NAN; // vwapx
            out[idx++] = money.mean();
            out[idx++] = money.std_sample();
            out[idx++] = money.skew();
            out[idx++] = money.kurt();
            out[idx++] = tmlag.mean();
            out[idx++] = tmlag.std_sample();
            out[idx++] = tmlag.skew();
            out[idx++] = tmlag.kurt();
            out[idx++] = mdiff.mean();
            out[idx++] = mdiff.std_sample();
            out[idx++] = mdiff.skew();
            out[idx++] = mdiff.kurt();
            out[idx++] = static_cast<double>(buy_order_ids[b].size());
            out[idx++] = static_cast<double>(sell_order_ids[b].size());
        }
    }
};

// ---- 单只股票的完整因子状态 ----
struct SymbolState {
    OrderMoments    buy_order;
    OrderMoments    sell_order;
    WithdrawMoments buy_withdraw;
    WithdrawMoments sell_withdraw;
    TradeMoments    both_trade;
    TradeMoments    buy_trade;
    TradeMoments    sell_trade;

    // 输出全量因子向量 (TOTAL_TS_FACTORS 个 double)
    void write_all_factors(double* out) const {
        double* p = out;
        buy_order.write_factors(p);     p += ORDER_FAMILY_FACTORS;
        sell_order.write_factors(p);    p += ORDER_FAMILY_FACTORS;
        buy_withdraw.write_factors(p);  p += WITHDRAW_FAMILY_FACTORS;
        sell_withdraw.write_factors(p); p += WITHDRAW_FAMILY_FACTORS;
        both_trade.write_factors(p);    p += TRADE_FAMILY_FACTORS;
        buy_trade.write_factors(p);     p += TRADE_FAMILY_FACTORS;
        sell_trade.write_factors(p);    // last, no advance needed
    }
};

} // namespace rtf
