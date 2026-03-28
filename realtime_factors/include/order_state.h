#pragma once
#include "event_types.h"
#include "time_utils.h"
#include <unordered_map>
#include <functional>
#include <cmath>

namespace rtf {

// 活跃订单记录
struct ActiveOrder {
    int32_t symbol;
    int32_t channel_id;
    int32_t order_id;
    int32_t time;       // ordertm
    double  price;      // orderpx
    int32_t volume;     // ordervol
    double  money;      // ordervol * orderpx
    int8_t  side;       // 66='B', 83='S'
    int32_t traded_vol; // 已成交量（用于判断订单是否完成）
};

// 复合 key: (channel_id, order_id)
struct OrderKey {
    int32_t channel_id;
    int32_t order_id;
    bool operator==(const OrderKey& o) const {
        return channel_id == o.channel_id && order_id == o.order_id;
    }
};

struct OrderKeyHash {
    size_t operator()(const OrderKey& k) const {
        return std::hash<int64_t>{}(
            (static_cast<int64_t>(k.channel_id) << 32) |
            static_cast<int64_t>(static_cast<uint32_t>(k.order_id)));
    }
};

// 深圳最新成交价记录
struct LatestTradePrice {
    double price;
    int32_t time;
};

class OrderStateStore {
public:
    using OrderCallback    = std::function<void(int8_t side, const ProcessedOrder&)>;
    using WithdrawCallback = std::function<void(int8_t side, const ProcessedWithdraw&)>;
    using TradeCallback    = std::function<void(const ProcessedTrade&)>;

    void on_order_sh(const OrderEvent& evt,
                     OrderCallback on_order,
                     WithdrawCallback on_withdraw);

    void on_trade_sh(const TradeEvent& evt,
                     OrderCallback on_order,
                     TradeCallback on_trade);

    void on_order_sz(const OrderEvent& evt,
                     OrderCallback on_order);

    void on_trade_sz(const TradeEvent& evt,
                     WithdrawCallback on_withdraw,
                     TradeCallback on_trade);

    // 获取活跃订单（用于测试验证）
    const ActiveOrder* find_order(int32_t channel_id, int32_t order_id) const;

    size_t active_order_count() const { return orders_.size(); }

private:
    std::unordered_map<OrderKey, ActiveOrder, OrderKeyHash> orders_;

    // 深圳最新成交价: key = (symbol << 8) | side
    std::unordered_map<int64_t, LatestTradePrice> sz_latest_trade_px_;

    int64_t sz_trade_px_key(int32_t symbol, int8_t side) const {
        return (static_cast<int64_t>(symbol) << 8) | static_cast<int64_t>(static_cast<uint8_t>(side));
    }

    void insert_order(const OrderEvent& evt, double price);
    void remove_order(int32_t channel_id, int32_t order_id);

    void complement_aggressive_order_sh(const TradeEvent& evt,
                                        int32_t order_id, int8_t side,
                                        OrderCallback on_order);

    ProcessedWithdraw make_withdraw(const ActiveOrder& orig,
                                    int32_t withdraw_time,
                                    double withdraw_price,
                                    int32_t withdraw_vol) const;

    ProcessedTrade make_trade(const TradeEvent& evt,
                              const ActiveOrder* buy_order,
                              const ActiveOrder* sell_order) const;
};

} // namespace rtf
