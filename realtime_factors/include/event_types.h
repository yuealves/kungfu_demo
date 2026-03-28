#pragma once
#include <cstdint>
#include <cmath>

namespace rtf {

// 交易所判定
inline bool is_sh(int32_t symbol) { return symbol >= 400000; }
inline bool is_sz(int32_t symbol) { return symbol < 400000; }

// 内部订单事件（从 KyStdOrderType 转换而来）
struct OrderEvent {
    int32_t symbol;
    int32_t channel_id;     // channelid
    int32_t order_id;       // orderid
    int32_t time;           // HHMMSSmmm
    double  price;          // orderpx
    int32_t volume;         // ordervol
    int8_t  side;           // 66='B', 83='S'
    int8_t  order_type;     // SH: 65='A'(新单), 68='D'(撤单); SZ: 50='2'(限价), 49='1'(对手方最优), 85='U'(本方最优)
};

// 内部成交事件
struct TradeEvent {
    int32_t symbol;
    int32_t channel_id;
    int32_t buy_order_id;   // tradebuyid
    int32_t sell_order_id;  // tradesellid
    int32_t time;           // HHMMSSmmm
    double  price;          // tradepx
    int32_t volume;         // tradevol
    int8_t  side;           // 66='B', 83='S' (SH从BSFlag; SZ从buyid>sellid推断)
    int8_t  trade_type;     // SZ: 70='F'(成交), 52='4'(撤单); SH: 不用
};

// 处理后的委托事件（用于因子计算）
struct ProcessedOrder {
    int32_t symbol;
    int32_t channel_id;
    int32_t order_id;
    int32_t time;
    double  price;
    int32_t volume;
    double  money;          // volume * price
};

// 处理后的撤单事件
struct ProcessedWithdraw {
    int32_t symbol;
    int32_t channel_id;
    int32_t withdraw_id;    // 对应原订单 id
    int32_t withdraw_time;
    double  withdraw_price;
    int32_t withdraw_vol;
    double  withdraw_money;
    int32_t order_time;     // 原订单时间
    int32_t order_vol;      // 原订单量
    int64_t withdraw_tmlag; // 连续竞价时间差 (ms)
};

// 处理后的成交事件
struct ProcessedTrade {
    int32_t symbol;
    int32_t channel_id;
    int32_t buy_order_id;
    int32_t sell_order_id;
    int32_t time;
    double  price;
    int32_t volume;
    double  money;          // volume * price
    int8_t  side;           // 66='B', 83='S'
    // 买方委托信息
    int32_t ordertm_buy;
    double  orderpx_buy;
    int32_t ordervol_buy;
    double  ordermoney_buy;
    // 卖方委托信息
    int32_t ordertm_sell;
    double  orderpx_sell;
    int32_t ordervol_sell;
    double  ordermoney_sell;
    // 衍生
    int64_t order_tmlag;      // |continuous_ms(ordertm_buy) - continuous_ms(ordertm_sell)|
    double  order_money_diff; // |ordermoney_buy - ordermoney_sell|
};

} // namespace rtf
