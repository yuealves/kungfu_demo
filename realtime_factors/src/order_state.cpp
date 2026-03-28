#include "order_state.h"
#include <cmath>

namespace rtf {

const ActiveOrder* OrderStateStore::find_order(int32_t channel_id, int32_t order_id) const {
    auto it = orders_.find({channel_id, order_id});
    return (it != orders_.end()) ? &it->second : nullptr;
}

void OrderStateStore::insert_order(const OrderEvent& evt, double price) {
    OrderKey key{evt.channel_id, evt.order_id};
    ActiveOrder ao;
    ao.symbol = evt.symbol;
    ao.channel_id = evt.channel_id;
    ao.order_id = evt.order_id;
    ao.time = evt.time;
    ao.price = price;
    ao.volume = evt.volume;
    ao.money = evt.volume * price;
    ao.side = evt.side;
    ao.traded_vol = 0;
    orders_[key] = ao;
}

void OrderStateStore::remove_order(int32_t channel_id, int32_t order_id) {
    orders_.erase({channel_id, order_id});
}

ProcessedWithdraw OrderStateStore::make_withdraw(const ActiveOrder& orig,
                                                  int32_t withdraw_time,
                                                  double withdraw_price,
                                                  int32_t withdraw_vol) const {
    ProcessedWithdraw pw;
    pw.symbol = orig.symbol;
    pw.channel_id = orig.channel_id;
    pw.withdraw_id = orig.order_id;
    pw.withdraw_time = withdraw_time;
    pw.withdraw_price = withdraw_price;
    pw.withdraw_vol = withdraw_vol;
    pw.withdraw_money = withdraw_vol * withdraw_price;
    pw.order_time = orig.time;
    pw.order_vol = orig.volume;
    pw.withdraw_tmlag = tm_to_continuous_ms(withdraw_time) - tm_to_continuous_ms(orig.time);
    return pw;
}

ProcessedTrade OrderStateStore::make_trade(const TradeEvent& evt,
                                           const ActiveOrder* buy_order,
                                           const ActiveOrder* sell_order) const {
    ProcessedTrade pt;
    pt.symbol = evt.symbol;
    pt.channel_id = evt.channel_id;
    pt.buy_order_id = evt.buy_order_id;
    pt.sell_order_id = evt.sell_order_id;
    pt.time = evt.time;
    pt.price = evt.price;
    pt.volume = evt.volume;
    pt.money = evt.volume * evt.price;
    pt.side = evt.side;

    if (buy_order) {
        pt.ordertm_buy = buy_order->time;
        pt.orderpx_buy = buy_order->price;
        pt.ordervol_buy = buy_order->volume;
        pt.ordermoney_buy = buy_order->money;
    } else {
        pt.ordertm_buy = 0;
        pt.orderpx_buy = NAN;
        pt.ordervol_buy = 0;
        pt.ordermoney_buy = NAN;
    }

    if (sell_order) {
        pt.ordertm_sell = sell_order->time;
        pt.orderpx_sell = sell_order->price;
        pt.ordervol_sell = sell_order->volume;
        pt.ordermoney_sell = sell_order->money;
    } else {
        pt.ordertm_sell = 0;
        pt.orderpx_sell = NAN;
        pt.ordervol_sell = 0;
        pt.ordermoney_sell = NAN;
    }

    if (buy_order && sell_order) {
        pt.order_tmlag = std::abs(
            tm_to_continuous_ms(buy_order->time) - tm_to_continuous_ms(sell_order->time));
        pt.order_money_diff = std::abs(buy_order->money - sell_order->money);
    } else {
        pt.order_tmlag = 0;
        pt.order_money_diff = NAN;
    }

    return pt;
}

// ============ SH 处理 ============

void OrderStateStore::on_order_sh(const OrderEvent& evt,
                                   OrderCallback on_order,
                                   WithdrawCallback on_withdraw) {
    if (evt.order_id <= 0) return;

    if (evt.order_type == 68) {
        // 撤单 (D)
        const auto* orig = find_order(evt.channel_id, evt.order_id);
        if (orig) {
            auto pw = make_withdraw(*orig, evt.time, evt.price, evt.volume);
            on_withdraw(orig->side, pw);
            if (evt.volume >= orig->volume) {
                remove_order(evt.channel_id, evt.order_id);
            }
        }
    } else if (evt.order_type == 65) {
        // 新单 (A)
        insert_order(evt, evt.price);
        ProcessedOrder po;
        po.symbol = evt.symbol;
        po.channel_id = evt.channel_id;
        po.order_id = evt.order_id;
        po.time = evt.time;
        po.price = evt.price;
        po.volume = evt.volume;
        po.money = evt.volume * evt.price;
        on_order(evt.side, po);
    }
}

void OrderStateStore::complement_aggressive_order_sh(const TradeEvent& evt,
                                                      int32_t order_id, int8_t side,
                                                      OrderCallback on_order) {
    auto it = orders_.find({evt.channel_id, order_id});
    if (it == orders_.end()) {
        // 激进单不在 order stream 中，从 trade 数据补全
        OrderEvent synth;
        synth.symbol = evt.symbol;
        synth.channel_id = evt.channel_id;
        synth.order_id = order_id;
        synth.time = evt.time;
        synth.price = evt.price;
        synth.volume = evt.volume;
        synth.side = side;
        synth.order_type = 65;
        insert_order(synth, evt.price);

        ProcessedOrder po;
        po.symbol = evt.symbol;
        po.channel_id = evt.channel_id;
        po.order_id = order_id;
        po.time = evt.time;
        po.price = evt.price;
        po.volume = evt.volume;
        po.money = evt.volume * evt.price;
        on_order(side, po);
    } else {
        // 已存在，累加成交量到 volume (Python: ordervol = ordervol + tradevol)
        auto& existing = it->second;
        existing.volume += evt.volume;
        existing.money = existing.volume * existing.price;
    }
}

void OrderStateStore::on_trade_sh(const TradeEvent& evt,
                                   OrderCallback on_order,
                                   TradeCallback on_trade) {
    if (evt.buy_order_id == 0 || evt.sell_order_id == 0) return;

    // 补全激进单
    if (evt.side == 66) {
        complement_aggressive_order_sh(evt, evt.buy_order_id, 66, on_order);
    } else if (evt.side == 83) {
        complement_aggressive_order_sh(evt, evt.sell_order_id, 83, on_order);
    }

    // 查找双方订单
    const auto* buy_ord = find_order(evt.channel_id, evt.buy_order_id);
    const auto* sell_ord = find_order(evt.channel_id, evt.sell_order_id);

    auto pt = make_trade(evt, buy_ord, sell_ord);
    on_trade(pt);

    // 更新已成交量并回收完成的订单
    auto* buy_mut = const_cast<ActiveOrder*>(find_order(evt.channel_id, evt.buy_order_id));
    if (buy_mut) {
        buy_mut->traded_vol += evt.volume;
        if (buy_mut->traded_vol >= buy_mut->volume) {
            remove_order(evt.channel_id, evt.buy_order_id);
        }
    }
    auto* sell_mut = const_cast<ActiveOrder*>(find_order(evt.channel_id, evt.sell_order_id));
    if (sell_mut) {
        sell_mut->traded_vol += evt.volume;
        if (sell_mut->traded_vol >= sell_mut->volume) {
            remove_order(evt.channel_id, evt.sell_order_id);
        }
    }
}

// ============ SZ 处理 ============

void OrderStateStore::on_order_sz(const OrderEvent& evt,
                                   OrderCallback on_order) {
    if (evt.order_id <= 0) return;

    // SZ 市价单: ordertype != 50('2') → 价格为 null → 需要用最新成交价回填
    double price = evt.price;
    if (evt.order_type != 50) {
        // 市价单价格回填: 买单查卖方最新成交价, 卖单查买方最新成交价
        int8_t lookup_side = (evt.side == 66) ? 83 : 66;
        auto it = sz_latest_trade_px_.find(sz_trade_px_key(evt.symbol, lookup_side));
        if (it != sz_latest_trade_px_.end()) {
            double factor = (evt.order_type == 49) ?
                ((evt.side == 66) ? 1.001 : 0.999) : 1.0;
            price = it->second.price * factor;
        }
        // 如果查不到最新成交价，price 保持原值
    }

    insert_order(evt, price);

    ProcessedOrder po;
    po.symbol = evt.symbol;
    po.channel_id = evt.channel_id;
    po.order_id = evt.order_id;
    po.time = evt.time;
    po.price = price;
    po.volume = evt.volume;
    po.money = evt.volume * price;
    on_order(evt.side, po);
}

void OrderStateStore::on_trade_sz(const TradeEvent& evt,
                                   WithdrawCallback on_withdraw,
                                   TradeCallback on_trade) {
    // 更新最新成交价 (仅正常成交)
    if (evt.trade_type == 70 && evt.buy_order_id != 0 && evt.sell_order_id != 0) {
        int8_t inferred_side = (evt.buy_order_id > evt.sell_order_id) ? 66 : 83;
        sz_latest_trade_px_[sz_trade_px_key(evt.symbol, inferred_side)] =
            {evt.price, evt.time};
    }

    // 撤单: trade stream 中 tradebuyid==0 或 tradesellid==0
    if (evt.sell_order_id == 0 && evt.buy_order_id != 0) {
        // 买方撤单
        const auto* orig = find_order(evt.channel_id, evt.buy_order_id);
        if (orig) {
            auto pw = make_withdraw(*orig, evt.time, orig->price, evt.volume);
            on_withdraw(66, pw);
            if (evt.volume >= orig->volume) {
                remove_order(evt.channel_id, evt.buy_order_id);
            }
        }
        return;
    }
    if (evt.buy_order_id == 0 && evt.sell_order_id != 0) {
        // 卖方撤单
        const auto* orig = find_order(evt.channel_id, evt.sell_order_id);
        if (orig) {
            auto pw = make_withdraw(*orig, evt.time, orig->price, evt.volume);
            on_withdraw(83, pw);
            if (evt.volume >= orig->volume) {
                remove_order(evt.channel_id, evt.sell_order_id);
            }
        }
        return;
    }

    // 正常成交: tradetype == 70 ('F')
    if (evt.trade_type != 70) return;
    if (evt.buy_order_id == 0 || evt.sell_order_id == 0) return;

    const auto* buy_ord = find_order(evt.channel_id, evt.buy_order_id);
    const auto* sell_ord = find_order(evt.channel_id, evt.sell_order_id);

    // 推断方向
    TradeEvent adjusted = evt;
    adjusted.side = (evt.buy_order_id > evt.sell_order_id) ? 66 : 83;

    auto pt = make_trade(adjusted, buy_ord, sell_ord);
    on_trade(pt);

    // 更新已成交量
    auto* buy_mut = const_cast<ActiveOrder*>(find_order(evt.channel_id, evt.buy_order_id));
    if (buy_mut) {
        buy_mut->traded_vol += evt.volume;
        if (buy_mut->traded_vol >= buy_mut->volume) {
            remove_order(evt.channel_id, evt.buy_order_id);
        }
    }
    auto* sell_mut = const_cast<ActiveOrder*>(find_order(evt.channel_id, evt.sell_order_id));
    if (sell_mut) {
        sell_mut->traded_vol += evt.volume;
        if (sell_mut->traded_vol >= sell_mut->volume) {
            remove_order(evt.channel_id, evt.sell_order_id);
        }
    }
}

} // namespace rtf
