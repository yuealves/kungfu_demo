#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE OrderStateTest
#include <boost/test/unit_test.hpp>
#include "order_state.h"
#include <vector>

using namespace rtf;

// Helper: 创建 SH 新单
static OrderEvent make_sh_order(int32_t symbol, int32_t ch, int32_t id,
                                 int32_t tm, double px, int32_t vol,
                                 int8_t side, int8_t type = 65) {
    return {symbol, ch, id, tm, px, vol, side, type};
}

// Helper: 创建 SH 撤单
static OrderEvent make_sh_cancel(int32_t symbol, int32_t ch, int32_t id,
                                  int32_t tm, double px, int32_t vol, int8_t side) {
    return {symbol, ch, id, tm, px, vol, side, 68};
}

// Helper: 创建 SH 成交
static TradeEvent make_sh_trade(int32_t symbol, int32_t ch, int32_t buyid, int32_t sellid,
                                 int32_t tm, double px, int32_t vol, int8_t side) {
    return {symbol, ch, buyid, sellid, tm, px, vol, side, 0};
}

// Helper: 创建 SZ 委托
static OrderEvent make_sz_order(int32_t symbol, int32_t ch, int32_t id,
                                 int32_t tm, double px, int32_t vol,
                                 int8_t side, int8_t type = 50) {
    return {symbol, ch, id, tm, px, vol, side, type};
}

// Helper: 创建 SZ 成交
static TradeEvent make_sz_trade(int32_t symbol, int32_t ch, int32_t buyid, int32_t sellid,
                                 int32_t tm, double px, int32_t vol, int8_t trade_type = 70) {
    return {symbol, ch, buyid, sellid, tm, px, vol, 0, trade_type};
}

BOOST_AUTO_TEST_CASE(test_sh_new_order_and_cancel) {
    OrderStateStore store;
    std::vector<ProcessedOrder> orders;
    std::vector<ProcessedWithdraw> withdraws;

    auto on_order = [&](int8_t, const ProcessedOrder& o) { orders.push_back(o); };
    auto on_withdraw = [&](int8_t, const ProcessedWithdraw& w) { withdraws.push_back(w); };

    // 新买单
    auto evt = make_sh_order(600000, 1, 100, 93000000, 10.5, 1000, 66);
    store.on_order_sh(evt, on_order, on_withdraw);
    BOOST_CHECK_EQUAL(orders.size(), 1u);
    BOOST_CHECK(store.find_order(1, 100) != nullptr);

    // 撤单
    auto cancel = make_sh_cancel(600000, 1, 100, 93001000, 10.5, 1000, 66);
    store.on_order_sh(cancel, on_order, on_withdraw);
    BOOST_CHECK_EQUAL(withdraws.size(), 1u);
    BOOST_CHECK_EQUAL(withdraws[0].withdraw_vol, 1000);
    // 全撤 → 订单移除
    BOOST_CHECK(store.find_order(1, 100) == nullptr);
}

BOOST_AUTO_TEST_CASE(test_sh_aggressive_order_from_trade) {
    OrderStateStore store;
    std::vector<ProcessedOrder> orders;
    std::vector<ProcessedTrade> trades;

    auto on_order = [&](int8_t, const ProcessedOrder& o) { orders.push_back(o); };
    auto on_trade = [&](const ProcessedTrade& t) { trades.push_back(t); };

    // 先注册一个卖单
    auto sell = make_sh_order(600000, 1, 200, 93000000, 10.5, 500, 83);
    store.on_order_sh(sell, on_order, [](int8_t, const ProcessedWithdraw&){});
    BOOST_CHECK_EQUAL(orders.size(), 1u);

    // 买方主动成交，但买单 (id=300) 不在 order stream 中 → 激进单补全
    auto trade = make_sh_trade(600000, 1, 300, 200, 93001000, 10.5, 200, 66);
    store.on_trade_sh(trade, on_order, on_trade);

    BOOST_CHECK_EQUAL(orders.size(), 2u);  // 补全了激进买单
    BOOST_CHECK_EQUAL(orders[1].order_id, 300);
    BOOST_CHECK_EQUAL(trades.size(), 1u);
    BOOST_CHECK_EQUAL(trades[0].buy_order_id, 300);
}

BOOST_AUTO_TEST_CASE(test_sz_cancel_from_trade_stream) {
    OrderStateStore store;
    std::vector<ProcessedOrder> orders;
    std::vector<ProcessedWithdraw> withdraws;
    std::vector<ProcessedTrade> trades;

    auto on_order = [&](int8_t, const ProcessedOrder& o) { orders.push_back(o); };
    auto on_withdraw = [&](int8_t, const ProcessedWithdraw& w) { withdraws.push_back(w); };
    auto on_trade = [&](const ProcessedTrade& t) { trades.push_back(t); };

    // 注册一个深圳买单
    auto buy = make_sz_order(1000, 1, 100, 93000000, 10.0, 500, 66);
    store.on_order_sz(buy, on_order);
    BOOST_CHECK_EQUAL(orders.size(), 1u);

    // 深圳撤单: tradesellid==0, tradebuyid==100
    auto cancel = make_sz_trade(1000, 1, 100, 0, 93001000, 0, 500, 52);
    store.on_trade_sz(cancel, on_withdraw, on_trade);

    BOOST_CHECK_EQUAL(withdraws.size(), 1u);
    BOOST_CHECK_EQUAL(withdraws[0].withdraw_id, 100);
    BOOST_CHECK_EQUAL(trades.size(), 0u);
}

BOOST_AUTO_TEST_CASE(test_sz_market_order_price_backfill) {
    OrderStateStore store;
    std::vector<ProcessedOrder> orders;

    auto on_order = [&](int8_t, const ProcessedOrder& o) { orders.push_back(o); };
    auto on_withdraw = [&](int8_t, const ProcessedWithdraw&) {};
    auto on_trade = [&](const ProcessedTrade&) {};

    // 先有一笔成交，建立最新价
    // buyid(50) < sellid(60) → inferred_side=83(sell)
    // 所以更新 sz_latest_trade_px_[symbol=1000, side=83] = 10.0
    auto trade = make_sz_trade(1000, 1, 50, 60, 93000000, 10.0, 100);
    store.on_trade_sz(trade, on_withdraw, on_trade);

    // 买方市价单 (ordertype=49, 对手方最优)
    // 买单 lookup_side=83 → 查到 10.0 → price = 10.0 * 1.001
    auto buy_mkt = make_sz_order(1000, 1, 200, 93001000, 0.0, 100, 66, 49);
    store.on_order_sz(buy_mkt, on_order);

    BOOST_CHECK_EQUAL(orders.size(), 1u);
    BOOST_CHECK_CLOSE(orders[0].price, 10.0 * 1.001, 1e-6);
}

BOOST_AUTO_TEST_CASE(test_sz_trade_direction_inference) {
    OrderStateStore store;
    std::vector<ProcessedOrder> orders;
    std::vector<ProcessedTrade> trades;

    auto on_order = [&](int8_t, const ProcessedOrder& o) { orders.push_back(o); };
    auto on_withdraw = [&](int8_t, const ProcessedWithdraw&) {};
    auto on_trade = [&](const ProcessedTrade& t) { trades.push_back(t); };

    // 注册买卖双方
    store.on_order_sz(make_sz_order(1000, 1, 100, 93000000, 10.0, 500, 66), on_order);
    store.on_order_sz(make_sz_order(1000, 1, 200, 93000000, 10.0, 500, 83), on_order);

    // buyid(100) < sellid(200) → inferred_side=83(sell)
    auto trade = make_sz_trade(1000, 1, 100, 200, 93001000, 10.0, 100);
    store.on_trade_sz(trade, on_withdraw, on_trade);

    BOOST_CHECK_EQUAL(trades.size(), 1u);
    BOOST_CHECK_EQUAL(trades[0].side, 83); // sell side
}
