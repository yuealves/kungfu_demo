#include "ts_engine.h"
#include "data_struct.hpp"
#include "sys_messages.h"
#include "JournalReader.h"
#include "Timer.h"

#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <csignal>
#include <cmath>
#include <sys/stat.h>

namespace rtf {

TSEngine::TSEngine(const EngineConfig& cfg) : cfg_(cfg) {}

OrderEvent TSEngine::convert_order(const KyStdOrderType& raw) const {
    OrderEvent evt;
    evt.symbol = raw.Symbol;
    evt.channel_id = static_cast<int32_t>(raw.Channel);
    evt.order_id = raw.OrderNumber;
    evt.time = raw.Time;
    // Journal 中 Price 为原始 ×10000 格式（整数存入 float），需要除以 10000
    evt.price = static_cast<double>(raw.Price) / 10000.0;
    evt.volume = raw.Volume;

    if (is_sh(raw.Symbol)) {
        // SH: FunctionCode 1→B(66), 2→S(83)
        evt.side = (raw.FunctionCode == 1) ? 66 : 83;
        // SH: OrderKind 2→A(65), 10→D(68), 11→S(83,过滤掉)
        if (raw.OrderKind == 2) evt.order_type = 65;
        else if (raw.OrderKind == 10) evt.order_type = 68;
        else evt.order_type = 83; // 将被过滤
    } else {
        // SZ: FunctionCode 1→B(66), 2→S(83)
        evt.side = (raw.FunctionCode == 1) ? 66 : 83;
        // SZ: OrderKind 1→50('2'限价), 2→49('1'对手方), 3→85('U'本方最优)
        if (raw.OrderKind == 1) evt.order_type = 50;
        else if (raw.OrderKind == 2) evt.order_type = 49;
        else if (raw.OrderKind == 3) evt.order_type = 85;
        else evt.order_type = 0;
    }
    return evt;
}

TradeEvent TSEngine::convert_trade(const KyStdTradeType& raw) const {
    TradeEvent evt;
    evt.symbol = raw.Symbol;
    evt.channel_id = raw.Channel;
    evt.buy_order_id = raw.BidOrder;
    evt.sell_order_id = raw.AskOrder;
    evt.time = raw.Time;
    // Journal 中 Price 为原始 ×10000 格式，需要除以 10000
    evt.price = static_cast<double>(raw.Price) / 10000.0;
    evt.volume = raw.Volume;

    if (is_sh(raw.Symbol)) {
        // SH: BSFlag 0→N(78), 1→B(66), 2→S(83)
        if (raw.BSFlag == 1) evt.side = 66;
        else if (raw.BSFlag == 2) evt.side = 83;
        else evt.side = 78; // neutral
        evt.trade_type = 0;
    } else {
        // SZ: FunctionCode 0→F(70成交), 1→4(52撤单)
        if (raw.FunctionCode == 0) evt.trade_type = 70;
        else evt.trade_type = 52;
        evt.side = 0; // SZ 方向由 on_trade_sz 内部推断
    }
    return evt;
}

void TSEngine::process_order(const KyStdOrderType& raw) {
    OrderEvent evt = convert_order(raw);
    order_count_++;

    auto on_order = [this](int8_t side, const ProcessedOrder& po) {
        auto& ss = symbol_states_[po.symbol];
        if (side == 66) ss.buy_order.feed(po);
        else            ss.sell_order.feed(po);
    };

    auto on_withdraw = [this](int8_t side, const ProcessedWithdraw& pw) {
        auto& ss = symbol_states_[pw.symbol];
        if (side == 66) ss.buy_withdraw.feed(pw);
        else            ss.sell_withdraw.feed(pw);
    };

    if (is_sh(raw.Symbol)) {
        if (evt.order_type == 83) return; // 过滤 SH 特殊委托 (OrderKind==11)
        order_store_.on_order_sh(evt, on_order, on_withdraw);
    } else {
        order_store_.on_order_sz(evt, on_order);
    }
}

void TSEngine::process_trade(const KyStdTradeType& raw) {
    TradeEvent evt = convert_trade(raw);
    trade_count_++;

    auto on_order = [this](int8_t side, const ProcessedOrder& po) {
        auto& ss = symbol_states_[po.symbol];
        if (side == 66) ss.buy_order.feed(po);
        else            ss.sell_order.feed(po);
    };

    auto on_withdraw = [this](int8_t side, const ProcessedWithdraw& pw) {
        auto& ss = symbol_states_[pw.symbol];
        if (side == 66) ss.buy_withdraw.feed(pw);
        else            ss.sell_withdraw.feed(pw);
    };

    auto on_trade = [this](const ProcessedTrade& pt) {
        auto& ss = symbol_states_[pt.symbol];
        ss.both_trade.feed(pt);
        if (pt.side == 66)      ss.buy_trade.feed(pt);
        else if (pt.side == 83) ss.sell_trade.feed(pt);
    };

    if (is_sh(raw.Symbol)) {
        order_store_.on_trade_sh(evt, on_order, on_trade);
    } else {
        order_store_.on_trade_sz(evt, on_withdraw, on_trade);
    }
}

void TSEngine::feed_order(const KyStdOrderType& order) {
    process_order(order);
}

void TSEngine::feed_trade(const KyStdTradeType& trade) {
    process_trade(trade);
}

const SymbolState* TSEngine::get_symbol_state(int32_t symbol) const {
    auto it = symbol_states_.find(symbol);
    return (it != symbol_states_.end()) ? &it->second : nullptr;
}

void TSEngine::dump_csv(const std::string& filename) const {
    std::ofstream ofs(filename);
    if (!ofs.is_open()) {
        std::cerr << "[dump_csv] failed to open " << filename << std::endl;
        return;
    }

    auto names = get_factor_names();

    // header
    ofs << "symbol";
    for (const auto& n : names) ofs << "," << n;
    ofs << "\n";

    // data
    std::vector<double> buf(TOTAL_TS_FACTORS);
    for (const auto& [sym, ss] : symbol_states_) {
        ss.write_all_factors(buf.data());
        ofs << sym;
        ofs << std::setprecision(10);
        for (int i = 0; i < TOTAL_TS_FACTORS; ++i) {
            ofs << ",";
            if (std::isnan(buf[i])) ofs << "";
            else ofs << buf[i];
        }
        ofs << "\n";
    }

    std::cerr << "[dump_csv] wrote " << symbol_states_.size()
              << " symbols to " << filename << std::endl;
}

void TSEngine::stop() {
    running_ = false;
}

void TSEngine::run() {
    using namespace kungfu::yijinjing;

    std::vector<std::string> dirs = {cfg_.journal_dir, cfg_.journal_dir, cfg_.journal_dir};
    std::vector<std::string> jnames = {
        "insight_stock_tick_data",
        "insight_stock_order_data",
        "insight_stock_trade_data",
    };

    std::string reader_name = cfg_.reader_name + "_" + parseNano(getNanoTime(), "%H%M%S");
    JournalReaderPtr reader = JournalReader::createReaderWithSys(
        dirs, jnames, 0, reader_name);

    std::cerr << "[TSEngine] reader=" << reader_name
              << " connected to Paged, waiting for data..." << std::endl;

    running_ = true;
    auto last_dump = std::chrono::steady_clock::now();
    auto last_log = std::chrono::steady_clock::now();
    int dump_seq = 0;

    while (running_) {
        FramePtr frame = reader->getNextFrame();
        if (frame.get() == nullptr) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));

            // 定时 dump
            if (cfg_.dump_csv) {
                auto now = std::chrono::steady_clock::now();
                if (std::chrono::duration_cast<std::chrono::seconds>(now - last_dump).count()
                    >= cfg_.dump_interval_seconds) {
                    mkdir(cfg_.csv_output_dir.c_str(), 0755);
                    std::string fname = cfg_.csv_output_dir + "/factors_"
                        + std::to_string(dump_seq++) + ".csv";
                    dump_csv(fname);
                    last_dump = now;
                }
            }
            continue;
        }

        short msg_type = frame->getMsgType();
        void* data = frame->getData();

        if (msg_type == MSG_TYPE_L2_ORDER) {
            process_order(*static_cast<KyStdOrderType*>(data));
        } else if (msg_type == MSG_TYPE_L2_TRADE) {
            process_trade(*static_cast<KyStdTradeType*>(data));
        }
        // 忽略其他 msg_type (tick=61, system messages 等)

        // 每 5 秒打印一次进度
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_log).count() >= 5) {
            std::cerr << "[TSEngine] orders=" << order_count_
                      << " trades=" << trade_count_
                      << " symbols=" << symbol_states_.size()
                      << " active_orders=" << order_store_.active_order_count()
                      << std::endl;
            last_log = now;
        }
    }

    // 最终 dump
    if (cfg_.dump_csv && !symbol_states_.empty()) {
        mkdir(cfg_.csv_output_dir.c_str(), 0755);
        dump_csv(cfg_.csv_output_dir + "/factors_final.csv");
    }

    std::cerr << "[TSEngine] done. orders=" << order_count_
              << " trades=" << trade_count_
              << " symbols=" << symbol_states_.size() << std::endl;
}

} // namespace rtf
