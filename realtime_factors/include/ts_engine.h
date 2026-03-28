#pragma once
#include "symbol_state.h"
#include "order_state.h"
#include "factor_schema.h"

#include <unordered_map>
#include <string>
#include <vector>
#include <atomic>

// Forward declare packed structs from parent project
struct KyStdOrderType;
struct KyStdTradeType;

namespace rtf {

struct EngineConfig {
    std::string journal_dir = "/shared/kungfu/journal/user/";
    std::string reader_name = "rtf_engine";
    int dump_interval_seconds = 60;  // 每 N 秒 dump 一次
    bool dump_csv = true;
    std::string csv_output_dir = "/tmp/rtf_output/";
};

class TSEngine {
public:
    explicit TSEngine(const EngineConfig& cfg);

    // 主循环（阻塞，直到 stop() 被调用）
    void run();
    void stop();

    // 手动灌入事件（用于测试，不经过 Journal）
    void feed_order(const KyStdOrderType& order);
    void feed_trade(const KyStdTradeType& trade);

    // 获取某只股票的因子状态（用于测试）
    const SymbolState* get_symbol_state(int32_t symbol) const;

    // 导出全市场因子快照到 CSV
    void dump_csv(const std::string& filename) const;

    // 获取已处理事件计数
    int64_t order_count() const { return order_count_; }
    int64_t trade_count() const { return trade_count_; }
    size_t symbol_count() const { return symbol_states_.size(); }

private:
    EngineConfig cfg_;
    std::atomic<bool> running_{false};

    OrderStateStore order_store_;
    std::unordered_map<int32_t, SymbolState> symbol_states_;

    int64_t order_count_ = 0;
    int64_t trade_count_ = 0;

    // Journal → 内部事件转换
    OrderEvent convert_order(const KyStdOrderType& raw) const;
    TradeEvent convert_trade(const KyStdTradeType& raw) const;

    // 处理单条事件
    void process_order(const KyStdOrderType& raw);
    void process_trade(const KyStdTradeType& raw);
};

} // namespace rtf
