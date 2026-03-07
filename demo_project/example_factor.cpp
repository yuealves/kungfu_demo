/**
 * 流式因子计算入门示例
 *
 * 演示如何：
 *   1. 通过 Paged 实时读取 L2 数据
 *   2. 按股票维护状态（滑动窗口）
 *   3. 每收到一个 tick 增量计算因子并输出
 *
 * 本例实现了 3 个简单因子：
 *   - vwap: 成交量加权平均价（最近 100 个 tick）
 *   - bid_ask_spread: 买卖价差
 *   - active_buy_ratio: 主买比例（最近 200 笔成交）
 *
 * 编译: 在 build/ 下 cmake .. && make example_factor
 * 运行: ./example_factor
 */

#include "JournalReader.h"
#include "Timer.h"
#include "data_struct.hpp"
#include "sys_messages.h"

#include <iostream>
#include <unordered_map>
#include <deque>
#include <csignal>
#include <thread>
#include <chrono>
#include <cmath>
#include <iomanip>

// ============================================================================
// 通用滑动窗口（可直接复用或扩展）
// ============================================================================

template<typename T>
class SlidingWindow {
    std::deque<T> data_;
    size_t max_size_;
    T sum_ = 0;

public:
    explicit SlidingWindow(size_t max_size) : max_size_(max_size) {}

    void push(T val) {
        data_.push_back(val);
        sum_ += val;
        if (data_.size() > max_size_) {
            sum_ -= data_.front();
            data_.pop_front();
        }
    }

    size_t size() const { return data_.size(); }
    bool full() const { return data_.size() >= max_size_; }
    T sum() const { return sum_; }
    double mean() const { return data_.empty() ? 0.0 : (double)sum_ / data_.size(); }

    // 简单的 std 计算（在线算法可优化，这里先用朴素方式）
    double std_dev() const {
        if (data_.size() < 2) return 0.0;
        double m = mean();
        double sq_sum = 0;
        for (auto& v : data_) sq_sum += (v - m) * (v - m);
        return std::sqrt(sq_sum / (data_.size() - 1));
    }

    T front() const { return data_.front(); }
    T back() const { return data_.back(); }
    const std::deque<T>& raw() const { return data_; }
};

// ============================================================================
// 每只股票的状态
// ============================================================================

struct StockState {
    // tick 级别的滑动窗口
    SlidingWindow<double> prices{100};       // 最近 100 个 tick 的价格
    SlidingWindow<double> volumes{100};      // 最近 100 个 tick 的成交量
    SlidingWindow<double> pv{100};           // price * volume，用于计算 VWAP

    // 逐笔成交级别
    SlidingWindow<int> buy_flags{200};       // 最近 200 笔成交的买卖标识（1=外盘, 0=内盘）

    // 最新 tick 的订单簿数据
    double bid1 = 0, ask1 = 0;
    double bid_vol1 = 0, ask_vol1 = 0;

    int tick_count = 0;
};

// ============================================================================
// 因子计算函数
// ============================================================================

// VWAP: 成交量加权平均价
double calc_vwap(const StockState& s) {
    double vol_sum = s.volumes.sum();
    if (vol_sum < 1e-9) return 0.0;
    return s.pv.sum() / vol_sum;
}

// 买卖价差（相对价差）
double calc_spread(const StockState& s) {
    double mid = (s.bid1 + s.ask1) / 2.0;
    if (mid < 1e-9) return 0.0;
    return (s.ask1 - s.bid1) / mid;
}

// 主买比例
double calc_active_buy_ratio(const StockState& s) {
    if (s.buy_flags.size() == 0) return 0.5;
    return s.buy_flags.mean();
}

// ============================================================================
// 主程序
// ============================================================================

static volatile bool g_running = true;
static void signal_handler(int) { g_running = false; }

int main(int argc, const char* argv[])
{
    using namespace kungfu::yijinjing;

    std::string journal_dir = "/shared/kungfu/journal/user/";
    std::string reader_name = "example_factor_" + parseNano(getNanoTime(), "%H%M%S");

    std::vector<std::string> dirs = {journal_dir, journal_dir, journal_dir};
    std::vector<std::string> jnames = {
        "insight_stock_tick_data",
        "insight_stock_order_data",
        "insight_stock_trade_data",
    };

    JournalReaderPtr reader = JournalReader::createReaderWithSys(
        dirs, jnames, 0, reader_name);

    std::cout << "[init] connected to Paged, waiting for data..." << std::endl;

    // 打印 CSV 表头
    std::cout << "symbol,time,price,vwap,spread,active_buy_ratio" << std::endl;

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // 按股票分组维护状态
    std::unordered_map<int, StockState> states;
    long output_count = 0;

    while (g_running) {
        FramePtr frame = reader->getNextFrame();
        if (frame.get() == nullptr) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            continue;
        }

        short msg_type = frame->getMsgType();
        void* data = frame->getData();

        if (msg_type == MSG_TYPE_L2_TICK) {
            // ---- 处理 Tick 快照 ----
            KyStdSnpType* tick = (KyStdSnpType*)data;
            int symbol = tick->Symbol;
            auto& s = states[symbol];

            // 更新滑动窗口
            s.prices.push(tick->Price);
            s.volumes.push(tick->Volume);
            s.pv.push((double)tick->Price * tick->Volume);

            // 更新订单簿
            s.bid1 = tick->BidPx1;
            s.ask1 = tick->AskPx1;
            s.bid_vol1 = tick->BidVol1;
            s.ask_vol1 = tick->AskVol1;

            s.tick_count++;

            // 计算并输出因子（这里每 10 个 tick 输出一次，实际可每个 tick 都输出）
            if (s.tick_count % 10 == 0) {
                double vwap = calc_vwap(s);
                double spread = calc_spread(s);
                double buy_ratio = calc_active_buy_ratio(s);

                std::cout << std::fixed << std::setprecision(4)
                          << symbol << ","
                          << tick->Time << ","
                          << tick->Price << ","
                          << vwap << ","
                          << spread << ","
                          << buy_ratio
                          << std::endl;

                output_count++;
            }
        }
        else if (msg_type == MSG_TYPE_L2_TRADE) {
            // ---- 处理逐笔成交 ----
            KyStdTradeType* trade = (KyStdTradeType*)data;
            int symbol = trade->Symbol;
            auto& s = states[symbol];

            // BSFlag: 'B'=66=外盘（主买），'S'=83=内盘（主卖）
            int is_buy = (trade->BSFlag == 'B') ? 1 : 0;
            s.buy_flags.push(is_buy);
        }
        else if (msg_type == MSG_TYPE_L2_ORDER) {
            // ---- 处理逐笔委托 ----
            // 示例中暂不使用，你可以在这里统计撤单率等
        }
    }

    std::cerr << "[done] output " << output_count << " factor records, "
              << states.size() << " symbols tracked" << std::endl;

    return 0;
}
