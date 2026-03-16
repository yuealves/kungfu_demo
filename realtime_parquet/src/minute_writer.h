#pragma once

#include "data_types.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include <memory>
#include <string>

static constexpr int ROW_GROUP_SIZE = 50000;

// MinuteWriter: 管理单个 Parquet 文件的增量写入
// 生命周期: open() → append_*() × N → close()
// 内部自动按 ROW_GROUP_SIZE 分 Row Group 写出（预压缩）
class MinuteWriter {
public:
    enum Type { TICK, ORDER, TRADE };

    explicit MinuteWriter(Type type);

    void open(const std::string& filepath);
    void append_tick(const KyStdSnpType* md, int64_t nano);
    void append_order(const KyStdOrderType* md, int64_t nano);
    void append_trade(const KyStdTradeType* md, int64_t nano);
    void flush();   // 写出当前 buffer 作为一个 Row Group
    void close();   // flush 残余 + 关闭文件
    long row_count() const { return total_rows_; }

private:
    static std::shared_ptr<arrow::Schema> tick_schema();
    static std::shared_ptr<arrow::Schema> order_schema();
    static std::shared_ptr<arrow::Schema> trade_schema();

    static std::string decode_exchange(int symbol);

    void init_builders();
    void flush_tick();
    void flush_order();
    void flush_trade();

    Type type_;
    std::shared_ptr<arrow::Schema> schema_;

    // Parquet file (lifetime = one minute)
    std::shared_ptr<arrow::io::FileOutputStream> outfile_;
    std::unique_ptr<parquet::arrow::FileWriter> writer_;

    // Arrow builders (lifetime = one Row Group, rebuilt after each flush)
    // Tick builders
    std::unique_ptr<arrow::Int64Builder>  t_nano_ts_;
    std::unique_ptr<arrow::StringBuilder> t_exchange_;
    std::unique_ptr<arrow::Int32Builder>  t_sym_, t_time_, t_afterMatchItem_, t_afterVolume_,
                                          t_matchItem_, t_volume_, t_bizIndex_;
    std::unique_ptr<arrow::Int64Builder>  t_accTurnover_, t_accVolume_, t_afterTurnover_,
                                          t_totalAskVol_, t_totalBidVol_, t_turnover_;
    std::unique_ptr<arrow::FloatBuilder>  t_afterPrice_, t_askAvgPrice_, t_bidAvgPrice_,
                                          t_high_, t_low_, t_open_, t_preClose_, t_price_;
    std::unique_ptr<arrow::FloatBuilder>  t_askPx_[10], t_bidPx_[10];
    std::unique_ptr<arrow::DoubleBuilder> t_askVol_[10], t_bidVol_[10];
    std::unique_ptr<arrow::Int8Builder>   t_bsFlag_;

    // Order builders
    std::unique_ptr<arrow::Int64Builder>  o_nano_ts_, o_channel_;
    std::unique_ptr<arrow::StringBuilder> o_exchange_;
    std::unique_ptr<arrow::Int32Builder>  o_sym_, o_bizIndex_, o_orderNumber_, o_orderOriNo_,
                                          o_time_, o_volume_;
    std::unique_ptr<arrow::Int8Builder>   o_functionCode_, o_orderKind_;
    std::unique_ptr<arrow::FloatBuilder>  o_price_;

    // Trade builders
    std::unique_ptr<arrow::Int64Builder>  d_nano_ts_;
    std::unique_ptr<arrow::StringBuilder> d_exchange_;
    std::unique_ptr<arrow::Int32Builder>  d_sym_, d_askOrder_, d_bidOrder_, d_bizIndex_,
                                          d_channel_, d_index_, d_time_, d_volume_;
    std::unique_ptr<arrow::Int8Builder>   d_bsFlag_, d_functionCode_, d_orderKind_;
    std::unique_ptr<arrow::FloatBuilder>  d_price_;

    std::string writing_path_;  // 写入时的临时路径（dot 前缀）
    std::string final_path_;    // close 后 rename 的最终路径

    int buffered_rows_ = 0;
    long total_rows_ = 0;
    bool is_open_ = false;
};
