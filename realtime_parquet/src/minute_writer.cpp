#include "minute_writer.h"

#include <iostream>
#include <stdexcept>

#define ARROW_OK_OR_THROW(expr)                                  \
    do {                                                         \
        auto _st = (expr);                                       \
        if (!_st.ok()) {                                         \
            throw std::runtime_error(_st.ToString());            \
        }                                                        \
    } while (0)

#define ARROW_ASSIGN_OR_THROW(lhs, expr)                         \
    do {                                                         \
        auto _res = (expr);                                      \
        if (!_res.ok()) {                                        \
            throw std::runtime_error(_res.status().ToString());  \
        }                                                        \
        lhs = std::move(*_res);                                  \
    } while (0)

// ---------------------------------------------------------------------------
// Schema 定义（与 archive_journal_demo 保持一致）
// ---------------------------------------------------------------------------

std::shared_ptr<arrow::Schema> MinuteWriter::tick_schema() {
    return arrow::schema({
        arrow::field("nano_timestamp", arrow::int64()),
        arrow::field("exchange", arrow::utf8()),
        arrow::field("Symbol", arrow::int32()),
        arrow::field("Time", arrow::int32()),
        arrow::field("AccTurnover", arrow::int64()),
        arrow::field("AccVolume", arrow::int64()),
        arrow::field("AfterMatchItem", arrow::int32()),
        arrow::field("AfterPrice", arrow::float32()),
        arrow::field("AfterTurnover", arrow::int64()),
        arrow::field("AfterVolume", arrow::int32()),
        arrow::field("AskAvgPrice", arrow::float32()),
        arrow::field("AskPx1", arrow::float32()),
        arrow::field("AskPx2", arrow::float32()),
        arrow::field("AskPx3", arrow::float32()),
        arrow::field("AskPx4", arrow::float32()),
        arrow::field("AskPx5", arrow::float32()),
        arrow::field("AskPx6", arrow::float32()),
        arrow::field("AskPx7", arrow::float32()),
        arrow::field("AskPx8", arrow::float32()),
        arrow::field("AskPx9", arrow::float32()),
        arrow::field("AskPx10", arrow::float32()),
        arrow::field("AskVol1", arrow::float64()),
        arrow::field("AskVol2", arrow::float64()),
        arrow::field("AskVol3", arrow::float64()),
        arrow::field("AskVol4", arrow::float64()),
        arrow::field("AskVol5", arrow::float64()),
        arrow::field("AskVol6", arrow::float64()),
        arrow::field("AskVol7", arrow::float64()),
        arrow::field("AskVol8", arrow::float64()),
        arrow::field("AskVol9", arrow::float64()),
        arrow::field("AskVol10", arrow::float64()),
        arrow::field("BSFlag", arrow::int8()),
        arrow::field("BidAvgPrice", arrow::float32()),
        arrow::field("BidPx1", arrow::float32()),
        arrow::field("BidPx2", arrow::float32()),
        arrow::field("BidPx3", arrow::float32()),
        arrow::field("BidPx4", arrow::float32()),
        arrow::field("BidPx5", arrow::float32()),
        arrow::field("BidPx6", arrow::float32()),
        arrow::field("BidPx7", arrow::float32()),
        arrow::field("BidPx8", arrow::float32()),
        arrow::field("BidPx9", arrow::float32()),
        arrow::field("BidPx10", arrow::float32()),
        arrow::field("BidVol1", arrow::float64()),
        arrow::field("BidVol2", arrow::float64()),
        arrow::field("BidVol3", arrow::float64()),
        arrow::field("BidVol4", arrow::float64()),
        arrow::field("BidVol5", arrow::float64()),
        arrow::field("BidVol6", arrow::float64()),
        arrow::field("BidVol7", arrow::float64()),
        arrow::field("BidVol8", arrow::float64()),
        arrow::field("BidVol9", arrow::float64()),
        arrow::field("BidVol10", arrow::float64()),
        arrow::field("High", arrow::float32()),
        arrow::field("Low", arrow::float32()),
        arrow::field("MatchItem", arrow::int32()),
        arrow::field("Open", arrow::float32()),
        arrow::field("PreClose", arrow::float32()),
        arrow::field("Price", arrow::float32()),
        arrow::field("TotalAskVolume", arrow::int64()),
        arrow::field("TotalBidVolume", arrow::int64()),
        arrow::field("Turnover", arrow::int64()),
        arrow::field("Volume", arrow::int32()),
        arrow::field("BizIndex", arrow::int32()),
    });
}

std::shared_ptr<arrow::Schema> MinuteWriter::order_schema() {
    return arrow::schema({
        arrow::field("nano_timestamp", arrow::int64()),
        arrow::field("exchange", arrow::utf8()),
        arrow::field("Symbol", arrow::int32()),
        arrow::field("BizIndex", arrow::int32()),
        arrow::field("Channel", arrow::int64()),
        arrow::field("FunctionCode", arrow::int8()),
        arrow::field("OrderKind", arrow::int8()),
        arrow::field("OrderNumber", arrow::int32()),
        arrow::field("OrderOriNo", arrow::int32()),
        arrow::field("Price", arrow::float32()),
        arrow::field("Time", arrow::int32()),
        arrow::field("Volume", arrow::int32()),
    });
}

std::shared_ptr<arrow::Schema> MinuteWriter::trade_schema() {
    return arrow::schema({
        arrow::field("nano_timestamp", arrow::int64()),
        arrow::field("exchange", arrow::utf8()),
        arrow::field("Symbol", arrow::int32()),
        arrow::field("AskOrder", arrow::int32()),
        arrow::field("BSFlag", arrow::int8()),
        arrow::field("BidOrder", arrow::int32()),
        arrow::field("BizIndex", arrow::int32()),
        arrow::field("Channel", arrow::int32()),
        arrow::field("FunctionCode", arrow::int8()),
        arrow::field("Index", arrow::int32()),
        arrow::field("OrderKind", arrow::int8()),
        arrow::field("Price", arrow::float32()),
        arrow::field("Time", arrow::int32()),
        arrow::field("Volume", arrow::int32()),
    });
}

std::string MinuteWriter::decode_exchange(int symbol) {
    if (symbol < 400000)
        return "SZ";
    else if (symbol < 700000)
        return "SH";
    return "";
}

// ---------------------------------------------------------------------------
// 构造 / Builder 初始化
// ---------------------------------------------------------------------------

MinuteWriter::MinuteWriter(Type type) : type_(type) {
    switch (type_) {
        case TICK:  schema_ = tick_schema();  break;
        case ORDER: schema_ = order_schema(); break;
        case TRADE: schema_ = trade_schema(); break;
    }
}

void MinuteWriter::init_builders() {
    buffered_rows_ = 0;

    if (type_ == TICK) {
        t_nano_ts_       = std::make_unique<arrow::Int64Builder>();
        t_exchange_      = std::make_unique<arrow::StringBuilder>();
        t_sym_           = std::make_unique<arrow::Int32Builder>();
        t_time_          = std::make_unique<arrow::Int32Builder>();
        t_accTurnover_   = std::make_unique<arrow::Int64Builder>();
        t_accVolume_     = std::make_unique<arrow::Int64Builder>();
        t_afterMatchItem_= std::make_unique<arrow::Int32Builder>();
        t_afterPrice_    = std::make_unique<arrow::FloatBuilder>();
        t_afterTurnover_ = std::make_unique<arrow::Int64Builder>();
        t_afterVolume_   = std::make_unique<arrow::Int32Builder>();
        t_askAvgPrice_   = std::make_unique<arrow::FloatBuilder>();
        for (int i = 0; i < 10; ++i) {
            t_askPx_[i]  = std::make_unique<arrow::FloatBuilder>();
            t_askVol_[i] = std::make_unique<arrow::DoubleBuilder>();
            t_bidPx_[i]  = std::make_unique<arrow::FloatBuilder>();
            t_bidVol_[i] = std::make_unique<arrow::DoubleBuilder>();
        }
        t_bsFlag_        = std::make_unique<arrow::Int8Builder>();
        t_bidAvgPrice_   = std::make_unique<arrow::FloatBuilder>();
        t_high_          = std::make_unique<arrow::FloatBuilder>();
        t_low_           = std::make_unique<arrow::FloatBuilder>();
        t_matchItem_     = std::make_unique<arrow::Int32Builder>();
        t_open_          = std::make_unique<arrow::FloatBuilder>();
        t_preClose_      = std::make_unique<arrow::FloatBuilder>();
        t_price_         = std::make_unique<arrow::FloatBuilder>();
        t_totalAskVol_   = std::make_unique<arrow::Int64Builder>();
        t_totalBidVol_   = std::make_unique<arrow::Int64Builder>();
        t_turnover_      = std::make_unique<arrow::Int64Builder>();
        t_volume_        = std::make_unique<arrow::Int32Builder>();
        t_bizIndex_      = std::make_unique<arrow::Int32Builder>();
    } else if (type_ == ORDER) {
        o_nano_ts_       = std::make_unique<arrow::Int64Builder>();
        o_exchange_      = std::make_unique<arrow::StringBuilder>();
        o_sym_           = std::make_unique<arrow::Int32Builder>();
        o_bizIndex_      = std::make_unique<arrow::Int32Builder>();
        o_channel_       = std::make_unique<arrow::Int64Builder>();
        o_functionCode_  = std::make_unique<arrow::Int8Builder>();
        o_orderKind_     = std::make_unique<arrow::Int8Builder>();
        o_orderNumber_   = std::make_unique<arrow::Int32Builder>();
        o_orderOriNo_    = std::make_unique<arrow::Int32Builder>();
        o_price_         = std::make_unique<arrow::FloatBuilder>();
        o_time_          = std::make_unique<arrow::Int32Builder>();
        o_volume_        = std::make_unique<arrow::Int32Builder>();
    } else {
        d_nano_ts_       = std::make_unique<arrow::Int64Builder>();
        d_exchange_      = std::make_unique<arrow::StringBuilder>();
        d_sym_           = std::make_unique<arrow::Int32Builder>();
        d_askOrder_      = std::make_unique<arrow::Int32Builder>();
        d_bsFlag_        = std::make_unique<arrow::Int8Builder>();
        d_bidOrder_      = std::make_unique<arrow::Int32Builder>();
        d_bizIndex_      = std::make_unique<arrow::Int32Builder>();
        d_channel_       = std::make_unique<arrow::Int32Builder>();
        d_functionCode_  = std::make_unique<arrow::Int8Builder>();
        d_index_         = std::make_unique<arrow::Int32Builder>();
        d_orderKind_     = std::make_unique<arrow::Int8Builder>();
        d_price_         = std::make_unique<arrow::FloatBuilder>();
        d_time_          = std::make_unique<arrow::Int32Builder>();
        d_volume_        = std::make_unique<arrow::Int32Builder>();
    }
}

// ---------------------------------------------------------------------------
// open / close
// ---------------------------------------------------------------------------

void MinuteWriter::open(const std::string& filepath) {
    ARROW_ASSIGN_OR_THROW(outfile_, arrow::io::FileOutputStream::Open(filepath));

    auto props = parquet::WriterProperties::Builder()
        .compression(arrow::Compression::SNAPPY)
        ->build();
    auto arrow_props = parquet::ArrowWriterProperties::Builder()
        .set_use_threads(true)
        ->store_schema()->build();

    ARROW_ASSIGN_OR_THROW(writer_, parquet::arrow::FileWriter::Open(
        *schema_, arrow::default_memory_pool(), outfile_, props, arrow_props));

    total_rows_ = 0;
    init_builders();
    is_open_ = true;
}

void MinuteWriter::close() {
    if (!is_open_) return;
    if (buffered_rows_ > 0) flush();
    ARROW_OK_OR_THROW(writer_->Close());
    ARROW_OK_OR_THROW(outfile_->Close());
    writer_.reset();
    outfile_.reset();
    is_open_ = false;
}

// ---------------------------------------------------------------------------
// append — 直接从 packed struct 填充 Arrow builders
// ---------------------------------------------------------------------------

void MinuteWriter::append_tick(const KyStdSnpType* md, int64_t nano) {
    std::string exch = decode_exchange(md->Symbol);

    ARROW_OK_OR_THROW(t_nano_ts_->Append(nano));
    ARROW_OK_OR_THROW(t_exchange_->Append(exch));
    ARROW_OK_OR_THROW(t_sym_->Append(md->Symbol));
    ARROW_OK_OR_THROW(t_time_->Append(md->Time));
    ARROW_OK_OR_THROW(t_accTurnover_->Append(md->AccTurnover));
    ARROW_OK_OR_THROW(t_accVolume_->Append(md->AccVolume));
    ARROW_OK_OR_THROW(t_afterMatchItem_->Append(md->AfterMatchItem));
    ARROW_OK_OR_THROW(t_afterPrice_->Append(md->AfterPrice));
    ARROW_OK_OR_THROW(t_afterTurnover_->Append(md->AfterTurnover));
    ARROW_OK_OR_THROW(t_afterVolume_->Append(md->AfterVolume));
    ARROW_OK_OR_THROW(t_askAvgPrice_->Append(md->AskAvgPrice));

    // AskPx1..AskPx10
    const float* askPx = &md->AskPx1;
    for (int i = 0; i < 10; ++i) ARROW_OK_OR_THROW(t_askPx_[i]->Append(askPx[i]));

    // AskVol1..AskVol10
    const float64_t* askVol = &md->AskVol1;
    for (int i = 0; i < 10; ++i) ARROW_OK_OR_THROW(t_askVol_[i]->Append(askVol[i]));

    ARROW_OK_OR_THROW(t_bsFlag_->Append(md->BSFlag));
    ARROW_OK_OR_THROW(t_bidAvgPrice_->Append(md->BidAvgPrice));

    // BidPx1..BidPx10
    const float* bidPx = &md->BidPx1;
    for (int i = 0; i < 10; ++i) ARROW_OK_OR_THROW(t_bidPx_[i]->Append(bidPx[i]));

    // BidVol1..BidVol10
    const float64_t* bidVol = &md->BidVol1;
    for (int i = 0; i < 10; ++i) ARROW_OK_OR_THROW(t_bidVol_[i]->Append(bidVol[i]));

    ARROW_OK_OR_THROW(t_high_->Append(md->High));
    ARROW_OK_OR_THROW(t_low_->Append(md->Low));
    ARROW_OK_OR_THROW(t_matchItem_->Append(md->MatchItem));
    ARROW_OK_OR_THROW(t_open_->Append(md->Open));
    ARROW_OK_OR_THROW(t_preClose_->Append(md->PreClose));
    ARROW_OK_OR_THROW(t_price_->Append(md->Price));
    ARROW_OK_OR_THROW(t_totalAskVol_->Append(md->TotalAskVolume));
    ARROW_OK_OR_THROW(t_totalBidVol_->Append(md->TotalBidVolume));
    ARROW_OK_OR_THROW(t_turnover_->Append(md->Turnover));
    ARROW_OK_OR_THROW(t_volume_->Append(md->Volume));
    ARROW_OK_OR_THROW(t_bizIndex_->Append(md->BizIndex));

    ++buffered_rows_;
    ++total_rows_;
    if (buffered_rows_ >= ROW_GROUP_SIZE) flush();
}

void MinuteWriter::append_order(const KyStdOrderType* md, int64_t nano) {
    std::string exch = decode_exchange(md->Symbol);

    ARROW_OK_OR_THROW(o_nano_ts_->Append(nano));
    ARROW_OK_OR_THROW(o_exchange_->Append(exch));
    ARROW_OK_OR_THROW(o_sym_->Append(md->Symbol));
    ARROW_OK_OR_THROW(o_bizIndex_->Append(md->BizIndex));
    ARROW_OK_OR_THROW(o_channel_->Append(md->Channel));
    ARROW_OK_OR_THROW(o_functionCode_->Append(md->FunctionCode));
    ARROW_OK_OR_THROW(o_orderKind_->Append(md->OrderKind));
    ARROW_OK_OR_THROW(o_orderNumber_->Append(md->OrderNumber));
    ARROW_OK_OR_THROW(o_orderOriNo_->Append(md->OrderOriNo));
    ARROW_OK_OR_THROW(o_price_->Append(md->Price));
    ARROW_OK_OR_THROW(o_time_->Append(md->Time));
    ARROW_OK_OR_THROW(o_volume_->Append(md->Volume));

    ++buffered_rows_;
    ++total_rows_;
    if (buffered_rows_ >= ROW_GROUP_SIZE) flush();
}

void MinuteWriter::append_trade(const KyStdTradeType* md, int64_t nano) {
    std::string exch = decode_exchange(md->Symbol);

    ARROW_OK_OR_THROW(d_nano_ts_->Append(nano));
    ARROW_OK_OR_THROW(d_exchange_->Append(exch));
    ARROW_OK_OR_THROW(d_sym_->Append(md->Symbol));
    ARROW_OK_OR_THROW(d_askOrder_->Append(md->AskOrder));
    ARROW_OK_OR_THROW(d_bsFlag_->Append(md->BSFlag));
    ARROW_OK_OR_THROW(d_bidOrder_->Append(md->BidOrder));
    ARROW_OK_OR_THROW(d_bizIndex_->Append(md->BizIndex));
    ARROW_OK_OR_THROW(d_channel_->Append(md->Channel));
    ARROW_OK_OR_THROW(d_functionCode_->Append(md->FunctionCode));
    ARROW_OK_OR_THROW(d_index_->Append(md->Index));
    ARROW_OK_OR_THROW(d_orderKind_->Append(md->OrderKind));
    ARROW_OK_OR_THROW(d_price_->Append(md->Price));
    ARROW_OK_OR_THROW(d_time_->Append(md->Time));
    ARROW_OK_OR_THROW(d_volume_->Append(md->Volume));

    ++buffered_rows_;
    ++total_rows_;
    if (buffered_rows_ >= ROW_GROUP_SIZE) flush();
}

// ---------------------------------------------------------------------------
// flush — finish builders → Table → WriteTable → 重建 builders
// ---------------------------------------------------------------------------

void MinuteWriter::flush() {
    if (buffered_rows_ == 0) return;

    switch (type_) {
        case TICK:  flush_tick();  break;
        case ORDER: flush_order(); break;
        case TRADE: flush_trade(); break;
    }

    init_builders();
}

void MinuteWriter::flush_tick() {
    std::shared_ptr<arrow::Array> a_nano, a_exch, a_sym, a_time, a_accTO, a_accVol,
        a_afterMI, a_afterP, a_afterTO, a_afterVol, a_askAvg,
        a_bsFlag, a_bidAvg,
        a_high, a_low, a_matchItem, a_open, a_preClose, a_price,
        a_totalAskVol, a_totalBidVol, a_turnover, a_volume, a_bizIndex;
    std::shared_ptr<arrow::Array> a_askPx[10], a_askVol[10], a_bidPx[10], a_bidVol[10];

    ARROW_OK_OR_THROW(t_nano_ts_->Finish(&a_nano));
    ARROW_OK_OR_THROW(t_exchange_->Finish(&a_exch));
    ARROW_OK_OR_THROW(t_sym_->Finish(&a_sym));
    ARROW_OK_OR_THROW(t_time_->Finish(&a_time));
    ARROW_OK_OR_THROW(t_accTurnover_->Finish(&a_accTO));
    ARROW_OK_OR_THROW(t_accVolume_->Finish(&a_accVol));
    ARROW_OK_OR_THROW(t_afterMatchItem_->Finish(&a_afterMI));
    ARROW_OK_OR_THROW(t_afterPrice_->Finish(&a_afterP));
    ARROW_OK_OR_THROW(t_afterTurnover_->Finish(&a_afterTO));
    ARROW_OK_OR_THROW(t_afterVolume_->Finish(&a_afterVol));
    ARROW_OK_OR_THROW(t_askAvgPrice_->Finish(&a_askAvg));
    for (int j = 0; j < 10; ++j) ARROW_OK_OR_THROW(t_askPx_[j]->Finish(&a_askPx[j]));
    for (int j = 0; j < 10; ++j) ARROW_OK_OR_THROW(t_askVol_[j]->Finish(&a_askVol[j]));
    ARROW_OK_OR_THROW(t_bsFlag_->Finish(&a_bsFlag));
    ARROW_OK_OR_THROW(t_bidAvgPrice_->Finish(&a_bidAvg));
    for (int j = 0; j < 10; ++j) ARROW_OK_OR_THROW(t_bidPx_[j]->Finish(&a_bidPx[j]));
    for (int j = 0; j < 10; ++j) ARROW_OK_OR_THROW(t_bidVol_[j]->Finish(&a_bidVol[j]));
    ARROW_OK_OR_THROW(t_high_->Finish(&a_high));
    ARROW_OK_OR_THROW(t_low_->Finish(&a_low));
    ARROW_OK_OR_THROW(t_matchItem_->Finish(&a_matchItem));
    ARROW_OK_OR_THROW(t_open_->Finish(&a_open));
    ARROW_OK_OR_THROW(t_preClose_->Finish(&a_preClose));
    ARROW_OK_OR_THROW(t_price_->Finish(&a_price));
    ARROW_OK_OR_THROW(t_totalAskVol_->Finish(&a_totalAskVol));
    ARROW_OK_OR_THROW(t_totalBidVol_->Finish(&a_totalBidVol));
    ARROW_OK_OR_THROW(t_turnover_->Finish(&a_turnover));
    ARROW_OK_OR_THROW(t_volume_->Finish(&a_volume));
    ARROW_OK_OR_THROW(t_bizIndex_->Finish(&a_bizIndex));

    std::vector<std::shared_ptr<arrow::Array>> columns = {
        a_nano, a_exch, a_sym, a_time, a_accTO, a_accVol,
        a_afterMI, a_afterP, a_afterTO, a_afterVol, a_askAvg,
    };
    for (int j = 0; j < 10; ++j) columns.push_back(a_askPx[j]);
    for (int j = 0; j < 10; ++j) columns.push_back(a_askVol[j]);
    columns.push_back(a_bsFlag);
    columns.push_back(a_bidAvg);
    for (int j = 0; j < 10; ++j) columns.push_back(a_bidPx[j]);
    for (int j = 0; j < 10; ++j) columns.push_back(a_bidVol[j]);
    columns.insert(columns.end(), {
        a_high, a_low, a_matchItem, a_open, a_preClose, a_price,
        a_totalAskVol, a_totalBidVol, a_turnover, a_volume, a_bizIndex,
    });

    auto table = arrow::Table::Make(schema_, columns);
    ARROW_OK_OR_THROW(writer_->WriteTable(*table, buffered_rows_));
}

void MinuteWriter::flush_order() {
    std::shared_ptr<arrow::Array> a_nano, a_exch, a_sym, a_bizIndex, a_channel,
        a_funcCode, a_orderKind, a_orderNum, a_orderOriNo, a_price, a_time, a_volume;

    ARROW_OK_OR_THROW(o_nano_ts_->Finish(&a_nano));
    ARROW_OK_OR_THROW(o_exchange_->Finish(&a_exch));
    ARROW_OK_OR_THROW(o_sym_->Finish(&a_sym));
    ARROW_OK_OR_THROW(o_bizIndex_->Finish(&a_bizIndex));
    ARROW_OK_OR_THROW(o_channel_->Finish(&a_channel));
    ARROW_OK_OR_THROW(o_functionCode_->Finish(&a_funcCode));
    ARROW_OK_OR_THROW(o_orderKind_->Finish(&a_orderKind));
    ARROW_OK_OR_THROW(o_orderNumber_->Finish(&a_orderNum));
    ARROW_OK_OR_THROW(o_orderOriNo_->Finish(&a_orderOriNo));
    ARROW_OK_OR_THROW(o_price_->Finish(&a_price));
    ARROW_OK_OR_THROW(o_time_->Finish(&a_time));
    ARROW_OK_OR_THROW(o_volume_->Finish(&a_volume));

    auto table = arrow::Table::Make(schema_, {
        a_nano, a_exch, a_sym, a_bizIndex, a_channel,
        a_funcCode, a_orderKind, a_orderNum, a_orderOriNo,
        a_price, a_time, a_volume,
    });

    ARROW_OK_OR_THROW(writer_->WriteTable(*table, buffered_rows_));
}

void MinuteWriter::flush_trade() {
    std::shared_ptr<arrow::Array> a_nano, a_exch, a_sym, a_askOrder, a_bsFlag,
        a_bidOrder, a_bizIndex, a_channel, a_funcCode, a_index, a_orderKind,
        a_price, a_time, a_volume;

    ARROW_OK_OR_THROW(d_nano_ts_->Finish(&a_nano));
    ARROW_OK_OR_THROW(d_exchange_->Finish(&a_exch));
    ARROW_OK_OR_THROW(d_sym_->Finish(&a_sym));
    ARROW_OK_OR_THROW(d_askOrder_->Finish(&a_askOrder));
    ARROW_OK_OR_THROW(d_bsFlag_->Finish(&a_bsFlag));
    ARROW_OK_OR_THROW(d_bidOrder_->Finish(&a_bidOrder));
    ARROW_OK_OR_THROW(d_bizIndex_->Finish(&a_bizIndex));
    ARROW_OK_OR_THROW(d_channel_->Finish(&a_channel));
    ARROW_OK_OR_THROW(d_functionCode_->Finish(&a_funcCode));
    ARROW_OK_OR_THROW(d_index_->Finish(&a_index));
    ARROW_OK_OR_THROW(d_orderKind_->Finish(&a_orderKind));
    ARROW_OK_OR_THROW(d_price_->Finish(&a_price));
    ARROW_OK_OR_THROW(d_time_->Finish(&a_time));
    ARROW_OK_OR_THROW(d_volume_->Finish(&a_volume));

    auto table = arrow::Table::Make(schema_, {
        a_nano, a_exch, a_sym, a_askOrder, a_bsFlag,
        a_bidOrder, a_bizIndex, a_channel, a_funcCode,
        a_index, a_orderKind, a_price, a_time, a_volume,
    });

    ARROW_OK_OR_THROW(writer_->WriteTable(*table, buffered_rows_));
}
