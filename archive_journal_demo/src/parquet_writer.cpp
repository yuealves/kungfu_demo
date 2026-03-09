#include "parquet_writer.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include <iostream>
#include <memory>
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
// Tick — KyStdSnpType 全部字段 + nano_timestamp + exchange
// ---------------------------------------------------------------------------
static std::shared_ptr<arrow::Schema> tick_schema() {
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

void write_tick_parquet(const std::vector<TickRecord>& records, const std::string& filepath) {
    auto schema = tick_schema();

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(filepath));

    auto props = parquet::WriterProperties::Builder()
        .compression(arrow::Compression::SNAPPY)
        ->build();
    auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();

    std::unique_ptr<parquet::arrow::FileWriter> writer;
    ARROW_ASSIGN_OR_THROW(writer, parquet::arrow::FileWriter::Open(
        *schema, arrow::default_memory_pool(), outfile, props, arrow_props));

    size_t total = records.size();
    for (size_t offset = 0; offset < total; offset += ROW_GROUP_SIZE) {
        size_t end = std::min(offset + (size_t)ROW_GROUP_SIZE, total);
        size_t n = end - offset;

        arrow::Int64Builder  nano_ts;
        arrow::StringBuilder exchange;
        arrow::Int32Builder  sym, time_f, afterMatchItem, afterVolume, matchItem, volume_f, bizIndex;
        arrow::Int64Builder  accTurnover, accVolume, afterTurnover, totalAskVol, totalBidVol, turnover;
        arrow::FloatBuilder  afterPrice, askAvgPrice, bidAvgPrice, high, low, open_f, preClose, price_f;
        arrow::FloatBuilder  askPx[10], bidPx[10];
        arrow::DoubleBuilder askVol[10], bidVol[10];
        arrow::Int8Builder   bsFlag;

        for (size_t i = offset; i < end; ++i) {
            const auto& r = records[i];
            ARROW_OK_OR_THROW(nano_ts.Append(r.nano_timestamp));
            ARROW_OK_OR_THROW(exchange.Append(r.exchange));
            ARROW_OK_OR_THROW(sym.Append(r.Symbol));
            ARROW_OK_OR_THROW(time_f.Append(r.Time));
            ARROW_OK_OR_THROW(accTurnover.Append(r.AccTurnover));
            ARROW_OK_OR_THROW(accVolume.Append(r.AccVolume));
            ARROW_OK_OR_THROW(afterMatchItem.Append(r.AfterMatchItem));
            ARROW_OK_OR_THROW(afterPrice.Append(r.AfterPrice));
            ARROW_OK_OR_THROW(afterTurnover.Append(r.AfterTurnover));
            ARROW_OK_OR_THROW(afterVolume.Append(r.AfterVolume));
            ARROW_OK_OR_THROW(askAvgPrice.Append(r.AskAvgPrice));
            for (int j = 0; j < 10; ++j) ARROW_OK_OR_THROW(askPx[j].Append(r.AskPx[j]));
            for (int j = 0; j < 10; ++j) ARROW_OK_OR_THROW(askVol[j].Append(r.AskVol[j]));
            ARROW_OK_OR_THROW(bsFlag.Append(r.BSFlag));
            ARROW_OK_OR_THROW(bidAvgPrice.Append(r.BidAvgPrice));
            for (int j = 0; j < 10; ++j) ARROW_OK_OR_THROW(bidPx[j].Append(r.BidPx[j]));
            for (int j = 0; j < 10; ++j) ARROW_OK_OR_THROW(bidVol[j].Append(r.BidVol[j]));
            ARROW_OK_OR_THROW(high.Append(r.High));
            ARROW_OK_OR_THROW(low.Append(r.Low));
            ARROW_OK_OR_THROW(matchItem.Append(r.MatchItem));
            ARROW_OK_OR_THROW(open_f.Append(r.Open));
            ARROW_OK_OR_THROW(preClose.Append(r.PreClose));
            ARROW_OK_OR_THROW(price_f.Append(r.Price));
            ARROW_OK_OR_THROW(totalAskVol.Append(r.TotalAskVolume));
            ARROW_OK_OR_THROW(totalBidVol.Append(r.TotalBidVolume));
            ARROW_OK_OR_THROW(turnover.Append(r.Turnover));
            ARROW_OK_OR_THROW(volume_f.Append(r.Volume));
            ARROW_OK_OR_THROW(bizIndex.Append(r.BizIndex));
        }

        // Finish all builders
        std::shared_ptr<arrow::Array> a_nano, a_exch, a_sym, a_time, a_accTO, a_accVol,
            a_afterMI, a_afterP, a_afterTO, a_afterVol, a_askAvg,
            a_bsFlag, a_bidAvg,
            a_high, a_low, a_matchItem, a_open, a_preClose, a_price,
            a_totalAskVol, a_totalBidVol, a_turnover, a_volume, a_bizIndex;
        std::shared_ptr<arrow::Array> a_askPx[10], a_askVol[10], a_bidPx[10], a_bidVol[10];

        ARROW_OK_OR_THROW(nano_ts.Finish(&a_nano));
        ARROW_OK_OR_THROW(exchange.Finish(&a_exch));
        ARROW_OK_OR_THROW(sym.Finish(&a_sym));
        ARROW_OK_OR_THROW(time_f.Finish(&a_time));
        ARROW_OK_OR_THROW(accTurnover.Finish(&a_accTO));
        ARROW_OK_OR_THROW(accVolume.Finish(&a_accVol));
        ARROW_OK_OR_THROW(afterMatchItem.Finish(&a_afterMI));
        ARROW_OK_OR_THROW(afterPrice.Finish(&a_afterP));
        ARROW_OK_OR_THROW(afterTurnover.Finish(&a_afterTO));
        ARROW_OK_OR_THROW(afterVolume.Finish(&a_afterVol));
        ARROW_OK_OR_THROW(askAvgPrice.Finish(&a_askAvg));
        for (int j = 0; j < 10; ++j) ARROW_OK_OR_THROW(askPx[j].Finish(&a_askPx[j]));
        for (int j = 0; j < 10; ++j) ARROW_OK_OR_THROW(askVol[j].Finish(&a_askVol[j]));
        ARROW_OK_OR_THROW(bsFlag.Finish(&a_bsFlag));
        ARROW_OK_OR_THROW(bidAvgPrice.Finish(&a_bidAvg));
        for (int j = 0; j < 10; ++j) ARROW_OK_OR_THROW(bidPx[j].Finish(&a_bidPx[j]));
        for (int j = 0; j < 10; ++j) ARROW_OK_OR_THROW(bidVol[j].Finish(&a_bidVol[j]));
        ARROW_OK_OR_THROW(high.Finish(&a_high));
        ARROW_OK_OR_THROW(low.Finish(&a_low));
        ARROW_OK_OR_THROW(matchItem.Finish(&a_matchItem));
        ARROW_OK_OR_THROW(open_f.Finish(&a_open));
        ARROW_OK_OR_THROW(preClose.Finish(&a_preClose));
        ARROW_OK_OR_THROW(price_f.Finish(&a_price));
        ARROW_OK_OR_THROW(totalAskVol.Finish(&a_totalAskVol));
        ARROW_OK_OR_THROW(totalBidVol.Finish(&a_totalBidVol));
        ARROW_OK_OR_THROW(turnover.Finish(&a_turnover));
        ARROW_OK_OR_THROW(volume_f.Finish(&a_volume));
        ARROW_OK_OR_THROW(bizIndex.Finish(&a_bizIndex));

        // Build column vector in schema order
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

        auto table = arrow::Table::Make(schema, columns);
        ARROW_OK_OR_THROW(writer->WriteTable(*table, n));
    }

    ARROW_OK_OR_THROW(writer->Close());
    ARROW_OK_OR_THROW(outfile->Close());
    std::cout << "Wrote " << total << " tick records to " << filepath << std::endl;
}

// ---------------------------------------------------------------------------
// Order — KyStdOrderType 全部字段 + nano_timestamp + exchange
// ---------------------------------------------------------------------------
static std::shared_ptr<arrow::Schema> order_schema() {
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

void write_order_parquet(const std::vector<OrderRecord>& records, const std::string& filepath) {
    auto schema = order_schema();

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(filepath));

    auto props = parquet::WriterProperties::Builder()
        .compression(arrow::Compression::SNAPPY)
        ->build();
    auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();

    std::unique_ptr<parquet::arrow::FileWriter> writer;
    ARROW_ASSIGN_OR_THROW(writer, parquet::arrow::FileWriter::Open(
        *schema, arrow::default_memory_pool(), outfile, props, arrow_props));

    size_t total = records.size();
    for (size_t offset = 0; offset < total; offset += ROW_GROUP_SIZE) {
        size_t end = std::min(offset + (size_t)ROW_GROUP_SIZE, total);
        size_t n = end - offset;

        arrow::Int64Builder  nano_ts, channel;
        arrow::StringBuilder exchange;
        arrow::Int32Builder  sym, bizIndex, orderNumber, orderOriNo, time_f, volume_f;
        arrow::Int8Builder   functionCode, orderKind;
        arrow::FloatBuilder  price;

        for (size_t i = offset; i < end; ++i) {
            const auto& r = records[i];
            ARROW_OK_OR_THROW(nano_ts.Append(r.nano_timestamp));
            ARROW_OK_OR_THROW(exchange.Append(r.exchange));
            ARROW_OK_OR_THROW(sym.Append(r.Symbol));
            ARROW_OK_OR_THROW(bizIndex.Append(r.BizIndex));
            ARROW_OK_OR_THROW(channel.Append(r.Channel));
            ARROW_OK_OR_THROW(functionCode.Append(r.FunctionCode));
            ARROW_OK_OR_THROW(orderKind.Append(r.OrderKind));
            ARROW_OK_OR_THROW(orderNumber.Append(r.OrderNumber));
            ARROW_OK_OR_THROW(orderOriNo.Append(r.OrderOriNo));
            ARROW_OK_OR_THROW(price.Append(r.Price));
            ARROW_OK_OR_THROW(time_f.Append(r.Time));
            ARROW_OK_OR_THROW(volume_f.Append(r.Volume));
        }

        std::shared_ptr<arrow::Array> a_nano, a_exch, a_sym, a_bizIndex, a_channel,
            a_funcCode, a_orderKind, a_orderNum, a_orderOriNo, a_price, a_time, a_volume;

        ARROW_OK_OR_THROW(nano_ts.Finish(&a_nano));
        ARROW_OK_OR_THROW(exchange.Finish(&a_exch));
        ARROW_OK_OR_THROW(sym.Finish(&a_sym));
        ARROW_OK_OR_THROW(bizIndex.Finish(&a_bizIndex));
        ARROW_OK_OR_THROW(channel.Finish(&a_channel));
        ARROW_OK_OR_THROW(functionCode.Finish(&a_funcCode));
        ARROW_OK_OR_THROW(orderKind.Finish(&a_orderKind));
        ARROW_OK_OR_THROW(orderNumber.Finish(&a_orderNum));
        ARROW_OK_OR_THROW(orderOriNo.Finish(&a_orderOriNo));
        ARROW_OK_OR_THROW(price.Finish(&a_price));
        ARROW_OK_OR_THROW(time_f.Finish(&a_time));
        ARROW_OK_OR_THROW(volume_f.Finish(&a_volume));

        auto table = arrow::Table::Make(schema, {
            a_nano, a_exch, a_sym, a_bizIndex, a_channel,
            a_funcCode, a_orderKind, a_orderNum, a_orderOriNo,
            a_price, a_time, a_volume,
        });

        ARROW_OK_OR_THROW(writer->WriteTable(*table, n));
    }

    ARROW_OK_OR_THROW(writer->Close());
    ARROW_OK_OR_THROW(outfile->Close());
    std::cout << "Wrote " << total << " order records to " << filepath << std::endl;
}

// ---------------------------------------------------------------------------
// Trade — KyStdTradeType 全部字段 + nano_timestamp + exchange
// ---------------------------------------------------------------------------
static std::shared_ptr<arrow::Schema> trade_schema() {
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

void write_trade_parquet(const std::vector<TradeRecord>& records, const std::string& filepath) {
    auto schema = trade_schema();

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(filepath));

    auto props = parquet::WriterProperties::Builder()
        .compression(arrow::Compression::SNAPPY)
        ->build();
    auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();

    std::unique_ptr<parquet::arrow::FileWriter> writer;
    ARROW_ASSIGN_OR_THROW(writer, parquet::arrow::FileWriter::Open(
        *schema, arrow::default_memory_pool(), outfile, props, arrow_props));

    size_t total = records.size();
    for (size_t offset = 0; offset < total; offset += ROW_GROUP_SIZE) {
        size_t end = std::min(offset + (size_t)ROW_GROUP_SIZE, total);
        size_t n = end - offset;

        arrow::Int64Builder  nano_ts;
        arrow::StringBuilder exchange;
        arrow::Int32Builder  sym, askOrder, bidOrder, bizIndex, channel, index_f, time_f, volume_f;
        arrow::Int8Builder   bsFlag, functionCode, orderKind;
        arrow::FloatBuilder  price;

        for (size_t i = offset; i < end; ++i) {
            const auto& r = records[i];
            ARROW_OK_OR_THROW(nano_ts.Append(r.nano_timestamp));
            ARROW_OK_OR_THROW(exchange.Append(r.exchange));
            ARROW_OK_OR_THROW(sym.Append(r.Symbol));
            ARROW_OK_OR_THROW(askOrder.Append(r.AskOrder));
            ARROW_OK_OR_THROW(bsFlag.Append(r.BSFlag));
            ARROW_OK_OR_THROW(bidOrder.Append(r.BidOrder));
            ARROW_OK_OR_THROW(bizIndex.Append(r.BizIndex));
            ARROW_OK_OR_THROW(channel.Append(r.Channel));
            ARROW_OK_OR_THROW(functionCode.Append(r.FunctionCode));
            ARROW_OK_OR_THROW(index_f.Append(r.Index));
            ARROW_OK_OR_THROW(orderKind.Append(r.OrderKind));
            ARROW_OK_OR_THROW(price.Append(r.Price));
            ARROW_OK_OR_THROW(time_f.Append(r.Time));
            ARROW_OK_OR_THROW(volume_f.Append(r.Volume));
        }

        std::shared_ptr<arrow::Array> a_nano, a_exch, a_sym, a_askOrder, a_bsFlag,
            a_bidOrder, a_bizIndex, a_channel, a_funcCode, a_index, a_orderKind,
            a_price, a_time, a_volume;

        ARROW_OK_OR_THROW(nano_ts.Finish(&a_nano));
        ARROW_OK_OR_THROW(exchange.Finish(&a_exch));
        ARROW_OK_OR_THROW(sym.Finish(&a_sym));
        ARROW_OK_OR_THROW(askOrder.Finish(&a_askOrder));
        ARROW_OK_OR_THROW(bsFlag.Finish(&a_bsFlag));
        ARROW_OK_OR_THROW(bidOrder.Finish(&a_bidOrder));
        ARROW_OK_OR_THROW(bizIndex.Finish(&a_bizIndex));
        ARROW_OK_OR_THROW(channel.Finish(&a_channel));
        ARROW_OK_OR_THROW(functionCode.Finish(&a_funcCode));
        ARROW_OK_OR_THROW(index_f.Finish(&a_index));
        ARROW_OK_OR_THROW(orderKind.Finish(&a_orderKind));
        ARROW_OK_OR_THROW(price.Finish(&a_price));
        ARROW_OK_OR_THROW(time_f.Finish(&a_time));
        ARROW_OK_OR_THROW(volume_f.Finish(&a_volume));

        auto table = arrow::Table::Make(schema, {
            a_nano, a_exch, a_sym, a_askOrder, a_bsFlag,
            a_bidOrder, a_bizIndex, a_channel, a_funcCode,
            a_index, a_orderKind, a_price, a_time, a_volume,
        });

        ARROW_OK_OR_THROW(writer->WriteTable(*table, n));
    }

    ARROW_OK_OR_THROW(writer->Close());
    ARROW_OK_OR_THROW(outfile->Close());
    std::cout << "Wrote " << total << " trade records to " << filepath << std::endl;
}
