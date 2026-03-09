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
// Tick
// ---------------------------------------------------------------------------
static std::shared_ptr<arrow::Schema> tick_schema() {
    return arrow::schema({
        arrow::field("nano_timestamp", arrow::int64()),
        arrow::field("nTime", arrow::int32()),
        arrow::field("hostTime", arrow::int32()),
        arrow::field("nStatus", arrow::int32()),
        arrow::field("code", arrow::utf8()),
        arrow::field("sPrefix", arrow::utf8()),
        arrow::field("uPreClose", arrow::uint32()),
        arrow::field("uOpen", arrow::uint32()),
        arrow::field("uHigh", arrow::uint32()),
        arrow::field("uLow", arrow::uint32()),
        arrow::field("uMatch", arrow::uint32()),
        arrow::field("uAskPrice1", arrow::uint32()),
        arrow::field("uAskPrice2", arrow::uint32()),
        arrow::field("uAskPrice3", arrow::uint32()),
        arrow::field("uAskPrice4", arrow::uint32()),
        arrow::field("uAskPrice5", arrow::uint32()),
        arrow::field("uAskPrice6", arrow::uint32()),
        arrow::field("uAskPrice7", arrow::uint32()),
        arrow::field("uAskPrice8", arrow::uint32()),
        arrow::field("uAskPrice9", arrow::uint32()),
        arrow::field("uAskPrice10", arrow::uint32()),
        arrow::field("uAskVol1", arrow::uint32()),
        arrow::field("uAskVol2", arrow::uint32()),
        arrow::field("uAskVol3", arrow::uint32()),
        arrow::field("uAskVol4", arrow::uint32()),
        arrow::field("uAskVol5", arrow::uint32()),
        arrow::field("uAskVol6", arrow::uint32()),
        arrow::field("uAskVol7", arrow::uint32()),
        arrow::field("uAskVol8", arrow::uint32()),
        arrow::field("uAskVol9", arrow::uint32()),
        arrow::field("uAskVol10", arrow::uint32()),
        arrow::field("uBidPrice1", arrow::uint32()),
        arrow::field("uBidPrice2", arrow::uint32()),
        arrow::field("uBidPrice3", arrow::uint32()),
        arrow::field("uBidPrice4", arrow::uint32()),
        arrow::field("uBidPrice5", arrow::uint32()),
        arrow::field("uBidPrice6", arrow::uint32()),
        arrow::field("uBidPrice7", arrow::uint32()),
        arrow::field("uBidPrice8", arrow::uint32()),
        arrow::field("uBidPrice9", arrow::uint32()),
        arrow::field("uBidPrice10", arrow::uint32()),
        arrow::field("uBidVol1", arrow::uint32()),
        arrow::field("uBidVol2", arrow::uint32()),
        arrow::field("uBidVol3", arrow::uint32()),
        arrow::field("uBidVol4", arrow::uint32()),
        arrow::field("uBidVol5", arrow::uint32()),
        arrow::field("uBidVol6", arrow::uint32()),
        arrow::field("uBidVol7", arrow::uint32()),
        arrow::field("uBidVol8", arrow::uint32()),
        arrow::field("uBidVol9", arrow::uint32()),
        arrow::field("uBidVol10", arrow::uint32()),
        arrow::field("uNumTrades", arrow::uint32()),
        arrow::field("iVolume", arrow::int64()),
        arrow::field("iTurnover", arrow::int64()),
        arrow::field("iTotalBidVol", arrow::int64()),
        arrow::field("iTotalAskVol", arrow::int64()),
        arrow::field("uWeightedAvgBidPrice", arrow::uint32()),
        arrow::field("uWeightedAvgAskPrice", arrow::uint32()),
        arrow::field("nIOPV", arrow::int32()),
        arrow::field("nYieldToMaturity", arrow::int32()),
        arrow::field("uHighLimited", arrow::uint32()),
        arrow::field("uLowLimited", arrow::uint32()),
        arrow::field("nSyl1", arrow::int32()),
        arrow::field("nSyl2", arrow::int32()),
        arrow::field("nSD2", arrow::int32()),
        arrow::field("sTradingPhraseCode", arrow::utf8()),
        arrow::field("nPreIOPV", arrow::int32()),
    });
}

void write_tick_parquet(const std::vector<TickRecord>& records, const std::string& filepath) {
    auto schema = tick_schema();

    auto outfile_result = arrow::io::FileOutputStream::Open(filepath);
    if (!outfile_result.ok()) {
        std::cerr << "Cannot open output file: " << filepath << std::endl;
        return;
    }
    auto outfile = *outfile_result;

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

        arrow::Int64Builder   nano_ts;
        arrow::Int32Builder   nTime, hostTime, nStatus;
        arrow::StringBuilder  code, sPrefix;
        arrow::UInt32Builder  uPreClose, uOpen, uHigh, uLow, uMatch;
        arrow::UInt32Builder  uAskPrice1, uAskPrice2, uAskPrice3, uAskPrice4, uAskPrice5;
        arrow::UInt32Builder  uAskPrice6, uAskPrice7, uAskPrice8, uAskPrice9, uAskPrice10;
        arrow::UInt32Builder  uAskVol1, uAskVol2, uAskVol3, uAskVol4, uAskVol5;
        arrow::UInt32Builder  uAskVol6, uAskVol7, uAskVol8, uAskVol9, uAskVol10;
        arrow::UInt32Builder  uBidPrice1, uBidPrice2, uBidPrice3, uBidPrice4, uBidPrice5;
        arrow::UInt32Builder  uBidPrice6, uBidPrice7, uBidPrice8, uBidPrice9, uBidPrice10;
        arrow::UInt32Builder  uBidVol1, uBidVol2, uBidVol3, uBidVol4, uBidVol5;
        arrow::UInt32Builder  uBidVol6, uBidVol7, uBidVol8, uBidVol9, uBidVol10;
        arrow::UInt32Builder  uNumTrades;
        arrow::Int64Builder   iVolume, iTurnover, iTotalBidVol, iTotalAskVol;
        arrow::UInt32Builder  uWeightedAvgBidPrice, uWeightedAvgAskPrice;
        arrow::Int32Builder   nIOPV, nYieldToMaturity;
        arrow::UInt32Builder  uHighLimited, uLowLimited;
        arrow::Int32Builder   nSyl1, nSyl2, nSD2;
        arrow::StringBuilder  sTradingPhraseCode;
        arrow::Int32Builder   nPreIOPV;

        for (size_t i = offset; i < end; ++i) {
            const auto& r = records[i];
            ARROW_OK_OR_THROW(nano_ts.Append(r.nano_timestamp));
            ARROW_OK_OR_THROW(nTime.Append(r.nTime));
            ARROW_OK_OR_THROW(hostTime.Append(r.hostTime));
            ARROW_OK_OR_THROW(nStatus.Append(r.nStatus));
            ARROW_OK_OR_THROW(code.Append(r.code));
            ARROW_OK_OR_THROW(sPrefix.Append(r.sPrefix));
            ARROW_OK_OR_THROW(uPreClose.Append(r.uPreClose));
            ARROW_OK_OR_THROW(uOpen.Append(r.uOpen));
            ARROW_OK_OR_THROW(uHigh.Append(r.uHigh));
            ARROW_OK_OR_THROW(uLow.Append(r.uLow));
            ARROW_OK_OR_THROW(uMatch.Append(r.uMatch));
            ARROW_OK_OR_THROW(uAskPrice1.Append(r.uAskPrice1));
            ARROW_OK_OR_THROW(uAskPrice2.Append(r.uAskPrice2));
            ARROW_OK_OR_THROW(uAskPrice3.Append(r.uAskPrice3));
            ARROW_OK_OR_THROW(uAskPrice4.Append(r.uAskPrice4));
            ARROW_OK_OR_THROW(uAskPrice5.Append(r.uAskPrice5));
            ARROW_OK_OR_THROW(uAskPrice6.Append(r.uAskPrice6));
            ARROW_OK_OR_THROW(uAskPrice7.Append(r.uAskPrice7));
            ARROW_OK_OR_THROW(uAskPrice8.Append(r.uAskPrice8));
            ARROW_OK_OR_THROW(uAskPrice9.Append(r.uAskPrice9));
            ARROW_OK_OR_THROW(uAskPrice10.Append(r.uAskPrice10));
            ARROW_OK_OR_THROW(uAskVol1.Append(r.uAskVol1));
            ARROW_OK_OR_THROW(uAskVol2.Append(r.uAskVol2));
            ARROW_OK_OR_THROW(uAskVol3.Append(r.uAskVol3));
            ARROW_OK_OR_THROW(uAskVol4.Append(r.uAskVol4));
            ARROW_OK_OR_THROW(uAskVol5.Append(r.uAskVol5));
            ARROW_OK_OR_THROW(uAskVol6.Append(r.uAskVol6));
            ARROW_OK_OR_THROW(uAskVol7.Append(r.uAskVol7));
            ARROW_OK_OR_THROW(uAskVol8.Append(r.uAskVol8));
            ARROW_OK_OR_THROW(uAskVol9.Append(r.uAskVol9));
            ARROW_OK_OR_THROW(uAskVol10.Append(r.uAskVol10));
            ARROW_OK_OR_THROW(uBidPrice1.Append(r.uBidPrice1));
            ARROW_OK_OR_THROW(uBidPrice2.Append(r.uBidPrice2));
            ARROW_OK_OR_THROW(uBidPrice3.Append(r.uBidPrice3));
            ARROW_OK_OR_THROW(uBidPrice4.Append(r.uBidPrice4));
            ARROW_OK_OR_THROW(uBidPrice5.Append(r.uBidPrice5));
            ARROW_OK_OR_THROW(uBidPrice6.Append(r.uBidPrice6));
            ARROW_OK_OR_THROW(uBidPrice7.Append(r.uBidPrice7));
            ARROW_OK_OR_THROW(uBidPrice8.Append(r.uBidPrice8));
            ARROW_OK_OR_THROW(uBidPrice9.Append(r.uBidPrice9));
            ARROW_OK_OR_THROW(uBidPrice10.Append(r.uBidPrice10));
            ARROW_OK_OR_THROW(uBidVol1.Append(r.uBidVol1));
            ARROW_OK_OR_THROW(uBidVol2.Append(r.uBidVol2));
            ARROW_OK_OR_THROW(uBidVol3.Append(r.uBidVol3));
            ARROW_OK_OR_THROW(uBidVol4.Append(r.uBidVol4));
            ARROW_OK_OR_THROW(uBidVol5.Append(r.uBidVol5));
            ARROW_OK_OR_THROW(uBidVol6.Append(r.uBidVol6));
            ARROW_OK_OR_THROW(uBidVol7.Append(r.uBidVol7));
            ARROW_OK_OR_THROW(uBidVol8.Append(r.uBidVol8));
            ARROW_OK_OR_THROW(uBidVol9.Append(r.uBidVol9));
            ARROW_OK_OR_THROW(uBidVol10.Append(r.uBidVol10));
            ARROW_OK_OR_THROW(uNumTrades.Append(r.uNumTrades));
            ARROW_OK_OR_THROW(iVolume.Append(r.iVolume));
            ARROW_OK_OR_THROW(iTurnover.Append(r.iTurnover));
            ARROW_OK_OR_THROW(iTotalBidVol.Append(r.iTotalBidVol));
            ARROW_OK_OR_THROW(iTotalAskVol.Append(r.iTotalAskVol));
            ARROW_OK_OR_THROW(uWeightedAvgBidPrice.Append(r.uWeightedAvgBidPrice));
            ARROW_OK_OR_THROW(uWeightedAvgAskPrice.Append(r.uWeightedAvgAskPrice));
            ARROW_OK_OR_THROW(nIOPV.Append(r.nIOPV));
            ARROW_OK_OR_THROW(nYieldToMaturity.Append(r.nYieldToMaturity));
            ARROW_OK_OR_THROW(uHighLimited.Append(r.uHighLimited));
            ARROW_OK_OR_THROW(uLowLimited.Append(r.uLowLimited));
            ARROW_OK_OR_THROW(nSyl1.Append(r.nSyl1));
            ARROW_OK_OR_THROW(nSyl2.Append(r.nSyl2));
            ARROW_OK_OR_THROW(nSD2.Append(r.nSD2));
            ARROW_OK_OR_THROW(sTradingPhraseCode.Append(r.sTradingPhraseCode));
            ARROW_OK_OR_THROW(nPreIOPV.Append(r.nPreIOPV));
        }

        // Finish arrays
        std::shared_ptr<arrow::Array> arr_nano_ts, arr_nTime, arr_hostTime, arr_nStatus,
            arr_code, arr_sPrefix, arr_uPreClose, arr_uOpen, arr_uHigh, arr_uLow, arr_uMatch,
            arr_uAskPrice1, arr_uAskPrice2, arr_uAskPrice3, arr_uAskPrice4, arr_uAskPrice5,
            arr_uAskPrice6, arr_uAskPrice7, arr_uAskPrice8, arr_uAskPrice9, arr_uAskPrice10,
            arr_uAskVol1, arr_uAskVol2, arr_uAskVol3, arr_uAskVol4, arr_uAskVol5,
            arr_uAskVol6, arr_uAskVol7, arr_uAskVol8, arr_uAskVol9, arr_uAskVol10,
            arr_uBidPrice1, arr_uBidPrice2, arr_uBidPrice3, arr_uBidPrice4, arr_uBidPrice5,
            arr_uBidPrice6, arr_uBidPrice7, arr_uBidPrice8, arr_uBidPrice9, arr_uBidPrice10,
            arr_uBidVol1, arr_uBidVol2, arr_uBidVol3, arr_uBidVol4, arr_uBidVol5,
            arr_uBidVol6, arr_uBidVol7, arr_uBidVol8, arr_uBidVol9, arr_uBidVol10,
            arr_uNumTrades, arr_iVolume, arr_iTurnover, arr_iTotalBidVol, arr_iTotalAskVol,
            arr_uWeightedAvgBidPrice, arr_uWeightedAvgAskPrice,
            arr_nIOPV, arr_nYieldToMaturity, arr_uHighLimited, arr_uLowLimited,
            arr_nSyl1, arr_nSyl2, arr_nSD2, arr_sTradingPhraseCode, arr_nPreIOPV;

        ARROW_OK_OR_THROW(nano_ts.Finish(&arr_nano_ts));
        ARROW_OK_OR_THROW(nTime.Finish(&arr_nTime));
        ARROW_OK_OR_THROW(hostTime.Finish(&arr_hostTime));
        ARROW_OK_OR_THROW(nStatus.Finish(&arr_nStatus));
        ARROW_OK_OR_THROW(code.Finish(&arr_code));
        ARROW_OK_OR_THROW(sPrefix.Finish(&arr_sPrefix));
        ARROW_OK_OR_THROW(uPreClose.Finish(&arr_uPreClose));
        ARROW_OK_OR_THROW(uOpen.Finish(&arr_uOpen));
        ARROW_OK_OR_THROW(uHigh.Finish(&arr_uHigh));
        ARROW_OK_OR_THROW(uLow.Finish(&arr_uLow));
        ARROW_OK_OR_THROW(uMatch.Finish(&arr_uMatch));
        ARROW_OK_OR_THROW(uAskPrice1.Finish(&arr_uAskPrice1));
        ARROW_OK_OR_THROW(uAskPrice2.Finish(&arr_uAskPrice2));
        ARROW_OK_OR_THROW(uAskPrice3.Finish(&arr_uAskPrice3));
        ARROW_OK_OR_THROW(uAskPrice4.Finish(&arr_uAskPrice4));
        ARROW_OK_OR_THROW(uAskPrice5.Finish(&arr_uAskPrice5));
        ARROW_OK_OR_THROW(uAskPrice6.Finish(&arr_uAskPrice6));
        ARROW_OK_OR_THROW(uAskPrice7.Finish(&arr_uAskPrice7));
        ARROW_OK_OR_THROW(uAskPrice8.Finish(&arr_uAskPrice8));
        ARROW_OK_OR_THROW(uAskPrice9.Finish(&arr_uAskPrice9));
        ARROW_OK_OR_THROW(uAskPrice10.Finish(&arr_uAskPrice10));
        ARROW_OK_OR_THROW(uAskVol1.Finish(&arr_uAskVol1));
        ARROW_OK_OR_THROW(uAskVol2.Finish(&arr_uAskVol2));
        ARROW_OK_OR_THROW(uAskVol3.Finish(&arr_uAskVol3));
        ARROW_OK_OR_THROW(uAskVol4.Finish(&arr_uAskVol4));
        ARROW_OK_OR_THROW(uAskVol5.Finish(&arr_uAskVol5));
        ARROW_OK_OR_THROW(uAskVol6.Finish(&arr_uAskVol6));
        ARROW_OK_OR_THROW(uAskVol7.Finish(&arr_uAskVol7));
        ARROW_OK_OR_THROW(uAskVol8.Finish(&arr_uAskVol8));
        ARROW_OK_OR_THROW(uAskVol9.Finish(&arr_uAskVol9));
        ARROW_OK_OR_THROW(uAskVol10.Finish(&arr_uAskVol10));
        ARROW_OK_OR_THROW(uBidPrice1.Finish(&arr_uBidPrice1));
        ARROW_OK_OR_THROW(uBidPrice2.Finish(&arr_uBidPrice2));
        ARROW_OK_OR_THROW(uBidPrice3.Finish(&arr_uBidPrice3));
        ARROW_OK_OR_THROW(uBidPrice4.Finish(&arr_uBidPrice4));
        ARROW_OK_OR_THROW(uBidPrice5.Finish(&arr_uBidPrice5));
        ARROW_OK_OR_THROW(uBidPrice6.Finish(&arr_uBidPrice6));
        ARROW_OK_OR_THROW(uBidPrice7.Finish(&arr_uBidPrice7));
        ARROW_OK_OR_THROW(uBidPrice8.Finish(&arr_uBidPrice8));
        ARROW_OK_OR_THROW(uBidPrice9.Finish(&arr_uBidPrice9));
        ARROW_OK_OR_THROW(uBidPrice10.Finish(&arr_uBidPrice10));
        ARROW_OK_OR_THROW(uBidVol1.Finish(&arr_uBidVol1));
        ARROW_OK_OR_THROW(uBidVol2.Finish(&arr_uBidVol2));
        ARROW_OK_OR_THROW(uBidVol3.Finish(&arr_uBidVol3));
        ARROW_OK_OR_THROW(uBidVol4.Finish(&arr_uBidVol4));
        ARROW_OK_OR_THROW(uBidVol5.Finish(&arr_uBidVol5));
        ARROW_OK_OR_THROW(uBidVol6.Finish(&arr_uBidVol6));
        ARROW_OK_OR_THROW(uBidVol7.Finish(&arr_uBidVol7));
        ARROW_OK_OR_THROW(uBidVol8.Finish(&arr_uBidVol8));
        ARROW_OK_OR_THROW(uBidVol9.Finish(&arr_uBidVol9));
        ARROW_OK_OR_THROW(uBidVol10.Finish(&arr_uBidVol10));
        ARROW_OK_OR_THROW(uNumTrades.Finish(&arr_uNumTrades));
        ARROW_OK_OR_THROW(iVolume.Finish(&arr_iVolume));
        ARROW_OK_OR_THROW(iTurnover.Finish(&arr_iTurnover));
        ARROW_OK_OR_THROW(iTotalBidVol.Finish(&arr_iTotalBidVol));
        ARROW_OK_OR_THROW(iTotalAskVol.Finish(&arr_iTotalAskVol));
        ARROW_OK_OR_THROW(uWeightedAvgBidPrice.Finish(&arr_uWeightedAvgBidPrice));
        ARROW_OK_OR_THROW(uWeightedAvgAskPrice.Finish(&arr_uWeightedAvgAskPrice));
        ARROW_OK_OR_THROW(nIOPV.Finish(&arr_nIOPV));
        ARROW_OK_OR_THROW(nYieldToMaturity.Finish(&arr_nYieldToMaturity));
        ARROW_OK_OR_THROW(uHighLimited.Finish(&arr_uHighLimited));
        ARROW_OK_OR_THROW(uLowLimited.Finish(&arr_uLowLimited));
        ARROW_OK_OR_THROW(nSyl1.Finish(&arr_nSyl1));
        ARROW_OK_OR_THROW(nSyl2.Finish(&arr_nSyl2));
        ARROW_OK_OR_THROW(nSD2.Finish(&arr_nSD2));
        ARROW_OK_OR_THROW(sTradingPhraseCode.Finish(&arr_sTradingPhraseCode));
        ARROW_OK_OR_THROW(nPreIOPV.Finish(&arr_nPreIOPV));

        auto table = arrow::Table::Make(schema, {
            arr_nano_ts, arr_nTime, arr_hostTime, arr_nStatus, arr_code, arr_sPrefix,
            arr_uPreClose, arr_uOpen, arr_uHigh, arr_uLow, arr_uMatch,
            arr_uAskPrice1, arr_uAskPrice2, arr_uAskPrice3, arr_uAskPrice4, arr_uAskPrice5,
            arr_uAskPrice6, arr_uAskPrice7, arr_uAskPrice8, arr_uAskPrice9, arr_uAskPrice10,
            arr_uAskVol1, arr_uAskVol2, arr_uAskVol3, arr_uAskVol4, arr_uAskVol5,
            arr_uAskVol6, arr_uAskVol7, arr_uAskVol8, arr_uAskVol9, arr_uAskVol10,
            arr_uBidPrice1, arr_uBidPrice2, arr_uBidPrice3, arr_uBidPrice4, arr_uBidPrice5,
            arr_uBidPrice6, arr_uBidPrice7, arr_uBidPrice8, arr_uBidPrice9, arr_uBidPrice10,
            arr_uBidVol1, arr_uBidVol2, arr_uBidVol3, arr_uBidVol4, arr_uBidVol5,
            arr_uBidVol6, arr_uBidVol7, arr_uBidVol8, arr_uBidVol9, arr_uBidVol10,
            arr_uNumTrades, arr_iVolume, arr_iTurnover, arr_iTotalBidVol, arr_iTotalAskVol,
            arr_uWeightedAvgBidPrice, arr_uWeightedAvgAskPrice,
            arr_nIOPV, arr_nYieldToMaturity, arr_uHighLimited, arr_uLowLimited,
            arr_nSyl1, arr_nSyl2, arr_nSD2, arr_sTradingPhraseCode, arr_nPreIOPV,
        });

        ARROW_OK_OR_THROW(writer->WriteTable(*table, n));
    }

    ARROW_OK_OR_THROW(writer->Close());
    ARROW_OK_OR_THROW(outfile->Close());
    std::cout << "Wrote " << total << " tick records to " << filepath << std::endl;
}

// ---------------------------------------------------------------------------
// Order
// ---------------------------------------------------------------------------
static std::shared_ptr<arrow::Schema> order_schema() {
    return arrow::schema({
        arrow::field("nano_timestamp", arrow::int64()),
        arrow::field("exchange", arrow::utf8()),
        arrow::field("nHostTime", arrow::int32()),
        arrow::field("sSecurityID", arrow::utf8()),
        arrow::field("nMDDate", arrow::int32()),
        arrow::field("nMDTime", arrow::int32()),
        arrow::field("nOrderIndex", arrow::int32()),
        arrow::field("nOrderType", arrow::int32()),
        arrow::field("iOrderPrice", arrow::int64()),
        arrow::field("iOrderQty", arrow::int64()),
        arrow::field("nOrderBSFlag", arrow::int32()),
        arrow::field("nChannelNo", arrow::int32()),
        arrow::field("iOrderNo", arrow::int64()),
        arrow::field("iApplSeqNum", arrow::int64()),
        arrow::field("nDataMultiplePowerOf10", arrow::int32()),
        arrow::field("nTradedQty", arrow::int32()),
    });
}

void write_order_parquet(const std::vector<OrderRecord>& records, const std::string& filepath) {
    auto schema = order_schema();

    auto outfile_result = arrow::io::FileOutputStream::Open(filepath);
    if (!outfile_result.ok()) {
        std::cerr << "Cannot open output file: " << filepath << std::endl;
        return;
    }
    auto outfile = *outfile_result;

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

        arrow::Int64Builder  nano_ts, iOrderPrice, iOrderQty, iOrderNo, iApplSeqNum;
        arrow::StringBuilder exchange, sSecurityID;
        arrow::Int32Builder  nHostTime, nMDDate, nMDTime, nOrderIndex, nOrderType;
        arrow::Int32Builder  nOrderBSFlag, nChannelNo, nDataMultiplePowerOf10, nTradedQty;

        for (size_t i = offset; i < end; ++i) {
            const auto& r = records[i];
            ARROW_OK_OR_THROW(nano_ts.Append(r.nano_timestamp));
            ARROW_OK_OR_THROW(exchange.Append(r.exchange));
            ARROW_OK_OR_THROW(nHostTime.Append(r.nHostTime));
            ARROW_OK_OR_THROW(sSecurityID.Append(r.sSecurityID));
            ARROW_OK_OR_THROW(nMDDate.Append(r.nMDDate));
            ARROW_OK_OR_THROW(nMDTime.Append(r.nMDTime));
            ARROW_OK_OR_THROW(nOrderIndex.Append(r.nOrderIndex));
            ARROW_OK_OR_THROW(nOrderType.Append(r.nOrderType));
            ARROW_OK_OR_THROW(iOrderPrice.Append(r.iOrderPrice));
            ARROW_OK_OR_THROW(iOrderQty.Append(r.iOrderQty));
            ARROW_OK_OR_THROW(nOrderBSFlag.Append(r.nOrderBSFlag));
            ARROW_OK_OR_THROW(nChannelNo.Append(r.nChannelNo));
            ARROW_OK_OR_THROW(iOrderNo.Append(r.iOrderNo));
            ARROW_OK_OR_THROW(iApplSeqNum.Append(r.iApplSeqNum));
            ARROW_OK_OR_THROW(nDataMultiplePowerOf10.Append(r.nDataMultiplePowerOf10));
            ARROW_OK_OR_THROW(nTradedQty.Append(r.nTradedQty));
        }

        std::shared_ptr<arrow::Array> arr_nano_ts, arr_exchange, arr_nHostTime, arr_sSecurityID,
            arr_nMDDate, arr_nMDTime, arr_nOrderIndex, arr_nOrderType,
            arr_iOrderPrice, arr_iOrderQty, arr_nOrderBSFlag, arr_nChannelNo,
            arr_iOrderNo, arr_iApplSeqNum, arr_nDataMultiplePowerOf10, arr_nTradedQty;

        ARROW_OK_OR_THROW(nano_ts.Finish(&arr_nano_ts));
        ARROW_OK_OR_THROW(exchange.Finish(&arr_exchange));
        ARROW_OK_OR_THROW(nHostTime.Finish(&arr_nHostTime));
        ARROW_OK_OR_THROW(sSecurityID.Finish(&arr_sSecurityID));
        ARROW_OK_OR_THROW(nMDDate.Finish(&arr_nMDDate));
        ARROW_OK_OR_THROW(nMDTime.Finish(&arr_nMDTime));
        ARROW_OK_OR_THROW(nOrderIndex.Finish(&arr_nOrderIndex));
        ARROW_OK_OR_THROW(nOrderType.Finish(&arr_nOrderType));
        ARROW_OK_OR_THROW(iOrderPrice.Finish(&arr_iOrderPrice));
        ARROW_OK_OR_THROW(iOrderQty.Finish(&arr_iOrderQty));
        ARROW_OK_OR_THROW(nOrderBSFlag.Finish(&arr_nOrderBSFlag));
        ARROW_OK_OR_THROW(nChannelNo.Finish(&arr_nChannelNo));
        ARROW_OK_OR_THROW(iOrderNo.Finish(&arr_iOrderNo));
        ARROW_OK_OR_THROW(iApplSeqNum.Finish(&arr_iApplSeqNum));
        ARROW_OK_OR_THROW(nDataMultiplePowerOf10.Finish(&arr_nDataMultiplePowerOf10));
        ARROW_OK_OR_THROW(nTradedQty.Finish(&arr_nTradedQty));

        auto table = arrow::Table::Make(schema, {
            arr_nano_ts, arr_exchange, arr_nHostTime, arr_sSecurityID,
            arr_nMDDate, arr_nMDTime, arr_nOrderIndex, arr_nOrderType,
            arr_iOrderPrice, arr_iOrderQty, arr_nOrderBSFlag, arr_nChannelNo,
            arr_iOrderNo, arr_iApplSeqNum, arr_nDataMultiplePowerOf10, arr_nTradedQty,
        });

        ARROW_OK_OR_THROW(writer->WriteTable(*table, n));
    }

    ARROW_OK_OR_THROW(writer->Close());
    ARROW_OK_OR_THROW(outfile->Close());
    std::cout << "Wrote " << total << " order records to " << filepath << std::endl;
}

// ---------------------------------------------------------------------------
// Trade
// ---------------------------------------------------------------------------
static std::shared_ptr<arrow::Schema> trade_schema() {
    return arrow::schema({
        arrow::field("nano_timestamp", arrow::int64()),
        arrow::field("exchange", arrow::utf8()),
        arrow::field("nHostTime", arrow::int32()),
        arrow::field("sSecurityID", arrow::utf8()),
        arrow::field("nMDDate", arrow::int32()),
        arrow::field("nMDTime", arrow::int32()),
        arrow::field("nTradeIndex", arrow::int32()),
        arrow::field("iTradeBuyNo", arrow::int64()),
        arrow::field("iTradeSellNo", arrow::int64()),
        arrow::field("nTradeBSflag", arrow::int32()),
        arrow::field("iTradePrice", arrow::int64()),
        arrow::field("iTradeQty", arrow::int64()),
        arrow::field("iTradeMoney", arrow::int64()),
        arrow::field("nTradeType", arrow::int32()),
        arrow::field("iApplSeqNum", arrow::int64()),
        arrow::field("nChannelNo", arrow::int32()),
        arrow::field("nDataMultiplePowerOf10", arrow::int32()),
    });
}

void write_trade_parquet(const std::vector<TradeRecord>& records, const std::string& filepath) {
    auto schema = trade_schema();

    auto outfile_result = arrow::io::FileOutputStream::Open(filepath);
    if (!outfile_result.ok()) {
        std::cerr << "Cannot open output file: " << filepath << std::endl;
        return;
    }
    auto outfile = *outfile_result;

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

        arrow::Int64Builder  nano_ts, iTradeBuyNo, iTradeSellNo, iTradePrice, iTradeQty, iTradeMoney, iApplSeqNum;
        arrow::StringBuilder exchange, sSecurityID;
        arrow::Int32Builder  nHostTime, nMDDate, nMDTime, nTradeIndex, nTradeBSflag, nTradeType, nChannelNo, nDataMultiplePowerOf10;

        for (size_t i = offset; i < end; ++i) {
            const auto& r = records[i];
            ARROW_OK_OR_THROW(nano_ts.Append(r.nano_timestamp));
            ARROW_OK_OR_THROW(exchange.Append(r.exchange));
            ARROW_OK_OR_THROW(nHostTime.Append(r.nHostTime));
            ARROW_OK_OR_THROW(sSecurityID.Append(r.sSecurityID));
            ARROW_OK_OR_THROW(nMDDate.Append(r.nMDDate));
            ARROW_OK_OR_THROW(nMDTime.Append(r.nMDTime));
            ARROW_OK_OR_THROW(nTradeIndex.Append(r.nTradeIndex));
            ARROW_OK_OR_THROW(iTradeBuyNo.Append(r.iTradeBuyNo));
            ARROW_OK_OR_THROW(iTradeSellNo.Append(r.iTradeSellNo));
            ARROW_OK_OR_THROW(nTradeBSflag.Append(r.nTradeBSflag));
            ARROW_OK_OR_THROW(iTradePrice.Append(r.iTradePrice));
            ARROW_OK_OR_THROW(iTradeQty.Append(r.iTradeQty));
            ARROW_OK_OR_THROW(iTradeMoney.Append(r.iTradeMoney));
            ARROW_OK_OR_THROW(nTradeType.Append(r.nTradeType));
            ARROW_OK_OR_THROW(iApplSeqNum.Append(r.iApplSeqNum));
            ARROW_OK_OR_THROW(nChannelNo.Append(r.nChannelNo));
            ARROW_OK_OR_THROW(nDataMultiplePowerOf10.Append(r.nDataMultiplePowerOf10));
        }

        std::shared_ptr<arrow::Array> arr_nano_ts, arr_exchange, arr_nHostTime, arr_sSecurityID,
            arr_nMDDate, arr_nMDTime, arr_nTradeIndex,
            arr_iTradeBuyNo, arr_iTradeSellNo, arr_nTradeBSflag,
            arr_iTradePrice, arr_iTradeQty, arr_iTradeMoney,
            arr_nTradeType, arr_iApplSeqNum, arr_nChannelNo, arr_nDataMultiplePowerOf10;

        ARROW_OK_OR_THROW(nano_ts.Finish(&arr_nano_ts));
        ARROW_OK_OR_THROW(exchange.Finish(&arr_exchange));
        ARROW_OK_OR_THROW(nHostTime.Finish(&arr_nHostTime));
        ARROW_OK_OR_THROW(sSecurityID.Finish(&arr_sSecurityID));
        ARROW_OK_OR_THROW(nMDDate.Finish(&arr_nMDDate));
        ARROW_OK_OR_THROW(nMDTime.Finish(&arr_nMDTime));
        ARROW_OK_OR_THROW(nTradeIndex.Finish(&arr_nTradeIndex));
        ARROW_OK_OR_THROW(iTradeBuyNo.Finish(&arr_iTradeBuyNo));
        ARROW_OK_OR_THROW(iTradeSellNo.Finish(&arr_iTradeSellNo));
        ARROW_OK_OR_THROW(nTradeBSflag.Finish(&arr_nTradeBSflag));
        ARROW_OK_OR_THROW(iTradePrice.Finish(&arr_iTradePrice));
        ARROW_OK_OR_THROW(iTradeQty.Finish(&arr_iTradeQty));
        ARROW_OK_OR_THROW(iTradeMoney.Finish(&arr_iTradeMoney));
        ARROW_OK_OR_THROW(nTradeType.Finish(&arr_nTradeType));
        ARROW_OK_OR_THROW(iApplSeqNum.Finish(&arr_iApplSeqNum));
        ARROW_OK_OR_THROW(nChannelNo.Finish(&arr_nChannelNo));
        ARROW_OK_OR_THROW(nDataMultiplePowerOf10.Finish(&arr_nDataMultiplePowerOf10));

        auto table = arrow::Table::Make(schema, {
            arr_nano_ts, arr_exchange, arr_nHostTime, arr_sSecurityID,
            arr_nMDDate, arr_nMDTime, arr_nTradeIndex,
            arr_iTradeBuyNo, arr_iTradeSellNo, arr_nTradeBSflag,
            arr_iTradePrice, arr_iTradeQty, arr_iTradeMoney,
            arr_nTradeType, arr_iApplSeqNum, arr_nChannelNo, arr_nDataMultiplePowerOf10,
        });

        ARROW_OK_OR_THROW(writer->WriteTable(*table, n));
    }

    ARROW_OK_OR_THROW(writer->Close());
    ARROW_OK_OR_THROW(outfile->Close());
    std::cout << "Wrote " << total << " trade records to " << filepath << std::endl;
}
