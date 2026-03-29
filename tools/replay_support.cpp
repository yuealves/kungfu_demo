#include "replay_support.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <algorithm>
#include <cstdint>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <exception>
#include <memory>
#include <mutex>
#include <iostream>
#include <queue>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

using ArrayMap = std::unordered_map<std::string, std::shared_ptr<arrow::Array>>;

template <typename T>
std::shared_ptr<T> getArray(const ArrayMap& arrays, const std::string& name) {
    auto it = arrays.find(name);
    if (it == arrays.end()) {
        throw std::runtime_error("missing parquet column: " + name);
    }
    auto arr = std::dynamic_pointer_cast<T>(it->second);
    if (!arr) {
        throw std::runtime_error("unexpected parquet column type: " + name);
    }
    return arr;
}

std::vector<std::string> listFiles(const std::string& dir_path,
                                   const std::string& contains,
                                   const std::string& suffix) {
    std::vector<std::string> result;
    DIR* dir = opendir(dir_path.c_str());
    if (!dir) {
        throw std::runtime_error("cannot open directory: " + dir_path);
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        std::string name = entry->d_name;
        if (name.find(contains) != std::string::npos &&
            name.size() >= suffix.size() &&
            name.compare(name.size() - suffix.size(), suffix.size(), suffix) == 0) {
            result.push_back(dir_path + "/" + name);
        }
    }
    closedir(dir);
    std::sort(result.begin(), result.end());
    return result;
}

std::shared_ptr<arrow::Table> readParquetTable(const std::string& path,
                                               const std::vector<std::string>& columns) {
    auto file_result = arrow::io::ReadableFile::Open(path);
    if (!file_result.ok()) {
        throw std::runtime_error("cannot open parquet: " + path + " - " + file_result.status().ToString());
    }

    auto reader_result = parquet::arrow::OpenFile(file_result.ValueOrDie(), arrow::default_memory_pool());
    if (!reader_result.ok()) {
        throw std::runtime_error("cannot create parquet reader: " + path + " - " + reader_result.status().ToString());
    }

    auto reader = std::move(reader_result).ValueOrDie();
    auto schema = reader->parquet_reader()->metadata()->schema();

    std::vector<int> indices;
    indices.reserve(columns.size());
    for (const auto& column : columns) {
        int idx = -1;
        for (int c = 0; c < schema->num_columns(); ++c) {
            if (schema->Column(c)->name() == column) {
                idx = c;
                break;
            }
        }
        if (idx < 0) {
            throw std::runtime_error("column not found in " + path + ": " + column);
        }
        indices.push_back(idx);
    }

    std::shared_ptr<arrow::Table> table;
    auto st = reader->ReadTable(indices, &table);
    if (!st.ok()) {
        throw std::runtime_error("cannot read parquet table: " + path + " - " + st.ToString());
    }

    auto combined = table->CombineChunks(arrow::default_memory_pool());
    if (!combined.ok()) {
        throw std::runtime_error("cannot combine parquet chunks: " + path + " - " + combined.status().ToString());
    }
    return combined.ValueOrDie();
}

ArrayMap makeArrayMap(const std::shared_ptr<arrow::Table>& table) {
    ArrayMap arrays;
    for (int i = 0; i < table->num_columns(); ++i) {
        arrays.emplace(table->field(i)->name(), table->column(i)->chunk(0));
    }
    return arrays;
}

void filterFramesAfterResume(std::vector<ReplayFrame>* frames,
                             std::size_t start_index,
                             long resume_nano) {
    auto begin = frames->begin() + static_cast<std::ptrdiff_t>(start_index);
    frames->erase(
        std::remove_if(begin, frames->end(), [resume_nano](const ReplayFrame& frame) {
            return frame.nano <= resume_nano;
        }),
        frames->end());
}

template <typename StructT>
ReplayFrame makeOwnedFrame(long nano, short msg_type, const StructT& payload) {
    auto owned = std::shared_ptr<void>(new StructT(payload), [](void* ptr) {
        delete static_cast<StructT*>(ptr);
    });

    ReplayFrame frame;
    frame.nano = nano;
    frame.msg_type = msg_type;
    frame.source = 0;
    frame.last_flag = 0;
    frame.req_id = 0;
    frame.owned_data = owned;
    frame.data = owned.get();
    frame.data_len = sizeof(StructT);
    return frame;
}

std::vector<std::string> tickColumns() {
    return {
        "nano_timestamp", "Symbol", "Time", "AccTurnover", "AccVolume",
        "AfterMatchItem", "AfterPrice", "AfterTurnover", "AfterVolume", "AskAvgPrice",
        "AskPx1", "AskPx2", "AskPx3", "AskPx4", "AskPx5", "AskPx6", "AskPx7", "AskPx8", "AskPx9", "AskPx10",
        "AskVol1", "AskVol2", "AskVol3", "AskVol4", "AskVol5", "AskVol6", "AskVol7", "AskVol8", "AskVol9", "AskVol10",
        "BSFlag", "BidAvgPrice",
        "BidPx1", "BidPx2", "BidPx3", "BidPx4", "BidPx5", "BidPx6", "BidPx7", "BidPx8", "BidPx9", "BidPx10",
        "BidVol1", "BidVol2", "BidVol3", "BidVol4", "BidVol5", "BidVol6", "BidVol7", "BidVol8", "BidVol9", "BidVol10",
        "High", "Low", "MatchItem", "Open", "PreClose", "Price",
        "TotalAskVolume", "TotalBidVolume", "Turnover", "Volume", "BizIndex"
    };
}

std::vector<std::string> orderColumns() {
    return {
        "nano_timestamp", "Symbol", "BizIndex", "Channel", "FunctionCode", "OrderKind",
        "OrderNumber", "OrderOriNo", "Price", "Time", "Volume"
    };
}

std::vector<std::string> tradeColumns() {
    return {
        "nano_timestamp", "Symbol", "AskOrder", "BSFlag", "BidOrder", "BizIndex",
        "Channel", "FunctionCode", "Index", "OrderKind", "Price", "Time", "Volume"
    };
}

void appendTickFrames(const std::string& file_path, long resume_nano, std::vector<ReplayFrame>* frames) {
    (void)resume_nano;
    auto table = readParquetTable(file_path, tickColumns());
    auto arrays = makeArrayMap(table);

    auto nano = getArray<arrow::Int64Array>(arrays, "nano_timestamp");
    auto symbol = getArray<arrow::Int32Array>(arrays, "Symbol");
    auto time = getArray<arrow::Int32Array>(arrays, "Time");
    auto acc_turnover = getArray<arrow::Int64Array>(arrays, "AccTurnover");
    auto acc_volume = getArray<arrow::Int64Array>(arrays, "AccVolume");
    auto after_match_item = getArray<arrow::Int32Array>(arrays, "AfterMatchItem");
    auto after_price = getArray<arrow::FloatArray>(arrays, "AfterPrice");
    auto after_turnover = getArray<arrow::Int64Array>(arrays, "AfterTurnover");
    auto after_volume = getArray<arrow::Int32Array>(arrays, "AfterVolume");
    auto ask_avg_price = getArray<arrow::FloatArray>(arrays, "AskAvgPrice");
    auto bs_flag = getArray<arrow::Int8Array>(arrays, "BSFlag");
    auto bid_avg_price = getArray<arrow::FloatArray>(arrays, "BidAvgPrice");
    auto high = getArray<arrow::FloatArray>(arrays, "High");
    auto low = getArray<arrow::FloatArray>(arrays, "Low");
    auto match_item = getArray<arrow::Int32Array>(arrays, "MatchItem");
    auto open = getArray<arrow::FloatArray>(arrays, "Open");
    auto pre_close = getArray<arrow::FloatArray>(arrays, "PreClose");
    auto price = getArray<arrow::FloatArray>(arrays, "Price");
    auto total_ask_volume = getArray<arrow::Int64Array>(arrays, "TotalAskVolume");
    auto total_bid_volume = getArray<arrow::Int64Array>(arrays, "TotalBidVolume");
    auto turnover = getArray<arrow::Int64Array>(arrays, "Turnover");
    auto volume = getArray<arrow::Int32Array>(arrays, "Volume");
    auto biz_index = getArray<arrow::Int32Array>(arrays, "BizIndex");

    std::vector<std::shared_ptr<arrow::FloatArray>> ask_px;
    std::vector<std::shared_ptr<arrow::DoubleArray>> ask_vol;
    std::vector<std::shared_ptr<arrow::FloatArray>> bid_px;
    std::vector<std::shared_ptr<arrow::DoubleArray>> bid_vol;
    for (int level = 1; level <= 10; ++level) {
        ask_px.push_back(getArray<arrow::FloatArray>(arrays, "AskPx" + std::to_string(level)));
        ask_vol.push_back(getArray<arrow::DoubleArray>(arrays, "AskVol" + std::to_string(level)));
        bid_px.push_back(getArray<arrow::FloatArray>(arrays, "BidPx" + std::to_string(level)));
        bid_vol.push_back(getArray<arrow::DoubleArray>(arrays, "BidVol" + std::to_string(level)));
    }

    for (int64_t i = 0; i < table->num_rows(); ++i) {
        long original_nano = nano->Value(i);

        KyStdSnpType payload{};
        payload.AccTurnover = acc_turnover->Value(i);
        payload.AccVolume = acc_volume->Value(i);
        payload.AfterMatchItem = after_match_item->Value(i);
        payload.AfterPrice = after_price->Value(i);
        payload.AfterTurnover = after_turnover->Value(i);
        payload.AfterVolume = after_volume->Value(i);
        payload.AskAvgPrice = ask_avg_price->Value(i);
        payload.BSFlag = bs_flag->Value(i);
        payload.BidAvgPrice = bid_avg_price->Value(i);
        payload.High = high->Value(i);
        payload.Low = low->Value(i);
        payload.MatchItem = match_item->Value(i);
        payload.Open = open->Value(i);
        payload.PreClose = pre_close->Value(i);
        payload.Price = price->Value(i);
        payload.Time = time->Value(i);
        payload.TotalAskVolume = total_ask_volume->Value(i);
        payload.TotalBidVolume = total_bid_volume->Value(i);
        payload.Turnover = turnover->Value(i);
        payload.Volume = volume->Value(i);
        payload.Symbol = symbol->Value(i);
        payload.BizIndex = biz_index->Value(i);

        payload.AskPx1 = ask_px[0]->Value(i); payload.AskPx2 = ask_px[1]->Value(i);
        payload.AskPx3 = ask_px[2]->Value(i); payload.AskPx4 = ask_px[3]->Value(i);
        payload.AskPx5 = ask_px[4]->Value(i); payload.AskPx6 = ask_px[5]->Value(i);
        payload.AskPx7 = ask_px[6]->Value(i); payload.AskPx8 = ask_px[7]->Value(i);
        payload.AskPx9 = ask_px[8]->Value(i); payload.AskPx10 = ask_px[9]->Value(i);

        payload.AskVol1 = ask_vol[0]->Value(i); payload.AskVol2 = ask_vol[1]->Value(i);
        payload.AskVol3 = ask_vol[2]->Value(i); payload.AskVol4 = ask_vol[3]->Value(i);
        payload.AskVol5 = ask_vol[4]->Value(i); payload.AskVol6 = ask_vol[5]->Value(i);
        payload.AskVol7 = ask_vol[6]->Value(i); payload.AskVol8 = ask_vol[7]->Value(i);
        payload.AskVol9 = ask_vol[8]->Value(i); payload.AskVol10 = ask_vol[9]->Value(i);

        payload.BidPx1 = bid_px[0]->Value(i); payload.BidPx2 = bid_px[1]->Value(i);
        payload.BidPx3 = bid_px[2]->Value(i); payload.BidPx4 = bid_px[3]->Value(i);
        payload.BidPx5 = bid_px[4]->Value(i); payload.BidPx6 = bid_px[5]->Value(i);
        payload.BidPx7 = bid_px[6]->Value(i); payload.BidPx8 = bid_px[7]->Value(i);
        payload.BidPx9 = bid_px[8]->Value(i); payload.BidPx10 = bid_px[9]->Value(i);

        payload.BidVol1 = bid_vol[0]->Value(i); payload.BidVol2 = bid_vol[1]->Value(i);
        payload.BidVol3 = bid_vol[2]->Value(i); payload.BidVol4 = bid_vol[3]->Value(i);
        payload.BidVol5 = bid_vol[4]->Value(i); payload.BidVol6 = bid_vol[5]->Value(i);
        payload.BidVol7 = bid_vol[6]->Value(i); payload.BidVol8 = bid_vol[7]->Value(i);
        payload.BidVol9 = bid_vol[8]->Value(i); payload.BidVol10 = bid_vol[9]->Value(i);

        frames->push_back(makeOwnedFrame(original_nano, MSG_TYPE_L2_TICK, payload));
    }
}

void appendOrderFrames(const std::string& file_path, long resume_nano, std::vector<ReplayFrame>* frames) {
    (void)resume_nano;
    auto table = readParquetTable(file_path, orderColumns());
    auto arrays = makeArrayMap(table);

    auto nano = getArray<arrow::Int64Array>(arrays, "nano_timestamp");
    auto symbol = getArray<arrow::Int32Array>(arrays, "Symbol");
    auto biz_index = getArray<arrow::Int32Array>(arrays, "BizIndex");
    auto channel = getArray<arrow::Int64Array>(arrays, "Channel");
    auto function_code = getArray<arrow::Int8Array>(arrays, "FunctionCode");
    auto order_kind = getArray<arrow::Int8Array>(arrays, "OrderKind");
    auto order_number = getArray<arrow::Int32Array>(arrays, "OrderNumber");
    auto order_ori_no = getArray<arrow::Int32Array>(arrays, "OrderOriNo");
    auto price = getArray<arrow::FloatArray>(arrays, "Price");
    auto time = getArray<arrow::Int32Array>(arrays, "Time");
    auto volume = getArray<arrow::Int32Array>(arrays, "Volume");

    for (int64_t i = 0; i < table->num_rows(); ++i) {
        long original_nano = nano->Value(i);

        KyStdOrderType payload{};
        payload.BizIndex = biz_index->Value(i);
        payload.Channel = channel->Value(i);
        payload.FunctionCode = function_code->Value(i);
        payload.OrderKind = order_kind->Value(i);
        payload.OrderNumber = order_number->Value(i);
        payload.OrderOriNo = order_ori_no->Value(i);
        payload.Price = price->Value(i);
        payload.Time = time->Value(i);
        payload.Volume = volume->Value(i);
        payload.Symbol = symbol->Value(i);
        payload.TradedQty = 0;
        payload.Date = 0;
        payload.Multiple = 0;

        frames->push_back(makeOwnedFrame(original_nano, MSG_TYPE_L2_ORDER, payload));
    }
}

void appendTradeFrames(const std::string& file_path, long resume_nano, std::vector<ReplayFrame>* frames) {
    (void)resume_nano;
    auto table = readParquetTable(file_path, tradeColumns());
    auto arrays = makeArrayMap(table);

    auto nano = getArray<arrow::Int64Array>(arrays, "nano_timestamp");
    auto symbol = getArray<arrow::Int32Array>(arrays, "Symbol");
    auto ask_order = getArray<arrow::Int32Array>(arrays, "AskOrder");
    auto bs_flag = getArray<arrow::Int8Array>(arrays, "BSFlag");
    auto bid_order = getArray<arrow::Int32Array>(arrays, "BidOrder");
    auto biz_index = getArray<arrow::Int32Array>(arrays, "BizIndex");
    auto channel = getArray<arrow::Int32Array>(arrays, "Channel");
    auto function_code = getArray<arrow::Int8Array>(arrays, "FunctionCode");
    auto index = getArray<arrow::Int32Array>(arrays, "Index");
    auto order_kind = getArray<arrow::Int8Array>(arrays, "OrderKind");
    auto price = getArray<arrow::FloatArray>(arrays, "Price");
    auto time = getArray<arrow::Int32Array>(arrays, "Time");
    auto volume = getArray<arrow::Int32Array>(arrays, "Volume");

    for (int64_t i = 0; i < table->num_rows(); ++i) {
        long original_nano = nano->Value(i);

        KyStdTradeType payload{};
        payload.AskOrder = ask_order->Value(i);
        payload.BSFlag = bs_flag->Value(i);
        payload.BidOrder = bid_order->Value(i);
        payload.BizIndex = biz_index->Value(i);
        payload.Channel = channel->Value(i);
        payload.FunctionCode = function_code->Value(i);
        payload.Index = index->Value(i);
        payload.OrderKind = order_kind->Value(i);
        payload.Price = price->Value(i);
        payload.Time = time->Value(i);
        payload.Volume = volume->Value(i);
        payload.Symbol = symbol->Value(i);
        payload.Date = 0;
        payload.Multiple = 0;
        payload.Money = 0;

        frames->push_back(makeOwnedFrame(original_nano, MSG_TYPE_L2_TRADE, payload));
    }
}

std::shared_ptr<arrow::Schema> orderTestSchema() {
    return arrow::schema({
        arrow::field("nano_timestamp", arrow::int64()),
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

std::vector<std::shared_ptr<arrow::Array>> makeOrderTestColumns(
    int64_t nano, int32_t symbol, int32_t biz_index, float price) {
    return {
        std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{nano})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{symbol})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{biz_index})),
        std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{22})),
        std::make_shared<arrow::Int8Array>(1, arrow::Buffer::Wrap(std::vector<int8_t>{66})),
        std::make_shared<arrow::Int8Array>(1, arrow::Buffer::Wrap(std::vector<int8_t>{49})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{23})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{24})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{price})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{93000001})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{25})),
    };
}

std::shared_ptr<arrow::Schema> tradeTestSchema() {
    return arrow::schema({
        arrow::field("nano_timestamp", arrow::int64()),
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

std::vector<std::shared_ptr<arrow::Array>> makeTradeTestColumns(
    int64_t nano, int32_t symbol, int32_t biz_index, float price) {
    return {
        std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{nano})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{symbol})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{41})),
        std::make_shared<arrow::Int8Array>(1, arrow::Buffer::Wrap(std::vector<int8_t>{83})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{44})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{biz_index})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{46})),
        std::make_shared<arrow::Int8Array>(1, arrow::Buffer::Wrap(std::vector<int8_t>{70})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{47})),
        std::make_shared<arrow::Int8Array>(1, arrow::Buffer::Wrap(std::vector<int8_t>{0})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{price})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{93000002})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{49})),
    };
}

std::shared_ptr<arrow::Schema> tickTestSchema() {
    return arrow::schema({
        arrow::field("nano_timestamp", arrow::int64()),
        arrow::field("Symbol", arrow::int32()),
        arrow::field("Time", arrow::int32()),
        arrow::field("AccTurnover", arrow::int64()),
        arrow::field("AccVolume", arrow::int64()),
        arrow::field("AfterMatchItem", arrow::int32()),
        arrow::field("AfterPrice", arrow::float32()),
        arrow::field("AfterTurnover", arrow::int64()),
        arrow::field("AfterVolume", arrow::int32()),
        arrow::field("AskAvgPrice", arrow::float32()),
        arrow::field("AskPx1", arrow::float32()), arrow::field("AskPx2", arrow::float32()), arrow::field("AskPx3", arrow::float32()),
        arrow::field("AskPx4", arrow::float32()), arrow::field("AskPx5", arrow::float32()), arrow::field("AskPx6", arrow::float32()),
        arrow::field("AskPx7", arrow::float32()), arrow::field("AskPx8", arrow::float32()), arrow::field("AskPx9", arrow::float32()),
        arrow::field("AskPx10", arrow::float32()),
        arrow::field("AskVol1", arrow::float64()), arrow::field("AskVol2", arrow::float64()), arrow::field("AskVol3", arrow::float64()),
        arrow::field("AskVol4", arrow::float64()), arrow::field("AskVol5", arrow::float64()), arrow::field("AskVol6", arrow::float64()),
        arrow::field("AskVol7", arrow::float64()), arrow::field("AskVol8", arrow::float64()), arrow::field("AskVol9", arrow::float64()),
        arrow::field("AskVol10", arrow::float64()),
        arrow::field("BSFlag", arrow::int8()),
        arrow::field("BidAvgPrice", arrow::float32()),
        arrow::field("BidPx1", arrow::float32()), arrow::field("BidPx2", arrow::float32()), arrow::field("BidPx3", arrow::float32()),
        arrow::field("BidPx4", arrow::float32()), arrow::field("BidPx5", arrow::float32()), arrow::field("BidPx6", arrow::float32()),
        arrow::field("BidPx7", arrow::float32()), arrow::field("BidPx8", arrow::float32()), arrow::field("BidPx9", arrow::float32()),
        arrow::field("BidPx10", arrow::float32()),
        arrow::field("BidVol1", arrow::float64()), arrow::field("BidVol2", arrow::float64()), arrow::field("BidVol3", arrow::float64()),
        arrow::field("BidVol4", arrow::float64()), arrow::field("BidVol5", arrow::float64()), arrow::field("BidVol6", arrow::float64()),
        arrow::field("BidVol7", arrow::float64()), arrow::field("BidVol8", arrow::float64()), arrow::field("BidVol9", arrow::float64()),
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

std::vector<std::shared_ptr<arrow::Array>> makeTickTestColumns(
    int64_t nano, int32_t symbol, int32_t biz_index, float price, float ask_px1) {
    return {
        std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{nano})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{symbol})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{93000000})),
        std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{1})),
        std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{2})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{3})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{4.0f})),
        std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{5})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{6})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{7.0f})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{ask_px1})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
        std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{1.0})),
        std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
        std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
        std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
        std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
        std::make_shared<arrow::Int8Array>(1, arrow::Buffer::Wrap(std::vector<int8_t>{1})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{8.0f})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
        std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
        std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
        std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
        std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
        std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{9.0f})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{10.0f})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{11})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{12.0f})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{13.0f})),
        std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{price})),
        std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{14})),
        std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{15})),
        std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{16})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{17})),
        std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{biz_index})),
    };
}

void writeTestParquetFile(const std::string& file_path,
                          const std::shared_ptr<arrow::Schema>& schema,
                          const std::vector<std::shared_ptr<arrow::Array>>& columns);

void writeSingleTickTestFile(const std::string& file_path,
                             int64_t nano,
                             int32_t symbol,
                             int32_t biz_index,
                             float price,
                             float ask_px1) {
    std::vector<int64_t> nano_values{nano};
    std::vector<int32_t> symbol_values{symbol};
    std::vector<int32_t> time_values{93000000};
    std::vector<int64_t> acc_turnover_values{1};
    std::vector<int64_t> acc_volume_values{2};
    std::vector<int32_t> after_match_item_values{3};
    std::vector<float> after_price_values{4.0f};
    std::vector<int64_t> after_turnover_values{5};
    std::vector<int32_t> after_volume_values{6};
    std::vector<float> ask_avg_price_values{7.0f};
    std::vector<float> ask_px1_values{ask_px1};
    std::vector<float> zero_float_values{0.0f};
    std::vector<double> ask_vol1_values{1.0};
    std::vector<double> zero_double_values{0.0};
    std::vector<int8_t> bs_flag_values{1};
    std::vector<float> bid_avg_price_values{8.0f};
    std::vector<float> high_values{9.0f};
    std::vector<float> low_values{10.0f};
    std::vector<int32_t> match_item_values{11};
    std::vector<float> open_values{12.0f};
    std::vector<float> pre_close_values{13.0f};
    std::vector<float> price_values{price};
    std::vector<int64_t> total_ask_volume_values{14};
    std::vector<int64_t> total_bid_volume_values{15};
    std::vector<int64_t> turnover_values{16};
    std::vector<int32_t> volume_values{17};
    std::vector<int32_t> biz_index_values{biz_index};

    writeTestParquetFile(
        file_path,
        tickTestSchema(),
        {
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(nano_values)),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(symbol_values)),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(time_values)),
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(acc_turnover_values)),
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(acc_volume_values)),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(after_match_item_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(after_price_values)),
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(after_turnover_values)),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(after_volume_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(ask_avg_price_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(ask_px1_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(ask_vol1_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::Int8Array>(1, arrow::Buffer::Wrap(bs_flag_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(bid_avg_price_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(zero_float_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(zero_double_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(high_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(low_values)),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(match_item_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(open_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(pre_close_values)),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(price_values)),
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(total_ask_volume_values)),
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(total_bid_volume_values)),
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(turnover_values)),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(volume_values)),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(biz_index_values)),
        });
}

void writeTestParquetFile(const std::string& file_path,
                          const std::shared_ptr<arrow::Schema>& schema,
                          const std::vector<std::shared_ptr<arrow::Array>>& columns) {
    auto table = arrow::Table::Make(schema, columns);

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    if (!file_result.ok()) {
        throw std::runtime_error("cannot create test parquet: " + file_path);
    }

    auto writer_result = parquet::arrow::FileWriter::Open(
        *schema,
        arrow::default_memory_pool(),
        file_result.ValueOrDie(),
        parquet::WriterProperties::Builder().compression(arrow::Compression::SNAPPY)->build(),
        parquet::ArrowWriterProperties::Builder().set_use_threads(false)->store_schema()->build());
    if (!writer_result.ok()) {
        throw std::runtime_error("cannot open test parquet writer: " + file_path);
    }

    auto writer = std::move(writer_result).ValueOrDie();
    auto st = writer->WriteTable(*table, table->num_rows());
    if (!st.ok()) {
        throw std::runtime_error("cannot write test parquet: " + file_path);
    }
    st = writer->Close();
    if (!st.ok()) {
        throw std::runtime_error("cannot close test parquet writer: " + file_path);
    }
}

std::string createTestParquetDir() {
    char templ[] = "/tmp/parquet_replay_testXXXXXX";
    char* dir = mkdtemp(templ);
    if (!dir) {
        throw std::runtime_error("cannot create temporary parquet test directory");
    }

    const std::string dir_path = dir;

    writeTestParquetFile(
        dir_path + "/20260326_tick_data_001.parquet",
        arrow::schema({
            arrow::field("nano_timestamp", arrow::int64()),
            arrow::field("Symbol", arrow::int32()),
            arrow::field("Time", arrow::int32()),
            arrow::field("AccTurnover", arrow::int64()),
            arrow::field("AccVolume", arrow::int64()),
            arrow::field("AfterMatchItem", arrow::int32()),
            arrow::field("AfterPrice", arrow::float32()),
            arrow::field("AfterTurnover", arrow::int64()),
            arrow::field("AfterVolume", arrow::int32()),
            arrow::field("AskAvgPrice", arrow::float32()),
            arrow::field("AskPx1", arrow::float32()), arrow::field("AskPx2", arrow::float32()), arrow::field("AskPx3", arrow::float32()),
            arrow::field("AskPx4", arrow::float32()), arrow::field("AskPx5", arrow::float32()), arrow::field("AskPx6", arrow::float32()),
            arrow::field("AskPx7", arrow::float32()), arrow::field("AskPx8", arrow::float32()), arrow::field("AskPx9", arrow::float32()),
            arrow::field("AskPx10", arrow::float32()),
            arrow::field("AskVol1", arrow::float64()), arrow::field("AskVol2", arrow::float64()), arrow::field("AskVol3", arrow::float64()),
            arrow::field("AskVol4", arrow::float64()), arrow::field("AskVol5", arrow::float64()), arrow::field("AskVol6", arrow::float64()),
            arrow::field("AskVol7", arrow::float64()), arrow::field("AskVol8", arrow::float64()), arrow::field("AskVol9", arrow::float64()),
            arrow::field("AskVol10", arrow::float64()),
            arrow::field("BSFlag", arrow::int8()),
            arrow::field("BidAvgPrice", arrow::float32()),
            arrow::field("BidPx1", arrow::float32()), arrow::field("BidPx2", arrow::float32()), arrow::field("BidPx3", arrow::float32()),
            arrow::field("BidPx4", arrow::float32()), arrow::field("BidPx5", arrow::float32()), arrow::field("BidPx6", arrow::float32()),
            arrow::field("BidPx7", arrow::float32()), arrow::field("BidPx8", arrow::float32()), arrow::field("BidPx9", arrow::float32()),
            arrow::field("BidPx10", arrow::float32()),
            arrow::field("BidVol1", arrow::float64()), arrow::field("BidVol2", arrow::float64()), arrow::field("BidVol3", arrow::float64()),
            arrow::field("BidVol4", arrow::float64()), arrow::field("BidVol5", arrow::float64()), arrow::field("BidVol6", arrow::float64()),
            arrow::field("BidVol7", arrow::float64()), arrow::field("BidVol8", arrow::float64()), arrow::field("BidVol9", arrow::float64()),
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
        }),
        {
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{200})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{3})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{93000000})),
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{1})),
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{2})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{3})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{4.0f})),
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{5})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{6})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{7.0f})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{56.0f})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{1.0})),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
            std::make_shared<arrow::Int8Array>(1, arrow::Buffer::Wrap(std::vector<int8_t>{1})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{8.0f})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})), std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{0.0f})),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})), std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
            std::make_shared<arrow::DoubleArray>(1, arrow::Buffer::Wrap(std::vector<double>{0.0})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{9.0f})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{10.0f})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{11})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{12.0f})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{13.0f})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{55.5f})),
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{14})),
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{15})),
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{16})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{17})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{18})),
        });

    writeTestParquetFile(
        dir_path + "/20260326_order_data_001.parquet",
        arrow::schema({
            arrow::field("nano_timestamp", arrow::int64()),
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
        }),
        {
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{100})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{1})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{11})),
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{22})),
            std::make_shared<arrow::Int8Array>(1, arrow::Buffer::Wrap(std::vector<int8_t>{66})),
            std::make_shared<arrow::Int8Array>(1, arrow::Buffer::Wrap(std::vector<int8_t>{49})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{23})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{24})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{33.5f})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{93000001})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{25})),
        });

    writeTestParquetFile(
        dir_path + "/20260326_trade_data_001.parquet",
        arrow::schema({
            arrow::field("nano_timestamp", arrow::int64()),
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
        }),
        {
            std::make_shared<arrow::Int64Array>(1, arrow::Buffer::Wrap(std::vector<int64_t>{150})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{2})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{41})),
            std::make_shared<arrow::Int8Array>(1, arrow::Buffer::Wrap(std::vector<int8_t>{83})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{44})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{45})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{46})),
            std::make_shared<arrow::Int8Array>(1, arrow::Buffer::Wrap(std::vector<int8_t>{70})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{47})),
            std::make_shared<arrow::Int8Array>(1, arrow::Buffer::Wrap(std::vector<int8_t>{0})),
            std::make_shared<arrow::FloatArray>(1, arrow::Buffer::Wrap(std::vector<float>{48.5f})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{93000002})),
            std::make_shared<arrow::Int32Array>(1, arrow::Buffer::Wrap(std::vector<int32_t>{49})),
        });

    return dir_path;
}

std::string createMultiFileParquetDir() {
    const std::string dir_path = createTestParquetDir();

    writeTestParquetFile(
        dir_path + "/20260326_order_data_002.parquet",
        orderTestSchema(),
        makeOrderTestColumns(450, 101, 111, 133.5f));

    writeTestParquetFile(
        dir_path + "/20260326_trade_data_002.parquet",
        tradeTestSchema(),
        makeTradeTestColumns(175, 202, 212, 248.5f));

    writeTestParquetFile(
        dir_path + "/20260326_tick_data_002.parquet",
        tickTestSchema(),
        makeTickTestColumns(300, 303, 318, 355.5f, 356.0f));

    return dir_path;
}

std::string createSplitTickParquetDir() {
    char templ[] = "/tmp/parquet_split_tick_testXXXXXX";
    char* dir = mkdtemp(templ);
    if (!dir) {
        throw std::runtime_error("cannot create split tick parquet test directory");
    }

    const std::string dir_path = dir;

    writeSingleTickTestFile(dir_path + "/20260326_tick_data_001.parquet", 100, 301, 401, 10.0f, 10.1f);
    writeSingleTickTestFile(dir_path + "/20260326_tick_data_002.parquet", 120, 302, 402, 20.0f, 20.1f);
    writeSingleTickTestFile(dir_path + "/20260326_tick_data_003.parquet", 150, 303, 403, 30.0f, 30.1f);
    writeSingleTickTestFile(dir_path + "/20260326_tick_data_004.parquet", 180, 304, 404, 40.0f, 40.1f);
    writeSingleTickTestFile(dir_path + "/20260326_tick_data_005.parquet", 200, 305, 405, 50.0f, 50.1f);

    return dir_path;
}

std::string createPoisonParquetDir() {
    const std::string dir_path = createTestParquetDir();

    FILE* file = std::fopen((dir_path + "/20260326_order_data_002.parquet").c_str(), "wb");
    if (!file) {
        throw std::runtime_error("cannot create poisoned parquet test file");
    }
    const char* text = "not a parquet file";
    std::fwrite(text, 1, std::strlen(text), file);
    std::fclose(file);

    return dir_path;
}

using AppendFramesFn = void (*)(const std::string&, long, std::vector<ReplayFrame>*);

struct ParquetFileDescriptor {
    std::string channel_name;
    std::string file_path;
    std::size_t file_index = 0;
    std::size_t total_files = 0;
};

struct HeapEntry {
    ReplayFrame frame;
    std::size_t channel_index = 0;
};

struct HeapEntryCompare {
    bool operator()(const HeapEntry& lhs, const HeapEntry& rhs) const {
        if (lhs.frame.nano != rhs.frame.nano) {
            return lhs.frame.nano > rhs.frame.nano;
        }
        if (lhs.frame.msg_type != rhs.frame.msg_type) {
            return lhs.frame.msg_type > rhs.frame.msg_type;
        }
        return lhs.channel_index > rhs.channel_index;
    }
};

class ParquetFileBuffer {
public:
    ParquetFileBuffer(ParquetFileDescriptor descriptor, long resume_nano, AppendFramesFn append_frames)
        : descriptor_(std::move(descriptor)) {
        append_frames(descriptor_.file_path, resume_nano, &frames_);
    }

    bool hasNext() const {
        return next_index_ < frames_.size();
    }

    const ReplayFrame& current() const {
        return frames_[next_index_];
    }

    void pop() {
        ++next_index_;
    }

    const ParquetFileDescriptor& descriptor() const {
        return descriptor_;
    }

private:
    ParquetFileDescriptor descriptor_;
    std::vector<ReplayFrame> frames_;
    std::size_t next_index_ = 0;
};

class ParquetChannelPrefetcher {
public:
    ParquetChannelPrefetcher(const std::string& channel_name,
                            std::vector<std::string> file_paths,
                            long resume_nano,
                            AppendFramesFn append_frames)
        : channel_name_(channel_name),
          file_paths_(std::move(file_paths)),
          resume_nano_(resume_nano),
          append_frames_(append_frames) {
        std::cout << "[parquet] " << channel_name_ << " files: " << file_paths_.size() << std::endl;
        active_buffer_ = loadNextBufferSync();
        if (active_buffer_) {
            logOpen(*active_buffer_);
        }
        worker_ = std::thread(&ParquetChannelPrefetcher::prefetchLoop, this);
        requestStandbyPrefetch();
    }

    ~ParquetChannelPrefetcher() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_worker_ = true;
            cond_.notify_all();
        }
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    bool hasCurrent() const {
        return active_buffer_ && active_buffer_->hasNext();
    }

    const ReplayFrame& current() const {
        return active_buffer_->current();
    }

    void pop() {
        active_buffer_->pop();
        if (!active_buffer_->hasNext()) {
            logFinish(*active_buffer_);
            promoteStandby();
        }
    }

private:
    std::unique_ptr<ParquetFileBuffer> makeBuffer(std::size_t file_index) {
        return std::unique_ptr<ParquetFileBuffer>(new ParquetFileBuffer(
            ParquetFileDescriptor{channel_name_, file_paths_[file_index], file_index + 1, file_paths_.size()},
            resume_nano_,
            append_frames_));
    }

    std::unique_ptr<ParquetFileBuffer> loadNextBufferSync() {
        while (next_file_index_ < file_paths_.size()) {
            std::unique_ptr<ParquetFileBuffer> buffer = makeBuffer(next_file_index_++);
            if (buffer->hasNext()) {
                return buffer;
            }
        }
        return std::unique_ptr<ParquetFileBuffer>();
    }

    std::unique_ptr<ParquetFileBuffer> loadNextBufferAsync() {
        while (true) {
            std::string file_path;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (next_file_index_ >= file_paths_.size()) {
                    return std::unique_ptr<ParquetFileBuffer>();
                }
                file_path = file_paths_[next_file_index_];
                ++next_file_index_;
            }

            std::size_t file_index = 0;
            for (std::size_t i = 0; i < file_paths_.size(); ++i) {
                if (file_paths_[i] == file_path) {
                    file_index = i;
                    break;
                }
            }
            std::unique_ptr<ParquetFileBuffer> buffer = makeBuffer(file_index);
            if (buffer->hasNext()) {
                return buffer;
            }
        }
    }

    void logOpen(const ParquetFileBuffer& buffer) const {
        const ParquetFileDescriptor& descriptor = buffer.descriptor();
        std::cout << "[parquet] open " << descriptor.channel_name
                  << " file " << descriptor.file_index << "/" << descriptor.total_files
                  << ": " << descriptor.file_path << std::endl;
    }

    void logFinish(const ParquetFileBuffer& buffer) const {
        const ParquetFileDescriptor& descriptor = buffer.descriptor();
        std::cout << "[parquet] finish " << descriptor.channel_name
                  << " file " << descriptor.file_index << "/" << descriptor.total_files << std::endl;
    }

    void requestStandbyPrefetch() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stop_worker_ || standby_loading_ || standby_ready_) {
            return;
        }
        if (next_file_index_ >= file_paths_.size()) {
            standby_ready_ = true;
            standby_exhausted_ = true;
            return;
        }
        standby_loading_ = true;
        standby_ready_ = false;
        standby_exhausted_ = false;
        standby_error_ = nullptr;
        cond_.notify_all();
    }

    void prefetchLoop() {
        while (true) {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cond_.wait(lock, [this]() {
                    return stop_worker_ || standby_loading_;
                });
                if (stop_worker_) {
                    return;
                }
            }

            std::unique_ptr<ParquetFileBuffer> buffer;
            std::exception_ptr error;
            try {
                buffer = loadNextBufferAsync();
            } catch (...) {
                error = std::current_exception();
            }

            {
                std::lock_guard<std::mutex> lock(mutex_);
                standby_buffer_ = std::move(buffer);
                standby_error_ = error;
                standby_exhausted_ = !standby_buffer_ && !standby_error_;
                standby_loading_ = false;
                standby_ready_ = true;
            }
            cond_.notify_all();
        }
    }

    void promoteStandby() {
        requestStandbyPrefetch();

        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this]() {
            return standby_ready_ || stop_worker_;
        });

        if (standby_error_) {
            std::exception_ptr error = standby_error_;
            standby_error_ = nullptr;
            standby_ready_ = false;
            lock.unlock();
            std::rethrow_exception(error);
        }

        active_buffer_ = std::move(standby_buffer_);
        standby_ready_ = false;
        standby_exhausted_ = false;
        lock.unlock();

        if (active_buffer_) {
            logOpen(*active_buffer_);
            requestStandbyPrefetch();
        }
    }

    std::string channel_name_;
    std::vector<std::string> file_paths_;
    std::size_t next_file_index_ = 0;
    long resume_nano_ = 0;
    AppendFramesFn append_frames_ = nullptr;
    std::unique_ptr<ParquetFileBuffer> active_buffer_;
    std::unique_ptr<ParquetFileBuffer> standby_buffer_;
    std::exception_ptr standby_error_;
    bool standby_loading_ = false;
    bool standby_ready_ = false;
    bool standby_exhausted_ = false;
    bool stop_worker_ = false;
    std::mutex mutex_;
    std::condition_variable cond_;
    std::thread worker_;
};

}  // namespace

class ParquetReplayStream::Impl {
public:
    Impl(const std::string& date_dir, long resume_nano) : resume_nano_(resume_nano) {
        channels_.emplace_back(new ParquetChannelPrefetcher(
            "tick", listFiles(date_dir, "_tick_data_", ".parquet"), 0, appendTickFrames));
        channels_.emplace_back(new ParquetChannelPrefetcher(
            "order", listFiles(date_dir, "_order_data_", ".parquet"), 0, appendOrderFrames));
        channels_.emplace_back(new ParquetChannelPrefetcher(
            "trade", listFiles(date_dir, "_trade_data_", ".parquet"), 0, appendTradeFrames));

        for (std::size_t i = 0; i < channels_.size(); ++i) {
            pushCurrent(i);
        }
    }

    bool hasNext() const {
        if (terminal_error_) {
            std::rethrow_exception(terminal_error_);
        }
        return !heap_.empty();
    }

    long firstNano() const {
        if (terminal_error_) {
            std::rethrow_exception(terminal_error_);
        }
        if (heap_.empty()) {
            throw std::runtime_error("no parquet replay frames available");
        }
        return heap_.top().frame.nano;
    }

    ReplayFrame popNext() {
        if (terminal_error_) {
            std::rethrow_exception(terminal_error_);
        }

        if (heap_.empty()) {
            throw std::runtime_error("no more parquet replay frames");
        }

        HeapEntry entry = heap_.top();
        heap_.pop();

        try {
            channels_[entry.channel_index]->pop();
            pushCurrent(entry.channel_index);
        } catch (...) {
            terminal_error_ = std::current_exception();
        }

        return entry.frame;
    }

private:
    void pushCurrent(std::size_t channel_index) {
        while (channels_[channel_index]->hasCurrent() &&
               channels_[channel_index]->current().nano <= resume_nano_) {
            channels_[channel_index]->pop();
        }
        if (channels_[channel_index]->hasCurrent()) {
            heap_.push(HeapEntry{channels_[channel_index]->current(), channel_index});
        }
    }

    std::vector<std::unique_ptr<ParquetChannelPrefetcher>> channels_;
    std::priority_queue<HeapEntry, std::vector<HeapEntry>, HeapEntryCompare> heap_;
    long resume_nano_ = 0;
    std::exception_ptr terminal_error_;
};

ReplayOptions parseReplayOptions(int argc, const char* argv[]) {
    ReplayOptions opts;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--source") {
            if (i + 1 >= argc) {
                throw std::runtime_error("missing value for --source");
            }
            opts.source = argv[++i];
        } else if (arg == "--date-dir") {
            if (i + 1 >= argc) {
                throw std::runtime_error("missing value for --date-dir");
            }
            opts.date_dir = argv[++i];
        } else {
            opts.speed = std::atof(arg.c_str());
            if (opts.speed <= 0) {
                opts.speed = 1.0;
            }
        }
    }

    if (opts.source != "journal" && opts.source != "parquet") {
        throw std::runtime_error("unsupported --source: " + opts.source);
    }
    if (opts.source == "parquet" && opts.date_dir.empty()) {
        throw std::runtime_error("--date-dir is required when --source parquet");
    }

    return opts;
}

ReplayOptions parseReplayOptionsForTest(const std::vector<std::string>& args) {
    std::vector<const char*> argv;
    argv.reserve(args.size());
    for (const auto& arg : args) {
        argv.push_back(arg.c_str());
    }
    return parseReplayOptions(static_cast<int>(argv.size()), argv.data());
}

ParquetReplayStream::ParquetReplayStream(const std::string& date_dir, long resume_nano)
    : impl_(new Impl(date_dir, resume_nano)) {
}

ParquetReplayStream::~ParquetReplayStream() = default;

bool ParquetReplayStream::hasNext() const {
    return impl_->hasNext();
}

long ParquetReplayStream::firstNano() const {
    return impl_->firstNano();
}

ReplayFrame ParquetReplayStream::popNext() {
    return impl_->popNext();
}

std::vector<ReplayFrame> loadParquetFrames(const std::string& date_dir, long resume_nano) {
    std::vector<ReplayFrame> frames;

    for (const auto& file_path : listFiles(date_dir, "_tick_data_", ".parquet")) {
        std::size_t start_index = frames.size();
        appendTickFrames(file_path, resume_nano, &frames);
        filterFramesAfterResume(&frames, start_index, resume_nano);
    }
    for (const auto& file_path : listFiles(date_dir, "_order_data_", ".parquet")) {
        std::size_t start_index = frames.size();
        appendOrderFrames(file_path, resume_nano, &frames);
        filterFramesAfterResume(&frames, start_index, resume_nano);
    }
    for (const auto& file_path : listFiles(date_dir, "_trade_data_", ".parquet")) {
        std::size_t start_index = frames.size();
        appendTradeFrames(file_path, resume_nano, &frames);
        filterFramesAfterResume(&frames, start_index, resume_nano);
    }

    std::sort(frames.begin(), frames.end(), [](const ReplayFrame& lhs, const ReplayFrame& rhs) {
        if (lhs.nano != rhs.nano) {
            return lhs.nano < rhs.nano;
        }
        return lhs.msg_type < rhs.msg_type;
    });

    return frames;
}

std::vector<ReplayFrame> loadParquetFramesForTest(const std::string& date_dir, long resume_nano) {
    return loadParquetFrames(date_dir, resume_nano);
}

std::string createTestParquetDirForTest() {
    return createTestParquetDir();
}

std::string createMultiFileParquetDirForTest() {
    return createMultiFileParquetDir();
}

std::string createSplitTickParquetDirForTest() {
    return createSplitTickParquetDir();
}

std::string createPoisonParquetDirForTest() {
    return createPoisonParquetDir();
}

std::vector<long> build_test_parquet_replay_nanos() {
    const std::string dir_path = createTestParquetDir();
    std::vector<long> nanos;
    for (const auto& frame : loadParquetFrames(dir_path, 0)) {
        nanos.push_back(frame.nano);
    }
    return nanos;
}
