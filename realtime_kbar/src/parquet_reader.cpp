#include "parquet_reader.h"

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>

#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <dirent.h>
#include <cstring>

// ─────────────────── Helpers ───────────────────

// List files in a directory matching a pattern
static std::vector<std::string> list_files(const std::string& dir_path,
                                            const std::string& contains,
                                            const std::string& suffix) {
    std::vector<std::string> result;
    DIR* dir = opendir(dir_path.c_str());
    if (!dir) return result;
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

// List parquet files in a directory
static std::vector<std::string> list_parquet_files(const std::string& dir_path) {
    std::vector<std::string> result;
    DIR* dir = opendir(dir_path.c_str());
    if (!dir) return result;
    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        std::string name = entry->d_name;
        if (name.size() > 8 && name.substr(name.size() - 8) == ".parquet")
            result.push_back(dir_path + "/" + name);
    }
    closedir(dir);
    std::sort(result.begin(), result.end());
    return result;
}

// Get filename stem (without path and extension)
static std::string file_stem(const std::string& path) {
    size_t slash = path.rfind('/');
    std::string name = (slash != std::string::npos) ? path.substr(slash + 1) : path;
    size_t dot = name.rfind('.');
    return (dot != std::string::npos) ? name.substr(0, dot) : name;
}

static std::shared_ptr<arrow::Table> read_parquet_columns(
    const std::string& path, const std::vector<std::string>& col_names)
{
    auto file_result = arrow::io::ReadableFile::Open(path);
    if (!file_result.ok())
        throw std::runtime_error("Cannot open: " + path + " — " + file_result.status().ToString());

    auto reader_result = parquet::arrow::OpenFile(
        file_result.ValueOrDie(), arrow::default_memory_pool());
    if (!reader_result.ok())
        throw std::runtime_error("Parquet open: " + reader_result.status().ToString());
    auto reader = std::move(reader_result).ValueOrDie();

    // Map column names to indices
    auto schema = reader->parquet_reader()->metadata()->schema();
    std::vector<int> indices;
    for (auto& name : col_names) {
        int idx = -1;
        for (int c = 0; c < schema->num_columns(); ++c) {
            if (schema->Column(c)->name() == name) { idx = c; break; }
        }
        if (idx < 0)
            throw std::runtime_error("Column not found: " + name + " in " + path);
        indices.push_back(idx);
    }

    std::shared_ptr<arrow::Table> table;
    auto st = reader->ReadTable(indices, &table);
    if (!st.ok()) throw std::runtime_error("Read columns: " + st.ToString());
    return table;
}

// ─────────────────── read_ht_ticks ───────────────────

std::vector<TickRecord> read_ht_ticks(const std::string& dir,
                                       const std::string& date_str,
                                       int32_t filter_symbol)
{
    std::string date_nodash = date_str;
    date_nodash.erase(std::remove(date_nodash.begin(), date_nodash.end(), '-'), date_nodash.end());

    std::string date_dir = dir + "/" + date_str;
    std::string pattern = date_nodash + "_tick_data_";

    auto files = list_files(date_dir, pattern, ".parquet");
    if (files.empty())
        throw std::runtime_error("No tick files found in " + date_dir);

    std::vector<std::string> cols = {
        "Symbol", "Time", "Price", "High", "Low",
        "AccVolume", "AccTurnover", "nano_timestamp", "exchange"
    };

    std::vector<TickRecord> all_ticks;

    for (auto& fpath : files) {
        auto table = read_parquet_columns(fpath, cols);
        auto combined_result = table->CombineChunks(arrow::default_memory_pool());
        if (!combined_result.ok())
            throw std::runtime_error("CombineChunks: " + combined_result.status().ToString());
        table = combined_result.ValueOrDie();

        int64_t nrows = table->num_rows();

        auto sym_arr   = std::static_pointer_cast<arrow::Int32Array>(table->GetColumnByName("Symbol")->chunk(0));
        auto time_arr  = std::static_pointer_cast<arrow::Int32Array>(table->GetColumnByName("Time")->chunk(0));
        auto price_arr = std::static_pointer_cast<arrow::FloatArray>(table->GetColumnByName("Price")->chunk(0));
        auto high_arr  = std::static_pointer_cast<arrow::FloatArray>(table->GetColumnByName("High")->chunk(0));
        auto low_arr   = std::static_pointer_cast<arrow::FloatArray>(table->GetColumnByName("Low")->chunk(0));
        auto vol_arr   = std::static_pointer_cast<arrow::Int64Array>(table->GetColumnByName("AccVolume")->chunk(0));
        auto tov_arr   = std::static_pointer_cast<arrow::Int64Array>(table->GetColumnByName("AccTurnover")->chunk(0));
        auto nano_arr  = std::static_pointer_cast<arrow::Int64Array>(table->GetColumnByName("nano_timestamp")->chunk(0));
        auto exch_arr  = std::static_pointer_cast<arrow::StringArray>(table->GetColumnByName("exchange")->chunk(0));

        all_ticks.reserve(all_ticks.size() + nrows);

        for (int64_t i = 0; i < nrows; ++i) {
            int32_t sym = sym_arr->Value(i);
            if (filter_symbol > 0 && sym != filter_symbol) continue;

            TickRecord rec;
            rec.symbol = sym;
            rec.time = time_arr->Value(i);
            rec.price = price_arr->Value(i);
            rec.high = high_arr->Value(i);
            rec.low = low_arr->Value(i);
            rec.acc_volume = vol_arr->Value(i);
            rec.acc_turnover = tov_arr->Value(i);
            rec.nano_timestamp = nano_arr->Value(i);
            rec.is_sh = (exch_arr->GetString(i) == "SH");
            all_ticks.push_back(rec);
        }
    }

    // Sort by (symbol, nano_timestamp)
    std::sort(all_ticks.begin(), all_ticks.end(),
              [](const TickRecord& a, const TickRecord& b) {
                  if (a.symbol != b.symbol) return a.symbol < b.symbol;
                  return a.nano_timestamp < b.nano_timestamp;
              });

    return all_ticks;
}

// ─────────────────── read_jq_bars ───────────────────

std::vector<JqBar> read_jq_bars(const std::string& path) {
    auto table = read_parquet_columns(path,
        {"datetime", "code", "open", "close", "high", "low", "money", "volume"});

    auto combined_result = table->CombineChunks(arrow::default_memory_pool());
    if (!combined_result.ok())
        throw std::runtime_error("CombineChunks: " + combined_result.status().ToString());
    table = combined_result.ValueOrDie();

    int64_t nrows = table->num_rows();

    auto dt_arr    = std::static_pointer_cast<arrow::TimestampArray>(
                         table->GetColumnByName("datetime")->chunk(0));
    auto code_arr  = std::static_pointer_cast<arrow::LargeStringArray>(
                         table->GetColumnByName("code")->chunk(0));
    auto open_arr  = std::static_pointer_cast<arrow::FloatArray>(
                         table->GetColumnByName("open")->chunk(0));
    auto close_arr = std::static_pointer_cast<arrow::FloatArray>(
                         table->GetColumnByName("close")->chunk(0));
    auto high_arr  = std::static_pointer_cast<arrow::FloatArray>(
                         table->GetColumnByName("high")->chunk(0));
    auto low_arr   = std::static_pointer_cast<arrow::FloatArray>(
                         table->GetColumnByName("low")->chunk(0));
    auto money_arr = std::static_pointer_cast<arrow::DoubleArray>(
                         table->GetColumnByName("money")->chunk(0));
    auto vol_arr   = std::static_pointer_cast<arrow::Int64Array>(
                         table->GetColumnByName("volume")->chunk(0));

    std::vector<JqBar> bars;
    bars.reserve(nrows);

    for (int64_t i = 0; i < nrows; ++i) {
        JqBar bar;
        bar.code = code_arr->GetString(i);

        // datetime is timestamp[us] — microseconds since epoch (UTC, no timezone)
        int64_t us = dt_arr->Value(i);
        time_t sec = us / 1000000;
        struct tm t;
        gmtime_r(&sec, &t);
        bar.bar_minute = t.tm_hour * 100 + t.tm_min;

        bar.open   = open_arr->Value(i);
        bar.close  = close_arr->Value(i);
        bar.high   = high_arr->Value(i);
        bar.low    = low_arr->Value(i);
        bar.money  = money_arr->Value(i);
        bar.volume = vol_arr->Value(i);
        bars.push_back(bar);
    }

    return bars;
}

// ─────────────────── read_prev_day_close ───────────────────

int32_t code_to_symbol(const std::string& code) {
    if (code.size() < 6) return 0;
    return std::stoi(code.substr(0, 6));
}

std::unordered_map<int32_t, float> read_prev_day_close(
    const std::string& jq_dir, const std::string& date_str)
{
    std::unordered_map<int32_t, float> result;

    auto all_files = list_parquet_files(jq_dir);
    std::vector<std::string> prev_files;
    for (auto& f : all_files) {
        std::string stem = file_stem(f);
        if (stem < date_str)
            prev_files.push_back(f);
    }
    if (prev_files.empty()) return result;

    std::string prev_file = prev_files.back();

    auto table = read_parquet_columns(prev_file, {"code", "close"});
    auto combined_result = table->CombineChunks(arrow::default_memory_pool());
    if (!combined_result.ok()) return result;
    table = combined_result.ValueOrDie();

    int64_t nrows = table->num_rows();
    auto code_arr  = std::static_pointer_cast<arrow::LargeStringArray>(
                         table->GetColumnByName("code")->chunk(0));
    auto close_arr = std::static_pointer_cast<arrow::FloatArray>(
                         table->GetColumnByName("close")->chunk(0));

    std::unordered_map<std::string, float> code_close;
    for (int64_t i = 0; i < nrows; ++i) {
        code_close[code_arr->GetString(i)] = close_arr->Value(i);
    }

    for (auto& [code, close] : code_close) {
        int32_t sym = code_to_symbol(code);
        if (sym > 0) result[sym] = close;
    }

    return result;
}
