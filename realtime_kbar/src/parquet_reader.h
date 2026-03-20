#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>

// A single HT tick row (fields needed for kbar synthesis)
struct TickRecord {
    int32_t symbol;
    int32_t time;           // HHMMSSmmm
    float   price;          // raw × 10000
    float   high;           // raw × 10000
    float   low;            // raw × 10000
    int64_t acc_volume;
    int64_t acc_turnover;
    int64_t nano_timestamp;
    bool    is_sh;          // true=SH(.XSHG), false=SZ(.XSHE)
};

// A single JQ price_1m bar
struct JqBar {
    std::string code;       // e.g. "000001.XSHE"
    int bar_minute;         // HHMM (e.g. 931, 1500)
    float open, close, high, low;
    double money;
    int64_t volume;
};

// Read all HT tick parquet files for a date.
// date_str: "YYYY-MM-DD", dir: root directory (e.g. "/data/tickl2_data_ht2_raw")
// Returns ticks sorted by (symbol, nano_timestamp).
std::vector<TickRecord> read_ht_ticks(const std::string& dir,
                                       const std::string& date_str,
                                       int32_t filter_symbol = 0);

// Read JQ price_1m parquet for a date.
// Returns all bars.
std::vector<JqBar> read_jq_bars(const std::string& path);

// Read prev day close from JQ price_1m directory.
// Returns map: symbol (int) → prev_day_close (float).
std::unordered_map<int32_t, float> read_prev_day_close(const std::string& jq_dir,
                                                        const std::string& date_str);

// Parse JQ code string to symbol int. "000001.XSHE" → 1
int32_t code_to_symbol(const std::string& code);
