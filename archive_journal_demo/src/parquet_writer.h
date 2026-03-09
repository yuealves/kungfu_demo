#pragma once

#include "l2_types.h"
#include <string>
#include <vector>

// 每个 row group 的行数上限
static constexpr int ROW_GROUP_SIZE = 100000;

// 压缩后每行的估算字节数（用于计算文件分片行数）
static constexpr size_t EST_BYTES_PER_ROW_TICK  = 110;
static constexpr size_t EST_BYTES_PER_ROW_ORDER = 30;
static constexpr size_t EST_BYTES_PER_ROW_TRADE = 36;

// max_rows_per_file = 0 表示不分片（但 num_threads > 1 时仍会拆分以并行写入）
// num_threads: 并行写入线程数，>1 时保证至少拆分为 num_threads 个文件
void write_tick_parquet(const std::vector<TickRecord>& records, const std::string& filepath, size_t max_rows_per_file = 0, int num_threads = 1);
void write_order_parquet(const std::vector<OrderRecord>& records, const std::string& filepath, size_t max_rows_per_file = 0, int num_threads = 1);
void write_trade_parquet(const std::vector<TradeRecord>& records, const std::string& filepath, size_t max_rows_per_file = 0, int num_threads = 1);
