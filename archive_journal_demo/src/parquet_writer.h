#pragma once

#include "l2_types.h"
#include <string>
#include <vector>

// 每个 row group 的行数上限
static constexpr int ROW_GROUP_SIZE = 100000;

void write_tick_parquet(const std::vector<TickRecord>& records, const std::string& filepath);
void write_order_parquet(const std::vector<OrderRecord>& records, const std::string& filepath);
void write_trade_parquet(const std::vector<TradeRecord>& records, const std::string& filepath);
