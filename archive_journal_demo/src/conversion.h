#pragma once

#include "data_types.h"
#include "l2_types.h"
#include <string>
#include <cstdint>

// 纳秒时间戳 → HHMMSSmmm 格式 int32
int32_t parse_nano(long nano);

// Symbol 数值 → 交易所字符串
std::string decode_exchange(int symbol);

// KyStd* → 统一输出记录
TickRecord  convert_tick(const KyStdSnpType& src, long nano);
OrderRecord convert_order(const KyStdOrderType& src, long nano);
TradeRecord convert_trade(const KyStdTradeType& src, long nano);
