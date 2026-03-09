#pragma once

#include "data_types.h"
#include "l2_types.h"
#include <string>
#include <cstdint>

// Symbol 数值 → 交易所字符串
std::string decode_exchange(int symbol);

// KyStd* → 输出记录（1:1 全字段保留）
TickRecord  convert_tick(const KyStdSnpType& src, long nano);
OrderRecord convert_order(const KyStdOrderType& src, long nano);
TradeRecord convert_trade(const KyStdTradeType& src, long nano);
