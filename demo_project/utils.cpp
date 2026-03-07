#include "utils.h"
#include <ctime>

static const long NANOSECONDS_PER_SECOND = 1000000000;

std::string decode_exchange(int symbol)
{
	std::string exchange = "";
	if(symbol < 400000)
		exchange = "SZ";
	else if(symbol < 700000)
		exchange = "SH";

	return exchange;
}

int32_t parse_nano(long nano) {
    if (nano <= 0) {
        return 0;
    }
    long seconds = nano / NANOSECONDS_PER_SECOND;
    int millisecond = (nano % NANOSECONDS_PER_SECOND) / 1000000;
    struct tm* dt;
    dt = localtime(&seconds);
    int hour = dt->tm_hour;
    int minute = dt->tm_min;
    int second = dt->tm_sec;
	int res = millisecond + second * 1000 + minute * 100000 + hour * 10000000;
	return res;
}
