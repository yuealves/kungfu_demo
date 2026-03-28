#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE TimeUtilsTest
#include <boost/test/unit_test.hpp>
#include "time_utils.h"

BOOST_AUTO_TEST_CASE(test_tm_int_to_ms) {
    // 93000000 = 09:30:00.000 → (9*3600+30*60)*1000 = 34200000
    BOOST_CHECK_EQUAL(rtf::tm_int_to_ms(93000000), 34200000LL);
    // 145959999 = 14:59:59.999
    BOOST_CHECK_EQUAL(rtf::tm_int_to_ms(145959999),
        (14*3600+59*60+59)*1000LL + 999);
    // 92500000 = 09:25:00.000
    BOOST_CHECK_EQUAL(rtf::tm_int_to_ms(92500000), (9*3600+25*60)*1000LL);
}

BOOST_AUTO_TEST_CASE(test_continuous_session) {
    // 集合竞价: 09:25:00.000 < OPEN_MS(09:29:57) → +5min
    int64_t ms_0925 = rtf::tm_int_to_ms(92500000);
    int64_t cont_0925 = rtf::apply_continuous_session(ms_0925);
    BOOST_CHECK_EQUAL(cont_0925, ms_0925 + 5*60*1000);

    // 午盘: 13:00:00.000 > CUTOFF_MS(12:59:57) → -90min
    int64_t ms_1300 = rtf::tm_int_to_ms(130000000);
    int64_t cont_1300 = rtf::apply_continuous_session(ms_1300);
    BOOST_CHECK_EQUAL(cont_1300, ms_1300 - 90*60*1000);

    // 正常盘中: 10:00:00.000 → 不变
    int64_t ms_1000 = rtf::tm_int_to_ms(100000000);
    BOOST_CHECK_EQUAL(rtf::apply_continuous_session(ms_1000), ms_1000);
}

BOOST_AUTO_TEST_CASE(test_tm_to_continuous_ms) {
    // 便捷函数: 组合 tm_int_to_ms + apply_continuous_session
    int64_t result = rtf::tm_to_continuous_ms(93000000);
    BOOST_CHECK_EQUAL(result, rtf::tm_int_to_ms(93000000));
}
