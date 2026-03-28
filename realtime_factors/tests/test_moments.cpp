#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE MomentsTest
#include <boost/test/unit_test.hpp>
#include "moments.h"
#include <cmath>
#include <vector>

static constexpr double EPS = 1e-9;

BOOST_AUTO_TEST_CASE(test_empty_moments) {
    rtf::Moments m;
    BOOST_CHECK_EQUAL(m.count(), 0);
    BOOST_CHECK(std::isnan(m.mean()));
    BOOST_CHECK(std::isnan(m.std_sample()));
    BOOST_CHECK(std::isnan(m.skew()));
    BOOST_CHECK(std::isnan(m.kurt()));
}

BOOST_AUTO_TEST_CASE(test_single_value) {
    rtf::Moments m;
    m.update(5.0);
    BOOST_CHECK_EQUAL(m.count(), 1);
    BOOST_CHECK_CLOSE(m.mean(), 5.0, EPS);
    BOOST_CHECK(std::isnan(m.std_sample())); // n=1, 样本std无定义
    BOOST_CHECK(std::isnan(m.skew()));       // n<3
    BOOST_CHECK(std::isnan(m.kurt()));       // n<4
}

BOOST_AUTO_TEST_CASE(test_known_sequence) {
    // 值: 1,2,3,4,5 → mean=3, sample_std=sqrt(2.5)≈1.5811
    // s1=15, s2=55, s3=225, s4=979
    // population var = 55/5 - 9 = 2.0
    // skew_num = 225/5 - 3*3*11 + 2*27 = 45-99+54 = 0 → skew=0
    // kurt_num = 979/5 - 4*3*45 + 6*9*11 - 3*81 = 195.8-540+594-243 = 6.8
    // kurt = 6.8/4 - 3 = 1.7 - 3 = -1.3
    rtf::Moments m;
    for (double v : {1.0, 2.0, 3.0, 4.0, 5.0}) m.update(v);
    BOOST_CHECK_EQUAL(m.count(), 5);
    BOOST_CHECK_CLOSE(m.mean(), 3.0, EPS);
    BOOST_CHECK_CLOSE(m.std_sample(), std::sqrt(2.5), 1e-6);
    BOOST_CHECK_SMALL(m.skew(), 1e-6);  // 对称分布 skew=0
    BOOST_CHECK_CLOSE(m.kurt(), -1.3, 1e-4);
}

BOOST_AUTO_TEST_CASE(test_merge_two_moments) {
    rtf::Moments a, b;
    a.update(1.0); a.update(2.0); a.update(3.0);
    b.update(4.0); b.update(5.0);

    rtf::Moments merged = rtf::Moments::merge(a, b);
    BOOST_CHECK_EQUAL(merged.count(), 5);
    BOOST_CHECK_CLOSE(merged.mean(), 3.0, EPS);

    // 应该和一次性输入等价
    rtf::Moments full;
    for (double v : {1.0, 2.0, 3.0, 4.0, 5.0}) full.update(v);
    BOOST_CHECK_CLOSE(merged.std_sample(), full.std_sample(), 1e-9);
    BOOST_CHECK_SMALL(merged.skew() - full.skew(), 1e-9);
    BOOST_CHECK_CLOSE(merged.kurt(), full.kurt(), 1e-6);
}

BOOST_AUTO_TEST_CASE(test_vwapx) {
    // vwapx = money_s1 / vol_s1
    rtf::Moments vol_m, money_m;
    vol_m.update(100); vol_m.update(200);
    money_m.update(1000); money_m.update(2200);
    // vwapx = (1000+2200) / (100+200) = 3200/300 ≈ 10.6667
    double vwapx = money_m.s1() / vol_m.s1();
    BOOST_CHECK_CLOSE(vwapx, 3200.0/300.0, 1e-9);
}

BOOST_AUTO_TEST_CASE(test_constant_values_skew_null) {
    // 所有值相同 → variance=0 → skew/kurt 应返回 NaN
    rtf::Moments m;
    for (int i = 0; i < 10; ++i) m.update(42.0);
    BOOST_CHECK(std::isnan(m.skew()));
    BOOST_CHECK(std::isnan(m.kurt()));
}
