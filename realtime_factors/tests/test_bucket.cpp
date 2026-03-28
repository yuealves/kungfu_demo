#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE BucketTest
#include <boost/test/unit_test.hpp>
#include "bucket_classify.h"

BOOST_AUTO_TEST_CASE(test_large) {
    BOOST_CHECK(rtf::is_large(100000, 0));      // vol >= 100k
    BOOST_CHECK(rtf::is_large(0, 1000000));      // money >= 1M
    BOOST_CHECK(rtf::is_large(200000, 2000000)); // both
    BOOST_CHECK(!rtf::is_large(99999, 999999));  // neither
}

BOOST_AUTO_TEST_CASE(test_small) {
    BOOST_CHECK(rtf::is_small(10000, 100000));   // cond1
    BOOST_CHECK(rtf::is_small(1000, 500000));    // cond2
    BOOST_CHECK(rtf::is_small(500, 50000));      // both
    BOOST_CHECK(!rtf::is_small(10001, 100001));  // neither
    BOOST_CHECK(!rtf::is_small(50000, 50000));   // vol too high
}

BOOST_AUTO_TEST_CASE(test_withdraw_buckets) {
    BOOST_CHECK(rtf::is_complete(100, 100));
    BOOST_CHECK(rtf::is_complete(150, 100));
    BOOST_CHECK(!rtf::is_complete(50, 100));
    BOOST_CHECK(rtf::is_partial(50, 100));
    BOOST_CHECK(!rtf::is_partial(100, 100));
    BOOST_CHECK(rtf::is_instant(0));
    BOOST_CHECK(rtf::is_instant(3000));
    BOOST_CHECK(!rtf::is_instant(3001));
    BOOST_CHECK(!rtf::is_instant(-1));
}
