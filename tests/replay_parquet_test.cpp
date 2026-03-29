#include "replay_support.hpp"

#include <cassert>
#include <cmath>
#include <vector>

namespace {

bool almostEqual(float lhs, float rhs) {
    return std::fabs(lhs - rhs) < 1e-6f;
}

}  // namespace

int main() {
    ReplayOptions opts = parseReplayOptionsForTest({
        "journal_replayer", "--source", "parquet", "--date-dir", "/tmp/2026-03-26", "10"
    });
    assert(opts.source == "parquet");
    assert(opts.date_dir == "/tmp/2026-03-26");
    assert(almostEqual(static_cast<float>(opts.speed), 10.0f));

    std::vector<long> nanos = build_test_parquet_replay_nanos();
    assert(nanos.size() == 3);
    assert(nanos[0] == 100);
    assert(nanos[1] == 150);
    assert(nanos[2] == 200);

    const std::string dir_path = createTestParquetDirForTest();
    auto frames = loadParquetFramesForTest(dir_path, 0);
    assert(frames.size() == 3);

    auto* order = reinterpret_cast<KyStdOrderType*>(frames[0].data);
    assert(order->Symbol == 1);
    assert(order->BizIndex == 11);
    assert(order->Channel == 22);
    assert(almostEqual(order->Price, 33.5f));
    assert(order->TradedQty == 0);
    assert(order->Date == 0);
    assert(order->Multiple == 0);

    auto* trade = reinterpret_cast<KyStdTradeType*>(frames[1].data);
    assert(trade->Symbol == 2);
    assert(trade->BidOrder == 44);
    assert(trade->Date == 0);
    assert(trade->Multiple == 0);
    assert(trade->Money == 0);

    auto* tick = reinterpret_cast<KyStdSnpType*>(frames[2].data);
    assert(tick->Symbol == 3);
    assert(almostEqual(tick->Price, 55.5f));
    assert(almostEqual(tick->AskPx1, 56.0f));
    assert(tick->BizIndex == 18);

    auto resumed = loadParquetFramesForTest(dir_path, 150);
    assert(resumed.size() == 1);
    assert(resumed[0].nano == 200);

    return 0;
}
