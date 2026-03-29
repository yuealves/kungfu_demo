#include "replay_support.hpp"

#include <cassert>
#include <cmath>
#include <iostream>
#include <sstream>
#include <string>
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

    ParquetReplayStream stream(dir_path, 0);
    assert(stream.hasNext());
    assert(stream.firstNano() == 100);

    ReplayFrame first = stream.popNext();
    ReplayFrame second = stream.popNext();
    ReplayFrame third = stream.popNext();

    assert(first.nano == 100);
    assert(second.nano == 150);
    assert(third.nano == 200);
    assert(!stream.hasNext());

    ParquetReplayStream stream_resume(dir_path, 150);
    assert(stream_resume.hasNext());
    assert(stream_resume.firstNano() == 200);
    ReplayFrame only = stream_resume.popNext();
    assert(only.nano == 200);
    assert(!stream_resume.hasNext());

    auto* resumed_tick = reinterpret_cast<KyStdSnpType*>(only.data);
    assert(resumed_tick->Symbol == 3);
    assert(almostEqual(resumed_tick->Price, 55.5f));
    assert(almostEqual(resumed_tick->AskPx1, 56.0f));
    assert(resumed_tick->BizIndex == 18);

    const std::string multi_dir_path = createMultiFileParquetDirForTest();
    auto expected_frames = loadParquetFramesForTest(multi_dir_path, 0);
    ParquetReplayStream multi_stream(multi_dir_path, 0);
    std::vector<ReplayFrame> actual_frames;
    while (multi_stream.hasNext()) {
        actual_frames.push_back(multi_stream.popNext());
    }
    assert(actual_frames.size() == expected_frames.size());
    for (std::size_t i = 0; i < expected_frames.size(); ++i) {
        assert(actual_frames[i].nano == expected_frames[i].nano);
        assert(actual_frames[i].msg_type == expected_frames[i].msg_type);
        assert(actual_frames[i].data_len == expected_frames[i].data_len);
    }

    const std::string split_tick_dir = createSplitTickParquetDirForTest();
    std::ostringstream parquet_log_capture;
    std::streambuf* original_cout = std::cout.rdbuf(parquet_log_capture.rdbuf());
    ParquetReplayStream split_tick_stream(split_tick_dir, 0);
    std::vector<long> split_tick_nanos;
    while (split_tick_stream.hasNext()) {
        split_tick_nanos.push_back(split_tick_stream.popNext().nano);
    }
    std::cout.rdbuf(original_cout);
    assert((split_tick_nanos == std::vector<long>{100, 120, 150, 180, 200}));
    const std::string parquet_logs = parquet_log_capture.str();
    assert(parquet_logs.find("[parquet] tick files: 5") != std::string::npos);
    assert(parquet_logs.find("[parquet] open tick file 1/5: ") != std::string::npos);
    assert(parquet_logs.find("[parquet] finish tick file 1/5") != std::string::npos);

    ParquetReplayStream split_tick_resume_stream(split_tick_dir, 150);
    std::vector<ReplayFrame> split_tick_resume_frames;
    while (split_tick_resume_stream.hasNext()) {
        split_tick_resume_frames.push_back(split_tick_resume_stream.popNext());
    }
    assert(split_tick_resume_frames.size() == 2);
    assert(split_tick_resume_frames[0].nano == 180);
    assert(split_tick_resume_frames[1].nano == 200);

    auto* resumed_split_first = reinterpret_cast<KyStdSnpType*>(split_tick_resume_frames[0].data);
    assert(resumed_split_first->Symbol == 304);
    assert(almostEqual(resumed_split_first->Price, 40.0f));
    assert(almostEqual(resumed_split_first->AskPx1, 40.1f));
    assert(resumed_split_first->BizIndex == 404);

    const std::string poison_dir_path = createPoisonParquetDirForTest();
    ParquetReplayStream poison_stream(poison_dir_path, 0);
    assert(poison_stream.hasNext());

    ReplayFrame poison_first = poison_stream.popNext();
    assert(poison_first.nano == 100);
    assert(poison_first.msg_type == MSG_TYPE_L2_ORDER);

    bool saw_poison_has_next_failure = false;
    try {
        (void)poison_stream.hasNext();
    } catch (const std::exception& ex) {
        const std::string message = ex.what();
        assert(message.find("20260326_order_data_002.parquet") != std::string::npos);
        saw_poison_has_next_failure = true;
    }
    assert(saw_poison_has_next_failure);

    bool saw_poison_failure = false;
    try {
        ReplayFrame poison_second = poison_stream.popNext();
        (void)poison_second;
    } catch (const std::exception& ex) {
        const std::string message = ex.what();
        assert(message.find("20260326_order_data_002.parquet") != std::string::npos);
        saw_poison_failure = true;
    }
    assert(saw_poison_failure);

    return 0;
}
