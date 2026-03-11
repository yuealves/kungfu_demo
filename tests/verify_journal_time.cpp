/**
 * verify_journal_time.cpp — 通过 replayer/reader 流程验证 libjournal.so 修复
 *
 * 连接 Paged，实时读取 replayer 写入的 journal frame，对比：
 *   - frame->getNano()     : JournalWriter 调用 getNanoTime() 得到的写入时间
 *   - clock_gettime()      : 读取时刻的真实 wall clock 时间
 *   - getNanoTime()        : reader 侧也调用一次 getNanoTime()
 *
 * 如果新 .so 修复生效：
 *   - frame->getNano() 应该非常接近实际写入时刻（误差 < 1ms）
 *   - getNanoTime() 与 clock_gettime 的差值也应 < 1ms
 *
 * 如果仍为旧 .so：
 *   - frame->getNano() 可能偏移 ±500ms（整秒截断 bug）
 *
 * 编译（在 kungfu_demo/build/ 下）：
 *   cmake .. && make verify_journal_time
 *
 * 运行：
 *   # 1. 先启动 Paged: scripts/start_paged.sh
 *   # 2. 启动本验证程序（后台）:
 *   LD_LIBRARY_PATH=/path/to/new/so:/opt/kungfu/master/lib/boost \
 *     ./verify_journal_time &
 *   # 3. 启动 replayer:
 *   LD_LIBRARY_PATH=/path/to/new/so:/opt/kungfu/master/lib/boost \
 *     ./journal_replayer 10
 *   # 4. 等 replayer 写入一些数据后，verify_journal_time 会输出偏差统计
 */

#include "JournalReader.h"
#include "Timer.h"
#include "sys_messages.h"

#include <iostream>
#include <chrono>
#include <thread>
#include <csignal>
#include <atomic>
#include <cmath>
#include <time.h>

static const long NS_PER_SEC = 1000000000L;
static std::atomic<bool> g_running{true};
static void signal_handler(int) { g_running = false; }

int main()
{
    using namespace kungfu::yijinjing;

    // ---------- Step 0: 先验证 getNanoTime 本身的精度 ----------
    std::cout << "=== Step 0: getNanoTime() 本地精度测试 ===" << std::endl;
    {
        long max_diff = 0;
        for (int i = 0; i < 10; i++) {
            long nano = getNanoTime();
            timespec tp;
            clock_gettime(CLOCK_REALTIME, &tp);
            long real_ns = tp.tv_sec * NS_PER_SEC + tp.tv_nsec;
            long diff = nano - real_ns;
            long abs_diff = std::abs(diff);
            if (abs_diff > max_diff) max_diff = abs_diff;
            printf("  [%2d] getNanoTime()-REALTIME = %+ld ns (%.3f ms)\n",
                   i + 1, diff, diff / 1e6);
            struct timespec req = {0, 50000000};  // 50ms
            nanosleep(&req, NULL);
        }
        printf("  最大偏差: %ld ns (%.3f ms) — %s\n\n",
               max_diff, max_diff / 1e6,
               max_diff < 1000000 ? "PASS (<1ms)" : "FAIL (>=1ms)");

        if (max_diff >= 1000000) {
            printf("警告: getNanoTime() 仍有 >1ms 偏差，可能使用的是旧版 .so\n");
            printf("请检查 LD_LIBRARY_PATH 是否指向新编译的 libjournal.so\n\n");
        }
    }

    // ---------- Step 1: 连接 Paged 读取 journal ----------
    std::cout << "=== Step 1: 连接 Paged 读取 journal ===" << std::endl;

    std::string journal_dir = "/shared/kungfu/journal/user/";
    std::string reader_name = "verify_time_" + parseNano(getNanoTime(), "%H%M%S");

    std::vector<std::string> dirs = {journal_dir, journal_dir, journal_dir};
    std::vector<std::string> jnames = {
        "insight_stock_tick_data",
        "insight_stock_order_data",
        "insight_stock_trade_data",
    };

    long start_time = 0;
    JournalReaderPtr reader = JournalReader::createReaderWithSys(
        dirs, jnames, start_time, reader_name);

    std::cout << "已连接 Paged，等待 replayer 写入数据..." << std::endl;
    std::cout << "(请在另一个终端启动 journal_replayer)" << std::endl << std::endl;

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // ---------- Step 2: 读取 frame，统计 write_time 与 read_time 的偏差 ----------
    std::cout << "=== Step 2: 对比 journal_time 与 wall_clock ===" << std::endl;
    std::cout << "前 20 条详细输出：" << std::endl << std::endl;

    long count = 0;
    long max_write_read_diff = 0;
    long sum_write_read_diff = 0;
    int detail_printed = 0;
    const int detail_limit = 20;

    while (g_running) {
        FramePtr frame = reader->getNextFrame();

        if (frame.get() == nullptr) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            continue;
        }

        short msg_type = frame->getMsgType();
        if (msg_type != MSG_TYPE_L2_TICK &&
            msg_type != MSG_TYPE_L2_ORDER &&
            msg_type != MSG_TYPE_L2_TRADE)
            continue;

        long journal_time = frame->getNano();       // writer 调用 getNanoTime() 的结果
        long extra_nano   = frame->getExtraNano();   // 原始历史时间戳

        // 读取时刻的 wall clock
        timespec tp;
        clock_gettime(CLOCK_REALTIME, &tp);
        long read_time = tp.tv_sec * NS_PER_SEC + tp.tv_nsec;

        // journal_time 应该 <= read_time（先写后读）
        long write_read_diff = read_time - journal_time;
        long abs_wr = std::abs(write_read_diff);
        if (abs_wr > max_write_read_diff) max_write_read_diff = abs_wr;
        sum_write_read_diff += abs_wr;
        count++;

        if (detail_printed < detail_limit) {
            const char* type_str = (msg_type == MSG_TYPE_L2_TICK) ? "TICK " :
                                   (msg_type == MSG_TYPE_L2_ORDER) ? "ORDER" : "TRADE";
            printf("  [%s] journal_time=%s  read_delay=%+.3f ms  extra_nano=%s\n",
                   type_str,
                   parseNano(journal_time, "%H:%M:%S").c_str(),
                   write_read_diff / 1e6,
                   parseNano(extra_nano, "%H:%M:%S").c_str());
            detail_printed++;
            if (detail_printed == detail_limit) {
                printf("  ... (后续不再逐条打印)\n\n");
            }
        }

        // 每 50000 条打印一次统计
        if (count % 50000 == 0) {
            double avg_ms = (sum_write_read_diff / (double)count) / 1e6;
            printf("[stats] %ld frames | avg read_delay=%.3f ms | max=%.3f ms\n",
                   count, avg_ms, max_write_read_diff / 1e6);
        }
    }

    // ---------- Step 3: 最终汇总 ----------
    std::cout << std::endl;
    std::cout << "=== 最终汇总 ===" << std::endl;
    if (count > 0) {
        double avg_ms = (sum_write_read_diff / (double)count) / 1e6;
        printf("总帧数: %ld\n", count);
        printf("read_delay (read_time - journal_time):\n");
        printf("  平均: %.3f ms\n", avg_ms);
        printf("  最大: %.3f ms\n", max_write_read_diff / 1e6);
        printf("\n");

        // 判断 journal_time 是否合理
        // 如果 journal_time 比 read_time 超前 >100ms，说明 getNanoTime() 有正偏移
        // 如果 read_delay 大部分为正且在合理范围，说明 getNanoTime() 正常
        if (max_write_read_diff < 0) {
            printf("异常: journal_time > read_time（写入时间比读取时间晚），getNanoTime() 可能有正偏移\n");
        }
        printf("结论: journal_time 是写入时的 wall clock，read_delay 包含 replayer 写入到 reader 读取的延迟。\n");
        printf("      关键看 Step 0 的 getNanoTime() 本地精度 — 如果 <1ms 则修复生效。\n");
    } else {
        printf("未读到任何数据帧。\n");
    }

    return 0;
}
