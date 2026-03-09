#include "journal_reader.h"
#include "data_types.h"
#include "l2_types.h"
#include "conversion.h"
#include "parquet_writer.h"

#include <arrow/util/thread_pool.h>

#include <atomic>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <sys/stat.h>

static void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " -i <journal_dir> [-o <output_dir>] [-s <target_mb>] [-t <threads>]" << std::endl;
    std::cerr << "  -i  journal 文件目录（必填）" << std::endl;
    std::cerr << "  -o  parquet 输出目录（默认 ./output/）" << std::endl;
    std::cerr << "  -s  单个 parquet 文件目标大小 MB（默认 128，0=不分片）" << std::endl;
    std::cerr << "  -t  并行线程数（默认 16）" << std::endl;
}

static void ensure_dir(const std::string& path) {
    mkdir(path.c_str(), 0755);
}

// 获取东八区当日日期字符串 "YYYYMMDD"
static std::string today_cst() {
    time_t now = time(nullptr);
    now += 8 * 3600; // UTC → UTC+8
    struct tm t;
    gmtime_r(&now, &t);
    char buf[16];
    snprintf(buf, sizeof(buf), "%04d%02d%02d", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
    return buf;
}

// ---------------------------------------------------------------------------
// 多线程并行读取 journal 文件的通用模板
// Work-stealing: 每个线程原���地取下一个文件，读到线程局部 vector，最后合并
// ---------------------------------------------------------------------------
template<typename RecordT, typename StructT, typename ConvertFn>
static std::vector<RecordT> read_journal_parallel(
    const std::vector<std::string>& files,
    int msg_type,
    ConvertFn convert_fn,
    int num_threads,
    const std::string& label)
{
    if (files.empty()) return {};

    int actual_threads = std::min((size_t)num_threads, files.size());
    std::cout << "[" << label << "] Found " << files.size()
              << " journal files, reading with " << actual_threads << " threads..." << std::endl;

    auto t0 = std::chrono::steady_clock::now();

    // 每个线程一个局部 vector，避免锁争用
    std::vector<std::vector<RecordT>> thread_results(actual_threads);
    std::atomic<size_t> next_file{0};
    std::atomic<size_t> done_count{0};
    std::mutex cout_mtx;

    auto worker = [&](int tid) {
        auto& local = thread_results[tid];
        while (true) {
            size_t fi = next_file.fetch_add(1);
            if (fi >= files.size()) break;

            const auto& filepath = files[fi];
            LocalJournalPage page;
            if (!page.load(filepath)) {
                std::lock_guard<std::mutex> lk(cout_mtx);
                std::cout << "[" << label << "] (" << (done_count + 1) << "/" << files.size()
                          << ") " << filepath << " -- SKIP (load failed)" << std::endl;
                done_count.fetch_add(1);
                continue;
            }

            size_t count_before = local.size();
            LocalFrameHeader* header;
            while ((header = page.nextFrame()) != nullptr) {
                if (header->msg_type == msg_type) {
                    auto* md = (StructT*)page.getFrameData(header);
                    local.push_back(convert_fn(*md, header->nano));
                }
            }

            size_t added = local.size() - count_before;
            size_t done = done_count.fetch_add(1) + 1;
            {
                std::lock_guard<std::mutex> lk(cout_mtx);
                std::cout << "[" << label << "] (" << done << "/" << files.size()
                          << ") " << filepath << " -> +" << added << std::endl;
            }
        }
    };

    if (actual_threads <= 1) {
        worker(0);
    } else {
        std::vector<std::thread> threads;
        for (int t = 0; t < actual_threads; ++t)
            threads.emplace_back(worker, t);
        for (auto& t : threads) t.join();
    }

    // 合并各线程结果
    size_t total = 0;
    for (auto& v : thread_results) total += v.size();

    std::vector<RecordT> records;
    records.reserve(total);
    for (auto& v : thread_results) {
        records.insert(records.end(),
                       std::make_move_iterator(v.begin()),
                       std::make_move_iterator(v.end()));
        v.clear();
        v.shrink_to_fit();
    }

    auto t1 = std::chrono::steady_clock::now();
    double secs = std::chrono::duration<double>(t1 - t0).count();
    std::cout << "[" << label << "] Done: " << records.size()
              << " records from " << files.size() << " files in "
              << std::fixed << std::setprecision(1) << secs << "s" << std::endl;

    return records;
}

int main(int argc, char* argv[]) {
    std::string input_dir;
    std::string output_dir = "./output/";
    size_t target_mb = 128;
    int num_threads = 16;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-i" && i + 1 < argc) {
            input_dir = argv[++i];
        } else if (arg == "-o" && i + 1 < argc) {
            output_dir = argv[++i];
        } else if (arg == "-s" && i + 1 < argc) {
            target_mb = std::stoul(argv[++i]);
        } else if (arg == "-t" && i + 1 < argc) {
            num_threads = std::stoi(argv[++i]);
            if (num_threads < 1) num_threads = 1;
        } else if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        }
    }

    if (input_dir.empty()) {
        print_usage(argv[0]);
        return 1;
    }

    if (!input_dir.empty() && input_dir.back() != '/') input_dir += '/';
    if (!output_dir.empty() && output_dir.back() != '/') output_dir += '/';
    ensure_dir(output_dir);

    // 设置 Arrow 内部线程池大小（parquet 写入时并行编码/压缩各列）
    (void)arrow::SetCpuThreadPoolCapacity(num_threads);

    // 根据目标文件大小计算每类数据的单文件最大行数
    size_t target_bytes = target_mb * 1024ULL * 1024ULL;
    size_t max_rows_tick  = target_mb > 0 ? target_bytes / EST_BYTES_PER_ROW_TICK  : 0;
    size_t max_rows_order = target_mb > 0 ? target_bytes / EST_BYTES_PER_ROW_ORDER : 0;
    size_t max_rows_trade = target_mb > 0 ? target_bytes / EST_BYTES_PER_ROW_TRADE : 0;

    std::cout << "Input:  " << input_dir << std::endl;
    std::cout << "Output: " << output_dir << std::endl;
    std::cout << "Threads: " << num_threads << std::endl;
    if (target_mb > 0) {
        std::cout << "Target file size: ~" << target_mb << " MB" << std::endl;
    }
    std::cout << std::endl;

    std::string date = today_cst();
    std::cout << "Date prefix: " << date << "\n" << std::endl;

    auto total_t0 = std::chrono::steady_clock::now();

    // ---- Tick: 并行读 → 并行写 → 释放 ----
    {
        auto files = getJournalFiles(input_dir, "insight_stock_tick_data");
        auto records = read_journal_parallel<TickRecord, KyStdSnpType>(
            files, MSG_TYPE_L2_TICK, convert_tick, num_threads, "tick");
        if (!records.empty()) {
            auto t0 = std::chrono::steady_clock::now();
            write_tick_parquet(records, output_dir + date + "_tick_data.parquet",
                               max_rows_tick, num_threads);
            auto t1 = std::chrono::steady_clock::now();
            double secs = std::chrono::duration<double>(t1 - t0).count();
            std::cout << "[tick] Write done in " << std::fixed << std::setprecision(1)
                      << secs << "s" << std::endl;
        }
    }
    std::cout << std::endl;

    // ---- Order: 并行读 → 并行写 → 释放 ----
    {
        auto files = getJournalFiles(input_dir, "insight_stock_order_data");
        auto records = read_journal_parallel<OrderRecord, KyStdOrderType>(
            files, MSG_TYPE_L2_ORDER, convert_order, num_threads, "order");
        if (!records.empty()) {
            auto t0 = std::chrono::steady_clock::now();
            write_order_parquet(records, output_dir + date + "_order_data.parquet",
                                max_rows_order, num_threads);
            auto t1 = std::chrono::steady_clock::now();
            double secs = std::chrono::duration<double>(t1 - t0).count();
            std::cout << "[order] Write done in " << std::fixed << std::setprecision(1)
                      << secs << "s" << std::endl;
        }
    }
    std::cout << std::endl;

    // ---- Trade: 并行读 → 并行写 → 释放 ----
    {
        auto files = getJournalFiles(input_dir, "insight_stock_trade_data");
        auto records = read_journal_parallel<TradeRecord, KyStdTradeType>(
            files, MSG_TYPE_L2_TRADE, convert_trade, num_threads, "trade");
        if (!records.empty()) {
            auto t0 = std::chrono::steady_clock::now();
            write_trade_parquet(records, output_dir + date + "_trade_data.parquet",
                                max_rows_trade, num_threads);
            auto t1 = std::chrono::steady_clock::now();
            double secs = std::chrono::duration<double>(t1 - t0).count();
            std::cout << "[trade] Write done in " << std::fixed << std::setprecision(1)
                      << secs << "s" << std::endl;
        }
    }

    auto total_t1 = std::chrono::steady_clock::now();
    double total_secs = std::chrono::duration<double>(total_t1 - total_t0).count();
    std::cout << "\nDone. Total: " << std::fixed << std::setprecision(1)
              << total_secs << "s" << std::endl;
    return 0;
}
