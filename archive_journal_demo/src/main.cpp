#include "journal_reader.h"
#include "data_types.h"
#include "l2_types.h"
#include "conversion.h"
#include "parquet_writer.h"

#include <iostream>
#include <string>
#include <vector>
#include <sys/stat.h>

static void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " -i <journal_dir> [-o <output_dir>] [-s <target_mb>]" << std::endl;
    std::cerr << "  -i  journal 文件目录（必填）" << std::endl;
    std::cerr << "  -o  parquet 输出目录（默认 ./output/）" << std::endl;
    std::cerr << "  -s  单个 parquet 文件目标大小 MB（默认 128，0=不分片）" << std::endl;
}

static void ensure_dir(const std::string& path) {
    mkdir(path.c_str(), 0755);
}

int main(int argc, char* argv[]) {
    std::string input_dir;
    std::string output_dir = "./output/";
    size_t target_mb = 128;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-i" && i + 1 < argc) {
            input_dir = argv[++i];
        } else if (arg == "-o" && i + 1 < argc) {
            output_dir = argv[++i];
        } else if (arg == "-s" && i + 1 < argc) {
            target_mb = std::stoul(argv[++i]);
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

    // 根据目标文件大小计算每类数据的单文件最大行数
    size_t target_bytes = target_mb * 1024ULL * 1024ULL;
    size_t max_rows_tick  = target_mb > 0 ? target_bytes / EST_BYTES_PER_ROW_TICK  : 0;
    size_t max_rows_order = target_mb > 0 ? target_bytes / EST_BYTES_PER_ROW_ORDER : 0;
    size_t max_rows_trade = target_mb > 0 ? target_bytes / EST_BYTES_PER_ROW_TRADE : 0;

    std::cout << "Input:  " << input_dir << std::endl;
    std::cout << "Output: " << output_dir << std::endl;
    if (target_mb > 0) {
        std::cout << "Target file size: ~" << target_mb << " MB" << std::endl;
    }
    std::cout << std::endl;

    // 收集所有记录
    std::vector<TickRecord>  tick_records;
    std::vector<OrderRecord> order_records;
    std::vector<TradeRecord> trade_records;

    // 处理 tick journal
    {
        auto files = getJournalFiles(input_dir, "insight_stock_tick_data");
        std::cout << "[tick] Found " << files.size() << " journal files" << std::endl;
        for (size_t fi = 0; fi < files.size(); ++fi) {
            const auto& filepath = files[fi];
            std::cout << "[tick] (" << (fi + 1) << "/" << files.size() << ") " << filepath << std::flush;
            LocalJournalPage page;
            if (!page.load(filepath)) {
                std::cout << " -- SKIP (load failed)" << std::endl;
                continue;
            }

            size_t count_before = tick_records.size();
            LocalFrameHeader* header;
            while ((header = page.nextFrame()) != nullptr) {
                if (header->msg_type == MSG_TYPE_L2_TICK) {
                    auto* md = (KyStdSnpType*)page.getFrameData(header);
                    tick_records.push_back(convert_tick(*md, header->nano));
                }
            }
            std::cout << " -> +" << (tick_records.size() - count_before)
                      << " (total: " << tick_records.size() << ")" << std::endl;
        }
    }

    // 处理 order journal
    {
        auto files = getJournalFiles(input_dir, "insight_stock_order_data");
        std::cout << "[order] Found " << files.size() << " journal files" << std::endl;
        for (size_t fi = 0; fi < files.size(); ++fi) {
            const auto& filepath = files[fi];
            std::cout << "[order] (" << (fi + 1) << "/" << files.size() << ") " << filepath << std::flush;
            LocalJournalPage page;
            if (!page.load(filepath)) {
                std::cout << " -- SKIP (load failed)" << std::endl;
                continue;
            }

            size_t count_before = order_records.size();
            LocalFrameHeader* header;
            while ((header = page.nextFrame()) != nullptr) {
                if (header->msg_type == MSG_TYPE_L2_ORDER) {
                    auto* md = (KyStdOrderType*)page.getFrameData(header);
                    order_records.push_back(convert_order(*md, header->nano));
                }
            }
            std::cout << " -> +" << (order_records.size() - count_before)
                      << " (total: " << order_records.size() << ")" << std::endl;
        }
    }

    // 处理 trade journal
    {
        auto files = getJournalFiles(input_dir, "insight_stock_trade_data");
        std::cout << "[trade] Found " << files.size() << " journal files" << std::endl;
        for (size_t fi = 0; fi < files.size(); ++fi) {
            const auto& filepath = files[fi];
            std::cout << "[trade] (" << (fi + 1) << "/" << files.size() << ") " << filepath << std::flush;
            LocalJournalPage page;
            if (!page.load(filepath)) {
                std::cout << " -- SKIP (load failed)" << std::endl;
                continue;
            }

            size_t count_before = trade_records.size();
            LocalFrameHeader* header;
            while ((header = page.nextFrame()) != nullptr) {
                if (header->msg_type == MSG_TYPE_L2_TRADE) {
                    auto* md = (KyStdTradeType*)page.getFrameData(header);
                    trade_records.push_back(convert_trade(*md, header->nano));
                }
            }
            std::cout << " -> +" << (trade_records.size() - count_before)
                      << " (total: " << trade_records.size() << ")" << std::endl;
        }
    }

    std::cout << "\n=== Summary ===" << std::endl;
    std::cout << "Tick records:  " << tick_records.size() << std::endl;
    std::cout << "Order records: " << order_records.size() << std::endl;
    std::cout << "Trade records: " << trade_records.size() << std::endl;

    // 写入 parquet
    if (!tick_records.empty()) {
        std::cout << "\nWriting tick parquet..." << std::endl;
        write_tick_parquet(tick_records, output_dir + "tick_data.parquet", max_rows_tick);
    }
    if (!order_records.empty()) {
        std::cout << "\nWriting order parquet..." << std::endl;
        write_order_parquet(order_records, output_dir + "order_data.parquet", max_rows_order);
    }
    if (!trade_records.empty()) {
        std::cout << "\nWriting trade parquet..." << std::endl;
        write_trade_parquet(trade_records, output_dir + "trade_data.parquet", max_rows_trade);
    }

    std::cout << "\nDone." << std::endl;
    return 0;
}
