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
    std::cerr << "Usage: " << prog << " -i <journal_dir> [-o <output_dir>]" << std::endl;
    std::cerr << "  -i  journal 文件目录（必填）" << std::endl;
    std::cerr << "  -o  parquet 输出目录（默认 ./output/）" << std::endl;
}

static void ensure_dir(const std::string& path) {
    mkdir(path.c_str(), 0755);
}

int main(int argc, char* argv[]) {
    std::string input_dir;
    std::string output_dir = "./output/";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-i" && i + 1 < argc) {
            input_dir = argv[++i];
        } else if (arg == "-o" && i + 1 < argc) {
            output_dir = argv[++i];
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

    // 收集所有记录
    std::vector<TickRecord>  tick_records;
    std::vector<OrderRecord> order_records;
    std::vector<TradeRecord> trade_records;

    // 处理 tick journal
    {
        auto files = getJournalFiles(input_dir, "insight_stock_tick_data");
        std::cout << "Found " << files.size() << " tick journal files" << std::endl;
        for (const auto& filepath : files) {
            std::cout << "Processing tick: " << filepath << std::endl;
            LocalJournalPage page;
            if (!page.load(filepath)) continue;

            LocalFrameHeader* header;
            while ((header = page.nextFrame()) != nullptr) {
                if (header->msg_type == MSG_TYPE_L2_TICK) {
                    auto* md = (KyStdSnpType*)page.getFrameData(header);
                    tick_records.push_back(convert_tick(*md, header->nano));
                }
            }
        }
    }

    // 处理 order journal
    {
        auto files = getJournalFiles(input_dir, "insight_stock_order_data");
        std::cout << "Found " << files.size() << " order journal files" << std::endl;
        for (const auto& filepath : files) {
            std::cout << "Processing order: " << filepath << std::endl;
            LocalJournalPage page;
            if (!page.load(filepath)) continue;

            LocalFrameHeader* header;
            while ((header = page.nextFrame()) != nullptr) {
                if (header->msg_type == MSG_TYPE_L2_ORDER) {
                    auto* md = (KyStdOrderType*)page.getFrameData(header);
                    order_records.push_back(convert_order(*md, header->nano));
                }
            }
        }
    }

    // 处理 trade journal
    {
        auto files = getJournalFiles(input_dir, "insight_stock_trade_data");
        std::cout << "Found " << files.size() << " trade journal files" << std::endl;
        for (const auto& filepath : files) {
            std::cout << "Processing trade: " << filepath << std::endl;
            LocalJournalPage page;
            if (!page.load(filepath)) continue;

            LocalFrameHeader* header;
            while ((header = page.nextFrame()) != nullptr) {
                if (header->msg_type == MSG_TYPE_L2_TRADE) {
                    auto* md = (KyStdTradeType*)page.getFrameData(header);
                    trade_records.push_back(convert_trade(*md, header->nano));
                }
            }
        }
    }

    std::cout << "\n=== Summary ===" << std::endl;
    std::cout << "Tick records:  " << tick_records.size() << std::endl;
    std::cout << "Order records: " << order_records.size() << std::endl;
    std::cout << "Trade records: " << trade_records.size() << std::endl;

    // 写入 parquet
    if (!tick_records.empty()) {
        write_tick_parquet(tick_records, output_dir + "tick_data.parquet");
    }
    if (!order_records.empty()) {
        write_order_parquet(order_records, output_dir + "order_data.parquet");
    }
    if (!trade_records.empty()) {
        write_trade_parquet(trade_records, output_dir + "trade_data.parquet");
    }

    std::cout << "Done." << std::endl;
    return 0;
}
