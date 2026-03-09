#include "journal_reader.h"
#include "data_types.h"
#include "l2_types.h"
#include "conversion.h"
#include "parquet_writer.h"

#include <ctime>
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

    std::string date = today_cst();
    std::cout << "Date prefix: " << date << "\n" << std::endl;

    // ---- Tick: 读完即写，写完释放 ----
    {
        std::vector<TickRecord> records;
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
            size_t count_before = records.size();
            LocalFrameHeader* header;
            while ((header = page.nextFrame()) != nullptr) {
                if (header->msg_type == MSG_TYPE_L2_TICK) {
                    auto* md = (KyStdSnpType*)page.getFrameData(header);
                    records.push_back(convert_tick(*md, header->nano));
                }
            }
            std::cout << " -> +" << (records.size() - count_before)
                      << " (total: " << records.size() << ")" << std::endl;
        }
        std::cout << "[tick] Total: " << records.size() << " records" << std::endl;
        if (!records.empty()) {
            write_tick_parquet(records, output_dir + date + "_tick_data.parquet", max_rows_tick);
        }
    } // tick_records 在这里释放

    // ---- Order: 读完即写，写完释放 ----
    {
        std::vector<OrderRecord> records;
        auto files = getJournalFiles(input_dir, "insight_stock_order_data");
        std::cout << "\n[order] Found " << files.size() << " journal files" << std::endl;
        for (size_t fi = 0; fi < files.size(); ++fi) {
            const auto& filepath = files[fi];
            std::cout << "[order] (" << (fi + 1) << "/" << files.size() << ") " << filepath << std::flush;
            LocalJournalPage page;
            if (!page.load(filepath)) {
                std::cout << " -- SKIP (load failed)" << std::endl;
                continue;
            }
            size_t count_before = records.size();
            LocalFrameHeader* header;
            while ((header = page.nextFrame()) != nullptr) {
                if (header->msg_type == MSG_TYPE_L2_ORDER) {
                    auto* md = (KyStdOrderType*)page.getFrameData(header);
                    records.push_back(convert_order(*md, header->nano));
                }
            }
            std::cout << " -> +" << (records.size() - count_before)
                      << " (total: " << records.size() << ")" << std::endl;
        }
        std::cout << "[order] Total: " << records.size() << " records" << std::endl;
        if (!records.empty()) {
            write_order_parquet(records, output_dir + date + "_order_data.parquet", max_rows_order);
        }
    } // order_records 在这里释放

    // ---- Trade: 读完即写，写完释放 ----
    {
        std::vector<TradeRecord> records;
        auto files = getJournalFiles(input_dir, "insight_stock_trade_data");
        std::cout << "\n[trade] Found " << files.size() << " journal files" << std::endl;
        for (size_t fi = 0; fi < files.size(); ++fi) {
            const auto& filepath = files[fi];
            std::cout << "[trade] (" << (fi + 1) << "/" << files.size() << ") " << filepath << std::flush;
            LocalJournalPage page;
            if (!page.load(filepath)) {
                std::cout << " -- SKIP (load failed)" << std::endl;
                continue;
            }
            size_t count_before = records.size();
            LocalFrameHeader* header;
            while ((header = page.nextFrame()) != nullptr) {
                if (header->msg_type == MSG_TYPE_L2_TRADE) {
                    auto* md = (KyStdTradeType*)page.getFrameData(header);
                    records.push_back(convert_trade(*md, header->nano));
                }
            }
            std::cout << " -> +" << (records.size() - count_before)
                      << " (total: " << records.size() << ")" << std::endl;
        }
        std::cout << "[trade] Total: " << records.size() << " records" << std::endl;
        if (!records.empty()) {
            write_trade_parquet(records, output_dir + date + "_trade_data.parquet", max_rows_trade);
        }
    } // trade_records 在这里释放

    std::cout << "\nDone." << std::endl;
    return 0;
}
