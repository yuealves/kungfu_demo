/**
 * Journal Replayer - 从本地历史 journal 文件读取���据，按原始时间间隔写入 Paged 管理的 journal，
 * 模拟线上 insight_gateway 实时写入行为。
 *
 * 用法: ./journal_replayer [speed]
 *   speed: 回放倍速，默认 1.0（实时），10 表示 10 倍速
 */

#include "JournalWriter.h"
#include "Timer.h"
#include "sys_messages.h"

#include <iostream>
#include <vector>
#include <algorithm>
#include <thread>
#include <chrono>
#include <csignal>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <cstring>

// ---- 本地 journal 文件 mmap 读取结构（与 data_consumer.cpp 一致）----

#define JOURNAL_SHORT_NAME_MAX_LENGTH 30
#define JOURNAL_FRAME_STATUS_WRITTEN 1

struct LocalPageHeader {
    unsigned char status;
    char journal_name[JOURNAL_SHORT_NAME_MAX_LENGTH];
    short page_num;
    long start_nano;
    long close_nano;
    int frame_num;
    int last_pos;
    short frame_version;
    short reserve_short[3];
    long reserve_long[9];
} __attribute__((packed));

struct LocalFrameHeader {
    volatile unsigned char status;
    short source;
    long nano;
    int length;
    unsigned int hash;
    short msg_type;
    unsigned char last_flag;
    int req_id;
    long extra_nano;
    int err_id;
} __attribute__((packed));

// ---- 回放用数据结构 ----

struct ReplayFrame {
    long nano;
    short msg_type;
    short source;
    unsigned char last_flag;
    int req_id;
    void* data;
    int data_len;
};

// mmap 页面持有者，保持 mmap 映射在整个回放过程中有效
class MmapPage {
public:
    void* buffer = nullptr;
    size_t size = 0;

    MmapPage() = default;
    ~MmapPage() { if (buffer) munmap(buffer, size); }
    MmapPage(const MmapPage&) = delete;
    MmapPage& operator=(const MmapPage&) = delete;
    MmapPage(MmapPage&& o) noexcept : buffer(o.buffer), size(o.size) { o.buffer = nullptr; o.size = 0; }

    bool load(const std::string& filepath) {
        int fd = open(filepath.c_str(), O_RDONLY);
        if (fd < 0) return false;
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        buffer = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        close(fd);
        if (buffer == MAP_FAILED) { buffer = nullptr; return false; }
        return true;
    }
};

// 获取目录下匹配的 journal 文件（按文件名排序）
std::vector<std::string> findJournalFiles(const std::string& dir, const std::string& jname) {
    std::vector<std::string> files;
    DIR* d = opendir(dir.c_str());
    if (!d) return files;
    std::string pattern = "yjj." + jname + ".";
    struct dirent* entry;
    while ((entry = readdir(d)) != nullptr) {
        std::string filename = entry->d_name;
        if (filename.find(pattern) == 0 && filename.find(".journal") != std::string::npos)
            files.push_back(dir + filename);
    }
    closedir(d);
    std::sort(files.begin(), files.end());
    return files;
}

static volatile bool g_running = true;
static void signal_handler(int) { g_running = false; }

int main(int argc, const char* argv[])
{
    double speed = 1.0;
    if (argc > 1) speed = std::atof(argv[1]);
    if (speed <= 0) speed = 1.0;

    std::string src_path = std::string(PROJECT_ROOT_DIR) + "/deps/new_journal_data/";
    std::string dst_dir = "/shared/kungfu/journal/user/";
    std::string writer_name = "journal_replayer";

    const std::string channel_names[] = {
        "insight_stock_tick_data",
        "insight_stock_order_data",
        "insight_stock_trade_data",
    };

    // ---- Step 1: mmap 加载所有历史页面，收集帧 ----
    std::vector<MmapPage> pages;
    std::vector<ReplayFrame> frames;

    for (auto& ch_name : channel_names) {
        auto files = findJournalFiles(src_path, ch_name);
        std::cout << "[load] " << ch_name << ": " << files.size() << " files" << std::endl;

        for (auto& filepath : files) {
            MmapPage page;
            if (!page.load(filepath)) {
                std::cerr << "[load] failed: " << filepath << std::endl;
                continue;
            }

            size_t pos = sizeof(LocalPageHeader);
            while (pos + sizeof(LocalFrameHeader) <= page.size) {
                auto* hdr = (LocalFrameHeader*)((char*)page.buffer + pos);
                if (hdr->status != JOURNAL_FRAME_STATUS_WRITTEN) break;
                if (hdr->length <= (int)sizeof(LocalFrameHeader) || pos + hdr->length > page.size) break;

                int data_len = hdr->length - sizeof(LocalFrameHeader);
                void* data = (char*)hdr + sizeof(LocalFrameHeader);

                frames.push_back({hdr->nano, hdr->msg_type, hdr->source,
                                  hdr->last_flag, hdr->req_id, data, data_len});
                pos += hdr->length;
            }

            pages.push_back(std::move(page));
        }
    }

    std::cout << "[load] total frames: " << frames.size() << std::endl;
    if (frames.empty()) {
        std::cerr << "No frames found in " << src_path << std::endl;
        return 1;
    }

    // ---- Step 2: 按 nano 时间排序（合并 3 个频道） ----
    std::sort(frames.begin(), frames.end(),
              [](const ReplayFrame& a, const ReplayFrame& b) { return a.nano < b.nano; });

    std::cout << "[time] "
              << kungfu::yijinjing::parseNano(frames.front().nano, "%Y%m%d-%H:%M:%S")
              << " -> "
              << kungfu::yijinjing::parseNano(frames.back().nano, "%Y%m%d-%H:%M:%S")
              << std::endl;
    std::cout << "[speed] " << speed << "x" << std::endl;

    // ---- Step 3: 创建 3 个 JournalWriter（通过 Paged） ----
    using namespace kungfu::yijinjing;
    JournalWriterPtr writers[3];
    for (int i = 0; i < 3; i++) {
        const std::string writer_suffixes[] = {"tick", "order", "trade"};
        writers[i] = JournalWriter::create(dst_dir, channel_names[i], writer_name + "_" + writer_suffixes[i]);
        std::cout << "[writer] " << channel_names[i] << " ready" << std::endl;
    }

    // msg_type -> writer index: 61(TICK)->0, 62(ORDER)->1, 63(TRADE)->2
    auto writerIndex = [](short msg_type) -> int {
        if (msg_type == MSG_TYPE_L2_TICK) return 0;
        if (msg_type == MSG_TYPE_L2_ORDER) return 1;
        if (msg_type == MSG_TYPE_L2_TRADE) return 2;
        return -1;
    };

    // ---- Step 4: 按时间间隔回放 ----
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    long first_nano = frames[0].nano;
    auto wall_start = std::chrono::steady_clock::now();
    long count = 0;
    long skipped = 0;

    std::cout << "[replay] starting..." << std::endl;

    for (auto& f : frames) {
        if (!g_running) break;

        // 按倍速 sleep 到目标时刻
        long delta_nano = f.nano - first_nano;
        auto target = wall_start + std::chrono::nanoseconds(static_cast<long long>(delta_nano / speed));
        std::this_thread::sleep_until(target);

        int idx = writerIndex(f.msg_type);
        if (idx < 0) { skipped++; continue; }

        writers[idx]->write_frame(f.data, f.data_len, f.source, f.msg_type, f.last_flag, f.req_id);
        count++;

        if (count % 100000 == 0) {
            std::cout << "[replay] " << count << "/" << frames.size()
                      << " | time: " << parseNano(f.nano, "%H:%M:%S") << std::endl;
        }
    }

    std::cout << "[done] replayed " << count << " frames";
    if (skipped > 0) std::cout << " (skipped " << skipped << " unknown msg_type)";
    std::cout << std::endl;

    return 0;
}
