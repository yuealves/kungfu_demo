#include "journal_reader.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <algorithm>
#include <iostream>
#include <cstring>

LocalJournalPage::LocalJournalPage() : buffer(nullptr), size(0), current_pos(0) {}

LocalJournalPage::~LocalJournalPage() {
    if (buffer) {
        munmap(buffer, size);
    }
}

bool LocalJournalPage::load(const std::string& filepath) {
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open file: " << filepath << std::endl;
        return false;
    }

    struct stat st;
    fstat(fd, &st);
    size = st.st_size;

    buffer = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (buffer == MAP_FAILED) {
        buffer = nullptr;
        return false;
    }

    current_pos = sizeof(LocalPageHeader);
    return true;
}

LocalFrameHeader* LocalJournalPage::nextFrame() {
    if (!buffer || current_pos >= size) return nullptr;

    LocalFrameHeader* header = (LocalFrameHeader*)((char*)buffer + current_pos);

    if (header->status != JOURNAL_FRAME_STATUS_WRITTEN) {
        return nullptr;
    }

    current_pos += header->length;
    return header;
}

void* LocalJournalPage::getFrameData(LocalFrameHeader* header) {
    return (void*)((char*)header + sizeof(LocalFrameHeader));
}

// 从文件名中提取 page number: "yjj.<name>.<page_num>.journal" → page_num
static int extract_page_num(const std::string& filepath) {
    // 找最后一个 '/' 后的文件名
    auto slash = filepath.rfind('/');
    std::string fname = (slash != std::string::npos) ? filepath.substr(slash + 1) : filepath;
    // "yjj.xxx.123.journal" → 找第二个 '.' 和第三个 '.' 之间的数字
    auto first_dot = fname.find('.');
    if (first_dot == std::string::npos) return 0;
    auto second_dot = fname.find('.', first_dot + 1);
    if (second_dot == std::string::npos) return 0;
    auto third_dot = fname.find('.', second_dot + 1);
    if (third_dot == std::string::npos) return 0;
    try {
        return std::stoi(fname.substr(second_dot + 1, third_dot - second_dot - 1));
    } catch (...) {
        return 0;
    }
}

std::vector<std::string> getJournalFiles(const std::string& dir, const std::string& jname) {
    std::vector<std::string> files;
    DIR* d = opendir(dir.c_str());
    if (!d) return files;

    std::string pattern = "yjj." + jname + ".";
    struct dirent* entry;
    while ((entry = readdir(d)) != nullptr) {
        std::string filename = entry->d_name;
        if (filename.find(pattern) == 0 && filename.find(".journal") != std::string::npos) {
            std::string path = dir;
            if (!path.empty() && path.back() != '/') path += '/';
            files.push_back(path + filename);
        }
    }
    closedir(d);

    // 按 page number 数字排序（不是字典序！）
    std::sort(files.begin(), files.end(),
        [](const std::string& a, const std::string& b) {
            return extract_page_num(a) < extract_page_num(b);
        });
    return files;
}
