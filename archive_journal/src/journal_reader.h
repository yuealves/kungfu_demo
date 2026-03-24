#pragma once

#include <cstdint>
#include <string>
#include <vector>

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

class LocalJournalPage {
public:
    void* buffer;
    int size;
    int current_pos;

    LocalJournalPage();
    ~LocalJournalPage();

    bool load(const std::string& filepath);
    LocalFrameHeader* nextFrame();
    void* getFrameData(LocalFrameHeader* header);
};

// 获取目录下所有匹配 "yjj.{jname}.*.journal" 的文件，按文件名排序
std::vector<std::string> getJournalFiles(const std::string& dir, const std::string& jname);
