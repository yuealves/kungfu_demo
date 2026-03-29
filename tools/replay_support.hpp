#pragma once

#include "data_struct.hpp"

#include <memory>
#include <string>
#include <vector>

struct ReplayOptions {
    std::string source = "journal";
    std::string date_dir;
    double speed = 1.0;
};

struct ReplayFrame {
    long nano = 0;
    short msg_type = 0;
    short source = 0;
    unsigned char last_flag = 0;
    int req_id = 0;
    std::shared_ptr<void> owned_data;
    void* data = nullptr;
    int data_len = 0;
};

class ParquetReplayStream {
public:
    ParquetReplayStream(const std::string& date_dir, long resume_nano);
    ~ParquetReplayStream();
    bool hasNext() const;
    long firstNano() const;
    ReplayFrame popNext();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

ReplayOptions parseReplayOptions(int argc, const char* argv[]);
ReplayOptions parseReplayOptionsForTest(const std::vector<std::string>& args);

std::vector<ReplayFrame> loadParquetFrames(const std::string& date_dir, long resume_nano);
std::vector<ReplayFrame> loadParquetFramesForTest(const std::string& date_dir, long resume_nano);
std::string createTestParquetDirForTest();
std::string createMultiFileParquetDirForTest();
std::string createSplitTickParquetDirForTest();
std::string createPoisonParquetDirForTest();

std::vector<long> build_test_parquet_replay_nanos();
