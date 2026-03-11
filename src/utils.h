#pragma once

#define DELETE_P(buf) if (buf) { delete buf; buf = NULL;}

#include <string>
#include <cassert>
#include <iostream>
#include <memory>
#include <algorithm>
#include <sstream>
#include <fstream>
#include <chrono>
#include <ctime>
#include <stdlib.h>
#include <unordered_map>

#if defined(WIN32) || defined(_WIN32)
#include <windows.h> 
#include <direct.h>
#include <sys/types.h>
#include <sys/stat.h>
#else
#include <sys/stat.h> 
#include <unistd.h> 
#endif

#include "base_define.h"
#include "mdc_client_factory.h"
#include "client_interface.h"
#include "message_handle.h"

#include <chrono>

long long getEpochTime();
int32_t parse_nano(long nano);
std::string decode_exchange(int symbol);

std::vector<std::string> read_csv_column(const std::string &filename, const std::string &column_name);

using namespace com::htsc::mdc::gateway;
using namespace com::htsc::mdc::model;
using namespace com::htsc::mdc::insight::model;

ESecurityType get_security_type_from_name(const std::string& name);
EMarketDataType get_data_type_from_name(const std::string& name);
EPlaybackExrightsType get_playback_exrights_type_from_name(const std::string& name);
std::string GetSplitPart(const std::string& filename);
std::vector<std::string> SplitTaskId(const std::string& task_id);

void save_debug_string(std::string base_folder, std::string data_type,
	std::string security_type, std::string SecurityId, const std::string& debug_string);

std::string get_security_type_name(const ESecurityType& securityType);

std::string get_data_type_name(const EMarketDataType& type);

bool create_folder(const std::string& folder_path);

bool folder_exist(const std::string& dir);

void msleep(int ms);

int realTime(int fake);

int realMinTime(int fake);

