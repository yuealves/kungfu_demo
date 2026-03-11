#include "utils.h"
#include <unordered_map>
#include <stdexcept>

const long NANOSECONDS_PER_SECOND = 1000000000;
long long getEpochTime() {
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

std::string GetSplitPart(const std::string& filename) {
    // 查找第一个下划线的位置
    size_t first_underscore = filename.find('_');
    if (first_underscore == std::string::npos) {
        return ""; // 如果找不到下划线，返回空字符串
    }

    // 查找第二个下划线的位置
    size_t second_underscore = filename.find('_', first_underscore + 1);
    if (second_underscore == std::string::npos) {
        return ""; // 如果找不到第二个下划线，返回空字符串
    }

    // 提取子字符串
    return filename.substr(0, second_underscore);
}

std::vector<std::string> SplitTaskId(const std::string& task_id) {
    std::vector<std::string> parts;
    std::stringstream ss(task_id);
    std::string item;

    while (std::getline(ss, item, '_')) {
        parts.push_back(item);
    }

    return parts;
}

std::string decode_exchange(int symbol)
{
	std::string exchange = "";
	if(symbol < 400000)
		exchange = "SZ";
	else if(symbol < 700000)
		exchange = "SH";

	return exchange;
}


int32_t parse_nano(long nano) {
    if (nano <= 0) {
        return 0;
    }
    // 先将纳秒转换为秒
    long seconds = nano / NANOSECONDS_PER_SECOND;
    // 计算毫秒部分
    int millisecond = (nano % NANOSECONDS_PER_SECOND) / 1000000;
    struct tm* dt;
    dt = localtime(&seconds);
    // 提取时分秒
    int hour = dt->tm_hour;
    int minute = dt->tm_min;
    int second = dt->tm_sec;
	int res = millisecond + second * 1000 + minute * 100000 + hour * 10000000;
	return res;
}


// 读取CSV文件并将指定列名的一列保存为std::vector
std::vector<std::string> read_csv_column(const std::string &filename, const std::string &column_name) {
    std::vector<std::string> column_data;
    std::ifstream file(filename);

    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return column_data;
    }

    std::string line;
    std::unordered_map<std::string, int> column_map;
    bool header_parsed = false;

    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string cell;

        if (!header_parsed) {
            // 解析头一行，获取列名和列索引的映射
            int column_index = 0;
            while (std::getline(ss, cell, ',')) {
                column_map[cell] = column_index;
                ++column_index;
            }
            header_parsed = true;

            // 检查列名是否存在
            if (column_map.find(column_name) == column_map.end()) {
                std::cerr << "Column name not found: " << column_name << std::endl;
                return column_data;
            }
        } else {
            // 读取相应列的数据
            int current_index = 0;
            int target_index = column_map[column_name];
            while (std::getline(ss, cell, ',')) {
                if (current_index == target_index) {
                    column_data.push_back(cell);
                    break;
                }
                ++current_index;
            }
        }
    }

    file.close();
    return column_data;
}

using namespace com::htsc::mdc::gateway;
using namespace com::htsc::mdc::model;
using namespace com::htsc::mdc::insight::model;
void save_debug_string(std::string base_folder, std::string data_type,
	std::string security_type, std::string SecurityId, const std::string& debug_string) {
	char file_name[128] = { 0 };
	snprintf(file_name, 128, "%s/%s_%s_%s.csv",
		base_folder.c_str(), data_type.c_str(), security_type.c_str(), SecurityId.c_str());
	std::ofstream ofs(file_name, std::fstream::out | std::fstream::app);
	if (ofs.good()) {
		ofs << debug_string << std::endl;
	}
	ofs.close();
}

std::string get_security_type_name(const ESecurityType& securityType) {
	std::string security_type;
	switch (securityType) {
	case StockType:
	{
		security_type = "StockType";
		break;
	}
	case IndexType:
	{
		security_type = "IndexType";
		break;
	}
	case BondType:
	{
		security_type = "BondType";
		break;
	}
	case FundType:
	{
		security_type = "FundType";
		break;
	}
	case OptionType:
	{
		security_type = "OptionType";
		break;
	}
	case FuturesType:
	{
		security_type = "FuturesType";
		break;
	}
	case SpotType:
	{
		security_type = "SpotType";
		break;
	}
	case ForexType:
	{
		security_type = "ForexType";
		break;
	}
	case WarrantType:
	{
		security_type = "WarrantType";
		break;
	}
	default:
	{
		security_type = "UnknownType";
		break;
	}
	}
	return security_type;
}

ESecurityType get_security_type_from_name(const std::string& name) {
    static const std::unordered_map<std::string, ESecurityType> name_to_type = {
        {"StockType", StockType},
        {"IndexType", IndexType},
        {"BondType", BondType},
        {"FundType", FundType},
        {"OptionType", OptionType},
        {"FuturesType", FuturesType},
        {"SpotType", SpotType},
        {"ForexType", ForexType},
        {"WarrantType", WarrantType}
    };

    auto it = name_to_type.find(name);
    if (it != name_to_type.end()) {
        return it->second;
    } else {
        throw std::invalid_argument("Invalid security type name: " + name);
    }
}

EMarketDataType get_data_type_from_name(const std::string& name) {
    static const std::unordered_map<std::string, EMarketDataType> name_to_type = {
        {"MD_TICK", MD_TICK},
        {"MD_TRANSACTION", MD_TRANSACTION},
        {"MD_ORDER", MD_ORDER},
        {"MD_CONSTANT", MD_CONSTANT},
        {"MD_KLINE_1MIN", MD_KLINE_1MIN},
        {"MD_KLINE_5MIN", MD_KLINE_5MIN},
        {"MD_KLINE_15MIN", MD_KLINE_15MIN},
        {"MD_KLINE_30MIN", MD_KLINE_30MIN},
        {"MD_KLINE_60MIN", MD_KLINE_60MIN},
        {"MD_KLINE_1D", MD_KLINE_1D},
        {"MD_TWAP_1MIN", MD_TWAP_1MIN},
        {"MD_VWAP_1MIN", MD_VWAP_1MIN},
        {"MD_VWAP_1S", MD_VWAP_1S},
        {"MD_SIMPLE_TICK", MD_SIMPLE_TICK},
        {"AD_UPSDOWNS_ANALYSIS", AD_UPSDOWNS_ANALYSIS},
        {"AD_INDICATORS_RANKING", AD_INDICATORS_RANKING},
        {"DYNAMIC_PACKET", DYNAMIC_PACKET},
        {"AD_FUND_FLOW_ANALYSIS", AD_FUND_FLOW_ANALYSIS},
        {"AD_VOLUME_BYPRICE", AD_VOLUME_BYPRICE},
        {"MD_ETF_BASICINFO", MD_ETF_BASICINFO},
        {"AD_ORDERBOOK_SNAPSHOT", AD_ORDERBOOK_SNAPSHOT}
    };

    auto it = name_to_type.find(name);
    if (it != name_to_type.end()) {
        return it->second;
    } else {
        throw std::invalid_argument("Invalid data type name: " + name);
    }
}

std::string get_data_type_name(const EMarketDataType& type) {
	std::string data_type;
	switch (type) {
	case MD_TICK:
	{
		data_type = "MD_TICK";
		break;
	}
	case MD_TRANSACTION:
	{
		data_type = "MD_TRANSACTION";
		break;
	}
	case MD_ORDER:
	{
		data_type = "MD_ORDER";
		break;
	}
	case MD_CONSTANT:
	{
		data_type = "MD_CONSTANT";
		break;
	}
	case MD_KLINE_1MIN:
	{
		data_type = "MD_KLINE_1MIN";
		break;
	}
	case MD_KLINE_5MIN:
	{
		data_type = "MD_KLINE_5MIN";
		break;
	}
	case MD_KLINE_15MIN:
	{
		data_type = "MD_KLINE_15MIN";
		break;
	}
	case MD_KLINE_30MIN:
	{
		data_type = "MD_KLINE_30MIN";
		break;
	}
	case MD_KLINE_60MIN:
	{
		data_type = "MD_KLINE_60MIN";
		break;
	}
	case MD_KLINE_1D:
	{
		data_type = "MD_KLINE_1D";
		break;
	}
	case MD_TWAP_1MIN:
	{
		data_type = "MD_TWAP_1MIN";
		break;
	}
	case MD_VWAP_1MIN:
	{
		data_type = "MD_VWAP_1MIN";
		break;
	}
	case MD_VWAP_1S:
	{
		data_type = "MD_VWAP_1S";
		break;
	}
	case MD_SIMPLE_TICK:
	{
		data_type = "MD_SIMPLE_TICK";
		break;
	}
	case AD_UPSDOWNS_ANALYSIS:
	{
		data_type = "AD_UPSDOWNS_ANALYSIS";
		break;
	}
	case AD_INDICATORS_RANKING:
	{
		data_type = "AD_INDICATORS_RANKING";
		break;
	}
	case DYNAMIC_PACKET:
	{
		data_type = "DYNAMIC_PACKET";
		break;
	}
	case AD_FUND_FLOW_ANALYSIS:
	{
		data_type = "AD_FUND_FLOW_ANALYSIS";
		break;
	}
	case AD_VOLUME_BYPRICE:
	{
		data_type = "AD_VOLUME_BYPRICE";
		break;
	}
	case MD_ETF_BASICINFO:
	{
		data_type = "MD_ETF_BASICINFO";
		break;
	}
	case AD_ORDERBOOK_SNAPSHOT:
	{
		data_type = "AD_ORDERBOOK_SNAPSHOT";
		break;
	}
	default:
	{
		data_type = "UnknownDataType";
	}
	}
	return data_type;
}

EPlaybackExrightsType get_playback_exrights_type_from_name(const std::string& name) {
    static const std::unordered_map<std::string, EPlaybackExrightsType> name_to_type = {
        {"DEFAULT_EXRIGHTS_TYPE", DEFAULT_EXRIGHTS_TYPE},
        {"NO_EXRIGHTS", NO_EXRIGHTS},
        {"FORWARD_EXRIGHTS", FORWARD_EXRIGHTS},
        {"BACKWARD_EXRIGHTS", BACKWARD_EXRIGHTS},
        {"UnknownExrightsType", DEFAULT_EXRIGHTS_TYPE}
    };

    auto it = name_to_type.find(name);
    if (it != name_to_type.end()) {
        return it->second;
    } else {
        throw std::invalid_argument("Invalid exrights type name: " + name);
    }
}

bool create_folder(const std::string& folder_path) {
	int ret = 0;
#if defined(_WIN32) || defined(WIN32)
	ret = _mkdir(folder_path.c_str());
#else
	ret = mkdir(folder_path.c_str(), 0777);
#endif
	return ret == 0;
}

bool folder_exist(const std::string& dir) {
#if defined(_WIN32) || defined(WIN32)
#if defined(_MSC_VER) && _MSC_VER >= 1900
	struct _stat fileStat;
	if ((_stat(dir.c_str(), &fileStat) == 0) && (fileStat.st_mode & _S_IFDIR)) {
		return true;
	} else {
		return false;
	}
#else
	struct _stat fileStat;
	if ((_stat(dir.c_str(), &fileStat) == 0) && (fileStat.st_mode & _S_IFDIR)) {
		return true;
	} else {
		return false;
	}
#endif
#else
	struct stat fileStat;
	if ((stat(dir.c_str(), &fileStat) == 0) && S_ISDIR(fileStat.st_mode)) {
		return true;
	} else {
		return false;
	}
#endif
}

void msleep(int ms) {
#if defined(WIN32) || defined(_WIN32)
	Sleep(ms);
#else
	sleep(ms / 1000);
#endif
}

int realTime(int fake){
    int real = 0;
    real += 3600000 * (fake / 10000000);
    fake %= 10000000;
    real += 60000 * (fake / 100000);
    fake %= 100000;
    real += fake;
    return real;
}

int realMinTime(int fake){
    int real = 0;
    real += 3600000 * (fake / 10000000);
    fake %= 10000000;
    real += 60000 * (fake / 100000);
    return real;
}