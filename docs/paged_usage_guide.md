# KungFu Paged (PageEngine) 使用指南

本文档详细记录 KungFu 框架中 Paged 进程的作用、架构、启动方式和使用方法，基于本地 Docker 环境的实际验证。

## 0. 版本信息

### 当前环境版本

| 项目 | 值 |
|------|---|
| **Git Tag** | `v1.0.0`（即 `1.0.0`） |
| **Git Commit** | `ebbc9e21f3b2efad743c402f453d7487d8d3f961` |
| **Commit 日期** | 2018-06-15 |
| **Commit 作者** | cjiang (Changhao Jiang) |
| **RPM 包名** | `kungfu-0.0.5-20240912141305.x86_64` |
| **RPM Build Date** | 2024-09-12（打包日期，非代码日期） |
| **RPM Build Host** | `roardog02` |
| **RPM Vendor** | `taurus.ai` |
| **操作系统** | CentOS Stream 8 (Docker) |
| **Python** | 2.7（系统自带） |
| **Boost** | 1.62.0 |
| **CMake** | 3.8.0（`/opt/kungfu/toolchain/cmake-3.8.0/`） |

RPM 包的 `Version: 0.0.5` 是 RPM spec 里的版本号，没跟着 git tag 走。实际代码版本以 git commit 为准。

### 版本确认方法

```bash
# 查看 RPM 包信息（包含 Git Version）
rpm -qi kungfu

# 关键输出：
# Version     : 0.0.5
# Release     : 20240912141305
# Vendor      : taurus.ai
# Description :
# Git Version : ebbc9e21f3b2efad743c402f453d7487d8d3f961
```

### GitHub 仓库

| 资源 | 链接 |
|------|------|
| 仓库（当前名） | https://github.com/kungfu-origin/kungfu |
| v1.0.0 Tag | https://github.com/kungfu-origin/kungfu/releases/tag/1.0.0 |
| 对应 Commit | https://github.com/kungfu-origin/kungfu/commit/ebbc9e21f3b2efad743c402f453d7487d8d3f961 |
| 知乎「易筋经初探」 | https://zhuanlan.zhihu.com/p/31099083 |

### 版本演进

```
v0.0.2 → v0.0.3 → v0.0.4 → v0.0.5 → v1.0.0 (← 当前环境)
  → v2.0.0 → v2.1.x → v2.4.x → v2.4.77（最新，架构大改）
```

v1.0.0 是纯 C++ / Python 2.7 / Boost.Python 的原始版本。从 v2.0.0 开始引入了 Electron/Vue.js 前端、pybind11 替代 Boost.Python 等重大变更，与 v1.0.0 架构差异很大。**v2.x 的文档对本环境基本不适用。**

### 技术栈对比

| 组件 | v1.0.0（当前） | v2.4.x（最新） |
|------|---------------|---------------|
| Python | 2.7 | 3.x |
| Python 绑定 | Boost.Python | pybind11 |
| 前端 | 无 | Electron + Vue.js |
| 进程管理 | supervisord | 内置 |
| 构建系统 | CMake | CMake + Node.js |

## 1. Paged 是什么

Paged（全称 PageEngine）是 KungFu yijinjing journal 系统的**内存页面管理守护进程**。它是整个 KungFu 多进程架构的核心协调中枢。

### 1.1 Paged 的三大职责

**（一）集中管理 mmap 内存映射**

客户端（reader/writer）不直接 mmap journal 文件，而是向 Paged 申请。协议流程如下（来自 `PageCommStruct.h`）：

```
client:  通过 paged.sock 发送注册请求      → PAGED_COMM_RAW
server:  分配通信槽位                       → PAGED_COMM_OCCUPIED
client:  填入 folder / jname               → PAGED_COMM_HOLDING
client:  指定 page_num                     → PAGED_COMM_REQUESTING
server:  mmap 对应文件，完成分配             → PAGED_COMM_ALLOCATED（成功）
                                           → PAGED_COMM_NON_EXIST（文件不存在）
                                           → PAGED_COMM_MEM_OVERFLOW（内存溢出）
```

**（二）协调多进程读写**

KungFu 实盘是多进程架构（行情网关、策略、交易网关各自独立进程），Paged 作为中心协调者：
- 跟踪在线客户端（通过 `register_client()` 注册）
- 防止多 writer 同时写同一个 journal（错误码 `PAGED_COMM_MORE_THAN_ONE_WRITE`）
- 管理共享通信缓冲区（`PAGE_ENGINE_COMM`），最多支持 1000 个并发客户端
- 通过 PID 检测自动清理已退出的客户端

**（三）路由系统级消息**

Paged 还处理 KungFu 系统级请求（来自 `PageSocketStruct.h`）：

| 常量 | 值 | 含义 |
|------|---|------|
| `PAGED_SOCKET_CONNECTION_TEST` | 0 | 连接测试 |
| `PAGED_SOCKET_STRATEGY_REGISTER` | 10 | 策略注册 |
| `PAGED_SOCKET_JOURNAL_REGISTER` | 11 | Journal 注册 |
| `PAGED_SOCKET_READER_REGISTER` | 12 | 注册 reader 客户端 |
| `PAGED_SOCKET_WRITER_REGISTER` | 13 | 注册 writer 客户端 |
| `PAGED_SOCKET_CLIENT_EXIT` | 19 | 客户端退出 |
| `PAGED_SOCKET_SUBSCRIBE` | 20 | 行情订阅 |
| `PAGED_SOCKET_TD_LOGIN` | 22 | 交易引擎登录 |

### 1.2 Paged 不做什么

**Paged 不存储数据。** 数据始终在 journal 文件里。Paged 只是帮客户端 mmap 文件并管理映射的生命周期。数据流是：

```
journal 文件 (磁盘) → Paged 做 mmap 分配 → 通过共享内存交给客户端 → 客户端直接读写内存
```

## 2. 架构：PageProvider 体系

### 2.1 类继承关系

```
IPageProvider (纯接口)
  └── PageProvider (抽象基类)
        ├── LocalPageProvider   — 直接本地 mmap，不需要 Paged
        └── ClientPageProvider  — 通过 Paged 服务获取 page
```

### 2.2 JournalReader 的两组工厂方法

`JournalReader` 提供了多个静态工厂方法，内部使用不同的 `PageProvider`：

**使用 LocalPageProvider（不需要 Paged）：**

```cpp
// 指定 dir + jname，直接本地 mmap 读取
JournalReader::create(const string& dir, const string& jname, long startTime, const string& readerName);
JournalReader::create(const string& dir, const string& jname, long startTime);
JournalReader::create(const vector<string>& dirs, const vector<string>& jnames, long startTime, const string& readerName);
```

**使用 ClientPageProvider（需要 Paged 运行）：**

```cpp
// "WithSys" = 使用系统服务 + 自动订阅系统 journal
JournalReader::createReaderWithSys(const vector<string>& dirs, const vector<string>& jnames, long startTime, const string& readerName);

// 只读系统 journal 的特殊 reader
JournalReader::createSysReader(const string& readerName);

// 带修改权限的 reader
JournalReader::createRevisableReader(const string& readerName);
```

### 2.3 createReaderWithSys 做了什么

`createReaderWithSys` 比普通 `create` 多做两件事：
1. 使用 `ClientPageProvider` 而非 `LocalPageProvider`，通过 `paged.sock` 与 Paged 通信
2. 自动添加系统 journal（`/shared/kungfu/journal/system/SYSTEM`），接收 Paged 广播的系统消息（如 `MSG_TYPE_PAGED_START=20`、`MSG_TYPE_PAGED_END=21`）

调用流程：
```
createReaderWithSys()
  → new ClientPageProvider(readerName, isWriting=false)
      → register_client()                          # 连接 paged.sock，发送 PAGED_SOCKET_READER_REGISTER
      → 获得 comm_file (PAGE_ENGINE_COMM) 和 hash_code
  → new JournalReader(clientPageProvider)
  → reader->addJournal(PAGED_JOURNAL_FOLDER, PAGED_JOURNAL_NAME)  # 自动添加系统 journal
  → reader->addJournal(dirs[i], jnames[i])          # 添加用户指定的 journal
  → reader->jumpStart(startTime)
```

## 3. 实盘进程结构

实盘机器上的进程树：

```
supervisord -c /opt/kungfu/master/etc/supervisor/supervisord.conf   # PID 3229
  └── python -u /opt/kungfu/master/bin/yjj server                   # PID 3230 (父)
        └── python -u /opt/kungfu/master/bin/yjj server             # PID 3231 (实际 PageEngine，CPU 占 99%)
```

`yjj server` 的启动代码（`/opt/kungfu/master/bin/yjj` 第 175-199 行）：

```python
def server(self):
    import libpaged
    engine = libpaged.PageEngine()

    # 启动 rconsole 远程管理线程
    rconsoled = RConsoleDaemon()
    rconsole_thread = threading.Thread(name="rconsole", target=rconsoled.run)
    rconsole_thread.start()

    # 添加任务
    task_temppage = libpaged.TempPage(engine)       # 临时页面管理
    engine.addTask(task_temppage)

    task_control = libpaged.Controller(engine)       # 交易时段控制器
    task_control.set_switch_day_time('17:00:00')     # 17:00 切换交易日
    task_control.add_engine_start_time('09:15:00')   # 09:15 引擎开始
    task_control.add_engine_end_time('16:30:00')     # 16:30 引擎结束
    engine.addTask(task_control)

    engine.setFreq(seconds=0.01)                     # 100Hz 轮询频率
    engine.start()                                   # 阻塞运行
```

## 4. 本地 Docker 环境搭建

### 4.1 环境确认

本地 Docker 已经包含所有必要组件：

| 组件 | 路径 | 说明 |
|------|------|------|
| yjj 命令行工具 | `/opt/kungfu/master/bin/yjj` | Python 2.7 脚本，Paged 入口 |
| libpaged.so | `/opt/kungfu/master/lib/yijinjing/libpaged.so` | PageEngine C++ 实现 |
| libjournal.so | `/opt/kungfu/master/lib/yijinjing/libjournal.so.1.1` | Journal 读写库 |
| Boost Python | `/opt/kungfu/master/lib/boost/` | Boost 1.62.0 |
| 通信缓冲区 | `/shared/kungfu/journal/PAGE_ENGINE_COMM` | 137KB，支持 1000 并发客户端 |
| 临时页面 | `/shared/kungfu/journal/TEMP_PAGE` | 128MB，页面分配用 |
| 系统 journal | `/shared/kungfu/journal/system/yjj.SYSTEM.1.journal` | 128MB |
| Unix socket 目录 | `/shared/kungfu/socket/` | paged.sock 所在目录 |
| PID 文件目录 | `/shared/kungfu/pid/` | paged.pid 所在目录 |
| 运行时目录 | `/shared/kungfu/runtime/` | yjj server 的工作目录 |
| 日志 | `/shared/kungfu/log/page_engine.log` | PageEngine 运行日志 |

### 4.2 启动 Paged

```bash
# 第一步：清理上次残留的 stale 文件
rm -f /shared/kungfu/pid/paged.pid \
      /shared/kungfu/socket/paged.sock \
      /shared/kungfu/socket/paged_rconsole.sock

# 第二步：启动 Paged（后台运行）
cd /shared/kungfu/runtime && \
KUNGFU_MAIN=/opt/kungfu/master/bin/yjj \
PYTHONPATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
nohup python2 -u /opt/kungfu/master/bin/yjj server > /shared/kungfu/log/paged_stdout.log 2>&1 &
```

启动成功的日志输出：
```
PageEngine %% - reset socket: /shared/kungfu/socket/paged.sock
PageEngine %% - loading page buffer: /shared/kungfu/journal/PAGE_ENGINE_COMM
PageEngine %% - creating writer: (f)/shared/kungfu/journal/system/ (n)SYSTEM
PageEngine %% - [RegClient] (name)paged (writer?)1
PageEngine %% - [RegJournal] (client)paged (idx)0
PageEngine %% - NEW TEMP PAGE: /shared/kungfu/journal/TEMP_PAGE
```

验证启动成功：
```bash
# 确认 socket 文件是真正的 Unix socket（不是空文件）
file /shared/kungfu/socket/paged.sock
# 应输出: paged.sock: socket

# 确认进程存在
ps -ef | grep "yjj server" | grep -v grep
```

### 4.3 查看 Paged 状态

```bash
# 方法一：命令行
KUNGFU_MAIN=/opt/kungfu/master/bin/yjj \
PYTHONPATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
python2 -u /opt/kungfu/master/bin/yjj status

# 方法二：交互式 console
python2 -u /opt/kungfu/master/bin/yjj interact
>>> engine.status()
```

status 输出示例：
```python
{
    'Client': {
        'paged': {
            'hash_code': -1548234844,
            'is_writing': True,
            'pid': 42120,
            'users': [0]          # 使用的通信槽位
        }
    },
    'File': {
        'Locking': ['/shared/kungfu/journal/TEMP_PAGE',
                     '/shared/kungfu/journal/system/yjj.SYSTEM.1.journal'],
        'Read': {},
        'Write': {('/shared/kungfu/journal/system', 'SYSTEM', 1, 0, True): 1}
    },
    'Task': ('running', 10000, {
        'KfController': {
            'engine_ends': ['2026-03-04 16:30:00'],
            'engine_starts': ['2026-03-05 09:15:00'],
            'switch_day': '2026-03-04 17:00:00'
        },
        'PidCheck': {},
        'TempPage': {}
    }),
    'Time': '2026-03-04 15:41:24'
}
```

### 4.4 停止 Paged

```bash
KUNGFU_MAIN=/opt/kungfu/master/bin/yjj \
PYTHONPATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
python2 -u /opt/kungfu/master/bin/yjj shutdown
```

### 4.5 使用 supervisord 管理（与实盘一致）

如果想完全复制实盘的进程管理方式：

```bash
# 启动（会自动拉起 yjj server）
/usr/bin/python2 /usr/bin/supervisord -c /opt/kungfu/master/etc/supervisor/supervisord.conf

# supervisor 配置路径
# 主配置: /opt/kungfu/master/etc/supervisor/supervisord.conf
# yjj 配置: /opt/kungfu/master/etc/supervisor/conf.d/yijinjing.conf
```

注意：supervisord 配置中 user 是 `bruce`，本地如果以 root 运行可能需要调整。

## 5. 运行依赖 Paged 的程序

### 5.1 C++ 程序（orig_insight_demo 风格）

```bash
# 确保 Paged 已启动，然后：
cd build && \
LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost:../lib \
./data_consumer
```

Paged 日志中会出现客户端注册记录：
```
[RegClient] (name)Consumer_154023_R (writer?)0
[RegJournal] (client)Consumer_154023_R (idx)1
[InPage] (folder)/path/to/data (jname)insight_stock_tick_data (pNum)1 (lpNum)0 (writer?)0
```

### 5.2 Python 程序

```python
import libjournal

data_path = '/workspace/valerie/streaming_factor/kungfu_demo/deps/data'

# createReader 在 Python 绑定中对应 C++ 的 createReaderWithSys
# 签名: createReader(dirs_list, jnames_list, reader_name, start_nano)
reader = libjournal.createReader(
    [data_path],
    ['insight_stock_tick_data'],
    'my_reader',
    long(0)
)

# Python API 中 Frame 的方法
frame = reader.next()            # 对应 C++ getNextFrame()
if frame is not None:
    msg_type = frame.msg_type()  # 注意是方法调用，不是属性
    nano = frame.nano()
    source = frame.source()
    # frame.get_data()  返回原始数据指针（Python 中不常用）
    # frame.get_str()   返回字符串表示

# Reader 的方法
# reader.addJ(dir, jname)        # 添加 journal
# reader.expireJ(idx)            # 过期某个 journal
# reader.restartJ(idx)           # 重启某个 journal
# reader.name()                  # 获取 reader 名称
```

运行 Python 程序前设置环境：
```bash
PYTHONPATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
python2 your_script.py
```

## 6. Journal 数据管理

### 6.1 文件命名规则

```
yjj.<channel_name>.<page_num>.journal
```

- `channel_name`：频道名，如 `insight_stock_tick_data`
- `page_num`：从 1 开始递增
- 每个文件固定 128MB（`JOURNAL_PAGE_SIZE = 0x8000000`）

### 6.2 存放历史数据

只需将 journal 文件放到任意目录，然后在代码中指定该目录路径。Paged 不要求数据必须在特定路径下——客户端注册时会告诉 Paged 数据目录在哪。

例如按日期组织：
```
/data/journals/
├── 20260205/
│   ├── yjj.insight_stock_tick_data.1.journal
│   ├── yjj.insight_stock_order_data.1.journal
│   └── yjj.insight_stock_trade_data.1.journal
├── 20260206/
│   ├── yjj.insight_stock_tick_data.1.journal
│   └── ...
```

代码中修改 `path` 指向对应日期目录即可。

### 6.3 实盘环境的 journal 目录结构

```
/shared/kungfu/journal/
├── PAGE_ENGINE_COMM              # 137KB 通信缓冲区
├── TEMP_PAGE                     # 128MB 临时页面
├── system/
│   └── yjj.SYSTEM.1.journal     # Paged 自身的系统 journal
├── MD/                           # 行情数据
│   ├── CTP/                      # CTP 行情 journal
│   └── XTP/                      # XTP 行情 journal
├── TD/                           # 交易数据
├── TD_RAW/                       # 原始交易数据
├── TD_Q/                         # 交易查询
├── TD_SEND/                      # 交易发送
├── strategy/                     # 策略 journal
└── user/                         # 用户自定义 journal（如 insight 数据）
```

## 7. 关键文件路径速查

### 7.1 可执行文件和库

| 文件 | 路径 |
|------|------|
| yjj 命令行 | `/opt/kungfu/master/bin/yjj` |
| journal_tool | `/opt/kungfu/master/bin/journal_tool` |
| journal_dumper | `/opt/kungfu/master/bin/journal_dumper` |
| libpaged.so | `/opt/kungfu/master/lib/yijinjing/libpaged.so` |
| libjournal.so | `/opt/kungfu/master/lib/yijinjing/libjournal.so.1.1` |
| libkflog.so | `/opt/kungfu/master/lib/yijinjing/libkflog.so` |

### 7.2 配置文件

| 文件 | 路径 |
|------|------|
| supervisor 主配置 | `/opt/kungfu/master/etc/supervisor/supervisord.conf` |
| yjj supervisor 配置 | `/opt/kungfu/master/etc/supervisor/conf.d/yijinjing.conf` |
| 环境变量 | `/opt/kungfu/master/etc/sysconfig/kungfu` |
| 日志配置 | `/opt/kungfu/master/etc/log4cplus/default.properties` |

### 7.3 运行时文件

| 文件 | 路径 |
|------|------|
| Paged socket | `/shared/kungfu/socket/paged.sock` |
| RConsole socket | `/shared/kungfu/socket/paged_rconsole.sock` |
| Supervisor socket | `/shared/kungfu/socket/kungfu-supervisor.sock` |
| PID 文件 | `/shared/kungfu/pid/paged.pid` |
| 通信缓冲区 | `/shared/kungfu/journal/PAGE_ENGINE_COMM` |
| 临时页面 | `/shared/kungfu/journal/TEMP_PAGE` |
| PageEngine 日志 | `/shared/kungfu/log/page_engine.log` |

### 7.4 头文件（开发用）

| 文件 | 路径 | 内容 |
|------|------|------|
| PageProvider.h | `deps/kungfu_include/` | LocalPageProvider / ClientPageProvider 定义 |
| PageCommStruct.h | `deps/kungfu_include/` | 通信缓冲区协议结构 |
| PageSocketStruct.h | `deps/kungfu_include/` | Socket 通信协议结构 |
| JournalReader.h | `deps/kungfu_include/` | JournalReader 及工厂方法 |
| YJJ_DECLARE.h | `deps/kungfu_include/` | 路径常量宏定义 |
| Page.h | `deps/kungfu_include/` | Page 类，单页 mmap 管理 |
| Journal.h | `deps/kungfu_include/` | Journal 类，跨页连续访问 |
| Frame.hpp | `deps/kungfu_include/` | Frame 类，帧数据访问器 |

## 8. 环境变量速查

启动 Paged 或运行依赖 KungFu 的程序前需要设置：

```bash
export KUNGFU_MAIN=/opt/kungfu/master/bin/yjj
export PYTHONPATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost
export LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost
```

完整环境变量参考 `/opt/kungfu/master/etc/sysconfig/kungfu`：
```bash
KUNGFU_HOME=/opt/kungfu
YJJ_HOME=/opt/kungfu/master
BOOST_LIB=/opt/kungfu/master/lib/boost
YJJ_LIB=/opt/kungfu/master/lib/yijinjing
```

## 9. 硬编码路径常量

以下路径硬编码在 `YJJ_DECLARE.h` 中，**不可通过配置修改**：

```cpp
#define KUNGFU_FOLDER        "/shared/kungfu/"
#define KUNGFU_JOURNAL_FOLDER "/shared/kungfu/journal/"
#define KUNGFU_SOCKET_FOLDER  "/shared/kungfu/socket/"
#define KUNGFU_LOG_FOLDER     "/shared/kungfu/log/"
#define PAGED_JOURNAL_FOLDER  "/shared/kungfu/journal/system/"
#define PAGED_JOURNAL_NAME    "SYSTEM"
```

这意味着 Paged 的 socket、通信缓冲区、系统 journal 等固定在 `/shared/kungfu/` 下，但**用户数据的 journal 文件可以放在任意目录**。

## 10. 常用 yjj 命令

```bash
# 以下命令均需先设置环境变量（见第 8 节）

yjj server     # 启动 PageEngine
yjj status     # 查看运行状态
yjj interact   # 进入交互式 console（可调用 engine.status() 等）
yjj shutdown   # 停止 PageEngine
yjj journal    # 调用 journal_tool 查看 journal 内容
yjj dump       # 调用 journal_dumper 导出 journal
yjj shell      # 启动带 KungFu 环境的 Python shell
```

## 11. 故障排查

### Paged 启动失败

```
# 检查是否有残留的 PID/socket 文件
ls -la /shared/kungfu/pid/paged.pid
ls -la /shared/kungfu/socket/paged.sock

# 清理后重试
rm -f /shared/kungfu/pid/paged.pid /shared/kungfu/socket/paged.sock /shared/kungfu/socket/paged_rconsole.sock
```

### createReaderWithSys 连接失败

```
# 确认 paged.sock 是 Unix socket（不是空文件）
file /shared/kungfu/socket/paged.sock
# 正确: paged.sock: socket
# 错误: paged.sock: empty（Paged 没有运行）

# 确认 Paged 进程在运行
ps -ef | grep "yjj server" | grep -v grep
```

### 读不到数据

```
# 检查 Paged 日志看是否成功注册了 journal
tail -f /shared/kungfu/log/page_engine.log
# 应看到: [InPage] (folder)/your/path (jname)your_channel (pNum)1

# 确认 journal 文件存在且命名正确
ls /your/path/yjj.your_channel.*.journal
```
