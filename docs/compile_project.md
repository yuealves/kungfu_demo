# 项目编译指南

本文档记录在 Docker 开发环境中编译以下两个项目的完整过程：

1. **libjournal.so** — KungFu v1.0.0 的 journal 库（含 `getNanoTime()` 时钟偏移修复）
2. **ht_insight_market_gateway** — 华泰 Insight 行情网关

---

## 1. 编译环境

| 组件 | 版本 | 路径 |
|------|------|------|
| GCC | 8.5.0 | `/usr/bin/g++` |
| CMake | 3.26.0 | `/usr/local/bin/cmake` |
| Python 2.7 | 2.7.18 | `/usr/bin/python2.7` |
| Boost | 1.62.0 | `/opt/kungfu/toolchain/boost-1.62.0/` |
| KungFu RPM | v1.0.0 | `/opt/kungfu/master/` |
| Arrow | 21.0.0 | `/usr/local/lib64/` |
| Parquet | 21.0.0 | `/usr/local/lib64/` |

**注意**：系统上同时存在 Python 3.7（`/usr/local/`）和 Boost 1.69.0（`/usr/local/`），CMake 默认会优先找到它们，必须在 cmake 命令中显式指定 Python 2.7 和 Boost 1.62.0 路径。

---

## 2. 编译 libjournal.so（KungFu 源码）

### 2.1 源码位置

```
kungfu_source/
├── CMakeLists.txt
├── config.in
├── yijinjing/
│   ├── journal/        # libjournal.so 源码
│   │   └── CMakeLists.txt
│   └── utils/
│       ├── Timer.cpp   # getNanoTime 修复在此
│       └── Timer.h
└── longfist/           # 头文件依赖
```

### 2.2 修复内容

`Timer.cpp` 的 `get_local_diff()` 函数，将整秒截断的时钟差值改为纳秒精度：

```cpp
// 修复后（71-79 行）
inline long get_local_diff()
{
    timespec tp_real, tp_mono;
    clock_gettime(CLOCK_REALTIME, &tp_real);
    clock_gettime(CLOCK_MONOTONIC, &tp_mono);
    long real_ns = tp_real.tv_sec * NANOSECONDS_PER_SECOND + tp_real.tv_nsec;
    long mono_ns = tp_mono.tv_sec * NANOSECONDS_PER_SECOND + tp_mono.tv_nsec;
    return real_ns - mono_ns;
}
```

### 2.3 CMakeLists.txt 修改

`yijinjing/journal/CMakeLists.txt` 第 39 行，将硬编码的 `python` 改为 CMake 变量：

```cmake
# 修改前（无法链接，系统上没有 libpython.so，只有 libpython2.7.so）
TARGET_LINK_LIBRARIES(${PROJECT_NAME} python ${Boost_LIBRARIES})

# 修改后
TARGET_LINK_LIBRARIES(${PROJECT_NAME} ${PYTHON_LIBRARIES} ${Boost_LIBRARIES})
```

**原因**：原始 CMakeLists 链接 `-lpython`，但系统上 Python 2.7 的库文件名为 `libpython2.7.so`，没有 `libpython.so` 这个符号链接。改用 `${PYTHON_LIBRARIES}` 后，CMake 的 FindPythonLibs 会正确解析为 `/usr/lib64/libpython2.7.so`。

### 2.4 编译命令

```bash
cd kungfu_source
mkdir -p build && cd build

cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DSKIP_DEPENDENCY_CHECKING=ON \
  -DPYTHON_EXECUTABLE=/usr/bin/python2.7 \
  -DPYTHON_INCLUDE_DIR=/usr/include/python2.7 \
  -DPYTHON_LIBRARY=/usr/lib64/libpython2.7.so

make journal -j$(nproc)
```

各参数说明：

| 参数 | 作用 |
|------|------|
| `-DCMAKE_BUILD_TYPE=Release` | Release 优化编译（-O3） |
| `-DSKIP_DEPENDENCY_CHECKING=ON` | 跳过 RPM 依赖检查（我们不打 RPM 包） |
| `-DPYTHON_EXECUTABLE=...` | 强制使用 Python 2.7（系统默认���到 Python 3.7） |
| `-DPYTHON_INCLUDE_DIR=...` | 指定 Python 2.7 头文件路径 |
| `-DPYTHON_LIBRARY=...` | 指定 Python 2.7 动态库路径 |

### 2.5 产物

```
kungfu_source/build/yijinjing/journal/
├── libjournal.so      -> libjournal.so.1.1 (symlink)
└── libjournal.so.1.1  (987KB, 实际文件)
```

### 2.6 验证编译成功

```bash
# 检查 getNanoTime 等符号是否导出
nm -D kungfu_source/build/yijinjing/journal/libjournal.so.1.1 | grep getNano
# 期望输出：
#   000000000006c320 W _ZN6kungfu9yijinjing11getNanoTimeEv
#   00000000000939e0 T _ZNK6kungfu9yijinjing9NanoTimer7getNanoEv
```

---

## 3. 编译 ht_insight_market_gateway

### 3.1 项目位置

```
kungfu_demo/ht_insight_market_gateway/insight_market_gateway/
├── CMakeLists.txt
├── build.sh              # 自带构建脚本（有 Arrow 检查，建议手动 cmake）
└── src/brokers/insight_tcp/
    ├── CMakeLists.txt    # 定义所有编译目标
    ├── lib/              # 第三方动态库
    └── include/          # 第三方头文件
```

### 3.2 预编译准备

#### 3.2.1 补全缺失的 libmdc_gateway_client.so

项目 `lib/` 目录下缺少 `libmdc_gateway_client.so`，需要从 `kungfu_demo/lib/` 复制：

```bash
cp kungfu_demo/lib/libmdc_gateway_client.so \
   kungfu_demo/ht_insight_market_gateway/insight_market_gateway/src/brokers/insight_tcp/lib/
```

#### 3.2.2 创建动态库符号链接

`lib/` 目录下的 `.so` 文件带版本号后缀，链接器按 `-lACE` 查找时需要不带后缀的 `libACE.so`：

```bash
cd kungfu_demo/ht_insight_market_gateway/insight_market_gateway/src/brokers/insight_tcp/lib/

ln -sf libACE.so.6.4.3     libACE.so
ln -sf libACE_SSL.so.6.4.3 libACE_SSL.so
ln -sf libprotobuf.so.11    libprotobuf.so
```

#### 3.2.3 修复 subscribe_symbol 的 std::filesystem 链接

GCC 8 的 `std::filesystem` 需要显式链接 `-lstdc++fs`。修改 `src/brokers/insight_tcp/CMakeLists.txt`：

```cmake
# 修改前
TARGET_LINK_LIBRARIES(subscribe_symbol consumer Arrow::arrow_shared Parquet::parquet_shared)

# 修改后
TARGET_LINK_LIBRARIES(subscribe_symbol consumer Arrow::arrow_shared Parquet::parquet_shared stdc++fs)
```

### 3.3 编译命令

```bash
cd kungfu_demo/ht_insight_market_gateway/insight_market_gateway
mkdir -p build && cd build

cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH="/usr/local/lib64/cmake" \
  -DPYTHON_EXECUTABLE=/usr/bin/python2.7 \
  -DPYTHON_INCLUDE_DIR=/usr/include/python2.7 \
  -DPYTHON_LIBRARY=/usr/lib64/libpython2.7.so \
  -DBoost_INCLUDE_DIR=/opt/kungfu/toolchain/boost-1.62.0/include \
  -DBoost_LIBRARY_DIR_RELEASE=/opt/kungfu/toolchain/boost-1.62.0/lib \
  -DBoost_LIBRARY_DIR_DEBUG=/opt/kungfu/toolchain/boost-1.62.0/lib \
  -DBoost_NO_SYSTEM_PATHS=ON \
  -DBoost_NO_BOOST_CMAKE=ON

make -j$(nproc)
```

各参数说明：

| 参数 | 作用 |
|------|------|
| `-DCMAKE_PREFIX_PATH=...` | Arrow/Parquet 的 CMake config 搜索路径 |
| `-DPYTHON_*` | 强制 Python 2.7（同上） |
| `-DBoost_INCLUDE_DIR=...` | 直接指定 Boost 1.62 头文件路径 |
| `-DBoost_LIBRARY_DIR_*=...` | 直接指定 Boost 1.62 库路径 |
| `-DBoost_NO_SYSTEM_PATHS=ON` | 禁止搜索系统路径下的 Boost |
| `-DBoost_NO_BOOST_CMAKE=ON` | 禁用 Boost 自带的 CMake config |

**关键问题**：系统 `/usr/local/include/boost/` 下有 Boost 1.69.0 的头文件，`/usr/local/lib/` 下有对应的 `libboost_python27.so`。如果不通过 `Boost_INCLUDE_DIR` / `Boost_LIBRARY_DIR_*` 精确指定，CMake 的 FindBoost 模块会找到 1.69.0 版本，但其 Boost.Python 组件名为 `libboost_python27.so`（带版本后缀），与 toolchain 1.62.0 的 `libboost_python.so` 不兼容，导致 "missing: python" 错误。单独设置 `BOOST_ROOT` + `Boost_NO_SYSTEM_PATHS` 不够，因为 FindBoost 仍然可能从 `/usr/local/include/` 读取 `version.hpp` 判定版本为 1.69。

### 3.4 产物

```
build/src/brokers/insight_tcp/
├── insight_gateway       # 可执行 - Insight 行情网关
├── data_consumer         # 可执行 - journal 数据消费者
├── data_fetcher          # 可执行 - 数据导出器
├── subscribe_symbol      # 可执行 - 订阅工具
├── test_data_writer      # 可执行 - 测试数据写入
├── libinsight_handle.so  # 共享库 - 行情处理
└── libconsumer.so        # 共享库 - 消费者
```

---

## 4. 常见问题

### Q: `cannot find -lpython`

**原因**：CMake 找到了 Python 3.7 或项目 CMakeLists 硬编码了 `python`。

**解决**：cmake 时显式传入 `-DPYTHON_EXECUTABLE=/usr/bin/python2.7 -DPYTHON_INCLUDE_DIR=/usr/include/python2.7 -DPYTHON_LIBRARY=/usr/lib64/libpython2.7.so`。对于 `kungfu_source` 项目，还需修改 `journal/CMakeLists.txt` 将 `python` 改为 `${PYTHON_LIBRARIES}`。

### Q: `Could NOT find Boost (missing: python) (found 1.69.0)`

**原因**：系统 `/usr/local/` 下有 Boost 1.69.0，CMake 优先找到了它。

**解决**：使用 `-DBoost_INCLUDE_DIR` 和 `-DBoost_LIBRARY_DIR_RELEASE` 精确指定 1.62.0 路径，配合 `-DBoost_NO_SYSTEM_PATHS=ON -DBoost_NO_BOOST_CMAKE=ON`。

### Q: `undefined reference to std::filesystem::create_directories`

**原因**：GCC 8 的 `<filesystem>` 实现在 `libstdc++fs` 中。

**解决**：在 CMakeLists.txt 中添加 `stdc++fs` 到 `TARGET_LINK_LIBRARIES`。

### Q: `cannot find -lmdc_gateway_client`

**原因**：`libmdc_gateway_client.so` 未放入项目 `lib/` 目录。

**解决**：从 `kungfu_demo/lib/` 复制。

### Q: 重新 cmake 后仍然找到错误的 Boost 版本

**原因**：CMake 缓存了之前的查找结果。

**解决**：完全删除 `build/` 目录重新 cmake：`rm -rf build && mkdir build && cd build`。

---

## 5. 在测试环境验证 libjournal.so 修复

测试环境（另一台 Docker）有完整的 `kungfu_demo` 项目代码和 journal 历史数据，但没有接华泰行情（无法运行 insight_gateway）。可以通过 replayer/reader 方式验证新 .so 是否生效。

### 5.1 需要传到测试环境的文件

```
# 必须
kungfu_source/build/yijinjing/journal/libjournal.so.1.1    # 新编译的 .so

# 验证程序源码（在测试环境编译）
kungfu_demo/tests/verify_nanotime.cpp        # 独立测试，不依赖 KungFu 头文件
kungfu_demo/tests/verify_journal_time.cpp    # replayer/reader 流程测试
kungfu_demo/scripts/verify_nanotime_fix.sh  # 一键验证脚本
```

CMakeLists.txt 已添加 `verify_journal_time` 目标，git pull 后即可编译。

**RPATH 说明**：CMakeLists.txt 全局设置了 `--enable-new-dtags`，所有可执行文件和共享库使用 RUNPATH（而非 RPATH）。RUNPATH 允许 `LD_LIBRARY_PATH` 覆盖库搜索路径，因此测试新版 `libjournal.so` 时只需设置 `LD_LIBRARY_PATH` 即可，无需重新编译。

**LD_LIBRARY_PATH 说明**：运行时需要包含以下路径：
- 新 .so 所在目录（如 `/tmp/new_journal`，放在最前面优先加载）
- `../lib`（项目本地第三方库：ACE、Protobuf、mdc_gateway_client）
- `/opt/kungfu/master/lib/yijinjing`（KungFu 原版库，作为 fallback）
- `/opt/kungfu/toolchain/boost-1.62.0/lib`（Boost 1.62.0）

### 5.2 方法一：独立测试（推荐，最直接）

`tests/verify_nanotime.cpp` 通过 `dlopen` 加载 libjournal.so，直接对比 `getNanoTime()` 与 `clock_gettime(CLOCK_REALTIME)`，不依赖任何 KungFu 头文件或 Paged 服务。

```bash
# Step 1: 将新 .so 放到测试环境某个目录
mkdir -p /tmp/new_journal
cp libjournal.so.1.1 /tmp/new_journal/
ln -sf libjournal.so.1.1 /tmp/new_journal/libjournal.so

# Step 2: 编译测试程序
cd kungfu_demo
g++ -std=c++11 -O2 -o verify_nanotime tests/verify_nanotime.cpp -ldl

# Step 3: 用新 .so 运行
LD_LIBRARY_PATH=/tmp/new_journal ./verify_nanotime

# Step 4: 用旧 .so 运行（对比）
LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing ./verify_nanotime
```

**期望输出对比**：

新 .so（修复后）：
```
loaded: /tmp/new_journal/libjournal.so
  [ 1] diff =       -670 ns  (  -0.001 ms)  OK
  [ 2] diff =       -640 ns  (  -0.001 ms)  OK
  ...
结论: PASS — 所有采样偏差 < 1ms，getNanoTime() 修复生效
```

旧 .so（未修复）：
```
loaded: /opt/kungfu/master/lib/yijinjing/libjournal.so
  [ 1] diff = -317134289 ns  (-317.134 ms)  FAIL
  [ 2] diff = -317133849 ns  (-317.134 ms)  FAIL
  ...
结论: FAIL — 20 个采样偏差 >= 1ms，getNanoTime() 存在时钟漂移
```

### 5.3 方法二：通过 replayer/reader 验证（完整链路）

使用 replayer + reader 流程，让新 .so 参与完整的 journal 写入/读取链路。`verify_journal_time` 程序会：

1. 先做 10 次 `getNanoTime()` vs `clock_gettime` 本地精度测试
2. 连接 Paged，读取 replayer 写入的 frame
3. 对比 frame 的 `journal_time`（getNanoTime 写入的时间戳）与读取时刻的 wall clock
4. 打印统计汇总

```bash
# Step 1: 准备新 .so
mkdir -p /tmp/new_journal
cp libjournal.so.1.1 /tmp/new_journal/
ln -sf libjournal.so.1.1 /tmp/new_journal/libjournal.so

# Step 2: 设置 LD_LIBRARY_PATH（新 .so 在最前面）
export LD_LIBRARY_PATH=/tmp/new_journal:../lib:/opt/kungfu/master/lib/yijinjing:/opt/kungfu/toolchain/boost-1.62.0/lib

# Step 3: 编译
cd kungfu_demo/build
cmake .. -DCMAKE_BUILD_TYPE=Release && make verify_journal_time journal_replayer -j$(nproc)

# Step 4: 启动 Paged（终端 1）
bash ../scripts/start_paged.sh

# Step 5: 启动验证 reader（终端 2）
export LD_LIBRARY_PATH=/tmp/new_journal:../lib:/opt/kungfu/master/lib/yijinjing:/opt/kungfu/toolchain/boost-1.62.0/lib
cd kungfu_demo/build
./verify_journal_time

# Step 6: 启动 replayer（终端 3）
export LD_LIBRARY_PATH=/tmp/new_journal:../lib:/opt/kungfu/master/lib/yijinjing:/opt/kungfu/toolchain/boost-1.62.0/lib
cd kungfu_demo/build
./journal_replayer 10
```

**注意**：Paged 服务本身也链接 libjournal.so（通过 Python 2 的 yjj 模块），但 Paged 的 .so 在 `/opt/kungfu/master/lib/yijinjing/` 下，由 `start_paged.sh` 中的 `LD_LIBRARY_PATH` 控制。如果想让 Paged 也使用新 .so，需要修改 `start_paged.sh` 中的 `LD_LIBRARY_PATH`。不过 Paged 主要做页面管理，不做时间戳运算，所以不替换也不影响验证。

### 5.4 一键验证脚本

```bash
bash scripts/verify_nanotime_fix.sh /tmp/new_journal/libjournal.so.1.1
```

脚本会自动编译并运行方法一的独立测试，并打印方法二的手动执行指南。

### 5.5 验证要点

| 检查项 | 通过标准 |
|--------|----------|
| `verify_nanotime` 用新 .so | 所有采样 diff < 1ms |
| `verify_nanotime` 用旧 .so | 应该看到 ~300ms 偏差（作为对照） |
| `verify_journal_time` Step 0 | getNanoTime() 本地精度 < 1ms |
| `verify_journal_time` Step 2 | frame 的 journal_time 是合理的近期 wall clock 时间 |
| `ldd` 确认加载路径 | `ldd verify_nanotime` 显示的 libjournal.so 指向新 .so 路径 |
