cmake_minimum_required(VERSION 3.18)
project(monitor_latency)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -O3 -Wall -fPIC")

# ============ Docker 环境依赖路径配置 ============
SET(TOOLCHAIN_DIR "/opt/kungfu/toolchain")
SET(KUNGFU_INCLUDE_DIR "/opt/kungfu/master/include")
SET(KUNGFU_LIB_DIR "/opt/kungfu/master/lib/yijinjing")
SET(BOOST_ROOT "${TOOLCHAIN_DIR}/boost-1.62.0")

SET(BOOST_INCLUDEDIR "${BOOST_ROOT}/include")
SET(BOOST_LIBRARYDIR "${BOOST_ROOT}/lib")

# KungFu 头文件
include_directories(${KUNGFU_INCLUDE_DIR})
include_directories(${BOOST_INCLUDEDIR})
include_directories(src)

# Python 2.7（KungFu 头文件间接引用）
find_package(PythonInterp 2)
find_package(PythonLibs 2)
include_directories(${PYTHON_INCLUDE_PATH})

# 库目录
link_directories(${KUNGFU_LIB_DIR})
link_directories(${BOOST_LIBRARYDIR})

# Boost 库
# libjournal.so 的完整 Boost 依赖（通过 ldd 确认）
SET(Boost_LIBRARIES
    ${BOOST_LIBRARYDIR}/libboost_filesystem.so
    ${BOOST_LIBRARYDIR}/libboost_system.so
    ${BOOST_LIBRARYDIR}/libboost_thread.so
    ${BOOST_LIBRARYDIR}/libboost_python.so
    ${BOOST_LIBRARYDIR}/libboost_chrono.so
    ${BOOST_LIBRARYDIR}/libboost_regex.so
    ${BOOST_LIBRARYDIR}/libboost_date_time.so
    ${BOOST_LIBRARYDIR}/libboost_locale.so
    ${BOOST_LIBRARYDIR}/libboost_serialization.so
    ${BOOST_LIBRARYDIR}/libboost_math_tr1.so
    ${BOOST_LIBRARYDIR}/libboost_program_options.so
    ${BOOST_LIBRARYDIR}/libboost_atomic.so
)

add_executable(monitor_latency src/main.cpp)
target_link_libraries(monitor_latency journal -lpthread python2.7 ${Boost_LIBRARIES})
