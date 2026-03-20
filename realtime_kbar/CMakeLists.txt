cmake_minimum_required(VERSION 3.18)
project(realtime_kbar)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -O3 -Wall -fPIC")

# ============ Docker 环境依赖路径配置 ============
SET(TOOLCHAIN_DIR "/opt/kungfu/toolchain")
SET(BOOST_ROOT "${TOOLCHAIN_DIR}/boost-1.62.0")
SET(BOOST_INCLUDEDIR "${BOOST_ROOT}/include")
SET(BOOST_LIBRARYDIR "${BOOST_ROOT}/lib")

# KungFu 头文件和库
SET(KUNGFU_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/deps/kungfu_include")
SET(KUNGFU_LIB_DIR "${CMAKE_SOURCE_DIR}/deps/kungfu_yijinjing_lib")

# Arrow + Parquet
include_directories("/usr/local/include/")
link_directories("/usr/local/lib64")
link_directories("/usr/local/lib")

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(Threads REQUIRED)

# Python 2.7（KungFu journal 依赖 Boost.Python）
FIND_PACKAGE(PythonLibs 2)

# Boost 库
SET(Boost_LIBRARIES
    ${BOOST_LIBRARYDIR}/libboost_locale.so
    ${BOOST_LIBRARYDIR}/libboost_date_time.so
    ${BOOST_LIBRARYDIR}/libboost_filesystem.so
    ${BOOST_LIBRARYDIR}/libboost_system.so
    ${BOOST_LIBRARYDIR}/libboost_thread.so
    ${BOOST_LIBRARYDIR}/libboost_serialization.so
    ${BOOST_LIBRARYDIR}/libboost_math_tr1.so
    ${BOOST_LIBRARYDIR}/libboost_python.so
    ${BOOST_LIBRARYDIR}/libboost_chrono.so
    ${BOOST_LIBRARYDIR}/libboost_regex.so
    ${BOOST_LIBRARYDIR}/libboost_program_options.so
    ${BOOST_LIBRARYDIR}/libboost_atomic.so
)

# ============ Binary 1: kbar_offline（只需 Arrow/Parquet） ============
add_executable(kbar_offline
    src/kbar_offline.cpp
    src/tick_bar_builder.cpp
    src/parquet_reader.cpp
    src/jq_comparator.cpp
)

target_include_directories(kbar_offline PRIVATE src)

target_link_libraries(kbar_offline
    Arrow::arrow_shared
    Parquet::parquet_shared
    Threads::Threads
)

# ============ Binary 2: kbar_realtime（需要 KungFu journal） ============
INCLUDE_DIRECTORIES(${BOOST_INCLUDEDIR})
INCLUDE_DIRECTORIES(${KUNGFU_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${PYTHON_INCLUDE_PATH})
LINK_DIRECTORIES(${KUNGFU_LIB_DIR})

add_executable(kbar_realtime
    src/kbar_realtime.cpp
    src/tick_bar_builder.cpp
)

target_include_directories(kbar_realtime PRIVATE src)

target_link_libraries(kbar_realtime
    Threads::Threads
    journal
    -lpthread
    python2.7
    ${Boost_LIBRARIES}
)
