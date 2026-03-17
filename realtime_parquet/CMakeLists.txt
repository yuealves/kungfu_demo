cmake_minimum_required(VERSION 3.18)
project(realtime_parquet)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall -fPIC")

# ============ Docker 环境依赖路径配置 ============
SET(TOOLCHAIN_DIR "/opt/kungfu/toolchain")
SET(BOOST_ROOT "${TOOLCHAIN_DIR}/boost-1.62.0")
SET(BOOST_INCLUDEDIR "${BOOST_ROOT}/include")
SET(BOOST_LIBRARYDIR "${BOOST_ROOT}/lib")

# KungFu 头文件和库（自包含在 deps/ 中，可通过 symlink 或实际文件）
SET(KUNGFU_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/deps/kungfu_include")
SET(KUNGFU_LIB_DIR "${CMAKE_SOURCE_DIR}/deps/kungfu_yijinjing_lib")

# Arrow + Parquet（通过 setup_parquet.sh 安装到 /usr/local/）
include_directories("/usr/local/include/")
link_directories("/usr/local/lib64")
link_directories("/usr/local/lib")

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(Threads REQUIRED)

# Python 2.7（KungFu journal 依赖 Boost.Python）
FIND_PACKAGE(PythonLibs 2)

# Boost 库（手动指定，避免 FIND_PACKAGE 版本检测问题）
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

# Include paths
INCLUDE_DIRECTORIES(${BOOST_INCLUDEDIR})
INCLUDE_DIRECTORIES(${KUNGFU_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${PYTHON_INCLUDE_PATH})

# Library paths
LINK_DIRECTORIES(${KUNGFU_LIB_DIR})

add_executable(realtime_parquet
    src/main.cpp
    src/minute_writer.cpp
    src/kbar_builder.cpp
)

target_include_directories(realtime_parquet PRIVATE src)

target_link_libraries(realtime_parquet
    Arrow::arrow_shared
    Parquet::parquet_shared
    Threads::Threads
    journal
    -lpthread
    python2.7
    ${Boost_LIBRARIES}
)
