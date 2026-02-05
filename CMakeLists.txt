cmake_minimum_required(VERSION 3.18)
project (playback)
set (CMAKE_BUILE_TYPE BUILD)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ============ 本地依赖路径配置 ============
# 依赖文件放在项目下的 deps/ 目录
SET(DEPS_DIR "${PROJECT_SOURCE_DIR}/deps")
SET(KUNGFU_INCLUDE_DIR "${DEPS_DIR}/kungfu_include")
SET(KUNGFU_LIB_DIR "${DEPS_DIR}/kungfu_yijinjing_lib")
SET(BOOST_ROOT "${DEPS_DIR}/boost-1.62.0")

SET(BOOST_INCLUDEDIR "${BOOST_ROOT}/include")
SET(BOOST_LIBRARYDIR "${BOOST_ROOT}/lib")

SET(Boost_USE_MULTITHREADED ON)
SET(Boost_USE_STATIC_LIBS OFF)
SET(Boost_USE_STATIC_RUNTIME OFF)
SET(Boost_NO_SYSTEM_PATHS ON)

include_directories("/usr/local/include/")

link_directories("/usr/local/lib64")
link_directories("/usr/local/lib")

# 手动指定 Boost 库（避免 FIND_PACKAGE 版本检测问题）
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
INCLUDE_DIRECTORIES(${BOOST_INCLUDEDIR})

# Python 2.7 配置 (使用 conda 环境)
SET(PYTHON_INCLUDE_DIR "/home/linwei/miniconda3/envs/py27/include/python2.7")
SET(PYTHON_LIBRARY "/home/linwei/miniconda3/envs/py27/lib")
INCLUDE_DIRECTORIES(${PYTHON_INCLUDE_DIR})
LINK_DIRECTORIES(${PYTHON_LIBRARY})

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -O3 -Wall -fPIC")

# Kungfu 头文件
INCLUDE_DIRECTORIES(${KUNGFU_INCLUDE_DIR})

# Kungfu yijinjing 库
LINK_DIRECTORIES(${KUNGFU_LIB_DIR})

link_directories("${PROJECT_SOURCE_DIR}/lib/")

SET(INSIGHT_API ACE ACE_SSL protobuf mdc_gateway_client)
set(CMAKE_INCLUDE_DIRECTORIES_PROJECT_BEFORE ON)

include_directories("${PROJECT_SOURCE_DIR}"
	"${PROJECT_SOURCE_DIR}/include"
	"${PROJECT_SOURCE_DIR}/include/mdc_gateway_client/protobuf"
	"${PROJECT_SOURCE_DIR}/include/mdc_gateway_client/include"
	"${PROJECT_SOURCE_DIR}/include/protobuf-3.1.0/include")

ADD_LIBRARY(insight_handle SHARED
	utils.cpp
	insight_types.cpp)

target_link_libraries(insight_handle journal -lpthread python2.7 ${Boost_LIBRARIES} ${INSIGHT_API})
ADD_LIBRARY(consumer SHARED data_consumer.cpp)
TARGET_LINK_LIBRARIES(consumer insight_handle)
ADD_EXECUTABLE(data_consumer data_consumer.cpp)

TARGET_LINK_LIBRARIES(data_consumer insight_handle)





