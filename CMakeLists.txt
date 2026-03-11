cmake_minimum_required(VERSION 3.18)
project (playback)
set (CMAKE_BUILE_TYPE BUILD)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ============ Docker 环境依赖路径配置 ============
SET(TOOLCHAIN_DIR "/opt/kungfu/toolchain")
SET(KUNGFU_INCLUDE_DIR "/opt/kungfu/master/include")
SET(KUNGFU_LIB_DIR "/opt/kungfu/master/lib/yijinjing")
SET(BOOST_ROOT "${TOOLCHAIN_DIR}/boost-1.62.0")

SET(BOOST_INCLUDEDIR "${BOOST_ROOT}/include")
SET(BOOST_LIBRARYDIR "${BOOST_ROOT}/lib")

SET(Boost_USE_MULTITHREADED ON)
SET(Boost_USE_STATIC_LIBS OFF)
SET(Boost_USE_STATIC_RUNTIME OFF)
# SET(Boost_NO_SYSTEM_PATHS ON)

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

# Python 2.7 配置
FIND_PACKAGE(PythonInterp 2)
FIND_PACKAGE(PythonLibs 2)
INCLUDE_DIRECTORIES(${PYTHON_INCLUDE_PATH})
link_directories(${PYTHON_LIBRARIES})

# 将项目根路径注入编译宏，代码中可用 PROJECT_ROOT_DIR 拼接相对路径
add_definitions(-DPROJECT_ROOT_DIR="${PROJECT_SOURCE_DIR}")

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall -fPIC")

# Kungfu 头文件
INCLUDE_DIRECTORIES(${KUNGFU_INCLUDE_DIR})

# Kungfu yijinjing 库
LINK_DIRECTORIES(${KUNGFU_LIB_DIR})

link_directories("${PROJECT_SOURCE_DIR}/lib/")

SET(INSIGHT_API ACE ACE_SSL protobuf mdc_gateway_client)
set(CMAKE_INCLUDE_DIRECTORIES_PROJECT_BEFORE ON)

include_directories("${PROJECT_SOURCE_DIR}/src"
	"${PROJECT_SOURCE_DIR}/include"
	"${PROJECT_SOURCE_DIR}/include/mdc_gateway_client/protobuf"
	"${PROJECT_SOURCE_DIR}/include/mdc_gateway_client/include"
	"${PROJECT_SOURCE_DIR}/include/protobuf-3.1.0/include")

ADD_LIBRARY(insight_handle SHARED
	src/utils.cpp
	src/insight_types.cpp)

target_link_libraries(insight_handle journal -lpthread python2.7 ${Boost_LIBRARIES} ${INSIGHT_API})
ADD_LIBRARY(consumer SHARED src/data_consumer.cpp)
TARGET_LINK_LIBRARIES(consumer insight_handle)
ADD_EXECUTABLE(data_consumer src/data_consumer.cpp)

TARGET_LINK_LIBRARIES(data_consumer insight_handle)

ADD_EXECUTABLE(journal_replayer tools/journal_replayer.cpp)
TARGET_LINK_LIBRARIES(journal_replayer insight_handle)

ADD_EXECUTABLE(live_reader tools/live_reader.cpp)
TARGET_LINK_LIBRARIES(live_reader insight_handle)

ADD_EXECUTABLE(verify_journal_time tests/verify_journal_time.cpp)
TARGET_LINK_LIBRARIES(verify_journal_time insight_handle)
# RUNPATH 替代 RPATH，允许 LD_LIBRARY_PATH 覆盖（测试新 libjournal.so 时需要）
set_target_properties(verify_journal_time PROPERTIES LINK_FLAGS "-Wl,--enable-new-dtags")

ADD_EXECUTABLE(verify_nanotime tests/verify_nanotime.cpp)
TARGET_LINK_LIBRARIES(verify_nanotime dl)
# verify_nanotime 通过 dlopen 加载 libjournal.so，清除 RPATH 让 LD_LIBRARY_PATH 生效
set_target_properties(verify_nanotime PROPERTIES SKIP_BUILD_RPATH TRUE)

