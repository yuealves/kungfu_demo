cmake_minimum_required(VERSION 3.18)
project(streaming_factors)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ============ Docker 环境依赖路径 ============
SET(TOOLCHAIN_DIR "/opt/kungfu/toolchain")
SET(KUNGFU_INCLUDE_DIR "/opt/kungfu/master/include")
SET(KUNGFU_LIB_DIR "/opt/kungfu/master/lib/yijinjing")
SET(BOOST_ROOT "${TOOLCHAIN_DIR}/boost-1.62.0")
SET(BOOST_INCLUDEDIR "${BOOST_ROOT}/include")
SET(BOOST_LIBRARYDIR "${BOOST_ROOT}/lib")

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

FIND_PACKAGE(PythonInterp 2)
FIND_PACKAGE(PythonLibs 2)

add_definitions(-DPROJECT_ROOT_DIR="${PROJECT_SOURCE_DIR}")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -O2 -Wall -fPIC")

INCLUDE_DIRECTORIES(
    ${KUNGFU_INCLUDE_DIR}
    ${BOOST_INCLUDEDIR}
    ${PYTHON_INCLUDE_PATH}
    "${PROJECT_SOURCE_DIR}"
)

LINK_DIRECTORIES(
    ${KUNGFU_LIB_DIR}
    ${BOOST_LIBRARYDIR}
)

# ---- 基础库（数据结构转换） ----
ADD_LIBRARY(insight_handle SHARED utils.cpp insight_types.cpp)
TARGET_LINK_LIBRARIES(insight_handle journal -lpthread python2.7 ${Boost_LIBRARIES})

# ---- 工具程序 ----
ADD_EXECUTABLE(journal_replayer journal_replayer.cpp)
TARGET_LINK_LIBRARIES(journal_replayer insight_handle)

ADD_EXECUTABLE(live_reader live_reader.cpp)
TARGET_LINK_LIBRARIES(live_reader insight_handle)

# ---- 入门示例 ----
ADD_EXECUTABLE(example_factor example_factor.cpp)
TARGET_LINK_LIBRARIES(example_factor insight_handle)

# ---- 你的流式因子引擎（取消注释并修改） ----
# ADD_EXECUTABLE(streaming_factors streaming_factors.cpp)
# TARGET_LINK_LIBRARIES(streaming_factors insight_handle)
