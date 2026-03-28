cmake_minimum_required(VERSION 3.18)
project(realtime_factors)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

SET(TOOLCHAIN_DIR "/opt/kungfu/toolchain")
SET(KUNGFU_INCLUDE_DIR "/opt/kungfu/master/include")
SET(KUNGFU_LIB_DIR "/opt/kungfu/master/lib/yijinjing")
SET(BOOST_ROOT "${TOOLCHAIN_DIR}/boost-1.62.0")
SET(BOOST_INCLUDEDIR "${BOOST_ROOT}/include")
SET(BOOST_LIBRARYDIR "${BOOST_ROOT}/lib")

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -O2 -Wall -fPIC -Wno-register")

INCLUDE_DIRECTORIES(
    ${KUNGFU_INCLUDE_DIR}
    ${BOOST_INCLUDEDIR}
    "${PROJECT_SOURCE_DIR}/include"
    "${PROJECT_SOURCE_DIR}/../src"   # 父项目的 data_struct.hpp, sys_messages.h
)

LINK_DIRECTORIES(${KUNGFU_LIB_DIR} ${BOOST_LIBRARYDIR})

# Boost 库列表
SET(Boost_LIBRARIES
    ${BOOST_LIBRARYDIR}/libboost_filesystem.so
    ${BOOST_LIBRARYDIR}/libboost_system.so
    ${BOOST_LIBRARYDIR}/libboost_thread.so
    ${BOOST_LIBRARYDIR}/libboost_chrono.so
    ${BOOST_LIBRARYDIR}/libboost_date_time.so
    ${BOOST_LIBRARYDIR}/libboost_locale.so
    ${BOOST_LIBRARYDIR}/libboost_serialization.so
    ${BOOST_LIBRARYDIR}/libboost_math_tr1.so
    ${BOOST_LIBRARYDIR}/libboost_regex.so
    ${BOOST_LIBRARYDIR}/libboost_program_options.so
    ${BOOST_LIBRARYDIR}/libboost_atomic.so
)

FIND_PACKAGE(PythonInterp 2)
FIND_PACKAGE(PythonLibs 2)
INCLUDE_DIRECTORIES(${PYTHON_INCLUDE_PATH})

# ---- 单元测试 ----
enable_testing()

add_executable(test_moments tests/test_moments.cpp)
target_link_libraries(test_moments ${BOOST_LIBRARYDIR}/libboost_unit_test_framework.so)
add_test(NAME moments COMMAND test_moments)

add_executable(test_time_utils tests/test_time_utils.cpp)
target_link_libraries(test_time_utils ${BOOST_LIBRARYDIR}/libboost_unit_test_framework.so)
add_test(NAME time_utils COMMAND test_time_utils)

add_executable(test_bucket tests/test_bucket.cpp)
target_link_libraries(test_bucket ${BOOST_LIBRARYDIR}/libboost_unit_test_framework.so)
add_test(NAME bucket COMMAND test_bucket)

add_executable(test_order_state tests/test_order_state.cpp src/order_state.cpp)
target_link_libraries(test_order_state ${BOOST_LIBRARYDIR}/libboost_unit_test_framework.so)
add_test(NAME order_state COMMAND test_order_state)

# ---- 主程序 ----
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

add_executable(realtime_factors src/main.cpp src/ts_engine.cpp src/order_state.cpp)
target_link_libraries(realtime_factors journal paged -lpthread python2.7 ${Boost_LIBRARIES})
