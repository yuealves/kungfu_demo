cmake_minimum_required(VERSION 3.18)
project (playback)
set (CMAKE_BUILE_TYPE BUILD)
set(CMAKE_CXX_STANDARD 17)

SET(TOOLCHAIN_DIR "/opt/kungfu/toolchain")

SET(BOOST_INCLUDEDIR "${TOOLCHAIN_DIR}/boost-1.62.0/include")
SET(BOOST_LIBRARYDIR "${TOOLCHAIN_DIR}/boost-1.62.0/lib")

SET(Boost_USE_MULTITHREADED ON)
SET(Boost_USE_STATIC_LIBS OFF)
SET(Boost_USE_STATIC_RUNTIME OFF)

include_directories("/usr/local/include/")

link_directories("/usr/local/lib64")
link_directories("/usr/local/lib")

FIND_PACKAGE(Boost 1.62.0 COMPONENTS locale date_time filesystem system thread serialization math_tr1 python chrono regex program_options REQUIRED)
INCLUDE_DIRECTORIES(${BOOST_INCLUDEDIR})

FIND_PACKAGE(PythonInterp 2)
FIND_PACKAGE(PythonLibs 2)
INCLUDE_DIRECTORIES(${PYTHON_INCLUDE_PATH})
link_directories(${PYTHON_LIBRARIES})

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -O3 -Wall -fPIC")

INCLUDE_DIRECTORIES(/opt/kungfu/master/include)

LINK_DIRECTORIES(/opt/kungfu/master/lib/yijinjing)
#set (EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/../build)

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





