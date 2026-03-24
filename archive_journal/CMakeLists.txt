cmake_minimum_required(VERSION 3.18)
project(archive_journal)

# 独立编译时设置全局配置；作为子目录时由父项目提供
if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall -fPIC")

    include_directories("/usr/local/include/")
    link_directories("/usr/local/lib64")
    link_directories("/usr/local/lib")
endif()

# Arrow + Parquet（使用 find_package，cmake config 已安装到 /usr/local/lib64/cmake/）
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(Threads REQUIRED)

add_executable(archive_journal
    src/main.cpp
    src/journal_reader.cpp
    src/conversion.cpp
    src/parquet_writer.cpp
)

target_include_directories(archive_journal PRIVATE src)

target_link_libraries(archive_journal
    Arrow::arrow_shared
    Parquet::parquet_shared
    Threads::Threads
)
