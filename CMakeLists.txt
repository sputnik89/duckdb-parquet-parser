cmake_minimum_required(VERSION 3.16)
project(parquet_parser LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(client
    src/client.cpp
    src/reader/thrift.cpp
    src/reader/metadata.cpp
    src/reader/column_info.cpp
    src/reader/column_reader.cpp
    src/reader/parquet_reader.cpp
    src/writer/thrift_writer.cpp
    src/writer/parquet_writer.cpp
)
target_include_directories(client PRIVATE include)