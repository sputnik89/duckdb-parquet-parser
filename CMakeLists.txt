cmake_minimum_required(VERSION 3.16)
project(parquet_parser LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(client
    src/client.cpp
    src/thrift.cpp
    src/metadata.cpp
    src/column_info.cpp
    src/column_reader.cpp
    src/parquet_reader.cpp
)
target_include_directories(parser PRIVATE include)

add_executable(index_test
    tests/index.cpp
    src/thrift.cpp
    src/metadata.cpp
    src/column_info.cpp
    src/column_reader.cpp
    src/parquet_reader.cpp
)
target_include_directories(index_test PRIVATE include)
