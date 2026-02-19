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

add_executable(inspect
    tests/inspect.cpp
    src/reader/thrift.cpp
    src/reader/metadata.cpp
    src/reader/column_info.cpp
    src/reader/column_reader.cpp
    src/reader/parquet_reader.cpp
)
target_include_directories(inspect PRIVATE include)

add_executable(roundtrip
    tests/roundtrip.cpp
    src/reader/thrift.cpp
    src/reader/metadata.cpp
    src/reader/column_info.cpp
    src/reader/column_reader.cpp
    src/reader/parquet_reader.cpp
    src/writer/thrift_writer.cpp
    src/writer/parquet_writer.cpp
)
target_include_directories(roundtrip PRIVATE include)

add_executable(page_count
    tests/page_count.cpp
    src/reader/thrift.cpp
    src/reader/metadata.cpp
    src/reader/column_info.cpp
    src/reader/column_reader.cpp
    src/reader/parquet_reader.cpp
)
target_include_directories(page_count PRIVATE include)

add_executable(index_test
    tests/index.cpp
    src/reader/thrift.cpp
    src/reader/metadata.cpp
    src/reader/column_info.cpp
    src/reader/column_reader.cpp
    src/reader/parquet_reader.cpp
)
target_include_directories(index_test PRIVATE include)
