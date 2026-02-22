#include "reader/parquet_reader.hpp"
#include "writer/parquet_writer.hpp"
#include <cstring>
#include <iostream>

static size_t chunk_size = 4096;
static std::string filepath = "/Users/kaiwenzheng/tpch/sf1/lineitem.parquet";

int main(int argc, char* argv[]) {    
    ParquetReader reader;
    reader.open(filepath);

    StringColumnIterator column_itr = reader.column_iterator("l_comment");
    std::string chunk;
    while (column_itr.has_next()) {
        auto [string_len, string] = column_itr.next();
        if (chunk.size() >= chunk_size) {
            // FIXME: consume this chunk
            chunk.clear();
        } else {
            chunk += std::to_string(string_len) + std::string(string);
        }
    }
    
    return 0;
}
