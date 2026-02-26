#include "reader/parquet_reader.hpp"
#include "writer/parquet_writer.hpp"
#include <cstring>
#include <iostream>
#include <vector>

static size_t chunk_size = 4096;
static std::string filepath = "/Users/kaiwenzheng/tpch/sf1/lineitem.parquet";

int main(int argc, char* argv[]) {
    ParquetReader reader;
    reader.open(filepath);

    size_t num_rows = static_cast<size_t>(reader.num_rows());
    std::vector<size_t> tuple_to_chunk(num_rows);

    StringColumnIterator column_itr = reader.column_iterator("l_comment");
    std::string chunk;
    size_t chunk_id = 0;

    while (column_itr.has_next()) {
        auto [pos, string_len, string] = column_itr.next();

        if (chunk.size() >= chunk_size) {
            // FIXME: consume this chunk
            chunk.clear();
            chunk_id++;
        }

        chunk += std::to_string(string_len) + std::string(string, string_len);
        tuple_to_chunk[pos] = chunk_id;
    }

    std::cout << "Total tuples: " << num_rows << std::endl;
    std::cout << "Total chunks: " << chunk_id + 1 << std::endl;

    return 0;
}
