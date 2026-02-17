#include "../include/parquet_reader.hpp"
#include <algorithm>
#include <iostream>
#include <map>
#include <vector>

static constexpr size_t CHUNK_THRESHOLD = 4096; 

struct PageEntry {
    size_t offset;   // starting offset of this page within the chunk
    size_t rg_idx;
    size_t col_idx;
};

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <parquet_file> <column_name>\n";
        return 1;
    }

    ParquetReader reader;
    if (!reader.open(argv[1])) return 1;

    int col_idx = reader.find_column(argv[2]);
    if (col_idx < 0) {
        std::cerr << "Column not found: " << argv[2] << "\n";
        return 1;
    }

    std::map<size_t, std::vector<PageEntry>> chunk_page_map;
    std::vector<uint8_t> chunk_data;
    size_t chunk_id = 0;

    auto itr = reader.page_iterator();
    while (itr.has_next()) {
        RawPage page = itr.next();
        if (page.column_idx != static_cast<size_t>(col_idx)) continue;

        if (chunk_data.size() + page.data.size() > CHUNK_THRESHOLD) {
            std::cout << std::endl;
            chunk_id++;
            chunk_data.clear();
        }
        // std::cout << page.data.size() << ", ";
        size_t offset_in_chunk = chunk_data.size();
        chunk_data.insert(chunk_data.end(), page.data.begin(), page.data.end());
        chunk_page_map[chunk_id].push_back({offset_in_chunk, page.row_group_idx, page.column_idx});
    }

    auto find_page = [&](size_t cid, size_t offset) -> const PageEntry* {
        auto it = chunk_page_map.find(cid);
        if (it == chunk_page_map.end()) return nullptr;
        const auto& pages = it->second;
        size_t lo = 0, hi = pages.size();
        while (lo < hi) {
            size_t mid = (lo + hi) / 2;
            if (pages[mid].offset <= offset) lo = mid + 1;
            else hi = mid;
        }
        if (lo == 0) return nullptr;
        return &pages[lo - 1];
    };

    struct Query {
        size_t chunk_id;
        size_t offset;
    };
    std::vector<Query> queries = {
        {0, 0}, {0, 500}, {0, 1042}, {0, 2000}, {1, 0}, {1, 1100},
    };

    for (const auto& q : queries) {
        const PageEntry* pe = find_page(q.chunk_id, q.offset);
        if (pe) {
            std::cout << "chunk=" << q.chunk_id << " offset=" << q.offset
                      << " -> page(rg=" << pe->rg_idx << ", col=" << pe->col_idx
                      << ", page_offset=" << pe->offset << ")\n";
        } else {
            std::cout << "chunk=" << q.chunk_id << " offset=" << q.offset << " -> not found\n";
        }
    }
}
