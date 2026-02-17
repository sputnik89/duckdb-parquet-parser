#include "parquet_reader.hpp"
#include "common.hpp"
#include <iomanip>
#include <iostream>
#include <string>

static constexpr size_t PREVIEW_ROWS = 10;

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <parquet_file>" << std::endl;
        return 1;
    }

    ParquetReader reader;
    if (!reader.open(argv[1])) {
        return 1;
    }

    auto itr = reader.page_iterator();
    while (itr.has_next()) {
        RawPage page = itr.next();
        printf("page.data.size()=%lu\n", page.data.size());
    }
    return 0;
}
