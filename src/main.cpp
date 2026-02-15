#include "parquet_reader.hpp"
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

    // File overview
    std::cout << "File: " << argv[1] << " (" << reader.file_size() << " bytes)" << std::endl;
    std::cout << reader.schema_string() << std::endl;

    // Column lookup by name
    const auto& cols = reader.columns();
    if (!cols.empty()) {
        const auto& first = reader.column(0);
        std::cout << "First column: " << first.name << " (type=" << first.type_name();
        if (!first.is_required()) {
            std::cout << ", nullable";
        }
        std::cout << ")" << std::endl;
    }

    // Preview of row values
    std::cout << "\n── Data Preview (first " << PREVIEW_ROWS << " rows) ──\n" << std::endl;

    // Print header row
    for (size_t i = 0; i < reader.num_columns(); i++) {
        if (i > 0) std::cout << " | ";
        std::cout << std::setw(15) << std::left << reader.column(i).name;
    }
    std::cout << std::endl;

    for (size_t i = 0; i < reader.num_columns(); i++) {
        if (i > 0) std::cout << "-+-";
        std::cout << std::string(15, '-');
    }
    std::cout << std::endl;

    // Read all columns from row group 0
    if (reader.num_row_groups() > 0) {
        std::vector<std::vector<Value>> col_data;
        col_data.reserve(reader.num_columns());
        for (size_t i = 0; i < reader.num_columns(); i++) {
            col_data.push_back(reader.read_column(reader.column(i).name, 0));
        }

        size_t rows_to_show = PREVIEW_ROWS;
        if (!col_data.empty() && col_data[0].size() < rows_to_show) {
            rows_to_show = col_data[0].size();
        }

        for (size_t row = 0; row < rows_to_show; row++) {
            for (size_t col = 0; col < col_data.size(); col++) {
                if (col > 0) std::cout << " | ";
                std::string val = col_data[col][row].to_string();
                if (val.size() > 15) val = val.substr(0, 12) + "...";
                std::cout << std::setw(15) << std::left << val;
            }
            std::cout << std::endl;
        }
    }

    // Page index demo
    std::cout << "\n── Page Index ──\n" << std::endl;
    std::cout << "Total data pages: " << reader.num_pages() << std::endl;
    if (reader.num_pages() > 0) {
        const auto& entry = reader.page_index_entry(0);
        std::cout << "Page 0: row_group=" << entry.row_group_idx
                  << ", column=" << entry.column_idx
                  << ", offset=" << entry.data_offset
                  << ", size=" << entry.data_size << std::endl;
        auto page_data = reader.read_page_data(0);
        std::cout << "Page 0 data: " << page_data.size() << " bytes read" << std::endl;
    }

    // Row group summary
    std::cout << "\n── Row Group Summary ──\n" << std::endl;
    for (size_t rg = 0; rg < reader.num_row_groups(); rg++) {
        const auto& row_group = reader.metadata().row_groups[rg];
        std::cout << "Row Group " << rg << ": "
                  << row_group.num_rows << " rows, "
                  << row_group.total_byte_size << " bytes, "
                  << row_group.columns.size() << " column chunks"
                  << std::endl;

        for (size_t cc = 0; cc < row_group.columns.size(); cc++) {
            const auto& chunk = row_group.columns[cc];
            if (!chunk.meta_data.has_value()) continue;
            const auto& meta = chunk.meta_data.value();

            // Column path
            std::string path;
            for (size_t p = 0; p < meta.path_in_schema.size(); p++) {
                if (p > 0) path += ".";
                path += meta.path_in_schema[p];
            }

            std::cout << "  Column " << cc << " [" << path << "]: "
                      << "type=" << parquet_type_name(meta.type)
                      << ", codec=" << compression_name(meta.codec)
                      << ", values=" << meta.num_values
                      << ", compressed=" << meta.total_compressed_size
                      << ", uncompressed=" << meta.total_uncompressed_size
                      << std::endl;

            // Read pages for this column chunk
            try {
                ColumnReader col_reader(
                    [&](size_t off, size_t len) { return reader.read_range(off, len); },
                    chunk, meta.type,
                    reader.column(cc).max_def_level,
                    reader.column(cc).max_rep_level);
                auto pages = col_reader.read_pages();
                for (const auto& page : pages) {
                    std::cout << "    Page " << page.page_num << ": "
                              << "type=" << page_type_name(page.type)
                              << ", values=" << page.num_values
                              << std::endl;
                }
            } catch (const std::exception& ex) {
                std::cout << "    (page scan skipped: " << ex.what() << ")" << std::endl;
            }
        }
    }

    return 0;
}
