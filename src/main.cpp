#include "parquet_reader.hpp"
#include <iostream>
#include <regex>
#include <string>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <parquet_file> [--regex-column <column> --regex <pattern> [--neg-regex]]" << std::endl;
        return 1;
    }

    std::string regex_column;
    std::string regex_pattern;
    bool neg_regex = false;

    // Parse optional args
    for (int i = 2; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--regex-column" && i + 1 < argc) {
            regex_column = argv[++i];
        } else if (arg == "--regex" && i + 1 < argc) {
            regex_pattern = argv[++i];
        } else if (arg == "--neg-regex") {
            neg_regex = true;
        }
    }

    bool regex_mode = !regex_column.empty() && !regex_pattern.empty();

    ParquetReader reader;
    if (!reader.open(argv[1])) {
        return 1;
    }

    reader.print_schema();
    std::cout << std::endl;

    const auto& columns = reader.get_columns();
    const auto& metadata = reader.get_metadata();
    const uint8_t* file_data = reader.get_file_data();
    size_t file_size = reader.get_file_size();

    if (regex_mode) {
        std::regex re(regex_pattern);

        // Find the target column index
        int target_col = -1;
        for (size_t c = 0; c < columns.size(); c++) {
            if (columns[c].name == regex_column) {
                target_col = static_cast<int>(c);
                break;
            }
        }
        if (target_col < 0) {
            std::cerr << "Column '" << regex_column << "' not found" << std::endl;
            return 1;
        }

        std::cout << "Pages with NO matches for column '" << regex_column
                  << "' with predicate " << (neg_regex ? "NOT LIKE" : "LIKE")
                  << " /" << regex_pattern << "/" << std::endl;
        std::cout << std::endl;

        int total_pages_with_match = 0;
        int total_data_pages = 0;
        for (size_t rg = 0; rg < metadata.row_groups.size(); rg++) {
            const auto& row_group = metadata.row_groups[rg];

            const auto& chunk = row_group.columns[target_col];
            const auto& col = columns[target_col];

            int16_t max_def = col.max_def_level;
            int16_t max_rep = col.max_rep_level;

            ColumnReader col_reader(file_data, file_size, chunk, col.type, max_def, max_rep);
            auto pages = col_reader.read_pages();

            int data_page_idx = 0;
            for (const auto& page : pages) {
                if (page.type == PageType::DICTIONARY_PAGE) continue;

                bool has_match = false;
                for (const auto& val : page.values) {
                    if (val.is_null) continue;
                    bool regex_matches = std::regex_search(val.to_string(), re);
                    // For LIKE: value satisfies predicate if regex matches
                    // For NOT LIKE: value satisfies predicate if regex does NOT match
                    bool satisfies = neg_regex ? !regex_matches : regex_matches;
                    if (satisfies) {
                        has_match = true;
                        break;
                    }
                }

                total_data_pages++;
                if (has_match) {
                    total_pages_with_match++;
                } else {
                    std::printf("  Row Group %zu, Page %d (%zu values)\n",
                        rg, data_page_idx, page.values.size());
                }
                data_page_idx++;
            }
        }
        std::cout << std::endl;
        std::cout << "Total data pages: " << total_data_pages << std::endl;
        std::cout << "Pages with at least one match: " << total_pages_with_match << std::endl;
        std::cout << "Pages with no matches: " << (total_data_pages - total_pages_with_match) << std::endl;
    } else {
        // Existing behavior: print page sizes
        int total_pages = 0;
        for (size_t rg = 0; rg < metadata.row_groups.size(); rg++) {
            const auto& row_group = metadata.row_groups[rg];
            std::cout << "── Row Group " << rg
                      << " (" << row_group.num_rows << " rows) ──" << std::endl;

            for (size_t c = 0; c < row_group.columns.size(); c++) {
                const auto& chunk = row_group.columns[c];
                if (!chunk.meta_data.has_value()) continue;
                const auto& meta = chunk.meta_data.value();

                std::cout << "  Column: ";
                for (size_t p = 0; p < meta.path_in_schema.size(); p++) {
                    if (p > 0) std::cout << ".";
                    std::cout << meta.path_in_schema[p];
                }
                std::cout << std::endl;

                int64_t offset = meta.data_page_offset;
                if (meta.dictionary_page_offset.has_value()) {
                    offset = std::min(offset, *meta.dictionary_page_offset);
                }

                int64_t values_read = 0;
                size_t col_pos = 0;
                const uint8_t* col_data = file_data + offset;
                size_t col_remaining = file_size - offset;
                int page_num = 0;

                while (values_read < meta.num_values) {
                    ThriftReader header_reader(col_data + col_pos, col_remaining - col_pos);
                    PageHeader page_header;
                    page_header.deserialize(header_reader);
                    col_pos += header_reader.position();

                    std::cout << "    Page " << page_num
                              << ": type=" << (page_header.type == PageType::DICTIONARY_PAGE ? "DICTIONARY" : "DATA")
                              << ", uncompressed_page_size=" << page_header.uncompressed_page_size
                              << ", compressed_page_size=" << page_header.compressed_page_size
                              << std::endl;

                    col_pos += page_header.compressed_page_size;
                    page_num++;
                    total_pages++;

                    if (page_header.type == PageType::DATA_PAGE && page_header.data_page_header.has_value()) {
                        values_read += page_header.data_page_header->num_values;
                    } else if (page_header.type == PageType::DICTIONARY_PAGE) {
                        // Dictionary pages don't count toward values_read
                    } else {
                        break;
                    }
                }
            }
            std::cout << std::endl;
        }
        std::cout << "Total pages: " << total_pages << std::endl;
    }

    return 0;
}
