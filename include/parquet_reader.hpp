#pragma once
#include "column_reader.hpp"
#include "metadata.hpp"
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

struct ColumnInfo {
    std::string name;
    ParquetType type;
    int column_index;    // index into row_group.columns
    int16_t max_def_level;
    int16_t max_rep_level;
    std::optional<FieldRepetitionType> repetition;
    std::optional<ConvertedType> converted_type;
};

class ParquetReader {
public:
    bool open(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary | std::ios::ate);
        if (!file.is_open()) {
            std::cerr << "Error: cannot open file " << filename << std::endl;
            return false;
        }

        file_size_ = static_cast<size_t>(file.tellg());
        if (file_size_ < 12) {
            std::cerr << "Error: file too small to be a Parquet file" << std::endl;
            return false;
        }

        file_data_.resize(file_size_);
        file.seekg(0, std::ios::beg);
        file.read(reinterpret_cast<char*>(file_data_.data()), file_size_);
        file.close();

        // Verify magic bytes PAR1 at start and end
        if (std::memcmp(file_data_.data(), "PAR1", 4) != 0) {
            std::cerr << "Error: missing PAR1 magic at start" << std::endl;
            return false;
        }
        if (std::memcmp(file_data_.data() + file_size_ - 4, "PAR1", 4) != 0) {
            std::cerr << "Error: missing PAR1 magic at end" << std::endl;
            return false;
        }

        // Read footer length (4 bytes before trailing PAR1)
        uint32_t footer_length;
        std::memcpy(&footer_length, file_data_.data() + file_size_ - 8, 4);

        if (footer_length + 8 > file_size_) {
            std::cerr << "Error: invalid footer length" << std::endl;
            return false;
        }

        // Deserialize FileMetaData from footer
        size_t footer_offset = file_size_ - 8 - footer_length;
        ThriftReader reader(file_data_.data() + footer_offset, footer_length);
        metadata_.deserialize(reader);

        // Build column info from schema
        build_column_info();

        return true;
    }

    const FileMetaData& get_metadata() const { return metadata_; }
    const std::vector<ColumnInfo>& get_columns() const { return columns_; }
    const uint8_t* get_file_data() const { return file_data_.data(); }
    size_t get_file_size() const { return file_size_; }

    std::vector<Value> read_column(int row_group_idx, int col_idx) {
        if (row_group_idx < 0 || row_group_idx >= static_cast<int>(metadata_.row_groups.size())) {
            throw std::runtime_error("Invalid row group index");
        }
        if (col_idx < 0 || col_idx >= static_cast<int>(columns_.size())) {
            throw std::runtime_error("Invalid column index");
        }

        const auto& col_info = columns_[col_idx];
        const auto& rg = metadata_.row_groups[row_group_idx];
        const auto& chunk = rg.columns[col_info.column_index];

        ColumnReader reader(file_data_.data(), file_size_, chunk,
                           col_info.type, col_info.max_def_level, col_info.max_rep_level);
        return reader.read_all();
    }

    void print_schema() const {
        std::cout << "Schema:" << std::endl;
        for (size_t i = 0; i < columns_.size(); i++) {
            const auto& col = columns_[i];
            std::cout << "  " << i << ": " << col.name
                      << " (" << parquet_type_name(col.type);
            if (col.converted_type.has_value() && col.converted_type.value() != ConvertedType::NONE) {
                std::cout << ", converted=" << static_cast<int>(col.converted_type.value());
            }
            if (col.repetition.has_value()) {
                switch (col.repetition.value()) {
                    case FieldRepetitionType::REQUIRED: std::cout << ", REQUIRED"; break;
                    case FieldRepetitionType::OPTIONAL: std::cout << ", OPTIONAL"; break;
                    case FieldRepetitionType::REPEATED: std::cout << ", REPEATED"; break;
                }
            }
            std::cout << ")" << std::endl;
        }
        std::cout << "Rows: " << metadata_.num_rows << std::endl;
        std::cout << "Row groups: " << metadata_.row_groups.size() << std::endl;
    }

private:
    void build_column_info() {
        // Walk schema tree to find leaf columns and compute max def/rep levels
        // Schema[0] is the root (message), its children follow
        columns_.clear();
        if (metadata_.schema.empty()) return;

        int col_index = 0;
        int16_t def_level = 0;
        int16_t rep_level = 0;
        build_columns_recursive(1, static_cast<int>(metadata_.schema.size()),
                                def_level, rep_level, col_index);
    }

    void build_columns_recursive(int schema_idx, int schema_end,
                                  int16_t def_level, int16_t rep_level,
                                  int& col_index) {
        while (schema_idx < schema_end) {
            const auto& elem = metadata_.schema[schema_idx];
            int16_t my_def = def_level;
            int16_t my_rep = rep_level;

            if (elem.repetition_type.has_value()) {
                if (elem.repetition_type.value() == FieldRepetitionType::OPTIONAL) {
                    my_def++;
                } else if (elem.repetition_type.value() == FieldRepetitionType::REPEATED) {
                    my_def++;
                    my_rep++;
                }
            }

            if (elem.num_children.has_value() && elem.num_children.value() > 0) {
                // Group node — recurse into children
                int children = elem.num_children.value();
                schema_idx++;
                int child_end = schema_idx;
                // Calculate the range of children in the flat schema
                int remaining = children;
                int idx = schema_idx;
                while (remaining > 0 && idx < schema_end) {
                    remaining--;
                    if (metadata_.schema[idx].num_children.has_value() &&
                        metadata_.schema[idx].num_children.value() > 0) {
                        // Skip over this group's descendants
                        idx = skip_schema_subtree(idx);
                    } else {
                        idx++;
                    }
                }
                child_end = idx;
                build_columns_recursive(schema_idx, child_end, my_def, my_rep, col_index);
                schema_idx = child_end;
            } else {
                // Leaf node — this is a column
                ColumnInfo info;
                info.name = elem.name;
                info.type = elem.type.value_or(ParquetType::BYTE_ARRAY);
                info.column_index = col_index++;
                info.max_def_level = my_def;
                info.max_rep_level = my_rep;
                info.repetition = elem.repetition_type;
                info.converted_type = elem.converted_type;
                columns_.push_back(info);
                schema_idx++;
            }
        }
    }

    int skip_schema_subtree(int idx) {
        int children = metadata_.schema[idx].num_children.value_or(0);
        idx++;
        for (int i = 0; i < children; i++) {
            if (metadata_.schema[idx].num_children.has_value() &&
                metadata_.schema[idx].num_children.value() > 0) {
                idx = skip_schema_subtree(idx);
            } else {
                idx++;
            }
        }
        return idx;
    }

    std::vector<uint8_t> file_data_;
    size_t file_size_ = 0;
    FileMetaData metadata_;
    std::vector<ColumnInfo> columns_;
};
