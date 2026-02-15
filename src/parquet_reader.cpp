#include "parquet_reader.hpp"
#include <cstring>
#include <iostream>
#include <sstream>

// ── ParquetReader ────────────────────────────────────────────────────────────

ParquetReader::~ParquetReader() {
    if (file_.is_open()) {
        file_.close();
    }
}

bool ParquetReader::open(const std::string& filename) {
    file_.open(filename, std::ios::binary | std::ios::ate);
    if (!file_.is_open()) {
        std::cerr << "Error: cannot open file " << filename << std::endl;
        return false;
    }

    file_size_ = static_cast<size_t>(file_.tellg());
    if (file_size_ < 12) {
        std::cerr << "Error: file too small to be a Parquet file" << std::endl;
        return false;
    }

    // Read first 4 bytes (PAR1 magic)
    auto header = read_range(0, 4);
    if (std::memcmp(header.data(), "PAR1", 4) != 0) {
        std::cerr << "Error: missing PAR1 magic at start" << std::endl;
        return false;
    }

    // Read last 8 bytes (footer length + trailing PAR1)
    auto trailer = read_range(file_size_ - 8, 8);
    if (std::memcmp(trailer.data() + 4, "PAR1", 4) != 0) {
        std::cerr << "Error: missing PAR1 magic at end" << std::endl;
        return false;
    }

    uint32_t footer_length;
    std::memcpy(&footer_length, trailer.data(), 4);

    if (footer_length + 8 > file_size_) {
        std::cerr << "Error: invalid footer length" << std::endl;
        return false;
    }

    // Read and deserialize footer
    size_t footer_offset = file_size_ - 8 - footer_length;
    auto footer_data = read_range(footer_offset, footer_length);
    ThriftReader reader(footer_data.data(), footer_length);
    metadata_.deserialize(reader);

    // Build column info from schema
    build_column_info();
    build_column_index();
    build_page_index();

    return true;
}

// ── Schema inspection ────────────────────────────────────────────────────────

size_t ParquetReader::num_columns() const { return columns_.size(); }
int64_t ParquetReader::num_rows() const { return metadata_.num_rows; }
size_t ParquetReader::num_row_groups() const { return metadata_.row_groups.size(); }

std::vector<std::string> ParquetReader::column_names() const {
    std::vector<std::string> names;
    names.reserve(columns_.size());
    for (const auto& col : columns_) {
        names.push_back(col.name);
    }
    return names;
}

const ColumnInfo& ParquetReader::column(size_t col_idx) const {
    if (col_idx >= columns_.size()) {
        throw std::runtime_error("Column index " + std::to_string(col_idx) + " out of range");
    }
    return columns_[col_idx];
}

const ColumnInfo& ParquetReader::column(const std::string& name) const {
    int idx = find_column(name);
    if (idx < 0) {
        throw std::runtime_error("Column not found: " + name);
    }
    return columns_[static_cast<size_t>(idx)];
}

int ParquetReader::find_column(const std::string& name) const {
    auto it = column_name_to_idx_.find(name);
    if (it == column_name_to_idx_.end()) return -1;
    return static_cast<int>(it->second);
}

std::string ParquetReader::schema_string() const {
    std::ostringstream ss;
    ss << "Schema:\n";
    for (size_t i = 0; i < columns_.size(); i++) {
        const auto& col = columns_[i];
        ss << "  " << i << ": " << col.name
           << " (" << col.type_name();
        if (col.converted_type.has_value() && col.converted_type.value() != ConvertedType::NONE) {
            ss << ", converted=" << col.converted_type_string();
        }
        if (col.repetition.has_value()) {
            switch (col.repetition.value()) {
                case FieldRepetitionType::REQUIRED: ss << ", REQUIRED"; break;
                case FieldRepetitionType::OPTIONAL: ss << ", OPTIONAL"; break;
                case FieldRepetitionType::REPEATED: ss << ", REPEATED"; break;
            }
        }
        ss << ")\n";
    }
    ss << "Rows: " << metadata_.num_rows << "\n";
    ss << "Row groups: " << metadata_.row_groups.size() << "\n";
    return ss.str();
}

// ── Column reading ───────────────────────────────────────────────────────────

std::vector<Value> ParquetReader::read_column(const std::string& col_name, size_t row_group_idx) {
    int col_idx = find_column(col_name);
    if (col_idx < 0) {
        throw std::runtime_error("Column not found: " + col_name);
    }
    return read_column_by_idx(static_cast<int>(row_group_idx), col_idx);
}

std::vector<Value> ParquetReader::read_column(const std::string& col_name) {
    int col_idx = find_column(col_name);
    if (col_idx < 0) {
        throw std::runtime_error("Column not found: " + col_name);
    }
    std::vector<Value> result;
    for (size_t rg = 0; rg < metadata_.row_groups.size(); rg++) {
        auto rg_values = read_column_by_idx(static_cast<int>(rg), col_idx);
        result.insert(result.end(), rg_values.begin(), rg_values.end());
    }
    return result;
}

std::vector<Value> ParquetReader::read_column_by_idx(int row_group_idx, int col_idx) {
    if (row_group_idx < 0 || row_group_idx >= static_cast<int>(metadata_.row_groups.size())) {
        throw std::runtime_error("Invalid row group index");
    }
    if (col_idx < 0 || col_idx >= static_cast<int>(columns_.size())) {
        throw std::runtime_error("Invalid column index");
    }

    const auto& col_info = columns_[col_idx];
    const auto& rg = metadata_.row_groups[row_group_idx];
    const auto& chunk = rg.columns[col_info.column_index];

    auto read_func = [this](size_t offset, size_t length) {
        return this->read_range(offset, length);
    };

    ColumnReader reader(read_func, chunk,
                       col_info.type, col_info.max_def_level, col_info.max_rep_level);
    return reader.read_all();
}

// ── Accessors ────────────────────────────────────────────────────────────────

const FileMetaData& ParquetReader::metadata() const { return metadata_; }
const std::vector<ColumnInfo>& ParquetReader::columns() const { return columns_; }
size_t ParquetReader::file_size() const { return file_size_; }

std::vector<uint8_t> ParquetReader::read_range(size_t offset, size_t length) {
    std::vector<uint8_t> buf(length);
    file_.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    file_.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(length));
    return buf;
}

// ── Raw page data API ────────────────────────────────────────────────────────

size_t ParquetReader::num_pages() const { return page_index_.size(); }

std::vector<uint8_t> ParquetReader::read_page_data(size_t global_page_id) const {
    if (global_page_id >= page_index_.size()) {
        throw std::runtime_error("Global page ID " + std::to_string(global_page_id) + " out of range");
    }
    const auto& entry = page_index_[global_page_id];
    // const_cast needed because read_range is non-const (seeks on ifstream)
    auto& self = const_cast<ParquetReader&>(*this);
    return self.read_range(entry.data_offset, entry.data_size);
}

const PageIndexEntry& ParquetReader::page_index_entry(size_t global_page_id) const {
    if (global_page_id >= page_index_.size()) {
        throw std::runtime_error("Global page ID " + std::to_string(global_page_id) + " out of range");
    }
    return page_index_[global_page_id];
}

// ── Private helpers ──────────────────────────────────────────────────────────

void ParquetReader::build_column_index() {
    column_name_to_idx_.clear();
    for (size_t i = 0; i < columns_.size(); i++) {
        column_name_to_idx_[columns_[i].name] = i;
    }
}

void ParquetReader::build_column_info() {
    columns_.clear();
    if (metadata_.schema.empty()) return;

    int col_index = 0;
    int16_t def_level = 0;
    int16_t rep_level = 0;
    build_columns_recursive(1, static_cast<int>(metadata_.schema.size()),
                            def_level, rep_level, col_index);
}

void ParquetReader::build_columns_recursive(int schema_idx, int schema_end,
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
            int children = elem.num_children.value();
            schema_idx++;
            int child_end = schema_idx;
            int remaining = children;
            int idx = schema_idx;
            while (remaining > 0 && idx < schema_end) {
                remaining--;
                if (metadata_.schema[idx].num_children.has_value() &&
                    metadata_.schema[idx].num_children.value() > 0) {
                    idx = skip_schema_subtree(idx);
                } else {
                    idx++;
                }
            }
            child_end = idx;
            build_columns_recursive(schema_idx, child_end, my_def, my_rep, col_index);
            schema_idx = child_end;
        } else {
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

int ParquetReader::skip_schema_subtree(int idx) {
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

void ParquetReader::build_page_index() {
    page_index_.clear();
    static constexpr size_t HEADER_READ_SIZE = 256;

    for (size_t rg_idx = 0; rg_idx < metadata_.row_groups.size(); rg_idx++) {
        const auto& rg = metadata_.row_groups[rg_idx];
        for (size_t col_idx = 0; col_idx < rg.columns.size(); col_idx++) {
            const auto& chunk = rg.columns[col_idx];
            if (!chunk.meta_data.has_value()) continue;
            const auto& meta = chunk.meta_data.value();

            int64_t offset = meta.data_page_offset;
            if (meta.dictionary_page_offset.has_value()) {
                offset = std::min(offset, *meta.dictionary_page_offset);
            }

            size_t cur_offset = static_cast<size_t>(offset);
            int64_t values_read = 0;

            while (values_read < meta.num_values) {
                auto header_buf = read_range(cur_offset, HEADER_READ_SIZE);
                ThriftReader header_reader(header_buf.data(), header_buf.size());
                PageHeader page_header;
                page_header.deserialize(header_reader);
                size_t header_size = header_reader.position();
                cur_offset += header_size;

                int32_t page_size = page_header.compressed_page_size;

                if (page_header.type == PageType::DATA_PAGE ||
                    page_header.type == PageType::DATA_PAGE_V2) {
                    page_index_.push_back({cur_offset, static_cast<size_t>(page_size),
                                           rg_idx, col_idx});
                    int32_t num_values = 0;
                    if (page_header.type == PageType::DATA_PAGE &&
                        page_header.data_page_header.has_value()) {
                        num_values = page_header.data_page_header->num_values;
                    }
                    values_read += num_values;
                }
                // Dictionary pages and other types: skip without assigning a global ID

                cur_offset += page_size;
            }
        }
    }
}
