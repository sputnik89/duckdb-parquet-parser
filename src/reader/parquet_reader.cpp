#include "reader/parquet_reader.hpp"
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

std::vector<uint8_t> ParquetReader::read_pages_chunk(size_t start_page_id, size_t end_page_id,
                                                      size_t max_bytes) const {
    if (start_page_id >= page_index_.size()) {
        throw std::runtime_error("Start page ID " + std::to_string(start_page_id) + " out of range");
    }
    if (end_page_id >= page_index_.size()) {
        throw std::runtime_error("End page ID " + std::to_string(end_page_id) + " out of range");
    }
    if (start_page_id > end_page_id) {
        throw std::runtime_error("Start page ID must be <= end page ID");
    }

    // Compute total size of all pages in range, capped at max_bytes
    size_t total_size = 0;
    for (size_t i = start_page_id; i <= end_page_id; i++) {
        total_size += page_index_[i].data_size;
        if (total_size >= max_bytes) {
            total_size = max_bytes;
            break;
        }
    }

    std::vector<uint8_t> result;
    result.reserve(total_size);

    auto& self = const_cast<ParquetReader&>(*this);
    for (size_t i = start_page_id; i <= end_page_id; i++) {
        const auto& entry = page_index_[i];
        size_t remaining = max_bytes - result.size();
        if (remaining == 0) break;

        size_t to_read = std::min(entry.data_size, remaining);
        auto page_data = self.read_range(entry.data_offset, to_read);
        result.insert(result.end(), page_data.begin(), page_data.end());
    }

    return result;
}

const PageIndexEntry& ParquetReader::page_index_entry(size_t global_page_id) const {
    if (global_page_id >= page_index_.size()) {
        throw std::runtime_error("Global page ID " + std::to_string(global_page_id) + " out of range");
    }
    return page_index_[global_page_id];
}

// ── Page iterator ────────────────────────────────────────────────────────

PageIterator::PageIterator(ParquetReader& reader, size_t start, size_t end)
    : reader_(reader), start_(start), end_(end), current_(start) {}

bool PageIterator::has_next() const { return current_ < end_; }

RawPage PageIterator::next() {
    if (!has_next()) {
        throw std::runtime_error("PageIterator: no more pages");
    }
    const auto& entry = reader_.page_index_entry(current_);
    RawPage page;
    page.page_id = current_;
    page.row_group_idx = entry.row_group_idx;
    page.column_idx = entry.column_idx;
    page.data = reader_.read_page_data(current_);
    current_++;
    return page;
}

void PageIterator::reset() { current_ = start_; }

PageIterator ParquetReader::page_iterator() {
    return PageIterator(*this, 0, page_index_.size());
}

PageIterator ParquetReader::page_iterator(size_t start_page_id, size_t end_page_id) {
    if (start_page_id > page_index_.size()) {
        throw std::runtime_error("start_page_id out of range");
    }
    if (end_page_id > page_index_.size()) {
        throw std::runtime_error("end_page_id out of range");
    }
    if (start_page_id > end_page_id) {
        throw std::runtime_error("start_page_id must be <= end_page_id");
    }
    return PageIterator(*this, start_page_id, end_page_id);
}

// ── StringColumnIterator ─────────────────────────────────────────────────────

StringColumnIterator ParquetReader::column_iterator(const std::string& col_name) {
    int col_idx = find_column(col_name);
    if (col_idx < 0) {
        throw std::runtime_error("Column not found: " + col_name);
    }
    const auto& col_info = columns_[col_idx];
    if (col_info.type != ParquetType::BYTE_ARRAY) {
        throw std::runtime_error("Column '" + col_name +
            "' is not BYTE_ARRAY (type: " + parquet_type_name(col_info.type) + ")");
    }
    return StringColumnIterator(*this, static_cast<size_t>(col_idx));
}

StringColumnIterator::StringColumnIterator(ParquetReader& reader, size_t col_idx)
    : reader_(reader), col_idx_(col_idx),
      rg_idx_(0), num_row_groups_(reader.num_row_groups()),
      cur_offset_(0), values_read_(0), total_values_(0),
      has_dict_(false), string_idx_(0),
      max_def_level_(reader.columns()[col_idx].max_def_level),
      max_rep_level_(reader.columns()[col_idx].max_rep_level) {
    if (num_row_groups_ > 0) {
        init_row_group();
        decode_next_page();
    }
}

void StringColumnIterator::init_row_group() {
    const auto& rg = reader_.metadata().row_groups[rg_idx_];
    const auto& col_info = reader_.columns()[col_idx_];
    const auto& chunk = rg.columns[col_info.column_index];
    const auto& meta = chunk.meta_data.value();

    int64_t offset = meta.data_page_offset;
    if (meta.dictionary_page_offset.has_value()) {
        offset = std::min(offset, *meta.dictionary_page_offset);
    }

    cur_offset_ = static_cast<size_t>(offset);
    values_read_ = 0;
    total_values_ = meta.num_values;
    has_dict_ = false;
    dictionary_.clear();
}

bool StringColumnIterator::has_next() const {
    return string_idx_ < page_strings_.size();
}

std::pair<size_t, const char*> StringColumnIterator::next() {
    if (!has_next()) {
        throw std::runtime_error("StringColumnIterator: no more strings");
    }

    const auto& str = page_strings_[string_idx_];
    std::pair<size_t, const char*> result{str.size(), str.data()};
    string_idx_++;

    if (string_idx_ >= page_strings_.size()) {
        decode_next_page();
    }

    return result;
}

bool StringColumnIterator::decode_next_page() {
    page_strings_.clear();
    string_idx_ = 0;

    while (page_strings_.empty()) {
        // Advance to next row group if current one is exhausted
        if (values_read_ >= total_values_) {
            rg_idx_++;
            while (rg_idx_ < num_row_groups_) {
                init_row_group();
                if (total_values_ > 0) break;
                rg_idx_++;
            }
            if (rg_idx_ >= num_row_groups_) {
                return false;
            }
        }

        // Read page header
        static constexpr size_t HEADER_READ_SIZE = 256;
        auto header_buf = reader_.read_range(cur_offset_, HEADER_READ_SIZE);
        ThriftReader header_reader(header_buf.data(), header_buf.size());
        PageHeader page_header;
        page_header.deserialize(header_reader);
        size_t header_size = header_reader.position();
        cur_offset_ += header_size;

        int32_t page_size = page_header.compressed_page_size;
        auto page_buf = reader_.read_range(cur_offset_, static_cast<size_t>(page_size));

        if (page_header.type == PageType::DICTIONARY_PAGE) {
            auto& dph = page_header.dictionary_page_header.value();
            ByteBuffer buf(page_buf.data(), page_buf.size());
            dictionary_.clear();
            dictionary_.reserve(dph.num_values);
            for (int32_t i = 0; i < dph.num_values; i++) {
                uint32_t len = buf.read<uint32_t>();
                const uint8_t* ptr = buf.read_bytes(len);
                dictionary_.emplace_back(reinterpret_cast<const char*>(ptr), len);
            }
            has_dict_ = true;
            cur_offset_ += page_size;
            continue;
        }

        if (page_header.type == PageType::DATA_PAGE) {
            auto& dph = page_header.data_page_header.value();
            int32_t num_values = dph.num_values;
            ByteBuffer buf(page_buf.data(), page_buf.size());

            // Read definition levels
            std::vector<int16_t> def_levels(num_values, max_def_level_);
            if (max_def_level_ > 0) {
                uint32_t def_len = buf.read<uint32_t>();
                RleDecoder def_decoder(buf.current(), def_len, bit_width(max_def_level_));
                def_decoder.get_batch(def_levels.data(), num_values);
                buf.read_bytes(def_len);
            }

            // Skip repetition levels
            if (max_rep_level_ > 0) {
                uint32_t rep_len = buf.read<uint32_t>();
                buf.read_bytes(rep_len);
            }

            // Count non-null values
            int32_t num_non_null = 0;
            for (int32_t i = 0; i < num_values; i++) {
                if (def_levels[i] == max_def_level_) num_non_null++;
            }

            bool use_dict = (dph.encoding == Encoding::PLAIN_DICTIONARY ||
                             dph.encoding == Encoding::RLE_DICTIONARY);

            if (use_dict && has_dict_) {
                uint8_t bw = buf.read_byte();
                RleDecoder idx_decoder(buf.current(),
                    static_cast<uint32_t>(buf.remaining()), bw);
                std::vector<int32_t> indices(num_non_null);
                idx_decoder.get_batch(indices.data(), num_non_null);

                int32_t idx_pos = 0;
                for (int32_t i = 0; i < num_values; i++) {
                    if (def_levels[i] == max_def_level_) {
                        int32_t idx = indices[idx_pos++];
                        if (idx >= 0 && idx < static_cast<int32_t>(dictionary_.size())) {
                            page_strings_.push_back(dictionary_[idx]);
                        }
                    }
                }
            } else {
                // PLAIN encoding
                for (int32_t i = 0; i < num_values; i++) {
                    if (def_levels[i] == max_def_level_) {
                        uint32_t len = buf.read<uint32_t>();
                        const uint8_t* ptr = buf.read_bytes(len);
                        page_strings_.emplace_back(
                            reinterpret_cast<const char*>(ptr), len);
                    }
                }
            }

            values_read_ += dph.num_values;
            cur_offset_ += page_size;
            continue;
        }

        // Unknown page type - skip
        cur_offset_ += page_size;
    }

    return true;
}

uint8_t StringColumnIterator::bit_width(int16_t max_level) {
    if (max_level <= 0) return 0;
    uint8_t bw = 0;
    int16_t v = max_level;
    while (v > 0) { bw++; v >>= 1; }
    return bw;
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
