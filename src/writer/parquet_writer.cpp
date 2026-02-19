#include "writer/parquet_writer.hpp"
#include "writer/rle_bp_encoder.hpp"
#include "writer/thrift_writer.hpp"
#include <cstring>
#include <stdexcept>

static void write_le32(std::ofstream& out, uint32_t val) {
    uint8_t buf[4];
    std::memcpy(buf, &val, 4);
    out.write(reinterpret_cast<const char*>(buf), 4);
}

ParquetWriter::ParquetWriter(const std::string& path, const std::vector<ColumnSpec>& columns)
    : columns_(columns) {
    file_.open(path, std::ios::binary | std::ios::trunc);
    if (!file_.is_open()) {
        throw std::runtime_error("ParquetWriter: cannot open " + path);
    }
    file_.write("PAR1", 4);
}

ParquetWriter::~ParquetWriter() {
    if (!closed_) {
        close();
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

uint8_t ParquetWriter::compute_bit_width(uint32_t max_value) {
    if (max_value == 0) return 1; // minimum 1 bit for dictionary encoding
    uint8_t bw = 0;
    while (max_value > 0) { bw++; max_value >>= 1; }
    return bw;
}

// Estimate the serialized size of a single value for page-splitting purposes.
size_t ParquetWriter::estimate_row_size(const Value& v, ParquetType type) {
    if (v.is_null) return 0;
    switch (type) {
        case ParquetType::BOOLEAN:  return 1;
        case ParquetType::INT32:
        case ParquetType::FLOAT:    return 4;
        case ParquetType::INT64:
        case ParquetType::DOUBLE:   return 8;
        case ParquetType::BYTE_ARRAY: {
            const std::string& s = std::get<std::string>(v.data);
            return 4 + s.size(); // 4-byte length prefix + data
        }
        default: return 0;
    }
}

// Split a column's values into pages, matching DuckDB's approach:
// accumulate estimated_page_size per row, start a new page when >= MAX_UNCOMPRESSED_PAGE_SIZE.
std::vector<ParquetWriter::PageBoundary>
ParquetWriter::compute_page_boundaries(const std::vector<Value>& values,
                                        ParquetType type) const {
    std::vector<PageBoundary> pages;
    if (values.empty()) return pages;

    size_t page_start = 0;
    size_t estimated_size = 0;

    for (size_t i = 0; i < values.size(); i++) {
        estimated_size += estimate_row_size(values[i], type);
        if (estimated_size >= MAX_UNCOMPRESSED_PAGE_SIZE) {
            // End current page at i (inclusive)
            pages.push_back({page_start, i - page_start + 1});
            page_start = i + 1;
            estimated_size = 0;
        }
    }
    // Remaining values form the last page
    if (page_start < values.size()) {
        pages.push_back({page_start, values.size() - page_start});
    }

    return pages;
}

// Page boundaries for dictionary-encoded columns: each value is a compact index.
std::vector<ParquetWriter::PageBoundary>
ParquetWriter::compute_page_boundaries_dict(size_t num_values, uint8_t bit_width) const {
    std::vector<PageBoundary> pages;
    if (num_values == 0) return pages;

    size_t bytes_per_value = std::max(size_t(1), size_t((bit_width + 7) / 8));
    size_t values_per_page = MAX_UNCOMPRESSED_PAGE_SIZE / bytes_per_value;
    if (values_per_page == 0) values_per_page = 1;

    for (size_t offset = 0; offset < num_values; offset += values_per_page) {
        size_t count = std::min(values_per_page, num_values - offset);
        pages.push_back({offset, count});
    }

    return pages;
}

// ── Level Encoding ───────────────────────────────────────────────────────────

// RLE-encode a vector of levels with a given bit_width.
std::vector<uint8_t> ParquetWriter::rle_encode_levels(const std::vector<int16_t>& levels,
                                                       uint8_t bit_width) {
    if (levels.empty() || bit_width == 0) return {};

    std::vector<uint8_t> result;
    uint32_t value_bytes = (bit_width + 7) / 8;

    size_t i = 0;
    while (i < levels.size()) {
        int16_t current = levels[i];
        size_t run_len = 1;
        while (i + run_len < levels.size() && levels[i + run_len] == current) {
            run_len++;
        }

        uint32_t header = static_cast<uint32_t>(run_len) << 1;
        while (header >= 0x80) {
            result.push_back(static_cast<uint8_t>(header | 0x80));
            header >>= 7;
        }
        result.push_back(static_cast<uint8_t>(header));

        uint64_t val = static_cast<uint64_t>(current);
        for (uint32_t b = 0; b < value_bytes; b++) {
            result.push_back(static_cast<uint8_t>(val & 0xFF));
            val >>= 8;
        }

        i += run_len;
    }

    return result;
}

// ── PLAIN Encoding ───────────────────────────────────────────────────────────

// PLAIN-encode non-null values from a pointer+count slice.
std::vector<uint8_t> ParquetWriter::plain_encode_values(const Value* values, size_t count,
                                                         ParquetType type) {
    std::vector<uint8_t> result;

    for (size_t i = 0; i < count; i++) {
        const auto& v = values[i];
        if (v.is_null) continue;

        switch (type) {
            case ParquetType::BOOLEAN: {
                bool b = std::get<bool>(v.data);
                result.push_back(b ? 1 : 0);
                break;
            }
            case ParquetType::INT32: {
                int32_t val = std::get<int32_t>(v.data);
                uint8_t buf[4];
                std::memcpy(buf, &val, 4);
                result.insert(result.end(), buf, buf + 4);
                break;
            }
            case ParquetType::INT64: {
                int64_t val = std::get<int64_t>(v.data);
                uint8_t buf[8];
                std::memcpy(buf, &val, 8);
                result.insert(result.end(), buf, buf + 8);
                break;
            }
            case ParquetType::FLOAT: {
                float val = std::get<float>(v.data);
                uint8_t buf[4];
                std::memcpy(buf, &val, 4);
                result.insert(result.end(), buf, buf + 4);
                break;
            }
            case ParquetType::DOUBLE: {
                double val = std::get<double>(v.data);
                uint8_t buf[8];
                std::memcpy(buf, &val, 8);
                result.insert(result.end(), buf, buf + 8);
                break;
            }
            case ParquetType::BYTE_ARRAY: {
                const std::string& s = std::get<std::string>(v.data);
                uint32_t len = static_cast<uint32_t>(s.size());
                uint8_t len_buf[4];
                std::memcpy(len_buf, &len, 4);
                result.insert(result.end(), len_buf, len_buf + 4);
                result.insert(result.end(), s.begin(), s.end());
                break;
            }
            default:
                throw std::runtime_error("ParquetWriter: unsupported type " +
                    std::to_string(static_cast<int>(type)));
        }
    }

    return result;
}

// Encode a PLAIN data page from a slice of values (pointer + count).
std::vector<uint8_t> ParquetWriter::encode_data_page(const Value* values, size_t count,
                                                      ParquetType type,
                                                      int16_t max_def_level) {
    std::vector<uint8_t> page_payload;

    if (max_def_level > 0) {
        std::vector<int16_t> def_levels;
        def_levels.reserve(count);
        for (size_t i = 0; i < count; i++) {
            def_levels.push_back(values[i].is_null ? 0 : max_def_level);
        }
        uint8_t bit_width = 0;
        int16_t tmp = max_def_level;
        while (tmp > 0) { bit_width++; tmp >>= 1; }

        std::vector<uint8_t> rle_data = rle_encode_levels(def_levels, bit_width);
        uint32_t rle_len = static_cast<uint32_t>(rle_data.size());
        uint8_t len_buf[4];
        std::memcpy(len_buf, &rle_len, 4);
        page_payload.insert(page_payload.end(), len_buf, len_buf + 4);
        page_payload.insert(page_payload.end(), rle_data.begin(), rle_data.end());
    }

    std::vector<uint8_t> value_data = plain_encode_values(values, count, type);
    page_payload.insert(page_payload.end(), value_data.begin(), value_data.end());

    int32_t page_size = static_cast<int32_t>(page_payload.size());
    int32_t num_values = static_cast<int32_t>(count);

    ThriftWriter tw;
    tw.write_i32(1, static_cast<int32_t>(PageType::DATA_PAGE));
    tw.write_i32(2, page_size);
    tw.write_i32(3, page_size);
    tw.write_struct_begin(5);
    {
        tw.write_i32(1, num_values);
        tw.write_i32(2, static_cast<int32_t>(Encoding::PLAIN));
        tw.write_i32(3, static_cast<int32_t>(Encoding::RLE));
        tw.write_i32(4, static_cast<int32_t>(Encoding::RLE));
    }
    tw.write_struct_end();
    tw.write_stop();

    std::vector<uint8_t> result;
    result.reserve(tw.size() + page_payload.size());
    result.insert(result.end(), tw.data(), tw.data() + tw.size());
    result.insert(result.end(), page_payload.begin(), page_payload.end());
    return result;
}

// ── Dictionary Encoding ──────────────────────────────────────────────────────

ParquetWriter::DictionaryResult
ParquetWriter::analyze_column(const std::vector<Value>& values) {
    DictionaryResult result;

    size_t num_non_null = 0;
    for (const auto& v : values) {
        if (v.is_null) continue;
        num_non_null++;

        auto it = result.dict_map.find(v.data);
        if (it == result.dict_map.end()) {
            uint32_t idx = static_cast<uint32_t>(result.dict_values.size());
            result.dict_map[v.data] = idx;
            result.dict_values.push_back(v);
        }
    }

    size_t dict_size = result.dict_values.size();
    // Threshold matching DuckDB: fall back to PLAIN if too many unique values
    if (dict_size == 0 || dict_size > num_non_null / 5) {
        result.dict_values.clear();
        result.dict_map.clear();
        return result;
    }

    result.use_dictionary = true;
    return result;
}

std::vector<uint8_t>
ParquetWriter::encode_dictionary_page(const DictionaryResult& dict, ParquetType type) {
    // PLAIN-encode all dictionary values
    std::vector<uint8_t> payload = plain_encode_values(
        dict.dict_values.data(), dict.dict_values.size(), type);

    int32_t page_size = static_cast<int32_t>(payload.size());
    int32_t num_values = static_cast<int32_t>(dict.dict_values.size());

    ThriftWriter tw;
    tw.write_i32(1, static_cast<int32_t>(PageType::DICTIONARY_PAGE));
    tw.write_i32(2, page_size); // uncompressed_page_size
    tw.write_i32(3, page_size); // compressed_page_size
    tw.write_struct_begin(7);   // dictionary_page_header
    {
        tw.write_i32(1, num_values);
        tw.write_i32(2, static_cast<int32_t>(Encoding::PLAIN_DICTIONARY));
    }
    tw.write_struct_end();
    tw.write_stop();

    std::vector<uint8_t> result;
    result.reserve(tw.size() + payload.size());
    result.insert(result.end(), tw.data(), tw.data() + tw.size());
    result.insert(result.end(), payload.begin(), payload.end());
    return result;
}

std::vector<uint8_t>
ParquetWriter::encode_dict_data_page(const Value* values, size_t count,
                                      const DictionaryResult& dict,
                                      int16_t max_def_level) {
    std::vector<uint8_t> page_payload;

    // Definition levels (same as PLAIN path)
    if (max_def_level > 0) {
        std::vector<int16_t> def_levels;
        def_levels.reserve(count);
        for (size_t i = 0; i < count; i++) {
            def_levels.push_back(values[i].is_null ? 0 : max_def_level);
        }
        uint8_t bw = 0;
        int16_t tmp = max_def_level;
        while (tmp > 0) { bw++; tmp >>= 1; }

        std::vector<uint8_t> rle_data = rle_encode_levels(def_levels, bw);
        uint32_t rle_len = static_cast<uint32_t>(rle_data.size());
        uint8_t len_buf[4];
        std::memcpy(len_buf, &rle_len, 4);
        page_payload.insert(page_payload.end(), len_buf, len_buf + 4);
        page_payload.insert(page_payload.end(), rle_data.begin(), rle_data.end());
    }

    // Dictionary indices: 1-byte bit_width prefix + RLE/BP encoded indices
    uint32_t dict_size = static_cast<uint32_t>(dict.dict_values.size());
    uint8_t bit_width = compute_bit_width(dict_size > 0 ? dict_size - 1 : 0);

    page_payload.push_back(bit_width);

    RleBpEncoder encoder(bit_width);
    for (size_t i = 0; i < count; i++) {
        if (values[i].is_null) continue;
        auto it = dict.dict_map.find(values[i].data);
        encoder.WriteValue(it->second);
    }
    encoder.FinishWrite(page_payload);

    // Page header
    int32_t page_size = static_cast<int32_t>(page_payload.size());
    int32_t num_values = static_cast<int32_t>(count);

    ThriftWriter tw;
    tw.write_i32(1, static_cast<int32_t>(PageType::DATA_PAGE));
    tw.write_i32(2, page_size);
    tw.write_i32(3, page_size);
    tw.write_struct_begin(5);
    {
        tw.write_i32(1, num_values);
        tw.write_i32(2, static_cast<int32_t>(Encoding::RLE_DICTIONARY));
        tw.write_i32(3, static_cast<int32_t>(Encoding::RLE));
        tw.write_i32(4, static_cast<int32_t>(Encoding::RLE));
    }
    tw.write_struct_end();
    tw.write_stop();

    std::vector<uint8_t> result;
    result.reserve(tw.size() + page_payload.size());
    result.insert(result.end(), tw.data(), tw.data() + tw.size());
    result.insert(result.end(), page_payload.begin(), page_payload.end());
    return result;
}

// ── Row Group Writing ────────────────────────────────────────────────────────

void ParquetWriter::write_row_group(const std::vector<std::vector<Value>>& columns) {
    if (closed_) {
        throw std::runtime_error("ParquetWriter: already closed");
    }
    if (columns.size() != columns_.size()) {
        throw std::runtime_error("ParquetWriter: column count mismatch");
    }

    int64_t num_rows = columns.empty() ? 0 : static_cast<int64_t>(columns[0].size());
    RowGroupMeta rg_meta;
    rg_meta.num_rows = num_rows;

    for (size_t c = 0; c < columns.size(); c++) {
        const auto& col_spec = columns_[c];
        const auto& col_values = columns[c];
        int16_t max_def_level = (col_spec.repetition == FieldRepetitionType::OPTIONAL) ? 1 : 0;

        // Analyze column for dictionary encoding
        auto dict = analyze_column(col_values);

        int64_t col_start = static_cast<int64_t>(file_.tellp());

        if (dict.use_dictionary) {
            // Write dictionary page
            int64_t dict_page_offset = col_start;
            auto dict_page = encode_dictionary_page(dict, col_spec.type);
            file_.write(reinterpret_cast<const char*>(dict_page.data()),
                        static_cast<std::streamsize>(dict_page.size()));

            int64_t data_page_start = static_cast<int64_t>(file_.tellp());

            // Compute bit_width for page boundary estimation
            uint32_t dict_size = static_cast<uint32_t>(dict.dict_values.size());
            uint8_t bit_width = compute_bit_width(dict_size > 0 ? dict_size - 1 : 0);

            // Split into data pages using dictionary-aware boundaries
            auto page_boundaries = compute_page_boundaries_dict(col_values.size(), bit_width);

            for (const auto& pb : page_boundaries) {
                auto page = encode_dict_data_page(
                    col_values.data() + pb.offset, pb.count,
                    dict, max_def_level);
                file_.write(reinterpret_cast<const char*>(page.data()),
                            static_cast<std::streamsize>(page.size()));
            }

            int64_t col_end = static_cast<int64_t>(file_.tellp());
            int64_t col_size = col_end - col_start;

            RowGroupMeta::ColumnChunkMeta cm;
            cm.data_page_offset = data_page_start;
            cm.total_uncompressed_size = col_size;
            cm.total_compressed_size = col_size;
            cm.num_values = static_cast<int64_t>(col_values.size());
            cm.dictionary_page_offset = dict_page_offset;
            cm.encoding = Encoding::RLE_DICTIONARY;
            rg_meta.columns.push_back(cm);
        } else {
            // PLAIN encoding (unchanged)
            auto page_boundaries = compute_page_boundaries(col_values, col_spec.type);

            for (const auto& pb : page_boundaries) {
                auto page = encode_data_page(
                    col_values.data() + pb.offset, pb.count,
                    col_spec.type, max_def_level);
                file_.write(reinterpret_cast<const char*>(page.data()),
                            static_cast<std::streamsize>(page.size()));
            }

            int64_t col_end = static_cast<int64_t>(file_.tellp());
            int64_t col_size = col_end - col_start;

            RowGroupMeta::ColumnChunkMeta cm;
            cm.data_page_offset = col_start;
            cm.total_uncompressed_size = col_size;
            cm.total_compressed_size = col_size;
            cm.num_values = static_cast<int64_t>(col_values.size());
            rg_meta.columns.push_back(cm);
        }
    }

    total_rows_ += num_rows;
    row_groups_.push_back(std::move(rg_meta));
}

// ── Footer ───────────────────────────────────────────────────────────────────

void ParquetWriter::close() {
    if (closed_) return;
    closed_ = true;

    int64_t footer_start = static_cast<int64_t>(file_.tellp());

    ThriftWriter tw;

    // field 1: version
    tw.write_i32(1, 2);

    // field 2: schema — list<SchemaElement>
    int32_t schema_count = 1 + static_cast<int32_t>(columns_.size());
    tw.write_list_begin(2, ThriftCompactType::CT_STRUCT, schema_count);
    {
        tw.push_field_state();
        tw.write_string(4, "schema");
        tw.write_i32(5, static_cast<int32_t>(columns_.size()));
        tw.write_stop();
        tw.pop_field_state();

        for (const auto& col : columns_) {
            tw.push_field_state();
            tw.write_i32(1, static_cast<int32_t>(col.type));
            tw.write_i32(3, static_cast<int32_t>(col.repetition));
            tw.write_string(4, col.name);
            if (col.converted_type.has_value() &&
                col.converted_type.value() != ConvertedType::NONE) {
                tw.write_i32(6, static_cast<int32_t>(col.converted_type.value()));
            }
            if (col.scale.has_value()) {
                tw.write_i32(7, col.scale.value());
            }
            if (col.precision.has_value()) {
                tw.write_i32(8, col.precision.value());
            }
            tw.write_stop();
            tw.pop_field_state();
        }
    }

    // field 3: num_rows
    tw.write_i64(3, total_rows_);

    // field 4: row_groups — list<RowGroup>
    tw.write_list_begin(4, ThriftCompactType::CT_STRUCT,
                         static_cast<int32_t>(row_groups_.size()));
    for (const auto& rg : row_groups_) {
        tw.push_field_state();

        tw.write_list_begin(1, ThriftCompactType::CT_STRUCT,
                             static_cast<int32_t>(rg.columns.size()));
        for (size_t c = 0; c < rg.columns.size(); c++) {
            tw.push_field_state();

            const auto& cm = rg.columns[c];
            const auto& col_spec = columns_[c];

            // ColumnChunk.file_offset — use earliest offset in the chunk
            int64_t file_offset = (cm.dictionary_page_offset >= 0)
                ? cm.dictionary_page_offset : cm.data_page_offset;
            tw.write_i64(2, file_offset);

            tw.write_struct_begin(3); // ColumnMetaData
            {
                tw.write_i32(1, static_cast<int32_t>(col_spec.type));

                // field 2: encodings list
                if (cm.encoding == Encoding::RLE_DICTIONARY) {
                    tw.write_list_begin(2, ThriftCompactType::CT_I32, 2);
                    tw.write_zigzag_raw(static_cast<int32_t>(Encoding::PLAIN));
                    tw.write_zigzag_raw(static_cast<int32_t>(Encoding::RLE_DICTIONARY));
                } else {
                    tw.write_list_begin(2, ThriftCompactType::CT_I32, 1);
                    tw.write_zigzag_raw(static_cast<int32_t>(Encoding::PLAIN));
                }

                tw.write_list_begin(3, ThriftCompactType::CT_BINARY, 1);
                tw.write_varint_raw(col_spec.name.size());
                tw.write_raw(col_spec.name.data(), col_spec.name.size());

                tw.write_i32(4, static_cast<int32_t>(CompressionCodec::UNCOMPRESSED));
                tw.write_i64(5, cm.num_values);
                tw.write_i64(6, cm.total_uncompressed_size);
                tw.write_i64(7, cm.total_compressed_size);
                tw.write_i64(9, cm.data_page_offset);

                // field 11: dictionary_page_offset (if dictionary encoding)
                if (cm.dictionary_page_offset >= 0) {
                    tw.write_i64(11, cm.dictionary_page_offset);
                }
            }
            tw.write_struct_end();

            tw.write_stop();
            tw.pop_field_state();
        }

        int64_t rg_total = 0;
        for (const auto& cm : rg.columns) rg_total += cm.total_compressed_size;
        tw.write_i64(2, rg_total);
        tw.write_i64(3, rg.num_rows);

        tw.write_stop();
        tw.pop_field_state();
    }

    tw.write_stop(); // end FileMetaData

    file_.write(reinterpret_cast<const char*>(tw.data()),
                static_cast<std::streamsize>(tw.size()));

    uint32_t footer_len = static_cast<uint32_t>(
        static_cast<int64_t>(file_.tellp()) - footer_start);
    write_le32(file_, footer_len);

    file_.write("PAR1", 4);
    file_.close();
}
