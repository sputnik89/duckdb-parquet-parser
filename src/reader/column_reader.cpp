#include "reader/column_reader.hpp"

ColumnReader::ColumnReader(ReadRangeFunc read_range,
                           const ColumnChunk& chunk, ParquetType type,
                           int16_t max_def_level, int16_t max_rep_level)
    : read_range_(std::move(read_range)),
      type_(type), max_def_level_(max_def_level), max_rep_level_(max_rep_level) {

    if (!chunk.meta_data.has_value()) {
        throw std::runtime_error("ColumnChunk has no metadata");
    }
    meta_ = &chunk.meta_data.value();
    if (meta_->codec != CompressionCodec::UNCOMPRESSED) {
        throw std::runtime_error("Only uncompressed parquet files are supported");
    }
}

std::vector<Value> ColumnReader::read_all() {
    std::vector<Value> result;

    // Determine starting offset
    int64_t offset = meta_->data_page_offset;
    if (meta_->dictionary_page_offset.has_value()) {
        offset = std::min(offset, *meta_->dictionary_page_offset);
    }

    size_t cur_offset = static_cast<size_t>(offset);
    int64_t values_read = 0;
    bool has_dict = false;
    std::vector<Value> dictionary;

    while (values_read < meta_->num_values) {
        // Read a small chunk for the page header (Thrift-encoded, typically < 256 bytes)
        static constexpr size_t HEADER_READ_SIZE = 256;
        auto header_buf = read_range_(cur_offset, HEADER_READ_SIZE);
        ThriftReader header_reader(header_buf.data(), header_buf.size());
        PageHeader page_header;
        page_header.deserialize(header_reader);
        size_t header_size = header_reader.position();
        cur_offset += header_size;

        int32_t page_size = page_header.compressed_page_size;

        // Read the page payload
        auto page_buf = read_range_(cur_offset, static_cast<size_t>(page_size));
        const uint8_t* page_data = page_buf.data();

        if (page_header.type == PageType::DICTIONARY_PAGE) {
            dictionary = read_dictionary_page(page_data, page_size,
                page_header.dictionary_page_header.value());
            has_dict = true;
            cur_offset += page_size;
            continue;
        }

        if (page_header.type == PageType::DATA_PAGE) {
            auto& dph = page_header.data_page_header.value();
            auto page_values = read_data_page(page_data, page_size, dph,
                has_dict ? &dictionary : nullptr);
            result.insert(result.end(), page_values.begin(), page_values.end());
            values_read += dph.num_values;
            cur_offset += page_size;
            continue;
        }

        // Skip unknown page types
        cur_offset += page_size;
    }

    return result;
}

std::vector<PageResult> ColumnReader::read_pages() {
    std::vector<PageResult> pages;

    int64_t offset = meta_->data_page_offset;
    if (meta_->dictionary_page_offset.has_value()) {
        offset = std::min(offset, *meta_->dictionary_page_offset);
    }

    size_t cur_offset = static_cast<size_t>(offset);
    int64_t values_read = 0;
    bool has_dict = false;
    std::vector<Value> dictionary;
    int page_num = 0;

    while (values_read < meta_->num_values) {
        static constexpr size_t HEADER_READ_SIZE = 256;
        auto header_buf = read_range_(cur_offset, HEADER_READ_SIZE);
        ThriftReader header_reader(header_buf.data(), header_buf.size());
        PageHeader page_header;
        page_header.deserialize(header_reader);
        cur_offset += header_reader.position();

        int32_t page_size = page_header.compressed_page_size;

        auto page_buf = read_range_(cur_offset, static_cast<size_t>(page_size));
        const uint8_t* page_data = page_buf.data();

        if (page_header.type == PageType::DICTIONARY_PAGE) {
            dictionary = read_dictionary_page(page_data, page_size,
                page_header.dictionary_page_header.value());
            has_dict = true;
            pages.push_back({page_num++, PageType::DICTIONARY_PAGE,
                page_header.dictionary_page_header->num_values, {}});
            cur_offset += page_size;
            continue;
        }

        if (page_header.type == PageType::DATA_PAGE) {
            auto& dph = page_header.data_page_header.value();
            auto page_values = read_data_page(page_data, page_size, dph,
                has_dict ? &dictionary : nullptr);
            values_read += dph.num_values;
            pages.push_back({page_num++, PageType::DATA_PAGE,
                dph.num_values, std::move(page_values)});
            cur_offset += page_size;
            continue;
        }

        cur_offset += page_size;
        page_num++;
    }

    return pages;
}

std::vector<Value> ColumnReader::read_dictionary_page(const uint8_t* data, int32_t size,
                                                       const DictionaryPageHeader& header) {
    std::vector<Value> dict;
    dict.reserve(header.num_values);
    ByteBuffer buf(data, size);

    for (int32_t i = 0; i < header.num_values; i++) {
        dict.push_back(read_plain_value(buf));
    }
    return dict;
}

std::vector<Value> ColumnReader::read_data_page(const uint8_t* data, int32_t size,
                                                 const DataPageHeader& header,
                                                 const std::vector<Value>* dictionary) {
    ByteBuffer buf(data, size);
    int32_t num_values = header.num_values;

    // Read definition levels
    std::vector<int16_t> def_levels(num_values, max_def_level_);
    if (max_def_level_ > 0) {
        uint32_t def_len = buf.read<uint32_t>();
        RleDecoder def_decoder(buf.current(), def_len,
            bit_width(max_def_level_));
        def_decoder.get_batch(def_levels.data(), num_values);
        buf.read_bytes(def_len);
    }

    // Read repetition levels
    std::vector<int16_t> rep_levels(num_values, 0);
    if (max_rep_level_ > 0) {
        uint32_t rep_len = buf.read<uint32_t>();
        RleDecoder rep_decoder(buf.current(), rep_len,
            bit_width(max_rep_level_));
        rep_decoder.get_batch(rep_levels.data(), num_values);
        buf.read_bytes(rep_len);
    }

    // Count non-null values
    int32_t num_non_null = 0;
    for (int32_t i = 0; i < num_values; i++) {
        if (def_levels[i] == max_def_level_) num_non_null++;
    }

    // Decode values
    std::vector<Value> values;
    bool use_dict = (header.encoding == Encoding::PLAIN_DICTIONARY ||
                     header.encoding == Encoding::RLE_DICTIONARY);

    if (use_dict && dictionary) {
        // RLE-encoded dictionary indices with 1-byte bit-width prefix
        uint8_t bw = buf.read_byte();
        RleDecoder idx_decoder(buf.current(), static_cast<uint32_t>(buf.remaining()), bw);
        std::vector<int32_t> indices(num_non_null);
        idx_decoder.get_batch(indices.data(), num_non_null);

        int32_t idx_pos = 0;
        for (int32_t i = 0; i < num_values; i++) {
            if (def_levels[i] < max_def_level_) {
                values.push_back(Value::null());
            } else {
                int32_t idx = indices[idx_pos++];
                if (idx >= 0 && idx < static_cast<int32_t>(dictionary->size())) {
                    values.push_back((*dictionary)[idx]);
                } else {
                    values.push_back(Value::null());
                }
            }
        }
    } else if (type_ == ParquetType::BOOLEAN) {
        // BOOLEAN PLAIN: values are bit-packed, one bit per value
        int32_t bit_idx = 0;
        uint8_t current_byte = 0;
        for (int32_t i = 0; i < num_values; i++) {
            if (def_levels[i] < max_def_level_) {
                values.push_back(Value::null());
            } else {
                if (bit_idx % 8 == 0) {
                    current_byte = buf.read_byte();
                }
                bool val = (current_byte >> (bit_idx % 8)) & 1;
                values.push_back(Value::from_bool(val));
                bit_idx++;
            }
        }
    } else {
        // PLAIN encoding for non-boolean types
        for (int32_t i = 0; i < num_values; i++) {
            if (def_levels[i] < max_def_level_) {
                values.push_back(Value::null());
            } else {
                values.push_back(read_plain_value(buf));
            }
        }
    }

    return values;
}

Value ColumnReader::read_plain_value(ByteBuffer& buf) {
    switch (type_) {
        case ParquetType::BOOLEAN: {
            uint8_t b = buf.read_byte();
            return Value::from_bool(b != 0);
        }
        case ParquetType::INT32: {
            int32_t v = buf.read<int32_t>();
            return Value::from_i32(v);
        }
        case ParquetType::INT64: {
            int64_t v = buf.read<int64_t>();
            return Value::from_i64(v);
        }
        case ParquetType::FLOAT: {
            float v = buf.read<float>();
            return Value::from_float(v);
        }
        case ParquetType::DOUBLE: {
            double v = buf.read<double>();
            return Value::from_double(v);
        }
        case ParquetType::BYTE_ARRAY: {
            uint32_t len = buf.read<uint32_t>();
            const uint8_t* ptr = buf.read_bytes(len);
            return Value::from_string(std::string(reinterpret_cast<const char*>(ptr), len));
        }
        case ParquetType::FIXED_LEN_BYTE_ARRAY: {
            throw std::runtime_error("FIXED_LEN_BYTE_ARRAY not supported without type_length");
        }
        case ParquetType::INT96: {
            const uint8_t* ptr = buf.read_bytes(12);
            int64_t low;
            int32_t high;
            std::memcpy(&low, ptr, 8);
            std::memcpy(&high, ptr + 8, 4);
            return Value::from_string("INT96(" + std::to_string(high) + ":" + std::to_string(low) + ")");
        }
        default:
            throw std::runtime_error("Unsupported type: " + std::to_string(static_cast<int>(type_)));
    }
}

uint8_t ColumnReader::bit_width(int16_t max_level) {
    if (max_level <= 0) return 0;
    uint8_t bw = 0;
    int16_t v = max_level;
    while (v > 0) { bw++; v >>= 1; }
    return bw;
}
