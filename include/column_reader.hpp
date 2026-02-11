#pragma once
#include "metadata.hpp"
#include "rle_decoder.hpp"
#include <algorithm>
#include <cstring>
#include <vector>

struct PageResult {
    int page_num;
    PageType type;
    int32_t num_values;
    std::vector<Value> values; // decoded values for data pages; empty for dict pages
};

class ColumnReader {
public:
    ColumnReader(const uint8_t* file_data, size_t file_size,
                 const ColumnChunk& chunk, ParquetType type,
                 int16_t max_def_level, int16_t max_rep_level)
        : file_data_(file_data), file_size_(file_size),
          type_(type), max_def_level_(max_def_level), max_rep_level_(max_rep_level) {

        if (!chunk.meta_data.has_value()) {
            throw std::runtime_error("ColumnChunk has no metadata");
        }
        meta_ = &chunk.meta_data.value();
        if (meta_->codec != CompressionCodec::UNCOMPRESSED) {
            throw std::runtime_error("Only uncompressed parquet files are supported");
        }
    }

    std::vector<Value> read_all() {
        std::vector<Value> result;

        // Determine starting offset
        int64_t offset = meta_->data_page_offset;
        if (meta_->dictionary_page_offset.has_value()) {
            offset = std::min(offset, *meta_->dictionary_page_offset);
        }

        const uint8_t* col_data = file_data_ + offset;
        size_t col_remaining = file_size_ - offset;
        size_t col_pos = 0;

        int64_t values_read = 0;
        bool has_dict = false;
        std::vector<Value> dictionary;

        while (values_read < meta_->num_values) {
            // Read page header
            ThriftReader header_reader(col_data + col_pos, col_remaining - col_pos);
            PageHeader page_header;
            page_header.deserialize(header_reader);
            size_t header_size = header_reader.position();
            col_pos += header_size;

            const uint8_t* page_data = col_data + col_pos;
            int32_t page_size = page_header.compressed_page_size;

            if (page_header.type == PageType::DICTIONARY_PAGE) {
                dictionary = read_dictionary_page(page_data, page_size,
                    page_header.dictionary_page_header.value());
                has_dict = true;
                col_pos += page_size;
                continue;
            }

            if (page_header.type == PageType::DATA_PAGE) {
                auto& dph = page_header.data_page_header.value();
                auto page_values = read_data_page(page_data, page_size, dph,
                    has_dict ? &dictionary : nullptr);
                result.insert(result.end(), page_values.begin(), page_values.end());
                values_read += dph.num_values;
                col_pos += page_size;
                continue;
            }

            // Skip unknown page types
            col_pos += page_size;
        }

        return result;
    }

    std::vector<PageResult> read_pages() {
        std::vector<PageResult> pages;

        int64_t offset = meta_->data_page_offset;
        if (meta_->dictionary_page_offset.has_value()) {
            offset = std::min(offset, *meta_->dictionary_page_offset);
        }

        const uint8_t* col_data = file_data_ + offset;
        size_t col_remaining = file_size_ - offset;
        size_t col_pos = 0;

        int64_t values_read = 0;
        bool has_dict = false;
        std::vector<Value> dictionary;
        int page_num = 0;

        while (values_read < meta_->num_values) {
            ThriftReader header_reader(col_data + col_pos, col_remaining - col_pos);
            PageHeader page_header;
            page_header.deserialize(header_reader);
            col_pos += header_reader.position();

            const uint8_t* page_data = col_data + col_pos;
            int32_t page_size = page_header.compressed_page_size;

            if (page_header.type == PageType::DICTIONARY_PAGE) {
                dictionary = read_dictionary_page(page_data, page_size,
                    page_header.dictionary_page_header.value());
                has_dict = true;
                pages.push_back({page_num++, PageType::DICTIONARY_PAGE,
                    page_header.dictionary_page_header->num_values, {}});
                col_pos += page_size;
                continue;
            }

            if (page_header.type == PageType::DATA_PAGE) {
                auto& dph = page_header.data_page_header.value();
                auto page_values = read_data_page(page_data, page_size, dph,
                    has_dict ? &dictionary : nullptr);
                values_read += dph.num_values;
                pages.push_back({page_num++, PageType::DATA_PAGE,
                    dph.num_values, std::move(page_values)});
                col_pos += page_size;
                continue;
            }

            col_pos += page_size;
            page_num++;
        }

        return pages;
    }

private:
    std::vector<Value> read_dictionary_page(const uint8_t* data, int32_t size,
                                            const DictionaryPageHeader& header) {
        std::vector<Value> dict;
        dict.reserve(header.num_values);
        ByteBuffer buf(data, size);

        for (int32_t i = 0; i < header.num_values; i++) {
            dict.push_back(read_plain_value(buf));
        }
        return dict;
    }

    std::vector<Value> read_data_page(const uint8_t* data, int32_t size,
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

    Value read_plain_value(ByteBuffer& buf) {
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

    static uint8_t bit_width(int16_t max_level) {
        if (max_level <= 0) return 0;
        uint8_t bw = 0;
        int16_t v = max_level;
        while (v > 0) { bw++; v >>= 1; }
        return bw;
    }

    const uint8_t* file_data_;
    size_t file_size_;
    const ColumnMetaData* meta_;
    ParquetType type_;
    int16_t max_def_level_;
    int16_t max_rep_level_;
};
