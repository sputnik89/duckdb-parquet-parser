#pragma once
#include "common.hpp"
#include <fstream>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>

struct ColumnSpec {
    std::string name;
    ParquetType type;
    FieldRepetitionType repetition;
    std::optional<ConvertedType> converted_type;
    std::optional<int32_t> scale;
    std::optional<int32_t> precision;
};

struct RowGroupMeta {
    int64_t num_rows;
    struct ColumnChunkMeta {
        int64_t data_page_offset;
        int64_t total_uncompressed_size;
        int64_t total_compressed_size;
        int64_t num_values;
        int64_t dictionary_page_offset = -1;
        Encoding encoding = Encoding::PLAIN;
    };
    std::vector<ColumnChunkMeta> columns;
};

class ParquetWriter {
public:
    // Max uncompressed page size threshold (matching duckdb-dpk)
    static constexpr size_t MAX_UNCOMPRESSED_PAGE_SIZE = 1024;

    ParquetWriter(const std::string& path, const std::vector<ColumnSpec>& columns);
    ~ParquetWriter();

    void write_row_group(const std::vector<std::vector<Value>>& columns);
    void close();

private:
    using ValueData = std::variant<bool, int32_t, int64_t, float, double, std::string>;

    struct DictionaryResult {
        bool use_dictionary = false;
        std::vector<Value> dict_values;         // unique values, index = position
        std::map<ValueData, uint32_t> dict_map; // value data -> index
    };

    struct PageBoundary {
        size_t offset;    // start index into the values vector
        size_t count;     // number of values in this page
    };

    // Page boundary computation
    std::vector<PageBoundary> compute_page_boundaries(const std::vector<Value>& values,
                                                       ParquetType type) const;
    std::vector<PageBoundary> compute_page_boundaries_dict(size_t num_values,
                                                            uint8_t bit_width) const;
    static size_t estimate_row_size(const Value& v, ParquetType type);

    // PLAIN encoding
    std::vector<uint8_t> encode_data_page(const Value* values, size_t count,
                                           ParquetType type,
                                           int16_t max_def_level);
    static std::vector<uint8_t> rle_encode_levels(const std::vector<int16_t>& levels,
                                                   uint8_t bit_width);
    static std::vector<uint8_t> plain_encode_values(const Value* values, size_t count,
                                                     ParquetType type);

    // Dictionary encoding
    DictionaryResult analyze_column(const std::vector<Value>& values);
    std::vector<uint8_t> encode_dictionary_page(const DictionaryResult& dict,
                                                 ParquetType type);
    std::vector<uint8_t> encode_dict_data_page(const Value* values, size_t count,
                                                const DictionaryResult& dict,
                                                int16_t max_def_level);
    static uint8_t compute_bit_width(uint32_t max_value);

    std::ofstream file_;
    std::vector<ColumnSpec> columns_;
    std::vector<RowGroupMeta> row_groups_;
    int64_t total_rows_ = 0;
    bool closed_ = false;
};
