#pragma once
#include "metadata.hpp"
#include "rle_decoder.hpp"
#include <algorithm>
#include <cstring>
#include <functional>
#include <vector>

// Callback type: read_range(offset, length) -> bytes
using ReadRangeFunc = std::function<std::vector<uint8_t>(size_t, size_t)>;

struct PageResult {
    int page_num;
    PageType type;
    int32_t num_values;
    std::vector<Value> values; // decoded values for data pages; empty for dict pages
};

class ColumnReader {
public:
    ColumnReader(ReadRangeFunc read_range,
                 const ColumnChunk& chunk, ParquetType type,
                 int16_t max_def_level, int16_t max_rep_level);

    std::vector<Value> read_all();
    std::vector<PageResult> read_pages();

private:
    std::vector<Value> read_dictionary_page(const uint8_t* data, int32_t size,
                                            const DictionaryPageHeader& header);
    std::vector<Value> read_data_page(const uint8_t* data, int32_t size,
                                      const DataPageHeader& header,
                                      const std::vector<Value>* dictionary);
    Value read_plain_value(ByteBuffer& buf);
    static uint8_t bit_width(int16_t max_level);

    ReadRangeFunc read_range_;
    const ColumnMetaData* meta_;
    ParquetType type_;
    int16_t max_def_level_;
    int16_t max_rep_level_;
};
