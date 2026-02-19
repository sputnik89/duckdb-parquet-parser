#pragma once
#include "thrift.hpp"
#include <optional>
#include <vector>
#include <string>

// ── SchemaElement ──────────────────────────────────────────────────────────────

struct SchemaElement {
    std::optional<ParquetType> type;
    std::optional<int32_t> type_length;
    std::optional<FieldRepetitionType> repetition_type;
    std::string name;
    std::optional<int32_t> num_children;
    std::optional<ConvertedType> converted_type;
    std::optional<int32_t> scale;
    std::optional<int32_t> precision;
    std::optional<int32_t> field_id;

    void deserialize(ThriftReader& reader);
};

// ── Statistics (simplified, just skip) ─────────────────────────────────────────

struct Statistics {
    void deserialize(ThriftReader& reader);
};

// ── ColumnMetaData ─────────────────────────────────────────────────────────────

struct ColumnMetaData {
    ParquetType type = ParquetType::INT32;
    std::vector<Encoding> encodings;
    std::vector<std::string> path_in_schema;
    CompressionCodec codec = CompressionCodec::UNCOMPRESSED;
    int64_t num_values = 0;
    int64_t total_uncompressed_size = 0;
    int64_t total_compressed_size = 0;
    int64_t data_page_offset = 0;
    std::optional<int64_t> index_page_offset;
    std::optional<int64_t> dictionary_page_offset;

    void deserialize(ThriftReader& reader);
};

// ── ColumnChunk ────────────────────────────────────────────────────────────────

struct ColumnChunk {
    std::optional<std::string> file_path;
    int64_t file_offset = 0;
    std::optional<ColumnMetaData> meta_data;

    void deserialize(ThriftReader& reader);
};

// ── DataPageHeader ─────────────────────────────────────────────────────────────

struct DataPageHeader {
    int32_t num_values = 0;
    Encoding encoding = Encoding::PLAIN;
    Encoding definition_level_encoding = Encoding::RLE;
    Encoding repetition_level_encoding = Encoding::RLE;

    void deserialize(ThriftReader& reader);
};

// ── DictionaryPageHeader ───────────────────────────────────────────────────────

struct DictionaryPageHeader {
    int32_t num_values = 0;
    Encoding encoding = Encoding::PLAIN_DICTIONARY;
    bool is_sorted = false;

    void deserialize(ThriftReader& reader);
};

// ── PageHeader ─────────────────────────────────────────────────────────────────

struct PageHeader {
    PageType type = PageType::DATA_PAGE;
    int32_t uncompressed_page_size = 0;
    int32_t compressed_page_size = 0;
    std::optional<int32_t> crc;
    std::optional<DataPageHeader> data_page_header;
    std::optional<DictionaryPageHeader> dictionary_page_header;

    void deserialize(ThriftReader& reader);
};

// ── RowGroup ───────────────────────────────────────────────────────────────────

struct RowGroup {
    std::vector<ColumnChunk> columns;
    int64_t total_byte_size = 0;
    int64_t num_rows = 0;

    void deserialize(ThriftReader& reader);
};

// ── KeyValue ───────────────────────────────────────────────────────────────────

struct KeyValue {
    std::string key;
    std::optional<std::string> value;

    void deserialize(ThriftReader& reader);
};

// ── FileMetaData ───────────────────────────────────────────────────────────────

struct FileMetaData {
    int32_t version = 0;
    std::vector<SchemaElement> schema;
    int64_t num_rows = 0;
    std::vector<RowGroup> row_groups;
    std::vector<KeyValue> key_value_metadata;
    std::optional<std::string> created_by;

    void deserialize(ThriftReader& reader);
};
