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

    void deserialize(ThriftReader& reader) {
        while (true) {
            auto fh = reader.read_field_begin();
            if (fh.type == ThriftCompactType::CT_STOP) break;
            switch (fh.field_id) {
                case 1: type = static_cast<ParquetType>(reader.read_i32()); break;
                case 2: type_length = reader.read_i32(); break;
                case 3: repetition_type = static_cast<FieldRepetitionType>(reader.read_i32()); break;
                case 4: name = reader.read_string(); break;
                case 5: num_children = reader.read_i32(); break;
                case 6: converted_type = static_cast<ConvertedType>(reader.read_i32()); break;
                case 7: scale = reader.read_i32(); break;
                case 8: precision = reader.read_i32(); break;
                case 9: field_id = reader.read_i32(); break;
                default: reader.skip(fh.type); break;
            }
        }
    }
};

// ── Statistics (simplified, just skip) ─────────────────────────────────────────

struct Statistics {
    void deserialize(ThriftReader& reader) {
        while (true) {
            auto fh = reader.read_field_begin();
            if (fh.type == ThriftCompactType::CT_STOP) break;
            reader.skip(fh.type);
        }
    }
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

    void deserialize(ThriftReader& reader) {
        while (true) {
            auto fh = reader.read_field_begin();
            if (fh.type == ThriftCompactType::CT_STOP) break;
            switch (fh.field_id) {
                case 1: type = static_cast<ParquetType>(reader.read_i32()); break;
                case 2: {
                    auto lh = reader.read_list_begin();
                    for (int32_t i = 0; i < lh.count; i++)
                        encodings.push_back(static_cast<Encoding>(reader.read_i32()));
                    break;
                }
                case 3: {
                    auto lh = reader.read_list_begin();
                    for (int32_t i = 0; i < lh.count; i++)
                        path_in_schema.push_back(reader.read_string());
                    break;
                }
                case 4: codec = static_cast<CompressionCodec>(reader.read_i32()); break;
                case 5: num_values = reader.read_i64(); break;
                case 6: total_uncompressed_size = reader.read_i64(); break;
                case 7: total_compressed_size = reader.read_i64(); break;
                case 9: data_page_offset = reader.read_i64(); break;
                case 10: index_page_offset = reader.read_i64(); break;
                case 11: dictionary_page_offset = reader.read_i64(); break;
                default: reader.skip(fh.type); break;
            }
        }
    }
};

// ── ColumnChunk ────────────────────────────────────────────────────────────────

struct ColumnChunk {
    std::optional<std::string> file_path;
    int64_t file_offset = 0;
    std::optional<ColumnMetaData> meta_data;

    void deserialize(ThriftReader& reader) {
        while (true) {
            auto fh = reader.read_field_begin();
            if (fh.type == ThriftCompactType::CT_STOP) break;
            switch (fh.field_id) {
                case 1: file_path = reader.read_string(); break;
                case 2: file_offset = reader.read_i64(); break;
                case 3: {
                    reader.read_struct_begin();
                    ColumnMetaData cmd;
                    cmd.deserialize(reader);
                    meta_data = std::move(cmd);
                    reader.read_struct_end();
                    break;
                }
                default: reader.skip(fh.type); break;
            }
        }
    }
};

// ── DataPageHeader ─────────────────────────────────────────────────────────────

struct DataPageHeader {
    int32_t num_values = 0;
    Encoding encoding = Encoding::PLAIN;
    Encoding definition_level_encoding = Encoding::RLE;
    Encoding repetition_level_encoding = Encoding::RLE;

    void deserialize(ThriftReader& reader) {
        while (true) {
            auto fh = reader.read_field_begin();
            if (fh.type == ThriftCompactType::CT_STOP) break;
            switch (fh.field_id) {
                case 1: num_values = reader.read_i32(); break;
                case 2: encoding = static_cast<Encoding>(reader.read_i32()); break;
                case 3: definition_level_encoding = static_cast<Encoding>(reader.read_i32()); break;
                case 4: repetition_level_encoding = static_cast<Encoding>(reader.read_i32()); break;
                default: reader.skip(fh.type); break;
            }
        }
    }
};

// ── DictionaryPageHeader ───────────────────────────────────────────────────────

struct DictionaryPageHeader {
    int32_t num_values = 0;
    Encoding encoding = Encoding::PLAIN_DICTIONARY;
    bool is_sorted = false;

    void deserialize(ThriftReader& reader) {
        while (true) {
            auto fh = reader.read_field_begin();
            if (fh.type == ThriftCompactType::CT_STOP) break;
            switch (fh.field_id) {
                case 1: num_values = reader.read_i32(); break;
                case 2: encoding = static_cast<Encoding>(reader.read_i32()); break;
                case 3: is_sorted = reader.read_bool(fh.type); break;
                default: reader.skip(fh.type); break;
            }
        }
    }
};

// ── PageHeader ─────────────────────────────────────────────────────────────────

struct PageHeader {
    PageType type = PageType::DATA_PAGE;
    int32_t uncompressed_page_size = 0;
    int32_t compressed_page_size = 0;
    std::optional<int32_t> crc;
    std::optional<DataPageHeader> data_page_header;
    std::optional<DictionaryPageHeader> dictionary_page_header;

    void deserialize(ThriftReader& reader) {
        while (true) {
            auto fh = reader.read_field_begin();
            if (fh.type == ThriftCompactType::CT_STOP) break;
            switch (fh.field_id) {
                case 1: type = static_cast<PageType>(reader.read_i32()); break;
                case 2: uncompressed_page_size = reader.read_i32(); break;
                case 3: compressed_page_size = reader.read_i32(); break;
                case 4: crc = reader.read_i32(); break;
                case 5: {
                    reader.read_struct_begin();
                    DataPageHeader dph;
                    dph.deserialize(reader);
                    data_page_header = std::move(dph);
                    reader.read_struct_end();
                    break;
                }
                case 6:
                    reader.skip(fh.type);
                    break;
                case 7: {
                    reader.read_struct_begin();
                    DictionaryPageHeader dph;
                    dph.deserialize(reader);
                    dictionary_page_header = std::move(dph);
                    reader.read_struct_end();
                    break;
                }
                case 8:
                    reader.skip(fh.type);
                    break;
                default: reader.skip(fh.type); break;
            }
        }
    }
};

// ── RowGroup ───────────────────────────────────────────────────────────────────

struct RowGroup {
    std::vector<ColumnChunk> columns;
    int64_t total_byte_size = 0;
    int64_t num_rows = 0;

    void deserialize(ThriftReader& reader) {
        while (true) {
            auto fh = reader.read_field_begin();
            if (fh.type == ThriftCompactType::CT_STOP) break;
            switch (fh.field_id) {
                case 1: {
                    auto lh = reader.read_list_begin();
                    for (int32_t i = 0; i < lh.count; i++) {
                        reader.read_struct_begin();
                        ColumnChunk cc;
                        cc.deserialize(reader);
                        columns.push_back(std::move(cc));
                        reader.read_struct_end();
                    }
                    break;
                }
                case 2: total_byte_size = reader.read_i64(); break;
                case 3: num_rows = reader.read_i64(); break;
                default: reader.skip(fh.type); break;
            }
        }
    }
};

// ── KeyValue ───────────────────────────────────────────────────────────────────

struct KeyValue {
    std::string key;
    std::optional<std::string> value;

    void deserialize(ThriftReader& reader) {
        while (true) {
            auto fh = reader.read_field_begin();
            if (fh.type == ThriftCompactType::CT_STOP) break;
            switch (fh.field_id) {
                case 1: key = reader.read_string(); break;
                case 2: value = reader.read_string(); break;
                default: reader.skip(fh.type); break;
            }
        }
    }
};

// ── FileMetaData ───────────────────────────────────────────────────────────────

struct FileMetaData {
    int32_t version = 0;
    std::vector<SchemaElement> schema;
    int64_t num_rows = 0;
    std::vector<RowGroup> row_groups;
    std::vector<KeyValue> key_value_metadata;
    std::optional<std::string> created_by;

    void deserialize(ThriftReader& reader) {
        while (true) {
            auto fh = reader.read_field_begin();
            if (fh.type == ThriftCompactType::CT_STOP) break;
            switch (fh.field_id) {
                case 1: version = reader.read_i32(); break;
                case 2: {
                    auto lh = reader.read_list_begin();
                    for (int32_t i = 0; i < lh.count; i++) {
                        reader.read_struct_begin();
                        SchemaElement se;
                        se.deserialize(reader);
                        schema.push_back(std::move(se));
                        reader.read_struct_end();
                    }
                    break;
                }
                case 3: num_rows = reader.read_i64(); break;
                case 4: {
                    auto lh = reader.read_list_begin();
                    for (int32_t i = 0; i < lh.count; i++) {
                        reader.read_struct_begin();
                        RowGroup rg;
                        rg.deserialize(reader);
                        row_groups.push_back(std::move(rg));
                        reader.read_struct_end();
                    }
                    break;
                }
                case 5: {
                    auto lh = reader.read_list_begin();
                    for (int32_t i = 0; i < lh.count; i++) {
                        reader.read_struct_begin();
                        KeyValue kv;
                        kv.deserialize(reader);
                        key_value_metadata.push_back(std::move(kv));
                        reader.read_struct_end();
                    }
                    break;
                }
                case 6: created_by = reader.read_string(); break;
                default: reader.skip(fh.type); break;
            }
        }
    }
};
