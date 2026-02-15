#include "metadata.hpp"

// ── SchemaElement ──────────────────────────────────────────────────────────────

void SchemaElement::deserialize(ThriftReader& reader) {
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

// ── Statistics ─────────────────────────────────────────────────────────────────

void Statistics::deserialize(ThriftReader& reader) {
    while (true) {
        auto fh = reader.read_field_begin();
        if (fh.type == ThriftCompactType::CT_STOP) break;
        reader.skip(fh.type);
    }
}

// ── ColumnMetaData ─────────────────────────────────────────────────────────────

void ColumnMetaData::deserialize(ThriftReader& reader) {
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

// ── ColumnChunk ────────────────────────────────────────────────────────────────

void ColumnChunk::deserialize(ThriftReader& reader) {
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

// ── DataPageHeader ─────────────────────────────────────────────────────────────

void DataPageHeader::deserialize(ThriftReader& reader) {
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

// ── DictionaryPageHeader ───────────────────────────────────────────────────────

void DictionaryPageHeader::deserialize(ThriftReader& reader) {
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

// ── PageHeader ─────────────────────────────────────────────────────────────────

void PageHeader::deserialize(ThriftReader& reader) {
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

// ── RowGroup ───────────────────────────────────────────────────────────────────

void RowGroup::deserialize(ThriftReader& reader) {
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

// ── KeyValue ───────────────────────────────────────────────────────────────────

void KeyValue::deserialize(ThriftReader& reader) {
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

// ── FileMetaData ───────────────────────────────────────────────────────────────

void FileMetaData::deserialize(ThriftReader& reader) {
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
