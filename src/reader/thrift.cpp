#include "reader/thrift.hpp"

ThriftReader::ThriftReader(const uint8_t* data, size_t size)
    : buf_(data, size), last_field_id_(0) {}

ThriftReader::FieldHeader ThriftReader::read_field_begin() {
    uint8_t byte = buf_.read_byte();
    if (byte == ThriftCompactType::CT_STOP) {
        return {0, ThriftCompactType::CT_STOP};
    }
    uint8_t type = byte & 0x0F;
    int16_t delta = (byte >> 4) & 0x0F;
    int16_t field_id;
    if (delta != 0) {
        field_id = last_field_id_ + delta;
    } else {
        field_id = static_cast<int16_t>(buf_.read_zigzag());
    }
    last_field_id_ = field_id;
    return {field_id, type};
}

bool ThriftReader::read_bool(uint8_t type_from_header) {
    return type_from_header == ThriftCompactType::CT_BOOLEAN_TRUE;
}

int8_t ThriftReader::read_i8() { return static_cast<int8_t>(buf_.read_byte()); }
int16_t ThriftReader::read_i16() { return static_cast<int16_t>(buf_.read_zigzag()); }
int32_t ThriftReader::read_i32() { return static_cast<int32_t>(buf_.read_zigzag()); }
int64_t ThriftReader::read_i64() { return static_cast<int64_t>(buf_.read_zigzag()); }

double ThriftReader::read_double() {
    return buf_.read<double>();
}

std::string ThriftReader::read_string() {
    uint32_t len = static_cast<uint32_t>(buf_.read_varint());
    const uint8_t* ptr = buf_.read_bytes(len);
    return std::string(reinterpret_cast<const char*>(ptr), len);
}

std::string ThriftReader::read_binary() { return read_string(); }

ThriftReader::ListHeader ThriftReader::read_list_begin() {
    uint8_t byte = buf_.read_byte();
    uint8_t size_nibble = (byte >> 4) & 0x0F;
    uint8_t elem_type = byte & 0x0F;
    int32_t count;
    if (size_nibble == 0x0F) {
        count = static_cast<int32_t>(buf_.read_varint());
    } else {
        count = size_nibble;
    }
    return {elem_type, count};
}

void ThriftReader::read_struct_begin() {
    field_id_stack_.push(last_field_id_);
    last_field_id_ = 0;
}

void ThriftReader::read_struct_end() {
    last_field_id_ = field_id_stack_.top();
    field_id_stack_.pop();
}

void ThriftReader::skip(uint8_t type) {
    using namespace ThriftCompactType;
    switch (type) {
        case CT_BOOLEAN_TRUE:
        case CT_BOOLEAN_FALSE:
            break;
        case CT_I8:
            buf_.read_byte();
            break;
        case CT_I16:
        case CT_I32:
        case CT_I64:
            buf_.read_varint();
            break;
        case CT_DOUBLE:
            buf_.read_bytes(8);
            break;
        case CT_BINARY:
            read_string();
            break;
        case CT_LIST:
        case CT_SET: {
            auto lh = read_list_begin();
            for (int32_t i = 0; i < lh.count; i++) skip(lh.elem_type);
            break;
        }
        case CT_MAP: {
            int32_t count = static_cast<int32_t>(buf_.read_varint());
            if (count > 0) {
                uint8_t kv_byte = buf_.read_byte();
                uint8_t key_type = (kv_byte >> 4) & 0x0F;
                uint8_t val_type = kv_byte & 0x0F;
                for (int32_t i = 0; i < count; i++) {
                    skip(key_type);
                    skip(val_type);
                }
            }
            break;
        }
        case CT_STRUCT: {
            read_struct_begin();
            while (true) {
                auto fh = read_field_begin();
                if (fh.type == CT_STOP) break;
                skip(fh.type);
            }
            read_struct_end();
            break;
        }
        default:
            throw std::runtime_error("ThriftReader::skip: unknown type " + std::to_string(type));
    }
}

size_t ThriftReader::position() const { return buf_.position(); }
size_t ThriftReader::remaining() const { return buf_.remaining(); }
