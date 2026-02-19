#include "writer/thrift_writer.hpp"

void ThriftWriter::write_byte(uint8_t b) {
    buf_.push_back(b);
}

void ThriftWriter::write_raw_bytes(const void* data, size_t len) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
    buf_.insert(buf_.end(), p, p + len);
}

void ThriftWriter::write_varint(uint64_t value) {
    while (value >= 0x80) {
        buf_.push_back(static_cast<uint8_t>(value | 0x80));
        value >>= 7;
    }
    buf_.push_back(static_cast<uint8_t>(value));
}

void ThriftWriter::write_zigzag(int64_t value) {
    uint64_t zz = static_cast<uint64_t>((value << 1) ^ (value >> 63));
    write_varint(zz);
}

void ThriftWriter::write_field(int16_t field_id, uint8_t type) {
    int16_t delta = field_id - last_field_id_;
    if (delta > 0 && delta <= 15) {
        write_byte(static_cast<uint8_t>((delta << 4) | type));
    } else {
        write_byte(type);
        write_zigzag(field_id);
    }
    last_field_id_ = field_id;
}

void ThriftWriter::write_bool(int16_t field_id, bool value) {
    uint8_t type = value ? ThriftCompactType::CT_BOOLEAN_TRUE
                         : ThriftCompactType::CT_BOOLEAN_FALSE;
    write_field(field_id, type);
}

void ThriftWriter::write_i32(int16_t field_id, int32_t value) {
    write_field(field_id, ThriftCompactType::CT_I32);
    write_zigzag(value);
}

void ThriftWriter::write_i64(int16_t field_id, int64_t value) {
    write_field(field_id, ThriftCompactType::CT_I64);
    write_zigzag(value);
}

void ThriftWriter::write_string(int16_t field_id, const std::string& value) {
    write_field(field_id, ThriftCompactType::CT_BINARY);
    write_varint(value.size());
    write_raw_bytes(value.data(), value.size());
}

void ThriftWriter::write_list_begin(int16_t field_id, uint8_t elem_type, int32_t count) {
    write_field(field_id, ThriftCompactType::CT_LIST);
    if (count < 15) {
        write_byte(static_cast<uint8_t>((count << 4) | elem_type));
    } else {
        write_byte(static_cast<uint8_t>(0xF0 | elem_type));
        write_varint(static_cast<uint64_t>(count));
    }
}

void ThriftWriter::write_struct_begin(int16_t field_id) {
    write_field(field_id, ThriftCompactType::CT_STRUCT);
    field_id_stack_.push(last_field_id_);
    last_field_id_ = 0;
}

void ThriftWriter::write_struct_end() {
    write_stop();
    last_field_id_ = field_id_stack_.top();
    field_id_stack_.pop();
}

void ThriftWriter::write_stop() {
    write_byte(ThriftCompactType::CT_STOP);
}

void ThriftWriter::write_zigzag_raw(int64_t value) {
    write_zigzag(value);
}

void ThriftWriter::write_varint_raw(uint64_t value) {
    write_varint(value);
}

void ThriftWriter::write_raw(const void* data, size_t len) {
    write_raw_bytes(data, len);
}

void ThriftWriter::push_field_state() {
    field_id_stack_.push(last_field_id_);
    last_field_id_ = 0;
}

void ThriftWriter::pop_field_state() {
    last_field_id_ = field_id_stack_.top();
    field_id_stack_.pop();
}
