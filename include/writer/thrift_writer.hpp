#pragma once
#include "common.hpp"
#include <stack>
#include <vector>

class ThriftWriter {
public:
    ThriftWriter() : last_field_id_(0) {}

    void write_field(int16_t field_id, uint8_t type);
    void write_bool(int16_t field_id, bool value);
    void write_i32(int16_t field_id, int32_t value);
    void write_i64(int16_t field_id, int64_t value);
    void write_string(int16_t field_id, const std::string& value);
    void write_list_begin(int16_t field_id, uint8_t elem_type, int32_t count);
    void write_struct_begin(int16_t field_id);
    void write_struct_end();
    void write_stop();

    // Raw writes for list elements (no field header)
    void write_zigzag_raw(int64_t value);
    void write_varint_raw(uint64_t value);
    void write_raw(const void* data, size_t len);

    // For struct elements inside lists: push/pop field state without writing a field header
    void push_field_state();
    void pop_field_state();

    const uint8_t* data() const { return buf_.data(); }
    size_t size() const { return buf_.size(); }
    const std::vector<uint8_t>& buffer() const { return buf_; }

private:
    void write_varint(uint64_t value);
    void write_zigzag(int64_t value);
    void write_raw_bytes(const void* data, size_t len);
    void write_byte(uint8_t b);

    std::vector<uint8_t> buf_;
    int16_t last_field_id_;
    std::stack<int16_t> field_id_stack_;
};
