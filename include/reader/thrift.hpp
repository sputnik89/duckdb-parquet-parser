#pragma once
#include "common.hpp"
#include <stack>
#include <string>

class ThriftReader {
public:
    ThriftReader(const uint8_t* data, size_t size);

    struct FieldHeader {
        int16_t field_id;
        uint8_t type;
    };

    FieldHeader read_field_begin();

    bool read_bool(uint8_t type_from_header);
    int8_t read_i8();
    int16_t read_i16();
    int32_t read_i32();
    int64_t read_i64();
    double read_double();
    std::string read_string();
    std::string read_binary();

    struct ListHeader {
        uint8_t elem_type;
        int32_t count;
    };

    ListHeader read_list_begin();
    void read_struct_begin();
    void read_struct_end();
    void skip(uint8_t type);

    size_t position() const;
    size_t remaining() const;

private:
    ByteBuffer buf_;
    int16_t last_field_id_;
    std::stack<int16_t> field_id_stack_;
};
