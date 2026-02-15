#pragma once
#include <iostream>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

// ── Parquet Enums ──────────────────────────────────────────────────────────────

enum class ParquetType : int32_t {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    INT96 = 3,
    FLOAT = 4,
    DOUBLE = 5,
    BYTE_ARRAY = 6,
    FIXED_LEN_BYTE_ARRAY = 7
};

enum class Encoding : int32_t {
    PLAIN = 0,
    GROUP_VAR_INT = 1,
    PLAIN_DICTIONARY = 2,
    RLE = 3,
    BIT_PACKED = 4,
    DELTA_BINARY_PACKED = 5,
    DELTA_LENGTH_BYTE_ARRAY = 6,
    DELTA_BYTE_ARRAY = 7,
    RLE_DICTIONARY = 8,
    BYTE_STREAM_SPLIT = 9
};

enum class CompressionCodec : int32_t {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    LZO = 3,
    BROTLI = 4,
    LZ4 = 5,
    ZSTD = 6,
    LZ4_RAW = 7
};

enum class PageType : int32_t {
    DATA_PAGE = 0,
    INDEX_PAGE = 1,
    DICTIONARY_PAGE = 2,
    DATA_PAGE_V2 = 3
};

enum class FieldRepetitionType : int32_t {
    REQUIRED = 0,
    OPTIONAL = 1,
    REPEATED = 2
};

enum class ConvertedType : int32_t {
    NONE = -1,
    UTF8 = 0,
    MAP = 1,
    MAP_KEY_VALUE = 2,
    LIST = 3,
    ENUM = 4,
    DECIMAL = 5,
    DATE = 6,
    TIME_MILLIS = 7,
    TIME_MICROS = 8,
    TIMESTAMP_MILLIS = 9,
    TIMESTAMP_MICROS = 10,
    UINT_8 = 11,
    UINT_16 = 12,
    UINT_32 = 13,
    UINT_64 = 14,
    INT_8 = 15,
    INT_16 = 16,
    INT_32 = 17,
    INT_64 = 18,
    JSON = 19,
    BSON = 20,
    INTERVAL = 21
};

// ── Thrift Compact Protocol Type Constants ─────────────────────────────────────

namespace ThriftCompactType {
    static constexpr uint8_t CT_STOP         = 0x00;
    static constexpr uint8_t CT_BOOLEAN_TRUE = 0x01;
    static constexpr uint8_t CT_BOOLEAN_FALSE= 0x02;
    static constexpr uint8_t CT_I8           = 0x03;
    static constexpr uint8_t CT_I16          = 0x04;
    static constexpr uint8_t CT_I32          = 0x05;
    static constexpr uint8_t CT_I64          = 0x06;
    static constexpr uint8_t CT_DOUBLE       = 0x07;
    static constexpr uint8_t CT_BINARY       = 0x08;
    static constexpr uint8_t CT_LIST         = 0x09;
    static constexpr uint8_t CT_SET          = 0x0A;
    static constexpr uint8_t CT_MAP          = 0x0B;
    static constexpr uint8_t CT_STRUCT       = 0x0C;
}

// ── ByteBuffer ─────────────────────────────────────────────────────────────────

class ByteBuffer {
public:
    ByteBuffer() : data_(nullptr), size_(0), pos_(0) {}
    ByteBuffer(const uint8_t* data, size_t size) : data_(data), size_(size), pos_(0) {}

    template <typename T>
    T read() {
        check(sizeof(T));
        T val;
        std::memcpy(&val, data_ + pos_, sizeof(T));
        pos_ += sizeof(T);
        return val;
    }

    const uint8_t* read_bytes(size_t n) {
        check(n);
        const uint8_t* ptr = data_ + pos_;
        pos_ += n;
        return ptr;
    }

    uint8_t read_byte() {
        check(1);
        return data_[pos_++];
    }

    uint64_t read_varint() {
        uint64_t result = 0;
        int shift = 0;
        while (true) {
            uint8_t b = read_byte();
            result |= uint64_t(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
            if (shift > 63) throw std::runtime_error("varint too long");
        }
        return result;
    }

    int64_t read_zigzag() {
        uint64_t v = read_varint();
        return static_cast<int64_t>((v >> 1) ^ -(v & 1));
    }

    size_t position() const { return pos_; }
    void set_position(size_t p) { pos_ = p; }
    size_t remaining() const { return size_ - pos_; }
    const uint8_t* current() const { return data_ + pos_; }
    const uint8_t* data() const { return data_; }
    size_t size() const { return size_; }

private:
    void check(size_t n) const {
        if (pos_ + n > size_) {
            throw std::runtime_error("ByteBuffer: read beyond end (pos=" +
                std::to_string(pos_) + " need=" + std::to_string(n) +
                " size=" + std::to_string(size_) + ")");
        }
    }

    const uint8_t* data_;
    size_t size_;
    size_t pos_;
};

// ── Value type for column data ─────────────────────────────────────────────────

struct Value {
    bool is_null = true;
    std::variant<bool, int32_t, int64_t, float, double, std::string> data;

    static Value null() { return Value{true, {}}; }
    static Value from_bool(bool v) { return Value{false, v}; }
    static Value from_i32(int32_t v) { return Value{false, v}; }
    static Value from_i64(int64_t v) { return Value{false, v}; }
    static Value from_float(float v) { return Value{false, v}; }
    static Value from_double(double v) { return Value{false, v}; }
    static Value from_string(std::string v) { return Value{false, std::move(v)}; }

    std::string to_string() const {
        if (is_null) return "NULL";
        return std::visit([](auto&& arg) -> std::string {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, bool>)
                return arg ? "true" : "false";
            else if constexpr (std::is_same_v<T, std::string>)
                return arg;
            else
                return std::to_string(arg);
        }, data);
    }
};

// ── Parquet type name helper ───────────────────────────────────────────────────

inline const char* parquet_type_name(ParquetType t) {
    switch (t) {
        case ParquetType::BOOLEAN:              return "BOOLEAN";
        case ParquetType::INT32:                return "INT32";
        case ParquetType::INT64:                return "INT64";
        case ParquetType::INT96:                return "INT96";
        case ParquetType::FLOAT:                return "FLOAT";
        case ParquetType::DOUBLE:               return "DOUBLE";
        case ParquetType::BYTE_ARRAY:           return "BYTE_ARRAY";
        case ParquetType::FIXED_LEN_BYTE_ARRAY: return "FIXED_LEN_BYTE_ARRAY";
        default:                                return "UNKNOWN";
    }
}

inline const char* encoding_name(Encoding e) {
    switch (e) {
        case Encoding::PLAIN:                return "PLAIN";
        case Encoding::GROUP_VAR_INT:        return "GROUP_VAR_INT";
        case Encoding::PLAIN_DICTIONARY:     return "PLAIN_DICTIONARY";
        case Encoding::RLE:                  return "RLE";
        case Encoding::BIT_PACKED:           return "BIT_PACKED";
        case Encoding::DELTA_BINARY_PACKED:  return "DELTA_BINARY_PACKED";
        case Encoding::DELTA_LENGTH_BYTE_ARRAY: return "DELTA_LENGTH_BYTE_ARRAY";
        case Encoding::DELTA_BYTE_ARRAY:     return "DELTA_BYTE_ARRAY";
        case Encoding::RLE_DICTIONARY:       return "RLE_DICTIONARY";
        case Encoding::BYTE_STREAM_SPLIT:    return "BYTE_STREAM_SPLIT";
        default:                             return "UNKNOWN";
    }
}

inline const char* compression_name(CompressionCodec c) {
    switch (c) {
        case CompressionCodec::UNCOMPRESSED: return "UNCOMPRESSED";
        case CompressionCodec::SNAPPY:       return "SNAPPY";
        case CompressionCodec::GZIP:         return "GZIP";
        case CompressionCodec::LZO:          return "LZO";
        case CompressionCodec::BROTLI:       return "BROTLI";
        case CompressionCodec::LZ4:          return "LZ4";
        case CompressionCodec::ZSTD:         return "ZSTD";
        case CompressionCodec::LZ4_RAW:      return "LZ4_RAW";
        default:                             return "UNKNOWN";
    }
}

inline const char* page_type_name(PageType t) {
    switch (t) {
        case PageType::DATA_PAGE:       return "DATA_PAGE";
        case PageType::INDEX_PAGE:      return "INDEX_PAGE";
        case PageType::DICTIONARY_PAGE: return "DICTIONARY_PAGE";
        case PageType::DATA_PAGE_V2:    return "DATA_PAGE_V2";
        default:                        return "UNKNOWN";
    }
}

inline const char* converted_type_name(ConvertedType ct) {
    switch (ct) {
        case ConvertedType::NONE:             return "NONE";
        case ConvertedType::UTF8:             return "UTF8";
        case ConvertedType::MAP:              return "MAP";
        case ConvertedType::MAP_KEY_VALUE:    return "MAP_KEY_VALUE";
        case ConvertedType::LIST:             return "LIST";
        case ConvertedType::ENUM:             return "ENUM";
        case ConvertedType::DECIMAL:          return "DECIMAL";
        case ConvertedType::DATE:             return "DATE";
        case ConvertedType::TIME_MILLIS:      return "TIME_MILLIS";
        case ConvertedType::TIME_MICROS:      return "TIME_MICROS";
        case ConvertedType::TIMESTAMP_MILLIS: return "TIMESTAMP_MILLIS";
        case ConvertedType::TIMESTAMP_MICROS: return "TIMESTAMP_MICROS";
        case ConvertedType::UINT_8:           return "UINT_8";
        case ConvertedType::UINT_16:          return "UINT_16";
        case ConvertedType::UINT_32:          return "UINT_32";
        case ConvertedType::UINT_64:          return "UINT_64";
        case ConvertedType::INT_8:            return "INT_8";
        case ConvertedType::INT_16:           return "INT_16";
        case ConvertedType::INT_32:           return "INT_32";
        case ConvertedType::INT_64:           return "INT_64";
        case ConvertedType::JSON:             return "JSON";
        case ConvertedType::BSON:             return "BSON";
        case ConvertedType::INTERVAL:         return "INTERVAL";
        default:                              return "UNKNOWN";
    }
}
