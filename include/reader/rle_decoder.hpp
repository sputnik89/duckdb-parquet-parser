#pragma once
#include "common.hpp"
#include <cstring>
#include <vector>

class RleDecoder {
public:
    RleDecoder() : data_(nullptr), size_(0), pos_(0), bit_width_(0),
                   repeat_count_(0), literal_count_(0), current_value_(0),
                   literal_pos_(nullptr), literal_bit_offset_(0) {}

    RleDecoder(const uint8_t* data, uint32_t size, uint8_t bit_width)
        : data_(data), size_(size), pos_(0), bit_width_(bit_width),
          repeat_count_(0), literal_count_(0), current_value_(0),
          literal_pos_(nullptr), literal_bit_offset_(0) {}

    template <typename T>
    void get_batch(T* out, uint32_t count) {
        for (uint32_t i = 0; i < count; i++) {
            if (repeat_count_ == 0 && literal_count_ == 0) {
                if (!next_counts()) {
                    for (; i < count; i++) out[i] = 0;
                    return;
                }
            }
            if (repeat_count_ > 0) {
                out[i] = static_cast<T>(current_value_);
                repeat_count_--;
            } else {
                out[i] = static_cast<T>(read_literal_value());
                literal_count_--;
            }
        }
    }

private:
    bool next_counts() {
        if (pos_ >= size_) return false;
        uint32_t indicator = read_varint32();
        if (indicator & 1) {
            // Literal (bit-packed) run
            uint32_t literal_groups = indicator >> 1;
            literal_count_ = literal_groups * 8;
            literal_pos_ = data_ + pos_;
            literal_bit_offset_ = 0;
            // Don't advance pos_ yet - we'll read bits from literal_pos_
        } else {
            // Repeated run
            repeat_count_ = indicator >> 1;
            current_value_ = read_fixed_width_value();
        }
        return true;
    }

    uint64_t read_literal_value() {
        if (bit_width_ == 0) return 0;
        uint64_t val = 0;
        for (uint8_t i = 0; i < bit_width_; i++) {
            uint32_t byte_idx = literal_bit_offset_ / 8;
            uint32_t bit_idx = literal_bit_offset_ % 8;
            if (literal_pos_[byte_idx] & (1 << bit_idx)) {
                val |= (uint64_t(1) << i);
            }
            literal_bit_offset_++;
        }
        // If we've exhausted all literals, advance pos_ past the bit-packed data
        if (literal_count_ == 1) {
            // This is the last literal value being read
            uint32_t total_bits_consumed = literal_bit_offset_;
            uint32_t bytes_consumed = (total_bits_consumed + 7) / 8;
            pos_ = static_cast<uint32_t>(literal_pos_ - data_) + bytes_consumed;
        }
        return val;
    }

    uint32_t read_varint32() {
        uint32_t result = 0;
        int shift = 0;
        while (pos_ < size_) {
            uint8_t b = data_[pos_++];
            result |= uint32_t(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        return result;
    }

    uint64_t read_fixed_width_value() {
        uint32_t bytes_needed = (bit_width_ + 7) / 8;
        uint64_t val = 0;
        for (uint32_t i = 0; i < bytes_needed && pos_ < size_; i++) {
            val |= uint64_t(data_[pos_++]) << (i * 8);
        }
        return val;
    }

    const uint8_t* data_;
    uint32_t size_;
    uint32_t pos_;
    uint8_t bit_width_;
    uint32_t repeat_count_;
    uint32_t literal_count_;
    uint64_t current_value_;

    // Literal run state
    const uint8_t* literal_pos_;
    uint32_t literal_bit_offset_;
};
