#pragma once
#include <cstdint>
#include <vector>

class RleBpEncoder {
public:
    explicit RleBpEncoder(uint8_t bit_width)
        : bit_width_(bit_width), byte_width_((bit_width + 7) / 8),
          rle_count_(0), rle_value_(0), bp_count_(0) {}

    void WriteValue(uint32_t value) {
        if (bp_count_ != 0) {
            // Already committed to a bit-packed run
            bp_buffer_[bp_count_++] = value;
            if (bp_count_ == BP_BLOCK_SIZE) {
                FlushBitPacked();
            }
            return;
        }

        if (rle_count_ == 0) {
            // Starting fresh
            rle_value_ = value;
            rle_count_ = 1;
            return;
        }

        if (rle_value_ == value) {
            rle_count_++;
            return;
        }

        // Value differs from current RLE value
        if (rle_count_ >= MINIMUM_RLE_COUNT) {
            // Enough for an RLE run — flush it and start new
            FlushRle();
            rle_value_ = value;
            rle_count_ = 1;
            return;
        }

        // Not enough for RLE — convert to bit-packed
        for (uint32_t i = 0; i < rle_count_; i++) {
            bp_buffer_[bp_count_++] = rle_value_;
        }
        bp_buffer_[bp_count_++] = value;
        rle_count_ = 0;

        if (bp_count_ == BP_BLOCK_SIZE) {
            FlushBitPacked();
        }
    }

    void FinishWrite(std::vector<uint8_t>& output) {
        if (rle_count_ > 0) {
            FlushRle();
        } else if (bp_count_ > 0) {
            FlushBitPackedPartial();
        }
        output.insert(output.end(), result_.begin(), result_.end());
    }

private:
    static constexpr uint32_t MINIMUM_RLE_COUNT = 4;
    static constexpr uint32_t BP_BLOCK_SIZE = 8;

    uint8_t bit_width_;
    uint8_t byte_width_;
    uint32_t rle_count_;
    uint32_t rle_value_;
    uint32_t bp_buffer_[BP_BLOCK_SIZE] = {};
    uint32_t bp_count_;
    std::vector<uint8_t> result_;

    void WriteVarint(uint32_t value) {
        while (value >= 0x80) {
            result_.push_back(static_cast<uint8_t>(value | 0x80));
            value >>= 7;
        }
        result_.push_back(static_cast<uint8_t>(value));
    }

    void FlushRle() {
        WriteVarint(static_cast<uint32_t>(rle_count_) << 1 | 0);
        uint32_t val = rle_value_;
        for (uint8_t i = 0; i < byte_width_; i++) {
            result_.push_back(static_cast<uint8_t>(val & 0xFF));
            val >>= 8;
        }
        rle_count_ = 0;
    }

    void FlushBitPacked() {
        // 1 group of 8 values
        WriteVarint(1u << 1 | 1);
        PackValues(bp_buffer_, BP_BLOCK_SIZE);
        bp_count_ = 0;
    }

    void FlushBitPackedPartial() {
        // Pad remaining with zeros to fill the group
        for (uint32_t i = bp_count_; i < BP_BLOCK_SIZE; i++) {
            bp_buffer_[i] = 0;
        }
        FlushBitPacked();
    }

    void PackValues(const uint32_t* values, uint32_t count) {
        uint32_t total_bits = count * bit_width_;
        uint32_t total_bytes = (total_bits + 7) / 8;
        size_t start = result_.size();
        result_.resize(start + total_bytes, 0);

        uint32_t bit_offset = 0;
        for (uint32_t i = 0; i < count; i++) {
            uint32_t val = values[i];
            for (uint8_t b = 0; b < bit_width_; b++) {
                if (val & (1u << b)) {
                    result_[start + bit_offset / 8] |= (1u << (bit_offset % 8));
                }
                bit_offset++;
            }
        }
    }
};
