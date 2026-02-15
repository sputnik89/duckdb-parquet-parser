#pragma once
#include "common.hpp"
#include <optional>
#include <string>

struct ColumnInfo {
    std::string name;
    ParquetType type;
    int column_index;    // index into row_group.columns
    int16_t max_def_level;
    int16_t max_rep_level;
    std::optional<FieldRepetitionType> repetition;
    std::optional<ConvertedType> converted_type;

    std::string type_name() const;
    std::string converted_type_string() const;
    bool is_required() const;
    bool is_optional() const;
    bool is_repeated() const;
};
