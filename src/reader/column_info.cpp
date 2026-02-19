#include "reader/column_info.hpp"

std::string ColumnInfo::type_name() const { return parquet_type_name(type); }

std::string ColumnInfo::converted_type_string() const {
    if (converted_type.has_value() && converted_type.value() != ConvertedType::NONE) {
        return converted_type_name(converted_type.value());
    }
    return "NONE";
}

bool ColumnInfo::is_required() const {
    return repetition.has_value() && repetition.value() == FieldRepetitionType::REQUIRED;
}

bool ColumnInfo::is_optional() const {
    return repetition.has_value() && repetition.value() == FieldRepetitionType::OPTIONAL;
}

bool ColumnInfo::is_repeated() const {
    return repetition.has_value() && repetition.value() == FieldRepetitionType::REPEATED;
}
