#pragma once
#include "column_info.hpp"
#include "column_reader.hpp"
#include "metadata.hpp"
#include <fstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

struct PageIndexEntry {
    size_t data_offset;    // file offset where the page data starts (after header)
    size_t data_size;      // compressed_page_size (raw data length)
    size_t row_group_idx;  // which row group
    size_t column_idx;     // which column (leaf column index)
};

struct RawPage {
    size_t page_id;
    size_t row_group_idx;
    size_t column_idx;
    std::vector<uint8_t> data;
};

class ParquetReader;

class StringColumnIterator {
public:
    bool has_next() const;
    std::pair<size_t, const char*> next();

private:
    friend class ParquetReader;
    StringColumnIterator(ParquetReader& reader, size_t col_idx);

    bool decode_next_page();
    void init_row_group();
    static uint8_t bit_width(int16_t max_level);

    ParquetReader& reader_;
    size_t col_idx_;

    size_t rg_idx_;
    size_t num_row_groups_;

    size_t cur_offset_;
    int64_t values_read_;
    int64_t total_values_;

    bool has_dict_;
    std::vector<std::string> dictionary_;

    std::vector<std::string> page_strings_;
    size_t string_idx_;

    int16_t max_def_level_;
    int16_t max_rep_level_;
};

class PageIterator {
public:
    PageIterator(ParquetReader& reader, size_t start, size_t end);

    bool has_next() const;
    RawPage next();
    void reset();

private:
    ParquetReader& reader_;
    size_t start_;
    size_t end_;
    size_t current_;
};

class ParquetReader {
public:
    ~ParquetReader();

    bool open(const std::string& filename);

    // ── Schema inspection ────────────────────────────────────────────────────

    size_t num_columns() const;
    int64_t num_rows() const;
    size_t num_row_groups() const;
    std::vector<std::string> column_names() const;
    const ColumnInfo& column(size_t col_idx) const;
    const ColumnInfo& column(const std::string& name) const;
    int find_column(const std::string& name) const;
    std::string schema_string() const;

    // ── Column reading ───────────────────────────────────────────────────────

    std::vector<Value> read_column(const std::string& col_name, size_t row_group_idx);
    std::vector<Value> read_column(const std::string& col_name);
    std::vector<Value> read_column_by_idx(int row_group_idx, int col_idx);

    // ── String column iteration ─────────────────────────────────────────────

    StringColumnIterator column_iterator(const std::string& col_name);

    // ── Raw page data API ────────────────────────────────────────────────────

    size_t num_pages() const;
    std::vector<uint8_t> read_page_data(size_t global_page_id) const;
    const PageIndexEntry& page_index_entry(size_t global_page_id) const;
    std::vector<uint8_t> read_pages_chunk(size_t start_page_id, size_t end_page_id,
                                           size_t max_bytes) const;
    PageIterator page_iterator();
    PageIterator page_iterator(size_t start_page_id, size_t end_page_id);

    // ── Accessors ────────────────────────────────────────────────────────────

    const FileMetaData& metadata() const;
    const std::vector<ColumnInfo>& columns() const;
    size_t file_size() const;
    std::vector<uint8_t> read_range(size_t offset, size_t length);

private:
    void build_column_index();
    void build_column_info();
    void build_page_index();
    void build_columns_recursive(int schema_idx, int schema_end,
                                  int16_t def_level, int16_t rep_level,
                                  int& col_index);
    int skip_schema_subtree(int idx);

    std::ifstream file_;
    size_t file_size_ = 0;
    FileMetaData metadata_;
    std::vector<ColumnInfo> columns_;
    std::unordered_map<std::string, size_t> column_name_to_idx_;
    std::vector<PageIndexEntry> page_index_;
};
