# duckdb-parquet-parser

A standalone C++17 Parquet file parser that reads Parquet metadata, schema, and page-level details without external dependencies.

## Prerequisites

Requires CMake 3.16+, a C++17 compiler, and [re2](https://github.com/google/re2).

### Installing re2

**macOS (Homebrew):**
```bash
brew install re2
```

**Ubuntu/Debian:**
```bash
sudo apt-get install libre2-dev
```

**From source:**
```bash
git clone https://github.com/google/re2.git
cd re2
mkdir build && cd build
cmake ..
make
sudo make install
```

## Building

```bash
mkdir build && cd build
cmake ..
make
```

This produces two executables in the build directory:

- **`parser`** — the main Parquet inspection tool
- **`index_test`** — a test program that builds a chunked inverted index over a column's pages (see [`tests/README.md`](tests/README.md))

## CLI Usage

### Print schema and page layout

```bash
./build/parser <parquet_file>
```

Prints the file schema, row groups, and page sizes for every column.

### Regex filtering mode

```bash
./build/parser <parquet_file> --regex-column <column> --regex <pattern> [--neg-regex]
```

Scans data pages for a specific column and reports which pages have no values matching the regex pattern.

- `--regex-column` — name of the column to scan
- `--regex` — regex pattern to match against values
- `--neg-regex` — invert the match (acts as NOT LIKE)

### Chunked inverted index test

```bash
./build/index_test <parquet_file> <column_name>
```

Groups a column's data pages into 4 KB chunks and builds an inverted index that maps byte offsets back to source pages. See [`tests/README.md`](tests/README.md) for details.

---

## Library API

The parser can be used as a C++ library by including the headers from `include/`. The main entry point is `ParquetReader` (defined in `parquet_reader.hpp`).

### ParquetReader

```cpp
#include "parquet_reader.hpp"

ParquetReader reader;
reader.open("data.parquet");
```

#### Schema Inspection

| Method | Description |
|--------|-------------|
| `size_t num_columns()` | Number of leaf columns |
| `int64_t num_rows()` | Total row count across all row groups |
| `size_t num_row_groups()` | Number of row groups |
| `std::vector<std::string> column_names()` | Names of all leaf columns |
| `const ColumnInfo& column(size_t idx)` | Column metadata by index |
| `const ColumnInfo& column(const std::string& name)` | Column metadata by name |
| `int find_column(const std::string& name)` | Column index by name, or -1 |
| `std::string schema_string()` | Human-readable schema dump |

#### Reading Decoded Column Data

Returns values decoded from PLAIN and dictionary-encoded pages:

```cpp
// Read an entire column (all row groups)
std::vector<Value> vals = reader.read_column("city");

// Read from a specific row group
std::vector<Value> vals = reader.read_column("city", 0);

// Read by row group and column indices
std::vector<Value> vals = reader.read_column_by_idx(/*row_group=*/0, /*col=*/2);
```

#### Raw Page Data Access

For low-level work with individual data pages:

| Method | Description |
|--------|-------------|
| `size_t num_pages()` | Total page count across all columns/row groups |
| `std::vector<uint8_t> read_page_data(size_t page_id)` | Raw bytes of a single page |
| `const PageIndexEntry& page_index_entry(size_t page_id)` | Offset/size/location metadata for a page |
| `std::vector<uint8_t> read_pages_chunk(size_t start, size_t end, size_t max_bytes)` | Read a contiguous range of pages up to a byte limit |
| `PageIterator page_iterator()` | Iterator over all pages |
| `PageIterator page_iterator(size_t start, size_t end)` | Iterator over a page range |

#### General Accessors

| Method | Description |
|--------|-------------|
| `const FileMetaData& metadata()` | Full Thrift-deserialized file metadata |
| `const std::vector<ColumnInfo>& columns()` | All column descriptors |
| `size_t file_size()` | Size of the Parquet file in bytes |
| `std::vector<uint8_t> read_range(size_t offset, size_t length)` | Read arbitrary bytes from the file |

### PageIterator

Lazy iterator over raw pages. Returned by `ParquetReader::page_iterator()`.

```cpp
auto it = reader.page_iterator();
while (it.has_next()) {
    RawPage page = it.next();
    // page.page_id, page.row_group_idx, page.column_idx, page.data
}
it.reset();  // rewind to beginning
```

### ColumnReader

Lower-level reader that decodes pages from a single column chunk. `ParquetReader::read_column` uses this internally, but it can be used directly:

```cpp
ColumnReader col_reader(
    [&](size_t off, size_t len) { return reader.read_range(off, len); },
    chunk,          // ColumnChunk from metadata
    ParquetType::BYTE_ARRAY,
    /*max_def_level=*/1,
    /*max_rep_level=*/0
);

// Decode all values
std::vector<Value> values = col_reader.read_all();

// Or get per-page results
std::vector<PageResult> pages = col_reader.read_pages();
for (auto& pr : pages) {
    // pr.page_num, pr.type, pr.num_values, pr.values
}
```

### Key Data Types

#### Value

A tagged union representing a single decoded Parquet value:

```cpp
struct Value {
    bool is_null;
    std::variant<bool, int32_t, int64_t, float, double, std::string> data;

    static Value null();
    static Value from_bool(bool v);
    static Value from_i32(int32_t v);
    static Value from_i64(int64_t v);
    static Value from_float(float v);
    static Value from_double(double v);
    static Value from_string(std::string v);

    std::string to_string() const;
};
```

#### ColumnInfo

Describes a leaf column's schema:

```cpp
struct ColumnInfo {
    std::string name;
    ParquetType type;
    int column_index;
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
```

#### PageIndexEntry / RawPage

```cpp
struct PageIndexEntry {
    size_t data_offset;    // file offset of page data (after header)
    size_t data_size;      // compressed page size in bytes
    size_t row_group_idx;
    size_t column_idx;
};

struct RawPage {
    size_t page_id;
    size_t row_group_idx;
    size_t column_idx;
    std::vector<uint8_t> data;
};
```

#### PageResult

Per-page decode result from `ColumnReader::read_pages()`:

```cpp
struct PageResult {
    int page_num;
    PageType type;          // DATA_PAGE, DICTIONARY_PAGE, etc.
    int32_t num_values;
    std::vector<Value> values;  // decoded values (empty for dictionary pages)
};
```

### Enums

All Parquet enums are defined in `common.hpp` with corresponding `*_name()` helpers that return string representations:

| Enum | Values | Helper |
|------|--------|--------|
| `ParquetType` | `BOOLEAN`, `INT32`, `INT64`, `INT96`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` | `parquet_type_name()` |
| `Encoding` | `PLAIN`, `PLAIN_DICTIONARY`, `RLE`, `RLE_DICTIONARY`, `DELTA_BINARY_PACKED`, ... | `encoding_name()` |
| `CompressionCodec` | `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `ZSTD`, `LZ4`, ... | `compression_name()` |
| `PageType` | `DATA_PAGE`, `INDEX_PAGE`, `DICTIONARY_PAGE`, `DATA_PAGE_V2` | `page_type_name()` |
| `ConvertedType` | `UTF8`, `DATE`, `TIMESTAMP_MILLIS`, `DECIMAL`, `JSON`, ... | `converted_type_name()` |
| `FieldRepetitionType` | `REQUIRED`, `OPTIONAL`, `REPEATED` | — |

### Limitations

- Only **uncompressed** Parquet files are currently supported.
- Read-only; no write support.
- Encodings supported: PLAIN and dictionary (PLAIN_DICTIONARY / RLE_DICTIONARY).