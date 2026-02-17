# Tests

## index.cpp — Chunked Inverted Index for Parquet Pages

`index.cpp` is a test program that builds a chunked inverted index over the data pages of a single Parquet column. Given a Parquet file and a column name, it groups raw page data into fixed-size chunks and constructs an index that maps any byte offset within a chunk back to the originating page.

### Usage

```bash
./build/index_test <parquet_file> <column_name>
```

### How It Works

#### 1. Page Iteration and Filtering

The program opens a Parquet file via `ParquetReader`, resolves the target column by name (`find_column`), and iterates over every page in the file using `PageIterator`. Pages belonging to other columns are skipped.

#### 2. Chunking

Pages are appended sequentially into an in-memory byte buffer (`chunk_data`). When adding the next page would exceed `CHUNK_THRESHOLD` (4096 bytes), the current chunk is finalized: the chunk ID is incremented and the buffer is cleared. This groups pages into roughly 4 KB chunks.

#### 3. Building the Index

For each page appended to a chunk, a `PageEntry` is recorded:

```
struct PageEntry {
    size_t offset;   // byte offset where this page starts within the chunk
    size_t rg_idx;   // row group index
    size_t col_idx;  // column index
};
```

These entries are stored in a `std::map<size_t, std::vector<PageEntry>>` keyed by chunk ID. Within each chunk, entries are ordered by their starting offset.

#### 4. Point Lookup via Binary Search

The `find_page` lambda resolves an arbitrary `(chunk_id, byte_offset)` pair to the page that contains that offset. It performs an upper-bound binary search on the offset-sorted entry list for the given chunk, then steps back one position to find the page whose range covers the queried offset.

#### 5. Sample Queries

A set of hardcoded queries exercises the lookup:

```
{chunk=0, offset=0}, {chunk=0, offset=500}, {chunk=0, offset=1042},
{chunk=0, offset=2000}, {chunk=1, offset=0}, {chunk=1, offset=1100}
```

Each query prints the matching page's row group, column index, and starting offset within the chunk, or "not found" if the chunk/offset is invalid.

### Key Dependencies

| Symbol | Header | Purpose |
|---|---|---|
| `ParquetReader` | `parquet_reader.hpp` | Opens files, resolves columns, provides page iterator |
| `PageIterator` | `parquet_reader.hpp` | Streams `RawPage` objects across all row groups and columns |
| `RawPage` | `parquet_reader.hpp` | Carries `page_id`, `row_group_idx`, `column_idx`, and raw `data` bytes |
