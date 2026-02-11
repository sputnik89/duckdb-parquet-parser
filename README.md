# duckdb-parquet-parser

A standalone C++17 Parquet file parser that reads Parquet metadata, schema, and page-level details without external dependencies.

## Building

Requires CMake 3.16+ and a C++17 compiler.

```bash
mkdir build && cd build
cmake ..
make
```

This produces the `parquet_parser` executable in the build directory.

## Usage

### Print schema and page layout

```bash
./build/parquet_parser <parquet_file>
```

Prints the file schema, row groups, and page sizes for every column.

### Regex filtering mode

```bash
./build/parquet_parser <parquet_file> --regex-column <column> --regex <pattern> [--neg-regex]
```

Scans data pages for a specific column and reports which pages have no values matching the regex pattern.

- `--regex-column` — name of the column to scan
- `--regex` — regex pattern to match against values
- `--neg-regex` — invert the match (acts as NOT LIKE)