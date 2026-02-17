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

## Usage

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