# parquet_flow

High-performance Parquet file writer in Zig with a C API, designed for capturing stock market order-by-order live feeds from C++ applications.

## Features

- **All Parquet physical types**: BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY
- **ZSTD compression** via system libzstd (6.6% compression ratio on mixed data)
- **Multi-row-group** support with configurable row group size
- **C API** shared library (`libparquet_flow.so`) with 17 exported symbols
- **Batch writer** — columnar API for writing complete datasets
- **Streaming sink** — lock-free SPSC ring buffer with background writer thread (190 ns/push)
- **MarketOrder struct** — 128-byte cache-friendly layout for order-by-order feeds

## Performance

| Benchmark | Codec | File Size | Throughput | Ratio |
|-----------|-------|-----------|------------|-------|
| 1GB mixed (9M rows) | UNCOMPRESSED | 1029 MB | 9M rows/sec | — |
| 1GB mixed (9M rows) | ZSTD | 68 MB | 7M rows/sec | 6.6% |
| Market orders (8M) | UNCOMPRESSED | 396 MB | 19M rows/sec | — |
| Market orders (8M) | ZSTD | 19 MB | 10M rows/sec | 5% |
| Ring buffer push | — | — | 190 ns/op | — |

All output files validated with DuckDB.

## Prerequisites

- **Zig** 0.14+ (tested with 0.16-dev master)
- **libzstd** (`apt install libzstd-dev` / `brew install zstd`)
- **DuckDB** (optional, for validation)

A helper script is included to install Zig from the official nightly builds:

```bash
source hooks/load_zig.sh
```

## Build

```bash
# Build executable + shared library
zig build

# Build with optimizations
zig build -Doptimize=ReleaseFast

# Run benchmarks
zig build run -Doptimize=ReleaseFast

# Run tests
zig build test
```

Outputs:
- `zig-out/bin/parquet_flow` — benchmark executable
- `zig-out/lib/libparquet_flow.so` — shared library for C/C++ integration

## C API

Link against `libparquet_flow.so` and include `include/parquet_flow.h`.

### Batch Writer

Write columnar data directly to Parquet files:

```c
#include "parquet_flow.h"

// Define schema
PfColumnDef cols[] = {
    { "timestamp_ns", PF_INT64,      1 },
    { "price",        PF_DOUBLE,     1 },
    { "symbol",       PF_BYTE_ARRAY, 1 },
};

PfWriter* w = pf_writer_create(cols, 3, PF_ZSTD);

// Set column data (zero-copy — caller owns the buffers)
pf_writer_set_i64(w, 0, timestamps, nrows);
pf_writer_set_f64(w, 1, prices, nrows);
pf_writer_set_bytes(w, 2, symbols, nrows);

// Write to file
pf_writer_write(w, "output.parquet", nrows);
pf_writer_destroy(w);
```

### Streaming Sink

Non-blocking ring buffer for hot-path ingestion with background Parquet writing:

```c
#include "parquet_flow.h"

PfSink* s = pf_sink_create("output_dir", cols, ncols,
                            sizeof(MyRow), PF_ZSTD, 100000);
pf_sink_start(s);

// Hot path — non-blocking, returns 0 on success, -1 if full
MyRow row = { ... };
pf_sink_push(s, &row);

// Shutdown — drains buffer and joins writer thread
pf_sink_stop(s);
printf("Files: %lld, Entries: %lld\n",
       pf_sink_files_written(s), pf_sink_entries_written(s));
pf_sink_destroy(s);
```

## Project Structure

```
parquet_flow_2/
├── build.zig                  # Build configuration
├── build.zig.zon              # Package metadata
├── include/
│   └── parquet_flow.h         # C API header
├── hooks/
│   └── load_zig.sh            # Zig installer script
└── src/
    ├── main.zig               # Benchmarks (1GB mixed, market orders, ring buffer)
    ├── c_api.zig              # C API exports (batch writer + streaming sink)
    ├── market_order.zig       # MarketOrder struct + schema
    ├── log_entry.zig          # Log entry struct
    ├── log_sink.zig           # Ring buffer log sink
    └── parquet/
        ├── types.zig          # Parquet type enums and schema definitions
        ├── thrift_encoder.zig # Thrift Compact Protocol encoder
        ├── encoding.zig       # PLAIN encoding for all physical types
        ├── compression.zig    # ZSTD compression via libzstd
        ├── page_writer.zig    # Data Page V1 assembly
        ├── column_writer.zig  # Column chunk writer
        ├── row_group_writer.zig # Row group writer
        └── file_writer.zig    # Parquet file assembler (PAR1 + footer)
```

## Validate Output

```bash
duckdb -c "SELECT count(*) FROM read_parquet('output/*.parquet');"
duckdb -c "DESCRIBE SELECT * FROM read_parquet('output/market_ZSTD.parquet');"
duckdb -c "SELECT * FROM read_parquet('output/market_ZSTD.parquet') LIMIT 5;"
```

## License

MIT
