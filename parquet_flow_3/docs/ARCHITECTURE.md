# ParquetFlow Architecture

High-performance, non-blocking log sink that writes Parquet files.
Designed for stock market order-book capture at wire speed.

## Component Diagram

```
┌─────────────────┐     ┌──────────────────────────────────────────────────┐
│  Any C/C++ App  │     │                ParquetFlow Library               │
│                 │     │                                                  │
│  hot thread ────┼──>──┤  C API (c_api.zig)                              │
│                 │     │    │                                             │
│                 │     │    v                                             │
│                 │     │  Ring Buffer (lock-free SPSC/MPSC)               │
│                 │     │    │                                             │
│                 │     │    v                        background thread    │
│                 │     │  Batch Accumulator ──────────────────────┐       │
│                 │     │                                         v       │
│                 │     │                              Parquet Writer      │
│                 │     │                              ├─ Schema           │
│                 │     │                              ├─ Column Encoder   │
│                 │     │                              ├─ Page Builder     │
│                 │     │                              ├─ Compression      │
│                 │     │                              ├─ Thrift Metadata  │
│                 │     │                              └─> .parquet file   │
└─────────────────┘     └──────────────────────────────────────────────────┘
```

## Data Flow

1. **App hot thread** calls `pqflow_log_record()` — writes fixed-size binary record into ring buffer. Returns immediately (never blocks).
2. **Ring buffer** — pre-allocated, cache-line aligned, lock-free. SPSC by default, MPSC variant available.
3. **Background writer thread** — drains ring buffer in batches. When batch is full or timeout expires, encodes columns and writes a Parquet row group.
4. **Parquet writer** — encodes each column (PLAIN/RLE), optionally compresses (ZSTD/Snappy/Gzip/None), writes pages, builds Thrift metadata footer.
5. **File rotation** — configurable by row count or byte size.

## Thread Model

```
Producer thread(s)          Consumer thread (1)
      │                           │
      │  pqflow_log_record()      │
      ├────► ring_buffer ─────────┤
      │      (atomic ops)         │  drain loop:
      │                           │    read batch from ring
      │                           │    decode fixed records
      │                           │    columnarize
      │                           │    encode + compress
      │                           │    write row group
      │                           │    if file full: rotate
```

- **Zero allocations on hot path** — ring buffer is pre-allocated, records are fixed-size
- **Single consumer** — no locks needed on write side
- **Cache-line padding** — read/write heads on separate cache lines (64 bytes)

## C API Surface

```c
// include/parquet_flow.h

typedef struct pqflow_sink* pqflow_sink_t;

typedef enum {
    PQFLOW_OK = 0,
    PQFLOW_ERR_FULL = 1,        // ring buffer full, record dropped
    PQFLOW_ERR_INVALID = 2,     // invalid args
    PQFLOW_ERR_IO = 3,          // file I/O error
    PQFLOW_ERR_SCHEMA = 4,      // schema not set
} pqflow_error;

typedef enum {
    PQFLOW_TYPE_BOOL = 0,
    PQFLOW_TYPE_I32 = 1,
    PQFLOW_TYPE_I64 = 2,
    PQFLOW_TYPE_I96 = 3,
    PQFLOW_TYPE_F32 = 4,
    PQFLOW_TYPE_F64 = 5,
    PQFLOW_TYPE_BYTE_ARRAY = 6,
    PQFLOW_TYPE_FIXED_BYTE_ARRAY = 7,
} pqflow_type;

typedef enum {
    PQFLOW_COMPRESS_NONE = 0,
    PQFLOW_COMPRESS_SNAPPY = 1,
    PQFLOW_COMPRESS_GZIP = 2,
    PQFLOW_COMPRESS_ZSTD = 6,
} pqflow_compression;

typedef struct {
    const char* name;
    pqflow_type type;
    int32_t type_length;    // for FIXED_BYTE_ARRAY
    int32_t nullable;       // 0 = required, 1 = optional
} pqflow_column_def;

typedef struct {
    const char* file_path;
    uint32_t ring_buffer_size;      // power of 2, default 1<<20
    uint32_t batch_size;            // rows per batch, default 65536
    uint32_t max_rows_per_file;     // 0 = unlimited
    pqflow_compression compression;
} pqflow_config;

pqflow_error pqflow_create(pqflow_sink_t* out, const pqflow_config* config);
pqflow_error pqflow_set_schema(pqflow_sink_t sink, const pqflow_column_def* columns, uint32_t num_columns);
pqflow_error pqflow_log(pqflow_sink_t sink, const void* record, uint32_t len);
pqflow_error pqflow_flush(pqflow_sink_t sink);
void         pqflow_destroy(pqflow_sink_t sink);
```

## Record Format

Records are flat binary structs. The schema defines column layout.
For fixed-size types, values are packed contiguously.
For BYTE_ARRAY columns: 4-byte LE length prefix + data.
Null bitmap: 1 bit per nullable column, packed at the start of the record.

## Parquet Writer Internals

### File Layout
```
"PAR1" (4 bytes)
[Row Group 1]
  [Column Chunk 1: PageHeader(thrift) + page_data, ...]
  [Column Chunk 2: PageHeader(thrift) + page_data, ...]
  ...
[Row Group 2]
  ...
FileMetaData (thrift, TCompactProtocol)
metadata_length (4 bytes LE)
"PAR1" (4 bytes)
```

### Encodings
- **PLAIN**: Default for all types. Values packed LE. Booleans bit-packed LSB-first.
- **RLE/Bit-Pack Hybrid**: For definition levels and boolean columns.
- **RLE_DICTIONARY**: Optional dictionary encoding for repeated string values.

### Compression
Applied per-page after encoding. Supported:
- UNCOMPRESSED (always)
- ZSTD (via Zig std.compress.zstd or libzstd C linkage)
- SNAPPY (via C libsnappy linkage)
- GZIP (via Zig std.compress.flate)

### Thrift Compact Protocol
Custom minimal implementation (~200 lines). Supports:
- Varint (ULEB128), ZigZag encoding
- Field delta encoding
- Struct, List, Binary, Bool, I16/I32/I64, Double serialization
- Only write path needed (no reader)

## File Organization

```
src/
  parquet/
    types.zig          -- Physical/logical types, enums
    schema.zig         -- Schema definitions, SchemaElement
    thrift.zig         -- Thrift compact protocol writer
    encoding.zig       -- PLAIN, RLE encoders
    compression.zig    -- Compression dispatch (none/zstd/snappy/gzip)
    page.zig           -- Data page + dictionary page construction
    writer.zig         -- FileWriter, RowGroupWriter, ColumnWriter
  sink/
    ring_buffer.zig    -- Lock-free SPSC ring buffer
    log_sink.zig       -- Top-level sink: ring buffer + writer thread
    batch.zig          -- Record batching and columnarization
  c_api.zig            -- C-exported API functions
  root.zig             -- Library root, pub imports
include/
  parquet_flow.h       -- C header
build.zig              -- Zig build script
tests/
  test_thrift.zig
  test_encoding.zig
  test_writer.zig
  test_ring_buffer.zig
  test_integration.zig
examples/
  market_data.zig      -- Stock market order capture example
```

## Memory Management

- Ring buffer: single mmap/alloc at init, fixed lifetime
- Parquet writer buffers: arena allocator per row group, freed on row group close
- Page buffers: reused across pages via arena reset
- No allocations on producer hot path
- Consumer thread uses bounded memory (configurable batch size)

## Stock Market Data Example Schema

```
timestamp_ns  : INT64  (required) -- nanosecond epoch
symbol        : FIXED_BYTE_ARRAY[8] (required) -- padded ticker
order_id      : INT64  (required)
side          : INT32  (required) -- 0=bid, 1=ask
price         : INT64  (required) -- price in microcents
quantity      : INT64  (required)
order_type    : INT32  (required) -- 0=new, 1=modify, 2=cancel, 3=trade
exchange      : INT32  (required) -- exchange code
```

Fixed 57-byte records at ~10M records/sec target throughput.
