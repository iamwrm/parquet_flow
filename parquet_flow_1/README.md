# parquet_flow

High-throughput, non-blocking binary log flow in Zig with Parquet output and C ABI for modern C++ integration.

## Methodology

### 1) Keep the hot path non-blocking

- Producers call `LogSink.tryRecord(payload)` only.
- The call never blocks; on pressure it drops (`drop-on-full`) instead of stalling latency-critical threads.
- Queue is an in-memory ring buffer to avoid allocator work in the producer path.

### 2) Move serialization + IO off hot threads

- A worker thread drains queued records and writes Parquet row groups.
- The worker batches by `row_group_rows` for better write amplification and metadata overhead.
- Data durability and format work happen outside latency-sensitive code.

### 3) Parquet writer scope and layout

- File format: magic/header/footer + compact-Thrift metadata.
- Physical types supported:
  - `BOOLEAN`, `INT32`, `INT64`, `INT96`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`
- Compression:
  - `UNCOMPRESSED`
  - `GZIP`
- Repetition support:
  - `required`: regular row-group APIs
  - `optional` / `repeated`: level-aware API (`definition_levels` / `repetition_levels`)
  - Current repeated support targets top-level primitive repeated fields (max def/rep level = 1).

### 4) C/C++ integration boundary

- Static library: `zig-out/lib/libparquet_flow_c.a`
- Header: `zig-out/include/parquet_flow_c.h`
- Required-only write path:
  - `pf_writer_write_row_group(...)`
- Nullable/repeated write path:
  - `pf_writer_write_row_group_with_levels(...)`

### 5) Performance posture for market feed capture

- Optimize for sustained throughput first, then compression ratio as a mode switch.
- Keep memory reuse in place (`scratch_*` buffers, batched writes).
- Prefer schema stability + append-only row groups for predictable latency and CPU.

## Verification Loop

Run this loop after every code change.

### 0) Bootstrap Zig

```bash
source hooks/load_zig.sh
```

### 1) Build and tests

```bash
ZIG_GLOBAL_CACHE_DIR=local_data/zig-cache \
ZIG_LOCAL_CACHE_DIR=.zig-cache \
local_data/zig/current/zig build

ZIG_GLOBAL_CACHE_DIR=local_data/zig-cache \
ZIG_LOCAL_CACHE_DIR=.zig-cache \
local_data/zig/current/zig build test
```

### 2) Demo smoke test (non-blocking log flow)

```bash
ZIG_GLOBAL_CACHE_DIR=local_data/zig-cache \
ZIG_LOCAL_CACHE_DIR=.zig-cache \
local_data/zig/current/zig build run
```

Expected: output file `local_data/log_flow.parquet` and non-zero `wrote=...`.

### 3) Throughput benchmark (required, uncompressed)

```bash
ZIG_GLOBAL_CACHE_DIR=local_data/zig-cache \
ZIG_LOCAL_CACHE_DIR=.zig-cache \
local_data/zig/current/zig build bench -- \
  --rows 1000000 \
  --row-size 256 \
  --row-group-rows 4096 \
  --repetition required \
  --compression uncompressed \
  --output local_data/bench_writer_required.parquet
```

### 4) Throughput benchmark (optional/nulls, uncompressed)

```bash
ZIG_GLOBAL_CACHE_DIR=local_data/zig-cache \
ZIG_LOCAL_CACHE_DIR=.zig-cache \
local_data/zig/current/zig build bench -- \
  --rows 1000000 \
  --row-size 256 \
  --row-group-rows 4096 \
  --repetition optional \
  --null-every 8 \
  --compression uncompressed \
  --output local_data/bench_writer_optional.parquet
```

### 5) Throughput benchmark (gzip mode)

```bash
ZIG_GLOBAL_CACHE_DIR=local_data/zig-cache \
ZIG_LOCAL_CACHE_DIR=.zig-cache \
local_data/zig/current/zig build bench -- \
  --rows 1000000 \
  --row-size 256 \
  --row-group-rows 4096 \
  --repetition required \
  --compression gzip \
  --output local_data/bench_writer_required_gzip.parquet

ZIG_GLOBAL_CACHE_DIR=local_data/zig-cache \
ZIG_LOCAL_CACHE_DIR=.zig-cache \
local_data/zig/current/zig build bench -- \
  --rows 1000000 \
  --row-size 256 \
  --row-group-rows 4096 \
  --repetition optional \
  --null-every 8 \
  --compression gzip \
  --output local_data/bench_writer_optional_gzip.parquet
```

### 6) File validity check

```bash
deps/parzig/zig-out/bin/parzig local_data/bench_writer_required.parquet
deps/parzig/zig-out/bin/parzig local_data/bench_writer_optional.parquet
```

Expected: valid metadata parse; optional run should show `null` values where definition level is 0.

### 7) C API smoke test (level-aware path)

Compile and run a tiny C program against:

- `include/parquet_flow_c.h`
- `zig-out/lib/libparquet_flow_c.a`

Then validate the produced Parquet file with `parzig`.

## Throughput Baseline (Measured February 7, 2026)

Workload: `1,000,000 rows`, `256 bytes/row`, `row_group_rows=4096`.

- Required + uncompressed:
  - `rows_per_sec=3,930,389.33`
  - `payload_mib_per_sec=959.57`
- Optional (`null_every=8`) + uncompressed:
  - `rows_per_sec=4,422,348.31`
  - `payload_mib_per_sec=1,079.67`
- Required + gzip:
  - `rows_per_sec=127,147.04`
  - `payload_mib_per_sec=31.04`
- Optional (`null_every=8`) + gzip:
  - `rows_per_sec=144,078.96`
  - `payload_mib_per_sec=35.18`
