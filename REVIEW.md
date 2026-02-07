# Code Review: Three parquet_flow Implementations

## Requirements

From the original prompt:
1. Non-blocking log sink (binary records -> parquet, off the hot thread)
2. Parquet compression
3. High throughput (stock market order-by-order feed capture)
4. Support all parquet column types
5. C API export for C++ integration
6. Zig implementation

---

## parquet_flow_1 (Parzig-based, Zig 0.13)

### Correctness

**Parquet format:** Delegates to the `parzig` library, so format correctness is inherited from an existing implementation. The custom Thrift encoder (`compact.zig`) is correct: proper zigzag `(v << 1) ^ (0 - sign)`, field delta encoding, varint.

**Writer (`writer.zig`):** Streams directly to file via buffered `std.Io.File.Writer` (128KB buffer). This is the only implementation that does streaming I/O rather than buffering the entire file in memory. Handles all 8 physical types with per-type validation. The level-aware path (`encodeLevelStreams`) correctly implements RLE encoding for definition/repetition levels.

**Ring buffer (`log_sink.zig`):** Slot-based queue with atomic `producer`/`consumer` indices. Memory ordering is correct. Uses condition variable signaling rather than spin-waiting.

**Issues:**
- The log sink hardcodes a single `byte_array` column schema, so it can only stream opaque blobs, not structured multi-column data.
- In the `write_row_group_with_levels` C API path, `expected_values` is passed as `null` (line 277), bypassing row-count validation.
- `float`/`double` encoding lacks the endianness fast path that `int32`/`int64` have.

### API

The most comprehensive C API. Two write paths:
- `pf_writer_write_row_group()` for required-only columns
- `pf_writer_write_row_group_with_levels()` for nullable/repeated fields

This is the only implementation supporting `REPEATED` fields. Includes `pf_writer_last_error()` for human-readable error strings. Uses Arrow-compatible `void*` values + `u32*` offsets for byte arrays.

**Weakness:** No streaming sink exported to C. The `LogSink` exists in Zig but isn't exposed in the C header.

### Performance

- **Compression:** GZIP via `std.compress.flate` (implemented and working).
- **Streaming I/O:** Only implementation that writes directly to file, not buffering in memory.
- **Scratch buffer reuse:** `scratch_page`, `scratch_compressed`, `scratch_header`, `scratch_levels` reused via `clearRetainingCapacity()`.
- **Endianness fast path:** `encodeInt32Slice`/`encodeInt64Slice` use `sliceAsBytes` on little-endian for zero-copy.
- **Throughput:** 3.9M rows/sec (256 bytes/row, uncompressed), 127K rows/sec with GZIP.

---

## parquet_flow_2 (Pure Zig + ZSTD, Zig 0.14+)

### Correctness

**Parquet format:** Clean modular decomposition: `types.zig` -> `encoding.zig` -> `page_writer.zig` -> `column_writer.zig` -> `row_group_writer.zig` -> `file_writer.zig`. Thrift encoder implements compact protocol correctly including struct nesting.

**Encoding:** All 8 physical types in PLAIN encoding. Boolean bit-packed LSB-first. Byte arrays length-prefixed with 4-byte LE u32. Fixed-length byte arrays raw-concatenated.

**Ring buffer (`log_sink.zig`):** SPSC ring buffer with power-of-2 masking. Correct memory ordering.

**Issues:**
- Entire file built in `ArrayList(u8)` before writing. For 9M rows of mixed data, that's ~1GB in RAM.
- Ring buffer transport is specialized to `MarketOrder`/`LogEntry` structs, not generic.

### API

Two well-designed C APIs:
- **Batch Writer:** `pf_writer_create/set_*/write/destroy` -- columnar interface, zero-copy (caller owns buffers).
- **Streaming Sink:** `pf_sink_create/start/push/stop/destroy` -- ring buffer with `pf_sink_push(s, &row)`. Includes stats functions.

**Weakness:** C header omits `INT96` and `FIXED_LEN_BYTE_ARRAY` from `PfColumnType` enum, so those types can't be used through the C API.

### Performance

- **Compression:** ZSTD via `libzstd`. 6.6% ratio on mixed data, 5% on market orders.
- **MarketOrder struct:** 128-byte cache-friendly layout with explicit padding.
- **Throughput:** 9M rows/sec (mixed, uncompressed), 19M rows/sec (market orders, uncompressed), 10M rows/sec (ZSTD). Ring buffer: 190ns/push.
- **Memory model:** Entire file in memory, single write at end.

---

## parquet_flow_3 (Pure Zig, Zig 0.16-dev)

### Correctness

**Parquet format:** Well-structured. Thrift implementation tested (3 inline tests). Schema builder handles all logical type annotations (STRING, TIMESTAMP, DECIMAL, UUID, etc.) -- the most complete logical type support of the three.

**Ring buffer (`ring_buffer.zig`):** Most rigorous. Explicit 64-byte cache-line padding with `align(CACHE_LINE)`. Compile-time power-of-2 enforcement. `drainBatch()` for bulk reads. 13 tests including concurrent SPSC stress tests.

**Writer (`writer.zig`):** FileWriter -> RowGroupWriter -> ColumnWriter hierarchy. Type-specific write methods. Definition levels with RLE/Bit-Pack Hybrid. Two tests verify file structure and optional columns.

**Critical issues:**
- **All compression is stubbed.** Only UNCOMPRESSED works.
- **`flushBatch()` is a no-op.** It resets the batch without writing Parquet. The ring buffer pipeline works, but never produces output files.
- BYTE_ARRAY columns are treated as fixed-size in the batch accumulator, which is a design mismatch with Parquet's variable-length semantics.
- 256-byte record size limit in the ring buffer.

### API

Simplest C API: `pqflow_create`, `pqflow_set_schema`, `pqflow_log`, `pqflow_flush`, `pqflow_destroy`. Clean opaque handle pattern.

**Weakness:** Only streaming sink (no batch writer). `pqflow_flush()` is a no-op. No way to write structured columnar data through the C API.

### Performance

- **Compression:** None functional.
- **Ring buffer:** Best engineering (cache-line alignment, compile-time enforcement, batch drain).
- **Throughput:** 2M records/sec for the writer benchmark. No ring buffer end-to-end numbers.
- **Documentation:** 776-line ARCHITECTURE.md, 25 tests.

---

## Comparative Summary

| Feature | pf_1 | pf_2 | pf_3 |
|---------|------|------|------|
| **All 8 physical types** | Yes | Yes | Yes |
| **REPEATED fields** | Yes | No | No |
| **Logical type annotations** | No | ConvertedType only | Full LogicalType union |
| **Compression (working)** | GZIP | ZSTD | None (stubs) |
| **Streaming file I/O** | Yes (only one) | No (in-memory) | No (in-memory) |
| **Ring buffer -> Parquet wired** | Yes | Yes | No (no-op) |
| **C API: batch writer** | Yes (level-aware) | Yes (columnar) | No |
| **C API: streaming sink** | No (Zig only) | Yes | Yes (incomplete) |
| **Throughput (uncompressed)** | 3.9M rows/s | 9-19M rows/s | 2M rows/s |
| **Test suite** | Via parzig | Inline tests | 25 tests (most thorough) |
| **Documentation** | Basic | Good | Excellent |
| **Zig version** | 0.13 | 0.14+ | 0.16-dev |

### Verdict

- **pf_1** is the most **production-complete**: streaming I/O, working GZIP, level-aware C API, end-to-end pipeline wired. Main limitation: log sink only streams opaque blobs, parzig dependency locks to Zig 0.13.

- **pf_2** has the **best raw performance** and **most practical C API** (batch + streaming). ZSTD with 5-6% ratios. In-memory file model is the main concern for very large datasets.

- **pf_3** has the **best engineering rigor** (cache-line ring buffer, 25 tests, 776-line architecture doc, full logical types) but is **functionally incomplete** -- no compression, ring-to-parquet pipeline not wired.
