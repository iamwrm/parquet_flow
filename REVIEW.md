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

## Methodology

All three implementations were built and benchmarked from source. Zig was bootstrapped via each project's `hooks/load_zig.sh`. Benchmarks run with `-Doptimize=ReleaseFast`. Output Parquet files validated with `pyarrow 23.0.0`. All numbers below are **measured**, not taken from READMEs.

**Environment:** Linux x86_64, Zig 0.16.0-dev

---

## parquet_flow_1 (Pure Zig, Zig 0.16.0-dev.2490)

### Correctness

**Build:** Compiles cleanly. **Tests pass.**

**Parquet format:** Custom Thrift encoder (`compact.zig`) with correct zigzag encoding, field delta encoding, varint. Streams directly to file via buffered `std.Io.File.Writer` (128KB buffer) -- the only implementation with streaming I/O. Handles all 8 physical types with per-type validation. Level-aware path (`encodeLevelStreams`) correctly implements RLE encoding for definition/repetition levels.

**Parquet validation:** Output files are **valid** -- pyarrow reads them correctly. Both the benchmark output (2M rows) and the log sink demo output (50K rows) pass validation.

**Ring buffer (`log_sink.zig`):** Slot-based queue with atomic `producer`/`consumer` indices. Correct memory ordering. Uses condition variable signaling rather than spin-waiting. The log sink demo works end-to-end: 50K records written, 0 dropped.

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

### Performance (Verified)

- **Compression:** GZIP via `std.compress.flate` (working, validated by pyarrow).
- **Streaming I/O:** Only implementation that writes directly to file, not buffering in memory.
- **Scratch buffer reuse:** `scratch_page`, `scratch_compressed`, `scratch_header`, `scratch_levels` reused via `clearRetainingCapacity()`.
- **Endianness fast path:** `encodeInt32Slice`/`encodeInt64Slice` use `sliceAsBytes` on little-endian for zero-copy.

**Measured throughput** (2M rows, 256 bytes/row, 4096 rows/group):

| Mode | rows/sec | Payload MiB/s | File MiB/s | README Claim |
|------|----------|---------------|------------|--------------|
| Uncompressed | **4,914,119** | 1,200 | 1,219 | 3.9M (conservative) |
| GZIP | **448,283** | 109 | 111 | 127K (very conservative) |
| Optional (null_every=5) | **5,128,841** | 1,252 | 1,021 | not claimed |

README claims were **conservative** -- actual performance is 1.26x (uncompressed) and 3.5x (GZIP) better than claimed.

---

## parquet_flow_2 (Pure Zig + ZSTD, Zig 0.16.0-dev.2510)

### Correctness

**Build:** Compiles cleanly (requires `libzstd-dev`). **Tests FAIL** with integer overflow panic.

**Parquet format:** Clean modular decomposition: `types.zig` -> `encoding.zig` -> `page_writer.zig` -> `column_writer.zig` -> `row_group_writer.zig` -> `file_writer.zig`. Thrift encoder implements compact protocol correctly including struct nesting.

**Parquet validation:** Output files are **valid** -- pyarrow reads all output files correctly:
- `bench_1gb_UNCOMPRESSED.parquet`: 9M rows, 4 columns -- VALID
- `bench_1gb_ZSTD.parquet`: 9M rows, ZSTD compression -- VALID
- `market_UNCOMPRESSED.parquet`: 8M rows, 8 columns -- VALID
- `market_ZSTD.parquet`: 8M rows, ZSTD compression -- VALID
- Log sink files (1,308 files): all valid, ~74 rows each

**Encoding:** All 8 physical types in PLAIN encoding. Boolean bit-packed LSB-first. Byte arrays length-prefixed with 4-byte LE u32.

**Ring buffer (`log_sink.zig`):** SPSC ring buffer with power-of-2 masking. Correct memory ordering. End-to-end pipeline works: 100K entries, 1,306 files produced, 0 dropped.

**Issues:**
- **Tests crash** with integer overflow panic (test runner timeout after 60s).
- Entire file built in `ArrayList(u8)` before writing. For 9M rows of mixed data, that's ~1GB in RAM.
- Ring buffer transport is specialized to `MarketOrder`/`LogEntry` structs, not generic.

### API

Two well-designed C APIs:
- **Batch Writer:** `pf_writer_create/set_*/write/destroy` -- columnar interface, zero-copy (caller owns buffers).
- **Streaming Sink:** `pf_sink_create/start/push/stop/destroy` -- ring buffer with `pf_sink_push(s, &row)`. Includes stats functions.

**Weakness:** C header omits `INT96` and `FIXED_LEN_BYTE_ARRAY` from `PfColumnType` enum, so those types can't be used through the C API.

### Performance (Verified)

- **Compression:** ZSTD via `libzstd`. 6.7% ratio on mixed data (confirmed).
- **MarketOrder struct:** 128-byte cache-friendly layout with explicit padding.
- **Memory model:** Entire file in memory, single write at end.

**Measured throughput:**

| Benchmark | rows/sec | README Claim | Discrepancy |
|-----------|----------|--------------|-------------|
| 1GB mixed (uncompressed) | **3M** | 9M | **3x overstated** |
| 1GB mixed (ZSTD) | **3M** | 10M | **3.3x overstated** |
| Market orders (uncompressed) | **6M** | 19M | **3.2x overstated** |
| Market orders (ZSTD) | **5M** | not separately claimed | -- |
| Ring buffer push | **20,345 ns** | 190 ns | **107x overstated** |

README claims are **significantly overstated**. The ring buffer latency claim (190ns vs actual 20,345ns) is off by two orders of magnitude. Throughput claims are 3x overstated across the board. The log sink produced 100K entries in 2,034ms with 1,306 output files.

---

## parquet_flow_3 (Pure Zig, Zig 0.16.0-dev.2510)

### Correctness

**Build:** Compiles cleanly. **Tests FAIL** with integer overflow panic.

**Parquet format:** Well-structured Thrift implementation with 3 inline tests. Schema builder handles all logical type annotations (STRING, TIMESTAMP, DECIMAL, UUID, etc.) -- the most complete logical type support of the three.

**Parquet validation: OUTPUT IS INVALID.** The market_data_example writes a 52MB file with correct PAR1 header/footer magic bytes, but **pyarrow cannot read it** -- Thrift deserialization fails with "TProtocolException: Invalid data". Despite having the most extensive schema support, the implementation produces non-standard Parquet metadata that conforming readers reject.

**Ring buffer (`ring_buffer.zig`):** Most rigorous design (64-byte cache-line padding, compile-time power-of-2 enforcement, `drainBatch()` for bulk reads). However, tests crash.

**Critical issues:**
- **Output files are not valid Parquet.** The Thrift metadata is malformed -- pyarrow rejects them.
- **Tests crash** with integer overflow panic.
- **All compression is stubbed.** Only UNCOMPRESSED works.
- **`flushBatch()` is a no-op.** It resets the batch without writing Parquet. The ring buffer pipeline works, but never produces output files.
- BYTE_ARRAY columns are treated as fixed-size in the batch accumulator, which is a design mismatch with Parquet's variable-length semantics.
- 256-byte record size limit in the ring buffer.

### API

Simplest C API: `pqflow_create`, `pqflow_set_schema`, `pqflow_log`, `pqflow_flush`, `pqflow_destroy`. Clean opaque handle pattern.

**Weakness:** Only streaming sink (no batch writer). `pqflow_flush()` is a no-op. No way to write structured columnar data through the C API.

### Performance (Verified)

- **Compression:** None functional.
- **Ring buffer:** Best engineering design (cache-line alignment, compile-time enforcement, batch drain) but tests crash.

**Measured throughput** (market_data_example, 1M rows):

| Mode | rows/sec | README Claim | Note |
|------|----------|--------------|------|
| Writer (uncompressed) | **5,497,945** | 2M | 2.75x better than claimed |

However, this throughput number is **meaningless** because the output file is invalid Parquet. Writing fast but producing corrupt output does not satisfy the requirements.

---

## Comparative Summary (Verified)

| Feature | pf_1 | pf_2 | pf_3 |
|---------|------|------|------|
| **Compiles** | Yes | Yes (needs libzstd) | Yes |
| **Tests pass** | **Yes** | No (integer overflow) | No (integer overflow) |
| **Output valid Parquet** | **Yes** | **Yes** | **No** (Thrift error) |
| **All 8 physical types** | Yes | Yes | Yes (but output invalid) |
| **REPEATED fields** | Yes | No | No |
| **Logical type annotations** | No | ConvertedType only | Full LogicalType union |
| **Compression (working)** | GZIP | ZSTD | None (stubs) |
| **Streaming file I/O** | Yes (only one) | No (in-memory) | No (in-memory) |
| **Ring buffer -> Parquet wired** | Yes | Yes | No (no-op) |
| **C API: batch writer** | Yes (level-aware) | Yes (columnar) | No |
| **C API: streaming sink** | No (Zig only) | Yes | Yes (incomplete) |
| **Verified throughput** | **4.9M rows/s** | **3-6M rows/s** | 5.5M rows/s (invalid output) |
| **README accuracy** | Conservative | **3-107x overstated** | Conservative |
| **Zig version** | 0.16.0-dev.2490 | 0.16.0-dev.2510 | 0.16.0-dev.2510 |

### Verdict (Updated with Verified Results)

- **pf_1** is the **only production-ready implementation**. It is the only one where tests pass, output validates with pyarrow, and the end-to-end pipeline (ring buffer -> parquet) works. Streaming I/O, working GZIP compression, level-aware C API. Its README performance claims were conservative (actual numbers are better). Main limitation: log sink only streams opaque blobs.

- **pf_2** produces **valid Parquet files** and has the most practical C API (batch + streaming), but its **tests crash** and its **README performance claims are grossly inflated** (3x on throughput, 107x on ring buffer latency). The ZSTD compression works correctly. The in-memory file model limits dataset size.

- **pf_3** is **functionally broken** despite having the best engineering rigor on paper. Its output files **cannot be read by pyarrow** -- the Thrift metadata is malformed. Tests crash. Compression is stubbed. The ring-to-parquet pipeline is a no-op. The 776-line architecture doc and 25 tests describe aspirational design, not working software.

### Bottom Line

Only pf_1 produces valid output from a passing test suite. If you need a working Parquet writer today, pf_1 is the only viable choice. pf_2 could be salvaged (the core Parquet encoding works, as validated by pyarrow) but needs test fixes and honest benchmarking. pf_3 needs fundamental correctness work before performance or API discussions are meaningful.
