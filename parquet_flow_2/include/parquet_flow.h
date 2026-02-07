#ifndef PARQUET_FLOW_H
#define PARQUET_FLOW_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Column type enum (matches Parquet physical types) */
typedef enum {
    PF_BOOLEAN    = 0,
    PF_INT32      = 1,
    PF_INT64      = 2,
    PF_FLOAT      = 4,
    PF_DOUBLE     = 5,
    PF_BYTE_ARRAY = 6,
} PfColumnType;

/* Compression codec */
typedef enum {
    PF_UNCOMPRESSED = 0,
    PF_ZSTD         = 6,
} PfCompression;

/* Column definition */
typedef struct {
    const char* name;
    int         col_type;   /* PfColumnType */
    int         required;   /* 1 = required, 0 = optional */
} PfColumnDef;

/* Variable-length byte slice for BYTE_ARRAY columns */
typedef struct {
    const uint8_t* data;
    int32_t        len;
} PfBytes;

/* ========== Batch Writer API ========== */
/* Columnar interface for writing Parquet files.
 * Usage:
 *   1. pf_writer_create() with schema
 *   2. pf_writer_set_*() for each column
 *   3. pf_writer_write() to flush to file
 *   4. pf_writer_destroy() to cleanup
 */

typedef struct WriterState PfWriter;

PfWriter* pf_writer_create(const PfColumnDef* cols, int ncols, int compression);
void      pf_writer_destroy(PfWriter* w);

int pf_writer_set_i32 (PfWriter* w, int col, const int32_t* vals, int64_t n);
int pf_writer_set_i64 (PfWriter* w, int col, const int64_t* vals, int64_t n);
int pf_writer_set_f32 (PfWriter* w, int col, const float*   vals, int64_t n);
int pf_writer_set_f64 (PfWriter* w, int col, const double*  vals, int64_t n);
int pf_writer_set_bool(PfWriter* w, int col, const bool*    vals, int64_t n);
int pf_writer_set_bytes(PfWriter* w, int col, const PfBytes* vals, int64_t n);
int pf_writer_set_def_levels(PfWriter* w, int col, const uint8_t* lvls, int64_t n);

/* Write all set columns to a Parquet file. Returns 0 on success, -1 on error. */
int pf_writer_write(PfWriter* w, const char* path, int64_t nrows);

/* ========== Streaming Sink API ========== */
/* Non-blocking ring-buffer sink with background Parquet writer thread.
 * Usage:
 *   1. pf_sink_create() with schema and output directory
 *   2. pf_sink_start() to spawn the writer thread
 *   3. pf_sink_push() from hot path (non-blocking, returns 0 or -1)
 *   4. pf_sink_stop() to drain and join
 *   5. pf_sink_destroy() to cleanup
 */

typedef struct SinkState PfSink;

PfSink* pf_sink_create(const char* output_dir, const PfColumnDef* cols, int ncols,
                        int row_size, int compression, int batch_size);
int     pf_sink_start(PfSink* s);
int     pf_sink_push(PfSink* s, const void* row);
void    pf_sink_stop(PfSink* s);
void    pf_sink_destroy(PfSink* s);
int64_t pf_sink_files_written(PfSink* s);
int64_t pf_sink_entries_written(PfSink* s);

#ifdef __cplusplus
}
#endif

#endif /* PARQUET_FLOW_H */
