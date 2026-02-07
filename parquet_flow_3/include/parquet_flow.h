/*
 * ParquetFlow - High-performance non-blocking Parquet log sink
 *
 * C API for writing structured records to Parquet files
 * from latency-sensitive hot paths (e.g., market data capture).
 *
 * Thread safety:
 *   - pqflow_log() is safe to call from a single producer thread
 *     concurrently with the internal consumer thread.
 *   - All other functions must be called from a single thread.
 */

#ifndef PARQUET_FLOW_H
#define PARQUET_FLOW_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ---------- Opaque handle ------------------------------------------------ */

typedef struct pqflow_sink* pqflow_sink_t;

/* ---------- Error codes -------------------------------------------------- */

typedef enum {
    PQFLOW_OK          = 0,  /* Success */
    PQFLOW_ERR_FULL    = 1,  /* Ring buffer full, record dropped */
    PQFLOW_ERR_INVALID = 2,  /* Invalid arguments */
    PQFLOW_ERR_IO      = 3,  /* File I/O error */
    PQFLOW_ERR_SCHEMA  = 4,  /* Schema not set or invalid */
} pqflow_error;

/* ---------- Physical types ----------------------------------------------- */

typedef enum {
    PQFLOW_TYPE_BOOL             = 0,
    PQFLOW_TYPE_I32              = 1,
    PQFLOW_TYPE_I64              = 2,
    PQFLOW_TYPE_I96              = 3,
    PQFLOW_TYPE_F32              = 4,
    PQFLOW_TYPE_F64              = 5,
    PQFLOW_TYPE_BYTE_ARRAY       = 6,
    PQFLOW_TYPE_FIXED_BYTE_ARRAY = 7,
} pqflow_type;

/* ---------- Compression codecs ------------------------------------------- */

typedef enum {
    PQFLOW_COMPRESS_NONE   = 0,
    PQFLOW_COMPRESS_SNAPPY = 1,
    PQFLOW_COMPRESS_GZIP   = 2,
    PQFLOW_COMPRESS_ZSTD   = 6,
} pqflow_compression;

/* ---------- Column definition -------------------------------------------- */

typedef struct {
    const char*    name;         /* Column name (null-terminated) */
    pqflow_type    type;         /* Physical type */
    int32_t        type_length;  /* Byte length for FIXED_BYTE_ARRAY, 0 otherwise */
    int32_t        nullable;     /* 0 = required, 1 = optional */
} pqflow_column_def;

/* ---------- Sink configuration ------------------------------------------- */

typedef struct {
    const char*        file_path;          /* Output file path (null-terminated) */
    uint32_t           ring_buffer_size;   /* Power of 2, default 1 << 20 */
    uint32_t           batch_size;         /* Rows per batch, default 65536 */
    uint32_t           max_rows_per_file;  /* 0 = unlimited */
    pqflow_compression compression;        /* Compression codec */
} pqflow_config;

/* ---------- API functions ------------------------------------------------ */

/*
 * Create a new sink. Allocates internal resources and opens the output file.
 *
 * @param out     Receives the sink handle on success.
 * @param config  Sink configuration. Must not be NULL.
 * @return PQFLOW_OK on success, error code otherwise.
 */
pqflow_error pqflow_create(pqflow_sink_t* out, const pqflow_config* config);

/*
 * Define the record schema. Must be called before pqflow_log().
 *
 * @param sink         Sink handle.
 * @param columns      Array of column definitions.
 * @param num_columns  Number of columns.
 * @return PQFLOW_OK on success, error code otherwise.
 */
pqflow_error pqflow_set_schema(pqflow_sink_t sink,
                               const pqflow_column_def* columns,
                               uint32_t num_columns);

/*
 * Log a fixed-size binary record. Non-blocking; returns immediately.
 * If the ring buffer is full, the record is dropped and PQFLOW_ERR_FULL
 * is returned.
 *
 * @param sink    Sink handle.
 * @param record  Pointer to the binary record.
 * @param len     Length of the record in bytes.
 * @return PQFLOW_OK on success, PQFLOW_ERR_FULL if buffer is full.
 */
pqflow_error pqflow_log(pqflow_sink_t sink, const void* record, uint32_t len);

/*
 * Flush buffered records to disk. Blocks until the current batch is written.
 *
 * @param sink  Sink handle.
 * @return PQFLOW_OK on success, error code otherwise.
 */
pqflow_error pqflow_flush(pqflow_sink_t sink);

/*
 * Destroy the sink. Flushes remaining data and frees all resources.
 * Safe to call with NULL (no-op).
 *
 * @param sink  Sink handle, or NULL.
 */
void pqflow_destroy(pqflow_sink_t sink);

#ifdef __cplusplus
}
#endif

#endif /* PARQUET_FLOW_H */
