#ifndef PARQUET_FLOW_C_H
#define PARQUET_FLOW_C_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct pf_writer_handle pf_writer_handle_t;

typedef struct pf_column_input {
    const void *values;
    uint64_t values_len;
    const uint32_t *offsets;
    uint64_t offsets_len;
} pf_column_input_t;

typedef struct pf_column_input_with_levels {
    const void *values;
    uint64_t values_len;
    const uint32_t *offsets;
    uint64_t offsets_len;
    const uint8_t *definition_levels;
    uint64_t definition_levels_len;
    const uint8_t *repetition_levels;
    uint64_t repetition_levels_len;
} pf_column_input_with_levels_t;

enum {
    PF_STATUS_OK = 0,
    PF_STATUS_INVALID_ARGUMENT = 1,
    PF_STATUS_NOT_OPEN = 2,
    PF_STATUS_INTERNAL = 3,
    PF_STATUS_OUT_OF_MEMORY = 4,
};

enum {
    PF_PHYSICAL_BOOLEAN = 0,
    PF_PHYSICAL_INT32 = 1,
    PF_PHYSICAL_INT64 = 2,
    PF_PHYSICAL_INT96 = 3,
    PF_PHYSICAL_FLOAT = 4,
    PF_PHYSICAL_DOUBLE = 5,
    PF_PHYSICAL_BYTE_ARRAY = 6,
    PF_PHYSICAL_FIXED_LEN_BYTE_ARRAY = 7,
};

enum {
    PF_REPETITION_REQUIRED = 0,
    PF_REPETITION_OPTIONAL = 1,
    PF_REPETITION_REPEATED = 2,
};

enum {
    PF_COMPRESSION_UNCOMPRESSED = 0,
    PF_COMPRESSION_GZIP = 2,
};

pf_writer_handle_t *pf_writer_create(const char *output_path, int32_t compression_code);
void pf_writer_destroy(pf_writer_handle_t *handle);

int32_t pf_writer_add_column(
    pf_writer_handle_t *handle,
    const char *name,
    int32_t physical_type_code,
    int32_t repetition_code,
    uint32_t type_length);

int32_t pf_writer_open(pf_writer_handle_t *handle);

int32_t pf_writer_write_row_group(
    pf_writer_handle_t *handle,
    uint64_t row_count,
    const pf_column_input_t *column_inputs,
    uint32_t column_count);

int32_t pf_writer_write_row_group_with_levels(
    pf_writer_handle_t *handle,
    uint64_t row_count,
    const pf_column_input_with_levels_t *column_inputs,
    uint32_t column_count);

int32_t pf_writer_close(pf_writer_handle_t *handle);
const char *pf_writer_last_error(const pf_writer_handle_t *handle);

#ifdef __cplusplus
}
#endif

#endif
