pub const PARQUET_MAGIC = "PAR1";

pub const PhysicalType = enum(i32) {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    INT96 = 3,
    FLOAT = 4,
    DOUBLE = 5,
    BYTE_ARRAY = 6,
    FIXED_LEN_BYTE_ARRAY = 7,
};

pub const Encoding = enum(i32) {
    PLAIN = 0,
    RLE = 3,
};

pub const CompressionCodec = enum(i32) {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    BROTLI = 4,
    ZSTD = 6,
    LZ4_RAW = 7,
};

pub const PageType = enum(i32) {
    DATA_PAGE = 0,
};

pub const FieldRepetitionType = enum(i32) {
    REQUIRED = 0,
    OPTIONAL = 1,
    REPEATED = 2,
};

pub const ConvertedType = enum(i32) {
    UTF8 = 0,
    TIMESTAMP_MILLIS = 9,
    TIMESTAMP_MICROS = 10,
};

pub const ColumnSpec = struct {
    name: []const u8,
    physical_type: PhysicalType,
    repetition_type: FieldRepetitionType,
    converted_type: ?ConvertedType = null,
    type_length: ?i32 = null,
};

pub const LOG_SCHEMA = [_]ColumnSpec{
    .{ .name = "timestamp_us", .physical_type = .INT64, .repetition_type = .REQUIRED, .converted_type = .TIMESTAMP_MICROS },
    .{ .name = "severity", .physical_type = .INT32, .repetition_type = .REQUIRED },
    .{ .name = "message", .physical_type = .BYTE_ARRAY, .repetition_type = .REQUIRED, .converted_type = .UTF8 },
    .{ .name = "source", .physical_type = .BYTE_ARRAY, .repetition_type = .OPTIONAL, .converted_type = .UTF8 },
};

pub const ColumnChunkInfo = struct {
    file_offset: i64,
    total_uncompressed_size: i64,
    total_compressed_size: i64,
    num_values: i64,
    data_page_offset: i64,
};

pub const RowGroupInfo = struct {
    columns: []const ColumnChunkInfo,
    total_byte_size: i64,
    num_rows: i64,
};
