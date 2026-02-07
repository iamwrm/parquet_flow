const std = @import("std");

/// Parquet magic bytes
pub const MAGIC: *const [4]u8 = "PAR1";

/// Parquet format version
pub const FORMAT_VERSION: i32 = 2;

/// Physical types supported by Parquet
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

/// Deprecated converted types (superseded by LogicalType)
pub const ConvertedType = enum(i32) {
    UTF8 = 0,
    MAP = 1,
    MAP_KEY_VALUE = 2,
    LIST = 3,
    ENUM = 4,
    DECIMAL = 5,
    DATE = 6,
    TIME_MILLIS = 7,
    TIME_MICROS = 8,
    TIMESTAMP_MILLIS = 9,
    TIMESTAMP_MICROS = 10,
    UINT_8 = 11,
    UINT_16 = 12,
    UINT_32 = 13,
    UINT_64 = 14,
    INT_8 = 15,
    INT_16 = 16,
    INT_32 = 17,
    INT_64 = 18,
    JSON = 19,
    BSON = 20,
    INTERVAL = 21,
};

/// Logical type annotations (union representation)
pub const LogicalType = union(enum) {
    STRING,
    MAP,
    LIST,
    ENUM,
    DECIMAL: struct { scale: i32, precision: i32 },
    DATE,
    TIME: struct { is_adjusted_to_utc: bool, unit: TimeUnit },
    TIMESTAMP: struct { is_adjusted_to_utc: bool, unit: TimeUnit },
    INTEGER: struct { bit_width: i8, is_signed: bool },
    UNKNOWN,
    JSON,
    BSON,
    UUID,
    FLOAT16,
};

pub const TimeUnit = enum {
    MILLIS,
    MICROS,
    NANOS,
};

/// Compression codecs
pub const CompressionCodec = enum(i32) {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    ZSTD = 6,
};

/// Encoding types
pub const Encoding = enum(i32) {
    PLAIN = 0,
    RLE = 3,
    RLE_DICTIONARY = 8,
};

/// Page types
pub const PageType = enum(i32) {
    DATA_PAGE = 0,
    DICTIONARY_PAGE = 2,
};

/// Field repetition types
pub const FieldRepetitionType = enum(i32) {
    REQUIRED = 0,
    OPTIONAL = 1,
    REPEATED = 2,
};
