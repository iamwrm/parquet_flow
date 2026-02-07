// Parquet modules
pub const parquet = @import("parquet/writer.zig");
pub const types = @import("parquet/types.zig");
pub const thrift = @import("parquet/thrift.zig");
pub const schema = @import("parquet/schema.zig");
pub const encoding = @import("parquet/encoding.zig");
pub const compression = @import("parquet/compression.zig");
pub const page = @import("parquet/page.zig");

// Sink modules
pub const log_sink = @import("sink/log_sink.zig");
pub const ring_buffer = @import("sink/ring_buffer.zig");
pub const batch = @import("sink/batch.zig");

// C API
pub const c_api = @import("c_api.zig");

// Ensure C API exports are compiled and linked.
comptime {
    _ = c_api;
}
