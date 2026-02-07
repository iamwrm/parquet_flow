const std = @import("std");
const types = @import("parquet/types.zig");
const schema_mod = @import("parquet/schema.zig");
const log_sink = @import("sink/log_sink.zig");
const batch_mod = @import("sink/batch.zig");

// ---------------------------------------------------------------------------
// C-compatible struct definitions (must match include/parquet_flow.h)
// ---------------------------------------------------------------------------

pub const PqflowError = enum(i32) {
    OK = 0,
    ERR_FULL = 1,
    ERR_INVALID = 2,
    ERR_IO = 3,
    ERR_SCHEMA = 4,
};

pub const PqflowType = enum(i32) {
    BOOL = 0,
    I32 = 1,
    I64 = 2,
    I96 = 3,
    F32 = 4,
    F64 = 5,
    BYTE_ARRAY = 6,
    FIXED_BYTE_ARRAY = 7,
};

pub const PqflowCompression = enum(i32) {
    NONE = 0,
    SNAPPY = 1,
    GZIP = 2,
    ZSTD = 6,
};

pub const PqflowColumnDef = extern struct {
    name: [*:0]const u8,
    type: PqflowType,
    type_length: i32,
    nullable: i32,
};

pub const PqflowConfig = extern struct {
    file_path: [*:0]const u8,
    ring_buffer_size: u32,
    batch_size: u32,
    max_rows_per_file: u32,
    compression: PqflowCompression,
};

// ---------------------------------------------------------------------------
// Internal sink wrapper
// ---------------------------------------------------------------------------

const SinkState = struct {
    sink: ?*log_sink.LogSink,
    allocator: std.mem.Allocator,
    file_path: []const u8,
    batch_size: u32,
    compression: PqflowCompression,
    // Owned copies of schema data that must outlive the sink
    column_defs: []batch_mod.ColumnDef,
};

// Opaque handle exposed to C.
const SinkHandle = opaque {};

fn toState(handle: *SinkHandle) *SinkState {
    return @ptrCast(@alignCast(handle));
}

fn toHandle(state: *SinkState) *SinkHandle {
    return @ptrCast(state);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn mapPhysicalTypeToBatch(t: PqflowType) batch_mod.PhysicalType {
    return @enumFromInt(@intFromEnum(t));
}

/// Get the byte size of a physical type.
fn physicalTypeSize(t: PqflowType, type_length: i32) u32 {
    return switch (t) {
        .BOOL => 1,
        .I32 => 4,
        .I64 => 8,
        .I96 => 12,
        .F32 => 4,
        .F64 => 8,
        .BYTE_ARRAY => 4,
        .FIXED_BYTE_ARRAY => @intCast(type_length),
    };
}

// ---------------------------------------------------------------------------
// Exported C API functions
// ---------------------------------------------------------------------------

export fn pqflow_create(out: *?*SinkHandle, config: *const PqflowConfig) callconv(.c) i32 {
    return createImpl(out, config) catch @intFromEnum(PqflowError.ERR_IO);
}

fn createImpl(out: *?*SinkHandle, config: *const PqflowConfig) !i32 {
    const allocator = std.heap.c_allocator;

    const file_path = if (@intFromPtr(config.file_path) != 0)
        std.mem.span(config.file_path)
    else
        return @intFromEnum(PqflowError.ERR_INVALID);

    const state = try allocator.create(SinkState);
    state.* = .{
        .sink = null,
        .allocator = allocator,
        .file_path = file_path,
        .batch_size = if (config.batch_size != 0) config.batch_size else 65536,
        .compression = config.compression,
        .column_defs = &.{},
    };

    out.* = toHandle(state);
    return @intFromEnum(PqflowError.OK);
}

export fn pqflow_set_schema(
    handle: ?*SinkHandle,
    columns: [*]const PqflowColumnDef,
    num_columns: u32,
) callconv(.c) i32 {
    return setSchemaImpl(handle, columns, num_columns) catch @intFromEnum(PqflowError.ERR_SCHEMA);
}

fn setSchemaImpl(
    handle: ?*SinkHandle,
    columns: [*]const PqflowColumnDef,
    num_columns: u32,
) !i32 {
    const sink_handle = handle orelse return @intFromEnum(PqflowError.ERR_INVALID);
    const state = toState(sink_handle);

    if (num_columns == 0) {
        return @intFromEnum(PqflowError.ERR_INVALID);
    }

    const allocator = state.allocator;

    // Build batch ColumnDef array with computed offsets
    const col_defs = try allocator.alloc(batch_mod.ColumnDef, num_columns);
    errdefer allocator.free(col_defs);

    var nullable_count: u32 = 0;
    for (0..num_columns) |i| {
        if (columns[i].nullable != 0) {
            nullable_count += 1;
        }
    }
    const null_bitmap_bytes: u32 = (nullable_count + 7) / 8;
    var offset: u32 = null_bitmap_bytes;

    for (0..num_columns) |i| {
        const c = columns[i];
        const size = physicalTypeSize(c.type, c.type_length);
        col_defs[i] = .{
            .name = std.mem.span(c.name),
            .physical_type = mapPhysicalTypeToBatch(c.type),
            .type_length = @intCast(c.type_length),
            .nullable = c.nullable != 0,
            .offset = offset,
            .size = size,
        };
        offset += size;
    }

    const schema_info = batch_mod.SchemaInfo{
        .columns = col_defs,
        .record_size = offset,
        .nullable_count = nullable_count,
        .null_bitmap_bytes = null_bitmap_bytes,
    };

    const sink_config = log_sink.SinkConfig{
        .batch_size = state.batch_size,
        .file_path = state.file_path,
    };

    const sink = try log_sink.LogSink.init(sink_config, schema_info, allocator);

    // Clean up old state if re-setting schema
    if (state.sink) |old_sink| {
        old_sink.deinit();
    }
    if (state.column_defs.len > 0) {
        allocator.free(state.column_defs);
    }

    state.column_defs = col_defs;
    state.sink = sink;

    return @intFromEnum(PqflowError.OK);
}

export fn pqflow_log(
    handle: ?*SinkHandle,
    record: [*]const u8,
    len: u32,
) callconv(.c) i32 {
    const sink_handle = handle orelse return @intFromEnum(PqflowError.ERR_INVALID);
    const state = toState(sink_handle);
    const sink = state.sink orelse return @intFromEnum(PqflowError.ERR_SCHEMA);

    if (len == 0) {
        return @intFromEnum(PqflowError.ERR_INVALID);
    }

    sink.log(record[0..len]) catch |err| {
        return switch (err) {
            log_sink.LogError.BufferFull => @intFromEnum(PqflowError.ERR_FULL),
            log_sink.LogError.RecordTooLarge => @intFromEnum(PqflowError.ERR_INVALID),
        };
    };

    return @intFromEnum(PqflowError.OK);
}

export fn pqflow_flush(handle: ?*SinkHandle) callconv(.c) i32 {
    const sink_handle = handle orelse return @intFromEnum(PqflowError.ERR_INVALID);
    const state = toState(sink_handle);
    const sink = state.sink orelse return @intFromEnum(PqflowError.ERR_SCHEMA);

    sink.flush();

    return @intFromEnum(PqflowError.OK);
}

export fn pqflow_destroy(handle: ?*SinkHandle) callconv(.c) void {
    const sink_handle = handle orelse return;
    const state = toState(sink_handle);
    const allocator = state.allocator;

    if (state.sink) |sink| {
        sink.deinit();
    }

    if (state.column_defs.len > 0) {
        allocator.free(state.column_defs);
    }

    allocator.destroy(state);
}
