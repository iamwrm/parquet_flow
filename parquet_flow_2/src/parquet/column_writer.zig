const std = @import("std");
const types = @import("types.zig");
const encoding = @import("encoding.zig");
const page_writer = @import("page_writer.zig");

/// Write one column chunk (all pages for one column within one row group).
/// Returns ColumnChunkInfo with offsets/sizes for the footer metadata.
pub fn writeColumnChunk(
    allocator: std.mem.Allocator,
    output: *std.ArrayList(u8),
    col_spec: types.ColumnSpec,
    col_data: ColumnData,
    num_rows: usize,
    codec: types.CompressionCodec,
) !types.ColumnChunkInfo {
    const data_page_offset: i64 = @intCast(output.items.len);

    // 1. Encode values with PLAIN encoding
    var encoded_values: std.ArrayList(u8) = .empty;
    defer encoded_values.deinit(allocator);

    switch (col_spec.physical_type) {
        .INT32 => try encoding.PlainEncoder.encodeI32(col_data.i32_values.?, allocator, &encoded_values),
        .INT64 => try encoding.PlainEncoder.encodeI64(col_data.i64_values.?, allocator, &encoded_values),
        .FLOAT => try encoding.PlainEncoder.encodeFloat(col_data.f32_values.?, allocator, &encoded_values),
        .DOUBLE => try encoding.PlainEncoder.encodeDouble(col_data.f64_values.?, allocator, &encoded_values),
        .BYTE_ARRAY => try encoding.PlainEncoder.encodeByteArray(col_data.byte_array_values.?, allocator, &encoded_values),
        .FIXED_LEN_BYTE_ARRAY => try encoding.PlainEncoder.encodeFixedLenByteArray(col_data.fixed_byte_array_values.?, allocator, &encoded_values),
        .BOOLEAN => try encoding.PlainEncoder.encodeBool(col_data.bool_values.?, allocator, &encoded_values),
        .INT96 => return error.UnsupportedType,
    }

    // 2. Encode definition levels if optional
    var encoded_def_levels: std.ArrayList(u8) = .empty;
    defer encoded_def_levels.deinit(allocator);

    const is_required = col_spec.repetition_type == .REQUIRED;
    if (!is_required) {
        if (col_data.def_levels) |levels| {
            try encoding.RleBitPackEncoder.encodeDefLevels(levels, 1, allocator, &encoded_def_levels);
        }
    }

    // 3. Write data page
    const num_values: u32 = @intCast(num_rows);
    _ = try page_writer.writeDataPageV1(
        allocator,
        output,
        encoded_values.items,
        encoded_def_levels.items,
        num_values,
        is_required,
        codec,
    );

    const total_size: i64 = @as(i64, @intCast(output.items.len)) - data_page_offset;

    return types.ColumnChunkInfo{
        .file_offset = data_page_offset,
        .total_uncompressed_size = total_size,
        .total_compressed_size = total_size,
        .num_values = @intCast(num_rows),
        .data_page_offset = data_page_offset,
    };
}

/// Column data container. Each field is an optional typed slice.
pub const ColumnData = struct {
    i32_values: ?[]const i32 = null,
    i64_values: ?[]const i64 = null,
    f32_values: ?[]const f32 = null,
    f64_values: ?[]const f64 = null,
    byte_array_values: ?[]const []const u8 = null,
    fixed_byte_array_values: ?[]const []const u8 = null,
    bool_values: ?[]const bool = null,
    def_levels: ?[]const u8 = null,
};

test "write INT64 column chunk" {
    const allocator = std.testing.allocator;
    var output: std.ArrayList(u8) = .empty;
    defer output.deinit(allocator);

    const values = [_]i64{ 100, 200, 300 };
    const info = try writeColumnChunk(
        allocator,
        &output,
        .{ .name = "test", .physical_type = .INT64, .repetition_type = .REQUIRED },
        .{ .i64_values = &values },
        3,
        .UNCOMPRESSED,
    );

    try std.testing.expect(info.total_compressed_size > 0);
    try std.testing.expectEqual(@as(i64, 3), info.num_values);
    try std.testing.expectEqual(@as(i64, 0), info.data_page_offset);
}

test "write optional BYTE_ARRAY column chunk" {
    const allocator = std.testing.allocator;
    var output: std.ArrayList(u8) = .empty;
    defer output.deinit(allocator);

    const strs = [_][]const u8{ "hello", "world" };
    const def_levels = [_]u8{ 1, 1 };
    const info = try writeColumnChunk(
        allocator,
        &output,
        .{ .name = "opt_col", .physical_type = .BYTE_ARRAY, .repetition_type = .OPTIONAL },
        .{ .byte_array_values = &strs, .def_levels = &def_levels },
        2,
        .UNCOMPRESSED,
    );

    try std.testing.expect(info.total_compressed_size > 0);
    try std.testing.expectEqual(@as(i64, 2), info.num_values);
}
