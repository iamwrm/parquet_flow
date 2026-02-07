const std = @import("std");
const types = @import("types.zig");
const column_writer = @import("column_writer.zig");
const ColumnData = column_writer.ColumnData;

/// Write all column chunks for one row group.
/// Returns RowGroupInfo with per-column metadata.
pub fn writeRowGroup(
    allocator: std.mem.Allocator,
    output: *std.ArrayList(u8),
    schema: []const types.ColumnSpec,
    columns: []const ColumnData,
    num_rows: usize,
    codec: types.CompressionCodec,
) !types.RowGroupInfo {
    const rg_start: i64 = @intCast(output.items.len);

    var col_infos = try allocator.alloc(types.ColumnChunkInfo, schema.len);
    errdefer allocator.free(col_infos);
    for (schema, 0..) |col_spec, i| {
        col_infos[i] = try column_writer.writeColumnChunk(
            allocator,
            output,
            col_spec,
            columns[i],
            num_rows,
            codec,
        );
    }

    const total_byte_size: i64 = @as(i64, @intCast(output.items.len)) - rg_start;

    return types.RowGroupInfo{
        .columns = col_infos,
        .total_byte_size = total_byte_size,
        .num_rows = @intCast(num_rows),
    };
}

pub fn freeRowGroupInfo(allocator: std.mem.Allocator, info: types.RowGroupInfo) void {
    allocator.free(info.columns);
}

test "write row group with two columns" {
    const allocator = std.testing.allocator;
    var output: std.ArrayList(u8) = .empty;
    defer output.deinit(allocator);

    const schema = [_]types.ColumnSpec{
        .{ .name = "id", .physical_type = .INT64, .repetition_type = .REQUIRED },
        .{ .name = "val", .physical_type = .INT32, .repetition_type = .REQUIRED },
    };

    const ids = [_]i64{ 1, 2, 3 };
    const vals = [_]i32{ 10, 20, 30 };

    const col_data = [_]ColumnData{
        .{ .i64_values = &ids },
        .{ .i32_values = &vals },
    };

    const info = try writeRowGroup(allocator, &output, &schema, &col_data, 3, .UNCOMPRESSED);
    defer freeRowGroupInfo(allocator, info);

    try std.testing.expectEqual(@as(i64, 3), info.num_rows);
    try std.testing.expectEqual(@as(usize, 2), info.columns.len);
    try std.testing.expect(info.total_byte_size > 0);
}
