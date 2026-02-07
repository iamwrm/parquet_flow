const std = @import("std");
const types = @import("types.zig");
const ThriftCompactEncoder = @import("thrift_encoder.zig").ThriftCompactEncoder;
const Type = @import("thrift_encoder.zig").Type;
const row_group_writer = @import("row_group_writer.zig");
const ColumnData = @import("column_writer.zig").ColumnData;
const LogEntry = @import("../log_entry.zig").LogEntry;

pub const WriteOptions = struct {
    codec: types.CompressionCodec = .UNCOMPRESSED,
    row_group_size: usize = 128 * 1024 * 1024, // 128MB default
};

/// Write a complete Parquet file from log entries.
pub fn writeLogEntries(
    allocator: std.mem.Allocator,
    path: []const u8,
    entries: []const LogEntry,
) !void {
    return writeLogEntriesWithOptions(allocator, path, entries, .{});
}

pub fn writeLogEntriesWithOptions(
    allocator: std.mem.Allocator,
    path: []const u8,
    entries: []const LogEntry,
    options: WriteOptions,
) !void {
    if (entries.len == 0) return;

    const num_rows = entries.len;
    var timestamps = try allocator.alloc(i64, num_rows);
    defer allocator.free(timestamps);
    var severities = try allocator.alloc(i32, num_rows);
    defer allocator.free(severities);
    var messages = try allocator.alloc([]const u8, num_rows);
    defer allocator.free(messages);
    var sources = try allocator.alloc([]const u8, num_rows);
    defer allocator.free(sources);
    var def_levels = try allocator.alloc(u8, num_rows);
    defer allocator.free(def_levels);

    for (entries, 0..) |*entry, i| {
        timestamps[i] = entry.timestamp_us;
        severities[i] = entry.severity;
        messages[i] = entry.getMessage();
        sources[i] = entry.getSource();
        def_levels[i] = if (entry.source_len > 0) 1 else 0;
    }

    const col_data = [_]ColumnData{
        .{ .i64_values = timestamps },
        .{ .i32_values = severities },
        .{ .byte_array_values = messages },
        .{ .byte_array_values = sources, .def_levels = def_levels },
    };

    const data = try writeParquetFile(allocator, &types.LOG_SCHEMA, &col_data, num_rows, options);
    defer allocator.free(data);

    try writeFileToDisk(path, data);
}

/// Build a complete Parquet file in memory and return the bytes.
pub fn writeParquetFile(
    allocator: std.mem.Allocator,
    schema: []const types.ColumnSpec,
    columns: []const ColumnData,
    num_rows: usize,
    options: WriteOptions,
) ![]u8 {
    var output: std.ArrayList(u8) = .empty;
    defer output.deinit(allocator);

    // 1. Write magic
    try output.appendSlice(allocator, types.PARQUET_MAGIC);

    // 2. Write row groups (split by row_group_size)
    var rg_infos: std.ArrayList(types.RowGroupInfo) = .empty;
    defer {
        for (rg_infos.items) |rgi| row_group_writer.freeRowGroupInfo(allocator, rgi);
        rg_infos.deinit(allocator);
    }

    var row_offset: usize = 0;
    while (row_offset < num_rows) {
        // Estimate rows per row group
        const remaining = num_rows - row_offset;
        const rg_rows = if (num_rows <= 1) remaining else blk: {
            const bytes_per_row = estimateBytesPerRow(columns, num_rows);
            const target_rows = if (bytes_per_row > 0) options.row_group_size / bytes_per_row else remaining;
            break :blk @min(@max(target_rows, 1), remaining);
        };

        // Slice column data for this row group
        var sliced: [32]ColumnData = undefined;
        for (columns, 0..) |col, i| {
            sliced[i] = sliceColumnData(col, row_offset, rg_rows);
        }

        const rg_info = try row_group_writer.writeRowGroup(
            allocator,
            &output,
            schema,
            sliced[0..columns.len],
            rg_rows,
            options.codec,
        );
        try rg_infos.append(allocator, rg_info);
        row_offset += rg_rows;
    }

    // 3. Write footer (FileMetaData via Thrift)
    const footer_start = output.items.len;
    try writeFileMetaData(allocator, &output, schema, rg_infos.items, @intCast(num_rows), options.codec);
    const footer_len: u32 = @intCast(output.items.len - footer_start);

    // 4. Write footer length (4 bytes LE)
    const footer_len_le = std.mem.nativeToLittle(u32, footer_len);
    try output.appendSlice(allocator, std.mem.asBytes(&footer_len_le));

    // 5. Write magic
    try output.appendSlice(allocator, types.PARQUET_MAGIC);

    return try output.toOwnedSlice(allocator);
}

fn estimateBytesPerRow(columns: []const ColumnData, num_rows: usize) usize {
    if (num_rows == 0) return 0;
    var total: usize = 0;
    for (columns) |col| {
        if (col.i32_values != null) {
            total += 4;
        } else if (col.i64_values != null) {
            total += 8;
        } else if (col.f32_values != null) {
            total += 4;
        } else if (col.f64_values != null) {
            total += 8;
        } else if (col.bool_values != null) {
            total += 1;
        } else if (col.byte_array_values) |vals| {
            // Sample first few values for average length
            var sample_bytes: usize = 0;
            const sample_count = @min(vals.len, 100);
            for (vals[0..sample_count]) |v| {
                sample_bytes += v.len + 4;
            }
            total += if (sample_count > 0) sample_bytes / sample_count else 32;
        } else if (col.fixed_byte_array_values) |vals| {
            if (vals.len > 0) total += vals[0].len;
        }
    }
    return total;
}

fn sliceColumnData(col: ColumnData, offset: usize, count: usize) ColumnData {
    return .{
        .i32_values = if (col.i32_values) |v| v[offset..][0..count] else null,
        .i64_values = if (col.i64_values) |v| v[offset..][0..count] else null,
        .f32_values = if (col.f32_values) |v| v[offset..][0..count] else null,
        .f64_values = if (col.f64_values) |v| v[offset..][0..count] else null,
        .byte_array_values = if (col.byte_array_values) |v| v[offset..][0..count] else null,
        .fixed_byte_array_values = if (col.fixed_byte_array_values) |v| v[offset..][0..count] else null,
        .bool_values = if (col.bool_values) |v| v[offset..][0..count] else null,
        .def_levels = if (col.def_levels) |v| v[offset..][0..count] else null,
    };
}

fn writeFileMetaData(
    allocator: std.mem.Allocator,
    output: *std.ArrayList(u8),
    schema: []const types.ColumnSpec,
    row_groups: []const types.RowGroupInfo,
    total_rows: i64,
    codec: types.CompressionCodec,
) !void {
    var enc = ThriftCompactEncoder.init(allocator);
    defer enc.deinit();

    enc.writeStructBegin();

    // field 1: version (i32) = 1
    try enc.writeI32Field(1, 1);

    // field 2: schema (list<SchemaElement>)
    try enc.writeListField(2, Type.STRUCT, @intCast(schema.len + 1));

    // Root SchemaElement
    {
        enc.writeStructBegin();
        try enc.writeBinaryField(4, "schema");
        try enc.writeI32Field(5, @intCast(schema.len));
        try enc.writeStructEnd();
    }

    // Column SchemaElements
    for (schema) |col| {
        enc.writeStructBegin();
        try enc.writeI32Field(1, @intFromEnum(col.physical_type));
        if (col.type_length) |tl| {
            try enc.writeI32Field(2, tl);
        }
        try enc.writeI32Field(3, @intFromEnum(col.repetition_type));
        try enc.writeBinaryField(4, col.name);
        if (col.converted_type) |ct| {
            try enc.writeI32Field(6, @intFromEnum(ct));
        }
        try enc.writeStructEnd();
    }

    // field 3: num_rows (i64)
    try enc.writeI64Field(3, total_rows);

    // field 4: row_groups (list<RowGroup>)
    try enc.writeListField(4, Type.STRUCT, @intCast(row_groups.len));
    for (row_groups) |rg| {
        try writeRowGroup(&enc, rg, schema, codec);
    }

    // field 6: created_by (binary)
    try enc.writeBinaryField(6, "parquet_flow_zig v0.2");

    try enc.writeStructEnd();

    try output.appendSlice(allocator, enc.getWritten());
}

fn writeRowGroup(enc: *ThriftCompactEncoder, rg: types.RowGroupInfo, schema: []const types.ColumnSpec, codec: types.CompressionCodec) !void {
    enc.writeStructBegin();

    try enc.writeListField(1, Type.STRUCT, @intCast(rg.columns.len));
    for (rg.columns, 0..) |col, i| {
        try writeColumnChunk(enc, col, schema[i], codec);
    }

    try enc.writeI64Field(2, rg.total_byte_size);
    try enc.writeI64Field(3, rg.num_rows);

    try enc.writeStructEnd();
}

fn writeColumnChunk(enc: *ThriftCompactEncoder, col: types.ColumnChunkInfo, spec: types.ColumnSpec, codec: types.CompressionCodec) !void {
    enc.writeStructBegin();

    try enc.writeI64Field(2, col.file_offset);

    try enc.writeStructField(3);
    {
        try enc.writeI32Field(1, @intFromEnum(spec.physical_type));

        try enc.writeListField(2, Type.I32, 2);
        try enc.writeI32(0); // PLAIN
        try enc.writeI32(3); // RLE

        try enc.writeListField(3, Type.BINARY, 1);
        try enc.writeBinary(spec.name);

        try enc.writeI32Field(4, @intFromEnum(codec));

        try enc.writeI64Field(5, col.num_values);
        try enc.writeI64Field(6, col.total_uncompressed_size);
        try enc.writeI64Field(7, col.total_compressed_size);
        try enc.writeI64Field(9, col.data_page_offset);
    }
    try enc.writeStructEnd(); // end ColumnMetaData

    try enc.writeStructEnd(); // end ColumnChunk
}

pub fn writeFileToDisk(path: []const u8, data: []const u8) !void {
    var path_buf: [4096:0]u8 = undefined;
    @memcpy(path_buf[0..path.len], path);
    path_buf[path.len] = 0;

    const fd = try std.posix.openat(
        std.posix.AT.FDCWD,
        path_buf[0..path.len :0],
        .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true },
        0o644,
    );
    defer std.posix.close(fd);

    var written: usize = 0;
    while (written < data.len) {
        const result = std.os.linux.write(fd, data[written..].ptr, data.len - written);
        const errno = std.os.linux.errno(result);
        if (errno != .SUCCESS) {
            return error.WriteFailed;
        }
        written += result;
    }
}

// ============================================================================
// Tests
// ============================================================================

test "write minimal parquet file" {
    const allocator = std.testing.allocator;

    const schema = [_]types.ColumnSpec{
        .{ .name = "value", .physical_type = .INT64, .repetition_type = .REQUIRED },
    };

    const values = [_]i64{ 42, 100, 999 };
    const col_data = [_]ColumnData{
        .{ .i64_values = &values },
    };

    const data = try writeParquetFile(allocator, &schema, &col_data, 3, .{});
    defer allocator.free(data);

    try std.testing.expectEqualSlices(u8, "PAR1", data[0..4]);
    try std.testing.expectEqualSlices(u8, "PAR1", data[data.len - 4 ..]);
    const footer_len = std.mem.readInt(u32, data[data.len - 8 .. data.len - 4][0..4], .little);
    try std.testing.expect(footer_len > 0);
    try std.testing.expect(footer_len < data.len);
}

test "write log entry parquet file" {
    const allocator = std.testing.allocator;

    var entries: [3]LogEntry = undefined;
    entries[0] = LogEntry.init(2, "hello", "main.zig");
    entries[1] = LogEntry.init(3, "warning msg", "server.zig");
    entries[2] = LogEntry.init(4, "error!", "db.zig");

    var timestamps: [3]i64 = undefined;
    var severities: [3]i32 = undefined;
    var messages: [3][]const u8 = undefined;
    var sources: [3][]const u8 = undefined;
    var def_levels: [3]u8 = undefined;

    for (&entries, 0..) |*e, i| {
        timestamps[i] = e.timestamp_us;
        severities[i] = e.severity;
        messages[i] = e.getMessage();
        sources[i] = e.getSource();
        def_levels[i] = if (e.source_len > 0) 1 else 0;
    }

    const col_data = [_]ColumnData{
        .{ .i64_values = &timestamps },
        .{ .i32_values = &severities },
        .{ .byte_array_values = &messages },
        .{ .byte_array_values = &sources, .def_levels = &def_levels },
    };

    const data = try writeParquetFile(allocator, &types.LOG_SCHEMA, &col_data, 3, .{});
    defer allocator.free(data);

    try std.testing.expectEqualSlices(u8, "PAR1", data[0..4]);
    try std.testing.expectEqualSlices(u8, "PAR1", data[data.len - 4 ..]);
}

test "write ZSTD compressed parquet file" {
    const allocator = std.testing.allocator;

    const schema = [_]types.ColumnSpec{
        .{ .name = "value", .physical_type = .INT64, .repetition_type = .REQUIRED },
    };

    const values = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    const col_data = [_]ColumnData{
        .{ .i64_values = &values },
    };

    const data = try writeParquetFile(allocator, &schema, &col_data, 10, .{ .codec = .ZSTD });
    defer allocator.free(data);

    try std.testing.expectEqualSlices(u8, "PAR1", data[0..4]);
    try std.testing.expectEqualSlices(u8, "PAR1", data[data.len - 4 ..]);
}

test "write DOUBLE column parquet file" {
    const allocator = std.testing.allocator;

    const schema = [_]types.ColumnSpec{
        .{ .name = "price", .physical_type = .DOUBLE, .repetition_type = .REQUIRED },
    };

    const values = [_]f64{ 1.5, 2.75, 3.14159 };
    const col_data = [_]ColumnData{
        .{ .f64_values = &values },
    };

    const data = try writeParquetFile(allocator, &schema, &col_data, 3, .{});
    defer allocator.free(data);

    try std.testing.expectEqualSlices(u8, "PAR1", data[0..4]);
    try std.testing.expectEqualSlices(u8, "PAR1", data[data.len - 4 ..]);
}
