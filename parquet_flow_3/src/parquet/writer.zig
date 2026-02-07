const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const thrift = @import("thrift.zig");
const CompactProtocolWriter = thrift.CompactProtocolWriter;
const schema_mod = @import("schema.zig");
const Schema = schema_mod.Schema;
const SchemaElement = schema_mod.SchemaElement;
const ColumnDef = schema_mod.ColumnDef;
const encoding = @import("encoding.zig");
const compression = @import("compression.zig");
const page_mod = @import("page.zig");

// Re-export sub-modules
pub const schema = schema_mod;
pub const parquet_types = types;
pub const parquet_thrift = thrift;
pub const parquet_encoding = encoding;
pub const parquet_compression = compression;
pub const parquet_page = page_mod;

// Force compile-time evaluation of all transitive imports
comptime {
    _ = types;
    _ = thrift;
    _ = schema_mod;
    _ = encoding;
    _ = compression;
    _ = page_mod;
}

/// Metadata for a single column chunk.
const ColumnChunkInfo = struct {
    physical_type: types.PhysicalType,
    path_in_schema: []const u8,
    codec: types.CompressionCodec,
    num_values: i64,
    total_uncompressed_size: i64,
    total_compressed_size: i64,
    data_page_offset: i64,
};

/// Writes data for a single column within a row group.
pub const ColumnWriter = struct {
    gpa: Allocator,
    column_def: ColumnDef,
    col_index: usize,
    codec: types.CompressionCodec,

    data_buf: std.ArrayList(u8),
    def_levels_buf: std.ArrayList(u8),
    num_values: i64,
    null_count: i64,

    pages_buf: std.ArrayList(u8),
    data_page_offset: ?i64,
    total_uncompressed_size: i64,
    total_compressed_size: i64,

    pub fn init(allocator: Allocator, col_def: ColumnDef, col_index: usize, codec: types.CompressionCodec) ColumnWriter {
        return .{
            .gpa = allocator,
            .column_def = col_def,
            .col_index = col_index,
            .codec = codec,
            .data_buf = .empty,
            .def_levels_buf = .empty,
            .num_values = 0,
            .null_count = 0,
            .pages_buf = .empty,
            .data_page_offset = null,
            .total_uncompressed_size = 0,
            .total_compressed_size = 0,
        };
    }

    pub fn deinit(self: *ColumnWriter) void {
        self.data_buf.deinit(self.gpa);
        self.def_levels_buf.deinit(self.gpa);
        self.pages_buf.deinit(self.gpa);
    }

    pub fn writeI32(self: *ColumnWriter, value: i32) !void {
        const bytes: [4]u8 = @bitCast(value);
        try self.data_buf.appendSlice(self.gpa, &bytes);
        if (self.column_def.repetition_type == .OPTIONAL) {
            try self.def_levels_buf.append(self.gpa, 1);
        }
        self.num_values += 1;
    }

    pub fn writeI64(self: *ColumnWriter, value: i64) !void {
        const bytes: [8]u8 = @bitCast(value);
        try self.data_buf.appendSlice(self.gpa, &bytes);
        if (self.column_def.repetition_type == .OPTIONAL) {
            try self.def_levels_buf.append(self.gpa, 1);
        }
        self.num_values += 1;
    }

    pub fn writeF32(self: *ColumnWriter, value: f32) !void {
        const bytes: [4]u8 = @bitCast(value);
        try self.data_buf.appendSlice(self.gpa, &bytes);
        if (self.column_def.repetition_type == .OPTIONAL) {
            try self.def_levels_buf.append(self.gpa, 1);
        }
        self.num_values += 1;
    }

    pub fn writeF64(self: *ColumnWriter, value: f64) !void {
        const bytes: [8]u8 = @bitCast(value);
        try self.data_buf.appendSlice(self.gpa, &bytes);
        if (self.column_def.repetition_type == .OPTIONAL) {
            try self.def_levels_buf.append(self.gpa, 1);
        }
        self.num_values += 1;
    }

    pub fn writeByteArray(self: *ColumnWriter, value: []const u8) !void {
        var len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_bytes, @intCast(value.len), .little);
        try self.data_buf.appendSlice(self.gpa, &len_bytes);
        try self.data_buf.appendSlice(self.gpa, value);
        if (self.column_def.repetition_type == .OPTIONAL) {
            try self.def_levels_buf.append(self.gpa, 1);
        }
        self.num_values += 1;
    }

    pub fn writeFixedByteArray(self: *ColumnWriter, value: []const u8) !void {
        try self.data_buf.appendSlice(self.gpa, value);
        if (self.column_def.repetition_type == .OPTIONAL) {
            try self.def_levels_buf.append(self.gpa, 1);
        }
        self.num_values += 1;
    }

    pub fn writeNull(self: *ColumnWriter) !void {
        try self.def_levels_buf.append(self.gpa, 0);
        self.num_values += 1;
        self.null_count += 1;
    }

    /// Flush accumulated values into a data page.
    pub fn flush(self: *ColumnWriter, file_offset: i64) !void {
        if (self.num_values == 0) return;

        // Encode definition levels if optional
        var def_level_data: ?[]u8 = null;
        defer if (def_level_data) |b| self.gpa.free(b);

        if (self.column_def.repetition_type == .OPTIONAL) {
            var dl_buf: std.ArrayList(u8) = .empty;
            defer dl_buf.deinit(self.gpa);

            try encoding.encodeDefinitionLevels(
                self.def_levels_buf.items,
                1,
                &dl_buf,
                self.gpa,
            );
            def_level_data = try dl_buf.toOwnedSlice(self.gpa);
        }

        var data_page = try page_mod.buildDataPage(
            self.data_buf.items,
            def_level_data,
            null,
            @intCast(self.num_values),
            .PLAIN,
            self.codec,
            self.gpa,
        );
        defer data_page.deinit();

        if (self.data_page_offset == null) {
            self.data_page_offset = file_offset + @as(i64, @intCast(self.pages_buf.items.len));
        }

        const page_total: i64 = @intCast(data_page.totalSize());
        self.total_compressed_size += page_total;
        self.total_uncompressed_size += page_total;

        try self.pages_buf.appendSlice(self.gpa, data_page.header_bytes);
        try self.pages_buf.appendSlice(self.gpa, data_page.data_bytes);

        self.data_buf.clearRetainingCapacity();
        self.def_levels_buf.clearRetainingCapacity();
    }

    fn getChunkInfo(self: *const ColumnWriter) ColumnChunkInfo {
        return .{
            .physical_type = self.column_def.physical_type,
            .path_in_schema = self.column_def.name,
            .codec = self.codec,
            .num_values = self.num_values,
            .total_uncompressed_size = self.total_uncompressed_size,
            .total_compressed_size = self.total_compressed_size,
            .data_page_offset = self.data_page_offset orelse 0,
        };
    }
};

/// Manages writing a single row group.
pub const RowGroupWriter = struct {
    gpa: Allocator,
    columns: std.ArrayList(ColumnWriter),
    num_rows: i64,

    pub fn init(allocator: Allocator, column_defs: []const ColumnDef, codec: types.CompressionCodec) !RowGroupWriter {
        var columns: std.ArrayList(ColumnWriter) = .empty;
        errdefer {
            for (columns.items) |*col| col.deinit();
            columns.deinit(allocator);
        }

        for (column_defs, 0..) |cd, i| {
            try columns.append(allocator, ColumnWriter.init(allocator, cd, i, codec));
        }

        return .{
            .gpa = allocator,
            .columns = columns,
            .num_rows = 0,
        };
    }

    pub fn deinit(self: *RowGroupWriter) void {
        for (self.columns.items) |*col| col.deinit();
        self.columns.deinit(self.gpa);
    }

    pub fn column(self: *RowGroupWriter, index: usize) *ColumnWriter {
        return &self.columns.items[index];
    }

    pub fn setNumRows(self: *RowGroupWriter, n: i64) void {
        self.num_rows = n;
    }
};

/// Top-level Parquet file writer.
/// Builds the entire file in an ArrayList(u8) buffer.
pub const FileWriter = struct {
    gpa: Allocator,
    output: std.ArrayList(u8),
    column_defs: []const ColumnDef,
    schema_val: Schema,
    row_groups_meta: std.ArrayList(RowGroupMeta),
    total_num_rows: i64,
    codec: types.CompressionCodec,
    closed: bool,

    const RowGroupMeta = struct {
        chunks: []ColumnChunkInfo,
        total_byte_size: i64,
        num_rows: i64,
    };

    pub fn init(allocator: Allocator, column_defs: []const ColumnDef, codec: types.CompressionCodec) !FileWriter {
        var output: std.ArrayList(u8) = .empty;
        errdefer output.deinit(allocator);

        try output.appendSlice(allocator, types.MAGIC);

        var s = try Schema.buildFromColumns(allocator, column_defs);
        errdefer s.deinit();

        return .{
            .gpa = allocator,
            .output = output,
            .column_defs = column_defs,
            .schema_val = s,
            .row_groups_meta = .empty,
            .total_num_rows = 0,
            .codec = codec,
            .closed = false,
        };
    }

    pub fn deinit(self: *FileWriter) void {
        self.output.deinit(self.gpa);
        self.schema_val.deinit();
        for (self.row_groups_meta.items) |meta| {
            self.gpa.free(meta.chunks);
        }
        self.row_groups_meta.deinit(self.gpa);
    }

    pub fn newRowGroup(self: *FileWriter) !RowGroupWriter {
        return RowGroupWriter.init(self.gpa, self.column_defs, self.codec);
    }

    pub fn closeRowGroup(self: *FileWriter, rg: *RowGroupWriter) !void {
        const base_offset: i64 = @intCast(self.output.items.len);

        var accumulated_offset: i64 = 0;
        for (rg.columns.items) |*col| {
            try col.flush(base_offset + accumulated_offset);
            accumulated_offset += @intCast(col.pages_buf.items.len);
        }

        var chunks = try self.gpa.alloc(ColumnChunkInfo, rg.columns.items.len);
        var total_byte_size: i64 = 0;

        for (rg.columns.items, 0..) |*col, i| {
            chunks[i] = col.getChunkInfo();
            total_byte_size += col.total_compressed_size;
        }

        for (rg.columns.items) |*col| {
            try self.output.appendSlice(self.gpa, col.pages_buf.items);
        }

        try self.row_groups_meta.append(self.gpa, .{
            .chunks = chunks,
            .total_byte_size = total_byte_size,
            .num_rows = rg.num_rows,
        });

        self.total_num_rows += rg.num_rows;
    }

    /// Finalize and return the complete file bytes.
    pub fn close(self: *FileWriter) ![]const u8 {
        if (self.closed) return self.output.items;
        self.closed = true;

        const footer_offset = self.output.items.len;

        var tw = CompactProtocolWriter.init(self.gpa);
        defer tw.deinit();

        try tw.writeFieldI32(1, types.FORMAT_VERSION);

        try tw.writeFieldList(2, .STRUCT, @intCast(self.schema_val.elements.items.len));
        for (self.schema_val.elements.items) |elem| {
            try tw.writeStructBegin();
            try schema_mod.writeSchemaElement(&tw, elem);
            try tw.writeStructEnd();
        }

        try tw.writeFieldI64(3, self.total_num_rows);

        try tw.writeFieldList(4, .STRUCT, @intCast(self.row_groups_meta.items.len));
        for (self.row_groups_meta.items) |rg_meta| {
            try tw.writeStructBegin();
            try writeRowGroup(&tw, rg_meta);
            try tw.writeStructEnd();
        }

        try tw.writeFieldString(6, "parquet_flow zig");

        try tw.writeFieldStop();

        try self.output.appendSlice(self.gpa, tw.getWritten());

        const metadata_len: u32 = @intCast(self.output.items.len - footer_offset);
        var len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_bytes, metadata_len, .little);
        try self.output.appendSlice(self.gpa, &len_bytes);

        try self.output.appendSlice(self.gpa, types.MAGIC);

        return self.output.items;
    }

    /// Write the finalized file to disk.
    pub fn writeToFile(self: *FileWriter, path: [*:0]const u8) !void {
        const file_bytes = try self.close();

        const fp = std.c.fopen(path, "wb") orelse return error.FileOpenFailed;
        defer _ = std.c.fclose(fp);

        var written: usize = 0;
        while (written < file_bytes.len) {
            const n = std.c.fwrite(file_bytes[written..].ptr, 1, file_bytes.len - written, fp);
            if (n == 0) return error.FileWriteFailed;
            written += n;
        }
    }
};

fn writeRowGroup(tw: *CompactProtocolWriter, meta: FileWriter.RowGroupMeta) !void {
    try tw.writeFieldList(1, .STRUCT, @intCast(meta.chunks.len));
    for (meta.chunks) |chunk| {
        try tw.writeStructBegin();
        try writeColumnChunk(tw, chunk);
        try tw.writeStructEnd();
    }

    try tw.writeFieldI64(2, meta.total_byte_size);
    try tw.writeFieldI64(3, meta.num_rows);
    try tw.writeFieldStop();
}

fn writeColumnChunk(tw: *CompactProtocolWriter, chunk: ColumnChunkInfo) !void {
    try tw.writeFieldI64(2, chunk.data_page_offset);

    try tw.writeFieldStruct(3);
    try tw.writeStructBegin();
    try writeColumnMetaData(tw, chunk);
    try tw.writeStructEnd();

    try tw.writeFieldStop();
}

fn writeColumnMetaData(tw: *CompactProtocolWriter, chunk: ColumnChunkInfo) !void {
    try tw.writeFieldI32(1, @intFromEnum(chunk.physical_type));

    try tw.writeFieldList(2, .I32, 2);
    try tw.writeI32(@intFromEnum(types.Encoding.PLAIN));
    try tw.writeI32(@intFromEnum(types.Encoding.RLE));

    try tw.writeFieldList(3, .BINARY, 1);
    try tw.writeString(chunk.path_in_schema);

    try tw.writeFieldI32(4, @intFromEnum(chunk.codec));

    try tw.writeFieldI64(5, chunk.num_values);
    try tw.writeFieldI64(6, chunk.total_uncompressed_size);
    try tw.writeFieldI64(7, chunk.total_compressed_size);
    try tw.writeFieldI64(9, chunk.data_page_offset);

    try tw.writeFieldStop();
}

// ---- Tests ----

const testing_alloc = std.testing;

test "thrift varint encoding" {
    var tw = CompactProtocolWriter.init(testing_alloc.allocator);
    defer tw.deinit();

    try tw.writeVarint(0);
    try testing_alloc.expectEqualSlices(u8, &.{0x00}, tw.getWritten());
    tw.reset();

    try tw.writeVarint(1);
    try testing_alloc.expectEqualSlices(u8, &.{0x01}, tw.getWritten());
    tw.reset();

    try tw.writeVarint(127);
    try testing_alloc.expectEqualSlices(u8, &.{0x7F}, tw.getWritten());
    tw.reset();

    try tw.writeVarint(128);
    try testing_alloc.expectEqualSlices(u8, &.{ 0x80, 0x01 }, tw.getWritten());
    tw.reset();

    try tw.writeVarint(300);
    try testing_alloc.expectEqualSlices(u8, &.{ 0xAC, 0x02 }, tw.getWritten());
}

test "thrift zigzag encoding" {
    var tw = CompactProtocolWriter.init(testing_alloc.allocator);
    defer tw.deinit();

    try tw.writeZigZag(0);
    try testing_alloc.expectEqualSlices(u8, &.{0x00}, tw.getWritten());
    tw.reset();

    try tw.writeZigZag(-1);
    try testing_alloc.expectEqualSlices(u8, &.{0x01}, tw.getWritten());
    tw.reset();

    try tw.writeZigZag(1);
    try testing_alloc.expectEqualSlices(u8, &.{0x02}, tw.getWritten());
    tw.reset();

    try tw.writeZigZag(-2);
    try testing_alloc.expectEqualSlices(u8, &.{0x03}, tw.getWritten());
}

test "thrift field delta encoding" {
    var tw = CompactProtocolWriter.init(testing_alloc.allocator);
    defer tw.deinit();

    try tw.writeFieldBegin(.I32, 1);
    try testing_alloc.expectEqual(@as(u8, 0x15), tw.getWritten()[0]);
    try tw.writeI32(42);

    try tw.writeFieldBegin(.BINARY, 2);
    try testing_alloc.expectEqual(@as(u8, 0x18), tw.getWritten()[tw.getWritten().len - 1]);
}

test "schema element count" {
    const columns = [_]ColumnDef{
        .{ .name = "a", .physical_type = .INT32 },
        .{ .name = "b", .physical_type = .INT64 },
    };

    var s = try Schema.buildFromColumns(testing_alloc.allocator, &columns);
    defer s.deinit();

    try testing_alloc.expectEqual(@as(usize, 3), s.elements.items.len);
    try testing_alloc.expectEqualSlices(u8, "schema", s.elements.items[0].name);
    try testing_alloc.expectEqual(@as(?i32, 2), s.elements.items[0].num_children);
}

test "write simple parquet file" {
    const allocator = testing_alloc.allocator;
    const columns = [_]ColumnDef{
        .{ .name = "timestamp_ns", .physical_type = .INT64, .repetition_type = .REQUIRED },
        .{ .name = "value", .physical_type = .DOUBLE, .repetition_type = .REQUIRED },
        .{ .name = "name", .physical_type = .BYTE_ARRAY, .repetition_type = .REQUIRED },
    };

    var fw = try FileWriter.init(allocator, &columns, .UNCOMPRESSED);
    defer fw.deinit();

    var rg = try fw.newRowGroup();
    defer rg.deinit();

    try rg.column(0).writeI64(1000000000);
    try rg.column(1).writeF64(3.14);
    try rg.column(2).writeByteArray("hello");

    try rg.column(0).writeI64(2000000000);
    try rg.column(1).writeF64(2.718);
    try rg.column(2).writeByteArray("world");

    try rg.column(0).writeI64(3000000000);
    try rg.column(1).writeF64(1.414);
    try rg.column(2).writeByteArray("test");

    rg.setNumRows(3);
    try fw.closeRowGroup(&rg);

    const file_bytes = try fw.close();

    try testing_alloc.expectEqualSlices(u8, "PAR1", file_bytes[0..4]);
    try testing_alloc.expectEqualSlices(u8, "PAR1", file_bytes[file_bytes.len - 4 ..]);

    const meta_len = std.mem.readInt(u32, file_bytes[file_bytes.len - 8 ..][0..4], .little);
    try testing_alloc.expect(meta_len > 0);
    try testing_alloc.expect(meta_len < file_bytes.len);

    const footer_start = file_bytes.len - 8 - meta_len;
    try testing_alloc.expect(footer_start >= 4);
}

test "write parquet file with optional column" {
    const allocator = testing_alloc.allocator;
    const columns = [_]ColumnDef{
        .{ .name = "id", .physical_type = .INT64, .repetition_type = .REQUIRED },
        .{ .name = "label", .physical_type = .BYTE_ARRAY, .repetition_type = .OPTIONAL },
    };

    var fw = try FileWriter.init(allocator, &columns, .UNCOMPRESSED);
    defer fw.deinit();

    var rg = try fw.newRowGroup();
    defer rg.deinit();

    try rg.column(0).writeI64(1);
    try rg.column(1).writeByteArray("foo");

    try rg.column(0).writeI64(2);
    try rg.column(1).writeNull();

    try rg.column(0).writeI64(3);
    try rg.column(1).writeByteArray("bar");

    rg.setNumRows(3);
    try fw.closeRowGroup(&rg);

    const file_bytes = try fw.close();

    try testing_alloc.expectEqualSlices(u8, "PAR1", file_bytes[0..4]);
    try testing_alloc.expectEqualSlices(u8, "PAR1", file_bytes[file_bytes.len - 4 ..]);
    try testing_alloc.expect(file_bytes.len > 20);
}
