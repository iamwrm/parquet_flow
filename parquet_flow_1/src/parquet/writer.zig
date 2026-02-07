const builtin = @import("builtin");
const std = @import("std");
const compact = @import("compact.zig");

pub const PhysicalType = enum(i32) {
    boolean = 0,
    int32 = 1,
    int64 = 2,
    int96 = 3,
    float = 4,
    double = 5,
    byte_array = 6,
    fixed_len_byte_array = 7,
};

pub const Repetition = enum(i32) {
    required = 0,
    optional = 1,
    repeated = 2,
};

pub const Compression = enum(i32) {
    uncompressed = 0,
    gzip = 2,
};

const Encoding = enum(i32) {
    plain = 0,
    plain_dictionary = 2,
    rle = 3,
};

const PageType = enum(i32) {
    data_page = 0,
};

pub const ColumnDef = struct {
    name: []const u8,
    physical_type: PhysicalType,
    repetition: Repetition = .required,
    /// Required only for `.fixed_len_byte_array`, ignored otherwise.
    type_length: i32 = 0,
};

pub const ByteArrayColumn = struct {
    values: []const u8,
    /// Offsets of length `rows + 1`; each payload is `values[offsets[i]..offsets[i+1]]`.
    offsets: []const u32,
};

pub const FixedLenByteArrayColumn = struct {
    values: []const u8,
};

pub const ColumnData = union(PhysicalType) {
    boolean: []const u8,
    int32: []const i32,
    int64: []const i64,
    int96: []const [12]u8,
    float: []const f32,
    double: []const f64,
    byte_array: ByteArrayColumn,
    fixed_len_byte_array: FixedLenByteArrayColumn,
};

pub const ColumnLevels = struct {
    /// Definition levels per encoded value in this column chunk.
    /// - required columns: null or empty
    /// - optional columns: length = rows
    /// - repeated columns: length = number of encoded level entries
    definition_levels: ?[]const u8 = null,
    /// Repetition levels per encoded value in this column chunk.
    /// - required / optional columns: null or empty
    /// - repeated columns: same length as `definition_levels`
    repetition_levels: ?[]const u8 = null,
};

const OwnedColumnDef = struct {
    name: []u8,
    physical_type: PhysicalType,
    repetition: Repetition,
    type_length: i32,
};

const ColumnChunkMeta = struct {
    column_index: usize,
    data_page_offset: i64,
    total_compressed_size: i64,
    total_uncompressed_size: i64,
    num_values: i64,
};

const RowGroupMeta = struct {
    chunk_start: usize,
    chunk_count: usize,
    total_byte_size: i64,
    num_rows: i64,
};

pub const Writer = struct {
    allocator: std.mem.Allocator,
    io: std.Io,

    file: std.Io.File,
    file_writer: std.Io.File.Writer,
    file_buffer: [128 * 1024]u8 = undefined,

    columns: []OwnedColumnDef,
    compression: Compression,

    chunk_meta: std.ArrayList(ColumnChunkMeta) = .empty,
    row_group_meta: std.ArrayList(RowGroupMeta) = .empty,

    created_by: []const u8 = "parquet_flow.zig",
    total_rows: i64 = 0,
    closed: bool = false,

    scratch_page: std.ArrayList(u8) = .empty,
    scratch_compressed: std.ArrayList(u8) = .empty,
    scratch_header: std.ArrayList(u8) = .empty,
    scratch_levels: std.ArrayList(u8) = .empty,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, output_path: []const u8) !Writer {
        const schema = [_]ColumnDef{.{
            .name = "payload",
            .physical_type = .byte_array,
            .repetition = .required,
        }};
        return initWithSchema(allocator, io, output_path, &schema, .uncompressed);
    }

    pub fn initWithSchema(
        allocator: std.mem.Allocator,
        io: std.Io,
        output_path: []const u8,
        schema: []const ColumnDef,
        compression: Compression,
    ) !Writer {
        if (schema.len == 0) return error.InvalidSchema;

        var owned_schema = try allocator.alloc(OwnedColumnDef, schema.len);
        errdefer {
            for (owned_schema) |owned| {
                if (owned.name.len > 0) allocator.free(owned.name);
            }
            allocator.free(owned_schema);
        }

        for (schema, 0..) |column, i| {
            if (column.name.len == 0) {
                return error.InvalidColumnName;
            }
            if (column.physical_type == .fixed_len_byte_array and column.type_length <= 0) {
                return error.InvalidFixedTypeLength;
            }

            owned_schema[i] = .{
                .name = try allocator.dupe(u8, column.name),
                .physical_type = column.physical_type,
                .repetition = column.repetition,
                .type_length = column.type_length,
            };
        }

        const file = try std.Io.Dir.cwd().createFile(io, output_path, .{});
        errdefer file.close(io);

        var self: Writer = .{
            .allocator = allocator,
            .io = io,
            .file = file,
            .file_writer = undefined,
            .columns = owned_schema,
            .compression = compression,
        };

        self.file_writer = .init(file, io, &self.file_buffer);
        try self.file_writer.interface.writeAll("PAR1");

        return self;
    }

    /// Backward-compatible helper for single byte-array payload column.
    pub fn writeRowGroup(self: *Writer, payload_bytes: []const u8, payload_offsets: []const u32) !void {
        if (self.columns.len != 1 or self.columns[0].physical_type != .byte_array) {
            return error.SchemaMismatch;
        }

        if (payload_offsets.len == 0) return;

        const rows = payload_offsets.len - 1;
        const columns = [_]ColumnData{.{
            .byte_array = .{
                .values = payload_bytes,
                .offsets = payload_offsets,
            },
        }};
        try self.writeRowGroupColumns(rows, &columns);
    }

    pub fn writeRowGroupColumns(self: *Writer, rows: usize, columns: []const ColumnData) !void {
        return self.writeRowGroupColumnsWithLevels(rows, columns, null);
    }

    pub fn writeRowGroupColumnsWithLevels(
        self: *Writer,
        rows: usize,
        columns: []const ColumnData,
        levels: ?[]const ColumnLevels,
    ) !void {
        if (self.closed) return error.WriterClosed;
        if (rows == 0) return;
        if (columns.len != self.columns.len) return error.ColumnCountMismatch;
        if (rows > std.math.maxInt(i32)) return error.TooManyRows;
        if (levels) |level_columns| {
            if (level_columns.len != self.columns.len) return error.ColumnCountMismatch;
        }

        const chunk_start = self.chunk_meta.items.len;
        var row_group_total: i64 = 0;

        for (self.columns, columns, 0..) |column_def, column_data, column_index| {
            const column_levels = if (levels) |level_columns| level_columns[column_index] else ColumnLevels{};
            const column_encode = try self.encodeColumnPage(column_def, column_data, rows, column_levels);
            const uncompressed_size = column_encode.uncompressed_size;

            const encoded = try self.compressIfNeeded(self.scratch_page.items);
            if (encoded.len > std.math.maxInt(i32)) return error.PageTooLarge;
            const compressed_size: i32 = @intCast(encoded.len);
            if (column_encode.num_values > std.math.maxInt(i32)) return error.TooManyRows;

            self.scratch_header.clearRetainingCapacity();
            try encodePageHeader(
                self.allocator,
                &self.scratch_header,
                @intCast(uncompressed_size),
                compressed_size,
                @intCast(column_encode.num_values),
            );

            const chunk_start_offset: i64 = @intCast(self.file_writer.logicalPos());
            try self.file_writer.interface.writeAll(self.scratch_header.items);
            try self.file_writer.interface.writeAll(encoded);
            const chunk_end_offset: i64 = @intCast(self.file_writer.logicalPos());

            const total_compressed = chunk_end_offset - chunk_start_offset;
            const total_uncompressed: i64 = @as(i64, @intCast(self.scratch_header.items.len)) +
                @as(i64, @intCast(uncompressed_size));

            row_group_total += total_compressed;

            try self.chunk_meta.append(self.allocator, .{
                .column_index = column_index,
                .data_page_offset = chunk_start_offset,
                .total_compressed_size = total_compressed,
                .total_uncompressed_size = total_uncompressed,
                .num_values = @intCast(column_encode.num_values),
            });
        }

        try self.row_group_meta.append(self.allocator, .{
            .chunk_start = chunk_start,
            .chunk_count = self.columns.len,
            .total_byte_size = row_group_total,
            .num_rows = @intCast(rows),
        });

        self.total_rows += @intCast(rows);
    }

    pub fn close(self: *Writer) !void {
        if (self.closed) return;

        var metadata: std.ArrayList(u8) = .empty;
        defer metadata.deinit(self.allocator);

        try encodeFileMetaData(
            self.allocator,
            &metadata,
            self.total_rows,
            self.columns,
            self.compression,
            self.row_group_meta.items,
            self.chunk_meta.items,
            self.created_by,
        );

        try self.file_writer.interface.writeAll(metadata.items);

        if (metadata.items.len > std.math.maxInt(u32)) return error.MetadataTooLarge;
        var footer_len: [4]u8 = undefined;
        std.mem.writeInt(u32, &footer_len, @intCast(metadata.items.len), .little);
        try self.file_writer.interface.writeAll(&footer_len);
        try self.file_writer.interface.writeAll("PAR1");

        try self.file_writer.end();
        self.file.close(self.io);
        self.closed = true;
    }

    pub fn deinit(self: *Writer) void {
        if (!self.closed) {
            self.file.close(self.io);
            self.closed = true;
        }

        for (self.columns) |column| {
            self.allocator.free(column.name);
        }
        self.allocator.free(self.columns);

        self.chunk_meta.deinit(self.allocator);
        self.row_group_meta.deinit(self.allocator);

        self.scratch_page.deinit(self.allocator);
        self.scratch_compressed.deinit(self.allocator);
        self.scratch_header.deinit(self.allocator);
        self.scratch_levels.deinit(self.allocator);
    }

    fn compressIfNeeded(self: *Writer, raw: []const u8) ![]const u8 {
        switch (self.compression) {
            .uncompressed => return raw,
            .gzip => {
                self.scratch_compressed.clearRetainingCapacity();
                try compressGzip(self.allocator, raw, &self.scratch_compressed);
                return self.scratch_compressed.items;
            },
        }
    }

    fn encodeColumnPage(
        self: *Writer,
        column_def: OwnedColumnDef,
        data: ColumnData,
        rows: usize,
        levels: ColumnLevels,
    ) !struct {
        uncompressed_size: usize,
        num_values: usize,
    } {
        self.scratch_page.clearRetainingCapacity();
        const level_meta = try self.encodeLevelStreams(column_def.repetition, rows, levels);
        try self.encodePlainColumnValues(column_def, data, level_meta.value_count);

        return .{
            .uncompressed_size = self.scratch_page.items.len,
            .num_values = level_meta.num_values,
        };
    }

    fn encodePlainColumnValues(
        self: *Writer,
        column_def: OwnedColumnDef,
        data: ColumnData,
        value_count: usize,
    ) !void {
        switch (data) {
            .boolean => |values| {
                if (column_def.physical_type != .boolean) return error.ColumnTypeMismatch;
                if (values.len != value_count) return error.RowCountMismatch;

                const byte_len = (value_count + 7) / 8;
                try self.scratch_page.appendNTimes(self.allocator, 0, byte_len);
                for (values, 0..) |value, idx| {
                    if (value == 0) continue;
                    const byte_idx = idx / 8;
                    const bit_idx: u3 = @intCast(idx % 8);
                    self.scratch_page.items[byte_idx] |= @as(u8, 1) << bit_idx;
                }
            },
            .int32 => |values| {
                if (column_def.physical_type != .int32) return error.ColumnTypeMismatch;
                if (values.len != value_count) return error.RowCountMismatch;
                try encodeInt32Slice(self.allocator, &self.scratch_page, values);
            },
            .int64 => |values| {
                if (column_def.physical_type != .int64) return error.ColumnTypeMismatch;
                if (values.len != value_count) return error.RowCountMismatch;
                try encodeInt64Slice(self.allocator, &self.scratch_page, values);
            },
            .int96 => |values| {
                if (column_def.physical_type != .int96) return error.ColumnTypeMismatch;
                if (values.len != value_count) return error.RowCountMismatch;
                for (values) |value| {
                    try self.scratch_page.appendSlice(self.allocator, &value);
                }
            },
            .float => |values| {
                if (column_def.physical_type != .float) return error.ColumnTypeMismatch;
                if (values.len != value_count) return error.RowCountMismatch;
                try encodeFloat32Slice(self.allocator, &self.scratch_page, values);
            },
            .double => |values| {
                if (column_def.physical_type != .double) return error.ColumnTypeMismatch;
                if (values.len != value_count) return error.RowCountMismatch;
                try encodeFloat64Slice(self.allocator, &self.scratch_page, values);
            },
            .byte_array => |values| {
                if (column_def.physical_type != .byte_array) return error.ColumnTypeMismatch;
                if (values.offsets.len != value_count + 1) return error.RowCountMismatch;
                try encodeByteArraySlice(self.allocator, &self.scratch_page, values.values, values.offsets);
            },
            .fixed_len_byte_array => |values| {
                if (column_def.physical_type != .fixed_len_byte_array) return error.ColumnTypeMismatch;
                if (column_def.type_length <= 0) return error.InvalidFixedTypeLength;

                const type_len: usize = @intCast(column_def.type_length);
                const expected_len = try std.math.mul(usize, value_count, type_len);
                if (values.values.len != expected_len) return error.RowCountMismatch;
                try self.scratch_page.appendSlice(self.allocator, values.values);
            },
        }
    }

    fn encodeLevelStreams(
        self: *Writer,
        repetition: Repetition,
        rows: usize,
        levels: ColumnLevels,
    ) !struct {
        num_values: usize,
        value_count: usize,
    } {
        return switch (repetition) {
            .required => blk: {
                if (levels.definition_levels != null and levels.definition_levels.?.len > 0) {
                    return error.InvalidLevels;
                }
                if (levels.repetition_levels != null and levels.repetition_levels.?.len > 0) {
                    return error.InvalidLevels;
                }
                break :blk .{ .num_values = rows, .value_count = rows };
            },
            .optional => blk: {
                if (levels.repetition_levels != null and levels.repetition_levels.?.len > 0) {
                    return error.InvalidLevels;
                }
                if (levels.definition_levels) |definition_levels| {
                    if (definition_levels.len != rows) return error.RowCountMismatch;
                    const value_count = try countPresentLevels(definition_levels, 1);
                    try encodeLevelStream(self.allocator, &self.scratch_page, &self.scratch_levels, definition_levels, 1);
                    break :blk .{ .num_values = rows, .value_count = value_count };
                }
                try encodeConstantLevelStream(
                    self.allocator,
                    &self.scratch_page,
                    &self.scratch_levels,
                    rows,
                    1,
                    1,
                );
                break :blk .{ .num_values = rows, .value_count = rows };
            },
            .repeated => blk: {
                const definition_levels = levels.definition_levels orelse return error.InvalidLevels;
                const repetition_levels = levels.repetition_levels orelse return error.InvalidLevels;
                if (definition_levels.len != repetition_levels.len) return error.InvalidLevels;
                if (definition_levels.len == 0) return error.InvalidLevels;

                const row_count = try countRowsFromRepetitionLevels(repetition_levels);
                if (row_count != rows) return error.RowCountMismatch;

                const value_count = try countPresentLevels(definition_levels, 1);
                try encodeLevelStream(self.allocator, &self.scratch_page, &self.scratch_levels, repetition_levels, 1);
                try encodeLevelStream(self.allocator, &self.scratch_page, &self.scratch_levels, definition_levels, 1);
                break :blk .{
                    .num_values = definition_levels.len,
                    .value_count = value_count,
                };
            },
        };
    }
};

fn encodeInt32Slice(gpa: std.mem.Allocator, out: *std.ArrayList(u8), values: []const i32) !void {
    try out.ensureTotalCapacity(gpa, out.items.len + values.len * @sizeOf(i32));
    if (builtin.cpu.arch.endian() == .little) {
        try out.appendSlice(gpa, std.mem.sliceAsBytes(values));
        return;
    }

    for (values) |value| {
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(i32, &bytes, value, .little);
        try out.appendSlice(gpa, &bytes);
    }
}

fn encodeInt64Slice(gpa: std.mem.Allocator, out: *std.ArrayList(u8), values: []const i64) !void {
    try out.ensureTotalCapacity(gpa, out.items.len + values.len * @sizeOf(i64));
    if (builtin.cpu.arch.endian() == .little) {
        try out.appendSlice(gpa, std.mem.sliceAsBytes(values));
        return;
    }

    for (values) |value| {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(i64, &bytes, value, .little);
        try out.appendSlice(gpa, &bytes);
    }
}

fn encodeFloat32Slice(gpa: std.mem.Allocator, out: *std.ArrayList(u8), values: []const f32) !void {
    try out.ensureTotalCapacity(gpa, out.items.len + values.len * @sizeOf(f32));
    for (values) |value| {
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, @bitCast(value), .little);
        try out.appendSlice(gpa, &bytes);
    }
}

fn encodeFloat64Slice(gpa: std.mem.Allocator, out: *std.ArrayList(u8), values: []const f64) !void {
    try out.ensureTotalCapacity(gpa, out.items.len + values.len * @sizeOf(f64));
    for (values) |value| {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, @bitCast(value), .little);
        try out.appendSlice(gpa, &bytes);
    }
}

fn encodeByteArraySlice(
    gpa: std.mem.Allocator,
    out: *std.ArrayList(u8),
    values: []const u8,
    offsets: []const u32,
) !void {
    if (offsets.len == 0) return error.InvalidOffsets;
    if (offsets[0] != 0) return error.InvalidOffsets;
    if (offsets[offsets.len - 1] != values.len) return error.InvalidOffsets;

    for (0..offsets.len - 1) |i| {
        const start: usize = @intCast(offsets[i]);
        const end: usize = @intCast(offsets[i + 1]);
        if (end < start or end > values.len) return error.InvalidOffsets;

        const len: usize = end - start;
        if (len > std.math.maxInt(u32)) return error.PayloadTooLarge;

        var len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_bytes, @intCast(len), .little);
        try out.appendSlice(gpa, &len_bytes);
        try out.appendSlice(gpa, values[start..end]);
    }
}

fn countPresentLevels(levels: []const u8, max_definition_level: u8) !usize {
    var value_count: usize = 0;
    for (levels) |level| {
        if (level > max_definition_level) return error.InvalidLevels;
        if (level == max_definition_level) {
            value_count += 1;
        }
    }
    return value_count;
}

fn countRowsFromRepetitionLevels(levels: []const u8) !usize {
    if (levels.len == 0) return error.InvalidLevels;
    if (levels[0] != 0) return error.InvalidLevels;

    var row_count: usize = 0;
    for (levels) |level| {
        if (level > 1) return error.InvalidLevels;
        if (level == 0) {
            row_count += 1;
        }
    }
    return row_count;
}

fn encodeLevelStream(
    gpa: std.mem.Allocator,
    out: *std.ArrayList(u8),
    scratch: *std.ArrayList(u8),
    levels: []const u8,
    max_level: u8,
) !void {
    if (max_level == 0) {
        if (levels.len != 0) return error.InvalidLevels;
        return;
    }
    if (levels.len == 0) return error.InvalidLevels;
    const bit_width = levelBitWidth(max_level);
    const level_width_bytes = levelWidthBytes(bit_width);

    scratch.clearRetainingCapacity();
    var i: usize = 0;
    while (i < levels.len) {
        const level = levels[i];
        if (level > max_level) return error.InvalidLevels;

        var run_len: usize = 1;
        while (i + run_len < levels.len and levels[i + run_len] == level and run_len < std.math.maxInt(u32)) {
            run_len += 1;
        }

        const header: u64 = @as(u64, @intCast(run_len)) << 1;
        try appendUVarInt(scratch, gpa, header);
        try appendLevelValue(scratch, gpa, level, level_width_bytes);
        i += run_len;
    }

    if (scratch.items.len > std.math.maxInt(u32)) return error.PageTooLarge;
    var stream_len: [4]u8 = undefined;
    std.mem.writeInt(u32, &stream_len, @intCast(scratch.items.len), .little);
    try out.appendSlice(gpa, &stream_len);
    try out.appendSlice(gpa, scratch.items);
}

fn encodeConstantLevelStream(
    gpa: std.mem.Allocator,
    out: *std.ArrayList(u8),
    scratch: *std.ArrayList(u8),
    count: usize,
    level: u8,
    max_level: u8,
) !void {
    if (max_level == 0) {
        if (count != 0) return error.InvalidLevels;
        return;
    }
    if (count == 0) return error.InvalidLevels;
    if (level > max_level) return error.InvalidLevels;

    const bit_width = levelBitWidth(max_level);
    const level_width_bytes = levelWidthBytes(bit_width);

    scratch.clearRetainingCapacity();
    const header: u64 = @as(u64, @intCast(count)) << 1;
    try appendUVarInt(scratch, gpa, header);
    try appendLevelValue(scratch, gpa, level, level_width_bytes);

    if (scratch.items.len > std.math.maxInt(u32)) return error.PageTooLarge;
    var stream_len: [4]u8 = undefined;
    std.mem.writeInt(u32, &stream_len, @intCast(scratch.items.len), .little);
    try out.appendSlice(gpa, &stream_len);
    try out.appendSlice(gpa, scratch.items);
}

fn appendLevelValue(out: *std.ArrayList(u8), gpa: std.mem.Allocator, value: u8, width_bytes: usize) !void {
    var remainder: u64 = value;
    var i: usize = 0;
    while (i < width_bytes) : (i += 1) {
        try out.append(gpa, @intCast(remainder & 0xff));
        remainder >>= 8;
    }
}

fn levelBitWidth(max_level: u8) u8 {
    return @intCast(std.math.log2_int_ceil(u16, @as(u16, max_level) + 1));
}

fn levelWidthBytes(bit_width: u8) usize {
    return (bit_width + 7) / 8;
}

fn appendUVarInt(buf: *std.ArrayList(u8), gpa: std.mem.Allocator, value: u64) !void {
    var remaining = value;
    while (true) {
        var byte: u8 = @intCast(remaining & 0x7f);
        remaining >>= 7;
        if (remaining == 0) {
            try buf.append(gpa, byte);
            break;
        }
        byte |= 0x80;
        try buf.append(gpa, byte);
    }
}

fn compressGzip(gpa: std.mem.Allocator, input: []const u8, out: *std.ArrayList(u8)) !void {
    var compressed_writer: std.Io.Writer.Allocating = .init(gpa);
    defer compressed_writer.deinit();

    // The compressor requires output writer capacity above 8 bytes.
    try compressed_writer.ensureTotalCapacity(64);

    var flate_buffer: [std.compress.flate.max_window_len]u8 = undefined;
    var compressor = try std.compress.flate.Compress.Huffman.init(
        &compressed_writer.writer,
        &flate_buffer,
        .gzip,
    );
    try compressor.writer.writeAll(input);
    try compressor.writer.flush();

    const compressed = try compressed_writer.toOwnedSlice();
    defer gpa.free(compressed);
    try out.appendSlice(gpa, compressed);
}

fn encodePageHeader(
    gpa: std.mem.Allocator,
    buf: *std.ArrayList(u8),
    uncompressed_size: i32,
    compressed_size: i32,
    num_values: i32,
) !void {
    var last_field_id: i16 = 0;

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 1, .i32);
    try compact.writeI32(buf, gpa, @intFromEnum(PageType.data_page));

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 2, .i32);
    try compact.writeI32(buf, gpa, uncompressed_size);

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 3, .i32);
    try compact.writeI32(buf, gpa, compressed_size);

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 5, .@"struct");
    try encodeDataPageHeader(gpa, buf, num_values);

    try compact.writeStructStop(buf, gpa);
}

fn encodeDataPageHeader(gpa: std.mem.Allocator, buf: *std.ArrayList(u8), num_values: i32) !void {
    var last_field_id: i16 = 0;

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 1, .i32);
    try compact.writeI32(buf, gpa, num_values);

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 2, .i32);
    try compact.writeI32(buf, gpa, @intFromEnum(Encoding.plain));

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 3, .i32);
    try compact.writeI32(buf, gpa, @intFromEnum(Encoding.rle));

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 4, .i32);
    try compact.writeI32(buf, gpa, @intFromEnum(Encoding.rle));

    try compact.writeStructStop(buf, gpa);
}

fn encodeFileMetaData(
    gpa: std.mem.Allocator,
    buf: *std.ArrayList(u8),
    total_rows: i64,
    schema: []const OwnedColumnDef,
    compression: Compression,
    row_groups: []const RowGroupMeta,
    chunks: []const ColumnChunkMeta,
    created_by: []const u8,
) !void {
    var last_field_id: i16 = 0;

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 1, .i32);
    try compact.writeI32(buf, gpa, 1);

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 2, .list);
    try compact.writeListHeader(buf, gpa, .@"struct", schema.len + 1);
    try encodeSchemaRoot(gpa, buf, schema.len);
    for (schema) |column| {
        try encodeSchemaColumn(gpa, buf, column);
    }

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 3, .i64);
    try compact.writeI64(buf, gpa, total_rows);

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 4, .list);
    try compact.writeListHeader(buf, gpa, .@"struct", row_groups.len);
    for (row_groups) |row_group| {
        try encodeRowGroup(gpa, buf, row_group, chunks, schema, compression);
    }

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 6, .binary);
    try compact.writeBinary(buf, gpa, created_by);

    try compact.writeStructStop(buf, gpa);
}

fn encodeSchemaRoot(gpa: std.mem.Allocator, buf: *std.ArrayList(u8), column_count: usize) !void {
    var last_field_id: i16 = 0;

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 4, .binary);
    try compact.writeBinary(buf, gpa, "schema");

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 5, .i32);
    try compact.writeI32(buf, gpa, @intCast(column_count));

    try compact.writeStructStop(buf, gpa);
}

fn encodeSchemaColumn(gpa: std.mem.Allocator, buf: *std.ArrayList(u8), column: OwnedColumnDef) !void {
    var last_field_id: i16 = 0;

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 1, .i32);
    try compact.writeI32(buf, gpa, @intFromEnum(column.physical_type));

    if (column.physical_type == .fixed_len_byte_array) {
        try compact.writeFieldHeader(buf, gpa, &last_field_id, 2, .i32);
        try compact.writeI32(buf, gpa, column.type_length);
    }

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 3, .i32);
    try compact.writeI32(buf, gpa, @intFromEnum(column.repetition));

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 4, .binary);
    try compact.writeBinary(buf, gpa, column.name);

    try compact.writeStructStop(buf, gpa);
}

fn encodeRowGroup(
    gpa: std.mem.Allocator,
    buf: *std.ArrayList(u8),
    row_group: RowGroupMeta,
    chunks: []const ColumnChunkMeta,
    schema: []const OwnedColumnDef,
    compression: Compression,
) !void {
    var last_field_id: i16 = 0;

    const row_group_chunks = chunks[row_group.chunk_start .. row_group.chunk_start + row_group.chunk_count];

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 1, .list);
    try compact.writeListHeader(buf, gpa, .@"struct", row_group_chunks.len);
    for (row_group_chunks) |chunk| {
        try encodeColumnChunk(gpa, buf, chunk, schema[chunk.column_index], compression);
    }

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 2, .i64);
    try compact.writeI64(buf, gpa, row_group.total_byte_size);

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 3, .i64);
    try compact.writeI64(buf, gpa, row_group.num_rows);

    try compact.writeStructStop(buf, gpa);
}

fn encodeColumnChunk(
    gpa: std.mem.Allocator,
    buf: *std.ArrayList(u8),
    chunk: ColumnChunkMeta,
    column: OwnedColumnDef,
    compression: Compression,
) !void {
    var last_field_id: i16 = 0;

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 2, .i64);
    try compact.writeI64(buf, gpa, chunk.data_page_offset);

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 3, .@"struct");
    try encodeColumnMetaData(gpa, buf, chunk, column, compression);

    try compact.writeStructStop(buf, gpa);
}

fn encodeColumnMetaData(
    gpa: std.mem.Allocator,
    buf: *std.ArrayList(u8),
    chunk: ColumnChunkMeta,
    column: OwnedColumnDef,
    compression: Compression,
) !void {
    var last_field_id: i16 = 0;

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 1, .i32);
    try compact.writeI32(buf, gpa, @intFromEnum(column.physical_type));

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 2, .list);
    try compact.writeListHeader(buf, gpa, .i32, 2);
    try compact.writeI32(buf, gpa, @intFromEnum(Encoding.plain));
    try compact.writeI32(buf, gpa, @intFromEnum(Encoding.rle));

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 3, .list);
    try compact.writeListHeader(buf, gpa, .binary, 1);
    try compact.writeBinary(buf, gpa, column.name);

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 4, .i32);
    try compact.writeI32(buf, gpa, @intFromEnum(compression));

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 5, .i64);
    try compact.writeI64(buf, gpa, chunk.num_values);

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 6, .i64);
    try compact.writeI64(buf, gpa, chunk.total_uncompressed_size);

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 7, .i64);
    try compact.writeI64(buf, gpa, chunk.total_compressed_size);

    try compact.writeFieldHeader(buf, gpa, &last_field_id, 9, .i64);
    try compact.writeI64(buf, gpa, chunk.data_page_offset);

    try compact.writeStructStop(buf, gpa);
}
