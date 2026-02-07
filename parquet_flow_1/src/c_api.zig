const std = @import("std");
const parquet = @import("parquet/writer.zig");

pub const PfColumnInput = extern struct {
    values: ?*const anyopaque,
    values_len: u64,
    offsets: ?[*]const u32,
    offsets_len: u64,
};

pub const PfColumnInputWithLevels = extern struct {
    values: ?*const anyopaque,
    values_len: u64,
    offsets: ?[*]const u32,
    offsets_len: u64,
    definition_levels: ?[*]const u8,
    definition_levels_len: u64,
    repetition_levels: ?[*]const u8,
    repetition_levels_len: u64,
};

const PF_STATUS_OK: c_int = 0;
const PF_STATUS_INVALID_ARGUMENT: c_int = 1;
const PF_STATUS_NOT_OPEN: c_int = 2;
const PF_STATUS_INTERNAL: c_int = 3;
const PF_STATUS_OUT_OF_MEMORY: c_int = 4;

const CWriter = struct {
    allocator: std.mem.Allocator = std.heap.c_allocator,
    output_path: []u8,
    compression: parquet.Compression,

    column_defs: std.ArrayList(parquet.ColumnDef) = .empty,
    column_names: std.ArrayList([]u8) = .empty,

    threaded: ?std.Io.Threaded = null,
    writer: ?parquet.Writer = null,

    last_error: [512]u8 = [_]u8{0} ** 512,

    fn setLastError(self: *CWriter, msg: []const u8) void {
        const n = @min(msg.len, self.last_error.len - 1);
        @memcpy(self.last_error[0..n], msg[0..n]);
        self.last_error[n] = 0;
    }

    fn clearLastError(self: *CWriter) void {
        self.last_error[0] = 0;
    }

    fn fail(self: *CWriter, status: c_int, msg: []const u8) c_int {
        self.setLastError(msg);
        return status;
    }

    fn failError(self: *CWriter, err: anyerror) c_int {
        return self.fail(mapStatus(err), @errorName(err));
    }

    fn destroy(self: *CWriter) void {
        if (self.writer) |*writer| {
            writer.deinit();
            self.writer = null;
        }
        if (self.threaded) |*threaded| {
            threaded.deinit();
            self.threaded = null;
        }

        for (self.column_names.items) |name| {
            self.allocator.free(name);
        }
        self.column_defs.deinit(self.allocator);
        self.column_names.deinit(self.allocator);

        self.allocator.free(self.output_path);
        self.allocator.destroy(self);
    }
};

var empty_error: [1:0]u8 = .{0};

export fn pf_writer_create(output_path: [*:0]const u8, compression_code: c_int) ?*CWriter {
    const compression = parseCompression(compression_code) orelse return null;

    const allocator = std.heap.c_allocator;
    const path = std.mem.span(output_path);
    if (path.len == 0) return null;

    const path_copy = allocator.dupe(u8, path) catch return null;
    errdefer allocator.free(path_copy);

    const handle = allocator.create(CWriter) catch return null;
    handle.* = .{
        .output_path = path_copy,
        .compression = compression,
    };

    return handle;
}

export fn pf_writer_destroy(handle: ?*CWriter) void {
    if (handle) |h| {
        h.destroy();
    }
}

export fn pf_writer_add_column(
    handle: *CWriter,
    name: [*:0]const u8,
    physical_type_code: c_int,
    repetition_code: c_int,
    type_length: u32,
) c_int {
    if (handle.writer != null) {
        return handle.fail(PF_STATUS_INVALID_ARGUMENT, "writer already open");
    }

    const physical_type = parsePhysicalType(physical_type_code) orelse {
        return handle.fail(PF_STATUS_INVALID_ARGUMENT, "invalid physical type");
    };
    const repetition = parseRepetition(repetition_code) orelse {
        return handle.fail(PF_STATUS_INVALID_ARGUMENT, "invalid repetition type");
    };

    const allocator = handle.allocator;
    const name_slice = std.mem.span(name);
    if (name_slice.len == 0) {
        return handle.fail(PF_STATUS_INVALID_ARGUMENT, "column name cannot be empty");
    }

    const name_copy = allocator.dupe(u8, name_slice) catch {
        return handle.fail(PF_STATUS_OUT_OF_MEMORY, "out of memory");
    };
    errdefer allocator.free(name_copy);

    handle.column_names.append(allocator, name_copy) catch {
        return handle.fail(PF_STATUS_OUT_OF_MEMORY, "out of memory");
    };
    errdefer _ = handle.column_names.pop();

    handle.column_defs.append(allocator, .{
        .name = name_copy,
        .physical_type = physical_type,
        .repetition = repetition,
        .type_length = @intCast(type_length),
    }) catch {
        return handle.fail(PF_STATUS_OUT_OF_MEMORY, "out of memory");
    };

    handle.clearLastError();
    return PF_STATUS_OK;
}

export fn pf_writer_open(handle: *CWriter) c_int {
    if (handle.writer != null) {
        return PF_STATUS_OK;
    }
    if (handle.column_defs.items.len == 0) {
        return handle.fail(PF_STATUS_INVALID_ARGUMENT, "no columns configured");
    }

    var threaded = std.Io.Threaded.init(handle.allocator, .{ .environ = .empty });
    const writer = parquet.Writer.initWithSchema(
        handle.allocator,
        threaded.io(),
        handle.output_path,
        handle.column_defs.items,
        handle.compression,
    ) catch |err| {
        threaded.deinit();
        return handle.failError(err);
    };

    handle.threaded = threaded;
    handle.writer = writer;
    handle.clearLastError();
    return PF_STATUS_OK;
}

export fn pf_writer_write_row_group(
    handle: *CWriter,
    row_count: u64,
    column_inputs: ?[*]const PfColumnInput,
    column_count: u32,
) c_int {
    if (handle.writer == null) {
        return handle.fail(PF_STATUS_NOT_OPEN, "writer not open");
    }

    if (column_count != handle.column_defs.items.len) {
        return handle.fail(PF_STATUS_INVALID_ARGUMENT, "column count mismatch");
    }

    const rows: usize = toUsize(row_count) catch {
        return handle.fail(PF_STATUS_INVALID_ARGUMENT, "row_count overflow");
    };

    if (column_count > 0 and column_inputs == null) {
        return handle.fail(PF_STATUS_INVALID_ARGUMENT, "column inputs pointer is null");
    }

    const allocator = handle.allocator;
    var converted = allocator.alloc(parquet.ColumnData, column_count) catch {
        return handle.fail(PF_STATUS_OUT_OF_MEMORY, "out of memory");
    };
    defer allocator.free(converted);

    const inputs = if (column_count == 0)
        (&[_]PfColumnInput{})
    else
        column_inputs.?[0..column_count];

    for (0..column_count) |idx| {
        convertColumnInput(handle.column_defs.items[idx], inputs[idx], rows, &converted[idx]) catch |err| {
            return handle.failError(err);
        };
    }

    if (handle.writer) |*writer| {
        writer.writeRowGroupColumns(rows, converted) catch |err| {
            return handle.failError(err);
        };
    } else {
        return handle.fail(PF_STATUS_NOT_OPEN, "writer not open");
    }

    handle.clearLastError();
    return PF_STATUS_OK;
}

export fn pf_writer_write_row_group_with_levels(
    handle: *CWriter,
    row_count: u64,
    column_inputs: ?[*]const PfColumnInputWithLevels,
    column_count: u32,
) c_int {
    if (handle.writer == null) {
        return handle.fail(PF_STATUS_NOT_OPEN, "writer not open");
    }

    if (column_count != handle.column_defs.items.len) {
        return handle.fail(PF_STATUS_INVALID_ARGUMENT, "column count mismatch");
    }

    const rows: usize = toUsize(row_count) catch {
        return handle.fail(PF_STATUS_INVALID_ARGUMENT, "row_count overflow");
    };

    if (column_count > 0 and column_inputs == null) {
        return handle.fail(PF_STATUS_INVALID_ARGUMENT, "column inputs pointer is null");
    }

    const allocator = handle.allocator;
    var converted = allocator.alloc(parquet.ColumnData, column_count) catch {
        return handle.fail(PF_STATUS_OUT_OF_MEMORY, "out of memory");
    };
    defer allocator.free(converted);

    var converted_levels = allocator.alloc(parquet.ColumnLevels, column_count) catch {
        return handle.fail(PF_STATUS_OUT_OF_MEMORY, "out of memory");
    };
    defer allocator.free(converted_levels);

    const inputs = if (column_count == 0)
        (&[_]PfColumnInputWithLevels{})
    else
        column_inputs.?[0..column_count];

    for (0..column_count) |idx| {
        const input = inputs[idx];
        convertColumnInput(handle.column_defs.items[idx], .{
            .values = input.values,
            .values_len = input.values_len,
            .offsets = input.offsets,
            .offsets_len = input.offsets_len,
        }, null, &converted[idx]) catch |err| {
            return handle.failError(err);
        };

        converted_levels[idx] = convertLevelsInput(input) catch |err| {
            return handle.failError(err);
        };
    }

    if (handle.writer) |*writer| {
        writer.writeRowGroupColumnsWithLevels(rows, converted, converted_levels) catch |err| {
            return handle.failError(err);
        };
    } else {
        return handle.fail(PF_STATUS_NOT_OPEN, "writer not open");
    }

    handle.clearLastError();
    return PF_STATUS_OK;
}

export fn pf_writer_close(handle: *CWriter) c_int {
    if (handle.writer == null) {
        return PF_STATUS_NOT_OPEN;
    }

    if (handle.writer) |*writer| {
        writer.close() catch |err| {
            return handle.failError(err);
        };
        writer.deinit();
        handle.writer = null;
    }

    if (handle.threaded) |*threaded| {
        threaded.deinit();
        handle.threaded = null;
    }

    handle.clearLastError();
    return PF_STATUS_OK;
}

export fn pf_writer_last_error(handle: ?*const CWriter) [*:0]const u8 {
    const h = handle orelse return &empty_error;
    return @ptrCast(&h.last_error[0]);
}

fn convertColumnInput(
    column: parquet.ColumnDef,
    input: PfColumnInput,
    expected_values: ?usize,
    out: *parquet.ColumnData,
) !void {
    switch (column.physical_type) {
        .boolean => {
            const values = try getSlice(u8, input.values, input.values_len);
            if (expected_values) |expected| {
                if (values.len != expected) return error.RowCountMismatch;
            }
            out.* = .{ .boolean = values };
        },
        .int32 => {
            const values = try getAlignedSlice(i32, input.values, input.values_len);
            if (expected_values) |expected| {
                if (values.len != expected) return error.RowCountMismatch;
            }
            out.* = .{ .int32 = values };
        },
        .int64 => {
            const values = try getAlignedSlice(i64, input.values, input.values_len);
            if (expected_values) |expected| {
                if (values.len != expected) return error.RowCountMismatch;
            }
            out.* = .{ .int64 = values };
        },
        .int96 => {
            const values_len = try toUsize(input.values_len);
            if (values_len % 12 != 0) return error.InvalidArgument;
            if (expected_values) |expected| {
                const expected_bytes = try std.math.mul(usize, expected, 12);
                if (values_len != expected_bytes) return error.RowCountMismatch;
            }
            const raw = input.values orelse return error.InvalidArgument;
            const ptr: [*]const [12]u8 = @ptrCast(raw);
            out.* = .{ .int96 = ptr[0 .. values_len / 12] };
        },
        .float => {
            const values = try getAlignedSlice(f32, input.values, input.values_len);
            if (expected_values) |expected| {
                if (values.len != expected) return error.RowCountMismatch;
            }
            out.* = .{ .float = values };
        },
        .double => {
            const values = try getAlignedSlice(f64, input.values, input.values_len);
            if (expected_values) |expected| {
                if (values.len != expected) return error.RowCountMismatch;
            }
            out.* = .{ .double = values };
        },
        .byte_array => {
            const values = try getSlice(u8, input.values, input.values_len);
            const offsets_len = try toUsize(input.offsets_len);
            const offsets_ptr = input.offsets orelse return error.InvalidArgument;
            const offsets = offsets_ptr[0..offsets_len];

            if (expected_values) |expected| {
                if (offsets.len != expected + 1) return error.RowCountMismatch;
            } else if (offsets.len == 0) {
                return error.InvalidArgument;
            }
            out.* = .{ .byte_array = .{ .values = values, .offsets = offsets } };
        },
        .fixed_len_byte_array => {
            if (column.type_length <= 0) return error.InvalidFixedTypeLength;
            const values = try getSlice(u8, input.values, input.values_len);
            const type_length: usize = @intCast(column.type_length);
            if (expected_values) |expected| {
                const expected_len = try std.math.mul(usize, expected, type_length);
                if (values.len != expected_len) return error.RowCountMismatch;
            } else if (values.len % type_length != 0) {
                return error.RowCountMismatch;
            }
            out.* = .{ .fixed_len_byte_array = .{ .values = values } };
        },
    }
}

fn convertLevelsInput(input: PfColumnInputWithLevels) !parquet.ColumnLevels {
    const definition_levels = try getOptionalSliceU8(input.definition_levels, input.definition_levels_len);
    const repetition_levels = try getOptionalSliceU8(input.repetition_levels, input.repetition_levels_len);
    return .{
        .definition_levels = definition_levels,
        .repetition_levels = repetition_levels,
    };
}

fn getSlice(comptime T: type, ptr: ?*const anyopaque, len64: u64) ![]const T {
    const len = try toUsize(len64);
    if (len == 0) return &.{};
    const raw = ptr orelse return error.InvalidArgument;
    const typed: [*]const T = @ptrCast(raw);
    return typed[0..len];
}

fn getAlignedSlice(comptime T: type, ptr: ?*const anyopaque, len64: u64) ![]const T {
    const len = try toUsize(len64);
    if (len == 0) return &.{};
    const raw = ptr orelse return error.InvalidArgument;
    const typed: [*]const T = @ptrCast(@alignCast(raw));
    return typed[0..len];
}

fn getOptionalSliceU8(ptr: ?[*]const u8, len64: u64) !?[]const u8 {
    const len = try toUsize(len64);
    if (len == 0) return null;
    const raw = ptr orelse return error.InvalidArgument;
    return raw[0..len];
}

fn toUsize(v: u64) !usize {
    if (v > std.math.maxInt(usize)) return error.LengthOverflow;
    return @intCast(v);
}

fn parsePhysicalType(code: c_int) ?parquet.PhysicalType {
    return switch (code) {
        0 => .boolean,
        1 => .int32,
        2 => .int64,
        3 => .int96,
        4 => .float,
        5 => .double,
        6 => .byte_array,
        7 => .fixed_len_byte_array,
        else => null,
    };
}

fn parseRepetition(code: c_int) ?parquet.Repetition {
    return switch (code) {
        0 => .required,
        1 => .optional,
        2 => .repeated,
        else => null,
    };
}

fn parseCompression(code: c_int) ?parquet.Compression {
    return switch (code) {
        0 => .uncompressed,
        2 => .gzip,
        else => null,
    };
}

fn mapStatus(err: anyerror) c_int {
    return switch (err) {
        error.OutOfMemory => PF_STATUS_OUT_OF_MEMORY,
        error.InvalidArgument,
        error.InvalidSchema,
        error.InvalidColumnName,
        error.InvalidFixedTypeLength,
        error.InvalidLevels,
        error.ColumnCountMismatch,
        error.ColumnTypeMismatch,
        error.RowCountMismatch,
        error.InvalidOffsets,
        error.PayloadTooLarge,
        error.PageTooLarge,
        error.MetadataTooLarge,
        error.TooManyRows,
        error.LengthOverflow,
        => PF_STATUS_INVALID_ARGUMENT,
        else => PF_STATUS_INTERNAL,
    };
}
