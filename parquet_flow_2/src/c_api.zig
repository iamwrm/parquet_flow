const std = @import("std");
const types = @import("parquet/types.zig");
const file_writer = @import("parquet/file_writer.zig");
const ColumnData = @import("parquet/column_writer.zig").ColumnData;

// C API types
pub const PfColumnType = enum(c_int) {
    PF_BOOLEAN = 0,
    PF_INT32 = 1,
    PF_INT64 = 2,
    PF_FLOAT = 4,
    PF_DOUBLE = 5,
    PF_BYTE_ARRAY = 6,
};

pub const PfCompression = enum(c_int) {
    PF_UNCOMPRESSED = 0,
    PF_ZSTD = 6,
};

pub const PfColumnDef = extern struct {
    name: [*:0]const u8,
    col_type: c_int,
    required: c_int,
};

pub const PfBytes = extern struct {
    data: [*]const u8,
    len: i32,
};

const MAX_COLUMNS = 32;

const WriterState = struct {
    allocator: std.mem.Allocator,
    gpa: std.heap.GeneralPurposeAllocator(.{}),
    schema: []types.ColumnSpec,
    schema_names: [MAX_COLUMNS][]u8,
    num_columns: usize,
    codec: types.CompressionCodec,
    // Column data pointers (set by pf_writer_set_*)
    col_data: [MAX_COLUMNS]ColumnData,
    // For byte array columns, we need to convert from PfBytes
    byte_array_slices: [MAX_COLUMNS]?[][]const u8,
    def_level_slices: [MAX_COLUMNS]?[]u8,
};

fn mapColumnType(ct: c_int) types.PhysicalType {
    return switch (ct) {
        0 => .BOOLEAN,
        1 => .INT32,
        2 => .INT64,
        4 => .FLOAT,
        5 => .DOUBLE,
        6 => .BYTE_ARRAY,
        else => .INT64,
    };
}

fn mapCompression(comp: c_int) types.CompressionCodec {
    return switch (comp) {
        6 => .ZSTD,
        else => .UNCOMPRESSED,
    };
}

export fn pf_writer_create(
    schema: [*]const PfColumnDef,
    num_columns: c_int,
    compression: c_int,
) callconv(.c) ?*WriterState {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    const allocator = gpa.allocator();

    const ncols: usize = @intCast(num_columns);
    const specs = allocator.alloc(types.ColumnSpec, ncols) catch return null;

    var state = allocator.create(WriterState) catch {
        allocator.free(specs);
        return null;
    };
    state.* = .{
        .allocator = allocator,
        .gpa = gpa,
        .schema = specs,
        .schema_names = undefined,
        .num_columns = ncols,
        .codec = mapCompression(compression),
        .col_data = [_]ColumnData{.{}} ** MAX_COLUMNS,
        .byte_array_slices = [_]?[][]const u8{null} ** MAX_COLUMNS,
        .def_level_slices = [_]?[]u8{null} ** MAX_COLUMNS,
    };

    for (0..ncols) |i| {
        const name_ptr = schema[i].name;
        const name_len = std.mem.len(name_ptr);
        const name_copy = allocator.alloc(u8, name_len) catch return null;
        @memcpy(name_copy, name_ptr[0..name_len]);
        state.schema_names[i] = name_copy;

        specs[i] = .{
            .name = name_copy,
            .physical_type = mapColumnType(schema[i].col_type),
            .repetition_type = if (schema[i].required != 0) .REQUIRED else .OPTIONAL,
        };
    }

    return state;
}

export fn pf_writer_set_i32(w: ?*WriterState, col_idx: c_int, data: [*]const i32, count: i64) callconv(.c) c_int {
    const s = w orelse return -1;
    const idx: usize = @intCast(col_idx);
    const n: usize = @intCast(count);
    s.col_data[idx].i32_values = data[0..n];
    return 0;
}

export fn pf_writer_set_i64(w: ?*WriterState, col_idx: c_int, data: [*]const i64, count: i64) callconv(.c) c_int {
    const s = w orelse return -1;
    const idx: usize = @intCast(col_idx);
    const n: usize = @intCast(count);
    s.col_data[idx].i64_values = data[0..n];
    return 0;
}

export fn pf_writer_set_f32(w: ?*WriterState, col_idx: c_int, data: [*]const f32, count: i64) callconv(.c) c_int {
    const s = w orelse return -1;
    const idx: usize = @intCast(col_idx);
    const n: usize = @intCast(count);
    s.col_data[idx].f32_values = data[0..n];
    return 0;
}

export fn pf_writer_set_f64(w: ?*WriterState, col_idx: c_int, data: [*]const f64, count: i64) callconv(.c) c_int {
    const s = w orelse return -1;
    const idx: usize = @intCast(col_idx);
    const n: usize = @intCast(count);
    s.col_data[idx].f64_values = data[0..n];
    return 0;
}

export fn pf_writer_set_bool(w: ?*WriterState, col_idx: c_int, data: [*]const bool, count: i64) callconv(.c) c_int {
    const s = w orelse return -1;
    const idx: usize = @intCast(col_idx);
    const n: usize = @intCast(count);
    s.col_data[idx].bool_values = data[0..n];
    return 0;
}

export fn pf_writer_set_bytes(w: ?*WriterState, col_idx: c_int, data: [*]const PfBytes, count: i64) callconv(.c) c_int {
    const s = w orelse return -1;
    const idx: usize = @intCast(col_idx);
    const n: usize = @intCast(count);

    // Convert PfBytes array to []const []const u8
    const slices = s.allocator.alloc([]const u8, n) catch return -1;
    // Free previous if any
    if (s.byte_array_slices[idx]) |prev| s.allocator.free(prev);

    for (0..n) |i| {
        const b = data[i];
        const blen: usize = @intCast(b.len);
        slices[i] = b.data[0..blen];
    }
    s.byte_array_slices[idx] = slices;
    s.col_data[idx].byte_array_values = slices;
    return 0;
}

export fn pf_writer_set_def_levels(w: ?*WriterState, col_idx: c_int, levels: [*]const u8, count: i64) callconv(.c) c_int {
    const s = w orelse return -1;
    const idx: usize = @intCast(col_idx);
    const n: usize = @intCast(count);
    s.col_data[idx].def_levels = levels[0..n];
    return 0;
}

export fn pf_writer_write(w: ?*WriterState, path: [*:0]const u8, num_rows: i64) callconv(.c) c_int {
    const s = w orelse return -1;
    const nrows: usize = @intCast(num_rows);
    const path_slice = std.mem.span(path);

    const data = file_writer.writeParquetFile(
        s.allocator,
        s.schema,
        s.col_data[0..s.num_columns],
        nrows,
        .{ .codec = s.codec },
    ) catch return -1;
    defer s.allocator.free(data);

    file_writer.writeFileToDisk(path_slice, data) catch return -1;

    // Reset column data for next write
    for (0..s.num_columns) |i| {
        s.col_data[i] = .{};
    }

    return 0;
}

export fn pf_writer_destroy(w: ?*WriterState) callconv(.c) void {
    const s = w orelse return;
    const allocator = s.allocator;

    // Free byte array conversion slices
    for (0..s.num_columns) |i| {
        if (s.byte_array_slices[i]) |sl| allocator.free(sl);
        if (s.def_level_slices[i]) |dl| allocator.free(dl);
    }

    // Free schema name copies
    for (0..s.num_columns) |i| {
        allocator.free(s.schema_names[i]);
    }
    allocator.free(s.schema);

    var gpa = s.gpa;
    allocator.destroy(s);
    _ = gpa.deinit();
}

// Streaming sink API
const SpscRingBuffer = @import("log_sink.zig").SpscRingBuffer;

const SINK_RING_CAPACITY = 16384;
const SINK_MAX_ROW_SIZE = 4096;

const SinkState = struct {
    allocator: std.mem.Allocator,
    gpa: std.heap.GeneralPurposeAllocator(.{}),
    ring: SpscRingBuffer([SINK_MAX_ROW_SIZE]u8, SINK_RING_CAPACITY),
    writer_thread: ?std.Thread,
    should_stop: std.atomic.Value(bool),
    output_dir: []u8,
    schema: []types.ColumnSpec,
    schema_names: [MAX_COLUMNS][]u8,
    field_offsets: [MAX_COLUMNS]usize,
    field_sizes: [MAX_COLUMNS]usize,
    num_columns: usize,
    row_size: usize,
    codec: types.CompressionCodec,
    batch_size: usize,
    files_written: std.atomic.Value(usize),
    entries_written: std.atomic.Value(usize),

    fn writerLoop(self: *SinkState) void {
        var batch_buf: [8192][SINK_MAX_ROW_SIZE]u8 = undefined;
        var file_seq: usize = 0;

        while (!self.should_stop.load(.acquire)) {
            const count = self.drainBatch(&batch_buf);
            if (count > 0) {
                self.writeBatch(batch_buf[0..count], file_seq);
                file_seq += 1;
            } else {
                nanosleep_ms(1);
            }
        }
        // Final drain
        while (true) {
            const count = self.drainBatch(&batch_buf);
            if (count == 0) break;
            self.writeBatch(batch_buf[0..count], file_seq);
            file_seq += 1;
        }
    }

    fn drainBatch(self: *SinkState, batch: *[8192][SINK_MAX_ROW_SIZE]u8) usize {
        var count: usize = 0;
        while (count < self.batch_size and count < 8192) {
            if (self.ring.pop()) |entry| {
                batch[count] = entry;
                count += 1;
            } else break;
        }
        return count;
    }

    fn writeBatch(self: *SinkState, rows: [][SINK_MAX_ROW_SIZE]u8, seq: usize) void {
        const nrows = rows.len;
        const allocator = self.allocator;

        // Allocate column arrays
        var col_data_arr: [MAX_COLUMNS]ColumnData = [_]ColumnData{.{}} ** MAX_COLUMNS;
        var i32_bufs: [MAX_COLUMNS]?[]i32 = [_]?[]i32{null} ** MAX_COLUMNS;
        var i64_bufs: [MAX_COLUMNS]?[]i64 = [_]?[]i64{null} ** MAX_COLUMNS;
        var f32_bufs: [MAX_COLUMNS]?[]f32 = [_]?[]f32{null} ** MAX_COLUMNS;
        var f64_bufs: [MAX_COLUMNS]?[]f64 = [_]?[]f64{null} ** MAX_COLUMNS;
        var str_bufs: [MAX_COLUMNS]?[][]const u8 = [_]?[][]const u8{null} ** MAX_COLUMNS;

        defer {
            for (0..self.num_columns) |c| {
                if (i32_bufs[c]) |b| allocator.free(b);
                if (i64_bufs[c]) |b| allocator.free(b);
                if (f32_bufs[c]) |b| allocator.free(b);
                if (f64_bufs[c]) |b| allocator.free(b);
                if (str_bufs[c]) |b| allocator.free(b);
            }
        }

        for (0..self.num_columns) |c| {
            switch (self.schema[c].physical_type) {
                .INT32 => {
                    const buf = allocator.alloc(i32, nrows) catch return;
                    for (rows, 0..) |row, r| {
                        const offset = self.field_offsets[c];
                        buf[r] = std.mem.readInt(i32, row[offset..][0..4], .little);
                    }
                    i32_bufs[c] = buf;
                    col_data_arr[c].i32_values = buf;
                },
                .INT64 => {
                    const buf = allocator.alloc(i64, nrows) catch return;
                    for (rows, 0..) |row, r| {
                        const offset = self.field_offsets[c];
                        buf[r] = std.mem.readInt(i64, row[offset..][0..8], .little);
                    }
                    i64_bufs[c] = buf;
                    col_data_arr[c].i64_values = buf;
                },
                .FLOAT => {
                    const buf = allocator.alloc(f32, nrows) catch return;
                    for (rows, 0..) |row, r| {
                        const offset = self.field_offsets[c];
                        const bits = std.mem.readInt(u32, row[offset..][0..4], .little);
                        buf[r] = @bitCast(bits);
                    }
                    f32_bufs[c] = buf;
                    col_data_arr[c].f32_values = buf;
                },
                .DOUBLE => {
                    const buf = allocator.alloc(f64, nrows) catch return;
                    for (rows, 0..) |row, r| {
                        const offset = self.field_offsets[c];
                        const bits = std.mem.readInt(u64, row[offset..][0..8], .little);
                        buf[r] = @bitCast(bits);
                    }
                    f64_bufs[c] = buf;
                    col_data_arr[c].f64_values = buf;
                },
                .BYTE_ARRAY => {
                    const buf = allocator.alloc([]const u8, nrows) catch return;
                    for (rows, 0..) |row, r| {
                        const offset = self.field_offsets[c];
                        const size = self.field_sizes[c];
                        // Field layout: [data][len_u16] where len_u16 is at offset+size-2
                        const len_offset = offset + size - 2;
                        const str_len: usize = std.mem.readInt(u16, row[len_offset..][0..2], .little);
                        buf[r] = row[offset..][0..str_len];
                    }
                    str_bufs[c] = buf;
                    col_data_arr[c].byte_array_values = buf;
                },
                else => {},
            }
        }

        var fname_buf: [512]u8 = undefined;
        const fname = std.fmt.bufPrint(&fname_buf, "{s}/sink_{d:0>6}.parquet", .{ self.output_dir, seq }) catch return;

        const data = file_writer.writeParquetFile(
            allocator,
            self.schema,
            col_data_arr[0..self.num_columns],
            nrows,
            .{ .codec = self.codec },
        ) catch return;
        defer allocator.free(data);

        file_writer.writeFileToDisk(fname, data) catch return;
        _ = self.files_written.fetchAdd(1, .release);
        _ = self.entries_written.fetchAdd(nrows, .release);
    }
};

fn nanosleep_ms(ms: u64) void {
    var req = std.os.linux.timespec{
        .sec = @intCast(ms / 1000),
        .nsec = @intCast((ms % 1000) * std.time.ns_per_ms),
    };
    while (true) {
        const rc = std.os.linux.nanosleep(&req, &req);
        const e = std.os.linux.errno(rc);
        if (e != .INTR) break;
    }
}

export fn pf_sink_create(
    output_dir: [*:0]const u8,
    schema: [*]const PfColumnDef,
    num_fields: c_int,
    row_size: c_int,
    compression: c_int,
    batch_size: c_int,
) callconv(.c) ?*SinkState {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    const allocator = gpa.allocator();

    const nfields: usize = @intCast(num_fields);
    const dir_str = std.mem.span(output_dir);
    const dir_copy = allocator.alloc(u8, dir_str.len) catch return null;
    @memcpy(dir_copy, dir_str);

    const specs = allocator.alloc(types.ColumnSpec, nfields) catch return null;

    var state = allocator.create(SinkState) catch return null;
    state.* = .{
        .allocator = allocator,
        .gpa = gpa,
        .ring = SpscRingBuffer([SINK_MAX_ROW_SIZE]u8, SINK_RING_CAPACITY).init(),
        .writer_thread = null,
        .should_stop = std.atomic.Value(bool).init(false),
        .output_dir = dir_copy,
        .schema = specs,
        .schema_names = undefined,
        .field_offsets = undefined,
        .field_sizes = undefined,
        .num_columns = nfields,
        .row_size = @intCast(row_size),
        .codec = mapCompression(compression),
        .batch_size = @intCast(batch_size),
        .files_written = std.atomic.Value(usize).init(0),
        .entries_written = std.atomic.Value(usize).init(0),
    };

    // Initialize field metadata
    var offset: usize = 0;
    for (0..nfields) |i| {
        const name_ptr = schema[i].name;
        const name_len = std.mem.len(name_ptr);
        const name_copy = allocator.alloc(u8, name_len) catch return null;
        @memcpy(name_copy, name_ptr[0..name_len]);
        state.schema_names[i] = name_copy;

        const pt = mapColumnType(schema[i].col_type);
        specs[i] = .{
            .name = name_copy,
            .physical_type = pt,
            .repetition_type = if (schema[i].required != 0) .REQUIRED else .OPTIONAL,
        };

        state.field_offsets[i] = offset;
        const field_size: usize = switch (pt) {
            .INT32, .FLOAT => 4,
            .INT64, .DOUBLE => 8,
            .BOOLEAN => 1,
            else => 0, // BYTE_ARRAY handled by user's struct layout
        };
        state.field_sizes[i] = field_size;
        offset += field_size;
    }

    return state;
}

export fn pf_sink_start(s: ?*SinkState) callconv(.c) c_int {
    const state = s orelse return -1;
    state.writer_thread = std.Thread.spawn(.{}, SinkState.writerLoop, .{state}) catch return -1;
    return 0;
}

export fn pf_sink_push(s: ?*SinkState, row: [*]const u8) callconv(.c) c_int {
    const state = s orelse return -1;
    var entry: [SINK_MAX_ROW_SIZE]u8 = undefined;
    @memcpy(entry[0..state.row_size], row[0..state.row_size]);
    if (state.ring.push(entry)) return 0;
    return -1;
}

export fn pf_sink_stop(s: ?*SinkState) callconv(.c) void {
    const state = s orelse return;
    state.should_stop.store(true, .release);
    if (state.writer_thread) |t| {
        t.join();
        state.writer_thread = null;
    }
}

export fn pf_sink_destroy(s: ?*SinkState) callconv(.c) void {
    const state = s orelse return;
    const allocator = state.allocator;

    for (0..state.num_columns) |i| {
        allocator.free(state.schema_names[i]);
    }
    allocator.free(state.schema);
    allocator.free(state.output_dir);

    var gpa = state.gpa;
    allocator.destroy(state);
    _ = gpa.deinit();
}

export fn pf_sink_files_written(s: ?*SinkState) callconv(.c) i64 {
    const state = s orelse return 0;
    return @intCast(state.files_written.load(.acquire));
}

export fn pf_sink_entries_written(s: ?*SinkState) callconv(.c) i64 {
    const state = s orelse return 0;
    return @intCast(state.entries_written.load(.acquire));
}
