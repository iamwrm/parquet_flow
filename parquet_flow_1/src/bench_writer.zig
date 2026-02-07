const std = @import("std");
const Io = std.Io;
const parquet_flow = @import("parquet_flow");

const BenchConfig = struct {
    rows: usize = 2_000_000,
    row_size: usize = 256,
    row_group_rows: usize = 4096,
    output_path: []const u8 = "local_data/bench_writer.parquet",
    compression: parquet_flow.parquet.Compression = .uncompressed,
    repetition: parquet_flow.parquet.Repetition = .required,
    null_every: usize = 0,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var threaded: Io.Threaded = .init(allocator, .{ .environ = init.minimal.environ });
    defer threaded.deinit();
    const io = threaded.io();

    var config = BenchConfig{};
    const show_help = try parseArgs(init, &config);
    if (show_help) {
        try printUsage(io);
        return;
    }

    if (config.rows == 0) return error.InvalidRows;
    if (config.row_size == 0) return error.InvalidRowSize;
    if (config.row_group_rows == 0) return error.InvalidRowGroupRows;
    if (config.repetition == .required and config.null_every != 0) return error.InvalidArgument;
    if (config.repetition != .required and config.repetition != .optional) return error.InvalidArgument;

    const max_payload_len = try std.math.mul(usize, config.row_group_rows, config.row_size);
    if (max_payload_len > std.math.maxInt(u32)) {
        return error.RowGroupPayloadTooLarge;
    }

    if (std.mem.lastIndexOfScalar(u8, config.output_path, '/')) |slash| {
        const dir_path = config.output_path[0..slash];
        if (dir_path.len > 0) {
            try std.Io.Dir.cwd().createDirPath(io, dir_path);
        }
    }

    var payload_template = try allocator.alloc(u8, max_payload_len);
    defer allocator.free(payload_template);

    var offsets_template = try allocator.alloc(u32, config.row_group_rows + 1);
    defer allocator.free(offsets_template);

    var optional_payload_template = try allocator.alloc(u8, max_payload_len);
    defer allocator.free(optional_payload_template);

    var optional_offsets_template = try allocator.alloc(u32, config.row_group_rows + 1);
    defer allocator.free(optional_offsets_template);

    var definition_levels_template = try allocator.alloc(u8, config.row_group_rows);
    defer allocator.free(definition_levels_template);

    fillPayload(payload_template);
    buildOffsets(offsets_template, config.row_size);

    const schema = [_]parquet_flow.parquet.ColumnDef{.{
        .name = "payload",
        .physical_type = .byte_array,
        .repetition = config.repetition,
    }};
    var writer = try parquet_flow.parquet.Writer.initWithSchema(
        allocator,
        io,
        config.output_path,
        &schema,
        config.compression,
    );
    defer writer.deinit();

    var row_groups: usize = 0;
    var rows_remaining = config.rows;

    const start = Io.Clock.Timestamp.now(io, .awake);

    while (rows_remaining > 0) {
        const batch_rows = @min(rows_remaining, config.row_group_rows);
        const payload_len = batch_rows * config.row_size;

        switch (config.repetition) {
            .required => {
                try writer.writeRowGroup(
                    payload_template[0..payload_len],
                    offsets_template[0 .. batch_rows + 1],
                );
            },
            .optional => {
                const optional_batch = buildOptionalBatch(
                    batch_rows,
                    config.row_size,
                    config.null_every,
                    payload_template,
                    definition_levels_template,
                    optional_payload_template,
                    optional_offsets_template,
                );

                const columns = [_]parquet_flow.parquet.ColumnData{.{
                    .byte_array = .{
                        .values = optional_payload_template[0..optional_batch.payload_len],
                        .offsets = optional_offsets_template[0 .. optional_batch.value_count + 1],
                    },
                }};
                const levels = [_]parquet_flow.parquet.ColumnLevels{.{
                    .definition_levels = definition_levels_template[0..batch_rows],
                    .repetition_levels = null,
                }};
                try writer.writeRowGroupColumnsWithLevels(batch_rows, &columns, &levels);
            },
            else => return error.InvalidArgument,
        }

        rows_remaining -= batch_rows;
        row_groups += 1;
    }

    try writer.close();

    const end = Io.Clock.Timestamp.now(io, .awake);
    const elapsed = start.durationTo(end).raw.nanoseconds;
    if (elapsed <= 0) return error.InvalidClockDuration;

    const elapsed_ns: f64 = @floatFromInt(@as(i128, elapsed));
    const seconds = elapsed_ns / @as(f64, std.time.ns_per_s);

    const payload_bytes_total = try std.math.mul(u128, config.rows, config.row_size);

    const output_file = try std.Io.Dir.cwd().openFile(io, config.output_path, .{ .mode = .read_only });
    defer output_file.close(io);
    const output_size = try output_file.length(io);

    const rows_per_sec = @as(f64, @floatFromInt(config.rows)) / seconds;
    const payload_mib_per_sec = (@as(f64, @floatFromInt(payload_bytes_total)) / (1024.0 * 1024.0)) / seconds;
    const file_mib_per_sec = (@as(f64, @floatFromInt(output_size)) / (1024.0 * 1024.0)) / seconds;

    var stdout_buffer: [2048]u8 = undefined;
    var stdout_writer_file: Io.File.Writer = .init(.stdout(), io, &stdout_buffer);
    const out = &stdout_writer_file.interface;

    try out.print("parquet writer benchmark\n", .{});
    try out.print("rows={d} row_size={d} row_group_rows={d} row_groups={d}\n", .{
        config.rows,
        config.row_size,
        config.row_group_rows,
        row_groups,
    });
    try out.print("repetition={s} null_every={d}\n", .{
        repetitionName(config.repetition),
        config.null_every,
    });
    try out.print("compression={s}\n", .{compressionName(config.compression)});
    try out.print("elapsed_s={d:.6}\n", .{seconds});
    try out.print("rows_per_sec={d:.2}\n", .{rows_per_sec});
    try out.print("payload_mib_per_sec={d:.2}\n", .{payload_mib_per_sec});
    try out.print("file_mib_per_sec={d:.2}\n", .{file_mib_per_sec});
    try out.print("output_size_bytes={d} output_path={s}\n", .{ output_size, config.output_path });
    try out.flush();
}

fn parseArgs(init: std.process.Init, config: *BenchConfig) !bool {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            return true;
        } else if (std.mem.eql(u8, arg, "--rows")) {
            const value = args.next() orelse return error.MissingValue;
            config.rows = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--row-size")) {
            const value = args.next() orelse return error.MissingValue;
            config.row_size = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--row-group-rows")) {
            const value = args.next() orelse return error.MissingValue;
            config.row_group_rows = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--output")) {
            config.output_path = args.next() orelse return error.MissingValue;
        } else if (std.mem.eql(u8, arg, "--compression")) {
            const value = args.next() orelse return error.MissingValue;
            config.compression = parseCompression(value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--repetition")) {
            const value = args.next() orelse return error.MissingValue;
            config.repetition = parseRepetition(value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--null-every")) {
            const value = args.next() orelse return error.MissingValue;
            config.null_every = try std.fmt.parseInt(usize, value, 10);
        } else {
            return error.InvalidArgument;
        }
    }

    return false;
}

fn printUsage(io: Io) !void {
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer_file: Io.File.Writer = .init(.stdout(), io, &stdout_buffer);
    const out = &stdout_writer_file.interface;

    try out.print(
        "usage: zig build bench -- [--rows N] [--row-size BYTES] [--row-group-rows N] [--output PATH] [--compression CODEC] [--repetition required|optional] [--null-every N]\n",
        .{},
    );
    try out.print(
        "defaults: --rows 2000000 --row-size 256 --row-group-rows 4096 --compression uncompressed --repetition required --null-every 0\n",
        .{},
    );
    try out.print("compression: uncompressed | gzip\n", .{});
    try out.print("repetition: required | optional\n", .{});
    try out.flush();
}

fn parseCompression(value: []const u8) ?parquet_flow.parquet.Compression {
    if (std.mem.eql(u8, value, "uncompressed")) return .uncompressed;
    if (std.mem.eql(u8, value, "gzip")) return .gzip;
    return null;
}

fn parseRepetition(value: []const u8) ?parquet_flow.parquet.Repetition {
    if (std.mem.eql(u8, value, "required")) return .required;
    if (std.mem.eql(u8, value, "optional")) return .optional;
    return null;
}

fn compressionName(compression: parquet_flow.parquet.Compression) []const u8 {
    return switch (compression) {
        .uncompressed => "uncompressed",
        .gzip => "gzip",
    };
}

fn repetitionName(repetition: parquet_flow.parquet.Repetition) []const u8 {
    return switch (repetition) {
        .required => "required",
        .optional => "optional",
        .repeated => "repeated",
    };
}

fn buildOptionalBatch(
    rows: usize,
    row_size: usize,
    null_every: usize,
    payload_template: []const u8,
    definition_levels: []u8,
    payload_out: []u8,
    offsets_out: []u32,
) struct {
    payload_len: usize,
    value_count: usize,
} {
    var payload_len: usize = 0;
    var value_count: usize = 0;
    offsets_out[0] = 0;

    for (0..rows) |idx| {
        const is_null = null_every != 0 and ((idx + 1) % null_every == 0);
        definition_levels[idx] = if (is_null) 0 else 1;

        if (is_null) continue;
        const start = idx * row_size;
        const end = start + row_size;
        @memcpy(payload_out[payload_len .. payload_len + row_size], payload_template[start..end]);
        payload_len += row_size;
        value_count += 1;
        offsets_out[value_count] = @intCast(payload_len);
    }

    return .{
        .payload_len = payload_len,
        .value_count = value_count,
    };
}

fn fillPayload(buf: []u8) void {
    var state: u64 = 0x9E3779B97F4A7C15;
    for (buf, 0..) |*byte, i| {
        state = state *% 6364136223846793005 +% 1;
        byte.* = @truncate((state >> 24) ^ @as(u64, @intCast(i)));
    }
}

fn buildOffsets(offsets: []u32, row_size: usize) void {
    offsets[0] = 0;
    for (0..offsets.len - 1) |i| {
        offsets[i + 1] = @intCast((i + 1) * row_size);
    }
}
