const std = @import("std");
const log_sink = @import("log_sink.zig");
const LogSink = log_sink.LogSink;
const LogEntry = @import("log_entry.zig").LogEntry;
const file_writer = @import("parquet/file_writer.zig");
const types = @import("parquet/types.zig");
const ColumnData = @import("parquet/column_writer.zig").ColumnData;
const MarketOrder = @import("market_order.zig").MarketOrder;
const market_order = @import("market_order.zig");

fn getMonotonicNs() u64 {
    var ts: std.os.linux.timespec = undefined;
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec);
}

fn nanosleep(ns: u64) void {
    var req = std.os.linux.timespec{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
    while (true) {
        const rc = std.os.linux.nanosleep(&req, &req);
        const e = std.os.linux.errno(rc);
        if (e != .INTR) break;
    }
}

pub fn main() !void {
    std.debug.print("parquet_flow v0.2: starting\n", .{});

    var gpa_state: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    // Create output directory
    const output_dir = "output";
    {
        const rc = std.os.linux.mkdir(output_dir, 0o755);
        const e = std.os.linux.errno(rc);
        if (e != .SUCCESS and e != .EXIST) return error.MkdirFailed;
    }

    // --- 1GB Mixed-Type Benchmark ---
    try benchmark1GB(gpa, output_dir);

    // --- Stock Market Order Benchmark ---
    try benchmarkMarketOrders(gpa, output_dir);

    // --- Log Sink Benchmark ---
    try benchmarkLogSink(gpa, output_dir);

    std.debug.print("\nDone. Validate: duckdb -c \"SELECT count(*) FROM read_parquet('output/*.parquet');\"\n", .{});
}

fn benchmark1GB(gpa: std.mem.Allocator, output_dir: []const u8) !void {
    std.debug.print("\n=== 1GB Mixed-Type Benchmark ===\n", .{});
    std.debug.print("  Schema: blob(64B), text(32B), int_val(INT64), float_val(DOUBLE)\n", .{});

    const num_rows: usize = 9_000_000;
    const raw_size_mb = num_rows * (64 + 4 + 32 + 4 + 8 + 8) / (1024 * 1024);
    std.debug.print("  Generating {d}M rows (~{d} MB raw)...\n", .{ num_rows / 1_000_000, raw_size_mb });

    // Schema
    const schema = [_]types.ColumnSpec{
        .{ .name = "blob", .physical_type = .BYTE_ARRAY, .repetition_type = .REQUIRED },
        .{ .name = "text", .physical_type = .BYTE_ARRAY, .repetition_type = .REQUIRED, .converted_type = .UTF8 },
        .{ .name = "int_val", .physical_type = .INT64, .repetition_type = .REQUIRED },
        .{ .name = "float_val", .physical_type = .DOUBLE, .repetition_type = .REQUIRED },
    };

    // Pre-generate data pools
    var blob_pool: [1024][64]u8 = undefined;
    for (&blob_pool, 0..) |*b, i| {
        var seed: u64 = @intCast(i);
        for (b) |*byte| {
            seed = seed *% 6364136223846793005 +% 1442695040888963407;
            byte.* = @truncate(seed >> 33);
        }
    }

    var text_pool: [1024][32]u8 = undefined;
    for (&text_pool, 0..) |*t, i| {
        _ = std.fmt.bufPrint(t, "order_{d:0>8}_{d:0>8}_pad__", .{ i, i *% 37 }) catch unreachable;
    }

    // Allocate column arrays
    var blobs = try gpa.alloc([]const u8, num_rows);
    defer gpa.free(blobs);
    var texts = try gpa.alloc([]const u8, num_rows);
    defer gpa.free(texts);
    var ints = try gpa.alloc(i64, num_rows);
    defer gpa.free(ints);
    var floats = try gpa.alloc(f64, num_rows);
    defer gpa.free(floats);

    for (0..num_rows) |i| {
        blobs[i] = &blob_pool[i % 1024];
        texts[i] = &text_pool[i % 1024];
        ints[i] = @intCast(i);
        floats[i] = @as(f64, @floatFromInt(i)) * 1.0001;
    }

    const col_data = [_]ColumnData{
        .{ .byte_array_values = blobs },
        .{ .byte_array_values = texts },
        .{ .i64_values = ints },
        .{ .f64_values = floats },
    };

    // Benchmark each codec
    const codecs = [_]struct { codec: types.CompressionCodec, name: []const u8 }{
        .{ .codec = .UNCOMPRESSED, .name = "UNCOMPRESSED" },
        .{ .codec = .ZSTD, .name = "ZSTD" },
    };

    for (codecs) |cc| {
        var fname_buf: [256]u8 = undefined;
        const fname = std.fmt.bufPrint(&fname_buf, "{s}/bench_1gb_{s}.parquet", .{ output_dir, cc.name }) catch unreachable;

        const start = getMonotonicNs();
        const data = try file_writer.writeParquetFile(gpa, &schema, &col_data, num_rows, .{ .codec = cc.codec });
        const write_elapsed = getMonotonicNs() - start;

        const file_size_mb = data.len / (1024 * 1024);
        const write_ms = write_elapsed / std.time.ns_per_ms;
        const rows_per_sec = if (write_elapsed > 0) @as(u64, num_rows) * std.time.ns_per_s / write_elapsed else 0;
        const mb_per_sec = if (write_elapsed > 0) @as(u64, raw_size_mb) * std.time.ns_per_s / write_elapsed else 0;
        const ratio = if (raw_size_mb > 0) @as(f64, @floatFromInt(file_size_mb)) / @as(f64, @floatFromInt(raw_size_mb)) * 100.0 else 0;

        std.debug.print("  {s}: {d}ms, {d}M rows/sec, {d} MB/sec, {d}MB file ({d:.1}% of raw)\n", .{
            cc.name, write_ms, rows_per_sec / 1_000_000, mb_per_sec, file_size_mb, ratio,
        });

        try file_writer.writeFileToDisk(fname, data);
        gpa.free(data);
    }
}

fn benchmarkMarketOrders(gpa: std.mem.Allocator, output_dir: []const u8) !void {
    std.debug.print("\n=== Stock Market Order Benchmark ===\n", .{});
    std.debug.print("  Schema: 8 columns (timestamp_ns, order_id, price, qty, side, type, symbol, exchange)\n", .{});

    const num_rows: usize = 8_000_000;
    std.debug.print("  Generating {d}M orders...\n", .{ num_rows / 1_000_000 });

    // Symbols and exchanges for variety
    const symbols = [_][]const u8{ "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM" };
    const exchanges = [_][]const u8{ "NYSE", "NASD", "BATS", "ARCA" };

    var orders = try gpa.alloc(MarketOrder, num_rows);
    defer gpa.free(orders);

    for (0..num_rows) |i| {
        orders[i] = MarketOrder.init(
            @as(i64, @intCast(1_700_000_000_000_000_000 + i * 100)), // ns timestamps
            @intCast(i + 1), // order ID
            100.0 + @as(f64, @floatFromInt(i % 10000)) * 0.01, // price
            @intCast((i % 1000) + 1), // quantity
            @intCast(i % 2), // side: BUY/SELL
            @intCast(i % 3), // order type: LIMIT/MARKET/CANCEL
            symbols[i % symbols.len],
            exchanges[i % exchanges.len],
        );
    }

    // Extract to columnar
    var ext = try market_order.extractColumns(gpa, orders);
    defer ext.deinit(gpa);
    const col_data = ext.toColumnData();

    // Benchmark with ZSTD
    const codecs = [_]struct { codec: types.CompressionCodec, name: []const u8 }{
        .{ .codec = .UNCOMPRESSED, .name = "UNCOMPRESSED" },
        .{ .codec = .ZSTD, .name = "ZSTD" },
    };

    for (codecs) |cc| {
        var fname_buf: [256]u8 = undefined;
        const fname = std.fmt.bufPrint(&fname_buf, "{s}/market_{s}.parquet", .{ output_dir, cc.name }) catch unreachable;

        const start = getMonotonicNs();
        const data = try file_writer.writeParquetFile(gpa, &market_order.MARKET_ORDER_SCHEMA, &col_data, num_rows, .{ .codec = cc.codec });
        const elapsed = getMonotonicNs() - start;

        const file_size_mb = data.len / (1024 * 1024);
        const elapsed_ms = elapsed / std.time.ns_per_ms;
        const rows_per_sec = if (elapsed > 0) @as(u64, num_rows) * std.time.ns_per_s / elapsed else 0;

        std.debug.print("  {s}: {d}ms, {d}M rows/sec, {d}MB file\n", .{
            cc.name, elapsed_ms, rows_per_sec / 1_000_000, file_size_mb,
        });

        try file_writer.writeFileToDisk(fname, data);
        gpa.free(data);
    }
}

fn benchmarkLogSink(gpa: std.mem.Allocator, output_dir: []const u8) !void {
    std.debug.print("\n=== Log Sink Ring Buffer Benchmark ===\n", .{});

    var sink = LogSink.init(gpa, output_dir);
    sink.start() catch |err| {
        std.debug.print("Failed to start log sink: {}\n", .{err});
        return;
    };

    const N: usize = 100_000;
    const start = getMonotonicNs();
    var pushed: usize = 0;
    var dropped: usize = 0;
    for (0..N) |_| {
        if (sink.log(2, "benchmark log message from main", "main.zig")) {
            pushed += 1;
        } else {
            dropped += 1;
        }
    }
    const elapsed = getMonotonicNs() - start;
    const ns_per_op = if (pushed > 0) elapsed / pushed else 0;

    std.debug.print("  Pushed {d}, dropped {d}, in {d}ms ({d} ns/push)\n", .{ pushed, dropped, elapsed / std.time.ns_per_ms, ns_per_op });

    nanosleep(2 * std.time.ns_per_s);
    sink.stop();
    std.debug.print("  Log sink: {d} files, {d} entries written\n", .{ sink.getFilesWritten(), sink.getEntriesWritten() });
}

test {
    _ = @import("parquet/thrift_encoder.zig");
    _ = @import("parquet/encoding.zig");
    _ = @import("parquet/compression.zig");
    _ = @import("parquet/page_writer.zig");
    _ = @import("parquet/column_writer.zig");
    _ = @import("parquet/row_group_writer.zig");
    _ = @import("parquet/file_writer.zig");
    _ = @import("parquet/types.zig");
    _ = @import("log_entry.zig");
    _ = @import("log_sink.zig");
    _ = @import("market_order.zig");
}
