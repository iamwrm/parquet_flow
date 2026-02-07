const std = @import("std");
const pf = @import("parquet_flow");
const FileWriter = pf.parquet.FileWriter;
const ColumnDef = pf.parquet.schema.ColumnDef;

const c = std.c;
const print = std.debug.print;

const SYMBOLS = [_][8]u8{
    "AAPL    ".*, "MSFT    ".*, "GOOGL   ".*, "AMZN    ".*,
    "TSLA    ".*, "META    ".*, "NVDA    ".*, "JPM     ".*,
};

const EXCHANGES = [_]i32{ 1, 2, 3, 4 }; // NYSE, NASDAQ, CBOE, ARCA

const COLUMNS = [_]ColumnDef{
    .{ .name = "timestamp_ns", .physical_type = .INT64 },
    .{ .name = "symbol", .physical_type = .FIXED_LEN_BYTE_ARRAY, .type_length = 8 },
    .{ .name = "order_id", .physical_type = .INT64 },
    .{ .name = "side", .physical_type = .INT32 },
    .{ .name = "price", .physical_type = .INT64 },
    .{ .name = "quantity", .physical_type = .INT64 },
    .{ .name = "order_type", .physical_type = .INT32 },
    .{ .name = "exchange", .physical_type = .INT32 },
};

fn getTimeNs() i64 {
    var ts: c.timespec = undefined;
    _ = c.clock_gettime(c.CLOCK.MONOTONIC, &ts);
    return ts.sec * 1_000_000_000 + ts.nsec;
}

pub fn main() !void {
    const num_records: usize = 1_000_000;
    const rows_per_group: usize = 65536;
    const file_path: [*:0]const u8 = "market_data_example.parquet";

    print("ParquetFlow Market Data Example\n", .{});
    print("Writing {d} order records to market_data_example.parquet\n", .{num_records});
    print("Rows per row group: {d}\n\n", .{rows_per_group});

    const allocator = std.heap.c_allocator;

    var fw = try FileWriter.init(allocator, &COLUMNS, .UNCOMPRESSED);
    defer fw.deinit();

    var prng = std.Random.DefaultPrng.init(42);
    const random = prng.random();

    const start_ns = getTimeNs();

    var order_id: i64 = 1_000_000;
    var rows_written: usize = 0;

    while (rows_written < num_records) {
        var rg = try fw.newRowGroup();
        defer rg.deinit();

        const batch_end = @min(rows_written + rows_per_group, num_records);
        const batch_count = batch_end - rows_written;

        for (0..batch_count) |j| {
            const i = rows_written + j;
            const sym = SYMBOLS[random.intRangeAtMost(usize, 0, SYMBOLS.len - 1)];

            try rg.column(0).writeI64(@as(i64, @intCast(i)) * 100);
            try rg.column(1).writeFixedByteArray(&sym);
            try rg.column(2).writeI64(order_id);
            try rg.column(3).writeI32(@intCast(random.intRangeAtMost(u1, 0, 1)));
            try rg.column(4).writeI64(10_000_000 + random.intRangeAtMost(i64, 0, 1_000_000));
            try rg.column(5).writeI64(1 + random.intRangeAtMost(i64, 0, 999));
            try rg.column(6).writeI32(@intCast(random.intRangeAtMost(u2, 0, 3)));
            try rg.column(7).writeI32(EXCHANGES[random.intRangeAtMost(usize, 0, EXCHANGES.len - 1)]);

            order_id += 1;
        }

        rg.setNumRows(@intCast(batch_count));
        try fw.closeRowGroup(&rg);
        rows_written += batch_count;
    }

    try fw.writeToFile(file_path);

    const end_ns = getTimeNs();
    const elapsed_ns = end_ns - start_ns;
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const elapsed_s = elapsed_ms / 1000.0;
    const throughput = @as(f64, @floatFromInt(num_records)) / elapsed_s;

    print("--- Results ---\n", .{});
    print("Records written:  {d}\n", .{num_records});
    print("Row groups:       {d}\n", .{fw.row_groups_meta.items.len});
    print("Elapsed:          {d:.2} ms\n", .{elapsed_ms});
    print("Throughput:       {d:.0} records/sec\n", .{throughput});
    print("Output file:      market_data_example.parquet\n", .{});
}
