const std = @import("std");
const testing = std.testing;
const pf = @import("parquet_flow");
const FileWriter = pf.parquet.FileWriter;
const ColumnDef = pf.parquet.schema.ColumnDef;
const types = pf.types;

test "parquet magic bytes constant" {
    try testing.expectEqualStrings("PAR1", types.MAGIC);
}

test "schema build from column defs" {
    const allocator = testing.allocator;
    const Schema = pf.schema.Schema;

    const columns = [_]ColumnDef{
        .{ .name = "id", .physical_type = .INT64 },
        .{ .name = "value", .physical_type = .DOUBLE },
        .{ .name = "label", .physical_type = .FIXED_LEN_BYTE_ARRAY, .type_length = 16 },
    };

    var s = try Schema.buildFromColumns(allocator, &columns);
    defer s.deinit();

    // Root element + 3 leaf elements
    try testing.expectEqual(@as(usize, 4), s.elements.items.len);
    try testing.expectEqualStrings("schema", s.elements.items[0].name);
    try testing.expectEqualStrings("id", s.elements.items[1].name);
    try testing.expectEqual(types.PhysicalType.DOUBLE, s.elements.items[2].physical_type.?);
    try testing.expectEqual(@as(i32, 16), s.elements.items[3].type_length.?);
}

test "write parquet file and verify magic bytes" {
    const allocator = testing.allocator;

    const columns = [_]ColumnDef{
        .{ .name = "timestamp_ns", .physical_type = .INT64 },
        .{ .name = "value", .physical_type = .INT64 },
    };

    var fw = try FileWriter.init(allocator, &columns, .UNCOMPRESSED);
    defer fw.deinit();

    // Write 100 rows across a single row group
    var rg = try fw.newRowGroup();
    defer rg.deinit();

    for (0..100) |i| {
        try rg.column(0).writeI64(@intCast(i * 1000));
        try rg.column(1).writeI64(@intCast(i * 42));
    }
    rg.setNumRows(100);
    try fw.closeRowGroup(&rg);

    const file_bytes = try fw.close();

    // Verify file size is reasonable
    try testing.expect(file_bytes.len > 8);

    // Check leading magic bytes
    try testing.expectEqualStrings("PAR1", file_bytes[0..4]);

    // Check trailing magic bytes
    try testing.expectEqualStrings("PAR1", file_bytes[file_bytes.len - 4 ..]);

    // Verify metadata length field
    const meta_len = std.mem.readInt(u32, file_bytes[file_bytes.len - 8 ..][0..4], .little);
    try testing.expect(meta_len > 0);
    try testing.expect(meta_len < file_bytes.len);
}

test "write parquet file with market data schema" {
    const allocator = testing.allocator;

    const market_columns = [_]ColumnDef{
        .{ .name = "timestamp_ns", .physical_type = .INT64 },
        .{ .name = "symbol", .physical_type = .FIXED_LEN_BYTE_ARRAY, .type_length = 8 },
        .{ .name = "order_id", .physical_type = .INT64 },
        .{ .name = "side", .physical_type = .INT32 },
        .{ .name = "price", .physical_type = .INT64 },
        .{ .name = "quantity", .physical_type = .INT64 },
        .{ .name = "order_type", .physical_type = .INT32 },
        .{ .name = "exchange", .physical_type = .INT32 },
    };

    var fw = try FileWriter.init(allocator, &market_columns, .UNCOMPRESSED);
    defer fw.deinit();

    var rg = try fw.newRowGroup();
    defer rg.deinit();

    // Write 10 sample records
    for (0..10) |i| {
        try rg.column(0).writeI64(@intCast(i * 100));
        try rg.column(1).writeFixedByteArray("AAPL    ");
        try rg.column(2).writeI64(@intCast(1000 + i));
        try rg.column(3).writeI32(0);
        try rg.column(4).writeI64(15000000);
        try rg.column(5).writeI64(100);
        try rg.column(6).writeI32(0);
        try rg.column(7).writeI32(1);
    }
    rg.setNumRows(10);
    try fw.closeRowGroup(&rg);

    const file_bytes = try fw.close();

    try testing.expectEqualStrings("PAR1", file_bytes[0..4]);
    try testing.expectEqualStrings("PAR1", file_bytes[file_bytes.len - 4 ..]);
    try testing.expect(file_bytes.len > 100); // Should be a reasonable size
}
