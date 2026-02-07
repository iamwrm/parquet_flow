const std = @import("std");
const ThriftCompactEncoder = @import("thrift_encoder.zig").ThriftCompactEncoder;
const types = @import("types.zig");
const compression = @import("compression.zig");

/// Assemble a Data Page V1 into the output buffer.
/// Returns the total number of bytes written (header + body).
pub fn writeDataPageV1(
    allocator: std.mem.Allocator,
    output: *std.ArrayList(u8),
    values_data: []const u8,
    def_levels_data: []const u8,
    num_values: u32,
    is_required: bool,
    codec: types.CompressionCodec,
) !usize {
    // Build page body: [def_levels (if optional)] [encoded_values]
    var page_body: std.ArrayList(u8) = .empty;
    defer page_body.deinit(allocator);

    if (!is_required and def_levels_data.len > 0) {
        try page_body.appendSlice(allocator, def_levels_data);
    }
    try page_body.appendSlice(allocator, values_data);

    const uncompressed_size: i32 = @intCast(page_body.items.len);

    // Compress page body
    const compressed = try compression.compressPage(allocator, page_body.items, codec);
    defer compressed.deinit(allocator);

    const compressed_size: i32 = @intCast(compressed.data.len);

    // Write PageHeader via Thrift Compact Protocol
    var enc = ThriftCompactEncoder.init(allocator);
    defer enc.deinit();

    enc.writeStructBegin();
    // field 1: type (PageType) - I32 enum
    try enc.writeI32Field(1, @intFromEnum(@as(types.PageType, .DATA_PAGE)));
    // field 2: uncompressed_page_size - I32
    try enc.writeI32Field(2, uncompressed_size);
    // field 3: compressed_page_size - I32
    try enc.writeI32Field(3, compressed_size);
    // field 5: data_page_header (DataPageHeader struct)
    try enc.writeStructField(5);
    {
        // DataPageHeader.field 1: num_values - I32
        try enc.writeI32Field(1, @intCast(num_values));
        // DataPageHeader.field 2: encoding - I32 (PLAIN = 0)
        try enc.writeI32Field(2, 0);
        // DataPageHeader.field 3: definition_level_encoding - I32 (RLE = 3)
        try enc.writeI32Field(3, 3);
        // DataPageHeader.field 4: repetition_level_encoding - I32 (RLE = 3)
        try enc.writeI32Field(4, 3);
    }
    try enc.writeStructEnd(); // end DataPageHeader
    try enc.writeStructEnd(); // end PageHeader

    const header_bytes = enc.getWritten();
    const start = output.items.len;
    try output.appendSlice(allocator, header_bytes);
    try output.appendSlice(allocator, compressed.data);
    return output.items.len - start;
}

test "data page v1 assembly" {
    const allocator = std.testing.allocator;
    var output: std.ArrayList(u8) = .empty;
    defer output.deinit(allocator);

    const values = [_]u8{ 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00 };
    const bytes_written = try writeDataPageV1(allocator, &output, &values, &.{}, 2, true, .UNCOMPRESSED);

    try std.testing.expect(bytes_written > 8);
    try std.testing.expect(output.items.len == bytes_written);
    try std.testing.expectEqualSlices(u8, &values, output.items[output.items.len - 8 ..]);
}
