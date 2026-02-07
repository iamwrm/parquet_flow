const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const thrift = @import("thrift.zig");
const CompactProtocolWriter = thrift.CompactProtocolWriter;
const compression = @import("compression.zig");

/// A built data page: serialized page header + compressed page data.
pub const DataPage = struct {
    header_bytes: []u8,
    data_bytes: []u8,
    gpa: Allocator,

    pub fn deinit(self: *DataPage) void {
        self.gpa.free(self.header_bytes);
        self.gpa.free(self.data_bytes);
    }

    pub fn totalSize(self: *const DataPage) usize {
        return self.header_bytes.len + self.data_bytes.len;
    }
};

/// Build a data page from encoded values and optional definition/repetition levels.
pub fn buildDataPage(
    encoded_values: []const u8,
    def_levels: ?[]const u8,
    rep_levels: ?[]const u8,
    num_values: i32,
    data_encoding: types.Encoding,
    codec: types.CompressionCodec,
    allocator: Allocator,
) !DataPage {
    // Assemble uncompressed page body: rep_levels | def_levels | encoded_values
    var uncompressed_size: usize = 0;
    if (rep_levels) |rl| uncompressed_size += rl.len;
    if (def_levels) |dl| uncompressed_size += dl.len;
    uncompressed_size += encoded_values.len;

    var uncompressed = try allocator.alloc(u8, uncompressed_size);
    defer allocator.free(uncompressed);

    var offset: usize = 0;
    if (rep_levels) |rl| {
        @memcpy(uncompressed[offset..][0..rl.len], rl);
        offset += rl.len;
    }
    if (def_levels) |dl| {
        @memcpy(uncompressed[offset..][0..dl.len], dl);
        offset += dl.len;
    }
    @memcpy(uncompressed[offset..][0..encoded_values.len], encoded_values);

    // Compress
    const compressed_data = try compression.compress(codec, uncompressed, allocator);

    // Serialize PageHeader
    var tw = CompactProtocolWriter.init(allocator);
    defer tw.deinit();

    try tw.writeFieldI32(1, @intFromEnum(types.PageType.DATA_PAGE));
    try tw.writeFieldI32(2, @intCast(uncompressed_size));
    try tw.writeFieldI32(3, @intCast(compressed_data.len));

    // field 5: DataPageHeader struct
    try tw.writeFieldStruct(5);
    try tw.writeStructBegin();
    try tw.writeFieldI32(1, num_values);
    try tw.writeFieldI32(2, @intFromEnum(data_encoding));
    try tw.writeFieldI32(3, @intFromEnum(types.Encoding.RLE));
    try tw.writeFieldI32(4, @intFromEnum(types.Encoding.RLE));
    try tw.writeStructEnd();

    try tw.writeFieldStop();

    // Copy header bytes
    const header_bytes = try allocator.alloc(u8, tw.getWritten().len);
    @memcpy(header_bytes, tw.getWritten());

    return DataPage{
        .header_bytes = header_bytes,
        .data_bytes = compressed_data,
        .gpa = allocator,
    };
}
