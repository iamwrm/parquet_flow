const std = @import("std");
const types = @import("types.zig");

const zstd = @cImport({
    @cInclude("zstd.h");
});

pub const CompressResult = struct {
    data: []const u8,
    alloc_slice: ?[]u8, // full allocation for freeing

    pub fn deinit(self: CompressResult, allocator: std.mem.Allocator) void {
        if (self.alloc_slice) |s| {
            allocator.free(s);
        }
    }
};

pub fn compressPage(
    allocator: std.mem.Allocator,
    input: []const u8,
    codec: types.CompressionCodec,
) !CompressResult {
    return switch (codec) {
        .UNCOMPRESSED => .{ .data = input, .alloc_slice = null },
        .ZSTD => try compressZstd(allocator, input),
        else => error.UnsupportedCompression,
    };
}

fn compressZstd(allocator: std.mem.Allocator, input: []const u8) !CompressResult {
    const bound = zstd.ZSTD_compressBound(input.len);
    const output = try allocator.alloc(u8, bound);
    errdefer allocator.free(output);

    const compressed_size = zstd.ZSTD_compress(
        output.ptr,
        output.len,
        input.ptr,
        input.len,
        1,
    );
    if (zstd.ZSTD_isError(compressed_size) != 0) {
        return error.ZstdCompressFailed;
    }

    return .{
        .data = output[0..compressed_size],
        .alloc_slice = output,
    };
}

test "ZSTD compress and decompress" {
    const allocator = std.testing.allocator;
    const input = "hello hello hello hello hello hello hello hello";

    const result = try compressPage(allocator, input, .ZSTD);
    defer result.deinit(allocator);

    try std.testing.expect(result.alloc_slice != null);
    try std.testing.expect(result.data.len < input.len);

    // Decompress and verify
    var decompressed = try allocator.alloc(u8, input.len);
    defer allocator.free(decompressed);
    const decompressed_size = zstd.ZSTD_decompress(
        decompressed.ptr,
        decompressed.len,
        result.data.ptr,
        result.data.len,
    );
    try std.testing.expect(zstd.ZSTD_isError(decompressed_size) == 0);
    try std.testing.expectEqualSlices(u8, input, decompressed[0..decompressed_size]);
}

test "UNCOMPRESSED passthrough" {
    const allocator = std.testing.allocator;
    const input = "raw data";

    const result = try compressPage(allocator, input, .UNCOMPRESSED);
    defer result.deinit(allocator);

    try std.testing.expect(result.alloc_slice == null);
    try std.testing.expectEqualSlices(u8, input, result.data);
}
