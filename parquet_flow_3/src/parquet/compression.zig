const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");

/// Compress data using the specified codec.
/// Returns owned slice that must be freed by the caller (with the same allocator).
/// For UNCOMPRESSED, returns a copy of the input.
pub fn compress(codec: types.CompressionCodec, input: []const u8, allocator: Allocator) ![]u8 {
    return switch (codec) {
        .UNCOMPRESSED => {
            const result = try allocator.alloc(u8, input.len);
            @memcpy(result, input);
            return result;
        },
        .GZIP => {
            // TODO: implement using std.compress.flate when API stabilizes
            return error.CompressionNotImplemented;
        },
        .SNAPPY => {
            // TODO: implement via C libsnappy linkage
            return error.CompressionNotImplemented;
        },
        .ZSTD => {
            // TODO: Zig std only has zstd decompressor; need libzstd C linkage for compression
            return error.CompressionNotImplemented;
        },
    };
}
