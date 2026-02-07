const std = @import("std");

/// PLAIN encoding for Parquet physical types.
pub const PlainEncoder = struct {
    pub fn encodeI32(values: []const i32, allocator: std.mem.Allocator, buf: *std.ArrayList(u8)) !void {
        for (values) |v| {
            const le = std.mem.nativeToLittle(i32, v);
            try buf.appendSlice(allocator, std.mem.asBytes(&le));
        }
    }

    pub fn encodeI64(values: []const i64, allocator: std.mem.Allocator, buf: *std.ArrayList(u8)) !void {
        for (values) |v| {
            const le = std.mem.nativeToLittle(i64, v);
            try buf.appendSlice(allocator, std.mem.asBytes(&le));
        }
    }

    pub fn encodeBool(values: []const bool, allocator: std.mem.Allocator, buf: *std.ArrayList(u8)) !void {
        var byte: u8 = 0;
        for (values, 0..) |v, i| {
            if (v) byte |= @as(u8, 1) << @intCast(i % 8);
            if ((i % 8) == 7 or i == values.len - 1) {
                try buf.append(allocator, byte);
                byte = 0;
            }
        }
    }

    pub fn encodeFloat(values: []const f32, allocator: std.mem.Allocator, buf: *std.ArrayList(u8)) !void {
        for (values) |v| {
            const bits: u32 = @bitCast(v);
            const le = std.mem.nativeToLittle(u32, bits);
            try buf.appendSlice(allocator, std.mem.asBytes(&le));
        }
    }

    pub fn encodeDouble(values: []const f64, allocator: std.mem.Allocator, buf: *std.ArrayList(u8)) !void {
        for (values) |v| {
            const bits: u64 = @bitCast(v);
            const le = std.mem.nativeToLittle(u64, bits);
            try buf.appendSlice(allocator, std.mem.asBytes(&le));
        }
    }

    pub fn encodeByteArray(values: []const []const u8, allocator: std.mem.Allocator, buf: *std.ArrayList(u8)) !void {
        for (values) |v| {
            const len: u32 = @intCast(v.len);
            const le = std.mem.nativeToLittle(u32, len);
            try buf.appendSlice(allocator, std.mem.asBytes(&le));
            try buf.appendSlice(allocator, v);
        }
    }

    pub fn encodeFixedLenByteArray(values: []const []const u8, allocator: std.mem.Allocator, buf: *std.ArrayList(u8)) !void {
        for (values) |v| {
            try buf.appendSlice(allocator, v);
        }
    }
};

/// RLE/Bit-Pack Hybrid encoding for repetition/definition levels.
pub const RleBitPackEncoder = struct {
    pub fn encodeDefLevels(def_levels: []const u8, bit_width: u8, allocator: std.mem.Allocator, buf: *std.ArrayList(u8)) !void {
        if (def_levels.len == 0) return;

        const length_pos = buf.items.len;
        try buf.appendSlice(allocator, &[4]u8{ 0, 0, 0, 0 });

        var i: usize = 0;
        while (i < def_levels.len) {
            const current_val = def_levels[i];
            var run_len: usize = 1;
            while (i + run_len < def_levels.len and def_levels[i + run_len] == current_val) {
                run_len += 1;
            }
            try writeVarInt(allocator, buf, @intCast(run_len << 1));
            const value_bytes = (@as(usize, bit_width) + 7) / 8;
            var val_buf = [_]u8{0} ** 4;
            val_buf[0] = current_val;
            try buf.appendSlice(allocator, val_buf[0..value_bytes]);
            i += run_len;
        }

        const encoded_len: u32 = @intCast(buf.items.len - length_pos - 4);
        const le = std.mem.nativeToLittle(u32, encoded_len);
        @memcpy(buf.items[length_pos..][0..4], std.mem.asBytes(&le));
    }

    fn writeVarInt(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), value: u64) !void {
        var v = value;
        while (v >= 0x80) {
            try buf.append(allocator, @intCast((v & 0x7F) | 0x80));
            v >>= 7;
        }
        try buf.append(allocator, @intCast(v));
    }
};

// ============================================================================
// Tests
// ============================================================================

test "PLAIN INT32 encoding" {
    const a = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(a);

    try PlainEncoder.encodeI32(&[_]i32{ 1, 256, -1 }, a, &buf);
    try std.testing.expectEqual(@as(usize, 12), buf.items.len);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x01, 0x00, 0x00, 0x00 }, buf.items[0..4]);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x00, 0x01, 0x00, 0x00 }, buf.items[4..8]);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xFF, 0xFF, 0xFF, 0xFF }, buf.items[8..12]);
}

test "PLAIN INT64 encoding" {
    const a = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(a);

    try PlainEncoder.encodeI64(&[_]i64{1000000}, a, &buf);
    try std.testing.expectEqual(@as(usize, 8), buf.items.len);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x40, 0x42, 0x0F, 0x00, 0x00, 0x00, 0x00, 0x00 }, buf.items);
}

test "PLAIN BYTE_ARRAY encoding" {
    const a = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(a);

    try PlainEncoder.encodeByteArray(&[_][]const u8{"hi"}, a, &buf);
    try std.testing.expectEqual(@as(usize, 6), buf.items.len);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x02, 0x00, 0x00, 0x00, 'h', 'i' }, buf.items);
}

test "PLAIN BOOLEAN encoding" {
    const a = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(a);

    try PlainEncoder.encodeBool(&[_]bool{ true, false, true, true, false, false, false, false, true }, a, &buf);
    try std.testing.expectEqual(@as(usize, 2), buf.items.len);
    try std.testing.expectEqual(@as(u8, 0x0D), buf.items[0]);
    try std.testing.expectEqual(@as(u8, 0x01), buf.items[1]);
}

test "PLAIN FLOAT encoding" {
    const a = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(a);

    try PlainEncoder.encodeFloat(&[_]f32{ 1.0, -1.0 }, a, &buf);
    try std.testing.expectEqual(@as(usize, 8), buf.items.len);
    // 1.0f = 0x3F800000 LE
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x00, 0x00, 0x80, 0x3F }, buf.items[0..4]);
}

test "PLAIN DOUBLE encoding" {
    const a = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(a);

    try PlainEncoder.encodeDouble(&[_]f64{1.0}, a, &buf);
    try std.testing.expectEqual(@as(usize, 8), buf.items.len);
    // 1.0 = 0x3FF0000000000000 LE
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F }, buf.items);
}

test "PLAIN FIXED_LEN_BYTE_ARRAY encoding" {
    const a = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(a);

    try PlainEncoder.encodeFixedLenByteArray(&[_][]const u8{ "ABCD", "EFGH" }, a, &buf);
    try std.testing.expectEqual(@as(usize, 8), buf.items.len);
    try std.testing.expectEqualSlices(u8, "ABCDEFGH", buf.items);
}

test "RLE definition levels encoding" {
    const a = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(a);

    const levels = [_]u8{ 1, 1, 1, 1, 1 };
    try RleBitPackEncoder.encodeDefLevels(&levels, 1, a, &buf);
    try std.testing.expectEqual(@as(usize, 6), buf.items.len);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x02, 0x00, 0x00, 0x00 }, buf.items[0..4]);
    try std.testing.expectEqual(@as(u8, 0x0A), buf.items[4]);
    try std.testing.expectEqual(@as(u8, 0x01), buf.items[5]);
}
