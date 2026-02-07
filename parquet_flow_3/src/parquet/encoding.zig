const std = @import("std");
const Allocator = std.mem.Allocator;

/// Encode values in PLAIN format (little-endian packed).
pub fn encodePlain(comptime T: type, values: []const T, buf: *std.ArrayList(u8), gpa: Allocator) !void {
    for (values) |v| {
        const bytes: [@sizeOf(T)]u8 = @bitCast(v);
        try buf.appendSlice(gpa, &bytes);
    }
}

/// Encode booleans in PLAIN format: bit-packed, LSB-first.
pub fn encodePlainBool(values: []const bool, buf: *std.ArrayList(u8), gpa: Allocator) !void {
    var byte: u8 = 0;
    var bit_index: u3 = 0;
    for (values) |v| {
        if (v) {
            byte |= @as(u8, 1) << bit_index;
        }
        if (bit_index == 7) {
            try buf.append(gpa, byte);
            byte = 0;
            bit_index = 0;
        } else {
            bit_index += 1;
        }
    }
    if (bit_index > 0) {
        try buf.append(gpa, byte);
    }
}

/// Encode byte arrays in PLAIN format: 4-byte LE length prefix + data for each.
pub fn encodePlainByteArray(values: []const []const u8, buf: *std.ArrayList(u8), gpa: Allocator) !void {
    for (values) |v| {
        var len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_bytes, @intCast(v.len), .little);
        try buf.appendSlice(gpa, &len_bytes);
        try buf.appendSlice(gpa, v);
    }
}

/// RLE/Bit-Pack Hybrid encoder.
pub fn encodeRleBitPackedHybrid(values: []const u32, bit_width: u5, buf: *std.ArrayList(u8), gpa: Allocator) !void {
    if (values.len == 0) return;
    if (bit_width == 0) return;

    const byte_width: u32 = (@as(u32, bit_width) + 7) / 8;
    var i: usize = 0;

    while (i < values.len) {
        // Count run of identical values
        var run_len: usize = 1;
        while (i + run_len < values.len and values[i + run_len] == values[i]) {
            run_len += 1;
        }

        if (run_len >= 8) {
            // RLE run: header = (count << 1)
            try writeVarintToBuf(@as(u64, run_len) << 1, buf, gpa);
            const val = values[i];
            var j: u32 = 0;
            while (j < byte_width) : (j += 1) {
                try buf.append(gpa, @as(u8, @intCast((val >> @intCast(j * 8)) & 0xFF)));
            }
            i += run_len;
        } else {
            // Bit-packed run: collect values until next RLE-eligible run
            var bp_count: usize = 0;
            var scan = i;
            while (scan < values.len) {
                var next_run: usize = 1;
                while (scan + next_run < values.len and values[scan + next_run] == values[scan]) {
                    next_run += 1;
                }
                if (next_run >= 8 and bp_count > 0) break;
                if (next_run >= 8 and bp_count == 0) {
                    bp_count += next_run;
                    scan += next_run;
                    break;
                }
                bp_count += next_run;
                scan += next_run;
            }

            // Round up to multiple of 8
            const padded_count = ((bp_count + 7) / 8) * 8;
            const num_groups = padded_count / 8;

            // Header: (num_groups << 1) | 1
            try writeVarintToBuf((@as(u64, num_groups) << 1) | 1, buf, gpa);

            // Bit-pack values
            var bit_buf: u64 = 0;
            var bits_in_buf: u6 = 0;

            var k: usize = 0;
            while (k < padded_count) : (k += 1) {
                const val: u32 = if (i + k < values.len) values[i + k] else 0;
                bit_buf |= @as(u64, val) << bits_in_buf;
                bits_in_buf += @intCast(bit_width);

                while (bits_in_buf >= 8) {
                    try buf.append(gpa, @as(u8, @intCast(bit_buf & 0xFF)));
                    bit_buf >>= 8;
                    bits_in_buf -= 8;
                }
            }
            if (bits_in_buf > 0) {
                try buf.append(gpa, @as(u8, @intCast(bit_buf & 0xFF)));
            }

            i += bp_count;
        }
    }
}

/// Encode definition levels: 4-byte LE length prefix, then RLE/bit-packed hybrid data.
pub fn encodeDefinitionLevels(values: []const u8, max_level: u8, out: *std.ArrayList(u8), gpa: Allocator) !void {
    if (max_level == 0) return;

    var bit_width: u5 = 0;
    {
        var tmp = max_level;
        while (tmp > 0) {
            bit_width += 1;
            tmp >>= 1;
        }
    }

    // Encode to temp buffer to compute length
    var tmp_buf: std.ArrayList(u8) = .empty;
    defer tmp_buf.deinit(gpa);

    // Convert u8 to u32
    var values32: std.ArrayList(u32) = .empty;
    defer values32.deinit(gpa);
    try values32.ensureTotalCapacity(gpa, values.len);
    for (values) |v| {
        try values32.append(gpa, @as(u32, v));
    }

    try encodeRleBitPackedHybrid(values32.items, bit_width, &tmp_buf, gpa);

    // Write 4-byte LE length prefix
    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, @intCast(tmp_buf.items.len), .little);
    try out.appendSlice(gpa, &len_bytes);
    try out.appendSlice(gpa, tmp_buf.items);
}

fn writeVarintToBuf(value: u64, buf: *std.ArrayList(u8), gpa: Allocator) !void {
    var v = value;
    while (v >= 0x80) {
        try buf.append(gpa, @as(u8, @intCast(v & 0x7F)) | 0x80);
        v >>= 7;
    }
    try buf.append(gpa, @as(u8, @intCast(v & 0x7F)));
}
