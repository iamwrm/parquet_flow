const std = @import("std");

/// Thrift Compact Protocol type IDs (4-bit).
pub const Type = struct {
    pub const BOOLEAN_TRUE: u4 = 1;
    pub const BOOLEAN_FALSE: u4 = 2;
    pub const I8: u4 = 3;
    pub const I16: u4 = 4;
    pub const I32: u4 = 5;
    pub const I64: u4 = 6;
    pub const DOUBLE: u4 = 7;
    pub const BINARY: u4 = 8;
    pub const LIST: u4 = 9;
    pub const SET: u4 = 10;
    pub const MAP: u4 = 11;
    pub const STRUCT: u4 = 12;
};

pub const ThriftCompactEncoder = struct {
    buf: std.ArrayList(u8),
    allocator: std.mem.Allocator,
    field_id_stack: [16]i16,
    stack_depth: u8,
    last_field_id: i16,

    pub fn init(allocator: std.mem.Allocator) ThriftCompactEncoder {
        return .{
            .buf = .empty,
            .allocator = allocator,
            .field_id_stack = [_]i16{0} ** 16,
            .stack_depth = 0,
            .last_field_id = 0,
        };
    }

    pub fn deinit(self: *ThriftCompactEncoder) void {
        self.buf.deinit(self.allocator);
    }

    pub fn getWritten(self: *const ThriftCompactEncoder) []const u8 {
        return self.buf.items;
    }

    pub fn reset(self: *ThriftCompactEncoder) void {
        self.buf.clearRetainingCapacity();
        self.stack_depth = 0;
        self.last_field_id = 0;
    }

    // --- Varint encoding ---

    pub fn writeVarint(self: *ThriftCompactEncoder, value: u64) !void {
        var v = value;
        while (v >= 0x80) {
            try self.buf.append(self.allocator, @intCast((v & 0x7F) | 0x80));
            v >>= 7;
        }
        try self.buf.append(self.allocator, @intCast(v));
    }

    pub fn writeZigzagVarint(self: *ThriftCompactEncoder, value: i64) !void {
        const zigzag: u64 = @bitCast((value << 1) ^ (value >> 63));
        try self.writeVarint(zigzag);
    }

    // --- Field headers ---

    pub fn writeFieldBegin(self: *ThriftCompactEncoder, field_id: i16, type_id: u4) !void {
        const delta = field_id - self.last_field_id;
        if (delta > 0 and delta <= 15) {
            try self.buf.append(self.allocator, (@as(u8, @intCast(delta)) << 4) | @as(u8, type_id));
        } else {
            try self.buf.append(self.allocator, @as(u8, type_id));
            try self.writeZigzagVarint(@as(i64, field_id));
        }
        self.last_field_id = field_id;
    }

    // --- Struct ---

    pub fn writeStructBegin(self: *ThriftCompactEncoder) void {
        self.field_id_stack[self.stack_depth] = self.last_field_id;
        self.stack_depth += 1;
        self.last_field_id = 0;
    }

    pub fn writeStructEnd(self: *ThriftCompactEncoder) !void {
        try self.buf.append(self.allocator, 0x00); // stop byte
        self.stack_depth -= 1;
        self.last_field_id = self.field_id_stack[self.stack_depth];
    }

    // --- List ---

    pub fn writeListBegin(self: *ThriftCompactEncoder, elem_type: u4, size: u32) !void {
        if (size < 15) {
            try self.buf.append(self.allocator, (@as(u8, @intCast(size)) << 4) | @as(u8, elem_type));
        } else {
            try self.buf.append(self.allocator, 0xF0 | @as(u8, elem_type));
            try self.writeVarint(@as(u64, size));
        }
    }

    // --- Primitive types ---

    pub fn writeBinary(self: *ThriftCompactEncoder, data: []const u8) !void {
        try self.writeVarint(@as(u64, data.len));
        try self.buf.appendSlice(self.allocator, data);
    }

    pub fn writeI32(self: *ThriftCompactEncoder, value: i32) !void {
        try self.writeZigzagVarint(@as(i64, value));
    }

    pub fn writeI64(self: *ThriftCompactEncoder, value: i64) !void {
        try self.writeZigzagVarint(value);
    }

    pub fn writeBool(self: *ThriftCompactEncoder, field_id: i16, value: bool) !void {
        const type_id: u4 = if (value) Type.BOOLEAN_TRUE else Type.BOOLEAN_FALSE;
        try self.writeFieldBegin(field_id, type_id);
    }

    /// Write an i32 field: field header + zigzag-varint value.
    pub fn writeI32Field(self: *ThriftCompactEncoder, field_id: i16, value: i32) !void {
        try self.writeFieldBegin(field_id, Type.I32);
        try self.writeI32(value);
    }

    /// Write an i64 field: field header + zigzag-varint value.
    pub fn writeI64Field(self: *ThriftCompactEncoder, field_id: i16, value: i64) !void {
        try self.writeFieldBegin(field_id, Type.I64);
        try self.writeI64(value);
    }

    /// Write a binary/string field: field header + varint length + bytes.
    pub fn writeBinaryField(self: *ThriftCompactEncoder, field_id: i16, data: []const u8) !void {
        try self.writeFieldBegin(field_id, Type.BINARY);
        try self.writeBinary(data);
    }

    /// Write a struct field header (caller must then write struct contents + writeStructEnd).
    pub fn writeStructField(self: *ThriftCompactEncoder, field_id: i16) !void {
        try self.writeFieldBegin(field_id, Type.STRUCT);
        self.writeStructBegin();
    }

    /// Write a list field header.
    pub fn writeListField(self: *ThriftCompactEncoder, field_id: i16, elem_type: u4, size: u32) !void {
        try self.writeFieldBegin(field_id, Type.LIST);
        try self.writeListBegin(elem_type, size);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "varint encoding" {
    var enc = ThriftCompactEncoder.init(std.testing.allocator);
    defer enc.deinit();

    try enc.writeVarint(0);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x00}, enc.getWritten());
    enc.reset();

    try enc.writeVarint(1);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x01}, enc.getWritten());
    enc.reset();

    try enc.writeVarint(127);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x7F}, enc.getWritten());
    enc.reset();

    try enc.writeVarint(128);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x80, 0x01 }, enc.getWritten());
    enc.reset();

    try enc.writeVarint(300);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xAC, 0x02 }, enc.getWritten());
    enc.reset();
}

test "zigzag varint encoding" {
    var enc = ThriftCompactEncoder.init(std.testing.allocator);
    defer enc.deinit();

    try enc.writeZigzagVarint(0);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x00}, enc.getWritten());
    enc.reset();

    try enc.writeZigzagVarint(-1);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x01}, enc.getWritten());
    enc.reset();

    try enc.writeZigzagVarint(1);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x02}, enc.getWritten());
    enc.reset();

    try enc.writeZigzagVarint(-2);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x03}, enc.getWritten());
    enc.reset();

    try enc.writeZigzagVarint(100);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xC8, 0x01 }, enc.getWritten());
    enc.reset();
}

test "field header short form" {
    var enc = ThriftCompactEncoder.init(std.testing.allocator);
    defer enc.deinit();

    try enc.writeFieldBegin(1, Type.I32);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x15}, enc.getWritten());

    try enc.writeFieldBegin(2, Type.I64);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x15, 0x16 }, enc.getWritten());
}

test "struct begin/end with stop byte" {
    var enc = ThriftCompactEncoder.init(std.testing.allocator);
    defer enc.deinit();

    enc.writeStructBegin();
    try enc.writeI32Field(1, 42);
    try enc.writeStructEnd();

    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x15, 0x54, 0x00 }, enc.getWritten());
}

test "list encoding" {
    var enc = ThriftCompactEncoder.init(std.testing.allocator);
    defer enc.deinit();

    try enc.writeListBegin(Type.STRUCT, 3);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x3C}, enc.getWritten());
    enc.reset();

    try enc.writeListBegin(Type.I32, 20);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xF5, 0x14 }, enc.getWritten());
    enc.reset();
}

test "binary encoding" {
    var enc = ThriftCompactEncoder.init(std.testing.allocator);
    defer enc.deinit();

    try enc.writeBinary("hello");
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x05, 'h', 'e', 'l', 'l', 'o' }, enc.getWritten());
}

test "nested struct field ids reset" {
    var enc = ThriftCompactEncoder.init(std.testing.allocator);
    defer enc.deinit();

    enc.writeStructBegin();
    try enc.writeI32Field(1, 10);
    try enc.writeStructField(2);
    try enc.writeI32Field(1, 20);
    try enc.writeStructEnd();
    try enc.writeI32Field(3, 30);
    try enc.writeStructEnd();

    const data = enc.getWritten();
    try std.testing.expectEqual(@as(u8, 0x15), data[0]);
    try std.testing.expectEqual(@as(u8, 0x14), data[1]);
    try std.testing.expectEqual(@as(u8, 0x1C), data[2]);
    try std.testing.expectEqual(@as(u8, 0x15), data[3]);
    try std.testing.expectEqual(@as(u8, 0x28), data[4]);
    try std.testing.expectEqual(@as(u8, 0x00), data[5]);
    try std.testing.expectEqual(@as(u8, 0x15), data[6]);
    try std.testing.expectEqual(@as(u8, 0x3C), data[7]);
    try std.testing.expectEqual(@as(u8, 0x00), data[8]);
}
