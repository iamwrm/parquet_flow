const std = @import("std");
const Allocator = std.mem.Allocator;

/// Thrift Compact Protocol type IDs
pub const ThriftType = enum(u8) {
    STOP = 0,
    BOOL_TRUE = 1,
    BOOL_FALSE = 2,
    BYTE = 3,
    I16 = 4,
    I32 = 5,
    I64 = 6,
    DOUBLE = 7,
    BINARY = 8,
    LIST = 9,
    SET = 10,
    MAP = 11,
    STRUCT = 12,
};

/// Thrift TCompactProtocol writer.
/// Operates on a growable byte buffer (unmanaged ArrayList(u8)).
pub const CompactProtocolWriter = struct {
    buffer: std.ArrayList(u8),
    field_id_stack: std.ArrayList(i16),
    last_field_id: i16,
    gpa: Allocator,

    pub fn init(allocator: Allocator) CompactProtocolWriter {
        return .{
            .buffer = .empty,
            .field_id_stack = .empty,
            .last_field_id = 0,
            .gpa = allocator,
        };
    }

    pub fn deinit(self: *CompactProtocolWriter) void {
        self.buffer.deinit(self.gpa);
        self.field_id_stack.deinit(self.gpa);
    }

    pub fn getWritten(self: *const CompactProtocolWriter) []const u8 {
        return self.buffer.items;
    }

    pub fn reset(self: *CompactProtocolWriter) void {
        self.buffer.clearRetainingCapacity();
        self.field_id_stack.clearRetainingCapacity();
        self.last_field_id = 0;
    }

    pub fn writeVarint(self: *CompactProtocolWriter, value: u64) !void {
        var v = value;
        while (v >= 0x80) {
            try self.buffer.append(self.gpa, @as(u8, @intCast(v & 0x7F)) | 0x80);
            v >>= 7;
        }
        try self.buffer.append(self.gpa, @as(u8, @intCast(v & 0x7F)));
    }

    pub fn writeZigZag(self: *CompactProtocolWriter, value: i64) !void {
        const encoded: u64 = @bitCast((value << 1) ^ (value >> 63));
        try self.writeVarint(encoded);
    }

    pub fn writeFieldBegin(self: *CompactProtocolWriter, field_type: ThriftType, field_id: i16) !void {
        const delta = field_id - self.last_field_id;
        if (delta > 0 and delta <= 15) {
            try self.buffer.append(self.gpa, @as(u8, @intCast(delta)) << 4 | @intFromEnum(field_type));
        } else {
            try self.buffer.append(self.gpa, @intFromEnum(field_type));
            try self.writeZigZag(@as(i64, field_id));
        }
        self.last_field_id = field_id;
    }

    pub fn writeFieldStop(self: *CompactProtocolWriter) !void {
        try self.buffer.append(self.gpa, 0x00);
    }

    pub fn writeStructBegin(self: *CompactProtocolWriter) !void {
        try self.field_id_stack.append(self.gpa, self.last_field_id);
        self.last_field_id = 0;
    }

    pub fn writeStructEnd(self: *CompactProtocolWriter) !void {
        try self.writeFieldStop();
        self.last_field_id = self.field_id_stack.pop().?;
    }

    pub fn writeListBegin(self: *CompactProtocolWriter, element_type: ThriftType, length: u32) !void {
        if (length < 15) {
            try self.buffer.append(self.gpa, @as(u8, @intCast(length)) << 4 | @intFromEnum(element_type));
        } else {
            try self.buffer.append(self.gpa, 0xF0 | @intFromEnum(element_type));
            try self.writeVarint(@as(u64, length));
        }
    }

    pub fn writeI16(self: *CompactProtocolWriter, value: i16) !void {
        try self.writeZigZag(@as(i64, value));
    }

    pub fn writeI32(self: *CompactProtocolWriter, value: i32) !void {
        try self.writeZigZag(@as(i64, value));
    }

    pub fn writeI64(self: *CompactProtocolWriter, value: i64) !void {
        try self.writeZigZag(value);
    }

    pub fn writeDouble(self: *CompactProtocolWriter, value: f64) !void {
        const bytes: [8]u8 = @bitCast(value);
        try self.buffer.appendSlice(self.gpa, &bytes);
    }

    pub fn writeBool(self: *CompactProtocolWriter, value: bool) !void {
        try self.buffer.append(self.gpa, if (value) @as(u8, 1) else @as(u8, 0));
    }

    pub fn writeBinary(self: *CompactProtocolWriter, data: []const u8) !void {
        try self.writeVarint(@as(u64, data.len));
        try self.buffer.appendSlice(self.gpa, data);
    }

    pub fn writeString(self: *CompactProtocolWriter, s: []const u8) !void {
        try self.writeBinary(s);
    }

    // ---- Convenience helpers ----

    pub fn writeFieldI32(self: *CompactProtocolWriter, field_id: i16, value: i32) !void {
        try self.writeFieldBegin(.I32, field_id);
        try self.writeI32(value);
    }

    pub fn writeFieldI64(self: *CompactProtocolWriter, field_id: i16, value: i64) !void {
        try self.writeFieldBegin(.I64, field_id);
        try self.writeI64(value);
    }

    pub fn writeFieldString(self: *CompactProtocolWriter, field_id: i16, value: []const u8) !void {
        try self.writeFieldBegin(.BINARY, field_id);
        try self.writeString(value);
    }

    pub fn writeFieldBool(self: *CompactProtocolWriter, field_id: i16, value: bool) !void {
        try self.writeFieldBegin(if (value) .BOOL_TRUE else .BOOL_FALSE, field_id);
    }

    pub fn writeFieldStruct(self: *CompactProtocolWriter, field_id: i16) !void {
        try self.writeFieldBegin(.STRUCT, field_id);
    }

    pub fn writeFieldList(self: *CompactProtocolWriter, field_id: i16, element_type: ThriftType, length: u32) !void {
        try self.writeFieldBegin(.LIST, field_id);
        try self.writeListBegin(element_type, length);
    }
};
