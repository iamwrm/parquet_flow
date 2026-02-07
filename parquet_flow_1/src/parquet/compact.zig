const std = @import("std");

pub const FieldType = enum(u8) {
    stop = 0,
    boolean_true = 1,
    boolean_false = 2,
    i8 = 3,
    i16 = 4,
    i32 = 5,
    i64 = 6,
    double = 7,
    binary = 8,
    list = 9,
    set = 10,
    map = 11,
    @"struct" = 12,
};

pub fn writeFieldHeader(
    buf: *std.ArrayList(u8),
    gpa: std.mem.Allocator,
    last_field_id: *i16,
    field_id: i16,
    field_type: FieldType,
) !void {
    const delta = field_id - last_field_id.*;
    if (delta > 0 and delta <= 15) {
        const header: u8 = (@as(u8, @intCast(delta)) << 4) | @intFromEnum(field_type);
        try buf.append(gpa, header);
    } else {
        try buf.append(gpa, @intFromEnum(field_type));
        try writeI16(buf, gpa, field_id);
    }
    last_field_id.* = field_id;
}

pub fn writeStructStop(buf: *std.ArrayList(u8), gpa: std.mem.Allocator) !void {
    try buf.append(gpa, @intFromEnum(FieldType.stop));
}

pub fn writeListHeader(
    buf: *std.ArrayList(u8),
    gpa: std.mem.Allocator,
    elem_type: FieldType,
    len: usize,
) !void {
    if (len <= 14) {
        const header: u8 = (@as(u8, @intCast(len)) << 4) | @intFromEnum(elem_type);
        try buf.append(gpa, header);
        return;
    }

    const header: u8 = 0xF0 | @intFromEnum(elem_type);
    try buf.append(gpa, header);
    try writeUVarInt(buf, gpa, @intCast(len));
}

pub fn writeI16(buf: *std.ArrayList(u8), gpa: std.mem.Allocator, value: i16) !void {
    try writeUVarInt(buf, gpa, zigZagI16(value));
}

pub fn writeI32(buf: *std.ArrayList(u8), gpa: std.mem.Allocator, value: i32) !void {
    try writeUVarInt(buf, gpa, zigZagI32(value));
}

pub fn writeI64(buf: *std.ArrayList(u8), gpa: std.mem.Allocator, value: i64) !void {
    try writeUVarInt(buf, gpa, zigZagI64(value));
}

pub fn writeBinary(buf: *std.ArrayList(u8), gpa: std.mem.Allocator, bytes: []const u8) !void {
    try writeUVarInt(buf, gpa, @intCast(bytes.len));
    try buf.appendSlice(gpa, bytes);
}

fn writeUVarInt(buf: *std.ArrayList(u8), gpa: std.mem.Allocator, value: u64) !void {
    var remaining = value;
    while (true) {
        var byte: u8 = @intCast(remaining & 0x7f);
        remaining >>= 7;
        if (remaining == 0) {
            try buf.append(gpa, byte);
            break;
        }
        byte |= 0x80;
        try buf.append(gpa, byte);
    }
}

fn zigZagI16(value: i16) u16 {
    const sign: u16 = @intFromBool(value < 0);
    return (@as(u16, @bitCast(value)) << 1) ^ @as(u16, 0) - sign;
}

fn zigZagI32(value: i32) u32 {
    const sign: u32 = @intFromBool(value < 0);
    return (@as(u32, @bitCast(value)) << 1) ^ @as(u32, 0) - sign;
}

fn zigZagI64(value: i64) u64 {
    const sign: u64 = @intFromBool(value < 0);
    return (@as(u64, @bitCast(value)) << 1) ^ @as(u64, 0) - sign;
}
