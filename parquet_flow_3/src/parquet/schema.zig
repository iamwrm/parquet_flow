const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const thrift = @import("thrift.zig");
const CompactProtocolWriter = thrift.CompactProtocolWriter;

/// Represents a single element in the Parquet schema tree.
pub const SchemaElement = struct {
    name: []const u8,
    physical_type: ?types.PhysicalType = null,
    type_length: ?i32 = null,
    repetition_type: ?types.FieldRepetitionType = null,
    num_children: ?i32 = null,
    converted_type: ?types.ConvertedType = null,
    logical_type: ?types.LogicalType = null,
    scale: ?i32 = null,
    precision: ?i32 = null,
};

/// Column definition used to build a schema.
pub const ColumnDef = struct {
    name: []const u8,
    physical_type: types.PhysicalType,
    type_length: ?i32 = null,
    repetition_type: types.FieldRepetitionType = .REQUIRED,
    converted_type: ?types.ConvertedType = null,
    logical_type: ?types.LogicalType = null,
};

/// Schema: a list of SchemaElements with a root element.
pub const Schema = struct {
    elements: std.ArrayList(SchemaElement),
    gpa: Allocator,

    pub fn init(allocator: Allocator) Schema {
        return .{
            .elements = .empty,
            .gpa = allocator,
        };
    }

    pub fn deinit(self: *Schema) void {
        self.elements.deinit(self.gpa);
    }

    /// Build schema from column definitions.
    pub fn buildFromColumns(allocator: Allocator, columns: []const ColumnDef) !Schema {
        var schema: Schema = .{
            .elements = .empty,
            .gpa = allocator,
        };
        errdefer schema.deinit();

        // Root element
        try schema.elements.append(allocator, .{
            .name = "schema",
            .num_children = @intCast(columns.len),
        });

        // Leaf elements
        for (columns) |col| {
            try schema.elements.append(allocator, .{
                .name = col.name,
                .physical_type = col.physical_type,
                .type_length = col.type_length,
                .repetition_type = col.repetition_type,
                .converted_type = col.converted_type,
                .logical_type = col.logical_type,
            });
        }

        return schema;
    }
};

/// Serialize a single SchemaElement to Thrift compact protocol.
pub fn writeSchemaElement(writer: *CompactProtocolWriter, elem: SchemaElement) !void {
    if (elem.physical_type) |pt| {
        try writer.writeFieldI32(1, @intFromEnum(pt));
    }
    if (elem.type_length) |tl| {
        try writer.writeFieldI32(2, tl);
    }
    if (elem.repetition_type) |rt| {
        try writer.writeFieldI32(3, @intFromEnum(rt));
    }
    try writer.writeFieldString(4, elem.name);
    if (elem.num_children) |nc| {
        try writer.writeFieldI32(5, nc);
    }
    if (elem.converted_type) |ct| {
        try writer.writeFieldI32(6, @intFromEnum(ct));
    }
    if (elem.scale) |s| {
        try writer.writeFieldI32(7, s);
    }
    if (elem.precision) |p| {
        try writer.writeFieldI32(8, p);
    }
    if (elem.logical_type) |lt| {
        try writeLogicalType(writer, lt);
    }
    try writer.writeFieldStop();
}

fn writeLogicalType(writer: *CompactProtocolWriter, lt: types.LogicalType) !void {
    try writer.writeFieldStruct(10);
    try writer.writeStructBegin();
    switch (lt) {
        .STRING => {
            try writer.writeFieldStruct(1);
            try writer.writeStructBegin();
            try writer.writeStructEnd();
        },
        .MAP => {
            try writer.writeFieldStruct(2);
            try writer.writeStructBegin();
            try writer.writeStructEnd();
        },
        .LIST => {
            try writer.writeFieldStruct(3);
            try writer.writeStructBegin();
            try writer.writeStructEnd();
        },
        .ENUM => {
            try writer.writeFieldStruct(4);
            try writer.writeStructBegin();
            try writer.writeStructEnd();
        },
        .DECIMAL => |d| {
            try writer.writeFieldStruct(5);
            try writer.writeStructBegin();
            try writer.writeFieldI32(1, d.scale);
            try writer.writeFieldI32(2, d.precision);
            try writer.writeStructEnd();
        },
        .DATE => {
            try writer.writeFieldStruct(6);
            try writer.writeStructBegin();
            try writer.writeStructEnd();
        },
        .TIME => |t| {
            try writer.writeFieldStruct(7);
            try writer.writeStructBegin();
            try writer.writeFieldBool(1, t.is_adjusted_to_utc);
            try writer.writeFieldStruct(2);
            try writer.writeStructBegin();
            try writeTimeUnit(writer, t.unit);
            try writer.writeStructEnd();
            try writer.writeStructEnd();
        },
        .TIMESTAMP => |t| {
            try writer.writeFieldStruct(8);
            try writer.writeStructBegin();
            try writer.writeFieldBool(1, t.is_adjusted_to_utc);
            try writer.writeFieldStruct(2);
            try writer.writeStructBegin();
            try writeTimeUnit(writer, t.unit);
            try writer.writeStructEnd();
            try writer.writeStructEnd();
        },
        .INTEGER => |i_val| {
            try writer.writeFieldStruct(10);
            try writer.writeStructBegin();
            try writer.writeFieldBegin(.BYTE, 1);
            try writer.buffer.append(writer.gpa, @as(u8, @bitCast(i_val.bit_width)));
            try writer.writeFieldBool(2, i_val.is_signed);
            try writer.writeStructEnd();
        },
        .UNKNOWN => {
            try writer.writeFieldStruct(11);
            try writer.writeStructBegin();
            try writer.writeStructEnd();
        },
        .JSON => {
            try writer.writeFieldStruct(12);
            try writer.writeStructBegin();
            try writer.writeStructEnd();
        },
        .BSON => {
            try writer.writeFieldStruct(13);
            try writer.writeStructBegin();
            try writer.writeStructEnd();
        },
        .UUID => {
            try writer.writeFieldStruct(14);
            try writer.writeStructBegin();
            try writer.writeStructEnd();
        },
        .FLOAT16 => {
            try writer.writeFieldStruct(15);
            try writer.writeStructBegin();
            try writer.writeStructEnd();
        },
    }
    try writer.writeStructEnd();
}

fn writeTimeUnit(writer: *CompactProtocolWriter, unit: types.TimeUnit) !void {
    switch (unit) {
        .MILLIS => {
            try writer.writeFieldStruct(1);
            try writer.writeStructBegin();
            try writer.writeStructEnd();
        },
        .MICROS => {
            try writer.writeFieldStruct(2);
            try writer.writeStructBegin();
            try writer.writeStructEnd();
        },
        .NANOS => {
            try writer.writeFieldStruct(3);
            try writer.writeStructBegin();
            try writer.writeStructEnd();
        },
    }
}
