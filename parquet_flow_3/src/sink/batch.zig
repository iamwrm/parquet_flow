const std = @import("std");
const Allocator = std.mem.Allocator;

/// Physical types (mirrors parquet/types.zig -- kept local to avoid cross-directory import
/// until a build.zig with proper module paths is set up).
pub const PhysicalType = enum(i32) {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    INT96 = 3,
    FLOAT = 4,
    DOUBLE = 5,
    BYTE_ARRAY = 6,
    FIXED_LEN_BYTE_ARRAY = 7,
};

/// Describes a single column within a record.
pub const ColumnDef = struct {
    name: []const u8,
    physical_type: PhysicalType,
    type_length: u32, // for FIXED_LEN_BYTE_ARRAY
    nullable: bool,
    /// Byte offset of this column's value within a record
    offset: u32,
    /// Byte size of this column's value within a record
    size: u32,
};

/// Schema information for splitting flat records into columns.
pub const SchemaInfo = struct {
    columns: []const ColumnDef,
    /// Total byte size of one record (fixed-size portion)
    record_size: u32,
    /// Number of nullable columns (for null bitmap sizing)
    nullable_count: u32,
    /// Byte size of the null bitmap at the start of each record
    null_bitmap_bytes: u32,
};

/// Accumulates raw records and splits them into per-column buffers
/// for the Parquet writer.
pub const BatchAccumulator = struct {
    column_buffers: []std.ArrayList(u8),
    null_bitmaps: []std.ArrayList(u8),
    schema: SchemaInfo,
    row_count: u32,
    max_rows: u32,
    allocator: Allocator,

    pub fn init(allocator: Allocator, schema: SchemaInfo, max_rows: u32) !BatchAccumulator {
        const num_cols = schema.columns.len;

        const column_buffers = try allocator.alloc(std.ArrayList(u8), num_cols);
        for (column_buffers) |*buf| {
            buf.* = .empty;
        }

        const null_bitmaps = try allocator.alloc(std.ArrayList(u8), num_cols);
        for (null_bitmaps) |*buf| {
            buf.* = .empty;
        }

        return BatchAccumulator{
            .column_buffers = column_buffers,
            .null_bitmaps = null_bitmaps,
            .schema = schema,
            .row_count = 0,
            .max_rows = max_rows,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BatchAccumulator) void {
        for (self.column_buffers) |*buf| {
            buf.deinit(self.allocator);
        }
        self.allocator.free(self.column_buffers);

        for (self.null_bitmaps) |*buf| {
            buf.deinit(self.allocator);
        }
        self.allocator.free(self.null_bitmaps);
    }

    /// Add a raw fixed-size record. Splits it into per-column values
    /// based on the schema's column offsets and sizes.
    pub fn addRecord(self: *BatchAccumulator, record: []const u8) !void {
        if (record.len < self.schema.record_size) return error.RecordTooShort;

        // Parse null bitmap from start of record (if any nullable columns)
        const null_bitmap = record[0..self.schema.null_bitmap_bytes];

        var nullable_idx: u32 = 0;
        for (self.schema.columns, 0..) |col, i| {
            const start = col.offset;
            const end = start + col.size;
            const value = record[start..end];

            if (col.nullable) {
                // Check null bit
                const byte_idx = nullable_idx / 8;
                const bit_idx: u3 = @intCast(nullable_idx % 8);
                const is_null = if (byte_idx < null_bitmap.len)
                    (null_bitmap[byte_idx] >> bit_idx) & 1 == 1
                else
                    false;

                if (is_null) {
                    // Append zeros for null value (placeholder)
                    for (0..col.size) |_| {
                        try self.column_buffers[i].append(self.allocator, 0);
                    }
                } else {
                    try self.column_buffers[i].appendSlice(self.allocator, value);
                }

                // Track null in per-column bitmap
                const row = self.row_count;
                const bm_byte = row / 8;
                const bm_bit: u3 = @intCast(row % 8);

                // Extend bitmap if needed
                while (self.null_bitmaps[i].items.len <= bm_byte) {
                    try self.null_bitmaps[i].append(self.allocator, 0);
                }
                if (is_null) {
                    self.null_bitmaps[i].items[bm_byte] |= @as(u8, 1) << bm_bit;
                }

                nullable_idx += 1;
            } else {
                try self.column_buffers[i].appendSlice(self.allocator, value);
            }
        }

        self.row_count += 1;
    }

    /// Returns true if the batch has reached max_rows.
    pub fn isFull(self: *const BatchAccumulator) bool {
        return self.row_count >= self.max_rows;
    }

    /// Reset all buffers for the next batch. Retains allocated capacity.
    pub fn reset(self: *BatchAccumulator) void {
        for (self.column_buffers) |*buf| {
            buf.clearRetainingCapacity();
        }
        for (self.null_bitmaps) |*buf| {
            buf.clearRetainingCapacity();
        }
        self.row_count = 0;
    }
};

test "BatchAccumulator basic" {
    const allocator = std.testing.allocator;

    // Simple schema: 2 non-nullable INT32 columns, no null bitmap
    const columns = [_]ColumnDef{
        .{ .name = "a", .physical_type = .INT32, .type_length = 0, .nullable = false, .offset = 0, .size = 4 },
        .{ .name = "b", .physical_type = .INT32, .type_length = 0, .nullable = false, .offset = 4, .size = 4 },
    };
    const schema = SchemaInfo{
        .columns = &columns,
        .record_size = 8,
        .nullable_count = 0,
        .null_bitmap_bytes = 0,
    };

    var batch = try BatchAccumulator.init(allocator, schema, 2);
    defer batch.deinit();

    // Record: a=1, b=2 (little-endian)
    const rec1 = [_]u8{ 1, 0, 0, 0, 2, 0, 0, 0 };
    try batch.addRecord(&rec1);

    try std.testing.expectEqual(@as(u32, 1), batch.row_count);
    try std.testing.expect(!batch.isFull());

    const rec2 = [_]u8{ 3, 0, 0, 0, 4, 0, 0, 0 };
    try batch.addRecord(&rec2);

    try std.testing.expectEqual(@as(u32, 2), batch.row_count);
    try std.testing.expect(batch.isFull());

    // Column a should have [1,0,0,0, 3,0,0,0]
    try std.testing.expectEqual(@as(usize, 8), batch.column_buffers[0].items.len);
    try std.testing.expectEqual(@as(u8, 1), batch.column_buffers[0].items[0]);
    try std.testing.expectEqual(@as(u8, 3), batch.column_buffers[0].items[4]);

    batch.reset();
    try std.testing.expectEqual(@as(u32, 0), batch.row_count);
    try std.testing.expect(!batch.isFull());
}
