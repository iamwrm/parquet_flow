const std = @import("std");

const CACHE_LINE = 64;

/// Lock-free single-producer single-consumer ring buffer.
/// Capacity must be a power of 2. All memory is inline (no heap allocation).
/// Read and write heads are cache-line aligned to prevent false sharing.
pub fn RingBuffer(comptime T: type, comptime capacity: u32) type {
    if (capacity == 0 or (capacity & (capacity - 1)) != 0) {
        @compileError("RingBuffer capacity must be a power of 2");
    }

    const mask: u32 = capacity - 1;

    return struct {
        const Self = @This();

        // Cache-line aligned write head (producer)
        write_head: std.atomic.Value(u32) align(CACHE_LINE) = std.atomic.Value(u32).init(0),
        _pad1: [CACHE_LINE - @sizeOf(std.atomic.Value(u32))]u8 = undefined,

        // Cache-line aligned read head (consumer)
        read_head: std.atomic.Value(u32) align(CACHE_LINE) = std.atomic.Value(u32).init(0),
        _pad2: [CACHE_LINE - @sizeOf(std.atomic.Value(u32))]u8 = undefined,

        // Data buffer
        buffer: [capacity]T = undefined,

        pub fn init() Self {
            return Self{};
        }

        /// Producer side -- returns false if full (NEVER blocks).
        pub fn tryPush(self: *Self, item: T) bool {
            const w = self.write_head.load(.monotonic);
            const next_w = (w +% 1) & mask;
            const r = self.read_head.load(.acquire);
            if (next_w == r) return false; // full
            self.buffer[w] = item;
            self.write_head.store(next_w, .release);
            return true;
        }

        /// Consumer side -- returns null if empty.
        pub fn tryPop(self: *Self) ?T {
            const r = self.read_head.load(.monotonic);
            const w = self.write_head.load(.acquire);
            if (r == w) return null; // empty
            const item = self.buffer[r];
            self.read_head.store((r +% 1) & mask, .release);
            return item;
        }

        /// Batch drain -- reads up to max_count items into output slice.
        /// Returns number of items actually read.
        pub fn drainBatch(self: *Self, output: []T, max_count: u32) u32 {
            var r = self.read_head.load(.monotonic);
            const w = self.write_head.load(.acquire);

            var count: u32 = 0;
            const limit = @min(max_count, @as(u32, @intCast(output.len)));

            while (count < limit and r != w) {
                output[count] = self.buffer[r];
                r = (r +% 1) & mask;
                count += 1;
            }

            if (count > 0) {
                self.read_head.store(r, .release);
            }

            return count;
        }

        /// Returns true if the buffer is empty.
        pub fn isEmpty(self: *Self) bool {
            const r = self.read_head.load(.monotonic);
            const w = self.write_head.load(.monotonic);
            return r == w;
        }

        /// Returns the number of items currently in the buffer.
        pub fn len(self: *Self) u32 {
            const w = self.write_head.load(.monotonic);
            const r = self.read_head.load(.monotonic);
            return (w -% r) & mask;
        }
    };
}

test "RingBuffer basic push/pop" {
    var rb = RingBuffer(u32, 4).init();

    try std.testing.expect(rb.isEmpty());
    try std.testing.expectEqual(@as(u32, 0), rb.len());

    try std.testing.expect(rb.tryPush(10));
    try std.testing.expect(rb.tryPush(20));
    try std.testing.expect(rb.tryPush(30));

    // Capacity is 4 but usable slots = 3 (one slot wasted for full detection)
    try std.testing.expect(!rb.tryPush(40));

    try std.testing.expectEqual(@as(?u32, 10), rb.tryPop());
    try std.testing.expectEqual(@as(?u32, 20), rb.tryPop());
    try std.testing.expectEqual(@as(?u32, 30), rb.tryPop());
    try std.testing.expectEqual(@as(?u32, null), rb.tryPop());
    try std.testing.expect(rb.isEmpty());
}

test "RingBuffer drainBatch" {
    var rb = RingBuffer(u32, 8).init();

    for (0..7) |i| {
        try std.testing.expect(rb.tryPush(@intCast(i)));
    }

    var output: [8]u32 = undefined;
    const count = rb.drainBatch(&output, 5);
    try std.testing.expectEqual(@as(u32, 5), count);
    for (0..5) |i| {
        try std.testing.expectEqual(@as(u32, @intCast(i)), output[i]);
    }
    try std.testing.expectEqual(@as(u32, 2), rb.len());
}
