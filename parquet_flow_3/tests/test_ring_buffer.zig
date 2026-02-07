const std = @import("std");
const ring_buffer_mod = @import("ring_buffer");
const RingBuffer = ring_buffer_mod.RingBuffer;

// ============================================================
// Single-threaded tests
// ============================================================

test "push and pop single item" {
    var rb = RingBuffer(u32, 4).init();

    try std.testing.expect(rb.tryPush(42));
    try std.testing.expectEqual(@as(?u32, 42), rb.tryPop());
}

test "tryPush returns false when full" {
    var rb = RingBuffer(u32, 4).init();

    // With capacity=4, usable slots = 3 (one sentinel)
    try std.testing.expect(rb.tryPush(1));
    try std.testing.expect(rb.tryPush(2));
    try std.testing.expect(rb.tryPush(3));
    try std.testing.expect(!rb.tryPush(4)); // should be full
}

test "tryPop returns null when empty" {
    var rb = RingBuffer(u32, 4).init();
    try std.testing.expectEqual(@as(?u32, null), rb.tryPop());
}

test "isEmpty and len" {
    var rb = RingBuffer(u32, 8).init();

    try std.testing.expect(rb.isEmpty());
    try std.testing.expectEqual(@as(u32, 0), rb.len());

    try std.testing.expect(rb.tryPush(10));
    try std.testing.expect(!rb.isEmpty());
    try std.testing.expectEqual(@as(u32, 1), rb.len());

    try std.testing.expect(rb.tryPush(20));
    try std.testing.expectEqual(@as(u32, 2), rb.len());

    _ = rb.tryPop();
    try std.testing.expectEqual(@as(u32, 1), rb.len());

    _ = rb.tryPop();
    try std.testing.expect(rb.isEmpty());
    try std.testing.expectEqual(@as(u32, 0), rb.len());
}

test "wraparound correctness" {
    var rb = RingBuffer(u32, 4).init();

    // Fill and drain multiple times to force wraparound
    for (0..20) |i| {
        try std.testing.expect(rb.tryPush(@intCast(i)));
        try std.testing.expectEqual(@as(?u32, @intCast(i)), rb.tryPop());
    }
}

test "fill, drain, refill" {
    var rb = RingBuffer(u32, 8).init();

    // Fill to capacity (7 usable slots)
    for (0..7) |i| {
        try std.testing.expect(rb.tryPush(@intCast(i)));
    }
    try std.testing.expect(!rb.tryPush(99));

    // Drain all
    for (0..7) |i| {
        try std.testing.expectEqual(@as(?u32, @intCast(i)), rb.tryPop());
    }
    try std.testing.expectEqual(@as(?u32, null), rb.tryPop());

    // Refill
    for (100..107) |i| {
        try std.testing.expect(rb.tryPush(@intCast(i)));
    }
    for (100..107) |i| {
        try std.testing.expectEqual(@as(?u32, @intCast(i)), rb.tryPop());
    }
}

test "drainBatch basic" {
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
    // 2 items remaining
    try std.testing.expectEqual(@as(u32, 2), rb.len());
}

test "drainBatch when empty" {
    var rb = RingBuffer(u32, 4).init();
    var output: [4]u32 = undefined;
    const count = rb.drainBatch(&output, 4);
    try std.testing.expectEqual(@as(u32, 0), count);
}

test "drainBatch drains all available when fewer than max_count" {
    var rb = RingBuffer(u32, 8).init();

    try std.testing.expect(rb.tryPush(10));
    try std.testing.expect(rb.tryPush(20));

    var output: [8]u32 = undefined;
    const count = rb.drainBatch(&output, 8);
    try std.testing.expectEqual(@as(u32, 2), count);
    try std.testing.expectEqual(@as(u32, 10), output[0]);
    try std.testing.expectEqual(@as(u32, 20), output[1]);
    try std.testing.expect(rb.isEmpty());
}

// ============================================================
// Concurrent tests
// ============================================================

test "concurrent producer/consumer - no data loss" {
    const num_items: u32 = 100_000;

    var rb = RingBuffer(u32, 1024).init();

    var consumer_error = std.atomic.Value(bool).init(false);

    const producer = try std.Thread.spawn(.{}, struct {
        fn run(ring: *RingBuffer(u32, 1024), count: u32) void {
            var i: u32 = 0;
            while (i < count) {
                if (ring.tryPush(i)) {
                    i += 1;
                } else {
                    std.Thread.yield() catch {};
                }
            }
        }
    }.run, .{ &rb, num_items });

    const consumer = try std.Thread.spawn(.{}, struct {
        fn run(ring: *RingBuffer(u32, 1024), count: u32, err_flag: *std.atomic.Value(bool)) void {
            var expected: u32 = 0;
            while (expected < count) {
                if (ring.tryPop()) |val| {
                    if (val != expected) {
                        err_flag.store(true, .release);
                        return;
                    }
                    expected += 1;
                } else {
                    std.Thread.yield() catch {};
                }
            }
        }
    }.run, .{ &rb, num_items, &consumer_error });

    producer.join();
    consumer.join();

    try std.testing.expect(!consumer_error.load(.acquire));
    try std.testing.expect(rb.isEmpty());
}

test "concurrent producer/consumer with drainBatch" {
    const num_items: u32 = 50_000;

    var rb = RingBuffer(u32, 1024).init();

    var consumer_error = std.atomic.Value(bool).init(false);

    const producer = try std.Thread.spawn(.{}, struct {
        fn run(ring: *RingBuffer(u32, 1024), count: u32) void {
            var i: u32 = 0;
            while (i < count) {
                if (ring.tryPush(i)) {
                    i += 1;
                } else {
                    std.Thread.yield() catch {};
                }
            }
        }
    }.run, .{ &rb, num_items });

    const consumer = try std.Thread.spawn(.{}, struct {
        fn run(ring: *RingBuffer(u32, 1024), count: u32, err_flag: *std.atomic.Value(bool)) void {
            var expected: u32 = 0;
            var buf: [64]u32 = undefined;
            while (expected < count) {
                const n = ring.drainBatch(&buf, 64);
                for (0..n) |i| {
                    if (buf[i] != expected) {
                        err_flag.store(true, .release);
                        return;
                    }
                    expected += 1;
                }
                if (n == 0) {
                    std.Thread.yield() catch {};
                }
            }
        }
    }.run, .{ &rb, num_items, &consumer_error });

    producer.join();
    consumer.join();

    try std.testing.expect(!consumer_error.load(.acquire));
    try std.testing.expect(rb.isEmpty());
}
