const std = @import("std");
const LogEntry = @import("log_entry.zig").LogEntry;
const file_writer = @import("parquet/file_writer.zig");

/// Single-Producer Single-Consumer lock-free ring buffer.
pub fn SpscRingBuffer(comptime T: type, comptime capacity: comptime_int) type {
    comptime {
        if (capacity == 0 or (capacity & (capacity - 1)) != 0)
            @compileError("capacity must be a power of 2");
    }
    const mask = capacity - 1;

    return struct {
        const Self = @This();

        buffer: [capacity]T,
        write_pos: std.atomic.Value(usize),
        read_pos: std.atomic.Value(usize),

        pub fn init() Self {
            return .{
                .buffer = undefined,
                .write_pos = std.atomic.Value(usize).init(0),
                .read_pos = std.atomic.Value(usize).init(0),
            };
        }

        /// Wait-free push. Returns false if ring is full.
        pub fn push(self: *Self, item: T) bool {
            const wp = self.write_pos.load(.monotonic);
            const rp = self.read_pos.load(.acquire);

            if (wp -% rp >= capacity) return false;

            self.buffer[wp & mask] = item;
            self.write_pos.store(wp +% 1, .release);
            return true;
        }

        /// Wait-free pop. Returns null if ring is empty.
        pub fn pop(self: *Self) ?T {
            const rp = self.read_pos.load(.monotonic);
            const wp = self.write_pos.load(.acquire);

            if (rp == wp) return null;

            const item = self.buffer[rp & mask];
            self.read_pos.store(rp +% 1, .release);
            return item;
        }

        pub fn len(self: *Self) usize {
            const wp = self.write_pos.load(.acquire);
            const rp = self.read_pos.load(.acquire);
            return wp -% rp;
        }
    };
}

const RING_CAPACITY = 8192;
const BATCH_SIZE = 1024;

fn nanosleep_ms(ms: u64) void {
    var req = std.os.linux.timespec{
        .sec = @intCast(ms / 1000),
        .nsec = @intCast((ms % 1000) * std.time.ns_per_ms),
    };
    while (true) {
        const rc = std.os.linux.nanosleep(&req, &req);
        const e = std.os.linux.errno(rc);
        if (e != .INTR) break;
    }
}

pub const LogSink = struct {
    ring: SpscRingBuffer(LogEntry, RING_CAPACITY),
    writer_thread: ?std.Thread,
    should_stop: std.atomic.Value(bool),
    output_dir: []const u8,
    allocator: std.mem.Allocator,
    files_written: std.atomic.Value(usize),
    entries_written: std.atomic.Value(usize),

    pub fn init(allocator: std.mem.Allocator, output_dir: []const u8) LogSink {
        return .{
            .ring = SpscRingBuffer(LogEntry, RING_CAPACITY).init(),
            .writer_thread = null,
            .should_stop = std.atomic.Value(bool).init(false),
            .output_dir = output_dir,
            .allocator = allocator,
            .files_written = std.atomic.Value(usize).init(0),
            .entries_written = std.atomic.Value(usize).init(0),
        };
    }

    pub fn start(self: *LogSink) !void {
        self.writer_thread = try std.Thread.spawn(.{}, writerLoop, .{self});
    }

    /// Non-blocking log push from the hot thread.
    pub fn log(self: *LogSink, severity: i32, msg: []const u8, src: []const u8) bool {
        const entry = LogEntry.init(severity, msg, src);
        return self.ring.push(entry);
    }

    pub fn stop(self: *LogSink) void {
        self.should_stop.store(true, .release);
        if (self.writer_thread) |t| {
            t.join();
            self.writer_thread = null;
        }
    }

    pub fn getFilesWritten(self: *LogSink) usize {
        return self.files_written.load(.acquire);
    }

    pub fn getEntriesWritten(self: *LogSink) usize {
        return self.entries_written.load(.acquire);
    }

    fn writerLoop(self: *LogSink) void {
        var batch: [BATCH_SIZE]LogEntry = undefined;
        var file_seq: usize = 0;

        while (!self.should_stop.load(.acquire)) {
            const count = self.drainBatch(&batch);
            if (count > 0) {
                self.writeBatch(batch[0..count], file_seq);
                file_seq += 1;
            } else {
                nanosleep_ms(1);
            }
        }

        // Final drain
        while (true) {
            const count = self.drainBatch(&batch);
            if (count == 0) break;
            self.writeBatch(batch[0..count], file_seq);
            file_seq += 1;
        }
    }

    fn drainBatch(self: *LogSink, batch: *[BATCH_SIZE]LogEntry) usize {
        var count: usize = 0;
        while (count < BATCH_SIZE) {
            if (self.ring.pop()) |entry| {
                batch[count] = entry;
                count += 1;
            } else break;
        }
        return count;
    }

    fn writeBatch(self: *LogSink, entries: []const LogEntry, seq: usize) void {
        var fname_buf: [512]u8 = undefined;
        const fname = std.fmt.bufPrint(&fname_buf, "{s}/log_{d:0>6}.parquet", .{ self.output_dir, seq }) catch return;

        file_writer.writeLogEntries(self.allocator, fname, entries) catch |err| {
            std.debug.print("parquet write error: {}\n", .{err});
            return;
        };
        _ = self.files_written.fetchAdd(1, .release);
        _ = self.entries_written.fetchAdd(entries.len, .release);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "SPSC ring buffer basic push/pop" {
    var ring = SpscRingBuffer(u32, 4).init();

    try std.testing.expect(ring.push(1));
    try std.testing.expect(ring.push(2));
    try std.testing.expect(ring.push(3));
    try std.testing.expect(ring.push(4));
    try std.testing.expect(!ring.push(5));

    try std.testing.expectEqual(@as(?u32, 1), ring.pop());
    try std.testing.expectEqual(@as(?u32, 2), ring.pop());
    try std.testing.expectEqual(@as(?u32, 3), ring.pop());
    try std.testing.expectEqual(@as(?u32, 4), ring.pop());
    try std.testing.expectEqual(@as(?u32, null), ring.pop());
}

test "SPSC ring buffer wraparound" {
    var ring = SpscRingBuffer(u32, 4).init();

    for (0..2) |_| {
        try std.testing.expect(ring.push(10));
        try std.testing.expect(ring.push(20));
        try std.testing.expectEqual(@as(?u32, 10), ring.pop());
        try std.testing.expectEqual(@as(?u32, 20), ring.pop());
    }
}

test "SPSC ring buffer len" {
    var ring = SpscRingBuffer(u32, 8).init();
    try std.testing.expectEqual(@as(usize, 0), ring.len());

    _ = ring.push(1);
    _ = ring.push(2);
    try std.testing.expectEqual(@as(usize, 2), ring.len());

    _ = ring.pop();
    try std.testing.expectEqual(@as(usize, 1), ring.len());
}
