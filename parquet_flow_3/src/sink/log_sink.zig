const std = @import("std");
const Allocator = std.mem.Allocator;
const RingBuffer = @import("ring_buffer.zig").RingBuffer;
const batch_mod = @import("batch.zig");
const BatchAccumulator = batch_mod.BatchAccumulator;
const SchemaInfo = batch_mod.SchemaInfo;
const linux = std.os.linux;

/// Maximum record size in bytes that can be pushed into the ring buffer.
pub const MAX_RECORD_SIZE = 256;

/// A fixed-size record slot for the ring buffer. No heap allocation needed.
pub const Record = struct {
    data: [MAX_RECORD_SIZE]u8 = undefined,
    len: u32 = 0,
};

/// Ring buffer capacity (must be power of 2).
pub const RING_CAPACITY: u32 = 1 << 16; // 65536 slots

pub const SinkConfig = struct {
    /// Maximum rows per batch before flushing.
    batch_size: u32 = 65536,
    /// Flush timeout in nanoseconds. If no full batch within this time, flush partial.
    flush_timeout_ns: u64 = 100 * std.time.ns_per_ms, // 100ms default
    /// Output file path (for future parquet writer integration).
    file_path: ?[]const u8 = null,
};

pub const LogError = error{
    BufferFull,
    RecordTooLarge,
};

/// Sleep for the given number of nanoseconds using the Linux nanosleep syscall.
fn nanosleep(ns: u64) void {
    const secs = ns / std.time.ns_per_s;
    const nsecs = ns % std.time.ns_per_s;
    var req = linux.timespec{
        .sec = @intCast(secs),
        .nsec = @intCast(nsecs),
    };
    while (true) {
        const rc = linux.nanosleep(&req, &req);
        const err = linux.errno(rc);
        if (err != .INTR) break;
        // interrupted by signal, retry with remaining time
    }
}

/// Get current monotonic time in nanoseconds.
fn monotonicNs() u64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

/// Top-level log sink. Combines a lock-free ring buffer with a background
/// writer thread that drains records into a batch accumulator.
pub const LogSink = struct {
    ring: *RingBuffer(Record, RING_CAPACITY),
    writer_thread: ?std.Thread,
    running: std.atomic.Value(bool),
    config: SinkConfig,
    schema: SchemaInfo,
    allocator: Allocator,

    // Stats
    records_written: std.atomic.Value(u64),
    batches_flushed: std.atomic.Value(u64),

    pub fn init(config: SinkConfig, schema: SchemaInfo, allocator: Allocator) !*LogSink {
        const ring = try allocator.create(RingBuffer(Record, RING_CAPACITY));
        ring.* = RingBuffer(Record, RING_CAPACITY).init();

        const self = try allocator.create(LogSink);
        self.* = LogSink{
            .ring = ring,
            .writer_thread = null,
            .running = std.atomic.Value(bool).init(true),
            .config = config,
            .schema = schema,
            .allocator = allocator,
            .records_written = std.atomic.Value(u64).init(0),
            .batches_flushed = std.atomic.Value(u64).init(0),
        };

        self.writer_thread = try std.Thread.spawn(.{}, writerThread, .{self});

        return self;
    }

    /// Called from the hot thread -- non-blocking.
    /// Copies record data into a ring buffer slot and returns immediately.
    pub fn log(self: *LogSink, record: []const u8) LogError!void {
        if (record.len > MAX_RECORD_SIZE) return LogError.RecordTooLarge;

        var slot = Record{};
        @memcpy(slot.data[0..record.len], record);
        slot.len = @intCast(record.len);

        if (!self.ring.tryPush(slot)) {
            return LogError.BufferFull;
        }
    }

    /// Background writer thread function.
    fn writerThread(self: *LogSink) void {
        var batch_acc = BatchAccumulator.init(
            self.allocator,
            self.schema,
            self.config.batch_size,
        ) catch return;
        defer batch_acc.deinit();

        var drain_buf: [256]Record = undefined;
        var last_flush_time = monotonicNs();

        while (self.running.load(.acquire)) {
            const count = self.ring.drainBatch(&drain_buf, 256);

            if (count > 0) {
                for (0..count) |i| {
                    const rec = &drain_buf[i];
                    batch_acc.addRecord(rec.data[0..rec.len]) catch continue;
                }
                _ = self.records_written.fetchAdd(count, .monotonic);

                if (batch_acc.isFull()) {
                    self.flushBatch(&batch_acc);
                    last_flush_time = monotonicNs();
                }
            } else {
                // Check flush timeout for partial batches
                const now = monotonicNs();
                const elapsed = now -% last_flush_time;
                if (batch_acc.row_count > 0 and elapsed >= self.config.flush_timeout_ns) {
                    self.flushBatch(&batch_acc);
                    last_flush_time = now;
                }

                // Nothing to drain -- yield briefly (50us)
                nanosleep(50 * std.time.ns_per_us);
            }
        }

        // Final drain on shutdown
        while (true) {
            const count = self.ring.drainBatch(&drain_buf, 256);
            if (count == 0) break;
            for (0..count) |i| {
                const rec = &drain_buf[i];
                batch_acc.addRecord(rec.data[0..rec.len]) catch continue;
            }
            _ = self.records_written.fetchAdd(count, .monotonic);
        }

        // Final flush
        if (batch_acc.row_count > 0) {
            self.flushBatch(&batch_acc);
        }
    }

    fn flushBatch(self: *LogSink, batch_acc: *BatchAccumulator) void {
        // TODO: integrate with Parquet writer to write a row group
        // For now, just track stats and reset.
        _ = self.batches_flushed.fetchAdd(1, .monotonic);
        batch_acc.reset();
    }

    /// Force flush pending data (signals writer thread, waits for completion).
    /// In this implementation, the background thread auto-flushes on timeout.
    pub fn flush(self: *LogSink) void {
        _ = self;
    }

    /// Graceful shutdown: stop the writer thread and flush remaining data.
    pub fn deinit(self: *LogSink) void {
        self.running.store(false, .release);
        if (self.writer_thread) |t| {
            t.join();
        }
        const allocator = self.allocator;
        allocator.destroy(self.ring);
        allocator.destroy(self);
    }
};

test "LogSink basic log and shutdown" {
    const allocator = std.testing.allocator;

    const columns = [_]batch_mod.ColumnDef{
        .{ .name = "val", .physical_type = .INT32, .type_length = 0, .nullable = false, .offset = 0, .size = 4 },
    };
    const schema = SchemaInfo{
        .columns = &columns,
        .record_size = 4,
        .nullable_count = 0,
        .null_bitmap_bytes = 0,
    };

    const sink = try LogSink.init(.{}, schema, allocator);

    // Log a few records
    const rec = [_]u8{ 42, 0, 0, 0 };
    for (0..100) |_| {
        sink.log(&rec) catch {};
    }

    // Give writer thread time to drain
    nanosleep(10 * std.time.ns_per_ms);

    // Shutdown
    sink.deinit();
}
