const std = @import("std");
const parquet_writer = @import("parquet/writer.zig");

pub const Config = struct {
    output_path: []const u8 = "local_data/log_flow.parquet",
    queue_capacity: usize = 8192,
    max_payload_bytes: usize = 1024,
    row_group_rows: usize = 2048,
    compression: parquet_writer.Compression = .uncompressed,
};

const SlotMeta = struct {
    len: u32 = 0,
};

const RecordBatch = struct {
    payload_offsets: std.ArrayList(u32) = .empty,
    payload_bytes: std.ArrayList(u8) = .empty,

    fn init(self: *RecordBatch, gpa: std.mem.Allocator, row_hint: usize, payload_hint: usize) !void {
        try self.payload_offsets.ensureTotalCapacity(gpa, row_hint + 1);
        try self.payload_bytes.ensureTotalCapacity(gpa, row_hint * payload_hint);
        try self.payload_offsets.append(gpa, 0);
    }

    fn append(self: *RecordBatch, gpa: std.mem.Allocator, payload: []const u8) !void {
        try self.payload_bytes.appendSlice(gpa, payload);
        if (self.payload_bytes.items.len > std.math.maxInt(u32)) {
            return error.PayloadTooLarge;
        }
        try self.payload_offsets.append(gpa, @intCast(self.payload_bytes.items.len));
    }

    fn rowCount(self: *const RecordBatch) usize {
        if (self.payload_offsets.items.len == 0) return 0;
        return self.payload_offsets.items.len - 1;
    }

    fn clear(self: *RecordBatch) void {
        self.payload_offsets.clearRetainingCapacity();
        self.payload_offsets.appendAssumeCapacity(0);
        self.payload_bytes.clearRetainingCapacity();
    }

    fn deinit(self: *RecordBatch, gpa: std.mem.Allocator) void {
        self.payload_offsets.deinit(gpa);
        self.payload_bytes.deinit(gpa);
    }
};

pub const LogSink = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,

    slots: []SlotMeta,
    slot_storage: []u8,

    producer: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    consumer: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    dropped: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    stop_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    notify_mutex: std.Io.Mutex = .init,
    notify_cond: std.Io.Condition = .init,

    worker: ?std.Thread = null,
    writer: parquet_writer.Writer,

    worker_error_mutex: std.Io.Mutex = .init,
    worker_error: ?anyerror = null,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, config: Config) !LogSink {
        if (config.queue_capacity == 0) return error.InvalidQueueCapacity;
        if (config.max_payload_bytes == 0) return error.InvalidPayloadSize;
        if (config.row_group_rows == 0) return error.InvalidRowGroupRows;

        const slots = try allocator.alloc(SlotMeta, config.queue_capacity);
        errdefer allocator.free(slots);
        @memset(slots, .{ .len = 0 });

        const storage_size = config.queue_capacity * config.max_payload_bytes;
        const slot_storage = try allocator.alloc(u8, storage_size);
        errdefer allocator.free(slot_storage);

        const schema = [_]parquet_writer.ColumnDef{.{ .name = "payload", .physical_type = .byte_array }};
        var writer = try parquet_writer.Writer.initWithSchema(
            allocator,
            io,
            config.output_path,
            &schema,
            config.compression,
        );
        errdefer writer.deinit();

        return .{
            .allocator = allocator,
            .io = io,
            .config = config,
            .slots = slots,
            .slot_storage = slot_storage,
            .writer = writer,
        };
    }

    pub fn start(self: *LogSink) !void {
        if (self.worker != null) return;
        self.worker = try std.Thread.spawn(.{}, workerEntry, .{self});
    }

    pub fn tryRecord(self: *LogSink, payload: []const u8) bool {
        if (self.worker == null) return false;
        if (payload.len == 0 or payload.len > self.config.max_payload_bytes) {
            _ = self.dropped.fetchAdd(1, .monotonic);
            return false;
        }

        const write_idx = self.producer.load(.monotonic);
        const read_idx = self.consumer.load(.acquire);

        if (write_idx - read_idx >= self.config.queue_capacity) {
            _ = self.dropped.fetchAdd(1, .monotonic);
            return false;
        }

        const slot_idx = write_idx % self.config.queue_capacity;
        const slot = self.slotPayload(slot_idx);
        @memcpy(slot[0..payload.len], payload);
        self.slots[slot_idx].len = @intCast(payload.len);

        self.producer.store(write_idx + 1, .release);

        self.notify_cond.signal(self.io);

        return true;
    }

    pub fn droppedCount(self: *const LogSink) u64 {
        return self.dropped.load(.monotonic);
    }

    pub fn shutdown(self: *LogSink) !void {
        self.stop_requested.store(true, .release);

        self.notify_cond.signal(self.io);

        if (self.worker) |thread| {
            thread.join();
            self.worker = null;
        }

        if (self.getWorkerError()) |err| {
            return err;
        }
    }

    pub fn deinit(self: *LogSink) void {
        if (self.worker != null) {
            self.shutdown() catch {};
        }

        self.writer.deinit();
        self.allocator.free(self.slot_storage);
        self.allocator.free(self.slots);
    }

    fn slotPayload(self: *LogSink, slot_idx: usize) []u8 {
        const begin = slot_idx * self.config.max_payload_bytes;
        const end = begin + self.config.max_payload_bytes;
        return self.slot_storage[begin..end];
    }

    fn queueIsEmpty(self: *LogSink) bool {
        return self.producer.load(.acquire) == self.consumer.load(.acquire);
    }

    fn setWorkerError(self: *LogSink, err: anyerror) void {
        self.worker_error_mutex.lockUncancelable(self.io);
        defer self.worker_error_mutex.unlock(self.io);

        if (self.worker_error == null) {
            self.worker_error = err;
        }
    }

    fn getWorkerError(self: *LogSink) ?anyerror {
        self.worker_error_mutex.lockUncancelable(self.io);
        defer self.worker_error_mutex.unlock(self.io);
        return self.worker_error;
    }

    fn workerEntry(self: *LogSink) void {
        self.workerLoop() catch |err| {
            self.setWorkerError(err);
        };
    }

    fn workerLoop(self: *LogSink) !void {
        var batch: RecordBatch = .{};
        defer batch.deinit(self.allocator);
        try batch.init(self.allocator, self.config.row_group_rows, self.config.max_payload_bytes);

        while (true) {
            const drained = try self.drainIntoBatch(&batch);

            if (batch.rowCount() >= self.config.row_group_rows) {
                try self.writer.writeRowGroup(batch.payload_bytes.items, batch.payload_offsets.items);
                batch.clear();
            }

            const stop = self.stop_requested.load(.acquire);
            if (stop and self.queueIsEmpty()) {
                break;
            }

            if (!drained) {
                self.notify_mutex.lockUncancelable(self.io);
                if (!self.stop_requested.load(.acquire) and self.queueIsEmpty()) {
                    self.notify_cond.waitUncancelable(self.io, &self.notify_mutex);
                }
                self.notify_mutex.unlock(self.io);
            }
        }

        if (batch.rowCount() > 0) {
            try self.writer.writeRowGroup(batch.payload_bytes.items, batch.payload_offsets.items);
        }

        try self.writer.close();
    }

    fn drainIntoBatch(self: *LogSink, batch: *RecordBatch) !bool {
        var drained_any = false;

        while (batch.rowCount() < self.config.row_group_rows) {
            const read_idx = self.consumer.load(.monotonic);
            const write_idx = self.producer.load(.acquire);

            if (read_idx == write_idx) {
                break;
            }

            const slot_idx = read_idx % self.config.queue_capacity;
            const len: usize = @intCast(self.slots[slot_idx].len);
            const payload = self.slotPayload(slot_idx)[0..len];

            try batch.append(self.allocator, payload);
            self.consumer.store(read_idx + 1, .release);
            drained_any = true;
        }

        return drained_any;
    }
};
