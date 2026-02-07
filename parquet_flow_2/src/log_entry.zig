const std = @import("std");

pub const LogEntry = struct {
    timestamp_us: i64,
    severity: i32,
    message: [256]u8,
    message_len: u16,
    source: [128]u8,
    source_len: u16,

    pub fn init(severity: i32, msg: []const u8, src: []const u8) LogEntry {
        var entry: LogEntry = .{
            .timestamp_us = getMicrosecondTimestamp(),
            .severity = severity,
            .message = undefined,
            .message_len = 0,
            .source = undefined,
            .source_len = 0,
        };
        entry.message_len = @intCast(@min(msg.len, entry.message.len));
        @memcpy(entry.message[0..entry.message_len], msg[0..entry.message_len]);
        entry.source_len = @intCast(@min(src.len, entry.source.len));
        @memcpy(entry.source[0..entry.source_len], src[0..entry.source_len]);
        return entry;
    }

    pub fn getMessage(self: *const LogEntry) []const u8 {
        return self.message[0..self.message_len];
    }

    pub fn getSource(self: *const LogEntry) []const u8 {
        return self.source[0..self.source_len];
    }

    fn getMicrosecondTimestamp() i64 {
        var ts: std.os.linux.timespec = undefined;
        _ = std.os.linux.clock_gettime(.REALTIME, &ts);
        return @as(i64, ts.sec) * std.time.us_per_s + @divTrunc(ts.nsec, std.time.ns_per_us);
    }
};

test "LogEntry init and read back" {
    const entry = LogEntry.init(2, "hello world", "test.zig");
    try std.testing.expectEqual(@as(i32, 2), entry.severity);
    try std.testing.expectEqualStrings("hello world", entry.getMessage());
    try std.testing.expectEqualStrings("test.zig", entry.getSource());
    try std.testing.expect(entry.timestamp_us > 0);
}

test "LogEntry truncates long messages" {
    const long_msg = "a" ** 300;
    const entry = LogEntry.init(0, long_msg, "x");
    try std.testing.expectEqual(@as(u16, 256), entry.message_len);
}
