const std = @import("std");
const Io = std.Io;
const parquet_flow = @import("parquet_flow");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var threaded: Io.Threaded = .init(allocator, .{ .environ = init.minimal.environ });
    defer threaded.deinit();
    const io = threaded.io();

    var sink = try parquet_flow.LogSink.init(allocator, io, .{
        .output_path = "local_data/log_flow.parquet",
        .queue_capacity = 64 * 1024,
        .max_payload_bytes = 512,
        .row_group_rows = 2048,
    });
    defer sink.deinit();

    try sink.start();

    var message_buffer: [256]u8 = undefined;
    var accepted: usize = 0;

    for (0..50_000) |idx| {
        const payload = try std.fmt.bufPrint(&message_buffer, "event-{d}: hot-path binary payload", .{idx});
        if (sink.tryRecord(payload)) {
            accepted += 1;
        }
    }

    try sink.shutdown();

    var stdout_buffer: [512]u8 = undefined;
    var stdout_file_writer: Io.File.Writer = .init(.stdout(), io, &stdout_buffer);
    const out = &stdout_file_writer.interface;

    try out.print(
        "wrote={d} dropped={d} output={s}\n",
        .{ accepted, sink.droppedCount(), "local_data/log_flow.parquet" },
    );
    try out.flush();
}
