const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Main executable
    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    exe_mod.linkSystemLibrary("zstd", .{});
    const exe = b.addExecutable(.{
        .name = "parquet_flow",
        .root_module = exe_mod,
    });
    b.installArtifact(exe);

    // Shared library (C API)
    const lib_mod = b.createModule(.{
        .root_source_file = b.path("src/c_api.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    lib_mod.linkSystemLibrary("zstd", .{});
    const lib = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "parquet_flow",
        .root_module = lib_mod,
    });
    b.installArtifact(lib);

    // Run step
    const run = b.addRunArtifact(exe);
    run.step.dependOn(b.getInstallStep());
    if (b.args) |args| run.addArgs(args);
    const run_step = b.step("run", "Run the parquet flow demo");
    run_step.dependOn(&run.step);

    // Test step
    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    test_mod.linkSystemLibrary("zstd", .{});
    const tests = b.addTest(.{ .root_module = test_mod });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);
}
