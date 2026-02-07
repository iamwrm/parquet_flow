const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Library root module
    const lib_mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    // Static library
    const static_lib = b.addLibrary(.{
        .name = "parquet_flow",
        .root_module = lib_mod,
        .linkage = .static,
    });
    b.installArtifact(static_lib);

    // Shared library (needs its own module since a file can only be in one module)
    const shared_mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    const shared_lib = b.addLibrary(.{
        .name = "parquet_flow",
        .root_module = shared_mod,
        .linkage = .dynamic,
    });
    b.installArtifact(shared_lib);

    // Install C header
    b.installFile("include/parquet_flow.h", "include/parquet_flow.h");

    // Tests -- import the library as a single module
    const test_mod = b.createModule(.{
        .root_source_file = b.path("tests/test_integration.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    test_mod.addImport("parquet_flow", b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    }));

    const tests = b.addTest(.{
        .root_module = test_mod,
    });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_tests.step);

    // Market data example
    const example_mod = b.createModule(.{
        .root_source_file = b.path("examples/market_data.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    example_mod.addImport("parquet_flow", b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    }));

    const example = b.addExecutable(.{
        .name = "market_data_example",
        .root_module = example_mod,
    });
    b.installArtifact(example);
}
