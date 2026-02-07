// Test runner for parquet writer tests.
// These tests are defined inline in src/parquet/writer.zig.
// This file provides a build-system-compatible entry point.
//
// To run tests directly:
//   zig test src/parquet/writer.zig
//
// To run via build system, the build.zig test step should reference
// src/parquet/writer.zig as the root source file.
comptime {
    _ = @import("writer");
}
