const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const lib = b.addStaticLibrary(.{
        .name = "jetkv",
        .root_source_file = .{ .path = "src/root.zig" },
        .target = target,
        .optimize = optimize,
    });

    b.installArtifact(lib);

    const lib_unit_tests = b.addTest(.{
        .root_source_file = .{ .path = "src/tests.zig" },
        .target = target,
        .optimize = optimize,
    });

    const jetkv_module = b.addModule("jetkv", .{ .root_source_file = .{ .path = "src/jetkv.zig" } });

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    if (target.result.os.tag != .windows) {
        const args_dep = b.dependency("args", .{ .target = target, .optimize = optimize });

        const fuzzer = b.addExecutable(.{
            .name = "fuzzer",
            .root_source_file = .{ .path = "utils/fuzzer.zig" },
            .target = b.host,
        });
        fuzzer.root_module.addImport("jetkv", jetkv_module);
        fuzzer.root_module.addImport("args", args_dep.module("args"));

        b.installArtifact(fuzzer);
    }
}
