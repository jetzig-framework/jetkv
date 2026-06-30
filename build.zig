const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zqlite = b.dependency("zqlite", .{
        .target = target,
        .optimize = optimize,
    }).module("zqlite");

    const mod = b.addModule("jetkv", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "zqlite", .module = zqlite },
        },
    });

    const exe = b.addExecutable(.{
        .name = "benchmark",
        .use_llvm = false,
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/benchmark.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    b.installArtifact(exe);
    const run = b.addRunArtifact(exe);
    const run_step = b.step("benchmark", "Run");
    run_step.dependOn(&run.step);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(
        &b.addRunArtifact(
            b.addTest(.{
                .name = "jetkv",
                .root_module = mod,
            }),
        ).step,
    );
}
