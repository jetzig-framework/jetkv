const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    //
    // const lib = b.addLibrary(.{
    //     .name = "jetkv",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/root.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //     }),
    // });
    //
    // b.installArtifact(lib);

    const mod = b.addModule("jetkv", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });

    const exe = b.addExecutable(.{
        .name = "benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    b.installArtifact(exe);
    const run = b.addRunArtifact(exe);
    const run_step = b.step("run", "Run");
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
