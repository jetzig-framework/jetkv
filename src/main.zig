const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Thread = std.Thread;
const Timestamp = std.Io.Timestamp;

const jetkv = @import("root.zig");

const count = 12_500;
const thread_count = 8;

const KV = jetkv.KV(jetkv.Backend.Valkey{
    .pool_size = 16,
});

pub fn main(init: std.process.Init) !void {
    var kv = try KV.init(init.io, init.gpa);
    defer kv.deinit(init.io, init.gpa);
    try kv.put(init.io, init.gpa, "foo", "bar");
    const timestamp_start = Timestamp.now(init.io, .real).toNanoseconds();

    const start: usize = @intCast(timestamp_start);

    var threads: [thread_count]Thread = undefined;
    for (0..thread_count) |index| {
        threads[index] = try Thread.spawn(
            .{ .allocator = init.gpa },
            work,
            .{ init.io, init.gpa, &kv },
        );
    }

    for (threads) |thread| thread.join();

    const timestamp_end = Timestamp.now(init.io, .real).toNanoseconds();
    const end: usize = @intCast(timestamp_end);
    std.debug.print(
        \\threads: {}
        \\total transactions: {}
        \\duration: {}
        \\average duration: {}
        \\transactions per second: {d}
        \\
    ,
        .{
            thread_count,
            count * thread_count,
            (end - start),
            ((end - start) / count / thread_count),
            ((count * thread_count) / @as(f32, @floatFromInt(end - start))) * std.time.ns_per_s,
        },
    );
}

fn work(io: Io, gpa: Allocator, kv: *KV) void {
    var i: usize = 0;
    while (i < count) : (i += 1) {
        var stack_fallback = std.heap.stackFallback(4096, gpa);
        const allocator = stack_fallback.get();
        const value = kv.get(io, allocator, "foo") catch |err| {
            std.debug.print("{s}\n", .{@errorName(err)});
            continue;
        };
        std.debug.assert(std.mem.eql(u8, value.?, "bar"));
    }
}
