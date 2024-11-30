const std = @import("std");

const jetkv = @import("jetkv.zig");

const count = 12_500;
const thread_count = 8;

const KV = jetkv.JetKV(.{
    .backend = .valkey,
    .valkey_backend_options = .{ .pool_size = 16 },
});

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var kv = try KV.init(allocator);
    try kv.put("foo", "bar");
    const start: usize = @intCast(std.time.nanoTimestamp());

    var threads: [thread_count]std.Thread = undefined;
    for (0..thread_count) |index| {
        threads[index] = try std.Thread.spawn(
            .{ .allocator = allocator },
            work,
            .{ allocator, &kv },
        );
    }

    for (threads) |thread| thread.join();

    const end: usize = @intCast(std.time.nanoTimestamp());
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
            std.fmt.fmtDuration(end - start),
            std.fmt.fmtDuration((end - start) / count / thread_count),
            ((count * thread_count) / @as(f32, @floatFromInt(end - start))) * std.time.ns_per_s,
        },
    );
}

fn work(gpa: std.mem.Allocator, kv: *KV) void {
    var i: usize = 0;
    while (i < count) : (i += 1) {
        var stack_fallback = std.heap.stackFallback(4096, gpa);
        const allocator = stack_fallback.get();
        const value = kv.get(allocator, "foo") catch |err| {
            std.debug.print("{s}\n", .{@errorName(err)});
            continue;
        };
        std.debug.assert(std.mem.eql(u8, value.?, "bar"));
    }
}
