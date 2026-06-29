const std = @import("std");
const Init = std.process.Init;
const Writer = Io.Writer;
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Thread = std.Thread;
const Timestamp = std.Io.Timestamp;

const Store = @import("Store.zig");
const jetkv = @import("root.zig");

const count = 12_500;
const thread_count = 8;

pub fn main(init: Init) !void {
    var valkey: jetkv.Valkey = try .init(init.io, init.gpa, .{
        .pool_size = 16,
    });
    defer valkey.deinit();
    const store = &valkey.store;
    try store.put("foo", "bar");
    const start_time: Timestamp = .now(init.io, .awake);

    var threads: [thread_count]Thread = undefined;
    for (0..thread_count) |index| {
        threads[index] = try .spawn(
            .{ .allocator = init.gpa },
            work,
            .{ init.gpa, store },
        );
    }

    for (threads) |thread| thread.join();

    const duration = start_time.untilNow(init.io, .awake);
    var buffer: Writer.Allocating = .init(init.gpa);
    defer buffer.deinit();
    const average_time = @divTrunc(duration.toNanoseconds(), count / thread_count);
    const average_duration: Io.Duration = .fromNanoseconds(average_time);
    try duration.format(&buffer.writer);
    const total = try buffer.toOwnedSlice();
    defer init.gpa.free(total);
    try average_duration.format(&buffer.writer);
    const average = try buffer.toOwnedSlice();
    defer init.gpa.free(average);
    const nanoseconds: f32 = @floatFromInt(duration.toNanoseconds());
    const transactions = count * thread_count;
    const tps = (transactions / nanoseconds) * std.time.ns_per_s;

    std.debug.print(
        \\threads: {[threads]d}
        \\total transactions: {[transactions]d}
        \\duration: {[duration]s}
        \\average_duration: {[ave_duration]s}
        \\transactions per second: {[tps]d}
        \\
    , .{
        .threads = thread_count,
        .transactions = transactions,
        .duration = total,
        .ave_duration = average,
        .tps = tps,
    });
}

fn work(gpa: Allocator, kv: *Store) void {
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
