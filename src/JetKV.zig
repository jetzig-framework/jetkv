const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const Backend = @import("Backend.zig");

pub const ValueType = enum { string, array };

pub fn Store(comptime config: anytype) type {
    const T = @TypeOf(config);
    return struct {
        const Self = @This();
        impl: T,

        pub fn init(io: Io, allocator: Allocator) !Self {
            return .{ .impl = try T.init(config, io, allocator) };
        }

        fn backend(self: *Self) *Backend {
            return &self.impl.interface;
        }

        pub fn deinit(self: *Self, io: Io, allocator: Allocator) void {
            self.backend().deinit(io, allocator);
        }

        pub fn get(self: *Self, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
            return self.backend().get(io, allocator, key);
        }

        pub fn put(self: *Self, io: Io, allocator: Allocator, key: []const u8, value: []const u8) !void {
            try self.backend().put(io, allocator, key, value);
        }

        pub fn putExpire(self: *Self, io: Io, allocator: Allocator, key: []const u8, value: []const u8, expiration: i32) !void {
            try self.backend().putExpire(io, allocator, key, value, expiration);
        }

        pub fn fetchRemove(self: *Self, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
            return self.backend().fetchRemove(io, allocator, key);
        }

        pub fn remove(self: *Self, io: Io, allocator: Allocator, key: []const u8) !void {
            try self.backend().remove(io, allocator, key);
        }

        pub fn append(self: *Self, io: Io, allocator: Allocator, key: []const u8, value: []const u8) !void {
            try self.backend().append(io, allocator, key, value);
        }

        pub fn prepend(self: *Self, io: Io, allocator: Allocator, key: []const u8, value: []const u8) !void {
            try self.backend().prepend(io, allocator, key, value);
        }

        pub fn pop(self: *Self, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
            return self.backend().pop(io, allocator, key);
        }

        pub fn popFirst(self: *Self, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
            return self.backend().popFirst(io, allocator, key);
        }
    };
}

const t = std.testing;

test "put and get a string value" {
    var kv = try Store(Backend.Memory{}).init(t.io, t.allocator);
    defer kv.deinit(t.io, t.allocator);

    try kv.put(t.io, t.allocator, "foo", "bar");

    if (try kv.get(t.io, t.allocator, "foo")) |capture| {
        defer t.allocator.free(capture);
        try t.expectEqualStrings("bar", capture);
    } else try t.expect(false);

    try t.expect(try kv.get(t.io, t.allocator, "baz") == null);
}

test "fetchRemove" {
    var kv = try Store(Backend.Memory{}).init(t.io, t.allocator);
    defer kv.deinit(t.io, t.allocator);

    try kv.put(t.io, t.allocator, "foo", "bar");

    if (try kv.fetchRemove(t.io, t.allocator, "foo")) |capture| {
        defer t.allocator.free(capture);
        try t.expectEqualStrings("bar", capture);
    } else try t.expect(false);

    try t.expect(try kv.get(t.io, t.allocator, "foo") == null);
}

test "remove" {
    var kv = try Store(Backend.Memory{}).init(t.io, t.allocator);
    defer kv.deinit(t.io, t.allocator);

    try kv.put(t.io, t.allocator, "foo", "bar");
    try kv.remove(t.io, t.allocator, "foo");
    try t.expect(try kv.get(t.io, t.allocator, "foo") == null);
}

test "append and pop a string array" {
    var kv = try Store(Backend.Memory{}).init(t.io, t.allocator);
    defer kv.deinit(t.io, t.allocator);

    const array = &[_][]const u8{ "bar", "baz", "qux", "quux" };
    for (array) |value| try kv.append(t.io, t.allocator, "foo", value);

    const popped = (try kv.pop(t.io, t.allocator, "foo")).?;
    defer t.allocator.free(popped);
    try t.expectEqualStrings("quux", popped);

    for (&[_][]const u8{ "bar", "baz", "qux" }) |value| {
        if (try kv.popFirst(t.io, t.allocator, "foo")) |capture| {
            defer t.allocator.free(capture);
            try t.expectEqualStrings(value, capture);
        } else try t.expect(false);
    }

    try t.expect(try kv.popFirst(t.io, t.allocator, "foo") == null);
}

test "prepend a value in an array" {
    var kv = try Store(Backend.Memory{}).init(t.io, t.allocator);
    defer kv.deinit(t.io, t.allocator);

    for (&[_][]const u8{ "bar", "baz", "qux" }) |item| try kv.append(t.io, t.allocator, "foo", item);
    try kv.prepend(t.io, t.allocator, "foo", "quux");

    for (&[_][]const u8{ "quux", "bar", "baz", "qux" }) |value| {
        if (try kv.popFirst(t.io, t.allocator, "foo")) |capture| {
            defer t.allocator.free(capture);
            try t.expectEqualStrings(value, capture);
        } else try t.expect(false);
    }
}

test "pop a value from an array" {
    var kv = try Store(Backend.Memory{}).init(t.io, t.allocator);
    defer kv.deinit(t.io, t.allocator);

    for (&[_][]const u8{ "bar", "baz", "qux" }) |item| try kv.append(t.io, t.allocator, "foo", item);

    for (&[_][]const u8{ "qux", "baz", "bar" }) |value| {
        if (try kv.pop(t.io, t.allocator, "foo")) |capture| {
            defer t.allocator.free(capture);
            try t.expectEqualStrings(value, capture);
        } else try t.expect(false);
    }
}

test "file-based storage" {
    var kv = try Store(Backend.File{
        .path = "/tmp/jetkv.db",
        .truncate = true,
    }).init(t.io, t.allocator);
    defer kv.deinit(t.io, t.allocator);

    try kv.put(t.io, t.allocator, "foo", "bar");

    if (try kv.get(t.io, t.allocator, "foo")) |capture| {
        defer t.allocator.free(capture);
        try t.expectEqualStrings("bar", capture);
    } else try t.expect(false);

    try t.expect(try kv.get(t.io, t.allocator, "baz") == null);
}

test "valkey backend" {
    try requireServer();
    var kv = try Store(Backend.Valkey{}).init(t.io, t.allocator);
    defer kv.deinit(t.io, t.allocator);

    try kv.put(t.io, t.allocator, "foo", "bar");

    if (try kv.get(t.io, t.allocator, "foo")) |capture| {
        defer t.allocator.free(capture);
        try t.expectEqualStrings("bar", capture);
    } else try t.expect(false);

    try t.expect(try kv.get(t.io, t.allocator, "baz") == null);
}

test "putExpire Valkey" {
    try requireServer();
    var valkey_kv = try Store(Backend.Valkey{}).init(t.io, t.allocator);
    defer valkey_kv.deinit(t.io, t.allocator);

    try valkey_kv.putExpire(t.io, t.allocator, "foo", "bar", 1);
    if (try valkey_kv.get(t.io, t.allocator, "foo")) |capture| {
        defer t.allocator.free(capture);
        try t.expectEqualStrings("bar", capture);
    } else try t.expect(false);

    const timeout: Io.Timeout = .{ .duration = .{ .raw = .fromNanoseconds(1_100_000_000), .clock = .real } };
    try timeout.sleep(t.io);

    try t.expect(try valkey_kv.get(t.io, t.allocator, "foo") == null);
}

test "putExpire Memory" {
    var memory_kv = try Store(Backend.Memory{}).init(t.io, t.allocator);
    defer memory_kv.deinit(t.io, t.allocator);

    try memory_kv.putExpire(t.io, t.allocator, "foo", "bar", 1);

    if (try memory_kv.get(t.io, t.allocator, "foo")) |capture| {
        defer t.allocator.free(capture);
        try t.expectEqualStrings("bar", capture);
    } else try t.expect(false);

    const timeout: Io.Timeout = .{ .duration = .{ .raw = .fromNanoseconds(1_100_000_000), .clock = .real } };
    try timeout.sleep(t.io);

    try t.expect(try memory_kv.get(t.io, t.allocator, "foo") == null);
}

fn requireServer() !void {
    const address = Io.net.IpAddress.parse("127.0.0.1", 6379) catch unreachable;
    const stream = address.connect(t.io, .{ .mode = .stream }) catch return error.SkipZigTest;
    stream.close(t.io);
}
