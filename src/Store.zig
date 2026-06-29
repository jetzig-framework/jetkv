const Store = @This();

vtable: *const VTable,

pub const VTable = struct {
    get: *const fn (*Store, Allocator, []const u8) anyerror!?[]const u8 = unimplementedGet,
    put: *const fn (*Store, []const u8, []const u8) anyerror!void = unimplementedPut,
    putExpire: *const fn (*Store, []const u8, []const u8, i32) anyerror!void = unimplementedPutExpire,
    fetchRemove: *const fn (*Store, Allocator, []const u8) anyerror!?[]const u8 = unimplementedFetchRemove,
    remove: *const fn (*Store, []const u8) anyerror!void = unimplementedRemove,
    append: *const fn (*Store, []const u8, []const u8) anyerror!void = unimplementedAppend,
    prepend: *const fn (*Store, []const u8, []const u8) anyerror!void = unimplementedPrepend,
    pop: *const fn (*Store, Allocator, []const u8) anyerror!?[]const u8 = unimplementedPop,
    popFirst: *const fn (*Store, Allocator, []const u8) anyerror!?[]const u8 = unimplementedPopFirst,
};

pub fn get(self: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    return self.vtable.get(self, allocator, key);
}

test get {
    var backends: Backends = try .init(t.io, t.allocator);
    defer backends.deinit();

    for (backends.stores()) |store| {
        try store.put("foo", "bar");
        const foo = try store.get(t.allocator, "foo") orelse
            return t.expect(false);
        defer t.allocator.free(foo);
        try t.expectEqualStrings("bar", foo);
        const baz = try store.get(t.allocator, "baz");
        try t.expectEqual(baz, null);
    }
}

pub fn put(self: *Store, key: []const u8, value: []const u8) !void {
    return self.vtable.put(self, key, value);
}

test put {
    var backends: Backends = try .init(t.io, t.allocator);
    defer backends.deinit();

    for (backends.stores()) |store| {
        try store.put("foo", "bar");
        const foo = try store.get(t.allocator, "foo") orelse
            return t.expect(false);
        defer t.allocator.free(foo);
        try t.expectEqualStrings("bar", foo);
        const baz = try store.get(t.allocator, "baz");
        try t.expectEqual(baz, null);
    }
}

pub fn putExpire(self: *Store, key: []const u8, value: []const u8, expiration: i32) !void {
    return self.vtable.putExpire(self, key, value, expiration);
}

test putExpire {
    try requireServer();

    var m: Memory = .init(t.io, t.allocator);
    defer m.deinit();

    var f: File = try .init(t.io, .{
        .path = "/tmp/jetkv_file.db",
    });
    defer f.deinit();

    var v: Valkey = try .init(t.io, t.allocator, .{});
    defer v.deinit();

    var s: SQLite = try .init(t.io, .{
        .path = "/tmp/jetkv_sqlite.db",
    });
    defer s.deinit();

    // File backend does not implement putExpire (no TTL support).
    for ([_]*Store{ &m.store, &v.store, &s.store }) |store| {
        try store.putExpire("foo", "bar", 1);
        const mem_foo = try store.get(t.allocator, "foo") orelse
            return t.expect(false);
        defer t.allocator.free(mem_foo);
        try t.expectEqualStrings("bar", mem_foo);

        const timeout: Timeout = .{
            .duration = .{
                .raw = .fromNanoseconds(1_100_000_000),
                .clock = .real,
            },
        };
        try timeout.sleep(t.io);
        try t.expect(try store.get(t.allocator, "foo") == null);
    }
}

pub fn fetchRemove(self: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    return self.vtable.fetchRemove(self, allocator, key);
}

test fetchRemove {
    var backends: Backends = try .init(t.io, t.allocator);
    defer backends.deinit();

    for (backends.stores()) |store| {
        try store.put("foo", "bar");
        const foo = try store.fetchRemove(t.allocator, "foo") orelse
            return t.expect(false);
        defer t.allocator.free(foo);
        try t.expectEqualStrings("bar", foo);
        try t.expect(try store.get(t.allocator, "foo") == null);
    }
}

pub fn remove(self: *Store, key: []const u8) !void {
    return self.vtable.remove(self, key);
}

test remove {
    var backends: Backends = try .init(t.io, t.allocator);
    defer backends.deinit();

    for (backends.stores()) |store| {
        try store.put("foo", "bar");
        try store.remove("foo");
        try t.expect(try store.get(t.allocator, "foo") == null);
    }
}

pub fn append(self: *Store, key: []const u8, value: []const u8) !void {
    return self.vtable.append(self, key, value);
}

test append {
    var backends: Backends = try .init(t.io, t.allocator);
    defer backends.deinit();

    for (backends.stores()) |store| {
        const items = &[_][]const u8{ "foo", "bar", "baz" };
        for (items) |v| try store.append("list", v);

        for (items) |e| {
            const popped = (try store.popFirst(t.allocator, "list")).?;
            defer t.allocator.free(popped);
            try t.expectEqualStrings(e, popped);
        }
        try t.expect(try store.popFirst(t.allocator, "list") == null);
    }
}

pub fn prepend(self: *Store, key: []const u8, value: []const u8) !void {
    return self.vtable.prepend(self, key, value);
}

test prepend {
    var backends: Backends = try .init(t.io, t.allocator);
    defer backends.deinit();

    for (backends.stores()) |store| {
        const items = &[_][]const u8{ "foo", "bar", "baz" };
        for (items) |v| try store.prepend("list", v);

        const expected = &[_][]const u8{ "baz", "bar", "foo" };
        for (expected) |e| {
            const popped = (try store.popFirst(t.allocator, "list")).?;
            defer t.allocator.free(popped);
            try t.expectEqualStrings(e, popped);
        }
    }
}

pub fn pop(self: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    return self.vtable.pop(self, allocator, key);
}

test pop {
    var backends: Backends = try .init(t.io, t.allocator);
    defer backends.deinit();

    for (backends.stores()) |store| {
        const items = &[_][]const u8{ "foo", "bar", "baz" };
        for (items) |v| try store.append("list", v);

        const expected = &[_][]const u8{ "baz", "bar", "foo" };
        for (expected) |e| {
            const popped = (try store.pop(t.allocator, "list")).?;
            defer t.allocator.free(popped);
            try t.expectEqualStrings(e, popped);
        }
        try t.expect(try store.pop(t.allocator, "list") == null);
    }
}

pub fn popFirst(self: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    return self.vtable.popFirst(self, allocator, key);
}

test popFirst {
    var backends: Backends = try .init(t.io, t.allocator);
    defer backends.deinit();

    for (backends.stores()) |store| {
        const items = &[_][]const u8{ "foo", "bar", "baz" };
        for (items) |v| try store.prepend("list", v);

        const expected = &[_][]const u8{ "baz", "bar", "foo" };
        for (expected) |e| {
            const popped = (try store.popFirst(t.allocator, "list")).?;
            defer t.allocator.free(popped);
            try t.expectEqualStrings(e, popped);
        }
    }
}

fn unimplementedGet(_: *Store, _: Allocator, _: []const u8) !?[]const u8 {
    return error.Unimplemented;
}

fn unimplementedPut(_: *Store, _: []const u8, _: []const u8) !void {
    return error.Unimplemented;
}

fn unimplementedPutExpire(_: *Store, _: []const u8, _: []const u8, _: i32) !void {
    return error.Unimplemented;
}

fn unimplementedFetchRemove(_: *Store, _: Allocator, _: []const u8) !?[]const u8 {
    return error.Unimplemented;
}

fn unimplementedRemove(_: *Store, _: []const u8) !void {
    return error.Unimplemented;
}

fn unimplementedAppend(_: *Store, _: []const u8, _: []const u8) !void {
    return error.Unimplemented;
}

fn unimplementedPrepend(_: *Store, _: []const u8, _: []const u8) !void {
    return error.Unimplemented;
}

fn unimplementedPop(_: *Store, _: Allocator, _: []const u8) !?[]const u8 {
    return error.Unimplemented;
}

fn unimplementedPopFirst(_: *Store, _: Allocator, _: []const u8) !?[]const u8 {
    return error.Unimplemented;
}

const Backends = struct {
    valkey: Valkey,
    memory: Memory,
    file: File,
    sqlite: SQLite,

    pub fn init(io: Io, allocator: Allocator) !Backends {
        try requireServer();
        return .{
            .valkey = try .init(io, allocator, .{}),
            .memory = .init(io, allocator),
            .file = try .init(io, .{ .path = "/tmp/jetkv_file.db" }),
            .sqlite = try .init(io, .{ .path = "/tmp/jetkv_sqlite.db" }),
        };
    }

    pub fn deinit(self: *Backends) void {
        self.valkey.deinit();
        self.memory.deinit();
        self.file.deinit();
        self.sqlite.deinit();
    }

    pub fn stores(self: *Backends) [4]*Store {
        return [_]*Store{
            &self.file.store,
            &self.memory.store,
            &self.valkey.store,
            &self.sqlite.store,
        };
    }
};

fn requireServer() !void {
    const address: IpAddress = try .parse("127.0.0.1", 6379);
    const stream = address.connect(t.io, .{ .mode = .stream }) catch
        return error.SkipZigTest;
    stream.close(t.io);
}

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const t = std.testing;
const IpAddress = Io.net.IpAddress;
const Timeout = Io.Timeout;
const Memory = @import("Memory.zig");
const File = @import("File.zig");
const Valkey = @import("Valkey.zig");
const SQLite = @import("SQLite.zig");
