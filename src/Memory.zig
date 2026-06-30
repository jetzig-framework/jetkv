const Memory = @This();

allocator: Allocator,
array_storage: StringHashMap(*Array) = undefined,
string_storage: StringHashMap(ExpirableString) = undefined,
mutex: Mutex = .init,
io: Io,
/// Interface
store: Store = .{
    .vtable = &.{
        .get = get,
        .put = put,
        .putExpire = putExpire,
        .fetchRemove = fetchRemove,
        .remove = remove,
        .append = append,
        .prepend = prepend,
        .pop = pop,
        .popFirst = popFirst,
    },
},

pub fn init(io: Io, allocator: Allocator) Memory {
    return .{
        .io = io,
        .array_storage = .init(allocator),
        .string_storage = .init(allocator),
        .allocator = allocator,
    };
}

pub fn deinit(self: *Memory) void {
    var string_it = self.string_storage.iterator();
    while (string_it.next()) |item| {
        self.allocator.free(item.key_ptr.*);
        self.allocator.free(item.value_ptr.*.string);
    }
    self.string_storage.deinit();

    var array_it = self.array_storage.iterator();
    while (array_it.next()) |item| {
        self.allocator.free(item.key_ptr.*);
        while (item.value_ptr.*.pop()) |node| {
            const array_node: *ArrayNode = @fieldParentPtr("node", node);
            self.allocator.free(array_node.data);
            self.allocator.destroy(array_node);
        }
        self.allocator.destroy(item.value_ptr.*);
    }
    self.array_storage.deinit();
    self.* = undefined;
}

const Array = DoublyLinkedList;
const ArrayNode = struct {
    node: DoublyLinkedList.Node = .{},
    data: []const u8,
};

const ExpirableString = struct {
    string: []const u8,
    expiry: ?i64,
};

fn get(s: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    const m: *Memory = @fieldParentPtr("store", s);
    m.mutex.lockUncancelable(m.io);
    const timestamp = Io.Timestamp.now(m.io, .real).toMilliseconds();

    return if (m.string_storage.get(key)) |entry| blk: {
        if (entry.expiry) |expiry| {
            if (expiry < timestamp) {
                m.mutex.unlock(m.io); // `remove` regains the lock
                try m.store.remove(key);
                break :blk null;
            }
        }

        errdefer m.mutex.unlock(m.io);
        const value = try allocator.dupe(u8, entry.string);
        m.mutex.unlock(m.io);
        break :blk value;
    } else blk: {
        m.mutex.unlock(m.io);
        break :blk null;
    };
}

fn put(s: *Store, key: []const u8, value: []const u8) !void {
    const m: *Memory = @fieldParentPtr("store", s);
    try m.putMaybeExpire(key, value, null);
}

fn putExpire(s: *Store, key: []const u8, value: []const u8, expiration: i32) !void {
    const m: *Memory = @fieldParentPtr("store", s);
    const timestamp = Io.Timestamp.now(m.io, .real).toMilliseconds();
    try m.putMaybeExpire(key, value, timestamp + (expiration * std.time.ms_per_s));
}

fn putMaybeExpire(m: *Memory, key: []const u8, value: []const u8, expiry: ?i64) !void {
    const duped_key = try m.allocator.dupe(u8, key);
    const duped_value = try m.allocator.dupe(u8, value);

    if (m.string_storage.fetchRemove(key)) |entry| {
        m.allocator.free(entry.key);
        m.allocator.free(entry.value.string);
    }

    m.mutex.lockUncancelable(m.io);
    defer m.mutex.unlock(m.io);

    try m.string_storage.put(duped_key, .{ .string = duped_value, .expiry = expiry });
}

fn fetchRemove(s: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    const m: *Memory = @fieldParentPtr("store", s);
    m.mutex.lockUncancelable(m.io);
    defer m.mutex.unlock(m.io);

    const entry = m.string_storage.fetchRemove(key) orelse return null;
    m.allocator.free(entry.key);
    const value = try allocator.dupe(u8, entry.value.string);
    m.allocator.free(entry.value.string);
    return value;
}

fn remove(s: *Store, key: []const u8) !void {
    const m: *Memory = @fieldParentPtr("store", s);
    m.mutex.lockUncancelable(m.io);
    defer m.mutex.unlock(m.io);

    const entry = m.string_storage.fetchRemove(key) orelse return;
    m.allocator.free(entry.key);
    m.allocator.free(entry.value.string);
}

fn append(s: *Store, key: []const u8, value: []const u8) !void {
    const m: *Memory = @fieldParentPtr("store", s);
    m.mutex.lockUncancelable(m.io);
    defer m.mutex.unlock(m.io);

    if (m.array_storage.get(key)) |array| {
        const node = try m.allocator.create(ArrayNode);
        node.* = .{ .data = try m.allocator.dupe(u8, value) };
        array.append(&node.node);
    } else {
        var array = try m.allocator.create(Array);
        array.* = .{};
        const node = try m.allocator.create(ArrayNode);
        node.* = .{ .data = try m.allocator.dupe(u8, value) };
        array.append(&node.node);
        try m.array_storage.put(try m.allocator.dupe(u8, key), array);
    }
}

fn prepend(s: *Store, key: []const u8, value: []const u8) !void {
    const m: *Memory = @fieldParentPtr("store", s);
    m.mutex.lockUncancelable(m.io);
    defer m.mutex.unlock(m.io);

    if (m.array_storage.get(key)) |array| {
        const node = try m.allocator.create(ArrayNode);
        node.* = .{ .data = try m.allocator.dupe(u8, value) };
        array.prepend(&node.node);
        return;
    }
    const array = try m.allocator.create(Array);
    array.* = .{};
    const node = try m.allocator.create(ArrayNode);
    node.* = .{ .data = try m.allocator.dupe(u8, value) };
    array.append(&node.node);
    try m.array_storage.put(try m.allocator.dupe(u8, key), array);
}

fn pop(s: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    const m: *Memory = @fieldParentPtr("store", s);
    m.mutex.lockUncancelable(m.io);
    defer m.mutex.unlock(m.io);

    const array = m.array_storage.get(key) orelse return null;
    const last_node = array.pop() orelse return null;
    const last_item: *ArrayNode = @fieldParentPtr("node", last_node);
    const value = try allocator.dupe(u8, last_item.data);
    m.allocator.free(last_item.data);
    m.allocator.destroy(last_item);
    return value;
}

fn popFirst(s: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    const m: *Memory = @fieldParentPtr("store", s);
    m.mutex.lockUncancelable(m.io);
    defer m.mutex.unlock(m.io);

    const array = m.array_storage.get(key) orelse return null;
    const last_node = array.popFirst() orelse return null;
    const last_item: *ArrayNode = @fieldParentPtr("node", last_node);
    const value = try allocator.dupe(u8, last_item.data);
    m.allocator.free(last_item.data);
    m.allocator.destroy(last_item);
    return value;
}

const t = std.testing;

test "append and pop a string array" {
    var memory: Memory = .init(t.io, t.allocator);
    defer memory.deinit();

    const array = &[_][]const u8{ "bar", "baz", "qux", "quux" };
    for (array) |value| try memory.store.append("foo", value);

    const popped = (try memory.store.pop(t.allocator, "foo")).?;
    defer t.allocator.free(popped);
    try t.expectEqualStrings("quux", popped);

    for (&[_][]const u8{ "bar", "baz", "qux" }) |value| {
        const capture = try memory.store.popFirst(t.allocator, "foo") orelse
            return t.expect(false);
        defer t.allocator.free(capture);
        try t.expectEqualStrings(value, capture);
    }

    try t.expect(try memory.store.popFirst(t.allocator, "foo") == null);
}

test "prepend a value in an array" {
    var memory: Memory = .init(t.io, t.allocator);
    defer memory.deinit();

    for (&[_][]const u8{ "bar", "baz", "qux" }) |item|
        try memory.store.append("foo", item);
    try memory.store.prepend("foo", "quux");

    for (&[_][]const u8{ "quux", "bar", "baz", "qux" }) |value| {
        const capture = try memory.store.popFirst(t.allocator, "foo") orelse
            return t.expect(false);
        defer t.allocator.free(capture);
        try t.expectEqualStrings(value, capture);
    }
}

test "pop a value from an array" {
    var memory: Memory = .init(t.io, t.allocator);
    defer memory.deinit();

    for (&[_][]const u8{ "bar", "baz", "qux" }) |item|
        try memory.store.append("foo", item);

    for (&[_][]const u8{ "qux", "baz", "bar" }) |value| {
        const capture = try memory.store.pop(t.allocator, "foo") orelse
            return t.expect(false);
        defer t.allocator.free(capture);
        try t.expectEqualStrings(value, capture);
    }
}

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Mutex = Io.Mutex;
const StringHashMap = std.StringHashMap;
const DoublyLinkedList = std.DoublyLinkedList;
const Store = @import("Store.zig");
