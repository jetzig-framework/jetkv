const Memory = @This();

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Mutex = Io.Mutex;
const StringHashMap = std.StringHashMap;
const DoublyLinkedList = std.DoublyLinkedList;

const jetkv = @import("../root.zig");
const Backend = jetkv.Backend;

/// Managed internally
internal: Internal = undefined,
/// Backend interface
interface: Backend = .{
    .vtable = &.{
        .deinit = deinit,
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

const Internal = struct {
    array_storage: StringHashMap(*Array) = undefined,
    string_storage: StringHashMap(ExpirableString) = undefined,
    mutex: Mutex = .init,
};

const Array = DoublyLinkedList;
const ArrayNode = struct {
    node: DoublyLinkedList.Node = .{},
    data: []const u8,
};

const ExpirableString = struct {
    string: []const u8,
    expiry: ?i64,
};

pub fn init(_: Memory, _: Io, allocator: Allocator) !Memory {
    return .{
        .internal = .{
            .array_storage = .init(allocator),
            .string_storage = .init(allocator),
        },
    };
}

fn deinit(b: *Backend, _: Io, allocator: Allocator) void {
    const m: *Memory = @fieldParentPtr("interface", b);
    var string_it = m.internal.string_storage.iterator();
    while (string_it.next()) |item| {
        allocator.free(item.key_ptr.*);
        allocator.free(item.value_ptr.*.string);
    }
    m.internal.string_storage.deinit();

    var array_it = m.internal.array_storage.iterator();
    while (array_it.next()) |item| {
        allocator.free(item.key_ptr.*);
        while (item.value_ptr.*.pop()) |node| {
            const array_node: *ArrayNode = @fieldParentPtr("node", node);
            allocator.free(array_node.data);
            allocator.destroy(array_node);
        }
        allocator.destroy(item.value_ptr.*);
    }
    m.internal.array_storage.deinit();
}

/// Fetch a String from the memory-based backend.
fn get(b: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    const m: *Memory = @fieldParentPtr("interface", b);
    m.internal.mutex.lockUncancelable(io);
    const timestamp = Io.Timestamp.now(io, .real).toMilliseconds();

    return if (m.internal.string_storage.get(key)) |entry| blk: {
        if (entry.expiry) |expiry| {
            if (expiry < timestamp) {
                m.internal.mutex.unlock(io); // `remove` regains the lock
                try m.interface.remove(io, m.internal.string_storage.allocator, key);
                break :blk null;
            }
        }

        errdefer m.internal.mutex.unlock(io);
        const value = try allocator.dupe(u8, entry.string);
        m.internal.mutex.unlock(io);
        break :blk value;
    } else blk: {
        m.internal.mutex.unlock(io);
        break :blk null;
    };
}

/// Add a String to the memory-based backend.
fn put(b: *Backend, io: Io, allocator: Allocator, key: []const u8, value: []const u8) !void {
    const m: *Memory = @fieldParentPtr("interface", b);
    try m.putMaybeExpire(io, allocator, key, value, null);
}

/// Store a String in the key-value store with an expiration time in seconds.
fn putExpire(b: *Backend, io: Io, allocator: Allocator, key: []const u8, value: []const u8, expiration: i32) !void {
    const m: *Memory = @fieldParentPtr("interface", b);
    const timestamp = Io.Timestamp.now(io, .real).toMilliseconds();
    try m.putMaybeExpire(io, allocator, key, value, timestamp + (expiration * std.time.ms_per_s));
}

fn putMaybeExpire(m: *Memory, io: Io, allocator: Allocator, key: []const u8, value: []const u8, expiry: ?i64) !void {
    const duped_key = try allocator.dupe(u8, key);
    const duped_value = try allocator.dupe(u8, value);

    if (m.internal.string_storage.fetchRemove(key)) |entry| {
        allocator.free(entry.key);
        allocator.free(entry.value.string);
    }

    m.internal.mutex.lockUncancelable(io);
    defer m.internal.mutex.unlock(io);

    try m.internal.string_storage.put(duped_key, .{ .string = duped_value, .expiry = expiry });
}

/// Remove a String from the memory-based backend, return it if found.
fn fetchRemove(b: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    const m: *Memory = @fieldParentPtr("interface", b);
    m.internal.mutex.lockUncancelable(io);
    defer m.internal.mutex.unlock(io);

    return if (m.internal.string_storage.fetchRemove(key)) |entry| blk: {
        allocator.free(entry.key);
        break :blk entry.value.string;
    } else null;
}

/// Remove a String from the memory-based backend.
fn remove(b: *Backend, io: Io, allocator: Allocator, key: []const u8) !void {
    const m: *Memory = @fieldParentPtr("interface", b);
    m.internal.mutex.lockUncancelable(io);
    defer m.internal.mutex.unlock(io);

    const entry = m.internal.string_storage.fetchRemove(key) orelse return;
    allocator.free(entry.key);
    allocator.free(entry.value.string);
}

/// Append a String to the end of an Array in the memory-based backend.
fn append(b: *Backend, io: Io, allocator: Allocator, key: []const u8, value: []const u8) !void {
    const m: *Memory = @fieldParentPtr("interface", b);
    m.internal.mutex.lockUncancelable(io);
    defer m.internal.mutex.unlock(io);

    if (m.internal.array_storage.get(key)) |array| {
        const node = try allocator.create(ArrayNode);
        node.* = .{ .data = try allocator.dupe(u8, value) };
        array.append(&node.node);
    } else {
        var array = try allocator.create(Array);
        array.* = .{};
        const node = try allocator.create(ArrayNode);
        node.* = .{ .data = try allocator.dupe(u8, value) };
        array.append(&node.node);
        try m.internal.array_storage.put(try allocator.dupe(u8, key), array);
    }
}

/// Insert a String to the start of an Array in the memory-based backend.
fn prepend(b: *Backend, io: Io, allocator: Allocator, key: []const u8, value: []const u8) !void {
    const m: *Memory = @fieldParentPtr("interface", b);
    m.internal.mutex.lockUncancelable(io);
    defer m.internal.mutex.unlock(io);

    if (m.internal.array_storage.get(key)) |array| {
        const node = try allocator.create(ArrayNode);
        node.* = .{ .data = try allocator.dupe(u8, value) };
        array.prepend(&node.node);
        return;
    }
    const array = try allocator.create(Array);
    array.* = .{};
    const node = try allocator.create(ArrayNode);
    node.* = .{ .data = try allocator.dupe(u8, value) };
    array.append(&node.node);
    try m.internal.array_storage.put(try allocator.dupe(u8, key), array);
}

/// Pop a String from an Array in the memory-based backend.
fn pop(b: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    const m: *Memory = @fieldParentPtr("interface", b);
    m.internal.mutex.lockUncancelable(io);
    defer m.internal.mutex.unlock(io);

    const array = m.internal.array_storage.get(key) orelse return null;
    const last_node = array.pop() orelse return null;
    const last_item: *ArrayNode = @fieldParentPtr("node", last_node);
    const value = try allocator.dupe(u8, last_item.data);
    allocator.free(last_item.data);
    allocator.destroy(last_item);
    return value;
}

/// Left-pop a String from an Array in the memory-based backend.
fn popFirst(b: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    const m: *Memory = @fieldParentPtr("interface", b);
    m.internal.mutex.lockUncancelable(io);
    defer m.internal.mutex.unlock(io);

    const array = m.internal.array_storage.get(key) orelse return null;
    const last_node = array.popFirst() orelse return null;
    const last_item: *ArrayNode = @fieldParentPtr("node", last_node);
    const value = try allocator.dupe(u8, last_item.data);
    allocator.free(last_item.data);
    allocator.destroy(last_item);
    return value;
}
