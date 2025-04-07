const std = @import("std");

const jetkv = @import("../../jetkv.zig");
const Options = @import("../JetKV.zig").Options;

allocator: std.mem.Allocator,
options: Options,
string_storage: std.StringHashMap(ExpirableString),
array_storage: std.StringHashMap(*Array),
mutex: std.Thread.Mutex,

const Self = @This();

const Array = std.DoublyLinkedList;
const ArrayNode = struct {
    node: std.DoublyLinkedList.Node = .{},
    data: []const u8,
};

const ExpirableString = struct {
    string: []const u8,
    expiry: ?i64,
};

/// Initialize a new memory-based storage backend.
pub fn init(allocator: std.mem.Allocator, options: Options) Self {
    return .{
        .allocator = allocator,
        .options = options,
        .string_storage = std.StringHashMap(ExpirableString).init(allocator),
        .array_storage = std.StringHashMap(*Array).init(allocator),
        .mutex = std.Thread.Mutex{},
    };
}

pub fn deinit(self: *Self) void {
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
}

/// Fetch a String from the memory-based backend.
pub fn get(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    self.mutex.lock();

    return if (self.string_storage.get(key)) |entry| blk: {
        if (entry.expiry) |expiry| {
            if (expiry < std.time.milliTimestamp()) {
                self.mutex.unlock(); // `remove` regains the lock
                try self.remove(key);
                break :blk null;
            }
        }

        errdefer self.mutex.unlock();
        const value = try allocator.dupe(u8, entry.string);
        self.mutex.unlock();
        break :blk value;
    } else blk: {
        self.mutex.unlock();
        break :blk null;
    };
}

/// Add a String to the memory-based backend.
pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
    try self.putMaybeExpire(key, value, null);
}

/// Store a String in the key-value store with an expiration time in seconds.
pub fn putExpire(self: *Self, key: []const u8, value: []const u8, expiration: i32) !void {
    try self.putMaybeExpire(key, value, std.time.milliTimestamp() + (expiration * std.time.ms_per_s));
}

fn putMaybeExpire(self: *Self, key: []const u8, value: []const u8, expiry: ?i64) !void {
    const duped_key = try self.allocator.dupe(u8, key);
    const duped_value = try self.allocator.dupe(u8, value);

    if (self.string_storage.fetchRemove(key)) |entry| {
        self.allocator.free(entry.key);
        self.allocator.free(entry.value.string);
    }

    self.mutex.lock();
    defer self.mutex.unlock();

    try self.string_storage.put(duped_key, .{ .string = duped_value, .expiry = expiry });
}

/// Remove a String from the memory-based backend, return it if found.
pub fn fetchRemove(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    self.mutex.lock();
    defer self.mutex.unlock();

    return if (self.string_storage.fetchRemove(key)) |entry| blk: {
        allocator.free(entry.key);
        break :blk entry.value.string;
    } else null;
}

/// Remove a String from the memory-based backend.
pub fn remove(self: *Self, key: []const u8) !void {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.string_storage.fetchRemove(key)) |entry| {
        self.allocator.free(entry.key);
        self.allocator.free(entry.value.string);
    }
}

/// Append a String to the end of an Array in the memory-based backend.
pub fn append(self: *Self, key: []const u8, value: []const u8) !void {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.array_storage.get(key)) |array| {
        const node = try self.allocator.create(ArrayNode);
        node.* = .{ .data = try self.allocator.dupe(u8, value) };
        array.append(&node.node);
    } else {
        var array = try self.allocator.create(Array);
        array.* = .{};
        const node = try self.allocator.create(ArrayNode);
        node.* = .{ .data = try self.allocator.dupe(u8, value) };
        array.append(&node.node);
        try self.array_storage.put(try self.allocator.dupe(u8, key), array);
    }
}

/// Insert a String to the start of an Array in the memory-based backend.
pub fn prepend(self: *Self, key: []const u8, value: []const u8) !void {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.array_storage.get(key)) |array| {
        const node = try self.allocator.create(ArrayNode);
        node.* = .{ .data = try self.allocator.dupe(u8, value) };
        array.prepend(&node.node);
    } else {
        const array = try self.allocator.create(Array);
        array.* = .{};
        const node = try self.allocator.create(ArrayNode);
        node.* = .{ .data = try self.allocator.dupe(u8, value) };
        array.append(&node.node);
        try self.array_storage.put(try self.allocator.dupe(u8, key), array);
    }
}

/// Pop a String from an Array in the memory-based backend.
pub fn pop(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.array_storage.get(key)) |array| {
        if (array.pop()) |last_node| {
            const last_item: *ArrayNode = @fieldParentPtr("node", last_node);
            const value = try allocator.dupe(u8, last_item.data);
            self.allocator.free(last_item.data);
            self.allocator.destroy(last_item);
            return value;
        } else {
            return null;
        }
    } else {
        return null;
    }
}

/// Left-pop a String from an Array in the memory-based backend.
pub fn popFirst(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.array_storage.get(key)) |array| {
        if (array.popFirst()) |last_node| {
            const last_item: *ArrayNode = @fieldParentPtr("node", last_node);
            const value = try allocator.dupe(u8, last_item.data);
            self.allocator.free(last_item.data);
            self.allocator.destroy(last_item);
            return value;
        } else {
            return null;
        }
    } else {
        return null;
    }
}
