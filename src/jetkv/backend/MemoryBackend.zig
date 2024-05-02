const std = @import("std");

const jetkv = @import("../../jetkv.zig");

allocator: std.mem.Allocator,
options: jetkv.JetKV.Options,
string_storage: std.StringHashMap([]const u8),
array_storage: std.StringHashMap(*Array),
mutex: std.Thread.Mutex,

const Self = @This();

const Array = std.DoublyLinkedList([]const u8);

/// Initialize a new memory-based storage backend.
pub fn init(allocator: std.mem.Allocator, options: jetkv.JetKV.Options) Self {
    return .{
        .allocator = allocator,
        .options = options,
        .string_storage = std.StringHashMap([]const u8).init(allocator),
        .array_storage = std.StringHashMap(*Array).init(allocator),
        .mutex = std.Thread.Mutex{},
    };
}

pub fn deinit(self: *Self) void {
    var string_it = self.string_storage.iterator();
    while (string_it.next()) |item| {
        self.allocator.free(item.key_ptr.*);
        self.allocator.free(item.value_ptr.*);
    }
    self.string_storage.deinit();

    var array_it = self.array_storage.iterator();
    while (array_it.next()) |item| {
        self.allocator.free(item.key_ptr.*);
        while (item.value_ptr.*.pop()) |node| {
            self.allocator.free(node.data);
            self.allocator.destroy(node);
        }
        self.allocator.destroy(item.value_ptr.*);
    }
    self.array_storage.deinit();
}

/// Fetch a String from the memory-based backend.
pub fn get(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    self.mutex.lock();
    defer self.mutex.unlock();

    return if (self.string_storage.get(key)) |value|
        try allocator.dupe(u8, value)
    else
        null;
}

/// Add a String to the memory-based backend.
pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.string_storage.fetchRemove(key)) |entry| {
        self.allocator.free(entry.key);
        self.allocator.free(entry.value);
    }
    try self.string_storage.put(
        try self.allocator.dupe(u8, key),
        try self.allocator.dupe(u8, value),
    );
}

/// Remove a String from the memory-based backend, return it if found.
pub fn fetchRemove(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    self.mutex.lock();
    defer self.mutex.unlock();

    return if (self.string_storage.fetchRemove(key)) |entry| blk: {
        allocator.free(entry.key);
        break :blk entry.value;
    } else null;
}

/// Remove a String from the memory-based backend.
pub fn remove(self: *Self, key: []const u8) !void {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.string_storage.fetchRemove(key)) |entry| {
        self.allocator.free(entry.key);
        self.allocator.free(entry.value);
    }
}

/// Append a String to the end of an Array in the memory-based backend.
pub fn append(self: *Self, key: []const u8, value: []const u8) !void {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.array_storage.get(key)) |array| {
        const node = try self.allocator.create(Array.Node);
        node.* = .{ .data = try self.allocator.dupe(u8, value) };
        array.append(node);
    } else {
        var array = try self.allocator.create(Array);
        array.* = .{};
        const node = try self.allocator.create(Array.Node);
        node.* = .{ .data = try self.allocator.dupe(u8, value) };
        array.append(node);
        try self.array_storage.put(try self.allocator.dupe(u8, key), array);
    }
}

/// Insert a String to the start of an Array in the memory-based backend.
pub fn prepend(self: *Self, key: []const u8, value: []const u8) !void {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.array_storage.get(key)) |array| {
        const node = try self.allocator.create(Array.Node);
        node.* = .{ .data = try self.allocator.dupe(u8, value) };
        array.prepend(node);
    } else {
        const array = try self.allocator.create(Array);
        array.* = .{};
        const node = try self.allocator.create(Array.Node);
        node.* = .{ .data = try self.allocator.dupe(u8, value) };
        array.append(node);
        try self.array_storage.put(try self.allocator.dupe(u8, key), array);
    }
}

/// Pop a String from an Array in the memory-based backend.
pub fn pop(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.array_storage.get(key)) |array| {
        if (array.pop()) |last_item| {
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
        if (array.popFirst()) |last_item| {
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
