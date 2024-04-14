const std = @import("std");

const jetkv = @import("../../jetkv.zig");

allocator: std.mem.Allocator,
options: jetkv.JetKV.Options,
string_storage: std.StringHashMap(jetkv.types.String),
array_storage: std.StringHashMap(*jetkv.types.Array),
mutex: std.Thread.Mutex,

const Self = @This();

/// Initialize a new memory-based storage backend.
pub fn init(allocator: std.mem.Allocator, options: jetkv.JetKV.Options) Self {
    return .{
        .allocator = allocator,
        .options = options,
        .string_storage = std.StringHashMap(jetkv.types.String).init(allocator),
        .array_storage = std.StringHashMap(*jetkv.types.Array).init(allocator),
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
        item.value_ptr.*.deinit();
        self.allocator.destroy(item.value_ptr.*);
    }
    self.array_storage.deinit();
}

/// Fetch a value from the memory-based backend.
pub fn get(self: *Self, comptime T: type, key: []const u8) ?T {
    self.mutex.lock();
    defer self.mutex.unlock();

    switch (T) {
        jetkv.types.String => return self.string_storage.get(key),
        jetkv.types.Array => return if (self.array_storage.get(key)) |value| value.* else null,
        else => unreachable,
    }
}

/// Add a value to the memory-based backend.
pub fn put(self: *Self, comptime T: type, key: []const u8, value: T) !void {
    self.mutex.lock();
    defer self.mutex.unlock();

    switch (T) {
        jetkv.types.String => {
            if (self.string_storage.fetchRemove(key)) |entry| {
                self.allocator.free(entry.key);
                self.allocator.free(entry.value);
            }
            try self.string_storage.put(
                try self.allocator.dupe(u8, key),
                try self.allocator.dupe(u8, value),
            );
        },
        jetkv.types.Array => {
            var array = try self.allocator.create(jetkv.types.Array);
            array.* = jetkv.types.Array.init(self.allocator);

            for (value.items()) |item| try array.append(item);

            if (self.array_storage.fetchRemove(key)) |*entry| {
                self.allocator.free(entry.key);
                entry.value.deinit();
            }

            return self.array_storage.put(try self.allocator.dupe(u8, key), array);
        },
        else => unreachable,
    }
}

/// Insert a String to the start of an Array in the memory-based backend.
pub fn prepend(self: *Self, key: []const u8, value: []const u8) !void {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.array_storage.get(key)) |array| {
        try array.prepend(value);
    } else {
        var array = try self.allocator.create(jetkv.types.Array);
        array.* = jetkv.types.Array.init(self.allocator);
        try array.append(value);
        try self.array_storage.put(key, array);
    }
}

/// Pop a String from an Array in the memory-based backend.
pub fn pop(self: *Self, key: []const u8) ?[]const u8 {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.array_storage.get(key)) |array| {
        const value = array.pop();
        return value;
    } else {
        return null;
    }
}
