const std = @import("std");

/// Generic storage, provides an interface for disk and memory storage.
pub const Storage = @import("Storage.zig");
/// A string that can be stored in the key-value store.
pub const String = @import("types.zig").String;
/// An array that can be stored in the key-value store.
pub const Array = @import("types.zig").Array;

storage: Storage,

const Self = @This();

/// Configure backend and options for a key-value store.
pub const Options = struct {
    backend: Storage.BackendType = .memory,
};

/// Initialize a new key-value store.
pub fn init(allocator: std.mem.Allocator, options: Options) Self {
    return .{
        .storage = Storage.init(allocator, options),
    };
}

/// Free allocated memory and deinitailize the key-value store.
pub fn deinit(self: *Self) void {
    self.storage.deinit();
}

/// Fetch a value from the key-value store.
pub fn get(self: *Self, comptime T: type, key: []const u8) ?T {
    return self.storage.get(T, key);
}

/// Store a value in the key-value store.
pub fn put(self: *Self, comptime T: type, key: []const u8, value: T) !void {
    try self.storage.put(T, key, value);
}

/// Pop a String from an Array the key-value store.
pub fn pop(self: *Self, key: []const u8) ?String {
    return self.storage.pop(key);
}

test "put and get a string value" {
    var jet_kv = Self.init(std.testing.allocator, .{});
    defer jet_kv.deinit();

    const key = "foo";
    const value = "bar";

    try jet_kv.put(String, key, value);

    if (jet_kv.get(String, key)) |capture| {
        try std.testing.expectEqualStrings(value, capture);
    } else {
        try std.testing.expect(false);
    }

    try std.testing.expect(jet_kv.get(String, "baz") == null);
}

test "put and get a string array" {
    var jet_kv = Self.init(std.testing.allocator, .{});
    defer jet_kv.deinit();

    const key = "foo";
    var value = Array.init(std.testing.allocator);
    defer value.deinit();

    try value.append("bar");
    try value.append("baz");
    try value.insert("qux");
    try value.append("quux");

    const popped = value.pop().?;
    defer std.testing.allocator.free(popped);

    try std.testing.expectEqualStrings("quux", popped);

    try jet_kv.put(Array, key, value);

    if (jet_kv.get(Array, key)) |*capture| {
        try std.testing.expectEqual(3, capture.size());
        try std.testing.expectEqualDeep(value.items(), capture.items());
    } else {
        try std.testing.expect(false);
    }

    try std.testing.expect(jet_kv.get(Array, "qux") == null);
}

test "pop a value from an array" {
    var jet_kv = Self.init(std.testing.allocator, .{});
    defer jet_kv.deinit();

    var kv_array = Array.init(std.testing.allocator);
    defer kv_array.deinit();

    try kv_array.append("bar");
    try kv_array.append("baz");
    try kv_array.append("qux");

    try jet_kv.put(Array, "foo", kv_array);

    if (jet_kv.pop("foo")) |string| {
        try std.testing.expectEqualStrings("qux", string);
        defer std.testing.allocator.free(string);
    } else {
        try std.testing.expect(false);
    }

    if (jet_kv.get(Array, "foo")) |value| {
        try std.testing.expectEqualDeep(&[_][]const u8{ "bar", "baz" }, value.items());
    }
}
