const std = @import("std");

/// Generic storage, provides an interface for disk and memory storage.
pub const Storage = @import("Storage.zig");
/// A string that can be stored in the key-value store.
pub const String = @import("types.zig").String;
/// An array that can be stored in the key-value store.
pub const Array = @import("types.zig").Array;

/// Supported types, used for comptime type resolution when a member of this enum is passed to a
/// JetKV function.
pub const value_types = enum { string, array };

storage: Storage,

const JetKV = @This();

/// Configure backend and options for a key-value store.
pub const Options = struct {
    backend: Storage.BackendType = .memory,
};

/// Initialize a new key-value store.
pub fn init(allocator: std.mem.Allocator, options: Options) JetKV {
    return .{
        .storage = Storage.init(allocator, options),
    };
}

/// Free allocated memory and deinitailize the key-value store.
pub fn deinit(self: *JetKV) void {
    self.storage.deinit();
}

/// Fetch a value from the key-value store.
pub fn get(self: *JetKV, comptime VT: value_types, key: []const u8) ?ValueType(VT) {
    return self.storage.get(ValueType(VT), key);
}

/// Store a value in the key-value store.
pub fn put(self: *JetKV, comptime VT: value_types, key: []const u8, value: ValueType(VT)) !void {
    try self.storage.put(ValueType(VT), key, value);
}

pub fn prepend(self: *JetKV, key: []const u8, value: []const u8) !void {
    try self.storage.prepend(key, value);
}

/// Pop a String from an Array the key-value store.
pub fn pop(self: *JetKV, key: []const u8) ?String {
    return self.storage.pop(key);
}

/// Resolve a type from `value_types` enum of `{ string, array }`
pub inline fn ValueType(VT: value_types) type {
    return switch (VT) {
        .string => String,
        .array => Array,
    };
}

test "put and get a string value" {
    var jet_kv = JetKV.init(std.testing.allocator, .{});
    defer jet_kv.deinit();

    const key = "foo";
    const value = "bar";

    try jet_kv.put(.string, key, value);

    if (jet_kv.get(.string, key)) |capture| {
        try std.testing.expectEqualStrings(value, capture);
    } else {
        try std.testing.expect(false);
    }

    try std.testing.expect(jet_kv.get(.string, "baz") == null);
}

test "put and get a string array" {
    var jet_kv = JetKV.init(std.testing.allocator, .{});
    defer jet_kv.deinit();

    const key = "foo";
    var value = Array.init(std.testing.allocator);
    defer value.deinit();

    try value.append("bar");
    try value.append("baz");
    try value.prepend("qux");
    try value.append("quux");

    const popped = value.pop().?;
    defer std.testing.allocator.free(popped);

    try std.testing.expectEqualStrings("quux", popped);

    try jet_kv.put(.array, key, value);

    if (jet_kv.get(.array, key)) |*capture| {
        try std.testing.expectEqual(3, capture.size());
        try std.testing.expectEqualDeep(value.items(), capture.items());
    } else {
        try std.testing.expect(false);
    }

    try std.testing.expect(jet_kv.get(.array, "qux") == null);
}

test "insert a value in an array" {
    var jet_kv = JetKV.init(std.testing.allocator, .{});
    defer jet_kv.deinit();

    var kv_array = Array.init(std.testing.allocator);
    defer kv_array.deinit();

    try kv_array.append("bar");
    try kv_array.append("baz");
    try kv_array.append("qux");

    try jet_kv.put(.array, "foo", kv_array);

    try jet_kv.prepend("foo", "quux");

    if (jet_kv.get(.array, "foo")) |value| {
        try std.testing.expectEqualDeep(&[_][]const u8{ "quux", "bar", "baz", "qux" }, value.items());
    }
}

test "pop a value from an array" {
    var jet_kv = JetKV.init(std.testing.allocator, .{});
    defer jet_kv.deinit();

    var kv_array = Array.init(std.testing.allocator);
    defer kv_array.deinit();

    try kv_array.append("bar");
    try kv_array.append("baz");
    try kv_array.append("qux");

    try jet_kv.put(.array, "foo", kv_array);

    if (jet_kv.pop("foo")) |string| {
        try std.testing.expectEqualStrings("qux", string);
        defer std.testing.allocator.free(string);
    } else {
        try std.testing.expect(false);
    }

    if (jet_kv.get(.array, "foo")) |value| {
        try std.testing.expectEqualDeep(&[_][]const u8{ "bar", "baz" }, value.items());
    }
}
