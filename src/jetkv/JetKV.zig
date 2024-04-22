const std = @import("std");

/// A string that can be stored in the key-value store.
/// Supported types, used for comptime type resolution when a member of this enum is passed to a
/// JetKV function.
pub const ValueType = enum { string, array };

/// Memory-based backend.
pub const MemoryBackend = @import("backend/MemoryBackend.zig");

/// File-based backend.
pub const FileBackend = @import("backend/FileBackend.zig");

backend: Backend,

const JetKV = @This();

/// Configure backend and options for a key-value store.
pub const Options = struct {
    backend: BackendType = .memory,
    file_backend_options: FileBackendOptions = .{},
};

/// Available storage backends.
pub const BackendType = enum { memory, file };

/// Options specific to the File-based backend.
pub const FileBackendOptions = struct {
    path: ?[]const u8 = null,
    address_space_size: u32 = FileBackend.addressSpace(4096),
    truncate: bool = false,
};

/// Generic storage back end, unifies memory and disk-based storage.
pub const Backend = union(enum) {
    memory: MemoryBackend,
    file: FileBackend,
};

/// Initialize a new key-value store.
pub fn init(allocator: std.mem.Allocator, options: Options) !JetKV {
    const backend = switch (options.backend) {
        .memory => Backend{ .memory = MemoryBackend.init(allocator, options) },
        .file => Backend{ .file = try FileBackend.init(options.file_backend_options) },
    };
    return .{ .backend = backend };
}

/// Free allocated memory and deinitailize the key-value store.
pub fn deinit(self: *JetKV) void {
    switch (self.backend) {
        inline else => |*capture| return capture.deinit(),
    }
}

/// Fetch a String from the key-value store.
pub fn get(self: *JetKV, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    return switch (self.backend) {
        inline else => |*capture| try capture.get(allocator, key),
    };
}

/// Store a String in the key-value store.
pub fn put(self: *JetKV, key: []const u8, value: []const u8) !void {
    switch (self.backend) {
        inline else => |*capture| try capture.put(key, value),
    }
}

/// Remove a String from the key-value store, return it if found.
pub fn fetchRemove(self: *JetKV, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    return switch (self.backend) {
        inline else => |*capture| try capture.fetchRemove(allocator, key),
    };
}

/// Remove a String from the key-value store.
pub fn remove(self: *JetKV, key: []const u8) !void {
    switch (self.backend) {
        inline else => |*capture| try capture.remove(key),
    }
}

/// Append a String at the end of an Array.
pub fn append(self: *JetKV, key: []const u8, value: []const u8) !void {
    switch (self.backend) {
        inline else => |*capture| try capture.append(key, value),
    }
}

/// Insert a String at the start of an Array.
pub fn prepend(self: *JetKV, key: []const u8, value: []const u8) !void {
    switch (self.backend) {
        inline else => |*capture| try capture.prepend(key, value),
    }
}

/// Pop a String from an Array the key-value store.
pub fn pop(self: *JetKV, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    return switch (self.backend) {
        inline else => |*capture| try capture.pop(allocator, key),
    };
}

/// Left-pop a String from an Array the key-value store.
pub fn popFirst(self: *JetKV, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    return switch (self.backend) {
        inline else => |*capture| return try capture.popFirst(allocator, key),
    };
}

test "put and get a string value" {
    var jet_kv = try JetKV.init(std.testing.allocator, .{});
    defer jet_kv.deinit();

    const key = "foo";
    const value = "bar";

    try jet_kv.put(key, value);

    if (try jet_kv.get(std.testing.allocator, key)) |capture| {
        defer std.testing.allocator.free(capture);
        try std.testing.expectEqualStrings(value, capture);
    } else {
        try std.testing.expect(false);
    }

    try std.testing.expect(try jet_kv.get(std.testing.allocator, "baz") == null);
}

test "fetchRemove" {
    var jet_kv = try JetKV.init(std.testing.allocator, .{});
    defer jet_kv.deinit();

    const key = "foo";
    const value = "bar";

    try jet_kv.put(key, value);

    if (try jet_kv.fetchRemove(std.testing.allocator, key)) |capture| {
        defer std.testing.allocator.free(capture);
        try std.testing.expectEqualStrings(value, capture);
    } else {
        try std.testing.expect(false);
    }

    try std.testing.expect(try jet_kv.get(std.testing.allocator, key) == null);
}

test "remove" {
    var jet_kv = try JetKV.init(std.testing.allocator, .{});
    defer jet_kv.deinit();

    const key = "foo";
    const value = "bar";

    try jet_kv.put(key, value);

    try jet_kv.remove(key);

    try std.testing.expect(try jet_kv.get(std.testing.allocator, key) == null);
}

test "append and popFirst a string array" {
    var jet_kv = try JetKV.init(std.testing.allocator, .{});
    defer jet_kv.deinit();

    const key = "foo";
    const array = &[_][]const u8{ "bar", "baz", "qux", "quux" };
    for (array) |value| try jet_kv.append(key, value);

    const popped = (try jet_kv.pop(std.testing.allocator, key)).?;
    defer std.testing.allocator.free(popped);

    try std.testing.expectEqualStrings("quux", popped);

    for (&[_][]const u8{ "bar", "baz", "qux" }) |value| {
        if (try jet_kv.popFirst(std.testing.allocator, key)) |capture| {
            defer std.testing.allocator.free(capture);
            try std.testing.expectEqualStrings(value, capture);
        } else {
            try std.testing.expect(false);
        }
    }

    try std.testing.expect(try jet_kv.popFirst(std.testing.allocator, "foo") == null);
}

test "prepend a value in an array" {
    var jet_kv = try JetKV.init(std.testing.allocator, .{});
    defer jet_kv.deinit();

    const array = &[_][]const u8{ "bar", "baz", "qux" };

    for (array) |item| try jet_kv.append("foo", item);
    try jet_kv.prepend("foo", "quux");

    if (try jet_kv.popFirst(std.testing.allocator, "foo")) |value| {
        defer std.testing.allocator.free(value);
        try std.testing.expectEqualStrings("quux", value);
    } else try std.testing.expect(false);

    if (try jet_kv.popFirst(std.testing.allocator, "foo")) |value| {
        defer std.testing.allocator.free(value);
        try std.testing.expectEqualStrings("bar", value);
    } else try std.testing.expect(false);

    if (try jet_kv.popFirst(std.testing.allocator, "foo")) |value| {
        defer std.testing.allocator.free(value);
        try std.testing.expectEqualStrings("baz", value);
    } else try std.testing.expect(false);

    if (try jet_kv.popFirst(std.testing.allocator, "foo")) |value| {
        defer std.testing.allocator.free(value);
        try std.testing.expectEqualStrings("qux", value);
    } else try std.testing.expect(false);
}

test "pop a value from an array" {
    var jet_kv = try JetKV.init(std.testing.allocator, .{});
    defer jet_kv.deinit();

    const array = &[_][]const u8{ "bar", "baz", "qux" };

    for (array) |item| try jet_kv.append("foo", item);

    if (try jet_kv.pop(std.testing.allocator, "foo")) |string| {
        try std.testing.expectEqualStrings("qux", string);
        defer std.testing.allocator.free(string);
    } else {
        try std.testing.expect(false);
    }

    if (try jet_kv.pop(std.testing.allocator, "foo")) |string| {
        try std.testing.expectEqualStrings("baz", string);
        defer std.testing.allocator.free(string);
    } else {
        try std.testing.expect(false);
    }

    if (try jet_kv.pop(std.testing.allocator, "foo")) |string| {
        try std.testing.expectEqualStrings("bar", string);
        defer std.testing.allocator.free(string);
    } else {
        try std.testing.expect(false);
    }
}

test "file-based storage" {
    var jet_kv = try JetKV.init(undefined, .{ .backend = .file, .file_backend_options = .{
        .path = "/tmp/jetkv.db",
        .address_space_size = FileBackend.addressSpace(1024),
    } });
    defer jet_kv.deinit();

    const key = "foo";
    const value = "bar";

    try jet_kv.put(key, value);

    if (try jet_kv.get(std.testing.allocator, key)) |capture| {
        defer std.testing.allocator.free(capture);
        try std.testing.expectEqualStrings(value, capture);
    } else {
        try std.testing.expect(false);
    }

    try std.testing.expect(try jet_kv.get(std.testing.allocator, "baz") == null);
}
