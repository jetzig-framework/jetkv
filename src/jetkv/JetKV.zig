const std = @import("std");

/// A string that can be stored in the key-value store.
/// Supported types, used for comptime type resolution when a member of this enum is passed to a
/// JetKV function.
pub const ValueType = enum { string, array };

/// Memory-based backend. Use in development for a simple, in-process store backed by a hash map.
pub const MemoryBackend = @import("backend/MemoryBackend.zig");

/// File-based backend. Persistent storage backed by a very simple on-disk hash table.
pub const FileBackend = @import("backend/FileBackend.zig");

/// Valkey-based backend. For use in production deployments to provide a centralized, battle-tested
/// key-value store.
/// https://valkey.io/
pub const ValkeyBackend = @import("backend/ValkeyBackend.zig").ValkeyBackend;
pub const ValkeyBackendOptions = @import("backend/ValkeyBackend.zig").Options;

/// Configure backend and options for a key-value store.
/// Available storage backends.
pub const BackendType = enum { memory, file, valkey };

pub const Options = struct {
    backend: BackendType = .memory,
    file_backend_options: FileBackend.Options = .{},
    valkey_backend_options: ValkeyBackendOptions = .{},
};

pub fn JetKV(comptime options: Options) type {
    return struct {
        backend: Backend,

        const Self = @This();

        /// Generic storage back end, unifies memory and disk-based storage.
        pub const Backend = union(BackendType) {
            memory: MemoryBackend,
            file: FileBackend,
            valkey: ValkeyBackend(options.valkey_backend_options),
        };

        /// Initialize a new key-value store.
        pub fn init(allocator: std.mem.Allocator) !Self {
            const backend = switch (options.backend) {
                .memory => Backend{
                    .memory = MemoryBackend.init(allocator, options),
                },
                .file => Backend{
                    .file = try FileBackend.init(options.file_backend_options),
                },
                .valkey => Backend{
                    .valkey = try ValkeyBackend(options.valkey_backend_options).init(allocator),
                },
            };
            return .{ .backend = backend };
        }

        /// Free allocated memory and deinitailize the key-value store.
        pub fn deinit(self: *Self) void {
            switch (self.backend) {
                inline else => |*backend| return backend.deinit(),
            }
        }

        /// Fetch a String from the key-value store.
        pub fn get(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
            return switch (self.backend) {
                inline else => |*backend| try backend.get(allocator, key),
            };
        }

        /// Store a String in the key-value store.
        pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
            switch (self.backend) {
                inline else => |*backend| try backend.put(key, value),
            }
        }

        /// Store a String in the key-value store with an expiration time in seconds.
        pub fn putExpire(self: *Self, key: []const u8, value: []const u8, expiration: i32) !void {
            switch (comptime options.backend) {
                .valkey => try self.backend.valkey.putExpire(
                    key,
                    value,
                    expiration,
                ),
                .memory => try self.backend.memory.putExpire(
                    key,
                    value,
                    expiration,
                ),
                // TODO: Currently `putExpire` is identical to `put` for file backend.
                .file => @compileError("putExpire is not supported by the FileBackend"),
            }
        }

        /// Remove a String from the key-value store, return it if found.
        pub fn fetchRemove(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
            return switch (self.backend) {
                inline else => |*backend| try backend.fetchRemove(allocator, key),
            };
        }

        /// Remove a String from the key-value store.
        pub fn remove(self: *Self, key: []const u8) !void {
            switch (self.backend) {
                inline else => |*backend| try backend.remove(key),
            }
        }

        /// Append a String at the end of an Array.
        pub fn append(self: *Self, key: []const u8, value: []const u8) !void {
            switch (self.backend) {
                inline else => |*backend| try backend.append(key, value),
            }
        }

        /// Insert a String at the start of an Array.
        pub fn prepend(self: *Self, key: []const u8, value: []const u8) !void {
            switch (self.backend) {
                inline else => |*backend| try backend.prepend(key, value),
            }
        }

        /// Pop a String from an Array the key-value store.
        pub fn pop(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
            return switch (self.backend) {
                inline else => |*backend| try backend.pop(allocator, key),
            };
        }

        /// Left-pop a String from an Array the key-value store.
        pub fn popFirst(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
            return switch (self.backend) {
                inline else => |*backend| return try backend.popFirst(allocator, key),
            };
        }
    };
}

test "put and get a string value" {
    var jet_kv = try JetKV(.{}).init(std.testing.allocator);
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
    var jet_kv = try JetKV(.{}).init(std.testing.allocator);
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
    var jet_kv = try JetKV(.{}).init(std.testing.allocator);
    defer jet_kv.deinit();

    const key = "foo";
    const value = "bar";

    try jet_kv.put(key, value);

    try jet_kv.remove(key);

    try std.testing.expect(try jet_kv.get(std.testing.allocator, key) == null);
}

test "append and popFirst a string array" {
    var jet_kv = try JetKV(.{}).init(std.testing.allocator);
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
    var jet_kv = try JetKV(.{}).init(std.testing.allocator);
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
    var jet_kv = try JetKV(.{}).init(std.testing.allocator);
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
    var jet_kv = try JetKV(.{
        .backend = .file,
        .file_backend_options = .{
            .path = "/tmp/jetkv.db",
            .address_space_size = FileBackend.addressSpace(1024),
        },
    }).init(std.testing.allocator);
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

test "valkey backend" {
    var jet_kv = try JetKV(.{
        .backend = .valkey,
        .valkey_backend_options = .{
            .pool_size = 8,
            .buffer_size = 8192,
        },
    }).init(std.testing.allocator);
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

test "putExpire" {
    var memory_jet_kv = try JetKV(.{ .backend = .memory }).init(std.testing.allocator);
    defer memory_jet_kv.deinit();

    var file_jet_kv = try JetKV(.{
        .backend = .file,
        .file_backend_options = .{ .path = "/tmp/jetkv.db", .truncate = true },
    }).init(std.testing.allocator);
    defer file_jet_kv.deinit();

    var valkey_jet_kv = try JetKV(.{
        .backend = .valkey,
        .valkey_backend_options = .{
            .pool_size = 8,
            .buffer_size = 8192,
        },
    }).init(std.testing.allocator);
    defer valkey_jet_kv.deinit();
    try valkey_jet_kv.backend.valkey.flush();

    const key = "foo";
    const value = "bar";

    try memory_jet_kv.putExpire(key, value, 1);
    // TODO
    // try file_jet_kv.putExpire(key, value, 1);
    try valkey_jet_kv.putExpire(key, value, 1);

    if (try memory_jet_kv.get(std.testing.allocator, key)) |capture| {
        defer std.testing.allocator.free(capture);
        try std.testing.expectEqualStrings("bar", capture);
    } else {
        try std.testing.expect(false);
    }

    // TODO
    // if (try file_jet_kv.get(std.testing.allocator, key)) |capture| {
    //     defer std.testing.allocator.free(capture);
    //     try std.testing.expectEqualStrings("bar", capture);
    // } else {
    //     try std.testing.expect(false);
    // }

    if (try valkey_jet_kv.get(std.testing.allocator, key)) |capture| {
        defer std.testing.allocator.free(capture);
        try std.testing.expectEqualStrings("bar", capture);
    } else {
        try std.testing.expect(false);
    }

    std.Thread.sleep(1.1 * std.time.ns_per_s);
    try std.testing.expect(try valkey_jet_kv.get(std.testing.allocator, "foo") == null);
    try std.testing.expect(try memory_jet_kv.get(std.testing.allocator, "foo") == null);
    // TODO
    // try std.testing.expect(try file_jet_kv.get(std.testing.allocator, "foo") == null);
}
