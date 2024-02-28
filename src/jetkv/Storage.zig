const std = @import("std");

const jetkv = @import("../jetkv.zig");
const types = @import("types.zig");

backend: Backend,

/// Memory-based back end for `Storage`.
pub const MemoryBackend = @import("backend/MemoryBackend.zig");

/// Available storage backends.
pub const BackendType = enum { memory };

/// Generic storage back end, unifies memory and disk-based storage.
pub const Backend = union(enum) {
    memory: MemoryBackend,
};

const Self = @This();

/// Initialize a new storage backend.
pub fn init(allocator: std.mem.Allocator, options: jetkv.JetKV.Options) Self {
    const backend = switch (options.backend) {
        .memory => Backend{ .memory = MemoryBackend.init(allocator, options) },
    };
    return .{ .backend = backend };
}

/// Deinitialize the storage backend.
pub fn deinit(self: *Self) void {
    switch (self.backend) {
        inline else => |*capture| return capture.deinit(),
    }
}

/// Fetch a value from the active storage backend.
pub fn get(self: *Self, comptime T: type, key: []const u8) ?T {
    switch (self.backend) {
        inline else => |*capture| return capture.get(T, key),
    }
}

/// Add a value to the active storage backend.
pub fn put(self: *Self, comptime T: type, key: []const u8, value: T) !void {
    switch (self.backend) {
        inline else => |*capture| try capture.put(T, key, value),
    }
}

/// Pop a String from an Array in the active storage backend.
pub fn pop(self: *Self, key: []const u8) ?types.String {
    switch (self.backend) {
        inline else => |*capture| return capture.pop(key),
    }
}
