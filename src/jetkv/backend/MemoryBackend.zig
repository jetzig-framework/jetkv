const std = @import("std");

const jetkv = @import("../../jetkv.zig");

allocator: std.mem.Allocator,
options: jetkv.JetKV.Options,
string_storage: std.StringHashMap(jetkv.types.String),

const Self = @This();

/// Initialize a new memory-based storage backend.
pub fn init(allocator: std.mem.Allocator, options: jetkv.JetKV.Options) Self {
    return .{
        .allocator = allocator,
        .options = options,
        .string_storage = std.StringHashMap(jetkv.types.String).init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    var it = self.string_storage.iterator();
    while (it.next()) |item| {
        self.allocator.free(item.key_ptr.*);
        self.allocator.free(item.value_ptr.*);
    }
    self.string_storage.deinit();
}

/// Fetch a value from the memory-based backend.
pub fn get(self: *Self, comptime T: type, key: []const u8) ?T {
    switch (T) {
        jetkv.types.String => return self.string_storage.get(key),
        else => unreachable,
    }
}

/// Add a value to the memory-based backend.
pub fn put(self: *Self, comptime T: type, key: []const u8, value: T) !void {
    switch (T) {
        jetkv.types.String => return try self.string_storage.put(
            try self.allocator.dupe(u8, key),
            try self.allocator.dupe(u8, value),
        ),
        else => unreachable,
    }
}
