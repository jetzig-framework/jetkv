const std = @import("std");

const jetkv = @import("../../jetkv.zig");

allocator: std.mem.Allocator,
options: jetkv.JetKV.Options,
string_storage: std.StringHashMap(jetkv.types.String),
array_storage: std.StringHashMap(jetkv.types.Array),

const Self = @This();

/// Initialize a new memory-based storage backend.
pub fn init(allocator: std.mem.Allocator, options: jetkv.JetKV.Options) Self {
    return .{
        .allocator = allocator,
        .options = options,
        .string_storage = std.StringHashMap(jetkv.types.String).init(allocator),
        .array_storage = std.StringHashMap(jetkv.types.Array).init(allocator),
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
        item.value_ptr.deinit();
    }
    self.array_storage.deinit();
}

/// Fetch a value from the memory-based backend.
pub fn get(self: *Self, comptime T: type, key: []const u8) ?T {
    switch (T) {
        jetkv.types.String => return self.string_storage.get(key),
        jetkv.types.Array => return self.array_storage.get(key),
        else => unreachable,
    }
}

/// Add a value to the memory-based backend.
pub fn put(self: *Self, comptime T: type, key: []const u8, value: T) !void {
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
            var array = jetkv.types.Array.init(self.allocator);

            for (value.items()) |item| try array.append(item);

            if (self.array_storage.fetchRemove(key)) |*entry| {
                self.allocator.free(entry.key);
                @constCast(&entry.value).deinit();
            }

            return self.array_storage.put(try self.allocator.dupe(u8, key), array);
        },
        else => unreachable,
    }
}
