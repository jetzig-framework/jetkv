const std = @import("std");

/// A string that can be stored as a value in the key-value store.
pub const String = []const u8;

/// An array that can be stored as a value in the key-value store.
pub const Array = struct {
    allocator: std.mem.Allocator,
    backend: std.ArrayList([]const u8),

    /// Initialize a new Array and corresponding backend.
    pub fn init(allocator: std.mem.Allocator) Array {
        return .{
            .allocator = allocator,
            .backend = std.ArrayList(String).init(allocator),
        };
    }

    /// Deinitialize the array and free memory.
    pub fn deinit(self: *Array) void {
        for (self.backend.items) |*item| self.allocator.free(item.*);
        self.backend.deinit();
    }

    /// Insert a string to the beginning of the array.
    pub fn insert(self: *Array, value: String) !void {
        try self.backend.insert(0, try self.allocator.dupe(u8, value));
    }

    /// Pop the last string added to the array and return it.
    pub fn pop(self: *const Array) ?String {
        return @constCast(self).backend.popOrNull();
    }

    /// Append a string to the Array.
    pub fn append(self: *Array, value: String) !void {
        try self.backend.append(try self.allocator.dupe(u8, value));
    }

    /// Return the items slice of the array backend.
    pub fn items(self: Array) []String {
        return self.backend.items;
    }

    /// Return the number of items in Array.
    pub fn size(self: Array) usize {
        return self.backend.items.len;
    }
};
