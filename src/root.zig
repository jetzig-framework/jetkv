const std = @import("std");

/// Interface for KV Store
pub const Store = @import("Store.zig");
pub const Memory = @import("Memory.zig");
pub const Valkey = @import("Valkey.zig");
pub const File = @import("File.zig");

/// Generate a valid address space size for a given number of addresses.
pub const addressSpaceSize = File.addressSpace;

pub const memory = Memory.init;
pub const file = File.init;
pub const valkey = Valkey.init;

test {
    std.testing.refAllDecls(@This());
    // Just to be safe
    _ = @import("File.zig");
    _ = @import("Valkey.zig");
    _ = @import("Memory.zig");
    _ = @import("SQLite.zig");
}
