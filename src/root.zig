const std = @import("std");

pub const Backend = @import("Backend.zig");
const valkey = @import("backend/Valkey.zig");

pub const KV = @import("JetKV.zig").KV;
pub const ValueType = @import("JetKV.zig").ValueType;

/// Generate a valid address space size for a given number of addresses.
pub const addressSpaceSize = Backend.File.addressSpace;

test {
    std.testing.refAllDecls(@This());
    // Just to be safe
    _ = @import("backend/File.zig");
    _ = @import("backend/Valkey.zig");
    _ = @import("backend/Memory.zig");
}
