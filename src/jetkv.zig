const std = @import("std");

pub const jetkv = @This();

/// Key-value store.
pub const JetKV = @import("jetkv/JetKV.zig");

/// Supported types, used for comptime type resolution when a member of this enum is passed to a
/// JetKV function.
pub const ValueType = JetKV.ValueType;

/// Generate a valid address space size for a given number of addresses.
pub const addressSpaceSize = JetKV.FileBackend.addressSpace;
