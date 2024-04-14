const std = @import("std");

/// Key-value store.
pub const JetKV = @import("jetkv/JetKV.zig");

/// Types that can be stored in the key-value store.
pub const types = @import("jetkv/types.zig");

/// Supported types, used for comptime type resolution when a member of this enum is passed to a
/// JetKV function.
pub const value_types = JetKV.value_types;

/// Resolve a type from `value_types` enum of `{ string, array }`
pub const ValueType = JetKV.ValueType;
