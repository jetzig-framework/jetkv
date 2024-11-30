const std = @import("std");

pub const jetkv = @This();

/// Key-value store.
pub const JetKV = @import("jetkv/JetKV.zig").JetKV;
/// General options for configuring all backends.
pub const Options = @import("jetkv/JetKV.zig").Options;

/// Supported types, used for comptime type resolution when a member of this enum is passed to a
/// JetKV function.
pub const ValueType = @import("jetkv/JetKV.zig").ValueType;

pub const FileBackend = @import("jetkv/backend/FileBackend.zig");
pub const MemoryBackend = @import("jetkv/backend/MemoryBackend.zig");
pub const ValkeyBackend = @import("jetkv/backend/ValkeyBackend.zig").ValkeyBackend;
pub const ValkeyBackendOptions = @import("jetkv/backend/ValkeyBackend.zig").Options;

/// Generate a valid address space size for a given number of addresses.
pub const addressSpaceSize = FileBackend.addressSpace;
