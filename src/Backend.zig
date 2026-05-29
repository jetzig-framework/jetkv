const Backend = @This();

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

pub const Memory = @import("backend/Memory.zig");
pub const File = @import("backend/File.zig");
pub const Valkey = @import("backend/Valkey.zig");

/// Reference init function for interfaces
pub fn InitFn(comptime T: type) type {
    return fn (comptime T, Io, Allocator) anyerror!T;
}

vtable: *const VTable,

pub const VTable = struct {
    deinit: *const fn (*Backend, Io, Allocator) void,
    get: *const fn (*Backend, Io, Allocator, []const u8) anyerror!?[]const u8 = unimplementedGet,
    put: *const fn (*Backend, Io, Allocator, []const u8, []const u8) anyerror!void = unimplementedPut,
    putExpire: *const fn (*Backend, Io, Allocator, []const u8, []const u8, i32) anyerror!void = unimplementedPutExpire,
    fetchRemove: *const fn (*Backend, Io, Allocator, []const u8) anyerror!?[]const u8 = unimplementedFetchRemove,
    remove: *const fn (*Backend, Io, Allocator, []const u8) anyerror!void = unimplementedRemove,
    append: *const fn (*Backend, Io, Allocator, []const u8, []const u8) anyerror!void = unimplementedAppend,
    prepend: *const fn (*Backend, Io, Allocator, []const u8, []const u8) anyerror!void = unimplementedPrepend,
    pop: *const fn (*Backend, Io, Allocator, []const u8) anyerror!?[]const u8 = unimplementedPop,
    popFirst: *const fn (*Backend, Io, Allocator, []const u8) anyerror!?[]const u8 = unimplementedPopFirst,
};

pub fn deinit(self: *Backend, io: Io, allocator: Allocator) void {
    return self.vtable.deinit(self, io, allocator);
}

pub fn get(self: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    return self.vtable.get(self, io, allocator, key);
}

pub fn put(self: *Backend, io: Io, allocator: Allocator, key: []const u8, value: []const u8) !void {
    return self.vtable.put(self, io, allocator, key, value);
}

pub fn putExpire(self: *Backend, io: Io, allocator: Allocator, key: []const u8, value: []const u8, expiration: i32) !void {
    return self.vtable.putExpire(self, io, allocator, key, value, expiration);
}

pub fn fetchRemove(self: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    return self.vtable.fetchRemove(self, io, allocator, key);
}

pub fn remove(self: *Backend, io: Io, allocator: Allocator, key: []const u8) !void {
    return self.vtable.remove(self, io, allocator, key);
}

pub fn append(self: *Backend, io: Io, allocator: Allocator, key: []const u8, value: []const u8) !void {
    return self.vtable.append(self, io, allocator, key, value);
}

pub fn pop(self: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    return self.vtable.pop(self, io, allocator, key);
}

pub fn prepend(self: *Backend, io: Io, allocator: Allocator, key: []const u8, value: []const u8) !void {
    return self.vtable.prepend(self, io, allocator, key, value);
}

pub fn popFirst(self: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    return self.vtable.popFirst(self, io, allocator, key);
}

fn unimplementedGet(_: *Backend, _: Io, _: Allocator, _: []const u8) !?[]const u8 {
    return error.Unimplemented;
}

fn unimplementedPut(_: *Backend, _: Io, _: Allocator, _: []const u8, _: []const u8) !void {
    return error.Unimplemented;
}

fn unimplementedPutExpire(_: *Backend, _: Io, _: Allocator, _: []const u8, _: []const u8, _: i32) !void {
    return error.Unimplemented;
}

fn unimplementedFetchRemove(_: *Backend, _: Io, _: Allocator, _: []const u8) !?[]const u8 {
    return error.Unimplemented;
}

fn unimplementedRemove(_: *Backend, _: Io, _: Allocator, _: []const u8) !void {
    return error.Unimplemented;
}

fn unimplementedAppend(_: *Backend, _: Io, _: Allocator, _: []const u8, _: []const u8) !void {
    return error.Unimplemented;
}

fn unimplementedPrepend(_: *Backend, _: Io, _: Allocator, _: []const u8, _: []const u8) !void {
    return error.Unimplemented;
}

fn unimplementedPop(_: *Backend, _: Io, _: Allocator, _: []const u8) !?[]const u8 {
    return error.Unimplemented;
}

fn unimplementedPopFirst(_: *Backend, _: Io, _: Allocator, _: []const u8) !?[]const u8 {
    return error.Unimplemented;
}
