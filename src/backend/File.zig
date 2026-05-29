const FileBackend = @This();

const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;
const File = Io.File;
const Mutex = Io.Mutex;
const Allocator = std.mem.Allocator;

const jetkv = @import("../root.zig");
const Backend = jetkv.Backend;
const keypairs = @import("../tests/keypairs.zig").keypairs;

path: []const u8,
address_space_size: u32 = FileBackend.addressSpace(4096),
truncate: bool = false,
/// Managed internally
internal: Internal = undefined,
/// Backend interface
interface: Backend = .{
    .vtable = &.{
        .deinit = deinit,
        .put = put,
        .get = get,
        .remove = remove,
        .fetchRemove = fetchRemove,
        .prepend = prepend,
        .pop = pop,
        .popFirst = popFirst,
        .append = append,
    },
},

const Internal = struct {
    file: File,
    address_space: u32,
    mutex: Mutex = .init,
};

pub fn init(comptime config: FileBackend, io: Io, _: Allocator) !FileBackend {
    if (config.address_space_size % bufSize(u32) != 0)
        return error.KVInvalidAddressSpaceSize;
    var backend: FileBackend = .{
        .address_space_size = config.address_space_size,
        .truncate = config.truncate,
        .path = config.path,
        .internal = .{
            .address_space = @divExact(config.address_space_size, bufSize(u32)),
            .file = try createFile(io, config.path, .{
                .read = true,
                .lock = .exclusive,
                .truncate = config.truncate,
            }),
        },
    };
    try backend.initAddressSpace(io);
    try backend.initHeader(io);
    return backend;
}

fn deinit(b: *Backend, io: Io, _: Allocator) void {
    const self: *FileBackend = @fieldParentPtr("interface", b);
    self.internal.file.close(io);
}

fn put(b: *Backend, io: Io, _: Allocator, key: []const u8, value: []const u8) !void {
    try validateKey(key);
    const self: *FileBackend = @fieldParentPtr("interface", b);
    self.lockUncancelable(io);
    defer self.unlock(io);
    try self.putString(io, key, value);
    try self.sync(io);
}

fn get(b: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    try validateKey(key);
    const self: *FileBackend = @fieldParentPtr("interface", b);
    self.lockUncancelable(io);
    defer self.unlock(io);
    return try self.getString(io, allocator, key);
}

fn remove(b: *Backend, io: Io, _: Allocator, key: []const u8) !void {
    try validateKey(key);
    const self: *FileBackend = @fieldParentPtr("interface", b);
    self.lockUncancelable(io);
    defer self.unlock(io);
    try self.removeString(io, key);
    try self.sync(io);
}

fn fetchRemove(b: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    try validateKey(key);
    const self: *FileBackend = @fieldParentPtr("interface", b);
    self.lockUncancelable(io);
    defer self.unlock(io);

    return if (try self.getString(io, allocator, key)) |capture| blk: {
        try self.removeString(io, key);
        try self.sync(io);
        break :blk capture;
    } else null;
}

pub fn lock(self: *FileBackend, io: Io) !void {
    self.internal.mutex.lock(io);
}

pub fn unlock(self: *FileBackend, io: Io) void {
    self.internal.mutex.unlock(io);
}

pub fn lockUncancelable(self: *FileBackend, io: Io) void {
    self.internal.mutex.lockUncancelable(io);
}

fn prepend(b: *Backend, io: Io, _: Allocator, key: []const u8, value: []const u8) !void {
    try validateKey(key);
    const self: *FileBackend = @fieldParentPtr("interface", b);
    self.lockUncancelable(io);
    defer self.unlock(io);
    const index = try self.locate(key);
    const address = try self.readIndexAddress(io, index) orelse {
        try self.createArray(io, index, null, key, value, .{ .linked = false });
        try self.incRefCount(io);
        try self.sync(io);
        return;
    };
    var key_buf: [max_key_len]u8 = undefined;
    const item = try self.readItem(io, address, &key_buf);

    if (!std.mem.eql(u8, item.key, key)) {
        // Collision
        try self.prependLinked(io, item, key, value, &key_buf);
        try self.sync(io);
        return;
    }
    if (address.type == .array and address.array_end_location != null) {
        // No collision
        const location = try self.prependItemToExistingArray(io, address, key, value);
        try self.updateLocation(io, index, location);
        try self.incRefCount(io);
    } else {
        // Overwrite string/re-use empty array
        try self.createArray(io, index, address, key, value, .{ .linked = false });
    }
    try self.sync(io);
}

fn pop(b: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    try validateKey(key);
    const self: *FileBackend = @fieldParentPtr("interface", b);
    self.lockUncancelable(io);
    defer self.unlock(io);

    const index = try self.locate(key);
    const value = if (try self.readIndexAddress(io, index)) |address| blk: {
        var key_buf: [max_key_len]u8 = undefined;
        const item = try self.readItem(io, address, &key_buf);
        if (std.mem.eql(u8, item.key, key)) {
            // No collision
            if (address.type != .array) return null;
            break :blk try self.popIndexed(io, allocator, item, &key_buf);
        } else {
            // Collision
            break :blk try self.popLinked(io, allocator, item, key, &key_buf);
        }
    } else null;
    try self.sync(io);
    return value;
}

fn popFirst(b: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    try validateKey(key);
    const self: *FileBackend = @fieldParentPtr("interface", b);
    self.lockUncancelable(io);
    defer self.unlock(io);

    var key_buf: [max_key_len]u8 = undefined;
    const index = try self.locate(key);
    const value = if (try self.readIndexAddress(io, index)) |address| blk: {
        const item = try self.readItem(io, address, &key_buf);
        if (std.mem.eql(u8, item.key, key)) {
            if (address.type != .array) return null;
            // No collision
            break :blk try self.popFirstIndexed(io, allocator, index, item);
        } else {
            // Collision
            break :blk try self.popFirstLinked(io, allocator, item, key, &key_buf);
        }
    } else null;

    try self.sync(io);
    return value;
}

fn append(b: *Backend, io: Io, _: Allocator, key: []const u8, value: []const u8) !void {
    const self: *FileBackend = @fieldParentPtr("interface", b);
    self.lockUncancelable(io);
    defer self.unlock(io);

    const index = try self.locate(key);
    const address = try self.readIndexAddress(io, index) orelse {
        try self.createArray(io, index, null, key, value, .{ .linked = false });
        return try self.incRefCount(io);
    };
    var key_buf: [max_key_len]u8 = undefined;
    const item = try self.readItem(io, address, &key_buf);
    if (!std.mem.eql(u8, item.key, key)) {
        // Collision
        var it = self.linkedListIterator(io, item.address, &key_buf);
        var previous_item = item;

        while (try it.next()) |linked_item| {
            const is_equal_key = std.mem.eql(u8, linked_item.key, key);
            if (!is_equal_key) {
                previous_item = linked_item;
                continue;
            }
            if (linked_item.address.type == .array) {
                if (linked_item.address.array_end_location == null) try self.createArray(
                    io,
                    previous_item.address.location,
                    previous_item.address,
                    key,
                    value,
                    .{ .linked = true },
                ) else try self.appendItemToExistingArray(
                    io,
                    linked_item.address,
                    key,
                    value,
                );
                return try self.incRefCount(io);
            }
            if (linked_item.address.type == .string) {
                // Overwrite string value
                try self.updateAddress(io, previous_item.address.location, .{ .type = .array });
                return try self.createArray(
                    io,
                    previous_item.address.location,
                    previous_item.address,
                    key,
                    value,
                    .{ .linked = true },
                );
            }
        }

        // No matches in linked list - create new array at EOF and link to final item in
        // linked list
        try self.createArray(io, previous_item.address.location, null, key, value, .{ .linked = true });
        return try self.incRefCount(io);
    }

    if (address.type == .array and address.array_end_location != null) {
        // No collision
        try self.appendItemToExistingArray(io, address, key, value);
        return try self.incRefCount(io);
    }
    // Overwrite string/re-use empty array
    return try self.createArray(io, index, address, key, value, .{ .linked = false });
}

const hasher = std.hash.Fnv1a_32.hash;

const ValueType = enum(u8) { string, array };

const AddressInfo = struct {
    type: ValueType,
    location: u32, // Not serialized
    linked_next_location: ?u32 = null, // Linked lists for collisions
    array_next_location: ?u32 = null, // Doubly-linked lists for arrays
    array_previous_location: ?u32 = null, // Doubly-linked lists for arrays
    array_end_location: ?u32 = null,
    key_len: u16,
    value_len: u32,
    max_key_len: u16,
    max_value_len: u32,
};

const Item = struct {
    key: []const u8,
    address: AddressInfo,
    file_backend: FileBackend,

    pub fn value(self: Item, io: Io, allocator: Allocator) ![]const u8 {
        return try self.file_backend.readValue(io, allocator, self.address);
    }
};

const LocationType = enum { self, previous, next, end };

const TypedLocation = struct {
    type: LocationType,
    location: ?u32,
};

const TypedAddress = struct {
    type: LocationType,
    address: AddressInfo,
};

const Header = struct {
    address_space_size: u32,
    ref_count: u32,
};

const header_len = bufSize(u32) + bufSize(u32);
const address_len = bufSize(u8) + (bufSize(u32) * 6) + (bufSize(u16) * 2);
const max_key_len = 1024;
const linked_next_location_offset = bufSize(u8);
const array_next_location_offset = bufSize(u8) + bufSize(u32);
const array_previous_location_offset = bufSize(u8) + (bufSize(u32) * 2);
const array_end_location_offset = bufSize(u8) + (bufSize(u32) * 3);
const endian = .little;
const empty = [address_len]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

/// Calculate an address space size for a given number of addresses.
pub fn addressSpace(size: u32) u32 {
    return bufSize(u32) * size;
}

/// Insert a String to the start of an Array in the file-based backend.
/// Pop a String from the end of an Array in the file-based backend.
/// Pop a String from the start of an Array in the file-based backend.
fn popIndexed(
    self: FileBackend,
    io: Io,
    allocator: Allocator,
    item: Item,
    key_buf: *[max_key_len]u8,
) !?[]const u8 {
    if (item.address.type != .array) return null;

    const end_location = item.address.array_end_location orelse return null;

    const last_item_address = try self.readAddress(io, end_location) orelse
        return null;
    const last_item = try self.readItem(io, last_item_address, key_buf);
    const value = try last_item.value(io, allocator);
    try self.shrinkArray(io, item.address, last_item.address);
    try self.decRefCount(io);
    try self.maybeTruncate(io, last_item.address);
    return value;
}

fn popLinked(
    self: FileBackend,
    io: Io,
    allocator: Allocator,
    item: Item,
    key: []const u8,
    key_buf: *[max_key_len]u8,
) !?[]const u8 {
    var it = self.linkedListIterator(io, item.address, key_buf);
    var previous_item = item;

    while (try it.next()) |linked_item| {
        if (!std.mem.eql(u8, linked_item.key, key)) {
            previous_item = linked_item;
            continue;
        }
        if (linked_item.address.type != .array) return null;

        const end_location = linked_item.address.array_end_location orelse return null;

        const last_item_address = try self.readAddress(io, end_location) orelse
            return null;
        const last_item = try self.readItem(io, last_item_address, key_buf);
        const value = try last_item.value(io, allocator);
        try self.shrinkArray(io, linked_item.address, last_item.address);

        if (linked_item.address.location == end_location)
            try self.updateAddress(io, previous_item.address.location, .{
                .linked_next_location = .{
                    .value = linked_item.address.linked_next_location,
                },
            });

        try self.decRefCount(io);
        try self.maybeTruncate(io, last_item.address);
        return value;
    }
    return null;
}

fn popFirstLinked(
    self: FileBackend,
    io: Io,
    allocator: Allocator,
    item: Item,
    key: []const u8,
    key_buf: *[max_key_len]u8,
) !?[]const u8 {
    var it = self.linkedListIterator(io, item.address, key_buf);
    var previous_item = item;

    while (try it.next()) |linked_item| {
        if (!std.mem.eql(u8, linked_item.key, key)) {
            previous_item = linked_item;
            continue;
        }
        switch (linked_item.address.type) {
            .array => {
                if (linked_item.address.array_next_location) |next_location| {
                    try self.updateAddress(
                        io,
                        previous_item.address.location,
                        .{ .linked_next_location = .{ .value = next_location } },
                    );
                    try self.updateAddress(
                        io,
                        next_location,
                        .{
                            .array_end_location = .{ .value = linked_item.address.array_end_location },
                            .linked_next_location = .{ .value = linked_item.address.linked_next_location },
                        },
                    );
                } else {
                    try self.updateAddress(
                        io,
                        linked_item.address.location,
                        .{ .array_end_location = .none, .array_next_location = .none },
                    );
                }
                const value = try linked_item.value(io, allocator);
                try self.decRefCount(io);
                return value;
            },
            .string => return null,
            // ValueType only has .string and .array
        }
    }
    return null;
}

fn popFirstIndexed(
    self: FileBackend,
    io: Io,
    allocator: Allocator,
    index: u32,
    item: Item,
) !?[]const u8 {
    if (item.address.array_next_location) |array_next_location| {
        try self.updateLocation(io, index, array_next_location);
        // Maintain possible next linked item
        try self.updateAddress(
            io,
            array_next_location,
            .{
                .array_end_location = .{ .value = item.address.array_end_location },
                .linked_next_location = .{ .value = item.address.linked_next_location },
            },
        );
    } else if (item.address.linked_next_location == null)
        try self.updateLocation(io, index, null)
    else
        try self.updateAddress(io, item.address.location, .{ .array_end_location = .none });
    const value = try item.value(io, allocator);
    try self.decRefCount(io);
    return value;
}

fn prependLinked(
    self: FileBackend,
    io: Io,
    item: Item,
    key: []const u8,
    value: []const u8,
    key_buf: *[max_key_len]u8,
) !void {
    var it = self.linkedListIterator(io, item.address, key_buf);
    var previous_item = item;

    while (try it.next()) |linked_item| {
        if (!std.mem.eql(u8, linked_item.key, key)) {
            previous_item = linked_item;
            continue;
        }
        switch (linked_item.address.type) {
            .array => {
                const location = try self.prependItemToExistingArray(
                    io,
                    linked_item.address,
                    key,
                    value,
                );
                try self.updateAddress(
                    io,
                    previous_item.address.location,
                    .{ .linked_next_location = .{ .value = location } },
                );
                try self.incRefCount(io);
                return;
            },
            .string => {
                // Overwrite string value
                try self.updateAddress(io, linked_item.address.location, .{ .type = .array });
                try self.createArray(
                    io,
                    linked_item.address.location,
                    linked_item.address,
                    key,
                    value,
                    .{ .linked = false },
                );
                break;
            },
            // ValueType only has .string and .array
        }
    }

    // No matches in linked list - create new array at EOF and link to final item in
    // linked list
    try self.createArray(
        io,
        previous_item.address.location,
        null,
        key,
        value,
        .{ .linked = true },
    );
    try self.incRefCount(io);
}

// Drop one item the end of an array and update end location.
fn shrinkArray(self: FileBackend, io: Io, first_item_address: AddressInfo, last_item_address: AddressInfo) !void {
    const previous_location = last_item_address.array_previous_location orelse {
        // We reached the first item
        return try self.updateAddress(io, first_item_address.location, .{ .array_end_location = .none });
    };
    // Nullify next item pointer for next-to-last item, update end location to
    // next-to-last item
    var location_buf: [bufSize(u32)]u8 = undefined;
    serialize(u32, 0, &location_buf);
    try self.internal.file.writePositionalAll(io, &location_buf, @as(u64, previous_location) + array_next_location_offset);

    try self.updateAddress(
        io,
        first_item_address.location,
        .{
            .array_end_location = .{
                .value = last_item_address.array_previous_location.?,
            },
        },
    );
}

// Initialize address space with zeroes.
fn initAddressSpace(self: FileBackend, io: Io) !void {
    const zero_buf: [4096]u8 = std.mem.zeroes([4096]u8);
    var offset: u64 = header_len;
    const end: u64 = header_len + self.address_space_size;
    while (offset < end) {
        const chunk: usize = @min(zero_buf.len, end - offset);
        try self.internal.file.writePositionalAll(io, zero_buf[0..chunk], offset);
        offset += chunk;
    }
}

fn initHeader(self: FileBackend, io: Io) !void {
    const header: Header = .{
        .address_space_size = self.address_space_size,
        .ref_count = 0,
    };
    var buf: [bufSize(Header)]u8 = undefined;
    serialize(Header, header, &buf);
    try self.internal.file.writePositionalAll(io, &buf, 0);
}

fn incRefCount(self: FileBackend, io: Io) !void {
    var header = try self.readHeader(io);
    header.ref_count += 1;
    try self.writeHeader(io, header);
}

fn decRefCount(self: FileBackend, io: Io) !void {
    var header = try self.readHeader(io);
    if (header.ref_count == 0) unreachable;
    header.ref_count -= 1;
    try self.writeHeader(io, header);
    if (header.ref_count == 0) {
        try self.setEndPos(io, header_len + self.address_space_size);
        try self.initAddressSpace(io);
    }
}

fn writeHeader(self: FileBackend, io: Io, header: Header) !void {
    var header_buf: [bufSize(Header)]u8 = undefined;
    serialize(Header, header, &header_buf);
    try self.internal.file.writePositionalAll(io, &header_buf, 0);
}

fn readHeader(self: FileBackend, io: Io) !Header {
    var header_buf: [bufSize(Header)]u8 = undefined;
    _ = try self.internal.file.readPositionalAll(io, &header_buf, 0);
    return .{
        .address_space_size = std.mem.readInt(u32, header_buf[0..4], endian),
        .ref_count = std.mem.readInt(u32, header_buf[4..8], endian),
    };
}

fn getEndPos(self: FileBackend, io: Io) !u32 {
    return @intCast(try self.internal.file.length(io));
}

fn setEndPos(self: FileBackend, io: Io, location: u32) !void {
    try self.internal.file.setLength(io, location);
}

fn sync(self: FileBackend, io: Io) !void {
    try self.internal.file.sync(io);
}

// Truncate the file if the given address + key/value reaches EOF.
fn maybeTruncate(self: FileBackend, io: Io, address: AddressInfo) !void {
    if (address.linked_next_location) |next_location|
        if (try self.readAddress(io, next_location)) |_|
            return; // We need to retain the link
    if (try self.isTerminatingAddress(io, address))
        try self.setEndPos(io, address.location);
}

fn isTerminatingAddress(self: FileBackend, io: Io, address: AddressInfo) !bool {
    const address_end = address.location +
        address_len +
        address.max_key_len + address.max_value_len;
    return address_end == try self.getEndPos(io);
}

fn getString(self: FileBackend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    const index = try self.locate(key);
    const location = try self.readLocation(io, index) orelse
        return null;
    const address = try self.readAddress(io, location) orelse
        return null;

    var key_buf: [max_key_len]u8 = undefined;
    const item = try self.readItem(io, address, &key_buf);
    if (std.mem.eql(u8, item.key, key))
        return try item.value(io, allocator);

    var it = self.linkedListIterator(io, item.address, &key_buf);

    while (try it.next()) |linked_item|
        if (std.mem.eql(u8, linked_item.key, key))
            return try linked_item.value(io, allocator);
    return null;
}

fn putString(self: FileBackend, io: Io, key: []const u8, value: []const u8) !void {
    const index = try self.locate(key);
    const address = try self.readIndexAddress(io, index) orelse {
        try self.writeString(io, index, null, key, value);
        return try self.incRefCount(io);
    };
    var key_buf: [max_key_len]u8 = undefined;
    const item = try self.readItem(io, address, &key_buf);
    if (!std.mem.eql(u8, item.key, key))
        return try self.writeLinkedString(io, item, key, value);
    if (key.len <= item.address.max_key_len and value.len <= item.address.max_value_len)
        return try self.updateString(io, item, key, value);
    try self.writeString(io, index, item.address, key, value);
}

fn removeString(self: FileBackend, io: Io, key: []const u8) !void {
    const index = try self.locate(key);
    const address = try self.readIndexAddress(io, index) orelse return;
    var key_buf: [max_key_len]u8 = undefined;
    const item = try self.readItem(io, address, &key_buf);
    if (!std.mem.eql(u8, item.key, key))
        return try self.removeLinkedString(io, item, key);
    try self.updateLocation(io, index, address.linked_next_location); // can be null
    try self.decRefCount(io);
}

/// Append a String to the end of an Array in the file-based backend.
pub const CreateArrayOptions = struct {
    linked: bool,
};

fn createArray(
    self: FileBackend,
    io: Io,
    index: u32,
    maybe_address: ?AddressInfo,
    key: []const u8,
    value: []const u8,
    options: CreateArrayOptions,
) !void {
    var address_buf: [address_len]u8 = undefined;

    const end_pos = try self.getEndPos(io);
    const linked_next_location = if (maybe_address) |address| address.linked_next_location else null;
    const address = makeAddress(.array, .{
        .location = end_pos,
        .linked_next_location = linked_next_location,
        .key = key,
        .value = value,
    });
    serialize(AddressInfo, address, &address_buf);

    try self.updateLocation(io, index + if (options.linked) linked_next_location_offset else 0, end_pos);

    const base: u64 = end_pos;
    try self.internal.file.writePositionalAll(io, &address_buf, base);
    try self.internal.file.writePositionalAll(io, key, base + address_len);
    try self.internal.file.writePositionalAll(io, value, base + address_len + key.len);
}

fn appendItemToExistingArray(
    self: FileBackend,
    io: Io,
    address: AddressInfo,
    key: []const u8,
    value: []const u8,
) !void {
    var address_buf: [address_len]u8 = undefined;
    const end_pos = try self.getEndPos(io);

    // Update next item pointer for previous item
    try self.updateAddress(
        io,
        address.array_end_location.?,
        .{
            .array_next_location = .{
                .value = end_pos,
            },
        },
    );

    const new_address = makeAddress(.array, .{
        .location = end_pos,
        .array_previous_location = address.array_end_location.?,
        .array_end = .none, // Array end is only stored at first item
        .key = key,
        .value = value,
    });

    serialize(AddressInfo, new_address, &address_buf);
    const base: u64 = end_pos;
    try self.internal.file.writePositionalAll(io, &address_buf, base);
    try self.internal.file.writePositionalAll(io, key, base + address_len);
    try self.internal.file.writePositionalAll(io, value, base + address_len + key.len);

    try self.updateAddress(io, address.location, .{ .array_end_location = .{ .value = end_pos } });
}

fn prependItemToExistingArray(
    self: FileBackend,
    io: Io,
    address: AddressInfo,
    key: []const u8,
    value: []const u8,
) !u32 {
    var address_buf: [address_len]u8 = undefined;
    const end_pos = try self.getEndPos(io);

    const new_address = makeAddress(.array, .{
        .location = end_pos,
        .array_next_location = address.location,
        .array_end = .{ .location = address.array_end_location orelse address.location },
        .key = key,
        .value = value,
    });

    // Update previous item pointer for previous item
    try self.updateAddress(io, address.location, .{
        .array_previous_location = .{ .value = end_pos },
        .array_end_location = .none,
    });

    serialize(AddressInfo, new_address, &address_buf);
    const base: u64 = end_pos;
    try self.internal.file.writePositionalAll(io, &address_buf, base);
    try self.internal.file.writePositionalAll(io, key, base + address_len);
    try self.internal.file.writePositionalAll(io, value, base + address_len + key.len);

    return end_pos;
}

fn locate(self: FileBackend, key: []const u8) !u32 {
    const value = try std.math.mod(
        u32,
        hash(key),
        self.internal.address_space,
    ) * bufSize(u32);
    return header_len + value;
}

// Fetch a location from the index, i.e. a pointer to an address.
fn readLocation(self: FileBackend, io: Io, index: u32) !?u32 {
    var location_buf: [bufSize(u32)]u8 = undefined;
    _ = try self.internal.file.readPositionalAll(io, &location_buf, index);
    const location = std.mem.readInt(u32, &location_buf, endian);
    return if (location == 0) null else location;
}

// Update a location pointer, either in the main index or as an address's next item pointer.
fn updateLocation(self: FileBackend, io: Io, index: u32, location: ?u32) !void {
    var location_buf: [bufSize(u32)]u8 = undefined;
    serialize(u32, location orelse 0, &location_buf);
    try self.internal.file.writePositionalAll(io, &location_buf, index);
}

const AddressUpdateOptions = struct {
    const AddressUpdateLocationValue = union(enum) {
        none: void,
        value: ?u32,
    };
    type: ?ValueType = null,
    linked_next_location: ?AddressUpdateLocationValue = null,
    array_next_location: ?AddressUpdateLocationValue = null,
    array_previous_location: ?AddressUpdateLocationValue = null,
    array_end_location: ?AddressUpdateLocationValue = null,
};

fn updateAddress(self: FileBackend, io: Io, location: u32, options: AddressUpdateOptions) !void {
    var buf: [4]u8 = undefined;

    if (options.type) |value_type| {
        serialize(ValueType, value_type, buf[0..1]);
        try self.internal.file.writePositionalAll(io, buf[0..1], location);
    }

    if (options.linked_next_location) |linked_next_location| {
        serialize(AddressUpdateOptions.AddressUpdateLocationValue, linked_next_location, buf[0..4]);
        try self.internal.file.writePositionalAll(io, buf[0..4], @as(u64, location) + linked_next_location_offset);
    }

    if (options.array_next_location) |array_next_location| {
        serialize(AddressUpdateOptions.AddressUpdateLocationValue, array_next_location, buf[0..4]);
        try self.internal.file.writePositionalAll(io, buf[0..4], @as(u64, location) + array_next_location_offset);
    }

    if (options.array_previous_location) |array_previous_location| {
        serialize(AddressUpdateOptions.AddressUpdateLocationValue, array_previous_location, buf[0..4]);
        try self.internal.file.writePositionalAll(io, buf[0..4], @as(u64, location) + array_previous_location_offset);
    }

    if (options.array_end_location) |array_end_location| {
        serialize(AddressUpdateOptions.AddressUpdateLocationValue, array_end_location, buf[0..4]);
        try self.internal.file.writePositionalAll(io, buf[0..4], @as(u64, location) + array_end_location_offset);
    }
}

fn readIndexAddress(self: FileBackend, io: Io, index: u32) !?AddressInfo {
    const location = try self.readLocation(io, index) orelse return null;
    return try self.readAddress(io, location);
}

fn readAddress(self: FileBackend, io: Io, location: u32) !?AddressInfo {
    // TODO: Save a few bytes by using different address formats for strings and arrays
    var buf: [address_len]u8 = undefined;
    const n = try self.internal.file.readPositionalAll(io, &buf, location);
    if (n < address_len) return null; // File was truncated.

    if (std.mem.eql(u8, &buf, &empty))
        return null;
    return .{
        .type = @enumFromInt(std.mem.readInt(u8, buf[0..1], endian)),
        .location = location, // For convenience
        .linked_next_location = deserialize(?u32, buf[1..5]),
        .array_next_location = deserialize(?u32, buf[5..9]),
        .array_previous_location = deserialize(?u32, buf[9..13]),
        .array_end_location = deserialize(?u32, buf[13..17]),
        .key_len = deserialize(u16, buf[17..19]),
        .value_len = deserialize(u32, buf[19..23]),
        .max_key_len = deserialize(u16, buf[23..25]),
        .max_value_len = deserialize(u32, buf[25..29]),
    };
}

// Write a string to the end of the file, update index to point to new location.
// `index` can be either a location in the main index or the location of an address's next item
// pointer.
fn writeString(
    self: FileBackend,
    io: Io,
    index: u32,
    maybe_address_info: ?AddressInfo,
    key: []const u8,
    value: []const u8,
) !void {
    const end_pos = try self.getEndPos(io);

    var location_buf: [bufSize(u32)]u8 = undefined;
    serialize(u32, end_pos, &location_buf);
    try self.internal.file.writePositionalAll(io, &location_buf, index);

    const max_value_len = bandedValueLength(value);

    var address_buf: [address_len]u8 = undefined;
    serialize(
        AddressInfo,
        .{
            .type = .string,
            .location = end_pos,
            .key_len = @intCast(key.len),
            .value_len = @intCast(value.len),
            .max_key_len = @intCast(key.len),
            .max_value_len = max_value_len,
            .linked_next_location = if (maybe_address_info) |address_info|
                address_info.linked_next_location
            else
                null,
        },
        &address_buf,
    );

    const base: u64 = end_pos;
    try self.internal.file.writePositionalAll(io, &address_buf, base);
    try self.internal.file.writePositionalAll(io, key, base + address_len);
    try self.internal.file.writePositionalAll(io, value, base + address_len + key.len);

    const pad_size: usize = max_value_len - value.len;
    if (pad_size > 0) {
        const zero_buf: [8192]u8 = std.mem.zeroes([8192]u8);
        try self.internal.file.writePositionalAll(io, zero_buf[0..pad_size], base + address_len + key.len + value.len);
    }
}

// Round value length up to a nearby number for over-allocation purposes to allow re-use of
// address space when updating values of similar length.
inline fn bandedValueLength(value: []const u8) u32 {
    var band: u32 = 256;
    while (band <= 8192) : (band <<= 1)
        if (value.len <= band) return band;
    return @intCast(value.len);
}

// Update a string in place - assume key and value lengths are within existing key and value bounds.
fn updateString(self: FileBackend, io: Io, item: Item, key: []const u8, value: []const u8) !void {
    var address_buf: [address_len]u8 = undefined;
    serialize(
        AddressInfo,
        .{
            .type = .string,
            .linked_next_location = item.address.linked_next_location,
            .location = item.address.location,
            .key_len = @intCast(key.len),
            .value_len = @intCast(value.len),
            .max_key_len = item.address.max_key_len,
            .max_value_len = item.address.max_value_len,
        },
        &address_buf,
    );

    const base: u64 = item.address.location;
    try self.internal.file.writePositionalAll(io, &address_buf, base);
    try self.internal.file.writePositionalAll(io, key, base + address_len);
    try self.internal.file.writePositionalAll(io, value, base + address_len + key.len);
}

// Follow links until end, then either:
// * Update an existing link if the key matches
// * Append to EOF if no link has the given key, updating the last link's next item pointer
fn writeLinkedString(self: FileBackend, io: Io, item: Item, key: []const u8, value: []const u8) !void {
    var key_buf: [max_key_len]u8 = undefined;
    var it = self.linkedListIterator(io, item.address, &key_buf);
    var previous_item = item;

    while (try it.next()) |linked_item| {
        if (!std.mem.eql(u8, linked_item.key, key)) {
            previous_item = linked_item;
            continue;
        }
        return try self.updateLinkedString(
            io,
            previous_item.address,
            linked_item.address,
            key,
            value,
        );
    }

    // Write string using the final item's next item pointer as index
    try self.writeString(
        io,
        previous_item.address.location + linked_next_location_offset,
        null,
        key,
        value,
    );
    try self.incRefCount(io);
}

fn removeLinkedString(self: FileBackend, io: Io, item: Item, key: []const u8) !void {
    var key_buf: [max_key_len]u8 = undefined;
    var it = self.linkedListIterator(io, item.address, &key_buf);
    var previous_item = item;

    while (try it.next()) |linked_item| {
        if (!std.mem.eql(u8, linked_item.key, key)) {
            previous_item = linked_item;
            continue;
        }
        const T = AddressUpdateOptions.AddressUpdateLocationValue;
        const linked_next_location: T = if (linked_item.address.linked_next_location) |location|
            .{ .value = location }
        else
            .none;
        try self.updateAddress(
            io,
            previous_item.address.location,
            .{ .linked_next_location = linked_next_location },
        );
        return try self.decRefCount(io);
    }
}

fn updateLinkedString(
    self: FileBackend,
    io: Io,
    previous_address: AddressInfo,
    address: AddressInfo,
    key: []const u8,
    value: []const u8,
) !void {
    const is_overwrite = isOverwrite(address, key, value);
    const end_pos = if (is_overwrite)
        address.location
    else
        try self.getEndPos(io);

    var new_address_buf: [address_len]u8 = undefined;
    serialize(
        AddressInfo,
        .{
            .type = .string,
            .location = end_pos,
            .key_len = @intCast(key.len),
            .value_len = @intCast(value.len),
            .linked_next_location = address.linked_next_location,
            .max_key_len = if (is_overwrite) address.max_key_len else @intCast(key.len),
            .max_value_len = if (is_overwrite) address.max_value_len else @intCast(value.len),
        },
        &new_address_buf,
    );

    if (!is_overwrite) try self.updateAddress(
        io,
        previous_address.location,
        .{ .linked_next_location = .{ .value = end_pos } },
    );

    const base: u64 = end_pos;
    try self.internal.file.writePositionalAll(io, &new_address_buf, base);
    try self.internal.file.writePositionalAll(io, key, base + address_len);
    try self.internal.file.writePositionalAll(io, value, base + address_len + key.len);
}

fn readItem(self: FileBackend, io: Io, address: AddressInfo, key_buf: *[max_key_len]u8) !Item {
    _ = try self.internal.file.readPositionalAll(io, key_buf[0..address.key_len], @as(u64, address.location) + address_len);

    return .{
        .file_backend = self,
        .address = address,
        .key = key_buf[0..address.key_len],
    };
}

fn readValue(self: FileBackend, io: Io, allocator: Allocator, address: AddressInfo) ![]const u8 {
    const value = try allocator.alloc(u8, @intCast(address.value_len));
    _ = try self.internal.file.readPositionalAll(io, value, @as(u64, address.location) + address_len + address.key_len);
    return value;
}

const LinkedListIterator = struct {
    address: ?AddressInfo,
    file_backend: FileBackend,
    key_buf: *[max_key_len]u8,
    io: Io,

    pub fn next(self: *LinkedListIterator) !?Item {
        if (self.address) |address| {
            const item = try self.file_backend.readItem(self.io, address, self.key_buf);
            if (address.linked_next_location) |next_location| {
                self.address = try self.file_backend.readAddress(self.io, next_location);
            } else {
                self.address = null;
            }
            return item;
        } else {
            return null;
        }
    }
};

fn linkedListIterator(self: FileBackend, io: Io, address: AddressInfo, key_buf: *[max_key_len]u8) LinkedListIterator {
    return .{ .address = address, .file_backend = self, .key_buf = key_buf, .io = io };
}

const AddressParams = struct {
    location: u32,
    key: []const u8,
    value: []const u8,
    linked_next_location: ?u32 = null,
    array_previous_location: ?u32 = null,
    array_next_location: ?u32 = null,
    array_end: union(enum) {
        location: u32,
        default: void,
        none: void,
    } = .default,
    max_key_len: ?u16 = null,
    max_value_len: ?u32 = null,
};

fn makeAddress(value_type: ValueType, params: AddressParams) AddressInfo {
    const array_end_location = switch (value_type) {
        .string => null,
        .array => switch (params.array_end) {
            .none => null,
            .default => params.location,
            .location => |location| location,
        },
    };

    return .{
        .type = value_type,
        .location = params.location,
        .linked_next_location = params.linked_next_location,
        .array_previous_location = params.array_previous_location,
        .array_next_location = params.array_next_location,
        .array_end_location = array_end_location,
        .key_len = @intCast(params.key.len),
        .value_len = @intCast(params.value.len),
        .max_key_len = params.max_key_len orelse @intCast(params.key.len),
        .max_value_len = params.max_value_len orelse @intCast(params.value.len),
    };
}

fn hash(input: []const u8) u32 {
    return hasher(input);
}

fn createFile(io: Io, path: []const u8, options: File.CreateFlags) !File {
    if (std.fs.path.dirname(path)) |dirname| {
        std.Io.Dir.createDirAbsolute(io, dirname, .default_dir) catch |err| {
            switch (err) {
                error.PathAlreadyExists => {},
                else => return err,
            }
        };
    }

    return std.Io.Dir.createFileAbsolute(io, path, options) catch |err| {
        std.debug.print("[jetkv] Error creating database file at: `{s}`\n", .{path});
        return err;
    };
}

fn serialize(T: type, value: T, buf: *[bufSize(T)]u8) void {
    switch (T) {
        u8, u16, u32 => std.mem.writeInt(T, buf, value, endian),
        ?u32 => std.mem.writeInt(u32, buf, value orelse 0, endian),
        AddressInfo => {
            serialize(ValueType, value.type, buf[0..1]);
            serialize(?u32, value.linked_next_location, buf[1..5]);
            serialize(?u32, value.array_next_location, buf[5..9]);
            serialize(?u32, value.array_previous_location, buf[9..13]);
            serialize(?u32, value.array_end_location, buf[13..17]);
            serialize(u16, value.key_len, buf[17..19]);
            serialize(u32, value.value_len, buf[19..23]);
            serialize(u16, value.max_key_len, buf[23..25]);
            serialize(u32, value.max_value_len, buf[25..29]);
        },
        Header => {
            serialize(u32, value.address_space_size, buf[0..4]);
            serialize(u32, value.ref_count, buf[4..8]);
        },
        ValueType => {
            serialize(u8, @intFromEnum(value), buf[0..1]);
        },
        AddressUpdateOptions.AddressUpdateLocationValue => switch (value) {
            .none => serialize(u32, 0, buf[0..4]),
            .value => |val| serialize(?u32, val, buf[0..4]),
        },
        else => @panic("Unsupported type: " ++ @typeName(T)),
    }
}

fn deserialize(T: type, buf: *[bufSize(T)]u8) T {
    return switch (T) {
        u8, u16, u32 => std.mem.readInt(T, buf, endian),
        ?u32 => if (std.mem.allEqual(u8, buf, 0)) null else std.mem.readInt(u32, buf, endian),
        else => @panic("Unsupported type: " ++ @typeName(T)),
    };
}

fn bufSize(T: type) u32 {
    return switch (T) {
        u8, u16, u32 => @divExact(@typeInfo(T).int.bits, 8),
        ?u32 => @divExact(@typeInfo(u32).int.bits, 8),
        AddressInfo => address_len,
        Header => header_len,
        ValueType => bufSize(u8),
        AddressUpdateOptions.AddressUpdateLocationValue => bufSize(u32),
        else => @panic("Unsupported type: " ++ @typeName(T)),
    };
}

fn isOverwrite(address: AddressInfo, key: []const u8, value: []const u8) bool {
    return key.len <= address.max_key_len and
        value.len <= address.max_value_len;
}

fn validateKey(key: []const u8) !void {
    if (key.len > max_key_len) return error.JetKVKeyTooLong;
}

const t = std.testing;
test "basic put/get" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "foo", "bar");
    try backend.put(t.io, t.allocator, "baz", "qux");

    if (try backend.get(t.io, t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("bar", foo);
    } else try t.expect(false);

    if (try backend.get(t.io, t.allocator, "baz")) |bar| {
        defer t.allocator.free(bar);
        try t.expectEqualStrings("qux", bar);
    } else try t.expect(false);
}

test "fetchRemove" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "foo", "bar");
    try backend.put(t.io, t.allocator, "baz", "qux");

    if (try backend.fetchRemove(t.io, t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("bar", foo);
    } else try t.expect(false);

    if (try backend.fetchRemove(t.io, t.allocator, "baz")) |bar| {
        defer t.allocator.free(bar);
        try t.expectEqualStrings("qux", bar);
    } else try t.expect(false);

    try t.expect(try backend.get(t.io, t.allocator, "foo") == null);
    try t.expect(try backend.get(t.io, t.allocator, "baz") == null);
}

test "fetchRemove collisions" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "foo", "bar");
    try backend.put(t.io, t.allocator, "baz", "qux");

    if (try backend.fetchRemove(t.io, t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("bar", foo);
    } else try t.expect(false);

    if (try backend.fetchRemove(t.io, t.allocator, "baz")) |bar| {
        defer t.allocator.free(bar);
        try t.expectEqualStrings("qux", bar);
    } else try t.expect(false);

    try t.expect(try backend.get(t.io, t.allocator, "foo") == null);
    try t.expect(try backend.get(t.io, t.allocator, "baz") == null);
}

test "remove" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "foo", "bar");
    try backend.put(t.io, t.allocator, "baz", "qux");
    try backend.put(t.io, t.allocator, "quux", "corge");

    try backend.remove(t.io, t.allocator, "foo");
    try backend.remove(t.io, t.allocator, "baz");

    try t.expect(try backend.get(t.io, t.allocator, "foo") == null);
    try t.expect(try backend.get(t.io, t.allocator, "baz") == null);

    if (try backend.get(t.io, t.allocator, "quux")) |value| {
        defer t.allocator.free(value);
        try t.expectEqualStrings("corge", value);
    }
}

test "overwriting equal length" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "foo", "bar");
    try backend.put(t.io, t.allocator, "foo", "baz");

    if (try backend.get(t.io, t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("baz", foo);
    } else try t.expect(false);
}

test "overwriting lesser length" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "foo", "abcdefghijklmno");
    try backend.put(t.io, t.allocator, "foo", "pqrs");

    if (try backend.get(t.io, t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("pqrs", foo);
    } else try t.expect(false);
}

test "overwriting mixed length, all lesser than initial" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "foo", "aaaaaaaaaaaa");

    const size_after_first_write = try kv.internal.file.length(t.io);

    try backend.put(t.io, t.allocator, "foo", "bbbbbbbb");
    try backend.put(t.io, t.allocator, "foo", "cccc");
    try backend.put(t.io, t.allocator, "foo", "ddd");
    try backend.put(t.io, t.allocator, "foo", "eeeeeeeee");

    if (try backend.get(t.io, t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("eeeeeeeee", foo);
    } else try t.expect(false);

    try t.expectEqual(size_after_first_write, try kv.internal.file.length(t.io));
}

test "overwriting increasingly lesser length" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "foo", "aaaaaaaaaaaa");

    const size_after_first_write = try kv.internal.file.length(t.io);

    try backend.put(t.io, t.allocator, "foo", "bbbbbbbb");
    try backend.put(t.io, t.allocator, "foo", "cccc");
    try backend.put(t.io, t.allocator, "foo", "dddddd");

    if (try backend.get(t.io, t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("dddddd", foo);
    } else try t.expect(false);

    try t.expectEqual(size_after_first_write, try kv.internal.file.length(t.io));
}

test "overwriting mixed length, all within over-alloc bounds of initial" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "foo", "aaaaaaaaaaaa");

    const size_after_first_write = try kv.internal.file.length(t.io);

    try backend.put(t.io, t.allocator, "foo", "bbbbbbbb");
    try backend.put(t.io, t.allocator, "foo", "cccc");
    try backend.put(t.io, t.allocator, "foo", "ddd");
    try backend.put(t.io, t.allocator, "foo", "eeeeeeeeeeeeeeeeeeee");

    if (try backend.get(t.io, t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("eeeeeeeeeeeeeeeeeeee", foo);
    } else try t.expect(false);

    try t.expectEqual(size_after_first_write, try kv.internal.file.length(t.io));
}

test "overwriting mixed length, all within over-alloc bounds of initial, longer strings" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "foo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

    const size_after_first_write = try kv.internal.file.length(t.io);

    try backend.put(t.io, t.allocator, "foo", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    try backend.put(t.io, t.allocator, "foo", "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");
    try backend.put(t.io, t.allocator, "foo", "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
    try backend.put(t.io, t.allocator, "foo", "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

    if (try backend.get(t.io, t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", foo);
    } else try t.expect(false);

    try t.expectEqual(size_after_first_write, header_len + kv.address_space_size + address_len + "foo".len + 1024);
}
test "collisions" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "foo", "baz");
    try backend.put(t.io, t.allocator, "foo", "qux");
    try backend.put(t.io, t.allocator, "bar", "quux");

    if (try backend.get(t.io, t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("qux", foo);
    } else try t.expect(false);

    if (try backend.get(t.io, t.allocator, "bar")) |bar| {
        defer t.allocator.free(bar);
        try t.expectEqualStrings("quux", bar);
    } else try t.expect(false);
}

test "many entries" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    @setEvalBranchQuota(10000);

    inline for (keypairs) |keypair| {
        try backend.put(t.io, t.allocator, keypair[0], keypair[1]);
    }

    inline for (keypairs) |keypair| {
        if (try backend.get(t.io, t.allocator, keypair[0])) |value| {
            defer t.allocator.free(value);
            try t.expectEqualStrings(keypair[1], value);
        } else {
            try t.expect(false);
        }
    }
}

test "array append/pop" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    const array = &[_][]const u8{ "foo", "bar", "baz" };
    for (array) |value| try backend.append(t.io, t.allocator, "array", value);

    const expected = &[_][]const u8{ "baz", "bar", "foo" };
    for (expected) |value| {
        const popped = (try backend.pop(t.io, t.allocator, "array")).?;
        defer t.allocator.free(popped);
        try t.expectEqualStrings(value, popped);
    }
    try t.expect(try backend.pop(t.io, t.allocator, "array") == null);
}

test "array append/popFirst" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    const array = &[_][]const u8{ "bar", "baz", "qux", "quux", "corge", "grault", "garply", "waldo", "fred", "plugh", "xyzzy", "thud" };

    for (array) |value| try backend.append(t.io, t.allocator, "array", value);

    for (array) |value| {
        const popFirstped = (try backend.popFirst(t.io, t.allocator, "array")).?;
        defer t.allocator.free(popFirstped);
        try t.expectEqualStrings(value, popFirstped);
    }
    try t.expect(try backend.popFirst(t.io, t.allocator, "array") == null);
}

test "array prepend/pop" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    const array = &[_][]const u8{ "bar", "baz", "qux", "quux", "corge", "grault", "garply", "waldo", "fred", "plugh", "xyzzy", "thud" };

    for (array) |value| try backend.prepend(t.io, t.allocator, "array", value);

    for (array) |value| {
        const popped = (try backend.pop(t.io, t.allocator, "array")).?;
        defer t.allocator.free(popped);
        try t.expectEqualStrings(value, popped);
    }
    try t.expect(try backend.popFirst(t.io, t.allocator, "array") == null);
}

test "array prepend/popFirst" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    const array = &[_][]const u8{ "bar", "baz", "qux", "quux", "corge", "grault", "garply", "waldo", "fred", "plugh", "xyzzy", "thud" };

    for (array) |value| try backend.prepend(t.io, t.allocator, "array", value);

    for (0..array.len) |index| {
        const popFirstped = (try backend.popFirst(t.io, t.allocator, "array")).?;
        defer t.allocator.free(popFirstped);
        try t.expectEqualStrings(array[array.len - 1 - index], popFirstped);
    }
    try t.expect(try backend.popFirst(t.io, t.allocator, "array") == null);
}

test "array append-pop-append-pop-append-pop" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.append(t.io, t.allocator, "array", "foo");
    try backend.append(t.io, t.allocator, "array", "bar");
    {
        const popped = try backend.pop(t.io, t.allocator, "array");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("bar", popped.?);
    }
    try backend.append(t.io, t.allocator, "array", "baz");
    try backend.append(t.io, t.allocator, "array", "foo");
    {
        const popped = try backend.pop(t.io, t.allocator, "array");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("foo", popped.?);
    }
    {
        const popped = try backend.pop(t.io, t.allocator, "array");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("baz", popped.?);
    }
    {
        const popped = try backend.pop(t.io, t.allocator, "array");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("foo", popped.?);
    }
    try t.expect(try backend.pop(t.io, t.allocator, "array") == null);
}

test "array collision popFirst" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.append(t.io, t.allocator, "array1", "foo");
    try backend.append(t.io, t.allocator, "array1", "bar");
    try backend.append(t.io, t.allocator, "array2", "baz");
    try backend.append(t.io, t.allocator, "array2", "qux");

    {
        const popped = try backend.popFirst(t.io, t.allocator, "array1");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("foo", popped.?);
    }

    {
        const popped = try backend.popFirst(t.io, t.allocator, "array2");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("baz", popped.?);
    }

    {
        const popped = try backend.popFirst(t.io, t.allocator, "array1");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("bar", popped.?);
    }

    {
        const popped = try backend.popFirst(t.io, t.allocator, "array2");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("qux", popped.?);
    }
}

test "array collision pop" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.append(t.io, t.allocator, "array1", "foo");
    try backend.append(t.io, t.allocator, "array1", "bar");
    try backend.append(t.io, t.allocator, "array2", "baz");
    try backend.append(t.io, t.allocator, "array2", "qux");

    {
        const popped = try backend.pop(t.io, t.allocator, "array1");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("bar", popped.?);
    }

    {
        const popped = try backend.pop(t.io, t.allocator, "array1");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("foo", popped.?);
    }

    {
        const popped = try backend.pop(t.io, t.allocator, "array2");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("qux", popped.?);
    }

    {
        const popped = try backend.pop(t.io, t.allocator, "array2");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("baz", popped.?);
    }
}

test "many append and popFirst" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    const initial_size = try kv.internal.file.length(t.io);

    @setEvalBranchQuota(10000);

    inline for (keypairs) |keypair| {
        try backend.append(t.io, t.allocator, "array", keypair[0]);
    }

    inline for (keypairs) |keypair| {
        const value = (try backend.popFirst(t.io, t.allocator, "array")).?;
        defer t.allocator.free(value);
        try t.expectEqualStrings(keypair[0], value);
    }

    try t.expectEqual(initial_size, try kv.internal.file.length(t.io));
}

test "many prepend and popFirst" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    const initial_size = try kv.internal.file.length(t.io);

    @setEvalBranchQuota(10000);

    inline for (keypairs) |keypair| {
        try backend.prepend(t.io, t.allocator, "array", keypair[0]);
    }

    inline for (0..keypairs.len) |index| {
        const value = (try backend.popFirst(t.io, t.allocator, "array")).?;
        defer t.allocator.free(value);
        try t.expectEqualStrings(keypairs[keypairs.len - index - 1][0], value);
    }

    try t.expectEqual(initial_size, try kv.internal.file.length(t.io));
}

test "many append and pop" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    const initial_size = try kv.internal.file.length(t.io);

    @setEvalBranchQuota(10000);

    inline for (keypairs) |keypair| {
        try backend.append(t.io, t.allocator, "array", keypair[0]);
    }

    inline for (0..keypairs.len) |index| {
        const value = (try backend.pop(t.io, t.allocator, "array")).?;
        defer t.allocator.free(value);
        try t.expectEqualStrings(keypairs[keypairs.len - index - 1][0], value);
    }

    try t.expectEqual(initial_size, try kv.internal.file.length(t.io));
}

test "many prepend and pop" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    const initial_size = try kv.internal.file.length(t.io);

    @setEvalBranchQuota(10000);

    inline for (keypairs) |keypair| {
        try backend.prepend(t.io, t.allocator, "array", keypair[0]);
    }

    inline for (keypairs) |keypair| {
        const value = (try backend.pop(t.io, t.allocator, "array")).?;
        defer t.allocator.free(value);
        try t.expectEqualStrings(keypair[0], value);
    }

    try t.expectEqual(initial_size, try kv.internal.file.length(t.io));
}

test "put string, overwrite array" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "foo", "bar");
    try backend.append(t.io, t.allocator, "foo", "baz");

    const popped = try backend.pop(t.io, t.allocator, "foo");
    defer t.allocator.free(popped.?);
    try t.expectEqualStrings("baz", popped.?);
}

test "put string, overwrite array with collisions" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "foo", "baz");
    try backend.put(t.io, t.allocator, "bar", "qux");
    try backend.append(t.io, t.allocator, "bar", "quux");

    const popped = try backend.pop(t.io, t.allocator, "bar");
    defer t.allocator.free(popped.?);
    try t.expectEqualStrings("quux", popped.?);
}

test "put string, pop array" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "foo", "baz");

    try t.expect(try backend.pop(t.io, t.allocator, "foo") == null);
}

test "bug: previous value returned for overwritten key" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    try backend.put(t.io, t.allocator, "ka", "spam");
    try backend.put(t.io, t.allocator, "1", "eggs");
    try backend.put(t.io, t.allocator, "1", "jetkv");

    const value = try backend.get(t.io, t.allocator, "1");
    defer t.allocator.free(value.?);
    try t.expectEqualStrings("jetkv", value.?);
}

test "bug: stale value returned for deleted key" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    try backend.put(t.io, t.allocator, "fnIEV", "spam");
    try backend.put(t.io, t.allocator, "7", "eggs");
    try backend.remove(t.io, t.allocator, "7");

    const value = try backend.get(t.io, t.allocator, "7");
    try t.expect(value == null);
}

test "bug: pop collisions" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    try backend.put(t.io, t.allocator, "foo", "bar");
    try backend.put(t.io, t.allocator, "bar", "baz");
    try backend.append(t.io, t.allocator, "qux", "quux");

    const value = try backend.pop(t.io, t.allocator, "qux");
    defer t.allocator.free(value.?);
    try t.expectEqualStrings("quux", value.?);
}

test "bug: popFirst collisions" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    try backend.put(t.io, t.allocator, "foo", "bar");
    try backend.put(t.io, t.allocator, "bar", "baz");
    try backend.append(t.io, t.allocator, "qux", "quux");

    const value = try backend.popFirst(t.io, t.allocator, "qux");
    defer t.allocator.free(value.?);
    try t.expectEqualStrings("quux", value.?);
}

test "bug: linked string removal" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 64,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "rYpIf4P", "spam");
    try backend.put(t.io, t.allocator, "MP", "spam");
    try backend.put(t.io, t.allocator, "q", "spam");
    try backend.remove(t.io, t.allocator, "MP");

    const value = try backend.get(t.io, t.allocator, "q");
    defer t.allocator.free(value.?);
    try t.expectEqualStrings("spam", value.?);
}

test "bug: replace first string in linked list with insufficient space for overwrite" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "J", "spam");
    try backend.put(t.io, t.allocator, "j", "spam");
    try backend.put(t.io, t.allocator, "J", "LbTHW3x0R7xhTpJOdBRS0xxHHseFh8eGSZ7beYzpeJpAKYI20m_llAFJf5E9ChcnIBRlMvL3vAsSI0InrZ2jDsDdxLsgzmPL7fKTTvkQpDj4PUZ0ezUjFFgIopzoY4DpjdC6sEmp3xWZMWeyWLyvrnO1B6nMTAS97nPGVGIMIEg8ClQnbB8rLafIYLq4NRcWW1ovw9l7FiuiHk089gOTOi5fjgiflDSXUgiwRIZ6j4VQmbKgQ8PCqNn64N5s9u1_cminIG_4mvNz6K8mCsu_6xKXa4hobCTMOZnNcSo68QW1JyVfT8uaUWascE3LbVTyOOCAhs89K_y");

    const value = try backend.get(t.io, t.allocator, "j");
    defer t.allocator.free(value.?);
    try t.expectEqualStrings("spam", value.?);
}

test "bug: overwrite linked string (from string) with array" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.append(t.io, t.allocator, "mch4o", "spma");
    try backend.put(t.io, t.allocator, "4", "spam");
    try backend.append(t.io, t.allocator, "4", "spam");

    {
        const value = try backend.get(t.io, t.allocator, "4");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("spam", value.?);
    }

    {
        const value = try backend.popFirst(t.io, t.allocator, "mch4o");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("spma", value.?);
    }
}

test "bug: popFirst linked string" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.append(t.io, t.allocator, "NfhsbHl", "spam");
    try backend.put(t.io, t.allocator, "_", "spam");
    try t.expect(try backend.popFirst(t.io, t.allocator, "_") == null);
}

test "bug: append to empty linked array" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 23,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "BZWOy", "spam");
    try backend.append(t.io, t.allocator, "", "spam");
    try backend.put(t.io, t.allocator, "j6", "spam");

    const value = try backend.pop(t.io, t.allocator, "");
    defer t.allocator.free(value.?);

    try t.expectEqualStrings("spam", value.?);
}

test "bug: popFirst from array linked from empty array" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "KrSA", "spam");
    try backend.append(t.io, t.allocator, "Q", "spam");
    try backend.append(t.io, t.allocator, "Q", "eggs");

    {
        const value = try backend.popFirst(t.io, t.allocator, "Q");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("spam", value.?);
    }

    {
        const value = try backend.popFirst(t.io, t.allocator, "Q");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("eggs", value.?);
    }
}

test "bug: overwrite linked string with array" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "V", "spam");
    try backend.put(t.io, t.allocator, "n", "spam");
    try backend.append(t.io, t.allocator, "V", "spam");

    const value = try backend.get(t.io, t.allocator, "n");
    defer t.allocator.free(value.?);

    try t.expectEqualStrings("spam", value.?);
}

test "bug: append to empty indexed array" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.append(t.io, t.allocator, "2", "spamspam");
    try backend.append(t.io, t.allocator, "_P", "spam");

    {
        const value = try backend.popFirst(t.io, t.allocator, "2");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("spamspam", value.?);
    }

    try backend.append(t.io, t.allocator, "2", "eggs");

    {
        const value = try backend.get(t.io, t.allocator, "2");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("eggs", value.?);
    }
}

test "bug: append to empty linked array after emptying" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "BCMB92d", "spam");
    try backend.append(t.io, t.allocator, "3", "spamspam");
    {
        const value = try backend.popFirst(t.io, t.allocator, "3");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("spamspam", value.?);
    }
    try backend.append(t.io, t.allocator, "3", "eggs");

    {
        const value = try backend.popFirst(t.io, t.allocator, "3");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("eggs", value.?);
    }
}

test "bug: popFirst with linked value (preserve link)" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 3,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.append(t.io, t.allocator, "rw", "spam");
    try backend.put(t.io, t.allocator, "m", "spam");
    try backend.put(t.io, t.allocator, "0", "spam");
    try backend.append(t.io, t.allocator, "m", "eggs");
    try backend.append(t.io, t.allocator, "m", "boom");

    {
        const value = try backend.popFirst(t.io, t.allocator, "m");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("eggs", value.?);
    }

    {
        const value = try backend.get(t.io, t.allocator, "0");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("spam", value.?);
    }
}

test "bug: infinite loop on linked array pop" {
    var kv: FileBackend = try .init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 3,
        .truncate = true,
    }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);

    try backend.put(t.io, t.allocator, "8", "spam");
    try backend.append(t.io, t.allocator, "r", "eggs");
    {
        const value = try backend.pop(t.io, t.allocator, "r");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings(
            "eggs",
            value.?,
        );
    }
    try backend.append(t.io, t.allocator, "8", "spamandeggs");
    try t.expect(try backend.pop(t.io, t.allocator, "2e") == null);
}
