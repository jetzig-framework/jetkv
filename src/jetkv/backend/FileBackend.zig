const std = @import("std");

const jetkv = @import("../../jetkv.zig");

/// Options specific to the File-based backend.
pub const Options = struct {
    path: ?[]const u8 = null,
    address_space_size: u32 = FileBackend.addressSpace(4096),
    truncate: bool = false,
};

options: Options,
mutex: std.Thread.Mutex,
path: []const u8,
file: std.fs.File,
address_space_size: u32,
address_space: u32,

const FileBackend = @This();

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

    pub fn value(self: Item, allocator: std.mem.Allocator) ![]const u8 {
        return try self.file_backend.readValue(allocator, self.address);
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

/// Initialize a new file-based storage backend.
pub fn init(options: Options) !FileBackend {
    if (try std.math.mod(u32, options.address_space_size, bufSize(u32)) != 0) {
        return error.JetKVInvalidAddressSpaceSize;
    }
    const path = options.path orelse return error.JetKVMissingFilePath;

    var backend = FileBackend{
        .options = options,
        .mutex = std.Thread.Mutex{},
        .path = path,
        .address_space_size = options.address_space_size,
        .address_space = @divExact(options.address_space_size, bufSize(u32)),
        .file = try createFile(
            path,
            .{ .read = true, .lock = .exclusive, .truncate = options.truncate },
        ),
    };
    try backend.initAddressSpace();
    try backend.initHeader();
    return backend;
}

/// Close database file.
pub fn deinit(self: *FileBackend) void {
    self.file.close();
}

/// Fetch a string from the file-based backend.
pub fn get(self: *FileBackend, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    try validateKey(key);

    self.mutex.lock();
    defer self.mutex.unlock();

    return try self.getString(allocator, key);
}

/// Add a String to the file-based backend.
pub fn put(self: *FileBackend, key: []const u8, value: []const u8) !void {
    try validateKey(key);

    self.mutex.lock();
    defer self.mutex.unlock();

    try self.putString(key, value);
    try self.sync();
}

/// Remove a String from the file-based backend.
pub fn remove(self: *FileBackend, key: []const u8) !void {
    try validateKey(key);

    self.mutex.lock();
    defer self.mutex.unlock();

    try self.removeString(key);
    try self.sync();
}

/// Remove a String from the file-based backend and return it if found.
pub fn fetchRemove(self: *FileBackend, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    try validateKey(key);

    self.mutex.lock();
    defer self.mutex.unlock();

    return if (try self.getString(allocator, key)) |capture| blk: {
        try self.removeString(key);
        try self.sync();
        break :blk capture;
    } else null;
}

/// Insert a String to the start of an Array in the file-based backend.
pub fn prepend(self: *FileBackend, key: []const u8, value: []const u8) !void {
    self.mutex.lock();
    defer self.mutex.unlock();

    try validateKey(key);

    const index = try self.locate(key);
    if (try self.readIndexAddress(index)) |address| {
        var key_buf: [max_key_len]u8 = undefined;
        const item = try self.readItem(address, &key_buf);
        const is_equal = std.mem.eql(u8, item.key, key);

        if (is_equal and address.type == .array and address.array_end_location != null) {
            // No collision
            const location = try self.prependItemToExistingArray(address, key, value);
            try self.updateLocation(index, location);
            try self.incRefCount();
        } else if (is_equal) {
            // Overwrite string/re-use empty array
            try self.createArray(index, address, key, value, .{ .linked = false });
        } else {
            // Collision
            try self.prependLinked(item, key, value, &key_buf);
        }
    } else {
        try self.createArray(index, null, key, value, .{ .linked = false });
        try self.incRefCount();
    }
    try self.sync();
}

/// Pop a String from the end of an Array in the file-based backend.
pub fn pop(self: *FileBackend, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    try validateKey(key);

    self.mutex.lock();
    defer self.mutex.unlock();

    const index = try self.locate(key);
    const value = if (try self.readIndexAddress(index)) |address| blk: {
        var key_buf: [max_key_len]u8 = undefined;
        const item = try self.readItem(address, &key_buf);
        if (std.mem.eql(u8, item.key, key)) {
            // No collision
            if (address.type != .array) return null;
            break :blk try self.popIndexed(allocator, item, &key_buf);
        } else {
            // Collision
            break :blk try self.popLinked(allocator, item, key, &key_buf);
        }
    } else null;

    try self.sync();

    return value;
}

/// Pop a String from the start of an Array in the file-based backend.
pub fn popFirst(self: *FileBackend, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    try validateKey(key);

    self.mutex.lock();
    defer self.mutex.unlock();

    var key_buf: [max_key_len]u8 = undefined;
    const index = try self.locate(key);
    const value = if (try self.readIndexAddress(index)) |address| blk: {
        const item = try self.readItem(address, &key_buf);
        if (std.mem.eql(u8, item.key, key)) {
            if (address.type != .array) return null;
            // No collision
            break :blk try self.popFirstIndexed(allocator, index, item);
        } else {
            // Collision
            break :blk try self.popFirstLinked(allocator, item, key, &key_buf);
        }
    } else null;

    try self.sync();
    return value;
}

fn popIndexed(
    self: FileBackend,
    allocator: std.mem.Allocator,
    item: Item,
    key_buf: *[max_key_len]u8,
) !?[]const u8 {
    if (item.address.type != .array) return null;

    const end_location = item.address.array_end_location orelse return null;

    if (try self.readAddress(end_location)) |last_item_address| {
        const last_item = try self.readItem(last_item_address, key_buf);
        const value = try last_item.value(allocator);
        try self.shrinkArray(item.address, last_item.address);
        try self.decRefCount();
        try self.maybeTruncate(last_item.address);
        return value;
    } else {
        return null;
    }
}

fn popLinked(
    self: FileBackend,
    allocator: std.mem.Allocator,
    item: Item,
    key: []const u8,
    key_buf: *[max_key_len]u8,
) !?[]const u8 {
    var it = self.linkedListIterator(item.address, key_buf);
    var previous_item = item;

    while (try it.next()) |linked_item| {
        const is_equal_key = std.mem.eql(u8, linked_item.key, key);
        if (is_equal_key and linked_item.address.type != .array) return null;

        if (is_equal_key and linked_item.address.type == .array) {
            const end_location = linked_item.address.array_end_location orelse return null;

            if (try self.readAddress(end_location)) |last_item_address| {
                const last_item = try self.readItem(last_item_address, key_buf);
                const value = try last_item.value(allocator);
                try self.shrinkArray(linked_item.address, last_item.address);

                if (linked_item.address.location == end_location) {
                    try self.updateAddress(
                        previous_item.address.location,
                        .{ .linked_next_location = .{ .value = linked_item.address.linked_next_location } },
                    );
                }

                try self.decRefCount();
                try self.maybeTruncate(last_item.address);
                return value;
            } else if (is_equal_key and linked_item.address.type == .string) {
                return null;
            } else if (is_equal_key) unreachable;
        }
        previous_item = linked_item;
    }
    return null;
}

fn popFirstLinked(
    self: FileBackend,
    allocator: std.mem.Allocator,
    item: Item,
    key: []const u8,
    key_buf: *[max_key_len]u8,
) !?[]const u8 {
    var it = self.linkedListIterator(item.address, key_buf);
    var previous_item = item;

    while (try it.next()) |linked_item| {
        const is_equal_key = std.mem.eql(u8, linked_item.key, key);
        if (is_equal_key and linked_item.address.type == .array) {
            if (linked_item.address.array_next_location) |next_location| {
                try self.updateAddress(
                    previous_item.address.location,
                    .{ .linked_next_location = .{ .value = next_location } },
                );
                try self.updateAddress(
                    next_location,
                    .{
                        .array_end_location = .{ .value = linked_item.address.array_end_location },
                        .linked_next_location = .{ .value = linked_item.address.linked_next_location },
                    },
                );
            } else {
                try self.updateAddress(
                    linked_item.address.location,
                    .{ .array_end_location = .none, .array_next_location = .none },
                );
            }
            const value = try linked_item.value(allocator);
            try self.decRefCount();
            return value;
        } else if (is_equal_key and linked_item.address.type == .string) {
            return null;
        } else if (is_equal_key) unreachable;
        previous_item = linked_item;
    }
    return null;
}

fn popFirstIndexed(
    self: FileBackend,
    allocator: std.mem.Allocator,
    index: u32,
    item: Item,
) !?[]const u8 {
    if (item.address.array_next_location) |array_next_location| {
        try self.updateLocation(index, array_next_location);
        // Maintain possible next linked item
        try self.updateAddress(
            array_next_location,
            .{
                .array_end_location = .{ .value = item.address.array_end_location },
                .linked_next_location = .{ .value = item.address.linked_next_location },
            },
        );
    } else if (item.address.linked_next_location == null) {
        try self.updateLocation(index, null);
    } else {
        try self.updateAddress(item.address.location, .{ .array_end_location = .none });
    }
    const value = try item.value(allocator);
    try self.decRefCount();
    return value;
}

fn prependLinked(
    self: FileBackend,
    item: Item,
    key: []const u8,
    value: []const u8,
    key_buf: *[max_key_len]u8,
) !void {
    var it = self.linkedListIterator(item.address, key_buf);
    var previous_item = item;

    while (try it.next()) |linked_item| {
        const is_equal_key = std.mem.eql(u8, linked_item.key, key);
        if (is_equal_key and linked_item.address.type == .array) {
            const location = try self.prependItemToExistingArray(
                linked_item.address,
                key,
                value,
            );
            try self.updateAddress(
                previous_item.address.location,
                .{ .linked_next_location = .{ .value = location } },
            );
            try self.incRefCount();
            return;
        } else if (is_equal_key and linked_item.address.type == .string) {
            // Overwrite string value
            try self.updateAddress(linked_item.address.location, .{ .type = .array });
            try self.createArray(
                linked_item.address.location,
                linked_item.address,
                key,
                value,
                .{ .linked = false },
            );
            break;
        } else if (is_equal_key) unreachable;

        previous_item = linked_item;
    }

    // No matches in linked list - create new array at EOF and link to final item in
    // linked list
    try self.createArray(previous_item.address.location, null, key, value, .{ .linked = true });
    try self.incRefCount();
}

// Drop one item the end of an array and update end location.
fn shrinkArray(self: FileBackend, first_item_address: AddressInfo, last_item_address: AddressInfo) !void {
    if (last_item_address.array_previous_location) |previous_location| {
        // Nullify next item pointer for next-to-last item, update end location to
        // next-to-last item
        var location_buf: [bufSize(u32)]u8 = undefined;
        serialize(u32, 0, &location_buf);
        try self.file.seekTo(previous_location + array_next_location_offset);
        try self.file.writeAll(&location_buf);

        try self.updateAddress(
            first_item_address.location,
            .{ .array_end_location = .{ .value = last_item_address.array_previous_location.? } },
        );
    } else {
        // We reached the first item
        try self.updateAddress(first_item_address.location, .{ .array_end_location = .none });
    }
}

// Initialize address space with zeroes.
fn initAddressSpace(self: FileBackend) !void {
    try self.file.seekTo(0 + header_len);
    const writer = self.file.writer();
    try writer.writeByteNTimes(0, @intCast(self.address_space_size));
}

fn initHeader(self: FileBackend) !void {
    try self.file.seekTo(0);
    const header: Header = .{
        .address_space_size = self.address_space_size,
        .ref_count = 0,
    };
    var buf: [bufSize(Header)]u8 = undefined;
    serialize(Header, header, &buf);
    try self.file.writeAll(&buf);
}

fn incRefCount(self: FileBackend) !void {
    var header = try self.readHeader();
    header.ref_count += 1;
    try self.writeHeader(header);
}

fn decRefCount(self: FileBackend) !void {
    var header = try self.readHeader();
    if (header.ref_count == 0) unreachable;
    header.ref_count -= 1;
    try self.writeHeader(header);
    if (header.ref_count == 0) {
        try self.setEndPos(header_len + self.address_space_size);
        try self.initAddressSpace();
    }
}

fn writeHeader(self: FileBackend, header: Header) !void {
    var header_buf: [bufSize(Header)]u8 = undefined;
    serialize(Header, header, &header_buf);
    try self.file.seekTo(0);
    try self.file.writeAll(&header_buf);
}

fn readHeader(self: FileBackend) !Header {
    var header_buf: [bufSize(Header)]u8 = undefined;
    try self.file.seekTo(0);
    _ = try self.file.readAll(&header_buf);
    return .{
        .address_space_size = std.mem.readInt(u32, header_buf[0..4], endian),
        .ref_count = std.mem.readInt(u32, header_buf[4..8], endian),
    };
}

fn getEndPos(self: FileBackend) !u32 {
    return @intCast(try self.file.getEndPos());
}

fn setEndPos(self: FileBackend, location: u32) !void {
    try self.file.setEndPos(location);
}

fn sync(self: FileBackend) !void {
    try self.file.sync();
}

// Truncate the file if the given address + key/value reaches EOF.
fn maybeTruncate(self: FileBackend, address: AddressInfo) !void {
    if (address.linked_next_location) |next_location| {
        if (try self.readAddress(next_location)) |_| return; // We need to retain the link
    }
    if (try self.isTerminatingAddress(address)) try self.setEndPos(address.location);
}

fn isTerminatingAddress(self: FileBackend, address: AddressInfo) !bool {
    const address_end = address.location + address_len + address.max_key_len + address.max_value_len;
    return address_end == try self.getEndPos();
}

fn getString(self: FileBackend, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    const index = try self.locate(key);
    const location = try self.readLocation(index) orelse return null;
    const address = try self.readAddress(location) orelse return null;

    var key_buf: [max_key_len]u8 = undefined;
    const item = try self.readItem(address, &key_buf);
    if (std.mem.eql(u8, item.key, key)) return try item.value(allocator);

    var it = self.linkedListIterator(item.address, &key_buf);

    while (try it.next()) |linked_item| {
        if (std.mem.eql(u8, linked_item.key, key)) {
            return try linked_item.value(allocator);
        }
    }
    return null;
}

fn putString(self: FileBackend, key: []const u8, value: []const u8) !void {
    const index = try self.locate(key);
    if (try self.readIndexAddress(index)) |address| {
        var key_buf: [max_key_len]u8 = undefined;
        const item = try self.readItem(address, &key_buf);
        if (std.mem.eql(u8, item.key, key)) {
            if (key.len <= item.address.max_key_len and value.len <= item.address.max_value_len) {
                try self.updateString(item, key, value);
            } else {
                try self.writeString(index, item.address, key, value);
            }
        } else {
            try self.writeLinkedString(item, key, value);
        }
    } else {
        try self.writeString(index, null, key, value);
        try self.incRefCount();
    }
}

fn removeString(self: FileBackend, key: []const u8) !void {
    const index = try self.locate(key);
    if (try self.readIndexAddress(index)) |address| {
        var key_buf: [max_key_len]u8 = undefined;
        const item = try self.readItem(address, &key_buf);
        if (std.mem.eql(u8, item.key, key)) {
            try self.updateLocation(index, address.linked_next_location); // can be null
            try self.decRefCount();
        } else {
            try self.removeLinkedString(item, key);
        }
    }
}

pub fn append(self: *FileBackend, key: []const u8, value: []const u8) !void {
    self.mutex.lock();
    defer self.mutex.unlock();

    const index = try self.locate(key);
    if (try self.readIndexAddress(index)) |address| {
        var key_buf: [max_key_len]u8 = undefined;
        const item = try self.readItem(address, &key_buf);
        const is_equal = std.mem.eql(u8, item.key, key);

        if (is_equal and address.type == .array and address.array_end_location != null) {
            // No collision
            try self.appendItemToExistingArray(address, key, value);
            try self.incRefCount();
            return;
        } else if (is_equal) {
            // Overwrite string/re-use empty array
            try self.createArray(index, address, key, value, .{ .linked = false });
            return;
        } else {
            // Collision
            var it = self.linkedListIterator(item.address, &key_buf);
            var previous_item = item;

            while (try it.next()) |linked_item| {
                const is_equal_key = std.mem.eql(u8, linked_item.key, key);
                if (is_equal_key and linked_item.address.type == .array) {
                    if (linked_item.address.array_end_location == null) {
                        try self.createArray(
                            previous_item.address.location,
                            previous_item.address,
                            key,
                            value,
                            .{ .linked = true },
                        );
                    } else {
                        try self.appendItemToExistingArray(linked_item.address, key, value);
                    }
                    try self.incRefCount();
                    return;
                } else if (is_equal_key and linked_item.address.type == .string) {
                    // Overwrite string value
                    try self.updateAddress(previous_item.address.location, .{ .type = .array });
                    try self.createArray(
                        previous_item.address.location,
                        previous_item.address,
                        key,
                        value,
                        .{ .linked = true },
                    );
                    return;
                } else if (is_equal_key) unreachable;

                previous_item = linked_item;
            }

            // No matches in linked list - create new array at EOF and link to final item in
            // linked list
            try self.createArray(previous_item.address.location, null, key, value, .{ .linked = true });
            try self.incRefCount();
        }
    } else {
        try self.createArray(index, null, key, value, .{ .linked = false });
        try self.incRefCount();
    }
}

const CreateArrayOptions = struct {
    linked: bool,
};

fn createArray(
    self: FileBackend,
    index: u32,
    maybe_address: ?AddressInfo,
    key: []const u8,
    value: []const u8,
    options: CreateArrayOptions,
) !void {
    var address_buf: [address_len]u8 = undefined;

    const end_pos = try self.getEndPos();
    const linked_next_location = if (maybe_address) |address| address.linked_next_location else null;
    const address = makeAddress(.array, .{
        .location = end_pos,
        .linked_next_location = linked_next_location,
        .key = key,
        .value = value,
    });
    serialize(AddressInfo, address, &address_buf);

    try self.updateLocation(index + if (options.linked) linked_next_location_offset else 0, end_pos);

    try self.file.seekTo(end_pos);
    try self.file.writeAll(&address_buf);
    try self.file.writeAll(key);
    try self.file.writeAll(value);
}

fn appendItemToExistingArray(
    self: FileBackend,
    address: AddressInfo,
    key: []const u8,
    value: []const u8,
) !void {
    var address_buf: [address_len]u8 = undefined;
    const end_pos = try self.getEndPos();

    // Update next item pointer for previous item
    try self.updateAddress(address.array_end_location.?, .{ .array_next_location = .{ .value = end_pos } });

    const new_address = makeAddress(.array, .{
        .location = end_pos,
        .array_previous_location = address.array_end_location.?,
        .array_end = .none, // Array end is only stored at first item
        .key = key,
        .value = value,
    });

    serialize(AddressInfo, new_address, &address_buf);
    try self.file.seekTo(end_pos);
    try self.file.writeAll(&address_buf);
    try self.file.writeAll(key);
    try self.file.writeAll(value);

    try self.updateAddress(address.location, .{ .array_end_location = .{ .value = end_pos } });
}

fn prependItemToExistingArray(
    self: FileBackend,
    address: AddressInfo,
    key: []const u8,
    value: []const u8,
) !u32 {
    var address_buf: [address_len]u8 = undefined;
    const end_pos = try self.getEndPos();

    const new_address = makeAddress(.array, .{
        .location = end_pos,
        .array_next_location = address.location,
        .array_end = .{ .location = address.array_end_location orelse address.location },
        .key = key,
        .value = value,
    });

    // Update previous item pointer for previous item
    try self.updateAddress(address.location, .{
        .array_previous_location = .{ .value = end_pos },
        .array_end_location = .none,
    });

    serialize(AddressInfo, new_address, &address_buf);
    try self.file.seekTo(end_pos);
    try self.file.writeAll(&address_buf);
    try self.file.writeAll(key);
    try self.file.writeAll(value);

    return end_pos;
}

fn locate(self: FileBackend, key: []const u8) !u32 {
    const value = try std.math.mod(u32, hash(key), self.address_space) * bufSize(u32);
    return header_len + value;
}

// Fetch a location from the index, i.e. a pointer to an address.
fn readLocation(self: FileBackend, index: u32) !?u32 {
    var location_buf: [bufSize(u32)]u8 = undefined;
    try self.file.seekTo(index);
    _ = try self.file.readAll(&location_buf);
    const location = std.mem.readInt(u32, &location_buf, endian);
    return if (location == 0) null else location;
}

// Update a location pointer, either in the main index or as an address's next item pointer.
fn updateLocation(self: FileBackend, index: u32, location: ?u32) !void {
    var location_buf: [bufSize(u32)]u8 = undefined;
    serialize(u32, location orelse 0, &location_buf);
    try self.file.seekTo(index);
    try self.file.writeAll(&location_buf);
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

fn updateAddress(self: FileBackend, location: u32, options: AddressUpdateOptions) !void {
    var buf: [4]u8 = undefined;

    if (options.type) |value_type| {
        serialize(ValueType, value_type, buf[0..1]);
        try self.file.seekTo(location);
        try self.file.writeAll(buf[0..1]);
    }

    if (options.linked_next_location) |linked_next_location| {
        serialize(AddressUpdateOptions.AddressUpdateLocationValue, linked_next_location, buf[0..4]);
        try self.file.seekTo(location + linked_next_location_offset);
        try self.file.writeAll(buf[0..4]);
    }

    if (options.array_next_location) |array_next_location| {
        serialize(AddressUpdateOptions.AddressUpdateLocationValue, array_next_location, buf[0..4]);
        try self.file.seekTo(location + array_next_location_offset);
        try self.file.writeAll(buf[0..4]);
    }

    if (options.array_previous_location) |array_previous_location| {
        serialize(AddressUpdateOptions.AddressUpdateLocationValue, array_previous_location, buf[0..4]);
        try self.file.seekTo(location + array_previous_location_offset);
        try self.file.writeAll(buf[0..4]);
    }

    if (options.array_end_location) |array_end_location| {
        serialize(AddressUpdateOptions.AddressUpdateLocationValue, array_end_location, buf[0..4]);
        try self.file.seekTo(location + array_end_location_offset);
        try self.file.writeAll(buf[0..4]);
    }
}

fn readIndexAddress(self: FileBackend, index: u32) !?AddressInfo {
    if (try self.readLocation(index)) |location| {
        return try self.readAddress(location);
    } else {
        return null;
    }
}

fn readAddress(self: FileBackend, location: u32) !?AddressInfo {
    // TODO: Save a few bytes by using different address formats for strings and arrays
    try self.file.seekTo(location);
    var buf: [address_len]u8 = undefined;
    if (try self.file.readAll(&buf) < address_len) return null; // File was truncated.

    if (std.mem.eql(u8, &buf, &empty)) {
        return null;
    } else {
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
}

// Write a string to the end of the file, update index to point to new location.
// `index` can be either a location in the main index or the location of an address's next item
// pointer.
fn writeString(
    self: FileBackend,
    index: u32,
    maybe_address_info: ?AddressInfo,
    key: []const u8,
    value: []const u8,
) !void {
    const end_pos = try self.getEndPos();

    var location_buf: [bufSize(u32)]u8 = undefined;
    serialize(u32, end_pos, &location_buf);
    try self.file.seekTo(index);
    _ = try self.file.writeAll(&location_buf);

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

    try self.file.seekTo(end_pos);
    const writer = self.file.writer();
    try writer.writeAll(&address_buf);
    try writer.writeAll(key);
    try writer.writeAll(value);
    try writer.writeByteNTimes(0, max_value_len - value.len);
}

// Round value length up to a nearby number for over-allocation purposes to allow re-use of
// address space when updating values of similar length.
inline fn bandedValueLength(value: []const u8) u32 {
    return if (value.len <= 256)
        256
    else if (value.len <= 512)
        512
    else if (value.len <= 1024)
        1024
    else if (value.len <= 4096)
        4096
    else if (value.len <= 8192)
        8192
    else
        @intCast(value.len);
}

// Update a string in place - assume key and value lengths are within existing key and value bounds.
fn updateString(self: FileBackend, item: Item, key: []const u8, value: []const u8) !void {
    try self.file.seekTo(item.address.location);
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

    try self.file.writeAll(&address_buf);
    try self.file.writeAll(key);
    try self.file.writeAll(value);
}

// Follow links until end, then either:
// * Update an existing link if the key matches
// * Append to EOF if no link has the given key, updating the last link's next item pointer
fn writeLinkedString(self: FileBackend, item: Item, key: []const u8, value: []const u8) !void {
    var key_buf: [max_key_len]u8 = undefined;
    var it = self.linkedListIterator(item.address, &key_buf);
    var previous_item = item;

    while (try it.next()) |linked_item| {
        if (std.mem.eql(u8, linked_item.key, key)) {
            try self.updateLinkedString(previous_item.address, linked_item.address, key, value);
            return;
        }

        previous_item = linked_item;
    }

    // Write string using the final item's next item pointer as index
    try self.writeString(previous_item.address.location + linked_next_location_offset, null, key, value);
    try self.incRefCount();
}

fn removeLinkedString(self: FileBackend, item: Item, key: []const u8) !void {
    var key_buf: [max_key_len]u8 = undefined;
    var it = self.linkedListIterator(item.address, &key_buf);
    var previous_item = item;

    while (try it.next()) |linked_item| {
        if (std.mem.eql(u8, linked_item.key, key)) {
            const T = AddressUpdateOptions.AddressUpdateLocationValue;
            const linked_next_location: T = if (linked_item.address.linked_next_location) |location|
                .{ .value = location }
            else
                .none;
            try self.updateAddress(
                previous_item.address.location,
                .{ .linked_next_location = linked_next_location },
            );
            try self.decRefCount();
            return;
        }

        previous_item = linked_item;
    }
}

fn updateLinkedString(
    self: FileBackend,
    previous_address: AddressInfo,
    address: AddressInfo,
    key: []const u8,
    value: []const u8,
) !void {
    const is_overwrite = isOverwrite(address, key, value);
    const end_pos = if (is_overwrite)
        address.location
    else
        try self.getEndPos();

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

    if (!is_overwrite) {
        try self.updateAddress(
            previous_address.location,
            .{ .linked_next_location = .{ .value = end_pos } },
        );
    }

    try self.file.seekTo(end_pos);
    try self.file.writeAll(&new_address_buf);
    try self.file.writeAll(key);
    try self.file.writeAll(value);
}

fn readItem(self: FileBackend, address: AddressInfo, key_buf: *[max_key_len]u8) !Item {
    try self.file.seekTo(address.location + address_len);
    _ = try self.file.readAll(key_buf[0..address.key_len]);

    return .{
        .file_backend = self,
        .address = address,
        .key = key_buf[0..address.key_len],
    };
}

fn readValue(self: FileBackend, allocator: std.mem.Allocator, address: AddressInfo) ![]const u8 {
    const value = try allocator.alloc(u8, @intCast(address.value_len));
    try self.file.seekTo(address.location + address_len + address.key_len);
    _ = try self.file.readAll(value);
    return value;
}

const LinkedListIterator = struct {
    address: ?AddressInfo,
    file_backend: FileBackend,
    key_buf: *[max_key_len]u8,

    pub fn next(self: *LinkedListIterator) !?Item {
        if (self.address) |address| {
            const item = try self.file_backend.readItem(address, self.key_buf);
            if (address.linked_next_location) |next_location| {
                self.address = try self.file_backend.readAddress(next_location);
            } else {
                self.address = null;
            }
            return item;
        } else {
            return null;
        }
    }
};

fn linkedListIterator(self: FileBackend, address: AddressInfo, key_buf: *[max_key_len]u8) LinkedListIterator {
    return .{ .address = address, .file_backend = self, .key_buf = key_buf };
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

fn createFile(path: []const u8, options: std.fs.File.CreateFlags) !std.fs.File {
    if (std.fs.path.dirname(path)) |dirname| {
        std.fs.makeDirAbsolute(dirname) catch |err| {
            switch (err) {
                error.PathAlreadyExists => {},
                else => return err,
            }
        };
    }

    return std.fs.createFileAbsolute(path, options) catch |err| {
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
    return key.len <= address.max_key_len and value.len <= address.max_value_len;
}

fn validateKey(key: []const u8) !void {
    if (key.len > max_key_len) return error.JetKVKeyTooLong;
}

test "basic put/get" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("foo", "bar");
    try backend.put("baz", "qux");

    if (try backend.get(std.testing.allocator, "foo")) |foo| {
        defer std.testing.allocator.free(foo);
        try std.testing.expectEqualStrings("bar", foo);
    } else try std.testing.expect(false);

    if (try backend.get(std.testing.allocator, "baz")) |bar| {
        defer std.testing.allocator.free(bar);
        try std.testing.expectEqualStrings("qux", bar);
    } else try std.testing.expect(false);
}

test "fetchRemove" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("foo", "bar");
    try backend.put("baz", "qux");

    if (try backend.fetchRemove(std.testing.allocator, "foo")) |foo| {
        defer std.testing.allocator.free(foo);
        try std.testing.expectEqualStrings("bar", foo);
    } else try std.testing.expect(false);

    if (try backend.fetchRemove(std.testing.allocator, "baz")) |bar| {
        defer std.testing.allocator.free(bar);
        try std.testing.expectEqualStrings("qux", bar);
    } else try std.testing.expect(false);

    try std.testing.expect(try backend.get(std.testing.allocator, "foo") == null);
    try std.testing.expect(try backend.get(std.testing.allocator, "baz") == null);
}

test "fetchRemove collisions" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("foo", "bar");
    try backend.put("baz", "qux");

    if (try backend.fetchRemove(std.testing.allocator, "foo")) |foo| {
        defer std.testing.allocator.free(foo);
        try std.testing.expectEqualStrings("bar", foo);
    } else try std.testing.expect(false);

    if (try backend.fetchRemove(std.testing.allocator, "baz")) |bar| {
        defer std.testing.allocator.free(bar);
        try std.testing.expectEqualStrings("qux", bar);
    } else try std.testing.expect(false);

    try std.testing.expect(try backend.get(std.testing.allocator, "foo") == null);
    try std.testing.expect(try backend.get(std.testing.allocator, "baz") == null);
}

test "remove" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("foo", "bar");
    try backend.put("baz", "qux");
    try backend.put("quux", "corge");

    try backend.remove("foo");
    try backend.remove("baz");

    try std.testing.expect(try backend.get(std.testing.allocator, "foo") == null);
    try std.testing.expect(try backend.get(std.testing.allocator, "baz") == null);

    if (try backend.get(std.testing.allocator, "quux")) |value| {
        defer std.testing.allocator.free(value);
        try std.testing.expectEqualStrings("corge", value);
    }
}

test "overwriting equal length" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("foo", "bar");
    try backend.put("foo", "baz");

    if (try backend.get(std.testing.allocator, "foo")) |foo| {
        defer std.testing.allocator.free(foo);
        try std.testing.expectEqualStrings("baz", foo);
    } else try std.testing.expect(false);
}

test "overwriting lesser length" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("foo", "abcdefghijklmno");
    try backend.put("foo", "pqrs");

    if (try backend.get(std.testing.allocator, "foo")) |foo| {
        defer std.testing.allocator.free(foo);
        try std.testing.expectEqualStrings("pqrs", foo);
    } else try std.testing.expect(false);
}

test "overwriting mixed length, all lesser than initial" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("foo", "aaaaaaaaaaaa");

    const size_after_first_write = try backend.file.getEndPos();

    try backend.put("foo", "bbbbbbbb");
    try backend.put("foo", "cccc");
    try backend.put("foo", "ddd");
    try backend.put("foo", "eeeeeeeee");

    if (try backend.get(std.testing.allocator, "foo")) |foo| {
        defer std.testing.allocator.free(foo);
        try std.testing.expectEqualStrings("eeeeeeeee", foo);
    } else try std.testing.expect(false);

    try std.testing.expectEqual(size_after_first_write, try backend.file.getEndPos());
}

test "overwriting increasingly lesser length" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("foo", "aaaaaaaaaaaa");

    const size_after_first_write = try backend.file.getEndPos();

    try backend.put("foo", "bbbbbbbb");
    try backend.put("foo", "cccc");
    try backend.put("foo", "dddddd");

    if (try backend.get(std.testing.allocator, "foo")) |foo| {
        defer std.testing.allocator.free(foo);
        try std.testing.expectEqualStrings("dddddd", foo);
    } else try std.testing.expect(false);

    try std.testing.expectEqual(size_after_first_write, try backend.file.getEndPos());
}

test "overwriting mixed length, all within over-alloc bounds of initial" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("foo", "aaaaaaaaaaaa");

    const size_after_first_write = try backend.file.getEndPos();

    try backend.put("foo", "bbbbbbbb");
    try backend.put("foo", "cccc");
    try backend.put("foo", "ddd");
    try backend.put("foo", "eeeeeeeeeeeeeeeeeeee");

    if (try backend.get(std.testing.allocator, "foo")) |foo| {
        defer std.testing.allocator.free(foo);
        try std.testing.expectEqualStrings("eeeeeeeeeeeeeeeeeeee", foo);
    } else try std.testing.expect(false);

    try std.testing.expectEqual(size_after_first_write, try backend.file.getEndPos());
}

test "overwriting mixed length, all within over-alloc bounds of initial, longer strings" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("foo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

    const size_after_first_write = try backend.file.getEndPos();

    try backend.put("foo", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    try backend.put("foo", "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");
    try backend.put("foo", "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
    try backend.put("foo", "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

    if (try backend.get(std.testing.allocator, "foo")) |foo| {
        defer std.testing.allocator.free(foo);
        try std.testing.expectEqualStrings("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", foo);
    } else try std.testing.expect(false);

    try std.testing.expectEqual(size_after_first_write, header_len + backend.address_space_size + address_len + "foo".len + 1024);
}
test "collisions" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("foo", "baz");
    try backend.put("foo", "qux");
    try backend.put("bar", "quux");

    if (try backend.get(std.testing.allocator, "foo")) |foo| {
        defer std.testing.allocator.free(foo);
        try std.testing.expectEqualStrings("qux", foo);
    } else try std.testing.expect(false);

    if (try backend.get(std.testing.allocator, "bar")) |bar| {
        defer std.testing.allocator.free(bar);
        try std.testing.expectEqualStrings("quux", bar);
    } else try std.testing.expect(false);
}

test "many entries" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    @setEvalBranchQuota(2000);

    const keypairs = @import("../../tests/keypairs.zig").keypairs;

    inline for (keypairs) |keypair| {
        try backend.put(keypair[0], keypair[1]);
    }

    inline for (keypairs) |keypair| {
        if (try backend.get(std.testing.allocator, keypair[0])) |value| {
            defer std.testing.allocator.free(value);
            try std.testing.expectEqualStrings(keypair[1], value);
        } else {
            try std.testing.expect(false);
        }
    }
}

test "array append/pop" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    const array = &[_][]const u8{ "foo", "bar", "baz" };
    for (array) |value| try backend.append("array", value);

    const expected = &[_][]const u8{ "baz", "bar", "foo" };
    for (expected) |value| {
        const popped = (try backend.pop(std.testing.allocator, "array")).?;
        defer std.testing.allocator.free(popped);
        try std.testing.expectEqualStrings(value, popped);
    }
    try std.testing.expect(try backend.pop(std.testing.allocator, "array") == null);
}

test "array append/popFirst" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    const array = &[_][]const u8{ "bar", "baz", "qux", "quux", "corge", "grault", "garply", "waldo", "fred", "plugh", "xyzzy", "thud" };

    for (array) |value| try backend.append("array", value);

    for (array) |value| {
        const popFirstped = (try backend.popFirst(std.testing.allocator, "array")).?;
        defer std.testing.allocator.free(popFirstped);
        try std.testing.expectEqualStrings(value, popFirstped);
    }
    try std.testing.expect(try backend.popFirst(std.testing.allocator, "array") == null);
}

test "array prepend/pop" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    const array = &[_][]const u8{ "bar", "baz", "qux", "quux", "corge", "grault", "garply", "waldo", "fred", "plugh", "xyzzy", "thud" };

    for (array) |value| try backend.prepend("array", value);

    for (array) |value| {
        const popped = (try backend.pop(std.testing.allocator, "array")).?;
        defer std.testing.allocator.free(popped);
        try std.testing.expectEqualStrings(value, popped);
    }
    try std.testing.expect(try backend.popFirst(std.testing.allocator, "array") == null);
}

test "array prepend/popFirst" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    const array = &[_][]const u8{ "bar", "baz", "qux", "quux", "corge", "grault", "garply", "waldo", "fred", "plugh", "xyzzy", "thud" };

    for (array) |value| try backend.prepend("array", value);

    for (0..array.len) |index| {
        const popFirstped = (try backend.popFirst(std.testing.allocator, "array")).?;
        defer std.testing.allocator.free(popFirstped);
        try std.testing.expectEqualStrings(array[array.len - 1 - index], popFirstped);
    }
    try std.testing.expect(try backend.popFirst(std.testing.allocator, "array") == null);
}

test "array append-pop-append-pop-append-pop" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.append("array", "foo");
    try backend.append("array", "bar");
    {
        const popped = try backend.pop(std.testing.allocator, "array");
        defer std.testing.allocator.free(popped.?);
        try std.testing.expectEqualStrings("bar", popped.?);
    }
    try backend.append("array", "baz");
    try backend.append("array", "foo");
    {
        const popped = try backend.pop(std.testing.allocator, "array");
        defer std.testing.allocator.free(popped.?);
        try std.testing.expectEqualStrings("foo", popped.?);
    }
    {
        const popped = try backend.pop(std.testing.allocator, "array");
        defer std.testing.allocator.free(popped.?);
        try std.testing.expectEqualStrings("baz", popped.?);
    }
    {
        const popped = try backend.pop(std.testing.allocator, "array");
        defer std.testing.allocator.free(popped.?);
        try std.testing.expectEqualStrings("foo", popped.?);
    }
    try std.testing.expect(try backend.pop(std.testing.allocator, "array") == null);
}

test "array collision popFirst" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.append("array1", "foo");
    try backend.append("array1", "bar");
    try backend.append("array2", "baz");
    try backend.append("array2", "qux");

    {
        const popped = try backend.popFirst(std.testing.allocator, "array1");
        defer std.testing.allocator.free(popped.?);
        try std.testing.expectEqualStrings("foo", popped.?);
    }

    {
        const popped = try backend.popFirst(std.testing.allocator, "array2");
        defer std.testing.allocator.free(popped.?);
        try std.testing.expectEqualStrings("baz", popped.?);
    }

    {
        const popped = try backend.popFirst(std.testing.allocator, "array1");
        defer std.testing.allocator.free(popped.?);
        try std.testing.expectEqualStrings("bar", popped.?);
    }

    {
        const popped = try backend.popFirst(std.testing.allocator, "array2");
        defer std.testing.allocator.free(popped.?);
        try std.testing.expectEqualStrings("qux", popped.?);
    }
}

test "array collision pop" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.append("array1", "foo");
    try backend.append("array1", "bar");
    try backend.append("array2", "baz");
    try backend.append("array2", "qux");

    {
        const popped = try backend.pop(std.testing.allocator, "array1");
        defer std.testing.allocator.free(popped.?);
        try std.testing.expectEqualStrings("bar", popped.?);
    }

    {
        const popped = try backend.pop(std.testing.allocator, "array1");
        defer std.testing.allocator.free(popped.?);
        try std.testing.expectEqualStrings("foo", popped.?);
    }

    {
        const popped = try backend.pop(std.testing.allocator, "array2");
        defer std.testing.allocator.free(popped.?);
        try std.testing.expectEqualStrings("qux", popped.?);
    }

    {
        const popped = try backend.pop(std.testing.allocator, "array2");
        defer std.testing.allocator.free(popped.?);
        try std.testing.expectEqualStrings("baz", popped.?);
    }
}

test "many append and popFirst" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    const initial_size = try backend.file.getEndPos();

    @setEvalBranchQuota(2001);

    const keypairs = @import("../../tests/keypairs.zig").keypairs;

    inline for (keypairs) |keypair| {
        try backend.append("array", keypair[0]);
    }

    inline for (keypairs) |keypair| {
        const value = (try backend.popFirst(std.testing.allocator, "array")).?;
        defer std.testing.allocator.free(value);
        try std.testing.expectEqualStrings(keypair[0], value);
    }

    try std.testing.expectEqual(initial_size, try backend.file.getEndPos());
}

test "many prepend and popFirst" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    const initial_size = try backend.file.getEndPos();

    @setEvalBranchQuota(2001);

    const keypairs = @import("../../tests/keypairs.zig").keypairs;

    inline for (keypairs) |keypair| {
        try backend.prepend("array", keypair[0]);
    }

    inline for (0..keypairs.len) |index| {
        const value = (try backend.popFirst(std.testing.allocator, "array")).?;
        defer std.testing.allocator.free(value);
        try std.testing.expectEqualStrings(keypairs[keypairs.len - index - 1][0], value);
    }

    try std.testing.expectEqual(initial_size, try backend.file.getEndPos());
}

test "many append and pop" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    const initial_size = try backend.file.getEndPos();

    @setEvalBranchQuota(2001);

    const keypairs = @import("../../tests/keypairs.zig").keypairs;

    inline for (keypairs) |keypair| {
        try backend.append("array", keypair[0]);
    }

    inline for (0..keypairs.len) |index| {
        const value = (try backend.pop(std.testing.allocator, "array")).?;
        defer std.testing.allocator.free(value);
        try std.testing.expectEqualStrings(keypairs[keypairs.len - index - 1][0], value);
    }

    try std.testing.expectEqual(initial_size, try backend.file.getEndPos());
}

test "many prepend and pop" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    const initial_size = try backend.file.getEndPos();

    @setEvalBranchQuota(2001);

    const keypairs = @import("../../tests/keypairs.zig").keypairs;

    inline for (keypairs) |keypair| {
        try backend.prepend("array", keypair[0]);
    }

    inline for (keypairs) |keypair| {
        const value = (try backend.pop(std.testing.allocator, "array")).?;
        defer std.testing.allocator.free(value);
        try std.testing.expectEqualStrings(keypair[0], value);
    }

    try std.testing.expectEqual(initial_size, try backend.file.getEndPos());
}

test "put string, overwrite array" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("foo", "bar");
    try backend.append("foo", "baz");

    const popped = try backend.pop(std.testing.allocator, "foo");
    defer std.testing.allocator.free(popped.?);
    try std.testing.expectEqualStrings("baz", popped.?);
}

test "put string, overwrite array with collisions" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("foo", "baz");
    try backend.put("bar", "qux");
    try backend.append("bar", "quux");

    const popped = try backend.pop(std.testing.allocator, "bar");
    defer std.testing.allocator.free(popped.?);
    try std.testing.expectEqualStrings("quux", popped.?);
}

test "put string, pop array" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("foo", "baz");

    try std.testing.expect(try backend.pop(std.testing.allocator, "foo") == null);
}

test "bug: previous value returned for overwritten key" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    try backend.put("ka", "spam");
    try backend.put("1", "eggs");
    try backend.put("1", "jetkv");

    const value = try backend.get(std.testing.allocator, "1");
    defer std.testing.allocator.free(value.?);
    try std.testing.expectEqualStrings("jetkv", value.?);
}

test "bug: stale value returned for deleted key" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    try backend.put("fnIEV", "spam");
    try backend.put("7", "eggs");
    try backend.remove("7");

    const value = try backend.get(std.testing.allocator, "7");
    try std.testing.expect(value == null);
}

test "bug: pop collisions" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();
    try backend.put("foo", "bar");
    try backend.put("bar", "baz");
    try backend.append("qux", "quux");

    const value = try backend.pop(std.testing.allocator, "qux");
    defer std.testing.allocator.free(value.?);
    try std.testing.expectEqualStrings("quux", value.?);
}

test "bug: popFirst collisions" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();
    try backend.put("foo", "bar");
    try backend.put("bar", "baz");
    try backend.append("qux", "quux");

    const value = try backend.popFirst(std.testing.allocator, "qux");
    defer std.testing.allocator.free(value.?);
    try std.testing.expectEqualStrings("quux", value.?);
}

test "bug: linked string removal" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 64,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("rYpIf4P", "spam");
    try backend.put("MP", "spam");
    try backend.put("q", "spam");
    try backend.remove("MP");

    const value = try backend.get(std.testing.allocator, "q");
    defer std.testing.allocator.free(value.?);
    try std.testing.expectEqualStrings("spam", value.?);
}

test "bug: replace first string in linked list with insufficient space for overwrite" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("J", "spam");
    try backend.put("j", "spam");
    try backend.put("J", "LbTHW3x0R7xhTpJOdBRS0xxHHseFh8eGSZ7beYzpeJpAKYI20m_llAFJf5E9ChcnIBRlMvL3vAsSI0InrZ2jDsDdxLsgzmPL7fKTTvkQpDj4PUZ0ezUjFFgIopzoY4DpjdC6sEmp3xWZMWeyWLyvrnO1B6nMTAS97nPGVGIMIEg8ClQnbB8rLafIYLq4NRcWW1ovw9l7FiuiHk089gOTOi5fjgiflDSXUgiwRIZ6j4VQmbKgQ8PCqNn64N5s9u1_cminIG_4mvNz6K8mCsu_6xKXa4hobCTMOZnNcSo68QW1JyVfT8uaUWascE3LbVTyOOCAhs89K_y");

    const value = try backend.get(std.testing.allocator, "j");
    defer std.testing.allocator.free(value.?);
    try std.testing.expectEqualStrings("spam", value.?);
}

test "bug: overwrite linked string (from string) with array" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.append("mch4o", "spma");
    try backend.put("4", "spam");
    try backend.append("4", "spam");

    {
        const value = try backend.get(std.testing.allocator, "4");
        defer std.testing.allocator.free(value.?);
        try std.testing.expectEqualStrings("spam", value.?);
    }

    {
        const value = try backend.popFirst(std.testing.allocator, "mch4o");
        defer std.testing.allocator.free(value.?);
        try std.testing.expectEqualStrings("spma", value.?);
    }
}

test "bug: popFirst linked string" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.append("NfhsbHl", "spam");
    try backend.put("_", "spam");
    try std.testing.expect(try backend.popFirst(std.testing.allocator, "_") == null);
}

test "bug: append to empty linked array" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 23,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("BZWOy", "spam");
    try backend.append("", "spam");
    try backend.put("j6", "spam");

    const value = try backend.pop(std.testing.allocator, "");
    defer std.testing.allocator.free(value.?);

    try std.testing.expectEqualStrings("spam", value.?);
}

test "bug: popFirst from array linked from empty array" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("KrSA", "spam");
    try backend.append("Q", "spam");
    try backend.append("Q", "eggs");

    {
        const value = try backend.popFirst(std.testing.allocator, "Q");
        defer std.testing.allocator.free(value.?);
        try std.testing.expectEqualStrings("spam", value.?);
    }

    {
        const value = try backend.popFirst(std.testing.allocator, "Q");
        defer std.testing.allocator.free(value.?);
        try std.testing.expectEqualStrings("eggs", value.?);
    }
}

test "bug: overwrite linked string with array" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("V", "spam");
    try backend.put("n", "spam");
    try backend.append("V", "spam");

    const value = try backend.get(std.testing.allocator, "n");
    defer std.testing.allocator.free(value.?);

    try std.testing.expectEqualStrings("spam", value.?);
}

test "bug: append to empty indexed array" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.append("2", "spamspam");
    try backend.append("_P", "spam");

    {
        const value = try backend.popFirst(std.testing.allocator, "2");
        defer std.testing.allocator.free(value.?);
        try std.testing.expectEqualStrings("spamspam", value.?);
    }

    try backend.append("2", "eggs");

    {
        const value = try backend.get(std.testing.allocator, "2");
        defer std.testing.allocator.free(value.?);
        try std.testing.expectEqualStrings("eggs", value.?);
    }
}

test "bug: append to empty linked array after emptying" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("BCMB92d", "spam");
    try backend.append("3", "spamspam");
    {
        const value = try backend.popFirst(std.testing.allocator, "3");
        defer std.testing.allocator.free(value.?);
        try std.testing.expectEqualStrings("spamspam", value.?);
    }
    try backend.append("3", "eggs");

    {
        const value = try backend.popFirst(std.testing.allocator, "3");
        defer std.testing.allocator.free(value.?);
        try std.testing.expectEqualStrings("eggs", value.?);
    }
}

test "bug: popFirst with linked value (preserve link)" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 3,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.append("rw", "spam");
    try backend.put("m", "spam");
    try backend.put("0", "spam");
    try backend.append("m", "eggs");
    try backend.append("m", "boom");

    {
        const value = try backend.popFirst(std.testing.allocator, "m");
        defer std.testing.allocator.free(value.?);
        try std.testing.expectEqualStrings("eggs", value.?);
    }

    {
        const value = try backend.get(std.testing.allocator, "0");
        defer std.testing.allocator.free(value.?);
        try std.testing.expectEqualStrings("spam", value.?);
    }
}

test "bug: infinite loop on linked array pop" {
    var backend = try FileBackend.init(.{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 3,
        .truncate = true,
    });
    defer backend.deinit();

    try backend.put("8", "spam");
    try backend.append("r", "eggs");
    {
        const value = try backend.pop(std.testing.allocator, "r");
        defer std.testing.allocator.free(value.?);
        try std.testing.expectEqualStrings(
            "eggs",
            value.?,
        );
    }
    try backend.append("8", "spamandeggs");
    try std.testing.expect(try backend.pop(std.testing.allocator, "2e") == null);
}
