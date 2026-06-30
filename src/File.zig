const FileStore = @This();

file: File,
io: Io,
address_space: u32,
mutex: Mutex = .init,
address_space_size: u32,
/// Interface
store: Store = .{
    .vtable = &.{
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

pub const Config = struct {
    path: []const u8,
    address_space_size: u32 = addressSpace(4096),
    truncate: bool = false,
};

pub fn init(io: Io, comptime config: Config) !FileStore {
    if (config.address_space_size % bufSize(u32) != 0)
        return error.KVInvalidAddressSpaceSize;
    var backend: FileStore = .{
        .address_space = @divExact(config.address_space_size, bufSize(u32)),
        .address_space_size = config.address_space_size,
        .io = io,
        .file = try createFile(io, config.path, .{
            .read = true,
            .lock = .exclusive,
            .truncate = config.truncate,
        }),
    };
    try backend.initAddressSpace();
    try backend.initHeader();
    return backend;
}

pub fn deinit(self: *FileStore) void {
    self.file.close(self.io);
}

fn put(s: *Store, key: []const u8, value: []const u8) !void {
    try validateKey(key);
    const self: *FileStore = @fieldParentPtr("store", s);
    self.lockUncancelable();
    defer self.unlock();
    try self.putString(key, value);
    try self.sync();
}

fn get(s: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    try validateKey(key);
    const self: *FileStore = @fieldParentPtr("store", s);
    self.lockUncancelable();
    defer self.unlock();
    return try self.getString(allocator, key);
}

fn remove(s: *Store, key: []const u8) !void {
    try validateKey(key);
    const self: *FileStore = @fieldParentPtr("store", s);
    self.lockUncancelable();
    defer self.unlock();
    try self.removeString(key);
    try self.sync();
}

fn fetchRemove(s: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    try validateKey(key);
    const self: *FileStore = @fieldParentPtr("store", s);
    self.lockUncancelable();
    defer self.unlock();

    return if (try self.getString(allocator, key)) |capture| blk: {
        try self.removeString(key);
        try self.sync();
        break :blk capture;
    } else null;
}

pub fn lock(self: *FileStore) !void {
    self.mutex.lock(self.io);
}

pub fn unlock(self: *FileStore) void {
    self.mutex.unlock(self.io);
}

pub fn lockUncancelable(self: *FileStore) void {
    self.mutex.lockUncancelable(self.io);
}

fn prepend(s: *Store, key: []const u8, value: []const u8) !void {
    try validateKey(key);
    const self: *FileStore = @fieldParentPtr("store", s);
    self.lockUncancelable();
    defer self.unlock();
    const index = try self.locate(key);
    const address = try self.readIndexAddress(index) orelse {
        try self.createArray(index, null, key, value, .{ .linked = false });
        try self.incRefCount();
        try self.sync();
        return;
    };
    var key_buf: [max_key_len]u8 = undefined;
    const item = try self.readItem(address, &key_buf);

    if (!std.mem.eql(u8, item.key, key)) {
        // Collision
        try self.prependLinked(item, key, value, &key_buf);
        try self.sync();
        return;
    }
    if (address.type == .array and address.array_end_location != null) {
        // No collision
        const location = try self.prependItemToExistingArray(address, key, value);
        try self.updateLocation(index, location);
        try self.incRefCount();
    } else {
        // Overwrite string/re-use empty array
        try self.createArray(index, address, key, value, .{ .linked = false });
    }
    try self.sync();
}

fn pop(s: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    try validateKey(key);
    const self: *FileStore = @fieldParentPtr("store", s);
    self.lockUncancelable();
    defer self.unlock();

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

fn popFirst(s: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    try validateKey(key);
    const self: *FileStore = @fieldParentPtr("store", s);
    self.lockUncancelable();
    defer self.unlock();

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

fn append(s: *Store, key: []const u8, value: []const u8) !void {
    const self: *FileStore = @fieldParentPtr("store", s);
    self.lockUncancelable();
    defer self.unlock();

    const index = try self.locate(key);
    const address = try self.readIndexAddress(index) orelse {
        try self.createArray(index, null, key, value, .{ .linked = false });
        return try self.incRefCount();
    };
    var key_buf: [max_key_len]u8 = undefined;
    const item = try self.readItem(address, &key_buf);
    if (!std.mem.eql(u8, item.key, key)) {
        // Collision
        var it = self.linkedListIterator(item.address, &key_buf);
        var previous_item = item;

        while (try it.next()) |linked_item| {
            const is_equal_key = std.mem.eql(u8, linked_item.key, key);
            if (!is_equal_key) {
                previous_item = linked_item;
                continue;
            }
            if (linked_item.address.type == .array) {
                if (linked_item.address.array_end_location == null) try self.createArray(
                    previous_item.address.location,
                    previous_item.address,
                    key,
                    value,
                    .{ .linked = true },
                ) else try self.appendItemToExistingArray(
                    linked_item.address,
                    key,
                    value,
                );
                return try self.incRefCount();
            }
            if (linked_item.address.type == .string) {
                // Overwrite string value
                try self.updateAddress(previous_item.address.location, .{ .type = .array });
                return try self.createArray(
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
        try self.createArray(previous_item.address.location, null, key, value, .{ .linked = true });
        return try self.incRefCount();
    }

    if (address.type == .array and address.array_end_location != null) {
        // No collision
        try self.appendItemToExistingArray(address, key, value);
        return try self.incRefCount();
    }
    // Overwrite string/re-use empty array
    return try self.createArray(index, address, key, value, .{ .linked = false });
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
    file_backend: FileStore,

    pub fn value(self: Item, allocator: Allocator) ![]const u8 {
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

/// Insert a String to the start of an Array in the file-based backend.
/// Pop a String from the end of an Array in the file-based backend.
/// Pop a String from the start of an Array in the file-based backend.
fn popIndexed(
    self: FileStore,
    allocator: Allocator,
    item: Item,
    key_buf: *[max_key_len]u8,
) !?[]const u8 {
    if (item.address.type != .array) return null;

    const end_location = item.address.array_end_location orelse return null;

    const last_item_address = try self.readAddress(end_location) orelse
        return null;
    const last_item = try self.readItem(last_item_address, key_buf);
    const value = try last_item.value(allocator);
    try self.shrinkArray(item.address, last_item.address);
    try self.decRefCount();
    try self.maybeTruncate(last_item.address);
    return value;
}

fn popLinked(
    self: FileStore,
    allocator: Allocator,
    item: Item,
    key: []const u8,
    key_buf: *[max_key_len]u8,
) !?[]const u8 {
    var it = self.linkedListIterator(item.address, key_buf);
    var previous_item = item;

    while (try it.next()) |linked_item| {
        if (!std.mem.eql(u8, linked_item.key, key)) {
            previous_item = linked_item;
            continue;
        }
        if (linked_item.address.type != .array) return null;

        const end_location = linked_item.address.array_end_location orelse return null;

        const last_item_address = try self.readAddress(end_location) orelse
            return null;
        const last_item = try self.readItem(last_item_address, key_buf);
        const value = try last_item.value(allocator);
        try self.shrinkArray(linked_item.address, last_item.address);

        if (linked_item.address.location == end_location)
            try self.updateAddress(previous_item.address.location, .{
                .linked_next_location = .{
                    .value = linked_item.address.linked_next_location,
                },
            });

        try self.decRefCount();
        try self.maybeTruncate(last_item.address);
        return value;
    }
    return null;
}

fn popFirstLinked(
    self: FileStore,
    allocator: Allocator,
    item: Item,
    key: []const u8,
    key_buf: *[max_key_len]u8,
) !?[]const u8 {
    var it = self.linkedListIterator(item.address, key_buf);
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
            },
            .string => return null,
            // ValueType only has .string and .array
        }
    }
    return null;
}

fn popFirstIndexed(
    self: FileStore,
    allocator: Allocator,
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
    } else if (item.address.linked_next_location == null)
        try self.updateLocation(index, null)
    else
        try self.updateAddress(item.address.location, .{ .array_end_location = .none });
    const value = try item.value(allocator);
    try self.decRefCount();
    return value;
}

fn prependLinked(
    self: FileStore,
    item: Item,
    key: []const u8,
    value: []const u8,
    key_buf: *[max_key_len]u8,
) !void {
    var it = self.linkedListIterator(item.address, key_buf);
    var previous_item = item;

    while (try it.next()) |linked_item| {
        if (!std.mem.eql(u8, linked_item.key, key)) {
            previous_item = linked_item;
            continue;
        }
        switch (linked_item.address.type) {
            .array => {
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
            },
            .string => {
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
            },
            // ValueType only has .string and .array
        }
    }

    // No matches in linked list - create new array at EOF and link to final item in
    // linked list
    try self.createArray(
        previous_item.address.location,
        null,
        key,
        value,
        .{ .linked = true },
    );
    try self.incRefCount();
}

// Drop one item the end of an array and update end location.
fn shrinkArray(self: FileStore, first_item_address: AddressInfo, last_item_address: AddressInfo) !void {
    const previous_location = last_item_address.array_previous_location orelse {
        // We reached the first item
        return try self.updateAddress(first_item_address.location, .{ .array_end_location = .none });
    };
    // Nullify next item pointer for next-to-last item, update end location to
    // next-to-last item
    var location_buf: [bufSize(u32)]u8 = undefined;
    serialize(u32, 0, &location_buf);
    try self.file.writePositionalAll(self.io, &location_buf, @as(u64, previous_location) + array_next_location_offset);

    try self.updateAddress(
        first_item_address.location,
        .{
            .array_end_location = .{
                .value = last_item_address.array_previous_location.?,
            },
        },
    );
}

// Initialize address space with zeroes.
fn initAddressSpace(self: FileStore) !void {
    const zero_buf: [4096]u8 = std.mem.zeroes([4096]u8);
    var offset: u64 = header_len;
    const end: u64 = header_len + self.address_space_size;
    while (offset < end) {
        const chunk: usize = @min(zero_buf.len, end - offset);
        try self.file.writePositionalAll(self.io, zero_buf[0..chunk], offset);
        offset += chunk;
    }
}

fn initHeader(self: FileStore) !void {
    const header: Header = .{
        .address_space_size = self.address_space_size,
        .ref_count = 0,
    };
    var buf: [bufSize(Header)]u8 = undefined;
    serialize(Header, header, &buf);
    try self.file.writePositionalAll(self.io, &buf, 0);
}

fn incRefCount(self: FileStore) !void {
    var header = try self.readHeader();
    header.ref_count += 1;
    try self.writeHeader(header);
}

fn decRefCount(self: FileStore) !void {
    var header = try self.readHeader();
    if (header.ref_count == 0) unreachable;
    header.ref_count -= 1;
    try self.writeHeader(header);
    if (header.ref_count == 0) {
        try self.setEndPos(header_len + self.address_space_size);
        try self.initAddressSpace();
    }
}

fn writeHeader(self: FileStore, header: Header) !void {
    var header_buf: [bufSize(Header)]u8 = undefined;
    serialize(Header, header, &header_buf);
    try self.file.writePositionalAll(self.io, &header_buf, 0);
}

fn readHeader(self: FileStore) !Header {
    var header_buf: [bufSize(Header)]u8 = undefined;
    _ = try self.file.readPositionalAll(self.io, &header_buf, 0);
    return .{
        .address_space_size = std.mem.readInt(u32, header_buf[0..4], endian),
        .ref_count = std.mem.readInt(u32, header_buf[4..8], endian),
    };
}

fn getEndPos(self: FileStore) !u32 {
    return @intCast(try self.file.length(self.io));
}

fn setEndPos(self: FileStore, location: u32) !void {
    try self.file.setLength(self.io, location);
}

fn sync(self: FileStore) !void {
    try self.file.sync(self.io);
}

// Truncate the file if the given address + key/value reaches EOF.
fn maybeTruncate(self: FileStore, address: AddressInfo) !void {
    if (address.linked_next_location) |next_location|
        if (try self.readAddress(next_location)) |_|
            return; // We need to retain the link
    if (try self.isTerminatingAddress(address))
        try self.setEndPos(address.location);
}

fn isTerminatingAddress(self: FileStore, address: AddressInfo) !bool {
    const address_end = address.location +
        address_len +
        address.max_key_len + address.max_value_len;
    return address_end == try self.getEndPos();
}

fn getString(self: FileStore, allocator: Allocator, key: []const u8) !?[]const u8 {
    const index = try self.locate(key);
    const location = try self.readLocation(index) orelse
        return null;
    const address = try self.readAddress(location) orelse
        return null;

    var key_buf: [max_key_len]u8 = undefined;
    const item = try self.readItem(address, &key_buf);
    if (std.mem.eql(u8, item.key, key))
        return try item.value(allocator);

    var it = self.linkedListIterator(item.address, &key_buf);

    while (try it.next()) |linked_item|
        if (std.mem.eql(u8, linked_item.key, key))
            return try linked_item.value(allocator);
    return null;
}

fn putString(self: FileStore, key: []const u8, value: []const u8) !void {
    const index = try self.locate(key);
    const address = try self.readIndexAddress(index) orelse {
        try self.writeString(index, null, key, value);
        return try self.incRefCount();
    };
    var key_buf: [max_key_len]u8 = undefined;
    const item = try self.readItem(address, &key_buf);
    if (!std.mem.eql(u8, item.key, key))
        return try self.writeLinkedString(item, key, value);
    if (key.len <= item.address.max_key_len and value.len <= item.address.max_value_len)
        return try self.updateString(item, key, value);
    try self.writeString(index, item.address, key, value);
}

fn removeString(self: FileStore, key: []const u8) !void {
    const index = try self.locate(key);
    const address = try self.readIndexAddress(index) orelse return;
    var key_buf: [max_key_len]u8 = undefined;
    const item = try self.readItem(address, &key_buf);
    if (!std.mem.eql(u8, item.key, key))
        return try self.removeLinkedString(item, key);
    try self.updateLocation(index, address.linked_next_location); // can be null
    try self.decRefCount();
}

/// Append a String to the end of an Array in the file-based backend.
pub const CreateArrayOptions = struct {
    linked: bool,
};

fn createArray(
    self: FileStore,
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

    const base: u64 = end_pos;
    try self.file.writePositionalAll(self.io, &address_buf, base);
    try self.file.writePositionalAll(self.io, key, base + address_len);
    try self.file.writePositionalAll(self.io, value, base + address_len + key.len);
}

fn appendItemToExistingArray(
    self: FileStore,
    address: AddressInfo,
    key: []const u8,
    value: []const u8,
) !void {
    var address_buf: [address_len]u8 = undefined;
    const end_pos = try self.getEndPos();

    // Update next item pointer for previous item
    try self.updateAddress(
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
    try self.file.writePositionalAll(self.io, &address_buf, base);
    try self.file.writePositionalAll(self.io, key, base + address_len);
    try self.file.writePositionalAll(self.io, value, base + address_len + key.len);

    try self.updateAddress(address.location, .{ .array_end_location = .{ .value = end_pos } });
}

fn prependItemToExistingArray(
    self: FileStore,
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
    const base: u64 = end_pos;
    try self.file.writePositionalAll(self.io, &address_buf, base);
    try self.file.writePositionalAll(self.io, key, base + address_len);
    try self.file.writePositionalAll(self.io, value, base + address_len + key.len);

    return end_pos;
}

fn locate(self: FileStore, key: []const u8) !u32 {
    const value = try std.math.mod(
        u32,
        hash(key),
        self.address_space,
    ) * bufSize(u32);
    return header_len + value;
}

// Fetch a location from the index, i.e. a pointer to an address.
fn readLocation(self: FileStore, index: u32) !?u32 {
    var location_buf: [bufSize(u32)]u8 = undefined;
    _ = try self.file.readPositionalAll(self.io, &location_buf, index);
    const location = std.mem.readInt(u32, &location_buf, endian);
    return if (location == 0) null else location;
}

// Update a location pointer, either in the main index or as an address's next item pointer.
fn updateLocation(self: FileStore, index: u32, location: ?u32) !void {
    var location_buf: [bufSize(u32)]u8 = undefined;
    serialize(u32, location orelse 0, &location_buf);
    try self.file.writePositionalAll(self.io, &location_buf, index);
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

fn updateAddress(self: FileStore, location: u32, options: AddressUpdateOptions) !void {
    var buf: [4]u8 = undefined;

    if (options.type) |value_type| {
        serialize(ValueType, value_type, buf[0..1]);
        try self.file.writePositionalAll(self.io, buf[0..1], location);
    }

    if (options.linked_next_location) |linked_next_location| {
        serialize(AddressUpdateOptions.AddressUpdateLocationValue, linked_next_location, buf[0..4]);
        try self.file.writePositionalAll(self.io, buf[0..4], @as(u64, location) + linked_next_location_offset);
    }

    if (options.array_next_location) |array_next_location| {
        serialize(AddressUpdateOptions.AddressUpdateLocationValue, array_next_location, buf[0..4]);
        try self.file.writePositionalAll(self.io, buf[0..4], @as(u64, location) + array_next_location_offset);
    }

    if (options.array_previous_location) |array_previous_location| {
        serialize(AddressUpdateOptions.AddressUpdateLocationValue, array_previous_location, buf[0..4]);
        try self.file.writePositionalAll(self.io, buf[0..4], @as(u64, location) + array_previous_location_offset);
    }

    if (options.array_end_location) |array_end_location| {
        serialize(AddressUpdateOptions.AddressUpdateLocationValue, array_end_location, buf[0..4]);
        try self.file.writePositionalAll(self.io, buf[0..4], @as(u64, location) + array_end_location_offset);
    }
}

fn readIndexAddress(self: FileStore, index: u32) !?AddressInfo {
    const location = try self.readLocation(index) orelse return null;
    return try self.readAddress(location);
}

fn readAddress(self: FileStore, location: u32) !?AddressInfo {
    // TODO: Save a few bytes by using different address formats for strings and arrays
    var buf: [address_len]u8 = undefined;
    const n = try self.file.readPositionalAll(self.io, &buf, location);
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
    self: FileStore,
    index: u32,
    maybe_address_info: ?AddressInfo,
    key: []const u8,
    value: []const u8,
) !void {
    const end_pos = try self.getEndPos();

    var location_buf: [bufSize(u32)]u8 = undefined;
    serialize(u32, end_pos, &location_buf);
    try self.file.writePositionalAll(self.io, &location_buf, index);

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
    try self.file.writePositionalAll(self.io, &address_buf, base);
    try self.file.writePositionalAll(self.io, key, base + address_len);
    try self.file.writePositionalAll(self.io, value, base + address_len + key.len);

    const pad_size: usize = max_value_len - value.len;
    if (pad_size > 0) {
        const zero_buf: [8192]u8 = std.mem.zeroes([8192]u8);
        try self.file.writePositionalAll(self.io, zero_buf[0..pad_size], base + address_len + key.len + value.len);
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
fn updateString(self: FileStore, item: Item, key: []const u8, value: []const u8) !void {
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
    try self.file.writePositionalAll(self.io, &address_buf, base);
    try self.file.writePositionalAll(self.io, key, base + address_len);
    try self.file.writePositionalAll(self.io, value, base + address_len + key.len);
}

// Follow links until end, then either:
// * Update an existing link if the key matches
// * Append to EOF if no link has the given key, updating the last link's next item pointer
fn writeLinkedString(self: FileStore, item: Item, key: []const u8, value: []const u8) !void {
    var key_buf: [max_key_len]u8 = undefined;
    var it = self.linkedListIterator(item.address, &key_buf);
    var previous_item = item;

    while (try it.next()) |linked_item| {
        if (!std.mem.eql(u8, linked_item.key, key)) {
            previous_item = linked_item;
            continue;
        }
        return try self.updateLinkedString(
            previous_item.address,
            linked_item.address,
            key,
            value,
        );
    }

    // Write string using the final item's next item pointer as index
    try self.writeString(
        previous_item.address.location + linked_next_location_offset,
        null,
        key,
        value,
    );
    try self.incRefCount();
}

fn removeLinkedString(self: FileStore, item: Item, key: []const u8) !void {
    var key_buf: [max_key_len]u8 = undefined;
    var it = self.linkedListIterator(item.address, &key_buf);
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
            previous_item.address.location,
            .{ .linked_next_location = linked_next_location },
        );
        return try self.decRefCount();
    }
}

fn updateLinkedString(
    self: FileStore,
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

    if (!is_overwrite) try self.updateAddress(
        previous_address.location,
        .{ .linked_next_location = .{ .value = end_pos } },
    );

    const base: u64 = end_pos;
    try self.file.writePositionalAll(self.io, &new_address_buf, base);
    try self.file.writePositionalAll(self.io, key, base + address_len);
    try self.file.writePositionalAll(self.io, value, base + address_len + key.len);
}

fn readItem(self: FileStore, address: AddressInfo, key_buf: *[max_key_len]u8) !Item {
    _ = try self.file.readPositionalAll(self.io, key_buf[0..address.key_len], @as(u64, address.location) + address_len);

    return .{
        .file_backend = self,
        .address = address,
        .key = key_buf[0..address.key_len],
    };
}

fn readValue(self: FileStore, allocator: Allocator, address: AddressInfo) ![]const u8 {
    const value = try allocator.alloc(u8, @intCast(address.value_len));
    _ = try self.file.readPositionalAll(self.io, value, @as(u64, address.location) + address_len + address.key_len);
    return value;
}

const LinkedListIterator = struct {
    address: ?AddressInfo,
    file_backend: FileStore,
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

fn linkedListIterator(self: FileStore, address: AddressInfo, key_buf: *[max_key_len]u8) LinkedListIterator {
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

fn createFile(io: Io, path: []const u8, options: File.CreateFlags) !File {
    if (std.fs.path.dirname(path)) |dirname| {
        Io.Dir.createDirAbsolute(io, dirname, .default_dir) catch |err| {
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
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("foo", "bar");
    try store.put("baz", "qux");

    if (try store.get(t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("bar", foo);
    } else try t.expect(false);

    if (try store.get(t.allocator, "baz")) |bar| {
        defer t.allocator.free(bar);
        try t.expectEqualStrings("qux", bar);
    } else try t.expect(false);
}

test "fetchRemove" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("foo", "bar");
    try store.put("baz", "qux");

    if (try store.fetchRemove(t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("bar", foo);
    } else try t.expect(false);

    if (try store.fetchRemove(t.allocator, "baz")) |bar| {
        defer t.allocator.free(bar);
        try t.expectEqualStrings("qux", bar);
    } else try t.expect(false);

    try t.expect(try store.get(t.allocator, "foo") == null);
    try t.expect(try store.get(t.allocator, "baz") == null);
}

test "fetchRemove collisions" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("foo", "bar");
    try store.put("baz", "qux");

    if (try store.fetchRemove(t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("bar", foo);
    } else try t.expect(false);

    if (try store.fetchRemove(t.allocator, "baz")) |bar| {
        defer t.allocator.free(bar);
        try t.expectEqualStrings("qux", bar);
    } else try t.expect(false);

    try t.expect(try store.get(t.allocator, "foo") == null);
    try t.expect(try store.get(t.allocator, "baz") == null);
}

test "remove" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("foo", "bar");
    try store.put("baz", "qux");
    try store.put("quux", "corge");

    try store.remove("foo");
    try store.remove("baz");

    try t.expect(try store.get(t.allocator, "foo") == null);
    try t.expect(try store.get(t.allocator, "baz") == null);

    if (try store.get(t.allocator, "quux")) |value| {
        defer t.allocator.free(value);
        try t.expectEqualStrings("corge", value);
    }
}

test "overwriting equal length" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("foo", "bar");
    try store.put("foo", "baz");

    if (try store.get(t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("baz", foo);
    } else try t.expect(false);
}

test "overwriting lesser length" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("foo", "abcdefghijklmno");
    try store.put("foo", "pqrs");

    if (try store.get(t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("pqrs", foo);
    } else try t.expect(false);
}

test "overwriting mixed length, all lesser than initial" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("foo", "aaaaaaaaaaaa");

    const size_after_first_write = try backend.file.length(t.io);

    try store.put("foo", "bbbbbbbb");
    try store.put("foo", "cccc");
    try store.put("foo", "ddd");
    try store.put("foo", "eeeeeeeee");

    if (try store.get(t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("eeeeeeeee", foo);
    } else try t.expect(false);

    try t.expectEqual(size_after_first_write, try backend.file.length(t.io));
}

test "overwriting increasingly lesser length" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("foo", "aaaaaaaaaaaa");

    const size_after_first_write = try backend.file.length(t.io);

    try store.put("foo", "bbbbbbbb");
    try store.put("foo", "cccc");
    try store.put("foo", "dddddd");

    if (try store.get(t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("dddddd", foo);
    } else try t.expect(false);

    try t.expectEqual(size_after_first_write, try backend.file.length(t.io));
}

test "overwriting mixed length, all within over-alloc bounds of initial" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("foo", "aaaaaaaaaaaa");

    const size_after_first_write = try backend.file.length(t.io);

    try store.put("foo", "bbbbbbbb");
    try store.put("foo", "cccc");
    try store.put("foo", "ddd");
    try store.put("foo", "eeeeeeeeeeeeeeeeeeee");

    if (try store.get(t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("eeeeeeeeeeeeeeeeeeee", foo);
    } else try t.expect(false);

    try t.expectEqual(size_after_first_write, try backend.file.length(t.io));
}

test "overwriting mixed length, all within over-alloc bounds of initial, longer strings" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("foo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

    const size_after_first_write = try backend.file.length(t.io);

    try store.put("foo", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    try store.put("foo", "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");
    try store.put("foo", "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
    try store.put("foo", "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

    if (try store.get(t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", foo);
    } else try t.expect(false);

    try t.expectEqual(size_after_first_write, header_len + backend.address_space_size + address_len + "foo".len + 1024);
}
test "collisions" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("foo", "baz");
    try store.put("foo", "qux");
    try store.put("bar", "quux");

    if (try store.get(t.allocator, "foo")) |foo| {
        defer t.allocator.free(foo);
        try t.expectEqualStrings("qux", foo);
    } else try t.expect(false);

    if (try store.get(t.allocator, "bar")) |bar| {
        defer t.allocator.free(bar);
        try t.expectEqualStrings("quux", bar);
    } else try t.expect(false);
}

test "many entries" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    @setEvalBranchQuota(10000);

    inline for (keypairs) |keypair| {
        try store.put(keypair[0], keypair[1]);
    }

    inline for (keypairs) |keypair| {
        if (try store.get(t.allocator, keypair[0])) |value| {
            defer t.allocator.free(value);
            try t.expectEqualStrings(keypair[1], value);
        } else {
            try t.expect(false);
        }
    }
}

test "array append/pop" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    const array = &[_][]const u8{ "foo", "bar", "baz" };
    for (array) |value| try store.append("array", value);

    const expected = &[_][]const u8{ "baz", "bar", "foo" };
    for (expected) |value| {
        const popped = (try store.pop(t.allocator, "array")).?;
        defer t.allocator.free(popped);
        try t.expectEqualStrings(value, popped);
    }
    try t.expect(try store.pop(t.allocator, "array") == null);
}

test "array append/popFirst" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    const array = &[_][]const u8{ "bar", "baz", "qux", "quux", "corge", "grault", "garply", "waldo", "fred", "plugh", "xyzzy", "thud" };

    for (array) |value| try store.append("array", value);

    for (array) |value| {
        const popFirstped = (try store.popFirst(t.allocator, "array")).?;
        defer t.allocator.free(popFirstped);
        try t.expectEqualStrings(value, popFirstped);
    }
    try t.expect(try store.popFirst(t.allocator, "array") == null);
}

test "array prepend/pop" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    const array = &[_][]const u8{ "bar", "baz", "qux", "quux", "corge", "grault", "garply", "waldo", "fred", "plugh", "xyzzy", "thud" };

    for (array) |value| try store.prepend("array", value);

    for (array) |value| {
        const popped = (try store.pop(t.allocator, "array")).?;
        defer t.allocator.free(popped);
        try t.expectEqualStrings(value, popped);
    }
    try t.expect(try store.popFirst(t.allocator, "array") == null);
}

test "array prepend/popFirst" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    const array = &[_][]const u8{ "bar", "baz", "qux", "quux", "corge", "grault", "garply", "waldo", "fred", "plugh", "xyzzy", "thud" };

    for (array) |value| try store.prepend("array", value);

    for (0..array.len) |index| {
        const popFirstped = (try store.popFirst(t.allocator, "array")).?;
        defer t.allocator.free(popFirstped);
        try t.expectEqualStrings(array[array.len - 1 - index], popFirstped);
    }
    try t.expect(try store.popFirst(t.allocator, "array") == null);
}

test "array append-pop-append-pop-append-pop" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.append("array", "foo");
    try store.append("array", "bar");
    {
        const popped = try store.pop(t.allocator, "array");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("bar", popped.?);
    }
    try store.append("array", "baz");
    try store.append("array", "foo");
    {
        const popped = try store.pop(t.allocator, "array");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("foo", popped.?);
    }
    {
        const popped = try store.pop(t.allocator, "array");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("baz", popped.?);
    }
    {
        const popped = try store.pop(t.allocator, "array");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("foo", popped.?);
    }
    try t.expect(try store.pop(t.allocator, "array") == null);
}

test "array collision popFirst" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.append("array1", "foo");
    try store.append("array1", "bar");
    try store.append("array2", "baz");
    try store.append("array2", "qux");

    {
        const popped = try store.popFirst(t.allocator, "array1");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("foo", popped.?);
    }

    {
        const popped = try store.popFirst(t.allocator, "array2");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("baz", popped.?);
    }

    {
        const popped = try store.popFirst(t.allocator, "array1");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("bar", popped.?);
    }

    {
        const popped = try store.popFirst(t.allocator, "array2");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("qux", popped.?);
    }
}

test "array collision pop" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.append("array1", "foo");
    try store.append("array1", "bar");
    try store.append("array2", "baz");
    try store.append("array2", "qux");

    {
        const popped = try store.pop(t.allocator, "array1");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("bar", popped.?);
    }

    {
        const popped = try store.pop(t.allocator, "array1");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("foo", popped.?);
    }

    {
        const popped = try store.pop(t.allocator, "array2");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("qux", popped.?);
    }

    {
        const popped = try store.pop(t.allocator, "array2");
        defer t.allocator.free(popped.?);
        try t.expectEqualStrings("baz", popped.?);
    }
}

test "many append and popFirst" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    const initial_size = try backend.file.length(t.io);

    @setEvalBranchQuota(10000);

    inline for (keypairs) |keypair| {
        try store.append("array", keypair[0]);
    }

    inline for (keypairs) |keypair| {
        const value = (try store.popFirst(t.allocator, "array")).?;
        defer t.allocator.free(value);
        try t.expectEqualStrings(keypair[0], value);
    }

    try t.expectEqual(initial_size, try backend.file.length(t.io));
}

test "many prepend and popFirst" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    const initial_size = try backend.file.length(t.io);

    @setEvalBranchQuota(10000);

    inline for (keypairs) |keypair| {
        try store.prepend("array", keypair[0]);
    }

    inline for (0..keypairs.len) |index| {
        const value = (try store.popFirst(t.allocator, "array")).?;
        defer t.allocator.free(value);
        try t.expectEqualStrings(keypairs[keypairs.len - index - 1][0], value);
    }

    try t.expectEqual(initial_size, try backend.file.length(t.io));
}

test "many append and pop" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    const initial_size = try backend.file.length(t.io);

    @setEvalBranchQuota(10000);

    inline for (keypairs) |keypair| {
        try store.append("array", keypair[0]);
    }

    inline for (0..keypairs.len) |index| {
        const value = (try store.pop(t.allocator, "array")).?;
        defer t.allocator.free(value);
        try t.expectEqualStrings(keypairs[keypairs.len - index - 1][0], value);
    }

    try t.expectEqual(initial_size, try backend.file.length(t.io));
}

test "many prepend and pop" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    const initial_size = try backend.file.length(t.io);

    @setEvalBranchQuota(10000);

    inline for (keypairs) |keypair| {
        try store.prepend("array", keypair[0]);
    }

    inline for (keypairs) |keypair| {
        const value = (try store.pop(t.allocator, "array")).?;
        defer t.allocator.free(value);
        try t.expectEqualStrings(keypair[0], value);
    }

    try t.expectEqual(initial_size, try backend.file.length(t.io));
}

test "put string, overwrite array" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("foo", "bar");
    try store.append("foo", "baz");

    const popped = try store.pop(t.allocator, "foo");
    defer t.allocator.free(popped.?);
    try t.expectEqualStrings("baz", popped.?);
}

test "put string, overwrite array with collisions" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("foo", "baz");
    try store.put("bar", "qux");
    try store.append("bar", "quux");

    const popped = try store.pop(t.allocator, "bar");
    defer t.allocator.free(popped.?);
    try t.expectEqualStrings("quux", popped.?);
}

test "put string, pop array" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("foo", "baz");

    try t.expect(try store.pop(t.allocator, "foo") == null);
}

test "bug: previous value returned for overwritten key" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;
    try store.put("ka", "spam");
    try store.put("1", "eggs");
    try store.put("1", "jetkv");

    const value = try store.get(t.allocator, "1");
    defer t.allocator.free(value.?);
    try t.expectEqualStrings("jetkv", value.?);
}

test "bug: stale value returned for deleted key" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;
    try store.put("fnIEV", "spam");
    try store.put("7", "eggs");
    try store.remove("7");

    const value = try store.get(t.allocator, "7");
    try t.expect(value == null);
}

test "bug: pop collisions" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;
    try store.put("foo", "bar");
    try store.put("bar", "baz");
    try store.append("qux", "quux");

    const value = try store.pop(t.allocator, "qux");
    defer t.allocator.free(value.?);
    try t.expectEqualStrings("quux", value.?);
}

test "bug: popFirst collisions" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;
    try store.put("foo", "bar");
    try store.put("bar", "baz");
    try store.append("qux", "quux");

    const value = try store.popFirst(t.allocator, "qux");
    defer t.allocator.free(value.?);
    try t.expectEqualStrings("quux", value.?);
}

test "bug: linked string removal" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 64,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("rYpIf4P", "spam");
    try store.put("MP", "spam");
    try store.put("q", "spam");
    try store.remove("MP");

    const value = try store.get(t.allocator, "q");
    defer t.allocator.free(value.?);
    try t.expectEqualStrings("spam", value.?);
}

test "bug: replace first string in linked list with insufficient space for overwrite" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("J", "spam");
    try store.put("j", "spam");
    try store.put("J", "LbTHW3x0R7xhTpJOdBRS0xxHHseFh8eGSZ7beYzpeJpAKYI20m_llAFJf5E9ChcnIBRlMvL3vAsSI0InrZ2jDsDdxLsgzmPL7fKTTvkQpDj4PUZ0ezUjFFgIopzoY4DpjdC6sEmp3xWZMWeyWLyvrnO1B6nMTAS97nPGVGIMIEg8ClQnbB8rLafIYLq4NRcWW1ovw9l7FiuiHk089gOTOi5fjgiflDSXUgiwRIZ6j4VQmbKgQ8PCqNn64N5s9u1_cminIG_4mvNz6K8mCsu_6xKXa4hobCTMOZnNcSo68QW1JyVfT8uaUWascE3LbVTyOOCAhs89K_y");

    const value = try store.get(t.allocator, "j");
    defer t.allocator.free(value.?);
    try t.expectEqualStrings("spam", value.?);
}

test "bug: overwrite linked string (from string) with array" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.append("mch4o", "spma");
    try store.put("4", "spam");
    try store.append("4", "spam");

    {
        const value = try store.get(t.allocator, "4");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("spam", value.?);
    }

    {
        const value = try store.popFirst(t.allocator, "mch4o");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("spma", value.?);
    }
}

test "bug: popFirst linked string" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.append("NfhsbHl", "spam");
    try store.put("_", "spam");
    try t.expect(try store.popFirst(t.allocator, "_") == null);
}

test "bug: append to empty linked array" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 23,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("BZWOy", "spam");
    try store.append("", "spam");
    try store.put("j6", "spam");

    const value = try store.pop(t.allocator, "");
    defer t.allocator.free(value.?);

    try t.expectEqualStrings("spam", value.?);
}

test "bug: popFirst from array linked from empty array" {
    var backend: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer backend.deinit();
    const store = &backend.store;

    try store.put("KrSA", "spam");
    try store.append("Q", "spam");
    try store.append("Q", "eggs");

    {
        const value = try store.popFirst(t.allocator, "Q");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("spam", value.?);
    }

    {
        const value = try store.popFirst(t.allocator, "Q");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("eggs", value.?);
    }
}

test "bug: overwrite linked string with array" {
    var file: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer file.deinit();
    try file.store.put("V", "spam");
    try file.store.put("n", "spam");
    try file.store.append("V", "spam");

    const value = try file.store.get(t.allocator, "n");
    defer t.allocator.free(value.?);

    try t.expectEqualStrings("spam", value.?);
}

test "append to empty indexed array" {
    var file: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer file.deinit();
    try file.store.append("2", "spamspam");
    try file.store.append("_P", "spam");
    {
        const value = try file.store.popFirst(t.allocator, "2");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("spamspam", value.?);
    }
    try file.store.append("2", "eggs");
    {
        const value = try file.store.get(t.allocator, "2");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("eggs", value.?);
    }
}

test "append to empty linked array after emptying" {
    var file: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 1024,
        .truncate = true,
    });
    defer file.deinit();
    try file.store.put("BCMB92d", "spam");
    try file.store.append("3", "spamspam");
    {
        const value = try file.store.popFirst(t.allocator, "3");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("spamspam", value.?);
    }
    try file.store.append("3", "eggs");
    {
        const value = try file.store.popFirst(t.allocator, "3");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("eggs", value.?);
    }
}

test "bug: popFirst with linked value (preserve link)" {
    var file: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 3,
        .truncate = true,
    });
    defer file.deinit();

    try file.store.append("rw", "spam");
    try file.store.put("m", "spam");
    try file.store.put("0", "spam");
    try file.store.append("m", "eggs");
    try file.store.append("m", "boom");

    {
        const value = try file.store.popFirst(t.allocator, "m");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("eggs", value.?);
    }

    {
        const value = try file.store.get(t.allocator, "0");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings("spam", value.?);
    }
}

test "bug: infinite loop on linked array pop" {
    var file: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .address_space_size = bufSize(u32) * 3,
        .truncate = true,
    });
    defer file.deinit();

    try file.store.put("8", "spam");
    try file.store.append("r", "eggs");
    {
        const value = try file.store.pop(t.allocator, "r");
        defer t.allocator.free(value.?);
        try t.expectEqualStrings(
            "eggs",
            value.?,
        );
    }
    try file.store.append("8", "spamandeggs");
    try t.expect(try file.store.pop(t.allocator, "2e") == null);
}

test "file-based storage" {
    var file: FileStore = try .init(t.io, .{
        .path = "/tmp/jetkv.db",
        .truncate = true,
    });
    defer file.deinit();

    try file.store.put("foo", "bar");

    const foo = try file.store.get(t.allocator, "foo") orelse
        return t.expect(false);
    defer t.allocator.free(foo);
    try t.expectEqualStrings("bar", foo);

    try t.expect(try file.store.get(t.allocator, "baz") == null);
}

const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;
const File = Io.File;
const Mutex = Io.Mutex;
const Allocator = std.mem.Allocator;
const Store = @import("Store.zig");
const keypairs = @import("keypairs.zig").keypairs;
