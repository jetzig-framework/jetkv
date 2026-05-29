const Valkey = @This();

const builtin = @import("builtin");
const std = @import("std");
const Io = std.Io;
const Mutex = Io.Mutex;
const Writer = Io.Writer;
const Condition = Io.Condition;
const Stream = Io.net.Stream;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const jetkv = @import("../root.zig");
const Backend = jetkv.Backend;

connect_mode: ConnectMode = .auto,
host: []const u8 = "127.0.0.1",
port: u16 = 6379,
pool_size: u16 = 8,
buffer_size: u32 = 4096,
connect_timeout: u64 = 1 * std.time.ns_per_s,
read_timeout: u64 = 1 * std.time.ns_per_s,
/// Managed internally
internal: Internal = undefined,
/// Backend interface
interface: Backend = .{
    .vtable = &.{
        .deinit = deinit,
        .get = get,
        .put = put,
        .putExpire = putExpire,
        .fetchRemove = fetchRemove,
        .remove = remove,
        .append = append,
        .prepend = prepend,
        .pop = pop,
        .popFirst = popFirst,
    },
},

pub fn init(comptime config: Valkey, io: Io, allocator: Allocator) !Valkey {
    const connections = try allocator.alloc(*Pool.Connection, config.pool_size);
    var connections_created: usize = 0;
    errdefer {
        for (connections[0..connections_created]) |conn| allocator.destroy(conn);
        allocator.free(connections);
    }

    for (0..config.pool_size) |index| {
        connections[index] = try allocator.create(Pool.Connection);
        connections_created += 1;
        connections[index].* = .{
            .host = config.host,
            .port = config.port,
            .index = index,
        };
    }
    const available = try allocator.alloc(bool, config.pool_size);
    errdefer allocator.free(available);
    @memset(available, true);

    const pool = try allocator.create(Pool);
    pool.* = Pool{
        .connections = connections,
        .pool_size = config.pool_size,
        .available = available,
    };
    errdefer {
        pool.deinit(io);
        allocator.destroy(pool);
    }
    var vk: Valkey = .{
        .buffer_size = config.buffer_size,
        .connect_mode = config.connect_mode,
        .connect_timeout = config.connect_timeout,
        .host = config.host,
        .pool_size = config.pool_size,
        .port = config.port,
        .read_timeout = config.read_timeout,
        .internal = .{
            .pool = pool,
        },
    };
    if (config.connect_mode == .auto) try vk.connect(io, allocator);
    return vk;
}

fn deinit(b: *Backend, io: Io, allocator: Allocator) void {
    const self: *Valkey = @fieldParentPtr("interface", b);
    self.internal.pool.deinit(io);
    for (self.internal.pool.connections) |connection|
        allocator.destroy(connection);
    allocator.free(self.internal.pool.available);
    allocator.free(self.internal.pool.connections);
    allocator.destroy(self.internal.pool);
}

fn get(b: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    const self: *Valkey = @fieldParentPtr("interface", b);
    const response = try self.execute(io, allocator, .get, .{key});
    std.debug.assert(response == .null or response == .string);
    return switch (response) {
        .string => |string| string.value,
        .null => null,
        else => error.ValkeyError,
    };
}

fn put(b: *Backend, io: Io, allocator: Allocator, key: []const u8, value: []const u8) !void {
    const self: *Valkey = @fieldParentPtr("interface", b);
    const response = try self.execute(io, allocator, .set, .{ key, value });
    std.debug.assert(response == .ok);
    if (response == .err) return debugError(response);
}

fn putExpire(b: *Backend, io: Io, allocator: Allocator, key: []const u8, value: []const u8, expiration: i32) !void {
    const self: *Valkey = @fieldParentPtr("interface", b);
    // TODO: pipeline
    const set_response = try self.execute(io, allocator, .set, .{ key, value });
    std.debug.assert(set_response == .ok);
    if (set_response == .err) return debugError(set_response);

    // Valkey expects a string as expiration time ? This is not clear from the docs:
    // https://valkey.io/commands/expire/
    var buf: [16]u8 = undefined;
    const expiration_formatted = try std.fmt.bufPrint(&buf, "{d}", .{expiration});
    const expire_response = try self.execute(
        io,
        allocator,
        .expire,
        .{ key, expiration_formatted },
    );

    std.debug.assert(expire_response == .integer);
    if (expire_response == .err) return debugError(expire_response);
}

fn fetchRemove(b: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    const self: *Valkey = @fieldParentPtr("interface", b);
    const response = try self.execute(io, allocator, .getdel, .{key});
    std.debug.assert(response == .null or response == .string);
    if (response == .err) return debugError(response);
    return switch (response) {
        .null => null,
        .string => |string| string.value,
        else => unreachable,
    };
}

fn remove(b: *Backend, io: Io, allocator: Allocator, key: []const u8) !void {
    const self: *Valkey = @fieldParentPtr("interface", b);
    const response = try self.execute(io, allocator, .del, .{key});
    std.debug.assert(response == .integer);
    std.debug.assert(response.integer.value == 1);
    if (response == .err) return debugError(response);
}

fn append(b: *Backend, io: Io, allocator: Allocator, key: []const u8, value: []const u8) !void {
    const self: *Valkey = @fieldParentPtr("interface", b);
    const response = try self.execute(io, allocator, .rpush, .{ key, value });
    std.debug.assert(response == .integer);
    if (response == .err) return debugError(response);
}

fn prepend(b: *Backend, io: Io, allocator: Allocator, key: []const u8, value: []const u8) !void {
    const self: *Valkey = @fieldParentPtr("interface", b);
    const response = try self.execute(io, allocator, .lpush, .{ key, value });
    std.debug.assert(response == .integer);
    if (response == .err) return debugError(response);
}

fn pop(b: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    const self: *Valkey = @fieldParentPtr("interface", b);
    const response = try self.execute(io, allocator, .rpop, .{key});
    std.debug.assert(response == .null or response == .string);
    if (response == .err) return debugError(response);
    return switch (response) {
        .null => null,
        .string => |string| string.value,
        else => unreachable,
    };
}

fn popFirst(b: *Backend, io: Io, allocator: Allocator, key: []const u8) !?[]const u8 {
    const self: *Valkey = @fieldParentPtr("interface", b);
    const response = try self.execute(io, allocator, .lpop, .{key});
    std.debug.assert(response == .null or response == .string);
    if (response == .err) return debugError(response);
    return switch (response) {
        .null => null,
        .string => |string| string.value,
        else => unreachable,
    };
}

/// Attempt to connect to the configured Valkey host.
pub fn connect(self: *Valkey, io: Io, allocator: Allocator) !void {
    for (self.internal.pool.connections) |connection|
        try connection.connect(io, allocator);
}

const Pool = struct {
    pool_size: u16 = 8,
    connections: []*Connection,
    available: []bool,
    mutex: Mutex = .init,
    condition: Condition = .init,

    const Connection = struct {
        host: []const u8,
        port: u16,
        index: usize,
        stream: Stream = undefined,
        state: enum { initial, connected } = .initial,
        comptime buffer_size: u32 = 8,

        pub fn connect(self: *Connection, io: Io, allocator: Allocator) !void {
            try self.initStream(io);
            self.state = .connected;
            try self.handshake(io, allocator);
        }

        pub fn deinit(self: *Connection, io: Io) void {
            if (self.state == .connected) self.stream.close(io);
        }

        fn initStream(self: *Connection, io: Io) !void {
            const address = Io.net.IpAddress.parse(self.host, self.port) catch
                try Io.net.IpAddress.resolve(io, self.host, self.port);
            self.stream = try address.connect(io, .{ .mode = .stream });
        }

        fn handshake(self: *Connection, io: Io, allocator: Allocator) !void {
            var arena: ArenaAllocator = .init(allocator);
            defer arena.deinit();
            const response = try self.execute(
                io,
                arena.allocator(),
                .hello,
                .{"3"},
            );
            if (response == .err) return error.ValkeyInvalidResponse;
        }

        pub fn execute(
            self: *Connection,
            io: Io,
            allocator: Allocator,
            comptime command_name: Command.Name,
            args: anytype,
        ) !Response {
            std.debug.assert(self.state == .connected);
            var aw: Writer.Allocating = .init(allocator);
            defer aw.deinit();

            const command: Command = .{ .name = command_name };
            try command.write(&aw.writer, args);
            const output = try aw.toOwnedSlice();
            defer allocator.free(output);
            var sw = self.stream.writer(io, &.{});
            try sw.interface.writeAll(output);

            var read_buf: [self.buffer_size]u8 = undefined;
            var stream_reader = self.stream.reader(io, &read_buf);
            return Response.parse(allocator, &stream_reader.interface, self.buffer_size) catch |err| {
                self.stream.close(io);
                self.state = .initial;
                if (err == error.ReadFailed) {
                    if (stream_reader.err) |reader_err| {
                        if (reader_err == error.Timeout) return error.ValkeyTimeout;
                    }
                    return error.EndOfStream;
                }
                return err;
            };
        }
    };

    pub fn acquire(self: *Pool, io: Io) !*Connection {
        self.mutex.lockUncancelable(io);
        errdefer self.mutex.unlock(io);

        while (true) {
            const available_index = self.firstAvailable() orelse {
                try self.condition.wait(io, &self.mutex);
                continue;
            };
            self.available[available_index] = false;
            self.mutex.unlock(io);
            return self.connections[available_index];
        }
    }

    pub fn release(self: *Pool, io: Io, connection: *const Connection) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.available[connection.index] = true;
        self.condition.broadcast(io);
    }

    pub fn deinit(self: *Pool, io: Io) void {
        for (self.connections) |connection|
            connection.deinit(io);
    }

    fn firstAvailable(self: *Pool) ?usize {
        if (comptime backend_supports_vectors) {
            const vec_available: @Vector(self.pool_size, bool) = self.available;
            return if (std.simd.firstTrue(vec_available)) |index| @intCast(index) else null;
        }
        return for (self.available, 0..) |available, index| {
            if (available) break index;
        } else null;
    }
};

const Internal = struct {
    state: enum { initial, connected } = .initial,
    pool: *Pool = undefined,
};

const ConnectMode = enum { auto, manual, lazy };
const backend_supports_vectors = switch (builtin.zig_backend) {
    .stage2_llvm, .stage2_c => true,
    else => false,
};

const Response = union(enum) {
    map: Map,
    err: Error,
    ok: void,
    string: String,
    null: void,
    integer: Integer,
    array: Array,

    const OK = "OK" ++ CRLF;

    const Map = struct {
        fn init() Map {
            return .{};
        }
    };

    const Array = struct {
        fn init() Array {
            return .{};
        }
    };

    const Error = struct {
        data: []const u8,
        buf: []u8,

        fn init(data: []const u8, comptime buffer_size: u32) Error {
            const err = "ERR ";
            std.debug.assert(data.len <= buffer_size);
            std.debug.assert(std.mem.startsWith(u8, data, err));
            const payload = data[err.len..];
            var buf: [buffer_size]u8 = undefined;
            @memcpy(buf[0..payload.len], data[err.len..]);
            return .{ .buf = &buf, .data = buf[0..payload.len] };
        }

        pub fn format(self: Error, writer: *Writer) !void {
            try writer.print("Error: {s}", .{self.data});
        }
    };

    const String = struct {
        value: []const u8,

        fn init(allocator: Allocator, data: []const u8) !String {
            var it = std.mem.tokenizeSequence(u8, data, CRLF);
            const first_token = it.next();
            std.debug.assert(first_token != null);
            std.debug.assert(first_token.?.len > 0);
            const len = try std.fmt.parseInt(u64, first_token.?, 0);
            const rest = it.rest();
            return if (rest.len == len + CRLF.len and std.mem.endsWith(u8, rest, "\r\n"))
                .{ .value = try allocator.dupe(u8, rest[0..len]) }
            else
                error.ValkeyInvalidResponse;
        }
    };

    const Integer = struct {
        value: i64,

        fn init(data: []const u8) !Integer {
            std.debug.assert(std.mem.endsWith(u8, data, CRLF));
            return .{ .value = try std.fmt.parseInt(i64, data[0 .. data.len - 2], 10) };
        }
    };

    pub fn format(self: Response, writer: *Writer) !void {
        switch (self) {
            .map => try writer.print("Response.map", .{}),
            .err => |err| try writer.print("Response.err{{ .err = {s} }}", .{err.data}),
            .ok => try writer.print("Response.ok", .{}),
            .string => |string| try writer.print(
                "Response.string{{ .value = {s} }}",
                .{string.value},
            ),
            .null => try writer.print("Response.null", .{}),
            .integer => |integer| try writer.print(
                "Response.integer{{ .value = {} }}",
                .{integer.value},
            ),
            .array => try writer.print("Response.array", .{}),
        }
    }

    fn parse(allocator: Allocator, r: *Io.Reader, comptime buffer_size: u32) !Response {
        const code = (try r.takeArray(1))[0];
        const parts_count = switch (code) {
            '-', '+', '_', ':' => 1,
            '*' => try readInt(r, buffer_size),
            '$' => 2,
            '%' => try readInt(r, buffer_size),
            else => {
                std.debug.print("Valkey Unsupported Response: `{c}`\n", .{code});
                return error.ValkeyUnsupportedResponse;
            },
        };

        if (code == '%') {
            // We parse Maps to ensure that we exhaust the response returned by Redis so
            // that the next response is clean but we don't support them as values. We
            // get a Map response from the initial `HELLO` handshake.
            for (0..parts_count) |_| {
                const key = try parse(allocator, r, buffer_size);
                const value = try parse(allocator, r, buffer_size);
                _ = key;
                _ = value;
            }
            return Response{ .map = Response.Map.init() };
        }

        // Backed by stack-fallback allocator:
        var aw: Writer.Allocating = .init(allocator);
        defer aw.deinit();

        if (code == '$') {
            const bytes_to_read = try readInt(r, buffer_size);
            try aw.writer.print("{}\r\n", .{bytes_to_read});
            var string_buf: [buffer_size]u8 = undefined;
            var remaining = bytes_to_read;
            while (remaining > 0) {
                const chunk = @min(string_buf.len, remaining);
                try r.readSliceAll(string_buf[0..chunk]);
                remaining -= chunk;
                _ = try aw.writer.write(string_buf[0..chunk]);
            }
            try readUntilTerminator(&aw.writer, r, buffer_size);
        } else try readParts(&aw.writer, parts_count, r, buffer_size);

        const payload = try aw.toOwnedSlice();
        defer allocator.free(payload);

        return switch (code) {
            '-' => Response{ .err = .init(payload, buffer_size) },
            '+' => if (std.mem.eql(u8, Response.OK, payload))
                Response.ok
            else
                error.ValkeyUnsupportedResponse,
            '$' => Response{ .string = try .init(allocator, payload) },
            '_' => Response.null,
            ':' => Response{ .integer = try .init(payload) },
            '*' => Response{ .array = .init() },
            else => error.ValkeyError,
        };
    }

    fn readInt(r: *Io.Reader, comptime buffer_size: u32) !u64 {
        var buf: [20]u8 = undefined;
        var writer: Writer = .fixed(&buf);
        try readUntilTerminator(&writer, r, buffer_size);
        const value = writer.buffered();
        std.debug.assert(std.mem.endsWith(u8, value, CRLF));
        return std.fmt.parseInt(u64, value[0 .. value.len - 2], 10);
    }

    fn readUntilTerminator(writer: *Writer, r: *Io.Reader, comptime buffer_size: u32) !void {
        var cr = false;
        var buf: [buffer_size]u8 = undefined;
        var cursor: usize = 0;
        while (true) {
            const byte = (try r.takeArray(1))[0];
            if (byte == '\r') cr = true;
            buf[cursor] = byte;
            cursor += 1;
            if (cursor == buf.len) {
                try writer.writeAll(buf[0..]);
                cursor = 0;
            }
            if (cr and byte == '\n') {
                if (cursor != buf.len) try writer.writeAll(buf[0..cursor]);
                break;
            }
        }
    }

    fn readParts(writer: *Writer, parts_count: usize, r: *Io.Reader, comptime buffer_size: u32) !void {
        for (0..parts_count) |_|
            try readUntilTerminator(writer, r, buffer_size);
    }
};

const Command = struct {
    name: Name,

    const Name = enum {
        hello,
        flushdb,
        get,
        set,
        del,
        getdel,
        rpush,
        rpop,
        lpush,
        lpop,
        expire,
    };

    pub fn write(comptime self: Command, writer: *Writer, args: anytype) !void {
        // Valkey supports multiple values for some commands. For consistency with other backends
        // we only support one value (e.g. `DEL`, `RPUSH`, etc.).
        const command = comptime blk: {
            const tag = @tagName(self.name);
            var command_buf: [tag.len]u8 = undefined;
            break :blk std.ascii.upperString(command_buf[0..], tag);
        };
        const args_len = @typeInfo(@TypeOf(args)).@"struct".fields.len;
        const prefix = std.fmt.comptimePrint(
            "*{1}{0s}${2}{0s}{3s}{0s}",
            .{ CRLF, args_len + 1, command.len, command },
        );
        try writer.writeAll(prefix);
        inline for (args) |arg| {
            const T = @TypeOf(arg);
            switch (@typeInfo(T)) {
                .int, .comptime_int => {
                    try writer.print(":{}{s}", .{ arg, CRLF });
                },
                // We only support strings - let Zig compiler catch failures if `arg` is not
                // coercable to `[]const u8`
                else => {
                    try writer.print("${1}{0s}{2s}{0s}", .{ CRLF, arg.len, arg });
                },
            }
        }
    }
};

pub fn execute(
    self: Valkey,
    io: Io,
    allocator: Allocator,
    comptime command_name: Command.Name,
    args: anytype,
) !Response {
    const connection = try self.internal.pool.acquire(io);
    if (connection.state == .initial) try connection.connect(io, allocator);
    defer self.internal.pool.release(io, connection);
    return connection.execute(io, allocator, command_name, args);
}

pub fn flush(self: Valkey, io: Io, allocator: Allocator) !void {
    const response = try self.execute(io, allocator, .flushdb, .{"sync"});
    std.debug.assert(response == .ok);
    if (response == .err) return debugError(response);
}

fn debugError(response: Response) error{ValkeyError} {
    std.debug.assert(response == .err);
    std.debug.print("{any}\n", .{response});
    return error.ValkeyError;
}

const CRLF = "\r\n";

const t = std.testing;

test "auto connect" {
    var kv: Valkey = try .init(.{}, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    for (kv.internal.pool.connections) |connection|
        try t.expectEqual(connection.state, .connected);
}

test "manual connect" {
    var kv: Valkey = try .init(.{ .connect_mode = .manual }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    for (kv.internal.pool.connections) |connection|
        try t.expectEqual(connection.state, .initial);
    try kv.connect(t.io, t.allocator);
    for (kv.internal.pool.connections) |connection|
        try t.expectEqual(connection.state, .connected);
}

test "lazy connect" {
    var kv: Valkey = try .init(.{ .connect_mode = .lazy }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    for (kv.internal.pool.connections) |connection|
        try t.expectEqual(connection.state, .initial);
    try kv.flush(t.io, t.allocator);
    for (kv.internal.pool.connections) |connection|
        if (connection.state == .connected) break else try t.expect(false);
}

test "put/get" {
    var kv: Valkey = try .init(.{}, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    try kv.flush(t.io, t.allocator);
    try backend.put(t.io, t.allocator, "foo", "bar");
    try backend.put(t.io, t.allocator, "baz", "qux");
    try backend.put(t.io, t.allocator, "quux", "corge");
    const foo = try backend.get(t.io, t.allocator, "foo");
    defer t.allocator.free(foo.?);
    try t.expectEqualStrings("bar", foo.?);
    const baz = try backend.get(t.io, t.allocator, "baz");
    defer t.allocator.free(baz.?);
    try t.expectEqualStrings("qux", baz.?);
    const quux = try backend.get(t.io, t.allocator, "quux");
    defer t.allocator.free(quux.?);
    try t.expectEqualStrings("corge", quux.?);
}

test "get missing key" {
    var kv: Valkey = try .init(.{}, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    try kv.flush(t.io, t.allocator);
    try t.expect(try backend.get(t.io, t.allocator, "foo") == null);
}

test "remove" {
    var kv: Valkey = try .init(.{}, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    try kv.flush(t.io, t.allocator);
    try backend.put(t.io, t.allocator, "foo", "bar");
    const foo1 = try backend.get(t.io, t.allocator, "foo");
    defer t.allocator.free(foo1.?);
    try t.expectEqualStrings("bar", foo1.?);
    try backend.remove(t.io, t.allocator, "foo");
    try t.expect(try backend.get(t.io, t.allocator, "foo") == null);
}

test "fetchRemove" {
    var kv: Valkey = try .init(.{}, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    try kv.flush(t.io, t.allocator);
    try backend.put(t.io, t.allocator, "foo", "bar");
    const foo1 = try backend.get(t.io, t.allocator, "foo");
    defer t.allocator.free(foo1.?);
    try t.expectEqualStrings("bar", foo1.?);
    const foo2 = try backend.fetchRemove(t.io, t.allocator, "foo");
    defer t.allocator.free(foo2.?);
    try t.expectEqualStrings("bar", foo2.?);
    try t.expect(try backend.get(t.io, t.allocator, "foo") == null);
}

test "append/pop" {
    var kv: Valkey = try .init(.{}, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    try kv.flush(t.io, t.allocator);
    try backend.append(t.io, t.allocator, "foo", "bar");
    const foo1 = try backend.pop(t.io, t.allocator, "foo");
    defer t.allocator.free(foo1.?);
    try t.expectEqualStrings("bar", foo1.?);
    try t.expect(try backend.pop(t.io, t.allocator, "foo") == null);
}

test "prepend/popFirst" {
    var kv: Valkey = try .init(.{}, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    try kv.flush(t.io, t.allocator);
    try backend.prepend(t.io, t.allocator, "foo", "bar");
    const foo1 = try backend.popFirst(t.io, t.allocator, "foo");
    defer t.allocator.free(foo1.?);
    try t.expectEqualStrings("bar", foo1.?);
    try t.expect(try backend.popFirst(t.io, t.allocator, "foo") == null);
}

test "putExpire" {
    var kv: Valkey = try .init(.{}, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    try kv.flush(t.io, t.allocator);
    try backend.putExpire(t.io, t.allocator, "foo", "bar", 1);
    const value1 = try backend.get(t.io, t.allocator, "foo");
    defer t.allocator.free(value1.?);
    try t.expectEqualStrings("bar", value1.?);
    const timeout: Io.Timeout = .{ .duration = .{ .raw = .fromNanoseconds(1_100_000_000), .clock = .real } };
    try timeout.sleep(t.io);
    try t.expect(try backend.get(t.io, t.allocator, "foo") == null);
}

test "put data exceeding buffer size" {
    var kv: Valkey = try .init(.{}, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    try kv.flush(t.io, t.allocator);
    const data = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    try backend.put(t.io, t.allocator, "foo", data);
    const value = try backend.get(t.io, t.allocator, "foo");
    defer t.allocator.free(value.?);
    try t.expectEqualStrings(data, value.?);
}

test "slow/incomplete response" {
    var thread = try std.Thread.spawn(
        .{ .allocator = t.allocator },
        launchTestServer,
        .{"$10\r\noops\r\n"},
    );
    defer thread.join();
    var kv: Valkey = try .init(.{ .port = 63379, .connect_mode = .lazy }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    try t.expectError(
        error.EndOfStream,
        backend.get(t.io, t.allocator, "foo"),
    );
}

test "invalid string response" {
    var thread = try std.Thread.spawn(
        .{ .allocator = t.allocator },
        launchTestServer,
        .{"$2\r\noops\r\n"},
    );
    defer thread.join();
    var kv: Valkey = try .init(.{ .port = 63379, .connect_mode = .lazy }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    try t.expectError(
        error.ValkeyInvalidResponse,
        backend.get(t.io, t.allocator, "foo"),
    );
}

test "invalid integer response" {
    var thread = try std.Thread.spawn(
        .{ .allocator = t.allocator },
        launchTestServer,
        .{":123"},
    );
    defer thread.join();
    var kv: Valkey = try .init(.{ .port = 63379, .connect_mode = .lazy }, t.io, t.allocator);
    const backend = &kv.interface;
    defer backend.deinit(t.io, t.allocator);
    try t.expectError(
        error.EndOfStream,
        backend.get(t.io, t.allocator, "foo"),
    );
}

fn launchTestServer(response: []const u8) void {
    const address = Io.net.IpAddress.parse("127.0.0.1", 63379) catch unreachable;
    var server = address.listen(t.io, .{}) catch unreachable;
    defer server.deinit(t.io);
    const stream = server.accept(t.io) catch unreachable;
    defer stream.close(t.io);
    var write_buf: [1024]u8 = undefined;
    var w = stream.writer(t.io, &write_buf);
    w.interface.writeAll(response) catch unreachable;
    w.interface.flush() catch unreachable;
}
