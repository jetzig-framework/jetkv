const std = @import("std");

const jetkv = @import("../../jetkv.zig");
const builtin = @import("builtin");

const backend_supports_vectors = switch (builtin.zig_backend) {
    .stage2_llvm, .stage2_c => true,
    else => false,
};

/// Options specific to the Valkey-based backend.
pub const Options = struct {
    connect: ConnectMode = .auto,
    host: []const u8 = "localhost",
    port: u16 = 6379,
    pool_size: u16 = 8,
    buffer_size: u32 = 4096,
    connect_timeout: u64 = 1 * std.time.ns_per_s,
    read_timeout: u64 = 1 * std.time.ns_per_s,

    pub const ConnectMode = enum { auto, manual, lazy };
};

pub fn ValkeyBackend(comptime options: Options) type {
    return struct {
        allocator: std.mem.Allocator,
        options: Options,
        state: enum { initial, connected } = .initial,
        pool: *Pool,

        const Self = @This();

        /// Initialize a new Valkey backend.
        pub fn init(allocator: std.mem.Allocator) !Self {
            var connections: [options.pool_size]*Pool.Connection = undefined;
            for (0..options.pool_size) |index| {
                connections[index] = try allocator.create(Pool.Connection);
                connections[index].* = .{
                    .allocator = allocator,
                    .host = options.host,
                    .port = options.port,
                    .index = index,
                };
            }
            const pool = try allocator.create(Pool);
            pool.* = Pool{ .connections = connections };
            var backend = Self{ .allocator = allocator, .options = options, .pool = pool };
            if (options.connect == .auto) try backend.connect();
            return backend;
        }

        /// Attempt to connect to the configured Valkey host.
        pub fn connect(self: *Self) !void {
            for (self.pool.connections) |connection| try connection.connect();
        }

        const CRLF = "\r\n";

        const Pool = struct {
            connections: [options.pool_size]*Connection,
            available: [options.pool_size]bool = @splat(true),
            mutex: std.Thread.Mutex = .{},
            condition: std.Thread.Condition = .{},

            const Connection = struct {
                allocator: std.mem.Allocator,
                host: []const u8,
                port: u16,
                index: usize,
                stream: std.net.Stream = undefined,
                state: enum { initial, connected } = .initial,

                const sock_flags = std.posix.SOCK.STREAM |
                    if (builtin.os.tag == .windows)
                    0
                else
                    std.posix.SOCK.CLOEXEC;

                pub fn connect(self: *Connection) !void {
                    try self.initStream();
                    self.state = .connected;
                    try self.handshake();
                }

                pub fn deinit(self: *Connection) void {
                    if (self.state == .connected) self.stream.close();
                }

                fn initStream(self: *Connection) !void {
                    const address_list = try std.net.getAddressList(
                        self.allocator,
                        self.host,
                        self.port,
                    );
                    defer address_list.deinit();
                    const address = for (address_list.addrs) |addr| {
                        break addr;
                    } else return error.ConnectionRefused;

                    const sockfd = try std.posix.socket(
                        address.any.family,
                        sock_flags,
                        std.posix.IPPROTO.TCP,
                    );
                    const timeout = std.posix.timeval{
                        .sec = @divFloor(options.read_timeout, std.time.ns_per_s),
                        .usec = @mod(options.read_timeout, std.time.ns_per_us) * std.time.ns_per_us,
                    };
                    try std.posix.setsockopt(
                        sockfd,
                        std.posix.SOL.SOCKET,
                        std.posix.SO.RCVTIMEO,
                        &std.mem.toBytes(timeout),
                    );
                    errdefer std.net.Stream.close(.{ .handle = sockfd });

                    try std.posix.connect(sockfd, &address.any, address.getOsSockLen());

                    self.stream = std.net.Stream{ .handle = sockfd };
                }

                fn handshake(self: *Connection) !void {
                    var arena = std.heap.ArenaAllocator.init(self.allocator);
                    defer arena.deinit();
                    const response = try self.execute(.hello, arena.allocator(), .{"3"});
                    if (response == .err) return error.ValkeyInvalidResponse;
                }

                pub fn execute(
                    self: *Connection,
                    comptime command_name: Command.Name,
                    maybe_allocator: ?std.mem.Allocator,
                    args: anytype,
                ) !Response {
                    std.debug.assert(self.state == .connected);
                    var stack_fallback = std.heap.stackFallback(
                        options.buffer_size,
                        self.allocator,
                    );
                    const allocator = maybe_allocator orelse stack_fallback.get();
                    var buf = std.ArrayList(u8).init(allocator);
                    defer buf.deinit();
                    const writer = buf.writer();

                    const command = Command{ .name = command_name };
                    try command.write(writer, args);
                    try self.stream.writeAll(buf.items);

                    const reader = self.stream.reader();
                    return Response.parse(allocator, reader) catch |err| {
                        self.stream.close();
                        self.state = .initial;
                        switch (err) {
                            error.WouldBlock => return error.ValkeyTimeout,
                            else => return err,
                        }
                    };
                }
            };

            pub fn acquire(self: *Pool) !*Connection {
                self.mutex.lock();
                errdefer self.mutex.unlock();

                while (true) {
                    const available_index = self.firstAvailable() orelse {
                        try self.condition.timedWait(&self.mutex, options.connect_timeout);
                        continue;
                    };
                    self.available[available_index] = false;
                    self.mutex.unlock();
                    return self.connections[available_index];
                }
            }

            pub fn release(self: *Pool, connection: *const Connection) void {
                self.mutex.lock();
                defer self.mutex.unlock();
                self.available[connection.index] = true;
                self.condition.broadcast();
            }

            pub fn deinit(self: *Pool) void {
                for (self.connections) |connection| {
                    connection.deinit();
                }
            }

            fn firstAvailable(self: *Pool) ?usize {
                if (comptime backend_supports_vectors) {
                    const vec_available: @Vector(options.pool_size, bool) = self.available;
                    return if (std.simd.firstTrue(vec_available)) |index| @intCast(index) else null;
                } else {
                    return for (self.available, 0..) |available, index| {
                        if (available) break index;
                    } else null;
                }
            }
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
                buf: [options.buffer_size]u8,

                fn init(data: []const u8) Error {
                    const err = "ERR ";
                    std.debug.assert(data.len <= options.buffer_size);
                    std.debug.assert(std.mem.startsWith(u8, data, err));
                    const payload = data[err.len..];
                    var buf: [options.buffer_size]u8 = undefined;
                    @memcpy(buf[0..payload.len], data[err.len..]);
                    return .{ .buf = buf, .data = buf[0..payload.len] };
                }

                pub fn format(self: Error, _: anytype, _: anytype, writer: anytype) !void {
                    try writer.print("Error: {s}", .{self.data});
                }
            };

            const String = struct {
                value: []const u8,

                fn init(allocator: std.mem.Allocator, data: []const u8) !String {
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

            pub fn format(self: Response, _: anytype, _: anytype, writer: anytype) !void {
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

            const Reader = struct {
                index: usize = 0,
                data: []const u8,

                pub fn readByte(self: *Reader) !u8 {
                    if (self.index < self.data.len) {
                        const byte = self.data[self.index];
                        self.index += 1;
                        return byte;
                    } else return error.EndOfStream;
                }
            };

            fn parse(allocator: std.mem.Allocator, reader: anytype) !Response {
                const code = try reader.readByte();

                const parts_count = switch (code) {
                    '-', '+', '_', ':' => 1,
                    '*' => try readInt(reader),
                    '$' => 2,
                    '%' => try readInt(reader),
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
                        const key = try parse(allocator, reader);
                        const value = try parse(allocator, reader);
                        _ = key;
                        _ = value;
                    }
                    return Response{ .map = Response.Map.init() };
                }

                // Backed by stack-fallback allocator:
                var array = std.ArrayList(u8).init(allocator);
                defer array.deinit();
                const writer = array.writer();

                if (code == '$') {
                    // TODO Refactor
                    const bytes_to_read = try readInt(reader);
                    try writer.print("{}\r\n", .{bytes_to_read});
                    var string_buf: [options.buffer_size]u8 = undefined;
                    var total_bytes_read: usize = 0;
                    while (true) {
                        const end = std.mem.min(
                            usize,
                            &.{ string_buf.len, bytes_to_read - total_bytes_read },
                        );
                        const bytes_read = try reader.read(
                            string_buf[0..end],
                        );
                        total_bytes_read += bytes_read;
                        try array.appendSlice(string_buf[0..bytes_read]);
                        if (total_bytes_read == bytes_to_read) {
                            try readUntilTerminator(writer, reader);
                            break;
                        }
                    }
                } else {
                    try readParts(writer, parts_count, reader);
                }

                const payload = array.items;

                return switch (code) {
                    '-' => Response{ .err = Response.Error.init(payload) },
                    '+' => if (std.mem.eql(u8, Response.OK, payload))
                        Response.ok
                    else
                        error.ValkeyUnsupportedResponse,
                    '$' => Response{ .string = try Response.String.init(allocator, payload) },
                    '_' => Response.null,
                    ':' => Response{ .integer = try Response.Integer.init(payload) },
                    '*' => Response{ .array = Response.Array.init() },
                    else => error.ValkeyError,
                };
            }

            fn readInt(reader: anytype) !u64 {
                var buf: [20]u8 = undefined;
                var stream = std.io.fixedBufferStream(&buf);
                const writer = stream.writer();
                try readUntilTerminator(writer, reader);
                const value = stream.getWritten();
                std.debug.assert(std.mem.endsWith(u8, value, CRLF));
                return try std.fmt.parseInt(u64, value[0 .. value.len - 2], 10);
            }

            fn readUntilTerminator(writer: anytype, reader: anytype) !void {
                var cr = false;
                var buf: [options.buffer_size]u8 = undefined;
                var cursor: usize = 0;
                while (true) {
                    const byte = try reader.readByte();
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

            fn readParts(writer: anytype, parts_count: usize, reader: anytype) !void {
                for (0..parts_count) |_| {
                    try readUntilTerminator(writer, reader);
                }
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

            pub fn write(comptime self: Command, writer: anytype, args: anytype) !void {
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

        /// Close the Valkey client socket if connected.
        pub fn deinit(self: *Self) void {
            self.pool.deinit();
            for (self.pool.connections) |connection| self.allocator.destroy(connection);
            self.allocator.destroy(self.pool);
        }

        pub fn execute(
            self: Self,
            comptime command_name: Command.Name,
            allocator: ?std.mem.Allocator,
            args: anytype,
        ) !Response {
            const connection = try self.pool.acquire();
            if (connection.state == .initial) try connection.connect();
            defer self.pool.release(connection);
            return connection.execute(command_name, allocator, args) catch |err|
                switch (err) {
                error.EndOfStream, error.BrokenPipe => blk: {
                    try connection.connect();
                    break :blk err;
                },
                else => err,
            };
        }

        pub fn get(self: Self, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
            const response = try self.execute(.get, allocator, .{key});
            std.debug.assert(response == .null or response == .string);
            return switch (response) {
                .string => |string| string.value,
                .null => null,
                else => error.ValkeyError,
            };
        }

        pub fn put(self: Self, key: []const u8, value: []const u8) !void {
            const response = try self.execute(.set, null, .{ key, value });
            std.debug.assert(response == .ok);
            if (response == .err) return debugError(response);
        }

        pub fn putExpire(self: Self, key: []const u8, value: []const u8, expiration: i32) !void {
            // TODO: pipeline
            const set_response = try self.execute(.set, null, .{ key, value });
            std.debug.assert(set_response == .ok);
            if (set_response == .err) return debugError(set_response);

            // Valkey expects a string as expiration time ? This is not clear from the docs:
            // https://valkey.io/commands/expire/
            var buf: [16]u8 = undefined;
            const expiration_formatted = try std.fmt.bufPrint(&buf, "{d}", .{expiration});
            const expire_response = try self.execute(.expire, null, .{ key, expiration_formatted });

            std.debug.assert(expire_response == .integer);
            if (expire_response == .err) return debugError(expire_response);
        }

        pub fn remove(self: Self, key: []const u8) !void {
            const response = try self.execute(.del, null, .{key});
            std.debug.assert(response == .integer);
            std.debug.assert(response.integer.value == 1);
            if (response == .err) return debugError(response);
        }

        pub fn fetchRemove(self: Self, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
            const response = try self.execute(.getdel, allocator, .{key});
            std.debug.assert(response == .null or response == .string);
            if (response == .err) return debugError(response);
            return switch (response) {
                .null => null,
                .string => |string| string.value,
                else => unreachable,
            };
        }

        pub fn append(self: Self, key: []const u8, value: []const u8) !void {
            const response = try self.execute(.rpush, null, .{ key, value });
            std.debug.assert(response == .integer);
            if (response == .err) return debugError(response);
        }

        pub fn prepend(self: Self, key: []const u8, value: []const u8) !void {
            const response = try self.execute(.lpush, null, .{ key, value });
            std.debug.assert(response == .integer);
            if (response == .err) return debugError(response);
        }

        pub fn pop(self: Self, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
            const response = try self.execute(.rpop, allocator, .{key});
            std.debug.assert(response == .null or response == .string);
            if (response == .err) return debugError(response);
            return switch (response) {
                .null => null,
                .string => |string| string.value,
                else => unreachable,
            };
        }

        pub fn popFirst(self: Self, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
            const response = try self.execute(.lpop, allocator, .{key});
            std.debug.assert(response == .null or response == .string);
            if (response == .err) return debugError(response);
            return switch (response) {
                .null => null,
                .string => |string| string.value,
                else => unreachable,
            };
        }

        pub fn flush(self: Self) !void {
            const response = try self.execute(.flushdb, null, .{"sync"});
            std.debug.assert(response == .ok);
            if (response == .err) return debugError(response);
        }

        fn debugError(response: Response) error{ValkeyError} {
            std.debug.assert(response == .err);
            std.debug.print("{}\n", .{response});
            return error.ValkeyError;
        }
    };
}

test "auto connect" {
    var backend = try ValkeyBackend(.{ .connect = .auto }).init(std.testing.allocator);
    defer backend.deinit();
    for (backend.pool.connections) |connection| {
        try std.testing.expectEqual(connection.state, .connected);
    }
}

test "manual connect" {
    var backend = try ValkeyBackend(.{ .connect = .manual }).init(std.testing.allocator);
    defer backend.deinit();
    for (backend.pool.connections) |connection| {
        try std.testing.expectEqual(connection.state, .initial);
    }
    try backend.connect();
    for (backend.pool.connections) |connection| {
        try std.testing.expectEqual(connection.state, .connected);
    }
}

test "lazy connect" {
    var backend = try ValkeyBackend(.{ .connect = .lazy }).init(std.testing.allocator);
    defer backend.deinit();
    for (backend.pool.connections) |connection| {
        try std.testing.expectEqual(connection.state, .initial);
    }
    try backend.flush();
    for (backend.pool.connections) |connection| {
        if (connection.state == .connected) break;
    } else try std.testing.expect(false);
}

test "put/get" {
    var backend = try ValkeyBackend(.{}).init(std.testing.allocator);
    defer backend.deinit();
    try backend.flush();
    try backend.put("foo", "bar");
    try backend.put("baz", "qux");
    try backend.put("quux", "corge");
    const foo = try backend.get(std.testing.allocator, "foo");
    defer std.testing.allocator.free(foo.?);
    try std.testing.expectEqualStrings("bar", foo.?);
    const baz = try backend.get(std.testing.allocator, "baz");
    defer std.testing.allocator.free(baz.?);
    try std.testing.expectEqualStrings("qux", baz.?);
    const quux = try backend.get(std.testing.allocator, "quux");
    defer std.testing.allocator.free(quux.?);
    try std.testing.expectEqualStrings("corge", quux.?);
}

test "get missing key" {
    var backend = try ValkeyBackend(.{}).init(std.testing.allocator);
    defer backend.deinit();
    try backend.flush();
    const foo = try backend.get(std.testing.allocator, "foo");
    try std.testing.expect(foo == null);
}

test "remove" {
    var backend = try ValkeyBackend(.{}).init(std.testing.allocator);
    defer backend.deinit();
    try backend.flush();
    try backend.put("foo", "bar");
    const foo1 = try backend.get(std.testing.allocator, "foo");
    defer std.testing.allocator.free(foo1.?);
    try std.testing.expectEqualStrings("bar", foo1.?);
    try backend.remove("foo");
    const foo2 = try backend.get(std.testing.allocator, "foo");
    try std.testing.expect(foo2 == null);
}

test "fetchRemove" {
    var backend = try ValkeyBackend(.{}).init(std.testing.allocator);
    defer backend.deinit();
    try backend.flush();
    try backend.put("foo", "bar");
    const foo1 = try backend.get(std.testing.allocator, "foo");
    defer std.testing.allocator.free(foo1.?);
    try std.testing.expectEqualStrings("bar", foo1.?);
    const foo2 = try backend.fetchRemove(std.testing.allocator, "foo");
    defer std.testing.allocator.free(foo2.?);
    try std.testing.expectEqualStrings("bar", foo2.?);
    const foo3 = try backend.get(std.testing.allocator, "foo");
    try std.testing.expect(foo3 == null);
}

test "append/pop" {
    var backend = try ValkeyBackend(.{}).init(std.testing.allocator);
    defer backend.deinit();
    try backend.flush();
    try backend.append("foo", "bar");
    const foo1 = try backend.pop(std.testing.allocator, "foo");
    defer std.testing.allocator.free(foo1.?);
    try std.testing.expectEqualStrings("bar", foo1.?);
    const foo2 = try backend.pop(std.testing.allocator, "foo");
    try std.testing.expect(foo2 == null);
}

test "prepend/popFirst" {
    var backend = try ValkeyBackend(.{}).init(std.testing.allocator);
    defer backend.deinit();
    try backend.flush();
    try backend.prepend("foo", "bar");
    const foo1 = try backend.popFirst(std.testing.allocator, "foo");
    defer std.testing.allocator.free(foo1.?);
    try std.testing.expectEqualStrings("bar", foo1.?);
    const foo2 = try backend.popFirst(std.testing.allocator, "foo");
    try std.testing.expect(foo2 == null);
}

test "putExpire" {
    var backend = try ValkeyBackend(.{}).init(std.testing.allocator);
    defer backend.deinit();
    try backend.flush();
    try backend.putExpire("foo", "bar", 1);
    const value1 = try backend.get(std.testing.allocator, "foo");
    defer std.testing.allocator.free(value1.?);
    std.time.sleep(1.1 * std.time.ns_per_s);
    const value2 = try backend.get(std.testing.allocator, "foo");

    try std.testing.expect(value2 == null);
    try std.testing.expectEqualStrings("bar", value1.?);
}

test "put data exceeding buffer size" {
    var backend = try ValkeyBackend(.{ .buffer_size = 32, .connect = .lazy }).init(std.testing.allocator);
    defer backend.deinit();
    try backend.flush();
    const data = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    try backend.put("foo", data);
    const value = try backend.get(std.testing.allocator, "foo");
    defer std.testing.allocator.free(value.?);
    try std.testing.expectEqualStrings(data, value.?);
}

test "put/get large data" {
    var backend = try ValkeyBackend(.{ .connect = .lazy }).init(std.testing.allocator);
    defer backend.deinit();
    try backend.flush();
    const stat = try std.fs.cwd().statFile("fixture/blog.json");
    const ts0 = std.time.nanoTimestamp();
    const data = try std.fs.cwd().readFileAlloc(std.testing.allocator, "fixture/blog.json", stat.size);
    defer std.testing.allocator.free(data);
    const ts1 = std.time.nanoTimestamp();
    try backend.put("foo", data);
    const ts2 = std.time.nanoTimestamp();
    const value = try backend.get(std.testing.allocator, "foo");
    defer std.testing.allocator.free(value.?);
    const ts3 = std.time.nanoTimestamp();

    try std.testing.expectEqualStrings(data, value.?);

    std.debug.print("large data load, put, get: {}, {}, {}\n", .{
        std.fmt.fmtDuration(@intCast(ts1 - ts0)),
        std.fmt.fmtDuration(@intCast(ts2 - ts1)),
        std.fmt.fmtDuration(@intCast(ts3 - ts2)),
    });
}

test "valkey slow/incomplete response" {
    var thread = try std.Thread.spawn(
        .{ .allocator = std.testing.allocator },
        launchTestServer,
        .{"$10\r\noops\r\n"},
    );
    defer thread.join();

    var backend = try ValkeyBackend(.{ .connect = .lazy, .host = "127.0.0.1", .port = 63379 })
        .init(std.testing.allocator);
    defer backend.deinit();

    try std.testing.expectError(error.ValkeyTimeout, backend.get(std.testing.allocator, "foo"));
}

test "valkey invalid string response" {
    var thread = try std.Thread.spawn(
        .{ .allocator = std.testing.allocator },
        launchTestServer,
        .{"$2\r\noops\r\n"},
    );
    defer thread.join();

    var backend = try ValkeyBackend(.{ .connect = .lazy, .host = "127.0.0.1", .port = 63379 })
        .init(std.testing.allocator);
    defer backend.deinit();

    try std.testing.expectError(
        error.ValkeyInvalidResponse,
        backend.get(std.testing.allocator, "foo"),
    );
}

test "valkey invalid integer response" {
    var thread = try std.Thread.spawn(
        .{ .allocator = std.testing.allocator },
        launchTestServer,
        .{":123"},
    );
    defer thread.join();

    var backend = try ValkeyBackend(.{ .connect = .lazy, .host = "127.0.0.1", .port = 63379 })
        .init(std.testing.allocator);
    defer backend.deinit();

    try std.testing.expectError(
        error.ValkeyTimeout, // we timed out waiting for `\r\n`
        backend.get(std.testing.allocator, "foo"),
    );
}

fn launchTestServer(response: []const u8) void {
    const address = std.net.Address.parseIp("127.0.0.1", 63379) catch unreachable;
    var server = address.listen(.{ .reuse_address = true }) catch unreachable;
    defer server.deinit();

    const connection = server.accept() catch unreachable;
    var buf: [1024]u8 = undefined;
    _ = connection.stream.read(&buf) catch unreachable;
    connection.stream.writeAll(response) catch unreachable;
}
