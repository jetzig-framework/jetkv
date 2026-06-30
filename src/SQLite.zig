const SQLite = @This();

conn: zqlite.Conn,
io: Io,
mutex: Mutex = .init,
/// Interface
store: Store = .{
    .vtable = &.{
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

pub const Config = struct {
    path: [:0]const u8,
    truncate: bool = false,
};

// pub fn init(io: Io, allocator: Allocator, comptime config: Config) !SQLite {
pub fn init(io: Io, comptime config: Config) !SQLite {
    if (config.truncate) {
        Dir.deleteFileAbsolute(io, config.path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }

    // const path_z = try allocator.dupeZ(u8, config.path);
    // defer allocator.free(path_z);

    const flags = zqlite.c.SQLITE_OPEN_CREATE | zqlite.c.SQLITE_OPEN_READWRITE;
    // const conn = try zqlite.open(path_z, flags);
    const conn = try zqlite.open(config.path, flags);

    var s: SQLite = .{
        .io = io,
        .conn = conn,
    };

    try s.conn.execNoArgs("PRAGMA journal_mode=WAL");
    try s.conn.execNoArgs(
        "CREATE TABLE IF NOT EXISTS kv_strings" ++
            "(key TEXT PRIMARY KEY, value TEXT NOT NULL, expiry INTEGER)",
    );
    try s.conn.execNoArgs(
        "CREATE TABLE IF NOT EXISTS kv_arrays" ++
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT NOT NULL, value TEXT NOT NULL, position INTEGER NOT NULL)",
    );
    try s.conn.execNoArgs("CREATE INDEX IF NOT EXISTS kv_arrays_key_pos ON kv_arrays (key, position)");

    return s;
}

pub fn deinit(self: *SQLite) void {
    self.conn.close();
}

fn get(s: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    const self: *SQLite = @fieldParentPtr("store", s);
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);

    const r = try self.conn.row(
        "SELECT value, expiry FROM kv_strings WHERE key = ?1",
        .{key},
    ) orelse return null;
    defer r.deinit();

    if (r.nullableInt(1)) |expiry| {
        const now = Io.Timestamp.now(self.io, .real).toMilliseconds();
        if (expiry < now) return null;
    }

    return try allocator.dupe(u8, r.text(0));
}

fn put(s: *Store, key: []const u8, value: []const u8) !void {
    const self: *SQLite = @fieldParentPtr("store", s);
    try self.putMaybeExpire(key, value, null);
}

fn putExpire(s: *Store, key: []const u8, value: []const u8, expiration: i32) !void {
    const self: *SQLite = @fieldParentPtr("store", s);
    const now = Io.Timestamp.now(self.io, .real).toMilliseconds();
    try self.putMaybeExpire(key, value, now + (@as(i64, expiration) * std.time.ms_per_s));
}

fn putMaybeExpire(self: *SQLite, key: []const u8, value: []const u8, expiry: ?i64) !void {
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);

    try self.conn.exec(
        "INSERT OR REPLACE INTO kv_strings (key, value, expiry) VALUES (?1, ?2, ?3)",
        .{ key, value, expiry },
    );
}

fn fetchRemove(s: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    const self: *SQLite = @fieldParentPtr("store", s);
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);

    const r = try self.conn.row(
        "SELECT value FROM kv_strings WHERE key = ?1",
        .{key},
    ) orelse return null;
    defer r.deinit();

    const value = try allocator.dupe(u8, r.text(0));
    errdefer allocator.free(value);

    try self.conn.exec("DELETE FROM kv_strings WHERE key = ?1", .{key});

    return value;
}

fn remove(s: *Store, key: []const u8) !void {
    const self: *SQLite = @fieldParentPtr("store", s);
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);

    try self.conn.exec("DELETE FROM kv_strings WHERE key = ?1", .{key});
}

fn append(s: *Store, key: []const u8, value: []const u8) !void {
    const self: *SQLite = @fieldParentPtr("store", s);
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);

    try self.conn.exec(
        "INSERT INTO kv_arrays (key, value, position) VALUES (?1, ?2, COALESCE((SELECT MAX(position) FROM kv_arrays WHERE key = ?1), -1) + 1)",
        .{ key, value },
    );
}

fn prepend(s: *Store, key: []const u8, value: []const u8) !void {
    const self: *SQLite = @fieldParentPtr("store", s);
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);

    try self.conn.exec(
        "INSERT INTO kv_arrays (key, value, position) VALUES (?1, ?2, COALESCE((SELECT MIN(position) FROM kv_arrays WHERE key = ?1), 1) - 1)",
        .{ key, value },
    );
}

fn pop(s: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    const self: *SQLite = @fieldParentPtr("store", s);
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);
    return self.popByOrder(allocator, key, "DESC");
}

fn popFirst(s: *Store, allocator: Allocator, key: []const u8) !?[]const u8 {
    const self: *SQLite = @fieldParentPtr("store", s);
    self.mutex.lockUncancelable(self.io);
    defer self.mutex.unlock(self.io);
    return self.popByOrder(allocator, key, "ASC");
}

fn popByOrder(self: *SQLite, allocator: Allocator, key: []const u8, comptime order: []const u8) !?[]const u8 {
    const sel_sql = "SELECT id, value FROM kv_arrays WHERE key = ?1 ORDER BY position " ++ order ++ " LIMIT 1";

    const r = try self.conn.row(sel_sql, .{key}) orelse return null;
    defer r.deinit();

    const row_id = r.int(0);
    const value = try allocator.dupe(u8, r.text(1));
    errdefer allocator.free(value);

    try self.conn.exec("DELETE FROM kv_arrays WHERE id = ?1", .{row_id});

    return value;
}

const t = std.testing;
test "sqlite overwrite" {
    var kv: SQLite = try .init(t.io, .{
        .path = "/tmp/jetkv_sqlite.db",
        .truncate = true,
    });
    defer kv.deinit();
    const backend = &kv.store;

    try backend.put("foo", "bar");
    try backend.put("foo", "baz");

    if (try backend.get(t.allocator, "foo")) |v| {
        defer t.allocator.free(v);
        try t.expectEqualStrings("baz", v);
    } else try t.expect(false);
}

const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;
const Mutex = Io.Mutex;
const Allocator = std.mem.Allocator;
const Store = @import("Store.zig");
const zqlite = @import("zqlite");
