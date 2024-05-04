const std = @import("std");
const args = @import("args");
const jetkv = @import("jetkv");

const Options = struct {
    @"address-space-size": u32 = 1024,
    @"db-file": []const u8 = "/tmp/jetkv_fuzzing.db",
    @"min-key-len": usize = 1,
    @"max-key-len": usize = 8,
    @"max-value-len": usize = 512,
    @"number-of-ops": usize = 10_000,
    seed: ?[]const u8 = null,
    help: bool = false,

    pub const meta = .{ .option_docs = .{
        .@"address-space-size" = "JetKV file backend's address space size to use",
        .@"db-file" = "Path to JetKV DB (will be deleted during fuzzing!)",
        .@"min-key-len" = "Min length of keys to generate",
        .@"max-key-len" = "Max length of keys to generate",
        .@"max-value-len" = "Max length of values to generate",
        .@"number-of-ops" = "Number of random operations to generate",
        .seed = "The seed to use for the randomness generator. Can be used to regenerate a sequence",
        .help = "Print this help and exit",
    } };

    pub const shorthands = .{
        .N = "number-of-ops",
    };
};

const OpTag = enum {
    append,
    get,
    remove,
    pop,
    popFirst,
    put,
    // Missing: append, fetchRemove, pop
};

const Entry = struct {
    key: []const u8,
    value: []const u8,
};

const Op = union(OpTag) {
    append: Entry,
    get: []const u8,
    remove: []const u8,
    pop: []const u8,
    popFirst: []const u8,
    put: Entry,
};

const Error = error{
    // A deleted or unknown key returned a value
    KeyPresent,
    // An expected key is not present
    KeyNotPresent,
    WrongValue,
    // XXX add panic?
    Other,
};

const Array = std.DoublyLinkedList([]const u8);

fn sameValue(allocator: std.mem.Allocator, T: type, oracle: *std.StringHashMap(T), verbose: bool, key: []const u8, retrieved: ?[]const u8, expected: ?[]const u8) !void {
    if (retrieved) |value| {
        defer allocator.free(value);
        if (expected) |expectedValue| {
            if (verbose) {
                try std.testing.expectEqualStrings(expectedValue, value);
            } else if (!std.mem.eql(u8, value, expectedValue)) return Error.WrongValue;
        } else {
            return Error.KeyPresent;
        }
    } else if (oracle.get(key)) |value| {
        const isPresent = switch (T) {
            Array => value.len > 0,
            else => true,
        };
        if (isPresent) {
            if (verbose) std.debug.print("Expected value not present for key {s}\n", .{key});
            return Error.KeyNotPresent;
        }
    }
}

fn withArray(allocator: ?std.mem.Allocator, map: *std.StringHashMap(Array), key: []const u8, f: fn (*Array) ?*Array.Node) ?[]const u8 {
    if (map.getPtr(key)) |array_ptr| {
        if (f(array_ptr)) |node| {
            defer {
                if (allocator) |a| a.destroy(node);
            }
            return node.data;
        }
    }
    return null;
}

fn freeArray(allocator: std.mem.Allocator, array: *Array) void {
    while (array.*.pop()) |node| {
        allocator.destroy(node);
    }
}

fn replay(allocator: std.mem.Allocator, dbPath: []const u8, addressSpaceSize: u32, ops: []const Op, verbose: bool, failsAt: *usize) !void {
    const pipe = try std.posix.pipe();
    const pid = try std.posix.fork();
    if (pid > 0) {
        defer std.posix.close(pipe[0]);
        std.posix.close(pipe[1]);

        while (true) {
            const bytesRead = try std.posix.read(pipe[0], std.mem.asBytes(failsAt));
            if (bytesRead == 0) {
                break;
            } else if (bytesRead < @sizeOf(usize)) {
                std.debug.print("[WARN] Read too short, result may be unreliable\n", .{});
            }
        }

        const result = std.posix.waitpid(pid, 0);
        switch (result.status) {
            0, 256 => return,
            1 => return Error.KeyPresent,
            2 => return Error.KeyNotPresent,
            3 => return Error.WrongValue,
            else => return Error.Other,
        }
    } else {
        defer std.posix.close(pipe[1]);
        std.posix.close(pipe[0]);

        replayChild(allocator, dbPath, addressSpaceSize, ops, verbose, pipe[1]) catch |err| {
            const exitStatus: u8 = switch (err) {
                Error.KeyPresent => 1,
                Error.KeyNotPresent => 2,
                Error.WrongValue => 3,
                Error.Other => unreachable(),
                else => 4,
            };
            std.posix.exit(exitStatus);
        };
        std.posix.exit(0);
    }
}

fn replayChild(allocator: std.mem.Allocator, dbPath: []const u8, addressSpaceSize: u32, ops: []const Op, verbose: bool, failsAtFd: std.posix.fd_t) !void {
    var jet_kv = try jetkv.JetKV.init(undefined, .{ .backend = .file, .file_backend_options = .{
        .path = dbPath,
        .address_space_size = jetkv.addressSpaceSize(addressSpaceSize),
        .truncate = true,
    } });
    defer jet_kv.deinit();

    var knownStrings = std.StringHashMap([]const u8).init(allocator);
    defer knownStrings.deinit();

    var knownArrays = std.StringHashMap(Array).init(allocator);
    defer knownArrays.deinit();
    defer {
        var it = knownArrays.valueIterator();
        while (it.next()) |value_ptr| {
            freeArray(allocator, value_ptr);
        }
    }

    for (ops, 0..) |op, i| {
        _ = try std.posix.write(failsAtFd, std.mem.asBytes(&i));
        switch (op) {
            .append => |entry| {
                if (verbose) std.debug.print("try kv.append(\"{s}\", \"{s}\");\n", .{ entry.key, entry.value });
                try jet_kv.append(entry.key, entry.value);

                _ = knownStrings.remove(entry.key);

                const value = try knownArrays.getOrPut(entry.key);
                if (!value.found_existing) {
                    value.value_ptr.* = Array{};
                }
                const node = try allocator.create(Array.Node);
                node.* = .{ .data = entry.value };
                value.value_ptr.append(node);
            },
            .get => |key| {
                if (verbose) std.debug.print("_ = try kv.get(allocator, \"{s}\");\n", .{key});
                const value = try jet_kv.get(allocator, key);
                // File backend also returns the first array elements (if there is any)
                const expected = knownStrings.get(key) orelse withArray(null, &knownArrays, key, struct {
                    fn first(array: *Array) ?*Array.Node {
                        return array.first;
                    }
                }.first);
                try sameValue(allocator, []const u8, &knownStrings, verbose, key, value, expected);
            },
            .remove => |key| {
                if (verbose) std.debug.print("try kv.remove(\"{s}\");\n", .{key});
                try jet_kv.remove(key);
                _ = knownStrings.remove(key);
                if (knownArrays.fetchRemove(key)) |array_entry| {
                    var array = array_entry.value;
                    freeArray(allocator, &array);
                }
            },
            .pop => |key| {
                if (verbose) std.debug.print("_ = try kv.pop(allocator, \"{s}\");\n", .{key});
                const value = try jet_kv.pop(allocator, key);
                const expected = withArray(allocator, &knownArrays, key, struct {
                    fn pop(array: *Array) ?*Array.Node {
                        return array.pop();
                    }
                }.pop);
                try sameValue(allocator, Array, &knownArrays, verbose, key, value, expected);
            },
            .popFirst => |key| {
                if (verbose) std.debug.print("_ = try kv.popFirst(allocator, \"{s}\");\n", .{key});
                const value = try jet_kv.popFirst(allocator, key);
                const expected = withArray(allocator, &knownArrays, key, struct {
                    fn popFirst(array: *Array) ?*Array.Node {
                        return array.popFirst();
                    }
                }.popFirst);
                try sameValue(allocator, Array, &knownArrays, verbose, key, value, expected);
            },
            .put => |entry| {
                if (verbose) std.debug.print("try kv.put(\"{s}\", \"{s}\");\n", .{ entry.key, entry.value });
                // Putting clears existing array (in file backend)
                if (knownArrays.fetchRemove(entry.key)) |array_entry| {
                    var array = array_entry.value;
                    freeArray(allocator, &array);
                }

                try jet_kv.put(entry.key, entry.value);
                try knownStrings.put(entry.key, entry.value);
            },
        }
    }
}

const Shrinker = struct {
    const Self = @This();
    const strategies = [_]*const fn (*Self, []const Op) std.mem.Allocator.Error!?std.ArrayList(Op){
        &removeFirstN,
        &removeRandomKey,
        &removeRandomOp,
    };
    allocator: std.mem.Allocator,
    currentStrategy: usize = 0,
    random: *std.Random,
    dbPath: []const u8,
    addressSpaceSize: u32,

    fn init(allocator: std.mem.Allocator, random: *std.Random, dbPath: []const u8, addressSpaceSize: u32) Self {
        return .{ .allocator = allocator, .random = random, .dbPath = dbPath, .addressSpaceSize = addressSpaceSize };
    }

    fn extractKey(op: Op) []const u8 {
        return switch (op) {
            .append => |entry| entry.key,
            .get => |key| key,
            .remove => |key| key,
            .pop => |key| key,
            .popFirst => |key| key,
            .put => |entry| entry.key,
        };
    }

    fn selectRandomKey(self: *Self, map: *std.StringHashMap(void)) []const u8 {
        const wanted = self.random.intRangeLessThan(usize, 0, map.count());
        var it = map.keyIterator();
        var i: usize = 0;
        while (it.next()) |key_ptr| {
            if (i == wanted) {
                return key_ptr.*;
            }
            i += 1;
        }
        unreachable();
    }

    fn removeFirstN(self: *Self, initial: []const Op) !?std.ArrayList(Op) {
        const n: usize = initial.len / 10;
        if (n <= 2 or initial.len <= n) return null;

        var reduced = std.ArrayList(Op).init(self.allocator);
        try reduced.appendSlice(initial[n..]);
        if (self.stillFails(reduced.items[0..])) {
            return reduced;
        }

        reduced.deinit();
        return null;
    }

    fn removeRandomKey(self: *Self, initial: []const Op) !?std.ArrayList(Op) {
        var uniqueKeys = std.StringHashMap(void).init(self.allocator);
        defer uniqueKeys.deinit();
        for (initial) |op| {
            try uniqueKeys.put(extractKey(op), {});
        }

        var reduced = std.ArrayList(Op).init(self.allocator);
        for (0..10) |_| {
            reduced.clearAndFree();
            const keyToFilter = self.selectRandomKey(&uniqueKeys);
            for (initial) |op| {
                if (!std.mem.eql(u8, keyToFilter, extractKey(op))) {
                    try reduced.append(op);
                }
            }
            if (self.stillFails(reduced.items[0..])) {
                return reduced;
            }
        }

        reduced.deinit();
        return null;
    }

    fn removeRandomOp(self: *Self, initial: []const Op) !?std.ArrayList(Op) {
        var reduced = std.ArrayList(Op).init(self.allocator);
        for (0..10) |_| {
            reduced.clearAndFree();
            try reduced.appendSlice(initial);
            const i = self.random.intRangeLessThan(usize, 0, reduced.items.len);
            _ = reduced.orderedRemove(i);
            if (self.stillFails(reduced.items[0..])) {
                return reduced;
            }
        }

        reduced.deinit();
        return null;
    }

    fn stillFails(self: *Self, ops: []const Op) bool {
        var failsAt: usize = 0;
        replay(self.allocator, self.dbPath, self.addressSpaceSize, ops, false, &failsAt) catch {
            return true;
        };
        return false;
    }

    pub fn shrink(self: *Self, initial: []const Op) !std.ArrayList(Op) {
        if (self.currentStrategy < strategies.len) {
            if (try strategies[self.currentStrategy](self, initial)) |reduced| {
                return reduced;
            } else {
                self.currentStrategy += 1;
                return self.shrink(initial);
            }
        } else {
            // Can't shrink further, give up
            var reduced = std.ArrayList(Op).init(self.allocator);
            try reduced.appendSlice(initial);
            return reduced;
        }
    }
};

fn newValue(allocator: std.mem.Allocator, rand: *std.Random, minLen: usize, maxLen: usize) ![]u8 {
    const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVXWYZ_0123456789";
    const len = rand.intRangeLessThan(usize, minLen, maxLen);
    const value = try allocator.alloc(u8, len);
    for (0..len) |i| value[i] = chars[rand.intRangeLessThan(usize, 0, chars.len)];
    return value;
}

fn parseSeedValue(seed: []u8, input: []const u8) void {
    if (seed.len * 2 != input.len) {
        std.debug.print("Invalid seed value '{s}', expected string of len {d}\n", .{ input, seed.len * 2 });
        std.posix.exit(1);
    }
    var i: usize = 0;
    while (i < input.len) : (i += 2) {
        const byte = std.fmt.parseInt(u8, input[i .. i + 2], 16) catch {
            std.debug.print("Only hexadecimal digits are allowed as seed input", .{});
            std.posix.exit(1);
        };
        seed[i / 2] = byte;
    }
}

fn initRng(Rng: type, options: *const Options) Rng {
    var seed: [Rng.secret_seed_length]u8 = undefined;
    if (options.seed) |seedInput| {
        parseSeedValue(&seed, seedInput);
    } else {
        std.crypto.random.bytes(&seed);
    }
    const rng = Rng.init(seed);
    std.debug.print("Initialized RNG with seed {s}\n", .{std.fmt.fmtSliceHexLower(&seed)});
    return rng;
}

fn fuzz(allocator: std.mem.Allocator, options: *const Options) !void {
    var rng = initRng(std.Random.DefaultCsprng, options);
    var random = rng.random();

    var ops = try std.ArrayList(Op).initCapacity(allocator, options.@"number-of-ops");
    defer ops.deinit();
    defer {
        for (ops.items) |op| {
            switch (op) {
                .append => |entry| {
                    allocator.free(entry.key);
                    allocator.free(entry.value);
                },
                .get => |key| allocator.free(key),
                .remove => |key| allocator.free(key),
                .pop => |key| allocator.free(key),
                .popFirst => |key| allocator.free(key),
                .put => |entry| {
                    allocator.free(entry.key);
                    allocator.free(entry.value);
                },
            }
        }
    }

    const minKeyLen = options.@"min-key-len";
    const maxKeyLen = options.@"max-key-len";
    for (0..options.@"number-of-ops") |_| {
        switch (random.enumValue(OpTag)) {
            .append => {
                const key = try newValue(allocator, &random, minKeyLen, maxKeyLen);
                const value = try newValue(allocator, &random, 0, options.@"max-value-len");
                try ops.append(Op{ .append = .{ .key = key, .value = value } });
            },
            .get => {
                const key = try newValue(allocator, &random, minKeyLen, maxKeyLen);
                try ops.append(Op{ .get = key });
            },
            .remove => {
                const key = try newValue(allocator, &random, minKeyLen, maxKeyLen);
                try ops.append(Op{ .remove = key });
            },
            .pop => {
                const key = try newValue(allocator, &random, minKeyLen, maxKeyLen);
                try ops.append(Op{ .pop = key });
            },
            .popFirst => {
                const key = try newValue(allocator, &random, minKeyLen, maxKeyLen);
                try ops.append(Op{ .popFirst = key });
            },
            .put => {
                const key = try newValue(allocator, &random, minKeyLen, maxKeyLen);
                const value = try newValue(allocator, &random, 0, options.@"max-value-len");
                try ops.append(Op{ .put = .{ .key = key, .value = value } });
            },
        }
    }

    var failsAt: usize = 0;
    replay(allocator, options.@"db-file", options.@"address-space-size", ops.items[0..], false, &failsAt) catch |err| {
        std.debug.print("Got error {?}, now trying to shrink\n", .{err});
        var shrinker = Shrinker.init(allocator, &random, options.@"db-file", options.@"address-space-size");
        var prevReduced = try ops.clone();
        var reduced = try shrinker.shrink(ops.items[0 .. failsAt + 1]);
        while (reduced.items.len < prevReduced.items.len) {
            prevReduced.deinit();
            prevReduced = reduced;
            reduced = try shrinker.shrink(prevReduced.items);
        }
        prevReduced.deinit();

        std.debug.print("Reduced to {d} (from initially {d})\n", .{ reduced.items.len, failsAt + 1 });

        defer reduced.deinit();
        try replay(allocator, options.@"db-file", options.@"address-space-size", reduced.items[0..], true, &failsAt);
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer std.debug.assert(gpa.deinit() == .ok);

    const options = try args.parseForCurrentProcess(Options, allocator, .print);
    defer options.deinit();

    if (options.options.help) {
        try args.printHelp(Options, "fuzzer", std.io.getStdOut().writer());
        return;
    }

    try fuzz(allocator, &options.options);
}
