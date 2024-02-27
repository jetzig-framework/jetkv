const std = @import("std");

const jetkv = @import("jetkv.zig");

test {
    std.testing.refAllDeclsRecursive(jetkv);
}
