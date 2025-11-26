const std = @import("std");

pub fn Arc(comptime T: type) type {
    return struct {
        const Self = @This();

        const Inner = struct {
            ptr: *T,
            ref_cnt: std.atomic.Value(usize),
        };

        inner: *Inner,

        pub fn init(value: T) !Self {
            var allocator = std.heap.page_allocator;
            const inner = try allocator.create(Inner);
            errdefer allocator.destroy(inner);
            inner.* = Inner{
                .ptr = try allocator.create(T),
                .ref_cnt = std.atomic.Value(usize).init(1),
            };
            inner.ptr.* = value;
            return Self{ .inner = inner };
        }

        pub fn clone(self: *const Self) Self {
            _ = self.inner.ref_cnt.fetchAdd(1, .acq_rel);
            return Self{ .inner = self.inner };
        }

        pub fn get(self: *Self) *T {
            return self.inner.ptr;
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) bool {
            if (self.inner.ref_cnt.fetchSub(1, .acq_rel) == 1) {
                allocator.destroy(self.inner);
                return true;
            }
            return false;
        }
    };
}

test "Arc usage example" {
    const allocator = std.heap.page_allocator;
    var arc1 = try Arc(i32).init(42);

    var arc2 = arc1.clone();

    std.debug.print("Value from arc1: {}\n", .{arc1.get().*});
    std.debug.print("Value from arc2: {}\n", .{arc2.get().*});

    try std.testing.expectEqual(false, arc2.deinit(allocator));
    try std.testing.expectEqual(true, arc1.deinit(allocator));
}
