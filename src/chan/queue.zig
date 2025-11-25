const std = @import("std");

const QueueError = error{
    Closed,
};

pub fn Bounded(comptime T: type, comptime mp: bool, comptime mc: bool) type {
    return struct {
        const Self = @This();

        /// padding for CPU cache line alignment
        const Slot = struct {
            seq: std.atomic.Value(u64) align(64),
            data: T,
            _pad: [@max(0, 64 - @alignOf(T))]u8,
        };

        capacity: usize,
        mask: usize,
        buffer: []Slot,
        head: u64 align(64) = 0,
        tail: u64 align(64) = 0,

        pub fn init(alloc: std.mem.Allocator, cap: usize) !*Self {
            const real_cap = std.math.ceilPowerOfTwo(usize, cap) catch unreachable;
            const self = try alloc.create(Self);
            errdefer alloc.destroy(self);

            var buf = try alloc.alignedAlloc(Slot, @enumFromInt(6), real_cap);
            errdefer alloc.free(buf);
            // Initialize sequence numbers
            var i: usize = 0;
            while (i < real_cap) : (i += 1) {
                buf[i].seq.store(@as(u64, i), .monotonic);
            }

            self.* = .{
                .capacity = real_cap,
                .mask = real_cap - 1,
                .buffer = buf,
            };
            return self;
        }

        pub fn deinit(self: *const Self, allocator: std.mem.Allocator) void {
            allocator.free(self.buffer);
            allocator.destroy(self);
        }

        pub fn tryEnqueue(self: *Self, value: T) bool {
            // Attempt to reserve a position if available by reading enqueue_pos and checking slot seq
            while (true) {
                const pos = self.loadHead();
                const idx = (@as(usize, pos) & self.mask);
                var cell = &self.buffer[idx];
                const seq = cell.seq.load(.acquire);

                if (seq == pos) {
                    if (self.canEnqueue(pos)) {
                        cell.data = value;
                        cell.seq.store(pos + 1, .release);
                        return true;
                    }
                } else if (seq < pos) {
                    // Slot seq behind pos -> queue is full
                    return false;
                }
            }
        }

        pub fn tryDequeue(self: *Self) ?T {
            while (true) {
                const pos = self.loadTail();
                const idx = (@as(usize, pos) & self.mask);
                var cell = &self.buffer[idx];
                const seq = cell.seq.load(.acquire);
                const expected = pos + 1;

                if (seq == expected) {
                    if (self.canDequeue(pos)) {
                        const ret = cell.data;
                        cell.seq.store(pos + @as(u64, self.capacity), .release);
                        return ret;
                    }
                } else if (seq < expected) {
                    // No item yet (empty)
                    return null;
                }
            }
        }

        /// for test
        fn enqueue(self: *Self, value: T) void {
            while (!self.tryEnqueue(value)) {
                std.atomic.spinLoopHint();
            }
        }

        /// for test
        fn dequeue(self: *Self) T {
            while (true) {
                if (self.tryDequeue()) |value| {
                    return value;
                }
                std.atomic.spinLoopHint();
            }
        }

        pub fn len(self: *Self) usize {
            return @as(usize, self.loadHead() - self.loadTail());
        }

        inline fn loadHead(self: *Self) u64 {
            if (mp) {
                return @atomicLoad(u64, &self.head, .acquire);
            } else {
                return self.head;
            }
        }

        inline fn loadTail(self: *Self) u64 {
            if (mc) {
                return @atomicLoad(u64, &self.tail, .acquire);
            } else {
                return self.tail;
            }
        }

        inline fn canEnqueue(self: *Self, old: u64) bool {
            if (mp) {
                return @cmpxchgWeak(u64, &self.head, old, old + 1, .acq_rel, .acquire) == null;
            } else {
                self.head = old + 1;
                return true;
            }
        }

        inline fn canDequeue(self: *Self, old: u64) bool {
            if (mc) {
                return @cmpxchgWeak(u64, &self.tail, old, old + 1, .acq_rel, .acquire) == null;
            } else {
                self.tail = old + 1;
                return true;
            }
        }
    };
}

/// for test
fn Mpmc(comptime T: type) type {
    return Bounded(T, true, true);
}

test "BoundedQueue basic operations" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const Queue = Mpmc(u64);
    var queue = try Queue.init(allocator, 8);
    defer queue.deinit(allocator);

    // Test basic enqueue/dequeue
    _ = queue.tryEnqueue(42);
    _ = queue.tryEnqueue(100);
    _ = queue.tryEnqueue(200);

    try testing.expectEqual(@as(u64, 42), queue.tryDequeue());
    try testing.expectEqual(@as(u64, 100), queue.tryDequeue());
    try testing.expectEqual(@as(u64, 200), queue.tryDequeue());
}

test "BoundedQueue try operations" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const Queue = Mpmc(u64);
    var queue = try Queue.init(allocator, 8);
    defer queue.deinit(allocator);

    // Test tryEnqueue on a fresh queue
    try testing.expect(queue.tryEnqueue(1) != false);
    try testing.expect(queue.tryEnqueue(2) != false);
    try testing.expect(queue.tryEnqueue(3) != false);
    try testing.expect(queue.tryEnqueue(4) != false);

    // Test tryDequeue
    try testing.expectEqual(@as(u64, 1), queue.tryDequeue());
    try testing.expectEqual(@as(u64, 2), queue.tryDequeue());

    // Queue should have space for more items
    try testing.expect(queue.tryEnqueue(5) != false);
    try testing.expect(queue.tryEnqueue(6) != false);

    // Fill the queue to test full condition
    for (0..4) |i| {
        queue.enqueue(@as(u64, i + 7));
    }

    // Queue should be full now (8 items total)
    try testing.expect(!queue.tryEnqueue(11));

    try testing.expectEqual(queue.len(), 8);
}

test "BoundedQueue empty queue behavior" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const Queue = Mpmc(u64);
    var queue = try Queue.init(allocator, 4);
    defer queue.deinit(allocator);

    // Test tryDequeue on empty queue
    try testing.expectEqual(@as(?u64, null), queue.tryDequeue());
}

test "BoundedQueue wraparound" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const Queue = Mpmc(u64);
    var queue = try Queue.init(allocator, 4);
    defer queue.deinit(allocator);

    // Fill and empty the queue multiple times to test wraparound
    for (0..3) |_| {
        queue.enqueue(10);
        queue.enqueue(20);
        queue.enqueue(30);
        queue.enqueue(40);

        try testing.expectEqual(@as(u64, 10), queue.dequeue());
        try testing.expectEqual(@as(u64, 20), queue.dequeue());
        try testing.expectEqual(@as(u64, 30), queue.dequeue());
        try testing.expectEqual(@as(u64, 40), queue.dequeue());
    }
}

test "BoundedQueue concurrent operations" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const Thread = std.Thread;

    for (0..100) |_| {
        const Queue = Mpmc(u64);
        var queue = try Queue.init(allocator, 16);
        defer queue.deinit(allocator);

        const num_items = 1000;

        // Producer thread
        const producer = try Thread.spawn(.{}, struct {
            fn f(q: *Queue) void {
                for (0..num_items) |i| {
                    q.enqueue(@as(u64, i));
                }
            }
        }.f, .{queue});

        // Consumer thread
        const consumer = try Thread.spawn(.{}, struct {
            fn f(q: *Queue) !void {
                errdefer std.process.exit(1);
                errdefer std.debug.print("BoundedQueue concurrent test iteration failed\n", .{});

                var sum: u64 = 0;
                for (0..num_items) |i| {
                    const j = q.dequeue();
                    sum +%= j;
                    try testing.expectEqual(@as(u64, i), j);
                }
                // Verify we got all items (sum of 0..999)
                const expected_sum = (num_items * (num_items - 1)) / 2;
                try testing.expectEqual(@as(u64, expected_sum), sum);
            }
        }.f, .{queue});

        producer.join();
        consumer.join();
        try testing.expectEqual(queue.len(), 0);
    }
}

test "BoundedQueue different types" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const Queue = Mpmc(i32);
    var queue = try Queue.init(allocator, 4);
    defer queue.deinit(allocator);

    queue.enqueue(-42);
    queue.enqueue(0);
    queue.enqueue(123);

    try testing.expectEqual(@as(i32, -42), queue.dequeue());
    try testing.expectEqual(@as(i32, 0), queue.dequeue());
    try testing.expectEqual(@as(i32, 123), queue.dequeue());
}

test "BoundedQueue capacity rounding" {
    const testing = std.testing;
    const allocator = testing.allocator;

    // Test that capacity is rounded up to power of 2
    const Queue = Mpmc(u64);
    var queue = try Queue.init(allocator, 5); // Should round up to 8
    defer queue.deinit(allocator);

    // Verify the capacity was rounded up
    try testing.expectEqual(@as(usize, 8), queue.capacity);

    // Should be able to enqueue 8 items
    for (0..8) |i| {
        queue.enqueue(@as(u64, i));
    }

    // 9th item should block (using regular enqueue to test blocking behavior)
    // We'll test this by using tryEnqueue which should return false when full
    try testing.expect(!queue.tryEnqueue(8));
}

test "BoundedQueue single producer multiple consumers" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const Thread = std.Thread;

    const Queue = Mpmc(u64);
    var queue = try Queue.init(allocator, 32);
    defer queue.deinit(allocator);

    const num_consumers = 4;
    const items_per_consumer = 250;
    const total_items = num_consumers * items_per_consumer;

    // Producer
    const producer = try Thread.spawn(.{}, struct {
        fn f(q: *Queue, count: usize) void {
            for (0..count) |i| {
                q.enqueue(@as(u64, i));
            }
        }
    }.f, .{ queue, total_items });

    // Consumers
    var consumers: [num_consumers]Thread = undefined;
    var consumed_counts: [num_consumers]usize = std.mem.zeroes([num_consumers]usize);

    for (&consumers, &consumed_counts) |*consumer, *count_ptr| {
        consumer.* = try Thread.spawn(.{}, struct {
            fn f(q: *Queue, count: *usize, items: usize) void {
                var local_count: usize = 0;
                while (local_count < items) {
                    if (q.tryDequeue() != null) {
                        local_count += 1;
                    }
                }
                count.* = local_count;
            }
        }.f, .{ queue, count_ptr, items_per_consumer });
    }

    producer.join();
    for (&consumers) |consumer| {
        consumer.join();
    }

    // Verify all items were consumed
    var total_consumed: usize = 0;
    for (consumed_counts) |count| {
        total_consumed += count;
    }
    try testing.expectEqual(total_items, total_consumed);
}
