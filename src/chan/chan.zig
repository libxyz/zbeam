const std = @import("std");
const queue = @import("queue.zig");
const testing = std.testing;
const ChanErr = @import("errs.zig").ChanErr;

pub const ChanKind = enum {
    MPMC,
    MPSC,
    SPSC,
};

fn QueueType(comptime T: type, comptime kind: ChanKind) type {
    return switch (kind) {
        .MPMC => queue.Bounded(T, true, true),
        .MPSC => queue.Bounded(T, true, false),
        .SPSC => queue.Bounded(T, false, false),
    };
}

pub const Options = struct {
    kind: ChanKind = .MPMC,
};

pub fn Sender(comptime T: type) type {
    return struct {
        const VTable = struct {
            send: *const fn (self: *anyopaque, v: T) void,
            close: *const fn (self: *anyopaque) void,
            deinit: *const fn (self: *anyopaque, allocator: std.mem.Allocator) void,
        };

        ctx: *anyopaque,
        vtable: *const VTable,
        ref_cnt: *std.atomic.Value(usize) align(64),
        tx_ref_cnt: *std.atomic.Value(usize) align(64),

        pub fn send(self: @This(), v: T) void {
            return self.vtable.send(self.ctx, v);
        }

        pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
            if (self.tx_ref_cnt.fetchSub(1, .acq_rel) == 1) {
                self.vtable.close(self.ctx);
                allocator.destroy(self.tx_ref_cnt);
            }
            if (self.ref_cnt.fetchSub(1, .acq_rel) == 1) {
                self.vtable.deinit(self.ctx, allocator);
                allocator.destroy(self.ref_cnt);
            }
        }

        pub fn clone(self: @This()) @This() {
            _ = self.ref_cnt.fetchAdd(1, .acq_rel);
            _ = self.tx_ref_cnt.fetchAdd(1, .acq_rel);
            return self;
        }
    };
}

pub fn Receiver(comptime T: type) type {
    return struct {
        const VTable = struct {
            recv: *const fn (self: *anyopaque) ?T,
            deinit: *const fn (self: *anyopaque, allocator: std.mem.Allocator) void,
        };

        ctx: *anyopaque,
        vtable: *const VTable,
        ref_cnt: *std.atomic.Value(usize) align(64),

        pub fn recv(self: @This()) ?T {
            return self.vtable.recv(self.ctx);
        }

        pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
            if (self.ref_cnt.fetchSub(1, .acq_rel) == 1) {
                self.vtable.deinit(self.ctx, allocator);
                allocator.destroy(self.ref_cnt);
            }
        }

        pub fn clone(self: @This()) @This() {
            _ = self.ref_cnt.fetchAdd(1, .acq_rel);
            return self;
        }
    };
}

pub fn Bounded(comptime T: type, comptime opts: Options) type {
    return struct {
        const Self = @This();
        const Pipe = struct {
            tx: Sender(T),
            rx: Receiver(T),

            pub fn deinit(self: *Pipe, allocator: std.mem.Allocator) void {
                self.tx.deinit(allocator);
                self.rx.deinit(allocator);
            }
        };

        const MAX_SPINS: u32 = 512;

        const tx_vtable = &Sender(T).VTable{
            .send = send,
            .close = close,
            .deinit = deinit,
        };
        const rx_vtable = &Receiver(T).VTable{
            .recv = recv,
            .deinit = deinit,
        };

        tx_waits: std.atomic.Value(u32) align(64) = .init(0),
        rx_waits: std.atomic.Value(u32) align(64) = .init(0),
        tx_key: std.atomic.Value(u32) align(64) = .init(0),
        rx_key: std.atomic.Value(u32) align(64) = .init(0),

        closed: std.atomic.Value(bool) align(64) = .init(false),

        queue: *QueueType(T, opts.kind),
        unbuffered: bool = false,

        pub fn init(allocator: std.mem.Allocator, capacity: usize) !Pipe {
            const self = try allocator.create(Self);
            errdefer allocator.destroy(self);
            self.* = .{
                .queue = try QueueType(T, opts.kind).init(allocator, @max(capacity, 2)),
                .unbuffered = capacity == 0,
            };

            const ref_cnt = try allocator.create(std.atomic.Value(usize));
            errdefer allocator.destroy(ref_cnt);
            ref_cnt.store(2, .monotonic); // one for tx, one for rx

            const tx_ref_cnt = try allocator.create(std.atomic.Value(usize));
            errdefer allocator.destroy(tx_ref_cnt);
            tx_ref_cnt.store(1, .monotonic); // one for tx

            return .{
                .tx = .{
                    .ctx = self,
                    .vtable = tx_vtable,
                    .ref_cnt = ref_cnt,
                    .tx_ref_cnt = tx_ref_cnt,
                },
                .rx = .{
                    .ctx = self,
                    .vtable = rx_vtable,
                    .ref_cnt = ref_cnt,
                },
            };
        }

        fn deinit(ctx: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            self.queue.deinit(allocator);
            allocator.destroy(self);
        }

        fn close(ctx: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            self.closed.store(true, .release);
            self.wakeAll(&self.rx_key, &self.rx_waits);
            self.wakeAll(&self.tx_key, &self.tx_waits); // optional for robustness
        }

        fn send(ctx: *anyopaque, v: T) void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const ack_snapshot = if (self.unbuffered) self.tx_key.load(.acquire) else 0;
            var spins: u32 = 1;

            while (true) {
                if (self.queue.tryEnqueue(v)) {
                    if (self.unbuffered) {
                        _ = self.tx_waits.fetchAdd(1, .acq_rel);
                        defer _ = self.tx_waits.fetchSub(1, .release);

                        self.notifyReceiver();
                        std.Thread.Futex.wait(@ptrCast(&self.tx_key), ack_snapshot);
                    } else {
                        self.notifyReceiver();
                    }
                    return;
                }

                if (spins < MAX_SPINS) {
                    for (0..spins) |_| {
                        std.atomic.spinLoopHint();
                    }
                    spins = @min(spins * 2, MAX_SPINS);
                    continue;
                }

                spins = 1;

                _ = self.tx_waits.fetchAdd(1, .acq_rel);
                defer _ = self.tx_waits.fetchSub(1, .release);

                const key_snapshot = self.tx_key.load(.acquire);

                if (self.queue.tryEnqueue(v)) {
                    self.notifyReceiver();
                    if (self.unbuffered) {
                        std.Thread.Futex.wait(@ptrCast(&self.tx_key), ack_snapshot);
                    }
                    return;
                }

                // NOTE: --> if receiver recv something here, futex will not sleep, because key changed
                std.Thread.Futex.wait(@ptrCast(&self.tx_key), key_snapshot);
            }
        }

        fn recv(ctx: *anyopaque) ?T {
            const self: *Self = @ptrCast(@alignCast(ctx));
            var spins: u32 = 1;

            while (true) {
                if (self.queue.tryDequeue()) |value| {
                    self.notifySender();
                    return value;
                }
                if (self.closed.load(.acquire)) {
                    return null;
                }
                if (spins < MAX_SPINS) {
                    for (0..spins) |_| {
                        std.atomic.spinLoopHint();
                    }
                    spins = @min(spins * 2, MAX_SPINS);
                    continue;
                }

                spins = 1;

                _ = self.rx_waits.fetchAdd(1, .acq_rel);
                defer _ = self.rx_waits.fetchSub(1, .release);

                const key_snapshot = self.rx_key.load(.acquire);

                // double-check after incrementing waiters
                if (self.queue.tryDequeue()) |value| {
                    _ = self.rx_waits.fetchSub(1, .release);
                    self.notifySender();
                    return value;
                }

                if (self.closed.load(.acquire)) {
                    _ = self.rx_waits.fetchSub(1, .release);
                    return null;
                }

                // NOTE: --> if sender send something here, futex will not sleep, because key changed
                std.Thread.Futex.wait(@ptrCast(&self.rx_key), key_snapshot);
            }
        }

        inline fn notifyReceiver(self: *Self) void {
            self.wake(&self.rx_key, &self.rx_waits);
        }

        inline fn notifySender(self: *Self) void {
            self.wake(&self.tx_key, &self.tx_waits);
        }

        inline fn wake(_: *Self, key: *std.atomic.Value(u32), waits: *std.atomic.Value(u32)) void {
            if (waits.load(.acquire) > 0) {
                _ = key.fetchAdd(1, .release);
                std.Thread.Futex.wake(@alignCast(key), 1);
            }
        }

        inline fn wakeAll(_: *Self, key: *std.atomic.Value(u32), waits: *std.atomic.Value(u32)) void {
            if (waits.load(.acquire) > 0) {
                _ = key.fetchAdd(1, .release);
                std.Thread.Futex.wake(@alignCast(key), std.math.maxInt(u32));
            }
        }
    };
}

test "init and deinit chan" {
    const allocator = std.testing.allocator;
    var pair = try Bounded(i32, .{}).init(allocator, 10);
    pair.deinit(allocator);
}

test "basic send and receive" {
    const allocator = std.testing.allocator;
    var pair = try Bounded(i32, .{}).init(allocator, 5);
    defer pair.deinit(allocator);

    // Test basic send/receive
    pair.tx.send(42);
    try testing.expectEqual(@as(i32, 42), pair.rx.recv());

    // Test multiple sends/receives
    pair.tx.send(1);
    pair.tx.send(2);
    pair.tx.send(3);

    try testing.expectEqual(@as(i32, 1), pair.rx.recv());
    try testing.expectEqual(@as(i32, 2), pair.rx.recv());
    try testing.expectEqual(@as(i32, 3), pair.rx.recv());
}

test "unbuffered channel" {
    const allocator = std.testing.allocator;

    for (0..100) |_| {
        var pair = try Bounded(i32, .{}).init(allocator, 0);
        defer pair.deinit(allocator);

        var sent_value: i32 = 0;

        // Spawn a thread to send a value
        const sender = try std.Thread.spawn(.{}, struct {
            fn f(ally: std.mem.Allocator, tx: Sender(i32), val: *i32) void {
                defer tx.deinit(ally);
                tx.send(99);
                val.* = 99;
            }
        }.f, .{ allocator, pair.tx.clone(), &sent_value });

        // Receive the value in the main thread
        const received = pair.rx.recv();
        try testing.expectEqual(@as(i32, 99), received.?);

        sender.join();
        try testing.expectEqual(@as(i32, 99), sent_value);
    }
}

test "channel cloning" {
    const allocator = std.testing.allocator;
    var pair = try Bounded(i32, .{}).init(allocator, 5);
    defer pair.deinit(allocator);

    // Clone sender
    var tx2 = pair.tx.clone();
    defer tx2.deinit(allocator);

    // Clone receiver
    var rx2 = pair.rx.clone();
    defer rx2.deinit(allocator);

    // Test that cloned channels work
    tx2.send(123);
    try testing.expectEqual(@as(i32, 123), rx2.recv());
}

test "channel closing" {
    const allocator = std.testing.allocator;
    var pair = try Bounded(i32, .{}).init(allocator, 5);

    // Release sender (this should close the channel)
    pair.tx.deinit(allocator);

    // Try to receive from closed channel
    try testing.expectEqual(@as(?i32, null), pair.rx.recv());

    // Now release receiver
    pair.rx.deinit(allocator);
}

test "multiple senders closing" {
    const allocator = std.testing.allocator;
    var pair = try Bounded(i32, .{}).init(allocator, 5);

    // Clone sender multiple times
    var tx2 = pair.tx.clone();
    var tx3 = pair.tx.clone();

    // Release original sender
    pair.tx.deinit(allocator);

    // Channel should still be open (tx2 and tx3 exist)
    tx2.send(42);
    try testing.expectEqual(@as(i32, 42), pair.rx.recv());

    // Release all senders
    tx2.deinit(allocator);
    tx3.deinit(allocator);

    // Now channel should be closed
    try testing.expectEqual(@as(?i32, null), pair.rx.recv());

    pair.rx.deinit(allocator);
}

test "different types" {
    const allocator = std.testing.allocator;

    // Test with different types
    {
        var pair = try Bounded(f64, .{}).init(allocator, 5);
        defer pair.deinit(allocator);

        pair.tx.send(3.14);
        try testing.expectEqual(@as(f64, 3.14), pair.rx.recv());
    }

    {
        var pair = try Bounded([]const u8, .{}).init(allocator, 5);
        defer pair.deinit(allocator);

        pair.tx.send("hello");
        try testing.expectEqualStrings("hello", pair.rx.recv().?);
    }
}

test "concurrent send and receive" {
    const allocator = std.testing.allocator;
    for (0..100) |_| {
        var pipe = try Bounded(u64, .{}).init(allocator, 100);

        const num_items = 1000;

        // Shared result storage
        var recv_sum: u64 = 0;

        // Spawn sender thread
        const sender = try std.Thread.spawn(.{}, struct {
            fn f(ally: std.mem.Allocator, tx: Sender(u64)) void {
                defer tx.deinit(ally);
                for (0..num_items) |i| {
                    tx.send(@as(u64, i));
                }
            }
        }.f, .{ allocator, pipe.tx.clone() });

        // Spawn receiver thread
        const receiver = try std.Thread.spawn(.{}, struct {
            fn f(ally: std.mem.Allocator, rx: Receiver(u64), sum: *u64) void {
                defer rx.deinit(ally);

                var local_sum: u64 = 0;
                var received_count: usize = 0;

                while (received_count < num_items) {
                    if (rx.recv()) |value| {
                        local_sum +%= value;
                        received_count += 1;
                    }
                }
                sum.* = local_sum;
            }
        }.f, .{ allocator, pipe.rx.clone(), &recv_sum });

        pipe.deinit(allocator);
        sender.join();
        receiver.join();

        // Verify we received all items with correct sum
        const expected_sum = (num_items * (num_items - 1)) / 2;
        try testing.expectEqual(expected_sum, recv_sum);
    }
}

test "stress test high contention" {
    const allocator = std.testing.allocator;
    var pipe = try Bounded(u16, .{}).init(allocator, 5); // Small buffer to create contention

    const num_threads = 10;
    const operations_per_sender = 100;

    // Shared result storage
    var total_sent: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);
    var total_received: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);

    // Spawn threads that both send and receive
    var threads: [num_threads]std.Thread = undefined;
    for (0..num_threads) |i| {
        threads[i] = try std.Thread.spawn(.{}, struct {
            fn f(ally: std.mem.Allocator, tx: Sender(u16), rx: Receiver(u16), sent: *std.atomic.Value(u32), received: *std.atomic.Value(u32), idx: usize) void {
                if (idx % 3 > 0) {
                    // Even indexed threads send first
                    rx.deinit(ally); // Even indexed threads send first
                    defer tx.deinit(ally);

                    for (0..operations_per_sender) |j| {
                        tx.send(@intCast(j));
                        _ = sent.fetchAdd(1, .acq_rel);
                    }
                } else {
                    tx.deinit(ally); // Odd indexed threads receive first
                    defer rx.deinit(ally);

                    while (rx.recv()) |_| {
                        _ = received.fetchAdd(1, .acq_rel);
                    }
                }
            }
        }.f, .{ allocator, pipe.tx.clone(), pipe.rx.clone(), &total_sent, &total_received, i });
    }

    // Clean up original pipe
    pipe.deinit(allocator);

    // Wait for all threads to finish
    for (&threads) |thread| {
        thread.join();
    }

    // In this stress test, we mainly verify that no deadlocks occurred
    // and that some operations completed successfully
    const sent_count = total_sent.load(.acquire);
    const received_count = total_received.load(.acquire);

    // We should have sent some items and received some items
    try testing.expect(sent_count > 0);
    try testing.expect(received_count > 0);
    try testing.expect(received_count == sent_count);
}
