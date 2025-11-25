const std = @import("std");
const queue = @import("queue.zig");
const testing = std.testing;
const ChanErr = @import("errs.zig").ChanErr;

pub fn Receiver(comptime T: type) type {
    return struct {
        ch: *const Chan(T),

        pub fn recv(self: @This()) ?T {
            return @constCast(self.ch).recv();
        }
        pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
            @constCast(self.ch).release(allocator);
        }
        pub fn clone(self: @This()) @This() {
            _ = @constCast(self.ch).ref_cnt.fetchAdd(1, .acq_rel);
            return self;
        }
    };
}

pub fn Sender(comptime T: type) type {
    return struct {
        ch: *const Chan(T),

        pub fn send(self: @This(), v: T) void {
            return @constCast(self.ch).send(v);
        }
        pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
            const p = @constCast(self.ch);
            if (p.tx_ref_cnt.fetchSub(1, .acq_rel) == 1) {
                p.close();
            }
            p.release(allocator);
        }
        pub fn clone(self: @This()) @This() {
            const p = @constCast(self.ch);
            _ = p.ref_cnt.fetchAdd(1, .acq_rel);
            _ = p.tx_ref_cnt.fetchAdd(1, .acq_rel);
            return self;
        }
    };
}

pub fn Chan(comptime T: type) type {
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

        ref_cnt: std.atomic.Value(usize) align(64) = .init(2),
        tx_ref_cnt: std.atomic.Value(usize) align(64) = .init(1),
        tx_waits: std.atomic.Value(u32) align(64) = .init(0),
        rx_waits: std.atomic.Value(u32) align(64) = .init(0),
        tx_key: std.atomic.Value(u32) align(64) = .init(0),
        rx_key: std.atomic.Value(u32) align(64) = .init(0),

        closed: std.atomic.Value(bool) align(64) = .init(false),

        queue: *queue.BoundedQueue(T),

        pub fn init(allocator: std.mem.Allocator, capacity: usize) !Pipe {
            const self = try allocator.create(Self);
            errdefer allocator.destroy(self);
            self.* = .{
                .queue = try queue.BoundedQueue(T).init(allocator, capacity),
            };

            return .{
                .tx = .{ .ch = self },
                .rx = .{ .ch = self },
            };
        }

        fn release(self: *Self, allocator: std.mem.Allocator) void {
            const prev = self.ref_cnt.fetchSub(1, .acq_rel);
            if (prev == 1) {
                self.deinit(allocator);
            }
        }

        fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.queue.deinit(allocator);
            allocator.destroy(self);
        }

        fn close(self: *Self) void {
            self.closed.store(true, .release);
            self.wakeAll(&self.rx_key, &self.rx_waits);
            self.wakeAll(&self.tx_key, &self.tx_waits); // optional for robustness
        }

        fn send(self: *Self, v: T) void {
            var spins: u32 = 1;
            while (true) {
                if (self.queue.tryEnqueue(v)) {
                    self.notifyReceiver();
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

                const key_snapshot = self.tx_key.load(.acquire);

                if (self.queue.tryEnqueue(v)) {
                    _ = self.tx_waits.fetchSub(1, .release);
                    return self.notifyReceiver();
                }

                // NOTE: --> if receiver recv something here, futex will not sleep, because key changed
                std.Thread.Futex.wait(@ptrCast(&self.tx_key), key_snapshot);
                _ = self.tx_waits.fetchSub(1, .release);
            }
        }

        fn recv(self: *Self) ?T {
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
                _ = self.rx_waits.fetchSub(1, .release);
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
    var pair = try Chan(i32).init(allocator, 10);
    pair.deinit(allocator);
}

test "basic send and receive" {
    const allocator = std.testing.allocator;
    var pair = try Chan(i32).init(allocator, 5);
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

test "channel cloning" {
    const allocator = std.testing.allocator;
    var pair = try Chan(i32).init(allocator, 5);
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
    var pair = try Chan(i32).init(allocator, 5);

    // Release sender (this should close the channel)
    pair.tx.deinit(allocator);

    // Try to receive from closed channel
    try testing.expectEqual(@as(?i32, null), pair.rx.recv());

    // Now release receiver
    pair.rx.deinit(allocator);
}

test "multiple senders closing" {
    const allocator = std.testing.allocator;
    var pair = try Chan(i32).init(allocator, 5);

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
        var pair = try Chan(f64).init(allocator, 5);
        defer pair.deinit(allocator);

        pair.tx.send(3.14);
        try testing.expectEqual(@as(f64, 3.14), pair.rx.recv());
    }

    {
        var pair = try Chan([]const u8).init(allocator, 5);
        defer pair.deinit(allocator);

        pair.tx.send("hello");
        try testing.expectEqualStrings("hello", pair.rx.recv().?);
    }
}

test "concurrent send and receive" {
    const allocator = std.testing.allocator;
    var pipe = try Chan(u64).init(allocator, 100);

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

test "stress test high contention" {
    const allocator = std.testing.allocator;
    var pipe = try Chan(u16).init(allocator, 5); // Small buffer to create contention

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
