const std = @import("std");
const zbeam = @import("src/root.zig");

const Bounded = zbeam.Bounded;
const ChanKind = @import("src/chan/bounded.zig").ChanKind;
const Options = @import("src/chan/bounded.zig").Options;

const NUM_ITERATIONS = 1_000_000;
const BUFFER_SIZE = 100;

// Benchmark function type
const BenchmarkFn = fn (std.mem.Allocator) anyerror!u64;

// Helper function to measure execution time
fn benchmark(allocator: std.mem.Allocator, name: []const u8, func: BenchmarkFn) !void {
    const start = std.time.nanoTimestamp();
    const operations = try func(allocator);
    const end = std.time.nanoTimestamp();
    const elapsed_ns = @as(u64, @intCast(end - start));
    const ops_per_second = @divTrunc(operations * 1_000_000_000, elapsed_ns);

    std.debug.print("{s}: {d} ns, {d} ops/sec\n", .{ name, elapsed_ns, ops_per_second });
}

// Benchmark MPMC channel - single threaded
fn bench_mpmc_single_threaded(allocator: std.mem.Allocator) !u64 {
    const tx, const rx = try Bounded(u64, .{ .kind = .MPMC }).init(allocator, BUFFER_SIZE);
    defer {
        tx.deinit(allocator);
        rx.deinit(allocator);
    }

    var i: u64 = 0;
    while (i < NUM_ITERATIONS) : (i += 1) {
        tx.send(i);
        _ = rx.recv();
    }
    return NUM_ITERATIONS;
}

// Benchmark MPSC channel - single threaded
fn bench_mpsc_single_threaded(allocator: std.mem.Allocator) !u64 {
    const tx, const rx = try Bounded(u64, .{ .kind = .MPSC }).init(allocator, BUFFER_SIZE);
    defer {
        tx.deinit(allocator);
        rx.deinit(allocator);
    }

    var i: u64 = 0;
    while (i < NUM_ITERATIONS) : (i += 1) {
        tx.send(i);
        _ = rx.recv();
    }
    return NUM_ITERATIONS;
}

// Benchmark SPSC channel - single threaded
fn bench_spsc_single_threaded(allocator: std.mem.Allocator) !u64 {
    const tx, const rx = try Bounded(u64, .{ .kind = .SPSC }).init(allocator, BUFFER_SIZE);
    defer {
        tx.deinit(allocator);
        rx.deinit(allocator);
    }

    var i: u64 = 0;
    while (i < NUM_ITERATIONS) : (i += 1) {
        tx.send(i);
        _ = rx.recv();
    }
    return NUM_ITERATIONS;
}

// Benchmark MPMC channel - multi threaded (multiple producers, multiple consumers)
fn bench_mpmc_multi_threaded(allocator: std.mem.Allocator) !u64 {
    const tx, const rx = try Bounded(u64, .{ .kind = .MPMC }).init(allocator, BUFFER_SIZE);

    const NUM_PRODUCERS = 4;
    const NUM_CONSUMERS = 4;
    const OPS_PER_THREAD = NUM_ITERATIONS / NUM_PRODUCERS;

    var producers: [NUM_PRODUCERS]std.Thread = undefined;
    var consumers: [NUM_CONSUMERS]std.Thread = undefined;

    // Clone receivers for consumers (MPMC supports receiver cloning)
    var rx_clones: [NUM_CONSUMERS]Bounded(u64, .{ .kind = .MPMC }).Rx = undefined;
    for (&rx_clones) |*rx_clone| {
        rx_clone.* = rx.clone();
    }

    // Spawn producers
    for (&producers, 0..) |*producer, i| {
        producer.* = try std.Thread.spawn(.{}, struct {
            fn f(ally: std.mem.Allocator, sender: Bounded(u64, .{ .kind = .MPMC }).Tx, start_val: u64, count: u64) void {
                defer sender.deinit(ally);
                var j: u64 = 0;
                while (j < count) : (j += 1) {
                    sender.send(start_val + j);
                }
            }
        }.f, .{ allocator, tx.clone(), @as(u64, i) * OPS_PER_THREAD, OPS_PER_THREAD });
    }

    // Shared counter for received items
    var received_count = std.atomic.Value(u64).init(0);

    // Spawn consumers
    for (&consumers, &rx_clones, 0..) |*consumer, *rx_clone, i| {
        _ = i; // Fix unused capture
        consumer.* = try std.Thread.spawn(.{}, struct {
            fn f(ally: std.mem.Allocator, receiver: Bounded(u64, .{ .kind = .MPMC }).Rx, counter: *std.atomic.Value(u64), target: u64) void {
                defer receiver.deinit(ally);
                var local_count: u64 = 0;
                while (local_count < target) {
                    if (receiver.recv()) |value| {
                        _ = counter.fetchAdd(1, .acq_rel);
                        local_count += 1;
                        _ = value; // Use the value to avoid optimization
                    }
                }
            }
        }.f, .{ allocator, rx_clone.*, &received_count, OPS_PER_THREAD });
    }

    // Wait for producers to finish
    for (producers) |producer| {
        producer.join();
    }

    // Wait for consumers to finish
    for (consumers) |consumer| {
        consumer.join();
    }

    // Clean up original channel endpoints
    tx.deinit(allocator);
    rx.deinit(allocator);

    return NUM_ITERATIONS;
}

// Benchmark MPSC channel - multi threaded (multiple producers, single consumer)
fn bench_mpsc_multi_threaded(allocator: std.mem.Allocator) !u64 {
    const tx, const rx = try Bounded(u64, .{ .kind = .MPSC }).init(allocator, BUFFER_SIZE);

    const NUM_PRODUCERS = 4;
    const OPS_PER_THREAD = NUM_ITERATIONS / NUM_PRODUCERS;

    var producers: [NUM_PRODUCERS]std.Thread = undefined;

    // Clone senders for producers (MPSC supports sender cloning)
    var tx_clones: [NUM_PRODUCERS]Bounded(u64, .{ .kind = .MPSC }).Tx = undefined;
    for (&tx_clones) |*tx_clone| {
        tx_clone.* = tx.clone();
    }

    // Spawn producers
    for (&producers, &tx_clones, 0..) |*producer, *tx_clone, i| {
        producer.* = try std.Thread.spawn(.{}, struct {
            fn f(ally: std.mem.Allocator, sender: Bounded(u64, .{ .kind = .MPSC }).Tx, start_val: u64, count: u64) void {
                defer sender.deinit(ally);
                var j: u64 = 0;
                while (j < count) : (j += 1) {
                    sender.send(start_val + j);
                }
            }
        }.f, .{ allocator, tx_clone.*, @as(u64, i) * OPS_PER_THREAD, OPS_PER_THREAD });
    }

    // Single consumer - MPSC doesn't support receiver cloning, so we move the original
    var received_count: u64 = 0;
    const consumer = try std.Thread.spawn(.{}, struct {
        fn f(ally: std.mem.Allocator, receiver: Bounded(u64, .{ .kind = .MPSC }).Rx, counter: *u64, target: u64) void {
            defer receiver.deinit(ally);
            var local_count: u64 = 0;
            while (local_count < target) {
                if (receiver.recv()) |value| {
                    counter.* += 1;
                    local_count += 1;
                    _ = value; // Use the value to avoid optimization
                }
            }
        }
    }.f, .{ allocator, rx, &received_count, NUM_ITERATIONS });

    // Wait for producers to finish
    for (producers) |producer| {
        producer.join();
    }

    // Wait for consumer to finish
    consumer.join();

    // Clean up original sender (all clones are already cleaned up)
    tx.deinit(allocator);
    // rx is already deinit by consumer

    return NUM_ITERATIONS;
}

// Benchmark SPSC channel - multi threaded (single producer, single consumer)
fn bench_spsc_multi_threaded(allocator: std.mem.Allocator) !u64 {
    const tx, const rx = try Bounded(u64, .{ .kind = .SPSC }).init(allocator, BUFFER_SIZE);

    // Single producer - SPSC doesn't support cloning, so we move the original
    const producer = try std.Thread.spawn(.{}, struct {
        fn f(ally: std.mem.Allocator, sender: Bounded(u64, .{ .kind = .SPSC }).Tx, count: u64) void {
            defer sender.deinit(ally);
            var j: u64 = 0;
            while (j < count) : (j += 1) {
                sender.send(j);
            }
        }
    }.f, .{ allocator, tx, NUM_ITERATIONS });

    // Single consumer - SPSC doesn't support cloning, so we move the original
    var received_count: u64 = 0;
    const consumer = try std.Thread.spawn(.{}, struct {
        fn f(ally: std.mem.Allocator, receiver: Bounded(u64, .{ .kind = .SPSC }).Rx, counter: *u64, target: u64) void {
            defer receiver.deinit(ally);
            var local_count: u64 = 0;
            while (local_count < target) {
                if (receiver.recv()) |value| {
                    counter.* += 1;
                    local_count += 1;
                    _ = value; // Use the value to avoid optimization
                }
            }
        }
    }.f, .{ allocator, rx, &received_count, NUM_ITERATIONS });

    // Wait for threads to finish
    producer.join();
    consumer.join();

    return NUM_ITERATIONS;
}

// Benchmark throughput for different buffer sizes
fn bench_throughput_mpmc(allocator: std.mem.Allocator, buffer_size: usize) !u64 {
    const tx, const rx = try Bounded(u64, .{ .kind = .MPMC }).init(allocator, buffer_size);
    defer {
        tx.deinit(allocator);
        rx.deinit(allocator);
    }

    const OPS_PER_TEST = 100_000;
    var i: u64 = 0;
    while (i < OPS_PER_TEST) : (i += 1) {
        tx.send(i);
        _ = rx.recv();
    }
    return OPS_PER_TEST;
}

// Benchmark latency for different kinds
fn bench_latency_comparison(allocator: std.mem.Allocator) !void {
    const iterations = 10_000;

    std.debug.print("\nLatency comparison (lower is better):\n", .{});

    // Benchmark MPMC
    {
        const tx, const rx = try Bounded(u64, .{ .kind = .MPMC }).init(allocator, 1);
        defer {
            tx.deinit(allocator);
            rx.deinit(allocator);
        }

        const start = std.time.nanoTimestamp();
        var i: u64 = 0;
        while (i < iterations) : (i += 1) {
            tx.send(i);
            _ = rx.recv();
        }
        const end = std.time.nanoTimestamp();

        const total_ns = @as(u64, @intCast(end - start));
        const avg_latency_ns = total_ns / iterations;

        std.debug.print("  MPMC: {d} ns average per operation\n", .{avg_latency_ns});
    }

    // Benchmark MPSC
    {
        const tx, const rx = try Bounded(u64, .{ .kind = .MPSC }).init(allocator, 1);
        defer {
            tx.deinit(allocator);
            rx.deinit(allocator);
        }

        const start = std.time.nanoTimestamp();
        var i: u64 = 0;
        while (i < iterations) : (i += 1) {
            tx.send(i);
            _ = rx.recv();
        }
        const end = std.time.nanoTimestamp();

        const total_ns = @as(u64, @intCast(end - start));
        const avg_latency_ns = total_ns / iterations;

        std.debug.print("  MPSC: {d} ns average per operation\n", .{avg_latency_ns});
    }

    // Benchmark SPSC
    {
        const tx, const rx = try Bounded(u64, .{ .kind = .SPSC }).init(allocator, 1);
        defer {
            tx.deinit(allocator);
            rx.deinit(allocator);
        }

        const start = std.time.nanoTimestamp();
        var i: u64 = 0;
        while (i < iterations) : (i += 1) {
            tx.send(i);
            _ = rx.recv();
        }
        const end = std.time.nanoTimestamp();

        const total_ns = @as(u64, @intCast(end - start));
        const avg_latency_ns = total_ns / iterations;

        std.debug.print("  SPSC: {d} ns average per operation\n", .{avg_latency_ns});
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Channel Benchmarks\n", .{});
    std.debug.print("==================\n\n", .{});

    std.debug.print("Single-threaded benchmarks:\n", .{});
    try benchmark(allocator, "MPMC single-threaded", bench_mpmc_single_threaded);
    try benchmark(allocator, "MPSC single-threaded", bench_mpsc_single_threaded);
    try benchmark(allocator, "SPSC single-threaded", bench_spsc_single_threaded);

    std.debug.print("\nMulti-threaded benchmarks:\n", .{});
    try benchmark(allocator, "MPMC multi-threaded", bench_mpmc_multi_threaded);
    try benchmark(allocator, "MPSC multi-threaded", bench_mpsc_multi_threaded);
    try benchmark(allocator, "SPSC multi-threaded", bench_spsc_multi_threaded);

    std.debug.print("\nThroughput comparison (different buffer sizes):\n", .{});
    try benchmark(allocator, "MPMC buffer_size=1", bench_throughput_mpmc_size1);
    try benchmark(allocator, "MPMC buffer_size=10", bench_throughput_mpmc_size10);
    try benchmark(allocator, "MPMC buffer_size=100", bench_throughput_mpmc_size100);
    try benchmark(allocator, "MPMC buffer_size=1000", bench_throughput_mpmc_size1000);

    try bench_latency_comparison(allocator);

    std.debug.print("\nBenchmarks completed!\n", .{});
}

fn bench_throughput_mpmc_size1(allocator: std.mem.Allocator) !u64 {
    return bench_throughput_mpmc(allocator, 1);
}

fn bench_throughput_mpmc_size10(allocator: std.mem.Allocator) !u64 {
    return bench_throughput_mpmc(allocator, 10);
}

fn bench_throughput_mpmc_size100(allocator: std.mem.Allocator) !u64 {
    return bench_throughput_mpmc(allocator, 100);
}

fn bench_throughput_mpmc_size1000(allocator: std.mem.Allocator) !u64 {
    return bench_throughput_mpmc(allocator, 1000);
}
