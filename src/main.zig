const std = @import("std");
const zbeam = @import("zbeam");
const zbench = @import("zbench");

pub fn main() !void {
    var bench = zbench.Benchmark.init(std.heap.page_allocator, .{});
    defer bench.deinit();
    try bench.add("My Benchmark", bmSPSC, .{});
    var buf: [1024]u8 = undefined;

    var stdout_ = std.fs.File.stdout().writer(&buf);
    const writer = &stdout_.interface;
    defer writer.flush() catch {};

    try bench.run(writer);
}

fn myBenchmark(allocator: std.mem.Allocator) void {
    // Code to benchmark here
    _ = allocator;
}

fn bmSPSC(allocator: std.mem.Allocator) void {
    const Chan = zbeam.Bounded(usize, .{ .kind = .SPSC });
    const tx, const rx = Chan.init(allocator, 64) catch {
        return;
    };

    defer rx.deinit(allocator);

    _ = std.Thread.spawn(.{}, struct {
        fn f(ally: std.mem.Allocator, sender: Chan.Tx) void {
            defer sender.deinit(ally);

            var i: usize = 0;
            while (i < 100_000) : (i += 1) {
                _ = sender.send(i);
            }
        }
    }.f, .{ allocator, tx }) catch {
        tx.deinit(allocator);
        return;
    };

    while (rx.recv()) |value| {
        _ = value;
    }
}
