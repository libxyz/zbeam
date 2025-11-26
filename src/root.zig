//! By convention, root.zig is the root source file when making a library.
const std = @import("std");

pub const Bounded = @import("chan/chan.zig").Bounded;
pub const Receiver = @import("chan/chan.zig").Receiver;
pub const Sender = @import("chan/chan.zig").Sender;

pub const ChanErr = @import("chan/errs.zig").ChanErr;

test {
    // 这行代码会让 Zig 递归地去检查并运行上面所有被引用(public)容器里的测试
    std.testing.refAllDecls(@This());
}
