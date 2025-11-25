//! By convention, root.zig is the root source file when making a library.
const std = @import("std");

pub const Chan = @import("chan/chan.zig").Chan;
pub const ChanErr = @import("chan/chan.zig").ChanErr;
