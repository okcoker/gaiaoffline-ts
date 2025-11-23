// How to run:
// zig run test.zig -O ReleaseFast

const std = @import("std");

pub fn main() !void {
    const file_path = "./test.csv.gz";

    std.debug.print("Reading: {s}\n", .{file_path});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const start_time = std.time.nanoTimestamp();

    // Open and read the gzipped file
    const file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();

    // Execute gunzip to decompress the file (fastest method)
    const argv = [_][]const u8{ "gunzip", "-c", file_path };
    var child = std.process.Child.init(&argv, allocator);
    child.stdout_behavior = .Pipe;

    try child.spawn();

    // Read the decompressed output
    const csv_data = try child.stdout.?.readToEndAlloc(allocator, std.math.maxInt(usize));
    defer allocator.free(csv_data);

    _ = try child.wait();

    // Count rows (simple line counting, ignoring comments)
    var count: u64 = 0;
    var lines = std.mem.splitScalar(u8, csv_data, '\n');

    var is_first_line = true;
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        if (line[0] == '#') continue;
        if (is_first_line) {
            is_first_line = false;
            continue; // Skip header
        }
        count += 1;
    }

    const end_time = std.time.nanoTimestamp();
    const duration_secs = @as(f64, @floatFromInt(end_time - start_time)) / 1_000_000_000.0;

    std.debug.print("\nParsed {d} rows in {d:.2}s\n", .{ count, duration_secs });
    const rate = @as(u64, @intFromFloat(@as(f64, @floatFromInt(count)) / duration_secs));
    std.debug.print("Rate: {d} rows/sec\n", .{rate});
}
