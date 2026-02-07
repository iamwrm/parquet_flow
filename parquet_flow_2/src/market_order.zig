const std = @import("std");
const types = @import("parquet/types.zig");
const ColumnData = @import("parquet/column_writer.zig").ColumnData;

/// Fixed-size market order struct for ring buffer transport.
/// 128 bytes — fits in 2 cache lines, power-of-2 for ring buffer alignment.
pub const MarketOrder = struct {
    timestamp_ns: i64, // nanosecond epoch timestamp
    order_id: i64, // unique order ID
    price: f64, // price as double
    quantity: i32, // order quantity
    side: i32, // 0=BUY, 1=SELL
    order_type: i32, // 0=LIMIT, 1=MARKET, 2=CANCEL
    _pad0: i32 = 0,
    symbol: [16]u8, // ticker, null-padded
    symbol_len: u8,
    exchange: [8]u8, // exchange code
    exchange_len: u8,
    payload: [46]u8, // raw binary payload
    payload_len: u8,
    _pad1: [3]u8 = .{ 0, 0, 0 },

    pub fn init(
        ts: i64,
        oid: i64,
        px: f64,
        qty: i32,
        s: i32,
        ot: i32,
        sym: []const u8,
        exch: []const u8,
    ) MarketOrder {
        var o: MarketOrder = .{
            .timestamp_ns = ts,
            .order_id = oid,
            .price = px,
            .quantity = qty,
            .side = s,
            .order_type = ot,
            .symbol = undefined,
            .symbol_len = 0,
            .exchange = undefined,
            .exchange_len = 0,
            .payload = undefined,
            .payload_len = 0,
        };
        const slen: u8 = @intCast(@min(sym.len, 16));
        @memcpy(o.symbol[0..slen], sym[0..slen]);
        if (slen < 16) @memset(o.symbol[slen..], 0);
        o.symbol_len = slen;

        const elen: u8 = @intCast(@min(exch.len, 8));
        @memcpy(o.exchange[0..elen], exch[0..elen]);
        if (elen < 8) @memset(o.exchange[elen..], 0);
        o.exchange_len = elen;

        @memset(&o.payload, 0);
        return o;
    }

    pub fn getSymbol(self: *const MarketOrder) []const u8 {
        return self.symbol[0..self.symbol_len];
    }

    pub fn getExchange(self: *const MarketOrder) []const u8 {
        return self.exchange[0..self.exchange_len];
    }

    pub fn getPayload(self: *const MarketOrder) []const u8 {
        return self.payload[0..self.payload_len];
    }
};

pub const MARKET_ORDER_SCHEMA = [_]types.ColumnSpec{
    .{ .name = "timestamp_ns", .physical_type = .INT64, .repetition_type = .REQUIRED },
    .{ .name = "order_id", .physical_type = .INT64, .repetition_type = .REQUIRED },
    .{ .name = "price", .physical_type = .DOUBLE, .repetition_type = .REQUIRED },
    .{ .name = "quantity", .physical_type = .INT32, .repetition_type = .REQUIRED },
    .{ .name = "side", .physical_type = .INT32, .repetition_type = .REQUIRED },
    .{ .name = "order_type", .physical_type = .INT32, .repetition_type = .REQUIRED },
    .{ .name = "symbol", .physical_type = .BYTE_ARRAY, .repetition_type = .REQUIRED, .converted_type = .UTF8 },
    .{ .name = "exchange", .physical_type = .BYTE_ARRAY, .repetition_type = .REQUIRED, .converted_type = .UTF8 },
};

/// Extract columnar data from a batch of MarketOrder structs.
/// Caller must call freeExtracted() when done.
pub const Extracted = struct {
    timestamps: []i64,
    order_ids: []i64,
    prices: []f64,
    quantities: []i32,
    sides: []i32,
    order_types: []i32,
    symbols: [][]const u8,
    exchanges: [][]const u8,

    pub fn toColumnData(self: *const Extracted) [8]ColumnData {
        return .{
            .{ .i64_values = self.timestamps },
            .{ .i64_values = self.order_ids },
            .{ .f64_values = self.prices },
            .{ .i32_values = self.quantities },
            .{ .i32_values = self.sides },
            .{ .i32_values = self.order_types },
            .{ .byte_array_values = self.symbols },
            .{ .byte_array_values = self.exchanges },
        };
    }

    pub fn deinit(self: *Extracted, allocator: std.mem.Allocator) void {
        allocator.free(self.timestamps);
        allocator.free(self.order_ids);
        allocator.free(self.prices);
        allocator.free(self.quantities);
        allocator.free(self.sides);
        allocator.free(self.order_types);
        allocator.free(self.symbols);
        allocator.free(self.exchanges);
    }
};

pub fn extractColumns(allocator: std.mem.Allocator, orders: []const MarketOrder) !Extracted {
    const n = orders.len;
    var ext: Extracted = .{
        .timestamps = try allocator.alloc(i64, n),
        .order_ids = try allocator.alloc(i64, n),
        .prices = try allocator.alloc(f64, n),
        .quantities = try allocator.alloc(i32, n),
        .sides = try allocator.alloc(i32, n),
        .order_types = try allocator.alloc(i32, n),
        .symbols = try allocator.alloc([]const u8, n),
        .exchanges = try allocator.alloc([]const u8, n),
    };

    for (orders, 0..) |*o, i| {
        ext.timestamps[i] = o.timestamp_ns;
        ext.order_ids[i] = o.order_id;
        ext.prices[i] = o.price;
        ext.quantities[i] = o.quantity;
        ext.sides[i] = o.side;
        ext.order_types[i] = o.order_type;
        ext.symbols[i] = o.getSymbol();
        ext.exchanges[i] = o.getExchange();
    }

    return ext;
}

test "MarketOrder size and init" {
    const o = MarketOrder.init(
        1_000_000_000,
        12345,
        150.75,
        100,
        0,
        0,
        "AAPL",
        "NYSE",
    );
    try std.testing.expectEqual(@as(i64, 1_000_000_000), o.timestamp_ns);
    try std.testing.expectEqual(@as(f64, 150.75), o.price);
    try std.testing.expectEqualSlices(u8, "AAPL", o.getSymbol());
    try std.testing.expectEqualSlices(u8, "NYSE", o.getExchange());
    try std.testing.expect(@sizeOf(MarketOrder) <= 128);
}
