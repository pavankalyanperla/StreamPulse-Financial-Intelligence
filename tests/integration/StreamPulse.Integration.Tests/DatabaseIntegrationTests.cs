using FluentAssertions;
using Npgsql;

namespace StreamPulse.Integration.Tests;

[TestFixture]
public class DatabaseIntegrationTests
{
    private const string ConnStr =
        "Host=localhost;Port=5432;Database=streampulse_db;Username=streampulse;Password=StreamPulse@2026";

    private static readonly string[] KnownSymbols       = ["AAPL", "GOOGL", "MSFT", "INFY", "TCS"];
    private static readonly string[] ExpectedHypertables = ["ohlcv_candles", "sentiment_scores", "alert_records"];

    // ── schema ────────────────────────────────────────────────────────────────

    [Test, CancelAfter(15_000)]
    public async Task TimescaleDB_Hypertables_Exist()
    {
        await using var conn = await OpenAsync();
        await using var cmd  = new NpgsqlCommand(
            "SELECT hypertable_name FROM timescaledb_information.hypertables", conn);
        await using var rdr  = await cmd.ExecuteReaderAsync();

        var found = new List<string>();
        while (await rdr.ReadAsync()) found.Add(rdr.GetString(0));

        foreach (var table in ExpectedHypertables)
            found.Should().Contain(table, $"hypertable '{table}' must exist in TimescaleDB");

        TestContext.Progress.WriteLine($"[DB] Hypertables: {string.Join(", ", found)}");
    }

    // ── ohlcv_candles ─────────────────────────────────────────────────────────

    [Test, CancelAfter(15_000)]
    public async Task OhlcvCandles_Have_Recent_Data()
    {
        await using var conn  = await OpenAsync();
        await using var cmd   = new NpgsqlCommand(
            "SELECT COUNT(*) FROM ohlcv_candles WHERE candle_time > NOW() - INTERVAL '1 hour'", conn);
        long count = (long)(await cmd.ExecuteScalarAsync())!;

        count.Should().BeGreaterThan(0,
            "stream processor must have written OHLCV candles in the last hour");

        TestContext.Progress.WriteLine($"[DB] OHLCV candles in last hour: {count}");
    }

    [Test, CancelAfter(15_000)]
    public async Task TimeBucket_Query_Works()
    {
        await using var conn = await OpenAsync();
        await using var cmd  = new NpgsqlCommand("""
            SELECT time_bucket('5 minutes', candle_time) AS bucket,
                   symbol,
                   AVG(close_price)::NUMERIC AS avg_close
            FROM ohlcv_candles
            GROUP BY bucket, symbol
            ORDER BY bucket DESC
            LIMIT 5
            """, conn);
        await using var rdr = await cmd.ExecuteReaderAsync();

        int rows = 0;
        while (await rdr.ReadAsync())
        {
            decimal avgClose = rdr.GetDecimal(2);
            avgClose.Should().BeGreaterThan(0,
                $"avg_close in time-bucket row {rows + 1} must be a positive price");
            rows++;
        }

        rows.Should().BeGreaterThan(0, "time_bucket() query must return at least one row");
        TestContext.Progress.WriteLine($"[DB] time_bucket query returned {rows} rows");
    }

    // ── alert_records ─────────────────────────────────────────────────────────

    [Test, CancelAfter(15_000)]
    public async Task AlertStats_Query_Works()
    {
        await using var conn = await OpenAsync();
        await using var cmd  = new NpgsqlCommand(
            "SELECT symbol, COUNT(*) FROM alert_records GROUP BY symbol", conn);
        await using var rdr  = await cmd.ExecuteReaderAsync();

        var stats = new Dictionary<string, long>();
        while (await rdr.ReadAsync())
            stats[rdr.GetString(0)] = rdr.GetInt64(1);

        foreach (var sym in KnownSymbols)
        {
            stats.Should().ContainKey(sym,
                $"alert_records must have at least one row for symbol {sym}");
            stats[sym].Should().BeGreaterThan(0,
                $"alert count for {sym} must be positive");
        }

        TestContext.Progress.WriteLine(
            $"[DB] Alert counts: {string.Join(", ", stats.Select(kv => $"{kv.Key}={kv.Value}"))}");
    }

    // ── helper ────────────────────────────────────────────────────────────────

    private static async Task<NpgsqlConnection> OpenAsync()
    {
        var conn = new NpgsqlConnection(ConnStr);
        await conn.OpenAsync();
        return conn;
    }
}
