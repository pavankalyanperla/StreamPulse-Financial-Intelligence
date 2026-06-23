using FluentAssertions;
using Npgsql;

namespace StreamPulse.Integration.Tests;

[TestFixture]
public class PipelineTraceTests
{
    private const string ConnStr =
        "Host=localhost;Port=5432;Database=streampulse_db;Username=streampulse;Password=StreamPulse@2026";

    private static readonly string[] KnownSymbols = ["AAPL", "GOOGL", "MSFT", "INFY", "TCS"];

    // 90 s total: 65 s wait + ~25 s overhead
    [Test, CancelAfter(90_000)]
    public async Task Full_Tick_Pipeline_Trace()
    {
        // a) Baseline row counts
        await using var conn1 = new NpgsqlConnection(ConnStr);
        await conn1.OpenAsync();

        long aaplBefore  = await ScalarLongAsync(conn1, "SELECT COUNT(*) FROM ohlcv_candles WHERE symbol = 'AAPL'");
        long alertBefore = await ScalarLongAsync(conn1, "SELECT COUNT(*) FROM alert_records");

        TestContext.Progress.WriteLine($"[TRACE] Baseline — AAPL candles: {aaplBefore}, alert_records: {alertBefore}");
        TestContext.Progress.WriteLine("[TRACE] Waiting 65 s for one full candle interval...");

        // c) Wait one OHLCV candle cycle
        await Task.Delay(65_000);

        // d-e) After counts
        await using var conn2 = new NpgsqlConnection(ConnStr);
        await conn2.OpenAsync();

        long aaplAfter  = await ScalarLongAsync(conn2, "SELECT COUNT(*) FROM ohlcv_candles WHERE symbol = 'AAPL'");
        long alertAfter = await ScalarLongAsync(conn2, "SELECT COUNT(*) FROM alert_records");

        TestContext.Progress.WriteLine($"[TRACE] After    — AAPL candles: {aaplAfter}, alert_records: {alertAfter}");

        aaplAfter.Should().BeGreaterThan(aaplBefore,
            "ingestion + processor must write at least 1 new AAPL candle in 65 s");
        // Anomaly service does not guarantee new alerts every 65 s (alerts only fire when
        // the Isolation Forest flags a tick). Assert the table is non-empty instead.
        alertAfter.Should().BeGreaterThan(0,
            "anomaly service must have written at least one alert to alert_records");

        // f) Sanity-check the latest AAPL candle
        await using var cmd = conn2.CreateCommand();
        cmd.CommandText =
            "SELECT open_price, close_price, total_volume FROM ohlcv_candles " +
            "WHERE symbol = 'AAPL' ORDER BY candle_time DESC LIMIT 1";
        await using var rdr = await cmd.ExecuteReaderAsync();

        (await rdr.ReadAsync()).Should().BeTrue("must have at least one AAPL candle in the database");

        decimal open = rdr.GetDecimal(0);
        decimal close = rdr.GetDecimal(1);
        long    vol   = rdr.GetInt64(2);

        open.Should().BeGreaterThan(0, "open_price must be a positive price");
        close.Should().BeGreaterThan(0, "close_price must be a positive price");
        vol.Should().BeGreaterThan(0,  "total_volume must be positive");

        TestContext.Progress.WriteLine($"[TRACE] Latest AAPL candle — open={open:F4} close={close:F4} vol={vol}");
        TestContext.Progress.WriteLine("[TRACE] Full pipeline trace PASSED");
    }

    [Test, CancelAfter(15_000)]
    public async Task Sentiment_Data_Persisted()
    {
        await using var conn = new NpgsqlConnection(ConnStr);
        await conn.OpenAsync();

        // a) At least one row per symbol (5 minimum)
        long total = await ScalarLongAsync(conn, "SELECT COUNT(*) FROM sentiment_scores");
        total.Should().BeGreaterThanOrEqualTo(5,
            "sentiment service must have scored at least one headline per symbol");

        // b) All 5 symbols present — reader disposed before next command (Npgsql: one active command per connection)
        var found = new List<string>();
        {
            await using var symCmd = conn.CreateCommand();
            symCmd.CommandText = "SELECT DISTINCT symbol FROM sentiment_scores";
            await using var rdr = await symCmd.ExecuteReaderAsync();
            while (await rdr.ReadAsync()) found.Add(rdr.GetString(0));
        }

        foreach (var sym in KnownSymbols)
            found.Should().Contain(sym, $"symbol {sym} must have at least one sentiment row");

        TestContext.Progress.WriteLine($"[TRACE] Sentiment symbols: {string.Join(", ", found)}");

        // c) Score range [-1, 1] — new scope so previous reader is fully disposed
        {
            await using var scoreCmd = conn.CreateCommand();
            scoreCmd.CommandText = "SELECT score FROM sentiment_scores";
            await using var scoreRdr = await scoreCmd.ExecuteReaderAsync();
            while (await scoreRdr.ReadAsync())
            {
                decimal s = scoreRdr.GetDecimal(0);
                s.Should().BeInRange(-1m, 1m, $"score {s} is out of expected [-1, 1] range");
            }
        }

        TestContext.Progress.WriteLine("[TRACE] All sentiment scores in [-1.0, 1.0]");
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private static async Task<long> ScalarLongAsync(NpgsqlConnection conn, string sql)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }
}
