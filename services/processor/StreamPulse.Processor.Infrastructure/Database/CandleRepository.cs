using Microsoft.Extensions.Logging;
using Npgsql;
using StreamPulse.Processor.Application.Interfaces;
using StreamPulse.Processor.Application.Models;
using StreamPulse.Processor.Application.Settings;

namespace StreamPulse.Processor.Infrastructure.Database;

public sealed class CandleRepository : ICandleRepository
{
    private readonly ILogger<CandleRepository> _logger;
    private readonly string _connectionString;

    private const string UpsertSql = """
        INSERT INTO ohlcv_candles
            (candle_time, symbol, open_price, high_price, low_price, close_price, total_volume, tick_count)
        VALUES
            (@candleTime, @symbol, @openPrice, @highPrice, @lowPrice, @closePrice, @totalVolume, @tickCount)
        ON CONFLICT (candle_time, symbol) DO UPDATE SET
            close_price  = EXCLUDED.close_price,
            total_volume = EXCLUDED.total_volume,
            tick_count   = EXCLUDED.tick_count;
        """;

    private const string SelectSql = """
        SELECT candle_time, symbol, open_price, high_price, low_price, close_price, total_volume, tick_count
        FROM ohlcv_candles
        WHERE symbol = @symbol AND candle_time BETWEEN @from AND @to
        ORDER BY candle_time ASC;
        """;

    public CandleRepository(ILogger<CandleRepository> logger, DatabaseSettings settings)
    {
        _logger = logger;
        _connectionString = settings.ConnectionString;
    }

    public async Task SaveCandleAsync(OhlcvCandle candle, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(UpsertSql, conn);
        cmd.Parameters.AddWithValue("candleTime", candle.CandleTime.UtcDateTime);
        cmd.Parameters.AddWithValue("symbol",     candle.Symbol);
        cmd.Parameters.AddWithValue("openPrice",  candle.OpenPrice);
        cmd.Parameters.AddWithValue("highPrice",  candle.HighPrice);
        cmd.Parameters.AddWithValue("lowPrice",   candle.LowPrice);
        cmd.Parameters.AddWithValue("closePrice", candle.ClosePrice);
        cmd.Parameters.AddWithValue("totalVolume", candle.TotalVolume);
        cmd.Parameters.AddWithValue("tickCount",  candle.TickCount);

        await cmd.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogInformation(
            "[DB] Saved {Symbol} candle for {CandleTime:u}",
            candle.Symbol, candle.CandleTime);
    }

    private const string RecentSql = """
        SELECT symbol, candle_time, open_price, high_price, low_price, close_price, total_volume, tick_count
        FROM ohlcv_candles
        WHERE symbol = @symbol
        ORDER BY candle_time DESC
        LIMIT @limit
        """;

    public async Task<IEnumerable<OhlcvCandle>> GetRecentCandlesAsync(
        string symbol, int limit, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(RecentSql, conn);
        cmd.Parameters.AddWithValue("symbol", symbol);
        cmd.Parameters.AddWithValue("limit",  limit);

        var results = new List<OhlcvCandle>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(new OhlcvCandle
            {
                Symbol      = reader.GetString(0),
                CandleTime  = new DateTimeOffset(reader.GetDateTime(1), TimeSpan.Zero),
                OpenPrice   = reader.GetDecimal(2),
                HighPrice   = reader.GetDecimal(3),
                LowPrice    = reader.GetDecimal(4),
                ClosePrice  = reader.GetDecimal(5),
                TotalVolume = reader.GetInt64(6),
                TickCount   = reader.GetInt32(7),
            });
        }
        results.Reverse(); // DESC → ASC for chart
        return results;
    }

    public async Task<IEnumerable<OhlcvCandle>> GetCandlesAsync(
        string symbol, DateTimeOffset from, DateTimeOffset to, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(SelectSql, conn);
        cmd.Parameters.AddWithValue("symbol", symbol);
        cmd.Parameters.AddWithValue("from",   from.UtcDateTime);
        cmd.Parameters.AddWithValue("to",     to.UtcDateTime);

        var results = new List<OhlcvCandle>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(new OhlcvCandle
            {
                CandleTime   = new DateTimeOffset(reader.GetDateTime(0), TimeSpan.Zero),
                Symbol       = reader.GetString(1),
                OpenPrice    = reader.GetDecimal(2),
                HighPrice    = reader.GetDecimal(3),
                LowPrice     = reader.GetDecimal(4),
                ClosePrice   = reader.GetDecimal(5),
                TotalVolume  = reader.GetInt64(6),
                TickCount    = reader.GetInt32(7),
            });
        }
        return results;
    }
}
