using Microsoft.Extensions.Logging;
using Npgsql;
using StreamPulse.Processor.Application.Settings;

namespace StreamPulse.Processor.Infrastructure.Database;

public sealed class DatabaseInitializer
{
    private readonly ILogger<DatabaseInitializer> _logger;
    private readonly string _connectionString;

    private const string CreateTableSql = """
        CREATE TABLE IF NOT EXISTS ohlcv_candles (
            candle_time     TIMESTAMPTZ     NOT NULL,
            symbol          VARCHAR(10)     NOT NULL,
            open_price      NUMERIC(12,4)   NOT NULL,
            high_price      NUMERIC(12,4)   NOT NULL,
            low_price       NUMERIC(12,4)   NOT NULL,
            close_price     NUMERIC(12,4)   NOT NULL,
            total_volume    BIGINT          NOT NULL,
            tick_count      INT             NOT NULL,
            PRIMARY KEY (candle_time, symbol)
        );
        """;

    private const string CreateHypertableSql =
        "SELECT create_hypertable('ohlcv_candles', 'candle_time', if_not_exists => TRUE);";

    public DatabaseInitializer(ILogger<DatabaseInitializer> logger, DatabaseSettings settings)
    {
        _logger = logger;
        _connectionString = settings.ConnectionString;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("[DB] Initializing TimescaleDB schema...");

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using (var cmd = new NpgsqlCommand(CreateTableSql, conn))
            await cmd.ExecuteNonQueryAsync(cancellationToken);

        await using (var cmd = new NpgsqlCommand(CreateHypertableSql, conn))
            await cmd.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogInformation("[DB] Schema ready — ohlcv_candles hypertable initialized.");
    }
}
