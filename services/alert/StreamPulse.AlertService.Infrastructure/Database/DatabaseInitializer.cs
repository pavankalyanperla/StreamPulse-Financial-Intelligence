using Microsoft.Extensions.Logging;
using Npgsql;
using StreamPulse.AlertService.Application.Settings;

namespace StreamPulse.AlertService.Infrastructure.Database;

public class DatabaseInitializer
{
    private readonly DatabaseSettings _settings;
    private readonly ILogger<DatabaseInitializer> _logger;

    public DatabaseInitializer(DatabaseSettings settings, ILogger<DatabaseInitializer> logger)
    {
        _settings = settings;
        _logger = logger;
    }

    public async Task InitializeAsync(CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(_settings.ConnectionString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS alert_records (
                id              BIGSERIAL,
                received_at     TIMESTAMPTZ      NOT NULL,
                symbol          VARCHAR(10)      NOT NULL,
                alert_type      VARCHAR(20)      NOT NULL,
                severity        VARCHAR(10)      NOT NULL,
                price           NUMERIC(12,4)    NOT NULL,
                volume          INT              NOT NULL,
                change_pct      NUMERIC(8,4)     NOT NULL,
                anomaly_score   DOUBLE PRECISION NOT NULL,
                alert_timestamp TIMESTAMPTZ      NOT NULL,
                PRIMARY KEY (id, received_at)
            );
            SELECT create_hypertable('alert_records', 'received_at', if_not_exists => TRUE);
            """;
        await cmd.ExecuteNonQueryAsync(ct);

        _logger.LogInformation("[DB] alert_records hypertable initialized");
    }
}
