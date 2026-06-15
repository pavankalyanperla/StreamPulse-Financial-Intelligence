using Microsoft.Extensions.Logging;
using Npgsql;
using StreamPulse.AlertService.Application.Interfaces;
using StreamPulse.AlertService.Application.Models;
using StreamPulse.AlertService.Application.Settings;

namespace StreamPulse.AlertService.Infrastructure.Database;

public class AlertRepository : IAlertRepository
{
    private readonly DatabaseSettings _settings;
    private readonly ILogger<AlertRepository> _logger;

    public AlertRepository(DatabaseSettings settings, ILogger<AlertRepository> logger)
    {
        _settings = settings;
        _logger = logger;
    }

    public async Task SaveAlertAsync(AnomalyAlert alert, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(_settings.ConnectionString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO alert_records
                (received_at, symbol, alert_type, severity, price, volume, change_pct, anomaly_score, alert_timestamp)
            VALUES
                (@receivedAt, @symbol, @alertType, @severity, @price, @volume, @changePct, @anomalyScore, @alertTimestamp)
            """;

        cmd.Parameters.AddWithValue("@receivedAt", DateTimeOffset.UtcNow);
        cmd.Parameters.AddWithValue("@symbol", alert.Symbol);
        cmd.Parameters.AddWithValue("@alertType", alert.AlertType);
        cmd.Parameters.AddWithValue("@severity", alert.Severity);
        cmd.Parameters.AddWithValue("@price", alert.Price);
        cmd.Parameters.AddWithValue("@volume", alert.Volume);
        cmd.Parameters.AddWithValue("@changePct", alert.ChangePct);
        cmd.Parameters.AddWithValue("@anomalyScore", alert.AnomalyScore);
        cmd.Parameters.AddWithValue("@alertTimestamp", alert.Timestamp);

        await cmd.ExecuteNonQueryAsync(ct);

        _logger.LogInformation("[DB] Saved {Severity} {AlertType} alert for {Symbol}",
            alert.Severity, alert.AlertType, alert.Symbol);
    }

    public async Task<IEnumerable<AlertRecord>> GetAlertsAsync(
        string? symbol, string? severity, int limit, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(_settings.ConnectionString);
        await conn.OpenAsync(ct);

        var conditions = new List<string>();
        if (!string.IsNullOrEmpty(symbol)) conditions.Add("symbol = @symbol");
        if (!string.IsNullOrEmpty(severity)) conditions.Add("severity = @severity");

        var where = conditions.Count > 0 ? "WHERE " + string.Join(" AND ", conditions) : string.Empty;

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT id, received_at, symbol, alert_type, severity, price, volume, change_pct, anomaly_score, alert_timestamp
            FROM alert_records
            {where}
            ORDER BY received_at DESC
            LIMIT @limit
            """;

        if (!string.IsNullOrEmpty(symbol)) cmd.Parameters.AddWithValue("@symbol", symbol);
        if (!string.IsNullOrEmpty(severity)) cmd.Parameters.AddWithValue("@severity", severity);
        cmd.Parameters.AddWithValue("@limit", limit);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var results = new List<AlertRecord>();
        while (await reader.ReadAsync(ct))
        {
            results.Add(new AlertRecord
            {
                Id = reader.GetInt64(0),
                ReceivedAt = reader.GetFieldValue<DateTimeOffset>(1),
                Symbol = reader.GetString(2),
                AlertType = reader.GetString(3),
                Severity = reader.GetString(4),
                Price = reader.GetDecimal(5),
                Volume = reader.GetInt32(6),
                ChangePct = reader.GetDecimal(7),
                AnomalyScore = reader.GetDouble(8),
                AlertTimestamp = reader.GetFieldValue<DateTimeOffset>(9),
            });
        }
        return results;
    }

    public async Task<IEnumerable<AlertStats>> GetStatsAsync(CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(_settings.ConnectionString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT
                symbol,
                COUNT(*)                                                           AS total,
                SUM(CASE WHEN severity = 'HIGH'   THEN 1 ELSE 0 END)              AS high_count,
                SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END)              AS medium_count,
                SUM(CASE WHEN severity = 'LOW'    THEN 1 ELSE 0 END)              AS low_count,
                MAX(received_at)                                                   AS last_alert_time,
                (array_agg(severity ORDER BY received_at DESC))[1]                AS last_severity
            FROM alert_records
            GROUP BY symbol
            ORDER BY symbol
            """;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var results = new List<AlertStats>();
        while (await reader.ReadAsync(ct))
        {
            results.Add(new AlertStats
            {
                Symbol = reader.GetString(0),
                TotalAlerts = (int)reader.GetInt64(1),
                HighCount = (int)reader.GetInt64(2),
                MediumCount = (int)reader.GetInt64(3),
                LowCount = (int)reader.GetInt64(4),
                LastAlertTime = reader.IsDBNull(5) ? null : reader.GetFieldValue<DateTimeOffset>(5),
                LastSeverity = reader.IsDBNull(6) ? null : reader.GetString(6),
            });
        }
        return results;
    }
}
