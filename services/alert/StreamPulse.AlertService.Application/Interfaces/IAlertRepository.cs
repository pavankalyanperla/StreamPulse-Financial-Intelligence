using StreamPulse.AlertService.Application.Models;

namespace StreamPulse.AlertService.Application.Interfaces;

public interface IAlertRepository
{
    Task SaveAlertAsync(AnomalyAlert alert, CancellationToken ct);
    Task<IEnumerable<AlertRecord>> GetAlertsAsync(string? symbol, string? severity, int limit, CancellationToken ct);
    Task<IEnumerable<AlertStats>> GetStatsAsync(CancellationToken ct);
}
