using StreamPulse.AlertService.Application.Models;

namespace StreamPulse.AlertService.Application.Interfaces;

public interface IEmailNotifier
{
    Task SendAlertEmailAsync(AnomalyAlert alert, CancellationToken ct);
}
