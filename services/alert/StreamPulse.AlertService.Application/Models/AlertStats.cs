namespace StreamPulse.AlertService.Application.Models;

public class AlertStats
{
    public string Symbol { get; set; } = string.Empty;
    public int TotalAlerts { get; set; }
    public int HighCount { get; set; }
    public int MediumCount { get; set; }
    public int LowCount { get; set; }
    public DateTimeOffset? LastAlertTime { get; set; }
    public string? LastSeverity { get; set; }
}
