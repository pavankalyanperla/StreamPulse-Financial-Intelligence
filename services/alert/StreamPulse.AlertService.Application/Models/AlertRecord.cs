namespace StreamPulse.AlertService.Application.Models;

public class AlertRecord
{
    public long Id { get; set; }
    public DateTimeOffset ReceivedAt { get; set; }
    public string Symbol { get; set; } = string.Empty;
    public string AlertType { get; set; } = string.Empty;
    public string Severity { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public int Volume { get; set; }
    public decimal ChangePct { get; set; }
    public double AnomalyScore { get; set; }
    public DateTimeOffset AlertTimestamp { get; set; }
}
