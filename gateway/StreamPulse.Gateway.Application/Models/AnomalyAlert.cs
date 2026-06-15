using System.Text.Json.Serialization;

namespace StreamPulse.Gateway.Application.Models;

public class AnomalyAlert
{
    [JsonPropertyName("symbol")] public string Symbol { get; set; } = string.Empty;
    [JsonPropertyName("alert_type")] public string AlertType { get; set; } = string.Empty;
    [JsonPropertyName("severity")] public string Severity { get; set; } = string.Empty;
    [JsonPropertyName("price")] public decimal Price { get; set; }
    [JsonPropertyName("volume")] public int Volume { get; set; }
    [JsonPropertyName("change_pct")] public decimal ChangePct { get; set; }
    [JsonPropertyName("timestamp")] public DateTimeOffset Timestamp { get; set; }
    [JsonPropertyName("anomaly_score")] public double AnomalyScore { get; set; }
}
