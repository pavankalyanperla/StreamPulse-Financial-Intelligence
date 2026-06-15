using System.Text.Json.Serialization;

namespace StreamPulse.Gateway.Application.Models;

public class LiveTick
{
    [JsonPropertyName("symbol")] public string Symbol { get; set; } = string.Empty;
    [JsonPropertyName("price")] public decimal Price { get; set; }
    [JsonPropertyName("volume")] public int Volume { get; set; }
    [JsonPropertyName("timestamp")] public DateTimeOffset Timestamp { get; set; }
    [JsonPropertyName("change_pct")] public decimal ChangePct { get; set; }
}
