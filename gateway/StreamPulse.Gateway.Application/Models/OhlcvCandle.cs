using System.Text.Json.Serialization;

namespace StreamPulse.Gateway.Application.Models;

public class OhlcvCandle
{
    [JsonPropertyName("symbol")] public string Symbol { get; set; } = string.Empty;
    [JsonPropertyName("openPrice")] public decimal OpenPrice { get; set; }
    [JsonPropertyName("highPrice")] public decimal HighPrice { get; set; }
    [JsonPropertyName("lowPrice")] public decimal LowPrice { get; set; }
    [JsonPropertyName("closePrice")] public decimal ClosePrice { get; set; }
    [JsonPropertyName("totalVolume")] public long TotalVolume { get; set; }
    [JsonPropertyName("candleTime")] public DateTimeOffset CandleTime { get; set; }
    [JsonPropertyName("tickCount")] public int TickCount { get; set; }
}
