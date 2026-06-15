using System.Text.Json.Serialization;

namespace StreamPulse.Gateway.Application.Models;

public class PriceForecast
{
    [JsonPropertyName("symbol")] public string Symbol { get; set; } = string.Empty;
    [JsonPropertyName("predicted_close")] public decimal PredictedClose { get; set; }
    [JsonPropertyName("confidence")] public double Confidence { get; set; }
    [JsonPropertyName("forecast_horizon")] public string ForecastHorizon { get; set; } = string.Empty;
    [JsonPropertyName("generated_at")] public string GeneratedAt { get; set; } = string.Empty;
    [JsonPropertyName("model_version")] public string ModelVersion { get; set; } = string.Empty;
}
