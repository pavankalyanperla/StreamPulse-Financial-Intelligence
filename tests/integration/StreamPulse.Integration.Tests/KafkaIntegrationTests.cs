using System.Text.Json;
using Confluent.Kafka;
using FluentAssertions;

namespace StreamPulse.Integration.Tests;

[TestFixture]
public class KafkaIntegrationTests
{
    private const string KafkaBootstrap = "localhost:9092";

    private static readonly string[] KnownSymbols    = ["AAPL", "GOOGL", "MSFT", "INFY", "TCS"];
    private static readonly string[] ValidSeverities  = ["LOW", "MEDIUM", "HIGH"];
    private static readonly string[] ValidDirections  = ["UP", "DOWN", "FLAT"];

    // ── raw-ticks ──────────────────────────────────────────────────────────────

    [Test, CancelAfter(30_000)]
    public void RawTicks_Topic_Is_Active()
    {
        // Latest: only read ticks arriving from now; ingestion emits 5/s so 10 s = ~50 messages
        var messages = Consume("raw-ticks", minMessages: 5, windowSeconds: 10, AutoOffsetReset.Latest);

        messages.Should().HaveCountGreaterThanOrEqualTo(5,
            "ingestion service must publish at least 5 ticks/second to raw-ticks");

        foreach (var msg in messages)
        {
            var root = JsonDocument.Parse(msg).RootElement;

            string  symbol  = root.GetProperty("symbol").GetString()!;
            decimal price   = root.GetProperty("price").GetDecimal();
            int     volume  = root.GetProperty("volume").GetInt32();

            KnownSymbols.Should().Contain(symbol, $"'{symbol}' must be one of the 5 tracked symbols");
            price.Should().BeGreaterThan(0,  "price must be positive");
            volume.Should().BeGreaterThan(0, "volume must be positive");
        }

        TestContext.Progress.WriteLine($"[KAFKA] raw-ticks: {messages.Count} messages validated");
    }

    // ── ohlcv-aggregated ───────────────────────────────────────────────────────

    [Test, CancelAfter(30_000)]
    public void OhlcvAggregated_Topic_Has_Messages()
    {
        // Earliest: candles exist from prior runs; one is enough
        var messages = Consume("ohlcv-aggregated", minMessages: 1, windowSeconds: 5, AutoOffsetReset.Earliest);

        messages.Should().HaveCountGreaterThanOrEqualTo(1,
            "stream processor must have published at least one OHLCV candle to ohlcv-aggregated");

        var root = JsonDocument.Parse(messages[0]).RootElement;

        // .NET processor serialises in camelCase
        root.GetProperty("openPrice").GetDecimal().Should().BeGreaterThan(0,  "openPrice > 0");
        root.GetProperty("highPrice").GetDecimal().Should().BeGreaterThan(0,  "highPrice > 0");
        root.GetProperty("lowPrice").GetDecimal().Should().BeGreaterThan(0,   "lowPrice > 0");
        root.GetProperty("closePrice").GetDecimal().Should().BeGreaterThan(0, "closePrice > 0");

        string sym = root.GetProperty("symbol").GetString()!;
        TestContext.Progress.WriteLine($"[KAFKA] ohlcv-aggregated: validated candle for {sym}");
    }

    // ── ml-forecasts ───────────────────────────────────────────────────────────

    [Test, CancelAfter(30_000)]
    public void MlForecasts_Topic_Has_Messages()
    {
        // Earliest: forecasts persist from prior LSTM runs
        var messages = Consume("ml-forecasts", minMessages: 1, windowSeconds: 5, AutoOffsetReset.Earliest);

        messages.Should().HaveCountGreaterThanOrEqualTo(1,
            "forecasting service must have published at least one forecast to ml-forecasts");

        var root = JsonDocument.Parse(messages[0]).RootElement;

        // Python model_dump() → snake_case
        decimal predicted = root.GetProperty("predicted_close").GetDecimal();
        string  direction = root.GetProperty("direction").GetString()!;

        predicted.Should().BeGreaterThan(0, "predicted_close must be a positive price");
        ValidDirections.Should().Contain(direction,
            $"direction '{direction}' must be UP, DOWN, or FLAT");

        TestContext.Progress.WriteLine($"[KAFKA] ml-forecasts: predicted_close={predicted:F4} direction={direction}");
    }

    // ── alerts ────────────────────────────────────────────────────────────────

    [Test, CancelAfter(30_000)]
    public void Alerts_Topic_Has_Messages()
    {
        // Earliest: anomaly service emits alerts continuously since start
        var messages = Consume("alerts", minMessages: 1, windowSeconds: 5, AutoOffsetReset.Earliest);

        messages.Should().HaveCountGreaterThanOrEqualTo(1,
            "anomaly service must have published at least one alert to alerts");

        var root = JsonDocument.Parse(messages[0]).RootElement;

        // Python model_dump() → snake_case
        string severity = root.GetProperty("severity").GetString()!;
        string symbol   = root.GetProperty("symbol").GetString()!;

        ValidSeverities.Should().Contain(severity,
            $"severity '{severity}' must be LOW, MEDIUM, or HIGH");
        KnownSymbols.Should().Contain(symbol,
            $"alert symbol '{symbol}' must be one of the 5 tracked symbols");

        TestContext.Progress.WriteLine($"[KAFKA] alerts: severity={severity} symbol={symbol}");
    }

    // ── helper ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Polls <paramref name="topic"/> until <paramref name="minMessages"/> are collected
    /// or <paramref name="windowSeconds"/> elapse, then returns whatever was collected.
    /// Uses a unique consumer group per call so tests never interfere with each other.
    /// </summary>
    private static List<string> Consume(
        string topic, int minMessages, int windowSeconds, AutoOffsetReset offsetReset)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = KafkaBootstrap,
            GroupId          = $"it-group-{Guid.NewGuid():N}",
            AutoOffsetReset  = offsetReset,
            EnableAutoCommit = false,
        };

        var results = new List<string>();

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe(topic);

        var deadline = DateTime.UtcNow.AddSeconds(windowSeconds);
        while (DateTime.UtcNow < deadline && results.Count < minMessages)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(1));
            if (result?.Message?.Value is { } value)
                results.Add(value);
        }

        consumer.Close();
        return results;
    }
}
