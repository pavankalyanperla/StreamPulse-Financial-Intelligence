using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using StreamPulse.Processor.Application.Interfaces;
using StreamPulse.Processor.Application.Models;

namespace StreamPulse.Processor.Infrastructure.Kafka;

public sealed class KafkaProducerService : IKafkaProducerService
{
    private readonly ILogger<KafkaProducerService> _logger;
    private readonly IProducer<string, string> _producer;

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public KafkaProducerService(
        ILogger<KafkaProducerService> logger,
        IConfiguration configuration)
    {
        _logger = logger;
        var bootstrapServers = configuration["KafkaSettings:BootstrapServers"] ?? "localhost:9092";
        var config = new ProducerConfig { BootstrapServers = bootstrapServers };
        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task PublishCandleAsync(OhlcvCandle candle, CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(candle, _jsonOptions);
        var message = new Message<string, string>
        {
            Key = candle.Symbol,
            Value = json,
        };

        await _producer.ProduceAsync("ohlcv-aggregated", message, cancellationToken);

        _logger.LogInformation(
            "[PUBLISHED] {Symbol} candle for {CandleTime:u}",
            candle.Symbol, candle.CandleTime);
    }
}
