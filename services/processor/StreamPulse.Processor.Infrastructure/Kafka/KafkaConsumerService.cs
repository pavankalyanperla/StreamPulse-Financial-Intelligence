using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Prometheus;
using StreamPulse.Processor.Application.Interfaces;
using StreamPulse.Processor.Application.Models;

namespace StreamPulse.Processor.Infrastructure.Kafka;

public sealed class KafkaConsumerService : IKafkaConsumerService
{
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly ICandleAggregator _aggregator;
    private readonly IKafkaProducerService _producer;
    private readonly ICandleRepository _repository;
    private readonly string _bootstrapServers;
    private readonly string _consumerGroup;

    private static readonly Counter _ticksConsumed = Metrics.CreateCounter(
        "streampulse_ticks_consumed_total",
        "Ticks consumed from Kafka",
        new CounterConfiguration { LabelNames = new[] { "symbol" } });

    private static readonly Counter _candlesPublished = Metrics.CreateCounter(
        "streampulse_candles_published_total",
        "Candles published to Kafka",
        new CounterConfiguration { LabelNames = new[] { "symbol" } });

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    public KafkaConsumerService(
        ILogger<KafkaConsumerService> logger,
        IConfiguration configuration,
        ICandleAggregator aggregator,
        IKafkaProducerService producer,
        ICandleRepository repository)
    {
        _logger = logger;
        _aggregator = aggregator;
        _producer = producer;
        _repository = repository;
        _bootstrapServers = configuration["KafkaSettings:BootstrapServers"] ?? "localhost:9092";
        _consumerGroup = configuration["KafkaSettings:ConsumerGroup"] ?? "processor-group";
    }

    public async Task StartConsumingAsync(CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = _consumerGroup,
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true,
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe("raw-ticks");

        _logger.LogInformation("[PROCESSOR] Subscribed to raw-ticks on {Servers}", _bootstrapServers);

        var lastFlush = DateTimeOffset.UtcNow;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Non-blocking poll with 500ms timeout so we can flush on schedule
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));

                if (result is not null)
                {
                    var tick = JsonSerializer.Deserialize<StockTick>(result.Message.Value, _jsonOptions);
                    if (tick is not null)
                    {
                        _aggregator.AddTick(tick);
                        _ticksConsumed.WithLabels(tick.Symbol).Inc();
                        _logger.LogInformation(
                            "[CONSUMED] {Symbol} | ${Price} | {Timestamp:u}",
                            tick.Symbol, tick.Price, tick.Timestamp);
                    }
                }

                // Flush every 5 seconds
                if ((DateTimeOffset.UtcNow - lastFlush).TotalSeconds >= 5)
                {
                    var candles = _aggregator.FlushCompletedCandles(DateTimeOffset.UtcNow).ToList();
                    foreach (var candle in candles)
                    {
                        _logger.LogInformation(
                            "[CANDLE] {Symbol} | O:{Open} H:{High} L:{Low} C:{Close} | vol:{Vol} | ticks:{Ticks}",
                            candle.Symbol,
                            candle.OpenPrice, candle.HighPrice, candle.LowPrice, candle.ClosePrice,
                            candle.TotalVolume, candle.TickCount);

                        await _producer.PublishCandleAsync(candle, cancellationToken);
                        _candlesPublished.WithLabels(candle.Symbol).Inc();

                        try
                        {
                            await _repository.SaveCandleAsync(candle, cancellationToken);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "[DB] Failed to save {Symbol} candle — Kafka publish unaffected", candle.Symbol);
                        }
                    }
                    lastFlush = DateTimeOffset.UtcNow;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown
        }
        finally
        {
            consumer.Close();
        }
    }
}
