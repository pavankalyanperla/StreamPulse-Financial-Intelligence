using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StreamPulse.Gateway.Application.Interfaces;
using StreamPulse.Gateway.Application.Models;
using StreamPulse.Gateway.Application.Settings;

namespace StreamPulse.Gateway.Infrastructure;

public class KafkaTickConsumer
{
    private readonly KafkaSettings _kafka;
    private readonly IStockBroadcaster _broadcaster;
    private readonly IRedisTickCache _cache;
    private readonly ILogger<KafkaTickConsumer> _logger;
    private static readonly JsonSerializerOptions _json = new() { PropertyNameCaseInsensitive = true };

    public KafkaTickConsumer(IOptions<KafkaSettings> kafka, IStockBroadcaster broadcaster, IRedisTickCache cache, ILogger<KafkaTickConsumer> logger)
    {
        _kafka = kafka.Value;
        _broadcaster = broadcaster;
        _cache = cache;
        _logger = logger;
    }

    public async Task StartConsumingAsync(CancellationToken ct)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            GroupId = "gateway-ticks-group",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true,
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe("raw-ticks");
        _logger.LogInformation("[GATEWAY] Kafka consumer subscribed to raw-ticks");

        int tickCount = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await Task.Run(() => consumer.Consume(TimeSpan.FromSeconds(1)), ct);
                if (result?.Message?.Value == null) continue;

                var tick = JsonSerializer.Deserialize<LiveTick>(result.Message.Value, _json);
                if (tick == null) continue;

                await _cache.SetTickAsync(tick, ct);
                await _broadcaster.BroadcastTickAsync(tick, ct);

                tickCount++;
                if (tickCount % 50 == 0)
                    _logger.LogInformation("[GATEWAY] Ticks processed: {Count} | Last: {Symbol} @ {Price}", tickCount, tick.Symbol, tick.Price);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[GATEWAY] Error consuming raw-ticks");
                await Task.Delay(1000, ct);
            }
        }

        consumer.Close();
    }
}
