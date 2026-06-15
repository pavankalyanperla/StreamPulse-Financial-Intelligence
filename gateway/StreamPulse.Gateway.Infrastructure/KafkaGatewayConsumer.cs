using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StreamPulse.Gateway.Application.Interfaces;
using StreamPulse.Gateway.Application.Models;
using StreamPulse.Gateway.Application.Settings;

namespace StreamPulse.Gateway.Infrastructure;

public class KafkaGatewayConsumer : IKafkaGatewayConsumer
{
    private readonly KafkaSettings _kafka;
    private readonly IStockBroadcaster _broadcaster;
    private readonly ILogger<KafkaGatewayConsumer> _logger;
    private static readonly JsonSerializerOptions _json = new() { PropertyNameCaseInsensitive = true };

    public KafkaGatewayConsumer(IOptions<KafkaSettings> kafka, IStockBroadcaster broadcaster, ILogger<KafkaGatewayConsumer> logger)
    {
        _kafka = kafka.Value;
        _broadcaster = broadcaster;
        _logger = logger;
    }

    public async Task StartConsumingAsync(CancellationToken ct)
    {
        var forecastTask = ConsumeForecasts(ct);
        var alertTask = ConsumeAlerts(ct);
        await Task.WhenAll(forecastTask, alertTask);
    }

    private async Task ConsumeForecasts(CancellationToken ct)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            GroupId = "gateway-forecasts-group",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true,
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe("ml-forecasts");
        _logger.LogInformation("[GATEWAY] Kafka consumer subscribed to ml-forecasts");

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await Task.Run(() => consumer.Consume(TimeSpan.FromSeconds(1)), ct);
                if (result?.Message?.Value == null) continue;

                var forecast = JsonSerializer.Deserialize<PriceForecast>(result.Message.Value, _json);
                if (forecast != null)
                    await _broadcaster.BroadcastForecastAsync(forecast, ct);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[GATEWAY] Error consuming ml-forecasts");
                await Task.Delay(1000, ct);
            }
        }

        consumer.Close();
    }

    private async Task ConsumeAlerts(CancellationToken ct)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            GroupId = "gateway-alerts-group",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true,
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe("alerts");
        _logger.LogInformation("[GATEWAY] Kafka consumer subscribed to alerts");

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await Task.Run(() => consumer.Consume(TimeSpan.FromSeconds(1)), ct);
                if (result?.Message?.Value == null) continue;

                var alert = JsonSerializer.Deserialize<AnomalyAlert>(result.Message.Value, _json);
                if (alert != null)
                    await _broadcaster.BroadcastAlertAsync(alert, ct);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[GATEWAY] Error consuming alerts");
                await Task.Delay(1000, ct);
            }
        }

        consumer.Close();
    }
}
