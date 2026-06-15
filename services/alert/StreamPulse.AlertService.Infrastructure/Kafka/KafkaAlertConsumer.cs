using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using StreamPulse.AlertService.Application.Interfaces;
using StreamPulse.AlertService.Application.Models;
using StreamPulse.AlertService.Application.Settings;

namespace StreamPulse.AlertService.Infrastructure.Kafka;

public class KafkaAlertConsumer : IKafkaAlertConsumer
{
    private readonly KafkaSettings _kafka;
    private readonly IAlertRepository _repository;
    private readonly IEmailNotifier _emailNotifier;
    private readonly ILogger<KafkaAlertConsumer> _logger;

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    public KafkaAlertConsumer(
        KafkaSettings kafka,
        IAlertRepository repository,
        IEmailNotifier emailNotifier,
        ILogger<KafkaAlertConsumer> logger)
    {
        _kafka = kafka;
        _repository = repository;
        _emailNotifier = emailNotifier;
        _logger = logger;
    }

    public async Task StartConsumingAsync(CancellationToken ct)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            GroupId = _kafka.ConsumerGroup,
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true,
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe("alerts");

        _logger.LogInformation("[ALERT SERVICE] Subscribed to alerts topic on {Servers}", _kafka.BootstrapServers);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result is null) continue;

                AnomalyAlert? alert;
                try
                {
                    alert = JsonSerializer.Deserialize<AnomalyAlert>(result.Message.Value, _jsonOptions);
                }
                catch (Exception ex)
                {
                    _logger.LogError("[ALERT] Failed to deserialize message: {Error} | Raw: {Raw}",
                        ex.Message, result.Message.Value);
                    continue;
                }

                if (alert is null) continue;

                _logger.LogInformation(
                    "[ALERT] Received {Severity} {AlertType} — {Symbol} | ${Price} | {ChangePct:+0.00;-0.00}%",
                    alert.Severity, alert.AlertType, alert.Symbol, alert.Price, alert.ChangePct);

                await _repository.SaveAlertAsync(alert, ct);

                if (alert.Severity.Equals("HIGH", StringComparison.OrdinalIgnoreCase))
                    await _emailNotifier.SendAlertEmailAsync(alert, ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError("[ALERT] Consumer error: {Error}", ex.Message);
                await Task.Delay(1000, ct);
            }
        }

        consumer.Close();
    }
}
