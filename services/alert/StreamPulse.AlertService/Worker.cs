using StreamPulse.AlertService.Application.Interfaces;

namespace StreamPulse.AlertService;

public class Worker(IKafkaAlertConsumer consumer, ILogger<Worker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("[ALERT SERVICE] Started, consuming alerts topic...");
        await consumer.StartConsumingAsync(stoppingToken);
    }
}
