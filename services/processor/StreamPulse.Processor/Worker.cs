using StreamPulse.Processor.Application.Interfaces;

namespace StreamPulse.Processor;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IKafkaConsumerService _consumer;

    public Worker(ILogger<Worker> logger, IKafkaConsumerService consumer)
    {
        _logger = logger;
        _consumer = consumer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("[PROCESSOR] Stream Processor started, consuming raw-ticks...");
        await _consumer.StartConsumingAsync(stoppingToken);
    }
}
