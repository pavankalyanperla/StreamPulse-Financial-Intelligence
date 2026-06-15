namespace StreamPulse.Processor.Application.Interfaces;

public interface IKafkaConsumerService
{
    Task StartConsumingAsync(CancellationToken cancellationToken);
}
