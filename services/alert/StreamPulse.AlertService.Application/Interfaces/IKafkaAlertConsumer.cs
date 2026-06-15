namespace StreamPulse.AlertService.Application.Interfaces;

public interface IKafkaAlertConsumer
{
    Task StartConsumingAsync(CancellationToken ct);
}
