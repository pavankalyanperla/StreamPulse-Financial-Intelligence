namespace StreamPulse.Gateway.Application.Interfaces;

public interface IKafkaGatewayConsumer
{
    Task StartConsumingAsync(CancellationToken ct);
}
