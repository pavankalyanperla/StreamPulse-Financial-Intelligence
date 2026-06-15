namespace StreamPulse.Gateway.Application.Settings;

public class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string ConsumerGroup { get; set; } = "gateway-group";
}
