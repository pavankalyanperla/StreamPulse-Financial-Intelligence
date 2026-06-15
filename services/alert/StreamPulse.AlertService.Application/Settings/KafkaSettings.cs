namespace StreamPulse.AlertService.Application.Settings;

public class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string ConsumerGroup { get; set; } = "alert-group";
}
