namespace MessagingPlayground.Infrastructure.Messaging.Kafka;

public class KafkaEventBusOptions
{
    public string Topic { get; set; } = string.Empty;
    public string Key { get; set; } = string.Empty;
    public string BootstrapServers { get; set; } = string.Empty;
}