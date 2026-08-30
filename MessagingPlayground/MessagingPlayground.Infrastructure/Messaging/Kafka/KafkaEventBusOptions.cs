using System.Collections.Generic;
using Confluent.Kafka;

namespace MessagingPlayground.Infrastructure.Messaging.Kafka;

public class KafkaEventBusOptions
{
    public string Topic { get; set; } = string.Empty;
    public string Key { get; set; } = string.Empty;
    public string GroupId { get; set; } = string.Empty;
    public string BootstrapServers { get; set; } = string.Empty;
    public IEnumerable<Header> Headers { get; set; } = new List<Header>();
}