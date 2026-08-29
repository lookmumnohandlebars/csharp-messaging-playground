using System;
using MessagingPlayground.Application;
using MessagingPlayground.Infrastructure.Messaging.Kafka;

namespace MessagingPlayground.Infrastructure.Messaging.SQS;

public class SqsMessageSerializer
{
    public string Serialize(EventBase @event)
    {
        return System.Text.Json.JsonSerializer.Serialize(@event);
    }

    public TEvent Deserialize<TEvent>(string json) where TEvent : EventBase
    {
        return System.Text.Json.JsonSerializer.Deserialize<TEvent>(json)!;
    }
}