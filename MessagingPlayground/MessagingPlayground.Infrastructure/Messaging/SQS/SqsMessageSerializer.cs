namespace MessagingPlayground.Infrastructure.Messaging.SQS;

public class SqsMessageSerializer
{
    public string Serialize<TEvent>(TEvent @event) where TEvent : EventBase
    {
        return System.Text.Json.JsonSerializer.Serialize(@event);
    }

    public TEvent Deserialize<TEvent>(string json) where TEvent : EventBase
    {
        return System.Text.Json.JsonSerializer.Deserialize<TEvent>(json)!;
    }
}