using System;
using Confluent.Kafka;

namespace MessagingPlayground.Infrastructure.Messaging.Kafka;

internal class KafkaSerializationFactory
{
    public ISerializer<TEvent> CreateSerializer<TEvent>() where TEvent : EventBase => new KafkaSerializer<TEvent>();

    public IDeserializer<TEvent> CreateDeserializer<TEvent>() where TEvent : EventBase => new KafkaDeserializer<TEvent>();

    private class KafkaSerializer<TEvent> : ISerializer<TEvent> where TEvent : EventBase
    {
        public byte[] Serialize(TEvent data, SerializationContext context)
        {
            var json = System.Text.Json.JsonSerializer.Serialize(data);
            return System.Text.Encoding.UTF8.GetBytes(json);
        }
    }

    private class KafkaDeserializer<TEvent> : IDeserializer<TEvent> where TEvent : EventBase
    {
        public TEvent Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            var json = System.Text.Encoding.UTF8.GetString(data);
            return System.Text.Json.JsonSerializer.Deserialize<TEvent>(json)!;
        }
    }
}

