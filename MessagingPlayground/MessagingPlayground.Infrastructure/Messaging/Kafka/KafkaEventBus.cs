using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using MessagingPlayground.Application;
using MessagingPlayground.Infrastructure.Bus;

namespace MessagingPlayground.Infrastructure.Messaging.Kafka;

public class KafkaEventBus<TEvent> : IEventBus<TEvent> where TEvent : EventBase
{
    private static KafkaSerializationFactory _serializationFactory = new KafkaSerializationFactory();
    
    IProducer<string, TEvent> _producer;
    IConsumer<string, TEvent> _consumer;
    KafkaEventBusOptions _options;

    public KafkaEventBus(
        KafkaEventBusOptions options
    )
    {
        _producer = new ProducerBuilder<string, TEvent>(new ProducerConfig
            {
                BootstrapServers = options.BootstrapServers
            })
            .SetValueSerializer(_serializationFactory.CreateSerializer<TEvent>())
            .Build();;
        _consumer = new ConsumerBuilder<string, TEvent>(new ConsumerConfig
            {
                BootstrapServers = options.BootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                Acks = Acks.All,
                GroupId = options.GroupId,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = true
            })
            .SetValueDeserializer(_serializationFactory.CreateDeserializer<TEvent>())
            .Build();
        _consumer.Subscribe(options.Topic);
        _options = options;
    }
    
    public async Task<EventPublishResult> PublishAsync(TEvent @event, CancellationToken cancellationToken)
    {
        var message = new Message<string, TEvent>
        {
            Key = _options.Key,
            Value = @event
        };
        _ = await _producer.ProduceAsync(_options.Topic, message, cancellationToken);
        return new EventPublishResult("Success");
    }

    public async Task<TEvent> ReceiveAsync(CancellationToken cancellationToken)
    {
        var totalTimeout = TimeSpan.FromSeconds(30);
        var start = DateTime.UtcNow;

        while (!cancellationToken.IsCancellationRequested && DateTime.UtcNow - start < totalTimeout)
        {
            var result = _consumer.Consume(TimeSpan.FromSeconds(1));
            if (result?.Message?.Value != null)
            {
                return await Task.FromResult(result.Message.Value);
            }
        }

        throw new TimeoutException("Timed out waiting for message from Kafka");
    }

    public void Dispose()
    {
        _producer.Dispose();
        _consumer.Dispose();
    }
}