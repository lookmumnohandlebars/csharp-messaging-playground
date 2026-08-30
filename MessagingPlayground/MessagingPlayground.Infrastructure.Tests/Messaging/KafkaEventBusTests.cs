using Confluent.Kafka;
using DotNet.Testcontainers.Builders;
using MessagingPlayground.Infrastructure.Messaging.Kafka;
using Shouldly;
using Testcontainers.Kafka;

namespace MessagingPlayground.Infrastructure.Tests.Messaging;

public class KafkaEventBusTests
{
    private KafkaEventBus<TestEvent> _kafkaEventBus;

    public KafkaEventBusTests()
    {
        var kafkaContainer = new KafkaBuilder("confluentinc/cp-kafka:6.2.1")
            // Use Zookeeper-based Confluent Platform 6.x image — it will start Zookeeper and Kafka together
            .WithEnvironment("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,TC-0:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT")
            .WithEnvironment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .WithEnvironment("KAFKA_NUM_PARTITIONS", "1")
            .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .Build();
        kafkaContainer.StartAsync().Wait();
        
        var bootstrapServers = kafkaContainer.GetBootstrapAddress();
        
        _kafkaEventBus = new KafkaEventBus<TestEvent>(new KafkaEventBusOptions
        {
            BootstrapServers = bootstrapServers,
            Key = "test-key",
            GroupId = "test-group",
            Topic = "test-topic"
        });
    }

    [Fact]
    public async Task PublishEvent_ShouldRaiseEvent()
    {
        var publishResult = await _kafkaEventBus.PublishAsync(new TestEvent("Hello, World!"), CancellationToken.None);
        publishResult.Result.ShouldBe("Success");
        
        var receivedEvent = await _kafkaEventBus.ReceiveAsync(CancellationToken.None);
        receivedEvent.Text.ShouldBe("Hello, World!");
    }
}