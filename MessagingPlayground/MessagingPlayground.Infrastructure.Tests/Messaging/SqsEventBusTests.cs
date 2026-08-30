using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using DotNet.Testcontainers.Builders;
using MessagingPlayground.Infrastructure.Messaging.SQS;
using Shouldly;
using Testcontainers.LocalStack;

namespace MessagingPlayground.Infrastructure.Tests.Messaging;

public class SqsEventBusTests
{
    private SqsEventBus<TestEvent> _eventBus;

    public SqsEventBusTests()
    {
        var localStackContainer = new LocalStackBuilder("localstack/localstack:4.12.0")
            .WithEnvironment("SERVICES", "sqs")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilExternalTcpPortIsAvailable(4566))
            .Build();
        localStackContainer.StartAsync().GetAwaiter().GetResult();

        var port = localStackContainer.GetMappedPublicPort(4566);
        var sqsUrl = $"http://localhost:{port}";

        var creds = new Amazon.Runtime.BasicAWSCredentials("test", "test");
        var sqsClient = new AmazonSQSClient(creds, new AmazonSQSConfig()
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = sqsUrl,
            UseHttp = true
        });

        var createResponse = sqsClient.CreateQueueAsync(new CreateQueueRequest("test")).Result;

        var options = new SqsEventBusOptions
        {
            QueueUrl = createResponse.QueueUrl
        };
        _eventBus = new SqsEventBus<TestEvent>(sqsClient, options);
    }
    
    [Fact]
    public async Task PublishEvent_ShouldRaiseEvent()
    {
        var publishResult = await _eventBus.PublishAsync(new TestEvent("Hello, World!"), CancellationToken.None);
        publishResult.Result.ShouldBe("Success");
        
        var receivedEvent = await _eventBus.ReceiveAsync(CancellationToken.None);
        receivedEvent.Text.ShouldBe("Hello, World!");
    }
}