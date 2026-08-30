using Azure.Messaging.ServiceBus;
using DotNet.Testcontainers.Builders;
using MessagingPlayground.Infrastructure.Messaging;
using Shouldly;
using Testcontainers.ServiceBus;

namespace MessagingPlayground.Infrastructure.Tests.Messaging;

public class AzureServiceBusTests
{
    private readonly AzureServiceBus<TestEvent> _eventBus;
    public const ushort ServiceBusPort = 5672;
    public const ushort ServiceBusHttpPort = 5300;
    
    public AzureServiceBusTests()
    {
        
        var serviceBusContainer = new ServiceBusBuilder()
            .WithImage("mcr.microsoft.com/azure-messaging/servicebus-emulator:latest")
            .WithAcceptLicenseAgreement(true)
            .WithPortBinding(ServiceBusPort, true)
            .WithPortBinding(ServiceBusHttpPort, true)
            .WithEnvironment("SQL_WAIT_INTERVAL", "0")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request =>
                request.ForPort(ServiceBusHttpPort).ForPath("/health")))
            .Build();
        
        serviceBusContainer.StartAsync().GetAwaiter().GetResult();
        
        var hostPort = serviceBusContainer.GetMappedPublicPort(ServiceBusPort);
        var connectionString = $"Endpoint=sb://localhost:{hostPort}/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey==aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa=;UseDevelopmentEmulator=true;";
        _eventBus = new AzureServiceBus<TestEvent>(new ServiceBusClient(connectionString), new AzureServiceBusOptions()
        {
            QueueName = "queue.1"
        });
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