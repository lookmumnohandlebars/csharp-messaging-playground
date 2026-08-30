using System.Threading.Channels;
using MessagingPlayground.Infrastructure.Messaging.InMemory;
using Shouldly;

namespace MessagingPlayground.Infrastructure.Tests.Messaging;

public class InMemoryEventBusTests
{
    private InMemoryEventBus<TestEvent> _eventBus;

    public InMemoryEventBusTests()
    {
        var channel = Channel.CreateUnbounded<TestEvent>();
        _eventBus = new InMemoryEventBus<TestEvent>(channel);
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