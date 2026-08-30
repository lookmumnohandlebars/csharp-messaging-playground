using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MessagingPlayground.Infrastructure.Bus;

namespace MessagingPlayground.Infrastructure.Messaging.InMemory;

public class InMemoryEventBus<TEvent> : IEventBus<TEvent> where TEvent : EventBase
{
    private readonly Channel<TEvent> _channel;
    
    public InMemoryEventBus(Channel<TEvent> channel)
    {
        _channel = channel;
    }
    
    public async Task<EventPublishResult> PublishAsync(TEvent @event, CancellationToken cancellationToken)
    {
        await _channel.Writer.WriteAsync(@event, cancellationToken);
        return new EventPublishResult("Success");
    }

    public async Task<TEvent> ReceiveAsync(CancellationToken cancellationToken)
    {
        return await _channel.Reader.ReadAsync(cancellationToken);
    }

    public void Dispose() { }
}