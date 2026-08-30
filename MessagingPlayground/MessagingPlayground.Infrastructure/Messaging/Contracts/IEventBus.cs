using System;
using System.Threading;
using System.Threading.Tasks;

namespace MessagingPlayground.Infrastructure.Bus;

public interface IEventBus<TEvent> : 
    IDisposable
    where TEvent : EventBase 
{
    Task<EventPublishResult> PublishAsync(TEvent @event, CancellationToken cancellationToken);
    Task<TEvent> ReceiveAsync(CancellationToken cancellationToken);
}