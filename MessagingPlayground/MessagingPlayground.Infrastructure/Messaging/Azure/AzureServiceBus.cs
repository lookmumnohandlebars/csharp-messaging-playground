using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using MessagingPlayground.Infrastructure.Bus;

namespace MessagingPlayground.Infrastructure.Messaging;

public class AzureServiceBus<TEvent> : IEventBus<TEvent> where TEvent : EventBase
{
    private readonly AzureServiceBusOptions _options;
    private readonly ServiceBusSender _sender;
    private readonly ServiceBusReceiver _receiver;

    public AzureServiceBus(ServiceBusClient client, AzureServiceBusOptions options)
    {
        _options = options;
        _sender = client.CreateSender(_options.QueueName);
        _receiver = client.CreateReceiver(_options.QueueName);
    }

    public void Dispose()
    {
        // TODO release managed resources here
    }

    public async Task<EventPublishResult> PublishAsync(TEvent @event, CancellationToken cancellationToken)
    {
        await _sender.SendMessageAsync(new ServiceBusMessage(JsonSerializer.Serialize(@event)), cancellationToken);
        return new EventPublishResult("Success");
    }

    public async Task<TEvent> ReceiveAsync(CancellationToken cancellationToken)
    {
        var message = await _receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1), cancellationToken);
        return JsonSerializer.Deserialize<TEvent>(message.Body) ?? throw new InvalidOperationException("Failed to deserialize the message body.");
    }
}

public class AzureServiceBusOptions
{
    public string QueueName { get; set; } = null!;
}