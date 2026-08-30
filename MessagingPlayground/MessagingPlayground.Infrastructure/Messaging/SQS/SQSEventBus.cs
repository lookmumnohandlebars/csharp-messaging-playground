using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using MessagingPlayground.Infrastructure.Bus;

namespace MessagingPlayground.Infrastructure.Messaging.SQS;

public class SqsEventBus<TEvent> :
    IEventBus<TEvent> where TEvent : EventBase
{
    private readonly static SqsMessageSerializer Serializer = new();
    
    private readonly IAmazonSQS _sqsClient;
    private readonly SqsEventBusOptions _options;
    
    public SqsEventBus(IAmazonSQS sqsClient, SqsEventBusOptions options)
    {
        _sqsClient = sqsClient;
        _options = options;
    }

    public async Task<EventPublishResult> PublishAsync(TEvent @event, CancellationToken cancellationToken)
    {
        var request = new SendMessageRequest(
            queueUrl: _options.QueueUrl,
            messageBody: Serializer.Serialize(@event)
        );
        _ = await _sqsClient.SendMessageAsync(request, cancellationToken);
        return new EventPublishResult("Success");
    }

    public async Task<TEvent> ReceiveAsync(CancellationToken cancellationToken)
    {
        var request = new ReceiveMessageRequest() {
            QueueUrl = _options.QueueUrl,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = 1
        };
        var response = await _sqsClient.ReceiveMessageAsync(request, cancellationToken);
        var message = response.Messages.FirstOrDefault();
        if (message == null)
        {
            return null!;
        }

        var eventInstance = Serializer.Deserialize<TEvent>(message.Body);
        if (eventInstance == null)
        {
            throw new InvalidOperationException("Failed to deserialize the message body.");
        }
        return eventInstance;
    }

    public void Dispose()
    {
        _sqsClient.Dispose();
    }
}