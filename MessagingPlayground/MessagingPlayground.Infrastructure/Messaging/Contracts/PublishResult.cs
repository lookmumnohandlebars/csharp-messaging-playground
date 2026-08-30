namespace MessagingPlayground.Infrastructure.Bus;

public record EventPublishResult(string Result)
{
    public string Result { get; } = Result;
}