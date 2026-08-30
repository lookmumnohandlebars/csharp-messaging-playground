namespace MessagingPlayground.Infrastructure.Tests.Messaging;

public class TestEvent : EventBase
{
    public TestEvent(string text)
    {
        Text = text;
    }

    public string Text { get; }
}