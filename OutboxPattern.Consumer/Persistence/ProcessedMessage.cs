namespace OutboxPattern.Consumer.Persistence;

public sealed class ProcessedMessage
{
    public string MessageId { get; set; } = default!;
    public DateTime ProcessedAtUtc { get; set; }
}
