namespace OutboxPattern.Infrastructure.Outbox;

public sealed class OutboxMessage
{
    // Essential
    public Guid Id { get; set; }                 // also the Service Bus MessageId
    public string EventType { get; set; } = default!;
    public string Payload { get; set; } = default!;   // JSON
    public Guid AggregateId { get; set; }        // the OrderId, for correlation
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? ProcessedAtUtc { get; set; }    // null = pending

    // Operational
    public int RetryCount { get; set; }
    public DateTime NextAttemptAtUtc { get; set; }
    public string? LastError { get; set; }
    public Guid? LockId { get; set; }
    public DateTime? LockedUntilUtc { get; set; }
}
