namespace OutboxPattern.Infrastructure.Outbox;

public interface IOutboxStore
{
    Task<List<OutboxMessage>> ClaimBatchAsync(Guid lockId, CancellationToken ct);

    Task MarkProcessedAsync(Guid messageId, Guid lockId, CancellationToken ct);

    Task ScheduleRetryAsync(OutboxMessage message, Guid lockId, Exception exception, CancellationToken ct);
}
