using OutboxPattern.Infrastructure.Outbox;

namespace OutboxPattern.UnitTests;

internal sealed class FakeOutboxStore : IOutboxStore
{
    private readonly List<OutboxMessage> _pending;

    public FakeOutboxStore(params OutboxMessage[] pending)
    {
        _pending = [.. pending];
    }

    public Exception? MarkException { get; set; }
    public List<Guid> ClaimLockIds { get; } = [];
    public List<(Guid MessageId, Guid LockId, CancellationToken Token)> MarkCalls { get; } = [];
    public List<(Guid MessageId, Guid LockId, Exception Exception)> RetryCalls { get; } = [];

    public Task<List<OutboxMessage>> ClaimBatchAsync(Guid lockId, CancellationToken ct)
    {
        ClaimLockIds.Add(lockId);
        return Task.FromResult(_pending.ToList());
    }

    public Task MarkProcessedAsync(Guid messageId, Guid lockId, CancellationToken ct)
    {
        MarkCalls.Add((messageId, lockId, ct));

        if (MarkException is not null)
            throw MarkException;

        return Task.CompletedTask;
    }

    public Task ScheduleRetryAsync(OutboxMessage message, Guid lockId, Exception exception, CancellationToken ct)
    {
        message.RetryCount++;
        RetryCalls.Add((message.Id, lockId, exception));
        return Task.CompletedTask;
    }
}
