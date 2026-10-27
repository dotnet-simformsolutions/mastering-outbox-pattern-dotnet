using Microsoft.Extensions.Logging;
using OutboxPattern.Infrastructure.Publishing;

namespace OutboxPattern.Infrastructure.Outbox;

public sealed class OutboxProcessor
{
    private readonly IOutboxStore _store;
    private readonly IOutboxPublisher _publisher;
    private readonly ILogger<OutboxProcessor> _logger;

    public OutboxProcessor(IOutboxStore store, IOutboxPublisher publisher, ILogger<OutboxProcessor> logger)
    {
        _store = store;
        _publisher = publisher;
        _logger = logger;
    }

    public async Task<int> ProcessBatchAsync(CancellationToken ct)
    {
        var lockId = Guid.NewGuid();
        var claimed = await _store.ClaimBatchAsync(lockId, ct);

        if (claimed.Count > 0)
            _logger.LogInformation("Claimed {ClaimedCount} outbox messages with lock {LockId}", claimed.Count, lockId);

        foreach (var message in claimed)
        {
            using var _ = _logger.BeginScope(new Dictionary<string, object>
            {
                ["OutboxMessageId"] = message.Id,
                ["OrderId"] = message.AggregateId
            });

            try
            {
                await _publisher.PublishAsync(message, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                OutboxMetrics.Failed.Add(1);
                await _store.ScheduleRetryAsync(message, lockId, ex, ct);
                continue; // only a failed send counts as a retry
            }

            try
            {
                // The send already happened; don't let shutdown cancel the bookkeeping.
                await _store.MarkProcessedAsync(message.Id, lockId, CancellationToken.None);
                _logger.LogInformation("Published {EventType}", message.EventType);
                OutboxMetrics.Published.Add(1);
            }
            catch (Exception ex)
            {
                // Let the lease expire. The event may be sent again, which consumers handle.
                _logger.LogWarning(ex, "Sent but not marked; event may be republished");
            }
        }

        return claimed.Count;
    }
}
