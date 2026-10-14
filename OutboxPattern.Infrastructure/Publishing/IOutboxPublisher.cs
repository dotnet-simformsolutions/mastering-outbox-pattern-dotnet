using OutboxPattern.Infrastructure.Outbox;

namespace OutboxPattern.Infrastructure.Publishing;

public interface IOutboxPublisher
{
    Task PublishAsync(OutboxMessage message, CancellationToken ct);
}
