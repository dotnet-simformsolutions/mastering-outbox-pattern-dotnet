using OutboxPattern.Infrastructure.Outbox;
using OutboxPattern.Infrastructure.Publishing;

namespace OutboxPattern.UnitTests;

internal sealed class StubPublisher : IOutboxPublisher
{
    private readonly Func<OutboxMessage, Task> _onPublish;

    public StubPublisher(Func<OutboxMessage, Task>? onPublish = null)
    {
        _onPublish = onPublish ?? (_ => Task.CompletedTask);
    }

    public List<Guid> Sent { get; } = [];

    public async Task PublishAsync(OutboxMessage message, CancellationToken ct)
    {
        await _onPublish(message);
        Sent.Add(message.Id);
    }
}
