using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OutboxPattern.Infrastructure.Outbox;

namespace OutboxPattern.Infrastructure.Publishing;

public sealed class FakeOutboxPublisher : IOutboxPublisher
{
    private readonly FakePublisherOptions _options;
    private readonly ILogger<FakeOutboxPublisher> _logger;
    private readonly ConcurrentDictionary<Guid, int> _sendCounts = new();
    private int _attempts;

    public FakeOutboxPublisher(IOptions<FakePublisherOptions> options, ILogger<FakeOutboxPublisher> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public IReadOnlyDictionary<Guid, int> SendCounts => _sendCounts;

    public Task PublishAsync(OutboxMessage message, CancellationToken ct)
    {
        var attempt = Interlocked.Increment(ref _attempts);
        if (attempt <= _options.FailFirstNAttempts)
        {
            throw new InvalidOperationException($"Simulated broker failure (attempt {attempt} of {_options.FailFirstNAttempts}).");
        }

        var sendCount = _sendCounts.AddOrUpdate(message.Id, 1, (_, count) => count + 1);
        _logger.LogInformation("Fake publisher sent {EventType} with MessageId {MessageId} (send #{SendCount})",
            message.EventType, message.Id, sendCount);

        if (sendCount > 1)
        {
            _logger.LogWarning("Duplicate send: MessageId {MessageId} has been sent {SendCount} times", message.Id, sendCount);
        }

        if (_options.SimulateCrashAfterPublish && sendCount == 1)
        {
            throw new InvalidOperationException("Simulated crash after publish, before the row was marked processed.");
        }

        return Task.CompletedTask;
    }
}
