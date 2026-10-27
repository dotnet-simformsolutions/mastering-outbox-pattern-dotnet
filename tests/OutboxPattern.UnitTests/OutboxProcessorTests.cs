using Microsoft.Extensions.Logging.Abstractions;
using OutboxPattern.Infrastructure.Outbox;
using OutboxPattern.Infrastructure.Publishing;

namespace OutboxPattern.UnitTests;

public class OutboxProcessorTests
{
    [Fact]
    public async Task SuccessfulSend_MarksProcessedWithTheClaimLock()
    {
        var message = NewMessage();
        var store = new FakeOutboxStore(message);
        var publisher = new StubPublisher();

        await CreateProcessor(store, publisher).ProcessBatchAsync(CancellationToken.None);

        Assert.Equal([message.Id], publisher.Sent);
        var mark = Assert.Single(store.MarkCalls);
        Assert.Equal(message.Id, mark.MessageId);
        Assert.Equal(store.ClaimLockIds.Single(), mark.LockId);
        Assert.Empty(store.RetryCalls);
    }

    [Fact]
    public async Task FailedSend_SchedulesRetryAndDoesNotMark()
    {
        var message = NewMessage();
        var store = new FakeOutboxStore(message);
        var brokerDown = new InvalidOperationException("Broker unavailable");
        var publisher = new StubPublisher(_ => throw brokerDown);

        await CreateProcessor(store, publisher).ProcessBatchAsync(CancellationToken.None);

        Assert.Empty(store.MarkCalls);
        var retry = Assert.Single(store.RetryCalls);
        Assert.Equal(message.Id, retry.MessageId);
        Assert.Equal(store.ClaimLockIds.Single(), retry.LockId);
        Assert.Same(brokerDown, retry.Exception);
        Assert.Equal(1, message.RetryCount);
    }

    [Fact]
    public async Task MarkFailureAfterSuccessfulSend_DoesNotScheduleRetryOrIncrementRetryCount()
    {
        var message = NewMessage();
        var store = new FakeOutboxStore(message)
        {
            MarkException = new InvalidOperationException("Database unavailable while marking")
        };
        var publisher = new StubPublisher();

        await CreateProcessor(store, publisher).ProcessBatchAsync(CancellationToken.None);

        Assert.Equal([message.Id], publisher.Sent);
        Assert.Single(store.MarkCalls);
        Assert.Empty(store.RetryCalls);
        Assert.Equal(0, message.RetryCount);
    }

    [Fact]
    public async Task Mark_UsesCancellationTokenNone_EvenWhenTheLoopIsCancelledAfterTheSend()
    {
        using var cts = new CancellationTokenSource();
        var message = NewMessage();
        var store = new FakeOutboxStore(message);
        var publisher = new StubPublisher(_ =>
        {
            cts.Cancel();
            return Task.CompletedTask;
        });

        await CreateProcessor(store, publisher).ProcessBatchAsync(cts.Token);

        Assert.True(cts.IsCancellationRequested);
        var mark = Assert.Single(store.MarkCalls);
        Assert.Equal(CancellationToken.None, mark.Token);
        Assert.False(mark.Token.CanBeCanceled);
    }

    private static OutboxProcessor CreateProcessor(IOutboxStore store, IOutboxPublisher publisher) =>
        new(store, publisher, NullLogger<OutboxProcessor>.Instance);

    private static OutboxMessage NewMessage() => new()
    {
        Id = Guid.NewGuid(),
        EventType = "OrderCreated",
        Payload = "{}",
        AggregateId = Guid.NewGuid(),
        CreatedAtUtc = DateTime.UtcNow,
        NextAttemptAtUtc = DateTime.UtcNow
    };
}
