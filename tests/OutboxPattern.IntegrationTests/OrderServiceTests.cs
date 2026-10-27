using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using OutboxPattern.Infrastructure.Orders;

namespace OutboxPattern.IntegrationTests;

[Collection(SqlServerCollection.Name)]
[Trait("Category", "Integration")]
public class OrderServiceTests(SqlServerFixture fixture)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task CreateOrderAsync_CommitsOrderAndOutboxMessageTogether()
    {
        await fixture.PrepareAsync();
        await using var db = fixture.CreateOrderDbContext();

        var order = await new OrderService(db).CreateOrderAsync(new CreateOrderRequest("customer-1", 10m), Ct);

        await using var verify = fixture.CreateOrderDbContext();
        Assert.Equal(1, await verify.Orders.CountAsync(o => o.Id == order.Id, Ct));
        var outbox = await verify.OutboxMessages.SingleAsync(m => m.AggregateId == order.Id, Ct);
        Assert.Equal(OrderCreated.EventType, outbox.EventType);
        Assert.Null(outbox.ProcessedAtUtc);
        Assert.Equal(0, outbox.RetryCount);
    }

    [Fact]
    public async Task CreateOrderAsync_ForcedFailureBeforeCommit_LeavesNeitherRow()
    {
        await fixture.PrepareAsync();
        var failBeforeCommit = new FailAfterSaveChangesInterceptor();
        await using var db = fixture.CreateOrderDbContext(failBeforeCommit);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            new OrderService(db).CreateOrderAsync(new CreateOrderRequest("customer-rollback", 10m), Ct));

        Assert.Equal(2, failBeforeCommit.RowsWrittenBeforeFailure);
        await using var verify = fixture.CreateOrderDbContext();
        Assert.Equal(0, await verify.Orders.CountAsync(Ct));
        Assert.Equal(0, await verify.OutboxMessages.CountAsync(Ct));
    }

    [Fact]
    public async Task NewOutboxRow_GetsNextAttemptAtUtcFromTheDatabaseClock()
    {
        await fixture.PrepareAsync();
        await using var db = fixture.CreateOrderDbContext();

        var order = await new OrderService(db).CreateOrderAsync(new CreateOrderRequest("customer-clock", 10m), Ct);

        await using var verify = fixture.CreateOrderDbContext();
        var outbox = await verify.OutboxMessages.SingleAsync(m => m.AggregateId == order.Id, Ct);
        Assert.NotEqual(default, outbox.NextAttemptAtUtc);

        var secondsFromDbClock = await verify.Database
            .SqlQuery<int>($"SELECT ABS(DATEDIFF(SECOND, NextAttemptAtUtc, SYSUTCDATETIME())) AS [Value] FROM OutboxMessages WHERE Id = {outbox.Id}")
            .SingleAsync(Ct);
        Assert.InRange(secondsFromDbClock, 0, 60);
    }

    private sealed class FailAfterSaveChangesInterceptor : SaveChangesInterceptor
    {
        public int RowsWrittenBeforeFailure { get; private set; }

        public override ValueTask<int> SavedChangesAsync(
            SaveChangesCompletedEventData eventData, int result, CancellationToken cancellationToken = default)
        {
            RowsWrittenBeforeFailure = result;
            throw new InvalidOperationException("Forced failure after SaveChanges, before commit.");
        }
    }
}
