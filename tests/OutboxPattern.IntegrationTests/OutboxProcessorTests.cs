using Microsoft.EntityFrameworkCore;
using OutboxPattern.Infrastructure.Orders;
using OutboxPattern.Infrastructure.Outbox;

namespace OutboxPattern.IntegrationTests;

[Collection(SqlServerCollection.Name)]
[Trait("Category", "Integration")]
public class OutboxProcessorTests(SqlServerFixture fixture)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    [Fact]
    public async Task FailedPublish_SchedulesRetryInTheFutureByTheDatabaseClock()
    {
        await fixture.PrepareAsync();
        var messageId = await CreateOrderAsync();
        var options = SqlServerFixture.FastOptions();
        options.BaseDelay = TimeSpan.FromSeconds(30);
        options.MaxDelay = TimeSpan.FromMinutes(1);
        await using var db = fixture.CreateOrderDbContext();
        var store = SqlServerFixture.CreateStore(db, options);
        var processor = SqlServerFixture.CreateProcessor(store, SqlServerFixture.CreateFakePublisher(failFirstNAttempts: 1));

        await processor.ProcessBatchAsync(Ct);

        var row = await GetRowAsync(messageId);
        Assert.Equal(1, row.RetryCount);
        Assert.Null(row.ProcessedAtUtc);
        Assert.Null(row.LockId);
        Assert.Null(row.LockedUntilUtc);
        Assert.Contains("Simulated broker failure", row.LastError);
        Assert.True(await IsNextAttemptInTheFutureAsync(messageId));
        Assert.Empty(await store.ClaimBatchAsync(Guid.NewGuid(), Ct));
    }

    [Fact]
    public async Task CrashAfterPublish_SendsTheSameMessageIdTwice()
    {
        await fixture.PrepareAsync();
        var messageId = await CreateOrderAsync();
        var publisher = SqlServerFixture.CreateFakePublisher(simulateCrashAfterPublish: true);
        await using var db = fixture.CreateOrderDbContext();
        var processor = SqlServerFixture.CreateProcessor(SqlServerFixture.CreateStore(db, SqlServerFixture.FastOptions()), publisher);

        await RunUntilAsync(processor, async () => (await GetRowAsync(messageId)).ProcessedAtUtc is not null);

        Assert.Equal(2, publisher.SendCounts[messageId]);
        Assert.Single(publisher.SendCounts);
        var row = await GetRowAsync(messageId);
        Assert.NotNull(row.ProcessedAtUtc);
        Assert.Equal(1, row.RetryCount);
    }

    [Fact]
    public async Task AfterMaxRetries_RowStaysUnclaimedAndCountsAsExhausted()
    {
        await fixture.PrepareAsync();
        var messageId = await CreateOrderAsync();
        var options = SqlServerFixture.FastOptions();
        options.MaxRetries = 2;
        await using var db = fixture.CreateOrderDbContext();
        var store = SqlServerFixture.CreateStore(db, options);
        var processor = SqlServerFixture.CreateProcessor(store, SqlServerFixture.CreateFakePublisher(failFirstNAttempts: 100));

        await RunUntilAsync(processor, async () => (await GetRowAsync(messageId)).RetryCount >= options.MaxRetries);
        await Task.Delay(options.MaxDelay * 2, Ct);

        Assert.Equal(0, await processor.ProcessBatchAsync(Ct));
        var row = await GetRowAsync(messageId);
        Assert.Equal(options.MaxRetries, row.RetryCount);
        Assert.Null(row.ProcessedAtUtc);
        Assert.NotNull(row.LastError);
        var stats = await store.GetStatsAsync(Ct);
        Assert.Equal(1, stats.ExhaustedCount);
        Assert.Equal(0, stats.PendingCount);
    }

    [Fact]
    public async Task SendSucceedsThenMarkFails_RowStaysPendingAndIsReclaimedAfterTheLeaseExpires()
    {
        await fixture.PrepareAsync();
        var messageId = await CreateOrderAsync();
        var options = SqlServerFixture.FastOptions();
        options.LeaseSeconds = 2;
        var publisher = SqlServerFixture.CreateFakePublisher();
        await using var db = fixture.CreateOrderDbContext();
        var store = SqlServerFixture.CreateStore(db, options);
        var processor = SqlServerFixture.CreateProcessor(new MarkFailingStore(store), publisher);

        await processor.ProcessBatchAsync(Ct);

        Assert.Equal(1, publisher.SendCounts[messageId]);
        var row = await GetRowAsync(messageId);
        Assert.Null(row.ProcessedAtUtc);
        Assert.Equal(0, row.RetryCount);
        Assert.NotNull(row.LockId);
        Assert.Empty(await store.ClaimBatchAsync(Guid.NewGuid(), Ct));

        var reclaimed = await ClaimWhenAvailableAsync(store);

        Assert.Equal(messageId, Assert.Single(reclaimed).Id);
        Assert.Equal(0, reclaimed[0].RetryCount);
    }

    private async Task<Guid> CreateOrderAsync()
    {
        await using var db = fixture.CreateOrderDbContext();
        var order = await new OrderService(db).CreateOrderAsync(new CreateOrderRequest("customer-it", 25m), Ct);
        return await db.OutboxMessages.Where(m => m.AggregateId == order.Id).Select(m => m.Id).SingleAsync();
    }

    private async Task<OutboxMessage> GetRowAsync(Guid messageId)
    {
        await using var db = fixture.CreateOrderDbContext();
        return await db.OutboxMessages.AsNoTracking().SingleAsync(m => m.Id == messageId);
    }

    private async Task<bool> IsNextAttemptInTheFutureAsync(Guid messageId)
    {
        await using var db = fixture.CreateOrderDbContext();
        return await db.Database
            .SqlQuery<bool>($"SELECT CAST(CASE WHEN NextAttemptAtUtc > SYSUTCDATETIME() THEN 1 ELSE 0 END AS bit) AS [Value] FROM OutboxMessages WHERE Id = {messageId}")
            .SingleAsync();
    }

    private static async Task RunUntilAsync(OutboxProcessor processor, Func<Task<bool>> done)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!await done())
        {
            Assert.True(DateTime.UtcNow < deadline, "Condition not reached before the timeout.");
            await processor.ProcessBatchAsync(Ct);
            await Task.Delay(100);
        }
    }

    private static async Task<List<OutboxMessage>> ClaimWhenAvailableAsync(SqlServerOutboxStore store)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (true)
        {
            var claimed = await store.ClaimBatchAsync(Guid.NewGuid(), Ct);
            if (claimed.Count > 0)
                return claimed;

            Assert.True(DateTime.UtcNow < deadline, "Row was not reclaimed before the timeout.");
            await Task.Delay(250);
        }
    }

    private sealed class MarkFailingStore(IOutboxStore inner) : IOutboxStore
    {
        public Task<List<OutboxMessage>> ClaimBatchAsync(Guid lockId, CancellationToken ct) =>
            inner.ClaimBatchAsync(lockId, ct);

        public Task MarkProcessedAsync(Guid messageId, Guid lockId, CancellationToken ct) =>
            throw new InvalidOperationException("Simulated database failure while marking the row processed.");

        public Task ScheduleRetryAsync(OutboxMessage message, Guid lockId, Exception exception, CancellationToken ct) =>
            inner.ScheduleRetryAsync(message, lockId, exception, ct);
    }
}
