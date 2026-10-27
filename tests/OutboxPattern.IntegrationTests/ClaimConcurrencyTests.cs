using OutboxPattern.Infrastructure.Outbox;

namespace OutboxPattern.IntegrationTests;

[Collection(SqlServerCollection.Name)]
[Trait("Category", "Integration")]
public class ClaimConcurrencyTests(SqlServerFixture fixture)
{
    private const int RowCount = 200;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task TwoConcurrentClaimers_GetDisjointRows()
    {
        await fixture.PrepareAsync();
        await SeedPendingRowsAsync();
        var options = SqlServerFixture.FastOptions();
        options.BatchSize = 30;
        await using var dbA = fixture.CreateOrderDbContext();
        await using var dbB = fixture.CreateOrderDbContext();
        var storeA = SqlServerFixture.CreateStore(dbA, options);
        var storeB = SqlServerFixture.CreateStore(dbB, options);
        var claimedByA = new List<Guid>();
        var claimedByB = new List<Guid>();

        while (true)
        {
            var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var claimA = Task.Run(async () => { await start.Task; return await storeA.ClaimBatchAsync(Guid.NewGuid(), Ct); });
            var claimB = Task.Run(async () => { await start.Task; return await storeB.ClaimBatchAsync(Guid.NewGuid(), Ct); });
            start.SetResult();

            var (a, b) = (await claimA, await claimB);
            if (a.Count == 0 && b.Count == 0)
                break;

            claimedByA.AddRange(a.Select(m => m.Id));
            claimedByB.AddRange(b.Select(m => m.Id));
        }

        Assert.Empty(claimedByA.Intersect(claimedByB));
        Assert.Equal(RowCount, claimedByA.Count + claimedByB.Count);
        Assert.Equal(RowCount, claimedByA.Concat(claimedByB).Distinct().Count());
        Assert.NotEmpty(claimedByA);
        Assert.NotEmpty(claimedByB);
    }

    private async Task SeedPendingRowsAsync()
    {
        await using var db = fixture.CreateOrderDbContext();
        for (var i = 0; i < RowCount; i++)
        {
            db.OutboxMessages.Add(new OutboxMessage
            {
                Id = Guid.NewGuid(),
                EventType = "OrderCreated",
                Payload = "{}",
                AggregateId = Guid.NewGuid(),
                CreatedAtUtc = DateTime.UtcNow
            });
        }

        await db.SaveChangesAsync();
    }
}
