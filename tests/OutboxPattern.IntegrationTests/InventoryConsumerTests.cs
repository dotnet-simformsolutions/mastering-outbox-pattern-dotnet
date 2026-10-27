using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using OutboxPattern.Consumer.Contracts;
using OutboxPattern.Consumer.Messaging;
using OutboxPattern.Consumer.Persistence;

namespace OutboxPattern.IntegrationTests;

[Collection(SqlServerCollection.Name)]
[Trait("Category", "Integration")]
public class InventoryConsumerTests(SqlServerFixture fixture)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task SameMessageHandledTwice_CreatesExactlyOneStockReservation()
    {
        await fixture.PrepareAsync();
        await using var services = BuildServices();
        var handler = services.GetRequiredService<InventoryMessageHandler>();
        var orderId = Guid.NewGuid();
        var messageId = Guid.NewGuid().ToString();
        var body = JsonSerializer.Serialize(new OrderCreated(orderId, "customer-dup", 10m, DateTime.UtcNow));

        await handler.HandleAsync(messageId, body, Ct);
        await handler.HandleAsync(messageId, body, Ct);

        await using var db = fixture.CreateInventoryDbContext();
        Assert.Equal(1, await db.StockReservations.CountAsync(r => r.OrderId == orderId, Ct));
        Assert.Equal(1, await db.ProcessedMessages.CountAsync(m => m.MessageId == messageId, Ct));
    }

    [Fact]
    public async Task ReplayDemo_ReservesStockOnce()
    {
        await fixture.PrepareAsync();
        await using var services = BuildServices();

        await ReplayDemo.RunAsync(services, Ct);

        await using var db = fixture.CreateInventoryDbContext();
        Assert.Equal(1, await db.StockReservations.CountAsync(Ct));
        Assert.Equal(1, await db.ProcessedMessages.CountAsync(Ct));
    }

    private ServiceProvider BuildServices() =>
        new ServiceCollection()
            .AddLogging()
            .AddDbContext<InventoryDbContext>(options => options.UseSqlServer(fixture.InventoryConnectionString))
            .AddSingleton<InventoryMessageHandler>()
            .BuildServiceProvider();
}
