using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using OutboxPattern.Consumer.Contracts;
using OutboxPattern.Consumer.Persistence;

namespace OutboxPattern.Consumer.Messaging;

public static class ReplayDemo
{
    public static async Task RunAsync(IServiceProvider services, CancellationToken ct)
    {
        var handler = services.GetRequiredService<InventoryMessageHandler>();
        var logger = services.GetRequiredService<ILogger<InventoryMessageHandler>>();

        var orderId = Guid.NewGuid();
        var messageId = Guid.NewGuid().ToString();
        var body = JsonSerializer.Serialize(new OrderCreated(orderId, "customer-replay", 42.50m, DateTime.UtcNow));

        logger.LogInformation("Replay demo: delivering MessageId {MessageId} for order {OrderId} twice", messageId, orderId);

        await handler.HandleAsync(messageId, body, ct);
        await handler.HandleAsync(messageId, body, ct);

        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
        var reservations = await db.StockReservations.CountAsync(r => r.OrderId == orderId, ct);

        logger.LogInformation("Replay demo result: {ReservationCount} stock reservation(s) for order {OrderId}", reservations, orderId);
    }
}
