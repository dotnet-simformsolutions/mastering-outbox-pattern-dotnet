using System.Text.Json;using OutboxPattern.Infrastructure.Outbox; using OutboxPattern.Infrastructure.Persistence;

namespace OutboxPattern.Infrastructure.Orders;

public sealed class OrderService
{
    private readonly OrderDbContext _db;

    public OrderService(OrderDbContext db)
    {
        _db = db;
    }

    public async Task<Order> CreateOrderAsync(CreateOrderRequest request, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        var order = new Order
        {
            Id = Guid.NewGuid(),
            CustomerId = request.CustomerId,
            Total = request.Total,
            CreatedAtUtc = now
        };
        var orderCreated = new OrderCreated(order.Id, order.CustomerId, order.Total, now);

        await using var tx = await _db.Database.BeginTransactionAsync(ct);

        _db.Orders.Add(order);
        _db.OutboxMessages.Add(new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = nameof(OrderCreated),
            Payload = JsonSerializer.Serialize(orderCreated),
            AggregateId = order.Id,
            CreatedAtUtc = now,
            NextAttemptAtUtc = now
        });

        await _db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);    // both rows, or neither

        return order;
    }
}
