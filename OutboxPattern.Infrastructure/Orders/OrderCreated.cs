namespace OutboxPattern.Infrastructure.Orders;

public sealed record OrderCreated(Guid OrderId, string CustomerId, decimal Total, DateTime CreatedAtUtc)
{
    public const string EventType = "orders.order-created.v1";
}
