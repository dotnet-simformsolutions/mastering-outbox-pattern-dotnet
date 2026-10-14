namespace OutboxPattern.Infrastructure.Orders;

public sealed record OrderCreated(Guid OrderId, string CustomerId, decimal Total, DateTime CreatedAtUtc);
