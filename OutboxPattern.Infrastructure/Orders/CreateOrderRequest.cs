namespace OutboxPattern.Infrastructure.Orders;

public sealed record CreateOrderRequest(string CustomerId, decimal Total);
