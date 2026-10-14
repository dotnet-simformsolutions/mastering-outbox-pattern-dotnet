namespace OutboxPattern.Consumer.Contracts;

public sealed record OrderCreated(Guid OrderId, string CustomerId, decimal Total, DateTime CreatedAtUtc);
