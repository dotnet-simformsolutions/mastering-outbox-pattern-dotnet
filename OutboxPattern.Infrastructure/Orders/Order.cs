namespace OutboxPattern.Infrastructure.Orders;

public sealed class Order
{
    public Guid Id { get; set; }
    public string CustomerId { get; set; } = default!;
    public decimal Total { get; set; }
    public DateTime CreatedAtUtc { get; set; }
}
