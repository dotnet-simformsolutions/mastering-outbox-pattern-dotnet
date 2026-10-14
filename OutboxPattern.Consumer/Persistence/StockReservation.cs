namespace OutboxPattern.Consumer.Persistence;

public sealed class StockReservation
{
    public Guid Id { get; set; }
    public Guid OrderId { get; set; }
    public DateTime ReservedAtUtc { get; set; }
}
