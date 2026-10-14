using Microsoft.EntityFrameworkCore;

namespace OutboxPattern.Consumer.Persistence;

public sealed class InventoryDbContext(DbContextOptions<InventoryDbContext> options) : DbContext(options)
{
    public DbSet<ProcessedMessage> ProcessedMessages => Set<ProcessedMessage>();
    public DbSet<StockReservation> StockReservations => Set<StockReservation>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ProcessedMessage>(processed =>
        {
            processed.HasKey(m => m.MessageId);
            processed.Property(m => m.MessageId).HasMaxLength(128);
            processed.Property(m => m.ProcessedAtUtc).HasColumnType("datetime2");
        });

        modelBuilder.Entity<StockReservation>(reservation =>
        {
            reservation.Property(r => r.Id).ValueGeneratedNever();
            reservation.Property(r => r.ReservedAtUtc).HasColumnType("datetime2");
            reservation.HasIndex(r => r.OrderId);
        });
    }
}
