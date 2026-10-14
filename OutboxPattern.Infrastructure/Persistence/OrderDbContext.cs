using Microsoft.EntityFrameworkCore;using OutboxPattern.Infrastructure.Orders; using OutboxPattern.Infrastructure.Outbox;

namespace OutboxPattern.Infrastructure.Persistence;

public sealed class OrderDbContext(DbContextOptions<OrderDbContext> options) : DbContext(options)
{
    public const int LastErrorMaxLength = 2000;
    public const int CustomerIdMaxLength = 100;

    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder)
    {
        configurationBuilder.Properties<DateTime>().HaveConversion<UtcDateTimeConverter>();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(order =>
        {
            order.Property(o => o.Id).ValueGeneratedNever();
            order.Property(o => o.CustomerId).HasMaxLength(CustomerIdMaxLength);
            order.Property(o => o.Total).HasPrecision(18, 2);
            order.Property(o => o.CreatedAtUtc).HasColumnType("datetime2");
        });

        modelBuilder.Entity<OutboxMessage>(outbox =>
        {
            outbox.Property(m => m.Id).ValueGeneratedNever();
            outbox.Property(m => m.EventType).HasMaxLength(200);
            outbox.Property(m => m.LastError).HasMaxLength(LastErrorMaxLength);
            outbox.Property(m => m.CreatedAtUtc).HasColumnType("datetime2");
            outbox.Property(m => m.ProcessedAtUtc).HasColumnType("datetime2");
            outbox.Property(m => m.NextAttemptAtUtc).HasColumnType("datetime2");
            outbox.Property(m => m.LockedUntilUtc).HasColumnType("datetime2");

            outbox.HasIndex(m => new { m.NextAttemptAtUtc, m.CreatedAtUtc })
                .HasDatabaseName("IX_OutboxMessages_Pending")
                .HasFilter("[ProcessedAtUtc] IS NULL");
        });
    }
}
