using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using OutboxPattern.Consumer.Persistence;
using OutboxPattern.Infrastructure.Outbox;
using OutboxPattern.Infrastructure.Persistence;
using OutboxPattern.Infrastructure.Publishing;
using Testcontainers.MsSql;

namespace OutboxPattern.IntegrationTests;

[CollectionDefinition(Name)]
public sealed class SqlServerCollection : ICollectionFixture<SqlServerFixture>
{
    public const string Name = "SqlServer";
}

public sealed class SqlServerFixture : IAsyncLifetime
{
    private MsSqlContainer? _container;

    public string? SkipReason { get; private set; }
    public string OrdersConnectionString { get; private set; } = "";
    public string InventoryConnectionString { get; private set; } = "";

    public async ValueTask InitializeAsync()
    {
        var serverConnectionString = Environment.GetEnvironmentVariable("TEST_SQL_CONNECTION");

        if (string.IsNullOrWhiteSpace(serverConnectionString))
        {
            try
            {
                _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();
                await _container.StartAsync();
                serverConnectionString = _container.GetConnectionString();
            }
            catch (Exception ex)
            {
                SkipReason = "No SQL Server available: TEST_SQL_CONNECTION is not set and Testcontainers could not start " +
                             $"SQL Server ({ex.GetType().Name}: {ex.Message})";
                return;
            }
        }

        var runId = Guid.NewGuid().ToString("N")[..8];
        OrdersConnectionString = WithDatabase(serverConnectionString, $"OutboxTests_{runId}_Orders");
        InventoryConnectionString = WithDatabase(serverConnectionString, $"OutboxTests_{runId}_Inventory");

        await using var orders = CreateOrderDbContext();
        await orders.Database.MigrateAsync();

        await using var inventory = CreateInventoryDbContext();
        await inventory.Database.MigrateAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (SkipReason is null)
        {
            await using var orders = CreateOrderDbContext();
            await orders.Database.EnsureDeletedAsync();

            await using var inventory = CreateInventoryDbContext();
            await inventory.Database.EnsureDeletedAsync();
        }

        if (_container is not null)
            await _container.DisposeAsync();
    }

    public async Task PrepareAsync()
    {
        if (SkipReason is not null)
            Assert.Skip(SkipReason);

        await using var orders = CreateOrderDbContext();
        await orders.Database.ExecuteSqlRawAsync("DELETE FROM OutboxMessages; DELETE FROM Orders;");

        await using var inventory = CreateInventoryDbContext();
        await inventory.Database.ExecuteSqlRawAsync("DELETE FROM StockReservations; DELETE FROM ProcessedMessages;");
    }

    public OrderDbContext CreateOrderDbContext(params IInterceptor[] interceptors) =>
        new(new DbContextOptionsBuilder<OrderDbContext>()
            .UseSqlServer(OrdersConnectionString)
            .AddInterceptors(interceptors)
            .Options);

    public InventoryDbContext CreateInventoryDbContext() =>
        new(new DbContextOptionsBuilder<InventoryDbContext>()
            .UseSqlServer(InventoryConnectionString)
            .Options);

    public static OutboxOptions FastOptions() => new()
    {
        BatchSize = 50,
        MaxRetries = 10,
        LeaseSeconds = 30,
        BaseDelay = TimeSpan.FromMilliseconds(50),
        MaxDelay = TimeSpan.FromMilliseconds(500)
    };

    public static SqlServerOutboxStore CreateStore(OrderDbContext db, OutboxOptions options) =>
        new(db, Options.Create(options), NullLogger<SqlServerOutboxStore>.Instance);

    public static OutboxProcessor CreateProcessor(IOutboxStore store, IOutboxPublisher publisher) =>
        new(store, publisher, NullLogger<OutboxProcessor>.Instance);

    public static FakeOutboxPublisher CreateFakePublisher(int failFirstNAttempts = 0, bool simulateCrashAfterPublish = false) =>
        new(Options.Create(new FakePublisherOptions
        {
            FailFirstNAttempts = failFirstNAttempts,
            SimulateCrashAfterPublish = simulateCrashAfterPublish
        }), NullLogger<FakeOutboxPublisher>.Instance);

    private static string WithDatabase(string connectionString, string database) =>
        new SqlConnectionStringBuilder(connectionString) { InitialCatalog = database }.ConnectionString;
}
