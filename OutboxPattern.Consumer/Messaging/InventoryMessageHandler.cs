using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using OutboxPattern.Consumer.Contracts;
using OutboxPattern.Consumer.Persistence;

namespace OutboxPattern.Consumer.Messaging;

public sealed class InventoryMessageHandler
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<InventoryMessageHandler> _logger;

    public InventoryMessageHandler(IServiceScopeFactory scopeFactory, ILogger<InventoryMessageHandler> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    public async Task HandleAsync(string messageId, string body, CancellationToken ct)
    {
        var evt = JsonSerializer.Deserialize<OrderCreated>(body)
            ?? throw new InvalidOperationException("Message body is not an OrderCreated event.");

        using var _ = _logger.BeginScope(new Dictionary<string, object>
        {
            ["MessageId"] = messageId,
            ["OrderId"] = evt.OrderId
        });

        await using var scope = _scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();

        await using var tx = await db.Database.BeginTransactionAsync(ct);

        db.ProcessedMessages.Add(new ProcessedMessage
        {
            MessageId = messageId,      // primary key
            ProcessedAtUtc = DateTime.UtcNow
        });
        db.StockReservations.Add(new StockReservation
        {
            Id = Guid.NewGuid(),
            OrderId = evt.OrderId,
            ReservedAtUtc = DateTime.UtcNow
        });

        try
        {
            await db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);
            _logger.LogInformation("Stock reserved for order {OrderId}", evt.OrderId);
        }
        catch (DbUpdateException ex) when (IsUniqueKeyViolation(ex))
        {
            _logger.LogInformation("Duplicate {MessageId} ignored", messageId);
        }
    }

    private static bool IsUniqueKeyViolation(DbUpdateException ex) =>
        ex.InnerException is SqlException { Number: 2627 or 2601 };
}
