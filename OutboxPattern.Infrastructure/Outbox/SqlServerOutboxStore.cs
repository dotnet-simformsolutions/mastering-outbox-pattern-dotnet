using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OutboxPattern.Infrastructure.Persistence;

namespace OutboxPattern.Infrastructure.Outbox;

public sealed class SqlServerOutboxStore
{
    private readonly OrderDbContext _db;
    private readonly OutboxOptions _options;
    private readonly ILogger<SqlServerOutboxStore> _logger;

    public SqlServerOutboxStore(OrderDbContext db, IOptions<OutboxOptions> options, ILogger<SqlServerOutboxStore> logger)
    {
        _db = db;
        _options = options.Value;
        _logger = logger;
    }

    public Task<List<OutboxMessage>> ClaimBatchAsync(Guid lockId, CancellationToken ct)
    {
        var batchSize = new SqlParameter("batchSize", _options.BatchSize);
        var maxRetries = new SqlParameter("maxRetries", _options.MaxRetries);
        var lockIdParameter = new SqlParameter("lockId", lockId);
        var leaseSeconds = new SqlParameter("leaseSeconds", _options.LeaseSeconds);

        return _db.OutboxMessages
            .FromSql($"""
                WITH batch AS (
                    SELECT TOP ({batchSize}) *
                    FROM OutboxMessages WITH (UPDLOCK, READPAST, ROWLOCK)
                    WHERE ProcessedAtUtc IS NULL
                      AND RetryCount < {maxRetries}
                      AND NextAttemptAtUtc <= SYSUTCDATETIME()
                      AND (LockedUntilUtc IS NULL OR LockedUntilUtc < SYSUTCDATETIME())
                    ORDER BY CreatedAtUtc
                )
                UPDATE batch
                SET LockId = {lockIdParameter},
                    LockedUntilUtc = DATEADD(SECOND, {leaseSeconds}, SYSUTCDATETIME())
                OUTPUT inserted.*;
                """)
            .AsNoTracking()
            .ToListAsync(ct);
    }

    public async Task MarkProcessedAsync(Guid messageId, Guid lockId, CancellationToken ct)
    {
        var updated = await _db.OutboxMessages
            .Where(m => m.Id == messageId && m.LockId == lockId)
            .ExecuteUpdateAsync(s => s
                .SetProperty(m => m.ProcessedAtUtc, DateTime.UtcNow)
                .SetProperty(m => m.LockId, (Guid?)null)
                .SetProperty(m => m.LockedUntilUtc, (DateTime?)null), ct);

        if (updated == 0)
            _logger.LogWarning("Lease lost; event may be published again");
    }

    public async Task ScheduleRetryAsync(OutboxMessage message, Guid lockId, Exception exception, CancellationToken ct)
    {
        var retryCount = message.RetryCount + 1;
        var delay = BackoffCalculator.GetDelay(retryCount, _options.BaseDelay, _options.MaxDelay);
        var lastError = Truncate($"{exception.GetType().Name}: {exception.Message}", OrderDbContext.LastErrorMaxLength);

        var updated = await _db.OutboxMessages
            .Where(m => m.Id == message.Id && m.LockId == lockId)
            .ExecuteUpdateAsync(s => s
                .SetProperty(m => m.RetryCount, retryCount)
                .SetProperty(m => m.NextAttemptAtUtc, DateTime.UtcNow.Add(delay))
                .SetProperty(m => m.LastError, lastError)
                .SetProperty(m => m.LockId, (Guid?)null)
                .SetProperty(m => m.LockedUntilUtc, (DateTime?)null), ct);

        OutboxMetrics.Failed.Add(1);

        if (updated == 0)
        {
            _logger.LogWarning(exception, "Publish failed and lease lost; another worker owns the message now");
            return;
        }

        if (retryCount >= _options.MaxRetries)
        {
            OutboxMetrics.Exhausted.Add(1);
            _logger.LogError(exception,
                "Publish failed {RetryCount} times; MaxRetries reached, message left in the outbox for manual re-queue",
                retryCount);
            return;
        }

        _logger.LogWarning(exception,
            "Publish failed (attempt {RetryCount} of {MaxRetries}); next attempt in {RetryDelaySeconds:F1}s",
            retryCount, _options.MaxRetries, delay.TotalSeconds);
    }

    public async Task<OutboxStats> GetStatsAsync(CancellationToken ct)
    {
        var pending = _db.OutboxMessages.Where(m => m.ProcessedAtUtc == null && m.RetryCount < _options.MaxRetries);

        var pendingCount = await pending.CountAsync(ct);
        var oldestCreatedAtUtc = await pending.MinAsync(m => (DateTime?)m.CreatedAtUtc, ct);
        var exhaustedCount = await _db.OutboxMessages
            .CountAsync(m => m.ProcessedAtUtc == null && m.RetryCount >= _options.MaxRetries, ct);

        double? oldestAgeSeconds = oldestCreatedAtUtc is null
            ? null
            : Math.Round((DateTime.UtcNow - oldestCreatedAtUtc.Value).TotalSeconds, 1);

        return new OutboxStats(pendingCount, oldestAgeSeconds, exhaustedCount);
    }

    private static string Truncate(string value, int maxLength) =>
        value.Length <= maxLength ? value : value[..maxLength];
}
