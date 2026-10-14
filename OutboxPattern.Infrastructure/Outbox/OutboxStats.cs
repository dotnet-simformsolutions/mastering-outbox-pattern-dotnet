namespace OutboxPattern.Infrastructure.Outbox;

public sealed record OutboxStats(int PendingCount, double? OldestPendingAgeSeconds, int ExhaustedCount);
