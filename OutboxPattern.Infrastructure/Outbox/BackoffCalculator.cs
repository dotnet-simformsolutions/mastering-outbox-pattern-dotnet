namespace OutboxPattern.Infrastructure.Outbox;

public static class BackoffCalculator
{
    private const double JitterFactor = 0.2;

    public static TimeSpan GetDelay(int retryCount, TimeSpan baseDelay, TimeSpan maxDelay)
    {
        var exponentialMs = baseDelay.TotalMilliseconds * Math.Pow(2, Math.Max(retryCount - 1, 0));
        var cappedMs = Math.Min(exponentialMs, maxDelay.TotalMilliseconds);
        var jitterMs = Random.Shared.NextDouble() * cappedMs * JitterFactor;

        return TimeSpan.FromMilliseconds(Math.Min(cappedMs + jitterMs, maxDelay.TotalMilliseconds));
    }
}
