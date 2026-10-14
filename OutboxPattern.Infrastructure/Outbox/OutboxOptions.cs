namespace OutboxPattern.Infrastructure.Outbox;

public sealed class OutboxOptions
{
    public const string SectionName = "Outbox";

    public string Publisher { get; set; } = "";
    public string QueueName { get; set; } = "orders";
    public int BatchSize { get; set; } = 50;
    public int MaxRetries { get; set; } = 10;
    public int LeaseSeconds { get; set; } = 30;
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(5);
}
