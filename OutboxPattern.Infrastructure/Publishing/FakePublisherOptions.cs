namespace OutboxPattern.Infrastructure.Publishing;

public sealed class FakePublisherOptions
{
    public const string SectionName = "Outbox:Fake";

    public int FailFirstNAttempts { get; set; }
    public bool SimulateCrashAfterPublish { get; set; }
}
