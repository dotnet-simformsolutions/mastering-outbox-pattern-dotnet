using System.Diagnostics.Metrics;

namespace OutboxPattern.Infrastructure.Outbox;

public static class OutboxMetrics
{
    public const string MeterName = "OutboxPattern.Outbox";

    private static readonly Meter Meter = new(MeterName);

    public static readonly Counter<long> Published = Meter.CreateCounter<long>("outbox.messages.published");
    public static readonly Counter<long> Failed = Meter.CreateCounter<long>("outbox.messages.failed");
    public static readonly Counter<long> Exhausted = Meter.CreateCounter<long>("outbox.messages.exhausted");
}
