using Azure.Messaging.ServiceBus;
using OutboxPattern.Infrastructure.Outbox;

namespace OutboxPattern.Infrastructure.Publishing;

public sealed class ServiceBusOutboxPublisher : IOutboxPublisher
{
    private readonly ServiceBusSender _sender;

    public ServiceBusOutboxPublisher(ServiceBusSender sender)
    {
        _sender = sender;
    }

    public async Task PublishAsync(OutboxMessage message, CancellationToken ct)
    {
        var sbMessage = new ServiceBusMessage(message.Payload)
        {
            MessageId = message.Id.ToString(),
            Subject = message.EventType,
            ContentType = "application/json",
            CorrelationId = message.AggregateId.ToString()
        };
        await _sender.SendMessageAsync(sbMessage, ct);
    }
}
