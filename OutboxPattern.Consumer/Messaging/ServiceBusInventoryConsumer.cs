using Azure.Messaging.ServiceBus;

namespace OutboxPattern.Consumer.Messaging;

public sealed class ServiceBusInventoryConsumer : BackgroundService
{
    private readonly ServiceBusClient _client;
    private readonly InventoryMessageHandler _handler;
    private readonly string _queueName;
    private readonly ILogger<ServiceBusInventoryConsumer> _logger;

    public ServiceBusInventoryConsumer(
        ServiceBusClient client,
        InventoryMessageHandler handler,
        IConfiguration configuration,
        ILogger<ServiceBusInventoryConsumer> logger)
    {
        _client = client;
        _handler = handler;
        _queueName = configuration["Inventory:QueueName"] ?? "orders";
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using var processor = _client.CreateProcessor(_queueName, new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 1
        });

        processor.ProcessMessageAsync += async args =>
        {
            await _handler.HandleAsync(args.Message.MessageId, args.Message.Body.ToString(), args.CancellationToken);
            await args.CompleteMessageAsync(args.Message, args.CancellationToken);
        };

        processor.ProcessErrorAsync += args =>
        {
            _logger.LogError(args.Exception, "Service Bus error from {ErrorSource}", args.ErrorSource);
            return Task.CompletedTask;
        };

        _logger.LogInformation("Inventory consumer listening on queue {QueueName}", _queueName);
        await processor.StartProcessingAsync(stoppingToken);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
        }

        await processor.StopProcessingAsync();
    }
}
