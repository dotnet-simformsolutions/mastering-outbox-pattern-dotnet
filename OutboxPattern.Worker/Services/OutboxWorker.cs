using Microsoft.Extensions.Options;
using OutboxPattern.Infrastructure;
using OutboxPattern.Infrastructure.Outbox;

namespace OutboxPattern.Worker.Services;

public sealed class OutboxWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxWorker> _logger;

    public OutboxWorker(IServiceScopeFactory scopeFactory, IOptions<OutboxOptions> options, ILogger<OutboxWorker> logger)
    {
        _scopeFactory = scopeFactory;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Outbox worker started: publisher {Publisher}, batch size {BatchSize}, polling every {PollingInterval}",
            _options.Publisher, _options.BatchSize, _options.PollingInterval);

        using var timer = new PeriodicTimer(_options.PollingInterval);

        do
        {
            try
            {
                using var scope = _scopeFactory.CreateScope();
                var processor = scope.ServiceProvider.GetRequiredService<OutboxProcessor>();
                await processor.ProcessBatchAsync(stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Outbox polling cycle failed; retrying on the next tick");
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }
}
