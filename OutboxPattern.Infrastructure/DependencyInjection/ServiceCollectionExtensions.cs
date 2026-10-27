using Azure.Messaging.ServiceBus;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OutboxPattern.Infrastructure.Orders;
using OutboxPattern.Infrastructure.Outbox;
using OutboxPattern.Infrastructure.Persistence;
using OutboxPattern.Infrastructure.Publishing;

namespace OutboxPattern.Infrastructure.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddOrdersDb(this IServiceCollection services, IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString("OrdersDb")
            ?? throw new InvalidOperationException("ConnectionStrings:OrdersDb is not configured.");

        services.AddDbContext<OrderDbContext>(options => options.UseSqlServer(connectionString));
        services.Configure<OutboxOptions>(configuration.GetSection(OutboxOptions.SectionName));
        services.AddScoped<OrderService>();
        services.AddScoped<SqlServerOutboxStore>();
        services.AddScoped<IOutboxStore>(sp => sp.GetRequiredService<SqlServerOutboxStore>());

        return services;
    }

    public static IServiceCollection AddOutboxPublishing(this IServiceCollection services, IConfiguration configuration)
    {
        var outboxOptions = configuration.GetSection(OutboxOptions.SectionName).Get<OutboxOptions>() ?? new OutboxOptions();

        switch (outboxOptions.Publisher)
        {
            case "Fake":
                services.Configure<FakePublisherOptions>(configuration.GetSection(FakePublisherOptions.SectionName));
                services.AddSingleton<IOutboxPublisher, FakeOutboxPublisher>();
                break;

            case "AzureServiceBus":
                var serviceBusConnectionString = configuration.GetConnectionString("ServiceBus")
                    ?? throw new InvalidOperationException("ConnectionStrings:ServiceBus is not configured.");

                services.AddSingleton(_ => new ServiceBusClient(
                    serviceBusConnectionString,
                    new ServiceBusClientOptions
                    {
                        RetryOptions = new ServiceBusRetryOptions { MaxRetries = 2, TryTimeout = TimeSpan.FromSeconds(10) }
                    }));
                services.AddSingleton(sp => sp.GetRequiredService<ServiceBusClient>().CreateSender(outboxOptions.QueueName));
                services.AddSingleton<IOutboxPublisher, ServiceBusOutboxPublisher>();
                break;

            default:
                throw new InvalidOperationException("Outbox:Publisher must be 'Fake' or 'AzureServiceBus'.");
        }

        services.AddScoped<OutboxProcessor>();

        return services;
    }
}
