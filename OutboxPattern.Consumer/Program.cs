using Azure.Messaging.ServiceBus;
using Microsoft.EntityFrameworkCore;
using OutboxPattern.Consumer.Messaging;
using OutboxPattern.Consumer.Persistence;

var builder = Host.CreateApplicationBuilder(args);

var inventoryConnectionString = builder.Configuration.GetConnectionString("InventoryDb")
    ?? throw new InvalidOperationException("ConnectionStrings:InventoryDb is not configured.");

builder.Services.AddDbContext<InventoryDbContext>(options => options.UseSqlServer(inventoryConnectionString));
builder.Services.AddSingleton<InventoryMessageHandler>();

if (args.Contains("--replay-demo"))
{
    using var demoHost = builder.Build();
    await ReplayDemo.RunAsync(demoHost.Services, CancellationToken.None);
    return;
}

builder.Services.AddSingleton(_ => new ServiceBusClient(
    builder.Configuration.GetConnectionString("ServiceBus")
        ?? throw new InvalidOperationException("ConnectionStrings:ServiceBus is not configured. Use --replay-demo to run without Azure.")));
builder.Services.AddHostedService<ServiceBusInventoryConsumer>();

var host = builder.Build();
host.Run();
