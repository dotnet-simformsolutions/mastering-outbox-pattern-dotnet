using OutboxPattern.Infrastructure.DependencyInjection;
using OutboxPattern.Worker.Services;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddOrdersDb(builder.Configuration);
builder.Services.AddOutboxPublishing(builder.Configuration);
builder.Services.AddHostedService<OutboxWorker>();

var host = builder.Build();
host.Run();
