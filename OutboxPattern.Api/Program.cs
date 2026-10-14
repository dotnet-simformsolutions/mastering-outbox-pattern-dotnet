using OutboxPattern.Api.Endpoints;
using OutboxPattern.Infrastructure.DependencyInjection;
using OutboxPattern.Infrastructure.Persistence;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOrdersDb(builder.Configuration);
builder.Services.AddHealthChecks().AddDbContextCheck<OrderDbContext>("orders-db");

var app = builder.Build();

app.MapOrderEndpoints();
app.MapOutboxEndpoints();
app.MapHealthChecks("/health");

app.Run();
