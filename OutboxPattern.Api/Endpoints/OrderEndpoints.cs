using Microsoft.EntityFrameworkCore;
using OutboxPattern.Infrastructure.Orders;
using OutboxPattern.Infrastructure.Persistence;

namespace OutboxPattern.Api.Endpoints;

public static class OrderEndpoints
{
    public static IEndpointRouteBuilder MapOrderEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/orders", CreateOrderAsync);
        app.MapGet("/orders/{id:guid}", GetOrderAsync);

        return app;
    }

    private static async Task<IResult> CreateOrderAsync(
        CreateOrderRequest request,
        OrderService orderService,
        ILoggerFactory loggerFactory,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(request.CustomerId) || request.CustomerId.Length > OrderDbContext.CustomerIdMaxLength || request.Total <= 0)
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                ["order"] = [$"CustomerId is required (max {OrderDbContext.CustomerIdMaxLength} characters) and Total must be greater than zero."]
            });
        }

        var order = await orderService.CreateOrderAsync(request, ct);
        loggerFactory.CreateLogger(typeof(OrderEndpoints))
            .LogInformation("Order {OrderId} created with its OrderCreated outbox message", order.Id);

        return Results.Created($"/orders/{order.Id}", order);
    }

    private static async Task<IResult> GetOrderAsync(Guid id, OrderDbContext db, CancellationToken ct) =>
        await db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.Id == id, ct) is { } order
            ? Results.Ok(order)
            : Results.NotFound();
}
