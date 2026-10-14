using OutboxPattern.Infrastructure.Outbox;

namespace OutboxPattern.Api.Endpoints;

public static class OutboxEndpoints
{
    public static IEndpointRouteBuilder MapOutboxEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/outbox/stats", async (SqlServerOutboxStore store, CancellationToken ct) =>
            Results.Ok(await store.GetStatsAsync(ct)));

        return app;
    }
}
