# Transactional Outbox Pattern: .NET 10 PoC

A small, runnable proof of concept of the **Transactional Outbox Pattern** with ASP.NET Core, EF Core, SQL Server and Azure Service Bus.

`POST /orders` saves an `Order` and an `OrderCreated` outbox row in **one** database transaction. A background worker publishes pending outbox rows to an Azure Service Bus queue. An Inventory consumer reserves stock and ignores duplicates. No broker call ever happens inside a database transaction.

## How it works

```text
  Client
    | POST /orders
    v
+-----------------------------------+
| OutboxPattern.Api                 |
|   BEGIN TRANSACTION               |
|     INSERT INTO Orders            |
|     INSERT INTO OutboxMessages    |
|   COMMIT                          |
+-----------------------------------+
                 |
        [ SQL Server: OutboxOrders ]
                 ^
                 | 1. claim pending rows (lease)
                 | 3. mark processed / schedule retry
+-----------------------------------+
| OutboxPattern.Worker              |
+-----------------------------------+
                 | 2. send (MessageId = outbox Id)
                 v
      [ Azure Service Bus queue ]
                 |
                 v
+-----------------------------------+
| OutboxPattern.Consumer            |   [ SQL Server: OutboxInventory ]
|   ProcessedMessages + Reservation |
|   in one transaction              |
+-----------------------------------+
```

- **Saving:** `OrderService.CreateOrderAsync` adds the order and its outbox row, calls `SaveChangesAsync` once and commits. Either both rows exist or neither does. `NextAttemptAtUtc` is filled in by SQL Server (`DEFAULT SYSUTCDATETIME()`), so a new row is eligible immediately by the database clock.
- **Claiming:** the worker claims up to `BatchSize` rows in one atomic SQL statement (`UPDLOCK, READPAST, ROWLOCK`). Each claimed row gets a `LockId` and a `LockedUntilUtc` lease. Several workers can run at once without picking the same row, and no transaction stays open while publishing.
- **Publishing:** each row is sent with `MessageId = outbox Id`, `Subject = EventType`, `ContentType = application/json` and `CorrelationId = OrderId`.
- **Marking:** after a successful send, `ProcessedAtUtc` is set, but only if the worker still holds the lease. Otherwise it logs `Lease lost; event may be published again`. Marking runs with `CancellationToken.None`, so a shutdown right after a send doesn't skip it.
- **Send succeeded, mark failed:** the worker logs `Sent but not marked; event may be republished` and does **not** schedule a retry or increase `RetryCount`. The lease expires and the row is published again, which is the at-least-once window.
- **Retrying:** when a send fails, `RetryCount` goes up, `LastError` is saved (max 2000 chars) and the next attempt is delayed. The delay doubles each time (2s, 4s, 8s…), has a little randomness added, and is capped at `MaxDelay`. The delay is calculated in C#, but `NextAttemptAtUtc` is written by SQL Server as `DATEADD(MILLISECOND, @delayMs, SYSUTCDATETIME())`.
- **Giving up:** after `MaxRetries` failures the row is logged at Error level and **left in place, never deleted**, so you can re-queue it.
- **Crash recovery:** if a worker dies, its lease expires and another worker picks the row up.
- **Delivery guarantee:** publishing is **at-least-once**, so an event may be sent more than once.
- **Consumer:** for each message it inserts a `ProcessedMessages` row (primary key = `MessageId`) and a `StockReservation` in one transaction. A duplicate hits the primary key and is logged as `Duplicate … ignored`. Stock is reserved once. This consumer-side table is the **Inbox pattern**, a separate pattern from the outbox and a natural partner to it.

### The claim query

```sql
WITH batch AS (
    SELECT TOP (@batchSize) *
    FROM OutboxMessages WITH (UPDLOCK, READPAST, ROWLOCK)
    WHERE ProcessedAtUtc IS NULL
      AND RetryCount < @maxRetries
      AND NextAttemptAtUtc <= SYSUTCDATETIME()
      AND (LockedUntilUtc IS NULL OR LockedUntilUtc < SYSUTCDATETIME())
    ORDER BY CreatedAtUtc
)
UPDATE batch
SET LockId = @lockId,
    LockedUntilUtc = DATEADD(SECOND, @leaseSeconds, SYSUTCDATETIME())
OUTPUT inserted.*;
```

> **One clock.** All time comparisons in the claim use the database clock (`SYSUTCDATETIME()`). Both columns it compares, `NextAttemptAtUtc` and `LockedUntilUtc`, are always written by SQL Server, so clock drift between app servers and the database can't make a row eligible too early or too late. `ProcessedAtUtc` is audit-only: the claim only checks it for `NULL`, so it is written from the app clock.

### Ordering

The claim orders by `CreatedAtUtc` only. A later event for the same order can be sent while an earlier one is waiting in backoff, and parallel workers can interleave events. Service Bus sessions are not implemented. If you need per-order (per-aggregate) ordering, production code must either:

- skip rows that have an earlier unprocessed row for the same `AggregateId`, or
- version events and have consumers ignore stale ones.

This PoC does neither: it only creates one event per order.

## Project structure

```text
OutboxPatternDemo.sln
OutboxPattern.Infrastructure/      shared by Api and Worker
  Orders/                          Order, OrderCreated, CreateOrderRequest, OrderService
  Outbox/                          OutboxMessage, IOutboxStore, SqlServerOutboxStore, OutboxProcessor,
                                   BackoffCalculator, OutboxOptions, OutboxStats, OutboxMetrics
  Publishing/                      IOutboxPublisher, ServiceBusOutboxPublisher, FakeOutboxPublisher
  Persistence/                     OrderDbContext
  DependencyInjection/             AddOrdersDb, AddOutboxPublishing
  Migrations/
OutboxPattern.Api/
  Endpoints/                       POST /orders, GET /orders/{id}, GET /outbox/stats
  OutboxPattern.Api.http           sample requests
OutboxPattern.Worker/
  Services/                        OutboxWorker (BackgroundService that runs OutboxProcessor)
OutboxPattern.Consumer/
  Contracts/                       OrderCreated
  Messaging/                       InventoryMessageHandler, ServiceBusInventoryConsumer, ReplayDemo
  Persistence/                     InventoryDbContext, ProcessedMessage, StockReservation
  Migrations/
tests/
  OutboxPattern.UnitTests/         xUnit, no database: backoff, processor publish/mark/retry rules
  OutboxPattern.IntegrationTests/  xUnit, real SQL Server: transactions, claims, retries, duplicates
docker-compose.yml                 optional SQL Server 2022 container
.env.example                       placeholder settings (copy to .env)
dotnet-tools.json                  local dotnet-ef tool
global.json                        tells dotnet test to use Microsoft.Testing.Platform (xUnit v3)
```

## Prerequisites

- .NET SDK 10 (`dotnet --list-sdks` shows `10.0.x`)
- A SQL Server instance, any of:
  - **LocalDB** (Windows, installed with Visual Studio). Works out of the box with the Development settings.
  - **SQL Server Developer or Express**. Set the connection strings (see Configuration).
  - **Docker**. `docker-compose.yml` starts SQL Server 2022.
- Optional: an **Azure Service Bus** namespace for the real end-to-end flow. You need Standard or Premium tier for duplicate detection.

## Setup

### 1. Database

**LocalDB**: nothing to do.

**Docker**: copy the env file and set a strong `MSSQL_SA_PASSWORD` in `.env`:

```bash
cp .env.example .env
```

```bash
docker compose up -d
```

When `docker compose ps` shows `healthy`, point the apps at the container:

```bash
export ConnectionStrings__OrdersDb="Server=localhost,1433;Database=OutboxOrders;User Id=sa;Password=<your-password>;TrustServerCertificate=True"
```

```bash
export ConnectionStrings__InventoryDb="Server=localhost,1433;Database=OutboxInventory;User Id=sa;Password=<your-password>;TrustServerCertificate=True"
```

### 2. Create the databases

```bash
dotnet tool restore
```

```bash
dotnet ef database update --project OutboxPattern.Infrastructure --startup-project OutboxPattern.Api --context OrderDbContext
```

```bash
dotnet ef database update --project OutboxPattern.Consumer --context InventoryDbContext
```

## Run

Run each command in its own terminal.

```bash
dotnet run --project OutboxPattern.Api
```

```bash
dotnet run --project OutboxPattern.Worker
```

The API listens on `http://localhost:5233`. Use [OutboxPattern.Api/OutboxPattern.Api.http](OutboxPattern.Api/OutboxPattern.Api.http) or curl:

```bash
curl -i -X POST http://localhost:5233/orders -H "Content-Type: application/json" -d "{\"customerId\":\"customer-42\",\"total\":129.90}"
```

```bash
curl http://localhost:5233/outbox/stats
```

```bash
curl http://localhost:5233/health
```

| Endpoint | Returns |
|---|---|
| `POST /orders` | `201 Created` with the order (`400` for an empty `customerId` or a `total` of 0 or less) |
| `GET /orders/{id}` | the order, or `404` |
| `GET /outbox/stats` | `pendingCount`, `oldestPendingAgeSeconds`, `exhaustedCount` |
| `GET /health` | `Healthy` when the database is reachable |

In Development the Worker uses the **Fake publisher**: it logs each "send" instead of talking to Azure. Within about 2 seconds of creating an order you should see:

```text
Claimed 1 outbox messages with lock ...
Fake publisher sent OrderCreated with MessageId ... (send #1)
Published OrderCreated
```

`pendingCount` in `/outbox/stats` then drops back to 0.

## Try the failure scenarios

All of these use the Fake publisher and a real SQL Server. Settings can be passed as environment variables or on the command line.

**Failed publish is retried with backoff.** The first 2 publish attempts throw:

```bash
dotnet run --project OutboxPattern.Worker -- --Outbox:Fake:FailFirstNAttempts=2
```

Create an order and watch the log. You'll see `Publish failed (attempt 1 of 10); next attempt in 2.xs`, then `attempt 2 … 4.xs`, then `Published OrderCreated`. The row keeps `RetryCount = 2` and the last error in `LastError`. While it waits, `NextAttemptAtUtc` is in the future by the database clock (`SELECT NextAttemptAtUtc, SYSUTCDATETIME() FROM OutboxMessages`).

**Retries exhausted, row kept for manual re-queue.**

```bash
dotnet run --project OutboxPattern.Worker -- --Outbox:MaxRetries=2 --Outbox:BaseDelay=00:00:01 --Outbox:Fake:FailFirstNAttempts=100
```

After 2 failures the Worker logs at Error: `MaxRetries reached, message left in the outbox for manual re-queue`. The row stays with `ProcessedAtUtc = NULL`. Use the re-queue SQL below and restart the Worker without the extra arguments to publish it.

**Crash after publish → the same event is sent twice.** The publisher "sends", then fails before the row is marked processed:

```bash
dotnet run --project OutboxPattern.Worker -- --Outbox:Fake:SimulateCrashAfterPublish=true
```

The log shows `send #1`, a failure, then `send #2` and `Duplicate send: MessageId … has been sent 2 times`. Both sends carry the same `MessageId`. This is the at-least-once window the outbox cannot close, and why the consumer must handle duplicates. The fake simulates the crash by throwing from the send, so the row goes through the retry path (`RetryCount = 1`).

**Send succeeded, mark failed.** There is no command-line switch for this one; the integration test `SendSucceedsThenMarkFails_RowStaysPendingAndIsReclaimedAfterTheLeaseExpires` covers it. The Worker logs `Sent but not marked; event may be republished`. The row keeps `RetryCount = 0` and its lease, and once `LeaseSeconds` pass another claim picks it up and sends it again.

**Duplicate delivery is ignored by the consumer.** This feeds the same `OrderCreated` message twice through the real handler against SQL Server. No Azure is needed:

```bash
dotnet run --project OutboxPattern.Consumer -- --replay-demo
```

```text
Stock reserved for order ...
Duplicate ... ignored
Replay demo result: 1 stock reservation(s) for order ...
```

**Several workers at once.** Start two or more Worker processes against the same database. Each pending row is claimed and published by exactly one of them.

> The Fake publisher and `--replay-demo` show the outbox side and the consumer side separately. They **do not replace a real Service Bus run**: broker delivery, duplicate detection, redelivery and dead-lettering only happen with Azure Service Bus.

## Running the tests

`dotnet test` uses Microsoft.Testing.Platform (set in `global.json`), which xUnit v3 needs on the .NET 10 SDK.

Everything (integration tests skip if no SQL Server is available):

```bash
dotnet test
```

Unit tests only (no database):

```bash
dotnet test --project tests/OutboxPattern.UnitTests
```

Integration tests only:

```bash
dotnet test --project tests/OutboxPattern.IntegrationTests
```

All integration tests are marked `[Trait("Category","Integration")]`, so you can also filter across the solution. `--ignore-exit-code 8` stops the project that ends up with zero matching tests from failing the run:

```bash
dotnet test --filter-not-trait "Category=Integration" --ignore-exit-code 8
```

**Where the integration tests get SQL Server**, in this order:

1. `TEST_SQL_CONNECTION`: a server connection string. Each run creates two uniquely named databases (`OutboxTests_<id>_Orders`, `OutboxTests_<id>_Inventory`), applies the migrations and drops them at the end. With LocalDB:

   ```bash
   export TEST_SQL_CONNECTION="Server=(localdb)\MSSQLLocalDB;Integrated Security=True;TrustServerCertificate=True"
   ```

   PowerShell:

   ```powershell
   $env:TEST_SQL_CONNECTION = 'Server=(localdb)\MSSQLLocalDB;Integrated Security=True;TrustServerCertificate=True'
   ```

2. **Testcontainers**: if `TEST_SQL_CONNECTION` is not set and Docker is running, a `mcr.microsoft.com/mssql/server:2022-latest` container is started for the run.
3. Otherwise every integration test is **skipped** with the reason, e.g. `No SQL Server available: TEST_SQL_CONNECTION is not set and Testcontainers could not start SQL Server (DockerUnavailableException: …)`.

The tests never use SQLite or the EF InMemory provider: transactions, row locks and unique keys need real SQL Server.

| Unit tests (`tests/OutboxPattern.UnitTests`) |
|---|
| Backoff doubles per retry with up to 20% jitter, and is capped at `MaxDelay` |
| A successful send is marked processed with the claim's lock |
| A failed send schedules a retry and is not marked |
| A mark failure after a successful send does not schedule a retry or increase `RetryCount` |
| The mark uses `CancellationToken.None` even when the loop is cancelled right after the send |

| Integration tests (`tests/OutboxPattern.IntegrationTests`) |
|---|
| `CreateOrderAsync` commits the order and the outbox row together; a forced failure before commit leaves neither |
| A new outbox row gets `NextAttemptAtUtc` from SQL Server |
| A failed publish sets `RetryCount = 1`, keeps `ProcessedAtUtc` null and puts `NextAttemptAtUtc` in the future by the database clock |
| A crash after publish sends the same `MessageId` twice |
| After `MaxRetries` the row stays, is not claimed again and counts as exhausted |
| Two claimers on separate connections, started together, never get the same row |
| Send succeeds then mark fails: not processed, `RetryCount` unchanged, reclaimed after the lease expires |
| The same message handled twice (and the `--replay-demo` path) creates exactly one `StockReservation` |

## Real Azure Service Bus

1. Create a queue named `orders` (or set `Outbox:QueueName` / `Inventory:QueueName`). **Duplicate detection needs the Standard or Premium tier and can only be enabled when the queue is created.** It drops repeated `MessageId`s within its window (10 minutes by default). It reduces duplicates but does not replace the consumer's inbox.
2. Store the connection string as a secret, never in appsettings:

```bash
dotnet user-secrets set "ConnectionStrings:ServiceBus" "<connection-string>" --project OutboxPattern.Worker
```

```bash
dotnet user-secrets set "ConnectionStrings:ServiceBus" "<connection-string>" --project OutboxPattern.Consumer
```

3. Run the Worker with the real publisher, and the Consumer without `--replay-demo`:

```bash
dotnet run --project OutboxPattern.Worker -- --Outbox:Publisher=AzureServiceBus
```

```bash
dotnet run --project OutboxPattern.Consumer
```

Create an order. The Worker logs `Published OrderCreated`, then the Consumer logs `Stock reserved for order …`. The consumer completes a message only after its transaction has committed or the duplicate was detected.

## Configuration

| Key | Default | Notes |
|---|---|---|
| `ConnectionStrings:OrdersDb` | LocalDB (Development) | Api and Worker |
| `ConnectionStrings:InventoryDb` | LocalDB (Development) | Consumer |
| `ConnectionStrings:ServiceBus` | — | user-secrets or env var only |
| `Outbox:Publisher` | `Fake` in Development, `AzureServiceBus` otherwise | `Fake` or `AzureServiceBus` |
| `Outbox:QueueName` / `Inventory:QueueName` | `orders` | |
| `Outbox:BatchSize` | `50` | rows claimed per poll |
| `Outbox:MaxRetries` | `10` | |
| `Outbox:LeaseSeconds` | `30` | keep it longer than a batch takes to publish |
| `Outbox:PollingInterval` | `00:00:02` | |
| `Outbox:BaseDelay` / `Outbox:MaxDelay` | `00:00:02` / `00:05:00` | retry backoff |
| `Outbox:Fake:FailFirstNAttempts` | `0` | Fake only |
| `Outbox:Fake:SimulateCrashAfterPublish` | `false` | Fake only |

Any key can be set as an environment variable with `__` instead of `:` (for example `Outbox__BatchSize=20`).

## Observability

- Worker and Consumer write single-line JSON logs with `OutboxMessageId`, `OrderId` and `MessageId` as fields. Payloads and secrets are never logged.
- `GET /outbox/stats`: `oldestPendingAgeSeconds` is the best "publisher is stuck" signal; `exhaustedCount` counts rows that hit `MaxRetries`.
- The Worker emits the counters `outbox.messages.published`, `outbox.messages.failed` and `outbox.messages.exhausted` on the meter `OutboxPattern.Outbox`. To watch them:

```bash
dotnet-counters monitor -n OutboxPattern.Worker --counters OutboxPattern.Outbox
```

## Inspect the data

```sql
-- OutboxOrders
SELECT Id, CustomerId, Total, CreatedAtUtc FROM Orders ORDER BY CreatedAtUtc DESC;

SELECT Id, EventType, AggregateId, CreatedAtUtc, ProcessedAtUtc, RetryCount,
       NextAttemptAtUtc, LastError, LockId, LockedUntilUtc
FROM OutboxMessages ORDER BY CreatedAtUtc DESC;

-- stuck events
SELECT Id, EventType, AggregateId, RetryCount, LastError
FROM OutboxMessages
WHERE ProcessedAtUtc IS NULL AND RetryCount >= 10;

-- re-queue one after fixing the cause
UPDATE OutboxMessages SET RetryCount = 0, NextAttemptAtUtc = SYSUTCDATETIME() WHERE Id = @id;

-- OutboxInventory
SELECT MessageId, ProcessedAtUtc FROM ProcessedMessages ORDER BY ProcessedAtUtc DESC;
SELECT OrderId, COUNT(*) AS Reservations FROM StockReservations GROUP BY OrderId;
```

With LocalDB and `sqlcmd` (note the `-I`):

```bash
sqlcmd -S "(localdb)\MSSQLLocalDB" -d OutboxOrders -I -W -Q "SELECT Id, RetryCount, ProcessedAtUtc, LastError FROM OutboxMessages"
```

## Troubleshooting

- **`ConnectionStrings:OrdersDb is not configured`**: the app is not running in Development. Use `dotnet run` (which applies the launch profile) or set the connection string as an environment variable.
- **`Outbox:Publisher must be 'Fake' or 'AzureServiceBus'`**: set `Outbox__Publisher`.
- **`ConnectionStrings:ServiceBus is not configured`**: you selected real mode without a connection string. Set it with user-secrets, or use the Fake publisher / `--replay-demo`.
- **`Msg 1934 … QUOTED_IDENTIFIER`** in `sqlcmd`: add `-I`. The filtered index on `OutboxMessages` requires `QUOTED_IDENTIFIER ON`.
- **LocalDB not found**: `sqllocaldb info` should list `MSSQLLocalDB`. Start it with `sqllocaldb start MSSQLLocalDB`.
- **`Cannot create file '…\OutboxOrders.mdf' because it already exists`** (or `Cannot open database "OutboxOrders"`): the database was removed from LocalDB but its files were left in your user profile folder. Either re-attach them to keep the data:

  ```powershell
  sqlcmd -S "(localdb)\MSSQLLocalDB" -Q "CREATE DATABASE OutboxOrders ON (FILENAME = '$env:USERPROFILE\OutboxOrders.mdf'), (FILENAME = '$env:USERPROFILE\OutboxOrders_log.ldf') FOR ATTACH"
  ```

  or delete `OutboxOrders.mdf` and `OutboxOrders_log.ldf` from `%USERPROFILE%` and run `dotnet ef database update` again for an empty database. The same applies to `OutboxInventory`.
- **Docker container unhealthy**: the SA password must meet SQL Server complexity rules. Check `docker compose logs sqlserver`.
- **Rows stay pending**: check `/outbox/stats`, the Worker log for `Publish failed`, and `LastError` on the row. Rows with `RetryCount >= MaxRetries` need the re-queue SQL above.
- **`Lease lost; event may be published again`**: publishing took longer than `LeaseSeconds`. Increase `LeaseSeconds` or lower `BatchSize`.
- **`dotnet ef` not found**: run `dotnet tool restore`.
