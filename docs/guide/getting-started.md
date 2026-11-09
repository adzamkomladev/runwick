# Getting started

## What you install

| You want | Install |
|---|---|
| Job classes only (shared with a producer that has no database code) | `Runwick.Contracts` |
| Run jobs | `Runwick` plus one store: `Runwick.Postgres`, `Runwick.Sqlite` or `Runwick.Redis` |
| Test your own code | `Runwick.Testing` (simulated; not a real store) |
| Web API and dashboard | `Runwick.AspNetCore`, `Runwick.Dashboard` |
| Metrics and traces | `Runwick.OpenTelemetry` |
| Transactional outbox | `Runwick.Outbox` plus `Runwick.Outbox.Postgres` or `Runwick.Outbox.Sqlite` |
| Command line | `dotnet tool install Runwick.Cli` (command `runwick`) |

All packages target `net10.0` only. You never need Node or npm; the dashboard's web files are inside the package.

## A job and its handler

```csharp
[JobName("email.welcome")]   // required: the stored name; CLR type names are never used
public sealed record SendWelcomeEmail(Guid UserId) : IQueueJob;

public sealed class SendWelcomeEmailHandler : IJobHandler<SendWelcomeEmail>
{
    public Task HandleAsync(SendWelcomeEmail job, CancellationToken cancellationToken)
    {
        // Do the work. Stop promptly when cancellationToken is cancelled. This code may run more than once.
        return Task.CompletedTask;
    }
}
```

## Wire it up

The same registration works for every store. Only the `Use...` line changes.

```csharp
builder.Services.AddRunwick(runwick =>
{
    runwick.Application("my-app")
        .UsePostgres(connectionString)          // or UseSqlite(...) or UseRedis(...), see below
        .AddJobsFromAssemblyContaining<SendWelcomeEmail>();
    runwick.AddHandler<SendWelcomeEmail, SendWelcomeEmailHandler>();
    runwick.AddWorker(w => w.Pool("main", p => p.Queues("default").FixedConcurrency(4)));   // omit in a producer-only app
});
```

Dispatch from anywhere in your app:

```csharp
await dispatcher.DispatchAsync(new SendWelcomeEmail(userId));   // IJobDispatcher from DI
```

`DispatchAsync` returns only after the store has confirmed the write. If you get an "outcome unknown" error, the write may or may not have happened; see [delivery semantics](delivery-semantics.md).

## PostgreSQL (reference store)

- PostgreSQL 16, 17 or 18, connected directly to the primary (no read replicas, no proxy profile certified).
- Create the schema once, with a role that may create schemas: `await RunwickPostgresMigrations.MigrateAsync(connectionString);` (namespace `Runwick.Postgres.Migrations`) or `runwick migrate --provider postgres --connection-env MY_CONN`. The running application can then use a role with data access only.
- `UsePostgres("Host=...;Database=...;...")`.
- Producers and workers can run on many hosts.

## Schedules (one-time setup)

Registering a schedule is refused (`scheduler_not_active`) until the application has raised the `scheduler` capability. Start the application once so it registers, then run `runwick store activate-capability --name scheduler --application YOUR_APP --provider postgres --connection-env MY_CONN` (`--provider sqlite` for SQLite, `--provider redis` for Redis), then restart. This is done once per application and cannot be undone. Rate limits need the same step with `--name rate_admission`. See [spec 23 §3.6](../spec/23-upgrades.md).

## SQLite (one machine)

- One database file on a local disk. Every process that uses it must be on the same machine. A file on a network share, or an in-memory database, is refused at startup.
- `await RunwickSqliteMigrations.MigrateAsync("Data Source=/var/lib/my-app/runwick.db");` then `UseSqlite("Data Source=/var/lib/my-app/runwick.db")`.
- Writes are durable (WAL with `synchronous=FULL`); a weaker setting is refused.
- Workers find new jobs by polling; there is no cross-process wake-up.

## Redis (one primary)

- Redis 8.2 or newer, one standalone primary started with `appendonly yes`, `appendfsync always`, `no-appendfsync-on-rewrite no`, `maxmemory-policy noeviction` and `aof-load-truncated no`. Runwick checks these at startup and while it runs, and refuses to run on anything else (replica, Cluster, Sentinel).
- `await RunwickRedisMigrations.MigrateAsync("host:6379");` then `UseRedis("host:6379")`.
- Workers and producers can run on many hosts. There is no way to commit a job together with a change in your business database; use the [outbox](delivery-semantics.md#the-outbox) instead.

## Check it works

The `samples/Runwick.Samples.Host` program starts a worker, dispatches jobs and prints how many succeeded: `dotnet run --project samples/Runwick.Samples.Host -- --conn "<connection string>" --migrate --dispatch 10`.
