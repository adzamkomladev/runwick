# Runwick

Runwick is a background-job library for .NET 10. Jobs are stored in PostgreSQL, SQLite or Redis and run by
workers inside your application or in separate processes. It includes schedules, retries, rate limits,
uniqueness, a transactional outbox, a management API, a web dashboard and a command line.

```csharp
[JobName("email.welcome")]
public sealed record SendWelcomeEmail(Guid UserId) : IQueueJob;

builder.Services.AddRunwick(runwick =>
{
    runwick.Application("my-app")
        .UsePostgres(connectionString)              // or UseSqlite(...) or UseRedis(...)
        .AddJobsFromAssemblyContaining<SendWelcomeEmail>();
    runwick.AddHandler<SendWelcomeEmail, SendWelcomeEmailHandler>();
    runwick.AddWorker(w => w.Pool("main", p => p.Queues("default").FixedConcurrency(4)));
});

await dispatcher.DispatchAsync(new SendWelcomeEmail(userId));
```

- Delivery is at-least-once. Make handlers safe to run twice, or use idempotency keys.
- Cancellation is cooperative: a running handler is asked to stop through its cancellation token.
- Exactly-once effects exist only for work done in the same database as the job (transactional completion).

Documentation: https://github.com/adzamkomladev/runwick (start with `docs/guide/getting-started.md`). Licence: MIT.
