> Historical draft preserved on 2026-10-29. Superseded by the [consolidated public API proposal](../public-api-proposal.md). The alternatives below are not the current contract.

# Spindle public API proposal

Status: draft for discussion. The user has selected **attributes first** for getting-started examples, with defaults beside each job class. Other choices below are recommendations, not approved contracts. Code is illustrative proposed API, not an implemented package.

Reconciliation notice: this draft was written before reading the full earlier session. Its mandatory handler-context argument, marker-interface removal, dispatch naming, lifecycle simplification, and retry accounting are **unapproved alternatives**, not replacements for the earlier design. See [planning continuity](../planning-continuity.md). The working handler baseline remains `HandleAsync(job, CancellationToken)` with optional scoped `IJobContext` injection. Further work should proceed on internal engineering rather than another public API planning cycle.

## 1. A job is data; a handler performs the work

```csharp
[Job("email.welcome", Version = 1)]
[Queue("emails")]
[Priority("medium")]
[Retry(MaxAttempts = 5)]
public sealed record SendWelcomeEmail(Guid UserId);

public sealed class SendWelcomeEmailHandler(IEmailSender emails)
    : IJobHandler<SendWelcomeEmail>
{
    public async Task HandleAsync(
        SendWelcomeEmail job,
        JobContext context,
        CancellationToken cancellationToken)
    {
        await emails.SendWelcomeAsync(job.UserId, cancellationToken);
    }
}
```

The attributes belong to the payload, so a producer can obtain defaults without referencing the handler assembly. Payloads are ordinary classes or records; a marker interface or base class is not required. Attributes are declarative metadata and must not resolve services or perform I/O.

The stable name `email.welcome` and payload version are persisted. CLR namespaces and assembly-qualified names are not the durable identity. A class rename leaves the persisted name unchanged. Registration rejects duplicate name/version mappings and ambiguous handlers.

One handler handles each job type/version on a worker. Multiple handlers for a single job would introduce fan-out semantics outside this contract.

Proposed handler contract:

```csharp
public interface IJobHandler<in TJob> where TJob : class
{
    Task HandleAsync(
        TJob job,
        JobContext context,
        CancellationToken cancellationToken);
}
```

`JobContext` provides read-only job ID, attempt ID, queue, priority, retry generation, and one-based attempt number. Durable acknowledgement and lease operations are internal. The cancellation token communicates execution cancellation, independently of the token used by the producer to enqueue.

## 2. Embedded hosting stays small

```csharp
builder.Services.AddSpindle(spindle =>
{
    spindle.UsePostgres(connectionString, postgres =>
        postgres.Schema = "spindle");

    spindle.AddJobsFromAssemblyContaining<SendWelcomeEmail>();
    spindle.AddHandlersFromAssemblyContaining<SendWelcomeEmailHandler>();

    spindle.AddWorker(worker =>
    {
        worker.Queues("emails");
        worker.MaxConcurrency = 8;
    });
});
```

Discovery scans explicitly selected assemblies at startup. It does not scan every assembly loaded in the process. Job discovery reads attributed payload contracts; handler discovery registers matching implementations. A producer requires contracts but does not require handlers.

Explicit registration is also supported:

```csharp
spindle.AddJob<SendWelcomeEmail>(); // reads the payload attributes
spindle.AddHandler<SendWelcomeEmail, SendWelcomeEmailHandler>();
```

For applications that cannot or do not want to annotate payloads:

```csharp
spindle.AddJob<SendWelcomeEmail>("email.welcome", version: 1)
    .ToQueue("emails")
    .WithPriority("medium")
    .WithMaxAttempts(5);
```

The explicit name/version must agree with any identity attribute; conflicting identities are errors, not overrides. Repeated identical registration is harmless; conflicting handlers or contracts fail startup.

`AddWorker` enables the hosted execution engine. Without it, the host is a producer. Multiple hosts use the same store and application namespace to coordinate work. No worker-mode field is stored on a job. Scheduler and management roles remain separately enabled.

| Deployment | Configuration |
|---|---|
| One application with embedded workers | Contracts, handlers, storage, worker in the application |
| Application plus external worker | Application registers contracts/storage; worker registers contracts/handlers/storage/worker |
| Multiple embedded replicas | Each replica uses the embedded configuration and shared storage |
| Applications plus multiple external workers | Producer configuration in applications; worker configuration in each worker replica |
| Hybrid | Enable worker registration in selected application replicas and external workers |

`MaxConcurrency = 8` is local to that worker pool; it does not mean eight across the cluster. Cluster-wide limits belong to shared queue/job/resource policy. Their configuration protocol must handle replicas disagreeing about settings rather than silently choosing the last writer.

## 3. Ordinary dispatch returns after persistence

```csharp
public interface IJobClient
{
    Task<EnqueueResult> EnqueueAsync<TJob>(
        TJob job,
        EnqueueOptions? options = null,
        CancellationToken cancellationToken = default)
        where TJob : class;
}

public readonly record struct JobId(Guid Value);

public sealed record EnqueueResult(JobId JobId, bool WasDuplicate);

public sealed record EnqueueOptions
{
    public string? Queue { get; init; }
    public string? Priority { get; init; }
    public DateTimeOffset? NotBefore { get; init; }
}
```

Basic dispatch:

```csharp
var result = await jobs.EnqueueAsync(
    new SendWelcomeEmail(user.Id),
    cancellationToken: cancellationToken);
```

Delayed dispatch and per-call routing:

```csharp
var result = await jobs.EnqueueAsync(
    new SendWelcomeEmail(user.Id),
    new EnqueueOptions
    {
        Priority = "high",
        NotBefore = DateTimeOffset.UtcNow.AddMinutes(10)
    },
    cancellationToken);
```

`NotBefore` is an eligibility time, not a promise of an exact start time. A past value means eligible immediately. The initial priority vocabulary is `low`, `medium`, and `high`; extending it remains an open decision. Queue and priority spelling is validated rather than silently corrected.

A successful ordinary enqueue means the storage driver confirmed its transaction/atomic persistence operation, subject to that driver's documented durability configuration. It does not mean the job ran. A failed or cancelled enqueue can have an ambiguous outcome if persistence succeeded but the response was lost. Cancelling enqueue does not cancel an existing job.

`WasDuplicate` supports the agreed unique-dispatch feature and identifies an existing logical job when that policy applies. Both active-job uniqueness and fixed-window idempotency are now confirmed scope. The [uniqueness and execution proposal](../uniqueness-and-execution.md) records recommended key scope, mismatch, retention, and manual-retry rules. Syntax is not frozen. Deduplication cannot depend solely on retained dashboard history.

## 4. Transactional dispatch is explicit and provider-specific

In `Spindle.Postgres`, propose a separate staging contract:

```csharp
public interface IPostgresJobClient
{
    Task<StagedJob> StageAsync<TJob>(
        TJob job,
        NpgsqlTransaction transaction,
        EnqueueOptions? options = null,
        CancellationToken cancellationToken = default)
        where TJob : class;
}

public sealed record StagedJob(JobId JobId, bool WasDuplicate);
```

Illustrative application flow:

```csharp
await using var connection = await dataSource.OpenConnectionAsync(ct);
await using var transaction = await connection.BeginTransactionAsync(ct);

await users.InsertAsync(connection, transaction, user, ct);

var staged = await postgresJobs.StageAsync(
    new SendWelcomeEmail(user.Id),
    transaction,
    cancellationToken: ct);

await transaction.CommitAsync(ct);
```

`StageAsync` executes the insert using the supplied transaction. Its successful return means accepted within that transaction, not committed. The caller owns commit, rollback, and disposal. Rollback removes the insertion; workers cannot reserve an uncommitted job. The returned ID may never become an executable job if the caller rolls back.

The transaction's connection is authoritative. The driver validates supported transaction state and uses the configured schema/application namespace on that connection. The caller must target the same Spindle store the workers use; there is no fallback to a separately opened connection. We must decide how configured-store compatibility is validated without brittle connection-string comparisons.

Wake-up hints must respect commit visibility. Polling remains sufficient if notification is unavailable or lost. Core contracts do not depend on EF Core or pretend that every storage driver supports an application database transaction.

An optional EF integration can adapt an application's actual transaction. Cross-store delivery needs an outbox with a separate contract; it cannot be simulated by staging against the queue's own connection.

## 5. Defaults, overrides, and stored policy

| Concern | Proposed rule |
|---|---|
| Default values | Library defaults, then payload attributes, then fluent job configuration |
| Per-dispatch overrides | Queue, priority, and eligibility time in this first proposal |
| Stable identity | Explicit name/version; conflicts are rejected |
| Retry budget | `MaxAttempts` includes the first attempt; five means at most five attempts in one generation |
| Default retry budget | Proposed default of five total attempts; backoff/jitter policy still needs its own specification |
| Default route/priority | Queue `default`, priority `medium` |
| Execution policy | Persist declarative queue/priority/retry-budget settings with the job |
| Capacity and pause | Shared live controls evaluated when reserving; changes affect existing pending jobs |
| Handler/middleware implementation | Resolved from the deployed worker; code is not serialized into job records |

Example fluent override while retaining attributes as the introductory style:

```csharp
spindle.ConfigureJob<SendWelcomeEmail>()
    .ToQueue("customer-emails")
    .WithMaxAttempts(3);
```

Persisting policy avoids silently changing the retry budget of queued jobs during deployment. This does not freeze handler logic or exception-classification code. Named policies need version/compatibility rules before they can be persisted by reference. A global safety ceiling may restrict a job even when its stored settings permit more.

## 6. Middleware surrounds an attempt

```csharp
public delegate Task JobExecutionDelegate(CancellationToken cancellationToken);

public interface IJobMiddleware
{
    Task InvokeAsync(
        JobContext context,
        JobExecutionDelegate next,
        CancellationToken cancellationToken);
}
```

Proposed registration:

```csharp
spindle.UseMiddleware<LoggingMiddleware>();
spindle.ConfigureJob<SendWelcomeEmail>()
    .UseMiddleware<TenantContextMiddleware>();
```

Global middleware runs outermost in registration order, followed by job middleware and then the handler. The engine creates one DI scope per attempt, shared by its middleware and handler. It retains that scope until user code actually exits, including a handler that ignores cancellation.

The continuation is single-use. Ordinary successful completion requires invoking and awaiting it exactly once. Silent short-circuiting must not acknowledge unexecuted work: deliberately skipping, deferring, or permanently rejecting a job needs a named outcome whose durable semantics are still to be specified. Middleware cannot acknowledge jobs, renew leases, or release durable permits. The engine owns those transitions.

The first interface supplies execution metadata, not a mutable payload. Whether typed middleware requires a separate generic contract remains open.

## 7. Execution rules to settle alongside the API

These are proposed policies, not additional product promises:

1. Reserve only after local capacity and distributed admission are available. Contention consumes no attempt.
2. Commit the reservation, attempt record, and permits atomically before calling user code. An attempt consumed by a crash between reservation and invocation counts toward the budget; otherwise repeated crashes could create unbounded retries. Dashboard outcomes must distinguish abandoned attempts from handler failures.
3. Returning normally proposes success. An exception proposes retry or permanent failure. The store accepts any outcome only from the current, unexpired owner.
4. Explicit cancellation of pending work is terminal immediately. For running work, persist a request and signal the token. A normal handler return can still finalize success; cancellation is not proof of undoing side effects. Final cancellation-versus-completion race rules remain to be formalized.
5. A timeout requests cancellation. If the handler ignores it, keep its local slot occupied and do not start a timeout retry while the same ownership remains valid. The eventual timeout outcome is a failed attempt even if the handler returns late. Lease-loss takeover can still overlap an uncooperative old handler.
6. Shutdown stops reservation first, allows a drain period, then requests cancellation and gives bookkeeping a bounded cleanup period. A handler that exits due to shutdown becomes eligible for recovery/retry, subject to budget. Never dispose its DI scope or claim it stopped merely because the host's waiting deadline expired.
7. Manual retry initially applies to failed or cancelled jobs, keeps the job ID, starts a new generation, and preserves history. Do not reset a currently valid running attempt. A succeeded job can be dispatched again as a new job. Deduplication-key conflicts and proof-of-stop limitations need explicit handling.

## 8. Review decisions and remaining work

Confirmed preferences: attributes lead documentation with fluent overrides; both active-job uniqueness and fixed-window dispatch idempotency are supported.

Recommended choices in this draft: plain payloads, typed handlers with explicit context, selected-assembly discovery, producer/worker separation, provider-specific transactional staging, stored declarative retry policy, local capacity-aware admission, and single-use middleware continuations.

Still to design before these APIs are frozen:

- Unique-dispatch policy, retention, mismatched payload handling, and ambiguous enqueue retries.
- Payload serialization, schema evolution, unsupported versions, and worker capability-aware reservation.
- Retry classification, backoff, cancellation/timeout precedence, and permanent-rejection/defer middleware outcomes.
- The control API for cancellation, manual retry, queue controls, and dashboard permissions.
- Distributed concurrency policy ownership, keyed fencing, and global limit changes.
- PostgreSQL schema/queries, transaction compatibility checks, and driver contracts.
- Scheduler, autoscaling, observability, and the dashboard integration contracts.

This document advances the API discussion; it does not approve implementation or remove any agreed v1 capability.
