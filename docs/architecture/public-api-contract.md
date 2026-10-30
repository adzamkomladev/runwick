# Consolidated public API and configuration contract

Status: reconciled engineering proposal, 2026-10-29. Illustrative C# describes a proposed API, not a published or compilable package. It consolidates the original baseline with the storage/worker/scheduler designs; it does not approve runtime implementation. The [initial API draft](../history/public-api-initial-draft.md) is retained only as history.

## 1. What stays and what is reconciled

| Area | Current working contract |
|---|---|
| Job contract | Class/record implements IQueueJob |
| Handler | IJobHandler<TJob>.HandleAsync(job, CancellationToken) |
| Execution context | Optional scoped IJobContext injection; not a required handler argument |
| Dispatch | IJobDispatcher.DispatchAsync |
| Introductory configuration | Attributes beside the job; fluent-only contracts and fluent overrides also supported |
| Job identity | Explicit stable name from attribute or fluent registration; current payload version plus upgrade paths |
| Handler registry | One handler per stable name per worker; versions select decoding/upgrading |
| Execution accounting | MaxExecutions counts committed Begin authorizations; separate pre-start abandonment guard |
| State | pending, reserved, running, retrying, succeeded, failed, cancelled |
| Deployment | Producer, worker, scheduler and management are separately enabled roles |

The first four names/signatures preserve the original session. Requiring an explicit durable name, exact attribute spelling, result types, configuration precedence, and public recovery handles below are proposals for review. They are not retroactively described as user-approved decisions.

## 2. Attribute-first job and handler

```csharp
public interface IQueueJob { }

public interface IJobHandler<in TJob> where TJob : class, IQueueJob
{
    Task HandleAsync(TJob job, CancellationToken cancellationToken);
}

[JobName("email.welcome")]
[PayloadVersion(2)]
[Queue("emails")]
[Priority("medium")]
[MaxExecutions(3)]
public sealed record SendWelcomeEmail(Guid UserId, string Locale) : IQueueJob;

public sealed class SendWelcomeEmailHandler(
    IEmailSender emails,
    IJobContext context) : IJobHandler<SendWelcomeEmail>
{
    public Task HandleAsync(SendWelcomeEmail job, CancellationToken ct) =>
        emails.SendWelcomeAsync(job.UserId, job.Locale, ct);
}
```

The example explicitly sets the selected baseline default of three executions. Omit IJobContext from the constructor when unused. Payloads are data; attributes cannot resolve services or execute I/O. The producer needs the contract assembly, not the handler assembly.

Stable names are case-sensitive bounded strings. Proposed rule: every durable contract has an explicit name, either `[JobName]` or `.Named(...)`; do not require the attribute itself. Reject a missing name at configuration validation instead of silently persisting a CLR namespace. Payload version defaults to 1 for a newly registered contract; changes to serialized meaning require an explicit new version.

One resolved identity cannot point to two current payload contracts/handlers in the same worker registry. A producer may register a contract without a handler. Discovery scans only explicitly selected assemblies; no automatic process-wide assembly scan. Discovery is deterministic and does not resolve handlers during registration.

## 3. Registration, roles, and driver profiles

```csharp
builder.Services.AddRunwick(runwick =>
{
    runwick.Application("customer-service");
    runwick.UsePostgres(connectionString, pg => pg.Schema = "runwick");

    runwick.AddJobsFromAssemblyContaining<SendWelcomeEmail>();
    runwick.AddHandlersFromAssemblyContaining<SendWelcomeEmailHandler>();

    runwick.AddWorker(worker =>
    {
        worker.Pool("email-workers", pool =>
        {
            pool.Queues("emails");
            pool.FixedConcurrency(8);
        });
    });
});
```

This is the concise hosting shape; [timing defaults](execution-defaults-and-timing.md) are selected in the final baseline, and compilation/runtime tests are still required. Validation builds a complete effective policy; runtime code never receives an unspecified required budget.

Producer-only hosts omit handler/worker registration. AddScheduler enables occurrence materialization independently; it does not imply workers. Management and dashboard registrations are separate. Multiple embedded/external worker hosts use the same application/store identity and contracts; no deployment-mode flag is stored on jobs.

Fluent-only metadata retains the same marker and handler contracts:

```csharp
runwick.AddJob<SendWelcomeEmail>()
    .Named("email.welcome")
    .AtVersion(2)
    .ToQueue("emails")
    .WithPriority("medium")
    .WithMaxExecutions(3);

runwick.AddHandler<SendWelcomeEmail, SendWelcomeEmailHandler>();
```

The attribute and fluent examples are alternatives or matching declarations, not permission to register conflicting identities. Explicit name/version declarations must agree. Repeating identical registrations is idempotent; conflicting same-layer assignments fail validation rather than depending on container registration order.

UseSqlite and UseRedis configure their own provider packages and an explicit acknowledgement profile. All drivers expose validated topology, supported operation versions, and effective persistence settings through diagnostics. A requested unsupported guarantee fails startup; no per-dispatch option downgrades persistence or turns a distributed ceiling into a local gate. Redis's production profile remains subject to the [driver feasibility gates](driver-capabilities.md).

## 4. Configuration resolution and lifetime

For overridable scalar job settings, resolve **library defaults → application job defaults → job attributes → fluent job configuration → permitted dispatch override**. Preserve a diagnostic source map for each effective value. An unset value inherits; null is not overloaded to mean both inherit and disable. Explicit disabling uses a named operation and is rejected where a mandatory guard is required.

| Concern | Override behavior | Durable/live behavior |
|---|---|---|
| Stable name and payload version | Declarations must agree; no dispatch override | Persisted identity; versions drive decoding |
| Queue and priority | All policy layers; dispatch may choose a registered route/lane | Snapshotted for the generation; no live queue move |
| Retry budget/backoff/timeout | Defaults, attributes, fluent; no dispatch override in initial API | Effective declarative policy snapshotted at dispatch |
| Pre-start abandonment guard | Application defaults and fluent policy | Snapshotted separately from MaxExecutions |
| NotBefore | Dispatch/schedule value | Stored eligibility; not an exact-start promise |
| Active/window uniqueness | Registered policy/key selector or explicit dispatch keys | Keys and request intent participate in atomic dispatch |
| Resource/type/queue membership | Registered declarative policies and pure key selection | Resolved scope identities snapshotted; no producer bypass |
| Shared ceilings, rates, pause | Seeded code configuration plus revisioned management controls | Live authority in store; replicas do not overwrite it on startup |
| Local fixed/automatic pool target | Worker configuration plus revisioned operational override | Local process control; not a shared ceiling |
| Handler and middleware implementation | Current validated worker registry | Deployed code; never serialized delegates |
| Schedule definition | Seed-if-absent, explicit revisioned edit | Immutable definition versions; controls survive deployments |

The [identity/configuration decision packet](identity-and-serialization.md) recommends `low`, `medium`, `high` for v1, retaining the current PostgreSQL CHECK. Arbitrary strings are not silently registered. Custom labels require an explicit extension/review before being advertised; the string-valued public contract remains unchanged.

An explicit priority override may raise or lower the job's declared priority within the configured lanes. A route override must target a registered queue and still acquire that destination queue's required scopes; job/resource limits remain attached. Operators may constrain allowed routes/overrides in application policy. A producer cannot alter shared capacities, lease duration, execution budget, or a resource fence through DispatchOptions.

For collection settings define composition explicitly: global middleware precedes per-job middleware; named attribute middleware is ordered by explicit Order then ordinal alias for ties; fluent per-job middleware appends in registration order. Reject duplicate aliases/types in an effective chain in the initial API. Identity conflicts, invalid budgets, and ambiguous same-layer scalar assignments are errors, not “last writer wins.”

## 5. Dispatch and durable acknowledgement

```csharp
public readonly record struct JobId(Guid Value);
public readonly record struct DispatchOperationId(Guid Value);
public enum DispatchDisposition { Created, Reused }
public enum DuplicateMatch { None, Active, Window, ActiveAndWindow }

public sealed record DispatchReceipt(
    JobId JobId,
    DispatchOperationId OperationId,
    DispatchDisposition Disposition,
    DuplicateMatch DuplicateMatch,
    bool PolicyDrift,                       // R3: reused under a window key whose stored execution policy differs
    string ConfirmedAcknowledgementProfile);

public sealed record DispatchOptions
{
    public string? Queue { get; init; }
    public string? Priority { get; init; }
    public DateTimeOffset? NotBefore { get; init; }
    public DispatchUniqueness? Uniqueness { get; init; }
}

public sealed record DispatchUniqueness(
    string? ActiveKey,
    string? WindowKey,
    TimeSpan? Window);

public interface IJobDispatcher
{
    Task<DispatchReceipt> DispatchAsync<TJob>(
        TJob job,
        DispatchOptions? options = null,
        CancellationToken cancellationToken = default)
        where TJob : class, IQueueJob;
}
```

Required business metadata is a bounded extension to the options/envelope, specified with the fingerprint rules below; raw ownership tokens and provider connections are never dispatch options.

```csharp
var receipt = await dispatcher.DispatchAsync(
    new SendWelcomeEmail(user.Id, "en"),
    new DispatchOptions
    {
        Priority = "high",
        Uniqueness = new DispatchUniqueness(
            ActiveKey: $"user:{user.Id}",
            WindowKey: requestId,
            Window: TimeSpan.FromHours(24))
    },
    cancellationToken);
```

These 24 hours are caller policy, not a library default. A window key requires a positive window; a window without a key is invalid. Registration may derive keys through a deterministic selector on the payload. Dispatch may supply keys where policy permits but cannot disable a required uniqueness dimension. If explicit and derived keys both exist they must agree; configuring an optional dimension is not a silent override of an enforced policy.

Created means a new logical job was committed/published and the configured acknowledgement profile was confirmed. Reused means the applicable active/window policy resolved an existing logical ID; payload is not replaced and the handler was not newly invoked. A reused job may already be terminal or have expired detail history. Receipt identity is not a current job-status snapshot.

NotBefore is an eligibility bound. Past instants are eligible immediately; omitted NotBefore means immediate intent rather than storing “now” in the request fingerprint. Cancellation of dispatch stops waiting/attempting that request; it is not a durable cancellation of an accepted job.

### Batch dispatch (added 2026-10-30)

```csharp
public sealed record BatchDispatchOptions
{
    public bool ContinueOnConflict { get; init; } = false;   // see below
}

public sealed record BatchItemResult(
    int Ordinal,
    DispatchReceipt? Receipt,            // present when Created or Reused
    BatchItemFailure? Failure);          // present only under ContinueOnConflict

public sealed record BatchDispatchReceipt(
    DispatchOperationId OperationId,
    IReadOnlyList<BatchItemResult> Items,
    string ConfirmedAcknowledgementProfile);

public interface IJobDispatcher
{
    // existing single-job DispatchAsync, plus:
    Task<BatchDispatchReceipt> DispatchManyAsync(
        IReadOnlyList<BatchDispatchItem> items,      // (IQueueJob job, DispatchOptions? options)
        BatchDispatchOptions? options = null,
        CancellationToken cancellationToken = default);
}
```

Semantics:

- **One command, one transaction, one receipt.** The whole batch is admitted atomically: either every item is durably created/reused or none is. The receipt lists one result per item by ordinal (same shape as bulk management commands: `(command_id, item_ordinal)`), so a lost reply resolves the whole batch through the existing receipt protocol.
- **Validation is all-or-nothing before any write.** An invalid item (identity, size, bad options) rejects the batch as *known not applied* with per-item diagnostics.
- **Reused is success.** Items whose active/window key resolves to an existing job are returned as `Reused` with `PolicyDrift` where applicable; they do not fail the batch.
- **Hard uniqueness conflicts** (window fingerprint mismatch, active/window composition conflict) fail the entire batch by default. With `ContinueOnConflict = true` the conflicting items are reported as per-item failures, nothing is created for them, and the remaining items still commit atomically together.
- **Lock order:** all dispatch keys across the batch are sorted and acquired once, then new jobs are created, then scopes are provisioned family by family — exactly the rule the scheduler's bulk materialization already uses. Dispatch still never locks an existing job.
- **Bounds:** at most 100 items and the 2 MiB prepared-envelope budget per batch (the same ceiling as a materialization plan); larger inputs must be split by the caller. Local admission counts a batch as one operation holding one envelope.
- **Prepared and staged variants** follow: `IPreparedJobDispatcher.PrepareMany` → immutable `PreparedBatch`, and provider `StageManyAsync(items, NpgsqlTransaction, …)` returning `StagedBatch` with no commit claim. Outbox sources may stage many messages in the caller's transaction; the relay still delivers one `ReceiveOutbox` per message.
- Unknown outcome, cancellation-after-send and `local_capacity_exceeded` behave exactly as for a single dispatch, applied to the batch as a whole.

### Uncertain outcome and caller recovery

Validation and uniqueness conflicts produce typed errors with stable codes. An I/O failure must distinguish **known not applied** from **outcome unknown**. Do not return a DispatchReceipt before its configured durability confirmation merely because an in-memory state change succeeded.

The [capacity budget proposal](capacity-defaults-and-budgets.md) adds a typed local-admission error with stable code `local_capacity_exceeded` and known-not-submitted disposition. It produces no DispatchReceipt and differs from a durable queue rejection or unknown commit. Acquire bounded local admission before allocating an additional serialized envelope where possible; prepared-request callers retain ownership of their existing envelope. Caller cancellation before submission retains its cancellation semantics. Once sent, the existing uncertain-outcome rules apply even if local I/O capacity is exhausted afterward.

Propose an advanced `IPreparedJobDispatcher` alongside the simple dispatcher:

```text
Prepare<TJob>(job, options) -> PreparedDispatch
DispatchAsync(PreparedDispatch, cancellationToken) -> DispatchReceipt
ResolveAsync(DispatchOperationId, cancellationToken) -> DispatchResolution
```

PreparedDispatch is immutable and contains the assigned operation ID/deadline plus frozen serialization, effective policy, and canonical command body. Prepare performs validation/serialization without writing storage. Reusing it retries the same command; it does not re-resolve defaults or redraw a deadline. It must not expose payload in ToString/logging. Its durable export codec is versioned and part of the wire-contract work, not arbitrary CLR serialization.

The convenience dispatcher prepares internally. An unknown-outcome exception carries that prepared request/recovery handle; cancellation after sending uses an OperationCanceledException-derived form carrying the same recovery data. Cancellation before submission may report known not applied. Callers that need to survive process death should persist the prepared envelope/operation identity with their business workflow, or use a stable fixed-window key covering their replay horizon.

Resolve returns Confirmed(receipt), NotObserved, Recovering, or ExpiredUnknown. NotObserved does not prove rollback: the original operation may still be executing. ExpiredUnknown must not be retried under a new ID merely to make the call succeed. A receipt may establish the logical result while a Redis persistence barrier still needs confirmation; do not report Confirmed until the requested profile is satisfied.

The concrete advanced type names and codec need implementation review, but the behavioral requirement is fixed in this proposal: callers must have a way to retry immutable intent and distinguish ambiguity. Resolve is application-scoped and subject to the same access boundary as dispatch metadata.

## 6. Caller-owned transactions remain staged

Provider-specific staging interfaces live in Runwick.Postgres and Runwick.Sqlite. Use `IPostgresJobDispatcher.StageAsync(job, NpgsqlTransaction, options, ct)` and its SQLite equivalent; they return `StagedDispatch`, not DispatchReceipt. They also accept a PreparedDispatch to preserve retry identity where applicable.

```csharp
await using var connection = await dataSource.OpenConnectionAsync(ct);
await using var transaction = await connection.BeginTransactionAsync(ct);
await users.InsertAsync(connection, transaction, user, ct);

var staged = await postgresDispatcher.StageAsync(
    new SendWelcomeEmail(user.Id, "en"), transaction,
    cancellationToken: ct);

await transaction.CommitAsync(ct); // caller owns the outcome
```

StagedDispatch reports proposed/resolved job ID, operation ID and duplicate disposition inside the transaction. It makes no committed durability claim. The driver neither commits/disposes/reopens the transaction nor performs invisible whole-business retries. If the caller rolls back, new job/key/receipt changes roll back together; a reused pre-existing job naturally still exists.

Validate an initialized store ID, application namespace, schema and operation compatibility on the supplied transaction's connection, as well as write capability. A copied store must receive a new identity before being treated as an independent store; an identity field alone cannot detect an accidentally reused clone. The caller must enlist against the workers' actual store. Do not compare secret-bearing connection strings or silently fall back to another connection.

Accepted 2026-10-30 (R6): the mirror operation for completion — `IPostgresJobCompletion.CompleteInTransactionAsync(NpgsqlTransaction, ct)` and a SQLite twin, callable from inside a handler — commits the handler's same-database side effects and the job's `succeeded` transition atomically. It returns a `StagedCompletion` token that the handler must return (or the engine detects); the engine then observes the caller's commit outcome instead of issuing its own Finish. Rules: same ownership checks and lock order as Finish, taken after the caller's business locks; Runwick never commits, rolls back or retries the caller's transaction; renewal continues as normal during the handler; an unknown commit reply follows the existing unknown-Finish receipt resolution; returning without a committed transaction is a pipeline fault, not success. This is the one place Runwick truthfully offers exactly-once; it is unavailable on the Cloud driver, where the inbox pattern applies. Scheduled with SP-011; engine consequences in the [review](../review-2026-10-30.md#r6--transactional-completion-exactly-once-for-same-database-effects).

Redis offers no general caller-business-transaction staging API. Cross-store delivery uses a source-store outbox with stable message IDs and a declared replay horizon; optional PostgreSQL and SQLite source adapters are selected in the [outbox baseline](transactional-outbox.md), with separate StagedOutboxMessage and ReceiveOutbox contracts. These guarantees are governed by the [driver matrix](driver-capabilities.md), not by overloading DispatchAsync with a generic transaction object.

## 7. Payload versions and safe registration

```csharp
runwick.ConfigureJob<SendWelcomeEmail>()
    .Upgrade<SendWelcomeEmailV1, SendWelcomeEmail>(
        fromVersion: 1,
        toVersion: 2,
        old => new SendWelcomeEmail(old.UserId, "en"));
```

The current handler receives SendWelcomeEmail. Historical DTOs are explicitly registered decoding types, not separate executable job identities, and need not implement IQueueJob. Adjacent steps compose to the registered current version. Validate gaps, cycles, duplicate routes and types at startup; no downgrade path is implied.

Upgrade delegates are pure deterministic transformations, without service resolution, external I/O or mutable captured state. Workers advertise only complete decoding tuples (job name, original payload version, serializer profile ID/version) with a full upgrade path. Malformed supported payloads fail permanently after Begin; unsupported versions/profiles remain waiting and visible to diagnostics. Never deserialize a persisted assembly-qualified name into an arbitrary runtime type.

Persist original payload bytes/version/profile and decode afresh on each authorized execution; upgrading does not rewrite queued history. Renaming a CLR type leaves its stable job name unchanged. The [serialization decision packet](identity-and-serialization.md) specifies the System.Text.Json profile, bounds and immutable decoding identity. Declared serializer/profile changes participate in request fingerprint compatibility; a deployment is not permission to reinterpret a retained window key silently.

## 8. Middleware and optional execution context

```csharp
public delegate Task<JobExecutionResult> JobExecutionDelegate();

public interface IJobMiddleware
{
    Task<JobExecutionResult> InvokeAsync(
        IJobContext context,
        JobExecutionDelegate next,
        CancellationToken cancellationToken);
}
```

JobExecutionResult is a closed outcome with factory methods `Succeeded()`, `Retry(reason, delay?)`, `Failed(code, message)`, and `Cancelled(reason)`. The public reason/error descriptor is bounded; internal exception serialization is not a public mutable AttemptError record. The engine classifies thrown exceptions; middleware may intentionally return an outcome.

An ordinary middleware calls and awaits next once and returns its result. An intentional successful short-circuit returns Succeeded explicitly. Retry consumes the Begin execution already authorized; it is not a free defer operation. Middleware cannot reset budgets, erase an engine timeout, refresh leases, or finalize jobs itself. The guarded continuation rejects repeated/late calls and joins already-started downstream work before scope cleanup.

IJobContext provides read-only job/attempt/generation identity, execution number, queue/priority, original/current payload version, upgraded payload access, schedule metadata, trace metadata and resource fencing values. Its Items collection is scoped to that attempt. Execution cancellation causes remain observable independently; the token is supplied to HandleAsync. Payload access can use `GetPayload<TJob>()` with a checked type; metadata/ownership cannot be mutated through the context.

Global middleware registration uses `UseMiddleware<T>()`. For job attributes, propose a named alias such as `[UseJobMiddleware("tenancy", Order = 100)]`, resolved by a worker registration `AddMiddleware<TenantMiddleware>("tenancy")`. This avoids forcing producer contract assemblies to reference worker implementation types. Unknown aliases fail worker validation; producer-only hosts need not instantiate them. Fluent job configuration may append registered middleware aliases/types under the ordering rules in §4.

Cache composition factories, not scoped middleware/handler/context instances. Current worker deployment selects executable middleware; it is not serialized into the job's retry policy. A behavior change therefore needs deployment compatibility review even though the persisted execution budget remains unchanged.

## 9. Capacity and schedule configuration surface

Fixed and automatic pool modes are mutually exclusive. `FixedConcurrency(8)` means eight local outstanding tickets, including pending finalization; `AutoConcurrency(min, max, ...)` selects the controller from the [capacity design](capacity-control.md). Naming a queue ceiling or job/resource rate policy configures shared store authority, not a local SemaphoreSlim.

The [D-07 packet](capacity-defaults-and-budgets.md) recommends an unspecified pool mode default to fixed 4, with automatic min/max explicit, a shared worker/store attempt budget, and configured pool maxima validated against that budget. These remain numerical proposals; example values above do not establish a different default.

Shared queue/type/resource policies have stable keys and seed-if-absent configuration. A key selector runs once during preparation and returns a bounded canonical resource identity. Store references include both scope identity and the job's required membership. No delegate or live capacity number is serialized as a permit. Runtime changes use expected policy revision; stronger membership changes require the previously specified quiesce/drain procedure.

Rate options must identify their algorithm. The current proposal is `RollingWindow(maxExecutions, window)` at Begin authorization, not physical downstream call rate. Priority options distinguish weighted, strict and strict-with-aging behavior. A rate-blocked Begin is an internal deferred reservation; callers still see the same queued job ID.

Illustrative schedule registration:

```csharp
runwick.AddSchedule("nightly-report", new BuildReport())
    .Cron("0 2 * * *", format: "five-field", timeZone: "Europe/Berlin")
    .OnGap("skip")
    .OnFold("earlier")
    .OnMisfire("run-once")
    .Overlap("serialize");

runwick.AddScheduler();
```

BuildReport represents another registered IQueueJob contract; the values above are explicit example choices. Schedule definition registration persists a deterministic template and versioned settings. It neither starts workers nor overwrites an existing conflicting definition. Stable schedule key, evaluator/zone-data version, revisioned controls and previewed occurrences follow the [scheduling design](scheduling-internals.md).

The [D-08 packet](scheduler-evaluator-and-defaults.md) recommends five-field cron/UTC by default, explicit six-field selection, skip gaps/earlier folds, run-once misfires after 60 s, allow overlap and skip-paused resume. Serialize in the example remains an explicit choice. The accepted grammar uses OR for two restricted day fields and rejects advanced parser-specific tokens. Registration previews and fingerprints include the exact evaluator/time-zone artifact profile; dependency types stay internal.

Manual triggers have their own operation ID and leave the recurring cursor alone. Job cancellation, queue pause and schedule pause are distinct controls. Serialize prevents overlapping valid grants, not backlog accumulation, strict chronological completion, or an expired handler's external side effects.

## 10. Management contracts and result semantics

Separate queries and commands from IJobDispatcher: proposed `IJobQueries`, `IJobControl`, `IQueueControl`, `IScheduleControl`, and `ICapacityControl`. Dashboard/HTTP code adapts these services; UI components do not write storage directly.

| Command family | Required caller context | Observable result |
|---|---|---|
| Cancel job | Job ID, expected generation/revision, command ID, reason | Requested for owned work or Cancelled for waiting work; AlreadyTerminal/conflict explicit |
| Retry failed/cancelled | Expected generation/revision, command ID, reason | Same job ID/new generation (`created_reason = manual_retry`); uniqueness conflict leaves it unchanged |
| Rerun succeeded | Expected generation/revision, command ID, reason | Same job ID/new generation (`created_reason = rerun_succeeded`), same retained payload and copied policy; requires retained payload (else HistoryUnavailable); reacquires active uniqueness like manual retry; permission `Jobs.Rerun` (confirmed 2026-10-30) |
| Pause/resume queue | Expected control revision, command ID | New control revision; already reserved work is not revoked |
| Pause/resume/edit schedule | Relevant expected revisions, command ID and explicit resume/cutover choice | New revisions plus materialization consequences/audit |
| Trigger schedule | Expected definition revision and command ID | Manual occurrence/job mapping |
| Change shared rate/ceiling | Expected policy revision, command ID, reason | Applied policy or conflict; rate-window freeze reported |
| Override pool target | Expected configuration/control revision, command ID, expiry | Fixed target override; actual outstanding work drains independently |

Retries are not permitted on still-owned running jobs; a succeeded job can only be re-run through the explicit Rerun command, never through Retry. Repeated same commands resolve by receipt; stale views conflict rather than silently affecting a newer generation. Query projections return revision tokens. Paginate histories and distinguish unavailable, not found, and retained identity with expired detail history.

Actor identity is supplied by a trusted host adapter/authenticated principal, not accepted from a browser's ActorId field. Application namespace comes from the configured authorized service boundary. The [management/dashboard design](management-and-dashboard.md) specifies authorization, audit redaction, Basic Auth/host-auth/OIDC, CSRF and transport safety. Public in-process service availability is not proof that an HTTP caller is authorized.

## 11. Request fingerprints and policy compatibility

The [dispatch wire/fingerprint proposal](dispatch-wire-contract.md) specifies the previously open identity inputs. It distinguishes immutable transport-command replay from business fixed-window idempotency. Changes to effective execution intent can conflict under an existing window key; trace metadata alone does not.

The generation policy includes the execution budget, timeout, retry evaluator/version/jitter/settings, abandonment guard, separate unstarted-recovery delay policy and scope membership. Dispatch-time changes affect newly created jobs only; manual retry copies the earlier effective generation policy. Management changing that policy during manual retry is a separate future operation, not an incidental fluent-config update.

The [execution defaults/timing packet](execution-defaults-and-timing.md) now recommends three executions, explicit retry/abandonment policies, no universal pipeline timeout and coherent worker lifetime budgets. These values require review/testing; scanner/scaler/schedule and other release defaults retain their own gates. Old sample numbers do not override the packet. No required runtime policy may remain unspecified at implementation acceptance.

## 12. API acceptance and remaining review

Validate the public surface with examples for producer-only, embedded, external, replicated and hybrid deployments; both annotated and fluent contracts; upgrade chains; ordinary/reused/unknown/staged dispatch; and management revision conflicts. The user-selected floor is .NET 10+; compile examples for the [platform/package targets](platform-and-packages.md) once projects exist. No snippets in this planning pass were compiled.

Required targeted cases include: identity conflicts; same-layer conflicting defaults; fluent-over-attribute scalar precedence; per-dispatch route recomputing queue membership without dropping resource limits; prepared replay after configuration changes; window-key conflicts after effective-policy changes; aliases usable from producer-only contracts; one scoped context per attempt; middleware short-circuit/repeated continuation; staged rollback; and no public option bypassing ownership/rate/persistence checks.

The [final baseline](../architecture-baseline.md) selects the public names/identity, configuration precedence, fixed priorities, middleware composition, prepared recovery APIs, acknowledgement behavior, optional outbox packages and numeric defaults. Compilation and runtime evidence remain outstanding; these are no longer separate planning topics.

Identity/serialization and timing packets refine D-03–D-05; [PostgreSQL integration](postgres-schema-and-operations.md) and [upgrades](postgres-upgrades-and-compatibility.md) now develop D-06/D-13. The [backlog](../implementation-backlog.md) and [checklist](../architecture-acceptance.md) track evidence. Provider/dependency selections and all six closure items are consolidated in the [final baseline](../architecture-baseline.md). No runtime implementation has started.
