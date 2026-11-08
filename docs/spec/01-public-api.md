# 01 — Public API

**Sources:** docs/architecture/public-api-contract.md §2–§10 (primary), docs/architecture/identity-and-serialization.md, docs/uniqueness-and-execution.md, docs/review-2026-10-30.md, docs/runwick-explained.md (examples; superseded where it conflicts), docs/architecture/dispatch-wire-contract.md, docs/architecture/execution-defaults-and-timing.md, docs/architecture/worker-engine-internals.md, docs/architecture/capacity-defaults-and-budgets.md, docs/architecture/management-retention-defaults.md, docs/architecture/platform-and-packages.md.

## Changes (2026-11-08, SP-025 spec)

- (2026-11-08, SP-019) Error table gains `transaction_profile_mismatch` (`RunwickErrorCodes.TransactionProfileMismatch`), raised by SQLite caller staging (20 §8 rule 1).

The reserved `UseRedis` (§18) is specified in 22: `UseRedis(configuration, configure)` with `RedisOptions` (22 §2.1), `RunwickRedisMigrations.MigrateAsync` (22 §9.1) and `AcknowledgementProfiles.RedisAofAlways = "redis.aof-always-single-primary-v1"` (22 §1). Redis has no staging or transactional-completion twin (22 §8). `DispatchOutcomeUnknownException` gains the code `store_recovering`: the Redis store held an unpublished intent when the call was made, so its outcome is not known yet; resend the same prepared request or call `ResolveAsync`, which can answer `DispatchResolutionStatus.Recovering` (22 §4.4, 03 Changes 2026-11-08, SP-025).

## Changes (2026-11-08, SP-019 spec)

The reserved SQLite twins (§18) are specified in 20: `UseSqlite(connectionString, configure)` with `SqliteOptions` (20 §2.1), `ISqliteJobDispatcher` and `ISqliteJobCompletion` (20 §8), `RunwickSqliteMigrations.MigrateAsync` (20 §9.1), and `AcknowledgementProfiles.SqliteWalFull = "sqlite.wal-synchronous-full"` (20 §1).

## Changes (2026-11-07, wider producer lane)

`ProducerOptions.MaxConcurrentCalls` defaults to **8** (was 2) and must be at least 1; the PostgreSQL store's producer lane and work pool follow it (06 Changes 2026-11-07, wider producer lane).

## Changes (2026-10-31, SP-016)

- §18: `AddScheduler()`, `AddSchedule<TJob>(...)` and `IScheduleControl` (preview) leave the reserved list; they are specified in 13 §2. New codes in §14: the schedule codes of 13 (`scheduler_not_active`, `duplicate_schedule`, `episode_unresolved`, `plan_too_large`, …).

## Changes (2026-10-31, SP-014)

- §18: `PoolBuilder.AutoConcurrency(min, max)` is built (11 §2) and leaves the reserved list.

## Changes (2026-10-31, SP-013)

- §8: `JobBuilder.RollingWindow(maxExecutions, window)` and `RollingWindowPerResource(selector, maxExecutions, window)` are built (10 §2). The queue rate builder stays reserved.
- §8: the M2 startup refusal `rate_policies_unsupported` applies only to a binary that does not know `rate_admission`. SP-013 builds know it. The application must still activate it (10 §6.1) before rate-scoped dispatch.
- §14 new codes: `invalid_rate_policy`, `too_many_rate_scopes` (registration), `rate_admission_not_active` (Dispatch, `StoreIncompatibleException`).
- §18: `RollingWindow` leaves the reserved list.

## Changes (2026-10-31, SP-011)

- §16 (R6) is no longer "not in M2": it is built in M3 on PostgreSQL, with the engine path normative in 08 §8. Misuse codes `no_active_attempt` / `completion_already_staged` surface as `JobCompletionException`. The SQLite twin is SP-019.
- §18: `IJobControl`, `IQueueControl`, `ICapacityControl` and `ManagementActor` exist as preview services (08 §10). `IJobQueries`, `IScheduleControl` and `IManagementSession` stay reserved.
- §14 new codes: `cancel_already_requested`, `no_change`, `scope_not_registered` (control results, 08 §10).

## Changes (2026-10-30)

Contradictions resolved:

| # | Resolution |
|---|---|
| A-1 | R3 (two digests) supersedes runwick-explained's single-digest fingerprint: a fixed-window re-dispatch whose business digest matches is `Reused`; a differing policy digest only sets `DispatchReceipt.PolicyDrift` (§11). |
| A-2 | R6 supersedes runwick-explained's `_runwick.CompleteInTransactionAsync(tx, ct)` form that discards the result: `IPostgresJobCompletion.CompleteInTransactionAsync` returns a `StagedCompletion`, which the engine detects or middleware returns via `JobExecutionResult.Completed(...)` (§16). |
| A-3 | runwick-explained's `StageAsync(job, transaction, ct)` passes the token in the `options` position and does not compile against the contract; the token is passed by name: `StageAsync(job, transaction, cancellationToken: ct)` (§15). |
| A-4 | uniqueness-and-execution.md's five states, reservation-counted `MaxAttempts = 5`, 10 s/5 min backoff and "no retry of succeeded jobs" are superseded by seven states, Begin-counted `MaxExecutions = 3`, 5 s/60 s backoff (06-defaults) and the M3 `RerunSucceeded` command. |
| A-5 | storage-design-inputs.md `.UpgradeFrom<T>` is superseded by `ConfigureJob<T>().Upgrade<TFrom,TTo>(fromVersion, toVersion, upgrade)` (§7). |
| A-6 | `[Queue]`/`[Priority]` keep their short names (D-03 frozen); the Hangfire `QueueAttribute` CS0104 collision is handled with a using alias (§4.2). |
| A-7 | runwick-explained's inbox example reads `job.JobId`; payloads carry no job ID. The job ID comes from `IJobContext.JobId` (§9, §19). |
| A-8 | identity-and-serialization.md lists `ExpiredUnknown` for "expired operation, no retained receipt", while the protocol makes a same-command re-send after `NotAfter` definitive under the command lock. Both hold: `ResolveAsync` (read-only) reports `ExpiredUnknown`; re-sending the prepared request returns the receipt or `command_expired` within the receipt horizon, and `expired_unknown` beyond it (§13). |

Implementation choices (concrete shapes the sources left open):

| # | Choice |
|---|---|
| I-1 | Assembly/namespace placement of every type (§1.1; revised 2026-10-30 to the implementation plan: middleware, result, dispatcher and receipt/batch/staged types live in `Runwick.Contracts` so producer contract assemblies can dispatch and declare middleware without the core package); all public types in namespace `Runwick`, provider types in `Runwick.Postgres`, `AddRunwick` in `Microsoft.Extensions.DependencyInjection`. |
| I-2 | Identifier regexes use `\A…\z` anchors (§2). Middleware aliases and PostgreSQL schema names get explicit grammars (§2.1). |
| I-3 | Library default queue is `"default"`; library default priority is `"medium"` (§6). A queue is "registered" when it is a job's effective queue, declared with `runwick.Queue(...)`, or subscribed by a pool. |
| I-4 | Assembly scanning registers only concrete classes carrying `[JobName]`; `AddHandler<TJob,THandler>` (and handler scanning) implicitly registers `TJob` (§5.3). |
| I-5 | `Application(name)` is required; there is no default application name. |
| I-6 | Timeout/backoff/abandonment policies have fluent and application-default forms only; the attribute layer covers identity, `Queue`, `Priority`, `MaxExecutions` and middleware aliases (the frozen attribute list). |
| I-7 | Fluent policy methods: `WithExecutionTimeout`, `WithoutExecutionTimeout`, `WithRetryDelay`, `WithMaxUnstartedAbandonments`, `WithUnstartedRecoveryDelay`; uniqueness selectors: `UniqueWhileActive`, `UniqueWithinWindow` (§5.4). |
| I-8 | Concurrency scopes per job ≤ 8 (queue + job-type + resources). `MaxConcurrent` capacity ≥ 1. Fencing is enabled for resource scopes seeded with capacity 1. |
| I-9 | Business metadata is `DispatchOptions.Metadata` (`IReadOnlyDictionary<string,string>`); diagnostic metadata is only W3C `traceparent`/`tracestate`, captured from `Activity.Current` (§10.3). |
| I-10 | `DispatchResolution` is a record with `Status`, `Receipt` (single) and `BatchReceipt` (batch) (§13). |
| I-11 | `BatchDispatchItem(IQueueJob Job, DispatchOptions? Options = null)`; registration lookup uses the job's runtime type; ordinals are 0-based; in-batch duplicates resolve in ordinal order (§12). |
| I-12 | `BatchItemFailure(int Ordinal, string Code, string Message, JobId? ConflictingJobId)` (§12). |
| I-13 | Exception hierarchy, `IRunwickError`, `ErrorDisposition` and the stable code list in §14 (names: `RunwickException`, `RunwickConfigurationException`, `DispatchValidationException`, `UniquenessConflictException`, `CommandConflictException`, `CommandExpiredException`, `LocalCapacityExceededException`, `StoreUnavailableException`, `StoreIncompatibleException`, `AdmissionClosedException`, `DispatchOutcomeUnknownException`, `DispatchCanceledAfterSendException`, `TransactionValidationException`, `JobOwnershipLostException`; `PermanentJobFailureException` in Contracts). |
| I-14 | `JobExecutionResult` is a sealed class with `Kind`/`ReasonCode`/`Message`/`RetryDelay`/`Completion`; its factories never throw on content — the engine validates when consuming (§8.3). Codes use the identifier grammar; messages are truncated at 1,024 UTF-8 bytes. |
| I-15 | `IJobContext` member list, `AttemptId`, `ResourceFence`, `JobTraceContext`, `JobCancellationCause`, `JobScheduleInfo` (§9). |
| I-16 | `ConfirmedAcknowledgementProfile` value for PostgreSQL is `"postgres.synchronous-commit"` (`AcknowledgementProfiles.PostgresSynchronousCommit`). |
| I-17 | `UsePostgres(string connectionString, Action<PostgresOptions>?)` is the only M2 overload; host-supplied `NpgsqlDataSource` registration is deferred. `PostgresOptions.Schema` defaults to `"runwick"`. |
| I-18 | `IRunwickJobRegistry` exposes the effective configuration with a per-value `ConfigurationSource` (the diagnostic source map, §6.2). |
| I-19 | Configuration validation runs once at host start (and at first resolution of a dispatcher if that is earlier) and reports every error in one `RunwickConfigurationException`. |
| I-20 | Worker registries are limited to 512 decoding tuples (the per-probe/Reserve capability-set bound; 03/04); more fails startup with `capability_set_too_large`. |
| I-21 | `PreparedDispatch`/`PreparedBatch` export via `Export()` and import via `IPreparedJobDispatcher.ImportPrepared`/`ImportPreparedBatch`. |
| I-22 | (2026-10-30, SP-002 implementation) §10.4/§13 named `payload_invalid`, `envelope_invalid` and `wrong_store`, while 02-serialization-and-wire §11 (and the WV-11 vectors) use the fine-grained `payload_*` and `prepared_envelope_*` codes. 02 wins: Prepare/import emit the fine-grained codes, which are added to `RunwickErrorCodes`; `payload_invalid`, `envelope_invalid` and `wrong_store` stay as reserved constants that M2 never emits. `envelope_too_large` remains the Prepare-time 2 MiB check. |
| I-23 | (2026-10-30, SP-004 implementation) MaxExecutions and MaxUnstartedAbandonments are 1–100, timeout ≤ 30 days, retry delays ≤ 24 h, matching 03 I-6 and the 04 DDL CHECKs; the earlier 1–1,000 text is superseded. Registry compilation and prepared-envelope import both enforce these bounds. |

## 1. Conventions

### 1.1 Assemblies, namespaces and lifetimes

| Type(s) | Assembly | Namespace | DI lifetime |
|---|---|---|---|
| `IQueueJob`, `IJobHandler<TJob>`, `IJobContext`, `JobId`, `AttemptId`, `DispatchOperationId`, `ResourceFence`, `JobTraceContext`, `JobCancellationCause`, `JobScheduleInfo`, attributes (§4), `PermanentJobFailureException`, `IJobMiddleware`, `JobExecutionDelegate`, `JobExecutionResult`, `StagedCompletion`, `IJobDispatcher`, `DispatchReceipt`, `DispatchOptions`, `DispatchUniqueness`, `DispatchDisposition`, `DuplicateMatch`, batch item/options/result/receipt types, `StagedDispatch`, `StagedBatch`, `AcknowledgementProfiles` | `Runwick.Contracts` | `Runwick` | `IJobContext`: scoped (attempt scope only); handlers and middleware: scoped; dispatcher: singleton |
| `IPreparedJobDispatcher`, `PreparedDispatch`, `PreparedBatch`, `DispatchResolution`, builders, options, `IRunwickJobRegistry`, exceptions, `RunwickErrorCodes` | `Runwick` | `Runwick` | dispatchers and registry: singleton |
| `AddRunwick` | `Runwick` | `Microsoft.Extensions.DependencyInjection` | — |
| `UsePostgres` extension | `Runwick.Postgres` | `Runwick` | — |
| `IPostgresJobDispatcher`, `PostgresOptions`, `IPostgresJobCompletion` | `Runwick.Postgres` | `Runwick.Postgres` | dispatcher: singleton; completion: scoped (attempt scope only, M3) |

Code blocks in this file list declarations by namespace; a block containing several `namespace X;` lines describes several source files. Member bodies shown (`=> …`) are normative only where they define observable behavior (e.g. `ToString`).

Rules for every public member:

- Nullable reference types are enabled; `?` marks every nullable parameter or property. `null` is never overloaded to mean both "inherit" and "disable".
- Asynchronous members return `Task`/`Task<T>` and take a trailing `CancellationToken cancellationToken = default`.
- No public type exposes a raw ownership token, a provider connection (except caller-supplied staging transactions), a lease, or a way to call Finish.
- `ToString()` of any type that carries payload, uniqueness keys, resource keys or metadata never includes them.
- Durations are `TimeSpan`, instants are `DateTimeOffset`. Every duration persisted or fingerprinted must be a whole number of milliseconds (the common time quantum); a non-whole value is rejected, never rounded to zero. `NotBefore` is the exception: it is rounded **upward** to the next millisecond (02-serialization-and-wire).

## 2. Identity grammar

### 2.1 Names

Names are ASCII, compared **ordinally and case-sensitively**, never trimmed (leading/trailing spaces are rejected by the grammar), never case-folded.

| Identity | Length | Exact regex (.NET, `RegexOptions.CultureInvariant`, no `IgnoreCase`) | Error code |
|---|---|---|---|
| Application name | 1–128 | `\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\z` | `invalid_application_name` |
| Queue name | 1–128 | `\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\z` | `invalid_queue_name` |
| Pool name | 1–128 | `\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\z` | `invalid_pool` |
| Schedule name (not in M2) | 1–128 | `\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\z` | — |
| Serializer profile ID | 1–128 | `\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\z` | — |
| Middleware alias | 1–128 | `\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\z` | `invalid_middleware_alias` |
| Metadata key | 1–128 | `\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\z` | `invalid_metadata` |
| Result reason/error code (§8.3) | 1–128 | `\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\z` | `middleware_contract_fault` |
| **Job name** | **1–200** | `\A[A-Za-z0-9][A-Za-z0-9._-]{0,199}\z` | `invalid_job_name` |
| PostgreSQL schema | 1–63 | `\A[a-z_][a-z0-9_]{0,62}\z` | `invalid_schema_name` |

The grammar is not a substitute for parameterization, identifier quoting or authorization.

### 2.2 Versions

| Identity | Domain | Error code |
|---|---|---|
| Payload version | `int`, 1 … 2,147,483,647; default 1; no downgrade inference | `invalid_payload_version` |
| Serializer profile version | `int`, ≥ 1 | — |
| Priority | exactly `"low"`, `"medium"` or `"high"` (ordinal) | `invalid_priority` |

The only serializer profile in M2 is `runwick.json` version 1 (02-serialization-and-wire). The decoding capability of a worker is the tuple **(job name, original payload version, serializer profile ID, serializer profile version)**.

### 2.3 Keys

Uniqueness keys (`ActiveKey`, `WindowKey`) and resource keys (`PerResource` selector output):

- 1–512 bytes when encoded as UTF-8;
- valid Unicode (no unpaired surrogates; malformed input is rejected, never replaced);
- no control characters (Unicode category `Cc`: U+0000–U+001F and U+007F–U+009F);
- opaque and case-sensitive: no trimming, Unicode normalization or case folding (composed and decomposed forms are different keys).

Violations fail with `invalid_key`. Keys are scoped by application and stable job name (uniqueness) or by application and scope kind (resources); queue and payload version never create a separate key scope.

### 2.4 Identifiers

`JobId`, `AttemptId` and `DispatchOperationId` wrap a `Guid` created with `Guid.CreateVersion7()`. Their canonical text is the lowercase 36-character `"D"` format. Ordering, where it matters (cursors, lock order), uses the canonical big-endian byte order of the UUID, never `Guid.CompareTo` (02-serialization-and-wire). Identifiers are never derived from or parsed into CLR names.

## 3. Job contract and handler

```csharp
namespace Runwick;

public interface IQueueJob { }

public interface IJobHandler<in TJob> where TJob : class, IQueueJob
{
    Task HandleAsync(TJob job, CancellationToken cancellationToken);
}

public readonly record struct JobId(Guid Value)
{
    public override string ToString() => Value.ToString("D");
}

public readonly record struct AttemptId(Guid Value)
{
    public override string ToString() => Value.ToString("D");
}
```

- A job contract is a class or record implementing `IQueueJob`. Payloads are data: attributes cannot resolve services or execute I/O.
- `HandleAsync` completing normally means success. Exceptions are classified by the engine (§17). The token is the execution cancellation token; cancellation is cooperative.
- `IJobContext` is optional: inject it into the handler's constructor when needed; omit it otherwise. It is never a `HandleAsync` argument.
- A producer needs only the contract assembly, not the handler assembly.

## 4. Attributes

### 4.1 Definitions

```csharp
namespace Runwick;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class JobNameAttribute(string name) : Attribute
{
    public string Name { get; } = name;
}

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class PayloadVersionAttribute(int version) : Attribute
{
    public int Version { get; } = version;
}

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class QueueAttribute(string name) : Attribute
{
    public string Name { get; } = name;
}

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class PriorityAttribute(string priority) : Attribute
{
    public string Priority { get; } = priority;
}

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class MaxExecutionsAttribute(int maxExecutions) : Attribute
{
    public int MaxExecutions { get; } = maxExecutions;
}

[AttributeUsage(AttributeTargets.Class, AllowMultiple = true, Inherited = false)]
public sealed class UseJobMiddlewareAttribute(string alias) : Attribute
{
    public string Alias { get; } = alias;
    public int Order { get; set; }          // default 0; lower runs outer
}
```

`Inherited = false`: a derived class never inherits a durable identity.

### 4.2 Attribute and fluent twins

| Attribute | Fluent twin (`AddJob<T>()` / `ConfigureJob<T>()`) | Application default twin (`JobDefaults`) | Layer rule |
|---|---|---|---|
| `[JobName("x")]` | `.Named("x")` | — | Identity: both may be present only if equal |
| `[PayloadVersion(n)]` | `.AtVersion(n)` | — | Identity: both may be present only if equal; default 1 |
| `[Queue("q")]` | `.ToQueue("q")` | `.ToQueue("q")` | Scalar precedence (§6) |
| `[Priority("p")]` | `.WithPriority("p")` | `.WithPriority("p")` | Scalar precedence |
| `[MaxExecutions(n)]` | `.WithMaxExecutions(n)` | `.WithMaxExecutions(n)` | Scalar precedence |
| `[UseJobMiddleware("a", Order = k)]` | `.UseMiddleware("a")` / `.UseMiddleware<T>()` | (global: `runwick.UseMiddleware<T>()`) | Collection composition (§8.4) |

Hangfire ships `Hangfire.QueueAttribute`. A file that imports both `Hangfire` and `Runwick` gets CS0104 on `[Queue]`. Resolve it with a using alias; the alias names the attribute class, so `[JobQueue(...)]` binds to Runwick's attribute:

```csharp
using JobQueue = Runwick.QueueAttribute;
using JobPriority = Runwick.PriorityAttribute;

[JobName("email.welcome")]
[JobQueue("emails")]
[JobPriority("medium")]
public sealed record SendWelcomeEmail(Guid UserId, string Locale) : IQueueJob;
```

## 5. Registration

### 5.1 Entry point

```csharp
namespace Microsoft.Extensions.DependencyInjection;

public static class RunwickServiceCollectionExtensions
{
    // Throws RunwickConfigurationException (runwick_already_added) if called twice on one collection.
    public static IServiceCollection AddRunwick(
        this IServiceCollection services,
        Action<Runwick.RunwickBuilder> configure);
}
```

### 5.2 `RunwickBuilder`

```csharp
namespace Runwick;

public sealed class RunwickBuilder
{
    public IServiceCollection Services { get; }

    // Identity of this host's logical application. Required; exactly one value.
    public RunwickBuilder Application(string name);

    // Job contracts and handlers.
    public JobBuilder<TJob> AddJob<TJob>() where TJob : class, IQueueJob;
    public JobBuilder<TJob> ConfigureJob<TJob>() where TJob : class, IQueueJob;
    public RunwickBuilder AddHandler<TJob, THandler>()
        where TJob : class, IQueueJob
        where THandler : class, IJobHandler<TJob>;
    public RunwickBuilder AddJobsFromAssemblyContaining<TMarker>();
    public RunwickBuilder AddHandlersFromAssemblyContaining<TMarker>();

    // Application job defaults (second precedence layer).
    public RunwickBuilder JobDefaults(Action<JobDefaultsBuilder> configure);

    // Queue declaration and its shared concurrency ceiling.
    public RunwickBuilder Queue(string name, Action<QueueBuilder>? configure = null);

    // Middleware.
    public RunwickBuilder UseMiddleware<TMiddleware>() where TMiddleware : class, IJobMiddleware;
    public RunwickBuilder AddMiddleware<TMiddleware>(string alias) where TMiddleware : class, IJobMiddleware;

    // Serializer metadata hook for runwick.json v1 (generated JsonTypeInfo); see 02.
    public RunwickBuilder AddJsonTypeInfoResolver(
        System.Text.Json.Serialization.Metadata.IJsonTypeInfoResolver resolver);

    // Roles and role options.
    public RunwickBuilder Producer(Action<ProducerOptions> configure);
    public RunwickBuilder AddWorker(Action<WorkerBuilder> configure);
}

namespace Runwick;   // in Runwick.Postgres.dll

public static class RunwickPostgresBuilderExtensions
{
    public static RunwickBuilder UsePostgres(
        this RunwickBuilder builder,
        string connectionString,
        Action<Runwick.Postgres.PostgresOptions>? configure = null);
}

namespace Runwick.Postgres;

public sealed class PostgresOptions
{
    public string Schema { get; set; } = "runwick";
}
```

Rules:

- Exactly one storage provider per `AddRunwick` (`missing_store`, `duplicate_store`). `UsePostgres` creates Runwick-owned data sources from the connection string (connection roles in 04-postgres); Runwick never logs the connection string.
- `AddWorker` may be called once (`duplicate_worker`). Omitting it yields a producer-only host: no worker hosted service runs. `AddScheduler`, `AddSchedule` and management registrations are not in M2.
- Repeating an identical registration is idempotent. Conflicting assignments within the same layer fail validation (`same_layer_conflict`); there is never "last writer wins".
- Registration does not instantiate handlers, middleware or scoped services.

### 5.3 Discovery and handler binding

- `AddJobsFromAssemblyContaining<TMarker>()` scans only `typeof(TMarker).Assembly`. It registers every non-abstract, non-generic-definition class implementing `IQueueJob` that carries `[JobName]`, regardless of accessibility, in ordinal order of `Type.FullName`. Classes without `[JobName]` are skipped (historical upgrade DTOs are therefore never picked up); register them with `AddJob<T>().Named(...)` if they are jobs.
- `AddHandlersFromAssemblyContaining<TMarker>()` registers every non-abstract, non-generic-definition class implementing one or more closed `IJobHandler<TJob>`; a class implementing several is bound to each `TJob`. Ordering is ordinal by `Type.FullName`.
- `AddHandler<TJob, THandler>()` (explicit or via scanning) also registers `TJob` as `AddJob<TJob>()` would.
- There is no process-wide or automatic assembly scan; discovery is deterministic and never resolves handlers.
- Each registered handler type is added to DI as scoped (`TryAddScoped`). One handler per stable job name per registry (`duplicate_handler`). A producer may register a contract without a handler.
- `ConfigureJob<TJob>()` configures a contract that must be registered by some other call before finalization (`job_not_registered`); it does not register by itself. `AddJob<TJob>()` and `ConfigureJob<TJob>()` return the same `JobBuilder<TJob>` surface and contribute to the same (fluent) layer.

### 5.4 `JobBuilder<TJob>` and `JobDefaultsBuilder`

```csharp
namespace Runwick;

public sealed class JobBuilder<TJob> where TJob : class, IQueueJob
{
    // Identity (must agree with attributes when both are present).
    public JobBuilder<TJob> Named(string name);
    public JobBuilder<TJob> AtVersion(int payloadVersion);

    // Routing.
    public JobBuilder<TJob> ToQueue(string queue);
    public JobBuilder<TJob> WithPriority(string priority);

    // Generation policy (snapshotted at dispatch).
    public JobBuilder<TJob> WithMaxExecutions(int maxExecutions);
    public JobBuilder<TJob> WithExecutionTimeout(TimeSpan timeout);
    public JobBuilder<TJob> WithoutExecutionTimeout();
    public JobBuilder<TJob> WithRetryDelay(TimeSpan initialDelay, TimeSpan maxDelay);
    public JobBuilder<TJob> WithMaxUnstartedAbandonments(int maxConsecutive);
    public JobBuilder<TJob> WithUnstartedRecoveryDelay(TimeSpan initialDelay, TimeSpan maxDelay);

    // Payload upgrades (§7).
    public JobBuilder<TJob> Upgrade<TFrom, TTo>(int fromVersion, int toVersion, Func<TFrom, TTo> upgrade)
        where TFrom : class
        where TTo : class;

    // Per-job middleware, appended in registration order (§8.4).
    public JobBuilder<TJob> UseMiddleware<TMiddleware>() where TMiddleware : class, IJobMiddleware;
    public JobBuilder<TJob> UseMiddleware(string alias);

    // Shared concurrency scopes (seeded if absent at startup).
    public JobBuilder<TJob> MaxConcurrent(int capacity);
    public JobBuilder<TJob> PerResource(Func<TJob, string> resourceKey, int capacity);

    // Registration-derived uniqueness keys (required dimensions, §11.4).
    public JobBuilder<TJob> UniqueWhileActive(Func<TJob, string> activeKey);
    public JobBuilder<TJob> UniqueWithinWindow(Func<TJob, string> windowKey, TimeSpan window);
}

public sealed class JobDefaultsBuilder
{
    public JobDefaultsBuilder ToQueue(string queue);
    public JobDefaultsBuilder WithPriority(string priority);
    public JobDefaultsBuilder WithMaxExecutions(int maxExecutions);
    public JobDefaultsBuilder WithExecutionTimeout(TimeSpan timeout);
    public JobDefaultsBuilder WithoutExecutionTimeout();
    public JobDefaultsBuilder WithRetryDelay(TimeSpan initialDelay, TimeSpan maxDelay);
    public JobDefaultsBuilder WithMaxUnstartedAbandonments(int maxConsecutive);
    public JobDefaultsBuilder WithUnstartedRecoveryDelay(TimeSpan initialDelay, TimeSpan maxDelay);
}

public sealed class QueueBuilder
{
    public QueueBuilder MaxConcurrent(int capacity);
}
```

Validation (all at startup, code `invalid_policy` unless stated):

| Member | Rule |
|---|---|
| `WithMaxExecutions(n)` / `[MaxExecutions(n)]` | 1 ≤ n ≤ 100. Counts committed Begins in a generation, including the first |
| `WithExecutionTimeout(t)` | 0 < t ≤ 30 days (2,592,000,000 ms), whole milliseconds. Measures the local pipeline (§17) |
| `WithoutExecutionTimeout()` | Explicitly disables a timeout inherited from a lower layer. Conflicts with `WithExecutionTimeout` in the same layer (`same_layer_conflict`) |
| `WithRetryDelay(i, m)` | Selects evaluator `exponential-v1` with equal jitter; 1 ms ≤ i ≤ m ≤ 24 h (86,400,000 ms); whole milliseconds |
| `WithMaxUnstartedAbandonments(k)` | 1 ≤ k ≤ 100. Mandatory guard: there is no way to disable it |
| `WithUnstartedRecoveryDelay(i, m)` | `exponential-v1`, equal jitter; 1 ms ≤ i ≤ m ≤ 24 h (86,400,000 ms); whole milliseconds |
| `MaxConcurrent(n)` (job or queue) | n ≥ 1 (`invalid_scope`) |
| `PerResource(selector, n)` | selector non-null; n ≥ 1 (`invalid_scope`); selector output validated per §2.3 at Prepare |
| scope count | queue + job-type + resource scopes resolved for one job ≤ 8 (`too_many_scopes`) |
| `UniqueWithinWindow(selector, w)` | w > 0, whole milliseconds (`invalid_uniqueness`) |

Library defaults for every policy value are normative in 06-defaults (MaxExecutions 3; no execution timeout; retry 5 s initial / 60 s max; abandonment guard 5; unstarted recovery 1 s / 30 s).

### 5.5 Shared concurrency scopes

| Call | Seeded scope `(kind, key, capacity)` | Fencing |
|---|---|---|
| `runwick.Queue("emails", q => q.MaxConcurrent(8))` | `("queue", "emails", 8)` | off |
| `ConfigureJob<T>().MaxConcurrent(5)` | `("job-type", <job name>, 5)` | off |
| `ConfigureJob<T>().PerResource(j => $"invoice:{j.InvoiceId}", 1)` | `("resource", <selector output>, 1)` | on when capacity = 1 |

- These compile to an internal `ScopeRequirement(kind, key, capacity)`. Queue and job-type scopes are seeded at startup with `RegisterScope/1` **seed-if-absent**; resource scopes are seeded if absent inside the dispatch transaction that first needs them (03-storage-protocol). An existing scope is never overwritten from code; a differing capacity is reported as drift.
- A job requires every scope it resolves to: its effective queue's scope (if declared), its job-type scope (if declared) and each resource scope. Reserve acquires a permit in all of them or none.
- A dispatch-time route override recomputes the queue scope for the destination queue and keeps the job-type and resource requirements.
- The resource selector is a pure, deterministic, thread-safe function run once during Prepare; it is never persisted. A throwing selector fails the dispatch with `key_selector_failed`. Two selectors producing the same key with different capacities fail with `scope_capacity_conflict`.
- These are distributed ceilings stored in the database, not local semaphores. Rate limits are not in M2.

### 5.6 Worker

```csharp
namespace Runwick;

public sealed class WorkerBuilder
{
    public WorkerBuilder Pool(string name, Action<PoolBuilder> configure);
    public WorkerBuilder MaxOutstandingAttempts(int maximum);     // worker/store group maximum
    public WorkerBuilder Configure(Action<WorkerOptions> configure);
}

public sealed class PoolBuilder
{
    public PoolBuilder Queues(params string[] queues);             // weight 1 each
    public PoolBuilder Queue(string queue, int weight);            // weight 1–1,000
    public PoolBuilder FixedConcurrency(int concurrency);          // n ≥ 1; target = hard maximum = n
    public PoolBuilder AutoConcurrency(int min, int max);          // not in M2: feature_not_available
    public PoolBuilder LaneSelection(LaneSelectionMode mode);
    public PoolBuilder LaneWeights(int high, int medium, int low); // 1–1,000 each; default 6:3:1
}

public enum LaneSelectionMode
{
    Weighted,          // default; aging (SP-012) applies from M3
    StrictWithAging,   // not in M2: feature_not_available
    Strict             // not in M2: feature_not_available; may starve lower lanes
}

public sealed class WorkerOptions
{
    public TimeSpan LeaseDuration { get; set; }
    public TimeSpan LeaseSafetyMargin { get; set; }
    public TimeSpan RenewalInterval { get; set; }             // nominal
    public TimeSpan RenewalIntervalMin { get; set; }          // per-attempt jitter range lower bound
    public TimeSpan RenewalIntervalMax { get; set; }          // per-attempt jitter range upper bound
    public TimeSpan LeaseWatchdogInterval { get; set; }
    public TimeSpan SchedulerAllowance { get; set; }
    public TimeSpan ControlRequestTimeout { get; set; }
    public TimeSpan ControlRetryBackoffInitial { get; set; }
    public TimeSpan ControlRetryBackoffMax { get; set; }
    public TimeSpan MinimumInvocationMargin { get; set; }
    public TimeSpan ClaimResolutionBudget { get; set; }
    public TimeSpan BeginResolutionBudget { get; set; }
    public TimeSpan CompletionResolutionBudget { get; set; }
    public TimeSpan CancellationGrace { get; set; }
    public TimeSpan CleanupBudget { get; set; }
    public TimeSpan OwnershipCommandLifetime { get; set; }
    public TimeSpan RecoveryScanInterval { get; set; }        // nominal
    public TimeSpan RecoveryScanIntervalMin { get; set; }
    public TimeSpan RecoveryScanIntervalMax { get; set; }
    public TimeSpan ShutdownBudget { get; set; }
    public TimeSpan ShutdownDrain { get; set; }
    public TimeSpan ShutdownCancellation { get; set; }
    public TimeSpan ShutdownFinalization { get; set; }
}
```

- Default values, the startup inequalities they must satisfy (`invalid_timing`) and the derived pressure gates are normative in 06-defaults and 05-worker-engine. Worker options are local worker policy: they are never dispatch options and never rewrite a persisted generation policy.
- A worker needs at least one pool (`worker_without_pool`); a pool needs at least one queue (`pool_without_queue`); pool names are unique (`duplicate_pool`). A pool with no concurrency call uses fixed 4. `FixedConcurrency(n)` requires n ≥ 1 (a zero target, which suspends admission, is an operational override, not in M2). Pool maxima must sum to at most `MaxOutstandingAttempts` (default 16; `pool_budget_exceeded`).
- A pool's subscribed queues are registered queues. Subscriptions are explicit; registering handlers never creates pools.
- The worker advertises a decoding tuple only for jobs with a bound handler and a complete upgrade path. At most 512 tuples per registry (`capability_set_too_large`).
- In M2 a worker refuses to start (`rate_policies_unsupported`) against an application whose store records enabled rate policies.

### 5.7 Producer options

```csharp
namespace Runwick;

public sealed class ProducerOptions
{
    public TimeSpan CommandLifetime { get; set; }      // default 5 min; maximum 5 min
    public int MaxWaitingOperations { get; set; }      // default 64
    public TimeSpan AdmissionWait { get; set; }        // default 1 s
    public int MaxConcurrentCalls { get; set; }        // default 8 (producer/admin mutation class; the PostgreSQL producer lane follows it), ≥ 1
}
```

`CommandLifetime` sets the frozen `NotAfter` offset assigned at Prepare. Hosts may lower but not raise these bounds (06-defaults).

### 5.8 Validation timing

The registry is compiled once into an immutable form when the host starts (or at first resolution of `IJobDispatcher`/`IPreparedJobDispatcher` if earlier). Validation collects **every** error and throws one `RunwickConfigurationException`. It validates names, versions, known queues/lanes, serializer metadata, upgrade graphs, middleware bindings (worker hosts only), scope declarations, policy completeness and worker timing. A producer-only host never needs middleware implementations to exist and does not fail on unknown aliases. After finalization, runtime code never sees an unspecified required value.

## 6. Configuration resolution

### 6.1 Precedence

Overridable scalar job settings resolve in this order (later wins):

**library defaults → application job defaults (`JobDefaults`) → job attributes → fluent job configuration → permitted dispatch override**

| Concern | Library default | `JobDefaults` | Attribute | Fluent | Dispatch override | Durable behavior |
|---|---|---|---|---|---|---|
| Stable job name | — (required) | — | `[JobName]` | `.Named` | never | Identity; must agree |
| Payload version | 1 | — | `[PayloadVersion]` | `.AtVersion` | never | Identity; must agree |
| Serializer profile | `runwick.json` v1 | — | — | — | never | Identity |
| Queue | `"default"` | `.ToQueue` | `[Queue]` | `.ToQueue` | `DispatchOptions.Queue` (registered queue only) | Snapshotted per generation; no live move |
| Priority | `"medium"` | `.WithPriority` | `[Priority]` | `.WithPriority` | `DispatchOptions.Priority` (raise or lower) | Snapshotted |
| MaxExecutions | 3 | `.WithMaxExecutions` | `[MaxExecutions]` | `.WithMaxExecutions` | never | Snapshotted |
| Execution timeout | none | `.WithExecutionTimeout`/`.WithoutExecutionTimeout` | — | same | never | Snapshotted |
| Retry delay policy | exponential-v1, 5 s / 60 s | `.WithRetryDelay` | — | same | never | Snapshotted |
| Unstarted abandonment guard | 5 | `.WithMaxUnstartedAbandonments` | — | same | never | Snapshotted separately from MaxExecutions |
| Unstarted recovery delay | exponential-v1, 1 s / 30 s | `.WithUnstartedRecoveryDelay` | — | same | never | Snapshotted |
| NotBefore | immediate | — | — | — | `DispatchOptions.NotBefore` | Stored eligibility, not an exact start |
| Uniqueness keys | none | — | — | `.UniqueWhileActive`/`.UniqueWithinWindow` | `DispatchOptions.Uniqueness` (must agree with derived keys) | Atomic with dispatch |
| Concurrency scopes | none | — | — | `.MaxConcurrent`/`.PerResource`; `runwick.Queue(...)` | never (route override recomputes queue scope only) | Membership snapshotted; capacity is live store authority |
| Middleware | none | — | `[UseJobMiddleware]` | `.UseMiddleware` | never | Current worker deployment; never serialized |

Rules:

- An unset value inherits. Explicit disabling uses a named operation (`WithoutExecutionTimeout`) and is impossible for mandatory guards.
- Identity declarations are not overrides: conflicting `[JobName]`/`.Named` or `[PayloadVersion]`/`.AtVersion` fail with `identity_conflict`. Two CLR types with the same job name fail with `duplicate_job_name`. Renaming a CLR type keeps its stable name; changing the stable name creates a different contract.
- A missing job name fails with `missing_job_name`; a CLR namespace/type/assembly name is never used as a fallback identity.
- `DispatchOptions` can never change shared capacities, lease duration, execution budget, timeout, retry policy, a resource fence, ownership or persistence.

### 6.2 Diagnostic source map

```csharp
namespace Runwick;

public enum ConfigurationSource { LibraryDefault, ApplicationDefault, Attribute, Fluent }

public readonly record struct EffectiveValue<T>(T Value, ConfigurationSource Source);

public sealed record RetryDelayPolicy(
    string Evaluator,          // "exponential-v1"
    string Jitter,             // "equal"
    TimeSpan InitialDelay,
    TimeSpan MaxDelay);

public sealed record ScopeDeclaration(string Kind, string? Key, int Capacity); // Key null for per-resource selectors

public sealed record JobDescriptor(
    string Name,
    Type PayloadType,
    EffectiveValue<int> PayloadVersion,
    IReadOnlyList<int> DecodablePayloadVersions,
    EffectiveValue<string> Queue,
    EffectiveValue<string> Priority,
    EffectiveValue<int> MaxExecutions,
    EffectiveValue<TimeSpan?> ExecutionTimeout,
    EffectiveValue<RetryDelayPolicy> RetryDelay,
    EffectiveValue<int> MaxUnstartedAbandonments,
    EffectiveValue<RetryDelayPolicy> UnstartedRecoveryDelay,
    IReadOnlyList<ScopeDeclaration> Scopes,
    IReadOnlyList<string> Middleware,      // effective per-job chain, outer to inner, by alias or type name
    Type? HandlerType);

public interface IRunwickJobRegistry
{
    string Application { get; }
    IReadOnlyList<JobDescriptor> Jobs { get; }       // ordinal order of Name
    IReadOnlyList<string> Queues { get; }            // registered queues, ordinal order
    JobDescriptor? Find(string jobName);
    JobDescriptor Get<TJob>() where TJob : class, IQueueJob;
}
```

## 7. Payload versions and upgrades

```csharp
runwick.ConfigureJob<SendWelcomeEmail>()
    .Upgrade<SendWelcomeEmailV1, SendWelcomeEmail>(
        fromVersion: 1,
        toVersion: 2,
        old => new SendWelcomeEmail(old.UserId, "en"));
```

- The handler always receives the registered current type. Historical DTOs are decoding types, not job identities, and need not implement `IQueueJob`.
- Every step is adjacent: `toVersion == fromVersion + 1` (`upgrade_not_adjacent`). Steps compose from each decodable version to the current version. The final step's `TTo` must be the current type; each step's `TFrom` must equal the previous step's `TTo`. Startup rejects gaps (`upgrade_gap`), cycles (`upgrade_cycle`), duplicate routes for one `fromVersion` (`upgrade_duplicate_route`) and one CLR type used for two versions or mismatched chain types (`upgrade_type_conflict`). No downgrade path is implied.
- Upgrade delegates are pure deterministic transformations: no service resolution, I/O or mutable captured state. They run only in the worker execution path, after Begin, under the execution budget — never during dispatch or fingerprinting.
- Stored bytes, payload version and profile are never rewritten; each authorized execution decodes afresh. A deterministic decode/upgrade failure of a supported tuple is a permanent payload failure after Begin (`payload_malformed` / `payload_upgrade_failed`); unsupported tuples are never reserved.

## 8. Middleware

### 8.1 Contract

```csharp
namespace Runwick;

public delegate Task<JobExecutionResult> JobExecutionDelegate();

public interface IJobMiddleware
{
    Task<JobExecutionResult> InvokeAsync(
        IJobContext context,
        JobExecutionDelegate next,
        CancellationToken cancellationToken);
}
```

`cancellationToken` is the same execution token passed to `HandleAsync`.

### 8.2 Semantics

- An ordinary middleware calls and awaits `next` **exactly once** and returns its result (or a result it substitutes).
- A middleware may short-circuit without calling `next`. Success without running the handler must be returned explicitly as `JobExecutionResult.Succeeded()`.
- `next` is guarded. Calling it a second time, calling it after the middleware's task completed, or completing the middleware's task while `next` is still running is a **pipeline contract fault**: the guard rejects the extra call (the returned task faults with `InvalidOperationException`), requests cancellation of downstream work, joins any already-started downstream task before the attempt scope is disposed, and latches the attempt outcome as `PermanentFailure` with code `pipeline_fault`, whatever the middleware later returns. A downstream task that ignores cancellation can keep the scope alive; the engine waits for it rather than disposing under it.
- An exception thrown by `next` propagates to the middleware; middleware may catch it and return a result. An exception escaping the pipeline is classified by the engine (§17).
- Middleware cannot extend leases, refresh authority, reset budgets, erase a latched timeout, or finalize jobs. `Retry` consumes the execution already authorized by Begin; it is not a free deferral.
- The terminal delegate invokes `HandleAsync` and returns `Succeeded()` when it completes normally.

### 8.3 `JobExecutionResult`

```csharp
namespace Runwick;

public enum JobExecutionResultKind { Succeeded, Retry, Failed, Cancelled, Completed }

public sealed class JobExecutionResult
{
    private JobExecutionResult() { }

    public JobExecutionResultKind Kind { get; }
    public string? ReasonCode { get; }              // Retry: reason; Failed: code; Cancelled: reason
    public string? Message { get; }                 // Failed only; ≤ 1,024 UTF-8 bytes (truncated with marker)
    public TimeSpan? RetryDelay { get; }            // Retry only
    public StagedCompletion? Completion { get; }    // Completed only (R6, M3)

    public static JobExecutionResult Succeeded();
    public static JobExecutionResult Retry(string reason, TimeSpan? delay = null);
    public static JobExecutionResult Failed(string code, string message);
    public static JobExecutionResult Cancelled(string reason);
    public static JobExecutionResult Completed(StagedCompletion completion);   // M3 (SP-011)
}
```

Factories never throw on content (they throw only `ArgumentNullException`); the engine validates when it consumes the result, so a malformed result cannot masquerade as a retryable exception:

| Result | Engine outcome | Validation |
|---|---|---|
| `Succeeded()` | Finish `Succeeded` | — |
| `Retry(reason, delay)` | Finish `RetryableFailure`, bounded by MaxExecutions | `reason` matches the code grammar (§2.1). `delay`: negative → `middleware_contract_fault` (PermanentFailure); otherwise rounded **up** to whole ms, 0 → 1 ms, capped at the generation's retry `MaxDelay`; no jitter is added to an explicit delay; null → policy delay |
| `Failed(code, message)` | Finish `PermanentFailure` with `code`/`message` | `code` grammar; else `middleware_contract_fault` |
| `Cancelled(reason)` | Finish `UserCancelled` | `reason` grammar; else `middleware_contract_fault` |
| `Completed(completion)` | Staged completion (§16) | `completion` must belong to the current attempt |

Message text longer than 1,024 UTF-8 bytes is truncated at a scalar boundary and suffixed with `…[truncated]`. Detailed exception text is stored separately (≤ 16 KiB, 06-defaults).

### 8.4 Registration and ordering

- `runwick.UseMiddleware<T>()` adds global middleware. `runwick.AddMiddleware<T>("alias")` binds an alias to a type for attribute/fluent use. Both register `T` as scoped; an instance is resolved from the attempt's DI scope per attempt. Composition factories are cached; scoped instances never are.
- `[UseJobMiddleware("alias", Order = n)]` names middleware in contract assemblies without referencing worker implementation types.
- Effective chain, outer to inner: **global middleware** in `UseMiddleware` call order → **attribute aliases** ordered by `Order` ascending, ties by ordinal alias → **fluent per-job middleware** in registration order → handler.
- A duplicate alias or type in one effective chain fails (`duplicate_middleware`). One alias bound to two types fails (`middleware_alias_conflict`). On a worker host an unknown alias fails (`unknown_middleware_alias`); the message names the job, the alias and the missing `AddMiddleware<T>("alias")` call. Producer-only hosts do not resolve aliases.
- Middleware is deployed code selected by the current worker; it is never serialized into a job's policy.

## 9. `IJobContext`

```csharp
namespace Runwick;

public interface IJobContext
{
    string Application { get; }
    JobId JobId { get; }
    AttemptId AttemptId { get; }
    int Generation { get; }                    // 1 for the dispatched generation
    int ExecutionNumber { get; }               // committed Begin number within the generation, from 1
    int MaxExecutions { get; }                 // effective generation budget
    string JobName { get; }
    string Queue { get; }
    string Priority { get; }
    int OriginalPayloadVersion { get; }        // version of the stored bytes
    int CurrentPayloadVersion { get; }         // registered current version handed to the handler
    string SerializerProfileId { get; }        // "runwick.json"
    int SerializerProfileVersion { get; }      // 1
    DateTimeOffset CreatedAt { get; }          // storage decision instant of job creation
    IReadOnlyDictionary<string, string> Metadata { get; }   // business metadata from dispatch
    JobTraceContext Trace { get; }
    IReadOnlyList<ResourceFence> Fences { get; }            // one per fencing-enabled permit held
    JobScheduleInfo? Schedule { get; }         // always null in M2
    JobCancellationCause CancellationCauses { get; }
    IDictionary<object, object?> Items { get; }             // per-attempt, not persisted

    TJob GetPayload<TJob>() where TJob : class, IQueueJob;
}

public readonly record struct ResourceFence(string ScopeKind, string ScopeKey, long Value);

public readonly record struct JobTraceContext(string? TraceParent, string? TraceState);

[Flags]
public enum JobCancellationCause
{
    None = 0,
    UserRequested = 1,
    Timeout = 2,
    Shutdown = 4,
    LeaseLost = 8
}

public sealed record JobScheduleInfo(
    string ScheduleName,
    DateTimeOffset ScheduledAt,
    DateTimeOffset CoveredFrom,
    DateTimeOffset CoveredUntil);
```

- One context instance per attempt, resolvable only inside the attempt's DI scope (resolution elsewhere throws `InvalidOperationException`). Everything except `Items` and `CancellationCauses` is immutable for the attempt.
- `GetPayload<TJob>()` returns the same upgraded instance passed to the handler; `TJob` must be exactly the registered current type, otherwise `InvalidOperationException`. The payload is decoded and upgraded before the DI scope and middleware are created, so middleware never observes an undecodable payload.
- `CancellationCauses` reports each locally observed cause independently; it is not inferred from one linked token. An `OperationCanceledException` thrown without an engine cause is not a user cancellation.
- `Fences` are monotonically increasing per scope across holders; they protect a resource only if that resource rejects lower values.
- The context exposes no database connection, ownership token, lease authority, renewal, progress or deadline-extension hook. Renewal is engine-owned by design: a handler cannot extend its own lease.
- `JobScheduleInfo` exposes both the scheduled instant and the covered range so a run-once job can report "covering 02:00–09:10" (scheduler, not in M2).

## 10. Dispatch

### 10.1 Types

```csharp
namespace Runwick;

public readonly record struct DispatchOperationId(Guid Value)
{
    public override string ToString() => Value.ToString("D");
}

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
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }   // business metadata
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

    Task<BatchDispatchReceipt> DispatchManyAsync(
        IReadOnlyList<BatchDispatchItem> items,
        BatchDispatchOptions? options = null,
        CancellationToken cancellationToken = default);
}

public static class AcknowledgementProfiles
{
    public const string PostgresSynchronousCommit = "postgres.synchronous-commit";
}
```

### 10.2 Semantics

- The contract is looked up by the job's **runtime type** (`job.GetType()`), which must be a registered job type (`job_not_registered`).
- `DispatchAsync` = local admission (§10.5) → Prepare (§13) → send → await confirmation. The convenience dispatcher prepares internally; on uncertainty its exceptions carry the prepared request (§14).
- **Created**: a new logical job was committed and the configured acknowledgement profile confirmed. A `dispatched` queue event is written in the same transaction.
- **Reused**: active/window policy resolved an existing logical job (§11). The payload is not replaced and the handler is not newly invoked. The reused job may already be terminal or have expired detail history. A receipt is identity, not a status snapshot.
- `ConfirmedAcknowledgementProfile` names the durability boundary that was confirmed before returning; for PostgreSQL it is `"postgres.synchronous-commit"` (driver-owned transaction committed with synchronous local commit). No receipt is returned before that confirmation.
- `NotBefore` is an eligibility bound, not an exact start time. Omitted = immediate intent (not "now"). A value is normalized to UTC and rounded up to the millisecond; past instants are eligible immediately (`eligible_at = max(requested, storage decision time)`).
- `Queue` must name a registered queue (`unknown_queue`; grammar `invalid_queue_name`). `Priority` must be `low`/`medium`/`high` (`invalid_priority`) and may raise or lower the declared priority.
- Cancelling `cancellationToken` stops waiting for this request. It never cancels an accepted job. Cancellation before submission throws a plain `OperationCanceledException` (known not applied); after submission it throws `DispatchCanceledAfterSendException` (outcome unknown).
- Dispatch is permitted while a queue is paused (pause, M3, only stops reservations).

### 10.3 Metadata

- `DispatchOptions.Metadata` carries **business metadata**: execution-relevant values such as tenant. Keys follow the identifier grammar (≤ 128); values are valid Unicode without control characters. Business metadata is part of the window business digest and is exposed as `IJobContext.Metadata`.
- **Diagnostic metadata** is only W3C `traceparent` and `tracestate`, captured from `Activity.Current` at Prepare. It is excluded from the business digest, included in the command fingerprint, and never replaces the original job's trace on reuse. Trace metadata never selects tenant, permission or execution authority.
- Business plus diagnostic metadata: at most 32 entries and 16 KiB encoded (sum of UTF-8 key and value bytes) (`metadata_too_large`). Invalid keys/values: `invalid_metadata`.

### 10.4 Payload

The payload is serialized with `runwick.json` v1 (02-serialization-and-wire) into a bounded buffer: ≤ 1 MiB UTF-8 (`payload_too_large`); structural violations (depth, string, property, array, numeric bounds; duplicate names; invalid Unicode) fail with `payload_invalid`; serializer exceptions fail with `payload_serialization_failed`. The complete prepared envelope (base64 payload plus all fields) is ≤ 2 MiB (`envelope_too_large`). After Prepare returns, mutating the source object cannot change the frozen request; concurrent mutation during Prepare is outside the supported contract.

### 10.5 Local admission

Producer/admin mutations pass a bounded local admission gate before any storage command is sent (budgets in 06-defaults: 8 concurrent calls by default, `MaxConcurrentCalls`, at most 64 waiting operations, 1 s admission wait, or the caller's earlier cancellation). When the gate rejects, the call throws `LocalCapacityExceededException` with stable code **`local_capacity_exceeded`** and disposition known-not-submitted (`ErrorDisposition.KnownNotApplied`): no storage command was sent and no `DispatchReceipt` exists. It is never reported as a durable queue rejection or an unknown commit.

- The convenience dispatcher acquires admission **before** preparing, so a rejected call allocates no envelope; its exception carries no prepared request.
- `IPreparedJobDispatcher.DispatchAsync(prepared)` keeps the caller's envelope; the exception references it and the caller may retry the same prepared request later.
- A batch counts as one operation holding one envelope.
- Once a command has been sent, uncertain-outcome rules apply even if local capacity is exhausted afterwards.
- Staging on a caller transaction (§15) uses the caller's connection budget and never raises `local_capacity_exceeded`.

## 11. Uniqueness

### 11.1 Dimensions

| Dimension | Question | Scope | Lifetime |
|---|---|---|---|
| Active key | "Is one already in flight?" | (application, job name, key) | Held while the job is `pending`, `reserved`, `running` or `retrying` (including delayed and retry-waiting); released atomically when the job becomes terminal; lease expiry never releases it; no independent TTL |
| Window key + window | "Did I already ask for this?" | (application, job name, key) | Fixed: expires at storage decision instant of first admission + window; never slides on duplicates; independent of job-history retention; survives the job's completion |

Uniqueness is **insertion** uniqueness: it never promises that a handler runs once, and it is not an execution lock (use a resource scope with capacity 1 for that).

### 11.2 Validation

- `ActiveKey`, `WindowKey`: null or a valid key (§2.3).
- `WindowKey` requires a positive whole-millisecond `Window`; a `Window` without a `WindowKey` is invalid (`invalid_uniqueness`). Expiry arithmetic is checked; overflow is rejected.
- All three null is equivalent to `Uniqueness = null`.
- Windows are caller policy; there is no library default window.

### 11.3 Results

Comparison takes place under the key locks with fresh storage time (03-storage-protocol). Bindings are compared using their stored fingerprint version.

| Situation | Result | `DuplicateMatch` | `PolicyDrift` |
|---|---|---|---|
| No live binding for any requested key | New job; bindings created; `Created` | `None` | false |
| Live window binding, business digest equal, no active key requested | `Reused` (original job, even if terminal) | `Window` | true iff policy digest differs |
| Live window binding equal; requested active key bound to the same job | `Reused` | `ActiveAndWindow` | true iff policy digest differs |
| Live window binding equal; requested active key unbound (job terminal) | `Reused`; the active key is not reclaimed | `Window` | true iff policy digest differs |
| Live window binding equal; requested active key bound to a **different** job | error `uniqueness_composition_conflict` | — | — |
| Live window binding, business digest differs | error `uniqueness_conflict` (payload not replaced) | — | — |
| No live window binding; live active binding | `Reused` (first writer wins; incoming payload discarded). If a window key was supplied, it is bound to the existing job, storing the **incoming** request's fingerprint | `Active` | false |

- The business digest covers: fingerprint/profile versions, application, job name, original payload version and canonical payload, resolved queue, priority and eligibility mode/instant, resolved scope membership and resource keys, active/window key values and window, business metadata. The policy digest covers MaxExecutions, timeout, retry evaluator/version/jitter/settings, abandonment guard and unstarted-recovery policy (02-serialization-and-wire). A route, payload, priority, scope or metadata change is a conflict; a policy-only change (including a changed library/application default) is `Reused` with `PolicyDrift = true`, and a `policy_drift` queue event is recorded. The original job keeps its original generation policy.
- Fingerprints are computed on submitted pre-upgrade payload intent; two versions that would upgrade to equal values are different requests.
- Changing only diagnostic trace metadata never conflicts.
- A new operation ID never bypasses a window conflict.

### 11.4 Derived versus explicit keys

- `UniqueWhileActive(selector)` / `UniqueWithinWindow(selector, window)` make that dimension **required** for the job: there is no dispatch option that disables it. When dispatch omits the explicit key, the derived value is used. A selector returning `null` fails with `uniqueness_required`; a returned value is validated per §2.3.
- If dispatch supplies an explicit key for a dimension that is also derived, both must be equal and the windows equal (`uniqueness_key_mismatch`).
- For a job without derived keys, dispatch-supplied keys are optional.
- Selectors run once during Prepare; a throwing selector fails with `key_selector_failed`.

## 12. Batch dispatch

```csharp
namespace Runwick;

public sealed record BatchDispatchItem(IQueueJob Job, DispatchOptions? Options = null);

public sealed record BatchDispatchOptions
{
    public bool ContinueOnConflict { get; init; } = false;
}

public sealed record BatchItemFailure(
    int Ordinal,
    string Code,
    string Message,
    JobId? ConflictingJobId);

public sealed record BatchItemResult(
    int Ordinal,
    DispatchReceipt? Receipt,            // present when Created or Reused
    BatchItemFailure? Failure);          // present only under ContinueOnConflict

public sealed record BatchDispatchReceipt(
    DispatchOperationId OperationId,
    IReadOnlyList<BatchItemResult> Items,
    string ConfirmedAcknowledgementProfile);
```

Semantics:

- **One command, one transaction, one receipt.** Every item is durably created/reused or none is. `Items` has exactly one entry per input item, ordered by `Ordinal` (0-based input index). Each item's `DispatchReceipt.OperationId` equals the batch operation ID. A lost reply resolves the whole batch through `ResolveAsync(OperationId)`.
- **Bounds:** 1–100 items (`batch_empty`, `batch_too_large`) and a 2 MiB total prepared-envelope budget for the batch (`batch_envelope_too_large`). Callers split larger inputs.
- **Validation is all-or-nothing before any write.** Any invalid item rejects the whole batch with `DispatchValidationException` (known not applied) whose `ItemFailures` lists every invalid ordinal.
- **Reused is success.** Reused items carry `PolicyDrift` where applicable.
- **Hard uniqueness conflicts** (`uniqueness_conflict`, `uniqueness_composition_conflict`) fail the entire batch by default with `UniquenessConflictException` listing every conflicting ordinal. With `ContinueOnConflict = true`, conflicting items get a `Failure` and no rows; all other items commit atomically together. If every item conflicts, the command still commits its receipt with only failures.
- **In-batch duplicates** resolve as if the items were dispatched sequentially in ordinal order inside the one transaction: a later item matching a binding created by an earlier ordinal is `Reused`.
- **Lock order:** all dispatch keys across the batch are sorted and locked once, then new jobs are created, then scopes are provisioned family by family. Dispatch never locks an existing job.
- Local admission, cancellation before/after send, unknown outcome and `local_capacity_exceeded` apply to the batch as a whole.

## 13. Prepared dispatch and recovery

```csharp
namespace Runwick;

public sealed class PreparedDispatch
{
    internal PreparedDispatch() { }

    public DispatchOperationId OperationId { get; }
    public DateTimeOffset NotAfter { get; }        // frozen absolute deadline
    public string Application { get; }
    public string JobName { get; }
    public int PayloadVersion { get; }
    public string Queue { get; }                    // resolved
    public string Priority { get; }                 // resolved

    public ReadOnlyMemory<byte> Export();           // versioned envelope (02)
    public override string ToString();              // "PreparedDispatch <operation-id> <job-name>"; no payload, keys or metadata
}

public sealed class PreparedBatch
{
    internal PreparedBatch() { }

    public DispatchOperationId OperationId { get; }
    public DateTimeOffset NotAfter { get; }
    public string Application { get; }
    public int Count { get; }

    public ReadOnlyMemory<byte> Export();
    public override string ToString();
}

public enum DispatchResolutionStatus { Confirmed, NotObserved, Recovering, ExpiredUnknown }

public sealed record DispatchResolution(
    DispatchOperationId OperationId,
    DispatchResolutionStatus Status,
    DispatchReceipt? Receipt,              // Confirmed single dispatch
    BatchDispatchReceipt? BatchReceipt);   // Confirmed batch dispatch

public interface IPreparedJobDispatcher
{
    PreparedDispatch Prepare<TJob>(TJob job, DispatchOptions? options = null)
        where TJob : class, IQueueJob;

    PreparedBatch PrepareMany(
        IReadOnlyList<BatchDispatchItem> items,
        BatchDispatchOptions? options = null);

    Task<DispatchReceipt> DispatchAsync(
        PreparedDispatch prepared,
        CancellationToken cancellationToken = default);

    Task<BatchDispatchReceipt> DispatchAsync(
        PreparedBatch prepared,
        CancellationToken cancellationToken = default);

    Task<DispatchResolution> ResolveAsync(
        DispatchOperationId operationId,
        CancellationToken cancellationToken = default);

    PreparedDispatch ImportPrepared(ReadOnlySpan<byte> envelope);
    PreparedBatch ImportPreparedBatch(ReadOnlySpan<byte> envelope);
}
```

Prepare:

- Validates and serializes without writing storage: resolves name/current version, serializes under the registered profile, merges configuration, validates route/priority, runs key/resource selectors once, normalizes instants, computes both window digests and the command fingerprint, binds the configured store identity and application, and assigns `OperationId` (`Guid.CreateVersion7()`) and `NotAfter` (= local UTC now + `ProducerOptions.CommandLifetime`, rounded down to the millisecond) **once**.
- The result is immutable. Dispatching it again retries the same command: no defaults, selectors or deadlines are re-resolved, even after configuration changes. Applications wanting new behavior prepare a new request with a new business key.
- Throws `DispatchValidationException` for invalid input. Never performs I/O.

Dispatch of a prepared request:

| Store observation | Result |
|---|---|
| Receipt exists with the same command fingerprint | The original receipt (historical result; nothing applied again) |
| Receipt exists with a different fingerprint under this operation ID | `CommandConflictException` (`command_conflict`) |
| No receipt, `NotAfter` not reached at the decision point | Applied now; receipt returned |
| No receipt, `NotAfter` passed, within the receipt horizon (`NotAfter` + 24 h) | `CommandExpiredException` (`command_expired`, known not applied: this operation can never apply) |
| No receipt, beyond the receipt horizon | `DispatchOutcomeUnknownException` (`expired_unknown`) |

ResolveAsync (read-only, application-scoped, same access boundary as dispatch, subject to local admission):

| Status | Meaning |
|---|---|
| `Confirmed` | A receipt exists and its acknowledgement profile is satisfied; `Receipt` or `BatchReceipt` is set |
| `NotObserved` | No receipt visible and `NotAfter` not yet passed. **Not proof of rollback**: the original operation may still be executing or apply later |
| `Recovering` | The store holds unpublished recovery intent for this operation (Redis); never returned by PostgreSQL |
| `ExpiredUnknown` | `NotAfter` has passed and no receipt is visible. The caller must not re-dispatch under a new operation ID merely to make the call succeed. Re-sending the same prepared request yields the definitive table above while within the receipt horizon |

- Resolve works by operation ID alone, even if the caller can no longer decode the historical payload. An operation ID that names a non-dispatch command fails with `not_a_dispatch_operation`.
- Callers that must survive process death persist the exported envelope or the operation ID with their business workflow, or use a fixed window key covering their replay horizon.

Export/import:

- `Export()` produces the versioned bounded JSON envelope with base64 original payload bytes, frozen effective intent, operation ID/deadline and serializer/fingerprint versions (format in 02-serialization-and-wire). The envelope contains business data and is not encrypted; keep it out of logs.
- Import validates schema, sizes and versions, recomputes every digest (a contained hash is never trusted), rejects unknown fields, and checks the application and store identity against this host (`wrong_store`). Invalid envelopes fail with `envelope_invalid`. Import never runs handlers, upgrades or type-name resolution, and never re-resolves defaults. Historical prepared policy is checked against current mandatory constraints, not overwritten; if it can no longer be honored, dispatch fails explicitly while receipt resolution remains available.

## 14. Errors

### 14.1 Types

```csharp
namespace Runwick;

public enum ErrorDisposition
{
    KnownNotApplied,   // nothing was written by this call (includes known-not-submitted)
    OutcomeUnknown     // the command may or may not have applied; resolve by operation ID
}

public interface IRunwickError
{
    string Code { get; }
    ErrorDisposition Disposition { get; }
}

public abstract class RunwickException : Exception, IRunwickError
{
    protected RunwickException(string code, ErrorDisposition disposition, string message, Exception? innerException = null);
    public string Code { get; }
    public ErrorDisposition Disposition { get; }
}

public sealed record ConfigurationError(string Code, string Subject, string Message);

public sealed class RunwickConfigurationException : RunwickException      // Code "configuration_invalid"
{
    public IReadOnlyList<ConfigurationError> Errors { get; }
}

public sealed class DispatchValidationException : RunwickException
{
    public IReadOnlyList<BatchItemFailure> ItemFailures { get; }        // empty for single dispatch
}

public sealed class UniquenessConflictException : RunwickException      // uniqueness_conflict | uniqueness_composition_conflict
{
    public JobId? ConflictingJobId { get; }                             // single dispatch
    public IReadOnlyList<BatchItemFailure> ItemFailures { get; }        // batch
}

public sealed class CommandConflictException : RunwickException { public DispatchOperationId OperationId { get; } }

public sealed class CommandExpiredException : RunwickException
{
    public DispatchOperationId OperationId { get; }
    public PreparedDispatch? Prepared { get; }
    public PreparedBatch? PreparedBatch { get; }
}

public sealed class LocalCapacityExceededException : RunwickException   // local_capacity_exceeded
{
    public PreparedDispatch? Prepared { get; }                          // set only for prepared-request calls
    public PreparedBatch? PreparedBatch { get; }
}

public sealed class StoreUnavailableException : RunwickException { }    // failed before the command was sent
public sealed class StoreIncompatibleException : RunwickException { }   // gate/operation/schema incompatibility
public sealed class AdmissionClosedException : RunwickException { }     // application mode closes admission

public sealed class DispatchOutcomeUnknownException : RunwickException  // outcome_unknown | expired_unknown
{
    public DispatchOperationId OperationId { get; }
    public PreparedDispatch? Prepared { get; }
    public PreparedBatch? PreparedBatch { get; }
}

public sealed class DispatchCanceledAfterSendException : OperationCanceledException, IRunwickError
{
    public string Code { get; }                                         // "cancelled_after_send"
    public ErrorDisposition Disposition { get; }                        // OutcomeUnknown
    public DispatchOperationId OperationId { get; }
    public PreparedDispatch? Prepared { get; }
    public PreparedBatch? PreparedBatch { get; }
}

public sealed class TransactionValidationException : RunwickException { }  // staging/completion transaction checks

public sealed class JobOwnershipLostException : RunwickException { }       // R6 staged completion, M3

namespace Runwick;   // in Runwick.Contracts.dll

public class PermanentJobFailureException(string code, string message, Exception? innerException = null)
    : Exception(message, innerException)
{
    public string Code { get; } = code;
}
```

Exception messages never contain payload, uniqueness keys, resource keys, metadata values or connection strings.

### 14.2 Stable codes

```csharp
namespace Runwick;

public static class RunwickErrorCodes
{
    // Configuration (RunwickConfigurationException.Errors[i].Code)
    public const string ConfigurationInvalid = "configuration_invalid";
    public const string RunwickAlreadyAdded = "runwick_already_added";
    public const string MissingApplication = "missing_application";
    public const string InvalidApplicationName = "invalid_application_name";
    public const string MissingStore = "missing_store";
    public const string DuplicateStore = "duplicate_store";
    public const string InvalidSchemaName = "invalid_schema_name";
    public const string MissingJobName = "missing_job_name";
    public const string InvalidJobName = "invalid_job_name";
    public const string IdentityConflict = "identity_conflict";
    public const string DuplicateJobName = "duplicate_job_name";
    public const string InvalidPayloadVersion = "invalid_payload_version";
    public const string InvalidQueueName = "invalid_queue_name";
    public const string UnknownQueue = "unknown_queue";
    public const string InvalidPriority = "invalid_priority";
    public const string SameLayerConflict = "same_layer_conflict";
    public const string InvalidPolicy = "invalid_policy";
    public const string JobNotRegistered = "job_not_registered";
    public const string DuplicateHandler = "duplicate_handler";
    public const string UpgradeNotAdjacent = "upgrade_not_adjacent";
    public const string UpgradeGap = "upgrade_gap";
    public const string UpgradeCycle = "upgrade_cycle";
    public const string UpgradeDuplicateRoute = "upgrade_duplicate_route";
    public const string UpgradeTypeConflict = "upgrade_type_conflict";
    public const string InvalidMiddlewareAlias = "invalid_middleware_alias";
    public const string UnknownMiddlewareAlias = "unknown_middleware_alias";
    public const string DuplicateMiddleware = "duplicate_middleware";
    public const string MiddlewareAliasConflict = "middleware_alias_conflict";
    public const string InvalidScope = "invalid_scope";
    public const string TooManyScopes = "too_many_scopes";
    public const string DuplicateWorker = "duplicate_worker";
    public const string WorkerWithoutPool = "worker_without_pool";
    public const string PoolWithoutQueue = "pool_without_queue";
    public const string DuplicatePool = "duplicate_pool";
    public const string InvalidPool = "invalid_pool";
    public const string PoolBudgetExceeded = "pool_budget_exceeded";
    public const string InvalidTiming = "invalid_timing";
    public const string CapabilitySetTooLarge = "capability_set_too_large";
    public const string FeatureNotAvailable = "feature_not_available";
    public const string UnsupportedCapability = "unsupported_capability";
    public const string RatePoliciesUnsupported = "rate_policies_unsupported";

    // Dispatch — known not applied
    public const string InvalidUniqueness = "invalid_uniqueness";
    public const string InvalidKey = "invalid_key";
    public const string UniquenessKeyMismatch = "uniqueness_key_mismatch";
    public const string UniquenessRequired = "uniqueness_required";
    public const string KeySelectorFailed = "key_selector_failed";
    public const string ScopeCapacityConflict = "scope_capacity_conflict";
    public const string InvalidMetadata = "invalid_metadata";
    public const string MetadataTooLarge = "metadata_too_large";
    public const string PayloadSerializationFailed = "payload_serialization_failed";
    public const string PayloadTooLarge = "payload_too_large";
    public const string PayloadInvalid = "payload_invalid";
    public const string EnvelopeTooLarge = "envelope_too_large";
    public const string EnvelopeInvalid = "envelope_invalid";
    public const string WrongStore = "wrong_store";
    public const string BatchEmpty = "batch_empty";
    public const string BatchTooLarge = "batch_too_large";
    public const string BatchEnvelopeTooLarge = "batch_envelope_too_large";
    public const string UniquenessConflict = "uniqueness_conflict";
    public const string UniquenessCompositionConflict = "uniqueness_composition_conflict";
    public const string CommandConflict = "command_conflict";
    public const string CommandExpired = "command_expired";
    public const string LocalCapacityExceeded = "local_capacity_exceeded";
    public const string StoreUnavailable = "store_unavailable";
    public const string StoreIncompatible = "store_incompatible";
    public const string AdmissionClosed = "admission_closed";
    public const string NotADispatchOperation = "not_a_dispatch_operation";
    public const string TransactionStoreMismatch = "transaction_store_mismatch";
    public const string TransactionInvalid = "transaction_invalid";

    // Dispatch — outcome unknown
    public const string OutcomeUnknown = "outcome_unknown";
    public const string CancelledAfterSend = "cancelled_after_send";
    public const string ExpiredUnknown = "expired_unknown";

    // Execution (attempt outcome codes assigned by the engine; R6 errors)
    public const string PayloadMalformed = "payload_malformed";
    public const string PayloadUpgradeFailed = "payload_upgrade_failed";
    public const string PipelineFault = "pipeline_fault";
    public const string MiddlewareContractFault = "middleware_contract_fault";
    public const string StagedCompletionNotCommitted = "staged_completion_not_committed";
    public const string ReservationAbandonmentExhausted = "reservation_abandonment_exhausted";
    public const string NoActiveAttempt = "no_active_attempt";
    public const string CompletionAlreadyStaged = "completion_already_staged";
    public const string OwnershipLost = "ownership_lost";
}
```

### 14.3 Code → exception → disposition

| Code(s) | Exception | Disposition |
|---|---|---|
| configuration codes | `RunwickConfigurationException` (one per validation, all errors listed) | KnownNotApplied |
| `job_not_registered`, `invalid_queue_name`, `unknown_queue`, `invalid_priority`, `invalid_uniqueness`, `invalid_key`, `uniqueness_key_mismatch`, `uniqueness_required`, `key_selector_failed`, `scope_capacity_conflict`, `too_many_scopes`, `invalid_metadata`, `metadata_too_large`, `payload_*`, `envelope_*`, `wrong_store`, `batch_*`, `not_a_dispatch_operation` | `DispatchValidationException` | KnownNotApplied |
| `uniqueness_conflict`, `uniqueness_composition_conflict` | `UniquenessConflictException` | KnownNotApplied |
| `command_conflict` | `CommandConflictException` | KnownNotApplied |
| `command_expired` | `CommandExpiredException` | KnownNotApplied |
| `local_capacity_exceeded` | `LocalCapacityExceededException` | KnownNotApplied (known-not-submitted) |
| `store_unavailable` (connection/pool acquisition failed before any byte of the command was sent) | `StoreUnavailableException` | KnownNotApplied |
| `store_incompatible` | `StoreIncompatibleException` | KnownNotApplied |
| `admission_closed` | `AdmissionClosedException` | KnownNotApplied |
| `transaction_store_mismatch`, `transaction_invalid` | `TransactionValidationException` | KnownNotApplied |
| `transaction_profile_mismatch` (SQLite caller connection without `foreign_keys = 1` and `synchronous = 2`, 20 §8) | `TransactionValidationException` | KnownNotApplied |
| `outcome_unknown` (I/O loss, timeout or cancellation around commit; any failure after send) | `DispatchOutcomeUnknownException` | OutcomeUnknown |
| `expired_unknown` | `DispatchOutcomeUnknownException` | OutcomeUnknown |
| `cancelled_after_send` | `DispatchCanceledAfterSendException` | OutcomeUnknown |
| (none) caller cancellation before submission | `OperationCanceledException` | not sent |
| `no_active_attempt`, `completion_already_staged` | `TransactionValidationException` | KnownNotApplied |
| `ownership_lost` | `JobOwnershipLostException` | KnownNotApplied (nothing staged; roll back) |

Unknown-outcome exceptions always carry the prepared request (`Prepared` or `PreparedBatch`) and the operation ID. The driver may internally retry a whole driver-owned transaction only when rollback is established, reusing the same command; it never generates a fresh operation ID to hide uncertainty.

## 15. Caller-owned transactions (PostgreSQL staging)

```csharp
namespace Runwick;

public sealed record StagedDispatch(
    JobId JobId,                         // proposed new or resolved existing job
    DispatchOperationId OperationId,
    DispatchDisposition Disposition,
    DuplicateMatch DuplicateMatch,
    bool PolicyDrift);

public sealed record StagedBatchItemResult(
    int Ordinal,
    StagedDispatch? Staged,
    BatchItemFailure? Failure);

public sealed record StagedBatch(
    DispatchOperationId OperationId,
    IReadOnlyList<StagedBatchItemResult> Items);

namespace Runwick.Postgres;

public interface IPostgresJobDispatcher
{
    Task<StagedDispatch> StageAsync<TJob>(
        TJob job,
        NpgsqlTransaction transaction,
        DispatchOptions? options = null,
        CancellationToken cancellationToken = default)
        where TJob : class, IQueueJob;

    Task<StagedDispatch> StageAsync(
        PreparedDispatch prepared,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default);

    Task<StagedBatch> StageManyAsync(
        IReadOnlyList<BatchDispatchItem> items,
        NpgsqlTransaction transaction,
        BatchDispatchOptions? options = null,
        CancellationToken cancellationToken = default);

    Task<StagedBatch> StageManyAsync(
        PreparedBatch prepared,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default);
}
```

```csharp
await using var connection = await dataSource.OpenConnectionAsync(ct);
await using var transaction = await connection.BeginTransactionAsync(ct);
await users.InsertAsync(connection, transaction, user, ct);

var staged = await postgresDispatcher.StageAsync(
    new SendWelcomeEmail(user.Id, "en"), transaction,
    cancellationToken: ct);

await transaction.CommitAsync(ct); // caller owns the outcome
```

- Staging returns `StagedDispatch`/`StagedBatch`, never `DispatchReceipt`: it makes **no** committed-durability claim. Job, key bindings, scopes, receipt and `dispatched` event are written on the caller's connection inside the caller's transaction with the same locks and lock order as dispatch.
- Runwick never commits, rolls back, disposes, reopens or retries the caller's transaction, and never substitutes another connection. If the caller rolls back, the new job, bindings and receipt roll back together; a reused pre-existing job naturally still exists.
- Before writing, the dispatcher validates on the transaction's own connection: initialized store identity equal to the configured store, application namespace, schema, operation compatibility and write capability. Failures throw `TransactionValidationException` (`transaction_store_mismatch`, `transaction_invalid` for a completed/null-connection/read-only transaction) or `StoreIncompatibleException`. Connection strings are never compared. A copied store needs a new store identity before it is treated as independent.
- A window binding created while staging expires at the storage decision instant inside the transaction plus the window; time spent before commit consumes part of the window.
- Staging uses the caller's connection budget: no local admission, no `local_capacity_exceeded`, no unknown-outcome exception (the commit is the caller's). After the caller commits, `ResolveAsync(OperationId)` reports `Confirmed`.
- Passing a `PreparedDispatch`/`PreparedBatch` preserves operation identity across caller retries in new transactions.
- `IPostgresJobDispatcher` is registered by `UsePostgres`. Redis has no staging API; cross-store delivery uses the outbox (not in M2).

## 16. Transactional completion (R6) — M3 (SP-011), not in M2

```csharp
namespace Runwick;

public sealed class StagedCompletion
{
    internal StagedCompletion() { }

    public JobId JobId { get; }
    public AttemptId AttemptId { get; }
    public Guid CommandId { get; }       // the staged Finish command; resolvable like any Finish receipt
}

namespace Runwick.Postgres;

public interface IPostgresJobCompletion
{
    Task<StagedCompletion> CompleteInTransactionAsync(
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default);
}
```

Behavior:

- Callable only from inside an attempt (`IPostgresJobCompletion` is resolvable only in the attempt scope; otherwise `no_active_attempt`). At most once per attempt (`completion_already_staged`).
- Executes Finish/1 with outcome `Succeeded` on the caller's connection: same ownership checks and lock order as Finish, taken after the caller's business locks; the receipt is written in the caller's transaction. Store identity is validated exactly as in §15. A stale owner gets `JobOwnershipLostException` (`ownership_lost`) and must roll back.
- Runwick never commits, rolls back or retries the caller's transaction. Lease renewal continues normally while the handler runs, so a long business transaction needs no longer lease.
- The two R6 forms:
  1. **Handler form (engine detects).** The handler calls `CompleteInTransactionAsync`, commits, and returns normally from `HandleAsync`. The attempt records the staged completion; the engine does **not** issue its own Finish and instead observes the caller's commit outcome by resolving the staged command's receipt.
  2. **Middleware form (returned token).** Middleware that performs the transactional completion returns `JobExecutionResult.Completed(staged)`; the engine verifies the token belongs to the current attempt and proceeds as in form 1.
- Outcome observation: receipt present → the job is `succeeded`. Pipeline returned normally but the transaction is still open or ended without commit → pipeline fault `staged_completion_not_committed` (PermanentFailure via the normal path; the engine never commits the caller's transaction). The handler threw after a committed completion → the commit stands; the exception is recorded as a diagnostic. The handler threw and the transaction rolled back → normal exception classification. Caller commit reply lost → existing unknown-Finish receipt resolution applies unchanged.
- Once a completion is staged, the normal path can never also finalize the attempt.
- This is the only exactly-once claim Runwick makes, and only for effects in the same database. It is unavailable on the hosted (Cloud) driver.

```csharp
public sealed class ApplyPaymentHandler(
    NpgsqlDataSource db,
    ILedger ledger,
    IPostgresJobCompletion completion) : IJobHandler<ApplyPayment>
{
    public async Task HandleAsync(ApplyPayment job, CancellationToken ct)
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        await using var tx = await connection.BeginTransactionAsync(ct);

        await ledger.CreditAsync(job.AccountId, job.Amount, tx, ct);
        StagedCompletion staged = await completion.CompleteInTransactionAsync(tx, ct);

        await tx.CommitAsync(ct);   // credit and completion commit together, or neither does
    }
}
```

## 17. Handler outcome classification (public view)

| Observation | Outcome |
|---|---|
| Timely normal return, or explicit `Succeeded()` | Succeeded (a concurrent durable cancellation request may coexist in history) |
| `Failed(...)`, `PermanentJobFailureException`, deterministic malformed payload/upgrade failure, pipeline/middleware contract fault | PermanentFailure |
| `Retry(...)` or any other unclassified exception | RetryableFailure, bounded by MaxExecutions |
| Pipeline duration ≥ configured execution timeout | TimedOut (retryable), even if user code returned normally after cancellation |
| Shutdown cancellation ends execution unsuccessfully | Interrupted (retryable) unless timeout or durable user cancellation decides |
| `Cancelled(reason)` | UserCancelled |
| `OperationCanceledException` without an engine cancellation cause | Exception classification (retryable) |
| Lease lost | No new Finish; storage recovery decides |

The pipeline clock starts at local invocation admission (including thread-pool delay, decoding, upgrades, DI activation and middleware) and stops when the awaited pipeline ends; it excludes Begin resolution, scope disposal and Finish acknowledgement. Storage applies the final precedence under its locks (03-storage-protocol, 05-worker-engine).

## 18. Reserved surfaces (not in M2)

These names are reserved so later milestones can add them without breaking the M2 surface: management services `IJobQueries`, `IJobControl`, `IQueueControl`, `ICapacityControl`, `IManagementSession`, SQLite `UseSqlite`/staging/completion twins, `UseRedis`, outbox `StagedOutboxMessage`/`ReceiveOutbox`. Their contracts are not specified here. Built in M3 and specified elsewhere: `RollingWindow` (10), `AutoConcurrency` (11), `AddScheduler`/`AddSchedule`/`IScheduleControl` (13).

## 19. Complete example

```csharp
using Runwick;
using Runwick.Postgres;

[JobName("email.welcome")]
[PayloadVersion(2)]
[Queue("emails")]
[Priority("medium")]
[MaxExecutions(3)]
public sealed record SendWelcomeEmail(Guid UserId, string Locale) : IQueueJob;

public sealed record SendWelcomeEmailV1(Guid UserId);          // historical decoding type

public sealed class SendWelcomeEmailHandler(
    IEmailSender emails,
    IJobContext context) : IJobHandler<SendWelcomeEmail>
{
    // context.JobId is stable across re-executions: use it to build provider idempotency keys.
    public Task HandleAsync(SendWelcomeEmail job, CancellationToken ct) =>
        emails.SendWelcomeAsync(job.UserId, job.Locale, $"{context.JobId}:welcome", ct);
}

public sealed class TenantMiddleware : IJobMiddleware
{
    public async Task<JobExecutionResult> InvokeAsync(
        IJobContext context, JobExecutionDelegate next, CancellationToken cancellationToken)
    {
        if (!context.Metadata.TryGetValue("tenant", out var tenant))
            return JobExecutionResult.Failed("tenant_missing", "Job has no tenant metadata.");

        using var scope = TenantScope.Enter(tenant);
        return await next();                       // exactly once
    }
}

builder.Services.AddRunwick(runwick =>
{
    runwick.Application("customer-service");
    runwick.UsePostgres(connectionString, pg => pg.Schema = "runwick");

    runwick.AddJobsFromAssemblyContaining<SendWelcomeEmail>();
    runwick.AddHandlersFromAssemblyContaining<SendWelcomeEmailHandler>();

    runwick.ConfigureJob<SendWelcomeEmail>()
        .Upgrade<SendWelcomeEmailV1, SendWelcomeEmail>(
            fromVersion: 1,
            toVersion: 2,
            old => new SendWelcomeEmail(old.UserId, "en"))
        .UseMiddleware("tenancy");

    runwick.Queue("emails", q => q.MaxConcurrent(8));
    runwick.AddMiddleware<TenantMiddleware>("tenancy");

    runwick.AddWorker(worker =>
    {
        worker.Pool("email-workers", pool =>
        {
            pool.Queues("emails");
            pool.FixedConcurrency(8);
        });
    });
});

// Fluent-only equivalent of the attributes above, written inside AddRunwick
// (an alternative or a matching declaration — never a conflicting one):
//
//     runwick.AddJob<SendWelcomeEmail>()
//         .Named("email.welcome")
//         .AtVersion(2)
//         .ToQueue("emails")
//         .WithPriority("medium")
//         .WithMaxExecutions(3);
//     runwick.AddHandler<SendWelcomeEmail, SendWelcomeEmailHandler>();

// Dispatch with both uniqueness dimensions (24 h is caller policy, not a library default):
var receipt = await dispatcher.DispatchAsync(
    new SendWelcomeEmail(user.Id, "en"),
    new DispatchOptions
    {
        Priority = "high",
        Metadata = new Dictionary<string, string> { ["tenant"] = tenantId },
        Uniqueness = new DispatchUniqueness(
            ActiveKey: $"user:{user.Id}",
            WindowKey: requestId,
            Window: TimeSpan.FromHours(24))
    },
    cancellationToken);

// Prepared request with explicit recovery:
var prepared = preparedDispatcher.Prepare(new SendWelcomeEmail(user.Id, "en"));
try
{
    receipt = await preparedDispatcher.DispatchAsync(prepared, cancellationToken);
}
catch (DispatchOutcomeUnknownException)
{
    var resolution = await preparedDispatcher.ResolveAsync(prepared.OperationId, CancellationToken.None);
    // Confirmed → resolution.Receipt; NotObserved → re-send `prepared` (same operation); never a new ID.
}
```

Producer-only hosts use the same registration without `AddHandler*`, `AddMiddleware` and `AddWorker`.
