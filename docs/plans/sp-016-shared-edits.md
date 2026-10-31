# SP-016 shared edits (apply at integration)

The SP-016 fork wrote only new files. This file lists every change SP-016 needs in files it was not allowed to edit. Each edit gives the file, an anchor (exact text as of 2026-10-31 in the m3 worktree) and the replacement. Anchors in files that SP-012 or SP-017 are also editing (marked †) may have moved; apply by meaning if the text no longer matches.

Nothing was built or run. Compile risks are listed at the end.

## New files (for reference)

| File | Content |
|---|---|
| `docs/spec/13-scheduler.md` | Spec: contract, records, planning, operations, lock order, lifetimes, defaults, case table |
| `src/Runwick.Postgres/Sql/0006_scheduler.sql` | Migration revision 6: four schedule tables, check-constraint widening |
| `src/Runwick/Protocol/ScheduleProtocol.cs` | Operation names, bounds, template, request/result records, `IScheduleStore`, canonical envelopes |
| `src/Runwick.Postgres/Store/PgStore.Schedules.cs` | `PgStore : IScheduleStore`, all eight operations and the due read |
| `src/Runwick.Postgres/Store/ScheduleReceiptCodec.cs` | Receipt encode/decode for registration, materialization, state |
| `src/Runwick/Scheduling/SchedulePlanner.cs` | `ScheduleRecurrence.For` (next-after function) and the pure planner |
| `src/Runwick/Scheduling/ScheduleBuilder.cs` | Public builder API (`AddScheduler`, `AddSchedule<TJob>`), enums, options, `ScheduleCompiler` |
| `src/Runwick/Scheduling/SchedulerService.cs` | Hosted scheduler role (seed, poll, plan, send, resend on unknown outcome) |
| `src/Runwick/Management/ScheduleControl.cs` | Public `IScheduleControl` + internal `ScheduleControlService` |
| `tests/Runwick.Postgres.Tests/ScheduleStoreTests.cs` | Real-store tests SCH-01…05, SCHED-09/10/12/13/15, PGX-02 schedule half, CMD-01/02/03, ISO-01 |

## Required code edits (without these the new files do not compile or do not work)

### E-1 `src/Runwick.Postgres/Store/PgStore.Dispatch.cs` — created reason parameter

Scheduled jobs are created with `created_reason = 'scheduled'`. `PgStore.Schedules.cs` calls `DispatchCoreAsync(tx, items, continueOnConflict: false, createdReason: "scheduled")`.

Anchor:
```csharp
    async Task<DispatchOutcome> DispatchCoreAsync(PgTx tx, IReadOnlyList<DispatchItem> items, bool continueOnConflict)
```
Replace with:
```csharp
    async Task<DispatchOutcome> DispatchCoreAsync(PgTx tx, IReadOnlyList<DispatchItem> items, bool continueOnConflict,
        string createdReason = "initial")
```
Anchor (generation insert, ~line 326):
```sql
                SELECT @application, g.job_id, 1, @now, 'initial', g.max_executions, g.max_unstarted, g.timeout_ms, g.retry, g.unstarted_retry
```
Replace `'initial'` with `@created_reason`, and add the parameter to that command next to its other `.P(...)` calls:
```csharp
.P("created_reason", createdReason)
```
If the insert is built inside a helper that does not see `createdReason`, thread it through as a parameter. `job_generations_reason_ck` (0001) already allows `'scheduled'`, so no constraint change is needed. Manual triggers also pass `"scheduled"` today.

### E-2 `src/Runwick/Dispatch/DispatchPreparer.cs` — expose PrepareItem

`ScheduleCompiler.Compile` calls `preparer.PrepareItem(job, options)`.

Anchor:
```csharp
    (DispatchIntent Intent, IReadOnlyList<ScopeRequirement> Scopes) PrepareItem(IQueueJob job, DispatchOptions? o)
```
Replace with:
```csharp
    internal (DispatchIntent Intent, IReadOnlyList<ScopeRequirement> Scopes) PrepareItem(IQueueJob job, DispatchOptions? o)
```

### E-3 `src/Runwick/Hosting/RunwickRuntime.cs` — known capability

Anchor:
```csharp
        ["transactional_completion"] = 1, ["rate_admission"] = 1, ["outbox_destination"] = 1,
```
Replace with:
```csharp
        ["transactional_completion"] = 1, ["rate_admission"] = 1, ["outbox_destination"] = 1,
        ["scheduler"] = 1,
```
Keep any entries SP-012/SP-017 added on the same line.

### E-4 `src/Runwick.Postgres/Store/PgTransaction.cs` † — creation gate and drain

Anchor:
```csharp
    public bool IsCreation => Operation is Operations.Dispatch or Operations.DispatchMany or Operations.Reserve
        or Operations.Begin or Operations.RegisterQueue or Operations.RegisterScope or OutboxOperations.ReceiveOutbox;
```
Replace with:
```csharp
    public bool IsCreation => Operation is Operations.Dispatch or Operations.DispatchMany or Operations.Reserve
        or Operations.Begin or Operations.RegisterQueue or Operations.RegisterScope or OutboxOperations.ReceiveOutbox
        || ScheduleOperations.IsCreation(Operation);
```
Anchor:
```csharp
        or Operations.RegisterApplication or Operations.EventsSince || Operations.IsAdministrative(Operation);
```
Replace with:
```csharp
        or Operations.RegisterApplication or Operations.EventsSince || Operations.IsAdministrative(Operation)
        || Operation is ScheduleOperations.SetScheduleState or ScheduleOperations.UpdateSchedule or ScheduleOperations.ResolveCatchUp
            or ScheduleOperations.RecordScheduleEvaluationFailure or ScheduleOperations.RevalidateSchedule;
```
Add `using Runwick.Protocol;` if it is not already present (`ScheduleOperations` lives there).

### E-5 `src/Runwick/Protocol/StoreProtocol.cs` — deadline cap

Anchor:
```csharp
        : operation == OutboxOperations.ReceiveOutbox ? OutboxOperations.TransportCap
```
Insert after it:
```csharp
        : ScheduleOperations.IsScheduler(operation) ? ScheduleOperations.SchedulerCap
```
RegisterSchedule keeps the 10 min default (its lifetime is 5 min). Scheduler commands use the durable (24 h) retention class, which the default already gives.

### E-6 `src/Runwick.Postgres/Migrations/PgMigrator.cs` † — migration and manifest

Anchor:
```csharp
        new("0005_readiness", LoadSql("0005_readiness.sql"), 5),
```
Insert after it (before SP-017's `0007_scaling` entry, if present):
```csharp
        new("0006_scheduler", LoadSql("0006_scheduler.sql"), 6),
```
Anchor:
```csharp
        [5] = ["ReadinessProbe@2", "ListUnsupportedBacklog"],
```
Insert after it:
```csharp
        [6] = ScheduleOperations.All,
```
Check that the `.sql` file is an embedded resource. If the csproj lists files instead of globbing `Sql/*.sql`, add `0006_scheduler.sql`.

### E-7 `src/Runwick.Postgres/PostgresServices.Store.cs` — store registration

Anchor:
```csharp
        services.AddSingleton<IOutboxDestinationStore>(sp => sp.GetRequiredService<PgStore>());
```
Insert after it:
```csharp
        services.AddSingleton<IScheduleStore>(sp => sp.GetRequiredService<PgStore>());
```

### E-8 `src/Runwick/Hosting/RunwickServiceCollectionExtensions.cs` — schedule control

Anchor:
```csharp
        services.AddSingleton<Management.ICapacityControl>(sp => sp.GetRequiredService<Management.ControlService>());
```
Insert after it:
```csharp
        services.AddSingleton(sp => new Management.ScheduleControlService(
            sp.GetRequiredService<RunwickRuntime>(), sp.GetService<IScheduleStore>(), sp.GetRequiredService<TimeProvider>()));
        services.AddSingleton<Management.IScheduleControl>(sp => sp.GetRequiredService<Management.ScheduleControlService>());
```

### E-9 `tests/Runwick.Harness/Store/SnapshotReader.cs` — snapshot tables

Anchor:
```csharp
        ("queue_events", "event_id"),
```
Insert before it:
```csharp
        ("schedules", "application, schedule_id"),
        ("schedule_definitions", "application, schedule_id, revision"),
        ("schedule_occurrences", "application, schedule_id, occurrence_key"),
        ("schedule_audit_ranges", "application, audit_id"),
```

### E-10 `tests/Runwick.Postgres.Tests/MigrationTests.cs` † — revision 6

- `SpecManifest`: `"schemaRevision":5` → the final revision (6, or 7 once SP-017 lands). Add to `"operations"`, following the existing literal's ordering: `"RegisterSchedule":[1]`, `"MaterializeOccurrences":[1]`, `"SetScheduleState":[1]`, `"UpdateSchedule":[1]`, `"TriggerSchedule":[1]`, `"ResolveCatchUp":[1]`, `"RecordScheduleEvaluationFailure":[1]`, `"RevalidateSchedule":[1]`.
- Doc comment listing phases: append `0006_scheduler`.
- Kill-resume theory: after `[InlineData("0005_readiness:activate", 5L)]` add
```csharp
    [InlineData("0006_scheduler:expand", 5L)]
    [InlineData("0006_scheduler:validate", 6L)]
    [InlineData("0006_scheduler:activate", 6L)]
```

### E-11 `src/Runwick/Errors/RunwickErrorCodes.cs` — optional constants

The new code uses string literals. For the public catalogue add:
```csharp
    // Schedules (13)
    public const string DuplicateScheduler = "duplicate_scheduler";
    public const string DuplicateSchedule = "duplicate_schedule";
    public const string ScheduleDefinitionMissing = "schedule_definition_missing";
    public const string InvalidSchedulePolicy = "invalid_schedule_policy";
    public const string ScheduleDelayUnsupported = "schedule_delay_unsupported";
    public const string ScheduleWindowUnsupported = "schedule_window_unsupported";
    public const string ScheduleOccurrenceReuseRequired = "schedule_occurrence_reuse_required";
    public const string SchedulerNotActive = "scheduler_not_active";
    public const string SchedulePaused = "schedule_paused";
    public const string EpisodeUnresolved = "episode_unresolved";
    public const string PlanTooLarge = "plan_too_large";
    public const string OccurrenceBelowHighWater = "occurrence_below_high_water";
    public const string CursorRewind = "cursor_rewind";
    public const string CutoffInFuture = "cutoff_in_future";
    public const string CatchUpLimit = "catch_up_limit";
    public const string CatchUpTooOld = "catch_up_too_old";
```

### E-12 Optional: `IJobContext.Schedule`

`IJobContext.Schedule` stays null. To fill it, Reserve/Begin would join `schedule_occurrences` on `(application, job_id)` and pass the key, the occurrence key and the scheduled-at time into the pipeline. This is not done because it touches `PgStore.Reserve.cs` † and `JobPipeline.cs` †.

## Constraint versions (0006)

0005_readiness and 0007_scaling touch no shared check constraints, so the names in 0006 are free today:

| Constraint | 0006 drops | 0006 adds |
|---|---|---|
| scope kinds | `concurrency_scopes_kind_ck` | `concurrency_scopes_kind_v2_ck` (+ `schedule`) |
| event kinds | `queue_events_kind_v3_ck` | `queue_events_kind_v4_ck` (+ 9 schedule kinds) |
| audit operations | `management_audit_operation_v2_ck` | `management_audit_operation_v3_ck` (+ 6 schedule ops) |
| audit targets | `management_audit_target_kind_v2_ck` | `management_audit_target_kind_v3_ck` (+ `schedule`) |

If a later migration widens the same constraints, it drops 0006's version, adds the next one and keeps 0006's values. If `SchemaContractTests` lists constraint names or event kinds, update it.

## Spec edits in existing files

| File | Edit |
|---|---|
| `docs/spec/00-overview.md` | Index: add row `13-scheduler.md`: schedules, materialization, misfires, manual triggers (SP-016). Line 79 row "Scheduler … `IJobContext.Schedule` is always null": point to 13 (keep the null note unless E-12 is done) |
| `docs/spec/01-public-api.md` | §18 Reserved surfaces: remove the scheduler entry and point to 13 §2 (`AddScheduler`, `AddSchedule<TJob>`, `IScheduleControl` preview) |
| `docs/spec/03-storage-protocol.md` | §8.4 lifetimes: scheduler family 30 s default / 60 s cap, RegisterSchedule 5 min; the operation list gains the eight schedule operations (13 §6) |
| `docs/spec/04-postgres.md` | Migration list: `0006_scheduler`, revision 6, with its tables and widened constraints (13 §3) |
| `docs/spec/05-worker-engine.md` | Note that the scheduler role is independent of workers and holds no lease (13 §4.1) |
| `docs/spec/06-defaults.md` | §1.12 table: poll 1 s, idle 5 s, jitter ±10 %, batch 32, plan 2 MiB, episode limit 1000 (max 10 000), catch-up age 1 min–31 d, pacing 250 ms, jobs/s and burst (13 §10) |
| `docs/spec/08-*.md` §3.1 | Lock rank: the schedule row ranks before dispatch keys and scopes (13 §8) |
| `docs/spec/15-*.md` §8 | Case table: add SCH-01…05 and SCHED-09…15 with test names from `ScheduleStoreTests` |

## Not done

- PGX-04 (two-store scheduler replicas) and P-06 (scheduler throughput gate) have no tests.
- SCHED-11 (local incompatibility never sets needs_review) and SCHED-14 have no tests. SCHED-11 is handled only in code: `SchedulerService.MaterializeOneAsync` records it in `LocalDiagnostics`.
- No pure planner unit tests in `tests/Runwick.Core.Tests/Scheduling/`.
- No delete/tombstone of schedules (deferred by decision).
- No public wrappers for UpdateSchedule and ResolveCatchUp on `IScheduleControl`. The store operations exist.
- `Coalesced` is always false for the first occurrence of a Normal plan, because the stored `next_due_at` does not carry the flag.
- `IJobContext.Schedule` (E-12).
- No test runs `SchedulerService` through the hosted path (seed and loop).

## Unverified compile risks

1. `PgStore.Schedules.cs`: nested conditional expressions that produce nullable tuples `(StoreStatus, string, string?)?` may need explicit casts.
2. `PgStore.Schedules.cs`: pattern variables are captured in lambdas inside conditional branches, and `(none, null)` relies on conditional typing in `DispatchScheduledAsync`.
3. Tuple-list conversions where the element names differ (`(Guid JobId, string Disposition)`).
4. `ScheduleControlService.StateAsync` passes `Store.SetScheduleStateAsync` as a method group to a generic `Func<StoreCommand<TRequest>, …>`, so inference may need explicit type arguments.
5. The names `ScheduleEvaluationException.Class` and `.WireName` are assumed from SP-015.
6. The `ReadOnlyAsync` and `CheckGate` signatures in `PgStore.Readiness.cs` † may change under SP-012.
7. `Fingerprints.Policy` and `ReceiptCodec.Policy/ReadPolicy` are assumed to be internal or more visible.
8. `ReadDueSchedulesAsync` reads `x.*` from a LATERAL subquery by ordinal, so the column order must match `ReadSnapshot`.
9. Tests: `FaultPoint` is internal, so the SCH-02 theory takes the point name as a string. `Jobs`, `Receipts` and `TextAsync` come from `DispatchSupport`.
10. Tests assume a schedule row starts at `cursor_revision` 1 and that each applied materialization adds 1 (ISO-01).
