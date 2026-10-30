# Internal storage command protocol

Status: engineering proposal for review, 2026-10-28. Builds on [planning continuity](../planning-continuity.md). This specifies internal mechanics; it does not replace the earlier public dispatcher/handler design. No runtime code has been implemented or tested.

2026-10-29 extension: [capacity control](capacity-control.md) proposes rate-grant checks and a committed RateLimited deferral result at Begin, with no execution-budget charge. The types below describe the initial execution milestone; the extension requires an operation-versioned update before rate limits are enabled. [Scheduling internals](scheduling-internals.md) adds separate atomic materialization/control commands using these receipt conventions.

The [driver matrix](driver-capabilities.md) distinguishes logical guarantees from physical transactions and acknowledgement profiles. The [Redis closure](redis-design-closure.md) selects recoverable intent and a direct-primary AOF-always profile. A pending intent reports Recovering/unknown, never Applied or confirmed no-effect. Common precision/encoding and the [contract suite](driver-contract-suite.md) apply.

The [identity/serialization decisions](identity-and-serialization.md) extend decoding capabilities with serializer profile ID/version and require original payload-byte preservation. JobContract below includes those fields; ready/Reserve filtering must match the complete tuple, not just job name/version. This is a versioned contract change for any future older implementation.

The [execution timing packet](execution-defaults-and-timing.md) now specifies proposed defaults, separate pre-start recovery delay, explicit retry evaluator/jitter versions, outcome boundaries and worker resolution/grace budgets. Generation policy below reflects those additions; no deployed protocol exists yet.

The [consolidated PostgreSQL contract](postgres-schema-and-operations.md) and [upgrade matrix](postgres-upgrades-and-compatibility.md) add transaction-level compatibility gates and explicit operation/result versions. The first release uses the consolidated semantics; an execution-only milestone cannot claim support for rate-enabled applications.

## 1. Decisions proposed in this item

1. Preserve `reserved` separately from `running` and `retrying` separately from newly pending work.
2. Create an attempt record when reserving. Assign its execution number and consume execution budget in a separate `BeginExecution` transaction before entering user middleware.
3. Bound repeated pre-start crashes with a separate reservation-abandonment budget; they do not silently consume handler executions or repeat without limit.
4. Record mutation command IDs and results atomically with their effects, to resolve a lost commit response.
5. Keep reservation plus all distributed permits atomic, and finalization plus permit release atomic.
6. For the first PostgreSQL implementation, use short, ordered scope-row locks with counted live permits. Numbered permit slots remain an optimization candidate, not an assumed design.

The extra begin transaction buys observable separation between reservation and invocation authorization. It does not prove that user code ran: a crash immediately after begin commits still consumes an execution attempt. This unavoidable gap is recorded rather than hidden. Begin precedes payload deserialization, per-attempt DI activation, and middleware invocation. Failures during that setup can therefore use normal Finish semantics; an unsupported contract is filtered before reservation.

## 2. Internal types

These sketches define field names and semantic types for the execution protocol. They are not public NuGet APIs. Internal enums are appropriate for closed engine decisions; persisted state codes and user-configured queues/priorities remain stable strings.

```csharp
internal sealed record StoreCommand<T>(
    string Application,
    Guid CommandId,
    DateTimeOffset NotAfter,
    T Body);

internal enum StoreStatus
{
    Applied, NoWork, QueuePaused, CapacityBlocked, RateLimited,
    StaleLease, InvalidState, BudgetExhausted,
    VersionConflict, UniquenessConflict, NotFound,
    CommandExpired, CommandConflict, UnsupportedVersion,
    AdmissionClosed, HistoryUnavailable
}

internal enum StoreEffect { ObservedNoEffect, Applied }

internal sealed record StoreError(
    string Code,
    long? ActualRevision = null,
    Guid? ConflictingJobId = null,
    Guid? BlockedScopeId = null,
    CandidateCursor? ResumeAfter = null);

internal sealed record StoreReply<T>(
    StoreStatus Status,
    StoreEffect Effect,
    bool Replayed,
    T? Value,
    StoreError? Error,
    DateTimeOffset DecidedAt) where T : class;

internal sealed record JobContract(
    string Name,
    int PayloadVersion,
    string SerializerProfileId,
    int SerializerProfileVersion);

internal sealed record CandidateCursor(DateTimeOffset EligibleAt, Guid JobId);

internal sealed record LeaseHandle(
    Guid JobId,
    Guid AttemptId,
    Guid WorkerInstanceId,
    Guid OwnershipToken,
    int Generation);

internal sealed record LeaseGrant(
    LeaseHandle Handle,
    DateTimeOffset ExpiresAt);

internal sealed record ResourceFence(Guid ScopeId, long Value);

internal sealed record JobPosition(
    Guid JobId,
    int Generation,
    long Revision,
    string State);

internal sealed record ReserveRequest(
    Guid WorkerInstanceId,
    string Queue,
    string Priority,
    IReadOnlyList<JobContract> SupportedContracts,
    CandidateCursor? After,
    TimeSpan LeaseDuration);

internal sealed record ReservedJob(
    JobPosition Position,
    LeaseGrant Lease,
    JobContract Contract,
    CandidateCursor Cursor,
    byte[] PayloadUtf8,
    ExecutionPolicySnapshot Policy,
    IReadOnlyList<ResourceFence> Fences);

internal sealed record ExecutionPolicySnapshot(
    int MaxExecutions,
    int MaxConsecutiveUnstartedAbandonments,
    TimeSpan? ExecutionTimeout,
    RetryPolicySnapshot Retry,
    RetryPolicySnapshot UnstartedRetry);

internal sealed record RetryPolicySnapshot(
    string Backoff,
    int EvaluatorVersion,
    TimeSpan InitialDelay,
    TimeSpan MaximumDelay,
    string JitterKind);

internal sealed record BeginRequest(LeaseHandle Lease);

internal sealed record BegunExecution(
    JobPosition Position,
    LeaseGrant Lease,
    int ExecutionNumber,
    long TotalExecutionNumber,
    DateTimeOffset AuthorizedAt);

internal sealed record RenewRequest(
    LeaseHandle Lease,
    TimeSpan LeaseDuration);

internal sealed record RenewedLease(
    LeaseGrant Lease,
    bool CancellationRequested);

internal enum AttemptOutcome
{
    Succeeded, RetryableFailure, PermanentFailure,
    UserCancelled, Interrupted, TimedOut
}

internal sealed record AttemptError(
    string Code, string ExceptionType, string Message,
    string? StackTrace);

internal sealed record FinishRequest(
    LeaseHandle Lease,
    AttemptOutcome Outcome,
    TimeSpan ExecutionElapsed,
    TimeSpan? RequestedRetryDelay,
    AttemptError? Error);

internal sealed record FinishedAttempt(
    JobPosition Position,
    string AttemptOutcome,
    DateTimeOffset? EligibleAt);

internal sealed record CancelRequest(
    Guid JobId,
    int ExpectedGeneration,
    long ExpectedRevision,
    string ActorId,
    string Reason);

internal sealed record RetryRequest(
    Guid JobId,
    int ExpectedGeneration,
    long ExpectedRevision,
    string ActorId,
    string Reason);

internal sealed record RecoverRequest(
    Guid JobId,
    Guid ExpectedAttemptId,
    Guid ExpectedOwnershipToken);

// Accepted 2026-10-30 (R2): neutral release of a reserved-not-begun attempt.
internal sealed record ReleaseRequest(LeaseHandle Lease);

internal sealed record ReleasedReservation(
    JobPosition Position,
    DateTimeOffset EligibleAt);
```

Required validation: positive bounded lease durations and budgets; nonnegative bounded delays/elapsed durations; finite bounded strings and payload/error sizes; bounded capability arrays; application scope on every query. A worker instance ID is unique to one process lifetime, never reused after restart. Ownership tokens are random identifiers for compare-and-set, not ordered resource fencing counters.

`Replayed` identifies a recorded past result; Effect distinguishes committed effects from an observational no-effect result. Committed RateLimited deferral has Effect=Applied and can be replayed. An accepted no-op with a retained receipt/audit is also Applied. DecidedAt is the stored decision time on replay. A successful reserve/begin/renew reply requires its value; rejected/deferral outcomes carry their typed metadata. A malformed combination is a driver contract violation.

## 3. Interfaces and atomic boundaries

```csharp
internal interface IJobReservationStore
{
    Task<StoreReply<ReservedJob>> TryReserveAsync(
        StoreCommand<ReserveRequest> command, CancellationToken ct);

    Task<StoreReply<BegunExecution>> BeginExecutionAsync(
        StoreCommand<BeginRequest> command, CancellationToken ct);

    Task<StoreReply<RenewedLease>> RenewAsync(
        StoreCommand<RenewRequest> command, CancellationToken ct);
}

internal interface IJobStateStore
{
    Task<StoreReply<FinishedAttempt>> FinishAsync(
        StoreCommand<FinishRequest> command, CancellationToken ct);

    Task<StoreReply<JobPosition>> RequestCancellationAsync(
        StoreCommand<CancelRequest> command, CancellationToken ct);

    Task<StoreReply<JobPosition>> RetryAsync(
        StoreCommand<RetryRequest> command, CancellationToken ct);

    Task<StoreReply<FinishedAttempt>> RecoverAsync(
        StoreCommand<RecoverRequest> command, CancellationToken ct);

    Task<StoreReply<ReleasedReservation>> ReleaseReservationAsync(
        StoreCommand<ReleaseRequest> command, CancellationToken ct);
}
```

Each call is one atomic semantic operation. Reservation cannot call an independently committing `IConcurrencyStore.Acquire` after committing a job claim. Driver-private helpers may be separated while sharing one transaction.

The first implementation reserves one job per transaction. The pool calls it only for already secured local capacity. Batching can follow after the one-job protocol passes contention tests. Batched renewal may group transport round trips, but must report independent per-lease outcomes; a stale lease must not roll back unrelated healthy leases.

An internal receipt query returns one of `Recorded` (operation name plus versioned reply), `NotObserved` (may still be executing), or `ExpiredUnknown`. Querying receipts is observational, not proof of current lease validity. Its concrete transport serialization is private to the driver.

## 4. Lost responses and replay

Every mutating command except Renew has a stable ID, immutable arguments, and an absolute NotAfter deadline backed by a receipt (Renew keeps the ID and deadline but no receipt; see below). Compute a canonical fingerprint of operation name/version, application, deadline, and body. Reusing an ID with a different fingerprint is CommandConflict.

Within the mutation transaction, insert/lock its command row. Another invocation of the same command waits for that transaction: if it commits, return the recorded result; if it rolls back, the retry may execute. Write result and effects together. Never commit an unfinished command placeholder.

Read an existing result before checking NotAfter. If no result exists and the deadline has passed, reject without performing the mutation. Retain receipts until after NotAfter plus a cleanup margin. Check current database time after acquiring the command lock and again at the mutation decision point after all other blocking locks; a command that expires while waiting must not apply. Cap how far in the future a new command deadline may be. A skewed caller clock may cause rejection; it must not cause duplicate application of a previously expired command.

Accepted 2026-10-30 (R1): **Renew carries no replayable receipt.** It keeps a command ID for transport correlation and logging only. Renew is the highest-volume ownership command (one per attempt per ~15 s), and receipt rows for it are pure write amplification. A lost Renew reply is already safe without a receipt: the worker's local safety deadline advances only on a fresh confirmed reply, so it simply issues a new ownership-checked Renew with a new command ID. If the earlier Renew committed, the new one extends the lease again (harmless); if the attempt was finalized or taken over meanwhile, the new Renew returns StaleLease/InvalidState, which is the correct observation. The per-attempt arbiter still serializes Renew after any unresolved Finish, so a stale Renew cannot be misread as a failed execution. Treat Renew like the ownership-checked heartbeat it is: validated, atomic, but observational for replay purposes. Reserve, Begin, Finish and Recover keep their receipts.

NoWork/QueuePaused/CapacityBlocked are observational outcomes and need not leave a committed receipt when no effect occurred. They may differ on a later retry. A read-only ready-work probe before creating a reservation command keeps ordinary idle polling from continually writing receipts. It is only a hint: TryReserve still rechecks everything atomically. This probe must never bypass resolution of an already-sent command whose commit outcome is unknown.

Transport errors distinguish confirmed rollback from commit outcome unknown. For unknown commit, retry the same command within its validity window or query its receipt; do not generate a fresh command ID for ordinary commands. ReceiveOutbox/1 has the narrowly defined exception below. Absence of a visible receipt is not proof that the first transaction failed.

Applied replay is historical. Before starting user code after resolving an uncertain reserve/begin response, perform a fresh ownership-checked renewal and inspect cancellation. The local execution registry permits invocation only once per attempt in that process. A new process does not adopt an old process's lease. This is not exactly-once execution across crashes.

An uncertain finish is retried with the same command ID and outcome. It must never re-enter the handler. If its receipt has expired, inspect current job/attempt history and report uncertainty rather than blindly applying an old command as new.

## 5. State and counters

States: pending → reserved → running → succeeded/retrying/failed/cancelled. Retrying returns to reserved when eligible. A reservation abandoned before begin returns to its prior waiting state, fails the reservation guard, or becomes cancelled.

- Every reservation creates an attempt with a lifetime reservation number and a nullable execution number.
- Begin assigns the generation execution number and increments the lifetime execution count atomically, then returns permission to invoke the pipeline.
- A reserved crash increments the generation's consecutive-unstarted-abandonment count. Its limit is a required policy value; repeated pre-start crashes cannot run forever. A reservation handed back through ReleaseReservation (graceful drain) does not increment it.
- Successful begin resets that consecutive counter. It consumes an execution even if the process dies before the actual method call.
- A running crash consumes the execution already counted. No second increment occurs during recovery.
- Manual retry creates a new generation row with zero generation counters and copied effective policy; lifetime counters and old rows remain intact.

This recommends start-based execution accounting over the later reservation-counted draft. It preserves reserved/running and the extra write/failure window. The timing packet proposes three executions, five consecutive unstarted abandonments and separate versioned delay policies; these are review values, not validated release defaults.

## 6. Command preconditions and results

| Command | Preconditions | Atomic effect | Important rejection |
|---|---|---|---|
| Reserve | Eligible supported job, unpaused queue, all limits admit | Attempt + job ownership + every permit + receipt | NoWork, QueuePaused, CapacityBlocked |
| Begin | Exact current unexpired lease, reserved, no durable cancellation | Running + execution counter + attempt start + receipt | StaleLease; a durable cancellation closes the unstarted attempt and returns InvalidState/cancelled_before_start, never a BegunExecution value |
| Renew | Exact current unexpired lease in reserved/running | Job and all associated permit expiries extended together; **no receipt** (R1) — an unknown reply is resolved by sending a fresh Renew | StaleLease; missing/mismatched permit is an invariant fault |
| Finish | Exact current unexpired running lease | Attempt outcome + job transition + permit release + receipt | StaleLease, InvalidState |
| Cancel | Expected generation/revision match | Request flag for owned jobs; terminal cancellation for waiting jobs; audit + receipt | VersionConflict, InvalidState |
| Manual retry | Failed/cancelled, expected generation/revision, active uniqueness available | New generation + waiting state + uniqueness + audit + receipt | VersionConflict, UniquenessConflict |
| Recover | Exact current attempt/token, lease expired | Close abandoned attempt + budget/cancellation transition + permit release + receipt | StaleLease (including a lease renewed before recovery gets its lock) |
| ReleaseReservation | Exact current unexpired lease, state `reserved` (never `running`) | Attempt closed as `released` + job back to its prior waiting state (pending if `started_count = 0`, otherwise retrying) with unchanged `eligible_at` + exact permit release + receipt; no execution, abandonment or rate charge; active uniqueness preserved; a durable cancellation request terminalizes instead | StaleLease, InvalidState (already running or terminal) |

Job revision changes on lifecycle/control mutations, not lease heartbeats. Historical cancel/finish flags cannot be applied to a later generation accidentally. The command ID covers replay of a successful admin action; expected revision covers an action composed against an obsolete view.

Cancellation of an owned reservation signals the worker but does not release its permits on the assumption it has stopped. Begin, controlled finalization, or recovery closes it. Queue pause alone does not revoke existing reservations.

Finish combines the engine's observed outcome with the current durable cancellation flag: normal timely success may win; an unsuccessful execution with a cancellation request becomes cancelled; otherwise retryable/timeout/interrupted outcomes use remaining execution budget. Permanent failure is terminal unless a concurrent cancellation takes precedence for an unsuccessful execution. Preserve both the observed error and the applied terminal reason in the attempt history. Timing uses engine-observed monotonic execution duration, not database acknowledgement latency.

## 7. Failure boundaries to implement first

1. Reserve commits but response disappears: replay finds the same attempt, never a second claim.
2. Begin commits but response disappears: resolve, revalidate ownership, and invoke only once locally.
3. Handler finishes but storage is unavailable: keep a pending finalization while ownership can be renewed; never report committed success prematurely.
4. Lease expires while waiting for a scope lock: recheck database time after locks and reject renewal/finalization.
5. A replacement acquires capacity from an expired permit: old-owner cleanup cannot delete replacement permits.
6. Cancellation and completion race: one job-row serialization order determines the result and preserves audit history.
7. Manual retry and old completion race: generation/token guards prevent the old attempt from changing the new generation.

See [PostgreSQL transaction design](postgres-storage-design.md) for records, indexes, lock order, SQL shapes, and validation work.

## Outbox destination extension — ReceiveOutbox/1

The [outbox baseline](transactional-outbox.md) adds a destination operation distinct from PreparedDispatch replay. Carry immutable source/message identity, destination binding, frozen intent and absolute AcceptUntil, plus the short transport command ID/NotAfter. The transport fingerprint includes all fields; the delivery fingerprint excludes only changing transport ID/deadline. Existing matching delivery mappings return the retained disposition; mismatched intent conflicts. New delivery requires a fresh authoritative instant before both deadlines.

Acquire gate → application → receipt → delivery identity → dispatch keys → new jobs/scopes; publish mapping, normal dispatch effects and receipt atomically. Keep the mapping through AcceptUntil +24 h independently of job detail. After an unknown transport request expires, only this operation may use a fresh request ID with the exact same immutable delivery identity/cutoff. It cannot extend the acceptance horizon or bypass receipt resolution. All destination drivers implement this operation before claiming outbox support. Source staging/relay operations are separately specified in the outbox document.
