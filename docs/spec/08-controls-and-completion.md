# 08 — Revisioned controls, audit, events and transactional completion (M3, SP-011)

Sources: storage-command-protocol §6; management-and-dashboard §4, §6; management-retention-defaults ("Command lifetimes", "Audit"); review R6/R7; uniqueness-and-execution (manual retry); spec 01 §16, §18; spec 03 §5.2, §6.4–§6.6, §8, §10–§11; spec 04 §3, §7.14–§7.16, §8.4–§8.6; spec 05 §3.3; spec 06 §1.8.

## Changes (2026-10-31, SP-016)

- §3.1 rule 5: schedule operations take the schedule row after the receipt and before queue control (13 §8). Control operations on jobs never lock a schedule row.

## Changes (2026-10-31)

### Contradictions resolved

- **RerunSucceeded placement.** Backlog §6b lists `RerunSucceeded` under SP-031; spec 00 §3, spec 03 §11 and the delivery plan list it under SP-011. → SP-011. `job_generations.created_reason` has existed since SP-005; SP-031 keeps the rest of its list.
- **Event cursor scope.** Backlog §4 SP-011 omits the versioned event schema and keyset cursor that the delivery plan includes. → Included here (§7). The delivery plan is the schedule authority (00 §2.3).
- **SQLite completion twin.** Backlog SP-011 names `CompleteInTransactionAsync` for "PostgreSQL/SQLite", but there is no SQLite provider before SP-019. → PostgreSQL only here; the SQLite twin is an SP-019 obligation.
- **`IJobStateStore` reserved methods.** Spec 03 §3.4 shows `RequestCancellationAsync`/`RetryAsync` on `IJobStateStore`; the M2 code never added them. → Control operations get their own internal interface `IJobControlStore` (§3), registered by drivers that support controls. `IRunwickStore` is unchanged, so drivers and test doubles without controls need no stubs. 03 §3.4 now points here.

### Implementation choices

- [choice] Administrative operations use the existing `durable` retention class: the same 24 h floor and 10 min cap that 03 §8.4 already gives administrative commands. No new class or partition is added.
- [choice] Event cursor (§7): `queue_events` gains `tx_id xid8` and `schema_version`. The cursor is `(tx_id, event_id)`, and a page only returns rows whose `tx_id` is below the reader's snapshot `xmin`. Identity values are assigned before commit, so a cursor over `event_id` alone can skip a row that a slower transaction commits later; the `xmin` horizon closes that gap.
- [choice] Bulk controls (CTL-05) are a client-side fan-out in the preview services. Each item is its own command, with its own receipt, audit row and per-item result. There is no bulk storage operation in M3.
- [choice] Schema revision 2 (`0002_controls`). An M3 binary writes only to a store at its own revision. M2 binaries are rejected by their own gate (`schema_incompatible`) once 0002 validates, so mixed M2/M3 fleets are not supported. The full upgrade matrix is SP-026.
- [choice] `ManagementActor` is host-supplied and trusted. Authorization, authentication and HTTP belong to SP-020.

## 1. Scope

This file is normative for:

- the storage operations `Cancel/1`, `ManualRetry/1`, `RerunSucceeded/1`, the public `SetQueueState/1`, `EditScopePolicy/1` and `EventsSince/1`;
- the trusted actor and `management_audit`;
- the versioned event schema;
- the R6 transactional completion protocol on the engine path;
- the five deployment shapes (§9).

Spec 03 keeps the state machine, precedence tables, receipt protocol and gates. This file adds only rows and operations.

Not claimed in SP-011:

- that a cancel stopped a handler (cancellation is cooperative; §4.1);
- exactly-once beyond same-database effects, or any exactly-once for SQLite before SP-019;
- authorization or HTTP (SP-020);
- retention-backed payload guarantees (SP-021; ManualRetry only checks the `payload_retained` flag);
- that queue pause cancels or revokes reservations.

## 2. Trusted actor

```csharp
namespace Runwick.Management;

public sealed record ManagementActor(string Subject, string? Display, string Scheme);
```

- `Subject`: 1–256 UTF-8 bytes, no control characters. A stable principal identifier, such as `user:42` or `svc:deployer`.
- `Display`: optional, ≤ 256 UTF-8 bytes. Never used for decisions.
- `Scheme`: 1–64 ASCII bytes. Names how the host authenticated the actor (`host`, `oidc`, `cli`, …).
- `Reason` (on each request): optional, ≤ 512 UTF-8 bytes.

Bounds are validated before submission (`InvalidRequest` `invalid_request`, detail names the field). The actor is part of the command fingerprint, so a retried command with a different actor is `CommandConflict`.

## 3. Storage operations

```csharp
internal interface IJobControlStore
{
    Task<StoreReply<ControlledJob>> CancelAsync(StoreCommand<CancelRequest> command, CancellationToken ct);          // Cancel/1
    Task<StoreReply<ControlledJob>> ManualRetryAsync(StoreCommand<RetryRequest> command, CancellationToken ct);      // ManualRetry/1
    Task<StoreReply<ControlledJob>> RerunSucceededAsync(StoreCommand<RetryRequest> command, CancellationToken ct);   // RerunSucceeded/1
    Task<StoreReply<QueueState>> SetQueueStateAsync(StoreCommand<QueueStateRequest> command, CancellationToken ct);  // SetQueueState/1
    Task<StoreReply<ScopePolicy>> EditScopePolicyAsync(StoreCommand<ScopePolicyRequest> command, CancellationToken ct); // EditScopePolicy/1
    Task<EventPage> EventsSinceAsync(string application, EventCursor? after, int limit, CancellationToken ct);       // EventsSince/1
}

internal sealed record ControlActor(string Subject, string? Display, string Scheme, string? Reason);

internal sealed record CancelRequest(Guid JobId, int ExpectedGeneration, long ExpectedRevision, ControlActor Actor);
internal sealed record RetryRequest(Guid JobId, int ExpectedGeneration, long ExpectedRevision, ControlActor Actor);
internal sealed record QueueStateRequest(string Queue, bool Paused, long ExpectedRevision, ControlActor Actor);
internal sealed record ScopePolicyRequest(string ScopeKind, string ScopeKey, int Capacity, long ExpectedPolicyRevision, ControlActor Actor);

internal sealed record ControlledJob(JobPosition Position, string Effect, string? TerminalReason, DateTimeOffset? CancelRequestedAt);
internal sealed record QueueState(string Queue, bool Paused, long Revision);
internal sealed record ScopePolicy(Guid ScopeId, string ScopeKind, string ScopeKey, int Capacity, long PolicyRevision, int LivePermits);
```

`ControlledJob.Effect` is one of:

| Effect | Operation | Meaning |
|---|---|---|
| `cancelled` | Cancel | T17: waiting job closed, `cancelled_while_waiting` |
| `cancel_requested` | Cancel | T22: owned job; request flag recorded, nothing released |
| `retried` | ManualRetry | T18 |
| `rerun` | RerunSucceeded | T19 |

### 3.1 Common rules

1. Every control operation is receipt-bearing, in the `durable` class. NotAfter defaults to 5 min, the server cap is 10 min, and `retain_until = NotAfter + 24 h` (03 §8.4).
2. Gates are exactly 03 §10.1. Control operations are not creation paths for the capability check. Admission mode `drain` allows them; `maintenance` rejects them (`application_maintenance`).
3. Every mutation carries an expected position (`ExpectedGeneration` + `ExpectedRevision` for jobs, `ExpectedRevision` for queues, `ExpectedPolicyRevision` for scopes). A mismatch is `VersionConflict` `revision_conflict` with `StoreError.Detail = "actual=<generation>/<revision>"` (jobs) or `"actual=<revision>"`. It rolls back and leaves no receipt and no audit.
4. A successful mutation writes, in one transaction:
   - the effect;
   - one `management_audit` row per target;
   - one `queue_events` row per effect (§7);
   - the receipt.

   A replay returns the stored reply (`Replayed = true`) and writes nothing. A rejected (rolled back) or expired command writes neither an audit row nor a receipt, so the same command may be resent after the condition changes.
5. Lock order (04 §5), unchanged ranks: gates → receipt → queue control → jobs → dispatch keys → concurrency scopes. A control operation takes only the ranks it needs, in this order.
6. Decision time is the fresh `clock_timestamp()` read after the last lock (`DecideAsync`). A command whose NotAfter passes while waiting for locks never applies.

### 3.2 Cancel/1

Locks: job row (`FOR NO KEY UPDATE`), then the active-key row if the job is waiting and has one.

| Current state | Result |
|---|---|
| not found | `NotFound` `job_not_found` |
| expected position differs | `VersionConflict` `revision_conflict` |
| `succeeded` / `failed` / `cancelled` | `InvalidState` `job_terminal` |
| `pending` / `retrying` (T17) | `state := cancelled`, `terminal_reason := cancelled_while_waiting`, `terminal_at := now`, `cancel_requested_*` set, revision + 1, active key cleared if it still names this job (03 §5.6), event `job_cancelled`. Effect `cancelled` |
| `reserved` / `running` with no request yet (T22) | `cancel_requested_at := now`, `cancel_reason`, `cancel_requested_by := actor.Subject`, revision + 1, event `job_cancel_requested`. Lease, permits and attempt are untouched. Effect `cancel_requested` |
| `reserved` / `running` with a request already recorded | `InvalidState` `cancel_already_requested` (no write) |

After T22 the existing precedence tables decide (03 §6.4–§6.5): Begin closes the attempt `cancelled_before_start`; Release closes it `cancelled`; Finish/Recover apply `cancellation_requested` to unsuccessful outcomes. Renew reports `CancellationRequested = true`, and the worker cancels the handler's token cooperatively (05 §11.3). Nothing in Cancel/1 assumes the handler stopped.

### 3.3 ManualRetry/1 and RerunSucceeded/1

Locks: job row, then the active-key row (if the job had an active key), then the payload row read.

| Check (in order) | Failure |
|---|---|
| job exists | `NotFound` `job_not_found` |
| expected position | `VersionConflict` `revision_conflict` |
| ManualRetry: state ∈ {`failed`, `cancelled`}; Rerun: state = `succeeded` | `InvalidState` `invalid_state` |
| `payload_retained` and the payload row exists | `HistoryUnavailable` `payload_unavailable` |
| queue registered | `InvalidRequest` `queue_not_registered` |
| active key (if any) unbound or still bound to this job | `UniquenessConflict` `uniqueness_conflict` (`ConflictingJobId` = holder) |

Effect (T18/T19):

- insert `job_generations` row `generation + 1`, `created_reason` = `manual_retry` / `rerun_succeeded`, `created_by` = actor subject, policy copied from the previous generation, `started_count = 0`, `consecutive_unstarted_count = 0`;
- `jobs`: `state := pending`, `generation + 1`, `revision + 1`, `eligible_at := now`, `terminal_at`, `terminal_reason` and `cancel_requested_*` cleared;
- the active key reacquired: bound to this job;
- event `job_retried` / `job_rerun`.

Lifetime counters (`reservation_count`, `execution_count`), old generations and attempts are untouched (03 §5.3). Scope requirements are unchanged; they belong to the job, not the generation.

### 3.4 SetQueueState/1 (public path)

The harness-only internal path from 04 §7.14 becomes the public operation, with the same statements plus audit and event:

- Locks: the `queue_controls` row `FOR UPDATE`.
- Missing row: `NotFound` `queue_not_registered`.
- Revision mismatch: `VersionConflict` `revision_conflict`.
- Setting the current value again: `InvalidState` `no_change` (no write, no audit).
- Otherwise: `paused`, `revision + 1`, `updated_by := actor.Subject`, `updated_at := now`, event `queue_paused` / `queue_resumed`.

Pause blocks Reserve only. Dispatch, Begin, Renew, Finish, Recover and ReleaseReservation are unaffected, and existing reservations are not cancelled (ADM-04).

### 3.5 EditScopePolicy/1

Edits a registered scope's **capacity** only. Membership — which jobs require which scope — is fixed at dispatch. There is no request shape that edits it, and a request naming an unregistered scope is `NotFound` `scope_not_registered`.

- Locks: the `concurrency_scopes` row `FOR UPDATE` (natural key `(scope_kind, scope_key)`).
- Bounds: capacity 0–10,000 (`InvalidRequest`).
- Revision mismatch: `VersionConflict` `revision_conflict`.
- Same capacity: `InvalidState` `no_change`.
- Effect: `capacity := new`, `policy_revision + 1`, `updated_at := now`, event `scope_policy_edited` with `{scope, kind, key, capacity, previous}`.
- A decrease **never revokes** a live permit (CTL-01). Admission stays blocked until the live count is below the new capacity. The reply reports the live permit count observed under the lock.

The `RegisterScope/1` startup seed does not overwrite an edited capacity: its existing first-writer rule keeps the stored value and reports `configuredCapacity` separately (04 §7.3).

## 4. Engine behavior for controls

### 4.1 Cancellation of an owned attempt

A Renew reply with `CancellationRequested = true` sets the attempt's durable-cancellation flag and adds `JobCancellationCause.UserRequested` (05 §11.3). The per-attempt arbiter serializes it with Begin/Finish/Release/Recover. The attempt is closed by whichever of those runs next, using the 03 §6 precedence. A cancel request on a job whose handler ignores its token has no further effect until the handler returns or the lease is lost.

### 4.2 Pause

The engine treats `QueuePaused` from Reserve as a cooldown for that lane (05 §8). It never releases or cancels work it already owns.

## 5. Management audit

```sql
CREATE TABLE runwick.management_audit (
    application         runwick.rw_identifier NOT NULL,
    audit_id            bigint GENERATED ALWAYS AS IDENTITY,
    command_id          uuid        NOT NULL,
    operation           text COLLATE "C" NOT NULL,
    target_kind         text COLLATE "C" NOT NULL,     -- job | queue | scope
    target_id           text        NOT NULL,          -- job UUID, queue name, scope "kind/key"
    actor_subject       text        NOT NULL,
    actor_display       text,
    actor_scheme        text COLLATE "C" NOT NULL,
    reason              text,
    expected_revision   bigint,
    resulting_revision  bigint,
    details             jsonb       NOT NULL DEFAULT '{}'::jsonb,
    occurred_at         timestamptz NOT NULL,
    PRIMARY KEY (application, audit_id),
    UNIQUE (application, command_id, target_kind, target_id)
);
```

- Written only by a successful mutation, in the same transaction as its effect and receipt.
- The unique constraint makes a second audit row for one command impossible, even if the code were wrong.
- Retention follows the administrative policy of SP-021. M3 never deletes audit rows.
- Audit rows hold identifiers and decision metadata, never payload bytes or error text.

## 6. Receipt payloads (codec v1 additions)

| Operation | `result` members |
|---|---|
| Cancel, ManualRetry, RerunSucceeded | `jobId`, `generation`, `revision`, `state`, `effect`, `terminalReason`?, `cancelRequestedAt`? |
| SetQueueState | `queue`, `paused`, `revision` |
| EditScopePolicy | `scopeId`, `kind`, `key`, `capacity`, `policyRevision`, `livePermits` |

Command fingerprint bodies (02 §6 canonical form, operation version 1):

| Operation | Body members |
|---|---|
| Cancel / ManualRetry / RerunSucceeded | `actor` {`display`?, `reason`?, `scheme`, `subject`}, `expectedGeneration`, `expectedRevision`, `jobId` |
| SetQueueState | `actor`, `expectedRevision`, `paused`, `queue` |
| EditScopePolicy | `actor`, `capacity`, `expectedPolicyRevision`, `key`, `kind` |

The M2 harness `SetQueueState` body had `{expectedRevision, paused, queue}`. The public body adds `actor`. The harness path stays for M2 tests and now writes `updated_by = 'harness'` and no audit, as before.

## 7. Events

### 7.1 Schema

`queue_events` gains:

- `schema_version smallint NOT NULL DEFAULT 1`;
- `tx_id xid8 NOT NULL DEFAULT pg_current_xact_id()`.

The index `events_cursor (application, tx_id, event_id)` serves the cursor. Rows already present when 0002 runs all receive the migration's transaction ID.

Event kinds after 0002 are the M2 kinds plus `job_cancelled`, `job_cancel_requested`, `job_retried`, `job_rerun`, `queue_paused`, `queue_resumed` and `scope_policy_edited`. Control events carry `actor_id = actor.Subject` and `command_id`.

Version 1 event record (public shape in SP-020; internal in M3):

```csharp
internal sealed record JobEvent(
    long EventId, ulong TxId, int SchemaVersion, string Kind, Guid? JobId, Guid? AttemptId, Guid? CommandId,
    DateTimeOffset OccurredAt, string? ActorId, JsonObject Details);

internal readonly record struct EventCursor(ulong TxId, long EventId);
internal sealed record EventPage(IReadOnlyList<JobEvent> Events, EventCursor? Next, bool Partial);
```

### 7.2 EventsSince/1

A read-only query (no receipt), with gates as a bounded query:

```sql
SELECT ... FROM runwick.queue_events
WHERE application = @application
  AND (tx_id, event_id) > (@tx, @id)
  AND tx_id < pg_snapshot_xmin(pg_current_snapshot())
ORDER BY tx_id, event_id
LIMIT @limit
```

- `limit` is 1–1,000 (default 100).
- `Next` is the last returned row's cursor, or `after` unchanged when the page is empty.
- `Partial` is true when rows exist beyond the horizon, i.e. committed later than the snapshot's xmin. The caller polls again later.

Guarantees:

- **EVT-01 (stability).** Re-reading from one cursor returns the same prefix of rows. A row committed after a reader advanced past its position is impossible: `tx_id` values of in-progress transactions are ≥ `xmin`, and those rows are never returned until their transaction ends.
- **EVT-02 (bounded under appends).** A page never exceeds `limit` and never blocks on writers.

Latency is bounded by the oldest transaction still running anywhere in the PostgreSQL cluster: transaction IDs and snapshot horizons are cluster-wide, so a long transaction in another database delays visibility, never order. The order is commit-safe but not wall-clock order. Retention deletion (SP-021) may remove rows before a slow reader reaches them. A cursor older than retention is reported by SP-021, not here.

## 8. Transactional completion (R6)

Normative for the engine path; the public shape is 01 §16.

### 8.1 Staging

`IPostgresJobCompletion` is a scoped service. Outside an attempt scope, or once the attempt's pipeline has ended, `CompleteInTransactionAsync` throws `JobCompletionException` (`no_active_attempt`). A second call in one attempt throws `JobCompletionException` (`completion_already_staged`). A staging that was rejected for a reason other than ownership loss may be retried.

Staging steps:

1. Validate the transaction as for staged dispatch (01 §15): open, not read-only, the same store identity → `TransactionValidationException`.
2. Freeze `FinishRequest(lease, Succeeded, elapsed = now − executionStart)` and a new Finish command ID (ownership lifetime, 30 s NotAfter).
3. Pause renewal for the attempt: the caller's transaction now holds the job row lock until it ends.
4. Run Finish/1 inside a savepoint on the caller's transaction (`PgRunner.StageAsync`): same gates, receipt, ownership checks and lock order as Finish, after the caller's own locks.
5. Map the reply:
   - `Applied` (fresh or replayed): the entry records the staged command. Return `StagedCompletion(jobId, attemptId, commandId)`.
   - `StaleLease` / `InvalidState`: throw `JobOwnershipLostException` (`ownership_lost`). The caller must roll back. Renewal stays paused, because ownership is gone.
   - Anything else: throw `TransactionValidationException` with the store code, and resume renewal.

Runwick never commits, rolls back or retries the caller's transaction.

### 8.2 Observation

When the pipeline ends, an entry with a staged command does **not** publish its own Finish until it has looked for the staged one:

1. **Middleware form.** A `Completed(token)` result must carry this attempt's `AttemptId` and the staged command ID. Otherwise the result is a pipeline contract fault (PermanentFailure, normal path).
2. **Caller's transaction still open** (its connection is still attached) → the staged completion is not committed. The normal path then publishes `PermanentFailure` `staged_completion_not_committed`. That Finish waits on the caller's row lock and settles whichever way the caller ends (step 4).
3. **Caller's transaction ended:**
   - Resolve the staged command with `ResolveCommand` (receipt lookup on the primary).
   - `Recorded` (Applied) → the attempt is settled as succeeded. No Finish is sent. A pipeline exception after the commit is kept as a diagnostic only (TXC-06).
   - `NotObserved` / `NotApplied` → the commit did not happen. If the pipeline returned normally, or returned `Completed`, publish `PermanentFailure` `staged_completion_not_committed` (TXC-05). If it threw, classify the exception normally.
4. **Single finalization.** If the caller's COMMIT reply was lost but the commit actually succeeded (TXC-03), the normal-path Finish of step 2 or 3 finds the attempt already closed: `StaleLease`/`InvalidState`, and the stored outcome is kept. Storage closes an attempt exactly once, so the normal path and the staged completion can never both finalize. The handler is never rerun.

TXC-01: if the caller rolls back and the process dies before the normal path settles, the attempt stays owned until its lease expires, and Recover applies the usual precedence.

## 9. Five deployments (MODE-01 … MODE-05)

All five use one PostgreSQL store and a schema at revision 2. Roles are independently configured processes:

| Case | Shape |
|---|---|
| MODE-01 | Single process: producer + worker |
| MODE-02 | Producer-only process + separate worker process(es) |
| MODE-03 | Worker-only processes on disjoint queues |
| MODE-04 | Control-only process (preview services, no worker) + worker process |
| MODE-05 | Producer + staged-completion handler in a worker process + control process, all concurrent |

Each case proves:

- every role starts against the shared store;
- a job dispatched by one role is processed by another;
- a control issued by the control role is observed through the events cursor.

The scheduler and outbox roles join in SP-016/017.

## 10. Preview control services

`Runwick.Management` (namespace in the core assembly). These are preview APIs: documented as such, and their shapes may change before SP-020. [choice] No `[Experimental]` attribute in M3, so hosts and tests need no diagnostic suppression.

```csharp
public interface IJobControl
{
    Task<JobControlResult> CancelAsync(JobTarget target, ManagementActor actor, string? reason = null, CancellationToken ct = default);
    Task<JobControlResult> RetryAsync(JobTarget target, ManagementActor actor, string? reason = null, CancellationToken ct = default);
    Task<JobControlResult> RerunAsync(JobTarget target, ManagementActor actor, string? reason = null, CancellationToken ct = default);
    Task<IReadOnlyList<JobControlResult>> CancelManyAsync(IReadOnlyList<JobTarget> targets, ManagementActor actor, string? reason = null, CancellationToken ct = default);
}

public interface IQueueControl
{
    Task<QueueControlResult> PauseAsync(string queue, long expectedRevision, ManagementActor actor, string? reason = null, CancellationToken ct = default);
    Task<QueueControlResult> ResumeAsync(string queue, long expectedRevision, ManagementActor actor, string? reason = null, CancellationToken ct = default);
}

public interface ICapacityControl
{
    Task<CapacityControlResult> SetScopeCapacityAsync(string scopeKind, string scopeKey, int capacity, long expectedPolicyRevision,
        ManagementActor actor, string? reason = null, CancellationToken ct = default);
}

public readonly record struct JobTarget(JobId JobId, int ExpectedGeneration, long ExpectedRevision);
```

Results are value records:

| Field | Content |
|---|---|
| `Succeeded` | Whether the control applied |
| `Code` | Stable code on failure (03 §9.3 plus `cancel_already_requested`, `no_change`, `scope_not_registered`) |
| Position | The new generation/revision/state |
| `Replayed` | Whether the reply came from a stored receipt |

Rules:

- A rejection is a result, not an exception.
- Store unavailability and an unknown outcome throw the existing exceptions. An unknown outcome is resolved by resending the same command internally until the budget ends.
- `IScheduleControl` arrives with SP-016.
- No HTTP and no authorization (SP-020).
