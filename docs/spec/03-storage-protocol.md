# 03 — Storage command protocol

**Sources:** docs/architecture/storage-command-protocol.md (primary), docs/uniqueness-and-execution.md, docs/review-2026-10-30.md (R1, R2, R3, batch dispatch, receipt-shape and job-ID notes), docs/architecture/execution-defaults-and-timing.md, docs/architecture/management-retention-defaults.md (command lifetimes and receipt floors), docs/storage-design-inputs.md, docs/architecture/postgres-schema-and-operations.md §1/§4 (version axes, command catalog, reply envelope), docs/architecture/postgres-storage-design.md §5–§8 (accounting effects), docs/architecture/capacity-control.md §4 (rate-aware Begin, recorded as not in M2), docs/architecture/worker-engine-internals.md §5 (readiness interface), docs/architecture/postgres-upgrades-and-compatibility.md §1 (gate semantics).

This file defines the driver-neutral storage contract: internal records and interfaces, the operation catalog, the seven-state job machine and its counters, command receipts and replay, failure classification and stable codes. It is binding on every driver. [04-postgres.md](04-postgres.md) implements it for PostgreSQL. Byte-level encodings (command fingerprint, canonical values, dispatch digests) are defined in [02-serialization-and-wire.md](02-serialization-and-wire.md); this file references them and does not restate them.

Scope is M1 + M2 (PostgreSQL execution through gate G-1). Items marked **not in M2** are named only so that interfaces and records leave room for them; an M2 driver rejects them (§11).

## Changes (2026-11-02, PF-06 batched finish)

Performance plan PF-06 adds `FinishMany/1`, a batched Finish. `Finish/1` is unchanged and stays the per-attempt operation; a driver without the new operation is used through it.

- **§4 catalog row:** `FinishMany/1` | `IFinishManyStore.FinishManyAsync` (optional interface) | ownership mutation, **no receipt of its own** (each item has its own) | — | 30 s / 60 s | PF-06 batched finish (store manifest from schema revision 11, 04 Changes 2026-11-02, PF-06 batched finish).
- **Request:** `FinishManyRequest(IReadOnlyList<StoreCommand<FinishRequest>> Items)`: 1 to 32 (`IFinishManyStore.MaxItems`) complete Finish/1 commands of the call's application, each with its own command ID, NotAfter and body, with distinct command IDs and distinct job IDs. Anything else, or any item whose body Finish/1 would refuse as `InvalidRequest`, makes the call `InvalidRequest` `invalid_request` with no effect. The call has its own command ID (correlation only, like `RenewMany`) and NotAfter (the call's deadline; a call decided at or after it is `CommandExpired` for every item, no effect).
- **Semantics: each item is exactly a Finish/1.** The item's fingerprint is the Finish/1 fingerprint of that command (operation `Finish`). The item writes, when it applies, a durable `Finish` receipt keyed by its own command ID (§8.1, §8.4: durable class, NotAfter + 24 h), with the Finish result of §8.5. It runs §8.2 for that command: an existing receipt replays (`Replayed = true`, the stored disposition and retry delay, nothing redrawn), a changed body is `CommandConflict`, `CommandExpired` and `deadline_too_far` are checked against the item's own NotAfter; then the ownership check (§6.6), the precedence table (§6.4) and the retry decision (§6.2), all at one decision time `now` for the call, read once after the last lock. An item that is refused (ownership gone, expired, conflict) has no effect and leaves no receipt, as Finish/1 (§8.1).
- **Outcomes:** `FinishedMany(Outcomes)`: one `StoreReply<FinishedAttempt>` per item, in request order, with exactly the status, effect, replay flag, value, code and decision time Finish/1 would give that command. A refused or replayed item never changes another item's outcome.
- **Reply status.** `Applied` when every item was decided: the transaction commits when at least one item applied now, and rolls back when every item was refused or replayed (nothing to write). Any broken invariant in any item (`InvariantFault`) rolls the whole call back; so does any failure of the call. Every other status (gates, `CommandExpired` for the call, `InvalidRequest`) applies nothing.
- **Why per-item receipts, unlike `ReserveAndBegin`.** A Finish already has its command ID and frozen body before it is batched, and the engine must keep resolving an unknown Finish by resubmitting that same command (§8.5 replay rules, 05 §12). With one receipt per item, a single `Finish/1` resubmission of an item replays what the batch recorded for it, and a batch that contains an already-recorded command replays it. A batch receipt would need a second key per item and would tie each Finish's resolution to the batch.
- **Lost reply.** If the reply of a `FinishMany` is lost (outcome unknown, or not applied), each item is resolved alone by resubmitting its own `Finish/1` command, within that Finish's own budget: an item that committed replays its receipt; an item that did not apply is decided now (or is `CommandExpired` after its NotAfter, definitively not applied within the receipt floor). `ResolveCommandAsync` on an item's command ID answers as for any Finish. The call's own command ID has no receipt and resolves as `NotObserved`/`NotApplied`, never as the items.
- **§10.2:** allowed in `open` and `drain`, like Finish.

## Changes (2026-11-02, PF-06 batched claim)

Performance plan PF-06 adds `ReserveAndBegin/1`, the batched claim that §11 left room for. `Reserve/1` and `Begin/1` are unchanged and stay the per-job operations; a driver without the new operation is used through them.

- **§4 catalog row:** `ReserveAndBegin/1` | `IReserveAndBeginStore.ReserveAndBeginAsync` (optional interface) | ownership mutation | ownership | 30 s / 60 s | PF-06 batched claim (store manifest from schema revision 10, 04 Changes 2026-11-02, PF-06 batched claim).
- **Request:** `ReserveAndBeginRequest(WorkerInstanceId, Queue, Priority, SupportedContracts, After, LeaseDuration, MaxJobs)`: Reserve/1's request plus `MaxJobs`, 1 to 32 (`IReserveAndBeginStore.MaxJobs`). The caller never asks for more jobs than it has free local slots (05 Changes 2026-11-02, PF-06 batched claim). Out of bounds is `InvalidRequest` `invalid_request` with no effect. The fingerprint covers Reserve/1's members plus `maxJobs`.
- **Semantics.** One transaction claims up to `MaxJobs` candidates of the lane, in candidate order (`eligible_at`, `job_id`, after the cursor), and for each claimed job runs exactly Reserve/1's decision and then Begin/1's decision, all at one decision time `now`, read once after the last lock (§1.3). Each job's outcome is the one Reserve/1 then Begin/1 would give it at that time, given the jobs before it in the same command: capacity is counted with their permits, rate windows with their grants. Fencing is per job (each permit gets its own fencing value). The ownership token and the defer jitter are drawn per job.
- **Outcomes, one per claimed job, in candidate order:** `ClaimOutcome(JobId, Status, Error, Reserved, Begun, Closed, Deferred)`:
  - `Applied`: reserved and begun; `Reserved` is the `ReservedJob` (with payload), `Begun` the `BegunExecution`.
  - `CapacityBlocked` `capacity_blocked`: not reserved, nothing written for it; `Error.BlockedScopeId` and `Error.ResumeAfter` as Reserve/1's.
  - `RateLimited` `rate_limited`: reserved and then deferred (T20, 10 §4): attempt closed `admission_deferred`, no grant charged; `Deferred` as Begin/1's.
  - `InvalidState` `cancelled_before_start` or `BudgetExhausted`: reserved and closed by Begin (T7/T8); `Closed` as Begin/1's.
  A job that is blocked, deferred or closed never changes another job's outcome.
- **Reply status.** `Applied` (with `ClaimedJobs(Outcomes)`) when at least one job was reserved. When no candidate exists, the reply is `NoWork`; a paused queue `QueuePaused`; when every candidate was capacity-blocked, `CapacityBlocked` with the first blocked scope and `ResumeAfter` = the last candidate's cursor, no effect, as Reserve/1. Any broken invariant in any job (`InvariantFault`) rolls the whole command back. Every other status is as Reserve/1 (gates, `CommandExpired`, `CommandConflict`).
- **Receipt: one command receipt, ownership class (§8.1, §8.4).** The command writes one receipt whose `result` holds every outcome, not one receipt per job. Reason: the receipt model is one row per command ID (§8.2), and a resubmission must return exactly the reply the first execution gave (§8.6). A receipt per job would need N command IDs, or a second key, and a lost reply could then be resolved only partly. `DispatchMany` is the precedent: one receipt, `items[]`. The class is ownership (NotAfter cap 60 s, retained NotAfter + 10 min): the receipt carries leases and tokens that are worth nothing after the lease, exactly like Reserve and Begin.
- **§8.5 row** `ReserveAndBegin`: `items`: [{`ordinal`, `jobId`, `status`, `code`?, `reserve`? (the Reserve row's members), `begin`? (the Begin row's members for that status), `blockedScopeId`?, `cursor`?}]. `CapacityBlocked` items have `blockedScopeId` and `cursor` and no `reserve`.
- **Replay.** A resubmitted command returns the recorded outcomes with `Replayed = true`, rebuilding each begun job's `ReservedJob` from its receipt item plus the immutable payload row, as Reserve/1. A replayed lease is historical: the worker confirms each begun job with a fresh ownership-checked Renew before running it (§8.5 replay rules).
- **§10.2:** allowed in `open` only, like Reserve (a creation for the drain gate).
- **§11 row** "Batched Reserve/Renew, `ReserveAndBegin/1`": `ReserveAndBegin/1` is now specified; a batched Reserve without Begin stays reserved.

## Changes (2026-11-02, PF-05)

No protocol change. The PostgreSQL driver runs the hot operations as server functions (04 Changes 2026-11-02, PF-05); every operation keeps its version, request, reply, codes, receipt shape (codec v1) and replay rule. Two clarifications that hold for every driver:

- **Decision time of an early rejection.** A rejection decided before the fresh clock read (gates, receipt conflict, `queue_not_registered`, Recover's `job_not_found` and `stale_lease`, dispatch validation bounds) carries no decision time, as before; the PostgreSQL driver reports it as the unset instant.
- **Values a driver may draw in storage.** Attempt IDs (UUIDv7 of the decision time, §2) and new scope IDs may be generated by the store; entropy for the retry draw (§6.2) and the defer jitter may be drawn by the client and sent with the command. Neither is part of the fingerprint.

## Changes (2026-11-02, PF-06)

Performance plan PF-06 adds `RenewMany/1`, the batched Renew that §3.3 left room for ("a batched Renew must report independent per-lease outcomes and a stale member must not roll back healthy ones"). `Renew/1` is unchanged and stays the per-attempt operation.

- **§4 catalog row:** `RenewMany/1` | `ILeaseBatchStore.RenewManyAsync` (optional interface; a driver without it is used through `Renew/1`) | ownership mutation, **no receipt** | — | 30 s / 60 s | PF-06 (store manifest from schema revision 8, 04 Changes 2026-11-02).
- **Request:** `RenewManyRequest(IReadOnlyList<LeaseHandle> Leases, TimeSpan LeaseDuration)`: 1 to 64 leases with distinct job IDs, one `LeaseDuration` within Renew/1's bounds. Anything else is `InvalidRequest` `invalid_request` with no effect.
- **Reply:** one command ID and one NotAfter, as Renew/1 (R1: no fingerprint, no receipt row; a lost reply is resolved by a new command, never by resubmission). The reply status is `Applied` when the transaction committed, with `RenewedLeases(Outcomes)`: one `LeaseRenewal(Status, Code, Value)` per requested lease, in request order. Each member's status and code are exactly those Renew/1 would give for that lease at the same decision time (`Applied` with a `RenewedLease`; `StaleLease`, `InvalidState`, `NotFound`; `InvariantFault` `permit_set_mismatch`). A refused member has no effect, and no member's refusal rolls back another member. Every other reply status (`CommandExpired`, `AdmissionClosed`, `UnsupportedVersion`, `InvalidRequest`, a transient failure, or `InvariantFault` for the batch) means no lease was renewed.
- **Semantics per member are §7's Renew row:** the exact current unexpired lease in `reserved`/`running`, ownership token, worker, attempt and generation checked against the locked job row (the fence); job and exact permit set extended to the same expiry; job revision unchanged (I-4, T21); no `queue_events` row. All members share one decision time `now` (fresh, after the last lock), and therefore one expiry.
- **§10.2:** `RenewMany` is allowed in `open` and `drain` like Renew.
- **§11 row** "Batched Reserve/Renew": batched Renew is now `RenewMany/1`; batched Reserve and `ReserveAndBegin/1` remain reserved.

## Changes (2026-10-31, SP-016)

- §4: the eight schedule operations (13 §6) and the due read. `RegisterSchedule`, `MaterializeOccurrences` and `TriggerSchedule` are creation paths for the gate and need capability `scheduler`.
- §8.4: `RegisterSchedule` 5 min / 10 min; every other schedule operation 30 s / 60 s; all `durable`.

## Changes (2026-10-31, SP-014)

- §4: `SetPoolOverride/1` (administrative, 5 min / 10 min, durable; 11 §5). `EstimateDemand` (query) and `RecordScalingEvent` (no receipt) are worker-internal, like `PublishPresence`.

## Changes (2026-10-31, SP-012)

- §3.5: `ReadySnapshot` gains `Queues` (saturation, control revision) and `UnsupportedLanes` (09 §2); `ReadinessProbe/2` is a query with no receipt. New internal `IReadinessDiagnostics` with `ListUnsupportedBacklog/1` (09 §5).
- `PublishPresence/1` also writes the instance's queue subscriptions (09 §3.1). Its contract is otherwise unchanged.

## Changes (2026-10-31, SP-017)

- `ReceiveOutbox/1` (14 §5): durable receipts, deadline cap 30 s, a creation path for the gate, capability `outbox_destination`. It checks a durable delivery identity before ordinary dispatch and stores the job mapping with the receipt. After its transport command expires, a new command is permitted only under the same delivery identity and AcceptUntil (14 §6.2); ordinary Dispatch keeps the rule that an expired unknown operation is never replaced.

## Changes (2026-10-31, SP-013)

- §3.1: `DispatchItem.Rates` carries the seed policy of each rate scope in `Intent.RateScopes`, outside the fingerprint (10 §3.1). `BeginReply.Deferred` reports a committed deferral.
- §3.4: new internal `IRateControlStore` with `ActivateCapabilities/1` and `EditRatePolicy/1` (10 §6). The reserved name `UpdateRatePolicy/1` becomes `EditRatePolicy/1`.
- §4 status table: `RateLimited` / `Applied` is in use (`EligibleAt` epoch ms in Detail).
- §5.2: T20 loses "not in M2". `admission_deferred` is in use.
- §6.3 is replaced by 10 §4. Lock rank "rate scopes" follows concurrency scopes.
- §8.4: `EditRatePolicy` and `ActivateCapabilities` are `durable` class, 5 min default NotAfter, 10 min cap.
- §10.3: an SP-013 binary advertises `rate_admission: 1`. The application requires it only after `ActivateCapabilities/1`. Until then Dispatch with rate scopes is `UnsupportedVersion` `rate_admission_not_active`.

## Changes (2026-10-31, SP-011)

- §3.4: the reserved `RequestCancellationAsync`/`RetryAsync` are replaced by the separate internal `IJobControlStore` (08 §3). `CancelRequest`/`RetryRequest` carry a `ControlActor` instead of `ActorId`/`Reason`.
- §5.2: T17, T18, T19 and T22 lose "not in M2" (08 §3.2–3.3). T20 stays reserved for SP-013.
- §8.4: Cancel, ManualRetry, RerunSucceeded, SetQueueState and EditScopePolicy are `durable` class, 5 min default NotAfter, 10 min cap (unchanged class table).
- §8.5: receipt members for the control operations are in 08 §6.
- §9.3 new codes: `cancel_already_requested`, `no_change`, `scope_not_registered`. `revision_conflict` is now used.
- §10.1: the writable schema revision is the binary's own (2 for SP-011 builds).
- §10.2: control operations and EventsSince are allowed in `drain`.
- §11 now lists only what is still reserved after SP-011.

## Changes (2026-10-30)

### Contradictions resolved

| # | Conflict | Resolution |
|---|---|---|
| C-1 | uniqueness-and-execution.md: five states, `MaxAttempts = 5` counted at reservation, 10 s / 5 min backoff. | Superseded. Seven states (§5), execution counted at Begin, MaxExecutions 3, retry 5 s / 60 s, unstarted delay 1 s / 30 s, separate pre-start abandonment guard 5 (§6, §7). |
| C-2 | runwick-explained.md: recovery of a reserved job returns it to `pending`. | Recovery (and ReleaseReservation) restores `pending` only when the generation's `started_count = 0`, otherwise `retrying` (§5.2 T6, T7). |
| C-3 | execution-defaults-and-timing.md lists receipt floors and OwnershipCommandLifetime for Reserve/Begin/Renew/Finish/Recover only; management-retention-defaults.md includes ReleaseReservation. | ReleaseReservation is an ownership command: 30 s lifetime, `ownership` receipt class (NotAfter + 10 min) (§8.4). |
| C-4 | storage-command-protocol.md §4 "every mutating command has a receipt" vs R1. | Renew has a command ID and NotAfter but **no receipt** (R1). Reserve, Begin, Finish, Recover, ReleaseReservation keep receipts (§8.1). |
| C-5 | Earlier single request fingerprint. | R3 business + policy digests; a policy-only difference is `Reused` with `PolicyDrift`, never a conflict (§4.2; 02 §7). |
| C-6 | Capability-set bound never given (postgres-storage-design §11, storage-design-inputs §2). | At most **512** distinct decoding tuples per Reserve and per readiness probe; 513 is `InvalidRequest` before any write. |
| C-7 | capacity-control §4 makes Begin rate-aware; postgres-schema-and-operations §1 says an execution-only milestone may not accept rate-enabled applications. | Begin/1 is defined with rate admission as a reserved step. **Rate-aware Begin is not in M2; an M2 worker refuses rate-enabled applications** (application capability `rate_admission` is unknown to M2 binaries, §10.3). |
| C-8 | storage-command-protocol.md §5 says Begin "consumes an execution" and the storage-design draft also counts reservations. | Both counters exist with distinct meanings: lifetime `reservation_count` (every Reserve) and per-generation `started_count` (every Begin). Only `started_count` is the execution budget (§6). |
| C-9 | job_attempts.outcome vocabulary (postgres-storage-design §2) has no value for a reservation closed because cancellation was found at Begin. | Added attempt outcome `cancelled_before_start` (§5.4). The source's InvalidState code `cancelled_before_start` is kept as the reply code and job terminal reason. |
| C-10 | Scope kind spellings vary (`job-type`, `job_type`, `job`). | Persisted and wire scope kinds are `queue`, `job-type`, `resource` (matches 02 IC-06). |
| C-11 | The protocol's `FinishedAttempt(Position, AttemptOutcome, EligibleAt)` cannot report which disposition storage applied when cancellation or budget overrides the observed outcome. | `FinishedAttempt` carries the observed outcome, applied disposition and terminal reason separately (§3.6). |

### Implementation choices

| ID | Choice |
|---|---|
| I-1 | `StoreStatus` gains `InvalidRequest` (validation failed before any write) and `InvariantFault` (driver detected a broken invariant and rolled back). Both have `Effect = ObservedNoEffect`. |
| I-2 | Transport-level failures are two exception types, `StoreNotAppliedException` and `StoreOutcomeUnknownException`, never `StoreReply` values (§9). |
| I-3 | Interfaces: `IStoreHandshake` (describe/seed: `RegisterApplication/1`, `RegisterQueue/1`, `RegisterScope/1`), `IJobDispatchStore`, `IJobReservationStore`, `IWorkReadinessStore` (probe, presence, expired-lease listing), `IJobStateStore` (§3). |
| I-4 | Every lifecycle state change and every recorded cancellation request increments `jobs.revision`; Renew never does. |
| I-5 | Command-deadline caps measured against storage time at receipt acquisition: ownership operations NotAfter ≤ decision + 60 s; Dispatch/DispatchMany/Register*/administrative ≤ decision + 10 min (nominal lifetimes 30 s / 5 min; the 2× allowance absorbs clock skew). Larger → `InvalidRequest` `deadline_too_far`. |
| I-6 | Validation bounds: LeaseDuration 1 s–1 h; MaxExecutions 1–100; MaxConsecutiveUnstartedAbandonments 1–100; ExecutionTimeout 1 ms–30 days; retry InitialDelay 1 ms–24 h, MaximumDelay InitialDelay–24 h; ≤ 8 concurrency scopes per job; ≤ 384 lanes per probe; ≤ 512 capability tuples per Reserve/probe/presence revision; ListExpiredLeases limit 1–100. |
| I-7 | A queue must be registered (`RegisterQueue/1`, seed-if-absent) before Dispatch can route to it; dispatching to an unregistered queue is `InvalidRequest` `queue_not_registered`. Producers and workers register every queue named by their registry at startup. |
| I-8 | An application control row is seeded by `RegisterApplication/1` (seed-if-absent, `admission_mode = open`, no required capabilities). Seeds never update an existing row. |
| I-9 | Resource fencing is enabled for a scope iff it is created with `kind = resource` and `capacity = 1`; Reserve issues a fence for every fencing-enabled scope it grants. The flag is immutable in M2. |
| I-10 | Receipt result payloads use receipt codec v1 (JSON, camelCase, instants as epoch milliseconds, UUIDs lowercase) (§8.5). |
| I-11 | `ReceiptResolution` distinguishes `Recorded`, `NotObserved`, `NotApplied` (deadline passed, still inside the receipt-retention floor, no receipt) and `ExpiredUnknown` (§8.6). |
| I-12 | Observational queries added to the catalog: `ReadinessProbe/1`, `PublishPresence/1`, `ListExpiredLeases/1`, `ResolveCommand/1`, `Describe/1` (§4). |
| I-13 | Attempt records carry `outcome` (what happened to the attempt) and `disposition` (the job state storage applied) separately, plus `applied_reason` (§5.4). |
| I-14 | Retry/unstarted jitter entropy comes from `RandomNumberGenerator`; the chosen delay is computed once inside the deciding transaction and stored in the receipt and attempt row (§7). |
| I-15 | Job, attempt, command and scope IDs are `Guid.CreateVersion7()`; ownership tokens are 16 random bytes from `RandomNumberGenerator` formatted as a UUID. Lock and cursor ordering uses canonical big-endian UUID byte order (`UuidOrder`, §2.4), never `Guid.CompareTo`. |
| I-16 | (2026-10-30, SP-002 implementation) C# shapes adjusted without changing semantics: `DispatchItem` wraps the frozen `DispatchIntent` (02), its `ScopeRequirement`s and digests, and `DispatchRequest`/`DispatchBatchRequest` also carry the store ID and the precomputed command fingerprint; policy snapshots reuse the millisecond `ExecutionPolicy`/`RetryPolicy` records (same fields as §2.3); the internal fence record is `StoreFence` (the public `ResourceFence` already exists); Begin returns `BeginReply(StoreReply<BegunExecution>, BeginClosed?)` so the Applied InvalidState/BudgetExhausted closing value of §2.1 has a typed home; `IJobDispatchStore.ResolveOperationAsync(application, commandId, notAfterBound)` serves ID-only `ResolveAsync` (bound = UUIDv7 time + 10 min cap). |

---

## 1. Principles

1. **Store is authority.** Eligibility, lease validity, retry instants, command deadlines and receipt retention are decided with storage time. Worker clocks never authorize ownership.
2. **One call, one atomic semantic operation.** A driver call either commits all of its effects (and its receipt, when it has one) or none. Reservation never calls an independently committing permit acquisition; finalization releases permits in the same transaction.
3. **Fresh time after locks.** Every ownership or deadline decision reads storage time *after* every potentially blocking lock has been acquired. A transaction-start timestamp is never used for these decisions.
4. **Stale owners observe, never mutate.** Every ownership-checked operation compares the exact `LeaseHandle` (job, attempt, worker instance, ownership token, generation). A mismatch changes nothing.
5. **Receipts resolve ambiguity.** Every receipt-bearing command has a stable ID, immutable fingerprinted arguments and an absolute NotAfter. Its result is written in the same transaction as its effects. A lost reply is resolved by resubmitting the *same* command, never a new ID (Renew excepted, §8.3).
6. **At-least-once.** Replay is historical. No protocol rule claims exactly-once execution across crashes. Lease expiry can let a replacement run while an old handler still runs; external effects need idempotency or fencing.
7. **Never across user code.** No transaction or row lock is held across handler execution, payload decoding, DI activation, backoff sleeps or network calls to third parties.

## 2. Common records

These are internal (`internal sealed record`) types in `Runwick.Core`. They are not public NuGet API. Internal enums are used for closed engine decisions; persisted state codes, queue names and priorities are stable strings.

### 2.1 Envelope, reply and errors

```csharp
internal sealed record StoreCommand<T>(
    string Application,          // identifier grammar (02 §1)
    Guid CommandId,              // Guid.CreateVersion7()
    DateTimeOffset NotAfter,     // absolute deadline, floored to 1 ms
    T Body);

internal enum StoreStatus
{
    Applied, NoWork, QueuePaused, CapacityBlocked, RateLimited,
    StaleLease, InvalidState, BudgetExhausted,
    VersionConflict, UniquenessConflict, NotFound,
    CommandExpired, CommandConflict, UnsupportedVersion,
    AdmissionClosed, HistoryUnavailable,
    InvalidRequest, InvariantFault                      // I-1
}

internal enum StoreEffect { ObservedNoEffect, Applied }

internal sealed record StoreError(
    string Code,                          // stable code, §9.3
    long? ActualRevision = null,
    Guid? ConflictingJobId = null,
    Guid? BlockedScopeId = null,
    CandidateCursor? ResumeAfter = null,
    string? Detail = null);               // bounded (≤ 512 UTF-8 bytes), never payload or secrets

internal sealed record StoreReply<T>(
    StoreStatus Status,
    StoreEffect Effect,
    bool Replayed,
    T? Value,
    StoreError? Error,
    DateTimeOffset DecidedAt) where T : class;
```

`Replayed = true` means the reply is a recorded past result read from a receipt; `DecidedAt` is then the stored decision time. `Effect` distinguishes committed effects from an observational no-effect outcome.

Allowed `Status`/`Effect`/`Value` combinations (any other combination is a driver contract violation and the engine treats it as `InvariantFault`):

| Status | Effect | Receipt written | Value | Error |
|---|---|---|---|---|
| Applied | Applied | yes (except Renew) | required | null |
| NoWork, QueuePaused, CapacityBlocked | ObservedNoEffect | no | null | required (CapacityBlocked carries `BlockedScopeId`, `ResumeAfter`) |
| RateLimited | Applied | yes | null | required (`EligibleAt` in Detail) — **not in M2** |
| StaleLease | ObservedNoEffect | no | null | required |
| InvalidState | ObservedNoEffect, or Applied when storage closed the attempt (`cancelled_before_start`) | only when Applied | Applied: the closing value (§3.4, §3.6); else null | required |
| BudgetExhausted | Applied (Begin closed the attempt and failed the job) | yes | null | required |
| VersionConflict, UniquenessConflict, NotFound, CommandExpired, CommandConflict, UnsupportedVersion, AdmissionClosed, InvalidRequest, InvariantFault | ObservedNoEffect | no | null | required |
| HistoryUnavailable | ObservedNoEffect | no (replay of an existing receipt) | null | required |

An ObservedNoEffect reply commits nothing: the driver rolls its transaction back. Such outcomes may differ on a later retry of the same command.

### 2.2 Identity and position

```csharp
internal sealed record JobContract(
    string Name,                     // job-name grammar
    int PayloadVersion,              // ≥ 1
    string SerializerProfileId,      // identifier grammar
    int SerializerProfileVersion);   // ≥ 1

internal sealed record CandidateCursor(DateTimeOffset EligibleAt, Guid JobId);

internal sealed record LeaseHandle(
    Guid JobId,
    Guid AttemptId,
    Guid WorkerInstanceId,
    Guid OwnershipToken,
    int Generation);

internal sealed record LeaseGrant(LeaseHandle Handle, DateTimeOffset ExpiresAt);

internal sealed record ResourceFence(Guid ScopeId, long Value);

internal sealed record JobPosition(
    Guid JobId,
    int Generation,
    long Revision,
    string State);                   // §5.1 state code
```

A worker instance ID is unique to one process lifetime and never reused after restart. Ownership tokens are random compare-and-set identifiers, not ordered fencing counters.

### 2.3 Policy snapshots

```csharp
internal sealed record ExecutionPolicySnapshot(
    int MaxExecutions,                          // default 3
    int MaxConsecutiveUnstartedAbandonments,    // default 5
    TimeSpan? ExecutionTimeout,                 // default null (none)
    RetryPolicySnapshot Retry,                  // default exponential/1/5 s/60 s/equal
    RetryPolicySnapshot UnstartedRetry);        // default exponential/1/1 s/30 s/equal

internal sealed record RetryPolicySnapshot(
    string Backoff,              // "exponential"
    int EvaluatorVersion,        // 1  (evaluator "exponential-v1")
    TimeSpan InitialDelay,
    TimeSpan MaximumDelay,
    string JitterKind);          // "equal"

internal sealed record ScopeRequirement(
    string Kind,                 // "queue" | "job-type" | "resource"
    string Key,                  // queue name | job name | resource key (1–512 UTF-8 bytes)
    int Capacity);               // ≥ 0; used only to seed an absent scope
```

The policy snapshot is persisted on the generation at dispatch and never rewritten while the generation is current. Durations are whole milliseconds (02 IC-14).

### 2.4 Ordering and time

- **Time quantum** is 1 ms. NotBefore and computed eligibility instants round **up**; lease expiries, NotAfter and window expiries round **down**; stored decision instants round down. Equality with a lease or command expiry means **expired**.
- **UUID order** is canonical big-endian byte order (the RFC 4122 string order), implemented by `UuidOrder : IComparer<Guid>` over `Guid.TryWriteBytes(span, bigEndian: true, out _)`. It equals PostgreSQL `uuid` ordering. `Guid.CompareTo` is never used for lock sorting or cursors. ENC-01 compares `UuidOrder` against server `ORDER BY`.
- **Text order** for identities and keys is ordinal over UTF-8 bytes (`string.CompareOrdinal` is not equivalent for non-BMP text; compare UTF-8 byte spans). It equals PostgreSQL `COLLATE "C"`.
- **Candidate order** is `(eligible_at, job_id)` ascending under the two rules above.

## 3. Interfaces and request/result records

Each method is one atomic semantic operation. Driver-private helpers may be separate methods sharing one transaction.

### 3.1 IStoreHandshake

```csharp
internal interface IStoreHandshake
{
    Task<StoreDescriptor> DescribeAsync(string application, CancellationToken ct);

    Task<StoreReply<ApplicationSeedResult>> RegisterApplicationAsync(
        StoreCommand<RegisterApplicationRequest> command, CancellationToken ct);

    Task<StoreReply<QueueSeedResult>> RegisterQueuesAsync(
        StoreCommand<RegisterQueuesRequest> command, CancellationToken ct);

    Task<StoreReply<ScopeSeedResult>> RegisterScopesAsync(
        StoreCommand<RegisterScopesRequest> command, CancellationToken ct);
}

internal sealed record StoreDescriptor(
    Guid StoreId,
    long SchemaRevision,
    int ProtocolEpoch,
    string MigrationMode,                       // normal | expand | maintenance
    int CompatibilityManifestVersion,
    ReadOnlyMemory<byte> ManifestDigest,        // 32 bytes
    ApplicationGate? Application,               // null when no row exists yet
    DateTimeOffset ObservedAt);

internal sealed record ApplicationGate(
    long Revision,
    int RequiredProtocolEpoch,
    IReadOnlyDictionary<string, int> RequiredCapabilities,   // capability → minimum version
    string AdmissionMode);                                    // open | drain | maintenance

internal sealed record RegisterApplicationRequest();           // seed-if-absent; no fields
internal sealed record ApplicationSeedResult(bool Created, ApplicationGate Gate);

internal sealed record RegisterQueuesRequest(IReadOnlyList<string> Queues);   // 1–128, distinct
internal sealed record QueueSeedResult(IReadOnlyList<QueueSeed> Queues);
internal sealed record QueueSeed(string Queue, bool Created, bool Paused, long Revision);

internal sealed record RegisterScopesRequest(IReadOnlyList<ScopeRequirement> Scopes); // 1–256, distinct (Kind, Key)
internal sealed record ScopeSeedResult(IReadOnlyList<ScopeSeed> Scopes);
internal sealed record ScopeSeed(
    Guid ScopeId, string Kind, string Key,
    bool Created, int StoredCapacity, int ConfiguredCapacity,
    long PolicyRevision, bool FencingEnabled);    // StoredCapacity ≠ ConfiguredCapacity → drift diagnostic
```

Seeds never overwrite: an existing queue keeps its pause state, an existing scope keeps its capacity. A capacity mismatch is returned as drift and logged by the caller; it is never written. The worker records the returned `scope_id → (kind, key)` map for CapacityBlocked classification.

### 3.2 IJobDispatchStore

```csharp
internal interface IJobDispatchStore
{
    Task<StoreReply<DispatchResult>> DispatchAsync(
        StoreCommand<DispatchRequest> command, CancellationToken ct);         // Dispatch/1

    Task<StoreReply<DispatchBatchResult>> DispatchManyAsync(
        StoreCommand<DispatchBatchRequest> command, CancellationToken ct);    // DispatchMany/1

    Task<ReceiptResolution> ResolveCommandAsync(
        CommandIdentity command, CancellationToken ct);                       // ResolveCommand/1
}

internal sealed record DispatchItem(
    Guid ProposedJobId,                        // Guid.CreateVersion7(), assigned at Prepare
    JobContract Contract,
    byte[] PayloadUtf8,                        // original bytes, ≤ 1 MiB, encoding version 1
    string Queue,
    string Priority,                           // low | medium | high
    DateTimeOffset? NotBefore,                 // null = immediate; ms ceiling
    ExecutionPolicySnapshot Policy,
    IReadOnlyList<ScopeRequirement> Scopes,    // ≤ 8, distinct (Kind, Key)
    string? ActiveKey,
    string? WindowKey,
    TimeSpan? Window,                          // non-null iff WindowKey non-null
    byte[] BusinessDigest,                     // 32 bytes (02 §7.1)
    byte[] PolicyDigest,                       // 32 bytes (02 §7.2)
    int FingerprintVersion,                    // 1
    IReadOnlyDictionary<string, string> BusinessMetadata,
    IReadOnlyDictionary<string, string> DiagnosticMetadata);   // traceparent/tracestate only

internal sealed record DispatchRequest(DispatchItem Item);
internal sealed record DispatchBatchRequest(
    IReadOnlyList<DispatchItem> Items,         // 1–100, ordinals 0..n−1
    bool ContinueOnConflict);

internal enum DispatchDisposition { Created, Reused }
internal enum DuplicateMatch { None, Active, Window, ActiveAndWindow }

internal sealed record DispatchResult(
    Guid JobId,
    DispatchDisposition Disposition,
    DuplicateMatch Match,
    bool PolicyDrift);

internal sealed record DispatchBatchItemResult(
    int Ordinal,
    DispatchResult? Result,                    // Created/Reused
    StoreError? Failure);                      // only under ContinueOnConflict

internal sealed record DispatchBatchResult(IReadOnlyList<DispatchBatchItemResult> Items);

internal sealed record CommandIdentity(
    string Application,
    Guid CommandId,
    string Operation,
    int OperationVersion,
    byte[] Fingerprint,                        // 32-byte command digest (02 §8.3)
    DateTimeOffset NotAfter);
```

Caller-owned transaction staging (`StageAsync`/`StageManyAsync`) executes `Dispatch/1` / `DispatchMany/1` on the caller's transaction through a provider-specific interface (PostgreSQL: 04 §7.2). It never commits, rolls back, disposes or retries that transaction, and returns the same result shape without a durability claim.

### 3.3 IJobReservationStore

```csharp
internal interface IJobReservationStore
{
    Task<StoreReply<ReservedJob>> TryReserveAsync(
        StoreCommand<ReserveRequest> command, CancellationToken ct);       // Reserve/1

    Task<StoreReply<BegunExecution>> BeginExecutionAsync(
        StoreCommand<BeginRequest> command, CancellationToken ct);         // Begin/1

    Task<StoreReply<RenewedLease>> RenewAsync(
        StoreCommand<RenewRequest> command, CancellationToken ct);         // Renew/1, no receipt
}

internal sealed record ReserveRequest(
    Guid WorkerInstanceId,
    string Queue,
    string Priority,
    IReadOnlyList<JobContract> SupportedContracts,   // 1–512, deduplicated, sorted (02 IC-18)
    CandidateCursor? After,
    TimeSpan LeaseDuration);                          // 1 s–1 h, whole ms

internal sealed record ReservedJob(
    JobPosition Position,
    LeaseGrant Lease,
    JobContract Contract,
    CandidateCursor Cursor,
    long ReservationNumber,
    byte[] PayloadUtf8,
    IReadOnlyDictionary<string, string> BusinessMetadata,
    IReadOnlyDictionary<string, string> DiagnosticMetadata,
    string Queue,
    string Priority,
    ExecutionPolicySnapshot Policy,
    IReadOnlyList<ResourceFence> Fences);

internal sealed record BeginRequest(LeaseHandle Lease);

internal sealed record BegunExecution(
    JobPosition Position,
    LeaseGrant Lease,                 // the existing lease; Begin never extends it
    int ExecutionNumber,              // generation started_count after increment
    long TotalExecutionNumber,        // lifetime execution_count after increment
    DateTimeOffset AuthorizedAt);

internal sealed record RenewRequest(LeaseHandle Lease, TimeSpan LeaseDuration);

internal sealed record RenewedLease(LeaseGrant Lease, bool CancellationRequested);
```

The first implementation reserves one job per transaction; the pool calls Reserve only with already-secured local capacity. Batched Reserve/Renew are later optimizations: a batched Renew must report independent per-lease outcomes and a stale member must not roll back healthy ones.

### 3.4 IJobStateStore

```csharp
internal interface IJobStateStore
{
    Task<StoreReply<FinishedAttempt>> FinishAsync(
        StoreCommand<FinishRequest> command, CancellationToken ct);              // Finish/1

    Task<StoreReply<FinishedAttempt>> RecoverAsync(
        StoreCommand<RecoverRequest> command, CancellationToken ct);             // Recover/1

    Task<StoreReply<ReleasedReservation>> ReleaseReservationAsync(
        StoreCommand<ReleaseRequest> command, CancellationToken ct);             // ReleaseReservation/1

    // Not in M2 (SP-011). Present so the interface does not change; an M2 driver
    // returns UnsupportedVersion "operation_not_supported" without writing.
    Task<StoreReply<JobPosition>> RequestCancellationAsync(
        StoreCommand<CancelRequest> command, CancellationToken ct);              // Cancel/1

    Task<StoreReply<JobPosition>> RetryAsync(
        StoreCommand<RetryRequest> command, CancellationToken ct);               // ManualRetry/1
}

internal enum AttemptOutcome
{
    Succeeded, RetryableFailure, PermanentFailure,
    UserCancelled, Interrupted, TimedOut
}

internal sealed record AttemptError(
    string Code,             // ≤ 128 ASCII
    string ExceptionType,    // ≤ 512 UTF-8 bytes
    string Message,          // ≤ 4 KiB UTF-8, sanitized
    string? StackTrace);     // whole stored error ≤ 16 KiB with explicit truncation marker

internal sealed record FinishRequest(
    LeaseHandle Lease,
    AttemptOutcome Outcome,
    TimeSpan ExecutionElapsed,        // ≥ 0, ms ceiling
    TimeSpan? RequestedRetryDelay,    // only with RetryableFailure; ≥ 0
    AttemptError? Error);

internal sealed record FinishedAttempt(
    JobPosition Position,
    string AttemptOutcome,            // §5.4 outcome code recorded on the attempt
    string Disposition,               // pending | retrying | succeeded | failed | cancelled
    string? TerminalReason,           // §5.5, when Disposition is terminal
    DateTimeOffset? EligibleAt);      // when Disposition is pending/retrying

internal sealed record RecoverRequest(
    Guid JobId,
    Guid ExpectedAttemptId,
    Guid ExpectedOwnershipToken);

internal sealed record ReleaseRequest(LeaseHandle Lease);

internal sealed record ReleasedReservation(
    JobPosition Position,
    string Disposition,               // pending | retrying | cancelled
    DateTimeOffset? EligibleAt);      // unchanged eligible_at when not cancelled

internal sealed record CancelRequest(
    Guid JobId, int ExpectedGeneration, long ExpectedRevision,
    string ActorId, string Reason);                       // not in M2

internal sealed record RetryRequest(
    Guid JobId, int ExpectedGeneration, long ExpectedRevision,
    string ActorId, string Reason);                       // not in M2
```

Transactional completion (R6, `CompleteInTransactionAsync`) is Finish/1 executed on the caller's connection with the same checks and lock order; it is **not in M2** (SP-011).

### 3.5 IWorkReadinessStore

```csharp
internal sealed record LaneKey(string Queue, string Priority);
internal sealed record ReadyLane(LaneKey Lane, DateTimeOffset OldestEligibleAt);
internal sealed record ReadySnapshot(DateTimeOffset ObservedAt, IReadOnlyList<ReadyLane> Lanes);

internal sealed record ExpiredLease(
    Guid JobId, Guid AttemptId, Guid OwnershipToken,
    string State, DateTimeOffset LeaseExpiresAt);

internal sealed record PresenceRecord(
    Guid InstanceId,
    long PresenceSequence,                       // strictly increasing per instance
    long CapabilityRevision,                     // fixed for the process lifetime in M2
    string Status,                               // starting | ready | draining | degraded | stopping
    int ProcessId,
    string HostName,                             // ≤ 256 UTF-8 bytes
    string BuildId,                              // ≤ 256 UTF-8 bytes
    IReadOnlyList<string> SubscribedQueues,      // ≤ 128
    IReadOnlyDictionary<string, int> PoolTargets,
    IReadOnlyList<JobContract> Capabilities,     // ≤ 512, the complete revision
    TimeSpan Expiry);                            // 20 s

internal sealed record PresenceAck(DateTimeOffset ExpiresAt, bool Accepted);  // false: older sequence ignored

internal interface IWorkReadinessStore
{
    Task<ReadySnapshot> ProbeAsync(
        string application,
        IReadOnlyList<LaneKey> lanes,                       // 1–384, distinct
        IReadOnlyList<JobContract> supportedContracts,      // 1–512
        CancellationToken ct);                              // ReadinessProbe/1

    Task<PresenceAck> PublishPresenceAsync(
        string application, PresenceRecord presence, CancellationToken ct);   // PublishPresence/1

    Task<IReadOnlyList<ExpiredLease>> ListExpiredLeasesAsync(
        string application, int limit, CancellationToken ct);                 // ListExpiredLeases/1
}
```

All three are observational: no receipt, no ownership authority. The probe returns only non-empty, unpaused, registered lanes and one storage observation time; it acquires no reservation lock, counts no backlog and fetches no payload. A stale non-empty hint may legitimately lead to `NoWork`. TryReserve rechecks everything atomically. A probe never bypasses resolution of an already-sent command whose outcome is unknown.

## 4. Operation catalog

`Name/Version` is the operation identity written into receipts and the compatibility manifest. Every listed operation is version 1. "Class" is the receipt retention class (§8.4).

| Operation | Interface method | Kind | Class | Lifetime (nominal / cap) | M2 |
|---|---|---|---|---|---|
| `Describe/1` | IStoreHandshake.DescribeAsync | query | — | — | yes |
| `RegisterApplication/1` | RegisterApplicationAsync | seed mutation | durable | 5 min / 10 min | yes |
| `RegisterQueue/1` | RegisterQueuesAsync | seed mutation | durable | 5 min / 10 min | yes |
| `RegisterScope/1` | RegisterScopesAsync | seed mutation | durable | 5 min / 10 min | yes |
| `Dispatch/1` | DispatchAsync, StageAsync | mutation | durable | 5 min / 10 min | yes |
| `DispatchMany/1` | DispatchManyAsync, StageManyAsync | mutation | durable | 5 min / 10 min | yes |
| `ResolveCommand/1` | ResolveCommandAsync | receipt read | — | — | yes |
| `ReadinessProbe/1` | IWorkReadinessStore.ProbeAsync | query | — | — | yes |
| `PublishPresence/1` | PublishPresenceAsync | replaceable heartbeat | — | — | yes |
| `ListExpiredLeases/1` | ListExpiredLeasesAsync | query | — | — | yes |
| `Reserve/1` | TryReserveAsync | ownership mutation | ownership | 30 s / 60 s | yes |
| `Begin/1` | BeginExecutionAsync | ownership mutation | ownership | 30 s / 60 s | yes (rate step not in M2, §6.3) |
| `Renew/1` | RenewAsync | ownership mutation, **no receipt** | — | 30 s / 60 s | yes |
| `Finish/1` | FinishAsync | ownership mutation | durable | 30 s / 60 s | yes |
| `Recover/1` | RecoverAsync | ownership mutation | ownership | 30 s / 60 s | yes |
| `ReleaseReservation/1` | ReleaseReservationAsync | ownership mutation | ownership | 30 s / 60 s | yes |
| `Cancel/1` | RequestCancellationAsync | administrative | durable | 5 min / 10 min | not in M2 |
| `ManualRetry/1` | RetryAsync | administrative | durable | 5 min / 10 min | not in M2 |
| `SetQueueState/1` | (internal; harness only in M2) | administrative | durable | 5 min / 10 min | harness only |
| `ActivateCapabilities/1`, `EditRatePolicy/1` | IRateControlStore | administrative | durable | 5 min / 10 min | M3 (10 §6) |
| `ReceiveOutbox/1` | IOutboxDestinationStore | creation | durable | 30 s / 30 s | M3 (14 §5) |
| `ReadinessProbe/2` | IWorkReadinessStore.ProbeAsync | query | — | — | M3 (09 §2) |
| `ListUnsupportedBacklog/1` | IReadinessDiagnostics | query | — | — | M3 (09 §5) |
| `RegisterSchedule/1` | IScheduleStore | creation | durable | 5 min / 10 min | M3 (13 §6) |
| `MaterializeOccurrences/1`, `TriggerSchedule/1` | IScheduleStore | creation | durable | 30 s / 60 s | M3 (13 §6) |
| `SetScheduleState/1`, `UpdateSchedule/1`, `ResolveCatchUp/1`, `RecordScheduleEvaluationFailure/1`, `RevalidateSchedule/1` | IScheduleStore | scheduler mutation | durable | 30 s / 60 s | M3 (13 §6) |
| `SetPoolOverride/1` | IScalingStore | administrative | durable | 5 min / 10 min | M3 (11 §5) |
| `EstimateDemand/1` | IScalingStore | query | — | — | M3 (11 §4) |
| `RecordScalingEvent/1` | IScalingStore | history append, no receipt | — | 10 s | M3 (11 §6) |
| `RerunSucceeded/1`, `UpdateConcurrencyPolicy/1`, `SetApplicationMode/1`, `CleanupBatch/1`, `RecordDisclosure/1` | — | — | durable | per family | not in M2 (RerunSucceeded: 08) |

Effects and preconditions:

| Operation | Preconditions | Atomic effect | Rejections |
|---|---|---|---|
| Dispatch / DispatchMany | Gates open; queues registered; items valid; keys resolvable | New job + payload + generation + scope requirements + key bindings + `dispatched`/`policy_drift` events + receipt; duplicates resolve to existing IDs | InvalidRequest, UniquenessConflict (whole batch unless ContinueOnConflict), UnsupportedVersion, AdmissionClosed, CommandConflict, CommandExpired |
| Reserve | Eligible supported job; queue registered and unpaused; every required scope admits | Attempt + job ownership + every permit (+ fences) + `attempt_reserved` event + receipt | NoWork, QueuePaused, CapacityBlocked |
| Begin | Exact current unexpired lease; state `reserved`; no durable cancellation; budget remaining | `running` + execution counters + attempt start + `attempt_started` event + receipt | StaleLease; durable cancellation → InvalidState `cancelled_before_start` (Applied, attempt closed, never a BegunExecution); exhausted budget → BudgetExhausted (Applied) |
| Renew | Exact current unexpired lease in `reserved`/`running` | Job and the exact permit set extended to the same expiry; no receipt | StaleLease, InvalidState; missing/mismatched permit → InvariantFault `permit_set_mismatch` |
| Finish | Exact current unexpired lease; state `running` | Attempt outcome + job transition (precedence §6.4) + permit release + active-key release when terminal + `attempt_finished` event + receipt | StaleLease, InvalidState |
| Recover | Exact current attempt/token; lease expired at decision time | Close abandoned attempt + budget/cancellation transition + permit release + active-key release when terminal + `attempt_recovered` event + receipt | StaleLease (including a lease renewed before Recover obtained its lock) |
| ReleaseReservation | Exact current unexpired lease; state `reserved` (never `running`) | Attempt closed `released`; job back to prior waiting state (`pending` if `started_count = 0`, else `retrying`) with unchanged `eligible_at`; exact permit release; `attempt_released` event; receipt; no execution, abandonment or rate charge; active uniqueness preserved. A durable cancellation request terminalizes instead (`cancelled`, `cancelled_before_start`). | StaleLease, InvalidState (`running` or terminal) |

Job revision changes on lifecycle and control mutations, never on Renew. The command ID covers replay of a successful action; an expected revision (Cancel/ManualRetry, not in M2) covers an action composed against an obsolete view.

## 5. Job state machine

### 5.1 States

| State | Meaning | Ownership columns | Terminal |
|---|---|---|---|
| `pending` | Waiting; generation has never started (`started_count = 0`); includes delayed first runs | null | no |
| `reserved` | Owned by one attempt that has not been authorized to execute | all set | no |
| `running` | Owned by one attempt whose execution has been authorized by Begin | all set | no |
| `retrying` | Waiting after at least one Begin in this generation (`started_count ≥ 1`) | null | no |
| `succeeded` | Final success | null | yes |
| `failed` | Permanent failure or exhausted budget | null | yes |
| `cancelled` | Cancelled | null | yes |

`pending` and `retrying` are the waiting states. Their distinction is exactly `started_count = 0` versus `≥ 1` in the current generation. The ownership columns are `current_attempt_id`, `worker_instance_id`, `ownership_token`, `lease_expires_at`: all set in `reserved`/`running`, all null otherwise. Terminal states require `terminal_at` and `terminal_reason`.

### 5.2 Legal transitions

Every transition not listed is illegal; a driver that observes a path to one rolls back with `InvariantFault` `invalid_transition`.

| # | From → To | Operation | Condition |
|---|---|---|---|
| T1 | ∅ → `pending` | Dispatch / DispatchMany (Created) | `eligible_at = ceil_ms(max(NotBefore, decision))` |
| T2 | `pending` → `reserved` | Reserve | eligible, supported, unpaused, all scopes admit |
| T3 | `retrying` → `reserved` | Reserve | same |
| T4 | `reserved` → `running` | Begin | lease valid, no cancellation request, `started_count < max_executions` |
| T5 | `reserved` → `cancelled` | Begin, ReleaseReservation, Recover | durable cancellation request present (`cancelled_before_start`) |
| T6 | `reserved` → `pending` / `retrying` | ReleaseReservation | no cancellation request; `pending` iff `started_count = 0`; `eligible_at` unchanged |
| T7 | `reserved` → `pending` / `retrying` | Recover (lease expired) | no cancellation request; `k = consecutive_unstarted_count + 1 < max_unstarted_abandonments`; `pending` iff `started_count = 0`; `eligible_at = decision + unstarted delay(k)` |
| T8 | `reserved` → `failed` | Recover (lease expired) | `k ≥ max_unstarted_abandonments` → `reservation_abandonment_exhausted` |
| T9 | `reserved` → `failed` | Begin | `started_count ≥ max_executions` (defensive; unreachable when invariants hold) → BudgetExhausted, `execution_budget_exhausted` |
| T10 | `running` → `succeeded` | Finish | outcome `Succeeded` |
| T11 | `running` → `retrying` | Finish | retryable class outcome, no cancellation request, `started_count < max_executions` |
| T12 | `running` → `failed` | Finish | `PermanentFailure` without cancellation request, or retryable class with `started_count ≥ max_executions` |
| T13 | `running` → `cancelled` | Finish | `UserCancelled`, or any unsuccessful outcome with a cancellation request |
| T14 | `running` → `retrying` | Recover (lease expired) | no cancellation request, `started_count < max_executions` |
| T15 | `running` → `failed` | Recover (lease expired) | `started_count ≥ max_executions` → `execution_budget_exhausted` |
| T16 | `running` → `cancelled` | Recover (lease expired) | cancellation request present → `cancellation_requested` |
| T17 | `pending` / `retrying` → `cancelled` | Cancel/1 — **not in M2** | expected generation/revision |
| T18 | `failed` / `cancelled` → `pending` | ManualRetry/1 — **not in M2** | new generation (`manual_retry`), active key reacquired |
| T19 | `succeeded` → `pending` | RerunSucceeded/1 — **not in M2** | new generation (`rerun_succeeded`) |
| T20 | `reserved` → `pending` / `retrying` | Begin rate deferral (`RateLimited`, `admission_deferred`) — **not in M2** | — |
| T21 | `reserved` / `running` → same | Renew | lease extension only; no state, revision or counter change |
| T22 | `reserved` / `running` → same | Cancel/1 on an owned job — **not in M2** | records the request flag only (`cancel_requested_at`), revision + 1 |

Queue pause revokes no lease and does not stop Begin/Renew/Finish of existing reservations. Cancellation of an owned reservation (not in M2) signals the worker but never releases its permits on the assumption that it stopped; Begin, Finish, ReleaseReservation or Recover closes it.

### 5.3 Counters changed by each transition

Counter definitions:

| Counter | Where | Meaning |
|---|---|---|
| `reservation_count` | `jobs`, lifetime | Number of Reserves ever applied; the new attempt's `reservation_number` is the value after increment (1, 2, …, across generations) |
| `execution_count` | `jobs`, lifetime | Number of Begins ever applied; returned as `TotalExecutionNumber` |
| `started_count` | `job_generations`, per generation | Executions authorized in this generation; the execution budget. The attempt's `execution_number` is the value after increment (1 … `max_executions`) |
| `consecutive_unstarted_count` (k) | `job_generations`, per generation | Consecutive reservations of this generation recovered as abandoned before Begin. The abandonment guard |
| `generation` | `jobs` | Current generation number, starts at 1; +1 only by ManualRetry/Rerun (not in M2) |
| `revision` | `jobs` | +1 on every state change and every recorded cancellation request (I-4) |

| Transition | reservation_count | execution_count | started_count | k | generation | revision | Attempt row |
|---|---|---|---|---|---|---|---|
| T1 Dispatch | 0 | 0 | 0 (new generation 1) | 0 | 1 | 1 | — |
| T2/T3 Reserve | +1 | — | — | — | — | +1 | insert: `reservation_number`, `reserved_at`, worker, token; `execution_number` null |
| T4 Begin | — | +1 | +1 | := 0 | — | +1 | `execution_number := started_count`, `authorized_at` |
| T5 cancelled before start | — | — | — | — | — | +1 | closed: outcome `cancelled_before_start` (Begin), `released` (Release) or `abandoned_before_start` (Recover); disposition `cancelled` |
| T6 Release | — | — | — | — (not incremented) | — | +1 | closed `released`, disposition `pending`/`retrying` |
| T7 Recover reserved | — | — | — | +1 | — | +1 | closed `abandoned_before_start`, disposition `pending`/`retrying`, `retry_delay_ms`, `next_eligible_at` |
| T8 Recover reserved, exhausted | — | — | — | +1 (= limit) | — | +1 | closed `abandoned_before_start`, disposition `failed` |
| T9 Begin, budget exhausted | — | — | — | — | — | +1 | closed `abandoned_before_start`, disposition `failed` |
| T10–T13 Finish | — | — (charged at Begin) | — | — | — | +1 | closed with engine outcome, `execution_elapsed_ms`, error, disposition, delay |
| T14–T16 Recover running | — | — (no second charge) | — | — | — | +1 | closed `abandoned_running`, disposition |
| T21 Renew | — | — | — | — | — | — | — |

Rules that follow from the table:

- Every Reserve creates exactly one attempt; an attempt is open while owned and closed exactly once; a closed attempt is immutable.
- Begin consumes an execution even if the process dies before the handler method is called. A crash in that gap is recorded, not hidden.
- A running crash consumes only the execution already counted at Begin; Recover never charges a second one.
- Successful Begin resets k. ReleaseReservation and rate deferral never increment k.
- ManualRetry (not in M2) creates generation + 1 with zero `started_count`/k and a copied effective policy; lifetime counters and old rows are untouched.

### 5.4 Attempt outcome and disposition codes

`outcome` (what happened to the attempt): `succeeded`, `retryable_failure`, `permanent_failure`, `user_cancelled`, `interrupted`, `timed_out` (from `AttemptOutcome`, snake_case), `abandoned_before_start`, `abandoned_running` (Recover), `released` (ReleaseReservation), `cancelled_before_start` (Begin found a cancellation request), `admission_deferred` (rate deferral, not in M2).

`disposition` (the job state storage applied): `pending`, `retrying`, `succeeded`, `failed`, `cancelled`. Both the observed error and the applied disposition/reason are preserved when cancellation or budget overrides the observed outcome.

### 5.5 Terminal reasons

| State | `terminal_reason` |
|---|---|
| `succeeded` | `succeeded` |
| `failed` | `permanent_failure`, `execution_budget_exhausted`, `reservation_abandonment_exhausted` |
| `cancelled` | `user_cancelled` (outcome UserCancelled), `cancellation_requested` (unsuccessful running outcome or running recovery with a durable request), `cancelled_before_start` (T5), `cancelled_while_waiting` (Cancel/1 on a waiting job, not in M2) |

### 5.6 Active uniqueness through the lifecycle

An active key binding is held from creation until the job becomes terminal, across `reserved`, `running`, `retrying`, lease expiry and recovery. Every transition into a terminal state clears the job's active binding in the same transaction, but only if the binding still names this job. Fixed-window bindings are never changed by lifecycle transitions. ReleaseReservation and Recover-to-waiting preserve the active binding.

## 6. Execution accounting and outcome precedence

### 6.1 Begin

Begin is the accounting point. It runs before payload deserialization, per-attempt DI activation and middleware invocation; failures in that setup are finalized through Finish under the already-counted execution. An unsupported contract is filtered before Reserve and never reaches Begin.

Begin decision order (after locks and fresh time):

1. Ownership check (§6.6). Failure → StaleLease / InvalidState, no effect.
2. Durable cancellation request present → close attempt `cancelled_before_start`, job `cancelled` (`cancelled_before_start`), release permits, clear active key; reply InvalidState `cancelled_before_start`, Effect Applied, receipt written.
3. `started_count ≥ max_executions` → close attempt `abandoned_before_start`, job `failed` (`execution_budget_exhausted`), release permits, clear active key; reply BudgetExhausted, Effect Applied, receipt written.
4. Rate admission (not in M2, §6.3).
5. Authorize: T4. Reply Applied with `BegunExecution`, carrying the **existing** lease expiry (Begin never grants a fresh lease).

### 6.2 Retry and unstarted delays

For a failed execution with execution number n (1-based, = `started_count` at decision), nominal delay in ms:

```text
b = min(MaximumDelay, InitialDelay × 2^(n−1))      saturating; no overflow
delay ∈ [ceil(b/2), b]                              uniform integer ms (equal jitter)
eligible_at = ceil_ms(decision_time + delay)
```

With defaults (3 executions, 5 s initial, 60 s maximum): after execution 1 wait 2.5–5 s; after execution 2 wait 5–10 s; execution 3 is terminal on a retryable outcome.

A trusted `RequestedRetryDelay` (RetryableFailure only) replaces the exponential calculation: negative → `InvalidRequest` `requested_delay_invalid`; round up to 1 ms; zero → 1 ms; cap at `MaximumDelay`; no jitter. The execution is still charged.

Unstarted recovery uses its own counter k (after increment): if `k ≥ MaxConsecutiveUnstartedAbandonments` (default 5) the generation fails with `reservation_abandonment_exhausted` — the **fifth** consecutive pre-Begin abandonment terminates. Otherwise `b = min(30 s, 1 s × 2^(k−1))` with the same jitter (defaults: 0.5–1 s, 1–2 s, 2–4 s, 4–8 s).

The evaluator is a pure function of (policy snapshot, n or k, sampled entropy). The driver samples entropy once per deciding transaction, persists the chosen delay and eligibility instant in the attempt row and receipt, and never redraws it on replay. A rolled-back transaction's draw is discarded. Eligibility is measured from the storage decision, not client receipt.

### 6.3 Rate-aware Begin (not in M2)

Begin/1's full definition includes all-or-none rolling-window rate grants after concurrency scopes, and a committed `RateLimited` deferral (T20: attempt `admission_deferred`, permits released, prior waiting state restored, no execution or abandonment charge, receipt kept). **This step is not in M2; the M2 worker refuses rate-enabled applications.** An application requiring capability `rate_admission` is rejected by the gate for Dispatch/DispatchMany/Reserve/Begin/Register* with `UnsupportedVersion` `capability_unsupported` (§10.3), and the M2 worker does not enable admission for it. An M2 driver that receives a `RateLimited` reply shape treats it as an invariant fault.

### 6.4 Finish precedence

Evaluated under the job lock with fresh time, after the ownership check (StaleLease/InvalidState first, which makes every completion proposal from a stale owner a no-op). `cancel` = the job's `cancel_requested_at` is non-null in the same transaction. `budget` = `started_count < max_executions`.

| # | Outcome | cancel | budget | Applied disposition | Terminal reason / delay |
|---|---|---|---|---|---|
| 1 | Succeeded | any | any | `succeeded` | `succeeded` (the request stays in history) |
| 2 | UserCancelled | any | any | `cancelled` | `user_cancelled` |
| 3 | PermanentFailure, RetryableFailure, Interrupted, TimedOut | yes | any | `cancelled` | `cancellation_requested`; observed error retained; no retry |
| 4 | PermanentFailure | no | any | `failed` | `permanent_failure` |
| 5 | RetryableFailure, Interrupted, TimedOut | no | yes | `retrying` | delay per §6.2 (RequestedRetryDelay only for RetryableFailure) |
| 6 | RetryableFailure, Interrupted, TimedOut | no | no | `failed` | `execution_budget_exhausted` |

Outcome classification is the engine's (05): a pipeline whose duration is ≥ the configured execution timeout is `TimedOut` even if it returned normally; an `OperationCanceledException` without an engine cancellation cause is an ordinary failure, not `UserCancelled`. Timing uses the engine's monotonic pipeline duration, never database acknowledgement latency. A cancellation request arriving after a committed success finds a terminal job.

### 6.5 Recover

Recover processes one candidate found by `ListExpiredLeases/1`. Under the job lock with fresh time it requires `current_attempt_id = ExpectedAttemptId`, `ownership_token = ExpectedOwnershipToken` and `lease_expires_at ≤ decision` (equality is expired); otherwise StaleLease (`stale_lease` or `lease_renewed`). It never recovers a lease renewed after discovery.

| Owned state | cancel | Condition | Result |
|---|---|---|---|
| `reserved` | yes | — | `cancelled` (`cancelled_before_start`); attempt `abandoned_before_start`; k unchanged |
| `reserved` | no | k + 1 ≥ limit | `failed` (`reservation_abandonment_exhausted`); k := k + 1 |
| `reserved` | no | k + 1 < limit | `pending` if `started_count = 0` else `retrying`; k := k + 1; `eligible_at = ceil_ms(decision + unstarted delay(k))` |
| `running` | yes | — | `cancelled` (`cancellation_requested`); attempt `abandoned_running` |
| `running` | no | `started_count < max_executions` | `retrying`; `eligible_at = ceil_ms(decision + retry delay(n = started_count))` |
| `running` | no | budget exhausted | `failed` (`execution_budget_exhausted`) |

Recover deletes only the expired attempt's permits (exact attempt and token). Expired permits never count toward admission, so availability does not depend on Recover having run. Recover does not refund rate grants (not in M2).

### 6.6 Ownership check

Applied by Begin, Renew, Finish and ReleaseReservation after locks, with decision time `now`:

| Check (in order) | Failure reply |
|---|---|
| Job row exists | NotFound `job_not_found` |
| `current_attempt_id = Lease.AttemptId` | if job is terminal: InvalidState `job_terminal`; else StaleLease `stale_lease` |
| `ownership_token`, `worker_instance_id`, `generation` equal the handle | StaleLease `ownership_mismatch` |
| `lease_expires_at > now` | StaleLease `lease_expired` |
| state allowed (Begin, ReleaseReservation: `reserved`; Finish: `running`; Renew: `reserved` or `running`) | InvalidState `invalid_state` |

## 7. Readiness, reservation candidates and capacity

- Reserve selects one candidate in one `(queue, priority)` lane, ordered by `(eligible_at, job_id)`, after the supplied cursor, whose full decoding tuple `(name, payload_version, serializer_profile_id, serializer_profile_version)` matches one of the worker's supported contracts. It never waits on a job row locked by another claimant. Strict FIFO across concurrent consumers is not promised.
- Lane choice, weights, aging and cooldowns belong to the worker (05). Storage validates eligibility and capacity.
- A job's concurrency requirements (queue / job-type / resource scopes) are resolved to scope IDs at dispatch and are immutable for the generation. Unlimited dimensions have no scope and no permit. Local worker limits are in memory and acquired before calling the driver.
- Capacity is counted live: the number of permits in a scope whose `lease_expires_at` is greater than the decision time, read after the scope lock. If any required scope's count is ≥ its capacity, Reserve returns CapacityBlocked with `BlockedScopeId` (the first blocked scope in UUID order) and `ResumeAfter` = the candidate's cursor, creating no attempt and no permit.
- After CapacityBlocked the caller advances the cursor in a **new** transaction; a transaction never acquires a second job while holding the first job's scope locks. The engine bounds scanning (8 blocked candidates per lane visit, 32 Reserve calls per pool round) and wraps the cursor.
- Permits carry the same expiry as the job lease and are renewed together with it. Renew verifies the expected permit set (one permit per required scope, exact attempt and token); a mismatch rolls back with `InvariantFault` `permit_set_mismatch` and the lease must no longer be treated as healthy. A permit is never repaired by silently extending only the job.
- **Fencing:** for each fencing-enabled scope (resource scope created with capacity 1, I-9) Reserve increments the scope's persistent fencing counter and records the new value on the permit; `ReservedJob.Fences` returns it. The counter is never reset or recreated under the same identity. A fence protects only resources that check it.
- Lowering a capacity (not in M2) blocks new admission until live usage falls below it; existing permits renew while draining. Adding or removing scope membership requires quiescing the affected queues.

## 8. Receipts and replay

### 8.1 Which operations have receipts

Receipt-bearing: `RegisterApplication`, `RegisterQueue`, `RegisterScope`, `Dispatch`, `DispatchMany`, `Reserve`, `Begin`, `Finish`, `Recover`, `ReleaseReservation` (and, not in M2, `Cancel`, `ManualRetry`, `SetQueueState`, rate/schedule/outbox/admin operations).

Not receipt-bearing: `Renew` (R1), `Describe`, `ResolveCommand`, `ReadinessProbe`, `PublishPresence`, `ListExpiredLeases`.

A receipt is written only when the operation commits effects (`Effect = Applied`). ObservedNoEffect outcomes (NoWork, QueuePaused, CapacityBlocked, StaleLease, conflicts, rejections) roll back and leave no receipt.

### 8.2 Receipt protocol

For a receipt-bearing command, inside its transaction and after the store and application gates:

1. Insert the receipt placeholder (operation, version, protocol epoch, command fingerprint and version, NotAfter, receipt codec version, retention class, `retain_until`) if absent; if a row with the same ID already exists, wait for any transaction holding it, then lock it. (PostgreSQL: `INSERT … ON CONFLICT DO NOTHING` followed by `SELECT … FOR UPDATE`.) The same command ID existing in another retention class is `CommandConflict`.
2. If the locked row was committed earlier: compare operation, version and fingerprint. Different → `CommandConflict` (`command_conflict`). Equal → return its recorded reply with `Replayed = true` and the stored `DecidedAt`; commit nothing new. A committed row without a result is impossible (§8.7); observing one is `InvariantFault` `receipt_incomplete`.
3. Read storage time. If no recorded result exists and `now ≥ NotAfter` → `CommandExpired` (roll back). If `NotAfter − now` exceeds the class cap (I-5) → `InvalidRequest` `deadline_too_far`.
4. Acquire all remaining locks in the lock order (04 §5).
5. Read fresh storage time (the decision time). If `decision ≥ NotAfter` → `CommandExpired` (roll back): a command that expires while waiting for locks never applies.
6. Apply effects; write status, result and `decided_at` into the receipt row; commit. Never commit a placeholder.

A concurrent invocation of the same command blocks at step 1 until the first transaction ends: if it committed, the second returns the recorded result; if it rolled back, the second may execute. The existing result is always read **before** the NotAfter check, so a resubmission after the deadline still returns a committed result.

### 8.3 Renew without a receipt (R1)

Renew keeps a command ID (correlation and logging only) and a NotAfter (validated at the decision point: `now ≥ NotAfter` → CommandExpired, no effect). It has no fingerprint and no receipt row. A lost Renew reply is resolved by sending a **new** ownership-checked Renew with a new command ID: if the earlier one committed, the new one extends again (harmless); if the attempt was finalized or taken over, the new one returns StaleLease/InvalidState, which is the correct observation. The per-attempt arbiter serializes Renew after any unresolved Begin/Finish/ReleaseReservation, so a stale Renew is never misread as a failed execution. Only a fresh, non-replayed reply can advance the worker's local authority deadline.

### 8.4 Command lifetimes and retention classes

| Class | Operations | NotAfter (nominal lifetime) | Server cap on NotAfter − now | `retain_until` |
|---|---|---|---|---|
| `ownership` | Reserve, Begin, Recover, ReleaseReservation | 30 s (`OwnershipCommandLifetime`) | 60 s | NotAfter + 10 min |
| `durable` | Finish | 30 s | 60 s | NotAfter + 24 h |
| `durable` | Dispatch, DispatchMany, RegisterApplication, RegisterQueue, RegisterScope (and not in M2: Cancel, ManualRetry, SetQueueState, other administrative) | default and maximum 5 min | 10 min | NotAfter + 24 h |
| — | Renew | 30 s | 60 s | no receipt |

The class is a pure function of the operation name. Receipts are retained at least until `retain_until`; a pending command is never pruned (its placeholder is uncommitted and invisible). Receipt deletion is the cleanup task (SP-021, not in M2; the G-1 harness prunes with the same predicate). Expired replay never newly applies, even after cleanup. Dispatch/business deduplication windows are a separate contract and are never shortened by receipt retention. Scheduler commands (30 s, NotAfter + 24 h) and outbox commands (30 s; delivery mapping AcceptUntil + 24 h) are not in M2.

### 8.5 Receipt result payloads (codec v1)

The receipt row stores `status` (the `StoreStatus` name), `decided_at` and `result` (JSON, receipt codec version 1). JSON members are camelCase; UUIDs are lowercase strings; instants are integer epoch milliseconds; durations are integer milliseconds. Receipts contain IDs and decision metadata, never payload bytes, metadata values or error text.

| Operation | `result` members |
|---|---|
| RegisterApplication | `created`, `revision`, `admissionMode` |
| RegisterQueue | `queues`: [{`queue`, `created`, `paused`, `revision`}] |
| RegisterScope | `scopes`: [{`scopeId`, `kind`, `key`, `created`, `storedCapacity`, `configuredCapacity`, `policyRevision`, `fencingEnabled`}] |
| Dispatch | `jobId`, `disposition` (`Created`/`Reused`), `match` (`None`/`Active`/`Window`/`ActiveAndWindow`), `policyDrift` |
| DispatchMany | `items`: [{`ordinal`, `jobId`?, `disposition`?, `match`?, `policyDrift`?, `failureCode`?, `conflictingJobId`?}] |
| Reserve | `jobId`, `generation`, `revision`, `state`, `attemptId`, `workerInstanceId`, `ownershipToken`, `leaseExpiresAt`, `reservationNumber`, `contract` {`name`, `payloadVersion`, `serializerProfileId`, `serializerProfileVersion`}, `queue`, `priority`, `cursor` {`eligibleAt`, `jobId`}, `policy` (02 §7.2 policy object), `fences`: [{`scopeId`, `value`}] |
| Begin (Applied) | `jobId`, `generation`, `revision`, `state`, `attemptId`, `workerInstanceId`, `ownershipToken`, `leaseExpiresAt`, `executionNumber`, `totalExecutionNumber`, `authorizedAt` |
| Begin (InvalidState / BudgetExhausted, Applied) | `jobId`, `generation`, `revision`, `state`, `attemptOutcome`, `terminalReason`, `errorCode` |
| Finish, Recover | `jobId`, `generation`, `revision`, `state`, `attemptId`, `attemptOutcome`, `disposition`, `terminalReason`?, `eligibleAt`?, `retryDelay`? |
| ReleaseReservation | `jobId`, `generation`, `revision`, `state`, `attemptId`, `disposition`, `eligibleAt`? |

Replay rules:

- A replayed Reserve rebuilds `ReservedJob` from the receipt plus the immutable payload row. If the payload is gone (retention), the reply is `HistoryUnavailable` (`payload_unavailable`), `Replayed = true`; a runnable job is never fabricated. Replay never substitutes a later generation's policy, fences or current ownership.
- A replayed lease (`Reserve`, `Begin`) is historical. Before starting user code after resolving an uncertain Reserve/Begin, the worker performs a fresh ownership-checked Renew and inspects cancellation. The local registry permits one invocation per attempt per process; a new process never adopts an old process's lease.
- A replayed Finish/Recover returns the stored disposition and the stored retry delay/eligibility; nothing is redrawn.
- An uncertain Finish is retried with the same command ID and outcome. It never re-enters the handler. If its receipt has expired, the worker inspects current job/attempt history and reports uncertainty instead of applying an old command as new.

### 8.6 Receipt resolution

`ResolveCommandAsync(CommandIdentity)` is authorized, application-scoped and observational. It joins the receipt row's lock queue (so an in-flight original transaction finishes first), then rolls back without writing:

| Finding | Resolution |
|---|---|
| Row exists, same operation/version/fingerprint | `Recorded(operation, reply)` (reply as §8.5, `Replayed = true`) |
| Row exists, different operation/version/fingerprint | `Conflict` (`command_conflict`) |
| No row, `now < NotAfter` | `NotObserved` — the original may still arrive or be executing; **not** proof of rollback |
| No row, `NotAfter ≤ now < NotAfter + class floor` | `NotApplied` — definitively not applied (a committed command would still have its receipt; holding the receipt lock excludes a late commit) |
| No row, `now ≥ NotAfter + class floor` | `ExpiredUnknown` — receipt may have been pruned; inspect history; never reapply under a fresh ID |

```csharp
internal abstract record ReceiptResolution
{
    internal sealed record Recorded(string Operation, int OperationVersion, string Status,
                                    DateTimeOffset DecidedAt, JsonElement Result) : ReceiptResolution;
    internal sealed record Conflict : ReceiptResolution;
    internal sealed record NotObserved : ReceiptResolution;
    internal sealed record NotApplied : ReceiptResolution;
    internal sealed record ExpiredUnknown : ReceiptResolution;
}
```

Resubmitting the same command has the same effect as resolution followed by execution: after NotAfter it returns the recorded reply or `CommandExpired` (definitively not applied while within the floor). Queries and receipt lookup always use the authoritative primary.

### 8.7 Receipt invariants

- A committed receipt always has `status`, `result` and `decided_at`. Drivers enforce this with a commit-time check (PostgreSQL: deferred constraint trigger, 04 §4.10).
- A receipt is finalized exactly once.
- A changed body under a retained command ID is `CommandConflict`; a rejected observation never permanently reserves an ID.
- Applied commands cannot commit without a complete result; administrative audit (not in M2) is written in the same transaction.

## 9. Failure boundaries and stable codes

### 9.1 Three ways a driver call ends

| Ending | Type | Meaning | Caller action |
|---|---|---|---|
| Decided | `StoreReply<T>` | The store decided in a committed transaction (Applied) or decided no-effect and rolled back (ObservedNoEffect) | Act on Status |
| Known not applied | `StoreNotAppliedException` (Code) | No effect can have committed: the command was never sent, the transaction was rolled back (deadlock, serialization, lock/statement timeout, constraint or trigger failure before/at COMMIT with an error reply), or local admission refused | May resend the **same** command within its NotAfter; for Renew a new command |
| Outcome unknown | `StoreOutcomeUnknownException` (Code, CommandId, Operation) | COMMIT may have been processed: I/O failure, timeout or cancellation after COMMIT was sent and before its reply | Resolve by resubmitting the same command or `ResolveCommandAsync`; never a new ID (Renew: send a new Renew) |

```csharp
internal enum StoreFailureKind { KnownNotApplied, OutcomeUnknown }

internal sealed class StoreNotAppliedException(string code, Exception? inner = null)
    : Exception(code, inner) { public string Code { get; } = code; }

internal sealed class StoreOutcomeUnknownException(
    string code, Guid commandId, string operation, Exception? inner = null)
    : Exception(code, inner)
{
    public string Code { get; } = code;
    public Guid CommandId { get; } = commandId;
    public string Operation { get; } = operation;
}
```

A lost COMMIT acknowledgement stays unknown even if a later timeout or connection close occurs. Absence of a visible receipt before NotAfter is not proof of failure. Known deadlock/serialization rollbacks may be retried by the driver with the same command identity while the deadline and the caller's budget permit (04 §10).

### 9.2 Classification by failure point

| Failure point | Classification |
|---|---|
| Local admission full before submission | Not applied, `local_capacity_exceeded` (no receipt, no storage command sent) |
| Connection acquisition/open fails | Not applied, `store_unavailable` |
| Statement error before COMMIT (including lock/statement timeout, deadlock, serialization failure) | Not applied, code per 04 §10; transaction rolled back |
| Error reply to COMMIT (deferred constraint/trigger, serialization at commit) | Not applied |
| I/O loss, timeout or cancellation before COMMIT is sent | Not applied (server rolls back an abandoned transaction) |
| I/O loss, timeout or cancellation after COMMIT is sent, before its reply | **Outcome unknown**, `commit_outcome_unknown` |
| Successful COMMIT reply | Decided |

### 9.3 Stable codes

Codes are lowercase snake_case, stable across versions, and appear in `StoreError.Code`, exceptions, logs and receipts.

| Code | Status / exception | Meaning |
|---|---|---|
| `no_work` | NoWork | No eligible supported candidate after the cursor |
| `queue_not_registered` | NoWork (Reserve) / InvalidRequest (Dispatch) | Queue control row absent |
| `queue_paused` | QueuePaused | Queue paused at decision |
| `capacity_blocked` | CapacityBlocked | A required scope is at capacity |
| `stale_lease` | StaleLease | Handle's attempt is not the current attempt |
| `ownership_mismatch` | StaleLease | Attempt matches but token/worker/generation differ |
| `lease_expired` | StaleLease | Lease expiry ≤ decision time |
| `lease_renewed` | StaleLease | Recover found an unexpired lease |
| `invalid_state` | InvalidState | Operation not allowed in current state |
| `job_terminal` | InvalidState | Job already terminal |
| `cancelled_before_start` | InvalidState (Applied) | Begin/Release found a durable cancellation; reservation closed, job cancelled |
| `execution_budget_exhausted` | BudgetExhausted | Begin found no remaining execution budget |
| `job_not_found` | NotFound | No such job in this application |
| `window_fingerprint_mismatch` | UniquenessConflict | Live window binding has a different business digest |
| `uniqueness_composition_conflict` | UniquenessConflict | Live active and window bindings name different jobs |
| `fingerprint_version_unsupported` | UnsupportedVersion | Stored binding fingerprint version not computable |
| `revision_conflict` | VersionConflict | Expected generation/revision mismatch (not in M2) |
| `command_expired` | CommandExpired | Deadline passed with no recorded result |
| `command_conflict` | CommandConflict | Same command ID, different operation/version/fingerprint or class |
| `deadline_too_far` | InvalidRequest | NotAfter beyond the class cap |
| `invalid_request` | InvalidRequest | Bounds/grammar/shape validation failed (detail names the field) |
| `requested_delay_invalid` | InvalidRequest | Negative RequestedRetryDelay or delay with a non-retryable outcome |
| `capability_set_too_large` | InvalidRequest | > 512 tuples |
| `operation_not_supported` | UnsupportedVersion | Operation/version not implemented by this binary or not accepted by the store manifest |
| `capability_unsupported` | UnsupportedVersion | Application requires a capability this binary lacks (e.g. `rate_admission`) |
| `schema_incompatible` | UnsupportedVersion | Store schema revision/epoch not in this binary's manifest |
| `store_identity_mismatch` | UnsupportedVersion | Store ID differs from the configured/prepared store |
| `application_not_registered` | AdmissionClosed | No application control row |
| `application_draining` | AdmissionClosed | Admission mode `drain` blocks this operation |
| `application_maintenance` | AdmissionClosed | Admission mode `maintenance` |
| `store_maintenance` | AdmissionClosed | Store migration mode `maintenance` (including an unvalidated install) |
| `payload_unavailable` | HistoryUnavailable | Receipt replay needs a pruned payload |
| `permit_set_mismatch` | InvariantFault | Renew/finalization found missing or extra permits |
| `receipt_incomplete` | InvariantFault | A receipt without result was observed or would be committed |
| `invalid_transition` | InvariantFault | Driver computed an illegal transition |
| `id_collision` | InvariantFault | Preallocated job/attempt/scope ID already exists |
| `constraint_violation` | InvariantFault | Unexpected database constraint failure (rolled back) |
| `local_capacity_exceeded` | StoreNotAppliedException | Local admission refused before submission |
| `store_unavailable` | StoreNotAppliedException | Connection could not be obtained/opened |
| `transient_conflict_exhausted` | StoreNotAppliedException | Deadlock/serialization/lock-timeout retries exhausted within budget |
| `statement_timeout` | StoreNotAppliedException | Statement cancelled by timeout before COMMIT |
| `commit_outcome_unknown` | StoreOutcomeUnknownException | COMMIT sent, reply not received |

## 10. Compatibility gates

### 10.1 Gate check in every transaction

Every mutation transaction (and every bounded query, read-only) first reads the store gate and then the application gate, holding shared locks until commit, and validates them *inside* the transaction; a cached startup result is never sufficient:

| Check | Failure |
|---|---|
| `store_id` equals the configured (or prepared-envelope) store ID | UnsupportedVersion `store_identity_mismatch` |
| `schema_revision` in the binary's writable set (readable set for queries); `protocol_epoch` supported; every operation the call uses is accepted by the store manifest | UnsupportedVersion `schema_incompatible` / `operation_not_supported` |
| Store `migration_mode` is `normal`, or `expand` with the binary declared compatible in the manifest | AdmissionClosed `store_maintenance` |
| Application row exists (except `RegisterApplication`) | AdmissionClosed `application_not_registered` |
| `required_protocol_epoch` ≤ binary epoch; every `required_capabilities` entry known at ≥ the required version | UnsupportedVersion `capability_unsupported` (creation paths only, §10.3) |
| `admission_mode` permits the operation (§10.2) | AdmissionClosed `application_draining` / `application_maintenance` |

### 10.2 Admission modes

| Operation | `open` | `drain` | `maintenance` |
|---|---|---|---|
| Dispatch, DispatchMany, Reserve, Begin, RegisterQueue, RegisterScope | allowed | rejected | rejected |
| Renew, Finish, Recover, ReleaseReservation | allowed | allowed | rejected unless the migration manifest lists them |
| PublishPresence, ReadinessProbe, ListExpiredLeases, ResolveCommand, Describe | allowed | allowed | Describe only (ResolveCommand if its receipt codec is listed) |
| RegisterApplication | allowed | no-op read | no-op read |

Drain intentionally blocks all new admission and work creation while settlement, recovery and receipt resolution continue. An M2 worker in drain mode releases its reserved-not-begun attempts with ReleaseReservation.

### 10.3 Capabilities in M2

An M2 binary advertises `{"execution": 1, "concurrency_permits": 1, "dispatch_uniqueness": 1}`. An application whose `required_capabilities` contains anything else (notably `rate_admission`) is a **rate-enabled or otherwise unsupported application**: the gate rejects Dispatch/DispatchMany/Reserve/Begin/RegisterQueue/RegisterScope with `capability_unsupported`, while Renew/Finish/Recover/ReleaseReservation remain available for settling work already owned. The M2 worker checks this at startup via `Describe/1` and never enables admission for such an application.

## 11. Not in M2 — reserved room

| Item | Where room is left |
|---|---|
| Cancel/1, ManualRetry/1, RerunSucceeded/1 | `IJobStateStore` methods (return `operation_not_supported`), T17–T19/T22, `cancel_requested_*` columns honored by Begin/Finish/Recover/ReleaseReservation, `generation`/`created_reason` |
| Rate-aware Begin, `RateLimited`, `admission_deferred` | `StoreStatus.RateLimited`, outcome code, lock rank "rate scopes" after concurrency scopes, capability `rate_admission` |
| Transactional completion (R6) | Finish/1 on a caller connection |
| Queue pause/resume, scope policy edits | `SetQueueState/1` (harness only), `queue_controls.revision`, `concurrency_scopes.policy_revision` |
| Management audit, retention cleanup, schedules, outbox | Receipt classes and operation names; no M2 records |
| Batched Reserve/Renew, `ReserveAndBegin/1` | Operation-versioned additions after measurement |
