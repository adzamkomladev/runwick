# 05 — Worker engine

**Sources:** [worker-engine-internals.md](../architecture/worker-engine-internals.md) (primary), [execution-defaults-and-timing.md](../architecture/execution-defaults-and-timing.md), [capacity-control.md](../architecture/capacity-control.md), [capacity-defaults-and-budgets.md](../architecture/capacity-defaults-and-budgets.md), [storage-command-protocol.md](../architecture/storage-command-protocol.md) (as it constrains the engine), [review-2026-10-30.md](../review-2026-10-30.md), [observability-and-operations.md](../architecture/observability-and-operations.md) (§5 health/readiness only).

Numeric values are restated here where the engine behavior depends on them; [06-defaults.md](06-defaults.md) is the single table of defaults, derived formulas and startup inequalities. Where the two disagree, 06 is authoritative for the number and this file for the behavior.

## Changes (2026-11-03)

Clarification of §9.6 (batched claim) against §8.3 and §8.4. No engine behavior changes.

- **Shares are over the snapshot.** The eligible set `E` of §8.3 is computed from the readiness snapshot, which may be up to 2 s old (§7.2). A queue that is missing from the snapshot (for example a queue resumed by another process, which is not notified: Changes 2026-11-02, idle caps) is not in `E` until the next probe. Every admission made from that snapshot goes to the queues it shows. This was already true for single claims.
- **A batch is n admissions from one snapshot.** With one eligible queue and one eligible lane, a batch of n makes the same n choices that n single claims from the same snapshot would make, and the selector commits once per begun job, so scores and aging counts end where n single claims leave them. A returning queue still starts at 0 (§8.4 step 4) and gets no burst.
- **What batching changes.** Only throughput: more admissions fit in the life of one snapshot, so more jobs of the visible queue are admitted before a hidden queue is seen. The bound on *when* a hidden queue is seen does not change (hint age + one probe + one claim). A test that measures shares after a membership change must start counting from the first admission a fresh snapshot allowed, not after a fixed number of admissions (15, CAP-02 real-store half; Core case `A_queue_hidden_by_a_stale_hint_…`).
- **Not chosen.** Turning the batch off whenever a pool serves more than one queue would keep the pre-batch admission count per snapshot, but it would remove the batched claim from every multi-queue pool and still not make the count exact: single claims on a fast store also admit many jobs per snapshot.

## Changes (2026-11-02, PF-06 batched finish)

New **§12.5 Batched finish**, used when the store offers `FinishMany/1` (03 Changes 2026-11-02, PF-06 batched finish). Without it the engine settles exactly as before. The settlement lane stays 2 concurrent calls (06).

- **When.** A completion worker that takes a Finish from the ready channel also takes every other Finish already waiting there, up to 32. With two or more it sends one `FinishMany`; with one it sends `Finish/1` as before. There is no coalescing delay. *[choice]* Reason: batching matters only when the lane is the bottleneck, and then Finishes are already waiting, because both lane calls are busy while new ones are published. A wait of a few milliseconds would add latency to every Finish on an idle worker and would not make batches larger under load.
- **Command.** Each item is the entry's frozen Finish command (§12.2), unchanged: same command ID, same body, same NotAfter. The call gets a new command ID and NotAfter = the latest item NotAfter. Every member holds its own arbiter slot (§6) for the whole call, as a single Finish does; the slots are taken in turn (members are distinct attempts, and no other holder of an arbiter waits for a second one). The call timeout is min(`ControlRequestTimeout`, the earliest member's remaining budget). An entry whose budget already ran out is not batched; it takes the single path, which latches it. Each item counts once in `FinishesSent`; `CompletionCoordinator.BatchesSent` counts calls.
- **Applied.** Each item's reply is applied exactly as the same Finish/1 reply in §12.4 (Applied, replayed or not; `StaleLease`/`InvalidState`; `CommandExpired`; `InvalidRequest`/`InvariantFault`/`CommandConflict`). An item reply that §12.4 retries (for example `NotFound` or a closed gate) resolves alone, as below.
- **Not Applied, or no reply.** Each item resolves alone through the existing path: the same Finish/1 command, at resolution priority after one control backoff, within its own `CompletionResolutionBudget` (still measured from its own publication, so the budget means what it meant). An item that the lost call committed replays its own receipt; one it did not commit is decided now. `UnsupportedVersion` or `InvalidRequest` also turns batching off for this worker, with a diagnostic; `InvariantFault` (one item broke an invariant and rolled the call back) resolves each item alone so the faulty one reports its own fault.

## Changes (2026-11-02, idle caps)

Decision (adzamkomla, 2026-11-02): now that the wake listener ships, an idle worker polls less often while its listener is healthy. This replaces the "Not changed" bullet of the PF-07 entry below.

- **Cap.** The idle backoff cap of §7.2 is **10 s while the listener is healthy** and 2 s otherwise. Healthy means: from a successful (re)subscription until the session is lost or refused, or the listener task ends. Before the first subscription, after a loss, and with a store that offers no notifications, the cap is 2 s, exactly as before, so a missed notification never costs more than it did. The cap is read at every idle wait. Base (100 ms), doubling, jitter (±20 %), the resets, the 2 s hint age and the 50 ms probe spacing do not change. The blocked lane/queue cooldown cap stays 2 s.
- **Loss.** A lost or refused session wakes every pool at once, so no wait begun under the 10 s cap outlives the session. The following waits are capped at 2 s until the resubscription, which still drops the snapshot and wakes the pools. Dropping notifications on receipt (`NotificationsSuppressed`, OBS-01) does not make the listener unhealthy: that is the worst case of a healthy session.
- **Why 10 s.** Five times fewer idle probes per polling group, and a worst case for un-notified work of 12 s + hint age + one probe and one claim. That is the bound OBS-01 now asserts (07 Changes 2026-11-02, idle caps). It stays below the presence expiry (20 s) and well below the lease and renewal timescales. A healthy session loses notifications only on notify-queue overflow.
- **Cost: work that becomes due by time.** Only committed dispatches with a job eligible now notify (04 Changes 2026-11-02, PF-07). Delayed dispatch (`NotBefore`), retry back-off, jobs released by another process's Recover or ReleaseReservation, a queue resumed by another process, and rate or concurrency room freed in another process are not notified. On a worker that has been idle long enough to reach the cap, they are found within 12 s + hint age instead of 2.4 s + hint age. Local wakes (local dispatch, ticket retirement, local control changes) are unchanged. A pool that has just reserved starts again from 100 ms, so short retry delays right after activity are found as quickly as before.
- **Undetected loss.** A session that dies silently is noticed by the listener's 30 s `SELECT 1` (or TCP keepalive). Until then the 10 s cap applies.
- **Store interface.** `IWakeNotificationSource.ListenAsync` gains an `onLost` callback, called when the session is lost or refused, before the reconnect back-off (04 Changes 2026-11-02, idle caps).

## Changes (2026-11-02, PF-07)

§7.2 "optional LISTEN notifications" now ships for stores that offer them (the PostgreSQL store does; scripted and decorated stores without them behave exactly as before). Nothing in this section's rules changes: the hint age, idle backoff (100 ms doubling to 2 s), ≥ 50 ms probe spacing and the coalescing wake signal stay as specified.

- **Listener.** One supervised, non-critical task per worker (`listener`), on the dedicated session of §18 (one unpooled connection, application name `<prefix>:listen`, outside the control and work pools). It runs `LISTEN runwick_wake` and, after 30 s without a notification, `SELECT 1` to prove the session is alive. A lost session is replaced after 1 s doubling to 30 s. Every successful (re)subscription drops the cached snapshot and wakes every pool ("Reconnect / resubscribe triggers a fresh probe"), since commits made while unsubscribed were not notified. A listener fault never closes admission.
- **Notification.** Payload `<application>/<queue>`; a notification for another application or a queue no pool serves is ignored. For a served queue: when the cached snapshot (younger than the hint age) already shows a due lane of that queue, nothing happens; otherwise the snapshot is dropped and the pools that serve the queue get the wake signal. The next probe still waits out the 50 ms spacing, so a burst of commits costs at most one probe per 50 ms per group.
- **Correctness.** Unchanged: a notification is a hint, never authority, and polling finds everything with every notification dropped. The engine's `NotificationsSuppressed` hook drops each notification on receipt (OBS-01, 07 Changes 2026-11-02, PF-07).
- **Not changed.** The idle backoff cap stays 2 s. Raising it once notifications ship would lower idle cost but changes OBS-01's bound and 06 defaults; that is deferred to a separate decision.

## Changes (2026-11-02, PF-06 batched claim)

New **§9.6 Batched claim**, used when the store offers `ReserveAndBegin/1` (03 Changes 2026-11-02, PF-06 batched claim). Without it the engine claims exactly as before.

- **When.** At each claim of a lane visit (§8.6) the pool holds one ticket. If the store offers the operation, it also takes every other free ticket of the pool at once, up to 32 in all. With two or more tickets it sends one `ReserveAndBegin` with `MaxJobs` = the number of tickets, so it never claims more jobs than it has free slots. With one ticket (one free slot) it keeps `Reserve/1` then `Begin/1`. The batch is used only when the selection had exactly one candidate lane (one eligible queue, one eligible lane in it) and was not an aging pick. Queue and lane shares divide admissions (§8.4), so with several ready lanes each admission stays its own selection and the shares are exactly as before. After a batch the selector commits once per begun job; with one candidate lane that leaves the scores where n single claims would, and advances the queue's aging count by n.
- **Command.** As the Reserve command (§9.1): ownership lifetime, the lane's cursor as `After`, `LeaseDuration`, the same admission class and the same one-claim-outstanding rule per pool (§8.8). Resolution is §9.2: an unknown commit keeps every ticket and resubmits the same command; the pool is suspended past `ClaimResolutionBudget`.
- **Applied.** Each outcome settles one ticket, in order:
  - `Applied`: the ticket moves into a new entry (§5), its lease registered at once with `d` from the command's send time (§9.1), and its Begin reply already known. The entry skips the Begin pre-checks and the Begin command (§9.3) and settles that reply as §9.4 does (a replayed reply needs a fresh Renew before invocation). Invocation admission (§9.5) is unchanged, including the margin, the cancellation check and the drain cutoff. The pre-check "admission still open" does not apply: the attempt is already begun in storage, so it runs like an attempt whose Begin was in flight when drain started.
  - `RateLimited`: the ticket returns; counted as a rate deferral and as a blocked admission outcome, as §9.4's committed deferral (10 §5).
  - `InvalidState` / `BudgetExhausted`: storage closed the attempt; the ticket returns.
  - `CapacityBlocked`: the ticket returns; recorded as a blocked outcome; `cursor := ResumeAfter`; queue or single-type lane scopes cool as in §8.6.
  - Tickets left over (fewer candidates than slots) return.

  The visit then ends (one batch per visit). Lane scores commit if any job began.
- **Not Applied.** Every ticket but the first returns. `NoWork`, `QueuePaused`, all-`CapacityBlocked` and `CommandExpired` are handled exactly as the same Reserve result in §8.6, with the first ticket. Any other reply (`UnsupportedVersion`, `InvalidRequest`, `InvariantFault`, `CommandConflict`, `AdmissionClosed`, …) turns the batch off for the pool, adds a diagnostic, and the first ticket claims at once with `Reserve/1` then `Begin/1`. From then on the pool claims one job at a time, whose replies apply §8.6 and §9.4 as before. The four no-effect replies above do not turn the batch off: they mean "nothing to claim", and retrying them as a single Reserve would only add idle round trips.
- **Counters.** `PoolCoordinator.BatchClaims` counts applied batches; every batch also counts once in `ReservesSent`.

## Changes (2026-11-02, PF-06)

§11.2 renewal waves, when the store offers `RenewMany/1` (03 Changes 2026-11-02, PF-06). Without it the engine renews exactly as before.

- **Wave.** When the first item in the due queue is due, every other live item due within `RenewalJitter / 2` (default 1.5 s) joins it, up to 64. A member therefore renews at most that much early and never later than its due time, so `due ≤ d − (2 × ControlRequestTimeout + ControlRetryBackoffMax + SchedulerAllowance)` still holds and the deadline math of §11.1 is unchanged.
- **Arbiter.** Each member takes its own per-attempt arbiter slot without waiting (only when it is free and nobody is queued, §6 rule 3) and holds it for the whole call. A member whose arbiter is busy (Begin, Finish, ReleaseReservation or resolution in flight) leaves the wave and renews alone through the existing path, which waits for the arbiter. A wave left with one member renews it with `Renew/1`.
- **Call.** One `RenewMany` on the renewal lane (still 4 concurrent calls at most), with a new command ID, NotAfter = wallNow + 30 s, and a timeout bounded by `ControlRequestTimeout` and by the earliest member deadline `d − now`. Every member counts as one renewal request for `RenewsSent`; the send time `s` is the call's send time for every member.
- **Outcomes.** Each member's outcome is applied exactly as a Renew/1 reply (§11.3): only a fresh `Applied` advances `d`, and only if it arrives before the member's current `d` and the member is not latched; `StaleLease`/`InvalidState` latch `OwnershipLost`; `permit_set_mismatch` latches `OwnershipLost(InvariantFault)`. A failed call (exception, timeout, or any non-`Applied` reply) and a member's transient outcome fall back to the per-attempt path: a new Renew per attempt with its own control backoff. A late reply never revives a latched attempt; the watchdog (§11.4) is unchanged.

## Changes (2026-10-31, SP-012)

- §7.2: probe starts are ≥ 50 ms apart per group, enforced in the coordinator (it was a stated rule only).
- §8.6: probe/2 queue evidence excludes a saturated queue's lanes for the snapshot's life; a changed queue control revision clears that queue's cooldowns; a retiring local attempt lifts its queue's exclusion (09 §4.2).
- §14 / §17: the first presence publication is retried 3 times; failing publication makes readiness `Degraded` (09 §3.2).
- A rate deferral sets no cooldown (09 C-5).

## Changes (2026-10-31, SP-016)

- The scheduler role (`AddScheduler`) is a separate hosted service, independent of workers. It holds no lease and never uses the renewal lane (13 §4.1). Scheduled jobs reach workers as ordinary jobs.

## Changes (2026-10-31, SP-014)

- §3.3 (startup): `AutoConcurrency(min, max)` is accepted; the M2 refusal is lifted (11 §2). Note 3 and the *[choice]* "AutoConcurrency" below are superseded.
- §4: a pool's ticket target can change at run time (11 §3.5). A reduction stops refill only; tickets are never revoked. The gate reports every outstanding change to the occupancy integrator.
- New non-critical loop `scaler` (every 5 s ± 10 %). Presence publishes each pool's current target. Health is unchanged by the scaler.

## Changes (2026-10-31, SP-013)

- §3.3 / §9.4 (C-2): a committed `RateLimited` Begin retires the entry with no scope, middleware or unstarted-guard charge, and returns the ticket at once (10 §5). The engine counts it (`RateDeferrals`). A `RateLimited` reply without a committed effect stays an invariant fault.
- §3.3 startup: the rate-enabled refusal (V-24) applies only to binaries that do not know `rate_admission`.

## Changes (2026-10-31, SP-011)

- §3.3: `JobExecutionResult.Completed` is no longer refused. A staged attempt is settled by receipt observation (08 §8.2), and a `Completed` token for an attempt that staged nothing is a pipeline contract fault.
- §11.3: renewal pauses while a staged completion holds the caller's transaction (08 §8.1 step 3).
- Durable cancellation from Cancel/1 T22 is observed through Renew as before. Nothing in the engine assumes the handler stopped.

## Changes (2026-10-30)

Contradictions resolved (later document wins):

| # | Conflict | Resolution in this spec |
|---|---|---|
| C-1 | capacity-control says a lane is aged when its oldest eligible age "exceeds" 60 s; capacity-defaults says "at least 60 s" | `age >= AgingThreshold` (CAP-03 tests equality) — §8.5 |
| C-2 | worker-engine-internals (follow-on note) says a committed `RateLimited` Begin retires the entry; M2 has no rate-aware Begin | Rate-aware Begin/1 is not in M2. The worker refuses rate-enabled applications at startup; a `RateLimited` reply is an invariant fault — §3.3, §9.4 |
| C-3 | worker-engine-internals §8 assumed every ownership command, including Renew, has a receipt | R1: Renew has no receipt; an unknown Renew is resolved by sending a fresh ownership-checked Renew with a new command ID — §6, §11 |
| C-4 | worker-engine-internals §9 originally let reserved-not-begun work expire during shutdown | R2: drain issues `ReleaseReservation`; expiry recovery is only the fallback for an unconfirmed release — §16 |
| C-5 | runwick-explained.md describes `reserved → pending` on recovery/release | Restored state is `pending` only if the generation's `started_count = 0`, otherwise `retrying` — §13, §16 |
| C-6 | capacity-defaults first draft used fixed 25 s / 30 s pressure margins | Derived `StopMargin` / `ResumeMargin` (review correction) — §15 |
| C-7 | worker-engine-internals local phase names (Claiming/ResolvingClaim, Executing, AwaitingFinish/ResolvingFinish) predate R2 | Consolidated phase set `PendingClaim, Reserved, Beginning, Running, CleaningUp, Finishing, Releasing, OwnershipLost, Retired` with `Releasing` added for R2 — §5 |
| C-8 | Capability-set bound was required ("bounded capability arrays") but never given | 512 deduplicated decoding tuples per probe / Reserve (implementation choice) — §3.2 |
| C-9 | `OBS-01` exists in both driver-contract-suite and observability-and-operations | Cited as `driver-contract-suite:OBS-01` / `observability-and-operations:OBS-01` |

Implementation choices made here (each marked *[choice]* where it appears):

1. Worker validation-matrix rows are given stable IDs `WRK-01`…`WRK-19` (§19).
2. Polling group = (store, application): the M2 registry is frozen per process, so every pool of one worker for one store/application shares one capability set (§7.2).
3. `AutoConcurrency` (SP-014) and revisioned pool-target overrides (SP-011/014) are not in M2; startup rejects `AutoConcurrency` (§3.3).
4. "Lower lane" for aging = an eligible lane of the queue whose weight is lower than the highest weight among that queue's currently eligible lanes (§8.5).
5. Renewal jitter is sampled independently for every scheduled renewal (§11.2).
6. A Finish or ReleaseReservation is never resubmitted under a new command ID; `CommandExpired` on a resubmission is a definitive no-effect resolution (§12.4).
7. `ReleaseReservation` uses the settlement lane of the control data source (§18).
8. Shutdown: pipelines still live at the cancellation cutoff (T0 + 20 s) latch abandonment; the finalization slice is reserved for attempts whose pipeline already ended (§16).
9. A Begin that resolves Applied at or after the drain cutoff (T0 + 15 s) is not invoked; it is finalized `Interrupted` without a scope (§16).
10. Recovery processes at most 32 expired-lease candidates per scan (§13).
11. Scope classification for `CapacityBlocked` uses only scope IDs known from the local registry's seed-if-absent provisioning; no separate bounded metadata cache in M2 (§8.7).
12. Liveness is unhealthy when the supervisor progress tick has not advanced for 10 × `LeaseWatchdogInterval` (§17).
13. User code is queued with `ExecutionContext` flow suppressed (§10.1).
14. A registry that cannot resolve a tuple it advertised latches `OwnershipLost(InvariantFault)` without a Finish (§10.1).

## 1. Scope

This file specifies the worker role for milestone M2 (PostgreSQL execution through gate G-1, backlog item SP-009): from host start to host stop, every task the worker owns, every local state an attempt passes through, and the exact admission, renewal, completion, recovery and shutdown behavior.

Not in M2 (interfaces leave room, nothing is implemented): rate-aware Begin and rate scopes; automatic concurrency controller and pool-target overrides; scheduler role; management/dashboard; outbox; Redis and SQLite drivers; transactional completion (`CompleteInTransactionAsync`, SP-011); batch Reserve or batched transport; local resource-key gates.

Terms: *attempt* = one reservation (`attempt_id`); *ticket* = one unit of a pool's local capacity; *authority* = the worker's conservative local belief that it still owns the lease; *owner mutation* = Begin, Renew, Finish, ReleaseReservation.

## 2. Ownership tree

One `WorkerHostedService : IHostedService` exists per configured worker instance. It is a direct `IHostedService` implementation (not `BackgroundService`) so staged stopping does not depend on one automatically cancelled token. Producer-only registration creates none of the components below. Embedded and external hosts instantiate the same tree.

```text
WorkerHostedService
└── WorkerSupervisor                       (one; owns every long-lived task)
    ├── ReadinessCoordinator               (one per polling group)
    ├── PoolCoordinator × N                (one per configured pool)
    │   ├── LocalCapacityGate              (tickets)
    │   └── attempts: PendingClaim → AttemptEntry → AttemptRunner (one per admitted attempt)
    ├── LeaseCoordinator                   (one per worker/store; renewal + safety watchdog)
    ├── CompletionCoordinator              (one per worker/store)
    ├── RecoveryLoop                       (one per worker/store/application)
    └── WorkerPresenceLoop                 (one per worker/store/application)
```

| Component | Owns | Must not own |
|---|---|---|
| WorkerSupervisor | Startup validation, worker instance ID, critical-task supervision, health, stop sequence | Application job scopes |
| PoolCoordinator | Target, local tickets, queue/lane selection, cursors, cooldowns, attempt membership | User code execution on its loop |
| ReadinessCoordinator | One in-flight probe, coalesced wake signal, polling backoff, optional notification subscription | Durable admission authority |
| AttemptRunner | Decode/upgrade, DI scope, middleware, handler, cleanup, observed outcome | Lease renewal scheduling |
| LeaseCoordinator | Due-time queue, bounded renewal I/O, safety deadlines, watchdog, loss latching | Handler or scoped-service references |
| CompletionCoordinator | Ready-ID channel, Finish submission/retries, receipt resolution | Handler re-execution |
| RecoveryLoop | Bounded expired-lease discovery and Recover commands | Treating a worker heartbeat as proof of job ownership |
| WorkerPresenceLoop | Capability snapshot, heartbeat, draining/health status | Extending a job lease |

Rules:

1. The worker instance ID is `Guid.CreateVersion7()` generated at startup, unique to the process lifetime, never reused, and never used to adopt an earlier process's attempts.
2. Every task is registered with the supervisor before it can run, observed for faults, and awaited during orderly shutdown.
3. A fault in PoolCoordinator, ReadinessCoordinator, LeaseCoordinator or CompletionCoordinator is **critical**: the supervisor closes admission for the affected store group, marks readiness unhealthy, and continues renewal/completion of existing attempts with whatever components remain. A worker must never look healthy while accepting work without renewals. A LeaseCoordinator fault therefore always closes admission; the watchdog (§11.4) is a separate timer and still latches loss.
4. Handler exceptions are attempt outcomes, never supervisor faults.
5. Telemetry/presence failures degrade observability and retry with backoff; they never fabricate lease loss or stop admission.
6. The scheduler is a separate role; a worker never calls it before reserving.

## 3. Startup

### 3.1 Order

1. Validate configuration (all inequalities in [06-defaults §3](06-defaults.md#3-startup-validation)); reject contradictory settings — never silently raise a lease or reduce a margin.
2. Build the immutable handler registry (§3.2). Do not instantiate scoped handlers or middleware.
3. Check store/application compatibility gates (schema revision, operation versions, application mode, required capabilities) on the store; reject rate-enabled applications (§3.3).
4. Provision registry scopes seed-if-absent (`RegisterScope/1`; queue, job-type and resource scopes from `Queue(...).MaxConcurrent`, `ConfigureJob<T>().MaxConcurrent`, `ConfigureJob<T>().PerResource`). Existing capacities are never overwritten; mismatches are reported as diagnostics. Record the returned `scope_id → (kind, key)` map for §8.7.
5. Publish the initial presence/capability snapshot (§14).
6. Start, under the supervisor: LeaseCoordinator, CompletionCoordinator, RecoveryLoop, PresenceLoop, ReadinessCoordinator, then PoolCoordinators. Admission is enabled only after all have started.

### 3.2 Registry and capabilities

`JobRegistration` is keyed by stable `job_name` and holds: current payload version and CLR type with the single handler factory; immutable serializer profiles and historical decoding types; ordered adjacent-version upgrade delegates to the current type; immutable effective job settings; a cached middleware composition factory.

Startup rejects: duplicate job names; missing upgrade steps; cycles; ambiguous handler bindings; incompatible types; unknown middleware aliases (the error names the job, the alias and the missing `AddMiddleware<T>("alias")` call); unknown queues/lanes.

The worker advertises a decoding tuple `(job_name, payload_version, serializer_profile_id, serializer_profile_version)` only when a complete decode/upgrade path to the current handler input exists. Tuples are deduplicated. *[choice]* A polling group's capability set is bounded to **512** tuples; a registry that would exceed it fails startup. The same ≤512 tuple arrays are passed to every probe and Reserve.

The registry is frozen for the process lifetime (capability changes require a new process and new instance ID). Internal dispatch uses cached typed invokers; any reflection happens at registration.

### 3.3 Refusals in M2

- **Rate-enabled application.** If configuration declares any rate policy, or the application's stored required capabilities include rate admission, startup fails with a compatibility error. Rate-aware Begin/1 is not in M2.
- *[choice]* **AutoConcurrency.** A pool configured with `AutoConcurrency(min, max)` fails startup in M2. Only `FixedConcurrency(n)` (n ≥ 1) or the default fixed 4 is accepted.
- **Incompatible store.** Unsupported operation version, schema revision or application mode fails startup; at runtime the same condition (`UnsupportedVersion`, `AdmissionClosed`) closes admission (§9.2).

## 4. Local capacity tickets

`LocalCapacityGate` is one per pool: a locked counter of outstanding idempotent tickets and a target. It is never implemented by swapping `SemaphoreSlim` instances.

| Rule | Requirement |
|---|---|
| Acquire | Succeeds only when admission is open for the pool and `outstandingTickets < target` |
| When | A ticket is acquired **before** a Reserve command is created; no durable reservation is ever requested without a held ticket |
| Hold | Through claim resolution, Begin, execution, cleanup, finalization (or ReleaseReservation), and until definitive loss of authority **and** end of all local user work |
| Release | Exactly once, when the holder reaches `Retired` (§5); a second release is a no-op on the idempotent ticket |
| Target reduction | Stops refilling; cancels nothing; outstanding may exceed the new target until tickets retire |
| Refill | Each retired ticket refills independently; a slow job never blocks refilling other tickets |
| Prefetch | 0: there is never a reservation beyond held tickets |
| Group bound | Σ pool maxima ≤ worker/store group maximum (16 default); validated at startup |
| Fixed target | `FixedConcurrency(n)` sets target = pool maximum = n; default n = 4 |

Pending completions occupy tickets: during a storage outage throughput drops instead of building an unbounded queue of successful handlers awaiting acknowledgement. Lost authority does not free a ticket while user code or cleanup is alive.

## 5. Attempt registry

### 5.1 Records

- **`PendingClaim`**, keyed by Reserve command ID. Holds the ticket, pool, lane, cursor argument, command (ID, NotAfter, body), first-submission timestamp and operation sequence. Owns the ticket until the claim resolves.
- **`AttemptEntry`**, keyed by `(application, attempt_id)`. Takes the ticket exactly once from an Applied PendingClaim. Holds: `LeaseHandle`, local authority deadline `d`, pool, immutable `ExecutionPolicySnapshot` and registration, invocation latch, runner task handle, current control command (ID, body, sequence, submission time, resolution state), cancellation causes (`User`, `Timeout`, `Shutdown`, `LeaseLost`) recorded individually with first-observed timestamps, frozen observed outcome, cleanup status, loss cause.

State changes use short in-memory critical sections only; no monitor is held across database I/O or user code. Every operation completion carries its local operation sequence: a late reply may resolve its own operation but never overwrites a newer ownership or terminal decision. After retirement the command resolver and invocation latch keep the disposition until all callbacks are joined, so a late Reserve replay cannot recreate an entry.

### 5.2 Phases

Local phases are separate from durable job states.

| Phase | Source name | Durable relationship | Ticket/slot rule | Commands allowed |
|---|---|---|---|---|
| `PendingClaim` | Claiming / ResolvingClaim | No job, or an unobserved committed reservation | Hold ticket; no other claim for it | Reserve (same ID until resolved) |
| `Reserved` | Reserved | Exact reserved lease | Hold; invoke nothing | Renew; Begin; ReleaseReservation (drain only) |
| `Beginning` | Beginning | Reserved, Begin in flight or unresolved | Hold; invoke nothing | Begin resolution only (same ID) |
| `Running` | Executing | Running; invocation latched once | Hold; renew while authority remains | Renew |
| `CleaningUp` | CleaningUp | Running; pipeline ended, scope disposing | Hold; renew | Renew |
| `Finishing` | AwaitingFinish / ResolvingFinish | Outcome frozen locally; durable state may already have changed | Hold; never invoke again | Finish (same ID); Renew only between resolved commands |
| `Releasing` | (new, R2) | Reserved; ReleaseReservation in flight or unresolved | Hold | ReleaseReservation (same ID) |
| `OwnershipLost` | OwnershipLost | Store rejected ownership, or a local deadline/budget latched loss or abandonment | Hold while any user task, cleanup or issued command is alive | Resolution of an already-submitted Finish/Release/Begin only; **no new owner mutation** |
| `Retired` | Retired | Local work ended; durable result resolved or authority relinquished | Release ticket exactly once | None |

`OwnershipLost` records a loss cause: `StaleLease`, `InvalidState`, `SafetyDeadline`, `GraceExpired`, `CleanupExpired`, `BeginUnresolved`, `CompletionUnresolved`, `ReleaseUnconfirmed`, `ShutdownCutoff`, `InvariantFault`. Once latched, an attempt never leaves `OwnershipLost` except to `Retired`, whatever replies arrive later.

### 5.3 Legal transitions

| From | To | Trigger |
|---|---|---|
| — | `PendingClaim` | Ticket acquired; Reserve command created with a fresh `Guid.CreateVersion7()` ID |
| `PendingClaim` | `Reserved` | Reserve `Applied` (fresh or replayed); ticket transferred once to a new `AttemptEntry` |
| `PendingClaim` | `Retired` (no entry) | `NoWork`, `QueuePaused`, `CapacityBlocked`, confirmed rollback, or `CommandExpired` on resubmission (definitive no effect) |
| `PendingClaim` | `PendingClaim` | Unknown commit: resubmit the same command; pool fresh claims suspended (§15.2) |
| `PendingClaim` | `Retired` (unresolved) | Control-I/O stop at shutdown (T0 + 25 s); recorded as unresolved claim; eventual reservation left to recovery |
| `Reserved` | `Beginning` | Begin submitted (pre-checks §9.3 pass) |
| `Reserved` | `Releasing` | Admission stopped (drain) and attempt not begun |
| `Reserved` | `OwnershipLost` | Renew `StaleLease`/`InvalidState`; safety deadline; invariant fault |
| `Beginning` | `Running` | Begin `Applied` and invocation admission (§9.5) passes; invocation latched |
| `Beginning` | `Retired` | Begin committed closure without execution: `InvalidState` with code `cancelled_before_start`, or `BudgetExhausted` |
| `Beginning` | `Finishing` | Begin `Applied` but invocation refused without creating a scope: pipeline deadline already elapsed (Finish `TimedOut`), durable cancellation observed on the post-ambiguity renewal (Finish `UserCancelled`), or the drain cutoff passed (Finish `Interrupted`, §16) |
| `Beginning` | `OwnershipLost` | `StaleLease`; `BeginResolutionBudget` expired unresolved (`BeginUnresolved`); safety deadline; post-ambiguity renewal fails |
| `Running` | `CleaningUp` | Pipeline ended; outcome and elapsed time frozen |
| `Running` | `Finishing` | Runner found the pipeline deadline elapsed before user code started (`TimedOut`, no scope created) |
| `Running` | `OwnershipLost` | Stale/safety loss; `CancellationGrace` expired with pipeline live; shutdown cancellation cutoff with pipeline live |
| `CleaningUp` | `Finishing` | Scope disposal completed (a cleanup exception is recorded separately) |
| `CleaningUp` | `OwnershipLost` | `CleanupBudget` expired with disposal live; stale/safety loss |
| `Finishing` | `Retired` | Finish `Applied` (fresh or replayed); definitive `StaleLease`/`InvalidState` (stored outcome not overwritten); receipt `ExpiredUnknown` after history inspection (unknown-durability diagnostic) |
| `Finishing` | `OwnershipLost` | `CompletionResolutionBudget` expired unresolved (`CompletionUnresolved`); safety deadline latched while Finish not yet submitted |
| `Releasing` | `Retired` | ReleaseReservation `Applied` (released, or terminalized by a durable cancellation); `StaleLease`/`InvalidState` |
| `Releasing` | `OwnershipLost` | Release unconfirmed at the drain cutoff (`ReleaseUnconfirmed`) |
| `OwnershipLost` | `Retired` | User task ended **and** cleanup ended **and** every issued command resolved or its provider task terminated (ambiguity recorded) |

Any transition not listed is illegal and is an invariant fault (logged; the entry latches `OwnershipLost/InvariantFault`).

Retirement conditions (all required): no live user task; scope disposed or never created; every issued command resolved, or its provider I/O task terminated with the ambiguity recorded. `NotAfter` passing alone does not prove an earlier transaction failed.

## 6. Per-attempt control arbiter

Begin, Renew, Finish and ReleaseReservation for one attempt pass through one asynchronous arbiter per `AttemptEntry`:

1. At most one lifecycle command is in flight per attempt, **including** resolution of its unknown commit.
2. The arbiter slot is held until the command's reply arrives or its provider task terminates after cancellation at `ControlRequestTimeout`. A hung provider task keeps the slot; the watchdog still latches loss independently.
3. When the slot frees, the next command is chosen in order: (a) resubmission/resolution of an unresolved Begin, Finish or ReleaseReservation (same ID, same body); (b) Finish or ReleaseReservation; (c) Begin; (d) Renew.
4. Renew carries a command ID for correlation only and **no receipt** (R1). A Renew whose reply is lost is not resolved by lookup: the next due Renew is a new command with a new ID. Its fresh reply alone can advance `d`.
5. A stale Renew observed after a possibly committed Finish is never interpreted as a failed execution: Finish is resolved first.
6. The arbiter is never held across handler execution. Different attempts proceed independently under bounded I/O.

## 7. Readiness coordinator

### 7.1 Interface

```csharp
internal sealed record LaneKey(string Queue, string Priority);
internal sealed record ReadyLane(LaneKey Lane, DateTimeOffset OldestEligibleAt);
internal sealed record ReadySnapshot(DateTimeOffset ObservedAt, IReadOnlyList<ReadyLane> Lanes);

internal interface IWorkReadinessStore
{
    Task<ReadySnapshot> ProbeAsync(
        string application,
        IReadOnlyList<LaneKey> lanes,
        IReadOnlyList<JobContract> supportedContracts,   // ≤ 512, deduplicated
        CancellationToken cancellationToken);
}
```

PostgreSQL shape — one round trip, one indexed lookup per requested lane, no locks, no payload, no counting:

```sql
SELECT l.queue, l.priority, r.eligible_at AS oldest_eligible_at,
       statement_timestamp() AS observed_at
FROM unnest(@queues::text[], @priorities::text[]) AS l(queue, priority)
CROSS JOIN LATERAL (
    SELECT j.eligible_at
    FROM runwick.jobs AS j
    WHERE j.application = @application
      AND j.queue = l.queue AND j.priority = l.priority
      AND j.state IN ('pending', 'retrying')
      AND j.eligible_at <= statement_timestamp()
      AND EXISTS (
          SELECT 1
          FROM unnest(@names::text[], @versions::integer[],
                      @profile_ids::text[], @profile_versions::integer[])
               AS c(name, version, profile_id, profile_version)
          WHERE c.name = j.job_name AND c.version = j.payload_version
            AND c.profile_id = j.serializer_profile_id
            AND c.profile_version = j.serializer_profile_version)
    ORDER BY j.eligible_at, j.job_id
    LIMIT 1
) AS r
WHERE NOT EXISTS (
    SELECT 1 FROM runwick.queue_controls AS qc
    WHERE qc.application = @application AND qc.queue = l.queue AND qc.paused);
```

Only nonempty lanes are returned. Lane and tuple arrays have equal lengths, non-null values and are bounded (lanes ≤ subscribed queues × 3; tuples ≤ 512). The probe is a hint: a stale nonempty lane can lead to `NoWork`; pause exclusion here does not replace Reserve's control-row check. The probe never bypasses resolution of an already-sent command.

### 7.2 Polling group and triggers

*[choice]* One polling group per (store, application). Its probe covers the union of all its pools' lanes; the cached snapshot is filtered back to each pool's subscriptions.

| Rule | Value |
|---|---|
| Probes in flight per group | 1 |
| Maximum hint age | 2 s (measured by local monotonic time since the snapshot was received); an expired snapshot requires a fresh probe before lane selection |
| Idle backoff | Nominal 100 ms doubling to 2 s (10 s while the wake listener is healthy, Changes 2026-11-02, idle caps), each delay × (1 + u), u uniform in [−0.20, +0.20]; reset on an Applied reservation or a useful wake hint |
| Notification-triggered probe spacing | ≥ 50 ms per group |
| Reconnect / resubscribe | Triggers a fresh probe |

One bounded wake signal (capacity 1, extra signals coalesced) carries: optional LISTEN notifications, local dispatch commits in the same process, ticket retirements, and control changes. No task is allocated per notification. Missed notifications are repaired by polling; the worker must discover all due work with every notification dropped (`driver-contract-suite:OBS-01`).

## 8. Pool coordinator and lane selection

### 8.1 Lanes and weights

Lanes are `(queue, priority)` pairs; priority ∈ {`low`, `medium`, `high`} and is an equality predicate, never a sort. Lane weights `high = 6`, `medium = 3`, `low = 1`. Queue weights default 1; configured enabled weights are integers 1–1,000; weight 0 is not allowed (unsubscribe instead). Priority modes: `weighted` (default, with aging), `strict-with-aging`, `strict` (no aging; may starve lower lanes — documented, not a defect).

Selection and settlement of claims are serial per pool; execution is concurrent. Weights divide **admission opportunities**: one unit per Applied reservation, independent of execution duration. They do not reserve CPU, preempt running handlers, give strict FIFO, or synchronize replicas.

### 8.2 Per-pool state

All integers are 64-bit with checked arithmetic.

```text
queueScore[q]            initial 0
laneScore[q][p]          initial 0
agingCount[q]            0..AgingEvery (saturating at 8)
agingPointer[q]          rotating index over q's lanes
cursor[q][p]             CandidateCursor? (eligible_at, job_id), initial null
cooldownUntil[q], cooldownUntil[q][p], cooldownStep[...]   monotonic instants / backoff index
roundReserves            0..32
tieBreakStart            rotating start index for ties (queues, lanes)
```

### 8.3 Opportunity algorithm

```text
loop:
  if admission closed for pool/group or pool suspended: wait for wake
  if !gate.TryAcquire(): wait for wake (ticket retirement / target change)
  snapshot := readiness.Current (refresh if older than 2 s)
  E := { q ∈ subscriptions | weight(q) > 0, now ≥ cooldownUntil[q],
         ∃ p: (q,p) ∈ snapshot, now ≥ cooldownUntil[q][p], not excluded this round }
  if E empty: release ticket; idle backoff; continue
  q := argmax_{x∈E} (queueScore[x] + weight(x))            ; ties: rotating tieBreakStart
  (p, aging) := chooseLane(q)                               ; §8.4, §8.5
  laneVisit(q, p, aging)                                    ; §8.6
  if roundReserves ≥ 32: roundReserves := 0; yield (await Task.Yield / next scheduling turn)
```

### 8.4 Weighted lane choice (smooth weighted round robin)

For the chosen queue `q` with eligible lanes `L`:

1. Trial scores `t[x] = laneScore[q][x] + w[x]` for x ∈ L. Pick the highest; ties by rotating start.
2. On an `Applied` reservation: commit `laneScore[q][x] = t[x]` for all x ∈ L, then `laneScore[q][winner] -= Σ_{x∈L} w[x]`. The queue level is committed the same way over `E`: `queueScore[x] = queueScore[x] + w[x]` for x ∈ E, then `queueScore[q] -= Σ_{x∈E} w[x]`.
3. On `NoWork`, `QueuePaused`, `CapacityBlocked`, `CommandExpired` or any non-Applied result: discard all trial scores (no successful-admission share consumed).
4. Membership change (lane/queue enters or leaves the eligible set): clamp every carried score to ± the new total weight of that level. A lane or queue returning from ineligibility starts at 0. A weight configuration revision resets all scores of the affected level to 0.

In `strict` and `strict-with-aging` modes step 1 picks the highest-weight eligible lane; queue-level selection is still weighted.

Under a stable continuously claimable set this yields queue shares proportional to queue weights (1:3 → 2,500/7,500 of 10,000) and lane shares 6,000/3,000/1,000 of 10,000 without aging.

### 8.5 Aging

Parameters: `AgingEvery = 8`, `AgingThreshold = 60 s`. Enabled in `weighted` and `strict-with-aging`; disabled in `strict`.

- `agingCount[q]` increments (saturating at 8) on every Applied reservation in `q` that was not an aging selection, including when no lane is aged.
- Lane age = `(snapshot.ObservedAt − OldestEligibleAt) + monotonicElapsedSinceSnapshotReceived`. A lane is **aged** when `age >= AgingThreshold` (equality counts — C-1).
- *[choice]* A **lower lane** is an eligible lane of `q` whose weight is lower than the highest weight among `q`'s currently eligible lanes.
- When `agingCount[q] == 8` and at least one aged lower lane exists, the next lane choice in `q` is an **aging attempt**: start at `agingPointer[q]`, pick the first aged lower lane in rotation.
- Aging attempt `Applied`: `agingCount[q] := 0`; reset every `laneScore[q][*] := 0`; queue level is charged normally.
- Aging attempt not Applied: advance `agingPointer[q]` past the tried lane; apply the normal outcome handling (§8.6) and budgets; then regular weighted selection may proceed for the next opportunity. `agingCount[q]` stays 8.
- Aging never rewrites persisted priority. It is an intentional modification of the weighted ratios, not a start-deadline guarantee.

### 8.6 Lane visit, cursors and outcomes

A lane visit issues Reserve commands for one lane until it ends. Each command uses `After = cursor[q][p]`; `roundReserves` increments for every Reserve sent (successful or not). Budgets: at most **8 blocked candidates per lane visit**; at most **32 Reserve calls per pool scheduling round**.

| Reserve result | Cursor | Hint / cooldown | Scores | Visit |
|---|---|---|---|---|
| `Applied` | Unchanged | Reset cooldown steps of lane and queue; idle backoff reset | Commit (§8.4/§8.5) | Ends; ticket → AttemptEntry |
| `NoWork` | `cursor := null` (wrap) | Remove lane from snapshot (hint invalidated) | Discard | Ends |
| `QueuePaused` | Unchanged | Cool down queue `q` (all lanes) | Discard | Ends |
| `CapacityBlocked`, queue scope covering `q` | `cursor := ResumeAfter` | Cool down all lanes of `q` | Discard | Ends |
| `CapacityBlocked`, job-type scope and lane configured single-type | `cursor := ResumeAfter` | Cool down lane | Discard | Ends |
| `CapacityBlocked`, resource or unknown scope | `cursor := ResumeAfter` | — | Discard | Continues on the same lane in a **new** command/transaction while blocked count < 8 and `roundReserves` < 32; on reaching 8 blocked candidates: cool down lane, end |
| `CommandExpired` (definitive) / confirmed rollback | Unchanged | — | Discard | Ends |
| Unknown commit | Unchanged | — | Discard (pending) | Suspend pool fresh claims; resolve same command (§9.2) |
| `AdmissionClosed`, `UnsupportedVersion` | Unchanged | — | Discard | Close admission for the group; readiness unhealthy |
| `CommandConflict`, malformed reply | Unchanged | — | Discard | Invariant fault: critical (§2 rule 3) |

For every non-Applied, non-unknown result the PendingClaim retires and releases its ticket; the next Reserve acquires a ticket again. Cursors are retained across rounds. Polling and cooldown expiry eventually recheck older skipped candidates and newly eligible jobs before the cursor. Recent examples of one job type in a lane are only a cost hint and never authorize a lane exclusion.

Cooldown: nominal 250 ms doubling per consecutive cooldown of the same lane/queue to 2 s, × (1 + u), u uniform in [−0.20, +0.20]; always positive, so wake storms cannot create a tight scan loop. Notifications cannot erase a cooldown; a relevant control revision (pause/resume, scope policy) or a known local release may trigger an earlier recheck, still subject to probe spacing.

### 8.7 Blocked-scope classification

`StoreError.BlockedScopeId` is classified *[choice]* using the `scope_id → (kind, key)` map returned by startup provisioning (§3.1 step 4): `queue` scope whose key is `q` → queue-wide; `job-type` scope → type scope (spelling aligned with 02/03/04 on 2026-10-30); `resource` scope or any ID absent from the map → unknown. Unknown/stale associations fall back to bounded scanning and never authorize an indefinite exclusion. Local resource-key gates are not part of the engine.

### 8.8 Cross-pool admission I/O

Reserve and Begin share the work data source's admission class (4 concurrent calls). Per pool, at most 1 Reserve (including resolution of an uncertain claim) is outstanding. Within the class, Begin and claim-resolution work that already own tickets precede fresh Reserve requests; fresh Reserves of different pools are served round robin, one request per turn.

## 9. Claim and Begin

### 9.1 Reserve command

`StoreCommand<ReserveRequest>(Application, CommandId = Guid.CreateVersion7(), NotAfter = wallNow + OwnershipCommandLifetime (30 s), Body)` with `ReserveRequest(WorkerInstanceId, Queue, Priority, SupportedContracts (≤512), After, LeaseDuration = 60 s)`. The body and ID are immutable once sent; arguments are never changed before the command resolves. Each request waits at most `ControlRequestTimeout` (5 s), including I/O admission; control retries use nominal 100 ms exponential backoff capped at 1 s with equal jitter.

On a fresh (`Replayed = false`) Applied reply, record `s` = local monotonic timestamp immediately before sending and set `d = s + (Lease.ExpiresAt − DecidedAt) − LeaseSafetyMargin`. A replayed Applied reply establishes no deadline: the entry is marked `needsFreshRenew`.

### 9.2 Claim resolution

- Unknown commit: keep the ticket, resubmit the **same** command (same ID, same body). The pool issues no other claim (fresh claims in that pool are suspended immediately).
- `ClaimResolutionBudget` = 15 s from first submission. On expiry: keep resolving with backoff, keep the pool's admission suspended, surface the unresolved claim in health/diagnostics. Never create a succession of replacement reservations.
- A resubmission after `NotAfter` returns either the recorded result (receipt read precedes the deadline check) or `CommandExpired` (definitively not applied).
- A claim resolved `Applied` during stopping is tracked like any other reservation and then released (§16).

### 9.3 Begin pre-checks

Before submitting Begin from `Reserved`:

1. Register the lease with the LeaseCoordinator (done on entry creation) and retain the exact registration/policy snapshot.
2. If `needsFreshRenew` or `d − now < MinimumInvocationMargin` (15 s): renew first through the arbiter. If a fresh renewal cannot be confirmed, do not Begin.
3. Admission still open (drain has not started; §16).

Begin command: same ownership lifetime (30 s), `BeginRequest(LeaseHandle)`. Begin's result retains the original lease; it grants no new lease. A fresh Applied Begin reply may update `d := max(d, s_begin + (ExpiresAt − DecidedAt) − LeaseSafetyMargin)`.

### 9.4 Begin results

| Result | Engine action |
|---|---|
| `Applied`, fresh | Invocation admission (§9.5) |
| `Applied`, replayed (after unknown) | Fresh ownership-checked Renew; inspect `CancellationRequested`; require margin; then §9.5. A replay alone never authorizes invocation |
| `InvalidState` / `cancelled_before_start` (Effect Applied) | Durable cancellation closed the unstarted attempt; no scope, no invocation → `Retired` |
| `BudgetExhausted` | Storage terminalized; → `Retired` |
| `StaleLease` | → `OwnershipLost(StaleLease)` |
| Unknown commit | Resubmit same command; no invocation while unresolved; pool fresh claims suspended; `BeginResolutionBudget` 15 s from first submission. On expiry: no invocation ever, stop renewal, → `OwnershipLost(BeginUnresolved)`, pool stays suspended; resolution continues until resolved or control-I/O stop. A later `Applied` resolution still does not invoke (the job is recovered as a running crash) |
| `CommandExpired` | Begin definitively not applied; reachable only after budget expiry; entry stays abandoned |
| `RateLimited` | Not in M2: invariant fault. Entry → `Retired` (the store committed the closure); group admission closed; readiness unhealthy |
| `AdmissionClosed`, `UnsupportedVersion` | Close group admission; stop renewing the entry → `OwnershipLost`; recovery handles it |

### 9.5 Invocation admission

Atomically, under the entry's short lock:

1. Require: no loss latch; `d − now ≥ MinimumInvocationMargin` (else renew first, as §9.3); no durable cancellation observed (if observed, do not invoke — finalize via Finish with `UserCancelled` without a scope); drain cutoff not reached (§16).
2. Latch invocation **once** (a second latch attempt is a no-op that returns "already invoked").
3. Record the local execution-start timestamp; the pipeline clock starts here.
4. Queue the runner on the shared thread pool (§10.1). Phase → `Running`.

A counted Begin is never permission to ignore a later loss.

## 10. Attempt runner

### 10.1 Order

1. Queue to the thread pool *[choice]* with `ExecutionContext` flow suppressed and **without** a cancellation token that could prevent the runner's bookkeeping or `finally` from running. No dedicated thread per job. User code never runs inline on a coordinator loop.
2. On start, recheck: loss latch (if latched: skip decode/DI/handler; the Begin stays counted; retire when nothing remains) and pipeline deadline (if `ExecutionTimeout` elapsed: no scope, Finish `TimedOut`).
3. Decode the original UTF-8 bytes with the registered serializer profile and run the upgrade chain to the current type. Upgrades are pure and deterministic. Malformed JSON or a deterministic conversion failure → `PermanentFailure` (payload). If a tuple the worker advertised cannot be resolved → configuration invariant fault: close admission for affected pools and *[choice]* latch `OwnershipLost(InvariantFault)` without a Finish, so recovery returns the job instead of discarding it.
4. Create one async DI scope (`IServiceScopeFactory.CreateAsyncScope()`); initialize the scoped `IJobContext` before resolving any middleware or the handler.
5. Run the cached pipeline factory with per-attempt instances; the execution `CancellationToken` is created inside the runner and passed to middleware/handler.
6. When the pipeline ends, capture the observed outcome and monotonic elapsed time **once** (frozen).
7. Phase → `CleaningUp`: `await scope.DisposeAsync()` after all awaited middleware/handler work (including joined downstream tasks) has ended. Cleanup status is tracked separately from the frozen outcome.
8. Publish the immutable Finish request (phase → `Finishing`); retry only storage finalization, never the pipeline. Release the ticket only on `Retired`.

`IJobContext` exposes read-only: job/attempt/generation IDs, execution number, queue/priority, original and current payload versions, upgraded payload access (`GetPayload<TJob>()` checked), trace metadata, cancellation causes, resource fencing values; and a per-attempt `Items` collection. It exposes no database connection, lease authority, or Finish capability. Cached pipeline factories never capture context or scoped services. Detached tasks started by user code are outside the engine's lifetime guarantees.

### 10.2 Middleware pipeline

```csharp
internal enum PipelineDecision { Succeeded, Retry, Failed, Cancelled }
internal sealed record PipelineResult(
    PipelineDecision Decision, string? ReasonCode, TimeSpan? RetryDelay, AttemptError? Error);
internal delegate ValueTask<PipelineResult> JobContinuation();
```

Order: global middleware (`UseMiddleware<T>()`, registration order) wraps per-job middleware (aliases, `Order`), which wraps the terminal handler. The terminal handler returns `Succeeded` when `HandleAsync` completes normally. Public outcomes: `JobExecutionResult.Succeeded()`, `Retry(reason, delay?)`, `Failed(code, message)`, `Cancelled(reason)`.

Guarded `next` (one guard per middleware frame):

| Misuse | Guard behavior | Outcome |
|---|---|---|
| `next` called a second time | Second call does not invoke downstream; throws a pipeline-contract exception into the caller; cancels downstream if still running | `PermanentFailure`, code `pipeline_contract_fault` |
| `next` called after the middleware returned | Rejected; downstream not invoked | `PermanentFailure`, `pipeline_contract_fault` |
| Middleware returns while `next` still running | Cancel downstream; **join** the downstream task before scope disposal (may hang if user code ignores cancellation; grace/abandonment rules apply) | `PermanentFailure`, `pipeline_contract_fault` |
| Short-circuit returning `Succeeded` without calling `next` | Legitimate | `Succeeded` |

Middleware cannot extend lease authority, reset the execution budget, refresh leases, finalize the job, or suppress a latched timeout.

### 10.3 Outcome classification

Evaluated once on the frozen observation, in this order:

| # | Observation | Finish outcome |
|---|---|---|
| 1 | Loss/abandonment latched | No Finish (resolve an already-submitted Finish only) |
| 2 | `ExecutionTimeout` set and `elapsed >= ExecutionTimeout` (including normal return after cancellation) | `TimedOut` |
| 3 | Normal handler return or explicit `Succeeded` | `Succeeded` (a concurrent durable cancellation may still coexist in history) |
| 4 | Pipeline misuse; negative requested retry delay | `PermanentFailure` (`pipeline_contract_fault`) |
| 5 | Explicit `Failed`; deterministic malformed payload / upgrade failure | `PermanentFailure` |
| 6 | Explicit `Cancelled` | `UserCancelled` |
| 7 | `OperationCanceledException` with engine cause `User` | `UserCancelled` |
| 8 | Unsuccessful exit with engine cause `Shutdown` (no timeout, no user cause) | `Interrupted` |
| 9 | Explicit `Retry(reason, delay?)` | `RetryableFailure` with validated delay (below) |
| 10 | `OperationCanceledException` without an engine cause; any other exception | Exception classifier; default `RetryableFailure` (application may classify permanent business failures) |

Storage applies its precedence under the job lock: timely success can win a concurrent user cancellation; an unsuccessful execution with a durable cancellation becomes cancelled; otherwise permanent failure terminates and retryable/timed-out/interrupted outcomes retry only while execution budget remains. The engine does not repeat that arithmetic.

Requested retry delay: negative → contract fault (row 4); otherwise round up to the 1 ms quantum, clamp 0 → 1 ms, cap at the policy `MaximumDelay`; no jitter. The execution is still charged. Error descriptors are bounded (stored detailed exception 16 KiB UTF-8 with explicit truncation marker; safe code/message separate).

### 10.4 Timeouts, cancellation grace, cleanup

- Pipeline clock: from invocation admission (§9.5 step 3) to the end of the complete awaited pipeline. Includes thread-pool queueing, decoding/upgrade, DI construction, middleware/handler. Excludes Begin resolution, scope disposal and Finish acknowledgement. Classification uses the recorded timestamps, not timer delivery.
- `ExecutionTimeout` default: none. When it elapses during execution: record cause `Timeout`, request cancellation, start grace.
- `CancellationGrace` = 10 s from the **first** locally observed `User`/`Timeout`/`Shutdown` cancellation request; repeated signals do not reset it. If the pipeline exits within grace: cleanup and finalize under remaining authority. If not: stop renewing, latch `OwnershipLost(GraceExpired)`; do not issue Finish, do not dispose the scope concurrently with live code, do not release permits or the ticket.
- `CleanupBudget` = 10 s from starting scope disposal. Cleanup exception after a timely success: diagnostic only; Finish `Succeeded`. Disposal still running at expiry: stop renewing, latch `OwnershipLost(CleanupExpired)`, retain ticket and task; report **unconfirmed completion** (never safe success, never a cleanup-induced retry). Cleanup must complete before a Finish is issued.
- Cancellation is dispatched with `CancellationTokenSource.CancelAsync()` off the LeaseCoordinator, tracked as a task with observed exceptions. Slow or throwing callbacks never delay other renewals. Completion of `CancelAsync` does not prove user work ended; the handler task is tracked separately.

## 11. Lease coordinator and renewal math

### 11.1 Authority deadline

For a fresh (non-replayed) Reserve/Begin/Renew reply:

```text
s = local monotonic timestamp immediately before sending (TimeProvider.GetTimestamp)
t = local monotonic receive time
r = Lease.ExpiresAt − DecidedAt            (database values from the reply)
d = s + r − LeaseSafetyMargin              (local authority deadline)
usable margin at receive = d − t
```

Defaults: r = 60 s, safety 10 s ⇒ `d = s + 50 s`; a reply arriving 4 s after sending leaves 46 s. A replayed result never establishes or refreshes `d`. A loss-latched entry ignores any later `d`. `now >= d` means lost (equality is expiry). Database expiry checks remain authoritative; a database clock jump can invalidate the estimate.

### 11.2 Scheduling

- One due-time priority queue keyed by `(due, attemptId)` with **one** current item per attempt. Rescheduling replaces the item (per-attempt version stamp; stale heap entries are compacted when they exceed the live count), so stale entries stay bounded.
- Next renewal due, computed from the send time of the last fresh request:

```text
J   ~ uniform [RenewalIntervalMin, RenewalIntervalMax] = [12 s, 18 s], sampled per scheduled renewal  [choice]
due = min(s + J, d − (2 × ControlRequestTimeout + ControlRetryBackoffMax + SchedulerAllowance))
    = min(s + J, d − 13 s)            ; defaults: d − 13 s = s + 37 s, so due = s + J ≤ s + 18 s
```

- If the reply arrives after `due`, enqueue the renewal immediately (do not sleep another interval); if `now >= d`, latch loss instead.
- Renewal I/O uses the control data source's renewal lane (4 concurrent calls), earliest safety deadline first, ahead of any new reservation I/O.
- Renew command: new ID per request, NotAfter = wallNow + 30 s, `RenewRequest(LeaseHandle, LeaseDuration = 60 s)`, request timeout 5 s. Transient error/timeout: retry (a new Renew) with control backoff (100 ms → 1 s, equal jitter) while `now < d` and the arbiter is free. Each request is bounded by the shorter of request timeout, remaining resolution budget and `d − now`.
- Renewal continues in `Reserved`, `Beginning` (between resolved commands), `Running`, `CleaningUp` and `Finishing` (between resolved commands). Cleanup/resolution budgets may span many successful renewals. A job with no `ExecutionTimeout` renews indefinitely while healthy.

### 11.3 Renew results

| Result | Action |
|---|---|
| `Applied`, fresh | `d := s + r − safety` (if not latched); schedule next due; if `CancellationRequested`, record cause `User` and request cancellation (Running) or let Begin observe it (`Reserved` → Begin returns `cancelled_before_start`) |
| `StaleLease`, `InvalidState` | Latch `OwnershipLost`; request cancellation (`LeaseLost`); stop renewing |
| Transport error / timeout (unknown) | No receipt resolution; next Renew is a new command (§6 rule 4); `d` unchanged |
| Permit mismatch / invariant fault | Latch `OwnershipLost(InvariantFault)`; diagnostics |

Renew writes no receipt and no `queue_events` row.

### 11.4 Safety watchdog

A periodic timer with interval `LeaseWatchdogInterval` ≤ 1 s checks every entry's `d`. It runs independently of the arbiter, the renewal I/O lane and any hung database request. Delayed timer delivery never extends a deadline. On `now >= d`: latch `OwnershipLost(SafetyDeadline)`, record cause `LeaseLost`, dispatch cancellation, stop renewing. Replies to requests that were in flight are then historical: they may resolve a submitted Finish/Release/Begin but never revive the attempt. No local timer proves PostgreSQL expired the lease.

### 11.5 Effects of a loss latch

Stop renewal; never submit a new Finish, Begin or ReleaseReservation; continue resolving an already-submitted Finish/Release from its receipt (a successful receipt is never reinterpreted as failure because the local deadline elapsed); keep observing and cleaning up live user work; keep the ticket; leave takeover to durable expiry and recovery. Distributed permits expire on their own; another process may retry the job. External side effects need application idempotency/fencing.

## 12. Completion coordinator

### 12.1 Structure

- At most one frozen completion per entry; the authoritative outcome stays on the entry until acknowledged.
- Ready-ID channel: `Channel.CreateBounded<AttemptKey>` with capacity = group attempt maximum (16), `BoundedChannelFullMode.Wait` (authoritative; never drops). It holds IDs only.
- Wake hints (coalesced) use a separate capacity-1 channel with `DropWrite`; registry scanning recovers anything a dropped hint would have signalled.
- A bounded worker set equal to the settlement lane (2 concurrent calls) processes ready IDs; failed writes are retried with backoff by re-enqueueing the ID, never by an unbounded sleeping task per failure.

### 12.2 Finish command

`FinishRequest(LeaseHandle, Outcome, ExecutionElapsed, RequestedRetryDelay, Error)` with a fresh command ID at first submission and NotAfter = wallNow + 30 s. ID and exact body are frozen for every retry. Finish passes through the arbiter (§6); while it is unresolved no Renew is issued for that attempt.

### 12.3 Budget

`CompletionResolutionBudget` = 30 s from publication of the frozen Finish (after cleanup), including time waiting in the channel. Each request is bounded by min(`ControlRequestTimeout`, remaining budget, remaining shutdown time). Retries use control backoff (100 ms → 1 s, equal jitter). On budget expiry: stop renewal, latch `OwnershipLost(CompletionUnresolved)`, keep resolving for diagnostics until control-I/O stop, report **unconfirmed outcome**, leave durable recovery to lease expiry. Never rerun the pipeline.

### 12.4 Finish results

| Result | Action |
|---|---|
| `Applied` (fresh or `Replayed = true`) | Stop renewal; retire after cleanup (already complete) |
| `StaleLease`, `InvalidState` | Definitive: ownership was gone; retire without overwriting the stored outcome |
| Unknown commit | Resubmit the same command (same ID/body) |
| `CommandExpired` on resubmission | Definitively not applied (no receipt existed when checked after the deadline). *[choice]* No new Finish ID; entry → `OwnershipLost(CompletionUnresolved)`; recovery applies |
| Receipt `ExpiredUnknown` | Inspect current job/attempt history; emit unknown-durability diagnostic; never invent success/failure, never reapply |

## 13. Recovery loop

- Cadence: nominal 5 s, each interval uniform in [4 s, 6 s]. Runs on every worker; storage arbitrates races by exact attempt/token.
- Probe the expired-lease index in the background I/O class:

```sql
SELECT job_id, current_attempt_id, ownership_token
FROM runwick.jobs
WHERE application = @application
  AND state IN ('reserved', 'running')
  AND lease_expires_at <= clock_timestamp()
ORDER BY lease_expires_at, job_id
LIMIT 32;            -- [choice] bounded batch per scan
```

- For each candidate, sequentially, issue `Recover` (`RecoverRequest(JobId, ExpectedAttemptId, ExpectedOwnershipToken)`, fresh ID, NotAfter = wallNow + 30 s; unknown → resubmit same command). Storage rechecks token and expiry after locks; `StaleLease` (renewed after the probe) is skipped.
- Storage effects (not engine logic, restated for test assertions): reserved expiry closes the attempt `abandoned_before_start`, increments the consecutive-unstarted count `k`, and fails the generation with `reservation_abandonment_exhausted` when `k >= MaxConsecutiveUnstartedAbandonments` (5); otherwise restores `pending` if `started_count = 0`, else `retrying`, with the unstarted delay. Running expiry closes `abandoned_running` using the execution already counted; retries if budget remains, else fails; a durable cancellation request wins. Only the expired attempt's permits are deleted.
- Presence expiry never triggers recovery. The loop stops at control-I/O stop (T0 + 25 s).

## 14. Presence loop

Publishes: worker instance ID, application, subscribed queues, capability revision and full decoding tuples (≤512), pool targets, build identifier, status (`starting`, `ready`, `draining`, `degraded`, `stopping`) and heartbeat expiry. Cadence 5 s ± 10% (uniform [4.5 s, 5.5 s]); expiry 20 s in store time. First publication happens before admission is enabled (§3.1). Uses the background I/O class (presence/readiness take precedence over heavy background work). Failures retry with backoff and only degrade observability. Presence is never lease authority; its expiry changes nothing about job ownership. No `queue_events` heartbeat rows are written.

## 15. Pressure gates and pool suspension

Control observations use local monotonic time and never depend on exported metrics.

### 15.1 Derived margins

```text
usableAuthority = LeaseDuration − LeaseSafetyMargin                      = 50 s
StopMargin      = usableAuthority − RenewalIntervalMax − ControlRequestTimeout − SchedulerAllowance
                = 50 − 18 − 5 − 2 = 25 s     (startup rejects StopMargin <= 0)
ResumeMargin    = StopMargin + ControlRequestTimeout = 30 s
```

### 15.2 Gates

| Gate | Scope | Condition | Effect |
|---|---|---|---|
| Unresolved claim/Begin | Pool | Any Reserve or Begin with unknown commit | Fresh claims in that pool stop immediately; free tickets are not used to build an uncertainty backlog |
| Claim/Begin budget expiry | Pool | `ClaimResolutionBudget` / `BeginResolutionBudget` expired unresolved | Pool admission suspended until the command resolves |
| Pressure stop | Worker/store group | An actionable control item (renewal/settlement) has waited 1 s for I/O; or any still-owned attempt has confirmed margin `d − now < StopMargin`; or the oldest pending Finish has waited 5 s; or an arbiter-blocked item exists | Stop new Reserve admission; continue resolving, renewing and settling |
| Timer lag | Group | Coordinator timer lag sampled once per second; lag > `SchedulerAllowance` (2 s) | Stop refill |
| Host pressure | Group | Configured host-pressure signal | Stop refill |
| Critical fault / incompatibility | Group | §2 rule 3; `AdmissionClosed`; `UnsupportedVersion`; `RateLimited`; invariant fault | Close admission (no automatic resume for invariant faults) |

Resume a pressure stop only after **three consecutive 1 s healthy checks**, each with: no expired or unresolved owner command; no control wait above 250 ms; minimum confirmed live margin ≥ `ResumeMargin` (no owned attempts satisfies this); no pending Finish older than 1 s; timer lag ≤ 2 s. Authority loss stays latched per attempt. Gates may pause admission below a configured minimum; safety always wins. CAP-10 must confirm the stop gate does not flap under ordinary jitter (healthy lowest margin before a due renewal ≈ `usableAuthority − RenewalIntervalMax − RTT` ≈ 31 s). Growth-freeze gates (p95 latency, admission-block ratio, timer lag > 250 ms) apply only to the automatic controller, which is not in M2.

## 16. Staged shutdown

Three separate lifetime controls: admission stop; per-attempt execution cancellation; control-I/O shutdown deadline. The host stopping token is never passed to Finish, ReleaseReservation or drain renewals.

`T0` = first locally observed stop signal (`IHostApplicationLifetime.ApplicationStopping` or `StopAsync` entry, whichever is first). Cutoffs are **absolute** from T0 and are not restarted by a later `StopAsync` or per attempt:

```text
DrainCutoff        = T0 + ShutdownDrain                                 = T0 + 15 s
CancellationCutoff = DrainCutoff + ShutdownCancellation                 = T0 + 20 s
ControlStopCutoff  = CancellationCutoff + ShutdownFinalization          = T0 + 25 s  (≤ T0 + ShutdownBudget)
```

Host cancellation (the `StopAsync` token, including one already cancelled on entry) overrides every phase: it acts as reaching `ControlStopCutoff` immediately.

| Step / interval | Behavior |
|---|---|
| 1. At T0 | Atomically close ticket acquisition and Reserve issuance; publish `draining`; readiness false (liveness unchanged); cancel/wake idle probes. A Reserve already in flight stays tracked even if its reply arrives while stopping. No new Begin is submitted |
| 2. T0 → DrainCutoff: resolve claims, release unbegun | Finish bounded claim resolution. Every confirmed reserved-not-begun attempt (`Reserved`, or an Applied claim arriving now) → `Releasing`: issue `ReleaseReservation` (ownership-checked, `ReleaseRequest(LeaseHandle)`, fresh ID, NotAfter 30 s; unknown → same ID). Applied → attempt closed `released`, permits deleted, job restored to `pending` if `started_count = 0` else `retrying` with unchanged `eligible_at`, no execution/abandonment charge (a durable cancellation terminalizes instead). Never manufacture a user cancellation or a Begin to release. A release unconfirmed at DrainCutoff → stop renewing, `OwnershipLost(ReleaseUnconfirmed)`; expiry recovery applies the guard (the exception path) |
| 3. T0 → DrainCutoff: graceful drain | Already-begun executions continue; renewal, completion writes and task observation continue. An attempt whose Begin was already submitted may be invoked if Begin resolves Applied before DrainCutoff and §9.5 passes. *[choice]* A Begin resolving Applied at/after DrainCutoff is not invoked; Finish `Interrupted` without a scope |
| 4. DrainCutoff → CancellationCutoff | Cancel remaining execution tokens (cause `Shutdown`; grace starts if not already started). Cooperative exits clean up and finalize with their actual outcome (§10.3) while authority remains; renewals continue |
| 5. CancellationCutoff → ControlStopCutoff | *[choice]* Pipelines still live at CancellationCutoff latch `OwnershipLost(GraceExpired)` (stop renewal, keep ticket/scope, no Finish); grace is shortened by this boundary. Remaining time serves cleanup of already-ended pipelines, their Finish submission and pending receipt resolution. No new execution |
| 6. At ControlStopCutoff (or host cancellation) | Stop renewal, recovery and presence; cancel remaining I/O; observe available results; mark remaining ownership/outcomes unresolved; return from `StopAsync`. Never acknowledge running code to empty the registry |

All ordinary budgets are shortened by the applicable boundary: claim/Begin/completion resolution and cleanup by `ControlStopCutoff`; cancellation grace by `CancellationCutoff`. Noncooperative handlers or disposal may outlive `StopAsync`: their records, tickets and scopes are retained while the process lives, eventual task faults are observed, and stuck-worker health is exposed. Runwick cannot hard-kill user code or guarantee DI after host disposal. Abrupt process death skips this sequence; durable leases handle it.

Deployment notes: .NET 10 `HostOptions.ShutdownTimeout` defaults to 30 s and hosted services stop sequentially, so Runwick may receive less; register the worker hosted service so it stops last (equivalently, `AddRunwick` last among worker-like services). Kubernetes `terminationGracePeriodSeconds` defaults to 30 s and SIGTERM precedes the host token. Runwick never overwrites host options.

## 17. Health

Registered with `Microsoft.Extensions.Diagnostics.HealthChecks`; checks read recent coordinator state and never reserve a job or perform unbounded I/O. Public probes return minimal status; protected diagnostics may show component state, compatibility and profile, never connection strings or secrets.

| Check | Healthy | Degraded | Unhealthy |
|---|---|---|---|
| Liveness | Supervisor progress tick advanced within 10 × `LeaseWatchdogInterval` *[choice]* and loops handling any outage | — | Tick stalled beyond that bound. Store outage alone never makes liveness unhealthy (avoid restart storms) |
| Readiness | Initialized; all critical coordinators running; store/application compatible; admission enabled in every pool | Admission enabled in at least one pool but another pool suspended (unresolved command, pressure); cleanup hung; stuck task after loss; presence publication failing | Not initialized; draining/stopping; critical fault; incompatible store/profile; admission closed for every pool (including store outage or pressure stop) |

During drain readiness is unhealthy while liveness stays healthy. Zero backlog, a paused queue, or unsupported backlog is not a failure. A worker unready due to store outage still renews, settles and recovers within the safety rules.

**Change 2026-10-30 (SP-009, contradiction with the package graph).** The `Runwick` package may reference only DI.Abstractions, Hosting.Abstractions, Logging.Abstractions and Options (eng/package-graph.json). `Microsoft.Extensions.Diagnostics.HealthChecks` is therefore not referenced in M2. The engine exposes the two checks as an internal snapshot, `WorkerEngine.Health()`, which reports liveness, readiness (`Healthy`/`Degraded`/`Unhealthy`), a status and bounded diagnostics, using the rules of the table above. Registering a `HealthChecks` adapter is deferred until a package that may take that dependency exists (`Runwick.AspNetCore`, M5).

## 18. I/O class mapping

| Operation | Data source / class | Limit |
|---|---|---|
| Renew | Control / renewal lane | 4 concurrent |
| Finish, Finish resolution, ReleaseReservation *[choice]* | Control / settlement lane | 2 concurrent |
| Reserve, Begin, their resolution | Work / admission class | 4 concurrent (per pool ≤ 1 outstanding Reserve) |
| Dispatch (producer in same process) | Work / producer-admin class | 2 concurrent; 64 waiters, 1 s admission wait, else `local_capacity_exceeded` |
| Probe, presence, recovery, receipt cleanup | Work / background class | 2 concurrent; heavy work ≤ 1 of 2 |
| Optional LISTEN | Dedicated session | 1 |

No borrowing between renewal and settlement. A call keeps its slot until the provider task terminates and its connection is disposed, even after the local wait deadline; a slot is never freed because `Task.WhenAny` returned.

## 19. Worker validation matrix

Deterministic engine tests use fake `TimeProvider`, controllable storage replies and task barriers; they prove task ownership and sequencing, not SQL isolation, and are paired with real PostgreSQL race tests.

| ID | Scenario | Required behavior / assertion |
|---|---|---|
| WRK-01 | One slow handler among fast ones (pool 4; one handler blocked, three complete) | Each retired ticket refills independently; outstanding stays 4 with the blocked one held; new Reserves are issued as the fast ones retire; no batch-wide wait |
| WRK-02 | Target reduction with in-flight claims (target 4 → 1 while 4 tickets outstanding) | Outstanding may exceed the new target; no ticket is acquired until outstanding < 1; no running attempt cancelled |
| WRK-03 | Lost Reserve/Begin reply, delayed duplicate response | Same command ID and attempt on every resubmission; one `AttemptEntry`; at most one DI scope and one local invocation; duplicate reply resolves the operation without recreating or re-invoking |
| WRK-04 | Shutdown while Reserve commits | The returned claim is tracked and released via ReleaseReservation; no handler starts after admission closes unless Begin was already authorized before DrainCutoff |
| WRK-05 | Synchronous handler prefix / blocking constructor | Never executed inline on a pool or lease coordinator thread; coordinator loops continue (renewals on time); thread-pool starvation separately observable |
| WRK-06 | Renewal deadline during slow pipeline | Renewal proceeds independently of the pipeline; uncertain authority latches loss, cancels, and is never revived by a later reply |
| WRK-07 | Finish commits, reply lost, a renewal becomes stale | Finish is resolved by resubmitting the same command; the stale renewal is never reported as a new failed execution; no Renew issued while Finish unresolved |
| WRK-08 | Storage outage after handler success | Pending outcomes bounded by tickets; tasks bounded; no invocation replay; pressure stop closes admission; outcomes acknowledged after recovery or reported unconfirmed at budget expiry |
| WRK-09 | Timeout ignored by handler | No premature scope disposal, permit release, or ticket replacement; after grace: renewal stops, ticket held; late result cannot claim timely success (classified `TimedOut` if ever finalized — it is not, loss latched) |
| WRK-10 | Slow/throwing cancellation callback | Other leases renew on schedule; callback tasks tracked; exceptions observed and logged |
| WRK-11 | Scoped service isolation and async disposal | Exactly one async scope per invoked attempt; `IJobContext` initialized before middleware/handler resolution; no scoped instance shared across attempts; scope disposed via `DisposeAsync` |
| WRK-12 | Middleware calls `next` twice / returns early | No second handler invocation; downstream task joined before disposal; outcome `PermanentFailure`/`pipeline_contract_fault` |
| WRK-13 | Upgrade v1→v2→v3, gap, malformed payload | Advertised tuples include only complete paths (gap version absent); original stored bytes unchanged; malformed supported payload → `PermanentFailure` after Begin; failures observable |
| WRK-14 | Mixed-type lane under a type ceiling | With repeated blocked earlier type, the cursor advances (`ResumeAfter`) and a later unblocked type is reserved within the 8-blocked budget or a later visit; no lane exclusion from examples |
| WRK-15 | Queue ceiling with 10k waiting jobs | Queue-scope `CapacityBlocked` cools all lanes of the queue; Reserve calls per scheduling round ≤ 32 and per lane visit ≤ 8 blocked; one probe per group in flight; not one blocked transaction per job per poll |
| WRK-16 | Dropped/coalesced notifications and reconnect (`driver-contract-suite:OBS-01`) | Polling discovers all due work; reconnect triggers a probe; no duplicate invocation |
| WRK-17 | Scope cleanup throws after timely success | Finish `Succeeded`; cleanup diagnostic preserved; no automatic rerun |
| WRK-18 | Host dies at each shutdown phase | Lease recovery remains possible; no durable acknowledgement for unfinished work; reserved-not-begun jobs either released or later recovered under the guard |
| WRK-19 | Presence expires while a job lease is healthy | Dashboard/presence uncertainty only; renewals continue; ownership unchanged; recovery does not act |

## 20. Timing acceptance cases TIME-01 – TIME-13

All use default settings from 06-defaults unless stated. "Store" assertions are on PostgreSQL rows; times are fake monotonic seconds unless noted.

| ID | Setup | Exact assertions |
|---|---|---|
| TIME-01 | Startup validation | Defaults pass: `18 + 2×5 + 1 + 2 = 31 < 50`; `15 >= 2×5 + 1 + 2 = 13`; `15 + 5 + 5 = 25 <= 25`; `StopMargin = 25 > 0`. Each fails startup with a named error: `LeaseDuration = 30 s` (31 ≮ 20; StopMargin = −5); `LeaseSafetyMargin = 0`; `LeaseSafetyMargin = LeaseDuration`; renewal jitter [18 s, 12 s]; `MinimumInvocationMargin = 12 s`; `ControlRequestTimeout = 10 s` (invocation 15 ≱ 23); `ShutdownDrain = 20 s` (30 > 25); `LeaseWatchdogInterval = 2 s`; negative, zero, non-whole-ms or overflowing durations. Passing defaults produce no claim of measured capacity |
| TIME-02 | Renew sent at s = 100; reply DecidedAt = D, ExpiresAt = D + 60 s, received t = 104 | `d = 150`; recorded margin 46 s; next due ∈ [112, 118]. Replayed Begin reply: `d` unchanged and a fresh Renew precedes invocation. Renew sent at 137, store stalls; watchdog latches loss at ≤ 151 (first tick with now ≥ 150); an Applied reply at 152 leaves phase `OwnershipLost`, schedules no renewal, submits no Finish |
| TIME-03 | (a) Begin Applied, invocation latched, thread pool blocked; watchdog latches loss before runner starts. (b) `ExecutionTimeout = 2 s`, runner start delayed 3 s | (a) zero scopes created, zero handler calls, no Finish; store `execution_count` +1 and later `abandoned_running` via recovery. (b) no scope, no handler call; Finish `TimedOut` with `ExecutionElapsed >= 2 s`; with MaxExecutions 3 at execution 1 the job becomes `retrying` |
| TIME-04 | `ExecutionTimeout = 10 s` | Pipeline ends at elapsed exactly 10.000 s → `TimedOut`. Ends at 9.999 s with the timeout callback delivered late at 10.5 s → `Succeeded`. Ends at 9 s and Finish acknowledgement takes 20 s → `Succeeded`, `ExecutionElapsed = 9 s` |
| TIME-05 | Retry exponential-v1, initial 5 s, max 60 s | n = 1: delay ∈ [2500, 5000] ms; n = 2: [5000, 10000]; n = 3 with MaxExecutions 3: terminal `failed`, no eligible_at. With MaxExecutions 10: n = 4 → [20000, 40000]; n = 5 → b = 60000 → [30000, 60000]; n = 64 saturates to 60000 without overflow. Initial 5 ms, n = 1 → [3, 5] ms (ceil). Explicit delay −1 ms → `PermanentFailure` (`pipeline_contract_fault`); 0 → 1 ms; 1.5 ms → 2 ms; 90 s → 60 s; no jitter applied. `eligible_at = DecidedAt + delay`. Lost reply: replay returns identical delay/eligible_at; entropy sampled once |
| TIME-06 | Unstarted recovery, initial 1 s, cap 30 s, guard 5 | Consecutive reserved expiries k = 1 → delay ∈ [500, 1000] ms; k = 2 → [1000, 2000]; k = 3 → [2000, 4000]; k = 4 → [4000, 8000]; k = 5 → `failed`, `reservation_abandonment_exhausted`. After 4 abandonments a successful Begin resets k to 0; a further reserved expiry gives k = 1. `ReleaseReservation` changes neither k nor `execution_count`. (Rate deferral: not in M2) |
| TIME-07 | Handler ignores cancellation; `ExecutionTimeout = 5 s` | Cancellation requested at elapsed 5 s; at 15 s grace expires: no Renew submitted afterwards, outstanding tickets unchanged, scope not disposed, permit rows untouched by the worker, no Finish. An Applied reply of a Renew in flight at 15 s does not restart renewal. Handler returns at 100 s: scope disposed, ticket released, still no Finish; recovery later closes `abandoned_running`, job `retrying` with execution 1 consumed |
| TIME-08 | Throwing callback; callback blocking 30 s; disposal hanging after timely success | Every other attempt's Renew is submitted no later than its due + 2 s; callback exceptions observed. Hung disposal: at CleanupBudget 10 s renewal stops, ticket retained, no Finish of any outcome, diagnostic "unconfirmed completion"; no `RetryableFailure` is fabricated |
| TIME-09 | Dropped replies for Reserve, Begin, Finish | Store holds one receipt row and one attempt per command ID across resubmissions. Unknown Reserve: pool issues no other Reserve; at 15 s claim surfaced unresolved and pool suspended; other pools continue. Unknown Begin: no invocation; at 15 s `OwnershipLost(BeginUnresolved)`, renewal stopped, pool suspended. Unknown Finish: identical ID and fingerprint on every resubmission, no Renew while unresolved, no second invocation; at 30 s `CompletionUnresolved` |
| TIME-10 | No-timeout job runs 600 s; separately `ExecutionTimeout = 2 s` | Renewal count ∈ [33, 50] (600/18 to 600/12), no loss, outcome `Succeeded`. Short timeout: cancellation requested at elapsed 2 s and outcome `TimedOut`, independent of the 60 s lease |
| TIME-11 | Host token cancelled (a) before StopAsync; (b) at T0 + 10 s; (c) at T0 + 17 s; StopAsync entered at T0 + 3 s with stop signal at T0 | Cutoffs stay T0 + 15/20/25 regardless of StopAsync entry. (a)/(b)/(c): control I/O stops immediately on the host token, no new budget starts, StopAsync returns promptly, remaining work reported unresolved. Every Finish/ReleaseReservation/Renew submitted during drain carries an internal token not linked to the host token |
| TIME-12 | Finish sent, reply lost, watchdog latches loss, receipt later found; separately receipt deleted (fault injection) | Resolution returns `Applied`, `Replayed = true`, outcome succeeded; entry retires with durable success; no StaleLease misreport; no new Finish. Deleted receipt: `ExpiredUnknown` → history inspected, unknown-durability diagnostic; a resubmission returns `CommandExpired` and nothing is reapplied |
| TIME-13 | Stress: all 4 renewal slots held by hung calls; scope row lock held 3 s; store clock +30 s and −30 s; thread pool blocked 3 s | Safety invariants hold: no Finish after a latch; no double invocation; forward jump causes `StaleLease` → loss; backward jump never extends `d`; pressure stop triggers when margin < StopMargin; margins recorded. Static inequalities are not taken as proof |

## 21. Delivery mapping

| Item | Verifies |
|---|---|
| SP-009 (M2) | WRK-01–19; TIME-01–04, TIME-07–12 (WRK-02/15/19 deferred per 07 §10) |
| SP-008 (M2) | TIME-05, TIME-06 (storage arithmetic); drain release behavior |
| SP-010 (M2, G-1) | End-to-end: rollback, death before/after Begin, stale Finish, dropped replies, ignored cancellation, shutdown at every phase |
| SP-012 (M3) | CAP-01–04 cost/shares/aging/cooldown evidence for §7–§8 |
| SP-027–029 | TIME-13, CAP-09–12 |
