> History; see [docs/spec](spec/00-overview.md) for the normative specification.

# Uniqueness and execution planning

Status: planning draft. Confirmed: Runwick will support both active-job uniqueness and a fixed idempotency retention window. Detailed semantics and execution defaults below are proposals for review. No runtime implementation has started.

Current-contract notice, 2026-10-29: use the [consolidated public API](architecture/public-api-contract.md), [wire/fingerprint contract](architecture/dispatch-wire-contract.md), and [storage protocol](architecture/storage-command-protocol.md). The historical five-state lifecycle, reservation-counted budget, and numerical defaults below are superseded/unapproved alternatives, retained for provenance rather than implementation guidance.

Reconciliation notice: the earlier session already established retries, cancellation, leases, and recovery. The five-state representation, reservation-based budget consumption, and numeric defaults below are unapproved alternatives to earlier sketches, not accepted replacements. See [planning continuity](planning-continuity.md). These details should now be resolved through the internal storage/engine design, without repeating the feature discussion.

## 1. Two uniqueness policies

| Policy | Intended use | Proposed behavior |
|---|---|---|
| Active-job uniqueness | Avoid redundant pending/running work for a resource | A matching pending or running job suppresses a new insertion and returns the existing job ID |
| Fixed retention window | Safely repeat a dispatch request, for example for 24 hours | A matching dispatch key returns its original job ID until the window expires, including after job completion |

Active includes delayed jobs and jobs waiting for an automatic retry. Lease expiry does not release active uniqueness: recovery continues the same logical job. The claim is released atomically when the job becomes terminal. This is insertion uniqueness, not a physical-execution lock; an expired worker may still be running user code.

The fixed window does not slide forward on duplicate requests. Its proposed anchor is storage admission time on the initial insertion, not handler completion. For transactional staging, time spent before commit consumes part of the window; it is not a promise of a full window after commit. We must specify the authoritative timestamp and locking order in each driver.

After a fixed window expires, a new job may be admitted while the original job is still active. Applications that need both constraints can use both policies together, provided the composition contract is supported atomically by the driver.

### Proposed key and conflict rules

- Scope keys to application namespace, stable job name, and caller-provided key. Queue and payload version do not implicitly create new key scopes. Applications can explicitly include tenant/resource identity in the key.
- Active uniqueness is first-writer-wins: a duplicate returns the existing job without replacing its payload. It does not promise a follow-up execution that processes newly arrived data. Coalescing/latest-value behavior is a separate feature, not silently implied.
- A fixed-window idempotency key represents the same request. Reusing it with an incompatible payload or dispatch intent should produce an explicit conflict rather than silently returning an unrelated job. Canonical request fingerprinting still needs a precise specification, including defaults, schema versions, and volatile metadata.
- Deduplication is atomic with insertion, including within a supplied transaction. Rollback removes a new job and its new key claims together.
- Keep fixed-window records independently of job-history retention. A duplicate can return a historical job ID after detailed job history has been purged; inspection must distinguish expired history from an unknown ID.
- When both policies apply and a new request key matches an existing active job, bind that request key to the existing job atomically. A later retry of that request must not create a new job simply because the active job has finished. Conflicting key-to-job mappings are errors, not silently reassigned aliases.
- Manual retry does not extend a fixed window. It must reacquire active uniqueness atomically; if another active job now owns that key, reject the retry with the conflicting job ID. Never create a new generation without its required claim.

Attribute/fluent/per-dispatch syntax remains to be designed. Both policies are confirmed v1 scope; the combination and conflict details above remain recommendations.

## 2. Proposed job lifecycle

Use five durable job states: `pending`, `running`, `succeeded`, `failed`, and `cancelled`.

`pending` includes immediate work, delayed work, and automatic retries, distinguished by eligibility time and a reason. A failure is terminal only after a permanent rejection or an exhausted attempt budget. Dead-letter management presents failed jobs without requiring a second copy of each payload.

| Trigger | Transition | Required behavior |
|---|---|---|
| Eligible job and available capacity | pending → running | Atomically create attempt, reserve job, and acquire required permits |
| Handler succeeds | running → succeeded | Current unexpired owner alone can finalize |
| Retryable failure, budget remains | running → pending | Record attempt failure and next eligible time |
| Permanent rejection or no budget | running → failed | Preserve attempts and terminal reason |
| Cancel pending job | pending → cancelled | Serialize against reservation; release active uniqueness |
| Cancel running job | running stays running initially | Persist request and signal handler token |
| Lease expires | running → pending/failed/cancelled | Recovery closes abandoned attempt and applies budget/cancellation policy atomically |
| Manual retry of failed/cancelled job | terminal → pending | New generation, same job ID, reacquired uniqueness, preserved history |

Manual retry of a succeeded job is not proposed; an explicit new dispatch is a new logical job. Manual retry and cancellation need audit records and expected-generation/state checks so stale dashboard actions cannot reset newer work.

## 3. Retry budget and classification

Recommended default: five total attempts in a generation. `MaxAttempts = 5` means the initial attempt plus four automatic retries.

An attempt is consumed when the reservation/attempt transaction commits, before user code starts. A crash in the gap can therefore consume an attempt without invoking the handler. This deliberately bounds repeated crash loops; the dashboard must label abandoned attempts accurately.

Waiting for capacity, paused queues, and rolled-back reservation transactions consume no attempt. A handler cancelled during shutdown still consumed its committed attempt. Manual retry is the explicit way to grant a fresh budget.

Default classification proposal:

- Unclassified handler exceptions are retryable within the budget.
- Applications can classify known permanent failures explicitly.
- Persisted malformed payloads that a supported handler cannot deserialize are terminal with an actionable reason.
- Workers should reserve only job identities/versions they support. An unavailable handler version remains observable pending work instead of burning attempts on every replica. Capability-aware reservation and alerting require a separate design.
- Failure to record an outcome is a storage/ownership problem, not another handler exception. Retry the bookkeeping within its ownership/deadline budget; do not re-enter user code in the same attempt.

Recommended backoff: exponential with equal jitter, a 10-second base, and a 5-minute cap. For retry index r starting at zero, upper bound = min(5 minutes, 10 seconds × 2^r); choose a delay uniformly between half that bound and the bound. Compute without numeric overflow. Persist the chosen next eligible time once, so restart does not repeatedly redraw it. The default five attempts yield four waits of 5–10, 10–20, 20–40, and 40–80 seconds.

Custom policies may suggest a later retry time, for example a server-provided retry-after duration, within configured bounds. Exact exception-policy interfaces remain open. Backoff waiting holds no worker slot or execution permit; active-job uniqueness remains held for the logical job.

## 4. Cancellation and timeouts

Producer cancellation only cancels waiting for dispatch; it does not cancel an already accepted job. Execution cancellation is a separate durable operation by job ID.

For pending jobs, cancellation is immediate when its transaction wins against reservation. For running jobs, cancellation is a persisted request delivered through the handler's token. Repeated requests are idempotent.

Proposed outcome precedence:

1. Ownership loss makes all completion proposals stale, regardless of their outcome. The old worker may report diagnostics but cannot change durable job state.
2. A normal return before the execution timeout may finalize success even when cancellation was requested. Preserve the cancellation request in audit history; success does not mean the request was never made.
3. If a running job has a durable user-cancellation request and does not complete successfully before its timeout, finalize cancelled after user code exits. If it throws another exception, retain the exception in attempt history while suppressing automatic retries.
4. Without a user-cancellation request, execution timeout is a failed attempt eligible for retry, including when user code eventually returns normally after the timeout.
5. Without either of those, a cooperative shutdown interruption becomes retryable pending work, or failed if the budget is exhausted. An unrelated OperationCanceledException is an ordinary handler failure, not proof of a user cancellation request.

Read cancellation state in the same ownership-checked transaction that finalizes the outcome. A cancellation request that arrives after committed success returns an already-terminal result. Record when user code finishes separately from when its outcome is persisted; delayed bookkeeping must not invent an execution timeout. Precise durable/local-clock reconciliation is part of the ownership-protocol design.

When a timeout or cancellation is ignored, keep the handler's local slot and DI scope until user code actually exits. While ownership remains valid, renew it and retain the associated permits; do not release them simply because a cancellation timer fired. Show the overdue/cancellation-requested condition in operations. Operators can terminate an isolated worker process if necessary; Runwick cannot guarantee termination of arbitrary in-process code.

Lease expiry can still allow a replacement worker while an old handler continues. Cancellation and uniqueness cannot eliminate that possibility. External side effects need idempotency, and external fencing works only when the receiving resource validates an appropriate fencing token.

## 5. Leases and recovery

Recommended initial operational defaults for validation: 60-second leases and renewal approximately every 15 seconds, with bounded jitter and renewal scheduled independently from job fetching. These are starting points for failure testing, not established production tuning.

Storage time determines reservation/renewal validity. Local monotonic elapsed time helps workers stop trusting ownership conservatively after renewal uncertainty; neither local wall clocks nor process heartbeats alone authorize a takeover. Database-side lease calculations must use a suitable current-time function after relevant lock waits, not an old transaction-start timestamp.

Renewal, outcome persistence, and permit release check the reservation identity. An expired owner cannot revive its lease. Failure/uncertainty that prevents proving timely renewal causes local cancellation and prohibits acknowledgement; recovery proceeds through storage.

Recovery closes each expired attempt once. A durable user-cancellation request leads to cancelled rather than automatic retry. Otherwise the attempt budget determines retry versus failed. Recovery and manual retry race through the same ownership/generation rules. A worker heartbeat is useful health information but is not sufficient evidence that every handler stopped.

Attempt IDs and ownership tokens are not automatically resource fencing counters. If resource fencing is offered, its order and scope must be defined per protected resource, including across different job IDs.

## 6. Graceful shutdown

1. Stop new reservations and scheduler admission on that host.
2. Continue renewing existing ownership while handlers drain.
3. At the drain deadline, signal handler cancellation.
4. Permit bounded final bookkeeping with a token independent of the cancelled handler token.
5. At the overall host deadline, stop waiting and rely on storage recovery for unfinished ownership; never report arbitrary user code as forcibly stopped.

A proposed initial budget is 30 seconds of drain plus 10 seconds for cancellation/bookkeeping, bounded by the host's actual shutdown deadline. The host may terminate earlier; correctness must not depend on receiving this grace period. Never dispose attempt-scoped services still used by a live handler.

## 7. Acceptance scenarios

- Two replicas racing to insert the same active key produce one logical job.
- Fixed-window duplicates still resolve after completion or history purge; expiry permits a new insertion.
- Active uniqueness survives retry delays and crash recovery; a fixed window alone does not prohibit overlapping executions after expiry.
- A combined-policy duplicate preserves the request-key association after the active job ends.
- A conflicting manual retry does not start a generation or steal another job's uniqueness claim.
- The initial run plus four failures exhaust a five-attempt budget; waiting for a permit consumes zero attempts.
- Retries spread over the specified jitter ranges and preserve eligibility across restart.
- A stale owner cannot complete, renew, or release replacement permits.
- Cancellation races yield one consistent terminal outcome with request/attempt history preserved.
- A timed-out handler ignoring its token does not free local capacity or have its DI scope disposed prematurely.
- Lost completion writes do not cause immediate in-process re-execution; recovery remains at-least-once.
- Unsupported job versions stay visible without repeated failed attempts.
- Shutdown remains recoverable with no grace period, and renewal cannot be starved by ordinary queue polling under the tested workload.

## Next planning topic

After reviewing these lifecycle recommendations, specify concurrency scopes, queue fairness, priority ordering, and the PostgreSQL atomic reservation/permit protocol. These determine how the public limits can be enforced across replicas without starving queues or over-reserving work.
