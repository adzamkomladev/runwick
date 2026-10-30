> History; see [docs/spec](spec/00-overview.md) for the normative specification.

# Storage design inputs: review notes against the PostgreSQL design

Status: review notes, 2026-10-28. Written after reading the original [Queue Library Design](https://chatgpt.com/share/6ae25a83-ff20-83e9-aadb-b4d731f0e2e0) conversation (messages 22–30, 36, 54, 60–62, 67) and the drafted [storage command protocol](architecture/storage-command-protocol.md) and [PostgreSQL storage design](architecture/postgres-storage-design.md).

Four design points were raised in review as things that shape the schema and reservation query. This document records, for each, what the original conversation agreed, what the drafted storage design already does about it, and what remains to add or challenge. Where the drafted design already answers the point, this document defers to it.

## 1. Priority: the lane model makes an ordering column unnecessary

### Agreed in the original conversation

- `"low"`, `"medium"`, `"high"` as strings, not enums, because the values are persisted (message 29, user decision).
- "Priority and queue names are stored as stable strings in the database" (message 30).
- Weighted fair scheduling by default, strict priority on request, and job aging (messages 22, 26, 30).
- Job priority orders work *within* a queue; queue weight distributes capacity *between* queues (message 30).

### What the drafted design does

`postgres-storage-design.md` §2 stores `priority text` with a CHECK constraint, and §5 has each reservation select **one queue and one priority lane**, ordered by `(eligible_at, job_id)`. The `jobs_ready` index is `(application, queue, priority, eligible_at, job_id) WHERE state IN ('pending','retrying')`, so priority is an equality predicate and never needs to be ordered. Fairness and lane selection are explicitly the pool manager's responsibility (§5).

This resolves the concern that motivated an integer rank column: there is no `ORDER BY priority` anywhere, so the string column is index-friendly as is. **No rank column is needed.** The agreed "stored as stable strings" rule stands untouched.

The lane model also gives aging a natural home: the pool manager occasionally offers a slot to a lower lane even when a higher lane has work. That is a selection policy, not a persisted value, and needs no schema support.

### What to add or challenge

1. **Lane polling cost.** A worker consuming N queues across 3 lanes has up to 3N lanes to probe. Each empty lane probe is a round trip. The protocol's read-only "ready-work probe" (`storage-command-protocol.md` §4) should be a single query that returns which `(queue, priority)` lanes currently have eligible, supported work, so the pool manager issues `TryReserve` only against non-empty lanes. Without that, idle polling cost scales with lanes × replicas, which is the Hangfire idle-polling lesson in `implementation-research.md` §1. Recommend specifying the probe's shape alongside the reservation query.
2. **CHECK constraint hard-codes the labels.** `priority IN ('low','medium','high')` in DDL means custom labels (still open in `architecture-plan.md`) would require a migration. If custom labels are a v1 possibility, validate labels in the library and keep the constraint to "non-empty bounded text". If they are ruled out for v1, the constraint is fine and this note closes.
3. **Per-dispatch override above the declared priority.** Recommend allowing it; the override is explicit at the call site. No storage impact.

## 2. Handler identity is the job name; version is a payload concern

### Agreed in the original conversation

- Persist stable name + payload schema version + JSON; CLR names are not durable identity; offer "explicit version upgrades or migration handlers" (message 36 §7).
- `[JobName]` was optional with a convention default (message 54 §3); `public-api-proposal.md` later made it mandatory.

### What the drafted design does

`jobs.job_name` and `jobs.payload_version` exist (§2), and the reservation predicate matches `(name, version)` pairs supplied by the worker (§5). Storage is version-aware; it does not say how the engine maps a reserved `(name, version)` to a handler. That is an engine-level decision, which is where this point actually lives.

### Recommendation for the engine layer

- Key the handler registry on `job_name` only; one handler per name per worker.
- `payload_version` selects the deserialization path. A contract registration declares its current version and one upgrade step from the previous version; steps compose in version order.

Illustrative shape (syntax provisional):

```csharp
runwick.AddJob<SendWelcomeEmail>()            // current version from [Job(..., Version = 2)]
    .UpgradeFrom<SendWelcomeEmailV1>(v1 => new SendWelcomeEmail(v1.UserId, Locale: "en"));
```

The worker's capability set for `email.welcome` is then `{1, 2}`, which is exactly the `SupportedContracts` list the protocol's `ReserveRequest` already carries.

Keying handlers on `(name, version)` instead would mean a worker on version 2 cannot drain version 1 jobs without a second handler, which recreates the rolling-deployment problem message 36 raised.

### Open

- **Optional vs mandatory `[JobName]`.** A CLR-derived default reintroduces the rename fragility the stable name exists to prevent. Recommend mandatory for durable dispatch; if a convention default is kept for the beginner path, emit a startup warning.
- **Capability-set size.** `postgres-storage-design.md` §11 lists "limits on supported contracts per worker" as unresolved. With one upgrade chain per name, the set is roughly (names × versions in the compatibility window); a bound of a few hundred pairs is realistic and should be stated.
- Downgrade is not supported; an older worker does not reserve newer versions.

## 3. Capability-aware reservation: already in the design, with one cost note

### What the drafted design does

The reservation SQL (§5) includes `EXISTS (SELECT 1 FROM unnest(@names, @versions) ...)`, the protocol says "an unsupported contract is filtered before reservation" (§1), and the validation matrix (§10) has a "Capability filtering" case. This point is resolved.

### What to add

**Stuck-at-the-top cost.** Unsupported jobs are excluded by the predicate, not by the cursor, so they are scanned past on every poll by every worker that cannot read them. During a rolling deployment this is a handful of rows and harmless. If a producer ships a version no worker can read, those rows sit at the head of the lane indefinitely and every poll pays for them.

Recommend, in priority order:

1. A metric and dashboard signal: "N pending jobs have no capable registered worker", derived by comparing pending `(name, version)` pairs against the union of registered workers' capability sets. This makes the condition visible and is enough for the first milestone.
2. Only if measured to matter: add `job_name` to the ready index so the planner can use it as an index condition rather than a post-filter.
3. Do not move such jobs to a separate state; that hides work and complicates recovery.

Record the capability set in the worker registration row so the dashboard can explain why a job is not being picked up.

## 4. Distributed permits: the drafted design is the lease-backed model; the open risk is scan cost under a full ceiling

### Agreed in the original conversation

- Four levels: worker pool (local), queue, job-type, resource key (distributed) (message 24).
- "Enforced centrally through lease-backed permits, not an in-memory `SemaphoreSlim`" (message 24); "durable, lease-backed permits so abandoned capacity can be reclaimed after crashes" (message 22).
- A `concurrency_leases` table in the schema sketch (message 36 §1).
- Reservation and permit acquisition are one atomic operation (messages 54 §9, 60 §3).
- Rate limiting is a separate concept from concurrency (message 24), and the reservation check includes rate limits (message 60 §4).

### What the drafted design does

`concurrency_scopes` + `concurrency_permits` (§2) with `lease_expires_at` per permit, renewed in the same transaction as the job lease (§6), counted live via `permits_live_count` (§3), locked by sorted scope-row `FOR UPDATE` (§4), with expected-count verification on renewal and a fencing counter for capacity-one resources. Counter rows and numbered slots are explicitly only "optimization candidates" (protocol §1). §10 names scope-lock serialization as "the baseline's explicit scalability risk" and lists the measurements.

This is the lease-backed permit model the original conversation asked for, and it is the right starting point: validity derives from expiry, so a crashed worker's permits stop counting when its lease lapses with no reconciliation sweep. Scope-row locks are a better choice than advisory locks because the row carries the capacity and the lock is tied to a real entity rather than a hash.

Nothing here needs replacing. The points below are additions.

### Addition A: short-circuit the cursor walk when the blocking scope covers the whole lane

§5 says: on `CapacityBlocked`, return the candidate cursor and scope, commit nothing, and advance the cursor in a new transaction. Each step costs a command receipt check, a queue-control share lock, a candidate select with row lock, scope-row locks, a permit count, and a rollback. That is correct for a resource-key scope (the next candidate may have a different key), but wasteful when the blocked scope is the **queue** scope or a **job-type** scope in a homogeneous lane: every remaining candidate in that lane is blocked by the same scope, and the worker walks the entire backlog one transaction at a time, on every poll, on every replica.

Recommend: `CapacityBlocked` already returns `BlockedScopeId`. The pool manager should stop scanning the lane when the blocked scope's kind is `queue`, and when it is `job_type` and the lane's recent candidates are all that type. For `resource` scopes, continue advancing the cursor but cap the number of blocked candidates examined per poll (§5 already says "bound each scanning pass"; make the bound explicit and small).

### Addition B: a cheap "scope full" hint, only if measurements require it

If the prototype shows that even bounded cursor walks are too expensive under a saturated queue-wide ceiling, add a hint the candidate query can pre-filter on:

```text
scope_full_hints: application, scope_id PK; full_until timestamptz
```

Written when a reservation returns `CapacityBlocked` with `full_until = decision_time + short interval`, and the candidate query excludes jobs whose required scopes have an unexpired hint. It is a hint, not the authority; the counted permits under scope lock remain the correctness check. Do not build this before the measurement in §10 shows it is needed.

### Addition C: rate limits are missing from the storage design

The original conversation kept rate limits (starts per window) distinct from concurrency (simultaneous executions) and listed them among the atomic reservation checks. Neither architecture document mentions them. A rate limit cannot be derived from live permit rows; it needs its own representation (a window counter or token-bucket row per scope), checked under the same scope-row lock and in the same lock order. Recommend adding a `rate_limit` column pair to `concurrency_scopes` (or a sibling table) and a line in the reservation step 5, or explicitly deferring rate limits to a later milestone in `architecture-plan.md`. Right now it is silently absent.

### Addition D: measurements to prioritise in §10

The §10 performance gates are right. Of them, run these first because they decide whether the counted-permit baseline survives:

1. Reservations/second and scope-lock wait p99 for one queue-wide ceiling with 1, 3, 6 replicas, saturated and unsaturated.
2. Poll cost per replica when that ceiling is saturated and the backlog is 10k jobs, with and without Addition A.
3. WAL bytes per renewal, since every renewal now updates the job row plus one permit row per scope and touches `jobs_expired_lease` and `permits_live_count`. A job with queue + type + resource scopes writes four rows per heartbeat.
4. Kill-test: a limit-of-one resource never has two valid holders across lease expiry and takeover.

### Open

- **Where ceilings live.** `concurrency_scopes.capacity` with `policy_revision` is the authority, and §4 covers policy edits. What remains is how code configuration seeds it: recommend seed-if-absent, never overwrite, so replicas disagreeing about configuration cannot flap the capacity.
- **Local keyed gates** (the TickerQ pattern, `implementation-research.md` §6) as a pre-check in front of the durable permit. They never satisfy a global ceiling; they only avoid wasted reservation transactions for resource keys the same process already holds.

## Interaction with the protocol's accounting and lifecycle choices

The drafted protocol made two decisions that the earlier planning docs listed as open, and they are better than the alternatives this reviewer had suggested:

| Topic | Earlier suggestion | Protocol decision | Position |
|---|---|---|---|
| Crash between reservation and invocation | Consume an execution attempt | Separate `Begin` transaction; pre-start crashes consume a distinct unstarted-abandonment budget, not an execution | Protocol's answer is better: it bounds crash loops without charging the handler budget for a crash that ran no user code. Accept it. |
| Job states | Five states with `pending_reason` | Seven states, keeping `reserved` and `retrying` distinct | Protocol's answer matches the original conversation and is what the begin/recover accounting needs. Accept it. |
| Handler signature, marker interface, dispatcher naming | Various | Not addressed; engine-level | No storage dependency either way. Decide once in the engine design. |

None of the four points above depends on the remaining open contract choices, so the storage work can proceed.
