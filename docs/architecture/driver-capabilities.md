# Storage driver capabilities and implementation boundaries

Final selections: [Redis design closure](redis-design-closure.md) selects recoverable intent and direct-primary AOF-always; [transactional outbox](transactional-outbox.md) selects optional PostgreSQL/SQLite sources and ReceiveOutbox on every full destination driver. Certification is still pending.

Status: engineering proposal, 2026-10-29. No driver is implemented or certified. PostgreSQL remains the reference implementation. SQLite and Redis remain v1 targets; a proposed feature is not marked supported until its applicable [contract tests](driver-contract-suite.md) pass.

Inputs: [execution protocol](storage-command-protocol.md), [PostgreSQL design](postgres-storage-design.md), [capacity control](capacity-control.md), [scheduling](scheduling-internals.md), and the newly inspected [BullMQ Redis implementation](bullmq-redis-review.md).

## 1. A capability describes semantics and a deployment profile

Do not expose only SupportsTransactions/SupportsDistributed booleans. The same server software can have different durability under different persistence/replication settings. Proposed immutable startup descriptor:

```text
DriverDescriptor:
  driver ID/version; engine/schema/operation versions
  supported topology and atomic partition boundary
  supported semantic-operation versions
  caller-transaction enlistment modes
  timestamp quantum and supported instant/duration range
  limits: payload bytes, scopes/job, batch size, capabilities/probe
  acknowledgement profile and failover assumptions
  notification availability; read consistency class
  evidence/configuration-check status
```

Startup validates requested features and configuration against this descriptor before admitting work. An unsupported guarantee fails explicitly; no fallback to local locks, approximate rate windows, or best-effort uniqueness. Report the exact server/provider versions and checked settings. If managed-service permissions prevent verification, report unverified configuration and do not label it verified; allow only a separately documented operator-attested profile where appropriate.

All three drivers must eventually support the full v1 execution, scheduling, control/history, uniqueness, and rate semantics within their supported topology. Notifications and caller-owned transaction enlistment are capabilities rather than requirements common to every driver. Implement operations, not a generic SQL CRUD abstraction.

## 2. Target capability matrix

“Target” below means required before claiming conformance, not available today. Redis entries are conditional on the recovery and durability gates in §5.

The later [management design](management-and-dashboard.md) adds atomic successful-administration audit alongside command effects/receipts, authorized indexed projections and bounded retention. All drivers must include these in their transaction/publication protocol. The [operations design](observability-and-operations.md) adds bounded query sampling and freshness semantics; unavailable statistics must not become zero-valued authoritative results.

| Capability | PostgreSQL | SQLite | Redis |
|---|---|---|---|
| Embedded/external worker | Target; same or different hosts | Target; processes on one host | Target; clients on multiple hosts |
| Multi-host replicas / mixed worker deployment | Target | Unsupported across hosts | Target within one primary history |
| Atomic partition | One compatible database transaction | One database file/transaction | One application namespace in one hash slot |
| Reservation + all permits | Short row-locked transaction | Serialized short write transaction | Bounded server-side command; partial-write recovery gate |
| Begin + all rate grants or deferral | Transaction | Write transaction | Server-side command with identical rate semantics |
| Renew / Finish / Recover ownership checks | Target | Target | Target; neither TTL alone nor a queue list is sufficient |
| Mutation receipts / commit ambiguity | Receipt and effects in one transaction | Receipt and effects in one transaction | Receipt plus publish/recovery protocol; gate before release |
| Both uniqueness modes | Target | Target | Target; explicit expiry/key lifecycle |
| Schedule occurrence + job + cursor | One transaction | One write transaction | Same-slot bounded command/recovery unit |
| Business transaction enlistment | Same Npgsql connection/transaction | Same SQLite connection and database file | No general external transaction enlistment |
| Cross-store atomic dispatch | Requires outbox integration | Requires outbox integration | Requires source-store outbox integration |
| Idle notification | Optional LISTEN/NOTIFY | Polling plus process-local hints | Optional wake signal/PubSub with polling fallback |
| Control/history/dashboard backend | Target | Target; bounded reader duration | Target with explicit indexes and retention |
| HA without acknowledged-history loss | Depends on replication/promotion profile | No built-in multi-host HA target | Not promised for ordinary asynchronous failover |
| Monotonic resource fences after history rollback | Not guaranteed by a restored database | Not guaranteed by a restored file | Not guaranteed after lossy failover/restore |

“All five deployment modes” remains an engine property. SQLite's same-host topology restriction limits where its processes may run; it does not redefine job/handler APIs.

## 3. PostgreSQL profile

Use the existing READ COMMITTED + ordered row-lock design, explicit parameterized SQL, and authoritative reads from the writable primary. Readiness snapshots may be stale; ownership, command resolution, and admission may not use an asynchronous read replica.

The persistent baseline requires ordinary WAL/fsync protections and synchronous local commit for driver-owned mutations. PostgreSQL documents that asynchronous commit can lose recently acknowledged transactions after a crash; it is a different acknowledgement profile, not an interchangeable performance switch. Caller-owned transaction settings belong to the caller and must be reported/validated without secretly committing or changing their transaction. [Commit durability](https://www.postgresql.org/docs/18/wal-async-commit.html)

Local durable commit does not guarantee promotion of an arbitrary replica without data loss. Synchronous replication and an appropriate promotion/fencing policy can strengthen that guarantee; Runwick cannot create those deployment properties just by connecting to PostgreSQL. Record the tested replication/promotion profile and test it separately. A stale writable primary must be fenced by infrastructure. [Replication reference](https://www.postgresql.org/docs/18/warm-standby.html#SYNCHRONOUS-REPLICATION)

Driver-owned deadlock/serialization errors are retried only after confirmed rollback, reusing the command ID within its deadline. Lost connection around COMMIT is unknown until resolved. Caller-owned transactions are never reopened or retried internally. Statement, lock, and transaction deadlines must leave renewal headroom.

Migration execution is explicit and serialized. Store schema/operation compatibility separately from application EF migrations. Rate-enabled Begin and scheduler operations require their new versions; an older worker must fail compatibility checks rather than omit those effects.

## 4. SQLite profile

Target a local file on a supported local filesystem, with WAL, foreign_keys enabled, read_uncommitted disabled, and synchronous=FULL for the persistent profile. Verify effective settings per connection where applicable. NORMAL is a separately described weaker power-loss profile; in-memory SQLite is a test/development profile, not the durable default. SQLite documents the WAL same-host requirement and its synchronous-mode tradeoffs. [WAL topology](https://www.sqlite.org/wal.html), [synchronous settings](https://www.sqlite.org/pragma.html#pragma_synchronous)

Preserve logical records/constraints/indexes, translating PostgreSQL UUID/JSON/time types into explicit SQLite encodings. Do not imitate SKIP LOCKED or row locks. SQLite serializes writers, including writers from different same-host processes. [Isolation reference](https://www.sqlite.org/isolation.html)

For a driver-owned mutation:

1. Enter a bounded process-local storage executor; acquire `BEGIN IMMEDIATE` with a short busy timeout.
2. Only after obtaining the writer lock, read a fresh decision time, resolve the command receipt, and validate job/cursor/revision/lease/policy.
3. Perform the whole semantic transition and receipt write in that transaction. No handler, middleware, recurrence computation, or remote call occurs while holding it.
4. Commit and return. Release the local executor slot in finally; close readers promptly so checkpoints can progress.

The database lock is the cross-process authority. A process-local write queue only reduces self-contention. All contenders, including cleanup and schedule batches, use small bounded transactions; prioritize renewal/finalization locally and measure inter-process starvation.

Treat busy-at-BEGIN as no mutation. A failed deferred snapshot upgrade requires whole-transaction rollback/retry. A busy COMMIT can leave the transaction active: finish or roll back that transaction before attempting a new one. Do not label every SQLITE_BUSY as rolled back. [Transaction behavior](https://www.sqlite.org/lang_transaction.html)

With Microsoft.Data.Sqlite, async ADO.NET methods execute synchronously. Use a bounded adapter executor so synchronous database work and lock waits do not run inline on the worker's lease/scheduling coordinator. This is storage work scheduling, not one thread per job; actual thread count and cancellation behavior are benchmark gates. [Provider limitation](https://learn.microsoft.com/en-us/dotnet/standard/data/sqlite/async)

Caller enlistment accepts an existing compatible connection/transaction on the same file, without commit/dispose/retry. Recommend the caller acquires its write transaction early; a deferred read snapshot may be impossible to upgrade. Cross-file ATTACH transactions are outside this target. A long business transaction can delay all queue writes, so test and expose that contention.

Use a fresh storage-time sample after writer acquisition. SQLite has no remote server clock; all supported processes share the host clock. Store timestamps as an explicit integer epoch representation. Enforce the common rounding/range contract in §6, not culture-dependent text dates.

Measure WAL/checkpoint growth under long dashboard reads, busy latency, scheduler batch interference, and renewal deadlines with 1/3/6 local processes. Same-host correctness does not imply multi-host scalability or throughput parity with PostgreSQL.

## 5. Redis profile and feasibility gates

### Key placement and state structures

Use a versioned Redis Functions/Lua command bundle with bounded inputs and work. A single command validates ownership, state, policies, and all expected revisions before mutating. A reference layout uses hashes for job/attempt/policy/receipt data, sorted sets for due work, lease expiries and rate grants, and explicit indexes for history/schedules. Preserve pending/reserved/running/retrying states. Redis Streams may carry events; their consumer groups do not replace Runwick's reservation/permit protocol.

BullMQ supplies concrete precedents for token locks, renewal results and delayed indexes. Runwick's additional fields and commands are intentional differences, documented in the [source review](bullmq-redis-review.md).

Encode the application's stable partition ID in a hash tag, such as `runwick:{encoded-application-id}:...`. All keys touched by a command—including cross-queue limits, receipts, deduplication, schedules and audit indexes—must be in that slot. Reject brace/key-injection ambiguity and cross-partition operations. Multiple applications may use different slots; one application is not sharded across slots in v1. Redis Cluster requires same-slot multi-key operations. [Cluster rules](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/)

Declare accessed keys explicitly. If candidate discovery is a separate read, pass candidate and required key names into the command and revalidate its state/revision/requirements there. Alternatively use fixed aggregate hashes with entity IDs as fields. Do not rely on undeclared dynamically generated key access for portable cluster support. Readiness/candidate reads remain hints. [EVAL key rules](https://raw.githubusercontent.com/redis/redis-doc/master/commands/eval.md)

Use Redis TIME for authoritative decisions. Do not use whole-job key TTL expiry as lifecycle cleanup: expiring a lock may establish lack of authority, but job/attempt history, permit accounting, active uniqueness, and recovery still require an explicit transition. Receipt cleanup cannot expire a command while its unresolved internal recovery record still depends on it. [Server time](https://redis.io/docs/latest/commands/time/)

### Atomic visibility is not SQL rollback

Redis scripts prevent interleaving during execution, and Lua command failures can raise runtime errors. Those facts do not establish that previously executed writes have been undone. The BullMQ finish path inspected in the review also has writes before later error returns. Runwick must distinguish pure rejection, committed transition, and unknown/recovery-required outcome. [Script execution](https://redis.io/docs/latest/develop/programmability/eval-intro/), [Lua errors](https://redis.io/docs/latest/develop/programmability/lua-api/)

First prototype the bounded validated command bundle: validate input types, key types, policy ranges, token/state checks, expected counts, and size limits before writes. Then inject a failure after every write position. If an unexpected failure can leave a partially applied operation visible, this bundle alone does **not** pass the shared contract. Do not classify it as success or confirmed rollback, or assume stalled-job recovery repairs every partial transition.

The selected design uses a **recoverable mutation intent**, scoped to the application:

1. Compute/validate a bounded mutation plan with exact idempotent after-images, chosen IDs, decision timestamp, fingerprint, and expected application revision. Persist it in one pending-intent field before projecting any writes.
2. Every authoritative read/write command checks that field. While it is populated, only bounded recovery of that intent proceeds; other operations return Recovering/unavailable rather than observing mixed state. Preflight reads used only as hints must still be revalidated later.
3. Apply deterministic assignments/deletes/index updates. Reapplication must not increment counters, redraw jitter, emit duplicate events, or allocate new IDs. Client recovery passes the complete declared key set and checks it against the stored plan.
4. After all projections are consistent, one final multi-field HSET on the application metadata hash publishes the next revision and command result and clears the pending field together. The command becomes externally Applied at this publication boundary.
5. A script error leaves an intent that can be replayed after the cause is fixed; it never returns a no-effect rejection after accepting that intent. Recovery uses the original decision and expires any now-stale lease before authorizing user code. A deadline passing does not cancel a previously accepted intent.

This is the selected design, still requiring prototype evidence, and is not attributed to BullMQ. It serializes recovery across the application, adds writes/memory, and can make that partition unavailable until recovery succeeds. It requires a protocol-level pending/recovery outcome and proof that persisted prefixes retain the guard before projections. No eviction, direct-key mutation, or unsupported restore may erase that guard independently. If this cost is unacceptable, review another representation or the advertised contract; do not quietly ship partial atomicity.

### Persistence and acknowledgement

Require noeviction and namespaced ACLs; all writers to Runwick's keys use its protocol. Separate bounded producer command deadlines from worker reconnect loops. Reconnect must resolve an old command or reject its expired deadline, not replay a stale mutation with a fresh ID. BullMQ's [production guidance](https://docs.bullmq.io/guide/going-to-production) supports treating persistence, memory policy, and producer/worker retries as explicit concerns.

Distinguish at least these profiles:

| Profile | Meaning and release treatment |
|---|---|
| Ephemeral/RDB-only or periodic AOF | Accepted work can be lost within the configured persistence window; never called the persistent no-acknowledged-loss profile |
| Persistent single primary | Selected appendfsync=always profile, noeviction, tested same-primary restart and intact history; certification pending |
| Asynchronous replica promotion | May lose acknowledged jobs, receipts, rate grants, permits and fencing increments; not certified as preserving the shared no-history-loss contract |

Redis WAIT improves replica acknowledgement but does not turn asynchronous replication into a strongly consistent system. WAITAOF can wait for prior writes on the same connection to be fsynced, but it still does not guarantee arbitrary failover preserves those writes. Inspect returned counts; a timeout is not rollback. [WAIT](https://redis.io/docs/latest/commands/wait/), [WAITAOF](https://redis.io/docs/latest/commands/waitaof/)

WAITAOF is excluded from the initial selected profile. If a future profile selects it, send it on the same physical connection/primary after publication and check the required local/replica counts. A reconnect cannot retrospectively barrier a prior connection's writes merely by sending WAIT. For a replay found on an intact primary history, write a fresh receipt-confirmation barrier on that connection, then wait for its persistence; test client routing and connection pinning. Distinguish Applied-in-memory from confirmation of the requested durability. A receipt contains the original semantic result, not proof that this new connection has met a persistence barrier.

Neither AOF nor an intent protocol repairs acknowledged history already lost in failover. A worker may still be executing a lease absent from the promoted replica; counters can rewind. Automatic promotion is excluded from the initial production profile; it is not ordinary transient reconnect. On a detected role/history discontinuity stop admission and invalidate local authority; detection alone cannot rule out all split-brain histories. A lossless HA claim requires an external deployment guarantee and dedicated tests, just as PostgreSQL promotion does.

Initial Redis implementation gates: partial-write recovery; selected persistence barrier/client behavior; one-slot key layout; server-time/numeric encoding; bounded scripts; and restart/failover tests. Do not announce feature parity until those pass. Redis remains in v1 scope, with these concrete engineering obligations.

## 6. Shared encodings, precision, and restore boundaries

IDs and keys use canonical application-scoped encodings independent of .NET Guid comparison quirks, locale, or database collation. Cursor order is eligible instant then canonical job-ID bytes. Store durations as checked integers and expose each driver's time quantum.

Proposed common minimum precision is one millisecond: round not-before eligibility/delay upward, lease expiry and command expiry downward, and test comparisons at equality. PostgreSQL may store finer values internally but must obey the same accepted input/output precision contract. All recurrence occurrence keys are normalized to the common encoding before dispatch. This deliberately resolves an earlier unspecified cross-driver boundary; review sub-millisecond requirements before freezing it.

Normalize once before command fingerprinting and preserve the canonical values on replay. Rate windows and positive duration policies must be valid multiples of the quantum, or be rejected during configuration validation; they must not silently round to zero. Check that downward expiry rounding still leaves a positive usable lease/deadline before accepting it. A provider cannot choose different fingerprint inputs for the same canonical command.

Redis sorted-set/Lua numeric representations need explicit range checks. Do not encode contemporary .NET ticks or a timestamp multiplied by a priority counter into a floating-point score without proving exactness. Millisecond epoch values within the supported range fit an exact-integer budget; lifetime bigint counters/fences may need decimal-string arithmetic or server integer operations with exact returned strings, not Lua tonumber. Reject overflow instead of silently losing uniqueness. Add round-trip and ordering tests near limits.

A database restore is not an ordinary worker restart. Lost receipts, occurrence high-water marks, and fencing counters can invalidate external assumptions in every driver. The restore runbook must quiesce old writers/workers, establish recovery scope, and reconcile external fencing/side effects before resuming. A new random local worker ID cannot make a rewound resource counter monotonic again. Never reuse a stale backup while advertising uninterrupted ownership history.

## 7. Business transactions and outbox boundary

Enlistment is limited to a compatible caller transaction that physically contains both business changes and Runwick writes. Same server, same hostname, or same Redis connection is not sufficient. The staged result does not mean the caller committed.

For a business store separate from the queue, define an outbox adapter boundary: the business transaction writes a stable message ID and payload; a relay dispatches with that ID; it marks delivered only after the queue's selected acknowledgement profile is confirmed. A crash between these steps causes repeat delivery. Deduplication retention must cover the allowed replay horizon; an expired window cannot promise unlimited deduplication. Restore can also resurrect delivery history and requires a runbook.

The [outbox closure](transactional-outbox.md) explicitly includes optional PostgreSQL/SQLite source adapters in v1. ReceiveOutbox uses a durable delivery identity independent of short request IDs, an immutable acceptance cutoff and retained mapping; generic Dispatch receipts alone are insufficient. No Redis MULTI/EXEC façade may claim atomicity with a PostgreSQL/SQLite business transaction.

## 8. Release gates and next design item

Implement PostgreSQL first, then port the semantic suite to SQLite and Redis. Every profile publishes its topology/configuration, tested versions, passed mandatory tests, unsupported features, and remaining durability limits. No skipped mandatory test counts as a pass. Use the [shared contract suite](driver-contract-suite.md) as the gate, not matching table names or matching benchmark throughput.

The API, wire, identity/serialization and timing packets reconcile behavior and lifetime policies. The [PostgreSQL integration](postgres-schema-and-operations.md) and [upgrade protocol](postgres-upgrades-and-compatibility.md) now add concrete gate/version/migration rules; other drivers must implement equivalent capability enforcement within their transaction/publication model. Provider selections and planning closure are recorded in the [final baseline](../architecture-baseline.md). Runtime implementation has not started.
