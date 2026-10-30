# PostgreSQL execution storage design

Status: proposed first implementation, 2026-10-28. Paired with the [internal command protocol](storage-command-protocol.md). This is a transaction and schema design, not an executed migration or validated performance claim.

Consolidation, 2026-10-29: [schema/operations](postgres-schema-and-operations.md) and [upgrade compatibility](postgres-upgrades-and-compatibility.md) now supply the full record inventory, gate-first locking, command/version catalog and migration rules. Those documents take precedence for these integration points; the detailed execution SQL below remains its foundation.

## 1. Scope and implementation boundaries

Implement the driver with Npgsql and explicit parameterized SQL. Business transactions are caller-owned; worker/control transactions are driver-owned. No EF dependency is required. Identifier interpolation is limited to validated, quoted schema identifiers; job data, names, keys, and actors are parameters.

First milestone covers dispatch, both uniqueness modes, reservation, execution begin, renewal, finalization, cancellation, manual retry, and recovery. Schedules, occurrence records, worker telemetry, and audit readers remain part of v1; their full query models follow this execution foundation.

Follow-up from the [storage review](../storage-design-inputs.md) and [worker-engine design](worker-engine-internals.md): add the combined lane readiness query, scope metadata lookup, worker capability/presence records, and bounded payload-header projection. Rate admission follows the first execution milestone but precedes v1 completion; permits alone are not rate limits. The identity/configuration packet recommends the existing low/medium/high CHECK for initial v1, pending architecture review.

That follow-on design is now drafted in [capacity control](capacity-control.md): separate rate-scope/grant records, concurrency-before-rate lock ordering, and atomic Begin/defer semantics. [Scheduling internals](scheduling-internals.md) adds versioned definitions, occurrence keys, cursor records, and materialization lock ordering. Treat these as explicit schema/protocol additions, not features already covered by the record list below.

Normal engine transactions use READ COMMITTED with explicit row locks and short timeouts. This is deliberate: after waiting for a scope lock, a subsequent statement must see permits committed by its prior holder. Do not reuse a permit count read before locking. Caller-owned dispatch transactions at other supported isolation levels may require the caller to retry the entire business transaction. [PostgreSQL isolation semantics](https://www.postgresql.org/docs/18/transaction-iso.html)

## 2. Physical records

Notation: all fields are non-null unless marked `?`. Every table is scoped by `application text`; IDs are `uuid`; all instants are `timestamptz`. The library validates bounded names/keys and limits JSON/error size. PKs and unique constraints below include application scope. Numeric counters have nonnegative CHECK constraints and checked overflow handling.

### jobs — frequently queried state

```text
application text, job_id uuid                         PK(application, job_id)
job_name text, payload_version integer                version >= 1
serializer_profile_id text, serializer_profile_version integer  version >= 1
queue text, priority text                            priority IN ('low','medium','high')
state text                                           CHECK allowed lifecycle strings
eligible_at timestamptz, created_at timestamptz
generation integer, revision bigint                  generation >= 1; revision >= 1
reservation_count bigint, execution_count bigint
current_attempt_id uuid?, worker_instance_id uuid?
ownership_token uuid?, lease_expires_at timestamptz?
cancel_requested_at timestamptz?, cancel_reason text?
terminal_at timestamptz?, terminal_reason text?
active_unique_key text?
```

Allowed states: pending, reserved, running, retrying, succeeded, failed, cancelled. A CHECK requires all four ownership columns together for reserved/running and all null otherwise. Terminal states require terminal_at; nonterminal states require it null. Cancellation metadata can remain on a succeeded job as historical evidence, and is cleared for a new manual-retry generation after its prior value is audited.

Queue/identity/priority and concurrency requirement membership are immutable during a generation in this milestone. Moving live jobs between queues is not a hidden mutation supported by changing a field.

### job_payloads — immutable data

```text
application text, job_id uuid                         PK; FK → jobs, ON DELETE CASCADE
payload_utf8 bytea, payload_encoding_version integer  original UTF-8; encoding version 1
serializer_profile_id text, serializer_profile_version integer
headers jsonb
```

Keep payload bytes out of candidate and heartbeat reads. A separate payload row is the proposed layout; benchmark it against inline payload storage before declaring it a performance improvement. headers contains bounded trace/application metadata, not credentials or arbitrary serialized CLR types.

The [identity/serialization decision](identity-and-serialization.md) supersedes the earlier payload jsonb proposal with original byte preservation. Validate bounded JSON before writes; drivers must not normalize the execution source. Insert identical immutable profile ID/version in jobs and job_payloads atomically; migration constraints/driver conformance must enforce consistency. No full jsonb payload shadow is required. Control/policy JSON remains structured jsonb where its normalized schema permits it.

### job_generations — policy and accounting history

```text
application text, job_id uuid, generation integer     PK; FK → jobs
created_at timestamptz, created_by text?
created_reason text                                  initial | scheduled | manual_retry | rerun_succeeded (immutable; added 2026-10-30)
max_executions integer                               > 0
max_unstarted_abandonments integer                    > 0
execution_timeout_ms bigint?                         > 0 when present
retry_policy jsonb, retry_policy_version integer
unstarted_retry_policy jsonb, unstarted_retry_policy_version integer
started_count integer, consecutive_unstarted_count integer
```

The current generation exists before the job is eligible. The driver's transaction creates/advances these rows consistently; a deferred FK from jobs(application, job_id, generation) to this key verifies the current-generation link at commit. Policies are snapshotted; only counters change while a generation is current.

The [execution timing packet](execution-defaults-and-timing.md) requires explicit evaluator/jitter settings in both execution and unstarted-recovery policies. Recovery selects and persists delay once; pre-start recovery terminates at its separate guard limit. Manual retry copies both policies into the new generation. Defaults remain proposed and must be covered by wire/schema compatibility vectors.

### job_attempts — one reservation and its outcome

```text
application text, attempt_id uuid                     PK
job_id uuid, generation integer                      FK → job_generations
reservation_number bigint                            UNIQUE(application, job_id, reservation_number)
execution_number integer?                            UNIQUE(application, job_id, generation, execution_number)
worker_instance_id uuid, ownership_token uuid
reserved_at timestamptz, authorized_at timestamptz?
finished_at timestamptz?, execution_elapsed_ms bigint?
outcome text?, error jsonb?
```

Null execution_number means execution was never authorized. PostgreSQL's ordinary unique constraint permits multiple null values; do not use NULLS NOT DISTINCT here. An attempt is open while owned and closed exactly once; completed history is immutable. Add UNIQUE(application, job_id, attempt_id) and a deferred FK from jobs(application, job_id, current_attempt_id) to it, preventing association with another job's attempt.

Outcome strings include succeeded, retryable_failure, permanent_failure, user_cancelled, interrupted, timed_out, abandoned_before_start, abandoned_running, admission_deferred, and released (ReleaseReservation during graceful drain; accepted 2026-10-30). Preserve both observed execution error and applied disposition/final job reason when cancellation wins a race.

### concurrency_scopes and concurrency_permits

```text
concurrency_scopes:
  application text, scope_id uuid                     PK
  scope_kind text, scope_key text                     UNIQUE(application, scope_kind, scope_key)
  capacity integer                                   >= 0
  policy_revision bigint
  fencing_enabled boolean, fencing_counter bigint

job_concurrency_requirements:
  application text, job_id uuid, scope_id uuid         PK
  FK → jobs; FK → concurrency_scopes

concurrency_permits:
  application text, scope_id uuid, attempt_id uuid     PK
  job_id uuid, ownership_token uuid
  lease_expires_at timestamptz
  fencing_value bigint?
  FK → concurrency_scopes
  FK(application, job_id, attempt_id) → job_attempts
```

The requirement table resolves the job's queue/type/resource limits to stable scope IDs. Unlimited dimensions have no permit row. Local worker limits stay in memory and are acquired before calling the driver.

Expiry is duplicated in jobs and permits for indexed admission counts. Renewal updates all rows in one transaction and verifies the expected permit count. A mismatch is an invariant failure: roll back, surface diagnostics, and stop treating the lease as healthy. Do not repair it by silently extending only the job.

Scope keys are canonical and bounded; keyed resource names cannot be allocated without a configured cardinality/retention policy. Fencing is initially meaningful for exclusive resources (capacity one): increment the resource's persistent counter when granting ownership. Never reset/recreate that counter under the same resource identity. Resource fencing is useful only when the external resource checks it.

### queue_controls

```text
application text, queue text                         PK
paused boolean, revision bigint
updated_at timestamptz, updated_by text?
```

Queue registration initializes this row. Reservation reads it under FOR SHARE; pause/resume updates it under an exclusive row lock. A completed pause therefore serializes with already-running reservation transactions. Any reservation admitted before pause commits may still execute; pause revokes no lease.

### dispatch_keys — active and window records

```text
application text, job_name text, key_kind text, key text   PK
bound_job_id uuid?
business_fingerprint bytea?, policy_fingerprint bytea?, fingerprint_version integer?   (R3: two digests)
expires_at timestamptz?, updated_at timestamptz
```

key_kind is active or window. Active bindings have no TTL; terminal transition clears bound_job_id. Window bindings retain original job ID/fingerprint until expiry, even if job history has been purged. These historical references intentionally have no FK to jobs. No delete of a key row during an active acquisition; initial implementation retains reusable rows and expires bindings under row lock. Physical key garbage collection needs a separate race-safe protocol.

### storage_commands and queue_events

```text
storage_commands:
  application text, command_id uuid                  PK
  operation text, protocol_version integer, protocol_epoch integer
  fingerprint bytea, fingerprint_version integer, not_after timestamptz
  receipt_codec_version integer, retention_class text, retain_until timestamptz
  result jsonb?, decided_at timestamptz?

queue_events:
  application text, event_id bigint GENERATED ALWAYS AS IDENTITY
  PRIMARY KEY(application, event_id)
  job_id uuid?, attempt_id uuid?, command_id uuid?
  event_kind text, occurred_at timestamptz
  actor_id text?, details jsonb
```

storage_commands result/decided_at may be null only inside an uncommitted transaction. Driver code must never commit the placeholder; a deferred constraint trigger should enforce this when migrations are authored. Receipt payloads contain IDs and decision metadata, not full job payloads. Reservation metadata includes the original policy, grant, and resource fences; replay must not substitute a later generation's settings or current ownership. Reconstructing an old reservation after payload retention has expired must report missing history, not fabricate a runnable job.

queue_events is execution/history data, not authority for replaying jobs. The consolidated schema adds a separate management_audit family with its own retention and trusted actor fields. Successful administrative mutations write audit and receipt atomically; execution events do not substitute for that audit. Lease heartbeats do not create an event on every renewal.

## 3. Initial indexes

These are executable SQL shapes once the described tables exist, not a complete migration:

```sql
CREATE INDEX jobs_ready
ON spindle.jobs (application, queue, priority, eligible_at, job_id)
WHERE state IN ('pending', 'retrying');

CREATE INDEX jobs_expired_lease
ON spindle.jobs (application, lease_expires_at, job_id)
WHERE state IN ('reserved', 'running');

CREATE INDEX jobs_terminal_retention
ON spindle.jobs (application, terminal_at, job_id)
WHERE state IN ('succeeded', 'failed', 'cancelled');

CREATE INDEX permits_live_count
ON spindle.concurrency_permits (application, scope_id, lease_expires_at);

CREATE INDEX permits_attempt
ON spindle.concurrency_permits (application, attempt_id, scope_id);

CREATE INDEX command_retention
ON spindle.storage_commands (application, retain_until, command_id);

CREATE INDEX events_job_history
ON spindle.queue_events (application, job_id, event_id);
```

Attempt history is covered by its unique job/reservation-number index. Add dashboard-specific indexes only against actual query shapes. The ready query must use the same literal state predicate; the predicate must not depend on current time. Lease-expiry index updates add write cost on every renewal; benchmark WAL volume, vacuum pressure, and lease batch size. [Partial index behavior](https://www.postgresql.org/docs/18/indexes-partial.html)

## 4. Global lock order

For commands that touch an existing job:

First acquire store_metadata FOR SHARE → application_controls FOR SHARE and validate compatibility/mode, retaining the locks through commit. This prefix and the scheduler/rate extensions are consolidated in the schema/operations document. Special gate-update commands take exclusive gate locks directly before lower-level locks; never upgrade a gate after locking a job.

1. storage_commands row for that command.
2. queue_controls row, if needed; FOR SHARE for reservation. Pause changes only its control row plus audit/receipt.
3. jobs row, FOR NO KEY UPDATE. If a later operation batches jobs, sort by job ID and acquire all job locks before lower-level locks.
4. dispatch_keys rows, sorted by stable bytewise (job_name, key_kind, key).
5. concurrency_scopes rows, sorted by scope_id, FOR UPDATE.
6. Rate scopes, when required, in sorted scope-ID order after all concurrency scopes.
7. Associated generation, attempt, grant and permit mutations, reached only through the already-locked owning job/scope.

All code paths use the same ordering. Scheduler operations add schedule-row locks before queue/job/key locks and never nest a new command receipt below them. Lock scope rows explicitly in canonical UUID order. A process never holds these locks while executing a handler, sleeping for retry, or waiting on external I/O. FK/unique-index effects and caller transactions still require deadlock/retry tests.

Dispatch is a documented exception with no existing-job lock: command → sorted key rows → creation of a brand-new job/generation/requirements. Duplicate dispatch reads existing jobs without locking them and creates no FK to them in key records. It must never hold a key and then lock the existing job, which could deadlock against finalization/manual retry. Resolve existing scopes without update locks; insert missing scopes with ON CONFLICT DO NOTHING in canonical (scope_kind, scope_key) order before inserting requirement FKs. Take existing-scope FK locks in sorted scope-ID order. Provisioning must not update existing scope policies or acquire jobs/dispatch keys afterward. FK and concurrent scope-creation interactions must be covered in tests.

Policy edits lock only scope rows and never acquire job rows afterward. Lowering capacity blocks new admission until live usage falls below the new value; existing permits may renew while draining. Introducing/removing scope membership requires quiescing the affected queues in this first design; it is not safe to add a new hard ceiling while existing executions have no corresponding permits.

Known local deadlocks/timeouts roll back the entire operation and may be retried with the same command ID inside its deadline. Business transactions may hold application locks outside this ordering; the caller owns whole-transaction retries. [PostgreSQL row locks and deadlocks](https://www.postgresql.org/docs/18/explicit-locking.html)

## 5. Reservation transaction

The request selects one queue and one priority lane. Queue weighting and lane fairness are the pool manager's responsibility; storage atomically validates eligibility and capacity. This does not promise strict FIFO across concurrent consumers.

1. Insert/lock command receipt and return an existing matching result if present.
2. Lock queue control FOR SHARE; if paused, return QueuePaused without claiming work.
3. Select one candidate using the query below. Match the worker-supported job name/payload version/serializer profile ID/profile version tuple before claiming.
4. Load its generation and concurrency requirement IDs; lock required scope rows in order.
5. Read clock_timestamp() **after** all potentially blocking locks and reject an expired command deadline. Count permits with expiry greater than that instant in each locked scope. If any count is at capacity, return CapacityBlocked with candidate cursor/scope, without an attempt or any permit. All other mutating commands also recheck their deadline at this decision point.
6. Allocate attempt ID/token, increment lifetime reservation number, insert the open attempt, set job to reserved, and insert all permits with the same expiry. Increment resource fence counters where applicable.
7. Store result metadata and commit. Only after confirmed commit return a runnable reservation and payload.

```sql
-- Executed inside the transaction, after locking queue_controls.
SELECT j.job_id, j.eligible_at, j.generation, j.revision
FROM spindle.jobs AS j
WHERE j.application = @application
  AND j.queue = @queue
  AND j.priority = @priority
  AND j.state IN ('pending', 'retrying')
  AND j.eligible_at <= statement_timestamp()
  AND (@after_job_id::uuid IS NULL
       OR (j.eligible_at, j.job_id) > (@after_time, @after_job_id))
  AND EXISTS (
      SELECT 1
      FROM unnest(@names::text[], @versions::integer[],
                  @profile_ids::text[], @profile_versions::integer[])
           AS c(name, version, profile_id, profile_version)
      WHERE c.name = j.job_name AND c.version = j.payload_version
        AND c.profile_id = j.serializer_profile_id
        AND c.profile_version = j.serializer_profile_version
  )
ORDER BY j.eligible_at, j.job_id
LIMIT 1
FOR NO KEY UPDATE OF j SKIP LOCKED;
```

Validate all four capability arrays have equal lengths, valid non-null values and a bounded tuple count; deduplicate tuples before submission. Apply the same matching rule to ready probes. SKIP LOCKED avoids waiting on another claimant's job row; it does not prove snapshot completeness or remove every kind of contention. PostgreSQL documents it specifically as useful for queue-like consumption. [SELECT locking semantics](https://www.postgresql.org/docs/18/sql-select.html)

After CapacityBlocked, advance the candidate cursor in a new transaction rather than acquiring another job while retaining the first job's scope locks. Bound each scanning pass and retain continuation until it reaches the end, then wrap. Do not permanently starve later work by resetting to a blocked oldest job every poll. Paused/unsupported/capacity-blocked backlog is distinct from runnable backlog in later autoscaling metrics.

## 6. Begin, renew, and finalization SQL boundaries

Begin locks command → job, validates the handle/expiry using fresh database time, and atomically assigns the next generation execution number, changes state to running, and records authorized_at. A persisted cancellation closes the unstarted attempt and releases permits through the terminal-finalization lock order instead of authorizing user code. Begin commits before payload deserialization, DI activation, or middleware execution; setup exceptions are finalized as execution failures through Finish. No transaction spans that setup or user code.

Renew locks command → job → every associated scope; capture a new decision instant after locks. Require exact job/attempt/worker/token/generation, an owned state, and existing expiry > decision time. Update job and matching permits to decision time + lease duration in one transaction. Do not acquire new permits on renewal. Validate each permit still belongs to the same attempt/token.

The job-row update shape is:

```sql
UPDATE spindle.jobs
SET lease_expires_at = @new_expiry
WHERE application = @application AND job_id = @job_id
  AND current_attempt_id = @attempt_id
  AND ownership_token = @token
  AND worker_instance_id = @worker_id
  AND generation = @generation
  AND state IN ('reserved', 'running')
  AND lease_expires_at > @decision_time;
```

This update is not the complete renew algorithm: job/scope locks, permit updates, row-count checks, and the command receipt share its transaction. UPDATE zero rows means stale ownership, never success.

Finish locks command → job → active uniqueness key if terminal → scopes. Validate ownership against fresh time after locks, close the attempt, update generation/job state, delete permits by exact attempt and token, and persist event/receipt. For retries set eligible_at to database decision time plus the chosen delay. Preserve active uniqueness while retrying. Terminal outcomes clear only an active key still bound to this job; fixed-window keys are unchanged.

A retry delay is selected once for the command and stored in its result. Shared retry-policy evaluation can be called inside the transaction after the generation counter is known; commit replay never redraws it. Recovery uses the same versioned pure policy evaluator.

Finalization is valid at its serialized decision point while owning locks; a delayed commit can occur after that instant. No competing takeover can change the locked job before this transaction commits or rolls back. Keep transaction deadlines bounded, and revalidate returned leases before use if their remaining lifetime is insufficient.

PostgreSQL current_timestamp/now() are transaction-start values; clock_timestamp() advances during the transaction. The ownership check must not use an old transaction-start instant after waiting on a lock. [Time functions](https://www.postgresql.org/docs/18/functions-datetime.html)

## 7. Recovery, cancellation, and manual retry

Recovery probes the expired-lease index, then processes candidates individually through RecoverAsync. The command locks the current job and required key/scope rows, rechecks exact attempt/token and current expiry, then closes the abandoned attempt. It must not recover a lease that renewed after the initial probe.

Reserved expiry increments the unstarted guard; running expiry uses the execution count already consumed by Begin. A durable cancellation request wins over automatic recovery retry. Clear current ownership and delete only the expired attempt's permits. Expired permits are already excluded from admission counts, so availability does not depend on the sweeper having deleted them first.

Cancellation locks the job, checks generation/revision, and records the request plus audit. For pending/retrying jobs, it terminalizes and clears active uniqueness. For reserved/running jobs it leaves ownership intact. Renew replies carry the cancellation flag; the later control channel may reduce notification latency but is not the durable authority.

Manual retry locks job → active key → relevant scopes if validation requires them; checks expected terminal state/generation/revision; reacquires active uniqueness; inserts generation+1; and changes the job to pending with a new eligibility instant. It does not reset lifetime counters or change payload. Clear previous cancellation only as part of this transaction. If another job holds the active key, return UniquenessConflict with its ID and change nothing.

## 8. Unique dispatch transaction

Normalize/serialize the input and compute requested keys before taking locks. The driver uses one transaction for receipt, key bindings, job/payload/generation creation, and its event. Batch dispatch (added 2026-10-30) is the same transaction over up to 100 items: one receipt with per-ordinal results, all key rows across the batch created/locked in one sorted pass, then all new jobs, then concurrency and rate scope provisioning family by family — the materialization ordering in [scheduling internals](scheduling-internals.md#4-materialization-transaction). Under `ContinueOnConflict`, conflicting ordinals are recorded in the receipt and simply produce no rows.

Create missing key rows with INSERT ... ON CONFLICT DO NOTHING, in the same sorted key order used for locking, then SELECT ... FOR UPDATE. Re-evaluate bindings and fixed-window expiry after lock acquisition using storage time. A duplicate active binding returns its job ID without replacing its payload. A fixed-window binding also compares the canonical business fingerprint; a mismatch returns a conflict, while a differing policy fingerprint with a matching business fingerprint returns Reused with PolicyDrift set (R3).

If both policies apply, an existing window binding identifies its original job even if that job is terminal; an absent/unbound active key need not be reclaimed merely to answer a duplicate request. A new window key that encounters an existing active job becomes an alias for that job in the same transaction. If simultaneous valid window and active bindings identify different jobs, this proposal returns an explicit conflict rather than selecting one or rewriting either binding. This composition policy requires review with the fingerprint specification.

The separate [wire/fingerprint proposal](dispatch-wire-contract.md) now defines versions, included effective dispatch settings, and canonical input encoding. It includes resolved defaults, not merely explicitly supplied options, and distinguishes command replay from windowed request equality. Until its codec/compatibility vectors are approved and validated, windowed dispatch remains a design rather than implementation-ready wire compatibility. Neither locale-dependent JSON formatting nor raw CLR type names are acceptable identity inputs.

In a supplied business transaction, all writes use its connection and remain staged. The driver does not commit, dispose, reopen, or invisibly retry the enclosing transaction. A savepoint may provide operation-local cleanup if the supported transaction mode allows it, but cannot substitute for whole-transaction retry after serialization failure.

## 9. Notification, migrations, and retention

Polling is sufficient for correctness. The first implementation can signal its local worker after its own commit. A caller-owned transaction has no implicit post-commit callback; workers discover it by polling unless the application opts into a supported commit notification path.

PostgreSQL NOTIFY inside a transaction is delivered only on commit, but notification queue exhaustion can fail the commit. Do not quietly make notification infrastructure a new failure condition for business transactions. Introduce LISTEN/NOTIFY as an optional, tested optimization with reconnect/resubscribe and fallback polling. Notifications contain only non-sensitive wake-up hints. [NOTIFY semantics](https://www.postgresql.org/docs/18/sql-notify.html)

Keep Spindle's migration ledger separate from EF. The [upgrade contract](postgres-upgrades-and-compatibility.md) specifies deployment-owned migration locking/checksums, transactional compatibility gates, expand/backfill/activate/contract phases and interrupted-DDL recovery. Workers check compatibility but do not invent migrations.

Retention uses bounded transactions. Default detail cleanup keeps required job/attempt/generation identity while removing eligible payload/error details; it records history expiry. Physical job purge requires all correctness dependencies to have ended and FK-safe removal order. Window keys, rate grants, audit, receipts and fences have independent lifetimes. Missing retained payload blocks manual retry with HistoryUnavailable. Apply sanitization before persistence as well as authorized projection; UI-only redaction is insufficient.

## 10. Validation matrix before implementation approval

| Test | Interleaving/fault | Required assertion |
|---|---|---|
| Last permit | Two jobs, two replicas, capacity one | At most one valid grant; loser has no attempt/partial permit |
| Multiple scopes | Jobs need A+B and B+A | Canonical locking avoids application-created lock-order cycles |
| Pause vs claim | Pause waits behind reservation share lock | Claims serialize before pause; no later admission while paused |
| Lease vs recovery | Pause renewal after job/scope lock acquisition | Fresh-time check prevents expired renewal; recover rechecks token |
| Lost reserve reply | Disconnect after commit before response | Same command resolves same attempt; no second reservation |
| Lost begin reply | Begin commits, reply lost | One counted execution; local invocation is once after revalidation |
| Lost finish reply | Finish commits, reply lost | Replay returns committed outcome without rerunning handler |
| Command expiry | Delete expired receipt, replay old command; separately block past deadline on a scope | No new mutation; expired/unknown result |
| Setup failure | Fail deserialization or DI activation after begin | Counted execution closes through Finish; no stranded reservation |
| Pre-start death | Kill after reserve, before begin | Unstarted guard increments, execution count does not |
| Post-begin death | Kill after begin, before user code | Authorized execution counted once; recovery bounded |
| Stale finish | B takes over, A resumes | A changes no current state or B permit |
| Cancel vs success | Swap lock acquisition order | One consistent outcome, preserved request/audit |
| Duplicate dispatch | Parallel same active/window keys | One job or explicit mismatch conflict; no orphan key bindings |
| Retry vs new dispatch | Reuse active key while old job is terminal | One active binding; retry conflict leaves generation unchanged |
| Scoped transaction rollback | Insert business row and job, then rollback | Neither is visible; no receipt or key survives |
| Capability filtering | Old/new workers share queue | Unsupported versions are never reserved by old worker |
| Session clocks | Non-UTC sessions and delayed lock acquisition | Lease decisions use actual storage instants |
| Capacity decrease | Reduce below current live count | Existing work drains; no new grants until below new cap |

Run against real PostgreSQL connections/processes; mocking IJobReservationStore cannot prove SQL locking. Start with controlled barriers and process kill points; add randomized repetitions only after deterministic interleavings are covered.

Performance gates to measure, not claim: idle queries per replica/queue, reservation and renewal p95/p99, time waiting for scope locks, WAL bytes/job, backlog-drain throughput, ready-query plans at realistic history sizes, and heartbeat deadlines during contention. Counted permits serialize admission/renewal for each shared scope; that is the baseline's explicit scalability risk. Compare numbered slots or aggregated permits only if measurements justify their added complexity.

## 11. Implementation sequence and unresolved gates

1. Approve command semantics and reserved/begin accounting; finalize request-fingerprint encoding.
2. Author migrations, parameterized statements, command receipt codec, and schema compatibility checks.
3. Prove reserve/begin/renew/finish plus last-permit and lost-response races.
4. Add recovery, cancellation, manual retry, and both deduplication policies; run the matrix.
5. Connect the shared worker engine, then measure contention and determine batching/wake-up optimizations.

The [final baseline](../architecture-baseline.md), [consolidated schema](postgres-schema-and-operations.md) and concrete identity/timing/capacity packets now select accounting, fingerprints, limits and retention. The authoritative first-milestone order is SP-001–010; cancellation/manual retry controls follow in SP-011. The older sequence above is superseded on that point. Contention and fault tests remain unexecuted.