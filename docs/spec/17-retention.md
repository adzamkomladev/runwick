# 17 — Retention and cleanup (SP-021)

**Sources:** [management-retention-defaults.md](../architecture/management-retention-defaults.md) (record classes, horizons, cleanup budgets, OPS-01–06), [management-and-dashboard.md](../architecture/management-and-dashboard.md) §6 (audit and retention) and §8 (MGMT-11), [observability-and-operations.md](../architecture/observability-and-operations.md) §6 and OBS-10, [driver-contract-suite.md](../architecture/driver-contract-suite.md) (RET-01), [03-storage-protocol.md](03-storage-protocol.md) §8.4 (receipt classes), [04-postgres.md](04-postgres.md) §5.1 and §7.16 (lock order, receipt cleanup), [08-controls-and-completion.md](08-controls-and-completion.md) (ManualRetry, audit), [10-rate-admission.md](10-rate-admission.md) §3.3 and §6.3 (grants, edit freeze), [13-scheduler.md](13-scheduler.md) §9 (occurrences), [14-outbox.md](14-outbox.md) §3.1 and §6.4 (outbox horizons), [16-management.md](16-management.md) (session, routes), [v1-delivery-plan.md](../v1-delivery-plan.md) §6 (SP-021).

## In short

Until now nothing in Runwick deletes anything: payloads, attempt errors, finished jobs, receipts, audit rows and history all grow forever. SP-021 adds a background cleanup that removes old records in small, resumable steps, and never removes a record that something still depends on.

- **What gets deleted, and when.** Each kind of record (a "record class") has its own age limit. Payloads and error details of jobs that finished well go after 7 days, of failed jobs after 30 days. The rest of a finished job (its summary, generations, attempts) goes after 90 days. Audit goes after 90 days, scaling history after 7, schedule occurrence history after 30, dead worker entries 24 hours after they expire. An administrator can change these numbers.
- **What is never deleted early.** Records that keep the system correct have fixed rules that no setting can shorten: command receipts until their replay window ends, uniqueness bindings until their window ends, rate grants until they leave the rate window, outbox mappings until their acceptance horizon has passed by 24 hours. Fences, scopes and schedules are never deleted.
- **Two steps for jobs.** First the *detail* expires: the payload and error text are deleted, but the job's identity, state and history stay, and the job is visibly marked "detail expired". Later the whole job row goes. A job that is running, waiting or being retried is never touched.
- **How it runs.** Every worker process runs a cleanup loop. It handles one small batch at a time (at most 100 records or 1 MiB of detail, aiming at 0.5 s of database work), waits 1 s, and sleeps 60 s when there is nothing to do. It skips rows that are locked, so it never waits on live work, and pauses when the worker is under pressure. Several processes can run it at once; a progress row per class makes sure only one batch of a class runs at a time, and lets cleanup resume where it stopped after a crash.
- **What people see.** The management API gains a retention page (policy and progress) and a revisioned, audited policy edit. Jobs show when their detail expired. Cleanup itself writes one summary audit row per class per day, not one row per deleted record.

Not in SP-021: an explicit "delete this now" request, legal hold, per-queue policies, backup erasure (backups and exports have host-owned retention; deleting a row does not erase copies), the dashboard page (SP-024), SQLite and Redis cleanup (they implement the same classes and predicates in SP-019 and SP-025).

## Changes

- (2026-11-08) New document.
- (2026-11-08) Amends the 03 §4 operation catalog: `CleanupBatch/1` writes **no receipt** (see §4.1); `EditRetentionPolicy/1` is a new administrative operation in class `durable`.

## 1. Record classes

A record class is a set of rows with one deletion predicate. "Policy" classes take their age from the retention policy (§2); "fixed" classes have a predicate set by the protocol that no policy can change.

| Class (`record_class`) | Rows | Kind | Default | Deletable when (all must hold) |
|---|---|---|---|---|
| `job_detail_completed` | `job_payloads` row, `job_attempts.error` text, of jobs in `succeeded`/`cancelled` | policy | 7 d | job terminal; `terminal_at < now − horizon`; `detail_expired_at IS NULL`; job row lockable (§4.3) |
| `job_detail_failed` | same, jobs in `failed` | policy | 30 d | same, `state = 'failed'` |
| `job_history` | `jobs` row and its cascade (generations, attempts, payload, concurrency and rate requirements) | policy | 90 d | job terminal; `terminal_at < now − horizon`; detail already expired; no live window binding to the job (§3.3); no permit row of the job |
| `job_events` | `queue_events` | policy (= `job_history`) | 90 d | `occurred_at < now − horizon` |
| `management_audit` | `management_audit` | policy | 90 d | `occurred_at < now − horizon` |
| `schedule_audit` | `schedule_audit_ranges` | policy (= `management_audit`) | 90 d | `created_at < now − horizon` |
| `scaling_events` | `scaling_events` | policy | 7 d | `created_at < now − horizon` |
| `schedule_occurrences` | `schedule_occurrences` | policy | 30 d | `materialized_at < now − horizon` (the schedule's `evaluated_through` high-water mark stays; 13 SCH-05) |
| `worker_presence` | `worker_instances` and cascade (capabilities, subscriptions) | policy | 24 h | `expires_at < now − horizon`. Presence is not authority (00 §9 rule 11; 09 RDY-02): no ownership check is needed |
| `receipts_ownership`, `receipts_durable` | `storage_commands` partitions | fixed | — | `retain_until < now` (04 §7.16, unchanged). An uncommitted placeholder is invisible and is never deleted |
| `dispatch_keys` | `dispatch_keys` | fixed | — | window key with `expires_at < now`, or any key with `bound_job_id IS NULL` and `updated_at < now − 1 h` [choice] |
| `rate_grants` | `rate_grants` | fixed | — | under the rate-scope lock: `authorized_at ≤ last_decision_at − window_ms` (§3.4) |
| `outbox_deliveries` | `outbox_deliveries` | fixed | — | under the delivery-identity lock: `retain_until < now` (14 §6.4) |

Never deleted by any class: `concurrency_scopes` (fences), `rate_scopes`, `queue_controls`, `application_controls`, `schedules`, `schedule_definitions`, `pool_controls`, `store_metadata`, `schema_migrations`, jobs in a non-terminal state, and any pending publication or recovery intent.

The source outbox (`runwick_outbox.outbox_messages`, in the application's database) is cleaned by the relay, not by this loop (§6).

## 2. Retention policy

One policy per application.

```csharp
public sealed record RetentionPolicy(
    TimeSpan? CompletedDetail,   // succeeded/cancelled payload + error text; default 7 d
    TimeSpan? FailedDetail,      // failed payload + error text; default 30 d
    TimeSpan? JobHistory,        // job summary, generations, attempts, events; default 90 d
    TimeSpan? Audit,             // management_audit, schedule_audit_ranges; default 90 d
    TimeSpan? ScalingEvents,     // default 7 d
    TimeSpan? ScheduleOccurrences, // default 30 d
    TimeSpan WorkerPresence);    // default 24 h; always finite
```

Rules:

- `null` means "keep forever" [choice]: an administrator who must keep audit for longer than any bound can do so. Presence is always finite.
- Every value is whole hours, between 1 h and 87,600 h (10 years). `Audit` is at least 7 d [choice].
- `JobHistory ≥ max(CompletedDetail, FailedDetail)`, with `null` as infinity: detail never outlives its job, and a job's row is deleted only after its detail has expired. A policy that breaks this is `invalid_retention_policy` (400).
- A policy changes only policy classes. Fixed classes (§1) are not in the policy at all; that is how a shortened policy cannot erase a receipt, binding, grant, mapping, intent or fence (OPS-04).
- An edit applies to batches that start after it commits. A batch that started earlier finishes under the old values.

### 2.1 Storage (PostgreSQL)

```sql
CREATE TABLE runwick.retention_policies (
    application              runwick.rw_identifier NOT NULL,
    revision                 bigint      NOT NULL,
    completed_detail_hours   integer,
    failed_detail_hours      integer,
    job_history_hours        integer,
    audit_hours              integer,
    scaling_events_hours     integer,
    occurrence_hours         integer,
    presence_hours           integer     NOT NULL,
    updated_at               timestamptz NOT NULL,
    updated_by               text,
    CONSTRAINT retention_policies_pk PRIMARY KEY (application),
    CONSTRAINT retention_policies_revision_ck CHECK (revision >= 1),
    CONSTRAINT retention_policies_range_ck CHECK (
        COALESCE(completed_detail_hours, 1) BETWEEN 1 AND 87600 AND COALESCE(failed_detail_hours, 1) BETWEEN 1 AND 87600
        AND COALESCE(job_history_hours, 1) BETWEEN 1 AND 87600 AND COALESCE(audit_hours, 168) BETWEEN 168 AND 87600
        AND COALESCE(scaling_events_hours, 1) BETWEEN 1 AND 87600 AND COALESCE(occurrence_hours, 1) BETWEEN 1 AND 87600
        AND presence_hours BETWEEN 1 AND 87600),
    CONSTRAINT retention_policies_order_ck CHECK (
        job_history_hours IS NULL
        OR (completed_detail_hours IS NOT NULL AND failed_detail_hours IS NOT NULL
            AND job_history_hours >= GREATEST(completed_detail_hours, failed_detail_hours))),
    CONSTRAINT retention_policies_updated_by_ck CHECK (updated_by IS NULL OR octet_length(updated_by) BETWEEN 1 AND 256)
);
```

No row means the defaults, reported as revision 0. The first edit inserts the row with revision 1 (`expectedRevision = 0`).

### 2.2 `EditRetentionPolicy/1`

Administrative operation, class `durable`, NotAfter 5 min (cap 10 min), receipt `NotAfter + 24 h` (08 §3). Lock order: gates → receipt → `retention_policies` row `FOR UPDATE` (new rank 2b, §4.3). Body: command ID, expected revision, the seven values, actor, reason (≤ 512 bytes). Results: Applied (revision + 1), `VersionConflict revision_conflict` (with actual), `InvalidState no_change` (same values), `InvalidRequest invalid_retention_policy`. Applied writes an audit row `operation = 'EditRetentionPolicy'`, `target_kind = 'application'`, `details = {"before": {…hours}, "after": {…hours}}`, in the same transaction.

## 3. Deletion rules per class (PostgreSQL)

All statements run inside the batch frame of §4.1. `@now` is `clock_timestamp()` read after the class lock. `@cursor_at, @cursor_id` is the class's progress cursor (§4.2).

### 3.1 Detail expiry (`job_detail_completed`, `job_detail_failed`)

```sql
-- C: candidates, locked; skipped if any transaction holds the job (a retry, a cancel, a reveal)
SELECT j.job_id, j.terminal_at,
       COALESCE(octet_length(p.payload_utf8), 0)
         + COALESCE((SELECT sum(octet_length(a.error::text)) FROM runwick.job_attempts a
                     WHERE a.application = j.application AND a.job_id = j.job_id AND a.error IS NOT NULL), 0) AS bytes
FROM runwick.jobs AS j
LEFT JOIN runwick.job_payloads AS p ON p.application = j.application AND p.job_id = j.job_id
WHERE j.application = @application
  AND j.state = ANY(@states)                       -- {succeeded, cancelled} or {failed}
  AND j.detail_expired_at IS NULL
  AND j.terminal_at < @now - @horizon
  AND (j.terminal_at, j.job_id) > (@cursor_at, @cursor_id)
ORDER BY j.terminal_at, j.job_id
LIMIT @rows                                         -- 100
FOR NO KEY UPDATE OF j SKIP LOCKED;
```

The driver keeps the prefix whose running `bytes` total is ≤ 1 MiB (always at least one row), then for those job IDs:

```sql
DELETE FROM runwick.job_payloads WHERE application = @application AND job_id = ANY(@ids);
UPDATE runwick.job_attempts
SET error = jsonb_build_object('code', error -> 'code', 'expired', true)
WHERE application = @application AND job_id = ANY(@ids) AND error IS NOT NULL;
UPDATE runwick.jobs
SET payload_retained = false, detail_expired_at = runwick.ms_floor(@now)
WHERE application = @application AND job_id = ANY(@ids);
```

- The row lock and the predicate are in one statement, so a ManualRetry or RerunSucceeded that commits first makes the job non-terminal and the row is not selected; one that comes after sees `payload_retained = false` and returns `HistoryUnavailable payload_unavailable` (08 §3, ManualRetry). Neither can see half a cleanup (OPS-02).
- Detail expiry does not change `revision` or `updated_at` [choice]: it is not a state transition, and a control sent with the current revision still gets its normal answer (`payload_unavailable` for a retry).
- The error code stays (management lists show it, 16 §2); the exception type, message and stack go.
- A RevealPayload or RevealError (16 §3) holds no job lock; one that reads after the commit returns 410 `detail_expired`. One that read before has already written its disclosure audit; nothing else is promised.

### 3.2 Job history (`job_history`)

```sql
SELECT j.job_id, j.terminal_at
FROM runwick.jobs AS j
WHERE j.application = @application
  AND j.state IN ('succeeded', 'failed', 'cancelled')
  AND j.terminal_at < @now - @horizon
  AND j.detail_expired_at IS NOT NULL
  AND (j.terminal_at, j.job_id) > (@cursor_at, @cursor_id)
  AND NOT EXISTS (SELECT 1 FROM runwick.dispatch_keys d
                  WHERE d.application = j.application AND d.bound_job_id = j.job_id
                    AND (d.key_kind = 'active' OR d.expires_at > @now))
  AND NOT EXISTS (SELECT 1 FROM runwick.concurrency_permits pm
                  WHERE pm.application = j.application AND pm.job_id = j.job_id)
ORDER BY j.terminal_at, j.job_id
LIMIT @rows
FOR UPDATE OF j SKIP LOCKED;

DELETE FROM runwick.jobs WHERE application = @application AND job_id = ANY(@ids);   -- FK cascade
```

- Index `jobs_terminal_retention` serves the scan. The binding check uses the new index `dispatch_keys_bound_job` (§5).
- A window binding is only ever set while its job is new or active (04 §7.2 step 5, cases e/f), and an active binding is cleared at the terminal transition (04 §7.10 K′). So the set of live bindings of a terminal job can only shrink, and checking without locking `dispatch_keys` is safe.
- Rows that fail a dependency check are stepped over by the cursor and found again on the next pass (§4.2).
- A purged job has no tombstone [choice]. `GetJob` of its ID is 404, as for an ID that never existed; references that outlive it (schedule occurrences, outbox mappings, rate grants, receipts) keep the job ID as a logical reference. Receipt replay of its Dispatch returns the recorded reply, as before (03 §8.6).
- `queue_events` of the job are a separate class (`job_events`) with the same horizon, so event cursor readers (08 §7) lose old rows by age, not by job.

### 3.3 Dispatch keys (`dispatch_keys`)

```sql
SELECT d.job_name, d.key_kind, d.key
FROM runwick.dispatch_keys AS d
WHERE d.application = @application
  AND ((d.key_kind = 'window' AND d.expires_at < @now)
       OR (d.bound_job_id IS NULL AND d.updated_at < @now - interval '1 hour'))
ORDER BY d.job_name, d.key_kind, d.key
LIMIT @rows
FOR UPDATE SKIP LOCKED;
-- then DELETE by primary key
```

The scan uses two partial indexes (§5), queried one after the other, each limited to `@rows`.

A deleted key is, for Dispatch, the same as an expired or unbound one (04 §7.2 step 5: "W absent or not live"). One change to Dispatch is needed because its step 3 inserts with `ON CONFLICT DO NOTHING` and then locks: if cleanup deletes a row between those two statements, the lock returns fewer rows than keys. **Dispatch repeats step 3 for the missing keys, at most 3 times, then fails `InvariantFault key_row_missing`** [choice]. The repeat happens before the decision time is read, so the decision is unchanged.

### 3.4 Rate grants (`rate_grants`)

Per rate scope, keyset by `rate_scope_id`, one scope per batch:

```sql
SELECT rate_scope_id, window_ms, last_decision_at
FROM runwick.rate_scopes
WHERE application = @application AND rate_scope_id > @cursor_id
ORDER BY rate_scope_id
LIMIT 1
FOR UPDATE;                                   -- lock_timeout 100 ms: on timeout, move to the next scope

DELETE FROM runwick.rate_grants
WHERE (application, rate_scope_id, attempt_id) IN (
    SELECT application, rate_scope_id, attempt_id FROM runwick.rate_grants
    WHERE application = @application AND rate_scope_id = @scope
      AND authorized_at <= @last_decision_at - @window_ms * interval '1 millisecond'
    ORDER BY authorized_at LIMIT @rows);
```

- The cutoff uses the scope's logical time `last_decision_at`, not the clock. Logical time never goes backward (10 §4 step 4), so the next Begin's window `(t − W, t]` with `t ≥ last_decision_at` contains no deleted grant.
- **Enlargement race (10 §7, deferred row).** An edit and cleanup both hold the scope lock, so cleanup always uses the window that is current under the lock. An enlarging edit sets `not_before = t + newWindow` (10 §6.3): no admission happens until every grant that could fall in the new window was written after the edit, so grants pruned under the old window can never be needed. A scope with `last_decision_at IS NULL` has no grants.

### 3.5 Outbox mappings (`outbox_deliveries`)

Candidates in `retain_until` order (index `outbox_deliveries_cleanup`), `LIMIT @rows`, no lock. For each, in identity order: the delivery-identity advisory lock (04 §5.1 rank 3a), then `DELETE … WHERE … AND retain_until < @now`. Every transport command has `NotAfter ≤ AcceptUntil < retain_until` (14 C-5), so no command can apply after its mapping is gone (OUT-02).

### 3.6 The other policy classes

`job_events`, `scaling_events`, `schedule_occurrences`, `worker_presence`: `DELETE … WHERE key IN (SELECT key … WHERE <age predicate> AND key > @cursor ORDER BY <age column>, key LIMIT @rows FOR UPDATE SKIP LOCKED)`. Indexes: `events_retention` (exists), `worker_instances_expiry` (exists), `scaling_events_retention` and `schedule_occurrences_retention` (new, §5).

`management_audit`, `schedule_audit`: scanned in primary-key order (`audit_id` ascending), deleting while `occurred_at` (or `created_at`) is older than the horizon and **stopping at the first row that is not** [choice]. Identity order is close to time order; a row committed out of order is kept a little longer than its horizon, never deleted early. No new index is needed.

### 3.7 Receipts

Unchanged from 04 §7.16, one partition per class, `LIMIT @rows`. A Reserve replay whose payload was removed by detail expiry stays `HistoryUnavailable payload_unavailable` (03 §8.6); ownership receipts live 10 min past NotAfter, well below the shortest detail horizon (1 h), so it is rare.

## 4. The cleanup loop

### 4.1 One batch

`CleanupBatch/1` (internal `IRetentionStore.RunBatchAsync(recordClass, limits)`): one transaction that handles at most `BatchRows` root rows (100) of one class and at most 1 MiB of decoded detail. It writes **no receipt** [choice]: every deletion re-checks its predicate under its own lock, so running a batch twice — after an unknown outcome or on two processes — deletes nothing extra. An unknown outcome is not resolved; the next batch simply runs.

Frame: `BEGIN; SET LOCAL lock_timeout = '100ms', statement_timeout = '2s'`; gates `FOR SHARE` (04 §5.1 ranks 1–2); lock the class's progress row (§4.2) with `FOR UPDATE SKIP LOCKED` — if it is locked, another process is running this class: `ROLLBACK`, result `skipped_locked`; read the policy row (no lock); run §3; update the progress row; `COMMIT`.

A lock timeout or statement timeout rolls back the batch, records `last_error_code` (`lock_timeout`, `statement_timeout`, or the SQLSTATE class) in a separate small transaction, and the class backs off (§4.4). Nothing is half-deleted.

### 4.2 Progress and summaries

```sql
CREATE TABLE runwick.cleanup_progress (
    application        runwick.rw_identifier NOT NULL,
    record_class       text COLLATE "C" NOT NULL,
    cursor_at          timestamptz,           -- last scanned sort instant (null: pass starts at the beginning)
    cursor_key         text,                  -- last scanned key, canonical text
    pass_started_at    timestamptz,
    caught_up_at       timestamptz,           -- last time a pass found nothing eligible
    last_batch_at      timestamptz,
    last_rows          integer     NOT NULL DEFAULT 0,
    total_rows         bigint      NOT NULL DEFAULT 0,
    summary_day        date,
    day_rows           bigint      NOT NULL DEFAULT 0,
    day_batches        integer     NOT NULL DEFAULT 0,
    day_failures       integer     NOT NULL DEFAULT 0,
    last_error_code    text COLLATE "C",
    last_error_at      timestamptz,
    CONSTRAINT cleanup_progress_pk PRIMARY KEY (application, record_class),
    CONSTRAINT cleanup_progress_class_ck CHECK (record_class IN (
        'job_detail_completed', 'job_detail_failed', 'job_history', 'job_events', 'management_audit',
        'schedule_audit', 'scaling_events', 'schedule_occurrences', 'worker_presence',
        'receipts_ownership', 'receipts_durable', 'dispatch_keys', 'rate_grants', 'outbox_deliveries')),
    CONSTRAINT cleanup_progress_error_ck CHECK (last_error_code IS NULL OR octet_length(last_error_code) <= 64)
);
```

- Rows are created on first use with `INSERT … ON CONFLICT DO NOTHING` in their own transaction.
- **Cursor.** Each batch scans forward from the cursor and stores the last *scanned* key (deleted or stepped over). When a batch returns fewer than `BatchRows` candidates, the pass is complete: the cursor resets to null and, if the pass deleted nothing, `caught_up_at = now`. A crash loses at most the uncommitted batch; the next batch resumes from the stored cursor. No scan uses OFFSET.
- **Daily summary audit.** When a batch sees `summary_day` older than the current UTC day, it first inserts one `management_audit` row for the closed day: `operation = 'RetentionSummary'`, `target_kind = 'application'`, `target_id = record_class`, actor subject `runwick:retention` with scheme `system`, `command_id` = the first 16 bytes of SHA-256(`application ‖ record_class ‖ day`) as a UUID, `details = {"day", "rows", "batches", "failures"}`; then it resets the day counters. At most 14 rows per application per day. The rows are themselves in class `management_audit`, so they expire like any audit row and the growth is bounded (no recursion). Policy edits are audited per §2.2; deleted rows are not audited individually.

### 4.3 Locks and order

Additions to 04 §5.1:

```text
2a cleanup_progress           FOR UPDATE SKIP LOCKED   (cleanup only; before anything below)
2b retention_policies         FOR UPDATE (EditRetentionPolicy only); cleanup reads it without a lock
```

Cleanup then takes only locks that already exist, in their ranks: outbox identity (3a), jobs (5, `SKIP LOCKED`), dispatch keys (6, `SKIP LOCKED`), rate scopes (7, with the 100 ms lock timeout), dependent rows through the already-locked owner (8). It never takes a concurrency-scope lock and never waits on a job row. Job rows are locked `FOR UPDATE` (history purge) or `FOR NO KEY UPDATE` (detail expiry), the same strength the job's own commands use, so any command holding the row makes cleanup skip it.

### 4.4 Scheduling and budgets

`RetentionOptions` (on the worker builder, `w.Retention(r => …)`):

| Option | Default | Bounds |
|---|---|---|
| `Enabled` | true | — |
| `BatchRows` | 100 | 1 – 1,000 |
| `BatchDetailBytes` | 1 MiB | 64 KiB – 16 MiB |
| `BatchTarget` | 500 ms | 50 ms – 2 s (statement timeout is 4 × target, at least 2 s) |
| `BatchPause` | 1 s | 0 – 60 s |
| `IdlePoll` | 60 s | 1 s – 1 h |
| `FailureBackoff` | 5 s doubling to 5 min | — |

- **Where it runs.** A non-critical background loop `retention` in every worker process (like `scaler`, 05), one per store group [choice]. Producer-only processes, schedulers and the standalone management host do not run it. Processes cooperate through the progress rows; there is no leader.
- **One batch at a time per process**, on the background class's heavy-work slot (06 §1.7: ≤ 1 of the 2 background calls). The slot is held until the provider task has ended, even after the local deadline.
- **Order.** The loop visits classes round-robin. After a batch of a class it waits `BatchPause` before that class's next batch; other classes may run meanwhile. A class whose pass completed with nothing deleted sleeps `IdlePoll`.
- **Yield.** No batch starts while the worker group's pressure stop is active (06 §1.6) or while the engine is draining. The 500 ms target is not preemption: a batch in progress finishes.
- **Throughput at defaults.** About one batch per second per process across all classes, so roughly 100 rows/s for a busy class with one process, more with several processes on different classes. A store that finishes more than about 5 million jobs per day needs larger `BatchRows`, a shorter `BatchPause`, or more worker processes; cleanup reports when it is behind (§5.2, 18).
- **Health.** Cleanup never changes readiness or liveness (05 §17 "cleanup hung" refers to attempt-scope cleanup, not this loop). Failures surface through progress, logs and metrics (18).

## 5. Store additions (PostgreSQL, next schema revision)

Migration `0016_retention.sql` (revision 16 in this file; renumbered in merge order if another migration lands first):

- Tables `retention_policies` (§2.1) and `cleanup_progress` (§4.2), owned by the runtime role with `SELECT, INSERT, UPDATE` (no `DELETE`).
- Indexes:
  - `jobs_detail_retention ON jobs (application, state, terminal_at, job_id) WHERE detail_expired_at IS NULL AND state IN ('succeeded','failed','cancelled')`;
  - `dispatch_keys_bound_job ON dispatch_keys (application, bound_job_id) WHERE bound_job_id IS NOT NULL` (adds one index write when Dispatch binds a key);
  - `dispatch_keys_window_expiry ON dispatch_keys (application, expires_at) WHERE key_kind = 'window' AND bound_job_id IS NOT NULL`;
  - `dispatch_keys_unbound ON dispatch_keys (application, updated_at) WHERE bound_job_id IS NULL`;
  - `scaling_events_retention ON scaling_events (application, created_at, event_id)`;
  - `schedule_occurrences_retention ON schedule_occurrences (application, materialized_at, schedule_id, occurrence_kind, occurrence_key)`.
- `management_audit` checks: operation adds `EditRetentionPolicy`, `RetentionSummary`; actor scheme `system` already matches the pattern.
- The runtime role gains `DELETE` on `job_payloads`, `jobs` (cascade targets already have it), `queue_events`, `management_audit`, `schedule_audit_ranges`, `scaling_events`, `schedule_occurrences`, `worker_instances`, `rate_grants`, `outbox_deliveries`, `dispatch_keys` where it does not have it. Audit is append-only through Runwick APIs (management-and-dashboard §6), not against a database administrator.
- The manifest adds `CleanupBatch/1` and `EditRetentionPolicy/1` at the new revision. An older binary never runs cleanup, so no capability activation is needed: the new tables are unused until a new worker starts.
- Dispatch's key-row repeat (§3.3) ships in the same release.

## 6. Source outbox (relay side)

`Runwick.Outbox.Postgres` and `Runwick.Outbox.Sqlite` relays delete delivered source rows in their own loop (same budgets as §4.4, one batch at a time, SKIP LOCKED):

`state = 'delivered' AND delivered_at < now − 7 d AND accept_until + 24 h < now`.

The intent and the identity row are deleted together, once both bounds have passed [choice: the intent is kept as long as the identity; 14 §3.1 allows dropping it earlier]. `pending` and `blocked` rows are never deleted automatically. Source schema version 2 adds the index `outbox_messages_delivered (delivered_at) WHERE state = 'delivered'` (additive; `InstallAsync` stays idempotent).

## 7. Management (additions to 16)

| Session call | Route | Permission | Result |
|---|---|---|---|
| `GetRetentionAsync` | `GET {prefix}/v1/retention` | `Settings.Read` | policy (values in hours, `null` = forever) and revision; per class: kind, `lastBatchAt`, `caughtUpAt`, `totalRows`, today's rows/batches/failures, `lastErrorCode`/`lastErrorAt`, and `behind` = true when `caughtUpAt` is null or older than 24 h [choice]. No counts of eligible rows |
| `EditRetentionPolicyAsync` | `POST {prefix}/v1/retention/policy` | `Retention.Manage` | body `commandId`, `expectedRevision`, the seven values, `reason?`; results as 16 §4 (409 `revision_conflict` with `actualRevision`, 400 `invalid_retention_policy`) |

Projection changes:

- `JobSummary` gains `DetailExpiredAt` (`DateTimeOffset?`), in `ListJobs` and `GetJob`. A job with detail expired still shows identity, state, generations, attempts and error **codes**.
- Reveal of an expired detail stays 410 `detail_expired` with the job ID (16 §3). A purged job is 404.
- `GetSettings` adds `retentionPolicyRevision`.

## 8. Cases

Test project `tests/Runwick.Postgres.Tests` (store-level cases, real PostgreSQL) and `tests/Runwick.AspNetCore.Tests` (MGMT-11); trait `Item=SP-021`. Clock-dependent cases move store time with the harness `StoreClock` offset rather than waiting. "Seed" means rows written directly by the harness with old timestamps.

| Case | What it runs |
|---|---|
| RET-01 | Retention racing live work, under barriers at each cleanup statement: (a) a job renewing and finishing while its class is cleaned: never selected, lease and Finish unaffected; (b) ManualRetry before and after detail expiry of the same failed job: one wins, the other gets the ordered answer (§3.1); (c) Materialize and Trigger while occurrences are deleted: no redispatch below the high-water mark, replays within 24 h return their receipt; (d) receipts at `retain_until ± 1 ms`: the one not yet expired survives and replays; (e) a window key at `expires_at ± 1 ms` and a Dispatch concurrently re-binding the same key: no duplicate job, Dispatch's repeat path exercised; (f) a terminal job with a live window binding or a permit is never purged; (g) fences and scope rows unchanged after a full pass of every class |
| MGMT-11 | Over HTTP: after detail expiry `GetJob` shows `detailExpiredAt`, state, generations and error codes; reveal is 410 `detail_expired`; after purge `GetJob` is 404; `GET retention` shows the policy and progress; `POST retention/policy` with a stale revision is 409, an invalid order is 400, an applied edit writes one `EditRetentionPolicy` audit row; without `Retention.Manage` 403; caller without `Settings.Read` 403 on `GET retention` |
| OPS-02 | 1,000 failed jobs at the 30 d edge; 8 tasks issue ManualRetry while cleanup runs. Every job ends either retried with its payload intact or detail-expired with `payload_unavailable` replies — never retried without payload. Active uniqueness: a terminal job whose key is re-claimed by a new dispatch during purge: the new job keeps its binding, the old job is purged only after its window binding expires |
| OPS-04 | Policy set to the minimum (1 h detail, 1 h history, 7 d audit) with store time advanced 2 h: receipts inside `retain_until`, window keys inside `expires_at`, rate grants inside the window, outbox mappings inside `retain_until`, all fences, scopes and schedules survive; a pending/blocked source outbox row survives; a policy with history < detail is refused |
| OPS-05 | 1,000,000 seeded expired jobs (detail and history), `BatchPause = 0`, while a 50-job/s dispatch-and-finish load runs. Every batch ≤ 100 rows and ≤ 1 MiB detail; batch database time recorded (p50/p95/max against the 500 ms target); the process is killed three times and restarted, and cleanup resumes from its cursor with no row left eligible at the end; Finish and Renew p99 during cleanup within 2 × the no-cleanup baseline [choice]; no batch waits on a job row lock (lock-wait events = 0) |
| OBS-10 (telemetry plan) | Same fixture as OPS-04 driven to many passes: no live receipt, recovery intent, uniqueness identity, rate grant inside its window or fence is deleted. Run here; 18 cites it |
| CLN-01 | Two processes run cleanup at once: each class has at most one batch at a time (`skipped_locked` observed), totals add up, no row deleted twice or missed |
| CLN-02 | Summary audit: across a UTC day boundary each class writes one `RetentionSummary` row with the closed day's counters; a repeated batch does not write a second one |
| CLN-03 | Batch failure: a `statement_timeout` fault rolls back the batch, records `last_error_code`, backs off, and the next batch succeeds from the same cursor |
| CLN-04 | Pressure and drain: with the pressure stop active or the engine draining, no batch starts; the in-progress batch finishes |

CLN-01 … 04 are new IDs for behaviour the source cases do not name.

## 9. Must not claim

- that deleting a row erases backups, replicas or exported copies;
- that cleanup keeps up with any load at defaults (§4.4);
- that `RetentionSummary` rows record which rows were deleted;
- that a purged job ID can be told apart from one that never existed.
