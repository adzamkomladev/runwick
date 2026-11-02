# 04 — PostgreSQL driver: schema, locking, SQL and migrations

**Sources:** docs/architecture/postgres-schema-and-operations.md (primary), docs/architecture/postgres-storage-design.md, docs/architecture/postgres-upgrades-and-compatibility.md, docs/storage-design-inputs.md, docs/architecture/storage-command-protocol.md, docs/architecture/capacity-control.md and docs/architecture/capacity-defaults-and-budgets.md (scope/permit tables, connection budgets), docs/architecture/provider-dependency-plan.md, docs/architecture/driver-contract-suite.md, docs/review-2026-10-30.md, docs/architecture/management-retention-defaults.md (receipt floors), docs/architecture/identity-and-serialization.md (bounds), docs/architecture/worker-engine-internals.md §5 (readiness shape).

This file is the PostgreSQL implementation of [03-storage-protocol.md](03-storage-protocol.md). The implementer writes `0001_initial.sql` verbatim from §4 and the operation statements from §7. Canonical encodings and digests are in [02-serialization-and-wire.md](02-serialization-and-wire.md); numeric defaults in 06; G-1 test definitions in [07-test-plan-g1.md](07-test-plan-g1.md).

Scope is M1 + M2. Records for rates, schedules, outbox, management audit, retention progress, worker subscriptions/pool snapshots and pool controls are **not in M2** and are not created by `0001_initial` (§3).

## Changes (2026-11-02, PF-06)

`RenewMany/1` (03 Changes 2026-11-02, PF-06) for PostgreSQL.

- **Migration `0008_renew_many` (schema revision 8).** No catalog change; the revision adds `"RenewMany": [1]` to the compatibility manifest (§8.5, cumulative). As with every revision, a binary that writes revision 7 meets a revision-8 store as `schema_incompatible`, and the reverse.
- **§5.2 row** `RenewMany/1`: gates SHARE / SHARE; receipt **none**; jobs FOR NO KEY UPDATE, all requested job rows in one statement, UUID order (rank 5); concurrency scopes FOR UPDATE, every required scope of those jobs in one statement, UUID order (rank 7); dependent writes job leases, then permit leases. Locks are taken across members in rank order, never member by member, so two batches, or a batch and a Renew/Finish, cannot cycle.
- **Statements (three round trips + COMMIT for any number of leases, as Renew/1):** the §6 gate batch; then one batch of J (`job_id = ANY(@job_ids) ORDER BY job_id FOR NO KEY UPDATE`), S (scopes of `job_concurrency_requirements` for those jobs, `ORDER BY scope_id FOR UPDATE`), the jobs' requirement rows and permit rows (dependent rows reached through the locked jobs, read by separate statements after the locks), and the decision clock (§5.1 rule 1). The driver then decides each lease against its own locked row with Renew/1's ownership rules, and checks its permit set (one permit per requirement, same attempt and token, expiry equal to the job's current lease expiry) **before** any write. Only passing leases go into the effects batch: one `UPDATE runwick.jobs … FROM unnest(job_ids, attempt_ids, tokens, workers, generations)` with every Renew/1 predicate per row (attempt, token, worker, generation, state, `lease_expires_at > now`) and `RETURNING job_id`, and one `UPDATE runwick.concurrency_permits … FROM unnest(job_ids, attempt_ids, tokens, previous_expiries) RETURNING job_id, scope_id`. Before COMMIT every passing lease must have exactly one job row and exactly its permit set returned; since each was checked under its locks, any difference is a broken invariant and rolls the whole batch back with `InvariantFault` `permit_set_mismatch`. When no lease passes, the transaction rolls back with an `Applied` reply carrying the refusals.
- **Fault points** are those of Renew/1 (07 table): `AfterLocksBeforeFreshTime` once after the lock batch, `AfterMutation` once after the effects batch.
- PERF-01: `RenewMany` = 3 commands (4 round trips) for 2 leases and for 64.

## Changes (2026-11-02, PF-04 leftover: rated Begin)

A rated Begin's window counts (10 §4) move into the rate-lock batch. The batch is K, R, then one statement that reads the decision clock and, per locked rate scope, the grants inside (t − W, t] with t = max(floor_ms(clock), the scopes' `last_decision_at`), computed in SQL from the same clock value. It replaces the bare clock read that ended the batch. The statement runs after R's `FOR UPDATE` in the same batch, so the scopes are locked by this transaction and the clock is still read after the last blocking lock (03 §1.3, rule 1 of "Changes (2026-11-01, PF-04)"); it is a separate statement, not R's target list. The driver recomputes t from R's rows and the clock it received and rolls back with `invalid_transition` on any mismatch. Lock order, the charge, the deferral (T20 and its hint reads) and `synchronous_commit` are unchanged. PERF-01: Begin rated 6 → 5 commands (7 → 6 round trips), for 1 and 8 scopes.

## Changes (2026-11-01, PF-03)

Performance plan PF-03: automatic statement preparation and a warm control pool, in §9.2. Round trips are unchanged (PERF-01).

1. **Preparation and plan stability.** The control and work data sources prepare up to 64 statements per physical connection, each after its second use, which saves parse and analysis on every later call. Every driver-owned connection starts with `plan_cache_mode = force_custom_plan` (a startup option, so it covers the transaction frame and the bare-connection readiness probe alike): a prepared statement is planned for each bind's values and never runs a cached generic plan. The Reserve candidate query and the readiness probe are therefore planned exactly as without preparation, whatever their arrays and the data distribution; no generic-plan guarantee is needed. PGX-11 checks the setting and that the control connection holds prepared statements.
2. **`0A000` `cached plan must not change result type`.** A schema change under a prepared statement makes PostgreSQL refuse the statement, before anything is applied. The runner rolls back, runs `UnprepareAll` on that connection, clears the data source's pool (every pooled connection prepared the same statements, and the retry budget is not spent one connection at a time) and retries the same command as a §10 transient conflict (same command ID and `NotAfter`; exhausted → `transient_conflict_exhausted`). A read-only call (probe, schedule and readiness reads) is retried once on the cleared pool. `RunwickPostgresMigrations.MigrateAsync` clears the pools of every store in its process on the migrated database (host, port, database) when it ends. DEP-08 changes a column type the gate statements return while three loops run the job path, and asserts no failed call, at least one `0A000` retry and no history violation.
3. **Staged paths unchanged.** `StageAsync` and `StageFinishAsync` run on the caller's connection with the caller's preparation settings; Runwick sets neither `Max Auto Prepare` nor `plan_cache_mode` there, and a `0A000` there propagates to the caller like any other PostgreSQL error (§6.1).
4. **Rolling upgrade.** An old binary with warm prepared statements meeting a newer schema gets `0A000` on its next call and retries it on fresh connections; the retry then gives either a correct result or, when the gate row's revision is no longer one it supports, `schema_incompatible`. Never a wrong answer: a prepared statement either runs against the schema it was prepared for, or is refused. The UPG-02 variant with an old binary is owed with UPG-02 itself.
5. **Warm control pool.** The control pool keeps 2 idle connections (the settlement lane width) past `Connection Idle Lifetime`, so the first Renew or Finish after a long idle does not pay a physical open (0.1–1 s, lease-critical). The work pool stays at 0. PGX-12 renews after the idle lifetime (shortened in the test) and asserts the same backends serve it.

## Changes (2026-11-01, PF-04)

§6.1's batching rule applied to the operation bodies of Dispatch/1 and DispatchMany/1 (driver-owned and staged), Reserve/1, Begin/1, Renew/1, Finish/1 (and `StageFinishAsync`) and the §7.10 close statements. The SQL text of every statement, the lock order (§5), fresh time after the last blocking lock (03 §1.3), the receipt replay branch and `synchronous_commit` are unchanged. Rules:

1. **Locks first, in order, then the clock.** Statements that need only the request or the already-locked row are batched behind the locking statement in lock order (Reserve: Q then the claim; then G, the payload read, S and the clock. Begin: J, G and the clock; K, S, R and a fresh clock when the plan locks more. Renew: J, S, clock. Finish: J, G, K, S, clock when the outcome is terminal regardless of the job; otherwise J and G first). The decision clock read is a separate statement after the last blocking lock in that batch, never in a locking statement's target list, where it could be evaluated before the lock wait. A clock read in a batch that is followed by further locks is discarded and read again after them.
2. **Reads behind a lock are separate statements.** G is not joined into J's `FOR NO KEY UPDATE`: under READ COMMITTED a joined, unlocked generation row would be re-checked from the pre-wait snapshot. G is a statement after J in the same batch and reads the generation the locked row names. Begin's `job_rate_requirements` check is an `EXISTS` in G's target list (requirements are immutable), which removes the separate requirements read for unrated jobs. K for Finish and Begin's closed plans is found through the locked job row.
3. **Effects and R4 in one batch, checked before COMMIT.** After the decision every effect's inputs are known. Values a `RETURNING` clause used to supply are predicted from locked rows (the job's new revision, execution number and total; each fence = scope counter + 1) and compared with the returned values; every row count is checked. Any mismatch rolls back with the reply the sequential code gave. Dispatch's step-10 deadline read is the last statement before R4 in the batch. Dispatch items that provision concurrency or rate scopes, the rate deferral's hint reads, and Reserve's live-permit count (scoped jobs) keep their own round trips.
4. **Never a decision ahead of its lock.** No batch contains a statement whose inputs depend on an earlier statement's result in the same batch. A claim batched behind Q (Reserve) is rolled back when Q says paused or unregistered.

PERF-01 after PF-04 (commands; round trips = + COMMIT): Dispatch 4, Reserve 5, Begin 4, Begin rated 6, Renew 3, Finish 4, `StageFinishAsync` 5 (+ `RELEASE`). Per job 17 commands, 21 round trips (64 before PF-02). Fault points: 07 "Changes (2026-11-01, PF-04)".

## Changes (2026-11-01, PF-02)

The §6.1 batching rule ("statements whose inputs do not depend on prior results may be sent together") is now applied to the protocol frame. Statements, their order on the server and every decision are unchanged:

- **Gate batch:** the §6.1 `set_config`, G1 and G2 go in one round trip. The server runs them in order, so G2's `FOR SHARE` follows G1's, and every gate check runs on the results before anything else is sent. RegisterApplication keeps a separate G2 after its seed insert, because the insert follows G1's checks. Staging sends G1 and G2 together (it has no `set_config`).
- **Receipt batch:** R1, R3 and the §6.3 clock read go in one round trip. R3 and the clock run after R1's insert, or after R1's wait on an in-flight holder. Their results are used only when R1 inserted; when it did not, R2 follows as before and both results are discarded. The only difference from the written sequence is that R3, a plain read with no lock, also runs when its result is not needed.
- **Staging:** the caller transaction's read-only check and `SAVEPOINT runwick_stage` go in one round trip. A read-only transaction has the savepoint released before the `transaction_invalid` rejection, so the caller's transaction is left as it was. `RELEASE` stays separate, after the checks.
- Saving: 4 round trips per receipted driver-owned call, 2 per Renew and PublishPresence, 4 per staged call. Per job (Dispatch + Reserve + Begin + Finish): 64 → 48. Fault-point effects are in 07 "Changes (2026-11-01, PF-02)".

## Changes (2026-10-31, SP-016)

- `0006_scheduler` (revision 6) adds `schedules`, `schedule_definitions`, `schedule_occurrences` and `schedule_audit_ranges` (13 §3). It widens `concurrency_scopes_kind_v2_ck` (`schedule`), `queue_events_kind_v4_ck` (nine schedule kinds), `management_audit_operation_v3_ck` (six schedule operations) and `management_audit_target_kind_v3_ck` (`schedule`). Jobs it creates carry `created_reason = 'scheduled'`.
- The manifest for revision 6 adds the eight schedule operations. Lock order gains "schedule row (one per command)" after the receipt (and outbox delivery identities), before queue controls (13 §8).

## Changes (2026-10-31, SP-014)

- `0007_scaling` (revision 7) adds `pool_controls` (per incarnation and pool, revisioned, no foreign key) and `scaling_events` (11 §5–§6). It widens the event kinds (`queue_events_kind_v5_ck`: `pool_override_set`) and the audit checks (`management_audit_operation_v4_ck`: `SetPoolOverride`; `management_audit_target_kind_v4_ck`: `pool`), on top of 0006's lists.
- The manifest for revision 7 adds `SetPoolOverride`, `EstimateDemand` and `RecordScalingEvent`. `EstimateDemand` is one read-only statement behind the gate, at most 2,000 due rows per lane (11 §4.1).

## Changes (2026-10-31, SP-012)

- `0005_readiness` (revision 5) adds `worker_subscriptions` and index `worker_subscriptions_queue` (09 §3.1). The manifest for revision 5 adds `ReadinessProbe` version 2 and `ListUnsupportedBacklog`; manifest additions may now name a version (`Name@v`).
- §7.4 is superseded by 09 §2 (probe/2: lane, unsupported and queue rows in one statement). The §5.2 lock matrix gains a no-lock read of `concurrency_scopes` and `concurrency_permits` for the saturation count.
- §7.12: PublishPresence inserts subscriptions in queue order with `ON CONFLICT DO NOTHING`.

## Changes (2026-10-31, SP-017)

- `0003_outbox` (revision 3) adds `outbox_deliveries` and `outbox_deliveries_cleanup` (14 §5.2). The manifest for revision 3 adds `ReceiveOutbox`.
- §5.1 rank 3a: the outbox delivery identity, a transaction advisory lock `pg_advisory_xact_lock(hashtextextended('runwick.outbox/' || application || '/' || source_store_id || '/' || message_id, 0))`, then the mapping row `FOR UPDATE`. Cleanup takes the same lock.

## Changes (2026-10-31, SP-013)

- `0004_rates` (revision 4) adds `rate_scopes`, `job_rate_requirements` and `rate_grants` (10 §3). It widens the event kinds (`queue_events_kind_v3_ck`: `attempt_admission_deferred`, `rate_policy_edited`, `capabilities_activated`) and the audit checks (`management_audit_operation_v2_ck`, `management_audit_target_kind_v2_ck`).
- Revision 3 is `0003_outbox` (SP-017). The manifest for revision 4 adds `ActivateCapabilities` and `EditRatePolicy`.
- Lock order gains "rate scopes (UUID order)" after concurrency scopes in Begin. Dispatch inserts rate seeds in (kind, key) order after the concurrency requirements.

## Changes (2026-10-31, SP-011)

- Migrations are an ordered list (`0001_initial`, `0002_controls`), each with its own checksum, ledger rows and target revision. Validate installs the target revision and a cumulative manifest (§8.4–8.5). The validation catalog subtracts `DROP CONSTRAINT`s of later migrations.
- `0002_controls` adds `management_audit` (08 §5), `queue_events.schema_version` and `tx_id` with index `events_cursor`, and widens the event kinds (`queue_events_kind_v2_ck`).
- §7.14: SetQueueState is public (08 §3.4); the harness path remains and writes no audit.
- Lock order is unchanged. Controls take queue control → jobs → dispatch keys → scopes as needed.

## Changes (2026-10-30)

### Contradictions resolved

| # | Conflict | Resolution |
|---|---|---|
| C-1 | postgres-upgrades-and-compatibility.md §7: "does not select a minimum supported server version or certify an Npgsql version". | Superseded by provider-dependency-plan: **Npgsql 10.0.3**; PostgreSQL **16–18**, **18 reference**. SQL stays PostgreSQL-16 compatible. |
| C-2 | storage-design-inputs.md proposes `rate_limit` columns on `concurrency_scopes`. | Superseded: separate `rate_scopes` / `job_rate_requirements` / `rate_grants` (capacity-control §4); not in M2 and not created here. |
| C-3 | postgres-storage-design / schema-and-operations: `storage_commands` PK `(application, command_id)`. | Review partition recommendation wins: `PARTITION BY LIST (retention_class)` with partitions `ownership` and `durable`; PK `(application, retention_class, command_id)`; class derived from operation name; a cross-partition lookup enforces `CommandConflict` for an ID reused under the other class (§6.3). |
| C-4 | postgres-storage-design §2 `storage_commands.protocol_version`; schema-and-operations "operation/version". | Column `operation_version`; `protocol_epoch` separate. |
| C-5 | postgres-storage-design §4/§6: Renew locks "command → job → scopes". | R1: Renew has no receipt and takes no `storage_commands` lock. |
| C-6 | postgres-storage-design §2: `job_payloads` FK `(application, job_id) → jobs`. | schema-and-operations wins: `UNIQUE (application, job_id, serializer_profile_id, serializer_profile_version)` on `jobs` and a composite FK from `job_payloads` (cascade on whole-job purge). |
| C-7 | schema-and-operations lists `dispatch_keys` "request_fingerprint_version and digest". | R3: `business_fingerprint`, `policy_fingerprint`, `fingerprint_version`. |
| C-8 | execution-defaults and timing receipt-floor list omits ReleaseReservation. | ReleaseReservation is in the `ownership` class (NotAfter + 10 min). |
| C-9 | Scope-kind spellings `job_type` / `job-type`. | `job-type` (02 IC-06). |
| C-10 | postgres-storage-design §5 candidate SQL uses `statement_timestamp()`; §1 requires fresh time after locks. | Both hold: the candidate eligibility predicate uses `statement_timestamp()` (stable, index-usable, not authority-critical); every ownership, capacity and deadline decision uses `clock_timestamp()` read after all locks. |
| C-11 | Some sources say queue pause takes an "exclusive row lock", the 07 harness uses `FOR UPDATE`. | `SetQueueState/1` (harness-only in M2) locks the queue row `FOR UPDATE`. `jobs` has no FK to `queue_controls`, so the exclusive lock never blocks Dispatch (ADM-04). |

### Implementation choices

| ID | Choice |
|---|---|
| P-1 | Only schema `runwick` is supported in M2 (no configurable schema name); every object is schema-qualified. |
| P-2 | Text identities use domains with `COLLATE "C"`: `rw_identifier` (1–128, identifier grammar), `rw_job_name` (1–200), `rw_key` (1–512 UTF-8 bytes, no Cc control code points); `rw_digest` = 32-byte `bytea`. Ordering of text identities is therefore bytewise. Fixed vocabularies (`state`, `priority`, kinds, reasons) are `text COLLATE "C"` with CHECKs. |
| P-3 | Immutable helpers `runwick.ms_floor(timestamptz)` / `runwick.ms_ceil(timestamptz)` implement the 1 ms quantum; authority instants (`eligible_at`, `lease_expires_at`, permit expiry, `not_after`, window `expires_at`) carry ms-alignment CHECKs. |
| P-4 | `jobs` columns added beyond the sources: `updated_at`, `cancel_requested_by`, `payload_retained` (default true), `detail_expired_at`. `job_attempts` adds `disposition`, `applied_reason`, `retry_delay_ms`, `next_eligible_at`, `closed_by_command_id`; outcome vocabulary adds `cancelled_before_start`. `job_generations` adds `policy_snapshot_version`. Policy JSON uses the 02 §7.2 retry-object member names. |
| P-5 | Queue registration is required (`queue_controls` row) before Dispatch can route to a queue; checked by a plain read, no FK from `jobs` (so pause never blocks dispatch). |
| P-6 | `concurrency_permits` has an additional FK to `job_concurrency_requirements (application, job_id, scope_id)`, so a permit can exist only for a declared requirement. `fencing_enabled` is set at scope creation to `kind = 'resource' AND capacity = 1` and is immutable in M2. |
| P-7 | Partial unique index `job_attempts_one_open` enforces at most one open attempt per job. |
| P-8 | Receipt-placeholder guard: deferred constraint trigger `AFTER INSERT … DEFERRABLE INITIALLY DEFERRED` created **on each partition** (portable across 16–18), raising SQLSTATE `RWR01`. |
| P-9 | `queue_events` is not partitioned in `0001`; event kinds are fixed by CHECK (§4.8). Worker queue subscriptions and pool targets are `jsonb` columns on `worker_instances` (no `worker_subscriptions` table in M2). Presence status vocabulary is 05's (`starting`, `ready`, `draining`, `degraded`, `stopping`). |
| P-10 | Installation publishes `store_metadata` with `schema_revision = 0`, `migration_mode = 'maintenance'`; the validate phase sets revision 1 / `normal` with the manifest. `schema_migrations` PK is `(migration_id, phase)`: one ledger row per phase. |
| P-11 | Migration lock: session advisory lock key `5941776742533524273` (`0x52756E7769636B31`, ASCII "Runwick1"), acquired with `pg_try_advisory_lock` polling (250 ms doubling to 2 s) for at most 60 s. |
| P-12 | Transaction timeouts: `lock_timeout = min(2 s, remaining operation budget)`, `statement_timeout = remaining budget`, `idle_in_transaction_session_timeout = 10 s`, set with `set_config(…, true)` per transaction. |
| P-13 | Deadlock (`40P01`), serialization (`40001`) and lock-timeout (`55P03`) rollbacks of driver-owned transactions are retried with the same command identity, at most 3 retries, control backoff (100 ms nominal, ×2, cap 1 s, equal jitter), within NotAfter and the caller's budget. |
| P-14 | Database roles: a migration role owns the schema; the runtime role gets DML only, with `store_metadata`/`schema_migrations` read-only and `application_controls` SELECT + INSERT (seed) only (§9.3). |
| P-15 | Indexes added beyond the sources: `jobs_queue_list`, `job_concurrency_requirements_scope`, `permits_job_attempt`, `permits_job_scope`, `command_by_id`, `events_retention`, `worker_capabilities_tuple`; `events_job_history` is partial (`job_id IS NOT NULL`). |
| P-16 | Readiness probe and expired-lease listing are single statements that take the store gate `FOR SHARE` in a CTE (one round trip each). |
| P-17 | Receipt cleanup (SP-021, not M2) shape is fixed here so the G-1 harness `PruneReceipts` uses the production predicate (§7.16). |

### Implementation choices (2026-10-30, SP-005 implementation)

| ID | Choice |
|---|---|
| P-18 | Public deployment API: `RunwickPostgresMigrations.MigrateAsync(connectionString, ct)` returns `PostgresMigrationResult(StoreId, SchemaRevision, AppliedPhases)`; failures throw `PostgresMigrationException` with `Code` ∈ `store_unavailable`, `server_version_unsupported`, `encoding_unsupported`, `standard_conforming_strings_off`, `migration_lock_unavailable`, `schema_not_empty`, `migration_checksum_mismatch`, `schema_validation_failed`, `migration_lock_timeout`, `migration_phase_failed`. The `0001_grants` step (§9.3) is not shipped in M2: the migration and runtime roles may be the same role; grants arrive with SP-021/SP-033. |
| P-19 | The §8.3 expected-object list is derived at build time from the embedded `0001_initial.sql` (table, index and named-constraint declarations), so the binary and its DDL cannot drift. |
| P-20 | `ReadinessProbe/1` and `ListExpiredLeases/1` validate the gate columns read-only: a store-identity or schema-revision/epoch mismatch throws `StoreIncompatibleException`; `maintenance` mode returns an empty result (no work, no recovery). |
| P-21 | `PublishPresence/1` with a non-increasing `presence_sequence` returns `PresenceAck(Accepted = false)` and commits nothing. |
| P-22 | Store-side bound checks (03 I-6) run before any SQL: LeaseDuration 1 s–1 h and 1–512 contracts for Reserve/Renew/Probe; Dispatch item policy bounds and ≤ 8 scopes return `InvalidRequest` `invalid_request`; a negative `RequestedRetryDelay` returns `InvalidRequest` `requested_delay_invalid`. |
| P-23 | Statement row-count violations inside a transaction (e.g. §7.10 "exactly one row") raise an internal invariant exception that the runner maps to `InvariantFault` `invalid_transition` with rollback and no receipt. |
| P-24 | When the configured connection string sets `Application Name`, that value replaces the `runwick:<application>` prefix and the driver appends `:control` / `:work` (the harness uses `rw:<processIdentity>` so the wire proxy can filter by identity and role, 07 §2.5). |
| P-25 | (2026-10-30, SP-006, CMD-04) A `lock_timeout`/`statement_timeout`/40P01/40001 rollback observed when local time is already at or past the command's `NotAfter` is reported as `CommandExpired` (`ObservedNoEffect`), not as a retryable not-applied error: the transaction rolled back and R1 rejects any later application of the same command, so the outcome is known. |
| P-26 | (2026-10-30, SP-006) Caller-owned staging (§6.1) runs inside a savepoint `runwick_stage` on the caller's transaction: a rejection (gate, receipt conflict/replay, uniqueness conflict, invariant) rolls back to the savepoint and leaves the caller's transaction usable; success releases the savepoint. Savepoints are not commits/rollbacks of the caller's transaction. A staged PostgreSQL error (40001/40P01/...) propagates to the caller unchanged as `PostgresException` (TX-03) and the caller's transaction is left aborted for the caller to roll back; read-only or completed transactions are rejected with `transaction_invalid` and a different store identity with `transaction_store_mismatch`. |
| P-27 | (2026-10-30, SP-008, OWN-05) Renew's permit update also requires each permit's `lease_expires_at` to equal the job's current `lease_expires_at` (read under the job lock); a permit whose expiry differs is "mismatched permit data" and yields the same rollback and `permit_set_mismatch` invariant fault as a missing or extra row (I-6). |

---

## 1. Targets and boundaries

- Server: PostgreSQL 16, 17, 18 (reference 18; certification images 16.15, 17.11, 18.6 pinned by digest in the harness). Required: `server_version_num ≥ 160000`, database encoding `UTF8`, `standard_conforming_strings = on`. No extension is required (`gen_random_uuid()` and `sha256()` are built in).
- Client: Npgsql **10.0.3**, explicit parameterized SQL, no EF/Dapper. Identifier interpolation is never used (P-1); job data, names, keys and actors are parameters with explicit `NpgsqlDbType`.
- Isolation: driver-owned transactions are `READ COMMITTED` with explicit row locks and short timeouts, so a statement issued after waiting for a lock sees rows committed by the previous holder. A permit count read before the scope lock is never reused.
- Topology: direct primary only. Ownership, admission and receipt resolution never read an asynchronous replica. HA/proxy profiles are not certified in M2.
- Notification: polling is sufficient for correctness. LISTEN/NOTIFY is not in M2; a process may signal its own local worker after its own commit.

## 2. Conventions

| Concern | Rule |
|---|---|
| Schema | `runwick` (P-1). All statements schema-qualify every object. |
| Application scope | Every table except `store_metadata` and `schema_migrations` has `application` as the leading key column; every query binds `@application`. |
| UUIDs | `uuid`. Job, attempt, command and scope IDs are `Guid.CreateVersion7()` generated by the client; ownership tokens are random. The server never generates them except `store_id`. Lock and cursor order is PostgreSQL `uuid` order = 03 §2.4 `UuidOrder`. |
| Text order | `COLLATE "C"` on every identity column; `ORDER BY` over unnested parameter arrays writes `COLLATE "C"` explicitly. |
| Instants | `timestamptz`. Decision time `@now` is `clock_timestamp()` read after all blocking locks. Stored instants: decision/created/updated/decided/finished `ms_floor(@now)`; lease and window expiry `ms_floor(@now + duration)`; eligibility `ms_ceil(…)`. |
| Durations | `bigint` milliseconds (`*_ms`) or integer ms inside policy JSON; never `interval` columns. |
| Counters | `integer`/`bigint` with non-negative CHECKs; increments are SQL `+ 1` on the locked row (PostgreSQL raises on overflow). |
| JSON | `jsonb` only for bounded control/metadata/receipt/policy values. Payload bytes are `bytea` and never pass through `jsonb`. |
| Parameters | `@name` placeholders; arrays as typed `text[]`, `integer[]`, `uuid[]`, `bigint[]`. |

## 3. Record inventory

| Record | M2 | Purpose |
|---|---|---|
| `store_metadata` | yes | Singleton store gate: identity, schema revision, epoch, migration mode, compatibility manifest |
| `schema_migrations` | yes | Migration ledger, one row per migration phase |
| `application_controls` | yes | Per-application gate: admission mode, required epoch/capabilities |
| `queue_controls` | yes | Registered queues; pause flag and revision |
| `jobs` | yes | Hot job row: identity tuple, seven states, generation/revision, counters, ownership, eligibility |
| `job_payloads` | yes | Original UTF-8 payload bytes, profile, headers; immutable |
| `job_generations` | yes | Per-generation policy snapshot and counters |
| `job_attempts` | yes | One row per reservation; outcome and disposition |
| `concurrency_scopes` | yes | Distributed concurrency limits (queue / job-type / resource) and fencing counters |
| `job_concurrency_requirements` | yes | Immutable job → scope membership |
| `concurrency_permits` | yes | Live lease-backed permits |
| `dispatch_keys` | yes | Active and fixed-window uniqueness bindings |
| `storage_commands` (+ `storage_commands_ownership`, `storage_commands_durable`) | yes | Command receipts, partitioned by retention class |
| `queue_events` | yes | Bounded execution history (`dispatched`, `policy_drift`, attempt lifecycle) |
| `worker_instances` | yes | Worker presence (never authority) |
| `worker_capabilities` | yes | Active capability revision tuples per worker instance |
| `rate_scopes`, `job_rate_requirements`, `rate_grants` | **not in M2** | Rate admission (capacity-control §4) |
| `management_audit` | **not in M2** | Administrative audit |
| `outbox_deliveries` | `0003_outbox` (M3, 14 §5.2) | Outbox destination identities |
| `schedules`, `schedule_definitions`, `schedule_occurrences`, `schedule_audit_ranges` | **not in M2** | Scheduler |
| `worker_subscriptions`, `worker_pool_snapshots`, `pool_controls`, `scaling_events` | **not in M2** | Pool/presence detail beyond the M2 columns |
| `retention_policies`, `cleanup_progress` | **not in M2** | Retention cleanup |

Later migrations add the not-in-M2 records with their own IDs, manifests and compatibility rules (§8.6).

## 4. DDL — `0001_initial.sql`

The block below is the complete expand-phase script. The migration runner executes it in **one transaction** on the migration session (§8). It is valid on PostgreSQL 16–18.

```sql
-- 0001_initial — expand phase. One transaction. Runwick schema revision 1 (installed unvalidated: schema_revision 0).
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE SCHEMA runwick;

-- §4.1 Domains and helper functions ------------------------------------------------------

CREATE DOMAIN runwick.rw_identifier AS text COLLATE "C"
    CONSTRAINT rw_identifier_grammar CHECK (VALUE ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$');

CREATE DOMAIN runwick.rw_job_name AS text COLLATE "C"
    CONSTRAINT rw_job_name_grammar CHECK (VALUE ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$');

CREATE DOMAIN runwick.rw_key AS text COLLATE "C"
    CONSTRAINT rw_key_bytes CHECK (octet_length(VALUE) BETWEEN 1 AND 512)
    CONSTRAINT rw_key_no_control CHECK (VALUE !~ '[\u0001-\u001F\u007F-\u009F]');

CREATE DOMAIN runwick.rw_digest AS bytea
    CONSTRAINT rw_digest_length CHECK (octet_length(VALUE) = 32);

CREATE FUNCTION runwick.ms_floor(ts timestamptz) RETURNS timestamptz
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    RETURN date_trunc('milliseconds', ts AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';

CREATE FUNCTION runwick.ms_ceil(ts timestamptz) RETURNS timestamptz
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    RETURN CASE WHEN runwick.ms_floor(ts) = ts THEN ts
                ELSE runwick.ms_floor(ts) + interval '1 millisecond' END;

-- §4.2 Store and application gates --------------------------------------------------------

CREATE TABLE runwick.store_metadata (
    singleton_id                    smallint    NOT NULL,
    store_id                        uuid        NOT NULL,
    schema_revision                 bigint      NOT NULL,
    protocol_epoch                  integer     NOT NULL,
    migration_mode                  text COLLATE "C" NOT NULL,
    compatibility_manifest_version  integer     NOT NULL,
    compatibility_manifest          jsonb       NOT NULL,
    manifest_digest                 runwick.rw_digest NOT NULL,
    revision                        bigint      NOT NULL,
    updated_at                      timestamptz NOT NULL,
    CONSTRAINT store_metadata_pk PRIMARY KEY (singleton_id),
    CONSTRAINT store_metadata_store_id_uq UNIQUE (store_id),
    CONSTRAINT store_metadata_singleton_ck CHECK (singleton_id = 1),
    CONSTRAINT store_metadata_schema_revision_ck CHECK (schema_revision >= 0),
    CONSTRAINT store_metadata_protocol_epoch_ck CHECK (protocol_epoch >= 1),
    CONSTRAINT store_metadata_migration_mode_ck CHECK (migration_mode IN ('normal', 'expand', 'maintenance')),
    CONSTRAINT store_metadata_unvalidated_ck CHECK (schema_revision >= 1 OR migration_mode = 'maintenance'),
    CONSTRAINT store_metadata_manifest_version_ck CHECK (compatibility_manifest_version >= 1),
    CONSTRAINT store_metadata_manifest_ck CHECK (jsonb_typeof(compatibility_manifest) = 'object'),
    CONSTRAINT store_metadata_revision_ck CHECK (revision >= 1)
);

CREATE TABLE runwick.schema_migrations (
    migration_id            text COLLATE "C" NOT NULL,
    phase                   text COLLATE "C" NOT NULL,
    checksum                runwick.rw_digest NOT NULL,
    target_schema_revision  bigint      NOT NULL,
    state                   text COLLATE "C" NOT NULL,
    runner_build            text        NOT NULL,
    started_at              timestamptz NOT NULL,
    completed_at            timestamptz,
    checkpoint              jsonb,
    diagnostic_code         text COLLATE "C",
    CONSTRAINT schema_migrations_pk PRIMARY KEY (migration_id, phase),
    CONSTRAINT schema_migrations_id_ck CHECK (migration_id ~ '^[0-9]{4}_[a-z0-9_]{1,100}$'),
    CONSTRAINT schema_migrations_phase_ck CHECK (phase IN ('preflight', 'expand', 'backfill', 'validate', 'activate', 'contract')),
    CONSTRAINT schema_migrations_target_ck CHECK (target_schema_revision >= 1),
    CONSTRAINT schema_migrations_state_ck CHECK (state IN ('started', 'applied', 'failed')),
    CONSTRAINT schema_migrations_completed_ck CHECK ((state = 'started') = (completed_at IS NULL)),
    CONSTRAINT schema_migrations_diagnostic_ck CHECK (diagnostic_code IS NULL OR state = 'failed'),
    CONSTRAINT schema_migrations_runner_ck CHECK (octet_length(runner_build) BETWEEN 1 AND 256),
    CONSTRAINT schema_migrations_checkpoint_ck CHECK (checkpoint IS NULL OR jsonb_typeof(checkpoint) = 'object')
);

CREATE TABLE runwick.application_controls (
    application             runwick.rw_identifier NOT NULL,
    revision                bigint      NOT NULL DEFAULT 1,
    required_protocol_epoch integer     NOT NULL DEFAULT 1,
    required_capabilities   jsonb       NOT NULL DEFAULT '{}'::jsonb,
    admission_mode          text COLLATE "C" NOT NULL DEFAULT 'open',
    created_at              timestamptz NOT NULL,
    updated_at              timestamptz NOT NULL,
    actor_subject           text,
    CONSTRAINT application_controls_pk PRIMARY KEY (application),
    CONSTRAINT application_controls_revision_ck CHECK (revision >= 1),
    CONSTRAINT application_controls_epoch_ck CHECK (required_protocol_epoch >= 1),
    CONSTRAINT application_controls_capabilities_ck CHECK (jsonb_typeof(required_capabilities) = 'object'),
    CONSTRAINT application_controls_mode_ck CHECK (admission_mode IN ('open', 'drain', 'maintenance')),
    CONSTRAINT application_controls_actor_ck CHECK (actor_subject IS NULL OR octet_length(actor_subject) BETWEEN 1 AND 256)
);

-- §4.3 Queue controls ------------------------------------------------------------------------

CREATE TABLE runwick.queue_controls (
    application runwick.rw_identifier NOT NULL,
    queue       runwick.rw_identifier NOT NULL,
    paused      boolean     NOT NULL DEFAULT false,
    revision    bigint      NOT NULL DEFAULT 1,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL,
    updated_by  text,
    CONSTRAINT queue_controls_pk PRIMARY KEY (application, queue),
    CONSTRAINT queue_controls_revision_ck CHECK (revision >= 1),
    CONSTRAINT queue_controls_updated_by_ck CHECK (updated_by IS NULL OR octet_length(updated_by) BETWEEN 1 AND 256)
);

-- §4.4 Jobs, payloads, generations, attempts ----------------------------------------------

CREATE TABLE runwick.jobs (
    application                 runwick.rw_identifier NOT NULL,
    job_id                      uuid        NOT NULL,
    job_name                    runwick.rw_job_name NOT NULL,
    payload_version             integer     NOT NULL,
    serializer_profile_id       runwick.rw_identifier NOT NULL,
    serializer_profile_version  integer     NOT NULL,
    queue                       runwick.rw_identifier NOT NULL,
    priority                    text COLLATE "C" NOT NULL,
    state                       text COLLATE "C" NOT NULL,
    eligible_at                 timestamptz NOT NULL,
    created_at                  timestamptz NOT NULL,
    updated_at                  timestamptz NOT NULL,
    generation                  integer     NOT NULL DEFAULT 1,
    revision                    bigint      NOT NULL DEFAULT 1,
    reservation_count           bigint      NOT NULL DEFAULT 0,
    execution_count             bigint      NOT NULL DEFAULT 0,
    current_attempt_id          uuid,
    worker_instance_id          uuid,
    ownership_token             uuid,
    lease_expires_at            timestamptz,
    cancel_requested_at         timestamptz,
    cancel_reason               text,
    cancel_requested_by         text,
    terminal_at                 timestamptz,
    terminal_reason             text COLLATE "C",
    active_unique_key           runwick.rw_key,
    payload_retained            boolean     NOT NULL DEFAULT true,
    detail_expired_at           timestamptz,
    CONSTRAINT jobs_pk PRIMARY KEY (application, job_id),
    CONSTRAINT jobs_profile_uq UNIQUE (application, job_id, serializer_profile_id, serializer_profile_version),
    CONSTRAINT jobs_payload_version_ck CHECK (payload_version >= 1),
    CONSTRAINT jobs_profile_version_ck CHECK (serializer_profile_version >= 1),
    CONSTRAINT jobs_priority_ck CHECK (priority IN ('low', 'medium', 'high')),
    CONSTRAINT jobs_state_ck CHECK (state IN ('pending', 'reserved', 'running', 'retrying', 'succeeded', 'failed', 'cancelled')),
    CONSTRAINT jobs_generation_ck CHECK (generation >= 1),
    CONSTRAINT jobs_revision_ck CHECK (revision >= 1),
    CONSTRAINT jobs_reservation_count_ck CHECK (reservation_count >= 0),
    CONSTRAINT jobs_execution_count_ck CHECK (execution_count >= 0 AND execution_count <= reservation_count),
    CONSTRAINT jobs_eligible_ms_ck CHECK (eligible_at = runwick.ms_floor(eligible_at)),
    CONSTRAINT jobs_lease_ms_ck CHECK (lease_expires_at IS NULL OR lease_expires_at = runwick.ms_floor(lease_expires_at)),
    CONSTRAINT jobs_ownership_ck CHECK (
        CASE WHEN state IN ('reserved', 'running')
             THEN current_attempt_id IS NOT NULL AND worker_instance_id IS NOT NULL
                  AND ownership_token IS NOT NULL AND lease_expires_at IS NOT NULL
             ELSE current_attempt_id IS NULL AND worker_instance_id IS NULL
                  AND ownership_token IS NULL AND lease_expires_at IS NULL
        END),
    CONSTRAINT jobs_terminal_ck CHECK (
        CASE WHEN state IN ('succeeded', 'failed', 'cancelled')
             THEN terminal_at IS NOT NULL AND terminal_reason IS NOT NULL
             ELSE terminal_at IS NULL AND terminal_reason IS NULL
        END),
    CONSTRAINT jobs_terminal_reason_ck CHECK (
        terminal_reason IS NULL
        OR (state = 'succeeded' AND terminal_reason = 'succeeded')
        OR (state = 'failed' AND terminal_reason IN ('permanent_failure', 'execution_budget_exhausted', 'reservation_abandonment_exhausted'))
        OR (state = 'cancelled' AND terminal_reason IN ('user_cancelled', 'cancellation_requested', 'cancelled_before_start', 'cancelled_while_waiting'))),
    CONSTRAINT jobs_cancel_ck CHECK (cancel_requested_at IS NOT NULL OR (cancel_reason IS NULL AND cancel_requested_by IS NULL)),
    CONSTRAINT jobs_cancel_reason_ck CHECK (cancel_reason IS NULL OR octet_length(cancel_reason) <= 512),
    CONSTRAINT jobs_cancel_by_ck CHECK (cancel_requested_by IS NULL OR octet_length(cancel_requested_by) BETWEEN 1 AND 256),
    CONSTRAINT jobs_payload_retained_ck CHECK (payload_retained OR state IN ('succeeded', 'failed', 'cancelled')),
    CONSTRAINT jobs_detail_expired_ck CHECK (detail_expired_at IS NULL OR state IN ('succeeded', 'failed', 'cancelled'))
);

CREATE TABLE runwick.job_payloads (
    application                 runwick.rw_identifier NOT NULL,
    job_id                      uuid        NOT NULL,
    payload_utf8                bytea       NOT NULL,
    payload_encoding_version    integer     NOT NULL DEFAULT 1,
    serializer_profile_id       runwick.rw_identifier NOT NULL,
    serializer_profile_version  integer     NOT NULL,
    headers                     jsonb       NOT NULL DEFAULT '{"business":{},"diagnostic":{}}'::jsonb,
    CONSTRAINT job_payloads_pk PRIMARY KEY (application, job_id),
    CONSTRAINT job_payloads_job_profile_fk FOREIGN KEY (application, job_id, serializer_profile_id, serializer_profile_version)
        REFERENCES runwick.jobs (application, job_id, serializer_profile_id, serializer_profile_version)
        ON DELETE CASCADE,
    CONSTRAINT job_payloads_size_ck CHECK (octet_length(payload_utf8) BETWEEN 2 AND 1048576),
    CONSTRAINT job_payloads_encoding_ck CHECK (payload_encoding_version = 1),
    CONSTRAINT job_payloads_headers_ck CHECK (
        jsonb_typeof(headers) = 'object'
        AND jsonb_typeof(headers -> 'business') = 'object'
        AND jsonb_typeof(headers -> 'diagnostic') = 'object')
);

CREATE TABLE runwick.job_generations (
    application                     runwick.rw_identifier NOT NULL,
    job_id                          uuid        NOT NULL,
    generation                      integer     NOT NULL,
    created_at                      timestamptz NOT NULL,
    created_by                      text,
    created_reason                  text COLLATE "C" NOT NULL,
    policy_snapshot_version         integer     NOT NULL DEFAULT 1,
    max_executions                  integer     NOT NULL,
    max_unstarted_abandonments      integer     NOT NULL,
    execution_timeout_ms            bigint,
    retry_policy                    jsonb       NOT NULL,
    retry_policy_version            integer     NOT NULL DEFAULT 1,
    unstarted_retry_policy          jsonb       NOT NULL,
    unstarted_retry_policy_version  integer     NOT NULL DEFAULT 1,
    started_count                   integer     NOT NULL DEFAULT 0,
    consecutive_unstarted_count     integer     NOT NULL DEFAULT 0,
    CONSTRAINT job_generations_pk PRIMARY KEY (application, job_id, generation),
    CONSTRAINT job_generations_job_fk FOREIGN KEY (application, job_id)
        REFERENCES runwick.jobs (application, job_id) ON DELETE CASCADE,
    CONSTRAINT job_generations_generation_ck CHECK (generation >= 1),
    CONSTRAINT job_generations_created_by_ck CHECK (created_by IS NULL OR octet_length(created_by) BETWEEN 1 AND 256),
    CONSTRAINT job_generations_reason_ck CHECK (created_reason IN ('initial', 'scheduled', 'manual_retry', 'rerun_succeeded')),
    CONSTRAINT job_generations_snapshot_version_ck CHECK (policy_snapshot_version >= 1),
    CONSTRAINT job_generations_max_executions_ck CHECK (max_executions BETWEEN 1 AND 100),
    CONSTRAINT job_generations_max_unstarted_ck CHECK (max_unstarted_abandonments BETWEEN 1 AND 100),
    CONSTRAINT job_generations_timeout_ck CHECK (execution_timeout_ms IS NULL OR execution_timeout_ms BETWEEN 1 AND 2592000000),
    CONSTRAINT job_generations_retry_ck CHECK (
        jsonb_typeof(retry_policy) = 'object'
        AND retry_policy ?& ARRAY['backoff', 'evaluatorVersion', 'initialDelay', 'jitterKind', 'maximumDelay']),
    CONSTRAINT job_generations_unstarted_retry_ck CHECK (
        jsonb_typeof(unstarted_retry_policy) = 'object'
        AND unstarted_retry_policy ?& ARRAY['backoff', 'evaluatorVersion', 'initialDelay', 'jitterKind', 'maximumDelay']),
    CONSTRAINT job_generations_retry_version_ck CHECK (retry_policy_version >= 1 AND unstarted_retry_policy_version >= 1),
    CONSTRAINT job_generations_started_ck CHECK (started_count BETWEEN 0 AND max_executions),
    CONSTRAINT job_generations_unstarted_ck CHECK (consecutive_unstarted_count BETWEEN 0 AND max_unstarted_abandonments)
);

CREATE TABLE runwick.job_attempts (
    application             runwick.rw_identifier NOT NULL,
    attempt_id              uuid        NOT NULL,
    job_id                  uuid        NOT NULL,
    generation              integer     NOT NULL,
    reservation_number      bigint      NOT NULL,
    execution_number        integer,
    worker_instance_id      uuid        NOT NULL,
    ownership_token         uuid        NOT NULL,
    reserved_at             timestamptz NOT NULL,
    authorized_at           timestamptz,
    finished_at             timestamptz,
    execution_elapsed_ms    bigint,
    outcome                 text COLLATE "C",
    disposition             text COLLATE "C",
    applied_reason          text COLLATE "C",
    retry_delay_ms          bigint,
    next_eligible_at        timestamptz,
    error                   jsonb,
    closed_by_command_id    uuid,
    CONSTRAINT job_attempts_pk PRIMARY KEY (application, attempt_id),
    CONSTRAINT job_attempts_job_attempt_uq UNIQUE (application, job_id, attempt_id),
    CONSTRAINT job_attempts_reservation_uq UNIQUE (application, job_id, reservation_number),
    CONSTRAINT job_attempts_execution_uq UNIQUE (application, job_id, generation, execution_number),
    CONSTRAINT job_attempts_generation_fk FOREIGN KEY (application, job_id, generation)
        REFERENCES runwick.job_generations (application, job_id, generation) ON DELETE CASCADE,
    CONSTRAINT job_attempts_reservation_number_ck CHECK (reservation_number >= 1),
    CONSTRAINT job_attempts_execution_number_ck CHECK (execution_number IS NULL OR execution_number >= 1),
    CONSTRAINT job_attempts_authorized_ck CHECK ((execution_number IS NULL) = (authorized_at IS NULL)),
    CONSTRAINT job_attempts_closed_ck CHECK (
        (finished_at IS NULL) = (outcome IS NULL)
        AND (finished_at IS NULL) = (disposition IS NULL)
        AND (finished_at IS NULL) = (closed_by_command_id IS NULL)),
    CONSTRAINT job_attempts_outcome_ck CHECK (outcome IS NULL OR outcome IN (
        'succeeded', 'retryable_failure', 'permanent_failure', 'user_cancelled', 'interrupted', 'timed_out',
        'abandoned_before_start', 'abandoned_running', 'released', 'cancelled_before_start', 'admission_deferred')),
    CONSTRAINT job_attempts_executed_outcome_ck CHECK (
        outcome IS NULL
        OR (outcome IN ('succeeded', 'retryable_failure', 'permanent_failure', 'user_cancelled', 'interrupted', 'timed_out', 'abandoned_running')) = (execution_number IS NOT NULL)),
    CONSTRAINT job_attempts_disposition_ck CHECK (disposition IS NULL OR disposition IN ('pending', 'retrying', 'succeeded', 'failed', 'cancelled')),
    CONSTRAINT job_attempts_reason_ck CHECK ((applied_reason IS NOT NULL) = (disposition IN ('succeeded', 'failed', 'cancelled'))),
    CONSTRAINT job_attempts_elapsed_ck CHECK (execution_elapsed_ms IS NULL OR (execution_elapsed_ms >= 0 AND execution_number IS NOT NULL)),
    CONSTRAINT job_attempts_retry_ck CHECK (
        (retry_delay_ms IS NULL) = (next_eligible_at IS NULL)
        AND (retry_delay_ms IS NULL OR (retry_delay_ms >= 1 AND disposition IN ('pending', 'retrying')))),
    CONSTRAINT job_attempts_error_ck CHECK (error IS NULL OR (jsonb_typeof(error) = 'object' AND octet_length(error::text) <= 20480))
);

-- Circular and current-pointer foreign keys, checked at commit.
ALTER TABLE runwick.jobs
    ADD CONSTRAINT jobs_current_generation_fk FOREIGN KEY (application, job_id, generation)
        REFERENCES runwick.job_generations (application, job_id, generation)
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT jobs_current_attempt_fk FOREIGN KEY (application, job_id, current_attempt_id)
        REFERENCES runwick.job_attempts (application, job_id, attempt_id)
        DEFERRABLE INITIALLY DEFERRED;

-- At most one open attempt per job.
CREATE UNIQUE INDEX job_attempts_one_open
    ON runwick.job_attempts (application, job_id) WHERE finished_at IS NULL;

-- §4.5 Concurrency scopes, requirements, permits -----------------------------------------

CREATE TABLE runwick.concurrency_scopes (
    application     runwick.rw_identifier NOT NULL,
    scope_id        uuid        NOT NULL,
    scope_kind      text COLLATE "C" NOT NULL,
    scope_key       runwick.rw_key NOT NULL,
    capacity        integer     NOT NULL,
    policy_revision bigint      NOT NULL DEFAULT 1,
    fencing_enabled boolean     NOT NULL,
    fencing_counter bigint      NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,
    CONSTRAINT concurrency_scopes_pk PRIMARY KEY (application, scope_id),
    CONSTRAINT concurrency_scopes_natural_uq UNIQUE (application, scope_kind, scope_key),
    CONSTRAINT concurrency_scopes_kind_ck CHECK (scope_kind IN ('queue', 'job-type', 'resource')),
    CONSTRAINT concurrency_scopes_capacity_ck CHECK (capacity >= 0),
    CONSTRAINT concurrency_scopes_revision_ck CHECK (policy_revision >= 1),
    CONSTRAINT concurrency_scopes_fencing_ck CHECK (fencing_counter >= 0 AND (fencing_enabled OR fencing_counter = 0)),
    CONSTRAINT concurrency_scopes_fencing_kind_ck CHECK (NOT fencing_enabled OR scope_kind = 'resource')
);

CREATE TABLE runwick.job_concurrency_requirements (
    application runwick.rw_identifier NOT NULL,
    job_id      uuid NOT NULL,
    scope_id    uuid NOT NULL,
    CONSTRAINT job_concurrency_requirements_pk PRIMARY KEY (application, job_id, scope_id),
    CONSTRAINT job_concurrency_requirements_job_fk FOREIGN KEY (application, job_id)
        REFERENCES runwick.jobs (application, job_id) ON DELETE CASCADE,
    CONSTRAINT job_concurrency_requirements_scope_fk FOREIGN KEY (application, scope_id)
        REFERENCES runwick.concurrency_scopes (application, scope_id)
);

CREATE TABLE runwick.concurrency_permits (
    application         runwick.rw_identifier NOT NULL,
    scope_id            uuid        NOT NULL,
    attempt_id          uuid        NOT NULL,
    job_id              uuid        NOT NULL,
    ownership_token     uuid        NOT NULL,
    lease_expires_at    timestamptz NOT NULL,
    fencing_value       bigint,
    CONSTRAINT concurrency_permits_pk PRIMARY KEY (application, scope_id, attempt_id),
    CONSTRAINT concurrency_permits_scope_fk FOREIGN KEY (application, scope_id)
        REFERENCES runwick.concurrency_scopes (application, scope_id),
    CONSTRAINT concurrency_permits_attempt_fk FOREIGN KEY (application, job_id, attempt_id)
        REFERENCES runwick.job_attempts (application, job_id, attempt_id),
    CONSTRAINT concurrency_permits_requirement_fk FOREIGN KEY (application, job_id, scope_id)
        REFERENCES runwick.job_concurrency_requirements (application, job_id, scope_id),
    CONSTRAINT concurrency_permits_lease_ms_ck CHECK (lease_expires_at = runwick.ms_floor(lease_expires_at)),
    CONSTRAINT concurrency_permits_fence_ck CHECK (fencing_value IS NULL OR fencing_value >= 1)
);

-- §4.6 Dispatch keys -----------------------------------------------------------------------

CREATE TABLE runwick.dispatch_keys (
    application             runwick.rw_identifier NOT NULL,
    job_name                runwick.rw_job_name NOT NULL,
    key_kind                text COLLATE "C" NOT NULL,
    key                     runwick.rw_key NOT NULL,
    bound_job_id            uuid,
    business_fingerprint    runwick.rw_digest,
    policy_fingerprint      runwick.rw_digest,
    fingerprint_version     integer,
    expires_at              timestamptz,
    created_at              timestamptz NOT NULL,
    updated_at              timestamptz NOT NULL,
    CONSTRAINT dispatch_keys_pk PRIMARY KEY (application, job_name, key_kind, key),
    CONSTRAINT dispatch_keys_kind_ck CHECK (key_kind IN ('active', 'window')),
    CONSTRAINT dispatch_keys_binding_ck CHECK (
        CASE key_kind
            WHEN 'active' THEN business_fingerprint IS NULL AND policy_fingerprint IS NULL
                               AND fingerprint_version IS NULL AND expires_at IS NULL
            ELSE (bound_job_id IS NULL AND business_fingerprint IS NULL AND policy_fingerprint IS NULL
                  AND fingerprint_version IS NULL AND expires_at IS NULL)
              OR (bound_job_id IS NOT NULL AND business_fingerprint IS NOT NULL AND policy_fingerprint IS NOT NULL
                  AND fingerprint_version IS NOT NULL AND expires_at IS NOT NULL)
        END),
    CONSTRAINT dispatch_keys_fingerprint_version_ck CHECK (fingerprint_version IS NULL OR fingerprint_version >= 1),
    CONSTRAINT dispatch_keys_expires_ms_ck CHECK (expires_at IS NULL OR expires_at = runwick.ms_floor(expires_at))
);

-- §4.7 Command receipts (partitioned by retention class) ----------------------------------

CREATE TABLE runwick.storage_commands (
    application             runwick.rw_identifier NOT NULL,
    retention_class         text COLLATE "C" NOT NULL,
    command_id              uuid        NOT NULL,
    operation               text COLLATE "C" NOT NULL,
    operation_version       integer     NOT NULL,
    protocol_epoch          integer     NOT NULL,
    fingerprint             runwick.rw_digest NOT NULL,
    fingerprint_version     integer     NOT NULL,
    not_after               timestamptz NOT NULL,
    receipt_codec_version   integer     NOT NULL,
    retain_until            timestamptz NOT NULL,
    status                  text COLLATE "C",
    result                  jsonb,
    decided_at              timestamptz,
    CONSTRAINT storage_commands_pk PRIMARY KEY (application, retention_class, command_id),
    CONSTRAINT storage_commands_class_ck CHECK (retention_class IN ('ownership', 'durable')),
    CONSTRAINT storage_commands_operation_ck CHECK (operation ~ '^[A-Z][A-Za-z]{1,63}$'),
    CONSTRAINT storage_commands_operation_class_ck CHECK (
        (operation IN ('Reserve', 'Begin', 'Recover', 'ReleaseReservation')) = (retention_class = 'ownership')),
    CONSTRAINT storage_commands_no_renew_ck CHECK (operation <> 'Renew'),
    CONSTRAINT storage_commands_versions_ck CHECK (
        operation_version >= 1 AND protocol_epoch >= 1 AND fingerprint_version >= 1 AND receipt_codec_version >= 1),
    CONSTRAINT storage_commands_not_after_ms_ck CHECK (not_after = runwick.ms_floor(not_after)),
    CONSTRAINT storage_commands_retain_ck CHECK (
        retain_until >= not_after + CASE retention_class WHEN 'ownership' THEN interval '10 minutes'
                                                         ELSE interval '24 hours' END),
    CONSTRAINT storage_commands_decision_ck CHECK (
        (status IS NULL) = (result IS NULL) AND (result IS NULL) = (decided_at IS NULL)),
    CONSTRAINT storage_commands_result_ck CHECK (result IS NULL OR jsonb_typeof(result) = 'object')
) PARTITION BY LIST (retention_class);

CREATE TABLE runwick.storage_commands_ownership
    PARTITION OF runwick.storage_commands FOR VALUES IN ('ownership');

CREATE TABLE runwick.storage_commands_durable
    PARTITION OF runwick.storage_commands FOR VALUES IN ('durable');

-- Receipt-placeholder guard: a committed receipt row must carry its decision.
CREATE FUNCTION runwick.storage_commands_assert_decided() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM runwick.storage_commands AS c
        WHERE c.application = NEW.application
          AND c.retention_class = NEW.retention_class
          AND c.command_id = NEW.command_id
          AND (c.status IS NULL OR c.result IS NULL OR c.decided_at IS NULL)) THEN
        RAISE EXCEPTION 'runwick receipt % committed without a decision', NEW.command_id
            USING ERRCODE = 'RWR01';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER storage_commands_ownership_decided
    AFTER INSERT ON runwick.storage_commands_ownership
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION runwick.storage_commands_assert_decided();

CREATE CONSTRAINT TRIGGER storage_commands_durable_decided
    AFTER INSERT ON runwick.storage_commands_durable
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION runwick.storage_commands_assert_decided();

-- §4.8 Execution history events ------------------------------------------------------------

CREATE TABLE runwick.queue_events (
    application runwick.rw_identifier NOT NULL,
    event_id    bigint GENERATED ALWAYS AS IDENTITY,
    job_id      uuid,
    attempt_id  uuid,
    command_id  uuid,
    event_kind  text COLLATE "C" NOT NULL,
    occurred_at timestamptz NOT NULL,
    actor_id    text,
    details     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT queue_events_pk PRIMARY KEY (application, event_id),
    CONSTRAINT queue_events_kind_ck CHECK (event_kind IN (
        'dispatched', 'policy_drift',
        'attempt_reserved', 'attempt_started', 'attempt_cancelled_before_start',
        'attempt_finished', 'attempt_recovered', 'attempt_released')),
    CONSTRAINT queue_events_actor_ck CHECK (actor_id IS NULL OR octet_length(actor_id) BETWEEN 1 AND 256),
    CONSTRAINT queue_events_details_ck CHECK (jsonb_typeof(details) = 'object' AND octet_length(details::text) <= 4096)
);

-- §4.9 Worker presence ---------------------------------------------------------------------

CREATE TABLE runwick.worker_instances (
    application         runwick.rw_identifier NOT NULL,
    instance_id         uuid        NOT NULL,
    process_id          integer     NOT NULL,
    host_name           text        NOT NULL,
    build_id            text        NOT NULL,
    status              text COLLATE "C" NOT NULL,
    capability_revision bigint      NOT NULL,
    presence_sequence   bigint      NOT NULL,
    subscriptions       jsonb       NOT NULL DEFAULT '[]'::jsonb,
    pool_targets        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    started_at          timestamptz NOT NULL,
    heartbeat_at        timestamptz NOT NULL,
    expires_at          timestamptz NOT NULL,
    CONSTRAINT worker_instances_pk PRIMARY KEY (application, instance_id),
    CONSTRAINT worker_instances_process_ck CHECK (process_id >= 0),
    CONSTRAINT worker_instances_host_ck CHECK (octet_length(host_name) BETWEEN 1 AND 256),
    CONSTRAINT worker_instances_build_ck CHECK (octet_length(build_id) BETWEEN 1 AND 256),
    CONSTRAINT worker_instances_status_ck CHECK (status IN ('starting', 'ready', 'draining', 'degraded', 'stopping')),
    CONSTRAINT worker_instances_revision_ck CHECK (capability_revision >= 1),
    CONSTRAINT worker_instances_sequence_ck CHECK (presence_sequence >= 1),
    CONSTRAINT worker_instances_subscriptions_ck CHECK (jsonb_typeof(subscriptions) = 'array' AND jsonb_array_length(subscriptions) <= 128),
    CONSTRAINT worker_instances_pool_targets_ck CHECK (jsonb_typeof(pool_targets) = 'object'),
    CONSTRAINT worker_instances_expiry_ck CHECK (expires_at > heartbeat_at AND heartbeat_at >= started_at)
);

CREATE TABLE runwick.worker_capabilities (
    application                 runwick.rw_identifier NOT NULL,
    instance_id                 uuid        NOT NULL,
    capability_revision         bigint      NOT NULL,
    job_name                    runwick.rw_job_name NOT NULL,
    payload_version             integer     NOT NULL,
    serializer_profile_id       runwick.rw_identifier NOT NULL,
    serializer_profile_version  integer     NOT NULL,
    CONSTRAINT worker_capabilities_pk PRIMARY KEY (
        application, instance_id, capability_revision,
        job_name, payload_version, serializer_profile_id, serializer_profile_version),
    CONSTRAINT worker_capabilities_instance_fk FOREIGN KEY (application, instance_id)
        REFERENCES runwick.worker_instances (application, instance_id) ON DELETE CASCADE,
    CONSTRAINT worker_capabilities_revision_ck CHECK (capability_revision >= 1),
    CONSTRAINT worker_capabilities_versions_ck CHECK (payload_version >= 1 AND serializer_profile_version >= 1)
);

-- §4.11 Indexes ------------------------------------------------------------------------------

CREATE INDEX jobs_ready
    ON runwick.jobs (application, queue, priority, eligible_at, job_id)
    WHERE state IN ('pending', 'retrying');

CREATE INDEX jobs_expired_lease
    ON runwick.jobs (application, lease_expires_at, job_id)
    WHERE state IN ('reserved', 'running');

CREATE INDEX jobs_terminal_retention
    ON runwick.jobs (application, terminal_at, job_id)
    WHERE state IN ('succeeded', 'failed', 'cancelled');

CREATE INDEX jobs_queue_list
    ON runwick.jobs (application, queue, created_at, job_id);

CREATE INDEX job_concurrency_requirements_scope
    ON runwick.job_concurrency_requirements (application, scope_id, job_id);

CREATE INDEX permits_live_count
    ON runwick.concurrency_permits (application, scope_id, lease_expires_at);

CREATE INDEX permits_attempt
    ON runwick.concurrency_permits (application, attempt_id, scope_id);

CREATE INDEX permits_job_attempt
    ON runwick.concurrency_permits (application, job_id, attempt_id);

CREATE INDEX permits_job_scope
    ON runwick.concurrency_permits (application, job_id, scope_id);

CREATE INDEX command_retention
    ON runwick.storage_commands (application, retain_until, command_id);

CREATE INDEX command_by_id
    ON runwick.storage_commands (application, command_id);

CREATE INDEX events_job_history
    ON runwick.queue_events (application, job_id, event_id)
    WHERE job_id IS NOT NULL;

CREATE INDEX events_retention
    ON runwick.queue_events (application, occurred_at, event_id);

CREATE INDEX worker_instances_expiry
    ON runwick.worker_instances (application, expires_at, instance_id);

CREATE INDEX worker_capabilities_tuple
    ON runwick.worker_capabilities (application, job_name, payload_version,
                                    serializer_profile_id, serializer_profile_version, instance_id);

-- §4.12 Bootstrap rows ---------------------------------------------------------------------

INSERT INTO runwick.store_metadata (
    singleton_id, store_id, schema_revision, protocol_epoch, migration_mode,
    compatibility_manifest_version, compatibility_manifest, manifest_digest, revision, updated_at)
VALUES (1, gen_random_uuid(), 0, 1, 'maintenance',
        1, '{}'::jsonb, sha256(convert_to('{}', 'UTF8')), 1, runwick.ms_floor(clock_timestamp()));
```

### 4.1 Domains and helpers

`rw_identifier`, `rw_job_name` and `rw_key` enforce 02 §1 grammar at the database as defence in depth; the library validates first and reports `identity_invalid`. `ms_floor`/`ms_ceil` are `IMMUTABLE` because they convert through UTC explicitly; they are independent of the session `TimeZone`.

### 4.2 Gates

- `store_metadata` has exactly one row (`singleton_id = 1`). `schema_revision = 0` exists only during installation and forces `maintenance`. `compatibility_manifest` holds the store manifest (§8.5); `manifest_digest` is its SHA-256 canonical digest computed by the runner; `revision` increments on every gate change.
- `application_controls.required_capabilities` is an object `{capability: minimumVersion}`. M2 binaries know `execution`, `concurrency_permits`, `dispatch_uniqueness` (all version 1). Any other key (e.g. `rate_admission`) refuses creation paths (03 §10.3).
- `schema_migrations` records each phase of each migration; `state = 'started'` is never treated as applied.

### 4.3 Queue controls

A row is created by `RegisterQueue/1` (seed-if-absent) and never deleted. `paused` and `revision` change only through `SetQueueState/1` (harness-only in M2). Reserve reads the row `FOR SHARE`; pause takes `FOR UPDATE`, so a completed pause serializes after reservations already holding the share lock, and any reservation admitted before the pause commits may still execute. Pause revokes no lease.

### 4.4 Jobs, payloads, generations, attempts

- `jobs_ownership_ck`: the four ownership columns are all set exactly in `reserved`/`running`. `jobs_terminal_ck` / `jobs_terminal_reason_ck`: terminal states require `terminal_at` and an allowed reason (03 §5.5).
- `jobs.generation` → `job_generations` and `jobs.current_attempt_id` → `job_attempts (application, job_id, attempt_id)` are **deferred** FKs, checked at commit; the current-attempt FK also prevents pointing at another job's attempt. A null `current_attempt_id` is not checked.
- `active_unique_key` records the active key the job claims; it is kept after the job becomes terminal (the binding itself is cleared in `dispatch_keys`).
- Queue, priority, identity tuple and scope membership are immutable for a generation; no statement updates them.
- `job_payloads` rows are immutable after insert. The composite FK guarantees the payload's profile tuple equals the job's. Payload rows may be deleted by retention only for terminal jobs (`payload_retained = false`); not in M2.
- `job_generations` counters are the only mutable columns of a generation; policy columns are snapshots. `started_count ≤ max_executions` and `consecutive_unstarted_count ≤ max_unstarted_abandonments` are CHECKed.
- `job_attempts`: null `execution_number` means execution was never authorized. Ordinary UNIQUE (not `NULLS NOT DISTINCT`) permits many unauthorized attempts. `job_attempts_executed_outcome_ck` ties executed outcomes to a non-null execution number. `error` holds `{code, exceptionType, message, stackTrace, truncated}`, ≤ 16 KiB of text plus JSON overhead.

### 4.5 Concurrency scopes, requirements, permits

- Natural key `(application, scope_kind, scope_key)`; queue scope key = queue name, job-type scope key = job name, resource scope key = the selector's canonical resource key (shared across job types).
- `capacity` is live authority, seeded if absent and never overwritten by configuration. `policy_revision` changes only through policy edits (not in M2).
- `fencing_counter` only increases; scope rows are never deleted or recreated under the same identity in M2.
- `concurrency_permits.lease_expires_at` duplicates the job lease for indexed live counts. Permits are deleted only by exact `(attempt_id, ownership_token)` on finalization/recovery/release; expired permits simply stop counting.

### 4.6 Dispatch keys

`key_kind = 'active'` rows have no TTL and no fingerprint; `bound_job_id` is non-null while the bound job is non-terminal. `key_kind = 'window'` rows are either unbound (all binding columns null — only possible for a row created by an aborted/conflicting batch item) or fully bound until `expires_at`; an expired binding is rebound in place under the row lock. Bindings have no FK to `jobs` (they outlive job history). Rows are never deleted in M2.

### 4.7 Command receipts

- Partitions: `storage_commands_ownership` (Reserve, Begin, Recover, ReleaseReservation; `retain_until ≥ not_after + 10 min`) and `storage_commands_durable` (everything else; `≥ not_after + 24 h`). `storage_commands_no_renew_ck` forbids Renew receipts (R1).
- `status`/`result`/`decided_at` are null only for an uncommitted placeholder. The constraint triggers raise `RWR01` at COMMIT if a placeholder would commit (PGX-05). Callers must not run `SET CONSTRAINTS ALL IMMEDIATE` inside a transaction that stages Runwick work.
- `command_by_id` supports the cross-class conflict probe and resolution by ID.

### 4.8 Queue events

Kinds written in M2: `dispatched` (per created job), `policy_drift` (per reused-with-drift item), `attempt_reserved`, `attempt_started`, `attempt_cancelled_before_start`, `attempt_finished`, `attempt_recovered`, `attempt_released`. No event is written by Renew, readiness probes or presence. `details` holds safe bounded fields only (≤ 4 KiB): e.g. `{"generation":1,"state":"retrying","outcome":"timed_out","disposition":"retrying","reason":null,"retryDelay":4213}`; never payload, metadata values or error text. Events are history, not replay authority.

### 4.9 Worker presence

`worker_instances` is upserted by `PublishPresence/1` with a strictly increasing `presence_sequence`; `expires_at = heartbeat_at + 20 s`. `worker_capabilities` holds complete capability revisions; queries join only `worker_instances.capability_revision`. No FK from jobs, attempts or permits references presence. Expired presence is pruned 24 h after expiry by cleanup (not in M2).

### 4.10 Receipt-placeholder trigger

`runwick.storage_commands_assert_decided()` re-reads the row by primary key at commit (the row may have been updated after the insert that queued the trigger) and raises `RWR01` if any decision column is null. It is attached as a deferred constraint trigger to each partition; a future partition must receive the same trigger in its migration.

### 4.11 Indexes

| Index | Columns / predicate | Serves |
|---|---|---|
| `jobs_ready` | `(application, queue, priority, eligible_at, job_id) WHERE state IN ('pending','retrying')` | Reserve candidate cursor `(eligible_at, job_id)`; readiness probe |
| `jobs_expired_lease` | `(application, lease_expires_at, job_id) WHERE state IN ('reserved','running')` | ListExpiredLeases |
| `jobs_terminal_retention` | `(application, terminal_at, job_id) WHERE state IN ('succeeded','failed','cancelled')` | Retention (later) |
| `jobs_queue_list` | `(application, queue, created_at, job_id)` | Bounded job lists |
| `jobs_profile_uq` | UNIQUE `(application, job_id, serializer_profile_id, serializer_profile_version)` | Payload profile FK |
| `job_attempts_*_uq`, `job_attempts_one_open` | see DDL | Attempt history; one open attempt |
| `job_concurrency_requirements_scope` | `(application, scope_id, job_id)` | Scope → jobs diagnostics, FK checks |
| `permits_live_count` | `(application, scope_id, lease_expires_at)` | Live permit count |
| `permits_attempt`, `permits_job_attempt`, `permits_job_scope` | see DDL | Exact-attempt renewal/release; FK checks |
| `command_retention` | `(application, retain_until, command_id)` (partitioned) | Receipt cleanup |
| `command_by_id` | `(application, command_id)` (partitioned) | Cross-class conflict probe |
| `events_job_history` | `(application, job_id, event_id) WHERE job_id IS NOT NULL` | Job history |
| `events_retention` | `(application, occurred_at, event_id)` | Event retention |
| `worker_instances_expiry` | `(application, expires_at, instance_id)` | Presence queries |
| `worker_capabilities_tuple` | `(application, job_name, payload_version, serializer_profile_id, serializer_profile_version, instance_id)` | "No capable worker" diagnostic |

The ready predicate is literal and time-independent; queries repeat exactly `state IN ('pending', 'retrying')`. Capability filtering is a post-filter over `jobs_ready`; adding `job_name` to that index is deferred until the unsupported-backlog workload shows need. Every renewal updates `jobs_expired_lease` and `permits_live_count`; WAL per renewal is a G-1 measurement.

### 4.12 Bootstrap row

The script ends by inserting the singleton store row with a fresh `store_id`, `schema_revision = 0` and `maintenance`. No application, queue or scope rows are created by migrations.

## 5. Lock order

### 5.1 Ranks

Every normal path acquires, in this order and never backwards:

```text
1  store_metadata             FOR SHARE            (FOR UPDATE only for a store-gate change)
2  application_controls       FOR SHARE            (FOR UPDATE only for an application-gate change; INSERT … DO NOTHING by RegisterApplication)
3  storage_commands receipt   INSERT … ON CONFLICT DO NOTHING, then SELECT … FOR UPDATE
3a outbox delivery identity  pg_advisory_xact_lock(hash(application, source, message)), then outbox_deliveries FOR UPDATE (ReceiveOutbox only)
   [schedule rows: spec 13]
4  queue_controls             sorted by queue (C)  FOR SHARE (Reserve) | FOR UPDATE (SetQueueState)
5  jobs (existing)            UUID order           FOR NO KEY UPDATE
6  dispatch_keys              sorted (job_name, key_kind, key) bytewise   FOR UPDATE
7  concurrency_scopes         UUID order           FOR UPDATE
   [not in M2: rate_scopes, UUID order, after all concurrency scopes]
8  dependent rows             job_generations, job_attempts, job_payloads, job_concurrency_requirements,
                              concurrency_permits, queue_events, worker_* — reached only through an already-locked owner
```

Rules:

1. Fresh `clock_timestamp()` is read after the last potentially blocking lock and is the decision time.
2. Rows of one rank are locked explicitly in sorted order with `ORDER BY … FOR …` (PostgreSQL locks rows in the sort order of the result). Query order is not relied on for implicit FK locks; those are controlled by insert order (§5.3).
3. A gate lock is never upgraded after a lower rank is held. Gate-changing transactions take the exclusive gate lock directly and never acquire job/key/scope rows afterward.
4. Fresh job creation (Dispatch) is the one deliberate exception to rank 5: brand-new job rows are inserted after key locks. New IDs are preallocated and asserted absent (plain INSERT; a unique violation is `id_collision`, never permission to update). **Dispatch never locks an existing job.**
5. No lock is held across handler execution, payload decoding, retry sleeps or external I/O. Caller-owned business transactions may hold their own locks outside this order; whole-transaction retry is the caller's.
6. A known deadlock or lock timeout rolls back the entire operation; the retry keeps the command identity (P-13).

### 5.2 Lock-order matrix

"—" means not touched. Ranks 1–3 are identical for every receipt-bearing mutation: store `FOR SHARE` → application `FOR SHARE` → receipt row.

| Operation | 1–2 Gates | 3 Receipt | 4 Queue control | 5 Jobs | 6 Dispatch keys | 7 Concurrency scopes | 8 Dependent writes (in this order) |
|---|---|---|---|---|---|---|---|
| RegisterApplication/1 | store SHARE; app INSERT…DO NOTHING then SHARE | yes | — | — | — | — | — |
| RegisterQueue/1 | SHARE / SHARE | yes | INSERT…DO NOTHING, sorted | — | — | — | — |
| RegisterScope/1 | SHARE / SHARE | yes | — | — | — | INSERT…DO NOTHING sorted (kind, key); no row lock on existing scopes | — |
| SetQueueState/1 (harness) | SHARE / SHARE | yes | FOR UPDATE (one row) | — | — | — | — |
| Dispatch/1, DispatchMany/1 | SHARE / SHARE | yes | plain read (registration check) | INSERT new rows, ascending `job_id`, **after** rank 6 | INSERT…DO NOTHING then FOR UPDATE, sorted once over the whole batch | INSERT…DO NOTHING sorted (kind, key); FK KEY SHARE via requirement inserts in ascending `scope_id` | key binding updates, payloads, generations, requirements (sorted scope_id), events |
| Reserve/1 | SHARE / SHARE | yes | FOR SHARE | candidate FOR NO KEY UPDATE SKIP LOCKED (one) | — | FOR UPDATE, sorted, the job's required scopes | fence counters (already locked), attempt insert, job update, permits insert (sorted scope_id), event |
| Begin/1 (authorize) | SHARE / SHARE | yes | — | FOR NO KEY UPDATE | — | — | generation, job, attempt, event |
| Begin/1 (cancel / exhausted) | SHARE / SHARE | yes | — | FOR NO KEY UPDATE | active key FOR UPDATE (if any) | FOR UPDATE sorted | attempt close, permits delete, job, key clear, event |
| Renew/1 | SHARE / SHARE | **none** | — | FOR NO KEY UPDATE | — | FOR UPDATE sorted | job lease, permit leases |
| Finish/1 | SHARE / SHARE | yes | — | FOR NO KEY UPDATE | active key FOR UPDATE when terminal | FOR UPDATE sorted | attempt close, permits delete, generation (none), job, key clear, event |
| Recover/1 | SHARE / SHARE | yes | — | FOR NO KEY UPDATE | active key FOR UPDATE when terminal | FOR UPDATE sorted | attempt close, permits delete, generation (k), job, key clear, event |
| ReleaseReservation/1 | SHARE / SHARE | yes | — | FOR NO KEY UPDATE | active key FOR UPDATE only when a cancellation terminalizes | FOR UPDATE sorted | attempt close, permits delete, job, key clear, event |
| ReadinessProbe/1 | store SHARE (CTE) | — | read | read (no lock) | — | — | — |
| ListExpiredLeases/1 | store SHARE (CTE) | — | — | read (no lock) | — | — | — |
| PublishPresence/1 | SHARE / SHARE | — | — | — | — | — | worker_instances upsert, worker_capabilities insert |
| ResolveCommand/1 | SHARE / SHARE | INSERT…DO NOTHING / FOR UPDATE, then ROLLBACK | — | — | — | — | — |
| Receipt cleanup (SP-021; harness) | SHARE / SHARE | DELETE … FOR UPDATE SKIP LOCKED | — | — | — | — | — |
| Store-gate change (migration validate/activate) | store FOR UPDATE | — | — | — | — | — | ledger |
| Application-gate change (harness; SetApplicationMode/1 not in M2) | store SHARE; app FOR UPDATE | yes | — | — | — | — | — |
| Not in M2: Cancel/1 | SHARE / SHARE | yes | — | FOR NO KEY UPDATE | active key only when a waiting job terminalizes | scopes only then | audit, event |
| Not in M2: ManualRetry/1 | SHARE / SHARE | yes | — | FOR NO KEY UPDATE | active key FOR UPDATE | if validation requires | new generation, audit |
| Not in M2: rate-aware Begin | SHARE / SHARE | yes | — | FOR NO KEY UPDATE | terminal active key | FOR UPDATE sorted | then rate scopes FOR UPDATE sorted, grants |

### 5.3 Implicit locks from foreign keys and why they cannot cycle

| Implicit lock | Taken by | Conflicts with | Why no cycle |
|---|---|---|---|
| KEY SHARE on `concurrency_scopes` row | Dispatch inserting `job_concurrency_requirements` (ascending scope_id); Reserve inserting permits | FOR UPDATE held by Reserve/Renew/Finish/Recover/Release | Dispatch waits holding only rank-6 keys and new rows; scope holders never wait for keys or for new jobs afterward |
| KEY SHARE on `job_concurrency_requirements` row | Reserve inserting permits | Nothing in M2 (requirements never updated/deleted) | — |
| KEY SHARE on `job_generations` row | Reserve inserting an attempt | Only DELETE/key update (none in M2); `started_count` updates take NO KEY UPDATE, which does not conflict | — |
| KEY SHARE on `job_attempts` row | Permit insert (own new attempt); deferred `jobs` FK at commit | Only DELETE (none in M2) | — |
| KEY SHARE on `jobs` row | Payload/generation/requirement inserts (own new job) | FOR UPDATE / DELETE (none on existing jobs in M2) | `FOR NO KEY UPDATE` on jobs does not conflict with KEY SHARE |
| Unique-index wait on `concurrency_scopes` natural key | Two dispatches provisioning the same scope | each other | Both insert in sorted (kind, key) order after sorted keys |
| Unique-index wait on `dispatch_keys` PK | Two dispatches creating the same key | each other | Both insert in one sorted pass before any other rank-6+ lock |

Forced-interleaving tests (PGX-04, ADM-03) remain required; written ordering is not a proof of deadlock freedom.

## 6. Transaction runner

### 6.1 Session and transaction setup

Every driver-owned operation: acquire a pooled connection from the operation's data source (§9), then:

```sql
BEGIN ISOLATION LEVEL READ COMMITTED;
SELECT set_config('lock_timeout', @lock_timeout_ms || 'ms', true),
       set_config('statement_timeout', @statement_timeout_ms || 'ms', true),
       set_config('idle_in_transaction_session_timeout', '10000ms', true);
```

`@lock_timeout_ms = min(2000, remaining)`, `@statement_timeout_ms = remaining`, where `remaining` is the operation's remaining local budget (03, 05). Statements whose inputs do not depend on prior results may be sent together in one `NpgsqlBatch`; a decision that depends on a lock result is never batched ahead of that lock.

Caller-owned staging (`StageAsync`/`StageManyAsync`) runs the same statements on the caller's `NpgsqlConnection`/`NpgsqlTransaction` without `BEGIN`, without `set_config` and without commit/rollback. Under `REPEATABLE READ`/`SERIALIZABLE` a lock conflict surfaces as `40001`; the driver reports it as not applied and the caller retries its whole transaction.

### 6.2 Gate statements (G1, G2)

```sql
-- G1
SELECT store_id, schema_revision, protocol_epoch, migration_mode,
       compatibility_manifest_version, manifest_digest, revision
FROM runwick.store_metadata
WHERE singleton_id = 1
FOR SHARE;

-- G2
SELECT revision, required_protocol_epoch, required_capabilities, admission_mode
FROM runwick.application_controls
WHERE application = @application
FOR SHARE;
```

The driver caches the parsed store manifest keyed by `(revision, manifest_digest)` and re-parses only when either changes; the gate *check* itself runs in every transaction (03 §10.1). A missing G2 row rejects with `application_not_registered` (except RegisterApplication). Many concurrent `FOR SHARE` lockers on one row create MultiXacts and write the tuple header; this cost is measured in PGX-07.

### 6.3 Receipt statements (R1–R4)

```sql
-- R1 placeholder
INSERT INTO runwick.storage_commands (
    application, retention_class, command_id, operation, operation_version, protocol_epoch,
    fingerprint, fingerprint_version, not_after, receipt_codec_version, retain_until)
VALUES (@application, @retention_class, @command_id, @operation, @operation_version, @protocol_epoch,
        @fingerprint, 1, @not_after, 1,
        @not_after + CASE @retention_class WHEN 'ownership' THEN interval '10 minutes'
                                            ELSE interval '24 hours' END)
ON CONFLICT DO NOTHING
RETURNING true AS inserted;

-- R2 existing row (only when R1 returned no row; waits for any in-flight holder)
SELECT operation, operation_version, fingerprint, fingerprint_version, not_after,
       status, result, decided_at
FROM runwick.storage_commands
WHERE application = @application
  AND retention_class = @retention_class
  AND command_id = @command_id
FOR UPDATE;

-- R3 cross-class conflict probe (when R1 inserted)
SELECT 1
FROM runwick.storage_commands
WHERE application = @application
  AND command_id = @command_id
  AND retention_class <> @retention_class
LIMIT 1;

-- R4 finalize (exactly one row must be updated)
UPDATE runwick.storage_commands
SET status = @status, result = @result, decided_at = @decided_at
WHERE application = @application
  AND retention_class = @retention_class
  AND command_id = @command_id
  AND result IS NULL;
```

Driver logic: R1 inserted → R3 (a row → `CommandConflict`, roll back). R1 not inserted → R2: operation/version/fingerprint differ → `CommandConflict`; `result` null → `InvariantFault receipt_incomplete`; otherwise return the recorded reply with `Replayed = true` and **roll back** (nothing new is written). Then `SELECT clock_timestamp()`: `≥ @not_after` → `CommandExpired`; `@not_after − now` above the class cap (60 s ownership operations, 10 min others) → `InvalidRequest deadline_too_far`. `@retention_class` is a pure function of `@operation` (03 §8.4); `@fingerprint` is the 02 §8.3 command digest; `@result` is receipt codec v1 (03 §8.5).

### 6.4 Decision time and finalization

After the last blocking lock:

```sql
SELECT clock_timestamp() AS now;
```

`now ≥ @not_after` → `CommandExpired`, roll back. Otherwise compute effects with `@now`, execute them, run R4 with `@decided_at = ms_floor(@now)`, and `COMMIT`. Every ObservedNoEffect decision issues `ROLLBACK`. Dispatch additionally re-reads `clock_timestamp()` immediately before R4 (its scope provisioning can wait after the decision point) and rolls back with `CommandExpired` if it is ≥ `@not_after`.

## 7. Operation SQL

Parameters prefixed `@`. "Gates" = §6.2; "Receipt" = §6.3 with the stated class; "Decide" = §6.4. Results use 03 §3 records.

### 7.1 RegisterApplication/1, RegisterQueue/1, RegisterScope/1

```sql
-- RegisterApplication: G1, then
INSERT INTO runwick.application_controls (application, created_at, updated_at)
VALUES (@application, runwick.ms_floor(clock_timestamp()), runwick.ms_floor(clock_timestamp()))
ON CONFLICT DO NOTHING
RETURNING true AS created;
-- then G2, Receipt (durable), Decide, R4, COMMIT.

-- RegisterQueue: Gates, Receipt (durable), then
INSERT INTO runwick.queue_controls (application, queue, created_at, updated_at)
SELECT @application, q.queue, runwick.ms_floor(clock_timestamp()), runwick.ms_floor(clock_timestamp())
FROM unnest(@queues::text[]) AS q(queue)
ORDER BY q.queue COLLATE "C"
ON CONFLICT DO NOTHING
RETURNING queue;                                   -- created queues
SELECT queue, paused, revision
FROM runwick.queue_controls
WHERE application = @application AND queue = ANY (@queues::text[]);
-- Decide, R4, COMMIT.

-- RegisterScope: Gates, Receipt (durable), then
INSERT INTO runwick.concurrency_scopes (
    application, scope_id, scope_kind, scope_key, capacity, fencing_enabled, created_at, updated_at)
SELECT @application, s.scope_id, s.kind, s.key, s.capacity,
       (s.kind = 'resource' AND s.capacity = 1),
       runwick.ms_floor(clock_timestamp()), runwick.ms_floor(clock_timestamp())
FROM unnest(@new_scope_ids::uuid[], @kinds::text[], @keys::text[], @capacities::integer[])
     AS s(scope_id, kind, key, capacity)
ORDER BY s.kind COLLATE "C", s.key COLLATE "C"
ON CONFLICT (application, scope_kind, scope_key) DO NOTHING
RETURNING scope_id;                                -- created scopes
SELECT c.scope_id, c.scope_kind, c.scope_key, c.capacity, c.policy_revision, c.fencing_enabled
FROM runwick.concurrency_scopes AS c
JOIN unnest(@kinds::text[], @keys::text[]) AS s(kind, key)
  ON c.scope_kind = s.kind AND c.scope_key = s.key
WHERE c.application = @application;
-- Decide, R4, COMMIT.
```

`@new_scope_ids` are client-generated v7 IDs used only if the row is created. Existing rows are never updated; a stored capacity different from the configured one is reported as drift in the result.

### 7.2 Dispatch/1 and DispatchMany/1 (and staging)

Dispatch/1 is DispatchMany/1 with one item and `ContinueOnConflict = false`; the receipt shape differs (03 §8.5). Steps:

1. **Gates; Receipt (durable).** Admission mode must be `open`.
2. **Queue registration check** (plain read):

```sql
SELECT queue
FROM runwick.queue_controls
WHERE application = @application AND queue = ANY (@queues::text[]);
```

   A missing queue → `InvalidRequest queue_not_registered`, roll back.

3. **Key rows**, all keys of all items, deduplicated and sorted once by (job_name, key_kind, key) bytewise:

```sql
INSERT INTO runwick.dispatch_keys (application, job_name, key_kind, key, created_at, updated_at)
SELECT @application, k.job_name, k.key_kind, k.key,
       runwick.ms_floor(clock_timestamp()), runwick.ms_floor(clock_timestamp())
FROM unnest(@key_job_names::text[], @key_kinds::text[], @keys::text[]) AS k(job_name, key_kind, key)
ORDER BY k.job_name COLLATE "C", k.key_kind COLLATE "C", k.key COLLATE "C"
ON CONFLICT DO NOTHING;

SELECT d.job_name, d.key_kind, d.key, d.bound_job_id,
       d.business_fingerprint, d.policy_fingerprint, d.fingerprint_version, d.expires_at
FROM runwick.dispatch_keys AS d
JOIN unnest(@key_job_names::text[], @key_kinds::text[], @keys::text[]) AS k(job_name, key_kind, key)
  ON d.job_name = k.job_name AND d.key_kind = k.key_kind AND d.key = k.key
WHERE d.application = @application
ORDER BY d.job_name, d.key_kind, d.key
FOR UPDATE OF d;
```

4. **Decide** (`@now`). No further blocking lock precedes the decision except scope provisioning (step 7), covered by the final deadline guard.
5. **Resolve items in ordinal order in memory**, applying each item's binding changes to the in-memory key state so later items of the same batch see them. For item i with active key A (optional) and window key W (optional):

| Case | Condition | Result | Key writes |
|---|---|---|---|
| a | W live (`bound_job_id` set and `expires_at > @now`) and `fingerprint_version ≠ 1` | `UnsupportedVersion fingerprint_version_unsupported` | none |
| b | W live, business digest differs | `UniquenessConflict window_fingerprint_mismatch`, `ConflictingJobId = W.bound` | none |
| c | W live, digest equal, A bound to a different job than W | `UniquenessConflict uniqueness_composition_conflict` | none |
| d | W live, digest equal | `Reused(W.bound)`, match `ActiveAndWindow` if A is bound to W.bound else `Window`; `PolicyDrift = policy digest differs` | none (an unbound A is not reclaimed) |
| e | W absent or not live; A bound | `Reused(A.bound)`, match `Active`, `PolicyDrift = false` | if W given: bind W → A.bound with the **incoming** digests, `expires_at = ms_floor(@now + window)` |
| f | otherwise | `Created(ProposedJobId)`, match `None` | bind A → new job; bind W → new job with digests and expiry |

   Conflicts (a–c): without `ContinueOnConflict` the whole command returns that status and rolls back; with it, the item reports its failure and writes nothing.

6. **Key writes** (rows already locked):

```sql
UPDATE runwick.dispatch_keys
SET bound_job_id = @bound_job_id,
    business_fingerprint = @business_fingerprint,   -- null for active
    policy_fingerprint = @policy_fingerprint,       -- null for active
    fingerprint_version = @fingerprint_version,     -- null for active
    expires_at = @expires_at,                        -- null for active
    updated_at = runwick.ms_floor(@now)
WHERE application = @application
  AND job_name = @job_name AND key_kind = @key_kind AND key = @key;
```

7. **New jobs** (case f items), in ascending `job_id`:

```sql
INSERT INTO runwick.jobs (
    application, job_id, job_name, payload_version, serializer_profile_id, serializer_profile_version,
    queue, priority, state, eligible_at, created_at, updated_at, active_unique_key)
SELECT @application, n.job_id, n.job_name, n.payload_version, n.profile_id, n.profile_version,
       n.queue, n.priority, 'pending',
       runwick.ms_ceil(GREATEST(COALESCE(n.not_before, @now), @now)),
       runwick.ms_floor(@now), runwick.ms_floor(@now), n.active_key
FROM unnest(@job_ids::uuid[], @job_names::text[], @payload_versions::integer[], @profile_ids::text[],
            @profile_versions::integer[], @job_queues::text[], @priorities::text[],
            @not_befores::timestamptz[], @active_keys::text[])
     AS n(job_id, job_name, payload_version, profile_id, profile_version, queue, priority, not_before, active_key)
ORDER BY n.job_id;

INSERT INTO runwick.job_payloads (
    application, job_id, payload_utf8, payload_encoding_version,
    serializer_profile_id, serializer_profile_version, headers)
SELECT @application, p.job_id, p.payload, 1, p.profile_id, p.profile_version, p.headers
FROM unnest(@job_ids::uuid[], @payloads::bytea[], @profile_ids::text[], @profile_versions::integer[],
            @headers::jsonb[]) AS p(job_id, payload, profile_id, profile_version, headers)
ORDER BY p.job_id;

INSERT INTO runwick.job_generations (
    application, job_id, generation, created_at, created_reason,
    max_executions, max_unstarted_abandonments, execution_timeout_ms,
    retry_policy, unstarted_retry_policy)
SELECT @application, g.job_id, 1, runwick.ms_floor(@now), 'initial',
       g.max_executions, g.max_unstarted, g.timeout_ms, g.retry, g.unstarted_retry
FROM unnest(@job_ids::uuid[], @max_executions::integer[], @max_unstarted::integer[],
            @timeouts_ms::bigint[], @retry_policies::jsonb[], @unstarted_policies::jsonb[])
     AS g(job_id, max_executions, max_unstarted, timeout_ms, retry, unstarted_retry)
ORDER BY g.job_id;
```

   `headers` = `{"business": {…}, "diagnostic": {…}}`. A `23505` on `jobs_pk` → `InvariantFault id_collision`.

8. **Scope provisioning**, family by family (only concurrency scopes in M2): all distinct `(kind, key, capacity)` of the created jobs, inserted with the RegisterScope statement (§7.1) in sorted (kind, key) order, then resolved to IDs without row locks; then requirements in ascending `scope_id`:

```sql
INSERT INTO runwick.job_concurrency_requirements (application, job_id, scope_id)
SELECT @application, r.job_id, r.scope_id
FROM unnest(@req_job_ids::uuid[], @req_scope_ids::uuid[]) AS r(job_id, scope_id)
ORDER BY r.scope_id, r.job_id;
```

   Provisioning never updates an existing scope and never acquires jobs or keys afterward.

9. **Events**: one `dispatched` per created job (`details`: `{"generation":1,"queue":…,"priority":…,"ordinal":i}`), one `policy_drift` per case-d item with drift (`job_id` = reused job; `details`: `{"ordinal":i,"match":…}`):

```sql
INSERT INTO runwick.queue_events (application, job_id, command_id, event_kind, occurred_at, details)
SELECT @application, e.job_id, @command_id, e.kind, runwick.ms_floor(@now), e.details
FROM unnest(@event_job_ids::uuid[], @event_kinds::text[], @event_details::jsonb[]) AS e(job_id, kind, details);
```

10. **Final deadline guard** (§6.4), R4, `COMMIT` (staging: return without commit).

### 7.3 Reserve/1

```sql
-- Gates; Receipt (ownership). Admission mode must be open.

-- Q: queue control
SELECT paused, revision
FROM runwick.queue_controls
WHERE application = @application AND queue = @queue
FOR SHARE;
-- no row → NoWork queue_not_registered; paused → QueuePaused queue_paused (ROLLBACK)

-- C: candidate (one row, never waits on another claimant)
SELECT j.job_id, j.eligible_at, j.generation, j.revision, j.reservation_count,
       j.job_name, j.payload_version, j.serializer_profile_id, j.serializer_profile_version,
       j.queue, j.priority
FROM runwick.jobs AS j
WHERE j.application = @application
  AND j.queue = @queue
  AND j.priority = @priority
  AND j.state IN ('pending', 'retrying')
  AND j.eligible_at <= statement_timestamp()
  AND (@after_job_id::uuid IS NULL
       OR (j.eligible_at, j.job_id) > (@after_eligible_at::timestamptz, @after_job_id::uuid))
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
FOR NO KEY UPDATE OF j SKIP LOCKED;
-- no row → NoWork no_work (ROLLBACK)

-- G: generation policy and counters (dependent of the locked job)
SELECT max_executions, max_unstarted_abandonments, execution_timeout_ms,
       retry_policy, unstarted_retry_policy, started_count, consecutive_unstarted_count
FROM runwick.job_generations
WHERE application = @application AND job_id = @job_id AND generation = @generation;

-- S: required scopes, locked in UUID order
SELECT s.scope_id, s.scope_kind, s.capacity, s.fencing_enabled
FROM runwick.concurrency_scopes AS s
WHERE s.application = @application
  AND s.scope_id IN (SELECT r.scope_id
                     FROM runwick.job_concurrency_requirements AS r
                     WHERE r.application = @application AND r.job_id = @job_id)
ORDER BY s.scope_id
FOR UPDATE OF s;

-- Decide: @now = clock_timestamp(); deadline check.

-- P: live permit counts at @now (fresh statement after the locks)
SELECT r.scope_id, count(p.attempt_id) AS live
FROM unnest(@scope_ids::uuid[]) AS r(scope_id)
LEFT JOIN runwick.concurrency_permits AS p
  ON p.application = @application
 AND p.scope_id = r.scope_id
 AND p.lease_expires_at > @now
GROUP BY r.scope_id
ORDER BY r.scope_id;
-- any live >= capacity → CapacityBlocked capacity_blocked,
--   BlockedScopeId = first blocked scope_id (UUID order), ResumeAfter = (eligible_at, job_id); ROLLBACK
```

Effects (`@expiry = ms_floor(@now + @lease_duration)`, `@reservation_number = reservation_count + 1`, `@attempt_id = v7`, `@token = random`):

```sql
UPDATE runwick.concurrency_scopes
SET fencing_counter = fencing_counter + 1
WHERE application = @application
  AND fencing_enabled
  AND scope_id = ANY (@scope_ids::uuid[])
RETURNING scope_id, fencing_counter;                     -- fences

INSERT INTO runwick.job_attempts (
    application, attempt_id, job_id, generation, reservation_number,
    worker_instance_id, ownership_token, reserved_at)
VALUES (@application, @attempt_id, @job_id, @generation, @reservation_number,
        @worker_instance_id, @token, runwick.ms_floor(@now));

UPDATE runwick.jobs
SET state = 'reserved',
    reservation_count = reservation_count + 1,
    revision = revision + 1,
    current_attempt_id = @attempt_id,
    worker_instance_id = @worker_instance_id,
    ownership_token = @token,
    lease_expires_at = @expiry,
    updated_at = runwick.ms_floor(@now)
WHERE application = @application AND job_id = @job_id
  AND state IN ('pending', 'retrying')
RETURNING revision;                                      -- exactly one row

INSERT INTO runwick.concurrency_permits (
    application, scope_id, attempt_id, job_id, ownership_token, lease_expires_at, fencing_value)
SELECT @application, p.scope_id, @attempt_id, @job_id, @token, @expiry, p.fence
FROM unnest(@scope_ids::uuid[], @fences::bigint[]) AS p(scope_id, fence)
ORDER BY p.scope_id;

INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, details)
VALUES (@application, @job_id, @attempt_id, @command_id, 'attempt_reserved', runwick.ms_floor(@now),
        jsonb_build_object('generation', @generation, 'reservationNumber', @reservation_number));

SELECT payload_utf8, headers
FROM runwick.job_payloads
WHERE application = @application AND job_id = @job_id;   -- must exist for a waiting job
-- R4 (Reserve receipt, 03 §8.5), COMMIT; only then return ReservedJob.
```

The capability arrays carry 1–512 deduplicated tuples with equal lengths and non-null values (validated before the transaction). After CapacityBlocked the next Reserve is a new command with `After = ResumeAfter`.

### 7.4 ReadinessProbe/1

One statement, one round trip, no reservation locks, no payload:

```sql
WITH gate AS (
    SELECT s.store_id, s.schema_revision, s.protocol_epoch, s.migration_mode, s.manifest_digest,
           clock_timestamp() AS observed_at
    FROM runwick.store_metadata AS s
    WHERE s.singleton_id = 1
    FOR SHARE
)
SELECT g.store_id, g.schema_revision, g.protocol_epoch, g.migration_mode, g.manifest_digest,
       g.observed_at, r.queue, r.priority, r.oldest_eligible_at
FROM gate AS g
LEFT JOIN LATERAL (
    SELECT l.queue, l.priority, c.eligible_at AS oldest_eligible_at
    FROM unnest(@lane_queues::text[], @lane_priorities::text[]) AS l(queue, priority)
    JOIN runwick.queue_controls AS q
      ON q.application = @application AND q.queue = l.queue AND NOT q.paused
    CROSS JOIN LATERAL (
        SELECT j.eligible_at
        FROM runwick.jobs AS j
        WHERE j.application = @application
          AND j.queue = l.queue
          AND j.priority = l.priority
          AND j.state IN ('pending', 'retrying')
          AND j.eligible_at <= g.observed_at
          AND EXISTS (
              SELECT 1
              FROM unnest(@names::text[], @versions::integer[],
                          @profile_ids::text[], @profile_versions::integer[])
                   AS t(name, version, profile_id, profile_version)
              WHERE t.name = j.job_name AND t.version = j.payload_version
                AND t.profile_id = j.serializer_profile_id
                AND t.profile_version = j.serializer_profile_version)
        ORDER BY j.eligible_at, j.job_id
        LIMIT 1
    ) AS c
) AS r ON true;
```

The driver validates the gate columns (03 §10.1, read-only) and returns `ReadySnapshot(observed_at, lanes with non-null queue)`. It performs at most one indexed `jobs_ready` lookup per requested lane (1–384 lanes) and may scan past unsupported rows; it is a hint, never authority.

### 7.5 Begin/1

```sql
-- Gates (mode open); Receipt (ownership).

-- J
SELECT state, generation, revision, current_attempt_id, worker_instance_id, ownership_token,
       lease_expires_at, cancel_requested_at, active_unique_key, job_name, terminal_at
FROM runwick.jobs
WHERE application = @application AND job_id = @job_id
FOR NO KEY UPDATE;

-- G
SELECT started_count, max_executions
FROM runwick.job_generations
WHERE application = @application AND job_id = @job_id AND generation = @generation;
```

Plan: `cancel_requested_at IS NOT NULL` → *cancel*; else `started_count ≥ max_executions` → *exhausted*; else *authorize*. For *cancel*/*exhausted* lock the active key (§7.10 K) when `active_unique_key` is set, then the scopes (§7.10 S). Decide, then the ownership check (03 §6.6, state `reserved`). Effects for *authorize*:

```sql
UPDATE runwick.job_generations
SET started_count = started_count + 1,
    consecutive_unstarted_count = 0
WHERE application = @application AND job_id = @job_id AND generation = @generation
  AND started_count < max_executions
RETURNING started_count;                                  -- ExecutionNumber

UPDATE runwick.jobs
SET state = 'running',
    execution_count = execution_count + 1,
    revision = revision + 1,
    updated_at = runwick.ms_floor(@now)
WHERE application = @application AND job_id = @job_id
  AND state = 'reserved'
  AND current_attempt_id = @attempt_id AND ownership_token = @token
RETURNING execution_count, revision, lease_expires_at;    -- TotalExecutionNumber; lease unchanged

UPDATE runwick.job_attempts
SET execution_number = @execution_number,
    authorized_at = runwick.ms_floor(@now)
WHERE application = @application AND attempt_id = @attempt_id
  AND finished_at IS NULL AND execution_number IS NULL;

INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, details)
VALUES (@application, @job_id, @attempt_id, @command_id, 'attempt_started', runwick.ms_floor(@now),
        jsonb_build_object('generation', @generation, 'executionNumber', @execution_number));
-- R4, COMMIT.
```

*cancel* and *exhausted* use the close-and-terminalize statements of §7.10 with: *cancel* → attempt outcome `cancelled_before_start`, disposition `cancelled`, job `cancelled`/`cancelled_before_start`, event `attempt_cancelled_before_start`, reply InvalidState (Applied); *exhausted* → outcome `abandoned_before_start`, disposition `failed`, job `failed`/`execution_budget_exhausted`, event `attempt_finished`, reply BudgetExhausted (Applied). Rate admission is not in M2.

### 7.6 Renew/1

No receipt (R1). Gates (allowed in `open` and `drain`).

```sql
-- J
SELECT state, generation, current_attempt_id, worker_instance_id, ownership_token,
       lease_expires_at, cancel_requested_at, terminal_at
FROM runwick.jobs
WHERE application = @application AND job_id = @job_id
FOR NO KEY UPDATE;

-- S: required scopes in UUID order
SELECT s.scope_id
FROM runwick.concurrency_scopes AS s
WHERE s.application = @application
  AND s.scope_id IN (SELECT r.scope_id
                     FROM runwick.job_concurrency_requirements AS r
                     WHERE r.application = @application AND r.job_id = @job_id)
ORDER BY s.scope_id
FOR UPDATE OF s;
```

Decide: `@now = clock_timestamp()`; `@now ≥ @not_after` → CommandExpired; ownership check (states `reserved`/`running`). `@new_expiry = ms_floor(@now + @lease_duration)`:

```sql
UPDATE runwick.jobs
SET lease_expires_at = @new_expiry
WHERE application = @application AND job_id = @job_id
  AND current_attempt_id = @attempt_id
  AND ownership_token = @token
  AND worker_instance_id = @worker_id
  AND generation = @generation
  AND state IN ('reserved', 'running')
  AND lease_expires_at > @now;
-- 0 rows → StaleLease (never success)

UPDATE runwick.concurrency_permits
SET lease_expires_at = @new_expiry
WHERE application = @application
  AND job_id = @job_id
  AND attempt_id = @attempt_id
  AND ownership_token = @token
RETURNING scope_id;
-- returned set must equal the required scope set from S exactly;
-- otherwise ROLLBACK and InvariantFault permit_set_mismatch
COMMIT;
```

Renew never acquires a permit, never writes an event and never changes `revision`. The reply is `RenewedLease(LeaseGrant(handle, @new_expiry), cancel_requested_at IS NOT NULL)` with `DecidedAt = ms_floor(@now)`.

### 7.7 Finish/1

```sql
-- Gates (open/drain); Receipt (durable); J as §7.5; G:
SELECT started_count, max_executions, retry_policy
FROM runwick.job_generations
WHERE application = @application AND job_id = @job_id AND generation = @generation;
```

Compute the disposition with the 03 §6.4 precedence table from `@outcome`, `cancel_requested_at` and the budget; when terminal and `active_unique_key` is set, lock K; lock S; Decide; ownership check (state `running`). For `retrying`, sample the delay (03 §6.2; `n = started_count`) and set `@eligible_at = ms_ceil(@now + delay)`. Then the §7.10 close statements with outcome = `@outcome` in snake_case, `execution_elapsed_ms = @elapsed_ms`, `error = @error`, event `attempt_finished`. R4, COMMIT.

### 7.8 Recover/1

```sql
-- Gates (open/drain); Receipt (ownership); J as §7.5 (includes started_count via G below).
SELECT started_count, max_executions, consecutive_unstarted_count, max_unstarted_abandonments,
       retry_policy, unstarted_retry_policy
FROM runwick.job_generations
WHERE application = @application AND job_id = @job_id AND generation = @generation;
```

Require `current_attempt_id = @expected_attempt_id` and `ownership_token = @expected_token` (else StaleLease `stale_lease`). Compute the 03 §6.5 result; lock K when terminal; lock S; Decide; require `lease_expires_at ≤ @now` (else StaleLease `lease_renewed`). For a reserved expiry without cancellation:

```sql
UPDATE runwick.job_generations
SET consecutive_unstarted_count = consecutive_unstarted_count + 1
WHERE application = @application AND job_id = @job_id AND generation = @generation
RETURNING consecutive_unstarted_count;                  -- k
```

Then the §7.10 close statements with outcome `abandoned_before_start` (reserved) or `abandoned_running` (running), event `attempt_recovered`, and the computed disposition/eligibility. R4, COMMIT.

### 7.9 ReleaseReservation/1

```sql
-- Gates (open/drain); Receipt (ownership); J as §7.5; G:
SELECT started_count
FROM runwick.job_generations
WHERE application = @application AND job_id = @job_id AND generation = @generation;
```

With `cancel_requested_at` set: lock K (if any), lock S, Decide, ownership check (state `reserved`), then §7.10 with outcome `released`, disposition `cancelled`, job `cancelled`/`cancelled_before_start`. Otherwise: lock S, Decide, ownership check, then §7.10 with outcome `released`, disposition `pending` (`started_count = 0`) or `retrying`, **`eligible_at` unchanged**, no counter change. Event `attempt_released`. R4, COMMIT.

### 7.10 Shared close-and-transition statements

```sql
-- K: active key (only when the transition is terminal and jobs.active_unique_key is not null)
SELECT bound_job_id
FROM runwick.dispatch_keys
WHERE application = @application AND job_name = @job_name
  AND key_kind = 'active' AND key = @active_key
FOR UPDATE;

-- S: required scopes in UUID order (same statement as §7.6 S)

-- A: close the attempt (exactly one row)
UPDATE runwick.job_attempts
SET finished_at = runwick.ms_floor(@now),
    outcome = @attempt_outcome,
    disposition = @disposition,
    applied_reason = @terminal_reason,            -- null unless terminal
    execution_elapsed_ms = @elapsed_ms,           -- Finish only
    retry_delay_ms = @retry_delay_ms,             -- null unless a delay was chosen
    next_eligible_at = @next_eligible_at,         -- null unless a delay was chosen
    error = @error,                               -- Finish only
    closed_by_command_id = @command_id
WHERE application = @application AND attempt_id = @attempt_id
  AND job_id = @job_id AND finished_at IS NULL;

-- P: release exactly this attempt's permits
DELETE FROM runwick.concurrency_permits
WHERE application = @application
  AND job_id = @job_id
  AND attempt_id = @attempt_id
  AND ownership_token = @token;

-- J': job transition (exactly one row)
UPDATE runwick.jobs
SET state = @new_state,
    eligible_at = COALESCE(@new_eligible_at, eligible_at),
    current_attempt_id = NULL,
    worker_instance_id = NULL,
    ownership_token = NULL,
    lease_expires_at = NULL,
    terminal_at = CASE WHEN @new_state IN ('succeeded', 'failed', 'cancelled')
                       THEN runwick.ms_floor(@now) END,
    terminal_reason = @terminal_reason,
    revision = revision + 1,
    updated_at = runwick.ms_floor(@now)
WHERE application = @application AND job_id = @job_id
  AND current_attempt_id = @attempt_id AND ownership_token = @token;

-- K': release the active binding only if it still names this job (terminal transitions only)
UPDATE runwick.dispatch_keys
SET bound_job_id = NULL,
    updated_at = runwick.ms_floor(@now)
WHERE application = @application AND job_name = @job_name
  AND key_kind = 'active' AND key = @active_key
  AND bound_job_id = @job_id;

-- E: event
INSERT INTO runwick.queue_events (application, job_id, attempt_id, command_id, event_kind, occurred_at, details)
VALUES (@application, @job_id, @attempt_id, @command_id, @event_kind, runwick.ms_floor(@now), @event_details);
```

Statement order A → P → J' → K' → E keeps every write behind locks already held. Row counts other than those stated roll back with `InvariantFault invalid_transition`. Fixed-window keys are never touched.

### 7.11 ListExpiredLeases/1

```sql
WITH gate AS (
    SELECT s.store_id, s.schema_revision, s.migration_mode, clock_timestamp() AS observed_at
    FROM runwick.store_metadata AS s
    WHERE s.singleton_id = 1
    FOR SHARE
)
SELECT g.store_id, g.schema_revision, g.migration_mode, g.observed_at,
       e.job_id, e.current_attempt_id, e.ownership_token, e.state, e.lease_expires_at
FROM gate AS g
LEFT JOIN LATERAL (
    SELECT j.job_id, j.current_attempt_id, j.ownership_token, j.state, j.lease_expires_at
    FROM runwick.jobs AS j
    WHERE j.application = @application
      AND j.state IN ('reserved', 'running')
      AND j.lease_expires_at <= g.observed_at
    ORDER BY j.lease_expires_at, j.job_id
    LIMIT @limit                                   -- 1–100; the worker uses 32
) AS e ON true;
```

Each candidate is processed by its own Recover/1 command, which rechecks everything under locks.

### 7.12 PublishPresence/1

Gates (allowed in `open` and `drain`), no receipt, `@now = clock_timestamp()`:

```sql
INSERT INTO runwick.worker_instances AS w (
    application, instance_id, process_id, host_name, build_id, status,
    capability_revision, presence_sequence, subscriptions, pool_targets,
    started_at, heartbeat_at, expires_at)
VALUES (@application, @instance_id, @process_id, @host_name, @build_id, @status,
        @capability_revision, @presence_sequence, @subscriptions, @pool_targets,
        runwick.ms_floor(@now), runwick.ms_floor(@now), runwick.ms_floor(@now + @expiry))
ON CONFLICT (application, instance_id) DO UPDATE
SET status = EXCLUDED.status,
    capability_revision = EXCLUDED.capability_revision,
    presence_sequence = EXCLUDED.presence_sequence,
    subscriptions = EXCLUDED.subscriptions,
    pool_targets = EXCLUDED.pool_targets,
    heartbeat_at = EXCLUDED.heartbeat_at,
    expires_at = EXCLUDED.expires_at
WHERE w.presence_sequence < EXCLUDED.presence_sequence
RETURNING w.expires_at;                            -- no row → older sequence, Accepted = false

INSERT INTO runwick.worker_capabilities (
    application, instance_id, capability_revision,
    job_name, payload_version, serializer_profile_id, serializer_profile_version)
SELECT @application, @instance_id, @capability_revision,
       c.name, c.version, c.profile_id, c.profile_version
FROM unnest(@names::text[], @versions::integer[], @profile_ids::text[], @profile_versions::integer[])
     AS c(name, version, profile_id, profile_version)
ON CONFLICT DO NOTHING;
COMMIT;
```

The capability insert is sent until one publication of that revision has been confirmed (it is idempotent). The complete revision and its presence row commit together.

### 7.13 ResolveCommand/1

Gates; R1 with the caller's `CommandIdentity`. If R1 inserted, the command has no receipt: `SELECT clock_timestamp()` and classify `NotObserved` / `NotApplied` / `ExpiredUnknown` by 03 §8.6. If R1 did not insert, R2 (waits for an in-flight holder) and classify `Recorded` / `Conflict`. Always `ROLLBACK`; resolution never writes.

### 7.14 SetQueueState/1 (harness only in M2)

```sql
-- Gates; Receipt (durable)
SELECT paused, revision
FROM runwick.queue_controls
WHERE application = @application AND queue = @queue
FOR UPDATE;
-- Decide; expected revision check
UPDATE runwick.queue_controls
SET paused = @paused, revision = revision + 1,
    updated_at = runwick.ms_floor(@now), updated_by = @actor
WHERE application = @application AND queue = @queue;
-- R4, COMMIT
```

### 7.15 Application-gate change (harness only in M2)

```sql
SELECT revision FROM runwick.store_metadata WHERE singleton_id = 1 FOR SHARE;
SELECT revision, admission_mode, required_capabilities
FROM runwick.application_controls
WHERE application = @application
FOR UPDATE;                                        -- waits for every in-flight FOR SHARE holder
UPDATE runwick.application_controls
SET admission_mode = @mode, required_capabilities = @required_capabilities,
    revision = revision + 1, updated_at = runwick.ms_floor(clock_timestamp()), actor_subject = @actor
WHERE application = @application AND revision = @expected_revision;
COMMIT;
```

Runs on the migration/admin role (the runtime role has no UPDATE on `application_controls`). Transactions that started before it commit under the old mode; later ones observe the new mode (PGX-01).

### 7.16 Receipt cleanup (SP-021; G-1 harness `PruneReceipts`)

```sql
-- Gates, then per class:
DELETE FROM runwick.storage_commands AS c
USING (
    SELECT s.application, s.retention_class, s.command_id
    FROM runwick.storage_commands AS s
    WHERE s.application = @application
      AND s.retention_class = @retention_class
      AND s.retain_until < clock_timestamp()
    ORDER BY s.retain_until, s.command_id
    LIMIT @batch                                   -- ≤ 1000
    FOR UPDATE SKIP LOCKED
) AS d
WHERE c.application = d.application
  AND c.retention_class = d.retention_class
  AND c.command_id = d.command_id;
```

An uncommitted placeholder is invisible and cannot be deleted; a row locked by a replaying transaction is skipped.

## 8. Migrations

### 8.1 Runner

- Explicit deployment command/API (and the development initializer calling the same runner); never worker startup or a dashboard request.
- Dedicated **physical** session to the primary with `Pooling=false`, connected as the migration role. Session advisory lock `pg_try_advisory_lock(5941776742533524273)` polled 250 ms → 2 s for at most 60 s (P-11); failure → `migration_lock_unavailable`. The lock is held across all phases, released with `pg_advisory_unlock` and the session closed; a session holding it is never returned to a pool. Losing the session aborts further migration work.
- Each phase uses `lock_timeout = 5 s` and `statement_timeout = 60 s` (`SET LOCAL`). The runner never terminates other sessions. A caller transaction holding a gate or table lock makes the phase fail with diagnostics; the store stays in its last recorded mode (PGX-08, UPG-03).
- Migration files are embedded resources `NNNN_name.sql`. **Checksum** = SHA-256 of the file's UTF-8 bytes after normalizing line endings to LF; recorded per phase in `schema_migrations.checksum`. An applied migration whose checksum differs from the binary's is a hard error `migration_checksum_mismatch`; a released migration body is never changed under the same ID.

### 8.2 Phases for `0001_initial`

| Phase | Transaction | Actions | Ledger |
|---|---|---|---|
| preflight | read-only | `server_version_num ≥ 160000`; `pg_database_encoding` UTF8; `standard_conforming_strings = on`; acquire advisory lock; if schema `runwick` exists: it must contain `schema_migrations`, otherwise abort `schema_not_empty`; read ledger; compare checksums; compute plan (skip completed phases) | recorded with expand |
| expand | one transaction | execute `0001_initial.sql` (§4); then insert ledger rows `('0001_initial','preflight',…,'applied')` and `('0001_initial','expand',…,'applied')` | `applied` in the same transaction |
| backfill | — | none for 0001 | — |
| validate | one transaction | `SELECT … FROM runwick.store_metadata WHERE singleton_id = 1 FOR UPDATE`; verify catalog (§8.3); `UPDATE runwick.store_metadata SET schema_revision = 1, migration_mode = 'normal', compatibility_manifest = @manifest, compatibility_manifest_version = 1, manifest_digest = @digest, revision = revision + 1, updated_at = runwick.ms_floor(clock_timestamp())` | `validate` `applied` in the same transaction |
| activate | one transaction | verify `schema_revision = 1` and `migration_mode = 'normal'`; 0001 activates no application capability (applications are seeded at runtime by RegisterApplication/1) | `activate` `applied` |
| contract | — | none for 0001 | — |

Failure handling: a failed transactional phase rolls back; the runner records `state = 'failed'` with `diagnostic_code` (SQLSTATE or runner code) in a separate short transaction when the ledger exists (for a failed 0001 expand it does not — nothing exists and the phase is simply retried). A committed phase whose reply was lost is detected from the ledger and catalog on the next run; no new migration identity is created. Re-running a fully applied migration is a no-op. The store remains `maintenance` (runtime rejects with `store_maintenance`) until validate commits.

### 8.3 Validation queries

```sql
-- every expected relation exists in schema runwick and is owned by the migration role
SELECT c.relname, c.relkind, pg_get_userbyid(c.relowner) AS owner
FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = 'runwick';

-- no invalid or not-ready index
SELECT i.indexrelid::regclass
FROM pg_index AS i
JOIN pg_class AS c ON c.oid = i.indexrelid
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = 'runwick' AND (NOT i.indisvalid OR NOT i.indisready);

-- every constraint validated; deferrable ones as declared
SELECT conname, contype, convalidated, condeferrable, condeferred
FROM pg_constraint AS k JOIN pg_namespace AS n ON n.oid = k.connamespace
WHERE n.nspname = 'runwick';

-- both receipt triggers present and enabled
SELECT tgname, tgrelid::regclass, tgenabled, tgdeferrable, tginitdeferred
FROM pg_trigger
WHERE tgname IN ('storage_commands_ownership_decided', 'storage_commands_durable_decided');
```

The runner compares results with the expected object list compiled into the binary (names, kinds, constraint names, `jobs_current_generation_fk`/`jobs_current_attempt_fk` deferrable initially deferred). Any difference → `schema_validation_failed`; schema revision is not advanced.

### 8.4 Later migrations

Expand → backfill (bounded keyset batches with checkpoints in `schema_migrations.checkpoint`) → validate → drain (application gate `drain` under its exclusive row lock, if semantics change) → activate (required capabilities/revision with receipt/audit; reopen) → contract (a later reviewed migration). `CREATE INDEX CONCURRENTLY` runs outside a transaction block as its own ledger phase and is verified with `indisvalid` before continuing; a blind `IF NOT EXISTS` retry is forbidden. FK/CHECK additions on large tables use `NOT VALID` then `VALIDATE CONSTRAINT`. Schema revision advances only after validated phases. Rollback to an older binary is allowed only if its manifest accepts the installed schema and active requirements.

### 8.5 Compatibility manifest (store, version 1)

Written by validate; digest = SHA-256 over `ASCII("runwick.manifest.v1") || 0x00 || Encode(manifest)` with the 02 §8 canonical encoder.

```json
{
  "manifestVersion": 1,
  "schemaRevision": 1,
  "protocolEpochs": [1],
  "receiptCodecs": [1],
  "commandFingerprintVersions": [1],
  "windowFingerprintVersions": [1],
  "retryEvaluators": ["exponential-v1"],
  "operations": {
    "Describe": [1], "RegisterApplication": [1], "RegisterQueue": [1], "RegisterScope": [1],
    "Dispatch": [1], "DispatchMany": [1], "ResolveCommand": [1],
    "ReadinessProbe": [1], "PublishPresence": [1], "ListExpiredLeases": [1],
    "Reserve": [1], "Begin": [1], "Renew": [1], "Finish": [1], "Recover": [1],
    "ReleaseReservation": [1], "SetQueueState": [1]
  },
  "expandCompatibleBinaries": []
}
```

A binary declares readable/writable schema revisions `[1]`, epochs `[1]` and the operations it uses; the gate accepts it when every used operation/version is listed and the schema revision and epoch match (03 §10.1).

### 8.6 Records added by later migrations

Rate tables, `management_audit`, outbox, scheduler, retention and pool records (§3) arrive as later numbered migrations with their own manifests and capability activation (e.g. `rate_admission` via `ActivateCapabilities/1`); none is created by 0001.

## 9. Connections and roles

### 9.1 Data sources per worker/store group

| Data source / class | Max connections or calls | Operations |
|---|---|---|
| **Control** data source | **6** connections, minimum 0 | Ownership operations only |
| — renewal lane | 4 concurrent calls | Renew (earliest local safety deadline first) |
| — settlement lane | 2 concurrent calls | Finish, ReleaseReservation and their resolution |
| **Work** data source | **8** connections, minimum 0 | Everything else |
| — admission class | 4 concurrent calls | Reserve, Begin and their resolution |
| — producer/admin class | 2 concurrent calls | Dispatch/DispatchMany, Register*, ResolveCommand for dispatch |
| — background class | 2 concurrent calls (heavy work ≤ 1) | ReadinessProbe, PublishPresence, ListExpiredLeases, Recover, receipt cleanup (SP-021) |
| **Migration** session | 1 dedicated physical session per runner, `Pooling=false` | Migration runner only; not counted in worker budgets |
| Listener | — | Not in M2 |
| Caller transaction | caller's connection | StageAsync/StageManyAsync (caller's budget) |

Class ceilings are partitions of the data-source maximum, not additions. No borrowing between renewal and settlement. A call keeps its slot until the provider task has ended and the connection is disposed, even after its local deadline. Producer-only processes create no control data source. Planning ceiling per worker group: 6 + 8 = 14 connections (+1 listener later); six workers + one runner = 6 × 14 + 1 = 85 (91 once listeners exist).

### 9.2 Npgsql settings (driver-owned data sources)

| Setting | Value |
|---|---|
| `Enlist` | `false` (ambient `TransactionScope` never captures worker transitions) |
| `Pooling` | `true` (control/work); `false` (migration) |
| `Minimum Pool Size` / `Maximum Pool Size` | 2 / 6 (control; 2 is the settlement lane width), 0 / 8 (work) (2026-11-01, PF-03) |
| `Multiplexing` | `false` |
| `Max Auto Prepare` | `64`, at Npgsql's default of 2 usages (2026-11-01, PF-03; `0` until then). The migration connection stays at `0`. |
| `Options` | `-c plan_cache_mode=force_custom_plan` appended to any configured options (2026-11-01, PF-03) |
| `Include Error Detail` | `false`; parameter logging disabled |
| `Application Name` | `runwick:<application>:control` / `:work` / `:migrate` |
| `Timeout` (connect) | ≤ remaining operation budget, default 5 s |
| `Command Timeout` | `0` at the provider; per-statement bounds come from `statement_timeout` and the operation's cancellation token |
| `Cancellation Timeout` | 2000 ms |
| `Keepalive` | 15 s |
| `Target Session Attributes` | `primary` when multiple hosts are configured |
| `Server Compatibility Mode` | default |

Host-supplied `NpgsqlDataSource` instances are accepted only if these settings are compatible (validated at startup); Runwick never disposes a caller-owned data source. Parameters use explicit types; `timestamptz` values are UTC `DateTime`/`DateTimeOffset` (no legacy timestamp switch).

### 9.3 Database roles and privileges

| Object | Migration role | Runtime role |
|---|---|---|
| schema `runwick` | owner | `USAGE` |
| `store_metadata`, `schema_migrations` | owner | `SELECT` |
| `application_controls` | owner | `SELECT`, `INSERT` |
| `queue_controls` | owner | `SELECT`, `INSERT`, `UPDATE` (SetQueueState is harness/M3) |
| `jobs`, `job_payloads`, `job_generations`, `job_attempts`, `concurrency_scopes`, `job_concurrency_requirements`, `concurrency_permits`, `dispatch_keys`, `queue_events`, `worker_instances`, `worker_capabilities` | owner | `SELECT`, `INSERT`, `UPDATE`, `DELETE` |
| `storage_commands` (and partitions) | owner | `SELECT`, `INSERT`, `UPDATE`, `DELETE` |
| `queue_events_event_id_seq` | owner | `USAGE` |
| functions `ms_floor`, `ms_ceil`, `storage_commands_assert_decided` | owner | `EXECUTE` |

Grants are applied by a separate runner step (`0001_grants`) parameterized with the configured runtime role name (validated identifier, quoted with `quote_ident`), not by `0001_initial.sql`. Runtime credentials need no DDL. Gate-row policy is cooperative protocol enforcement for supported clients, not a sandbox against arbitrary SQL under those credentials; incompatible old clients are stopped/revoked before activation.

## 10. Error mapping

`PostgresException.SqlState` (or the transport failure) maps to 03 §9 classes:

| SQLSTATE / failure | Where | Classification | Driver action / code |
|---|---|---|---|
| `40P01` deadlock_detected | any statement before COMMIT | not applied | Roll back; retry same command (P-13); exhausted → `transient_conflict_exhausted` |
| `40001` serialization_failure | statement or COMMIT | not applied | Same as 40P01 (driver-owned); staging: surface to caller, no retry |
| `55P03` lock_not_available | `lock_timeout` | not applied | Same as 40P01 |
| `57014` query_canceled | `statement_timeout` or provider cancel before COMMIT | not applied | Roll back; no automatic retry; `statement_timeout` |
| `RWR01` | COMMIT (receipt trigger) | not applied | `InvariantFault receipt_incomplete` |
| `23505` unique_violation on `jobs_pk`, `job_attempts_pk`, `concurrency_scopes_pk` | insert | not applied | `InvariantFault id_collision` |
| `23505` on `job_attempts_one_open` | Reserve | not applied | `InvariantFault invalid_transition` |
| `23502`, `23503`, `23514`, `23P01`, other `23xxx` | any | not applied | `InvariantFault constraint_violation` (library validation should have rejected first) |
| `42P01`, `42703`, `42883`, `42704` undefined object | any | not applied | `UnsupportedVersion schema_incompatible` |
| `42501` insufficient_privilege | any | not applied | configuration fault `store_unavailable` (logged with role) |
| `25P02` in_failed_sql_transaction | any | not applied | driver defect; roll back |
| `53300` too_many_connections, `08001`/`08004`/`08006` at open, `28xxx` auth | connection open | not applied | `store_unavailable` |
| `57P01`/`57P02`/`57P03` admin/crash shutdown, `08003`/`08006` connection failure, I/O exception, `TimeoutException`, `OperationCanceledException` | **before** COMMIT was written | not applied | `store_unavailable` (connection discarded) |
| same failures | **after** COMMIT was written, before its reply | **outcome unknown** | `commit_outcome_unknown`; resolve by resubmitting the same command or ResolveCommand/1 |
| `40003` statement_completion_unknown | COMMIT | outcome unknown | `commit_outcome_unknown` |
| COMMIT reply received | — | decided | — |

The driver tracks whether the `COMMIT` message has been written to the socket; Npgsql's `CommitAsync` exceptions are classified with that flag. Provider cancellation is never treated as evidence of rollback once COMMIT was sent. Server error detail and parameters are never logged.

## 11. Tests

Harness (07): xunit v3, `Testcontainers.PostgreSql`; `RUNWICK_TEST_PG` (connection string to an existing server; no Docker) overrides containers; `RUNWICK_TEST_PG_IMAGE` selects the image (default `postgres:18`); certification runs the contract suite on 16, 17 and 18. Tests use real connections and processes, deterministic barriers and fault points; mocking a store interface cannot prove locking.

### 11.1 PGX cases

| ID | Required proof (M2 form) |
|---|---|
| PGX-01 | Application-gate changes (§7.15: `open → drain → maintenance`, and a required capability added) race every M2 command family (Dispatch, DispatchMany, Stage, Reserve, Begin, Renew, Finish, Recover, ReleaseReservation, PublishPresence, Register*): each in-flight transaction either completes under the old gate before the change commits, or observes the new gate before any mutation; drain still permits Renew/Finish/Recover/ReleaseReservation. Store-gate change (validate phase) races likewise. |
| PGX-02 | Full serializer tuple and immutable payload/profile consistency: `job_payloads` profile must equal `jobs` profile (composite FK rejects a mismatch); Reserve never claims a job whose `(name, version, profile id, profile version)` is not supported (ADM-01, SER cases); payload bytes returned by Reserve equal dispatched bytes exactly. Schedule/manual-retry parts: not in M2. |
| PGX-03 | An unimplemented capability cannot be silently ignored: with `required_capabilities = {"rate_admission":1}` every M2 creation path returns `UnsupportedVersion capability_unsupported` and writes nothing; settlement paths remain available; the M2 worker refuses to enable admission. Rate grant/deferral parts: not in M2. |
| PGX-04 | Finish versus duplicate dispatch of the same active key (Finish clears the binding; dispatch either reuses the active job or creates a new one, never both, never a deadlock); overlapping DispatchMany batches with the same keys and scopes in opposite input orders; dispatch scope provisioning racing Reserve on the same scope. Manual-retry/materialization parts: not in M2. |
| PGX-05 | All FK/unique/check/deferred constraints hold after injected rollback and lost COMMIT at every fault point; a transaction that would commit a receipt placeholder fails with `RWR01` and leaves nothing; deferred current-generation/current-attempt FKs reject a dangling pointer at commit; `job_attempts_one_open` rejects a second open attempt. |
| PGX-06 | Detail retention (M2 form): a terminal job keeps job/attempt/generation identity; receipts, dispatch-window bindings and presence have independent lifetimes; Reserve-receipt replay whose payload row was removed by the harness returns `HistoryUnavailable payload_unavailable`. Manual-retry/rate/audit parts: not in M2. |
| PGX-07 | `EXPLAIN (ANALYZE, BUFFERS)` at 10⁵ and 10⁶ history rows: Reserve candidate uses `jobs_ready` with `(eligible_at, job_id)` index condition; probe uses one `jobs_ready` lookup per lane; ListExpiredLeases uses `jobs_expired_lease`; permit count uses `permits_live_count`. Gate `FOR SHARE` overhead (MultiXact/WAL) and renewal latency stay within the G-1 budgets recorded by SP-010. |
| PGX-08 | A caller business transaction holding the store/application gate (`StageAsync` left open) blocks the application-gate change and the migration validate phase; both fail with bounded `lock_timeout` diagnostics and no partial commit; the store stays in its last recorded mode. |
| PGX-09 | Millisecond quantum (floor/ceil at equality boundaries, expiry equality = expired), bytewise `COLLATE "C"` order of names/keys, UUID order equal to `UuidOrder`, and generation policy JSON equal to the 02 vectors. Cross-driver vectors: other drivers not in M2. |

### 11.2 PostgreSQL validation matrix (maps to driver-suite cases)

| Scenario | Assertion | Case |
|---|---|---|
| Last permit: two jobs, two replicas, capacity 1 | At most one valid grant; loser has no attempt or partial permit | ADM-02 |
| Multiple scopes A+B vs B+A | Sorted scope locks; no lock-order cycle | ADM-03 |
| Pause vs claim | Pause waits behind Reserve's share lock; no admission after pause commits; Dispatch never blocked | ADM-04 |
| Lease vs recovery: renewal paused after locks | Fresh-time check rejects expired renewal; Recover rechecks token/expiry | OWN-01, OWN-03 |
| Lost Reserve / Begin / Finish reply | Same command resolves the same effect; one attempt, one execution, no rerun | CMD-03, RUN-02, END-01 |
| Command expiry: pruned receipt; deadline passes while blocked on a scope lock | No new mutation; `CommandExpired` / `ExpiredUnknown` | CMD-04, CMD-05 |
| Setup failure after Begin | Counted execution closes through Finish | RUN-01 |
| Death before / after Begin | Abandonment guard vs execution count each updated once | RUN-01, TIME-06 |
| Stale Finish after takeover | Old owner changes nothing and cannot delete replacement permits | OWN-02 |
| Duplicate dispatch, same active/window keys in parallel | One job or explicit conflict; no orphan binding | UNI-01–03 |
| Rollback of a staged business transaction | Neither business row, job, key nor receipt visible | TX-01 |
| Capability filtering, old/new workers | Unsupported tuples never reserved | ADM-01 |
| Non-UTC session `TimeZone`, delayed lock acquisition | Decisions use actual storage instants; `ms_floor`/`ms_ceil` session-independent | ENC-01, OWN-03 |
| Missing/extra permit row on Renew | `permit_set_mismatch`, rollback, no partial success | OWN-05 |
| Deadlock/serialization injection | Same command identity on retry | CMD-01 |
| Kill/restart of the primary after acknowledged commit | Acknowledged receipts, jobs and permits present | driver-suite §5 PostgreSQL |
