# 23 — Upgrades, rollback, drain, maintenance and restore (SP-026)

**Sources:** [docs/architecture/postgres-upgrades-and-compatibility.md](../architecture/postgres-upgrades-and-compatibility.md) (the UPG-01 to UPG-10 definitions, §8), [postgres-schema-and-operations.md](../architecture/postgres-schema-and-operations.md) (gates, PGX-01), [03-storage-protocol.md](03-storage-protocol.md) §10 (gates and admission modes), [04-postgres.md](04-postgres.md) §7.15 and §8 (gate change, migrator), [20-sqlite.md](20-sqlite.md) §9, [22-redis.md](22-redis.md) §2.4 and §9, [13-scheduler.md](13-scheduler.md) and [12-schedule-evaluator.md](12-schedule-evaluator.md) §6.3 (profile and data cutovers), [19-cli.md](19-cli.md), [00-overview.md](00-overview.md) §9 rule 12, [v1-delivery-plan.md](../v1-delivery-plan.md) §7 (SP-026).

## In short

This file says what an operator may and may not do when Runwick's code or database changes: upgrading, rolling back, pausing work, and restoring or copying a database. It adds one honest simplification to the older design papers: **in v1 a binary works with exactly one store revision**. There is no window in which an old and a new binary share a store. So an upgrade is "drain, stop old, migrate, start new, reopen", and the way back is "restore the backup taken before the migration", which is a history discontinuity (§7).

Four things are new in code:

1. `SetAdmissionModeAsync`, `GetStatusAsync` and `ReassignStoreIdAsync` on `RunwickPostgresMigrations`, `RunwickSqliteMigrations` and `RunwickRedisMigrations`, and the CLI commands `store admission`, `store status` and `store reidentify` for PostgreSQL and SQLite.
2. A receipt this binary cannot decode makes `ResolveCommand` fail with an explicit incompatibility instead of answering "not found" or "conflict" (§8).
3. The UPG-01 to UPG-10 tests (§11).
4. The runbooks [upgrade](../runbooks/upgrade.md) and [restore](../runbooks/restore.md), every step of which was run.

## Changes

- (2026-11-08, SP-028) §3.6: the public path to activate `scheduler` and `rate_admission` (library calls on the three migration classes and `runwick store activate-capability`); closes the SP-027 gap.
- (2026-11-08, SP-027) A process started while its application is drained now fails with `AdmissionClosedException` and a message that names the fix, instead of `StoreIncompatibleException "Store handshake failed"`; the order migrate, reopen, start is documented in the runbook (§4 step 5). Scenarios SCN-25 to SCN-27 run it on all three stores.
- (2026-11-08) Written for SP-026. Decisions where the sources were silent, each marked [choice] below: one revision per binary and an empty `expandCompatibleBinaries` list are stated as the v1 rule (§1); `open` to `maintenance` directly is refused (§3.2); admission and identity commands run as the migration role and record only `actor_subject`, no management-audit row (§3.4); an identity change is refused unless every application is in maintenance and no live lease or presence is recorded (§7.2); CLI support covers PostgreSQL and SQLite only, because `Runwick.Cli` does not reference `Runwick.Redis` (§3.5).
- (2026-11-08) 03 §10.2's wording "unless the migration manifest lists them" for settlement paths in `maintenance` is made concrete: no manifest lists any, so `maintenance` rejects Renew, Finish, Recover and ReleaseReservation (§3.1). This matches the code and PGX-01.
- (2026-11-08) 19 gains the three `store` commands (see its Changes); 04 §7.15 stops being "harness only": the same statements are the product's `SetAdmissionModeAsync`.

## 1. Targets and boundaries

- A binary declares exactly one store revision (`PgMigrator.CurrentRevision`, SQLite and Redis revision 1) and protocol epoch 1. Every operation checks it inside its transaction (03 §10.1). A store at another revision gives `UnsupportedVersion schema_incompatible` for every operation, creation or settlement, in both directions: old binary on newer store, new binary on older store.
- The manifest field `expandCompatibleBinaries` is empty in every manifest v1 writes. A store whose `migration_mode` is `expand` therefore admits no binary (`store_maintenance`) [choice]. The migrator never writes `expand`; the mode exists for a future migration that declares compatible binaries, and until one does, "additive and invisible to old clients" is **not** a v1 promise.
- Migrations only go forward. There are no down migrations. Each migration advances the **rollback floor** (§5).
- A migration's three phases (expand, validate, activate) run in separate short transactions, each with `lock_timeout` 5 s and `statement_timeout` 60 s (04 §8.1). None of migrations 0001 to 0017 has a backfill or a concurrent index build; the checkpoint column and the `CREATE INDEX CONCURRENTLY` ledger phase of 04 §8.4 are specified but have no user yet. A migration that adds one must come with its own interrupted-phase test (UPG-05).
- Runwick does not stop old processes. A process that does not check the gates (a pre-gate client with unrestricted SQL credentials) is outside every guarantee here; revoke its credentials.

## 2. The matrix per driver

"Applies" means the case is run on that driver. "N/A" has its reason.

| Case | PostgreSQL | SQLite | Redis |
|---|---|---|---|
| UPG-01 concurrent migrators, lost lock connection, checksum mismatch | Applies (advisory lock) | Applies in part: the write lock is the migration lock; concurrent migrate and checksum are SQL-10. Lost-connection is N/A: one transaction, a dead process rolls back | Applies in part: lock key with a 60 s expiry; concurrent migrate and foreign keys are REDIS-14. Checksum is N/A: there is no SQL body, the manifest digest is compared instead |
| UPG-02 only compatible old/new clients run; activation rejects old required-semantic writers | Applies | Applies (revision gate); required-capability rejection is covered by the shared RATE cases | Applies (layout gate; capability rejection by the shared RATE cases) |
| UPG-03 long caller transaction blocks the gate change; no partial commit | Applies | Applies: an open caller write transaction holds the file write lock; the change waits then fails with `migration_lock_timeout` | N/A: Redis has no caller transactions (22 §8); the only concurrent writer is a pending intent, which the change refuses with `intent_pending` (tested) |
| UPG-04 drain and maintenance block every creation path, keep settlement | Applies | Applies | Applies |
| UPG-05 interrupted DDL, index and backfill phases resume from the real catalog | Applies | N/A: the whole migration is one transaction; a crash leaves nothing (SQL-10) | N/A: install is one `HSET`; a crash leaves nothing or a complete `rw:store` |
| UPG-06 rollback after semantic activation is refused | Applies | Applies (revision gate and capability gate) | Applies (layout gate and capability gate) |
| UPG-07 old receipt resolves without reapplication; unknown codec is explicit | Applies | Applies (§8) | Applies (§8) |
| UPG-08 expired presence or lease is not proof of a stopped writer | Applies | Applies | Partly: Redis reports no lease or presence counts; the quiescence notice is still returned, the counts are `null` |
| UPG-09 restore and clone identity binding, history limits | Applies (database copy) | Applies (file copy) | Applies for identity (§7.2); restore detection is REDIS-15 |
| UPG-10 bounded locks and renewal margins; failed migration stays in a safe mode | Applies | Applies for the admission change under writer load; failed-migration is N/A (one transaction) | Applies for the admission change under load; failed-migration is N/A |

## 3. Drain and maintenance

### 3.1 Semantics (03 §10.2, kept as is)

| | `open` | `drain` | `maintenance` |
|---|---|---|---|
| Dispatch, DispatchMany, ReceiveOutbox, ManagementDispatch, staged dispatch, Reserve, ReserveAndBegin, Begin, RegisterQueue, RegisterScope, MaterializeOccurrences, schedule triggers, ManualRetry, RerunSucceeded (all creation of work) | allowed | **rejected** `application_draining` | **rejected** `application_maintenance` |
| Renew, RenewMany, Finish, FinishMany, Recover, ReleaseReservation | allowed | allowed | **rejected** `application_maintenance` |
| Cancel, controls, EventsSince, ResolveCommand, PublishPresence, ReadinessProbe, ListExpiredLeases | allowed | allowed | ResolveCommand and RegisterApplication allowed; the rest rejected |

"Rejected" always means nothing was written (a no-effect reply). Drain exists so work can settle. Maintenance exists so nothing at all changes while the database is copied, restored or re-identified; because it rejects Renew, any lease still held there will expire. This is why **maintenance is reached only from drain** (§3.2) and only after in-flight work has settled or been abandoned on purpose.

### 3.2 Transitions [choice]

Allowed: `open` to `drain`, `drain` to `open`, `drain` to `maintenance`, `maintenance` to `drain`, `maintenance` to `open`. Refused with `drain_first`: `open` to `maintenance`. Setting the mode an application already has changes nothing (no revision bump, `Changed = false`). An unknown mode is `invalid_mode`.

### 3.3 The change itself

Same statements as 04 §7.15: take the store gate row `FOR SHARE`, then the application row `FOR UPDATE` (this waits for every in-flight transaction that holds the gate shared), update the mode, bump the application revision, record `actor_subject`. In flight transactions finish under the old mode; later ones see the new one (PGX-01). The wait is bounded: `lock_timeout` 5 s, then `migration_lock_timeout` and **nothing changed** (UPG-03). SQLite waits for the file write lock for 5 s the same way. Redis runs one script per application and refuses an application that has a pending intent.

### 3.4 API

```csharp
// on RunwickPostgresMigrations, RunwickSqliteMigrations, RunwickRedisMigrations (connection string / configuration first)
Task<IReadOnlyList<AdmissionChange>> SetAdmissionModeAsync(string connection, string? application, string mode, string actor, CancellationToken ct = default);
Task<StoreAdminStatus> GetStatusAsync(string connection, CancellationToken ct = default);
Task<StoreIdentityChange> ReassignStoreIdAsync(string connection, CancellationToken ct = default);
```

`application` null means every registered application, in one transaction (PostgreSQL, SQLite) or one at a time after checking all of them first (Redis). Errors are the driver's migration exception with codes: `store_unavailable`, `store_not_initialized`, `application_not_registered`, `invalid_mode`, `drain_first`, `migration_lock_timeout`, `store_not_quiescent`, `intent_pending` (Redis). They run as the migration role. They write `actor_subject` on the application row and **no `management_audit` row** [choice]: the audit table's rows need an application-level actor model that a migration-role connection does not have; the revision bump and `actor_subject` are the record.

### 3.5 CLI

```
runwick store status      --provider postgres|sqlite --connection-env VAR
runwick store admission   --mode open|drain|maintenance [--application NAME] --provider … --connection-env VAR
runwick store reidentify  --confirm --provider … --connection-env VAR
runwick store activate-capability --name scheduler|rate_admission [--application NAME] --provider … --connection-env VAR
```

Redis is not offered: `Runwick.Cli` references only the PostgreSQL and SQLite packages (19 §1, `eng/package-graph.json`), and `migrate --provider redis` is likewise absent. A Redis operator uses the library calls. [choice]

### 3.6 Activating a capability [choice]

Specs 10 §6.1 and 13 §5 name `ActivateCapabilities/1` but no caller. The operator path is `RunwickPostgresMigrations.ActivateCapabilityAsync`, `RunwickSqliteMigrations.ActivateCapabilityAsync` and `RunwickRedisMigrations.ActivateCapabilityAsync` (`connection, application?, capability, actor`) and the CLI command above. Only `scheduler` and `rate_admission` are accepted (`invalid_capability` otherwise); the version is the binary's own. Like `store admission` it runs as the migration role, merges into `required_capabilities`, never lowers one, bumps the application's gate revision and returns one `CapabilityChange` per application (`changed: false` when already active). Without `--application` every registered application is raised. An unregistered application is `application_not_registered`: a fresh application is registered by its first start, so start it once (the scheduler's startup error then names this command), activate, restart. Redis refuses an application with a pending intent (`intent_pending`).

Difference from the store operation: this path writes no `capabilities_activated` event and no audit row (the actor is kept in the row's actor field), the same as `store admission`. Proof: `CapabilityActivationContractTests` (all three drivers) and `CliUpgradeTests.Activate_capability_*`.

## 4. The upgrade, in order

The runbook is [docs/runbooks/upgrade.md](../runbooks/upgrade.md). The reasoning:

1. **Back up.** The backup is the only way back (§5).
2. **Drain** every application (`store admission --mode drain`). New work stops; workers keep renewing and finishing. Wait until `store status` shows no owned leases, or decide to abandon what remains. Drain does not wait for handlers.
3. **Stop old processes.** Workers, producers, schedulers, dashboards. Expired leases and presence rows do not show that they stopped (§6).
4. **Migrate** (`migrate`). The runner serializes on the advisory lock (UPG-01), applies each migration in three short phases, validates the real catalog and function bodies, and only then publishes the new revision. A lost migration session aborts the run with `migration_connection_lost` (retryable; rerun resumes from the ledger). A failure leaves the last revision and mode `normal`; the new binary cannot run on it (`schema_incompatible`) and neither can anything else, so nothing wrong can happen (UPG-05).
5. **Reopen** (`store admission --mode open`). Reopen before starting processes: a process that starts while its application is drained fails its startup handshake (`AdmissionClosed application_draining`), because registering its queues is a creation path. This is existing behavior (03 §10.2), not a new rule. Since SP-027 the failure is a clear, retryable `AdmissionClosedException` ("Startup refused: the application's admission is closed … Reopen it with 'runwick store admission --mode open', then start this process again") from host start, not a `StoreIncompatibleException`; the runtime does not latch it, so a later call after reopening succeeds. The supported order is therefore: migrate, reopen, start.
6. **Start the new binaries.** Their first Describe checks the revision.

Between steps 4 and 6 the store is at the new revision, so even a stray old binary that somehow runs is refused by the revision gate; nothing new can run until step 6.

## 5. Rollback floor

The rollback floor is the lowest store revision a deployed binary accepts: **the revision of that binary's last migration**. Consequences:

- After step 4 of §4, the previous binary cannot run, creation or settlement (`schema_incompatible`). There is no "roll the binary back, keep the data".
- The way back is restoring the backup taken in step 1 and running the previous binary and CLI. Everything written after the backup is lost (§7.3).
- A binary also refuses to migrate a store that is ahead of it: a ledger row for a migration it does not know is `migration_unknown`, and the CLI `doctor` reports "CLI older than store".
- Raising a required capability (`ActivateCapabilities`) is a second, separate floor: the capability cannot be lowered, and a binary without it fails every creation path with `capability_unsupported` while its Renew, Finish, Recover and ReleaseReservation still work (10 §6.1; the UPG-02 and UPG-06 subset).
- The documented floor for this release is revision 17 for PostgreSQL, 1 for SQLite and Redis.

## 6. Status and quiescence

`store status` returns the store ID, store revision, the revision this binary expects, the migration mode, each application's admission mode, gate revision and required capabilities, and for PostgreSQL and SQLite three counts: owned leases (jobs with a lease), worker presences, and presences already expired. It always carries `StoreAdminRules.QuiescenceNotice`: *expired or absent lease and presence rows do not prove that no old process is running; stop processes yourself.* A lease that expired is still a lease row until Recover moves the job; an expired presence is still only a heartbeat that stopped arriving (a paused or partitioned process looks the same). Runwick never treats these as permission to proceed with something that needs a stopped writer. The one place it uses them, `ReassignStoreId`, uses **live** rows as a reason to refuse, never expired rows as a reason to allow (§7.2). (UPG-08.)

## 7. Restore, copy and clone

The runbook is [docs/runbooks/restore.md](../runbooks/restore.md).

### 7.1 Identity rules

- The store ID (a UUID in the store metadata) is bound into every command fingerprint and every prepared envelope (02, 03 §10.1). A command or envelope made for one store ID is refused by another with `store_identity_mismatch` before anything is written.
- A **restore of the same database** keeps its ID. That is correct for "the same system, earlier state" and is also why a restore can be silent (§7.3).
- A **copy that will run beside the original** (a clone, a staging copy made from production, a restored backup brought up while the original still exists) **must receive a new ID** before any Runwick process connects to it. Otherwise prepared envelopes and in-flight commands aimed at production would be accepted by the clone.

### 7.2 Giving a copy a new identity [choice]

`ReassignStoreIdAsync` (CLI `store reidentify --confirm`) takes the migration lock, then refuses with `store_not_quiescent` unless **every application is in `maintenance`** and the store records **no unexpired worker presence and no unexpired lease**. It then writes a new random ID and bumps the store revision counter. The old ID is returned and printed. Consequences, all intended:

- Prepared envelopes and commands bound to the old ID fail `store_identity_mismatch`. A process still holding the old ID is refused on its next call (no-effect), which is how an old process attached to the wrong database is exposed.
- A command resent after the change with the old binding is not resolved by its old identity.
- Outbox deliveries are keyed by the source's store ID, not this one's; re-identifying a destination does not change them.
- Redis: every application's `meta` copy is rewritten first and `rw:store` last. A failure in between leaves the applications refusing with `store_identity_mismatch`; rerun the command.

The command is offline by design: it needs maintenance mode, and maintenance rejects settlement, so any lease still live would be lost. Nothing in Runwick stops processes for you (§6).

### 7.3 What a restore loses (history-discontinuity limits)

A restored database is an earlier state. Runwick cannot detect this on PostgreSQL or SQLite, and detects only some cases on Redis (22 §2.4: a lower revision, a missing `rw:store`). After a restore these are lost or repeated:

| Lost or repeated | Effect |
|---|---|
| Jobs, attempts and results committed after the backup | They are gone. Work that ran may run again, effects outside the database are not undone |
| Command receipts after the backup | A command acknowledged to a caller is now unknown: `ResolveCommand` says not observed, expired-unknown or not applied; **resending it creates the job again**. Idempotency keys that lived only in the lost window are forgotten |
| Rate grants and window high-water marks | A rate scope may admit again what it already admitted |
| Fencing counters | A counter can go backwards: a new holder receives a fence value an earlier holder already used, so a resource that compares fences can accept a stale holder |
| Schedule cursors and occurrences | The cursor goes back; occurrences already run are materialized again according to the misfire policy (13 §5) |
| Outbox receipts | Messages already delivered may be received again (at-least-once, 14 §8) |
| Audit rows and events after the backup | Gone; event cursors held by readers may point past the new end |

Runwick makes no claim of "no history loss" after a restore or a lossy failover (00 §9 rule 12). After a restore: stop all old writers first, restore, run `store status`, reconcile external effects by hand, then bring workers up. A restore of an older **schema** revision needs the matching older binary (§5).

## 8. Receipts across versions (UPG-07)

- A receipt written by this binary resolves after drain, after maintenance and after a restart: `ResolveCommand` is allowed in `drain` and `maintenance`, joins the receipt row's lock queue, rolls back, and never reapplies the command. Resending the same command returns the recorded reply with `Replayed = true` (CMD-01).
- A receipt whose codec version or operation version this binary does not know makes `ResolveCommand` throw `StoreIncompatibleException`: not "not found", not "conflict", not success, and the row is kept. On PostgreSQL this is built (`PgStore.ResolveCoreAsync`).
- **Gap.** The replay path inside the SQL functions of 0009 to 0017 still reports a receipt row with a foreign operation version as `command_conflict` (fail-closed, nothing is reapplied, but not named). Changing it means a new migration that replaces `rw_receipt_v1` for every operation; there is no second codec, so nothing can reach it in v1: such a row can only be written by another revision, and the revision gate refuses that first. The replay-path gap is listed in the report as owed to the first migration that introduces a second codec. (2026-11-08, SP-028 G-4) `ResolveCommand` on SQLite (codec and operation version) and on Redis (operation version; a Redis receipt has no codec field) now throws `StoreIncompatibleException` for a receipt it cannot decode, as on PostgreSQL, and SQLite and Redis migrators refuse a store that is ahead of them with `migration_unknown` (a ledger row, or a layout revision, this binary does not know).

## 9. Scheduler profile and data cutovers (13, 12)

A schedule definition is immutable and names its evaluator profile, TZDB release label and TZDB hash (12 §6.3). Rules for changing them:

1. **Roll forward in this order:** deploy binaries that know the new profile or TZDB artifact to every host that runs a scheduler; only then edit definitions to use it (`UpdateSchedule/1`, a forward cutover: new definition revision, `evaluated_through` never decreases, an old plan is rejected; SCHED-13).
2. A process that lacks the profile or artifact skips that schedule locally with `schedule_missing_profile` or `schedule_missing_artifact`; the shared row is untouched, no job is created, nothing is marked for review (SCHED-11). So if a cutover reaches hosts before the binaries do, the schedule stops materializing on the old hosts only, and the diagnostic says why.
3. **Rolling the binary back after a cutover** does the same on the old binary: the schedule is skipped. Edit the definition back (a second forward cutover) or roll forward; never restore for this.
4. **Drain pauses materialization** (it is a creation path). The cursor does not move while drained. After reopening, the missed range is handled by each schedule's misfire policy and grace (13 §5), so a long drain on a schedule with `skip` loses those fires and with `catch_up` runs them subject to the episode limit and age.
5. A restore moves cursors back (§7.3).

Tested by the existing SCHED-11, SCHED-13 and SCH cases plus the drain test in §11.

## 10. Load and failure (UPG-10)

- The admission change, the status read and the identity change hold their row locks only for their own short transaction, never across a handler, and give up after 5 s without writing. With workers renewing on the same application, a drain/open cycle must not delay any Renew past the renewal margin; the test records the longest Renew while ten cycles run.
- A migration that fails validate leaves the previous revision, mode `normal`, a `failed` ledger row with the diagnostic code, and no application able to run on it with the new binary (`schema_incompatible`): the safe recorded mode. A rerun after repair continues from the ledger.
- Retention cleanup and migrations do not run together by design: cleanup is a runtime operation and the revision change refuses it; cleanup's own budgets are 17's.

## 11. Tests

Trait `Item=SP-026`. PostgreSQL class `UpgradeTests` in `tests/Runwick.Postgres.Tests`; the store-agnostic admission and identity cases are in `tests/Runwick.Contract.Tests/UpgradeContractTests.cs` (PostgreSQL and SQLite); Redis in `tests/Runwick.Redis.Tests/RedisUpgradeTests.cs` (database 12 of the test server, flushed by the test); CLI in `tests/Runwick.Cli.Tests/CliUpgradeTests.cs`.

| Case | What it runs |
|---|---|
| UPG-01 | A migrator whose session is killed after a phase while a second waits: the second takes the lock and finishes; each ledger phase exists once. Plus the existing concurrent-runner and checksum-mismatch tests (INIT-01) |
| UPG-02 | Store revision one ahead, one behind: every operation refused `schema_incompatible`, nothing written. `expand` mode: every operation refused `store_maintenance`. Plus the existing old-binary activation tests (RATE) |
| UPG-03 | A caller staging transaction holds the gate: the change waits, then either times out with nothing changed and the staged job commits whole, or completes after the commit |
| UPG-04 | Drain and maintenance follow §3.1 on the real client calls, `drain_first`, `invalid_mode`, no-op repeat, reopen. Races with in-flight families are PGX-01 (RateGateRaceTests). Run on PostgreSQL, SQLite and Redis |
| UPG-05 | An index made invalid before validate: validate fails `schema_validation_failed`, the revision does not advance, the ledger holds `failed`, clients refuse the store; repair and rerun complete. Plus the 51 kill-after-phase cases (INIT-01) |
| UPG-06 | A migrator meeting a ledger row it does not know fails `migration_unknown`; status shows store revision above the binary's; capability lowering stays refused (RATE subset) |
| UPG-07 | Receipt resolves in open, drain and maintenance with no new job; resend replays; a forged codec version makes Resolve throw `StoreIncompatibleException` and changes nothing |
| UPG-08 | Status counts expired presence and owned-but-expired leases and carries the notice; drain succeeds without waiting for them; re-identify refuses live rows and allows expired rows only in maintenance |
| UPG-09 | Copy keeps the ID until re-identified; after the change the old envelope and a client bound to the old ID are refused `store_identity_mismatch`, a new client works; a restore of a pre-dispatch copy loses an acknowledged receipt (resolve says not observed) and repeats a fence value |
| UPG-10 | Ten drain/open cycles and status reads while leases renew: every Renew applied, longest Renew and longest change recorded and bounded; failed-migration safe mode is UPG-05 |
| SCHED upgrade | A drained application refuses a materialization plan and leaves the cursor; after reopen the same plan applies |
| CLI-UPG | The three `store` commands on PostgreSQL and SQLite, error codes and exit classes |

## 12. Must not claim

- No mixed-version operation, no rolling upgrade across schema revisions, no zero-downtime migration. Drain is a pause of new work, not of running work.
- No downgrade. Rollback is a restore, and a restore is a history discontinuity (§7.3).
- No proof that an old process has stopped, from leases, presence or time (§6).
- No detection of a restore or copy by identity alone; a clone must be re-identified by the operator (§7.2).
- No audit row for admission or identity changes (§3.4).
- No CLI for Redis (§3.5), and no unknown-codec naming in the SQL replay path, SQLite or Redis (§8).
