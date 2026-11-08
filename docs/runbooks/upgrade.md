# Runbook: upgrading Runwick

Normative rules: [spec 23](../spec/23-upgrades.md). This page is the procedure. **Every step names the command or test that proves it.** The commands were run on 2026-11-08 against PostgreSQL 18 (container `runwick-pg`) and a SQLite file; the output is in [transcript-2026-11-08.txt](transcript-2026-11-08.txt), and [run-runbooks.sh](run-runbooks.sh) repeats the run.

Read this first: **a Runwick binary works with exactly one store revision.** The moment the new revision is published, the previous binary cannot run at all (creation or settlement), and there is no downgrade. The way back is the backup from step 1 (see [restore](restore.md)). Plan for downtime of new work, not for a rolling upgrade.

The commands use the CLI of the **new** release for `migrate` and of the **currently deployed** release for `store …` while the old schema is still in place (the `store` commands only touch the application gate rows, which have not changed shape). Replace `RUNWICK_STORE` with the name of an environment variable holding the migration-role connection string. `--provider sqlite` works the same way on a SQLite file. Redis has no CLI yet: use `RunwickRedisMigrations.SetAdmissionModeAsync`, `GetStatusAsync` and `MigrateAsync` from code ([spec 23 §3.5](../spec/23-upgrades.md)).

## Before you start

- You have the migration-role credentials (the runtime role cannot do any of this).
- You know which processes use the store: workers, producers, schedulers, dashboards, CLI hosts. You can stop each of them. Runwick cannot do this for you and cannot prove that you did ([spec 23 §6](../spec/23-upgrades.md)).

## Steps

### 1. Back up

PostgreSQL: `pg_dump -Fc <database> > before-upgrade.dump`. SQLite: stop all processes on the file, then copy `runwick.db` (and `-wal`, `-shm` if present) or use `sqlite3 runwick.db ".backup before-upgrade.db"`. Redis: take an AOF/RDB copy per your own procedure.

Proof: `pg_dump -Fc` ran in the transcript (294,618 bytes) and was restored in [restore step 3](restore.md#3-restore-it-somewhere-else). The restore is exercised by tests UPG-09.

### 2. Look, then drain

```
runwick store status    --provider postgres --connection-env RUNWICK_STORE
runwick store admission --provider postgres --connection-env RUNWICK_STORE --mode drain
```

`store status` shows the store revision, the revision this CLI expects, each application's admission mode, and the counts of owned leases and worker presences. Drain stops all new work (dispatch, reserve, begin, schedule materialization, retries) and lets running work renew and finish. Add `--application NAME` for one application.

Proof: transcript "upgrade.md step 2" (drain applied, revision bumped); tests UPG-04 (`UpgradeContractTests`, all three drivers) and PGX-01 (`RateGateRaceTests`: drain racing every command family); UPG-03 (a long caller transaction makes the change wait up to 5 s, then fail with `migration_lock_timeout` and nothing changed).

If you try maintenance before drain you get `drain_first` (transcript, exit 1). If `migration_lock_timeout` appears, a business transaction that stages jobs has been open for more than 5 s: wait for it, then repeat.

### 3. Wait for work to settle, then stop every old process

Run `store status` until `ownedLeases` is 0, or decide to abandon what remains (an abandoned lease expires and is recovered later by the new version). Then **stop the old workers, producers, schedulers and dashboards yourself.** Do not read `expiredWorkerPresences` or an expired lease as "stopped": a paused or partitioned process looks identical.

Proof: tests UPG-08 (`Expired_presence_and_leases_are_reported_never_taken_as_proof`, PostgreSQL and SQLite): drain does not wait for handlers; status keeps showing the lease and carries the "unverified" notice.

### 4. Migrate

```
runwick migrate --provider postgres --connection-env RUNWICK_STORE
```

Prints the store ID, the revision and the phases it applied. Safe to repeat: a second run applies nothing. If it fails:

| Code | Meaning | Do |
|---|---|---|
| `migration_connection_lost` (exit 4) | The session holding the migration lock died | Run again; it resumes from the ledger. Transcript: killed at 20 ledger rows (revision 11), second run applied the rest to 17 |
| `migration_lock_timeout` / `migration_lock_unavailable` (exit 4) | Another migrator or a long transaction | Wait, run again |
| `schema_validation_failed` (exit 1) | A real object differs from what this release expects (an invalid index, a changed function body) | Do not force. The store stays at the last good revision in mode `normal` and no binary can run on it. Read the message, repair or restore, run again. Test UPG-05 |
| `migration_checksum_mismatch` (exit 1) | A released migration was changed | Stop. You are running a different build than the one that migrated this store |
| `migration_unknown` (exit 1) | The store is **newer** than this CLI | You are rolling back. Use the matching older CLI, or restore ([restore](restore.md)) |

Proof: transcript "interrupted first install" and "migrate again"; tests UPG-01 (session loss with a second migrator waiting) and the INIT-01 kill-after-every-phase cases, UPG-05, UPG-06.

### 5. Reopen admission

```
runwick store admission --provider postgres --connection-env RUNWICK_STORE --mode open
```

Reopen **before** starting the new processes: a process that starts while its application is drained fails its startup handshake (`AdmissionClosed application_draining`), because registering its queues is a creation path. This was observed: the sample host exits at startup (transcript "while drained"). Between reopening and starting, nothing can run, and the old binary is already refused by the revision gate.

Proof: transcript "upgrade.md step 6"; UPG-04.

### 6. Start the new processes

Start workers, then producers, then dashboards. Check `runwick doctor --connection-env RUNWICK_STORE` (the revision check passes when the CLI and store agree) and `store status`.

Proof: transcript: sample host `--dispatch 2` → "2 succeeded"; CLI-08 (`doctor`).

### 7. If a schedule uses a new evaluator profile or time-zone data

Only after **every** scheduler host runs the new release, edit the schedule definitions to the new profile (a forward cutover). A host that lacks the profile skips that schedule locally and says so; it never marks it for review. After a long drain, check the schedule's misfire policy: skipped or caught-up fires follow it. See [spec 23 §9](../spec/23-upgrades.md). Proof: SCHED-11, SCHED-13 and the drain case (`UpgradeTests.A_drain_pauses_materialization_without_moving_the_cursor`).

## If it goes wrong

- **Before step 4 finished:** nothing changed shape. Reopen (`--mode open`) and keep running the old version.
- **After step 4:** the old version cannot run. Restore the step 1 backup, run the old version and the old CLI ([restore](restore.md)). Work done after the backup is lost.
- **A process from before is still running:** its next call is refused (the revision gate after a migration, `store_identity_mismatch` after a re-identification) and nothing is written. Stop it anyway.
