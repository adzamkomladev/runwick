# Runbook: restoring, copying and cloning a Runwick store

Normative rules: [spec 23 §7](../spec/23-upgrades.md). Every step names the command or test that proves it; the commands were run on 2026-11-08 ([transcript](transcript-2026-11-08.txt), [run-runbooks.sh](run-runbooks.sh)).

**A restore is a step back in time.** Runwick cannot tell that it happened. After a restore: jobs and results written after the backup are gone; commands you were told succeeded are unknown (resending one creates the job again); rate windows and fencing counters can repeat what they already issued; schedule cursors move back and fires run again according to each schedule's misfire policy; event cursors held by readers can point past the new end. Runwick does not reconcile effects outside its database. You do. The full table is [spec 23 §7.3](../spec/23-upgrades.md). Proof of the limits: test UPG-09 (`A_copy_needs_a_new_identity_and_a_restore_loses_what_the_snapshot_never_had`): an acknowledged dispatch resolves as not confirmed on the restored copy, resending it creates the job again, and the restored fence counter issues a value the original had already issued.

Two different situations, two different answers:

| Situation | Identity | Steps |
|---|---|---|
| **Replace** a broken or rolled-back store; the original is gone and will never run again | Keep the ID | 1, 2, 3 (restore over), 5, 6 |
| **Copy** that runs beside the original (staging, clone, restore-to-inspect, disaster-recovery rehearsal) | **New ID, before anything connects** | 1, 2, 3, **4**, 5, 6 |

When unsure, take the second row: a new ID is harmless, a duplicate ID lets envelopes and in-flight commands meant for one store land in the other.

## Steps

### 1. Stop everything that can write

Workers, producers, schedulers, dashboards, CLI hosts, and any other process holding credentials for the store you are replacing. Runwick cannot stop them or prove they stopped ([spec 23 §6](../spec/23-upgrades.md)). If the old store is still reachable, put it in maintenance first so a straggler is refused instead of writing:

```
runwick store admission --provider postgres --connection-env RUNWICK_STORE --mode drain
runwick store admission --provider postgres --connection-env RUNWICK_STORE --mode maintenance
```

Proof: transcript (restore section, same commands on the copy); UPG-04.

### 2. Know what you are restoring

Take the backup's store revision with `store status` on a restored copy (step 3). A backup from an older schema needs the **older binary and CLI** that match it (the rollback floor, [spec 23 §5](../spec/23-upgrades.md)); a newer CLI will migrate it forward, which is fine for a restore and not fine for a rollback you meant to keep old.

### 3. Restore it somewhere else

PostgreSQL: `createdb restored && pg_restore -d restored --no-owner before-upgrade.dump`. SQLite: stop all processes, copy the file back (with its `-wal`/`-shm` if you copied them; prefer `.backup`). Redis: restore the AOF per your own procedure (a restore that goes back in revision is detected by running processes as `history_discontinuity`, REDIS-15, and they stop; one that doesn't is not).

Then look:

```
runwick store status --provider postgres --connection-env RUNWICK_RESTORED
```

Transcript: the restored database reports the **same store ID** as the original and the application in `open`.

Proof: `pg_restore` in the transcript; UPG-09.

### 4. Copy only: give it a new identity

Identity change is **offline**. It refuses unless every application is in `maintenance` and the store records no live worker presence or lease:

```
runwick store admission --provider postgres --connection-env RUNWICK_RESTORED --mode drain
runwick store admission --provider postgres --connection-env RUNWICK_RESTORED --mode maintenance
runwick store reidentify --provider postgres --connection-env RUNWICK_RESTORED --confirm
```

Observed in the transcript: with the application open it fails `store_not_quiescent` ("Put every application in maintenance first"); in maintenance, right after the restore, it still fails because the dump carried the presence rows of the processes that were running ("1 live worker presences"); once those rows expired it succeeded (`oldStoreId` … `newStoreId` …). Expiry only lets the command run; **you** stopped the processes in step 1. Expired rows do not prove anything.

After this, prepared envelopes and commands bound to the old ID are refused by the copy with `store_identity_mismatch` and nothing is written. A process that was still holding the old identity is exposed the same way. Test: `UpgradeContractTests.A_new_store_identity_refuses_everything_bound_to_the_old_one` (PostgreSQL, SQLite), `RedisUpgradeTests.A_new_store_identity_refuses_everything_bound_to_the_old_one`, `UpgradeTests` UPG-08/UPG-09, `CliUpgradeTests`.

SQLite: `reidentify` on the copied file works the same (transcript, last section: a copied file got a new ID while the original kept its own). Redis: `RunwickRedisMigrations.ReassignStoreIdAsync` (library only); it rewrites every application's copy first and `rw:store` last, so an interruption leaves the applications refusing until you rerun it.

### 5. Reopen

```
runwick store admission --provider postgres --connection-env RUNWICK_RESTORED --mode open
```

Reopen before you start any process: a process that starts while its application is drained or in maintenance fails its startup handshake.

### 6. Start, then reconcile

Start workers. Then, by hand:

1. **Effects.** Anything the lost window did outside the database (emails sent, payments taken) still happened. Decide per job type whether to re-run or suppress.
2. **Lost acknowledgements.** A command your callers saw succeed may be unknown now. Resending creates the job again unless it has an idempotency key that survived (keys written after the backup did not).
3. **Schedules.** Check each schedule's cursor and misfire policy; fires since the backup run again or are skipped according to it.
4. **Fences.** Resources that compare fencing tokens may see a lower value than before. Restart the resource's guard or bump its expectation.
5. **Outbox.** Messages delivered after the backup may arrive again (at-least-once; destination deduplication, [spec 14 §8](../spec/14-outbox.md)).

Proof: transcript "an application run against the restored database works under its new identity" (sample host: "1 succeeded"); UPG-09 for items 2 and 4; SCHED cases and [spec 23 §9](../spec/23-upgrades.md) for item 3.
