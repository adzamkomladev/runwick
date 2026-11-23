## PostgreSQL receipt compaction (migration 0030)

Written 2026-11-23. Branch `pg-receipt-compaction`, from `main` at `2e9f2c8`. Migration `0030_receipt_by_key`, schema revision 30, on by default: stored rows, receipt results, replies and replay are unchanged, so nothing a caller sees changes. Measurements are single 2,000-job drains on a private PostgreSQL 18.7 server (no other writers, so the WAL figures are this drain's alone). Byte counts do not depend on host load; the jobs/s figures do and are not a result.

### Short answer

Every command a worker or producer sends leaves a receipt row, so a retried command with the same ID gets the same answer. On PostgreSQL that receipt is written twice: a placeholder is inserted when the command starts and is then updated with the decision. Receipts were about **1.5 kB of WAL per drained job, 14% of all WAL** (11 kB per job). Two changes cut that by **22%, to 1.2 kB per job** (about 3% of all WAL):

1. **One receipt index fewer.** `command_by_id (application, command_id)` was written on every receipt row version but only read by two checks that look a command up without its class. A receipt has one of two classes, so those checks now name the class and use the primary key.
2. **The decision update stays on its page.** The decided row (about 460 bytes for a Finish, about 2 kB for a batched claim) is bigger than its placeholder. On a full page PostgreSQL has to put it elsewhere, and then it writes a new entry into every receipt index. Half of the durable receipts went that way. The receipt tables now keep 30% of each new page free (`fillfactor = 70`), and 89% of those updates now stay on the page, so no index entries are written for them.

The cost is disk space: the receipt tables take about 25% more space for the same rows, and the receipt tables plus their indexes about 8% more. Receipts are kept 10 minutes (claims) or 24 hours (everything else) past their deadline, so this is a bounded amount.

Not built: storing a smaller receipt result, the way Redis did (claim receipt 1,263 → 244 bytes). That is where most of the remaining bytes are (see "What is left"), but it changes the stored receipt format, so old and new receipts would both have to be readable and `ResolveCommand` would return a different raw result. That needs a receipt codec version 2 and its own replay proofs, so it is left for a decision.

### What a receipt costs today (revision 29)

Method (`eng/perf-pg-receipts/measure.sh`): a 20-job warm-up drain creates and migrates the database, the receipts are emptied, CHECKPOINT, then a 2,000-job drain (`--payload 512`, 16 workers, default batched claim and finish). WAL is read with `pg_walinspect` for the drain's LSN range and grouped by the relation each record touches ("record" = the whole WAL record including headers; a record touches one relation here).

| Relation | WAL per job, revision 29 | Revision 30 |
|---|---:|---:|
| `storage_commands_durable` (heap) | 727 | 656 |
| `storage_commands_ownership` (heap) | 269 | 251 |
| durable `command_retention` index | 182 | 141 |
| durable primary key | 142 | 105 |
| durable `command_by_id` | 130 | – |
| ownership indexes (three, then two) | 59 | 31 |
| **All receipt relations** | **1,509** | **1,184** |
| All WAL of the drain | 10,963 | 10,573 |

The total WAL line moves by more than the receipt lines explain (other relations vary run to run by a few hundred bytes per job: autovacuum and checkpoint timing); read the receipt lines, not the total.

Receipts per drained job and their size (both revisions identical):

| Receipt | Per job | Row bytes | Of which the stored result |
|---|---:|---:|---:|
| Finish (durable, one per job, written through FinishMany) | 1.00 | 464 | 311 |
| ReserveAndBegin (ownership, one per claim of about 5.5 jobs) | 0.18 | about 2,100 | about 1,950 |
| DispatchMany (durable, one per 100-job bulk add) | 0.01 | 3,200 | 3,040 |

Placeholder inserts and decision updates, durable / ownership partition:

| | Inserts | Updates | Of which HOT (no index entries) |
|---|---:|---:|---:|
| Revision 29 | 2,045 / 359 | 2,045 / 357 | 1,010 (49%) / 277 (78%) |
| Revision 30 | 2,045 / 348 | 2,045 / 346 | 1,812 (89%) / 346 (100%) |

Sizes after the drain, bytes (heap + indexes): revision 29 durable 1,114,112 + 458,752, ownership 548,864 + 114,688, total 2,236,416; revision 30 durable 1,392,640 + 327,680, ownership 622,592 + 65,536, total 2,408,448. No full-page images worth counting appeared on receipt relations in either run: the drains were short and started right after a checkpoint, and command IDs are time-ordered (UUIDv7), so index inserts land on the same few right-hand pages. Earlier evidence agrees on the scale: making both receipt partitions unlogged (the G2 probe, 2026-11-15) cut WAL 8% to 12%.

### Choosing the fill factor

Probes with the same revision-30 binary, the fill factor set by hand before the drain (`FF=` in `measure.sh`):

| Fill factor | Durable updates HOT | Receipt WAL per job | Durable heap after the drain |
|---|---:|---:|---:|
| 100 (default, index already dropped) | 49% | 1,382 | 1.14 MB |
| 70 | 86% | 1,215 | 1.38 MB |
| 50 | 97% | 1,159 | 1.71 MB |

70 takes most of the gain for half the extra space of 50. A FinishMany call decides up to its whole batch in one update statement, so a page needs room for several decided rows at once; that is why a few updates still miss at 70.

### What changed

- `0030_receipt_by_key.sql`: `DROP INDEX command_by_id`; `fillfactor = 70` on both receipt partitions (new pages only, nothing is rewritten, the lock is brief); `rw_receipt_v1`, `rw_schedule_group_set_v1` and `dispatch_group_v1` dropped and recreated with the same names, arguments and bodies except the one predicate. The R3 check (is this ID already used by a command of the other class?) now reads `retention_class = CASE @class WHEN 'ownership' THEN 'durable' ELSE 'ownership' END`; the schedule group's "does any of these commands already have a receipt" check reads `retention_class IN ('ownership', 'durable')`. Both are primary-key lookups on PostgreSQL 16 to 18 (no skip scan needed).
- The driver's own R3 statement (`PgRunner`) and management's `CommandNotAfter` read the same way.
- Schema validation: an index that a later migration drops (and does not recreate) is no longer expected.
- Replay is untouched: R2 (read the existing receipt and return its stored decision) already used the primary key, and no stored value changed.

### Tests

- New `ReceiptByKeyTests` (PostgreSQL project), both passed on a private PostgreSQL 18 (one run of the class alone, 15 s):
  - RCPT-01 `Receipts_written_at_revision_29_replay_after_the_upgrade_to_30`: puts the store back to exactly revision 29's shape (the index, the default fill factor and the three revision-29 function bodies, checked against revision 29's expected function digests), writes a Dispatch, a ReserveAndBegin, a Finish and 40 more jobs through those functions, replays one, marks the store as revision 29 and runs the migrator, which applies only 0030's three phases. Then: the index is gone, the fill factor is set, the receipt count is unchanged, and the Dispatch, ReserveAndBegin and Finish replay with `Replayed = true`, the same job, attempt, value and decision time, and no new jobs. `CommandNotAfter` finds receipts of both classes. A newly opened client accepts the store's functions and dispatches.
  - RCPT-02 `The_same_id_under_the_other_retention_class_conflicts_in_both_directions`: a Dispatch ID reused for ReserveAndBegin and for Reserve, and a ReserveAndBegin ID reused for Dispatch, are each `CommandConflict` with no effect; the store snapshot and receipt count are unchanged.
- `MigrationTests`: manifest at revision 30, kill-and-resume after each 0030 phase (expand 29, validate 30, activate 30). Not run by me.
- Not run by me (for Sonnet): the full PostgreSQL and Contract projects. The ones that exercise the changed statements: `CommandIdentityTests` (CMD-02 already reuses a Dispatch ID for Reserve), `ScheduleStoreTests` and `ScheduleCaseTests` (the schedule group check), `GroupedAdmissionTests` (dispatch_group_v1), `UpgradeTests`, `MigrationTests`, `SchemaContractTests`.

### Paired drains (for Sonnet)

`eng/perf-pg-receipts/pairs.sh` alternates base and candidate on a fresh database per arm (each binary creates and migrates its own database with a 20-job warm-up, then CHECKPOINT and the measured drain), and prints jobs/s, WAL bytes per job and receipt-table growth per arm. Use a private server so the WAL figure is the drain's own:

```
docker run -d --name runwick-rcpt-pg -e POSTGRES_PASSWORD=pg -p 127.0.0.1:55641:5432 postgres:18 \
  -c shared_buffers=512MB -c max_connections=300 -c max_wal_size=8GB
git worktree add /tmp/rcpt-base 2e9f2c8   # main before this change
dotnet build -c Release /tmp/rcpt-base/tests/Runwick.Workloads -o /tmp/rcpt/base-bin
dotnet build -c Release tests/Runwick.Workloads -o /tmp/rcpt/cand-bin       # this branch
eng/perf-pg-receipts/pairs.sh /tmp/rcpt/base-bin /tmp/rcpt/cand-bin /tmp/rcpt/pairs 5 5000
```

Expected: WAL per job about 3% lower (the receipt share above), receipt-table growth about 8% higher, jobs/s unchanged within noise. `eng/perf-pg-receipts/measure.sh <bin> <db> 2000` gives the per-relation breakdown for one binary.

### What is left

- The stored results are now the largest part: about 310 bytes per Finish and about 2 kB per batched claim, written in full in the WAL of the decision update. A compact result (field codes, no repeated job ID, no nulls, no lease and policy echo that the claim reply can rebuild) would roughly halve the heap WAL, about another 400 bytes per job by the Redis precedent. That needs receipt codec version 2, readers for both versions, and replay tests across the switch; it is a decision, not a free change.
- The placeholder insert plus decision update is the receipt protocol (R1 to R4) itself; folding them would change how concurrent sends of the same command wait for each other, so it is not touched.
- SQLite has the same `command_by_id` index; not changed here.
