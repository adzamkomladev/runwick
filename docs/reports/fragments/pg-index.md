# PostgreSQL `jobs` index diet (migration 0029)

Written 2026-11-23. Branch `pg-index`, from `main-merge`. Measurements on a throwaway PostgreSQL 18 server on the shared host (load average 60 to 70 during the runs). Sizes, plans and WAL attribution do not depend on load; no timing claim is made here.

## Short answer

Every `jobs` update writes a new entry into every index whose condition the new row meets, because no update can be HOT (each state change touches `state`, `lease_expires_at` or `eligible_at`). Two of those entries were not needed, and migration `0029_jobs_index_diet` (schema revision 29) removes them:

1. **`jobs_profile_uq` is gone.** It was a unique index on (application, job ID, serializer profile, profile version) that existed only so the payload table could point at it. Every version of every job wrote to it. The payload now points at the job's primary key, and three small triggers keep the rule the old key enforced: a payload's serializer profile must equal its job's.
2. **`jobs_terminal_retention` now holds only finished jobs whose detail has expired.** That is the only set the history purge reads. Finished jobs that still have their detail are already in `jobs_detail_retention`, so a finish now writes one retention entry instead of two.

On a 205,000-job retained store the `jobs` indexes shrink from 97 MB to 70 MB. During a 2,000-job drain the WAL written to `jobs_profile_uq` and `jobs_terminal_retention` was 2.6 kB per job (0.3 kB of record data, the rest full-page images after the checkpoint); in the candidate those two write nothing during a drain. Claim, probe, recovery and list plans use the same indexes as before. Two reads had to change to keep their index use: the job stats read of ended jobs and the detail-expiry read. The second was slow before this change too and is now 60 times faster.

## Every index on `jobs` and who needs it

Entries written per job on the success path (create, claim, finish) before → after: **22 → 18**. Each renewal and each detail expiry: 7 → 6.

| Index | Key / condition | Read by | Written on | Decision |
|---|---|---|---|---|
| `jobs_pk` | (application, job_id) | every keyed read, claim by key (0025), foreign keys | every version | keep |
| `jobs_profile_uq` | UNIQUE (application, job_id, profile id, profile version) | only the payload foreign key check | every version | **dropped**, triggers keep the rule |
| `jobs_ready` | (application, queue, priority, eligible_at, job_id) where pending/retrying | claim candidates, readiness probe, wake check, telemetry, scaler, unsupported backlog, live stats | creation, retry | keep (small: 0.35 MB) |
| `jobs_expired_lease` | (application, lease_expires_at, job_id) where reserved/running | ListExpiredLeases (recovery), live stats | claim, renewal | keep (small) |
| `jobs_created_list` | (application, created_at desc, job_id desc) | unfiltered job list, created stats | every version | keep |
| `jobs_state_list` | (application, state, created_at desc, job_id desc) | state-filtered job list | every version | keep (see "Not done") |
| `jobs_queue_list` | (application, queue, created_at, job_id) | queue-filtered job list, queue stats, created stats by queue | every version | keep |
| `jobs_name_list` | (application, queue, job_name) | ListJobNames skip scan | every version | keep (1.4 MB, deduplicated) |
| `jobs_detail_retention` | (application, state, terminal_at, job_id) where terminal and detail kept | detail expiry; ended stats (detail kept) | finish | keep |
| `jobs_terminal_retention` | (application, terminal_at, job_id) where terminal | history purge; ended stats | finish, detail expiry | **narrowed** to `detail_expired_at IS NOT NULL` |

## Sizes (205,000 succeeded jobs, heap 39 MB, after VACUUM ANALYZE)

| Index | Revision 26 | Revision 29 |
|---|---:|---:|
| `jobs_queue_list` | 15.1 MB | 15.1 MB |
| `jobs_profile_uq` | 15.1 MB | – |
| `jobs_state_list` | 15.0 MB | 15.0 MB |
| `jobs_detail_retention` | 14.9 MB | 14.9 MB |
| `jobs_terminal_retention` | 12.5 MB | 8 kB (no detail expired yet in this fixture) |
| `jobs_created_list` | 12.4 MB | 12.4 MB |
| `jobs_pk` | 10.4 MB | 10.4 MB |
| `jobs_name_list`, `jobs_ready`, `jobs_expired_lease` | 1.9 MB | 1.9 MB |
| **All `jobs` indexes** | **97 MB** | **70 MB** |

Once detail expires, `jobs_terminal_retention` grows again, but only for the jobs waiting for their history purge, and each such job leaves `jobs_detail_retention` at the same time. So every finished job sits in one retention index, not two.

## WAL per job

Method: `pg_walinspect` over a 2,000-job drain (`--db-reuse`) on a fresh copy of each template, `CHECKPOINT` just before, block data and full-page images grouped by relation (`eng/perf-pg-index/walattr.sh`). `full_page_writes` on.

| Relation | Revision 26 bytes/job (record data + full pages) | Revision 29 bytes/job |
|---|---:|---:|
| `jobs_profile_uq` | 2,522 (241 + 2,282) | – |
| `jobs_terminal_retention` | 109 (74 + 35) | 6 |
| `jobs_pk` | 2,206 | 942 |
| `jobs_state_list` | 363 | 320 |
| other `jobs` indexes | 1,048 | 711 |

Estimate of WAL saved: **about 0.3 kB of record data per job** for certain (the two indexes' entries; plus a WAL record header for each of the 4 avoided entries), **plus up to about 2.3 kB per job of full-page images** when a checkpoint falls inside the work. `jobs_profile_uq`, like `jobs_pk`, has a random-UUID key, so after each checkpoint nearly every insert dirties a fresh page and writes a full image of it. The full-page share depends on checkpoint timing: the two single runs had different checkpoint luck (`jobs_pk` itself shows 2,206 against 942), so the paired drains below should settle the total. The total server WAL of the two single runs (63.7 MB against 45.6 MB) is not a fair comparison for the same reason.

## Plan proofs

Fixture: the 205,000-job template, with detail expiry applied to every job that ended more than 7 days ago (a store whose cleanup has kept up), then `VACUUM ANALYZE` (`eng/perf-pg-index/explain.sh`). `EXPLAIN (ANALYZE, BUFFERS)`.

**History purge (17 §3.2).** Same plan in both: `Index Scan using jobs_terminal_retention`, `Index Cond: application, terminal_at < cutoff`, 500 rows, 1,507 → 1,505 buffers. The narrowed index matches the purge's own conditions exactly.

**Detail expiry (17 §3.1).** The old read was already poor: its state test is a parameter array (`state = ANY(@states)`), which cannot prove the partial `jobs_detail_retention` condition, so the planner used `jobs_terminal_retention` and stepped over every job whose detail had already expired:

```
Revision 26, old read (165 rows):  300 ms, 178,217 buffers
  Index Scan using jobs_terminal_retention on jobs j
    Filter: ((detail_expired_at IS NULL) AND (state = ANY ('{succeeded,failed,cancelled}')))
    Rows Removed by Filter: 175526
```

With the narrowed index that path is gone, and the old read fell back to a sequential scan of `jobs` (202 ms, a hash join over all payloads). So the read was rewritten, same rows, same order, same cursor: one ordered `jobs_detail_retention` range per state (state is that index's first column, so a state array gives no order on its own), merged and cut to the batch size, with the literal terminal state list repeated so the partial index can be proven, the rows locked inside each range (`FOR NO KEY UPDATE SKIP LOCKED`), and the payload size read by key:

```
Revision 29, new read (164 rows):  4.8 ms, 1,490 buffers
  Nested Loop
    Function Scan on unnest s
    Limit -> LockRows -> Index Scan using jobs_detail_retention on jobs x
      Index Cond: (application = 'bench' AND state = s.state AND terminal_at < cutoff)
  SubPlan: Index Scan using job_payloads_pk
```

The new read would be just as fast on revision 26; the speed-up comes from the rewrite. A side effect: a range may lock up to one batch per state of rows that the merged batch does not keep. They are released at commit and are the next batch's rows anyway.

**Job stats, ended jobs.** Rewritten as two branches, detail kept and detail expired, each matching one partial index:

```
1 day, revision 26:  Bitmap Index Scan on jobs_terminal_retention              143 ms, 8,068 rows
1 day, revision 29:  Append: Index Scan using jobs_detail_retention (8,065 rows)
                           + Index Scan using jobs_terminal_retention (0 rows) 114 to 131 ms
30 days, both:       Seq Scan on jobs (74% of the table qualifies)             2.07 s → 1.88 s
```

A 30-day window covering most of the table is a sequential scan before and after; that is the planner's correct choice for a whole-range aggregate and is not new.

**Claim, probe, recovery, lists.** None of their indexes changed. The only plan test that named a removed index was the wake check in `ReadinessPlanTests` (PGX-07), which allowed `jobs_profile_uq` as an alternative to `jobs_pk`; it now allows `jobs_ready` and `jobs_pk` only. That test runs at 10⁵ and 10⁶ rows and is part of the full PostgreSQL suite below.

## Why dropping the composite key is safe

The old key `job_payloads (application, job_id, profile id, profile version) → jobs (…)` refused three things: a payload with no job, a payload whose profile differs from its job's, and a profile change on a job that has a payload. After 0029:

- no job: `job_payloads_job_fk (application, job_id) → jobs`, `ON DELETE CASCADE` as before;
- profile differs: `job_payloads_profile_insert` and `job_payloads_profile_update`, statement-level triggers that join the new payload rows to their jobs by key once per statement (so a set insert of 1,000 payloads costs one join, not 1,000 calls);
- job profile change: `jobs_profile_fixed`, a row trigger that only fires when an UPDATE names a profile column and its value really changes, and then refuses if the job has a payload. No released statement names those columns, so it never runs in normal work.

Both triggers raise `foreign_key_violation` (23503) with the old constraint name, so error handling that looked at the code or name sees the same thing. Schema validation now fails a store that lacks any of the three triggers. Payloads are never updated and a job's profile is never changed by Runwick, so this is a safety net either way.

Migration cost: on the 205,000-job store the whole migration took 1.9 s. Adding the new foreign key checks every payload, and the index rebuild reads `jobs`; writes to both tables wait meanwhile. The upgrade page now says so.

## Tests

- New `PayloadProfileAgreementTests` (PostgreSQL project): the composite key and unique index are gone; matching payloads (single and set insert) are accepted; a different profile on insert (single, and one row of a set), a payload profile update, and a profile change on a job with a payload are each refused with 23503 and the old constraint name; a same-value update and a change on a job without a payload are accepted; an orphan payload is still refused. Passes.
- `MigrationTests`: manifest at revision 29, kill-and-resume after each 0029 phase (expand 26, validate 29, activate 29). With the new test class: 85 of 85 passed.
- `RetentionTests` (PostgreSQL) and the new test: 15 of 15 passed. Contract `JobStatsContractTests` and retention contracts: 46 of 46 passed.
- Smoke: a 200-job drain on a fresh database at revision 29 reconciled; the 2,000-job drain on the retained copy reconciled with no diagnostics.
- Not run by me (for Sonnet): the full PostgreSQL and Contract projects, including `ReadinessPlanTests` at 10⁶ rows.

## Not done

- `jobs_state_list`, `jobs_created_list`, `jobs_queue_list` and `jobs_name_list` are written on every version. Making the state list partial would mean serving live-state filters (pending, running) from `jobs_ready` or `jobs_expired_lease` plus a sort, which costs in proportion to the backlog on every dashboard page. Folding `jobs_queue_list` and `jobs_name_list` (perf plan C1) needs the queue-filtered list rewritten as a per-name merge. Both change dashboard read cost, so they are left for a decision with list-latency measurements.
- Receipt compaction (`storage_commands_durable`, `storage_commands_ownership`): not changed. Their updates are already 78% and 69% HOT, and the remaining gains need the replay-proof work; not low risk enough for this migration.
- The detail-expiry read's error-size lookup uses `job_attempts_errors` with a filter on job ID (it reads every errored attempt of the application per row). That was true before this change and is unchanged; with many errored attempts it would want a keyed index on `job_attempts`.
- No timing claim. The paired drains below measure throughput and total WAL.

## Commands for Sonnet

Tests (run from the branch root, PostgreSQL via the test harness, Redis needed for the Contract project's Redis cases):

```bash
export PATH=/root/.dotnet:$PATH DOTNET_ROOT=/root/.dotnet
dotnet build Runwick.slnx -c Release -v q
dotnet test tests/Runwick.Postgres.Tests -c Release --no-build
dotnet test tests/Runwick.Contract.Tests -c Release --no-build
```

Paired drains on retained data (five pairs, 5,000 jobs, 205,000 retained, same shape as the keyed-claim run):

```bash
# base binaries: main-merge; candidate binaries: pg-index (both Release builds of tests/Runwick.Workloads)
docker run -d --name runwick-pgidx-sonnet -e POSTGRES_PASSWORD=pg -p 55631:5432 postgres:18 \
  -c shared_buffers=512MB -c max_connections=300 -c shared_preload_libraries=auto_explain,pg_stat_statements
eng/perf-pg-index/setup.sh <base-bin-dir> <pg-index>/src/Runwick.Cli/bin/Release/net10.0/Runwick.Cli <outdir>
eng/perf-pg-index/pairs.sh <base-bin-dir> <cand-bin-dir> <outdir>/pairs 5 5000
eng/perf-pg-index/walattr.sh pgi_ret <base-bin-dir> 2000      # optional: per-index WAL, base
eng/perf-pg-index/walattr.sh pgi_ret_c <cand-bin-dir> 2000    # optional: per-index WAL, candidate
docker rm -f runwick-pgidx-sonnet
```

Copy each `Release/net10.0` Workloads folder to its own directory first so a rebuild cannot change an arm mid-run. `pairs.sh` prints per arm: jobs/s, server WAL bytes and `jobs` index growth; compare medians and paired ratios.
