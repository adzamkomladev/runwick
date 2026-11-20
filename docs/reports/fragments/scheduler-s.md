# Scheduler lag: cutting the database cost of a schedule group

Written 2026-11-19 and 2026-11-20. Plan item "Scheduler S" in `docs/plans/perf-plan-2-remaining.md`: P-06 scheduler lag p99 ≤ 13 s with 1,000 minute schedules at the default 100 jobs/s allowance, measured from the scheduled instant to durable materialization, with every occurrence accounted for.

## Short answer

**The 13 s gate is met, by main and by this branch, on this host (8 CPUs, not the reference host).** Across 20 paired runs on the branch, the worst p99 was 12.85 s (groups of four, 1-minute load average about 10) and 10.89 s with groups of eight. Main met the gate in 15 of its 16 runs; the exception was a group-of-eight run during a burst of other activity on the host (load average median 28, peak 108), where it fell 520 occurrences behind and its p99 went past 60 s. The branch's group-of-eight run in the same round, at load median 21 and peak 37, kept p99 at 10.58 s.

What changed is the database cost of a schedule group. Migration 0026 writes a whole group with one statement per table instead of running the single-plan path once per plan. Server time per plan fell from 6 to 11 ms to 2 to 3 ms with groups of eight, and from 8 to 11 ms to 4 to 5.6 ms with groups of four. WAL per plan did not change (about 5.5 to 6 kB). Lag p99 hardly moves on a calm host, because it is pinned near 10 s by the 100 jobs/s allowance (1,000 schedules ÷ 100/s), not by the database. The cut buys headroom: a group of eight now needs about 20 ms of database time per 80 ms of allowance, against 50 to 90 ms before.

Every occurrence was accounted for in every run. There were no duplicates, no orphan jobs and no shared jobs. Every expected occurrence was materialized, apart from the 520 in the main run above, which are counted as unmaterialized. Every catch-up completed without needing review.

## Where the time goes

All 1,000 schedules fall due at the top of each minute. The scheduler releases at most 100 plans per second (burst 32), so the last schedule of a minute cannot be materialized before about 10 s. That is the floor of p99; the 13 s gate leaves 3 s on top of it. Whether a run meets the gate depends on whether the database keeps up with 100 plans per second. Grouping runs at most two groups at a time, so a group of eight must finish its function and commit in about 160 ms, and a group of four in about 80 ms.

I measured one group of eight on current main (86c0407) with `pg_stat_statements` tracking nested statements, on a quiet host (load 3 to 7). The group function took 6.3 ms of server time per plan, plus about 13 to 21 ms of commit per group. Per plan, it ran about fifteen statements and three nested function calls:

| Per plan, inside `materialize_schedule_group_v2` | Server time | WAL |
|---|---:|---:|
| `rw_dispatch_core_v3` call (the generic dispatch path, for one keyless job) | 3.09 ms | 2.5 kB |
| of which: insert into `jobs` (8 secondary indexes) | 0.48 ms | 0.9 kB |
| of which: insert into `job_payloads`, `job_generations` | 0.59 ms | 1.0 kB |
| of which: `dispatched` event | 0.19 ms | 0.6 kB |
| of which: key sorting, queue check, result JSON and the rest of the call itself | about 1.8 ms | — |
| receipt insert (`rw_receipt_v1`) and finalize (`rw_finalize_v1`) | about 0.75 ms | 1.0 kB |
| occurrence insert, schedule lock and update, `schedule_materialized` event | 0.6 ms | 1.9 kB |
| group overhead per plan: passing 50 arguments into the single-plan core, savepoints, reply JSON | about 1.3 ms | — |
| **Total** | **6.3 ms** | **5.5 kB** |

What this says:

- **The cost is not I/O.** A whole group of eight touched about 875 shared buffers and wrote about 43 kB of WAL on main, and about 765 buffers and 39 to 46 kB after the change (nested `auto_explain` with `ANALYZE, BUFFERS, WAL`). Every statement was an index lookup or an insert into a few indexes; nothing read from disk.
- **The cost is per-statement and per-call overhead.** Of the 6.3 ms, the inserts themselves are about 2 ms. The rest is the executor and PL/pgSQL running a generic path one plan at a time: about fifteen statements, three nested function calls with up to 50 array arguments each, a savepoint per plan, and JSON built and merged per plan.
- **Triggers and indexes are not the problem.** The only triggers on these rows are the foreign-key checks (0.02 ms each) and the deferred decided-receipt check (0.02 ms). The eight secondary indexes on `jobs` serve the worker claim and the dashboard; dropping any of them would move cost to those paths, so I left them.
- **Evaluation is cheap**: 0.06 to 0.09 ms per plan.

On a loaded host every one of those statements slows down together, which is why the 2026-11-15 rerun saw 12 to 16 ms per plan and missed the gate.

## What changed and why it is safe

Migration `0026_schedule_group_set` (schema revision 26) adds `materialize_schedule_group_v3`. The driver sends every group of two to eight plans to it. Nothing else changes: no table, index, trigger, event kind, receipt format or option, and the scheduler loop is untouched.

For a group of new plans, version 3 does the same work with one statement per table for the whole group:

1. Checks, before writing anything, that the group is the easy case: no command has a receipt yet, no deadline has passed or is too far out, no client-side refusal, every queue registered, the `scheduler` capability active, no server fault hooks armed.
2. Inserts all receipts in command-ID order, then locks all schedule rows in schedule-ID order (the same lock order as version 2).
3. Runs the same plan checks as before against the locked rows: not found, disabled, paused, needs review, revision conflict, plan too large, occurrence below the high-water mark, cursor rewind. A refused plan gets the same reply as before and its receipt is deleted, so the scheduler can replan it.
4. For the accepted plans, in job-ID order: one insert each into `jobs`, `job_payloads`, `job_generations` and `schedule_occurrences`, one update of `schedules`, one insert of all events (per plan, `dispatched` then `schedule_materialized`, the order version 2 writes), the wake notification, and one receipt finalization.

Every other case goes to version 2, unchanged: a replay after a lost reply (a receipt exists), expiry, a clock behind the cutoff, an existing occurrence row, a duplicate job ID, or a deadline passed during the writes. If version 3 finds such a case after it has written, it raises an error inside its own savepoint, which undoes all of its writes, receipts included, and then calls version 2 with the original request. So those cases keep the outcomes they had and the tests they had.

Why the guarantees hold:

- **Replay is exact.** The receipt is written with the same command ID, fingerprint, deadline, retention and result as before; a resend finds it and version 2 replays the stored result. A test drops the reply of a whole group written by the new path, then resends it as a group and one command alone: every plan comes back as a replay with its original job, and nothing new is written.
- **No duplicate occurrences, no cursor rewind.** The occurrence primary key, the high-water check and the revision check are the same as before. An occurrence that already exists sends the whole group back to version 2.
- **Pause and edit races.** Plans are checked against the schedule row after it is locked, as before; a pause or an edit between planning and writing refuses that plan only.
- **Catch-up limits, pacing, DST and fairness are unchanged.** Groups carry only single-occurrence plans with no catch-up episode, review, resume policy or audit range; catch-up and every other plan shape still use the single-plan path, and planning (including DST handling) happens in the scheduler as before.
- **Rolling upgrade.** Versions 1 and 2 stay. A client at revision 25 or older keeps calling them; this client calls version 3 only.
- **Plans stay index lookups.** `auto_explain` on the first draft showed two statements scanning all 1,000 schedules of the application and the receipt finalize scanning the receipt partition, because the planner had stale statistics. Every statement now names its rows by key (`= ANY` of an array), and the second sample showed index lookups only.
- **Shared tables.** No table, index or trigger changed, so the worker claim, finish and dispatch paths run exactly the SQL they ran before. I did not rerun the worker drain benchmark for that reason.

## Tests

New PostgreSQL contract tests in `tests/Runwick.Contract.Tests/ScheduleContractTests.cs` (they take version 2 away during the call, so a group that does not finish in the new path fails the test):

- A clean group of eight is written by the new path; its rows, receipt results and event details match a single-command materialization field for field; each plan's `dispatched` event directly precedes its `schedule_materialized` event; after a lost reply the resend (as a group and alone) replays every plan with its original job and writes nothing.
- A group with one stale plan (control revision moved after planning) and one paused plan: those two are refused with `revision_conflict` and `schedule_paused`, keep no receipt and no effect, the other two apply, and the paused command applies when resent after resume.
- Rollback partway through a group: an occurrence row the plan does not know about makes the new path fail after writing jobs, payloads and generations for all three plans. Nothing it wrote survives; version 2 then applies two plans and refuses the third with `occurrence_exists`.

The two existing group tests (rollback of one item after mutation with replay after a lost commit reply, and overlapping groups sharing a command) pass through the new function and its fallback.

| Suite | Result |
|---|---|
| Core (`Runwick.Core.Tests`) | 455 / 455 passed |
| Schedule contracts on PostgreSQL, SQLite and Redis (`ScheduleContractTests`) | 76 / 76 passed (73 before plus 3 new) |
| Full PostgreSQL suite (`Runwick.Postgres.Tests`) | 405 / 406 passed. The one failure, `CapacityShareTests.Queue_and_lane_shares_hold…` (a timing range check on capacity shares, which this change does not touch), passed 6 / 6 when its class was rerun alone |
| PostgreSQL migration and upgrade tests (`*Migration*`, `*Upgrade*`) | 92 / 92 passed, including kill-and-resume at each phase of 0026 |

Final counts are on the branch rebased onto main 997b8a0. Redis ran on a dedicated `redis:8.2-alpine` with `appendfsync always`; PostgreSQL tests started their own containers.

## Benchmark tables

All runs used P-06s: 1,000 minute schedules, the default 100 jobs/s allowance, two workers, grouping on with two groups in flight, and a fresh PostgreSQL 18.6 container per run (`fsync` and `synchronous_commit` on). Main is 86c0407, the commit before 0026. The branch was measured at its key-array fix, and the rebase onto 997b8a0 changed nothing in the measured path. Arm order was rotated each round, and every run held `flock /tmp/runwick-bench.lock`. "Server ms/plan" is the top-level `pg_stat_statements` time of the group function divided by the plans it wrote, so unlike the nested profile above it includes everything. Load is the 1-minute load average on this 8-CPU host, sampled every 30 s. No run saw an evaluation-budget failure.

### Full runs: 15-minute steady window plus a 1,000-occurrence catch-up

| Arm | Load (median, max) | Acknowledged | p50 | p95 | p99 | Server ms/plan | WAL/plan | Catch-up, restart to last occurrence |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| main, groups of 4 | 3.5, 7.7 | 15,000 / 15,000 | 5.28 s | 10.27 s | 10.86 s | 7.74 | 5,922 B | 41 s |
| main, groups of 8 | 4.4, 9.7 | 15,000 / 15,000 | 5.34 s | 10.32 s | 11.23 s | 8.17 | 6,075 B | 45 s |
| branch, groups of 4 | 3.3, 7.1 | 15,000 / 15,000 | 5.35 s | 10.36 s | 11.24 s | 4.18 | 5,991 B | 52 s |
| branch, groups of 8 | 2.6, 12.3 | 15,000 / 15,000 | 5.28 s | 10.34 s | 10.86 s | 2.36 | 6,248 B | 41 s |

Each catch-up emitted all 1,000 missed occurrences (1,001 to 1,005 including occurrences due during the restart), in chunks of at most 32, with `needsReview` false. All four arms had 0 duplicates, 0 orphan jobs, 0 shared jobs and 0 violations.

### Short paired runs: 2-minute warm-up, 3-minute window, three rounds

p99, with the 1-minute load median in brackets:

| Round | main, 4 | main, 8 | branch, 4 | branch, 8 |
|---|---:|---:|---:|---:|
| 2 | 11.18 s (8.7) | 11.43 s (6.3) | 10.63 s (11.9) | 10.89 s (4.1) |
| 3 | 10.48 s (12.9) | **> 60 s, 520 of 3,000 not materialized** (27.8, peak 108) | 10.82 s (8.2) | 10.58 s (20.8) |
| 4 | 10.49 s (11.6) | 10.77 s (11.0) | 12.85 s (9.8) | 10.60 s (11.6) |

| Arm | Server ms/plan, rounds 2 to 4 | WAL/plan |
|---|---|---|
| main, 4 | 8.8, 8.2, 10.1 | 5.6 to 5.8 kB |
| main, 8 | 6.3, 18.6, 8.7 | 5.6 to 6.0 kB |
| branch, 4 | 4.7, 3.9, 5.0 | 5.4 to 5.7 kB |
| branch, 8 | 2.0, 3.0, 3.1 | 5.5 kB |

### Synthetic load: eight busy-loop processes beside each run

Same short shape, two rounds, with eight CPU-bound processes started for each run (load median 17 to 33):

| Arm | p99, round 1 | p99, round 2 | Server ms/plan | Load median |
|---|---:|---:|---:|---|
| main, 4 | 10.95 s | 10.39 s | 10.7, 10.5 | 26.4, 19.1 |
| main, 8 | 10.17 s | 10.37 s | 10.1, 10.4 | 28.0, 17.5 |
| branch, 4 | 10.12 s | 10.11 s | 4.5, 5.6 | 31.9, 19.0 |
| branch, 8 | 10.01 s | 10.25 s | 2.3, 2.8 | 32.9, 25.5 |

Pure CPU load roughly doubles main's per-plan cost, but it still fits in the allowance, so both builds pass. The one failure (main, round 3 above) came with a load spike far above the core count, which points to I/O or memory pressure from other work on the host rather than CPU alone. I can't reproduce that on demand, so treat it as one data point, not a rate.

## What's left

- **Reference-host run.** These numbers come from a shared 8-CPU development host. A run on the reference host would confirm the gate where it is officially judged.
- **Group size default.** `MaterializationGroupSize` stays at 4 and `GroupMaterializations` stays off by default. Groups of eight now cost about half as much per plan as groups of four. Whether to turn grouping on by default, or raise the size, is a separate decision; I changed no defaults.
- **Replica claim (step 4).** Not built. The database is no longer the limit at the default allowance, so an opt-in replica claim isn't needed for this gate.
- **The fallback path.** Replays, expiry, existing occurrences and server fault hooks still go through version 2 one plan at a time, at the old cost. They are rare in steady operation.
- **SQLite and Redis** are unchanged. This work is PostgreSQL only.
