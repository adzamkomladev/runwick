# PostgreSQL and SQLite: delayed jobs picked up seconds late (fixed)

Written 2026-11-19. Follows the Redis fix in perf-round-2.md ("Redis: delayed jobs picked up seconds late (fixed)"). Branch: the worktree branch of this work, commits `8f2a42c` and `c5d394f` on top of main `a86e48f`.

## Short answer

On PostgreSQL and SQLite a delayed job sent to a worker that had been idle for a while could start long after it fell due: up to the idle back-off cap, which is 10 seconds plus or minus 20 % on PostgreSQL with a working listener, and 2 seconds plus or minus 20 % on SQLite. Both stores now do what Redis does.

- The readiness check now tells the worker when the next delayed or retrying job is due, so an idle worker sleeps until exactly then.
- A delayed dispatch that becomes the earliest waiting job of its queue and priority wakes the idle worker once it has committed. The worker then runs a readiness check and learns the due time.
- Jobs queued behind an earlier delayed job wake nothing, and an empty store gets no extra polling.

On PostgreSQL, at 100 delayed offers per second, five seconds ahead, the median p99 wait past due fell from 8.2 s to 2.6 s over six paired rounds, and the worst round from 14.1 s to 4.4 s. Traced runs at the same host load show the wake arriving within 50 ms and the worker sleeping until the due time. They came in at 0.4 to 0.8 s p99. The remaining seconds in the untraced rounds came from the workers falling behind on a host with a load average of 17 to 28 (p50 up to 1.4 s, not just the first jobs), not from an idle sleep. On SQLite the old worst case was already bounded at 2.4 s, and on this host the tails of both arms (1 to 3 s at p95) come from write and fsync stalls, so the benchmark shows no clear difference. A traced run confirms the wake and the sleep until due. Drain throughput did not drop on either store.

## Cause

The worker engine was already right (perf-round-2.md, Redis section): an idle pool backs off up to its cap between readiness checks, and sleeps until `NextDueAt` when the check names one. Two pieces were missing in the PostgreSQL and SQLite stores:

1. Their readiness check never filled in `NextDueAt`. So an idle pool waiting for a delayed job could only keep backing off, up to the cap.
2. A delayed dispatch sent no wake. Only jobs that were due right away did (PostgreSQL `NOTIFY` after commit, SQLite's in-process hint). A worker that had reached the cap before the first delayed job arrived slept through its due time.

While measuring the first version of the fix, a third problem showed up. The PostgreSQL wake sender skips a send when the background connection lane (two connections, shared with readiness checks and presence) is busy, and until now it dropped the hints it held. For jobs that are due now this costs little, because the next dispatch sends again. For delayed jobs it loses the one hint that matters: the jobs queued behind the first one never send it again. In the first six benchmark rounds this left two fix rounds with multi-second tails (5.2 s and 8.4 s p99).

## Fix

**Readiness check (`NextDueAt`).** Both stores look up, for each probed lane that is not paused, the first `pending` or `retrying` job due after the check time, and return the earliest of them.

- PostgreSQL: one more branch in the single probe statement, an index-only lookup on `jobs_ready` per lane (`ORDER BY eligible_at LIMIT 1`), aggregated to one `next` row.
- SQLite: one indexed query per lane on `runwick_jobs_ready`.

The value covers any job, including types this worker cannot run. A wrong guess costs one extra check, never correctness.

**Wake for a delayed dispatch.** Both stores follow their existing wake path and add the "earliest" check:

- PostgreSQL. `Dispatch` and `DispatchMany` hand a committed, not-replayed delayed creation to the existing post-commit wake sender. That sender already runs off the dispatch path, at most once per 100 ms. Per lane it keeps the creation due first. Its one statement then sends `NOTIFY` for that job's queue only if the job is still waiting and no not-yet-due job of its lane comes before it in claim order (`eligible_at`, `job_id`). That takes one unique-index lookup and one `jobs_ready` range bounded on both sides, 7 buffers at a million rows. A send that finds the lane busy now keeps its hints for the next try.
- SQLite. After commit, never on replay, the in-process hint now also names the queue of a delayed creation that, read after the commit, is the earliest not-yet-due job of its lane. This is one indexed read per lane with a delayed creation in the call, and only when a worker in the same process is listening. Each queue is still named at most once per call. A failed read sends no hint: the dispatch has committed and polling covers it.

**Why the "earliest" check, and not a simpler "wake on every delayed dispatch".** On PostgreSQL the simpler rule would cost a `NOTIFY` per lane per 100 ms during a delayed stream (PostgreSQL serialises commits that send notifications), plus a readiness check on every listening idle worker per wake. The earliest check uses the same number of statements, runs in the background sender rather than in the dispatch call, reads 7 buffers, and almost never notifies: in a steady stream every new job is queued behind an earlier one. So it costs less and wakes only when the worker could not know. On SQLite the check is one indexed read, cheaper than the readiness check (several queries per lane) that every wake would trigger.

**Retries.** No store hint is added for a retry. A retry is scheduled by a finish in the same process, whose freed slot wakes its own pool, and with this change that pool's next readiness check (within about 100 ms) names the retry's due time. So a running worker starts its own retries on time.

**No schema change, no migration.** No SQL function changed; only statements sent by the driver. Older binaries on the same database are unaffected.

## Tests

New or changed, each run on main's code and on the fix:

| Test | Main | Fix |
|---|---|---|
| `WakeNotificationTests.A_dispatch_notifies_when_it_is_due_now_or_the_earliest_delayed_job_of_its_lane` (PostgreSQL; the Redis test's pattern plus two batch steps) | fails (no wake for the first delayed job) | passes |
| `SqliteDispatchGroupingTests.Local_work_hint_names_a_queue_when_a_job_is_due_now_or_the_earliest_delayed_job_of_its_lane` | fails | passes |
| `CoreContractTests.The_probe_names_the_earliest_not_yet_due_job_of_the_probed_lanes` (all three stores; probed lanes only, a due job does not hide it, a retrying job counts) | fails on PostgreSQL and SQLite, passes on Redis | passes |
| `WakeNotificationTests.A_delayed_wake_held_up_by_a_busy_background_lane_is_sent_once_the_lane_frees` | not run (main has no delayed wake; without `c5d394f` the held hint is dropped, so the test should fail, but that was not run) | passes |
| `WakeNotificationTests` WAKE-01: the first delayed job now wakes, the one queued behind it does not | | passes |
| `SqliteDispatchGroupingTests` batch hint: a delayed item queued behind an earlier one wakes nothing | | passes |
| `ReadinessPlanTests` (PGX-07): the probe's new branch and the wake check use only `jobs_ready` (and the job's unique index) at 10⁵ and 10⁶ rows, with bounded buffers, and the probe names the delayed job behind a due backlog | | passes |

Full runs with the fix (PostgreSQL 18 and Redis 8.2 in dedicated containers, on a shared host):

| Suite | Total | Passed | Failed | Skipped |
|---|---|---|---|---|
| Core | 454 | 454 | 0 | 0 |
| Contract (all three stores) | 499 | 499 | 0 | 0 |
| Sqlite | 56 | 56 | 0 | 0 |
| Postgres | 403 | 398 | 3 | 2 |

The three PostgreSQL failures (`CapacityOutageTests`, `ScalerOutageTests`, `StoreOutageTests`) and two skips (`requires-docker`) need a server that the test harness starts and restarts itself. With `RUNWICK_TEST_PG` set they stop at setup ("RestartStore needs a container") before any store code runs. They were not run with a harness-managed container, because this work only used containers named `rw-dlypg-*`.

## Probe cost

Plans from PGX-07 at 10⁶ jobs (10 % pending and due, 12 lanes, one delayed job):

| | Main | Fix |
|---|---|---|
| Probe, shared buffers | 110 | 148 (the new branch: 38, index-only, 12 lookups) |
| Wake check, shared buffers | none | 7 (at 10⁵ rows as well) |

Wall-clock timing of 500 probes, 4 lanes, 2,000 jobs, three alternating rounds per arm, median of seven samples each. The host load average was 16 to 22, so these numbers are mostly noise:

| | Main rounds | Fix rounds |
|---|---|---|
| PostgreSQL, µs per probe | 8,263 / 8,120 / 7,335 | 9,961 / 10,610 / 7,807 |
| SQLite, µs per probe | 581 / 1,296 / 1,516 | 827 / 1,518 / 1,381 |

The fix adds one index descent per lane, which the buffer counts measure exactly. The timings show no change larger than the noise between rounds of the same arm. The drain check below is the throughput test.

## Benchmarks

Delayed workload: 100 offers/s for 30 s, each due 5 s after its offer, 16 workers, 16 callers, after a 12 s warm-up so the worker reaches its idle cap. Paired rounds, arm order alternating each round, every run under `flock /tmp/runwick-bench.lock`, a fresh database per run. "Main" is `a86e48f`, "fix" is this branch. Wait past due is `latencyMs − 5000` in ms, measured from the dispatch call to the finish, so it slightly overstates the wait. The load is the 1-minute load average before and after each run.

**PostgreSQL 18** (fsync on, a dedicated container), with the busy-lane fix:

| Round | Load main | Load fix | Main p99 | Main max | Fix p99 | Fix max | Main jobs/s | Fix jobs/s |
|---|---|---|---|---|---|---|---|---|
| 1 | 27.9/24.7 | 24.7/22.8 | 14,072 | 14,228 | 2,878 | 3,022 | 62.1 | 83.9 |
| 2 | 19.6/19.5 | 22.8/19.6 | 5,997 | 6,124 | 2,385 | 2,496 | 85.6 | 84.9 |
| 3 | 19.5/19.1 | 19.1/21.3 | 6,555 | 6,669 | 4,447 | 4,543 | 85.4 | 76.7 |
| 4 | 23.2/18.4 | 21.3/23.2 | 8,042 | 8,150 | 3,559 | 3,717 | 80.2 | 78.0 |
| 5 | 18.4/18.3 | 18.3/17.6 | 8,352 | 8,479 | 458 | 557 | 85.6 | 85.6 |
| 6 | 17.3/21.5 | 17.6/18.1 | 13,133 | 13,232 | 1,610 | 1,768 | 71.2 | 85.6 |
| **Median** | | | **8,197** | **8,315** | **2,632** | **2,759** | 82.8 | 84.4 |

The first set of six rounds, before the busy-lane fix, at load 13 to 27: main median p99 7,999 ms (worst 13,852), fix 1,671 ms (worst 8,412; the two slow fix rounds were the dropped hints described above).

In the fix rounds above, p50 was 77 to 1,410 ms, against 1,248 to 13,032 ms on main. When the fix is slow it is slow across the whole run: the workers fall behind at this load. Three traced fix runs (temporary build, not committed) at load 17 to 22 gave p99 542, 791 and 392 ms. In them the first delayed offer's `NOTIFY` arrived 48 ms after the dispatch, and the pool's readiness checks named the due time and slept until it.

**SQLite** (WAL, `synchronous=FULL`, host disk):

| Round | Load main | Load fix | Main p99 | Main max | Fix p99 | Fix max | Main jobs/s | Fix jobs/s |
|---|---|---|---|---|---|---|---|---|
| 1 | 12.6/12.5 | 12.5/14.1 | 1,445 | 1,670 | 1,396 | 1,571 | 85.7 | 85.2 |
| 2 | 11.4/14.4 | 14.1/11.4 | 830 | 972 | 319 | 417 | 85.6 | 85.7 |
| 3 | 14.4/26.6 | 26.6/21.0 | 3,301 | 3,408 | 2,759 | 2,846 | 80.4 | 80.0 |
| 4 | 18.8/15.4 | 21.2/17.8 | 1,475 | 1,686 | 3,385 | 3,507 | 85.6 | 76.3 |
| 5 | 15.4/11.6 | 11.6/12.6 | 1,059 | 1,241 | 529 | 604 | 85.6 | 85.7 |
| 6 | 13.4/12.5 | 12.6/13.4 | 1,071 | 1,252 | 703 | 774 | 85.6 | 85.5 |
| **Median** | | | **1,258** | **1,461** | **1,049** | **1,172** | 85.6 | 85.4 |

On SQLite, main's idle miss was at most 2.4 s, and the tails here reach p95 in both arms (for example fix round 4, p95 3,128 ms), so they are write-lock and fsync stalls on a loaded disk rather than the first-batch miss. A traced fix run at load 8 gave p99 192 ms and max 533 ms. The pool was idle at its 2 s cap with no due time, the first delayed dispatch's hint woke it 448 ms into a 2,048 ms wait, the readiness check returned the due time 4.6 s ahead, and the pool slept exactly until it.

Every run settled every offer except a few rounds of both arms where up to 7 % were still unfinished at the end of the 5 s grace period on the busiest host (the same pattern in main and fix).

**Drain check** (20,000 jobs dispatched first, 16 workers, jobs/s). A drain has no delayed jobs. The only new cost on it is the probe's extra lookup.

| Store | Main (rounds) | Fix (rounds) | Median main / fix |
|---|---|---|---|
| PostgreSQL (load 5 to 16) | 246, 263, 215, 265, 298, 294, 207 | 212, 209, 198, 273, 312, 296, 283 | 263 / 273 |
| SQLite (load 7 to 14) | 245, 286, 241 | 238, 274, 246 | 245 / 246 |

The first three PostgreSQL rounds favoured main and the next four favoured the fix. Within each arm the rounds spread far more (198 to 312) than the medians differ, so neither store shows a throughput drop.

## What's left

- **Paths without the delayed wake.** Delayed jobs created by staging in a caller's transaction, outbox delivery, management dispatch and schedule materialization still wake only when they are due now. On PostgreSQL these go through the transactional `rw_dispatch_core_v3`, and changing it needs a migration that replaces that function. An idle worker still learns their due time from any readiness check (`NextDueAt` now covers every job), so the worst case is one back-off step after the dispatch, up to the cap.
- **Retries by another process.** A retry is started on time by the process that scheduled it. Retries released by recovery in another process, a queue resumed elsewhere, and limit room freed elsewhere are still found by polling (05, idle caps).
- **SQLite across processes.** The hint is in-process only, as before. A worker in another process learns the due time from its next readiness check (cap 2.4 s).
- **A quiet-host rerun.** The host load average was 12 to 28 for every benchmark. A rerun on a quiet host would show how far the PostgreSQL fix rounds drop once the workers keep up (the traced runs suggest well under a second).
