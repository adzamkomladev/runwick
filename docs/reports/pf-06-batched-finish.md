# PF-06 batched finish: FinishMany/1

Date: 2026-11-02. Branch `worktree-agent-a4c2693eeb62c8fc3`, from main `5946f6b`. Spec: "Changes (2026-11-02, PF-06 batched finish)" in 03, 04, 05 and 07.

## What changed

When a job ends, the worker tells the database with a **Finish** call: "this job succeeded" (or failed, or was cancelled). Before this change, every job needed its own Finish. A worker sends at most 2 Finishes at a time (its *settlement lane* is 2 wide), and on this host a Finish takes about 80–150 ms under load. So one worker could confirm only about 13–25 jobs per second, however many jobs it ran in parallel. That is why 64 workers drained a queue no faster than 16.

Claiming was already batched (ReserveAndBegin, the earlier PF-06 change). This change does the same for finishing.

There is a new store operation, **FinishMany/1**. One call finishes up to 32 jobs in one database transaction. Each job in the call (an *item*) behaves exactly like its own Finish:

- it keeps its own command ID and its own saved reply (*receipt*);
- it gets the same ownership check, the same retry decision, the same events;
- it gets its own outcome. If one item is refused (for example, its lease was taken over), the others still apply.

The worker now sends every Finish that is waiting at the same moment as one FinishMany. A lone Finish still goes out as a plain Finish. The settlement lane is still 2 wide.

## The numbers

**Round trips.** A *round trip* is one message to the database and one answer back. One FinishMany costs 2 round trips (the call, then COMMIT), whether it carries 2 items or 8. The test PERF-01 now checks this exactly, for 2 and for 8 items.

| Per job (Dispatch + claim + finish), *n* jobs per batch | Round trips |
|---|---|
| Before (batched claim, single Finish) | 4 + 2/*n* (4.5 at *n* = 4; 4.25 at *n* = 8) |
| After (batched claim, batched finish) | 2 + 4/*n* (3 at *n* = 4; 2.5 at *n* = 8) |

**Drain benchmark.** 2000 jobs are inserted, then the workers drain them: `Runwick.Workloads bench --mode drain --n 2000 --workers 16` (and `--workers 64`), against the `runwick-pg` server, a fresh database per run. Runs were interleaved: main, branch, main, branch.

| Run | Workers | main `5946f6b`: jobs/s (drain time) | branch: jobs/s (drain time) |
|---|---|---|---|
| round 1 | 16 | 28.4 (70 s) | **43.4** (46 s) |
| round 1 | 64 | 36.1 (55 s) | **53.9** (37 s) |
| round 2 | 16 | 16.7 (120 s) | **35.6** (56 s) |
| round 2 | 64 | 22.4 (89 s) | **122.0** (16 s) |

- The branch was faster in all four pairs: about 1.5 to 2 times at 16 workers, and 1.5 to 5.4 times at 64 workers.
- With the change, 64 workers now drain faster than 16 (54 and 122 jobs/s, against 43 and 36). On main, 64 workers gained little over 16. That was the symptom this change set out to fix.
- Every run finished all 2000 jobs, ran each job exactly once, replayed no Finish, and reported no diagnostics.

**Shortened P-01** (`perf`, as in `eng/run-perf.sh`): 100 jobs/s offered, one worker, 16 slots, 100 ms handler, 3 minutes measured. Interleaved: main, branch, main, branch.

| Run | Finishes/s | Jobs run | Worker CPU ms/job | Worker KiB/job | Drained? | Violations |
|---|---|---|---|---|---|---|
| main r1 | 19.92 | 3492 | 18.5 | 121.8 | yes | 0 |
| branch r1 | 30.19 | 5320 | 10.1 | 88.4 | yes | 0 |
| main r2 | 30.39 | 5300 | 13.3 | 121.4 | yes | 0 |
| branch r2 | 18.41 | 3188 | 14.4 | 91.1 | yes | 0 |

- **P-01 throughput did not show a clear change.** Main itself varied from 19.9 to 30.4 finishes/s between its two runs, and the branch from 30.2 to 18.4. The host noise is larger than any effect. In P-01 one worker with 16 slots is limited by how long every store call takes on this loaded host (claim, finish, commit), not only by the 2-wide settlement lane.
- **Memory per job fell by about a quarter** in both branch runs (88–91 KiB against 121–122 KiB). That is the steady, repeatable signal: one call carries several jobs.
- Batches were real: on the branch, the average FinishMany carried about 6 items (median 4–5, largest 16). In r1, 5870 of 6767 finishes went in 1001 batches; 897 went alone, when only one Finish was waiting.
- The floor (isolated single calls) does not use the batch and stayed within noise (Finish p50 18–27 ms in all four runs).

*Non-reference host*: a shared 8-vCPU KVM guest, with other agents' builds and tests running. Load average (1 min) was 8–15 during the runs; each run's load is recorded in the scratchpad `measure/load.txt` and in each `summary.md`. The drain benchmark went through docker-proxy to `runwick-pg`; P-01 used its own Testcontainers `postgres:18` at the container IP. Read the gains as "clearly better on the drain", not as exact factors.

## 1. Commits

| Commit | What |
|---|---|
| `4bce1a0` | The store: migration `0011_finish_many` (schema revision 11), `finish_many_v1`, shared Finish functions, `finish_v2`, the C# call, tests FM-01 to FM-07, PERF-01, manifest |
| `04198f8` | The engine: the completion coordinator sends queued Finishes as one FinishMany; fallback; `TimingStore`; FM-08 to FM-10 |
| `606f268` | Spec Changes entries in 03, 04, 05, 07 |
| `51c8bb9` | Fix found by the full suite: FM-10 delayed the batch call itself, so its batch was sometimes cut short before reaching the store. Now only the single Finish is delayed. The coordinator also records the last batch failure, for diagnostics. |
| `b85cda6` | Same cause in FM-08 and FM-09: the test hosts time a call out after 1.25 s, and the tests held the batch 1.5 s. They now hold only single Finishes. |
| `220dad0` | CAP-10 (an older test) settles each Finish alone, so it still builds the settlement queue it is about (section 6). 07 entry updated. |
| (this report) | This file |

## 2. How it works

**The Finish decision is written once.** Finish's per-job steps moved into shared SQL functions:

- `rw_finish_plan_v1`: given the job's state and the outcome, what the job becomes (succeeded, retrying, failed, cancelled) and why;
- `rw_finish_job_v1`: the ownership check, the retry delay, and closing the attempt.

The single Finish (`finish_v2`) and the batch (`finish_many_v1`) both call them. A released function is never changed in place, so `finish_v1` is dropped and replaced by `finish_v2`, which runs the same steps in the same order.

**What one FinishMany call does.**

1. It checks the bounds (1–32 items, no repeated command or job) and the gates.
2. For each item, in command-ID order, it checks that item's receipt. An item seen before replays its saved reply. An item with a changed body is a conflict.
3. It locks every job still to decide, in one statement, in UUID order.
4. It locks the unique keys of jobs that are ending for good, then every concurrency scope those jobs use, each in sorted order.
5. It reads the clock **once**, after the last lock.
6. For each item, in request order, it runs exactly Finish's decision and writes that item's receipt.
7. It replies with one outcome per item.

**Lock order.** The task listed "jobs, then scopes, then key/rate ranks". The project's lock order (04 §5) puts unique keys (rank 6) before concurrency scopes (rank 7), and the single Finish already locks in that order, so the batch follows 04 §5. Finish has no rate step, so there are no rate locks. FM-07 runs four batch finishers in reverse order and two renewers with single finishes, over shared and paired scopes: no deadlock.

**A refused item does not touch the others.** A single Finish that does nothing rolls its whole transaction back. Inside a batch, the other items must still commit, and a database function here has no sub-transactions. So when an item is refused, the batch deletes the empty receipt row it just inserted for it. Nobody else ever saw that row, so this looks the same as a rollback to everyone else.

**Fault hooks.** FinishMany has the test hooks `AfterLocksBeforeFreshTime` and `BeforeFirstMutation` once, and `AfterMutation` once per applied item (step = the item's position). They are stripped from production builds, like the others.

## 3. Receipts and a lost reply

**Why each item keeps its own receipt.** A Finish already has its command ID and its frozen body before the worker decides to batch it. The worker must be able to ask again about that exact Finish if it does not hear back. With one receipt per item, a plain Finish of the same command later replays what the batch recorded, and a batch that contains an already-recorded command replays it. The batch call itself has no receipt; its ID is only for tracing.

**If the whole batch reply is lost** (the network drops, or the call times out), the worker does not know which items committed. It does not retry the batch. It resolves **each item on its own**, by resending that item's own Finish command, exactly as it resolves a single lost Finish today:

- an item the lost batch committed finds its receipt and gets the saved reply back (*replayed*); nothing happens twice;
- an item the batch did not commit is decided now, as a normal Finish;
- after the item's deadline, an item that never applied gets `CommandExpired`, which means for certain that it did not apply.

Each item uses its own `CompletionResolutionBudget`, measured from when that Finish was first published. So the budget means what it meant before: the time allowed to settle one job.

The same per-item path is used when the batch reply is not `Applied`. If the store says it does not know FinishMany (`UnsupportedVersion`) or rejects the request (`InvalidRequest`), the worker also turns batching off for itself and writes a diagnostic.

## 4. When the worker batches

A completion worker that takes one Finish from its queue also takes every other Finish already waiting there, up to 32. Two or more go out as FinishMany; one goes out as a plain Finish.

There is **no waiting window** to collect more. Batching helps only when the settlement lane is the bottleneck. In that case Finishes are already waiting, because both lane calls are busy while new ones arrive. A wait of a few milliseconds would slow down every Finish on a quiet worker and would not make batches larger under load. The measured batches (about 6 items on average) show that queued Finishes are enough.

Each item keeps its place in the per-job ordering (its *arbiter* slot), exactly as a single Finish does. An item whose budget has already run out is not batched; it takes the single path.

## 5. Tests

New Postgres tests in `FinishManyTests` (07 Changes, FM-01 to FM-10). Every case ends with the ReferenceModel check of every committed history.

| Case | What it shows |
|---|---|
| FM-01 | Per-item isolation: one stale lease and one finished job are refused; the others apply, each with its own receipt. Bounds are checked. |
| FM-02 | Replaying a whole batch gives back the saved replies exactly; a single Finish of an item replays the batch's receipt; a changed body is that item's conflict only. |
| FM-03 | Lost batch reply: each item resolves alone; applied items replay; after the deadline the refused one is `CommandExpired`. |
| FM-04 | Mixed outcomes in one batch: succeeded, failed with retry, failed for good, cancelled by the user, cancel requested, and a keyed job that releases its key. |
| FM-05 | Fault hook: a crash after item 2 commits nothing; the same items then apply once. |
| FM-06 | A renewal racing a batched finish, both ways round: no deadlock, the right one wins. |
| FM-07 | Lock order under contention: no deadlock, every job finishes once. |
| FM-08 | The worker really batches when the lane is busy. |
| FM-09 | A worker's lost batch reply: every item resolves by its own receipt; no job runs twice. |
| FM-10 | Fallback: with the function missing, the worker turns batching off and settles every Finish alone. |

Also: MIG (migration 0011 in the kill-phase rows, manifest revision 11 with `FinishMany: [1]`), operation version list, and PERF-01 (FinishMany = 1 command for 2 and for 8 items).

## 6. Full suites

All runs used Testcontainers on the loaded host (load 5–15). Core and Outbox ran on `51c8bb9`. The commits after it change only Postgres test files and the 07 spec, so they cannot affect Core or Outbox. Postgres ran again on the final code commit, `220dad0`.

| Suite | Commit | Result |
|---|---|---|
| Core | `51c8bb9` | 387 of 387 passed |
| Outbox | `51c8bb9` | 36 of 36 passed |
| Postgres | `220dad0` | 316 of 317 passed; the one failure is timing (below) |

To sort each failure, I reran it alone, interleaved with main's build (`5946f6b`) at the same load.

**Final run, `220dad0`.** WRK-08 (`StoreOutageTests`, a store outage while 16 Finishes are pending): one Finish ran out of its time budget. The store took 34.8 s to come back, and the test's own comment says a restore after about 35 s legitimately expires the Finishes. Alone it passed 3 out of 3 on main and 3 out of 3 on the branch. **Cause: host load.**

**Earlier full runs, and what came of them.**

- **Caused by this change, now fixed:**
  - FM-10, FM-09 (new tests). The tests held the batch call 1.5 s, longer than the test hosts' 1.25 s call timeout, so a batch sometimes never reached the store. Fixed in `51c8bb9` and `b85cda6`. All ten FinishMany tests then passed 3 times in a row.
  - CAP-10 (`CapacityFaultTests`, an older test), once in three full runs. It needs 16 Finishes queued behind the 2-wide lane so that admission stops. Batching settles them in one or two calls, so the stop happened only when renewals also queued. Fixed in `220dad0`: that test's worker settles each Finish alone. It then passed 3 out of 3.
- **Not caused by this change:**
  - `CapacityShareTests.Queue_and_lane_shares_hold_…`: the same assertion (line 215) fails when run alone on **main** too (2 out of 2), and on the branch (2 out of 2). This test was already broken or load-sensitive before this change. It passed in two of the four full runs.
  - `RateAdmissionReplicaTests.A_crash_after_begin_…` ("peer did not emit 'atBarrier' in time"): passed alone 3 out of 3 on the branch. Main's copied build lacks the helper peer process, so it could not run there. The test never finishes a job through FinishMany. Cause: host load.
  - `EventCursorTests.A_slow_writer_…`: passed alone 2 out of 2 on both builds. Cause: host load.
  - TIME-12 (`WorkerFaultTests.Finish_receipt_found_…`): uses one slot, so its Finish is never batched. Passed alone 3 out of 3 on both builds. Cause: host load.
  - Outbox `Deadline_expiring_while_waiting_for_the_delivery_identity_lock_…`: passed alone 3 out of 3 on both builds. Cause: host load.

## 7. Not done, and limits

- **P-01 shows no clear throughput change** on this host; the noise between runs of the same build was about 1.5 times. A reference-host run would settle it.
- **No coalescing window.** This was a choice (section 4). If a future workload shows small batches while the lane is busy, a short window could be tried.
- **The settlement lane is still 2 wide**, as asked.
- **No test yet stops admission while batching is on.** CAP-10 now turns batching off to build its queue. A test that slows FinishMany itself until Finishes wait 5 s would cover the pressure stop with batching on.
- **`CapacityShareTests.Queue_and_lane_shares_hold_…` fails on main too.** It is not touched here and needs its own look.
- Nothing was pushed or merged.
