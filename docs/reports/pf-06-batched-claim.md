# PF-06 batched claim: ReserveAndBegin/1

Date: 2026-11-02. Branch `worktree-agent-a000c0616153f2099`, from main `a78f821` (PF-05). Spec: "Changes (2026-11-02, PF-06 batched claim)" in 03, 04, 05 and 07.

## What changed

Before this change, a worker claimed one job at a time. A claim was two store calls: Reserve (take the job) and then Begin (start it). The pool allowed only one Reserve in flight at a time. So a worker with 16 free slots filled them one by one, two calls per job. That serial claim was the ceiling on how fast a worker could start jobs.

Now there is a new store operation, **ReserveAndBegin/1**. One call claims up to 32 jobs and begins each of them, in one transaction. Each job gets its own outcome. If one job is blocked or deferred, the others are not affected. A worker with more than one free slot now uses it to fill those slots in one call.

## The numbers

**Round trips.** One call costs 2 round trips (the call, then COMMIT), whether it claims 2 jobs or 8. A *round trip* is one message to the database and one answer back.

| Per job (Dispatch + claim + Finish) | Round trips |
|---|---|
| Before: Reserve + Begin | 8 |
| After: one ReserveAndBegin for *n* jobs | 4 + 2/*n* (5 at *n* = 2; 4.5 at *n* = 4; 4.25 at *n* = 8) |

PERF-01 now checks `ReserveAndBegin` = 1 command for 2 jobs and for 8 jobs. These counts are exact, not measured.

**Throughput.** This comes from the shortened P-01 run: 100 jobs/s offered, one worker, 16 slots, 100 ms handler, 3 minutes measured. Runs were interleaved: main, after, main, after.

| Run | Finishes/s | Jobs run | Worker CPU ms/job | Worker KiB/job | Drained? | Violations |
|---|---|---|---|---|---|---|
| main r1 (`a78f821`) | 6.96 | 1219 | 28.5 | 218 | no | 0 |
| **after r1** | **23.45** | 4091 | 13.9 | 128 | yes | 0 |
| main r2 | 13.95 | 2406 | 25.9 | 214 | no | 0 |
| **after r2** | **27.06** | 4755 | 13.3 | 121 | yes | 0 |

- Confirmed finishes per second rose about **2 to 3.4 times**. It went from 7–14/s to 23–27/s. Both "after" runs drained their backlog; neither main run did.
- Worker CPU and allocation per job fell by about half. The worker sends one call for several jobs instead of two calls per job.
- In the "after" runs the average batch claimed about 3.7 jobs (largest 16). r1 had 1357 batches for 5125 jobs; r2 had 1652 batches for 6148 jobs. A few single Reserve + Begin claims remain (291 and 329): those happen when only one slot was free.
- *Non-reference host*: the shared 8-vCPU KVM guest, with other agents' builds and tests running. Load (1 min) was 10–13 during these runs. Each run used its own Testcontainers `postgres:18` at the container IP, so docker-proxy was not in the path. Main's two runs differ by 2 times, so read the gain as "large", not as an exact factor. Output: `artifacts/perf/pf06-ab-{main,after}-{1,2}` (local, not committed).

**Not changed.** The floor (isolated single calls) and the floor client's cost per job do not use the batch, and they stayed within the run-to-run noise:

| | main r1 | after r1 | main r2 | after r2 |
|---|---|---|---|---|
| Reserve p50 ms | 32.3 | 35.6 | 36.4 | 29.1 |
| Begin p50 ms | 23.5 | 25.0 | 29.5 | 30.0 |
| Floor client CPU ms/job | 41.5 | 55.5 | 54.6 | 43.9 |
| Idle worker statements/s | 2.52 | 2.34 | 2.53 | 2.52 |

## 1. Commits

| Commit | What |
|---|---|
| `aea8ca9` | The store: migration `0010_reserve_and_begin` (schema revision 10), `reserve_and_begin_v1`, shared per-job functions, `reserve_v2`/`begin_v2`, the C# store call, tests RAB-01 to RAB-08, PERF-01, PGX-07, manifest |
| `5c8ce00` | The engine: a worker with more than one free slot claims them in one call; fallback; `TimingStore`; RAB-09, RAB-10 |
| `cc44d0a` | Spec Changes entries in 03, 04, 05, 07 |
| `55d83fb` | Fix found by the full suite: batch only when one lane is ready (lane shares), and TIME-09 keeps testing the single path |
| `6f4a935` | Cherry-pick of main's `8201cb0` (fault-session hang fix in the test harness), as the coordinator asked |

## 2. How it works

**One function, written once.** Reserve's and Begin's per-job steps moved into two shared SQL functions:

- `rw_reserve_job_v1`: capacity check, fences, attempt, job, permits, event;
- `rw_begin_job_v1`: ownership check, cancel or budget close, the rate step with its committed deferral, start.

The single operations (`reserve_v2`, `begin_v2`) and the batch (`reserve_and_begin_v1`) all call them. So the rate step and the deferral reply exist once, not as copies. A released function is never changed in place, so `reserve_v1` and `begin_v1` are dropped and replaced by the `_v2` versions. They run the same statements in the same order. The migrator now understands `DROP FUNCTION`.

**What one call does.**

1. It runs the usual gates and receipt checks.
2. It locks the queue control row.
3. It claims up to *n* waiting jobs with `SKIP LOCKED`: a row another worker holds is skipped, never waited for.
4. It locks, in rank order:
   - the unique keys of jobs whose Begin will close them;
   - every concurrency scope of every claimed job, in UUID order;
   - every rate scope of the rated jobs, in UUID order.
5. It reads the clock **once**, after the last lock.
6. For each job, in order, it runs Reserve's decision, then Begin's.
7. It writes one receipt.

**Guarantees kept.**

- **Fencing per job.** Each job gets its own permit and fencing value. A later job in the batch sees the earlier jobs' permits, so a scope never goes over capacity.
- **Lock order.** The task asked for "claimed jobs in UUID order". The batch takes the jobs in queue order instead, but with `SKIP LOCKED`. A lock that never waits cannot be part of a deadlock. Every lock that can wait comes after the job locks, in the ranked order, each rank in one sorted statement. Single Reserve already works this way. RAB-08 ran 4 batch claimers and 2 single claimers over shared scopes, with pairs of scopes in both orders: no deadlock.
- **Fresh time.** The clock is read once, after the last lock.
- **`synchronous_commit`** is unchanged: the same commit as every other operation.
- **Receipts.** The call writes **one receipt for the whole command**, holding every job's outcome in `items[]`. It does not write one receipt per job. Reason: in the protocol's model (03 §8.2) a receipt is one row per command ID. A resent command must get back exactly the reply the first one got. Per-job receipts would need several command IDs, and a lost reply could then come back only in part. `DispatchMany` already works this way. The receipt is in the *ownership* class (60 s cap, kept 10 minutes), like Reserve and Begin, because it holds leases.

**Outcomes per job.**

| Outcome | Meaning |
|---|---|
| `Applied` | reserved and started |
| `CapacityBlocked` | a scope was full; nothing written for this job |
| `RateLimited` | reserved, then deferred by its rate limit (no rate charge) |
| `InvalidState` / `BudgetExhausted` | a cancel request or a spent budget closed it |

If every candidate is blocked, nothing is written and the reply looks like a blocked Reserve. Any broken invariant rolls the whole command back.

**The engine.**

- **When it batches.** A pool takes all its free slots (up to 32) and sends one call. It does this only when it has two or more free slots *and* only one lane is ready. A *lane* is a queue and priority pair. Lane and queue shares are counted per admission. A batch taken from one of several ready lanes broke those shares in the full test run (`CapacityShareTests`). With several ready lanes, the pool claims one job at a time, as before.
- **Begun jobs** go straight to the "about to run" step, with their Begin reply already known; no separate Begin is sent. Every other outcome returns its slot.
- **Fallback.**
  - No work, a paused queue, every candidate blocked, or an expired command: handled exactly like the same Reserve reply.
  - Any other non-Applied reply (for example `UnsupportedVersion`): the pool records a diagnostic, turns the batch off, and claims with Reserve and Begin from then on.
  - Why the four "nothing to claim" replies do not fall back: retrying them as a single Reserve would only add idle round trips.

## 3. Tests

**New cases** (`ReserveAndBeginTests`). Every case ends with the ReferenceModel check of all committed histories.

| Case | What it shows |
|---|---|
| RAB-01 | Per-job isolation: a blocked job between begun jobs gets its own outcome; all blocked writes nothing |
| RAB-02 | N limits: 0 and 33 are refused; a batch takes at most N; a second batch skips locked rows without waiting |
| RAB-03 | Rate deferral inside a batch: two rated jobs start, two defer, an unrated job starts |
| RAB-04 | Cancel inside a batch: a cancel that waits on the batch reaches only its own job |
| RAB-05 | Cancel request and spent budget close their jobs inside a batch, release keys and permits |
| RAB-06 | Lost reply: resending the same command returns every outcome from the receipt; a changed body is a conflict |
| RAB-07 | Fault hook: a failure after job 2 rolls back the whole command; resending applies once |
| RAB-08 | Lock order under contention: no deadlock, no scope over capacity, each job once |
| RAB-09 | A worker with 8 slots uses the batch; 40 jobs run once each |
| RAB-10 | Fallback when the function is missing: the worker turns the batch off and still runs every job |

**Changed:** PERF-01 (new counts), PGX-07 (the batch's claim query, both the normal plan and the cached generic plan, uses the ready index), MIG and manifest (revision 10), TIME-09 (parts a and b now use one slot, and part a2 covers a lost batch reply).

**Full suites on the final code (`6f4a935`):** see the table below.

| Suite | Passed | Failed | Load (1 min) |
|---|---|---|---|
| Core | 387 / 387 | 0 | 8 → 4 |
| Outbox | 36 / 36 | 0 | 4 → 6 |
| Postgres | 303 / 304 | 1 | 6 → 11 |

The one failure was CMD-04, `Deadline_expiring_during_a_lock_wait…`. Its part (c) is a plain Dispatch with a short deadline; it expired before it applied (`CommandExpired`, expected `Applied`). It does not touch the claim path. The test's own comment notes this load sensitivity. It passed when rerun alone on this build (load 14), and on main's build (load 10). It also passed in both earlier full runs. Attribution: load.

**Earlier full run, on `5c8ce00` (before the lane fix), load 16–18.** Postgres had 9 failures out of 304. Each was rerun alone or explained:

| Test | Cause | Result |
|---|---|---|
| CapacityShareTests (3 cases) | **Real:** a batch from one of several ready lanes skewed shares | Fixed in `55d83fb`; all pass |
| WorkerFaultTests TIME-09 | **Real:** the test arms faults on Reserve/Begin, which a 2-slot worker no longer sends | Fixed in `55d83fb`; passes |
| PeerDeploymentModeTests MODE-02 | 32 completions counted for 30 jobs, with batching from several workers | Passed alone after `55d83fb`, and in the full run on `55d83fb`. Not reproduced, so the cause is not proven |
| RoundTripBudgetTests | The scheduler's 250 ms evaluation budget ran out under CPU load | Load; passed in later runs |
| AdmissionTests, RateAdmissionReplicaTests | Barrier timeouts | Load; the PF-05 report saw the same on main; passed in later runs |
| StoreOutageTests | The store came back 40 s after the kill, past the Finish budget | Load; passed in later runs |

The full run on `55d83fb` passed every suite: Core 387/387, Outbox 36/36, Postgres 304/304, load 10–37.

## 4. Not done, and limits

- **No latency claim.** This host is shared and loaded. Throughput moved far more than the noise; single-call latency did not move.
- **Several ready lanes do not batch.** Making batches keep exact lane shares needs the selector to plan several admissions at once. That is left for later.
- **A replayed batch whose payload rows were pruned** fails the call (the same unclassified failure as a replayed Reserve today). It does not return `HistoryUnavailable`.
- **MODE-02's extra completions** were seen once and not again. If it comes back, check whether one job ran in two worker processes.
