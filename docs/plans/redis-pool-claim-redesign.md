# Redis against BullMQ: how a worker pool claims (design note)

Written 2026-11-23. Plan item "Redis F" in [perf-plan-2-remaining.md](perf-plan-2-remaining.md). Goal: Runwick on Redis at 0.9× BullMQ's drain throughput or better with the same persistence (AOF, `appendfsync always`), and pickup and enqueue p95/p99 within 10%. Today the best Runwick setting (buffered finish, layout revision 2, lean scripts) drains at about 0.55× to 0.59× BullMQ with the log on disk and about 0.37× with the log on memory.

**Decision:** build **claim ahead** (`WorkerOptions.ClaimAhead`): a pool may claim a few more jobs than it has handler slots, so each claim carries more jobs and a slot that frees starts its next job without waiting for a claim. It changes no stored data, no script and no guarantee. By the model below it should move the disk drain from about 0.55× to roughly 0.75× to 0.85× BullMQ. It will **not** close the memory-log gap, and it is unlikely to reach 0.9× on disk by itself. What would is named at the end; it needs a decision from you because it weakens what Runwick stores.

## 1. Where the Redis time per job goes now

Redis runs every script on one thread. With `appendfsync always` it also writes and fsyncs the log on that thread, once per pass of its event loop, before it answers the clients served in that pass. So a pass costs the CPU of every script in it plus one fsync, and the drain rate is about:

> jobs per second ≈ B ÷ (F + c × B)

where B is the jobs carried in one pass, F the fsync time and c the script CPU per job.

Fitting the 2026-11-23 drains ("Redis: lean-path trims and a Functions library, not merged" in the round-two report: Runwick 821 jobs/s on disk, 1,177 on memory; BullMQ 1,507 and 3,157) with the batch sizes from `slowdrain.sh` (8.4 jobs per claim):

| | Runwick (buffered finish) | BullMQ |
|---|---:|---:|
| Script CPU per job, c (memory log: 1 ÷ rate) | about 0.7–0.85 ms | about 0.32–0.39 ms |
| Jobs per pass, B | about 8 (one claim of the pool in flight) | about 16 (each of 16 workers has one call in flight) |
| Pass time on disk | about 10 ms | about 10.6 ms |
| Implied fsync, F | about 4 ms | about 4 ms |

Inside Runwick's 0.7 ms (profiles in [redis-stored-data-design.md](redis-stored-data-design.md) §1.5, after the lean scripts and layout revision 2):

- **Fixed cost per script call**, about 150–200 µs: loading the shared Lua frame, the gates, the receipt lookup. At 8 jobs per call that is about 20–25 µs per job.
- **Decision work**, about 400 µs per job: reading candidates and job hashes, ownership, state and permit checks, building replies.
- **Building and writing stored data**, about 200 µs per job: receipt, attempt record, events, state indexes.

Two different limits follow:

- **Disk log.** Both systems spend similar time per pass, but BullMQ carries about twice the jobs in each pass. Runwick's pool waits for each claim's reply, which comes after the fsync, before it sends the next claim. That is the main disk gap, and it is what the 32-slot run measured: 1.37× faster than 16 slots.
- **Memory log.** fsync is almost free, so the rate is about 1 ÷ c, and Runwick's c is about 2.3× BullMQ's. No change to batching closes that; only less Lua work per job does.

## 2. What BullMQ does per job that Runwick does not

From the harness (`eng/bench-compare/redis-group/bench-base.mjs`, BullMQ 6.7.2, 16 workers, `removeOnComplete: {count: 1000}`):

- **One short script call per job per worker.** BullMQ's `moveToFinished` finishes a job and fetches the next one in the same call, so each worker keeps exactly one call in flight. Sixteen workers mean about sixteen jobs per Redis pass, one fsync for all of them.
- **Small scripts.** Each script is a few dozen lines. There is no shared frame of hundreds of helpers.
- **Little stored per job.** The job hash is updated in place. There is no receipt per command, no attempt history (a retry overwrites the fields), and the completed set is trimmed to 1,000 entries. Events go to one capped stream.
- **No replay or fencing per command.** A lost reply is not replayed from a receipt. A stalled job is moved back by a lock check, not by an attempt generation.

Runwick's receipts, replay, fencing per attempt, store-side permits and history are the guarantees in [22-redis.md](../spec/22-redis.md). They are the reason for most of the 400 µs of decision work and the 200 µs of writes.

## 3. Candidates

Estimates use the model in §1 with F = 4.3 ms and c = 0.7 ms per job unless stated. "Disk" and "memory" are the expected share of BullMQ's drain rate.

| Candidate | What it changes | Estimate | Verdict |
|---|---|---|---|
| **A. Claim ahead** | A pool claims up to `ClaimAhead` jobs beyond its slots. They wait in memory, leased and renewed, for a free handler slot. Claims carry more jobs (B about 16–32 instead of 8), and a freed slot starts at once. | Disk: B = 24 gives about 0.77×, B = 32 about 0.81×. Memory: the fixed per-call cost is shared by more jobs, about 4% faster, so about 0.38×. | **Build.** No guarantee, stored data or script changes. Small engine change. |
| B. The pool's claim carries the waiting finishes | One call per pool tick both finishes and claims. | Measured 0.70× to 0.83× of buffered finish on disk. The claim waits behind the finishes, and the combined script costs more. | Rejected (branch `worktree-agent-a2560555b3f291e9a`). |
| C. One call that finishes the previous batch and claims the next without the claim waiting | The claim half runs first in the script. | The fsync is shared already when the calls arrive together. Saves one fixed cost per pass (about 2–3% of c). | Not worth it. |
| D. Lean-script trims, Redis Functions | Fewer commands per call; helpers defined once per server. | Measured: no drain gain (0.96× to 1.02× for the trims, 0.83× to 0.89× for Functions). | Rejected (branches `redis-next` 7ac41df, `redis-functions` 8cbb8f7). |
| E. Precomputed lane ordering | Keep a ready list per pool instead of choosing candidates in the script. | Candidate choice is a small part of the 400 µs. Under 5%. | Not worth it. |
| F. Move decision work into the worker | The worker builds the after-image. Lua only checks a revision per job and writes. | c might fall 30–40%, to about 0.45 ms. Memory: about 0.55×. Disk, with A: about 32 ÷ (4.3 + 14.4), roughly 1.0×. | Large rewrite: every script, replay and recovery path has to be proved again. Not in this round. |
| G. A weaker opt-in "lean ledger" profile | No claim receipt (a lost claim reply is left to lease expiry), one event row per job instead of three, no attempt history beyond the last attempt. | The stored-data design measured writes at about a third of the worker-path CPU. Dropping most of them gives c about 0.5 ms. Memory: about 0.5×. Disk, with A: about 0.9×. | Needs your decision: it weakens replay and history. See §5. |

## 4. What is built: claim ahead

- **Option:** `WorkerOptions.ClaimAhead`, 0 to 32, default 0 (rule V-26). It applies to fixed pools only. Automatic pools ignore it, because their target moves. Spec: [05-worker-engine.md](../spec/05-worker-engine.md) Changes 2026-11-23 (new §9.8), [06-defaults.md](../spec/06-defaults.md) "Prefetch beyond tickets".
- **How it works.** The pool's ticket gate allows target + `ClaimAhead` tickets, so the existing batched claim (`ReserveAndBegin`, up to 32 jobs) asks for more jobs. Each claimed job becomes an entry exactly as today: its lease is registered and renewed, and Begin is already decided in the claim. Before invocation admission, the entry waits for one of the pool's run slots (a semaphore of size target). The slot is released when the handler returns, or at retirement if the job never ran.
- **Guarantees kept.** Receipts, replay of a lost claim reply, fencing and at-least-once are unchanged, because nothing in the store or the scripts changes:
  - A waiting job is fenced like a running one. If it loses its lease, it is never invoked.
  - A worker that dies leaves its waiting jobs to lease expiry and recovery, as it does its running ones.
  - A waiting job also stops waiting, without running, once a cancellation is observed or the drain cutoff passes, and is then finished as `UserCancelled` or `Interrupted`.
- **What a user sees differently.** A waiting job is already begun in the store:
  - It counts as running in management reads.
  - Its recorded start is the claim time.
  - It holds any store-side concurrency or rate permits while it waits.
  - Up to `ClaimAhead` jobs can sit on one busy worker while another worker is idle.

  That is why the option is off by default.
- **Works on every store.** The change is in the engine, so PostgreSQL and SQLite pools can use it too. Only Redis is the target here.

## 5. If claim ahead is not enough

If the paired drains show claim ahead short of 0.9× on disk, as the model expects, the next step is G or F, and both are your call:

- **G (a weaker opt-in profile)** is the smaller build. It is a second set of lean writers behind a store option, and it states plainly what it gives up: a lost claim reply then waits for lease expiry instead of replaying, `/v1/events` shows one row per job step, and job detail keeps only the last attempt.
- **F (decisions in the worker)** keeps every guarantee but means rewriting the worker scripts.

Neither closes the memory-log gap to 0.9×. BullMQ's per-job Lua is about a third of Runwick's, because it stores and checks much less.
