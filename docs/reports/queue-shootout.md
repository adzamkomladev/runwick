# Job-queue shootout: Runwick against River, pg-boss, FastEndpoints, BullMQ and Laravel Horizon

Date: 2026-11-09. Runwick code: `6d1d0b0` plus the harness commit on branch `claude/affectionate-lichterman-448f26`. One machine, one day, three rounds per test. Please read the "How far to trust these numbers" section before quoting anything.

## The short version

- **Runwick is the slowest or near the slowest of the ten setups in almost every test.** It was built for strict durability (a receipt for every command, a confirmed finish for every job, Redis only under `appendfsync always`), and it pays for it. The gap is not small: adding jobs one at a time from 16 callers, Runwick-PostgreSQL did 238 jobs a second against River 944, pg-boss 735 and BullMQ 2,653 (925 with its Redis made as safe as Runwick's).
- **The fairest comparison for Runwick-Redis is BullMQ and Horizon with `appendfsync always`**, because that is the safety level Runwick requires. There, Runwick-Redis adds 332 jobs a second from 16 callers (BullMQ 925, Horizon 302), and drains a 20,000-job backlog at 241 a second with 64 workers (BullMQ 1,754, Horizon 164). So against equally safe Redis queues Runwick is about 3 times slower at adding and 7 times slower at running than BullMQ, and about level with Horizon.
- **Bulk adding is Runwick's weakest spot.** `DispatchMany` takes at most 100 jobs per call and managed 690 to 1,220 jobs a second. River, pg-boss and BullMQ add 3,000 to 9,500 a second in bulk.
- **Waiting time at a steady 100 jobs a second:** BullMQ finishes a job in 9 ms (median), River in 180 ms, pg-boss in 300 ms, Runwick-Redis in 158 ms, Runwick-PostgreSQL in 1.25 s (it only just keeps up). At 500 a second only BullMQ, River and pg-boss kept their waits under a second, and none of them had all 500 jobs a second sent (see the table). No Runwick store kept up.
- **SQLite is the slowest Runwick store** (about 36 jobs a second with 16 workers; the 20,000-job drains hit the time cap). That is expected for one local writer, but it is the number to know.
- **A real problem showed up in Runwick-Redis:** with 8 callers adding 100 jobs at a time, 23 to 29 percent of the calls came back with "outcome unknown" (`store_recovering`). No jobs ran twice and none ran that were not accepted, but 6 to 10 percent of the jobs offered were not stored. Details in its own section below.

## What was run

Ten setups (the eight you asked for, plus BullMQ and Horizon again with the safer Redis setting, plus FastEndpoints):

| Setup | Backend | Version |
|---|---|---|
| Runwick (.NET 10.0.403) | PostgreSQL | main `6d1d0b0` |
| Runwick | SQLite (a file on local disk, WAL, as Runwick configures it) | same |
| Runwick | Redis, `appendfsync always` | same |
| River (Go) | PostgreSQL | v0.50.0, pgx 5.12.0, Go toolchain 1.26.0 (pulled by go.mod; Go 1.25.0 installed) |
| pg-boss (Node 26.13.0) | PostgreSQL | 12.38.0, pg 8.23.1 |
| FastEndpoints job queues (.NET) | PostgreSQL | FastEndpoints.JobQueues 8.3.0, Npgsql 9.0.3 |
| BullMQ (Node 26.13.0) | Redis, `everysec` and `always` | 6.7.2, ioredis 6.0.0 |
| Laravel Horizon (PHP 8.3.6, phpredis) | Redis, `everysec` and `always` | Horizon 5.50.1, Laravel 13.40.0, Composer 2.8.12 |

Servers: PostgreSQL 18.6 in Docker (`runwick-pg`, default settings: `synchronous_commit=on`, `fsync=on`, 128 MB shared buffers), each system in a fresh database. Redis 8.2.11 in two throwaway Docker containers, one with `appendfsync everysec`, one with `always`; both with `appendonly yes`, `maxmemory-policy noeviction`, `aof-load-truncated no`, no snapshots. Host: 8 vCPUs (AMD EPYC), 23 GB, Ubuntu 24.04, shared with other sessions.

Every job is a no-op with a 1 KiB payload. Tests:

1. **Push:** single adds from 1 caller (2,000 jobs) and 16 callers (10,000 jobs), and a bulk add of 20,000 where the system has one (`addBulk`, `InsertMany`, pg-boss `insert`, Runwick `DispatchMany`, Horizon `Queue::bulk`). FastEndpoints has no bulk call.
2. **Process:** 20,000 jobs added first, then workers started and timed until the last job is recorded finished, with 1, 16 and 64 workers. A drain still unfinished after 120 s (1 worker) or 300 s is stopped and reported as "only x% done".
3. **Finish and wait:** jobs arrive at 100 and at 500 a second for 30 seconds (up to 16 adds in flight), 16 workers. The wait is from the add call to the job being recorded finished.

"Finish" is not timed on its own. The drain figures already are the rate at which finished jobs are durably recorded, because each run stops only when the store shows the last job done (River: completion event after its batch update; pg-boss: the table says `completed`; BullMQ: after `moveToFinished`; Horizon: the queue and its reserved set are empty; FastEndpoints: no incomplete row left; Runwick: finish confirmed). How each one finishes is in the per-system notes.

Each test ran 3 times, with the order of systems rotated each round and one system at a time. Every cell below is the median, with lowest to highest in brackets.

## Results

`⚑n/m` means n of m runs started with the one-minute load average above 8. "[only x% done]" means the drain hit its time cap. "[sent only x% of offered]" means the adding side could not send all arrivals, so the wait numbers describe a system that was not keeping up.

### Push (jobs added per second)

| System | enqueue-c1 | enqueue-c16 | bulk |
|---|---|---|---|
| runwick-pg | 51.5 (49.9–54.6) ⚑1/3 | 238 (150–289) · p50 51.5 / p99 351 ms | 1,048 (698–1,080) ⚑1/3 |
| runwick-sqlite | 113 (44.1–131) ⚑2/3 | 83.2 (59.1–87.0) · p50 174 / p99 641 ms ⚑1/3 | 1,223 (848–1,348) ⚑1/3 |
| runwick-redis | 90.6 (43.1–91.1) | 332 (179–435) · p50 30.4 / p99 304 ms ⚑1/3 | 686 (411–846) ⚑1/3 |
| river | 110 (75.4–123) ⚑1/3 | 944 (649–993) · p50 13.1 / p99 95.6 ms | 8,474 (7,659–8,747) ⚑1/3 |
| pgboss | 122 (66.9–134) | 735 (600–884) · p50 16.9 / p99 94.9 ms ⚑1/3 | 9,504 (6,451–9,778) ⚑1/3 |
| fastendpoints | 199 (193–210) | 1,570 (453–1,908) · p50 7.8 / p99 44.7 ms | n/a |
| bullmq-everysec | 575 (431–646) ⚑2/3 | 2,653 (2,251–5,170) · p50 4.7 / p99 23.6 ms ⚑1/3 | 6,320 (5,551–7,072) ⚑1/3 |
| bullmq-always | 117 (114–127) | 925 (679–960) · p50 13.6 / p99 80.5 ms | 3,081 (3,037–3,111) |
| horizon-everysec | 141 (103–152) | 1,198 (965–1,381) · p50 9.9 / p99 68.2 ms | 234 (219–235) ⚑1/3 |
| horizon-always | 59.3 (57.4–69.5) | 302 (285–311) · p50 38.1 / p99 248 ms | 94.6 (84.4–103) ⚑2/3 |

### Process (20,000-job backlog drained, jobs/s)

| System | drain-w1 | drain-w16 | drain-w64 |
|---|---|---|---|
| runwick-pg | 16.8 (13.0–22.0) [only 10% done] ⚑1/3 | 152 (110–158) ⚑1/3 | 368 (340–572) ⚑1/3 |
| runwick-sqlite | 13.3 (9.1–14.8) [only 8% done] ⚑1/3 | 36.4 (28.6–37.5) [only 55% done] ⚑1/3 | 41.6 (30.5–42.7) [only 62% done] ⚑1/3 |
| runwick-redis | 32.7 (31.0–34.6) [only 20% done] | 193 (176–200) | 241 (231–272) |
| river | 155 (111–159) | 1,020 (555–1,069) | 2,659 (1,831–3,653) |
| pgboss | 2.0 (2.0–2.0) [only 1% done] ⚑1/3 | 601 (574–792) | 2,082 (1,659–2,471) |
| fastendpoints | 42.8 (34.5–43.3) [only 26% done] | 212 (176–222) | 330 (305–331) ⚑1/3 |
| bullmq-everysec | 372 (280–392) | 1,832 (1,664–2,836) | 3,928 (3,418–4,182) |
| bullmq-always | 102 (102–133) [only 62% done] | 875 (638–909) | 1,754 (1,729–1,776) |
| horizon-everysec | 33.9 (26.0–37.5) [only 20% done] | 214 (211–266) | 381 (274–444) |
| horizon-always | 11.2 (10.5–12.4) [only 7% done] ⚑1/3 | 80.0 (68.9–82.4) ⚑1/3 | 164 (162–176) |

### Steady arrivals: jobs finished per second, enqueue-to-finished latency

| System | steady-100 | steady-500 |
|---|---|---|
| runwick-pg | 91.8 (88.7–92.7) · p50 1,253 / p99 2,963 ms [sent only 93% of offered] | 94.6 (87.0–110) · p50 11,873 / p99 17,700 ms [sent only 32% of offered] |
| runwick-sqlite | 37.2 (21.0–38.3) · p50 4,858 / p99 6,562 ms [sent only 42% of offered] ⚑1/3 | 35.5 (21.7–41.7) · p50 4,799 / p99 6,184 ms [sent only 8% of offered] ⚑1/3 |
| runwick-redis | 99.9 (73.6–99.9) · p50 158 / p99 826 ms ⚑1/3 | 109 (92.9–138) · p50 21,036 / p99 28,052 ms [sent only 53% of offered] ⚑1/3 |
| river | 96.3 (83.7–96.7) · p50 180 / p99 609 ms | 261 (261–325) · p50 203 / p99 486 ms [sent only 53% of offered] |
| pgboss | 92.2 (88.1–98.4) · p50 301 / p99 897 ms [sent only 93% of offered] | 380 (362–390) · p50 294 / p99 651 ms [sent only 77% of offered] |
| fastendpoints | 94.8 (94.4–98.0) · p50 11.9 / p99 344 ms ⚑3/3 | 246 (186–288) · p50 3,046 / p99 20,869 ms [sent only 86% of offered] |
| bullmq-everysec | 98.2 (98.1–99.8) · p50 9.0 / p99 206 ms | 431 (415–437) · p50 45.0 / p99 495 ms [sent only 86% of offered] |
| bullmq-always | 99.4 (95.6–99.5) · p50 56.0 / p99 667 ms | 373 (353–460) · p50 118 / p99 665 ms [sent only 75% of offered] |
| horizon-everysec | 87.0 (86.4–99.1) · p50 72.2 / p99 1,008 ms [sent only 87% of offered] ⚑3/3 | 96.2 (92.9–97.8) · p50 68.4 / p99 789 ms [sent only 19% of offered] ⚑1/3 |
| horizon-always | 23.4 (22.8–31.2) · p50 63.7 / p99 366 ms [sent only 24% of offered] ⚑1/3 | 28.0 (22.0–34.8) · p50 54.4 / p99 311 ms [sent only 6% of offered] |

Cells: median (min–max) over rounds. ⚑n/m = n of m runs started above load 8. [only x% done] = the run hit its time cap or could not keep up.

### How to read the steady-arrival rows

At 100 a second most systems kept up, so waits are meaningful. At 500 a second many could not even be fed that fast (the adding side is the limit), so the "finished per second" there is what the system managed, not what was offered. For Horizon the producer is a single PHP process, which tops out near 140 adds a second; Horizon is normally fed by many web requests, so its 500-a-second row measures the harness. Treat it as "not tested at 500".

## Per-system notes

**Runwick (PostgreSQL / SQLite / Redis).** Defaults, one change: the Producer admits as many concurrent calls as callers. Workers: a fixed pool of 1, 16 or 64 slots. A job is claimed in batches (up to 32 per call), and each finish is confirmed with its own receipt. Durability: every add and finish is a receipted command, stale workers are refused (fenced finish), and the Redis profile only starts under AOF `always` with the settings above. A crash after "accepted" loses nothing. Why it trails: five rows written per add plus a receipt, a notify inside every add transaction on PostgreSQL (the earlier investigation `docs/reports/perf-investigation.md` measured this and a prototype that helps), a 100-job cap per bulk call, and one confirmed finish per job.

**River.** `FetchCooldown` 1 ms for drains and steady runs (default is 100 ms, which caps 16 workers near 160 jobs/s; I kept the default-setting steady results in `artifacts/shootout-default-settings`: 84 and 150 finished/s at 100 and 500 offered). One `INSERT` per add; bulk through `InsertMany`; the backlog for drains was added with `InsertManyFast`. Finished jobs wait in memory and are marked complete in batched updates, so a crash can run a job again that had already finished (at-least-once). Postgres commits are waited on.

**pg-boss.** Polls every 0.5 s, its minimum. So the worker takes one batch per poll: for drains one worker with batch size = the worker count (1, 16 or 64) and burst on; for the steady test one worker with a batch holding one second of arrivals. Batch size 1 with 16 workers (the "default" shape) is capped near 32 jobs/s by polling: 32 finished/s at 100 and 500 offered, with waits of 30 to 40 seconds (kept in `artifacts/shootout-default-settings`). One `UPDATE` completes a whole fetched batch. Docs: the pg-boss site (pgboss.io) gives no concrete throughput numbers; the batch sizing is my reading of its polling model.

**FastEndpoints job queues.** There is no PostgreSQL provider in the package. The docs require you to write one; I wrote the documented minimum with Npgsql (`INSERT` on add, a `SELECT` of due rows when the queue starts or probes, `UPDATE` on completion). Queues are in memory: the add call writes the row and hands the job over; storage is read back at start-up and at probe intervals. `MaxConcurrency` was set to the worker count. The numbers therefore describe FastEndpoints plus my small provider, not a product feature. No lease or fencing was used (distributed mode off), so it is single-process and at-least-once. Its queue id is the MD5 of the command type name.

**BullMQ.** Production guide followed: `maxRetriesPerRequest: null` for workers, keep a bounded number of finished jobs (`removeOnComplete`/`removeOnFail` count 1000), `maxmemory-policy noeviction`. One worker process with `concurrency` 1, 16 or 64 (Node is single-threaded, so these are concurrent handlers, not threads). Finish: a Lua script moves the job to completed after every job. Durability: with AOF `everysec` a crash can lose about one second of acknowledged jobs; with `always` it cannot, at about a third of the speed (adds: 2,653 to 925 a second).

**Laravel Horizon.** `php artisan horizon` (master process, one supervisor, `balance=simple`, minProcesses = maxProcesses = the worker count). Each worker is its own PHP process that runs one job at a time (PHP has no threads), so 64 workers is 64 PHP processes. Queue connection redis with phpredis, `block_for=5` (docs: blocking pop instead of polling every 3 s), `tries=1`, memory 128, timeout 60. Horizon also writes a record for every job (for its dashboard), which is its cost: adds and finishes both touch several keys. A first run used `balance=false`, which starts at one worker and scales up slowly; those drain rows are in `artifacts/shootout-horizon-balance-false` (16 workers: 258 jobs/s, 64: 401) and were replaced. Latency in the steady test is from Horizon's own `created_at` to `completed_at` records. Durability follows Redis (`everysec` can lose about a second).

## The Runwick-Redis error with 8 concurrent bulk callers

The first full run failed in all nine Runwick-Redis drain runs: the backlog is added by 8 callers sending `DispatchMany` calls of 100 jobs, and the run crashed with an unhandled `Runwick.DispatchOutcomeUnknownException` (log text: "Dispatch ... outcome unknown; resolve it with the same operation ID."). With one caller everything works, so the headline Runwick-Redis drain rows use one caller for the (untimed) backlog add. The 8-caller case was then re-run with a diagnostic (`--check`) that records each failure, asks the store what happened to the operation, and compares jobs run with jobs accepted. Three runs, 200 calls of 100 jobs each, `appendfsync always`:

| Run | Calls with unknown outcome | Store said "applied" | Store said "not seen" | Jobs accepted | Jobs run | Run minus accepted |
|---|---|---|---|---|---|---|
| 1 | 45 of 200 | 31 | 14 | 18,600 | 18,600 | 0 |
| 2 | 58 of 200 | 38 | 20 | 18,000 | 18,000 | 0 |
| 3 | 48 of 200 | 35 | 13 | 18,700 | 18,700 | 0 |

- **Exact error:** `Runwick.Protocol.StoreOutcomeUnknownException: store_recovering`, surfaced as `DispatchOutcomeUnknownException`. Asking the store afterwards threw `StoreNotAppliedException: store_recovering` 2 to 3 times per run (it answered on a later try). The Redis code that raises it: `RxRunner` gives up when the write script keeps answering `RECOVERING` (after 8 tries or the 5-second budget), or a large pending write is `STUCK` (`src/Runwick.Redis/Store/RxRunner.cs`, lines near 166 and 199).
- **Frequency:** 23 to 29 percent of the 8-caller bulk calls. Zero of 600 calls with one caller (three runs).
- **Lost or duplicated jobs:** none duplicated, none run that were not accepted (jobs run equals jobs accepted in all three runs, after a 5-second grace). 1,300 to 2,000 of the 20,000 jobs (6 to 10 percent) were in calls the store reported as "not seen"; the caller was told the outcome was unknown and has to re-send. Nothing was silently lost, but the application must handle the unknown outcome; this harness's plain bulk add did not and crashed.
- **Not established:** why several callers trigger the recovery path (the cause is in the Redis store, and I did not root-cause it), and whether a "not seen" answer could later turn into an applied write after the 5-second grace. I looked for jobs run beyond accepted and found none in that window only.

This looks like a real concurrency weakness in the Redis store rather than a benchmark quirk. It should be looked at by the owner of `RxRunner`. Raw files: `artifacts/redis-bulk-check`, including the first crashed attempt.

## What this means for Runwick

- Where it trails and why: see the Runwick note above. In short, durability machinery per command and per finish, single-add cost on PostgreSQL (notify lock), a small bulk-call cap, and a single connection path on SQLite.
- Where it is fine: Runwick-Redis single adds (91 a second from one caller) are level with River and pg-boss; 16-caller adds on Redis (332) are level with Horizon at equal safety (302); Runwick-PostgreSQL has a 1.25-second median wait at 100 a second (SQLite is worse, 4.9 s) but finished nearly everything it was sent; no run lost or duplicated an accepted job.
- The comparisons that matter for decisions are the like-for-like ones: Runwick-Redis against BullMQ/Horizon with `always`, and Runwick-PostgreSQL against River and pg-boss (same database, same commit rule).
- Biggest levers, from this data and the earlier investigation: grouping single adds into bulk calls (bulk Runwick is 2 to 15 times its 16-caller single-add rate, depending on the store), raising the bulk cap, sending the wake message after commit, and widening the finish lane for 64-worker drains. None is tried here.

## How far to trust these numbers

- **Host load.** The host was shared. Of 237 recorded runs, 41 started above load 8 (flagged ⚑ in the tables); loads ranged from 2.6 to 26.7 on 8 cores. I waited up to 60 seconds for quiet before each run and then ran anyway. Use the numbers for ordering and rough size, not for decimals. Runs that overlapped with builds or tests in other sessions show as wide min-to-max ranges.
- **One database and Redis container each**, reached through the Docker proxy, one machine. Not a reference host. Network latency to a real server would help the Redis systems relatively less than the PostgreSQL ones.
- **Settings differ on purpose and are listed above.** Notable non-defaults: River fetch cooldown 1 ms; pg-boss batch sizing; BullMQ trimming of finished jobs; Horizon fixed worker count and `block_for=5`; Redis `noeviction` and `aof-load-truncated no` (needed by Runwick). A stock-settings River and pg-boss steady run is in `artifacts/shootout-default-settings`.
- **Not tuned:** Runwick uses its default claim and finish settings. PostgreSQL itself was not tuned for anyone.
- **Drains of 1 worker are capped** (120 s), so "jobs/s" there is over a partial drain.
- **Steady rows at 500 a second** mostly measure how fast the adding side can send; read them with the "sent" labels.
- FastEndpoints' row depends on my own storage provider. I did not research River's or pg-boss's official throughput guidance beyond the pages that were reachable; BullMQ and Laravel are cited below.

## Sources

- BullMQ, Going to production: https://docs.bullmq.io/guide/going-to-production (AOF, `noeviction`, `maxRetriesPerRequest: null`, remove finished jobs).
- Laravel Horizon (12.x docs, same options in 13): https://laravel.com/docs/12.x/horizon (balancing strategies, supervisors, `tries`, `timeout`).
- Laravel queues, Redis `block_for`: https://laravel.com/docs/13.x/queues
- FastEndpoints job queues: https://fast-endpoints.com/docs/job-queues (storage provider interface, `MaxConcurrency`, queue ids).
- River fetch settings (default `FetchPollInterval` 1 s, `FetchCooldown` 100 ms) came from a River pull request, not from an official docs page I could fetch: https://github.com/riverqueue/river/pull/1150
- pg-boss: https://pgboss.io/

## Running it again

One command per system (each builds nothing itself; first build `dotnet build tests/Runwick.Workloads -c Release`, `go build` in `eng/bench-compare/river`, `npm ci` in `pgboss` and `bullmq`, `dotnet build` in `fastendpoints`, `eng/bench-compare/horizon/setup.sh`). Redis containers: `docker run -d --name bench-redis-everysec -p 127.0.0.1:56391:6379 redis:8.2-alpine redis-server --appendonly yes --appendfsync everysec --no-appendfsync-on-rewrite no --maxmemory-policy noeviction --aof-load-truncated no --save ""`, and the same on port 56392 with `--appendfsync always`.

```bash
python3 eng/bench-compare/shootout.py --out artifacts/shootout --one runwick-pg --round 1
```

Replace `runwick-pg` with `runwick-sqlite`, `runwick-redis`, `river`, `pgboss`, `fastendpoints`, `bullmq-everysec`, `bullmq-always`, `horizon-everysec` or `horizon-always`. For the whole matrix with rotating order: `python3 eng/bench-compare/shootout.py --out artifacts/shootout --rounds 3`. Then `python3 eng/bench-compare/summarize_shootout.py artifacts/shootout`. Runwick-Redis bulk-add check: `dotnet tests/Runwick.Workloads/bin/Release/net10.0/Runwick.Workloads.dll bench --store redis --redis 127.0.0.1:56392 --mode drain --n 20000 --workers 16 --calls 8 --check`. The Redis instance you point `--redis` at is flushed.

Raw results and per-run load snapshots: `artifacts/shootout` (git-ignored, in the worktree).
