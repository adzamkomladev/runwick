# Why the v0.1.5 queues drain at different rates

I investigated the benchmark report on `515f089` in a separate checkout and verified current GitHub main again at `110b967`. Production source and benchmark harnesses at both revisions match the benchmarked release `de189dc`. I downloaded the original per-round JSON and inspected the implementation revisions recorded in the BullMQ .NET 1.3.7 and Wolverine 6.65.1 NuGet packages. New experiments used the release code on a shared development host, isolated PostgreSQL 18 and Redis 8.2 containers, and fresh SQLite files. The .NET processes used server GC and a logical processor count of four. The host and database containers did not have the CI four-core isolation. Compare arms within each experiment. [Published report](https://github.com/adzamkomladev/runwick/blob/515f089/docs/reports/queue-benchmark-2026-11-23.md) · [Original run and artifacts](https://github.com/adzamkomladev/runwick/actions/runs/45273562448)

## Redis already has a setting worth using for short jobs

I drained 10,000 ready 1 KiB jobs with 16 slots, disk-backed AOF and `appendfsync always`. I ran the five arms sequentially and reversed their order on a fresh instance. All ten arms counted 10,000 finishes. The tuned Runwick arm used the stock binary with `--buffered-finish --claim-ahead 16 --claims-in-flight 2`.

| Jobs/s | Forward order | Reverse order |
|---|---:|---:|
| Runwick default | 540 | 407 |
| Runwick tuned | 1,616 | 959 |
| BullMQ Node 6.7.2 | 1,741 | 1,044 |
| BullMQ .NET 1.3.7 | 1,191 | 605 |
| Wolverine 6.65.1 | 1,351 | 1,483 |

**Runwick reached 92.8% and 91.8% of BullMQ Node.** The tuned worker cut reserve calls from 1,478 to 864 and from 1,512 to 883. FinishMany calls fell from 1,027 to 321 and from 919 to 349. More jobs fit in each durable operation. Claim-ahead lets the worker hold leased jobs before a handler slot opens. Buffered finish lets execution continue while the store confirms earlier finishes. The published CI harness supplied neither flag. Existing five-round experiments also reached 94–104% of Node on a disk log, but only 63% with the best tested arm on a memory log. The runtime and storage device matter. The .NET and Wolverine rates varied too much to promise a ranking across hosts. [CI settings](https://github.com/adzamkomladev/runwick/blob/515f089/eng/bench-compare/ci/run.py) · [Five-round claim-ahead experiment and failure tests](https://github.com/adzamkomladev/runwick/blob/515f089/docs/reports/perf-round-2.md#redis-claim-ahead-of-free-slots-perf-round-two-redis-f)

**Where the competitor spends less work.** Wolverine's inline sender calls Redis StreamAdd (XADD) for an ordinary ready envelope. Its consumer group reads batches and acknowledges stream IDs. BullMQ .NET completes with a lock token and asks the same Lua invocation for the next job. PostgreSQL has a corresponding fused finish-and-fetch function. Runwick also has a combined operation, but all original Runwick drain rounds recorded zero combined calls. The successful experiment above uses the buffered path. On a fast device, Runwick's remaining work includes reading job hashes, calculating the attempt and receipt, updating indexes and emitting lifecycle events. These are separate costs from fsync. [Wolverine sender](https://github.com/JasperFx/wolverine/blob/1a8fde0675c9e334aea64fd8d83de3968b700ee7/src/Transports/Redis/Wolverine.Redis/Internal/InlineRedisStreamSender.cs) · [Wolverine receiver](https://github.com/JasperFx/wolverine/blob/1a8fde0675c9e334aea64fd8d83de3968b700ee7/src/Transports/Redis/Wolverine.Redis/Internal/RedisStreamListener.cs) · [BullMQ .NET Redis backend](https://github.com/taskforcesh/bullmq/blob/04071cad240dc706e8fcb8d44e53dd8d835557ec/dotnet/src/BullMQ/RedisBackend.cs)

I would expose this as a documented, opt-in profile for short, homogeneous jobs in fixed pools. Claimed jobs appear running and hold permits while waiting. Qualify it on mixed handler durations, two competing workers, worker loss and p99 pickup before changing defaults. If Redis enqueue speed is the objective, benchmark it separately. Wolverine performs one stream append on that path, and the Runwick worker flags affect drain only.

## PostgreSQL has two different bottlenecks

**Bulk enqueue.** Seven sampled nested plans for `rw_payload_profile_check` used `jobs_name_list` with only the application in the index condition. PostgreSQL filtered by job ID afterward. One plan discarded 2,950 rows per lookup across 100 loops. The trigger guards payload/job serializer compatibility. A point lookup by application and job ID should remain cheap as the backlog grows. [Trigger definition](https://github.com/adzamkomladev/runwick/blob/515f089/src/Runwick.Postgres/Sql/0028_jobs_index_diet.sql)

I seeded 1,000 jobs per fresh database and timed 10,000 more through the stock bulk API, one caller, 100 jobs per call. Both arms had the same seed. The only intervention was `ANALYZE runwick.jobs` after seeding, outside the clock. Runs were ordered control, analyzed, analyzed, control. All four completed without a failed dispatch.

| Measure over the 10,000 timed jobs | Control 1 | Analyzed 1 | Control 2 | Analyzed 2 |
|---|---:|---:|---:|---:|
| Bulk adds/s | 733 | 1,445 | 488 | 1,460 |
| Validation query time, ms | 6,271 | 22 | 7,214 | 22 |
| Validation query shared-buffer hits | 1,427,890 | 30,000 | 1,421,547 | 30,000 |

That is **2.0× and 3.0× end-to-end on this fixture**, with the validation check still active. A separate three-pair prototype using a correlated lateral lookup with LIMIT 1 still produced over a million buffer hits in candidate runs and no repeatable throughput gain. My first choice is to record cold and analyzed plans in CI and qualify an early statistics refresh after the first substantial load. A product fix should bound lookup cost on a fresh store and on retained data. Test a versioned integrity-check or index change against correct and mismatched profiles if fresh-store plans remain unstable. Running ANALYZE on every dispatch would add work to the hot path.

**Drain.** Increasing slots really did improve batching. At 16 slots, CI used approximately 2,450 reserve calls per 20,000 jobs. At 64 slots it used 626 reserve calls and exactly 625 finish batches in every round. Throughput rose from 728 to 1,321/s. Once the 32-item batches are full, a new worker does little to reduce database work per job.

In a new 4,000-job, 64-slot trace, Runwick settled all 4,000 with 125 plain claim calls and 125 plain finish calls. Across the fixture (including seeding), PostgreSQL recorded 4,166 receipt-function calls and 4,167 decided-receipt trigger calls. Their function self-times were 1.91 s and 1.12 s respectively. Plain claim and finish added 1.42 s and 1.40 s. These are cumulative function timings across sessions. The usage update also touched 4,000 attempt rows after the close. My next drain experiment would reuse the grouped-producer receipt strategy for successful plain finishes: register eligible receipts set-wise, route replays and conflicts individually, and preserve one independently resolvable result per command. Measure receipt calls/job, WAL/job and durable finishes/s. A second independent experiment can fold resource usage into the attempt-close update. [Grouped producer receipt implementation](https://github.com/adzamkomladev/runwick/blob/515f089/src/Runwick.Postgres/Sql/0029_dispatch_group.sql) · [Finish path](https://github.com/adzamkomladev/runwick/blob/515f089/src/Runwick.Postgres/Sql/0013_set_finish.sql) · [Usage update](https://github.com/adzamkomladev/runwick/blob/515f089/src/Runwick.Postgres/Sql/0018_attempt_usage.sql)

At 16 single-add callers the CI harness allocated 16 producer slots. All three PostgreSQL rounds report **zero producer groups**. Grouping takes waiting callers beyond the slot limit. Test the same 16 callers with 4, 8 and 16 producer slots and record group size, refusal count and p99. On the steady 500/s test only 5,280–5,675 jobs had finished by the end of offering roughly 15,000. The published 365/s includes the later drain. Both enqueue and execution need attention for this workload. [Runwick harness](https://github.com/adzamkomladev/runwick/blob/515f089/tests/Runwick.Workloads/BenchMode.cs)

**Distance to River and pg-boss.** At 16 slots, 90% of their CI medians means 5,063 and 5,659 durable jobs/s respectively, about seven to eight times Runwick's published drain. At 64 slots, River's 90% threshold is 13,654/s. The producer plan fix does not establish a worker speedup. The River harness uses a 1 ms fetch cooldown, InsertMany for timed bulk adds and InsertManyFast to seed drains outside the clock. The pg-boss harness uses one worker callback over a batch of 16 or 64, enables burst fetch, and verifies completion in the database. Both timed bulk APIs use chunks of 1,000. These are good batch-processing targets. Track independent handler dispatch and equal-sized producer batches alongside them. [River harness](https://github.com/adzamkomladev/runwick/blob/515f089/eng/bench-compare/river/main.go) · [pg-boss harness](https://github.com/adzamkomladev/runwick/blob/515f089/eng/bench-compare/pgboss/bench.mjs)

## SQLite spends its single writer on repeated job updates

At 64 slots the median CI commit count fell from 3,262 to 1,270 per 20,000 finishes, yet throughput moved from 826 to 935/s. In two new local pairs, all 4,000 jobs finished. Runwick recorded 333 and 301/s and Wolverine 1,398 and 1,582/s. In a separate instrumented Runwick drain, 4,000 jobs consumed **91,766 writer statements** and 864 commits. The statement profile counted 12,000 job updates across two prefixes and 12,000 queue-event inserts. The timing probe changes throughput, so its counts are more useful than its wall time.

Runwick's ReserveAndBeginBody loops through candidates, calls ReserveJob and BeginJob separately, constructs both results and then fetches payloads per job. Wolverine SQLite defaults to durable mode, takes up to 20 envelopes, transfers ready rows into the inbox transactionally and immediately loops again after a nonempty batch. Its benchmark counts handler completions. I would specialize the ordinary SQLite reserve-and-begin path to read each job once and apply one physical ready-to-running update, retaining both logical revision increments, events, the historical replay reply, fencing and the lease. Batch the payload reads. Keep the current path for cancellation, scopes and uncommon outcomes. Compare statements per settled job and time spent holding the writer before changing the commit strategy. [Runwick claim code](https://github.com/adzamkomladev/runwick/blob/515f089/src/Runwick.Sqlite/Store/SqliteStore.Ownership.cs) · [Wolverine SQLite listener](https://github.com/JasperFx/wolverine/blob/1a8fde0675c9e334aea64fd8d83de3968b700ee7/src/Persistence/Wolverine.Sqlite/Transport/SqliteQueueListener.cs)

## Two corrections to the benchmark interpretation

- **Wolverine PostgreSQL's 77/s is explained by its listener.** A full default receive batch is 20. After handing it off, the listener waits for a notification or 250 ms. On a preseeded backlog with no new producer wakes, that predicts about 80/s before overhead. All six rounds processed 20,000 in 259.6–260.3 s. Raising handler parallelism leaves the receive batch and this wait unchanged. [Queue defaults](https://github.com/JasperFx/wolverine/blob/1a8fde0675c9e334aea64fd8d83de3968b700ee7/src/Persistence/Wolverine.Postgresql/Transport/PostgresqlQueue.cs) · [Receive loop](https://github.com/JasperFx/wolverine/blob/1a8fde0675c9e334aea64fd8d83de3968b700ee7/src/Persistence/Wolverine.Postgresql/Transport/PostgresqlQueueListener.cs)
- **BullMQ .NET PostgreSQL's reported 33/s is a timeout.** Five of six rounds observed 19,944–19,983 completions by the 600 s cutoff. The successful 16-slot round observed all 20,000 in 19.633 s, or 1,019/s. The .NET backend has a set-based bulk insert and fuses completion with claiming the next job. The original harness constructs an autorunning worker before subscribing to Completed and tracks success callbacks only. Those are measurement risks. The CI artifacts alone cannot identify the missing jobs' states. I reproduced a failure mode with a fresh 20,000-job PostgreSQL 18 backlog at 64 slots, attached events before RunAsync, and counted database states. At the 60-second diagnostic cutoff, 19,892 jobs had emitted Completed, 108 had emitted Failed with `40P01: deadlock detected`, and the database had 108 failed, zero active and zero waiting. All 20,000 outcomes were accounted for. The server log showed competing `move_to_completed_fetch` transactions. This explains how a success-only counter can wait to its deadline after failed no-op jobs. The historical rounds lack the SQLSTATEs needed to attribute every missing success to this mechanism. Publish those five rounds as incomplete, report time to 50%, 95%, 99% and 100%, attach event listeners before startup, and cross-check waiting, active and failed counts at cutoff. [BullMQ worker](https://github.com/taskforcesh/bullmq/blob/04071cad240dc706e8fcb8d44e53dd8d835557ec/dotnet/src/BullMQ/Worker.cs) · [PostgreSQL backend](https://github.com/taskforcesh/bullmq/blob/04071cad240dc706e8fcb8d44e53dd8d835557ec/dotnet/src/BullMQ/PostgresBackend.cs) · [Original harness](https://github.com/adzamkomladev/runwick/blob/515f089/eng/bench-compare/bullmq-dotnet/Program.cs)

For the next comparison, publish defaults and tuned profiles separately on the same machine in alternating order. Report time from worker startup and from first claim, handler returns and durably settled outcomes, offered/accepted/refused counts, and completions before the offer window closes. The current resource sampler includes startup, seeding and shutdown when it divides total CPU and bytes by completions. Runwick's drain clock runs from first claim to confirmed finish, whereas Wolverine starts before constructing the host and stops at the handler counter. Collect phase-specific CPU before assigning the difference to the worker. Track p99 latency alongside backlog throughput. At 500 offers/s in the original SQLite runs, Runwick's median finish latency was 93 ms and Wolverine's was 2.92 s, while Runwick also refused 546–697 offers per round. Those two facts matter together. [CI sampler](https://github.com/adzamkomladev/runwick/blob/515f089/eng/bench-compare/ci/run.py) · [Wolverine harness](https://github.com/adzamkomladev/runwick/blob/515f089/eng/bench-compare/wolverine/Program.cs)

The 90% target is already reproducible for the short-job Redis drain against BullMQ Node on this disk profile. For PostgreSQL bulk admission there is now a measured, correctable plan problem. For PostgreSQL drain and SQLite the proposed changes still require implementation, crash/replay tests and matched measurements. CPU-heavy jobs should have their own target: in CI Runwick SQLite and Wolverine SQLite processed 38.8 and 41.1/s respectively, already within 6% despite the large empty-job gap.


## Experiment: Redis finish batches of 32 versus 64

**Result: 64 does not help. Keep 32 and do not test 100.** Allowing 64 finishes per FinishMany call made each call carry
more jobs (about 41 instead of 30) and cut finish calls by about a quarter. But throughput did not rise (median 1,253
against 1,333 jobs/s; 32 was faster in three of four pairs), and each job waited about 12 ms longer for its durable finish.

Setup: Runwick at `110b967`, Redis 8.2 with AOF `appendfsync always` and no snapshots, in a container limited to 2 CPUs on
the shared development host; worker with server GC and 4 logical processors. Every round drained 20,000 ready 1 KiB jobs
with 16 slots, `--buffered-finish --claim-ahead 16 --claims-in-flight 2`, and the default 25 ms finish flush interval,
from a freshly flushed Redis. Only the batch cap changed. To raise it, the experiment branch `exp/finish-batch-64` reads an
environment variable in place of the constant `IFinishManyStore.MaxItems` (default still 32). Redis's Lua finish script
already accepted 64 items, so no store change was needed. Four pairs ran in alternating order (32-64, 64-32, 32-64,
64-32), then four traced rounds for the finish latency. Tracing slows the worker, so the untraced rounds give the
throughput. All 12 rounds finished all 20,000 jobs, none through the combined finish-and-claim call.

| Untraced rounds | Cap 32 | Cap 64 |
|---|---:|---:|
| Durable finishes/s, pairs 1–4 | 1,478 · 1,322 · 1,339 · 1,327 | 1,203 · 1,303 · 1,432 · 1,051 |
| Median finishes/s | **1,333** | **1,253** |
| FinishMany calls per 20,000 jobs | 652–665 | 459–519 |
| Jobs per FinishMany call | 30.1–30.7 | 38.5–43.6 |
| Claim calls per 20,000 jobs | 1,725–1,745 | 1,733–1,761 |

| Traced rounds: handler start to durable finish (no-op handler) | Cap 32 | Cap 64 |
|---|---:|---:|
| p50, ms | 29.0 · 25.8 | 40.5 · 39.2 |
| p99, ms | 106.5 · 82.2 | 118.5 · 93.0 |
| Jobs per call | 28.5 · 30.4 | 40.2 · 42.9 |

Why it does not help (see also part 3 of the claim-ahead experiment below, where full 64-job batches still did not help): at a cap of 64 the batch never fills. With 16 slots and 16 claimed ahead, at most about 32 jobs are
held at once, plus whatever finishes while a call is in flight, so a batch stops at about 40 jobs. It also waits longer
to collect them, which adds latency. Claim calls did not change (about 1,740 per 20,000 jobs, around 11.5 jobs each). With
finish calls already carrying 30 jobs each, claiming is now the bigger share of round trips. The next Redis experiment
should therefore target claims (larger claim batches, or more claim-ahead with the cap kept at 32), not finish batches.
Rates on this shared host swung by about ±15% between rounds of the same arm, so a difference under 10% would not be
visible here. Raw rounds, the run script and the override diff: [`evidence/finish-batch-64/`](evidence/finish-batch-64/).

## Experiment: Redis claims in flight, 1 to 4

**Result: 2 is the right setting.** Going from 1 to 2 claim calls in flight per pool raised throughput by 22%, and
every round at 2 beat every round at 1. Going to 3 or 4 gave +5% and +3%, which is inside this host's noise. Those
settings also cost more Redis calls, because each claim comes back with fewer jobs.

Same setup as the finish-batch experiment above (16 slots, buffered finish, 16 claimed ahead, 20,000 jobs, freshly flushed
Redis), with the finish cap at the normal 32. Only `--claims-in-flight` changed. Four passes in different orders
(1-2-3-4, 4-3-2-1, 2-4-1-3, 3-1-4-2); all 16 rounds finished all 20,000 jobs.

| Claims in flight | 1 | 2 | 3 | 4 |
|---|---:|---:|---:|---:|
| Durable finishes/s, passes 1–4 | 1,167 · 1,223 · 1,125 · 1,122 | 1,269 · 1,460 · 1,371 · 1,424 | 1,462 · 1,465 · 1,470 · 1,002 | 1,590 · 1,442 · 1,384 · 1,428 |
| Median finishes/s | 1,146 | **1,398** | 1,463 | 1,435 |
| Claim calls per 20,000 jobs | ~1,200 | ~1,735 | ~2,255 | ~2,650 |
| Jobs per claim call | 16.7 | 11.5 | 8.9 | 7.5 |
| Jobs per finish call | 30.4 | 30.4 | 30.4 | 30.3 |

With 16 slots and 16 claimed ahead, the worker's appetite is fixed. More claims in flight split the same jobs into more,
smaller claims rather than claiming more at once. One claim in flight leaves the worker idle while each claim is in
transit; a second one hides that wait, and after that there is nothing left to hide. To make claims cheaper per job, the
next lever is the claim size, by raising claim-ahead or the per-claim batch, not more claims in flight. Raw rounds and the
run script: [`evidence/claims-in-flight/`](evidence/claims-in-flight/).

## Experiment: Redis claim-ahead 16 to 64

**Result: bigger claims are the lever that works.** Raising claim-ahead from 16 to 32 (Runwick's current maximum) gave
+25% and won all four passes. Raising it to 64 gave another +16% and won all four pairs. Together that is about 1.45x
claim-ahead 16 (multiplying the two within-part gains; the parts ran at different times). Larger finish batches still did
not help on top of it.

Same setup as the experiments above (16 slots, buffered finish, 2 claims in flight, finish cap 32, 20,000 jobs, freshly
flushed Redis). Runwick rejects claim-ahead above 32, so the 64 arms ran with that check raised to 64 on the experiment
branch (`src/Runwick/Worker/WorkerSettings.cs`; diff in the evidence folder). Every one of the 28 rounds finished all
20,000 jobs. The three parts ran at different times, and this host's baseline drifts by 10–20% between sessions, so compare
arms within a part, not across parts.

| Part 1 (4 passes, orders 16-24-32, 32-24-16, 24-32-16, 16-32-24) | 16 | 24 | 32 |
|---|---:|---:|---:|
| Durable finishes/s | 1,129 · 1,059 · 1,373 · 1,276 | 1,475 · 1,514 · 1,324 · 1,336 | 1,652 · 1,593 · 1,418 · 1,336 |
| Median finishes/s | 1,203 | 1,406 | **1,506** |
| Jobs per claim call | 11.5 | 14.1 | 16.5 |

| Part 2 (4 alternating pairs) | 32 | 64 |
|---|---:|---:|
| Durable finishes/s | 1,292 · 1,624 · 1,572 · 1,732 | 1,887 · 1,808 · 1,652 · 1,984 |
| Median finishes/s | 1,598 | **1,847** |
| Claim calls per 20,000 jobs | ~1,215 | ~759 |
| Jobs per claim call | 16.4 | 26.4 |

| Part 3: claim-ahead 64, finish cap 32 vs 64 (4 alternating pairs) | Finish cap 32 | Finish cap 64 |
|---|---:|---:|
| Durable finishes/s | 1,905 · 1,994 · 1,816 · 1,655 | 1,914 · 1,530 · 1,836 · 1,519 |
| Median finishes/s | **1,861** | 1,683 |
| Jobs per finish call | 30.8–31.1 | 52.6–55.6 |

Part 3 corrects the explanation in the finish-batch section above. With 80 jobs held, 64-job finish batches do fill (about
54 per call), and throughput still does not improve; the pairs split 2–2 and the median is lower. Finish calls are not
the limit at this rate. Claim round trips are.

**What this costs.** Claim-ahead 64 means one worker with 16 slots holds up to 80 jobs. Jobs claimed ahead show as running
and keep their leases while they wait for a slot. If that worker dies, they wait for lease expiry before another worker can
run them. A second worker also finds the queue emptier, which makes fairness between workers worse. Short, homogeneous jobs
in fixed pools suit this; long or mixed jobs do not. Before raising the 0–32 limit or recommending 64 in docs, qualify it
with: two competing workers, worker loss mid-drain (time until claimed-ahead jobs run elsewhere), mixed handler durations,
and p99 pickup at a steady rate. The drain numbers above say nothing about those. Raw rounds, scripts and the branch diff:
[`evidence/claim-ahead/`](evidence/claim-ahead/).

## Qualification: what claim-ahead 64 costs

**Verdict: safe to offer as an opt-in for backlog-heavy pools of short, similar jobs; not as a default.** No job was lost
or run incorrectly in any test. The costs are the ones expected from holding more jobs: a crashed worker strands up to
slots + claim-ahead jobs for one lease (about a minute by default), and two equal workers share the work less evenly.
Steady-rate latency could not be tested at 500 jobs/s on this host.

Every arm: Redis as above, 16 slots, buffered finish, 2 claims in flight, finish cap 32; only claim-ahead changed (0, 16,
64). Claim-ahead 64 used the raised check on the experiment branch. The driver is
[`evidence/claim-ahead-qualification/qual.py`](evidence/claim-ahead-qualification/qual.py). Per-job timings come from
each worker's ledger (accepted, handler start, confirmed finish, wall-clock ms).

**Worker crash.** 3,000 jobs of 100 ms each were seeded. Worker A ran for 4 s and was killed with SIGKILL; worker B started
at once. Jobs A held could only move to B when A's 60 s leases expired.

| Claim-ahead | 0 | 16 | 64 (three runs) |
|---|---:|---:|---:|
| Jobs that waited for A's leases | 19 | 48 | 77 · 80 · 91 |
| ...of which A had claimed but never started | 0 | 16 | 60 · 64 · 64 |
| Their wait after the kill, s | 66.6 | 62.6 | 63.9 · 66.4 · 62.7 |
| Kill to last job finished, s | 69.0 | 70.2 | 77.6 · 80.1 · 75.5 |

As designed, held jobs wait one lease, so the number stranded grows with claim-ahead: about 16 running jobs, plus finishes
still buffered, plus the claimed-ahead jobs. Two claim-ahead-64 runs first appeared to finish 2,984–2,985 of 3,000. A
count of job states read from Redis in the two reruns showed all 3,000 succeeded. Worker A had stored those finishes but
was killed before writing them to its log, so this is a logging gap in the test, not in Runwick.

**Two workers, mixed job lengths.** Worker A offered 300 jobs/s for 30 s (4 callers) and worked the queue; worker B joined as
soon as A was ready. Jobs took 20 ms, except every 20th, which took 1 s, so one worker alone could not keep up. Only jobs
added after both workers were running are counted (5,300–8,000 per run).

| Claim-ahead | 0 | 16 | 64 |
|---|---:|---:|---:|
| Share of jobs run by A / B | 50/50 · 53/47 | 55/45 · 57/43 | **72/28 · 61/39** |
| Pickup p50, ms | 155 · 202 | 252 · 177 | 224 · 255 |
| Pickup p99, ms | 547 · 701 | 823 · 607 | 571 · 803 |
| Worst pickup, ms | 824 · 755 | 1,349 · 737 | 1,656 · 1,216 |

The more a worker may claim ahead, the more of the queue the first claimer takes. At 64 the split reached 72/28. Median
and p99 pickup did not change beyond noise. The worst case grew to 1.2–1.7 s: a short job claimed ahead can sit behind a
1 s job on a busy worker while the other worker has free slots.

**Steady rate, one worker.** The test asks for 500 jobs/s, but this host's producer reached only 210–290 jobs/s even with
4 callers, so it measures that lower rate. Results were
(two runs per arm, add-to-finish latency): claim-ahead 0 p50 326 · 250 ms, p99 926 · 672 ms; 16: 259 · 274, 502 · 639;
64: 332 · 273, 1,007 · 680. No difference beyond noise; at this rate the producer, not claiming, sets latency. Repeat on
CI runners, where Runwick Redis added about 3,000 jobs/s with 16 callers.

**Before shipping a higher limit:** keep claim-ahead opt-in. Document that a crashed worker's claimed-ahead jobs wait one
lease, and that workers share less evenly. Run the steady test where 500/s can be offered. Crash tests should count job states in the store at the end, as the
reruns did, because a worker's own log can miss finishes it stored just before it was killed. Raw results, ledgers and the branch diff: [`evidence/claim-ahead-qualification/`](evidence/claim-ahead-qualification/).

## How to qualify the next release

1. For Redis, add an explicit tuned CI arm beside defaults. Use the existing workload command with `--store redis --mode drain --n 20000 --workers 16 --calls 1 --buffered-finish --claim-ahead 16 --claims-in-flight 2` on the harness's dedicated disposable Redis instance. Follow with steady 500/s and delayed pickup. The drain result has already repeated. The remaining acceptance question is whether the claim policy holds its latency and fairness targets.
2. For PostgreSQL, compare independent empty and seeded fixtures on the same private server. For the measured stats experiment, seed with `bench --store postgres --mode bulk --n 1000 --calls 1`, run `ANALYZE runwick.jobs` in the analyzed arm, then time `--db-reuse --mode bulk --n 10000 --calls 1` against that database. Enable nested pg_stat_statements collection and reset its counters after setup. Reproduce the 30,000 buffer hits across 100 validation calls, then repeat at a larger retained row count. Benchmark the finish-receipt prototype on a preloaded backlog to isolate drain.
3. For SQLite, run `bench --store sqlite --mode drain --n 4000 --workers 16 --calls 8 --sqlite-profile-statements --trace` for the statement counts. Run timing pairs separately with tracing off. Require the specialized path to preserve both logical transitions, independent completion and replay after a lost response. The CI 90% target against Wolverine at 16 slots is about 2,012 finishes/s. Compare both libraries at the same durable completion boundary before declaring it met.
