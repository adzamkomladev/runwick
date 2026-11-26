# Stage 2, Redis: the add script is the cost, and the worker's claim-ahead setting is the drain lever

26 November 2026 · Runwick v0.1.5 behaviour · Redis 8.2.11, `appendonly yes`, `appendfsync always`, data on `tmpfs` · 8 shared vCPU AMD EPYC · [overview and method](queue-stage2-overview.md) · raw rounds in [`evidence/stage2/`](evidence/stage2/) (`dispatch-redis-tmpfs.json`, `concurrent-redis-tmpfs.json`, `redis-replay-tmpfs.json`, `redis-tmpfs/`, text in `summary-redis.txt`)

## Numbers

### Single add, one caller (median of 5 rounds, range in brackets)

| Arm | ms per add | Jobs/s | Process CPU ms | Redis CPU ms | Redis commands |
|---|---:|---:|---:|---:|---:|
| Public `DispatchAsync(job)` | 2.38 (2.25–2.76) | 420 | 1.21 | 1.20 | 21 |
| Preparation alone | 0.035 (0.033–0.068) | | 0.045 | 0 | |
| A command prepared before the timer | 2.25 (1.83–3.08) | 444 | 1.06 | 1.11 | 21 |
| The bare store call | 2.09 (1.93–2.42) | 478 | 1.01 | 1.02 | 21 |
| Raw replay of the same script call, plain client, fresh IDs | 2.69 | 372 | 0.92 | 1.30 | 21 |
| `PING` / `ECHO` of the same size | 1.04 / 1.34 | | 0.57 / 0.67 | 0.19 / 0.20 | |

Reading it: one caller spends about 1.0 ms just on the round trip (Docker port forwarding to a container on the same machine), 1.0 to 1.5 ms on the script, 0.25 ms on Runwick's own glue (admission, building the store command, decoding the receipt: public minus prepare minus store call), and 0.035 ms (1.5%) on preparation. The raw replay is slower than Runwick's store call in this table because the two ran at different times on a noisy machine (ranges of 0.4 ms are normal here); the fair statement is that a plain client sending the same call is **not faster** than Runwick's store layer.

Preparation split, per job: serialize payload 0.008 ms, validate and build the intent 0.015 ms, digests 0.037 ms; whole `Prepare` 0.038 ms.

### Many callers (5 rounds, 12,000 adds per arm; jobs/s)

| Callers | Public | Prepared first | Bare store call | Raw replay (plain client) | Redis CPU per add |
|---:|---:|---:|---:|---:|---:|
| 16 | 1,224 (1,016–1,465) | 1,458 (1,122–1,673) | 1,400 (1,230–1,634) | 1,665 (1,195–1,821) | 0.66–0.74 ms |
| 64 | 1,772 (1,699–1,990) | 1,852 (1,790–1,959) | 1,907 (1,727–2,050) | 2,075 (1,970–2,254) | 0.52–0.57 ms |

With 64 callers the public path reaches 85% of what a plain client sending the same 21-command script can do, and 93% of the bare store call. Preparing first moves it by 5%, the whole Runwick store layer by 8%. Every arm tops out near 2,000 adds/s with Redis's CPU per add at 0.5 to 0.7 ms: one Redis thread running a 21-command script per add, back to back, is about 1,500 to 2,000 per second. (Earlier, the CI run's 2,100 adds/s flat from 16 to 64 callers is the same ceiling on a faster machine.)

### Bulk add, 100 jobs per call (median of 5 rounds)

| Arm | ms per call | Jobs/s | Process CPU ms | Redis CPU ms | Redis commands | Allocated |
|---|---:|---:|---:|---:|---:|---:|
| Public `DispatchManyAsync` | 26.1 (22.6–40.0) | 3,830 | 11.7 | 18.0 | 418 | 3.6 MB |
| Preparation alone | 3.3 (2.3–4.0) | | 3.7 | 0 | | 2.7 MB |
| Prepared first | 18.8 (16.3–28.8) | 5,320 | 4.8 | 18.5 | 418 | 1.0 MB |
| The bare store call | 19.6 (17.0–25.1) | 5,110 | 3.5 | 15.8 | 418 | 0.9 MB |

Preparation is 13% of the public bulk call (3.3 of 26.1 ms) and 74% of its allocated bytes. The store call is 4.2 Redis commands and about 160 µs of Redis CPU per job. If preparation were free, bulk would rise by at most 13%, below the plan's 20% bar for integrating a change.

### Drain, 20,000 ready jobs, tuned to what the public worker actually does (jobs/s, median and range)

| 16 slots | Rounds | Jobs/s | Claim size | Finish size | Redis CPU per job | p99 handler return to confirm |
|---|---:|---:|---:|---:|---:|---:|
| Public worker (default: finish not buffered) | 5 | 948 (860–1,015) | 6.5 | 7.6 | 0.67 ms | 34 ms |
| Direct driver, same sizes | 5 | 1,057 (675–1,149) | 5.5 | 5.5 | 0.65 ms | 31 ms |
| Direct, flush 0 ms / 20 ms | 3 / 3 | 972 / 1,078 | 5.5 | 5.5 | 0.70 / 0.63 ms | 33 / 23 ms |
| Public, claim ahead 16 | 3 | **1,288 (1,196–1,338)** | 12.4 | 11.8 | 0.53 ms | 54 ms |
| Public, 2 claims in flight | 3 | 910 (652–932) | 6.0 | 6.3 | 0.73 ms | 42 ms |

| 64 slots | Rounds | Jobs/s | Claim size | Finish size | Redis CPU per job |
|---|---:|---:|---:|---:|---:|
| Public worker | 3 | 1,335 (1,175–1,405) | 23.3 | 18.5 | 0.57 ms |
| Direct driver, same sizes | 3 | 1,312 (983–1,389) | 21.3 | 10.7 | 0.60 ms |
| Public, claim ahead 32 | 3 | **1,773 (1,719–2,278)** | 31.7 | 20.0 | 0.43 ms |
| Public, 2 claims in flight | 3 | 1,561 (1,514–1,996) | 21.9 | 18.4 | 0.51 ms |

The direct driver's batch sizes drift from the public worker's (the public worker's finish size is 7.6 where the direct driver's is 5.5 at 16 slots, and 18.5 against 10.7 at 64), so the "matched" pair is only roughly matched (the public worker may group finishes across claims, which the direct driver does not). The 11% gap at 16 slots is within the direct driver's 675 to 1,149 spread. The direct driver's sample (the share of 2 ms samples in which a slot was free while unclaimed backlog existed) was 85% at 16 slots and 41% at 64: a free slot and unclaimed backlog existed together for most of the run, which is the gap that claiming ahead closes.

Seeding the 20,000 jobs is not in these numbers. Startup (worker start to first claim) took 110 to 480 ms and is kept apart from the drain time.

## First attributable cost, and what would falsify it

**Single and bulk add:** the add script inside Redis. One add runs 21 commands (bulk: 4.2 per job) costing 0.5 to 0.7 ms of Redis CPU, and that is what caps adds near 2,000/s. Evidence: (1) preparation is 1.5% of a single add and the .NET store layer 8% at 64 callers; (2) a plain client replaying the same call with fresh IDs reaches 2,075/s at 64 callers, Runwick 1,772/s; (3) a replay that I got wrong the first time (the job ID still sat inside the item JSON, so Redis took its 6-command rejection path instead of a real add) ran at 2,500 to 6,400/s with 0.15 to 0.30 ms of Redis CPU: about three times the real work for a third of the commands. That wrong run is kept out of the tables and is not evidence of a valid add; it only shows the cost follows commands.

**What would falsify it:** a script that does the same durable work with 6 commands (same job record, receipt and uniqueness checks) should lift the raw-replay ceiling at 64 callers by about 2× or more. If it lifts it by less than 1.3×, the cost is not the command count (candidates would be per-key work, script compile or argument size). Separately, if a plain client at 64 callers ever exceeded about 3,000/s with the full 21-command script on this host, the ceiling would not be the script.

**Drain:** with the same batch sizes, the public worker and a direct driver draining through the same Redis calls are within noise (948 vs 1,057 at 16 slots, 1,335 vs 1,312 at 64). What changes the rate is **scheduling**: claiming ahead of free slots lifts 16 slots by 36% and 64 slots by 33% and lowers Redis CPU per job (0.67 to 0.53 and 0.57 to 0.43 ms) because claims and finishes are larger. This matches the earlier tuned-profile result (about a third).

**What would falsify it:** if making claim-ahead 16 the default does not lift the matched-pair public rate by at least 20% over seven pairs, or if fairness with several competing workers, pickup latency or lease margin degrade by more than 10%, the scheduling diagnosis fails. The multi-lane check was not done.

## Next action by the plan's decision table

| Observation | Action |
|---|---|
| Preparation is 1.5% of a single add, 13% of bulk | No Stage 3 work on shared serialization for singles; bulk preparation is worth one profiling pass but cannot exceed +13%. |
| Direct and public worker drain are equal at matched sizes; separation only when claim timing changes | Stage 4: test a scheduling-policy change (claim ahead on by default for Redis, or sized from slots) and check fairness, pickup and lease margin with competing workers. |
| Both arms plateau and Redis counters name the expensive operation (21 commands per add, 0.4 to 0.7 ms per drained job) | Stage 3 and 4 Redis provider work: cut commands per add and per claim and finish in the scripts. |

## Not checked

Disk-backed Redis (the AOF `always` cost on a real disk; here the same raw replay with `appendfsync no` made no difference on `tmpfs`), several worker processes at once, multiple lanes, production network round trips, fewer than seven pairs (only screening), a script with fewer commands (the falsifying experiment itself), EventPipe allocation profiles.

An EventPipe CPU sample of 64 public callers was taken (`eng/bench-compare/trace-top.py`). It put about 65% of managed CPU samples inside `DispatchPreparer.Prepare`, which would contradict the numbers above. The controlled test does not support it (preparing first changed throughput by 5%, and a single `Prepare` costs 0.038 ms), and the sample is contaminated by threads spinning in the garbage collector and thread pool (the same trace shows 27% of samples in `PollGC` and spin waits). I do not use that trace as evidence.
