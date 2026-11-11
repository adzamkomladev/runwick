# Runwick performance, round two

## SQLite baseline · 2026-11-11

The first workload used the shipped SQLite provider at `11fa7f7` with `BenchMode.cs` from the queue shootout branch overlaid in an isolated checkout. The profiler build used the same runtime source plus the opt-in writer timings later committed in `8af3b29`. Both used .NET SDK `10.0.403`, bundled SQLite `3.53.3`, a fresh local ext4 database, WAL, and `synchronous=FULL`. The host has eight shared vCPUs and QEMU SSD storage. These runs do not qualify the release on a reference host.

| Workload | Offered | Accepted | Durably settled | Settled/s | Single Reserve / ReserveAndBegin sends |
|---|---:|---:|---:|---:|---:|
| Fresh drain, 16 workers, 1,000 jobs | 1,000 | 1,000 | 1,000 | 89.8 | ≈47 / 79 |
| Steady, 16 workers, 100/s for 30 s | 3,000 | 1,395 | 1,395 | 40.6 including tail drain | ≈38 / 247 |
| Profiled drain, 16 workers, 1,500 jobs | 1,500 | 1,500 | 1,500 | 85.4 | 86 / 102 |

The uninstrumented `reservesSent` counter includes both operations and counts resends. The first two single-Reserve figures subtract `batchClaims` from `reservesSent`. The instrumented result counts each executed operation separately. The steady run refused 1,605 offers with `local_capacity_exceeded`, recorded a dispatch-to-durable-finish p99 of 6.29 s, and logged presence and retention warnings under load. The short fresh drain differs from the historical shootout that reported about 36/s.

### Writer profile

The instrumented drain completed 1,500 jobs across 690 committed writes including background operations. The writer recorded zero exhausted busy-lock waits. The one-minute load average fell from 20.52 to 16.84 during the sample. The 10 ms sampler observed a mean of 7.48 held jobs out of 16 slots, which includes executing and settling attempts.

| Operation | `BEGIN IMMEDIATE` p99 | Writer queue p99 | Transaction body mean | Commit mean |
|---|---:|---:|---:|---:|
| Reserve | 0.09 ms | 386 ms | 7.6 ms | 8.6 ms |
| Begin | 4.05 ms | 390 ms | 7.1 ms | 8.2 ms |
| ReserveAndBegin | 1.12 ms | 86.5 ms | 32.9 ms | 9.2 ms |
| Finish | 0.30 ms | 257 ms | 6.6 ms | 8.9 ms |
| FinishMany | 0.45 ms | 208 ms | 28.6 ms | 8.4 ms |

The transaction body includes managed preparation, receipt and result handling, and statement execution. The commit measurement includes SQLite work inside `COMMIT`, such as automatic checkpointing when it runs. Writer-queue wait includes time behind other writes. These measurements support testing a one-slot combined claim and removing redundant settlement statements. They leave open the respective costs of statement preparation, synchronization and checkpoints.

Run the same profile with the pinned SDK after building the Release workload runner.

```sh
dotnet tests/Runwick.Workloads/bin/Release/net10.0/Runwick.Workloads.dll bench \
  --store sqlite --mode drain --n 1500 --workers 16 --calls 1 --payload 1024 \
  --trace --out artifacts/perf/round-two/profile-drain.json
```

The initial steady fixture uses `--mode steady --rate 100 --seconds 30 --workers 16 --calls 16 --payload 1024 --trace`. These are no-op jobs with a nominal 1 KiB payload. Retained-data, multi-process and large-payload coverage are still pending.

### Q1 worker claim comparison

Five alternating pairs used the same workload binary source with and without the worker changes. The candidate enables `ReserveAndBegin` for a single eligible lane with only one free slot, restores work wakeups for cached lanes, and rechecks admission before submitting a new claim. Both builds had the same SQLite writer instrumentation. Each run followed a warm-up run per build and seeded 1,000 jobs through eight bulk callers before starting 16 workers. Every run settled all 1,000 jobs.

| SQLite drain | Baseline | Worker candidate |
|---|---:|---:|
| Median settled jobs/s (min–max) | 103.1 (80.3–125.7) | 122.4 (105.3–132.8) |
| Writer commits during drain, median | 486 | 453 |
| Standalone Reserve sends, range | 52–58 | 0 |
| Time from dispatch to handler start, median of run p99s | 9.84 s | 8.23 s |

The paired throughput gains were 20.9%, 2.1%, 2.2%, 5.6% and 35.7%, with a median of **5.6%**. Starting load ranged from 6.9 to 14.3. The claim-path change and reduced write commits are measurable in this fixture. The size of the throughput gain is inconclusive on this host and the results do not establish the several-fold goal. The median number of committed finish calls was 288 before and 304 after, so settlement remains a candidate for reducing commits.

```sh
BENCH_STORE=sqlite BENCH_TRACE=1 BENCH_ROUNDS=5 BENCH_N=1000 BENCH_TESTS=drain-w16 \
  eng/bench-compare/ab.sh "$baseline_dll" "$candidate_dll" artifacts/perf/round-two/worker-drain-ab
```

The same builds ran five pairs at 100 offered jobs/s for 12 s with 16 producer calls and 16 workers. Both settled every accepted job by the end of the tail drain. Baseline and candidate accepted medians were 653 and 630 of about 1,200 offers. Median confirmed settlements during the offer window were 376 and 388. Median p99 from dispatch to finish was 6.52 s before and 5.86 s after. The paired throughput changes ranged from −16% to +54%. The worker change alone left the producer overloaded, which is the next batching test.

### Q2 SQLite finish statements

The candidate writes resource usage in the attempt-close update. `FinishMany` also avoids per-item savepoints around `FinishJob`. An ownership refusal occurs before job writes and removes only its undecided receipt. An exception still rolls back the outer transaction. This removes one update per finish with usage and two statements per newly applied batch member. The SQLite `FinishMany` and attempt-usage contracts passed 9/9, including mixed outcomes, fault rollback and lost-reply replay.

Five 1,000-job, 16-worker drain pairs compared this change on top of the worker candidate. Baseline and candidate medians were 121.4 and 123.7 durable completions/s. Paired changes were −6.4%, −12.1%, +24.4%, +68.3% and +9.1%. Every run settled all 1,000 jobs. Mean `FinishMany` transaction body times had a median of about 14.9 ms on both builds, with varying batch sizes. The throughput effect remains inconclusive.

## Scheduler

The controlled-clock S0 regression materialized only 32 of 33 simultaneous schedules after 500 ms. With S1 the next page completes without waiting for the poll interval. A bounded keyset scan also reaches later eligible rows past a full page that cannot progress. The Release scheduling suite passed 174/174, and PostgreSQL, Redis and SQLite schedule contracts passed 68/68. The real P-06s comparison is pending.
