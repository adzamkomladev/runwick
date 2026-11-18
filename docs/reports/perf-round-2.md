# Runwick performance, round two

Status as of 2026-11-13: the changes below were measured on a shared host with default durability. The 100/s sustained and 2 s p99 targets, Redis parity and strict failover, and the full SP-029 release workload and quiet-host soak remain open.

| Workload | Best established result and its limit |
|---|---|
| SQLite producer and retained receipts | Grouping lifted median 16-caller add throughput from 102.1 to 226.7/s. The indexed receipt check delivered a 2.05× median paired gain on the 5,000-retained fixture. With 50,000 retained jobs, the integrated build settled 93.83/s during offers and recorded 2.56 s due-to-handler p99. |
| PostgreSQL direct dispatch | Moving the wake after commit raised median throughput from 226.6 to 301.8/s at 16 callers. The short integrated P-01 run settled 82.09/s during measurement at roughly 100 offers/s. |
| PostgreSQL outbox | Destination commit wait samples pointed to the transactional notification lock. The post-commit wake candidate passed its focused tests and identity smoke check. Five paired runs improved delivery throughput and p99, but no candidate arm met both P-09 thresholds. |
| Redis | Bounded inline projection eliminated observed normal-path recoveries in the matched bulk fixture. Steady completions at 40 and 100 offers/s were close to BullMQ; saturated throughput and steady offer-to-finish tails still lag. The Sentinel observer detects promotion but leaves continuity unresolved. |
| Scheduler | The eight-plan prototype preserved occurrence identity in two 15-minute steady measurements, with p99 samples on either side of 13 s. The completed retry also recovered all 1,000 outage occurrences. |
| Store dashboard | The Store page provides cached, authorized provider observations. Paired snapshot runs kept median completion rates close, with enough p99 variation to leave the 5% budget open. The authenticated HTTP pairs also kept completion rates close and left the p99 budget open. |

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

The initial steady fixture uses `--mode steady --rate 100 --seconds 30 --workers 16 --calls 16 --payload 1024 --trace`. These are no-op jobs with a nominal 1 KiB payload. The harness can now preserve a drained fixture with `--sqlite-keep` and copy it into a fresh database with `--sqlite-seed`. An eight-job, nominal 1 MiB drain and a 20-job, 64 KiB drain copied from a 100-terminal-job fixture both settled every job. Larger retained-history and multi-process runs are still pending.

In the 64 KiB smoke run, the writer recorded 818 statements during drain and opened one connection. After shutdown the WAL held 953 log frames at the configured 1,000-page automatic checkpoint threshold. The harness queries `PRAGMA wal_checkpoint(NOOP)` after the timed interval on SQLite 3.51 and later. This reads frame counts without running a checkpoint. A WAL hook would replace SQLite's automatic checkpoint callback. [SQLite checkpoint interface](https://www.sqlite.org/c3ref/wal_checkpoint_v2.html), [SQLite automatic checkpoint interface](https://www.sqlite.org/c3ref/wal_autocheckpoint.html).

Five fresh SQLite drain pairs compared the instrumented round-two baseline with the integrated `e1cb660` build on local ext4 with WAL and synchronous FULL. All ten arms accepted, invoked and durably settled exactly 1,000 jobs using 16 worker slots. Median throughput was 101.8 and 113.1/s, respectively. The paired median ratio was 1.23× and the range was 0.77–1.88×. This comparison leaves the several-fold fresh-database target open. Raw evidence is under `artifacts/perf/round-two/sqlite-round-two-total/`.

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

### Q1 SQLite producer grouping

Five alternating pairs isolated the queue collection step. The baseline used the same admission code, statement bodies, counters and current worker. Its writer executed each admitted dispatch individually. The candidate committed up to eight already-queued compatible dispatches together, capped by 1 MiB of estimated prepared envelope bytes. Tests confirmed independent outcomes and receipts for new, replayed, conflicting, invalid, expired and cancelled calls. A forced shared commit failure rolled back both jobs and both receipts, and each command retried with its original identity.

| Workload | Baseline | Grouped |
|---|---:|---:|
| 500 single adds, one caller, jobs/s median (min–max) | 78.5 (58.3–90.3) | 84.2 (41.9–101.1) |
| 500 single adds, 16 callers, jobs/s median (min–max) | 102.1 (87.5–111.8) | 226.7 (189.2–258.0) |
| 16 callers, dispatch commits per 500 accepted jobs, median | 500 | 64 |
| 16 callers, enqueue p99 median | 553 ms | 462 ms |
| 100/s offered for 12 s, accepted then durably settled, median | 722 | 982 |
| 100/s offered, durably settled during the 12 s offer window, median | 439 | 864 |
| 100/s offered, refusals, median | 479 | 218 |
| 100/s offered, due-to-start p99 median among accepted jobs | 4.46 s | 1.58 s |
| 100/s offered, dispatch-to-finish p99 median among accepted jobs | 5.84 s | 2.61 s |

The 16-caller enqueue gain was 1.97–2.51× across pairs, with a median of 2.16×. The grouped writer averaged 7.81 commands per dispatch commit at saturation and 7.19 under steady arrivals (medians across runs). With one caller both builds committed each command individually. The median paired single-caller throughput ratio was 1.02×, with a range of 0.72–1.48×. All accepted jobs in the steady test settled by the end of the tail drain. The remaining admission refusals leave sustained 100/s open.

In the steady runs, median producer writer-queue p99 fell from 1.61 s to 429 ms. The grouped writer spent 26.4 ms on average in the transaction body, compared with 4.2 ms per single dispatch, while committing roughly seven dispatches at a time. Median `FinishMany` writer-queue p99 moved from 125 to 160 ms and combined-claim p99 from 152 to 257 ms. These no-op workloads exercised little renewal traffic. The longer writer occupancy needs a lease-margin check with longer-running jobs and several processes before increasing the group cap.

A separate Linux `strace` diagnostic counted WAL synchronization calls for 200 adds from 16 callers, including setup and shutdown. Single dispatches used 215 `fsync`/`fdatasync` calls on the WAL, and the grouped run used 34. The grouped writer used 25 eight-command transactions. This sample measured syscall counts with tracing enabled.

```sh
BENCH_STORE=sqlite BENCH_TRACE=1 BENCH_ROUNDS=5 BENCH_ENQUEUE_N=500 \
  BENCH_SECONDS=12 BENCH_STEADY_CALLS=16 BENCH_TESTS='enqueue-c1 enqueue-c16 steady-100' \
  eng/bench-compare/ab.sh "$singleton_dll" "$grouped_dll" artifacts/perf/round-two/producer-ab
```

### Q2 SQLite finish statements

The candidate writes resource usage in the attempt-close update. `FinishMany` also avoids per-item savepoints around `FinishJob`. An ownership refusal occurs before job writes and removes only its undecided receipt. An exception still rolls back the outer transaction. This removes one update per finish with usage and two statements per newly applied batch member. The SQLite `FinishMany` and attempt-usage contracts passed 9/9, including mixed outcomes, fault rollback and lost-reply replay.

Five 1,000-job, 16-worker drain pairs compared this change on top of the worker candidate. Baseline and candidate medians were 121.4 and 123.7 durable completions/s. Paired changes were −6.4%, −12.1%, +24.4%, +68.3% and +9.1%. Every run settled all 1,000 jobs. Mean `FinishMany` transaction body times had a median of about 14.9 ms on both builds, with varying batch sizes. The throughput effect remains inconclusive.

A follow-up keeps the receipt lookup and deadline checks, then inserts decided receipts for applied `FinishMany` items together. The writer lock and distinct command IDs protect the receipt checks until commit. All eight focused SQLite batch and usage contracts passed. Five alternating 1,000-job drain pairs with 16 workers used independent copies of the same 5,000-terminal-job fixture. Every run settled all 1,000 jobs.

| Retained SQLite drain, median | Previous receipt path | Grouped decided receipts |
|---|---:|---:|
| Durable completions/s | 40.1 | 40.2 |
| Native writer statements | 26,323 | 24,710 |
| Writer commits | 440 | 434 |
| Mean `FinishMany` transaction body | 119.6 ms | 105.3 ms |
| Process managed allocation during drain | 177.7 MB | 179.5 MB |

The paired throughput ratios ranged from 0.94× to 1.13× with a median of 1.00×. Statement counts fell 5.5–6.7% across pairs. The shorter batch transaction body left throughput nearly unchanged on this retained fixture. Raw results, the retained seed and the comparison script are under `artifacts/perf/round-two/sqlite-q2-receipts/`.

### Q3 SQLite writers in separate processes

Six runs drained copies of the same freshly seeded 1,000-job SQLite file with 16 total worker slots. One process used a single 16-slot worker. Four processes used four slots each and shared the same file. Every run durably settled 1,000 jobs with 1,000 attempts and 1,000 handler invocations. The rate uses the stored first attempt authorization and last finish times. Launch-through-drain includes process startup and polling until all finishes were visible.

| SQLite no-op drain | One process | Four processes |
|---|---:|---:|
| Median jobs/s (min–max) | 111.7 (111.3–134.3) | 47.9 (42.1–48.9) |
| Median launch-through-drain | 10.33 s | 22.07 s |
| Worst worker-operation `BEGIN IMMEDIATE` p99, range across runs | 0.44–3.89 ms | 2.19–2.23 s |

Multiple writers contended on the file despite the same total slot count. They also sent more separate finish calls as each process collected a smaller batch. The largest waits were near the configured two-second SQLite busy timeout. The next experiment sweeps the existing busy timeout and measures refusals, lease safety and writer wait with the same process count. Raw results and the process runner are under `artifacts/perf/round-two/sqlite-processes/`.

Microsoft.Data.Sqlite 10.0.14 retries busy or locked results from both prepare and step. Its managed one-second command timeout can outlast a shorter native busy timeout, and the native wait or final 150 ms sleep can overshoot the managed deadline. The Q3 prototype compares the managed path with a native `BEGIN IMMEDIATE` call under the existing SQLite busy handler and Runwick's same-command retries. [Managed prepare loop](https://github.com/dotnet/efcore/blob/v10.0.14/src/Microsoft.Data.Sqlite.Core/SqliteCommand.cs#L420-L493), [managed step loop](https://github.com/dotnet/efcore/blob/v10.0.14/src/Microsoft.Data.Sqlite.Core/SqliteDataReader.cs#L135-L190), [SQLite busy timeout](https://www.sqlite.org/c3ref/busy_timeout.html).

The contention sweep ran three trials per setting with four processes and 16 slots total. All 12 runs completed 1,000 jobs with exactly 1,000 attempts and invocations. Redis crash tests ran on the same host, so throughput is directional.

| Writer lock path | Busy timeout | Median jobs/s | Median of worst worker-operation `BEGIN IMMEDIATE` p99 |
|---|---:|---:|---:|
| Managed | 2,000 ms | 38.1 | 2,231 ms |
| Managed | 100 ms | 36.3 | 1,256 ms |
| Native | 100 ms | 39.2 | 277 ms |
| Native | 500 ms | 42.2 | 617 ms |

The native path reduced long writer waits. Throughput remained within 36–42 jobs/s at the medians. The prototype remains isolated while the retry budget under an externally held write lock is evaluated. Raw runs and the summarizer are under `artifacts/perf/round-two/sqlite-q3-busy/`.

### Q4 SQLite WAL and retained read snapshots

Three alternating pairs drained 2,000 jobs with 16 worker slots and eight seed callers on copies of the 5,000-terminal-job fixture, using the Q5 index seek. One variant held a separate read-only transaction from the end of seeding until after the drain. Every run settled all 2,000 jobs once with the same WAL and synchronous FULL settings and default 1,000-page autocheckpoint.

| WAL after drain | Default | Pinned reader |
|---|---:|---:|
| File size (min–max) | 4.85–4.98 MB | 99.46–105.00 MB |
| Log frames (min–max) | 378–768 | 24,140–25,486 |
| Checkpointed frames at end (min–max) | 0 | 0–207 |

After the pinned reader closed, one passive checkpoint backfilled all observed log frames in every run. The calls took 49–525 ms on a host shared with the integration suites. Drain throughput varied from 93–149/s in controls and 55–155/s with a pinned reader, leaving the throughput effect inconclusive. The pinned readers retained more than the 64 MiB journal size limit. The experiment shows the cost of holding a long snapshot and leaves the default checkpoint policy in place. Measuring dashboard and retention activity alongside production-like loads is still open.

```sh
dotnet "$workload_dll" bench --store sqlite --mode drain --n 2000 --workers 16 --calls 8 \
  --trace --sqlite-seed "$terminal_fixture" --sqlite-hold-read --out "$output"
```

Omit `--sqlite-hold-read` for the control. Raw results and the runner are under `artifacts/perf/round-two/sqlite-q4-wal/`.

### Q5 SQLite receipt lookups with retained history

SQLite 3.53.3 chose the primary key for the replay lookup and the undecided-receipt check. The primary key orders by retention class before command ID, so both plans searched only on application and visited unrelated receipts. Both queries now use the existing `runwick_command_by_id` index to seek by application and command ID. Replay still examines both retention classes and prefers the command's own class. All 15 focused receipt, finish and transaction tests passed.

Five alternating 1,000-job drain pairs used copies of the same 5,000-terminal-job fixture, one process with 16 worker slots, 1 KiB payloads and WAL with synchronous FULL. All ten runs accepted, invoked and durably settled 1,000 jobs each. The full Redis test suite shared the host during part of the comparison.

| Retained SQLite drain | Primary key search | Command ID seek |
|---|---:|---:|
| Median durable completions/s (min–max) | 68.1 (60.4–70.7) | 145.2 (99.9–159.1) |
| Median writer time in the two receipt queries | 8,002 ms | 35 ms |
| Median writer commits | 476 | 478 |

The median paired throughput ratio was 2.05×, with a range of 1.65–2.56×. A separate 30-second open-loop pair used 16 callers, 16 worker slots and 100 offers/s on the same retained fixture.

| SQLite steady at 100 offers/s | Primary key search | Command ID seek |
|---|---:|---:|
| Offered | 3,001 | 3,001 |
| Accepted and eventually completed | 844 | 3,001 |
| Completed before offers stopped | 434 | 2,990 |
| Local admission refusals | 2,157 | 0 |
| Due-to-handler p99 among accepted jobs | 12.88 s | 0.80 s |

The candidate kept pace in this short run. Sustained capacity with larger retained history, maintenance and readers is still open. Query plans, provider binary checksums and raw results are under `artifacts/perf/round-two/sqlite-q5-receipt-index/`.

A four-minute diagnostic at `2978f04` offered 24,000 nominal 1 KiB no-op jobs at 100/s on a copy of the 5,000-terminal-job fixture. It used 16 callers, 16 worker slots, SQLite 3.53.3, local ext4, and WAL with synchronous FULL. It accepted and settled 21,774 jobs once. Of those, 21,710 settled during offers, or 90.5/s. The other 2,226 offers were refused by local capacity. Due-to-handler p95/p99 was 1.82/3.94 s and offer-to-finish p99 was 5.61 s. It missed both the sustained rate and due-to-start targets on this shared host.

The writer recorded zero exhausted busy waits. Dispatch grouped 21,774 jobs into 3,978 commits. Summing each recorded phase's mean by its call count, statements and commit accounted for about 76 s in dispatch, 88 s in settlement, and 70 s in combined claims across the run. Writer queue p99 was 543 ms for dispatch, 3.81 s for sampled presence writes and 8.51 s for cleanup. The workload recorded one control-wait pressure stop and repeated presence publication failures. This sample locates the rate limit on the shared writer and records background delays under contention. Raw results and exact build inputs are under `artifacts/perf/round-two/sustained-100/`.

The same fixture exposed a correlated detail-retention query that scanned all application attempts for each eligible job. Selecting the bounded candidate jobs first and summing error bytes by their job IDs lets SQLite seek into an existing index. On 100 candidates from the retained fixture, the query ran 2,503,811 VM instructions before and 6,155 after. A single local query timing moved from 106.9 to 3.6 ms. The query retains the oldest-first order and the existing payload-plus-error byte limit. The 42 focused retention and settlement contracts passed across all three providers.

The job-history purge lookup also searched the dispatch-key primary index by application alone. On a copy with 5,000 window bindings, half expired, directing the lookup to the existing bound-job index reduced the work for 100 eligible jobs from 2,702,003 to 7,213 VM instructions. Both variants applied the existing live-binding and permit exclusions. The SQLite retention contracts passed 7/7, including protection of live windows and permits. These are query-level measurements. The production changes add no statistics writes. Ready, expired-lease and distinct-job-name queries already used their expected indexes in the retained fixture.

A larger fixture seeded and settled all 50,000 jobs once at 145.6/s. A copy then received 24,000 offers over four minutes at 100/s. It accepted and settled 22,565 jobs once and completed 22,518 during offers, or 93.83/s. Local capacity refused 1,435 offers. Due-to-handler p99 was 2.56 s and producer writer queue p99 was 549 ms. This single run on `e1cb660` missed both the sustained rate and due-to-start targets. Raw results are under `artifacts/perf/round-two/sqlite-retained-50k/`.

### Q6 SQLite local wake experiment

An isolated candidate sends a queue hint after a direct dispatch commits. Local workers using the same file and application can receive it across separate store instances. Workers retain the normal polling cap for remote writers, staged transactions and missed hints. The focused test held a dispatch before commit, then verified that the second store instance received the hint after commit even when another subscriber threw. Replaying the accepted dispatch did not send another hint. All three dispatch-group tests passed.

Five alternating pairs offered three jobs/s for 15 seconds with 16 worker slots. All offers were accepted and settled once. Median due-to-handler p50/p95 moved from 143/346 ms to 30/117 ms. The candidate lowered p50 in every pair. There were about 45 observations per run, too few for a stable p99 estimate. In a separate five-pair 1,000-job drain the median paired throughput ratio was 1.09×, with a range of 0.85–1.29×. The candidate remains isolated pending a quieter check of saturated throughput. The patch and raw results are under `artifacts/perf/round-two/sqlite-q6-wake/`.

### B4 SQLite settlement experiment

An isolated build used one completion consumer to give ready finishes time to collect behind the previous call. Five paired 1,000-job drains with 16 workers reduced median committed settlement calls from 289 to 183 and total writer commits from 442 to 376. Median throughput was 101.6 jobs/s with four consumers and 104.4 with one. The median paired gain was 3.9%, and individual pairs varied from a 16.6% drop to a 39.9% gain. Production uses four completion consumers. The next finish experiment should account for transaction body cost and protect unrelated finishes during receipt resolution.

A follow-up left the completion consumers in place and grouped adjacent standalone SQLite `Finish` calls on the existing writer. Each command retained its own deadline, receipt and savepoint and completed after the shared commit. The queue still gave renewal priority, and staged finishes and `FinishMany` used their existing paths. Five focused Release tests passed, including scoped jobs, stale and expired items, rollback, cancellation and replay after a lost reply.

Five alternating 60-second pairs used 100 offers/s, 16 caller and worker slots, and copies of the same 5,000-terminal-job fixture. All accepted jobs settled once in every arm, and the writer reported zero exhausted busy waits. Median settlement commits fell from 489 to 348 per 1,000 completions, with reductions in all pairs. The paired median reduction was 30.1%. Median settlement statement and commit time fell from 3.84 to 3.25 ms per completed job.

The median paired completion-rate change during offers was −1.18/s, ranging from −37.10 to +2.83/s. The paired median due-to-handler p99 change was +255 ms. In the slowest candidate arm, dispatch and finish commits both slowed and 2,204 of 6,000 offers were refused. The sampler saw no overlapping test, build or benchmark. The runtime change stays isolated because the throughput and tails cross the acceptance band. Raw data and the patch are under `artifacts/perf/round-two/sqlite-finish-group/`.

## Scheduler

The controlled-clock S0 regression materialized only 32 of 33 simultaneous schedules after 500 ms. With S1 the next page completes without waiting for the poll interval. A bounded keyset scan also reaches later eligible rows past a full page that cannot progress. The Release scheduling suite passed 174/174, and PostgreSQL, Redis and SQLite schedule contracts passed 68/68.

A shortened P-06s pair used PostgreSQL 18.6, 1,000 schedules, the default 100 jobs/s scheduler allowance, 2 minutes of warm-up, 2.1 minutes of measurement and a 60-second lag tail. The measured interval crossed three minute boundaries on S0 and two on S1. Both runs materialized every expected occurrence. There were zero censored occurrences and zero correctness violations. Catch-up workload qualification is still pending.

| Scheduled time to recorded materialization | S0 | S1 |
|---|---:|---:|
| Materialized / expected | 3,000 / 3,000 | 2,000 / 2,000 |
| p50 | 27.80 s | 15.21 s |
| p95 | 52.28 s | 25.35 s |
| p99 | 54.49 s | 26.10 s |
| Max | 56.34 s | 26.40 s |
| Productive poll waits, whole scheduler lifetime | 161 / 155.6 s | 6 / 5.2 s |

S1 cut p99 by 52.1% in this pair. The 13 s target remains open. In S1, serial materialization averaged 26.702 ms per command, including 16.184 ms in the database function and 9.798 ms in commit. Mean driver-lane wait was 0.001 ms, evaluation 0.086 ms, and allowance wait 0.003 ms. At those costs 1,000 serial commands take roughly 26.7 s. The next experiment varies bounded materialization concurrency with the allowance held constant.

```sh
dotnet "$scheduler_workload_dll" run P-06s --out "$output" \
  --warmup-min 2 --measure-min 2.1 --lag-tail-s 60 --catchup-occurrences 0
```

The fixture reconciles committed occurrence rows after the run. Its lag uses `materialized_at`, assigned inside the transaction before COMMIT. A post-commit measurement is needed to qualify a run close to the 13 s gate. Raw results, start/load logs, the baseline instrumentation patch and binary checksums are under `artifacts/perf/round-two/p06s/`.

An isolated S2 prototype retained one evaluator and one 100 jobs/s allowance, with at most one pending command per schedule. A single 1/2/4 sweep used the same Release binary, 1,000 schedules, 2 minutes of warm-up, 2 minutes of measurement and a 60-second tail. Each run materialized all 2,000 expected occurrences with zero censoring, duplicates, lease losses or correctness violations.

| Pending commands | Materialization p95 / p99 | Mean PostgreSQL lane wait | Mean function / commit |
|---:|---:|---:|---:|
| 1 | 29.61 / 30.87 s | 0.001 ms | 16.93 / 9.92 ms |
| 2 | 19.74 / 20.60 s | 0.002 ms | 19.40 / 14.05 ms |
| 4 | 25.81 / 26.97 s | 34.41 ms | 20.14 / 16.15 ms |

Two pending commands reduced p99 by 33% in this sweep. All settings exceeded the 13 s gate. Four commands queued behind the two-slot PostgreSQL background lane and also ran during a noisier load interval. The prototype remains isolated pending paired qualification. Logs and the patch are under `artifacts/perf/round-two/p06s-s2/`.

S4 returns the next eligible scan time on an empty head read. It subtracts read and materialization time from the local wait using a monotonic anchor. Notifications and bounded polls cover edits the host has yet to observe. The eight scheduler-loop tests and all 71 shared scheduler contracts passed on PostgreSQL, Redis and SQLite, including cold starts with clock skew and the cursor head recheck.

The measured two-slot mean would have to drop from 34.4 ms to below 26 ms per command before due-read overhead to reach 1,000 schedules in 13 s. The next experiment groups independent plans under shared commits while preserving per-plan decisions and original receipt identities. The design notes are under `artifacts/perf/round-two/p06s-s3/`.

The isolated S3 prototype ran a shortened P-06s pair with two groups in flight and either one or four independent plans per group. Both runs used 1,000 schedules, the same 100 jobs/s allowance, two minutes of warm-up, two minutes of measurement and a 60-second tail. Each acknowledged all 2,000 expected occurrences with zero revision conflicts, lease losses or correctness violations. The Redis crash suite shared the host.

| Post-commit scheduler acknowledgment | One plan | Four plans |
|---|---:|---:|
| p50 | 14.58 s | 6.11 s |
| p95 | 25.88 s | 11.05 s |
| p99 | 27.29 s | 11.38 s |

The prototype processed 5,647 plans in 1,412 grouped commits over the scheduler lifetime and met the 13 s target in this pair. Both commit-acknowledgment logs are under `artifacts/perf/round-two/p06s-s3/`.

The hardened implementation landed with schema revision 22 and opt-in `SchedulerOptions.GroupMaterializations`. It uses a shared materialization function, at most two groups of four independent plans, and up to 2 MiB of payload per group. Every plan retains its receipt identity, deadline and result. Unsupported plans use the single-command path. The 50 PostgreSQL and SQLite schedule contracts and 180 scheduler tests passed, including per-item rollback, same-ID replay after a lost commit reply, overlapping groups and shutdown. The migration suite passed 78 cases and the fresh-install snapshot passed after correcting the expected schema revision to 22.

The hardened build ran the full 15-minute steady window with 1,000 schedules on PostgreSQL 18.6. It acknowledged all 15,000 expected occurrences. Post-commit p50/p95/p99 was 6.82/14.09/19.63 s, missing the 13 s p99 gate. Across the scheduler lifetime it processed 19,000 plans in 4,750 grouped commits. Mean database function and commit phases were 61.3 and 24.9 ms per group. Mean background-lane wait was 0.002 ms. The host was shared and compilation and integration work overlapped some measured intervals.

After 1,000 seconds of downtime, the restart materialized all 1,000 catch-up occurrences within 65.3 s of restart, with at most 32 occurrences in a command and no review state. Across the complete run, 21,359 created occurrences mapped to 21,359 scheduled jobs with zero duplicate instants, orphan occurrences, shared jobs or lease losses. The catch-up correctness check passed. Steady-state qualification and a matched full control remain open. Raw acknowledgments, marks, load and results are under `artifacts/perf/round-two/p06s-s3-production/`.

One matched short pair at `e1cb660` compared the existing four-plan group with an isolated eight-plan group. Both used two groups in flight, the 2 MiB group payload cap, and the existing 100 jobs/s allowance. Each arm used a fresh PostgreSQL 18.6 fixture, two minutes of warm-up and three minutes of measurement. The eight-plan arm ran first. Each acknowledged all 3,000 measured occurrences, with no duplicate instants, orphan or shared jobs, or lease losses.

Post-commit p50/p95/p99 was 8.12/13.12/13.99 s with four plans and 6.52/10.99/11.61 s with eight. Mean function and commit time per group was 56.1/25.6 ms and 95.3/27.4 ms, respectively. The eight-plan arm met the 13 s target in this short comparison. Longer steady-state and catch-up qualification for the larger groups remain open. Patch, acknowledgment identities and results are under `artifacts/perf/round-two/p06s-s3-group-size/`.

The first 15-minute eight-plan run stopped during the planned downtime before catch-up completed. Its raw acknowledgment log still contains 15,000 distinct measured occurrences, with 1,000 at each of 15 minute boundaries. The nearest-rank post-commit p50/p95/p99 was 5.92/10.78/12.47 s. These records establish the steady acknowledgment timing for that fixture. Catch-up and full end-of-run reconciliation require a complete repeat.

The repeat recorded 15,000 distinct measured acknowledgments, again with 1,000 per minute boundary. Its raw post-commit p50/p95/p99 was 6.20/11.87/13.86 s. The two long steady samples therefore fall on either side of the 13 s gate. After 1,000 seconds of downtime the repeat recovered all 1,000 missed occurrences within 69.3 s of restart, with at most 32 per command and no review state. Across the full run, 21,757 occurrences matched 21,757 scheduled jobs, with zero duplicates, orphans, shared jobs or lease losses. The additive migration candidate remains isolated. Raw results are under `artifacts/perf/round-two/p06s-s3-group-size/group-8-long-retry/`.

The additive revision 23 candidate preserves migrations 21 and 22 and retains four as the default group size when grouping is enabled. Its Release build passed, followed by 181 core scheduler tests, 82 PostgreSQL migration and upgrade tests, 26 PostgreSQL scheduler contracts, 24 applicable SQLite scheduler contracts and 23 applicable Redis scheduler contracts. The Redis-only filter excludes a lost-reply theory with no Redis data. The migration remains isolated pending timing qualification. Evidence is under `artifacts/perf/round-two/p06s-s3-group-size/additive/`.

## PostgreSQL post-commit wake

Migration 0021 adds a versioned direct-dispatch path at schema revision 21. Direct dispatch now queues a best-effort wake after durable commit for newly created jobs due at the database decision time. Staged dispatch, scheduler, outbox and management writes retain their transactional notification paths. The 79 targeted wake, migration and upgrade tests passed, including a forced wake failure followed by resolving the accepted dispatch.

The first five pairs compared transactional and post-commit dispatch wakes with 500 adds per run. Both builds used schema 21 on PostgreSQL 18.6 with `fsync`, `synchronous_commit` and `full_page_writes` enabled. The enqueue timer included each new process's connection and application startup. At 16 callers, median commit p99 fell from 168 to 66 ms, while median end-to-end p99 rose from 940 to 1,293 ms. The median paired throughput ratio was 0.94× at 16 callers.

Five follow-up pairs ran 128 warm-up adds in the measured process before timing 750 further adds. Both builds ran the same workload and diagnostic code against schema 21. The baseline used the transactional dispatch function and the candidate used the post-commit driver.

| Warm enqueue | Transactional wake | Post-commit wake |
|---|---:|---:|
| One caller, median jobs/s (min–max) | 49.6 (46.0–55.0) | 51.3 (49.4–56.9) |
| One caller, median p95 / p99 | 42.5 / 72.9 ms | 38.9 / 62.7 ms |
| 16 callers, median jobs/s (min–max) | 226.6 (206.4–236.7) | 301.8 (276.8–373.8) |
| 16 callers, median p95 / p99 | 155.1 / 246.0 ms | 120.3 / 182.2 ms |
| 16 callers, median commit p99 | 196.4 ms | 86.6 ms |

The 16-caller throughput ratio improved in all five pairs (median 1.46×, range 1.17–1.63×). The one-caller ratios ranged from 0.91× to 1.15× with a median of 1.05×. The median 16-caller function phase mean rose from 19.9 to 27.6 ms. The function and commit phases include the server response and client scheduling. Raw cold and warm results are under `artifacts/perf/round-two/postgres-a1-ab/` and `postgres-a1-warm-ab/`.

A separate 16-caller, 2,400-add sample read `pg_stat_activity` and `pg_locks` every 50 ms. The transactional run recorded 1,604 active-session observations waiting on an ungranted heavyweight `AccessExclusiveLock` for object ID zero out of 2,441 active observations. That shape matches PostgreSQL's commit-order notification lock. The post-commit run recorded zero such waits across 1,518 active observations. Its most frequent classified wait was `LWLock:WALWrite` with 431 observations. No advisory lock wait was sampled in either run. The counts describe sampled sessions and do not measure individual request durations. [PostgreSQL 18.6 notification implementation](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/async.c#L840-L868).

```sh
BENCH_TRACE=1 BENCH_ROUNDS=5 BENCH_ENQUEUE_N=750 BENCH_WARMUP_JOBS=128 \
  BENCH_TESTS='enqueue-c1 enqueue-c16' \
  eng/bench-compare/ab.sh "$transactional_dll" "$post_commit_dll" artifacts/perf/round-two/postgres-a1-warm-ab
```

### A6 PostgreSQL call text and policy cache experiment

An isolated candidate reused the fixed capabilities JSON, named SQL text per function version, and JSON for immutable retry-policy objects. Both builds already had Npgsql auto-prepare set to 64 statements per connection with a two-use threshold. The exact SQL budget and wake tests passed after the budget test counted physical connection spans separately from SQL commands.

Five pairs warmed 128 adds before timing 350. At one caller, median process allocation fell from 125.7 to 113.4 kB per add, with a 9–10% reduction across pairs. Median throughput moved from 57.3 to 49.2/s and enqueue p99 from 46.6 to 77.6 ms. The paired throughput ratios ranged from 0.70× to 1.25×, with function and commit time also moving between runs. At 16 callers median throughput moved from 363.7 to 404.6/s, with paired ratios of 0.70–1.30× and nearly unchanged median allocation. The single-caller tail crossed the acceptance band in three pairs, so the cache remains an experiment for a quieter rerun. Raw data and the patch are under `artifacts/perf/round-two/postgres-a6-ab/` and `next-build/a6.patch`.

### PostgreSQL physical write profile

An isolated PostgreSQL 18.6 container with `fsync`, `synchronous_commit` and `full_page_writes` enabled accepted and settled 20,000 nominal 1 KiB no-op jobs with 16 workers at 174.9 completions/s. The profile used the schema-21 build with the experimental A6 client cache and opt-in tracing. The isolated server reported 239.1 MB of additional WAL, about 12.0 kB per accepted and completed job including enqueue, migration and fixture setup. Drain allocated about 82.7 kB of managed memory per job in the benchmark process.

| Relation | Inserts | Updates | HOT updates | Index bytes after drain |
|---|---:|---:|---:|---:|
| `jobs` | 20,000 | 40,000 | 0 | 17.1 MB |
| `job_attempts` | 20,000 | 40,000 | 13,277 | 9.4 MB |
| `queue_events` | 80,000 | 0 | 0 | 15.0 MB |
| `storage_commands_durable` | 20,202 | 20,202 | 13,754 | 4.2 MB |

The jobs table still had an estimated 15,463 dead tuples after two autovacuum passes. The `jobs_name_list` index recorded 6.40 million entries returned across 523 scans in this fixture without dashboard traffic. Nested SQL plans are needed to attribute those reads before changing the index. Server self-time was 24.6 s for `rw_claim_plain_v1`, 22.7 s for `rw_receipt_v1` and 14.8 s for the decided-receipt constraint trigger across the run. The profile points to attempt-close writes, receipt work and job-index maintenance as the next measured experiments. Raw table, index, function and WAL stats are under `artifacts/perf/round-two/postgres-write-profile/`.

A4's isolated finish prototype combined resource usage with attempt close. A single 5,000-job pair settled every job with one attempt and recorded usage for each. Attempt updates fell from 10,000 to 5,000 and WAL from 64.9 to 63.1 MB. Drain throughput moved from 118.7 to 125.2/s while the Redis fault suite shared the host. Focused PostgreSQL tests covered staged rollback, exact replay and batched usage. The migration remains isolated. Raw results are under `artifacts/perf/round-two/postgres-a4-pair/`.

The rebased A4 candidate passed 99 PostgreSQL finish, capacity, migration and upgrade tests and seven shared PostgreSQL contracts. Two final replay and staging checks passed after strengthening the replay-only assertion. Five warmed alternating pairs used fresh PostgreSQL 18.6 containers with full durability and 5,000 jobs per arm. Every arm settled all jobs once, with usage on every attempt. Attempt updates fell from two to one per close in every pair. Whole-fixture WAL per close fell by 2.78% at the paired median, with a 1.86–4.18% reduction across pairs. The WAL window includes setup and enqueue.

Median throughput was 141.1/s on the baseline and 128.0/s on the candidate. Paired changes ranged from −28.4% to +58.5% with a median of +1.0%. Median handler-to-finish p99 was 185.2 versus 174.0 ms, with paired changes from −72.5% to +27.3%. These ranges cross the 10% acceptance band, so revision 23 remains an isolated candidate pending quieter measurements. Raw results and the exact patch are under `artifacts/perf/round-two/postgres-a4-production/`.

A separate PostgreSQL 18.6 diagnostic used sampled nested `auto_explain` with buffers and WAL. It settled 5,000 jobs once and wrote 65.5 MB of WAL during the fixture. The retained plan sample showed plain claim and readiness queries using `jobs_ready`, both receipt partitions using their command-ID indexes, and distinct finish attempt-close and usage updates. PostgreSQL reported 2.46 million entries returned by `jobs_name_list`, but Docker log rotation removed the early plan records. The remaining samples did not attribute that index. Raw plans and counters are under `artifacts/perf/round-two/postgres-nested/`.

The rerun retained the full log. It settled all 5,000 jobs once and recorded 2.88 million entries returned by `jobs_name_list`. Of 21 sampled uses of that index, nine came from the plain finish function, nine from the combined claim's payload fetch, and three from the finish batch's dispatch-key classification query. They used bitmap scans keyed only by application, despite the small sets of requested job IDs. This points to keyed joins in those paths before changing the dashboard index. The full log and per-plan attribution are under `artifacts/perf/round-two/postgres-nested-full/`.

An isolated C1 candidate binds only the claim payload's job-row lookup to each returned job ID, preserving the batched payload scan. A PostgreSQL 18.6 fixture reproduced the stale-statistics plan. For 11 requested IDs, it moved from scanning 4,813 job entries to 11 keyed lookups and from 4,831 to 56 shared buffer hits. The healthy generic 32-item plan stayed at 139 hits. A 12-item replay moved from 72 to 59. All 15 focused PostgreSQL claim and migration tests and 12 claim and worker-batch contracts passed.

Five alternating C1 pairs used a `43b3135` baseline, fresh PostgreSQL 18.6 containers, 5,000 jobs per arm, 16 worker slots and a 2% nested plan sample. Every arm accepted, invoked and durably settled all 5,000 jobs. Baseline samples contained 17 application-wide job scans across 73 sampled payload queries. The candidate had zero such scans across 69 sampled queries. Median whole-fixture `jobs_name_list` entries read fell from 3.31 million to 2.14 million. Claim function p95 improved in all five pairs, with a median paired change of −3.5%.

Median drain throughput moved from 146.2 to 139.0/s, with paired changes from −25.4% to +24.0% and a median of −4.1%. Handler-to-finish p99 was 133.8 versus 210.1 ms by arm median. The paired median change was +9.4%, ranging from −4.1% to +100.7%. Whole-fixture WAL was nearly flat. The candidate stays isolated because the shared-host tails and throughput cross the 10% acceptance band. Raw results and the exact patch are under `artifacts/perf/round-two/postgres-c1-payload/`.

The A5 source assessment traced a wait on a higher command ID during receipt seeding. A set-based insert followed by conflict and clock checks could delay the lower ID's checks until after a concurrent cross-class insert or deadline change. Rolling back the speculative insert releases its locks, and the retry still observes the later state. A successful insert in a subtransaction would also need a replacement for the current receipt discard's top-level transaction-ID check. The assessment records a concurrency and fault test matrix. No A5 migration was built.

Across the five C1 baseline logs, 361 sampled finish-batch receipt inserts took a median 0.179 ms and p95 0.607 ms. The paired cross-class probes took 0.010 and 0.023 ms. This sample covers fresh receipts. Replay and retention races remain outside that profile. The assessment, parser and results are under `artifacts/perf/round-two/postgres-a5/`.

### PostgreSQL producer batching experiments

The isolated B1 SQL proof passed against a disposable migrated database. It covered per-command replies, mixed refusals and replay, event attribution, delayed eligibility, and rollback on a proposed job ID collision. The proof used one transaction and rolled back its fixture. Producer queue integration, cancellation, recovery, and timed comparisons remain open. Source and proof are under `artifacts/perf/round-two/pg-b1/`.

A follow-up B1 Npgsql transport proof verified precommit invisibility, rollback, and per-command replay after a simulated lost reply. The fixture initially rejected two test deadlines with sub-millisecond precision. The corrected fixture passed, and all 29,952 measured jobs reconciled across 54 arms. The three paired repeats per cell compared singleton submissions with explicit groups of eight ready jobs. At 16 callers the median paired throughput ratios were 1.17× at 1 KiB and 1.13× at 64 KiB, with both ranges crossing parity. At eight callers the corresponding ratios were 0.92× and 0.78×. Groups of eight 1 MiB items exceeded the proposed 2 MiB producer queue bound and regressed in every eight- and sixteen-caller pair, with median ratios of 0.31× and 0.26×. The one-caller control used the same singleton route in both arms and still varied. Grouping reduced submissions and commits by 87.5% when groups were full, with similar WAL per job for the small payloads. These results support keeping any follow-up byte bounded and checking whether real admission fills groups. They do not qualify production batching. Evidence is under `artifacts/perf/round-two/pg-b1/transport/`.

B2 compared the existing array membership checks with ordinal joins in a revision-23 experiment at the current 100-item cap. Both arms passed seven focused tests. Each measured arm reconciled 10,000 jobs plus 1,000 warm-up jobs across jobs, payloads, generations, events, and receipts. At one caller, the median paired throughput ratio was 1.03× with a range of 0.90–1.20×. At eight callers, the candidate was slower in every pair. Median throughput fell from 3,651 to 2,955 jobs/s and median paired batch p99 increased 51%. The join stays out of the runtime. The larger cap remains untested. Results are under `artifacts/perf/round-two/pg-b2-membership/`.

### PostgreSQL short P-01

The `2978f04` build ran one standard P-01 short repetition on PostgreSQL 18.6 with one minute of warm-up, three minutes of measurement, one 16-slot worker and 100 ms handlers. The producer averaged 99.66 offers/s and 98.65 accepted/s. The database window recorded 82.09 durable completions/s and backlog growth of 17.73 jobs/s. All 23,518 accepted jobs completed after a 48-second tail drain, with one invocation and execution per job.

Due-to-Begin p99 was 46.44 s in the worker's measurement and drain samples. Pipeline-to-finish p99 was 208 ms. There were no sampled worker pressure stops or database lock waiters in the measurement window. The worker's samples nearest the window boundaries recorded 5,112 applied combined claims for 14,872 items and zero standalone Reserve calls. Of 14,876 confirmed finishes in those samples, 9,151 came through 1,980 finish batches. Managed allocation in the worker process was approximately 103 kB per confirmed finish. The rate, backlog and due-to-start gates remain open on this shared host. Raw P-01 results are under `artifacts/perf/round-two/sustained-100/P-01/`.

## Redis Phase 0

A dedicated Redis 8.2.11 server used `appendonly yes`, `appendfsync always` and `noeviction`. A healthy store published both test batches, but each went through intent recovery. The probe measured intent shape with a separate injected interruption after intent storage. The traces shown here came from fault-free submissions.

| Command | After-images | Distinct written keys | Serialized intent bytes | Projection arguments | Script replies |
|---|---:|---:|---:|---:|---|
| `DispatchMany(100)`, nominal 1 KiB jobs | 210 | 209 | 238,927 | 6,528 | `dispatch:P`, `recovery:MORE`, `recovery:PUBLISHED`, `dispatch:R` |
| `FinishMany(32)` | 105 | 72 | 69,483 | 2,050 | `finish_many:P`, `recovery:PUBLISHED` |

The bounded inline projection changed both healthy traces to a single `R` reply. A 100-job batch with 8 KiB payloads still used recovery. All 11 targeted regression and recovery tests passed. REDIS-16 sent 80 batches with eight concurrent callers, stored all 8,000 jobs, and recorded zero `RECOVERING` replies. The loaded-host REDIS-07 sample on the largest fallback intent observed Renew p99 of 879 ms.

Five alternating bulk-add pairs submitted 2,000 jobs per run in batches of 100 with eight callers. Every run used a fresh Redis 8.2.11 container with AOF `always`, and accepted every job. Median throughput rose from 684.5 to 903.5 jobs/s. Paired ratios ranged from 1.19× to 1.75×, with a median of 1.38×. Median recovery calls fell from 150 to zero. Median `EVALSHA` p99 histogram buckets rose from 67.6 to 135.3 ms as more projection work moved into each call. Those buckets aggregate all scripts. [Redis histogram bucket format](https://redis.io/docs/latest/commands/latency-histogram/).

A separate REDIS-07 diagnostic pair ran a single background producer continuously alongside renewals. Each 100-job write carried about 1 KiB per payload and projected 210 after-images across 209 keys. In 20 seconds the baseline ran 117 writes and the inline candidate ran 176. Renew p99 fell from 684.4 to 201.0 ms and RenewMany(64) p99 from 340.0 to 191.6 ms across 143 baseline and 156 candidate samples. Both tests passed. The largest fallback intent is a separate case and its observed 879 ms Renew p99 still exceeds the 250 ms reference target. These are short shared-host runs. Raw results and the fixture patch are under `artifacts/perf/round-two/redis-a7-renewal-*`.

The workload runner can count Redis script names and reply statuses with `--trace`. `LATENCY HISTOGRAM evalsha eval` gives server-side distributions by Redis command. Combine the histogram with the client counters to identify the workload mix. `INFO commandstats` supplies cumulative execution time and call totals, and `INFO persistence` supplies AOF size and rewrite state. These server counters cover the dedicated benchmark instance. [Redis latency histograms](https://redis.io/docs/latest/commands/latency-histogram/), [Redis INFO](https://redis.io/docs/latest/commands/info/).

R0 documents the promotion, acknowledgement and old-primary fencing decisions for Sentinel and Cluster. [Redis failover contract proposal](redis-ha-contract.md).

An isolated Sentinel fixture used Redis 8.2.11, one primary, two replicas and three Sentinels. The primary confirmed the seed with `WAITAOF 1 2`. After disconnecting it from the fixture network, Sentinel promoted a replica that retained the seed. Both nodes reported the primary role. A client on the isolated node wrote a new key and received `WAITAOF 1 0`. The promoted history had no such key. This reproduced the need to revoke the former primary's write authority independently of primary discovery and local persistence. The initial hostname-based attempt stalled on DNS resolution after network detachment. The successful run used numeric fixture addresses to isolate failover behavior. Runner, server logs and acknowledgement counts are under `artifacts/perf/round-two/redis-sentinel-observation/`.

A separate R1 probe used the pinned StackExchange.Redis 3.4.20 client with independent Sentinel and data-node credentials. Its managed connection routed a read to the promoted node before revalidation. A direct pinned connection refused primary-only reads during isolation. It reconnected to its original endpoint, which then reported the replica role. An explicit new connection to the discovered endpoint passed the node-specific role and AOF checks. The probe used read-only ACLs over authenticated TCP and all three data nodes ended with zero keys. The result supports using Sentinel for discovery and binding work connections to a verified endpoint generation. Enabling writes still requires the authority and continuity proof from R0. Reproducer and sanitized evidence are under `artifacts/perf/round-two/redis-sentinel-client/`.

In a separate AOF-always primary/replica fixture, the same client library reported local and replica counts of 1/1 on the physical connection that sent the write. After an injected socket loss between a write and its acknowledgement, `WAITAOF 1 1` again returned 1/1 on the replacement connection. `CLIENT ID` changed from 13 to 17. The counts alone could not associate the acknowledgement with the preceding mutation. The runner and probe are under `artifacts/perf/round-two/redis-waitaof-affinity/`.

An R3 routing probe moved one key while two same-slot test keys were split between primaries. A Lua script declared only its metadata key and derived the second name. It changed metadata and then failed on the nonlocal key. Declaring both keys returned `TRYAGAIN` before any script mutation. After completing the slot move, the declared-key script succeeded on the new owner. This used three disposable Redis primaries and synthetic keys. Runner and raw replies are under `artifacts/perf/round-two/redis-cluster-key-discovery/`.

The R3 source audit covers 48 embedded Lua operations, 15 management branches and four inline scripts. Its key manifest distinguishes caller-known keys from keys discovered in stored state. The design covers bounded read-only discovery and authoritative revalidation before writes, including claim payload replay, early receipt confirmation and projection from a pending intent. It also covers global metadata, cursor-based scans across primaries and node-specific profile checks. The inventory and source checksums passed. The manifest and reproducer are under `artifacts/perf/round-two/redis-r3-key-audit/`.

A follow-up Sentinel candidate observer checks seed agreement, pins a separate read-only connection to the discovered endpoint, verifies its role, AOF profile, store identity and revision, then rechecks the discovery and node process identity. It returns observations with continuity unresolved and provides no connection for job commands. The Release build and all three live Redis 8.2.11 fixture cases passed. Initial and promoted endpoints passed the read-only checks, and wrong identities and revision regressions were rejected. An unreachable seed returned discovery unavailable. The isolated old primary continued to report its primary role. The fixture, patch and results are under `artifacts/perf/round-two/redis-r1-discovery/`.

### Redis throughput against BullMQ

Five alternating pairs per workload compared Runwick at `30fc1dd` with BullMQ 6.7.2 on Node.js 26.13.1. Each run used a fresh standalone Redis 8.2.11 container with AOF `always`, `noeviction` and RDB saves disabled. Both clients sent roughly 1 KiB no-op payloads. Both accepted every measured add and settled all seeded jobs in the drain. Runwick counted confirmed durable finishes. BullMQ counted worker completion events after the finish script replied and kept up to 1,000 completed or failed jobs. Runwick kept its command receipts, lifecycle history, and interrupted-intent recovery.

| Workload | Runwick median jobs/s (min–max) | BullMQ median jobs/s (min–max) | Median paired Runwick/BullMQ |
|---|---:|---:|---:|
| 1,500 single adds, 16 callers, after 128 warm-up adds | 362.6 (228.5–655.4) | 745.3 (599.4–2,088.2) | 45% |
| 2,000 bulk adds, batches of 100, 8 callers | 948.3 (620.6–1,112.0) | 2,724.8 (1,283.2–3,730.1) | 31% |
| 1,500-job drain, 64 worker slots | 240.3 (182.9–262.2) | 1,032.4 (712.9–1,629.2) | 23% |

Median single-add p95/p99 was 90.9/159.8 ms for Runwick and 57.7/128.1 ms for BullMQ. Runwick timed the drain from first claim and BullMQ from worker startup. The observed paired throughput ratios ranged from 31–73% for single adds, 25–48% for bulk and 16–36% for drain. Host load varied while the integration suites ran. The observed pairs fell short of the 90% target. The subsequent steady-arrival pairs use equal offered load.

| Whole-fixture median per measured job | Runwick AOF | BullMQ AOF | Runwick Redis CPU | BullMQ Redis CPU |
|---|---:|---:|---:|---:|
| Single adds | 7.53 kB | 1.76 kB | 1.44 ms | 0.44 ms |
| Bulk adds | 5.09 kB | 1.62 kB | 0.77 ms | 0.23 ms |
| Drain | 17.36 kB | 2.63 kB | 3.88 ms | 0.55 ms |

The AOF and server CPU counters cover setup and warm-up or seeding as applicable. In the first Runwick drain, 1,500 completions used 67 combined claim-and-begin calls, 82 individual finish calls and 84 finish batches. All returned directly without intent recovery. The CPU and AOF gap persisted while ordinary claims and finishes already shared script calls. Measure the bytes and CPU spent on payloads, after-images and lifecycle records before changing the number of sockets. Raw results and the runner are under `artifacts/perf/round-two/redis-bullmq-pair/`.

Five equal-offer pairs at 40/s used 16 workers and 16 producer calls, with 12 seconds of startup and 30 seconds of measurement. Median durable completions during offers were 39.93/s for Runwick and 39.70/s for BullMQ 6.7.2. Every accepted job settled. Median offer-to-finish p95/p99 was 665/1,382 ms and 545/954 ms, respectively. The paired p99 ratios ranged from 0.71× to 2.49×, with a median of 1.52×. These latencies include settlement. The harness counts locally refused arrivals for both clients. Raw results are under `artifacts/perf/round-two/redis-bullmq-steady/`.

At 100 offers/s under the same steady fixture, five pairs had median durable completion rates of 99.43/s for Runwick and 99.00/s for BullMQ. Median refused offers per arm were zero and nine. Every accepted job settled. Median offer-to-finish p95/p99 was 1,417/1,795 ms and 308/441 ms, respectively. The paired p99 ratios ranged from 0.75× to 5.06×, with a median of 4.07×. These short pairs show comparable steady completion rates at the offered load and a substantial latency gap. The Runwick trace also captured due-to-handler p95/p99: medians of 540/1,165 ms at 40 offers/s and 1,333/1,744 ms at 100 offers/s. Its p99 ranged from 788–2,170 ms and 1,497–1,982 ms, respectively. The BullMQ arm recorded finish time only, so these traces cannot establish the due-to-start parity ratio. The delayed-job fixture below measures both enqueue and due-to-handler timing.

Five delayed-job pairs at 40 offers/s used the same standalone Redis 8.2.11 AOF-always fixture and 16 producer calls and workers, with each job scheduled five seconds ahead. In the 30-second window shifted by that delay, median durable throughput was 39.97/s for Runwick and 39.77/s for BullMQ. Runwick settled all 6,000 offers, and BullMQ settled all 5,857 accepted offers. Median enqueue p95/p99 was 70/248 ms and 68/210 ms. Median due-to-handler p95/p99 was 1,140/1,860 ms and 464/745 ms. The paired due-to-handler p99 ratio ranged from 0.12× to 6.69× with a median of 3.05×. This workload meets the throughput band at 40/s and leaves the latency band unqualified. Whole-fixture Redis counters divided by completed jobs gave median AOF sizes of 20.94 and 3.11 kB per job, and Redis CPU of 10.22 and 6.60 ms per job, respectively. The counters include fixture setup and the startup window. Results are under `artifacts/perf/round-two/redis-bullmq-delayed/`.

Five delayed-job pairs at 100 offers/s gave median durable completion rates of 99.87/s for Runwick and 99.27/s for BullMQ in the shifted 30-second window. Runwick settled all 15,000 offers. BullMQ refused 165 locally and settled all 14,835 accepted offers. Neither system recorded any identity mismatch. Median enqueue p95/p99 was 100/196 ms and 53/115 ms, respectively. Median due-to-handler p95/p99 was 1,333/1,719 ms and 303/461 ms, respectively. Runwick's paired due-to-handler p99 ratio ranged from 1.32× to 5.15× with a median of 3.43×. The throughput band held and the due-to-handler p99 band was missed in every pair. Whole-fixture Redis counters gave median AOF sizes of 20.34 and 3.18 kB per completed job and CPU times of 6.66 and 3.65 ms per completed job. Results are in the same delayed-job directory.

### F1 Redis work hints

The isolated prototype publishes a best-effort queue hint after an acknowledged direct dispatch creates ready work. A separate subscription connection receives hints. Workers keep the existing two-second polling cap for lost publications, delayed jobs and other write paths. Pub/Sub broadcasts to current subscribers and adds no durable Redis write. StackExchange.Redis 3.4.20 avoids blocking list pops on its multiplexers, and Redis Pub/Sub uses at-most-once delivery. [Pinned client documentation](https://github.com/StackExchange/StackExchange.Redis/blob/3.4.20/docs/PipelinesMultiplexers.md) · [Redis Pub/Sub](https://redis.io/docs/latest/develop/pubsub/).

On Redis 8.2.11 with AOF `always`, the focused provider test confirmed both independent subscribers received the hint after one durable job and receipt existed. A separate fixture denied `PUBLISH`; all eight accepted jobs settled once through polling with zero hints. One eight-second sparse smoke per arm settled all eight jobs. Due-to-handler p50 was 484 ms with polling and 38 ms with hints. This sample verifies the path and is too small to qualify tail latency. The prototype stays isolated pending repeated sparse and saturated measurements. Evidence is under `artifacts/perf/round-two/redis-f1/`.

Five alternating 30-second pairs per load compared the same build with hints off and on. At one offer/s, median due-to-handler p50 fell from 410 to 28 ms and p99 from 903 to 373 ms. Every sparse pair improved p99. At 500 offers/s, median completions during offers were 114.4 and 114.6/s. Paired throughput changes ranged from −18.1% to +12.7%, and both arms refused thousands of offers per run. Every accepted job settled once. The saturated spread crosses the 10% regression band, so hints stay isolated. Results are under `artifacts/perf/round-two/redis-f1/five-pairs/`.

### F5 Redis AOF byte attribution

A separate AOF-always profile captured the full incremental AOF from fresh Redis 8.2.11 fixtures with automatic rewrite disabled for observation. One fixture bulk-added 1,500 jobs in groups of 100. Another added the same jobs and settled all 1,500 once with 64 worker slots. Parsing the persisted RESP commands accounted for every byte in both files.

| AOF command category | Bulk only | Seed and drain |
|---|---:|---:|
| Total | 7.64 MB | 26.04 MB |
| Guard and packed intent writes | 3.60 MB | 13.64 MB |
| Job hash projection, including attempt fields | 1.40 MB | 4.31 MB |
| Receipt projection | 0.18 MB | 3.32 MB |
| Event and audit projection | 0.44 MB | 1.93 MB |
| Live payload projection | 1.64 MB | 1.64 MB |

The raw payload values accounted for 1.52 MB inside the intents and were also stored in the live payload keys. A payload reference could eliminate at most that duplicate value volume before accounting for staging and recovery overhead. That is about 20% of bulk bytes and 6% of whole-drain bytes in this fixture. Claim and finish amplification also needs attention. Raw AOFs, typed counters, parser and runner are under `artifacts/perf/round-two/redis-f5-aof/`.

The MessagePack analysis found 2.23 MB of reply result bytes already present in the same command's receipt after-images, or 8.58% of whole-drain AOF. `ReserveAndBegin` accounted for 2.03 MB. Existing recovery scripts consume the full stored reply, so removing that copy requires a compatible format transition. The intent encoding is retained. The byte attribution and reproduction command are alongside the raw AOFs.

An isolated CPU candidate encodes each applied table result once for the receipt, packed intent and live reply. Nine fixed-clock checks through the production Lua prelude matched the exact reply, receipt and persisted intent bytes. The existing recovery script read the candidate's interrupted intent. Eight focused provider tests passed on Redis 8.2.11 with AOF `always`, including prefix faults, interrupted bulk, replay after expiry and intent bounds. The clock fixture now initializes its own schema, and its replay test also passed alone against an empty server.

Five alternating pairs per workload compared the `2978f04` baseline and the candidate on fresh Redis 8.2.11 AOF-always fixtures. All arms accepted 1,500 jobs. The 64-slot drains each settled 1,500 jobs once, and neither workload entered intent recovery.

| F5 reply encoding | Baseline median | Candidate median | Paired median change (min–max) |
|---|---:|---:|---:|
| Durable drain completions/s | 263.6 | 355.6 | +32.5% (+24.2% to +77.8%) |
| Redis CPU ms per job, whole drain fixture | 3.88 | 2.80 | −26.6% (−33.2% to −21.1%) |
| Drain due-to-handler p99 | 5.79 s | 4.32 s | −24.1% (−42.0% to −19.3%) |
| Bulk adds/s, eight callers | 926.3 | 848.2 | −2.4% (−26.9% to +19.9%) |

Whole-fixture AOF size was identical across all bulk runs and stayed near 17.36 kB/job in the drain. Handler-to-finish p99 improved in four drain pairs and rose 14.2% in one. The median paired time from reservation to handler start rose 22.9% at p99.

Five more alternating pairs used 10,000 bulk jobs per arm with eight callers. Every job was accepted without intent recovery. Median throughput was 1,355 versus 1,471 adds/s, with a paired median change of +9.4% and a range of −23.0% to +16.7%. Whole-fixture Redis CPU per job had a paired median change of −8.7%, ranging from −13.8% to +16.4%. The bulk throughput spread still crosses the acceptance band. The shared-prelude candidate stays isolated. The next isolated change scopes encoding reuse to the worker's claim and settlement scripts so the producer script bytes are identical. Patch and evidence are under `artifacts/perf/round-two/redis-f5-reply/`.

The narrower claim and settlement candidate preserved the exact Dispatch, DispatchMany, plain Finish and recovery scripts. Six fixed-clock comparisons matched live and replay replies, receipt and projection bytes, and interrupted intent bytes. Both targeted AOF crash cases passed. In five 1,500-job drain pairs, median throughput moved from 254.1 to 312.8/s, with a paired median gain of 9.0% and a range of −7.5% to +68.5%. Redis CPU per job fell in all five pairs, by 13.6% at the paired median. Every accepted job settled once and ordinary drains entered no recovery. One settlement p99 rose 17.1%, and one reservation-to-handler p99 rose 105.4%. The candidate remains isolated pending repeated tail measurements. Results are under `artifacts/perf/round-two/redis-f5-drain-only/`.

### F4 Redis producer depth and settlement instrumentation

The isolated workload records actual outstanding calls and EVALSHAs after the producer gate. It can vary producer depth and settlement consumers independently while retaining the existing renewal lane and lease timing. A one-second smoke reached eight outstanding EVALSHAs and settled 47 jobs. All 47 reconciled after SIGKILL and AOF restart following the confirmations. The run recorded 146 EVALSHAs and 12 finish batches. It produced no renewal samples.

The first depth sweep used one 70-second closed-loop run per depth on Redis 8.2.11 with AOF `always`, 64 worker slots, four settlement consumers, and a 512-job backlog threshold. Each run included 24 jobs with 65-second handlers to exercise renewals. Every requested depth was observed in outstanding producer scripts. All offers were accepted and settled once, with exact job IDs and no pending intent after SIGKILL and AOF restart.

| Producer depth | Accepted and settled | Durable completions/s including drain | EVALSHA calls per settled job | Settlement p99 | Overlapping renewal calls, longest |
|---:|---:|---:|---:|---:|---:|
| 1 | 4,055 | 57.9 | 2.99 | 70 ms | 21, 128 ms |
| 8 | 12,175 | 173.7 | 1.49 | 193 ms | 21, 182 ms |
| 16 | 15,829 | 225.8 | 1.26 | 206 ms | 22, 143 ms |
| 64 | 11,515 | 160.4 | 1.23 | 308 ms | 10, 106 ms |

Depth 16 finished 15,790 jobs during offers. Depth 64 finished 10,972 during offers and had a settlement maximum of 852 ms. Whole-fixture Redis CPU per job was 9.05, 4.52, 3.86 and 4.81 ms by depth. Minimum observed renewal margin exceeded 31 s in every arm, with zero stale or error replies. Each row has too few overlapping renewal calls to qualify p99. The depth comparison is directional, with longer repeated runs and the settlement-consumer sweep still pending. Raw samples and the isolated harness are under `artifacts/perf/round-two/redis-f4/`.

## Outbox relay

Phase 0.6 traces claim, transport record, destination receive and delivered mark separately. D1 assigns the UUIDv7 transport identity and deadline in the claim transaction for PostgreSQL and SQLite. A reclaim preserves the pair while its deadline remains live. Replacements respect the acceptance horizon. The relay checks lease authority before the first send, and subsequent source writes stay token-guarded. The eight focused tests passed, including a lost reply to a committed claim and later reuse of its transport identity.

One short P-09 pair used four relay slots at 100 offered messages/s, six seconds of warm-up and about 18 seconds of measurement. The baseline accepted and delivered all 2,396 offers and the candidate all 2,371. Neither run recorded a duplicate mapping, blocked row or backlog after drain.

| P-09 measurement | Separate transport write | Transport in claim |
|---|---:|---:|
| Staged/s during measurement | 97.12 | 99.09 |
| Confirmed deliveries/s during measurement | 36.96 | 63.60 |
| Ready-to-confirm p95 / p99 | 22.68 / 22.77 s | 9.95 / 10.01 s |
| Standalone transport-record calls during measurement | 668 | 0 |
| Mean claim / destination / delivered mark | 17.17 / 46.28 / 21.49 ms | 12.77 / 34.56 / 11.30 ms |

The baseline transport-record call averaged 20.14 ms, including 9.02 ms waiting for the source control lane. Starting host load averaged over one minute was 12.57 on the baseline and 7.71 on the candidate, so the throughput and latency effects are directional. The 100/s and 2 s p99 targets remain open. This pair disabled injected mark loss and used two relay source sessions in both runs. Raw results and the runner are under `artifacts/perf/round-two/p09-d1/`.

The isolated D2 candidate batches ready PostgreSQL delivered marks behind the current mark. It locks rows in message-ID order, keeps each lease token and confirms applied IDs after the implicit transaction finishes. All 23 relay tests and four final targeted cases passed, including mixed lease tokens, deferred commit rejection, lost reply and queued-mark abort.

One pair used 30 seconds of warm-up, 60 seconds of measurement, four relay slots and 100 offered messages/s. Confirmed deliveries rose from 42.75 to 50.04/s during measurement, and ready-to-confirm p99 moved from 86.71 to 69.81 s. The candidate grouped 624 of 8,984 marks into 302 commits. It used 8,662 applied mark commits, 3.6% fewer than a commit per delivered message. Both arms eventually delivered every staged message with one destination mapping, zero blocked messages and no remaining backlog. The baseline and candidate recorded 12 and 15 relay lease losses. The candidate shared the host with the Contract suite. This is a directional pair, both P-09 gates remain open, and the candidate stays isolated. Raw results and the runner are under `artifacts/perf/round-two/p09-d2/`.

The source already claims up to the relay's free slots. During the D2 pair's measurement windows, 2,029 and 2,318 claim calls averaged 25.4 and 22.4 ms. Source claim lane wait averaged 0.021 and 0.003 ms. Destination function and commit together averaged 43.6 and 35.1 ms.

The D3 slot diagnostic kept the production D1 path, source lanes, eight staging callers, 100 offers/s, 30 seconds of warm-up and 60 seconds of measurement. It used PostgreSQL 18.6 and one run per slot count.

| Relay slots | Staged/s | Confirmed deliveries/s | Ready-to-confirm p95 / p99 | Relay lease losses |
|---:|---:|---:|---:|---:|
| 4 | 100.55 | 49.25 | 64.98 / 65.51 s | 12 |
| 8 | 100.53 | 101.20 | 3.85 / 4.09 s | 22 |
| 16 | 100.49 | 100.49 | 3.16 / 3.33 s | 54 |

Every staged message reached one destination mapping and job. No run left a blocked, pending or leased message after drain. The eight- and sixteen-slot runs met the rate target and missed the 2 s p99 gate. Mean source claim lane wait was below 0.02 ms in every run. At sixteen slots, mean destination commit and delivered-mark calls increased to 32.3 and 56.8 ms from 17.9 and 15.0 ms at eight slots. This directional sweep keeps the default relay settings and points to settlement contention as slot count increases. Raw results and runner are under `artifacts/perf/round-two/p09-d3-slots/`.

A sixteen-slot D2 pair used the same 100/s offered rate and windows. The isolated mark-batching candidate reduced applied mark commits from one per message to 6,042 for 8,982 deliveries, a 32.7% reduction. It grouped 4,922 marks into 1,982 commits. Confirmed rate moved from 95.20 to 99.99/s and ready-to-confirm p99 from 7.22 to 1.60 s. Every message reconciled to one destination job. Lease losses moved from 76 to 54. The candidate met the p99 gate and fell just short of the 100/s gate. Starting load was 7.17 on the baseline and 3.04 on the candidate, so this pair is directional. Raw results are under `artifacts/perf/round-two/p09-d2/slots-16-pair/`.

Across all five sixteen-slot D2 pairs, median confirmations increased from 75.73 to 98.73/s and median ready-to-confirm p99 fell from 9.27 to 2.61 s. Applied mark commits fell by 29.0–32.7% across candidate runs. Each pair reconciled all staged messages to one destination job per message. Two candidate runs met the p99 target and one met the rate target. The patch remains isolated because each run missed at least one gate. The four additional pairs and combined summary are under `artifacts/perf/round-two/p09-d2/slots-16-repeat/`.

The D4 source assessment found that a destination batch would need per-item rollback for refusals after writes and a lock order spanning all items. In the five sixteen-slot D2 pairs, the median of the baseline runs’ mean destination function and commit phases was 20.05 and 68.14 ms. The client-observed commit phase includes backend waits, server response and client scheduling. A dedicated wait and WAL timing probe is the next attribution step. The design is under `artifacts/perf/round-two/p09-d4/`.

The live D4 diagnostic sampled destination sessions 1,142 times during a 60-second, sixteen-slot P-09 run on PostgreSQL 18.6. The exact heavyweight `NOTIFY` lock accounted for 88.3% of active commit backend observations. WAL sync accounted for 4.5%. Destination commit averaged 74.4 ms at the client. Every staged message reconciled exactly once. The wait sampler adds load, so this run attributes observed wait states without estimating a throughput gain. The next isolated candidate moves the destination wake after durable commit. Raw data are under `artifacts/perf/round-two/p09-d4/destination-waits/`.

The isolated destination-wake candidate builds on `bd139a6` with a new revision-23 entry point. It leaves `ReceiveOutbox/1` receipts and delivery mapping intact and uses the existing bounded wake pump after confirmed commit for due, newly created first deliveries. The focused PostgreSQL and outbox runs passed 80 and 27 tests, respectively. A 12-second smoke measurement reconciled all 1,786 staged messages with one destination job each after drain. It recorded 57.80 confirmations/s and 6.65 s ready-to-confirm p99 during measurement, missing both P-09 thresholds. That single short run establishes fixture correctness; the five matched pairs are still needed to estimate the effect. This candidate changes the wake path only. Grouped destination delivery is separate work. Evidence is under `artifacts/perf/round-two/p09-d4/qualification/`.

Five alternating pairs at 100 offered messages/s and 16 relay slots reconciled all 89,831 staged messages across both arms. Median confirmed throughput rose from 88.71 to 96.10/s, with improvements in all five pairs. Median ready-to-confirm p99 fell from 12.97 to 10.70 s and improved in all five pairs. The candidate exceeded 100 confirmations/s twice, with a best p99 of 4.26 s. Median destination commit p99 fell from 312 to 83 ms. A separate diagnostic with 1,181 snapshots saw no active commits waiting on the heavyweight notification lock. WAL write lock and WAL sync accounted for 57.1% and 20.4% of active commit observations. The candidate remains isolated pending the P-09 delivery and tail targets. Raw results are under `artifacts/perf/round-two/p09-d4/qualification/`.

## Worker payload decoding

E1 uses the strict payload scanner without constructing canonical values before worker deserialization. The same scanner still builds the canonical form for fingerprints. All 102 focused boundary, serialization and wire tests passed, including rejection-code parity and the existing canonical vectors.

Five alternating Release builds ran a synchronous decode probe with warm-up and four equally sized ASCII string properties. The table reports median bytes allocated on the calling thread per decode, including deserialization.

| Payload bytes | Baseline | Validation only | Reduction |
|---:|---:|---:|---:|
| 1,021 | 8,984 | 3,176 | 65% |
| 65,533 | 460,536 | 132,200 | 71% |
| 1,048,029 | 6,322,143 | 2,098,112 | 67% |

The probe excludes receipt parsing, store reads and handler execution. The allocation probe source and per-run outputs are under `artifacts/perf/round-two/`.

### E2 Producer fingerprints and envelope export

Dispatch streams its command digest over the canonical body. Single dispatch keeps the private business-encoding buffer through fingerprinting, and long strings use pooled UTF-8 scratch that is cleared on return. Envelope export encodes base64 in blocks and writes ASCII builder chunks directly to the owned output array. Unicode still uses the existing UTF-8 conversion. Independent Node vectors and the added Unicode, padding and ownership cases passed. The final candidate passed 109 core and golden checks and all nine cross-provider wire contracts.

| Payload | Serialize, scan and fingerprint, baseline → candidate | ASCII envelope export, baseline → candidate |
|---|---:|---:|
| 1 KiB | 42.6 → 36.8 kB | 20.8 → 10.5 kB |
| 64 KiB | 1.08 → 0.68 MB | 0.64 → 0.28 MB |
| 1 MiB | 18.21 → 11.92 MB | 9.81 → 4.22 MB |

The Release probe measured calling-thread allocations and produced identical dispatch digests and encoded lengths at all three sizes. The workload throughput effect is still unmeasured. Probe source and raw outputs are under `artifacts/perf/round-two/producer-e2/`.

### E3 Settlement reply decoding

PostgreSQL and Redis use a shared forward decoder for normal `FinishMany` replies. Other shapes fall back to the existing decoder. PostgreSQL obtains UTF-8 from its result column and decodes the item fields after commit. SQLite already has an in-memory result and continues to read it directly. Receipt v1 persistence and replay bytes are unchanged.

A captured 10,286-byte Redis reply with 25 outcomes allocated 114.6 kB with the old decoder and 27.3 kB with the forward reader, or 37.7 kB with UTF-8 conversion included. This measures decoding only. Two parser tests, 11 PostgreSQL finish-provider tests and 42 shared finish and retention contracts passed. The cold Redis overlap fixture preloads the scripts so a `NOSCRIPT` resend cannot reorder the paused commands. The case passed after three independent script-cache flushes. Probe and test output are under `artifacts/perf/round-two/finish-many-e3/`.

### E4 PostgreSQL dispatch arguments and E5 context items

PostgreSQL builds the existing dispatch arrays directly and binds fresh parameters for each attempt. A Release probe measured parameter preparation and attachment at 17,016 → 11,472 bytes for one job and 41,344 → 35,792 bytes for an eight-item staged batch. All 44 focused PostgreSQL staging, retry, wire and schedule checks passed, including grouped materialization.

Job context allocates its items dictionary on first access. The same probe measured 136 → 56 bytes per context when items went unused and 136 bytes when accessed. Concurrent getters returned the same dictionary, and four focused worker cases passed. Probe source and raw outputs are under `artifacts/perf/round-two/producer-e4-e5/`.

The attempt supervisor now waits for pipeline completion, the wake signal or the timeout in one operation. It removes the outer cancellation source and skips the cleanup delay when disposal already completed. Each pending wait still owns one cancellable delay. Five alternating allocation probes measured 2,795 → 1,176 bytes per wait when pipeline completion won and 1,680 → 1,264 bytes when a wake signal won. Each sample measured 30,000 synthetic waits in a warmed process. All 15 focused worker cases passed, including timeout boundaries, grace, hung disposal, shutdown and polling. End-to-end allocation qualification remains open. Source and results are under `artifacts/perf/round-two/e5-attempt-wait/`.

The short P-03 diagnostic at `e1cb660` used PostgreSQL 18.6, one minute of warm-up and three minutes of measurement per payload size. Each fixture ran a 16-slot worker and 100 ms handlers. The two arms sustained about 1 MB/s of accepted payload ingress. Every accepted job settled once with no dispatch failures or correctness violations.

| Nominal payload | Offers / durable finishes per second | Dispatch / pipeline-to-finish p99 | Producer / worker allocation per job |
|---|---:|---:|---:|
| 64 KiB | 15.70 / 15.63 | 195 / 147 ms | 1.36 / 0.40 MB |
| Near 1 MiB | 0.97 / 0.97 | 414 / 573 ms | 17.39 / 3.91 MB |

Peak worker RSS was 214 and 209 MB respectively. WAL per completed job was 13.5 and 30.3 kB with synthetic repeated-character payloads. The workload rejected an oversized payload with `payload_too_large` and stored zero jobs for that check. This was a diagnostic on the shared host with one repetition per size. Raw results are under `artifacts/perf/round-two/p03-payload/`.

The boundary fixture now constructs 1,048,577 serialized bytes with individually valid strings. The focused rerun returned `payload_too_large` and stored zero jobs. Results are under `artifacts/perf/round-two/p03-payload/limit-exact/`.

## Store dashboard

M1 adds a Store page and read-only snapshot for callers with `Settings.Read`. It reuses the process health probe for store and admission metadata, retains the last descriptor for up to five minutes, and labels stale and unavailable facts. Browser refreshes read the cached snapshot. Chromium checked the page at desktop and mobile widths. A follow-up uses the same probe to show PostgreSQL server and SQLite library versions with their respective node and process scope. All seven focused dashboard tests passed, including the real PostgreSQL outage, version expiry and generated asset checks. Provider collectors, topology observations and paired dashboard overhead measurements are still pending.

The first M2 collector samples SQLite database and WAL file sizes and process writer counters on management hosts. It adds file metadata reads to the background health check. Store-wide facts require an explicit `StoreDiagnostics.Read` grant. The focused test verified the real SQLite observations, the denied viewer state, and immediate removal of cached values after grant revocation. The rebuilt asset hash and embed check passed.

M2 adds seven bounded PostgreSQL queries on the management host's background lane. The cached, timestamped sample covers database sessions, lock waits and size, Runwick jobs-table size and maintenance counters, and connected-node WAL, recovery and replication observations. A separate statistics permission gates the affected facts, and restricted backend access degrades them independently. Four focused tests passed on real PostgreSQL and SQLite fixtures, including a denied `pg_stat_wal` read, outage retention and expiry, grant revocation, and the asset embed. Chromium checked the denied view and simulated available and stale states at desktop and mobile widths. Pool wait metrics, live replica transitions and dashboard-on workload cost remain to be measured.

M2 also samples the configured Redis node with one bounded, selected-section `INFO` request on the background lane. It uses the existing profile connection. The cached fields cover memory, client and command activity, evictions, AOF state, and replication indicators. The page labels Redis command activity as a node observation and shows the sample time. Two Redis driver tests and four dashboard tests passed, including real ACL denial, missing fields, permission revocation and generated assets. Live replica behavior and workload overhead remain unmeasured.

The Redis Store page also shows pending-intent status for the current application. A primary-only background `HMGET` verifies application store identity and exposes a pending flag and valid projection index under `StoreDiagnostics.Read`. A compatibility failure caused by pending recovery can still produce a fresh intent observation. Redis ACL denial degrades that fact independently of node statistics. Three Redis tests and one dashboard integration test passed, covering stale expiry, identity mismatch, redaction and grant revocation. Chromium verified the pending and permission-denied rows at desktop and mobile widths using synthetic snapshots with the rebuilt assets. Workload overhead remains open. Results are under `artifacts/perf/round-two/store-redis-intent/`.

M3 retains up to 12 check transitions and 12 samples of a provider gauge per management process. It marks gaps and clears the gauge series on store identity changes. The Store page distinguishes host observations from provider metrics and requires the diagnostics grant for the gauge. Two deterministic history tests and eight dashboard checks passed. Chromium verified the desktop and mobile tables and the viewer permission state.

The M4 workload flag `bench --store-viewers 20` starts the co-hosted management sampler during steady load, polls the cached Store snapshot as authorized viewers and serializes the response. Five alternating pairs used 50 offers/s for 30 seconds on the retained SQLite fixture. The viewer arms returned diagnostics on all 600 polls. The first attempt used an incorrect final timer interval and was discarded before rerunning the five pairs.

| SQLite co-hosted Store viewer comparison | No viewers | 20 viewers and sampler |
|---|---:|---:|
| Median completions during offers per second | 49.83 | 49.77 |
| Median offer-to-finish p99 | 835 ms | 1,164 ms |
| Median due-to-handler p99 | 522 ms | 884 ms |
| Total refused offers | 0 | 214 |

All refusals came from one viewer arm. The median paired due-to-handler p99 ratio was 1.52×, ranging from 0.79× to 2.55×. The median snapshot call took 0.34–0.50 ms across viewer arms. Its p99 ranged from 250 to 422 ms. The comparison includes cold startup and JSON serializer initialization. It measures the session and serialization path on the shared host and misses the 5% p99 budget. Raw results are under `artifacts/perf/round-two/store-viewer-cost/sqlite-current/`.

Five more pairs used a 12-second startup period in both arms and primed serialization before offers began in the viewer arm. All 600 polls returned diagnostics. Snapshot call p99 ranged from 3.0 to 21.3 ms. Median durable completions during offers were 49.50/s with zero viewers and 49.93/s with 20 viewers. The zero-viewer arms refused 211 offers across four runs. The viewer arms accepted all 7,500 offers. Every accepted job settled. The paired due-to-handler p99 ratio ranged from 0.73× to 1.57×, with a median of 1.14×. This variation spans the 5% p99 budget. The shared-host comparison leaves the SQLite gate open. The HTTP qualification results are below. Raw results are under `artifacts/perf/round-two/store-viewer-cost/sqlite-warmed/`.

The PostgreSQL Store viewer comparison completed five alternating pairs at 40 offers/s with 12 seconds of startup and 30 seconds of measurement per arm. All 600 viewer polls returned diagnostics. Median durable completions during offers were 39.90 and 39.93/s. Due-to-handler p99 was 431 and 489 ms by arm median. Its paired ratio ranged from 0.17× to 1.78×, with a median of 1.24×. The shared-host comparison leaves the 5% p99 gate open. The first measured Redis viewer arm settled all 1,035 accepted jobs, refused 165 offers, and returned diagnostics on 79 of 119 polls. The runner stopped on its all-polls diagnostics assertion. The Redis repeat records partial observations alongside job accounting. Results are under `artifacts/perf/round-two/store-viewer-cost/network/`.

The Redis repeat completed five alternating pairs on Redis 8.2.11 with `appendfsync always`, using the same 12-second startup and 30 seconds at 40 offers/s as PostgreSQL. Each arm accepted and settled all 1,200 jobs, with one invocation per job. All 600 viewer polls returned diagnostics. Median durable completions during offers were 39.90/s without viewers and 39.97/s with 20 viewers. Due-to-handler p99 was 1.85 and 1.89 seconds by arm median. The median paired p99 ratio was 0.88×, ranging from 0.18× to 3.74×. That spread leaves the 5% p99 budget unverified. The repeat measures the cached management session and JSON serialization on the shared host; it does not measure HTTP overhead. Raw results are under `artifacts/perf/round-two/store-viewer-cost/network-redis-repeat/`.

The short PostgreSQL P-07 run added 100,000 seeded terminal jobs and ran successive three-minute windows at 100 offers/s with zero and 20 viewers. Durable completions moved from 79.10 to 70.18/s, an 11.3% reduction. All 720 viewer polls completed without error. Overall management call p99 was 245 ms and succeeded-list p99 was 313 ms. Cleanup expired 60,664 completed-job detail records. After reconciling the seed, all 107,201 live jobs settled once, with zero lease losses and zero final correctness violations. This single shared-host run exercises the live management lists and retention, while the Store snapshot pairs measure cached observations. Results are under `artifacts/perf/round-two/p07-retained-management/`.

The authenticated HTTP benchmark ran five pairs per store. Both arms warmed the same co-hosted collector and Kestrel route. The viewer arm made 300 Store page requests per provider with 20 identities polling every 10 seconds. All 900 responses returned diagnostics and every accepted job settled. PostgreSQL refused eight offers in the viewer arms. Median durable completions/s without and with viewers were 49.90 and 49.90 for SQLite, 39.93 and 39.97 for PostgreSQL, and 39.97 and 40.00 for Redis. Paired due-to-handler p99 ratios ranged from 0.33–4.52×, 0.73–5.92×, and 0.42–2.90×, respectively. Median HTTP response p99 was 31, 129, and 55 ms, respectively. The shared-host tail variation leaves the 5% gate open. Loopback HTTP covers the authenticated route and full response body. Browser rendering, TLS, and proxy overhead are outside this fixture. Raw results are under `artifacts/perf/round-two/store-viewer-cost/http/results/`.

## Full-suite verification

The worker checkpoint at `6011483` passed 426 Core and 475 shared contract tests. The integrated snapshot at `d0efb88` passed all 383 PostgreSQL tests with zero skips on a Docker-managed server, including CAP-10, TIME-09, TXC-05 and the store-outage cases. The same snapshot passed all 48 outbox and 70 Redis tests with zero skips, including the Redis AOF kill-and-restart sweep. The SQLite receipt-index snapshot at `30fc1dd` passed all 53 SQLite, 430 Core and 56 Dashboard tests. Its Contract run passed 474 of 478. All nine targeted retries of the affected cases passed on the same build.

The Contract failures involved process-wide usage counts during concurrent worker tests, the PostgreSQL event-visibility horizon, and a Redis upgrade test that temporarily overrides the shared fixture's layout revision. The affected tests now run in non-parallel collections. All 14 focused checks passed with that change. The full Contract rerun at `38f40e0` passed 479 of 480. Its remaining failure was the PostgreSQL usage contract's CPU assertion. The test burned CPU for 300 ms of wall time and recorded 243 ms of CPU against a 250 ms threshold on the shared host. The fixture now measures elapsed process CPU for the burn and retains a cancellation bound. All three focused provider cases passed. The corrected `f36cd6a` snapshot passed all 480 Contract tests with zero skips. Logs are under `artifacts/perf/round-two/suites-f36cd6a/`.

The `38f40e0` snapshot passed all 439 Core and 53 SQLite tests. Its Dashboard rerun passed 55 tests before the package check stalled. The test process's output readers and a detached MSBuild node had the same pipe inodes after `dotnet pack` exited. Terminating that fixture node released the pipes and the test passed. The package test now disables reuse for the subprocess's MSBuild nodes and drains both output streams. The focused package test and all 57 Dashboard tests passed on the isolated `38f40e0` snapshot with that correction.

The later `fa390b0` snapshot passed all 387 PostgreSQL and 48 outbox tests with zero skips. The outbox run included a dedicated AOF-always Redis fixture. All 57 Dashboard tests passed again on exact integrated `e1cb660`, including the new Redis intent observation. Logs and test results are under `artifacts/perf/round-two/suites-integrated/`, `suites-isolation/`, `suites-38f40e0/`, `suites-fa390b0/` and `suites-e1cb660/`.

## Integrated short SP-029 matrix

The short profile ran all 14 cases from exact `88a8016` on the shared host, with each case on its own pinned PostgreSQL container. It includes a 10-minute soak smoke for P-10. Every verdict is informational because the host is not the reference host. The host rebooted partway through P-08a. The first nine cases (P-01 to P-07) had already finished and were kept. P-08a was rerun from the saved runner binary, followed by the rest. The interrupted partial run is kept under `sp029-integrated-short/interrupted-by-reboot/`. The runner outputs, summary and per-case status are in `artifacts/perf/round-two/sp029-integrated-short/`, and `case-status.json` holds the verification.

All 14 cases exited 0 with zero correctness violations and zero run failures. Throughput and latency gates mostly missed, as expected on a shared host with one worker.

| Case | Result | Against the plan's threshold |
|---|---|---|
| P-01 | 84.51 finishes/s, backlog growing 14.05/s, due-to-Begin p99 41.5 s | Would miss throughput and latency. |
| P-01t | Throughput 79.4 → 85.0/s, allocations +7.3% | Would pass the instrumentation cost limits. |
| P-02 | Sustainable 100/s | No threshold. |
| P-03 | 1 MiB + 1 B refused, zero stored | Would pass. |
| P-04 | 0 pp share difference over 2,000 admissions | Shares were equal, but the run had 2,000 admissions against the required 10,000, so the summary marks it a miss. |
| P-05 | 3 replicas / 1 replica = 2.03 | Would pass. |
| P-06 | Lag p99 29.5 s, catch-up 305 of 300, no lease loss | Would miss the 13 s lag bound. |
| P-07 | 720 polls, zero errors, list p99 196 ms, worker 80.72 → 76.42/s (5.3% loss) | Would pass. |
| P-08 | Admission paused during the outage and worker reported ready 6.1 s after restoration | Would pass. See the note below. |
| P-09 | 81.81 deliveries/s, ready-to-confirm p99 87 s | Would miss. |
| P-10 | Post-GC heap last/first quarter 1.003 over 0.17 h | Smoke only. A 24 h run is required. |

P-08's summary row says NO DATA because `Analysis.cs` returns null for readiness unless a new reserve counter is sampled after the worker reports ready, and none was. The worker's admission transitions show it closed under the outage, reopened, was degraded at 20:50:35 and ready at 20:50:40, against restoration at 20:50:34. The 6.1 s is read from those transitions by hand. The analysis rule still needs a fix before this case can produce a verdict automatically.

P-08a and P-08b (1 s and 5 s control latency) completed with zero violations. Their detailed results are in the result files. P-08c's producer saw 6,080 `store_unavailable` refusals during the outage and one outcome-unknown, which replayed to a single acceptance.

## SQLite process-local wake (Q6) and PostgreSQL retained storage (C5)

**SQLite wake.** The candidate publishes a process-local hint after a due job commits, so a worker in the same process claims it without waiting for its poll. It adds no cross-process signal, and workers keep the normal polling cap for remote and staged writes. Five alternating pairs ran from exact `88a8016` on the shared host, each with a 15 s low-traffic run (3 offers/s) and a 1,000-job saturated drain. All 20 runs reconciled exactly.

| Run | Metric | Baseline → candidate (median of 5 pairs) | Paired ratio, range |
|---|---|---|---|
| Low traffic | due-to-handler p50 | 125 → 34 ms | 0.27×, 0.22–0.35 |
| Low traffic | due-to-handler p99 | 505 → 389 ms | 0.73×, 0.49–0.80 |
| Drain | completions/s | 131.7 → 132.2 | 1.00×, 0.84–1.15 |
| Drain | due-to-handler p99 | 7.55 → 7.70 s | 1.02×, 0.86–1.21 |

Low-traffic latency improved in every pair. Saturated throughput and latency are unchanged within a wide spread, which is the expected result because a busy worker finds work without a wake. The candidate's focused test passed 3 of 3, including a held pre-commit barrier that shows no hint before commit, a replay that sends no second hint, and a subscriber that throws without disturbing the others.

The patch is integrated into the working tree: `SqliteStore.Notifications.cs`, the dispatch hooks, an `ILocalWorkWakeSource` interface, the worker listener, and the test. The full SQLite project passes 54 of 54. The Core project passes 438 of 439. The one failure, `SerializationCaseTests.Prepare_allocation_is_linear_in_payload_size` (1 MiB must allocate at most 2.2× a 512 KiB prepare), fails identically on a clean `HEAD` archive with a ratio of 3.0, so it is unrelated to this patch. It needs its own look, because earlier suites on this branch reported the Core project fully passing. On the final tree, with the wake patch and a disposable AOF-always Redis, the Contract project passes 480 of 480 and the Outbox project passes 48 of 48. A first pass without a Redis server failed only Redis-provider cases with "Could not connect to Redis" (159 Contract, 2 Outbox) and no SQLite or PostgreSQL case. Its logs are kept under `q6-integrate/no-redis-run/`. Logs are under `artifacts/perf/round-two/q6-integrate*`.

**PostgreSQL retained storage (C5).** A disposable P-07 fixture was observed every 30 s through seeding and measurement: 213,462 jobs, 453,848 events, 20 minutes, zero correctness violations. The fixture ran PostgreSQL 18.6 with `fsync` and `synchronous_commit` on and `full_page_writes` off, so its WAL byte counts (2.0 GB, 2,804 full-page images) understate what a default server would write. Autovacuum was on with default thresholds.

| Table | Updates | HOT updates | Dead / live tuples at end |
|---|---|---|---|
| `jobs` | 303,773 | 0 (0.0%) | 26,597 / 213,454 |
| `job_attempts` | 226,924 | 72,204 (31.8%) | 4,101 / 213,364 |
| `job_generations` | 113,462 | 62,742 (55.3%) | 10,497 / 213,421 |
| `storage_commands_durable` | 226,936 | 177,642 (78.3%) | 4,481 / 226,882 |
| `storage_commands_ownership` | 35,439 | 24,349 (68.7%) | 899 / 19,367 |
| `job_payloads` | 0 (76,849 deletes) | – | 12,620 / 136,600 |

No `jobs` update is HOT, because every state change touches an indexed column (`state`, `lease_expires_at` or `eligible_at`). The `jobs` indexes total 129 MB against a 47 MB heap. Lowering the table's fill factor therefore cannot help on its own. Any change has to remove or narrow `jobs` indexes, which is a schema decision with its own correctness and query-plan checks. Autovacuum kept dead tuples at or below about 12% on every table while the run was active. The evidence does not support partitioning yet. The raw snapshots are in `artifacts/perf/round-two/pg-c5/observations/`.

## Redis Cluster recovery proof (R3) and grouped dispatch proof (F3)

Both proofs first failed with exit 127 because `rg` was missing from the host `PATH` after the reboot. Those outputs are preserved under `failed-no-rg/`, and both reran after linking `rg`.

R3 used a real three-node Cluster with a real `RegisterQueue` intent in flight during slot migration. The pending intent survived `TRYAGAIN` before any mutation, an `ASK` redirect, a `NOSCRIPT` on the target node and a `MOVED` reply. It then published, and replaying the same intent afterwards completed it: the queue had no confirmation before the replay and carried a confirmation timestamp after it. A discovery against an incomplete key set returned `REDISCOVER` instead of a partial answer. This shows that declared keys let a migration-time retry finish. It does not qualify route changes or old-primary fencing, which remain open. Evidence is in `redis-r3-next/run-9ZZrdvFI/`.

F3 loaded a standalone Redis candidate built from the production Lua prelude, dispatch core and recovery script, with AOF always. It injected 17 write boundaries and checked 39 jobs for independent per-command receipts and events. This is a correctness proof on synthetic prepared items only. It does not cover producer admission, cancellation, routing, client fault mapping, a live AOF restart or throughput, and the grouped script is not wired into the production client, so it shows no throughput gain. Evidence is in `redis-f3/`.

## Laravel database queue comparison

Laravel 13.42.0's `database` queue driver was measured against Runwick on the same database engine, so Runwick SQLite and PostgreSQL have a PHP baseline. The harness is `eng/bench-compare/laravel/`. Each arm used 16 workers (Laravel: 16 `queue:work database --sleep=1` processes), 1 KiB no-op jobs and a fresh database per run. The tests were a 20,000-job drain and 30 s steady runs at 40 and 100 offers/s, five alternating pairs per database. Steady latency is the dispatch call start to handler end. Runs with any failed job are excluded from the summaries. All figures are medians with the range over five pairs.

**SQLite.** Both arms used WAL, synchronous FULL and a 5 s busy timeout. Laravel ran in the pinned `php:8.4-cli` image with `BEGIN IMMEDIATE`, and all 30 runs had zero failed jobs. The Runwick arm is the build from before the process-local wake patch.

| Test | Metric | Runwick | Laravel | Paired Laravel/Runwick |
|---|---|---|---|---|
| Drain | jobs/s | 195.5 (161–205) | 125.0 (77–136) | 0.6× (0.5–0.7) |
| 40 offers/s | jobs/s | 39.9 | 39.4 | 1.0× |
| 40 offers/s | p50 / p99 ms | 60 / 961 | 184 / 884 | p50 2.1×, p99 1.2× (0.3–1.3) |
| 100 offers/s | jobs/s | 99.7 (94–100) | 89.6 (71–95) | 0.9× (0.7–1.0) |
| 100 offers/s | p50 / p99 ms | 136 / 949 | 660 / 3,328 | p50 5.6×, p99 2.3× (1.6–3.5) |

**PostgreSQL.** Laravel ran natively on PHP 8.3 against a fresh pinned PostgreSQL 18.6 container per run with server defaults. All 30 runs had zero failed jobs.

| Test | Metric | Runwick | Laravel | Paired Laravel/Runwick |
|---|---|---|---|---|
| Drain | jobs/s | 173.7 (107–198) | 213.2 (196–242) | 1.2× (1.1–2.1) |
| 40 offers/s | jobs/s | 39.9 | 39.6 | 1.0× |
| 40 offers/s | p50 / p99 ms | 83 / 852 | 115 / 851 | p50 1.2×, p99 1.0× (0.5–1.2) |
| 100 offers/s | jobs/s | 99.8 (92–100) | 93.2 (48–97) | 0.9× (0.5–1.0) |
| 100 offers/s | p50 / p99 ms | 269 / 1,555 | 114 / 517 | p50 0.5× (0.0–1.2), p99 0.3× (0.1–0.5) |

So on SQLite Runwick is faster at saturation and at 100 offers/s, while the two are equal at 40 offers/s. On PostgreSQL Laravel's driver drains faster and has lower tail latency at 100 offers/s. Round 4 of the PostgreSQL pairs was noisy in both arms (Runwick p50 4.9 s, Laravel 48 jobs/s), so its ranges are wide.

Read these with four caveats:

- **Not equal work.** Runwick pays for leases with a fence, command receipts and an event stream. Laravel has none of these: after `retry_after` a second worker can take a job, and the first worker's late delete removes it from under the second. A faster Laravel drain on PostgreSQL is not a like-for-like cost.
- **Polling.** Laravel's workers poll, so `--sleep=1` adds up to a second of idle pickup delay. With 16 desynchronized workers it showed as roughly 100 ms at these rates.
- **Different runtimes.** The SQLite Laravel arm ran on PHP 8.4 in a container, and the PostgreSQL arm ran natively on PHP 8.3. Runwick ran natively. Both PHP runtimes have opcache off for the CLI.
- **Shared host.** The 16 PHP workers keep the load average at 8 to 16 by themselves, so the load snapshots do not show outside interference. Treat the figures as diagnostics.

**PHP 8.3 finding.** On PHP 8.3 Laravel's SQLite queue is unreliable under 16 workers. Laravel reads `transaction_mode` only from PHP 8.4, so every pop transaction stays `DEFERRED`. A read-then-write upgrade then fails immediately with `SQLITE_BUSY` ("database is locked") despite a verified 5 s busy timeout. With `--tries=1` Laravel records those errors as `MaxAttemptsExceededException`, hiding the real cause. In the first run 13 of 15 SQLite Laravel runs failed jobs (251 of 20,000 in one drain), and a 2,000-job diagnostic gave 10 jobs/s with 31 failures. The same drain on PHP 8.4 gave 98.5 jobs/s with none. The harness now refuses to report SQLite unless the PHP version applies `BEGIN IMMEDIATE`, and records worker errors when jobs fail. The first-run outputs are kept in `laravel-queues/pairs/`. Its PostgreSQL results are the ones used above, and its SQLite results are not used.

## PostgreSQL bulk cap of 1,000 items (B2)

**Change.** The public PostgreSQL `DispatchMany` cap is now 1,000 items, up from 100. SQLite and Redis keep 100. The cap lives in the client (`IBatchLimitSource` on the PostgreSQL store, read by the preparer, caller-owned staging and prepared-envelope import). The dispatch SQL never checked the count, so there is no migration. Bytes stay bounded by the existing 2 MiB batch envelope, so 1,000 items fit only when each averages under about 2 KiB: 512-byte and 640-byte payloads fit, 1 KiB payloads do not (`batch_envelope_too_large`, rejected before any write). Spec 01 §12, 02 §4.1 and 04 (changes 2026-11-14) record it.

**Tests.** `BatchCapTests` (PostgreSQL): 1,000 mixed plain, keyed and resource items commit once and replay to the same receipt, including through an exported and imported envelope; 1,001 items fail with `batch_too_large` and no table changes; a window-key conflict at ordinal 999 rolls back all 1,000 and `ContinueOnConflict` commits the other 999; the envelope byte cap still applies. A Core test covers the preparer and envelope reader at the cap and one over. The shared contract tests now assert one over each store's own cap, and passed 24 of 24 on SQLite, PostgreSQL and Redis (Redis 8.2.11 with AOF always). On the final tree Core passes 440 of 440, Outbox 48 of 48 and PostgreSQL 391 of 391 (logs in `artifacts/perf/round-two/pg-b2-cap/suites.log`).

**Measurement.** Bulk dispatch of 20,000 jobs with 512-byte payloads on a fresh PostgreSQL 18 container, default durability, five alternating pairs per caller count, on this shared host. Raw files and `summary.json` are in `artifacts/perf/round-two/pg-b2-cap/`. No dispatch failed in any run.

| Callers | Items per call | Median jobs/s | Paired throughput ratio (1,000 / 100) | Call p99 | WAL per job |
|---|---|---|---|---|---|
| 1 | 100 | 1,023 | 1.14 (0.97 to 1.25) | 297 ms | 3.4 KB |
| 1 | 1,000 | 1,215 | | 2,314 ms | 3.4 KB |
| 8 | 100 | 2,479 | 0.65 (0.61 to 0.76, all five pairs lower) | 1,652 ms | 3.5 KB |
| 8 | 1,000 | 1,678 | | 6,649 ms | 4.9 KB |

**Reading.** With one caller, 1,000-item calls raise throughput by about 14%, but the range crosses parity and a call now takes about 2.3 s at p99. With eight callers, 1,000-item calls lose about 35% throughput in every pair, and call p99 is four times higher. Per-job WAL is flat at one caller and about 40% higher at eight by arm median (4.9 against 3.5 KB; the paired median ratio is only 1.04). Call latency is the only proxy for how long a call holds locks; lock wait was not measured separately. The cap is a ceiling for single-caller bulk loads, not a better default. Keep 100 as the recommended size and document that larger calls raise latency and hurt concurrent producers.

**Not done.** Per-ordinal lock-time and WAL profiling at 100 (the plan's profile-first step), 64 KiB and mixed payload sizes, and a rerun on the reference host.

## Redis `appendfsync everysec` profile (G4)

**Change.** An opt-in second persistence profile, `redis-aof-everysec-single-primary-v1`, selected with `RedisOptions.Persistence = RedisPersistence.AofEverySecond`. The default stays `appendfsync always`. Startup, the migrator, the periodic check and the reconnect check all require the setting that matches the chosen profile, so a server changed underneath a running host stops further writes with `store_profile_mismatch`. Receipts and the Store page carry `redis.aof-everysec-single-primary-v1`, and outbox receipts store `redis.aof-everysec.v1`. Spec 22, the Redis page and the delivery-guarantees page record it.

**Tests (`RedisEverySecondTests`, three, all pass).** Each profile accepts only its own `appendfsync` and the migrator agrees; a host on the everysec profile reports it in its receipt, is refused after the server is switched to `always`, and recovers when it is switched back; 200 confirmed dispatches all resolve as confirmed after `SIGKILL` and restart of the Redis container. The whole Redis test project passes with these changes (76 of 76, 1.08 hours, including the kill-and-restart crash cases); the Contract and Outbox suites show no failures apart from one load-sensitive PostgreSQL CPU-usage test that passes when run alone.

**Measurement.** Drain of 20,000 jobs of 1 KiB, 8 producer callers, 16 workers, on two throwaway Redis 8.2.11 servers on this shared host (`always` on one, `everysec` on the other), five alternating pairs per variant. The "rewrite" variant issues `BGREWRITEAOF` every 3 s during the run (179 rewrites on one server by the end). Every run exited 0 and reconciled all 20,000 jobs. Raw files and `summary.json` are in `artifacts/perf/round-two/redis-g4/`.

| Variant | Insert jobs/s, always | everysec | Paired ratio | Drain jobs/s, always | everysec | Paired ratio |
|---|---|---|---|---|---|---|
| plain | 1,415 | 1,648 | 1.16 (1.05 to 1.45) | 177 | 272 | 1.54 (1.42 to 1.64) |
| with rewrites | 1,136 | 1,539 | 1.35 (1.20 to 1.73) | 147 | 221 | 1.51 (1.25 to 1.82) |

**Reading.** `everysec` drained about 1.5 times faster in all ten pairs and inserted 1.16 to 1.35 times faster. The profile removes the per-command fsync wait, so the gain is largest where commands are small and frequent (the worker path). Drain rates here are low in absolute terms because the host is shared and each job costs several scripts; compare ratios, not rates.

**Acknowledged-loss exposure.** Killing the Redis process loses nothing acknowledged: the 200 confirmed dispatches all survived `SIGKILL` and an AOF reload, because the appended bytes are with the kernel. The exposure the profile adds is an operating-system or machine crash, which can lose about the last second of acknowledged writes. A power loss was **not simulated** (it needs a VM or hardware-level cut), so the one-second figure is Redis's documented behavior, not a measurement here. Receipts under this profile must not be read as machine-crash durable. Replicas and failover remain unsupported under both profiles.

**Not done.** Measuring real loss under an OS crash or power cut, a quiet-host rerun, and the paired comparison against BullMQ under the everysec setting (the delayed BullMQ comparison remains under `always`).

## Buffered finish (G1)

**Status 2026-11-15:** landed as an opt-in worker profile, `WorkerOptions.UseBufferedFinish()`. Off by default. The default behaviour and every default value are unchanged.

### What it does

Today a job holds its worker slot until the database has confirmed that the job finished. With buffered finish, the slot goes back to the pool the moment the handler returns and the finish is queued. The finishes are then sent to the store together, about every 25 ms, or sooner when 32 have collected. The next job starts while the previous finish is still on its way.

The job itself is not treated as finished until the store says so. While its finish is waiting, the job keeps everything that fences it: its entry in the worker, its lease, its concurrency and rate permits, and its unique key. Only the local slot is reused. This is what the plan asked for ("remote permits remain held until storage accepts the finish").

### Limits, so memory and risk stay bounded

- At most 256 unsettled buffered finishes (setting: 1 to 4,096).
- At most 4 MiB of estimated finish size, counting error text (setting: 64 KiB to 64 MiB).
- If the oldest buffered finish is more than 1 s old, no more slots are given back until it settles.
- When any limit is hit the slot is simply kept until the finish settles, which is exactly what the default does. That is how refilling stops. Nothing is dropped or queued without bound.
- A partial batch never waits past its flush interval (25 ms by default, 1 ms to 1 s), and it is sent at once if the worker is shutting down or any collected job is within about 13 seconds of its local authority deadline.

### What it can lose

A worker that dies while finishes are still only in its memory has not told the store those jobs finished. Their leases run out, recovery returns them to retry, and the handler runs again. This is the same at-least-once behaviour Runwick always has, but the window is wider: up to the buffer, a few tens of milliseconds of completed work. Handlers must be safe to run twice. A normal shutdown sends the buffer first. If the store does not answer during shutdown, the outcome is unresolved, just as without the option.

### Tests

- Core (fake time, scripted store with `FinishMany`): off by default and its limits are validated (V-22); a held store lets later jobs start only with the option on (1 job against 6); the count limit stops slot reuse at the bound and everything settles once the store accepts (4 of 10 started while held, 10 of 10 settled after); the byte limit holds; simultaneous finishes go out in one call and a lone finish waits only the interval; a drain flushes at once; a buffered attempt stays tracked in `Finishing`, fenced and unretired until accepted; finishes that never reach the store leave the job to expire and be recovered with no finish recorded. 8 of 8 pass; the Core project passes 449 of 449.
- Contract, SQLite, PostgreSQL and Redis: 60 jobs settle once each with one Finish receipt each and the buffer empties; a full buffer (limit 2, 8 slots) keeps slots and loses nothing; a graceful stop sends 16 buffered finishes; on SQLite and Redis, a worker whose finishes never reached the store leaves 8 jobs that another worker recovers and runs again (each job then has two attempts). 11 of 11 pass. The neighbouring finish-batching and worker contract tests (45) still pass.
- Not covered on a real store: a hard kill of the worker process (the crash case stops the first worker normally while its finishes are held; PostgreSQL has no equivalent hold in the harness).

### Measurement

Paired default against buffered drain runs, alternating order, 4,000 jobs of 512 bytes, 4 and 16 workers, on SQLite and PostgreSQL 18 (docker, same host). Every run took `flock /tmp/runwick-bench.lock`. The host is shared with the running SP-029 qualification and other agents' work, so the numbers are informational. Raw files: `artifacts/perf/round-two/g1-buffered-finish/` in the G1 worktree.

All 40 runs finished (exit 0) and every run reconciled: all 4,000 jobs drained, finished and invoked once.

| Store, workers | Default (jobs/s, median) | Buffered (jobs/s, median) | Median paired ratio | Ratio in each pair |
|---|---|---|---|---|
| SQLite, 4 | 50.7 | 70.4 | 1.34 | 1.82, 1.08, 1.34, 1.32, 1.71 |
| SQLite, 16 | 147.1 | 171.6 | 1.17 | 0.98, 1.17, 2.06, 1.03, 1.24 |
| PostgreSQL, 4 | 49.1 | 59.1 | 1.17 | 1.17, 1.32, 1.23, 1.05, 1.00 |
| PostgreSQL, 16 | 106.0 | 125.2 | 1.18 | 1.14, 1.77, 0.69, 1.34, 1.18 |

Reading it honestly: buffered finish was faster in 18 of 20 pairs, and the median gain is 17% to 34%. At four workers the slot is the limit, so the gain is largest and most consistent (never below 1.0). At sixteen workers the pairs are spread widely (0.69 to 2.06), so the gain there is likely real but the size is not pinned down. These are low absolute rates because the host was busy with the SP-029 qualification and other benchmarks, so only the paired ratios mean anything, and they are informational. No arm was run on Redis.

### Open

- Redis was not benchmarked for G1; its contract tests pass. The 16-worker pairs are too spread out to fix the size of the gain; a quiet host would help.
- A hard-kill test of a real worker process is not done.
- Interaction with auto-concurrency: the scaler sees lower occupancy during the finish round trip. Not measured.
- Whether to make this a per-queue setting instead of per worker. Today it applies to every pool of the worker.

## PostgreSQL grouped producer admission (B1)

### What was built

`PostgresOptions.GroupProducerAdmission` (default off) lets one process commit several independent `DispatchAsync` calls in one PostgreSQL transaction.

- **How callers are grouped.** A caller that finds nothing in flight sends its own command at once and waits for no one. Callers that arrive while a send is in flight wait in a small queue (at most 8 commands and 2 MiB, counting each payload plus 2 KiB) and leave together when the slot frees. There is no timer, so the "at most 2 ms collection wait" in the plan is never spent: the queue fills while the previous send is on the wire. A command that does not fit the queue takes the ordinary path. The number of groups in flight is 1 by default; 2 is available for measurement through an internal setting.
- **What is grouped.** Only plain `Dispatch/1`: no uniqueness key, no concurrency or rate scope, no schedule header. Batches, calls on your own transaction, the outbox, schedules and management dispatch never join a group.
- **The database side.** Migration `0023_dispatch_group` (schema revision 23) adds `dispatch_group_v1`. It takes the gates once, then checks each command's receipt in command-ID order with the existing receipt helper. A replay, a conflict, an expired deadline, a bad request or an unregistered queue answers for that command alone, and the placeholder it inserted is removed. The rest share one decision time and insert their jobs, payloads, first generation and one event each, in the same shape a single dispatch writes. A parity test compares the stored rows of a grouped job with a single job's, apart from IDs and times.
- **Receipts and recovery.** Every command keeps its own operation ID, receipt, deadline and result. If anything fails before COMMIT is sent (including a member reaching its deadline after the writes, or a job ID collision), the whole group rolls back and every command retries alone with its original identity. If COMMIT was sent and the reply is lost, each command gets an outcome-unknown error carrying its own command ID, and `ResolveAsync` or a resend returns the original receipt.
- **Cancellation.** A caller that cancels while queued leaves the queue and nothing is sent for it. A caller that cancels after its group was sent stops waiting with its recoverable operation ID, and the transaction still settles its receipt.
- **Shutdown.** Disposing the store fails queued commands as not applied and drains groups already sent (cancelled after 10 seconds).

### Tests

Nine focused tests in `GroupedAdmissionTests` pass: one group with single-dispatch row parity and individual replays; a lone caller never forms a group; six mixed outcomes in one group (fresh, replay, conflict, expired, unregistered queue, fresh) with no receipt left for refused members; a job-ID collision rolling the group back and three commands retrying alone; cancellation while queued; cancellation after submission and resolving by ID; a lost reply giving outcome unknown to all members and replays on resend; disposal; and non-plain commands skipping the queue. The whole PostgreSQL project passes (403 tests; two timed out under the host's heavy load and passed alone), the Contract project passes 480 of 480, and the migration tests cover the new migration. The Core project passes 440 of 441; the one failure (`WorkerEngineTests.Hung_disposal_latches_unconfirmed_completion_without_a_retryable_finish`) also fails on the main checkout's build, so it does not come from this change.

### Measurement

Single-job `DispatchAsync` from 1, 8 and 16 callers, grouping off against on with one group in flight (g1) and with two (g2), alternating arm order for each pair, 3 pairs at 1 KiB and 2 pairs at 64 KiB and 1 MiB. Every run took `/tmp/runwick-bench.lock`.

**These numbers are informational and very noisy.** The host was running the SP-029 qualification and other agents' work, with a load average between 19 and 54 on 8 cores, and a single caller managed only 40 to 50 jobs/s. Rates are the median of the pairs; the ratio is the median of the per-pair grouped ÷ off ratios, with each pair in brackets.

| Payload | Callers | Off (jobs/s) | g1 (jobs/s), mean group | g1 ÷ off | g2 (jobs/s), mean group | g2 ÷ off |
|---|---|---|---|---|---|---|
| 1 KiB | 1 | 48.8 | 39.2, none formed | 0.92 [1.26, 0.62, 0.92] | n/a | n/a |
| 1 KiB | 8 | 187.1 | 113.3, 4.5 | 0.63 [0.63, 0.90, 0.58] | 108.8, 4.9 | 0.60 [0.60, 1.04, 0.56] |
| 1 KiB | 16 | 231.8 | 212.5, 8.0 | 0.66 [0.57, 1.04, 0.66] | 322.8, 5.6 | 1.57 [1.66, 1.57, 0.87] |
| 64 KiB | 1 | 40.8 | 44.9, none formed | 1.10 [1.09, 1.12] | n/a | n/a |
| 64 KiB | 8 | 167.4 | 99.3, 4.1 | 0.60 [0.72, 0.47] | 124.5, 3.4 | 0.74 [0.55, 0.93] |
| 64 KiB | 16 | 168.3 | 132.7, 8.0 | 0.79 [0.81, 0.77] | 185.2, 5.5 | 1.12 [1.29, 0.94] |
| 1 MiB | 8 | 36.5 | 31.7, none formed | 0.88 [0.60, 1.16] | 36.0, none formed | 0.98 [1.06, 0.91] |

A 1 MiB payload leaves room for one command in the 2 MiB queue, so no group ever formed there and those rows only show the cost of the extra bookkeeping. WAL per job is not reported: the host-wide WAL counter included other databases' writes.

### Reading

- Grouping with one group in flight lost throughput at 8 callers (a ratio of 0.58 to 0.90 at 1 KiB, 0.47 to 0.72 at 64 KiB) and at 16 callers with one group in flight (a median of 0.66 at 1 KiB). The off arm has up to 8 or 16 connections committing at once, and one group at a time serializes them.
- Two groups in flight did better at 16 callers: 1.57 times at 1 KiB (two pairs near 1.6, one at 0.87) and 1.12 times at 64 KiB (1.29 and 0.94). That is the only cell where grouping looks useful, and three pairs on a loaded host cannot confirm it. At 8 callers two in flight still lost (0.60 and 0.74).
- A lone caller was neither helped nor held back beyond noise: no group formed, and the ratio at 1 KiB moved between 0.62 and 1.26 pair to pair.
- No fallbacks happened in any run, so the rollback path costs nothing in the measured mix.

### What stays open

- The measurement needs a quiet host with a separate database host before grouped admission is recommended. Until then it stays off by default and unrecommended; the plan's gate (lower SQL or commit cost under concurrency with no singleton latency regression) is not met at 8 callers.
- Whether two groups in flight is the right default at 16 or more callers, and whether the producer lane size should change with it.
- Server-side fault points (`BeforeFirstMutation`, `AfterMutation`) are not wired into the group function. Tests use the client fault points before, at and after COMMIT, applied to the group's first command.
- The admission lease still belongs to the caller. A caller that stops waiting releases its lease while its group is in flight. The abandoned work is bounded (at most 8 commands, one or two groups) rather than tracked per caller.
- Storage telemetry records one `Dispatch` operation per group, not one per command.
- Raw runs: `artifacts/perf/round-two/pg-b1-group/` in the worktree (`pairs.sh`, `summarize.py`, `summary.json`, the 46 result files). Git ignores it.

## Grouped outbox delivery (D4)

**What changed.** The relay can now send several messages it claimed together to the destination as one transaction. Each message keeps everything that made single delivery safe: its own transport command ID and deadline, its own receipt, its own delivery mapping and its own reply. After the destination answers, the relay marks each message delivered at the source under that message's own lease token, as before. Only the connection, the transaction and the commit are shared.

It is off by default. `OutboxRelayOptions.DeliveryGroupSize` is 1 (each message alone, exactly the old behavior); 2 to 16 turns grouping on. Only plain messages are grouped (no uniqueness key, no concurrency scope, no rate limit), so a group never takes a row lock that another group might also want. Every other message in the same claim is sent alone at the same time.

**PostgreSQL destination.** Migration `0023_outbox_group` (schema revision 23) adds one driver-only function, `receive_outbox_group_v1`. It checks the shape of the whole group first (2 to 16 items, no repeated command or message identity, no keyed item, at most 2 MiB of payload) before it creates any receipt. It then runs each item through the existing `receive_outbox_v3` in its own subtransaction, in command-ID order. An item that is refused (conflict, expired command, passed horizon, invalid) rolls back with its receipt placeholder and reports its own reply, and the others commit. No protocol operation was added, and the manifest changes only its schema revision. The migration file may need a different number when it is merged with the other PostgreSQL migration in this round.

**SQLite destination.** No relay change was needed. SQLite already commits concurrent producer calls together (up to 8 calls or 1 MiB queued at the same moment, each in its own savepoint). `ReceiveOutbox/1` now goes through that same path, so relay slots that send at the same time share one commit.

**Redis destination.** Not changed. It does not implement the grouped call, so the option is ignored and every message is sent alone. A grouped Redis script would be a separate change.

**If a group fails.** Any answer that is not a complete, applied group reply (a lost reply, an unreachable destination, a timeout, a refusal of the whole group) makes the relay send each command alone with its original command ID. The per-command receipts and the delivery mapping answer the ones the group already applied, so no second job can appear. This is covered by a test that applies the group and then loses the reply.

**Tests.** Seven new test methods in `GroupedDeliveryTests` (11 runs, counting both source adapters), all passing:

- a protocol-level group with fresh, replayed, conflicting and expired commands, each getting its own reply and receipt, with no leftover receipt for the refused ones;
- groups the store refuses before any effect (one command, a repeated command, a repeated message identity);
- the relay delivering twelve messages as groups, one job and one mapping each, on both source adapters;
- a lost group reply followed by single resends (one receipt per command, no second job);
- one message that blocks at the relay while its claim mates are delivered and confirmed;
- delivered marks lost after a group (written but reply lost, and never written), repaired by the mappings;
- the option having no effect on a SQLite destination.

The whole Outbox project passed 57 of 57 before the last test was added (the last test passed on its own, 2 of 2), the SQLite project passes (54 of 54), and the PostgreSQL migration, operation-version, round-trip-budget, wake-notification and batch-cap cases pass (83 of 83) with the revision 23 golden manifest. The outbox and relay contract cases in the Contract project pass (14 of 14).

**What is not done.**

- P-09 numbers: see the measurement section below; they are informational because the host is shared.
- Batched source marks (D2) are not part of this change; the mark of each message is still its own source call.
- The earlier isolated destination-wake candidate (`p09-d4/receive-wake.patch`) was not adopted here. It adds its own migration and entry point that would have to be reconciled with `0023_outbox_group`.
- Grouping on Redis.

### P-09 measurement (informational, shared host)

Setup: the P-09 workload (`--rate 100`, 8 business writers, 1 KiB intents, 30 s warm-up, 60 s measurement, 2 min drain), PostgreSQL 18 source and destination in a fresh pinned container with fsync, synchronous commit and full-page writes on. The same build runs both arms: group size 1 against group size 4 at 4 relay slots. The host was shared with the SP-029 qualification run and several other agents' builds and benchmarks, so absolute rates are far below earlier quiet-host numbers and only the paired comparison means anything.

Three alternating pairs each at 4 slots (group 4) and 16 slots (group 8), same build, group size 1 against the group size:

| Config | Pair | Confirmed/s off → on | Destination commit mean per call off → on | Avg group |
|---|---|---|---|---|
| 4 slots, group 4 | 1 | 19.5 → 25.3 | 72.8 → 44.0 ms | 2.3 |
| | 2 | 27.0 → 21.6 | 41.3 → 55.9 ms | 2.3 |
| | 3 | 33.1 → 38.3 | 29.6 → 24.1 ms | 2.3 |
| 16 slots, group 8 | 1 | 60.4 → 69.5 | 85.1 → 61.7 ms | 2.9 |
| | 2 | 54.4 → 69.3 | 94.8 → 50.9 ms | 2.9 |
| | 3 | 49.8 → 46.6 | 103.0 → 117.6 ms | 2.9 |

Medians: at 4 slots 27.0 → 25.3 confirmed/s (paired median ratio 1.16, with one pair worse); at 16 slots 54.4 → 69.3/s (paired median ratio 1.15, with one pair worse). Ready-to-confirm p99 at 16 slots was 35.3 s off and 39.3 s on (ratio 1.11, so no better). At 4 slots most runs never drained and report no p99. There were no fallbacks, and every message that was delivered had one mapping, one job and no mark mismatch in all twelve runs. Groups were small (2.3 and 2.9 commands on average) because a claim only returns as many messages as there are free slots, so a larger group size does nothing until the claim itself is larger.

**Reading.** Grouping cuts destination commits per delivery in most pairs and helps median throughput by about 15%, but two of six pairs went the other way, the host was loaded by other work, and the tail did not improve. **P-09 is not met at either slot count** (100 deliveries/s and p99 at most 2 s), so grouped delivery stays opt-in and the default stays 1. The remaining cost sits at the source (one claim, one transport write and one delivered mark per message) and in the small claim size, not only in destination commits. The next steps with a real chance are batched source marks (D2) and claiming more messages per call than there are free slots, measured on a quiet host.

Raw results: `artifacts/perf/round-two/outbox-d4/slots4-g4/` and `slots16-g8/` in the worktree (git-ignored); `python3 summarize.py <dir>` prints the paired medians.

## Scheduler work area S: round-two follow-up (2026-11-15)

Everything here ran on the shared host while the SP-029 qualification and four other agents were also running. The host load average was 17 to 50 on eight cores. Treat every timing as informational. The correctness checks are not affected by load: every run acknowledged all 3,000 expected occurrences, with no duplicate instants, no orphan occurrences, no shared jobs and no lease losses.

### What changed

1. **Group sizes 2 to 8 are now an option** (`SchedulerOptions.MaterializationGroupSize`, default 4, PostgreSQL migration 0023, additive). This is the isolated eight-plan candidate from the earlier report, landed with its tests: Core scheduling 181, PostgreSQL migrations 72 and the schedule contracts 73 (SQLite, PostgreSQL and Redis) all passed. Migration 0023 takes the next free number, so it will need renumbering if another change lands a migration first.
2. **The grouped loop is pipelined.** It reads the next due page while the current page commits, keeps groups in flight across the page boundary, and prepares the next group while both slots are busy. Before this, each page of 32 schedules drained both groups, then waited for a read, then waited for the first new group to fill. Two new Core tests cover it: the second page is read before any group commits, and shutdown leaves no group running after a full page. Spec 13 has a dated note.
3. **The P-06s workload gained `--scheduler-replicas N`** (equal aggregate allowance: 100/N jobs/s and burst 32/N per replica) and `--jobs-per-second` / `--burst` for the scheduler process.

### Pipelining, paired short runs (one round)

Same 1,000 schedules, 100 jobs/s allowance, two groups in flight, two minutes of warm-up, three of measurement, a fresh PostgreSQL 18.6 container per run. "Base" is commit 295acb9 and "pipelined" is a85849f. Post-commit acknowledgment lag:

| Arm | p50 | p95 | p99 | Mean due-read wait | Mean function per group |
|---|---:|---:|---:|---:|---:|
| Four plans, no pipelining | 9.55 s | 17.40 s | 18.61 s | 36.7 ms | 90.8 ms |
| Eight plans, no pipelining | 8.96 s | 20.44 s | 22.18 s | 48.1 ms | 146.4 ms |
| Eight plans, pipelined | 8.89 s | 20.28 s | 22.42 s | 6.6 ms | 154.7 ms |

Pipelining did what it was meant to do: the loop now almost never waits on a due read (48 ms down to 6.6 ms per page). It did not move p99 here. At this host load the database function took 146 to 155 ms per group of eight, against 95 ms on the quiet host, so two groups in flight give about 70 plans/s, below the 100/s allowance. The run is limited by the database, not by the loop. One pair is not enough to separate a small gain from noise, and the later rounds were cancelled to free the shared benchmark lock. The earlier quiet-host eight-plan runs (p99 12.47 and 13.86 s) remain the closest evidence for the 13 s gate, and the pipelined loop needs a quiet-host repeat before it can be called better.

Why the 13 s gate is still open: per plan, the database function costs 12 to 16 ms at every group size, so grouping saves round trips and commits but not the per-plan work (job and payload insert, two events, the occurrence row, the schedule update, the receipt). That work is the next lever, and any change to it needs its own replay and rollback proofs. I did not touch it.

### S6: replica contention at equal aggregate allowance

One run each, eight-plan pipelined groups, 1,000 schedules, aggregate 100 jobs/s split evenly:

| Schedulers | Plans sent for 6,000 occurrences | Revision conflicts | p50 | p99 |
|---:|---:|---:|---:|---:|
| 1 | 6,000 | 0 | 8.65 s | 16.86 s |
| 2 | 8,405 | 1,464 | 11.19 s | 21.63 s |
| 4 | 15,650 | 7,240 | 20.08 s | 39.32 s |

Contention is material. Every replica reads the same ordered head of the due index, so they plan the same schedules, and the loser of each race wastes a plan and a lock wait. Two replicas send 40% more plans than needed and four send 2.6 times as many. Correctness holds (zero duplicates), but adding replicas makes lag worse, not better, at a fixed aggregate allowance. Revision checks remain the safety mechanism. Conflict counts do not depend on host load, so this conclusion is sturdier than the lag figures.

I did not build an ownership protocol. The plan asks for crash recovery, rebalance and abandoned-claim tests for that, and it is a design of its own. The cheapest direction to evaluate is a short claim written through the existing revision check (advance a schedule's scan time by a few seconds before planning it), so a second replica skips it; the cost is one more write per schedule.

### Decisions on the other S items

- **S2 (overlap independent plans):** superseded by the grouped S3 path, which already keeps two groups in flight. Four pending commands queue behind the two-slot PostgreSQL background lane, so the lane is the cap.
- **S4 (durable due hints):** landed earlier; unchanged.
- **S5 (cache compiled recurrences):** not justified. Evaluation costs 0.064 ms per plan on average (0.4 ms at p99), against about 12 ms of database work per plan.

### What is still open

- A quiet-host P-06s repeat of the four- against eight-plan, pipelined and not, including the 15-minute steady window and the 1,000-occurrence catch-up.
- The per-plan database cost (see above).
- A replica ownership design, if running more than one scheduler matters for P-06.

Raw runs: `artifacts/sched-s/arms/` and `artifacts/sched-s/replicas/` in the worktree (git ignores them), with `run-arms.sh`, `run-replicas.sh` and `summarize.py` beside them.

## Redis grouped dispatch and the BullMQ comparison (work area F)

Written 2026-11-15. Informational: every number below was measured on the shared host, with the full SP-029 matrix and other agents' test suites running at the same time (load average 20 to 60 on 8 cores). Benchmarks ran one at a time under a shared lock, alternating arms inside each round, so each pair saw the same neighbours. They still carry host noise. They do not replace a quiet-host run.

### What was built

An opt-in way to send several plain dispatches to Redis in one call (`RedisOptions.GroupDispatches`, default off, with `MaxGroupItems`, `MaxGroupBytes` and `MaxGroupsInFlight`). It is the F3 proof wired into the client.

- A caller that finds a free send slot goes out alone at once, as before. A caller that arrives while every slot is busy waits in a small bounded queue and goes out with the others waiting, when the next slot frees. Nothing waits on a timer. If the queue is full, or one dispatch is larger than a group may carry, that dispatch goes alone.
- Each member keeps its own command ID, fingerprint, deadline, receipt and event. Resending one later, alone or in another group, returns its original job. Delayed dispatches can be grouped. Dispatches with uniqueness keys, concurrency scopes, rate scopes or schedule headers never are.
- The Lua script decides each member alone. When it cannot group a set (repeated command ID, an unsupported shape, too many keys or writes, too large, or slow to decide) it says `group_fallback` before writing anything, and the client sends each member alone with its original ID.
- A lost reply gives every member an "outcome unknown" for its own command, to resolve or resend as before. A caller cancelled while waiting leaves nothing behind. A caller cancelled after the group was sent gets the real outcome. Shutdown fails never-sent members as not applied.
- No layout revision, no manifest change. Groups write the ordinary job, receipt and event rows.

Tests: 10 new (`RedisGroupedDispatchTests`) cover concurrent grouping with one event per command, lone callers never grouping, delayed members, queued cancel, cancel after send, a lost group reply, every write prefix of a group (the injected-failure sweep, 3 members, until a run completes) with recovery and resend, a declined group, a mixed group (replay, fresh, expired) and a kill and restart of Redis with 160 grouped dispatches all confirmed afterwards. The whole Redis project passes 86 of 86 (1.49 hours, including the kill-and-restart cases). The Contract project passes 480 of 480.

### Does it help? Mostly no, on this host

Five alternating pairs at 16 producer calls, 10,000 one-kilobyte dispatches each (median):

| Arm | Jobs/s | Enqueue p95 / p99 (ms) |
|---|---|---|
| Runwick, grouping off | 432 | 75 / 142 |
| Runwick, grouping on (2 slots, groups of about 6) | 360 | 97 / 191 |
| BullMQ 6.7.2 | 897 | 40 / 86 |

Grouping on was 0.81× of grouping off (paired ratios 0.79 to 1.03). Runwick without grouping was 0.49× of BullMQ.

Three alternating pairs at 64 producer calls:

| Arm | Jobs/s | Enqueue p99 (ms) |
|---|---|---|
| Runwick, grouping off | 684 | 277 |
| Grouping on, 2 slots (groups of 8) | 559 | 409 |
| Grouping on, 4 slots | 532 | 452 |
| BullMQ | 2,491 | 81 |

Three more pairs with more sends in flight (64 callers): off 705 jobs/s, 8 slots 761 (paired median 1.19×, pairs 1.19, 0.78, 1.24), 16 slots of 4 items 718 (1.14×). So grouping can break even or gain a little when enough sends are in flight, but the effect sits inside this host's noise.

### Why: the script costs too much per job, not the number of calls

- Redis reports 1,213 µs per single dispatch script call against 157 µs for a BullMQ add (same run, same host). Redis spent 12.7 s of CPU on 10,500 Runwick dispatches and 2.0 s on 10,500 BullMQ adds.
- With phase marks inside the real script (single dispatch, microseconds): reading the application and checking the gates 146, deciding 184, preparing events 90, ordering the writes 170, packing the intent 69, writing the guard 41, writing the data 218; about 1,000 in total. A group of 6 costs about 650 to 700 per job: the fixed part is shared, the rest is per job.
- Without the .NET client, over plain sockets, eight connections get about 280 to 350 jobs/s with single dispatches and about 900 to 920 jobs/s with groups of 8 (script time 1.3 to 1.9 ms per single call, 2.5 to 2.6 ms per group of 8). With `appendfsync always`, one fsync covers everything in an event-loop pass, so the number of jobs in flight decides throughput. That is why grouping can triple the raw ceiling.
- Through the .NET client the gain disappears. Grouping lowers the jobs in flight when few sends are allowed (2 slots of 8 is 16 jobs in flight against 64 callers sending alone), and it adds a queue hop, one reply parse per group and a thread-pool switch. With enough slots it roughly breaks even.

The 90% of BullMQ target needs script work per job to fall several times, not only fewer calls. That means changing what the dispatch writes (per-job events, receipts, indexes and the stored intent that is then written again), which is a protocol change, not a client change.

### Steady, delayed and drain workloads

Three alternating pairs each, 16 callers and workers, 1 KiB payloads.

- **Steady 500 offers/s.** Both systems are overloaded. Runwick accepted a median 256 jobs/s (7,331 refused locally of 15,000), grouping on 201, BullMQ 441 (1,775 refused). Runwick's due-to-handler p99 was 31.7 s (backlog: workers finish about 160 jobs/s) against BullMQ's offer-to-finish p99 of 0.5 s. This is a worker and finish-path limit, not a dispatch limit.
- **Delayed 100 offers/s, 5 s ahead.** Throughput is at parity: 100.0 against 96.6 jobs/s (BullMQ refused 102 locally, Runwick none). Enqueue p99: Runwick 233 ms, grouping on 265, BullMQ 290. Due-to-handler p95/p99: Runwick 494/689 ms, grouping on 888/1,348, BullMQ 691/864. Paired Runwick/BullMQ p99 ratios were 0.74, 1.71 and 1.43 (median 1.43), better than the earlier five-pair run (3.43×) but still outside the 10% band. Grouping did not help this workload.
- **Drain of 20,000 pre-seeded jobs, 16 workers.** Runwick 159 jobs/s, BullMQ 664 jobs/s (0.23×, pairs 0.25, 0.22, 0.23). Grouping does not apply: this is the claim and finish path.

### Verdict against the targets

Not met. At matched `appendfsync always`, saturated enqueue is 0.28× to 0.49× of BullMQ, drain 0.23×, steady 500 is 0.59× accepted, and delayed pickup p99 is 1.4× BullMQ's. Throughput at 100 delayed offers/s meets the 90% band. Grouped dispatch is correct and tested, but it is not a reason to turn on by default, and it should not be described as a way to reach the BullMQ target.

### Work hints, pipeline depth and intent encoding

- **Pipeline depth.** The right number of sends in flight depends on the fsync time and the caller count, not on a fixed small value. With 4 slots or fewer the grouped path under-fills Redis; with 8 to 16 it breaks even. Left at 2 by default because it is opt-in and conservative.
- **Intent encoding.** The script stores each command's after-images as a packed intent, then writes the same data a second time as it projects. Packing costs about 70 µs per job and the guard write 40 µs. Cutting both needs the recovery model to change; not attempted.
- **Ordering writes.** The bytewise comparator used to order hash fields costs about 80 to 110 µs per job. Replacing it with Lua's built-in string order saves about 12% of a single dispatch (992 to 876 µs) and 21% of a grouped one (697 to 551 µs per job), but built-in order follows the server's locale, so it is only safe if the script proves a bytewise locale or keeps its own comparator. Not applied.
- **Work hints.** The due-to-handler gap on delayed jobs comes from the worker's idle polling and its snapshot of ready work (up to two seconds old), which is shared engine behaviour across stores, not Redis script time. Not changed here.

### Evidence

Runner, raw results, the profile script and the raw-socket test are under `artifacts/perf/round-two/redis-f-grouped/` in the worktree for this branch (git ignores artifacts). Commands: `pairs.sh <workload> <rounds> [arms]`, `profile.py`, `rawthroughput.py`.

## Redis dispatch script diet (work area F follow-up)

Written 2026-11-15. Informational: measured on the shared host while other agents' test suites ran (load average well above the core count). Benchmarks ran one at a time under the shared bench lock, with arms alternating inside each round. Host noise is large, so read the end-to-end numbers as direction, not as exact gains.

### What was asked

The Redis F report found the dispatch script costs about 1.2 ms per job inside Redis, against 0.16 ms for a BullMQ add. This follow-up looks for script work that the guarantees (receipt, event, uniqueness, generation, fences) do not need, without changing what is stored, what is replied, or what a receipt says.

### What changed (3598d4c)

All changes keep the stored data and the replies identical. No layout change, so no migration.

- **Fields are written in order, not sorted.** The script wrote each hash's fields in bytewise order, which meant sorting them at the end of every dispatch. A new job hash has 28 fields and a receipt has 11. They are now written from fixed lists that are already in bytewise order (`U.JOB_FIELDS`, `RECEIPT_FIELDS`, through a new `U.hset_sorted`). Any later write to the same hash in the same command drops the shortcut and falls back to sorting, so a hash that was changed twice is still ordered correctly.
- **The event row is built directly.** Its eight keys are fixed, so the JSON text is assembled in bytewise key order instead of building a table and sorting its keys. The output text is the same as before.
- **The result is encoded once.** The receipt, the stored intent's reply and the final reply each encoded the same result table. It is now encoded once per command.
- **A cheaper bytewise comparator.** `U.blt` settles the first byte before entering its loop, and sorts of one item skip the sort. The ordering is the same (it is still an explicit byte comparison; no locale is involved).
- **Not adopted: Lua's built-in string order.** Measured at 11 µs against 104 µs for the 28 job fields, but it follows the server's locale, and the Redis spec (22, the Text row of the encoding table) says scripts never compare strings with Lua `<`. Nothing here proves the server runs a bytewise locale in every deployment, so ordering stayed explicit. A bucket-by-first-byte variant (66 µs) was also tried; the fixed lists beat it by removing the sort rather than speeding it up.

### Where the time goes in a single dispatch (microseconds, before the change)

Reading the application and gates 140, deciding 175, events 85, ordering writes 175, packing the intent 65, writing the guard 42, writing the data 195, about 940 to 1,000 in total. After the change: events 52, ordering 78, packing 46, the rest unchanged.

A finer profile (new `DEEP=1` mode of `profile.py`) shows what is left and cannot be cut without protocol changes:

- About 100 µs is spent before the script's own code runs: building the chunk's roughly 75 top-level functions for every call (prelude about 60, jobs about 40). Bisecting the prelude by section showed no single part dominating; it is the number of closures. Fixing it means shipping fewer, shared functions or caching them, which touches the claim and finish scripts another agent is editing, so it was not attempted.
- Reading the meta hash and checking gates, about 50 to 75 µs.
- Writing the data, about 200 µs for 11 Redis commands per dispatch (job, payload, 3 created indexes, jobtypes, ready index, receipt, receipt expiry, events, meta).
- Packing and storing the intent, then writing the same data again, about 110 µs. Cutting this needs the recovery model to change, not attempted.

### Script time, paired (`profile.py`, old Lua against new Lua, same Redis, back to back)

| | before | after | change |
|---|---|---|---|
| single dispatch, µs per job | 1,136 to 1,214 | 978 to 1,044 | about 13 to 15 percent less |
| group of 6, µs per job | 801 to 818 | 662 to 677 | about 17 percent less |

### End to end (bench harness, 10,000 enqueues, fresh AOF-always Redis 8.2 per run, grouping off)

Arms: `base` (tree before the change), `off` (this change), `bullmq`. Five rounds at 16 callers, three at 64.

| callers | base jobs/s (median) | this change | BullMQ | change / base | change / BullMQ |
|---|---|---|---|---|---|
| 16 | 375 | 401 | 559 | 1.07 by medians, per-round ratios 0.8 to 1.2 | 0.67 |
| 64 | 531 | 671 | 1,851 | 1.26 by medians, per-round ratios 1.06 to 1.30 | 0.36 |

Redis CPU seconds per run (user plus system): 16 callers, base 13.3 to 15.4, this change 10.5 to 13.3, BullMQ 3.0 to 4.0. At 64 callers, base 11.2 to 12.3, this change 8.9 to 9.9, BullMQ 1.9 to 3.3. So Redis does about 20 percent less work for the same ten thousand jobs. At 16 callers the throughput gain is within the noise (one round went the other way); the drop in Redis CPU is the steadier signal. BullMQ's own rounds varied by almost a factor of two (541 to 994 at 16 callers; 783 to 1,981 at 64), which shows how loaded the host was.

The gap to BullMQ is still large: the diet takes script time per dispatch from about 1.2 ms to about 1.0 ms, and the 90 percent target needs about 0.2 ms.

### Tests

- New `RedisDispatchLayoutTests`: the two pre-ordered lists are bytewise sorted and equal to the lists in the Lua source (so a field added in one place fails), a real dispatch writes every field of a job and receipt, the event row's keys are in bytewise order, and a resend replays the same job.
- Full Redis project (RUNWICK_TEST_REDIS, redis:8.2): 87 passed, 1 failed. The failure is `RedisSoakTests.Eight_connections_run_1200_mixed_commands...`: `ScheduleEvaluationException: The 250 ms evaluation budget elapsed` inside the C# cron planner, on a heavily loaded host. It is client-side schedule evaluation, not script code; re-run alone with the other Redis tests finished, the soak class passed (3 of 3).
- Contract project (with RUNWICK_TEST_REDIS set, so the Redis cases ran): 488 passed, 3 failed. All three failures are the Postgres case (`TelemetryPeerContractTests.A_peer_killed_after_Finish...`, `ManagementContractTests.A_read_the_caller_cancels...`, `BufferedFinishContractTests.A_graceful_stop_sends_the_buffered_finishes`), none the Redis case. This change touches only Redis Lua, so they are not caused by it, but I did not run the unchanged tree to prove they fail there too.

### Not done

- Chunk setup cost (about 100 µs per call) and the intent write-twice cost are the two remaining large items; both need work shared with the claim and finish scripts or a recovery-model change.
- No quiet-host run. No AOF bytes comparison was made: the stored commands are the same, only their order of construction changed, but the byte count was not measured.
- Grouped dispatch (`--redis-group`) end to end was not re-run; only its script time was (table above).

### Tools

`eng/bench-compare/redis-group/profile.py` gained `DEEP=1` (finer phase marks) and `LUA_DIR=<path>` (profile another checkout's scripts back to back). `pairs.sh` gained an arm `base` that runs a workload binary given by `BASE_DLL`.

## Redis worker path: where the time goes and what was cut (work area F follow-up)

Written 2026-11-15. Informational. Every timing below was taken on the shared host while other agents' suites and the SP-029 matrix were running (load average 10 to 22 on 8 cores). Runs were alternated inside each round and held the shared bench lock, so each trio saw the same neighbours, but the numbers are still noisy. Script CPU numbers use `os.clock` (the script's own CPU time), so host load does not distort them.

### Short answer

The worker-side Lua scripts now cost about 40% less CPU per job, and drained throughput rose from 136 to 180 jobs/s (1.32×) on the host disk and from 155 to 248 jobs/s (1.6×) with the log kept in memory. That is not close to the targets. BullMQ drained 697 and 843 jobs/s in the same rounds, so Runwick is at 0.26× and 0.29× of it. The ≥90% throughput target is **not met**, and the ≤10% p95/p99 target is **not met** (steady 500 and delayed 100 are both still far off).

Closing the rest means making a drained job cost roughly five times less in Redis (about 1.1 ms now against about 0.2 ms for BullMQ). That needs a protocol change to what each job writes, not more script tuning. Details are in "What is left".

### Where the time went (one drained job, claim plus finish)

Script CPU per job, microseconds, `profile_worker.py`, 40 calls per size, median:

| Batch size | Claim before | Claim after | Finish before | Finish after |
|---|---|---|---|---|
| 1 | 1,953 | 1,154 | 1,156 | 837 |
| 8 | 1,244 | 696 | 730 | 421 |
| 16 | 1,320 | 591 | 806 | 446 |

At batch 8 a job costs 1,974 µs before and 1,117 µs after (43% less). Redis's own time per script call, from `INFO commandstats`, went from 14,245 µs to 6,256 µs per call over a whole drain (this includes the bulk insert calls, the read-only probes and the AOF write).

Where the claim cost sat before (single claim, µs): reading the application and deciding about 340, events 200, building the write list 280, packing the stored intent 430, writing the guard 40, applying the writes 270. Encoding the three copies of the result and sorting object keys with a byte-by-byte comparator written in Lua sat inside those steps; the changes below target them, and the after-numbers are the measure.

### What was changed (no stored data or reply changes)

All of it is in `src/Runwick.Redis/Lua` and was checked with a differential run (`diffcheck.py`): the same fixed dispatch, claim, finish-many and finish sequence goes through the old and the new scripts and every stored key is compared (36 keys, timestamps and the time-derived part of attempt IDs masked). Result: 0 differences. Replies and receipts are unchanged.

1. **Encode each result once.** The receipt, the stored intent and the final reply all carried the same JSON, which was built three times. It is now built once and reused (`U.rjson`, and `finish_many.lua`).
2. **Native byte-order sort: tried, then dropped.** The first version checked 13 byte pairs and used Lua's own `<` when they matched the C locale. A probe of sample pairs cannot prove a whole collation, and it broke rule REDIS-11 (no script orders text with Lua `<`), so it was removed on merge. The dispatch diet's cheaper bytewise comparator is kept instead. The sort-related part of the measured gain below therefore does not carry over.
3. **A quicker JSON string writer** (no `gsub` when nothing needs escaping) and a shorter path in the encoder.
4. **Event rows written directly** in key order, instead of building a table and sorting its keys for every event.
5. **Begin reuses the reserved job inside `ReserveAndBegin`**, instead of reading the job hash again after Reserve just built it.
6. **Wake hints.** After an intent is whole, the script publishes the queue name on `<prefix>wake` for each due-now job it made ready (a plain `PUBLISH`, not part of the intent). `RedisStore` now implements `IWakeNotificationSource` for the work channel, so an idle worker usually starts within milliseconds of a due-now dispatch. Redis keeps no message for a listener that was not there, so polling stays the way correctness is guaranteed, as before.
7. **Idle sleep until the next due job.** The readiness probe now returns `NextDueAt`, the earliest job in the probed lanes that is not due yet, and an idle pool sleeps until then (never under the 50 ms probe spacing) instead of its idle back-off. PostgreSQL and SQLite leave it empty, so nothing changes for them.

Spec and public page: `docs/spec/22-redis.md`, `docs/spec/05-worker-engine.md`, `docs/Runwick.Docs/content/docs/redis.mdx`.

### Measured and left alone

- **Claim size and finish batching.** Both already batch up to 32. Batch size changes cost per job only a little (table above, 8 against 16), so a larger claim does not help; the fixed part per call is small now.
- **Claim plus begin in one call.** It already exists (`ReserveAndBegin`).
- **Round trips per job.** About 0.17 claim calls per drained job (3,455 for 20,000) and about 0.22 finish and probe calls (about 4,500), so the call count is not the problem.
- **Fsync.** On this host's disk one `appendfsync always` write takes about 7 to 10 ms, and many clients share one fsync (about 4,100 SETs/s with 50 clients, 143/s with one). Putting the log on tmpfs raised drain throughput only from 180 to 248 jobs/s, so fsync is a real cost but not the main one.
- **A hand-built reply for the claim result** (skipping the table that is then encoded) would save perhaps another 10% of script CPU but couples the script to the reply layout; not done.

### Drain of 20,000 jobs, 16 workers (three alternating rounds each, jobs/s)

| Log on | Runwick before | Runwick after | BullMQ 6.7.2 | After ÷ before | After ÷ BullMQ |
|---|---|---|---|---|---|
| host disk, fsync always | 136 (146, 134, 136) | 180 (180, 220, 171) | 697 (769, 546, 697) | 1.32× | 0.26× |
| tmpfs, fsync always (no disk wait) | 155 (155, 172, 152) | 248 (246, 289, 248) | 843 (809, 971, 843) | 1.60× | 0.29× |

Every "after" round beat its paired "before" round. The stored AOF size per run was the same before and after (about 354 MB, 17.7 KB per job) against 56 MB (2.8 KB per job) for BullMQ.

By an estimate from the run's command statistics, Redis was about 40% busy during the drain in the tmpfs "after" runs (about 25 s of script time in a 65 s drain), so the 16 workers are mostly waiting on claim and finish calls to come back rather than Redis being saturated. This is an inference, not a separate measurement: with 16 jobs in flight and a claim plus a finish taking tens of milliseconds each, the cycle time bounds throughput. BullMQ's scripts cost about 200 µs per call and its commands are tiny, so its calls return quickly and 16 workers keep Redis busy.

### Steady 500 offers/s and delayed 100 offers/s

Two and three alternating rounds, 16 callers and 16 workers. These are the noisiest numbers here; during the steady runs the host load average was above 20, and the "before" arm itself ran at 72 to 96 jobs/s, below the 160 jobs/s of the earlier report.

- **Steady 500.** Completions: before 96 and 73 (median 84), after 84 and 107 (median 96). Due-to-handler p99: before 35.8 s and 47.2 s, after 54.5 s and 30.4 s. BullMQ refused 5,900 to 7,700 of 15,000 locally and its offer-to-finish p99 was 1.0 to 1.2 s. No usable gain can be claimed; both Runwick arms are overloaded and far behind BullMQ.
- **Delayed 100, 5 s ahead.** Completions per second: before 81 (81, 85, 75), after 85 (85, 86, 61). Due-to-handler p99: before 4.95 s, 2.24 s, 5.94 s; after 1.94 s, 5.55 s, 7.37 s. The pickup lag spread is wider than any difference between arms, and round 3 of the "after" arm ran while the host was also running other suites (it fell behind its own offer rate and its enqueue p99 was 1.6 s). The mechanism (an idle pool now sleeps to the next due time) is tested and works, but this host could not show a stable gain in the lag, so none is claimed. BullMQ's offer-to-finish p99 was 0.6 to 0.7 s (this excludes the 5 s delay, which Runwick's "latencyMs" includes), so the comparison there is not like for like; the due-to-handler stage is the fair one.

### Tests

- New: `RedisWorkerPathTests` (2: the probe names the earliest not-yet-due job, and only a due-now dispatch publishes a wake) and one Core case (`A_store_that_names_the_next_due_job_ends_the_idle_wait_at_that_time`). The scripted store gained a `NextDueIn` switch.
- Results for the full Redis, Core and Contract projects are in the final message of this task and in `artifacts/perf/round-two/redis-worker-diet/tests-*.log`.

### What is left

- **Per-job script cost is still about five times BullMQ's.** What remains is the model, not the code: each command stores its after-images as a packed intent, then writes the same data again as it projects, and each job writes a receipt, an event and several index rows. That is why a job costs 17.7 KB of log against 2.8 KB. Cutting it needs the recovery model to change (for example intents only for commands that cannot finish inline, or an event stream that is not a hash plus a sorted set per event).
- **A claim that returns the next jobs together with the finish of the previous ones** (one call per settle) would halve the calls per job once scripts are cheaper. It changes `ReserveAndBegin` and `FinishMany` replies and the engine's slot logic, so it was not attempted here.
- **A quiet-host rerun** of steady 500 and delayed 100 is needed before any claim about the pickup lag.

### Evidence

Runner scripts, raw results and logs are in `artifacts/perf/round-two/redis-worker-diet/` in the worktree (git ignores artifacts); the profile, differential and paired-run scripts are tracked in `eng/bench-compare/redis-group/` (`profile_worker.py`, `diffcheck.py`, `pairs-worker.sh`, see its README).

## PostgreSQL claim reads its jobs by key (A/C and worker B recheck)

Written 2026-11-17. Informational: every timing was taken on the shared host while the SP-029 run and other test suites were running (load average 20 to 40 on 8 cores). Arms alternated inside each pair so both saw the same neighbours. The plan comparisons and the read counts do not depend on load.

**Short answer.** The recheck against retained data found a real cost in the worker's claim that the earlier empty-table runs could not see. When a backlog arrives after the table statistics were last taken, the batched claim reads the whole ready backlog twice per call. That is fixed by reading and updating the claimed jobs by their key. In five paired drains on a copy of a 205,000-job retained database the fixed claim drained 1.46 times faster (median, every pair at least 1.23). The change is a new migration (`0025_claim_keyed`, schema revision 25), kept on its branch for review.

### What the nested plans showed

Setup: a PostgreSQL 18.7 server of its own (fsync, synchronous_commit and full_page_writes on), a database holding 205,000 succeeded `bench` jobs (the source drain's 5,000 real jobs, cloned with new identities and times spread over 30 days, then `ANALYZE`), and a copy of it for every run. Each run drained 5,000 new 512-byte jobs with 16 workers while `auto_explain` logged 2% of statements, nested ones included, with buffers and WAL. The claim function's set-based statement (`rw_claim_plain_v1`, inside `reserve_and_begin_v2`) was the only statement reading many rows:

| | Baseline (`v1`) | Keyed (`v2`) |
|---|---:|---:|
| Sampled claim statements | 15 | 19 |
| Mean statement time (loaded host) | 39.1 ms | 13.7 ms |
| Mean shared buffers hit per statement | 2,764 | 403 |
| Scans of the ready index (`jobs_ready`) | 30, up to 4,918 rows each | none |
| One sampled plan | 49.9 ms | 3.6 ms |

Why: the statistics were taken while no job was waiting, so the planner expected one ready job. Its plan scanned the ready index (every waiting job, 4,918 rows near the start of the drain) and, for each, looked in the claim's job list. The claim runs that scan once to read the jobs and again to update them. So a claim cost grows with the backlog, and a retained, bursty store (statistics rarely refreshed, big backlogs) pays most. The earlier A6 and C1 runs used small, fresh tables where the two plans cost almost the same.

The other statements in the claim and finish paths read their jobs by key in this fixture. The attempt-close usage update (the A4 candidate's target) took 4.6 ms and 207 buffers on average over 6 sampled calls, so it is not the first change to make. The finish-path plans showed no application-wide scans on retained data. The earlier `jobs_name_list` scans came from small, stale fixtures.

### The change

`rw_claim_plain_v2` is `rw_claim_plain_v1` with two changes to how rows are reached, none to what is written:

1. The claimed jobs are read through a lateral lookup by (application, job ID) with `OFFSET 0`, and the state test is applied to what it returns. This keeps the ready index out of the plan.
2. The jobs update drops its state test. The row was read in the same statement as pending or retrying, the candidate claim holds its lock, and every change to a job raises its revision, so `revision = s.revision` already means the state is unchanged. A row that no longer matches is not updated, the count check fails and the whole command rolls back, as before.

`reserve_and_begin_v3` is `reserve_and_begin_v2` calling it, with the correlated payload lookup from the earlier C1 candidate (`OFFSET 0` on the creation-time lookup). Released functions stay in place and the worker now calls `v3`. No table or index changes. Spec 04 has the dated change text.

### Tests

- PostgreSQL project: 400 of 400 passed before the final test was added (24 minutes), including the migration kill-and-resume cases with `0025_claim_keyed` expand, validate and activate, the manifest at revision 25, the set-based versus per-job claim equality test, the lost-reply and fault tests, and the plan tests.
- New `A_claim_reaches_its_jobs_by_key_even_when_statistics_have_not_seen_the_backlog` (RAB-12): statistics taken on an empty table, 1,500 jobs dispatched, one claim of eight in a rolled-back transaction. The keyed claim fetched fewer than 300 job rows through indexes (passes). The same test pointed at `reserve_and_begin_v2` fetched 3,024 and failed, as expected.
- Contract project: 491 of 491 passed (20 minutes), including the batched-claim, fallback and worker contracts that hide `reserve_and_begin_v3`.

### Paired drains

Each pair: a fresh copy of the 205,000-job template per arm (both arms start from the same data), 5,000 jobs, 16 workers, order alternating, every arm reconciled all 5,000 jobs with no diagnostics. Baseline is the branch point; candidate adds the migration. WAL is the server's growth after the copy, so it includes the checkpoint after the copy.

| Pair | Baseline jobs/s | Keyed jobs/s | Ratio | Baseline WAL MB | Keyed WAL MB |
|---:|---:|---:|---:|---:|---:|
| 1 | 49.2 | 61.4 | 1.25 | 189.4 | 178.1 |
| 2 | 56.5 | 69.5 | 1.23 | 183.2 | 172.9 |
| 3 | 59.6 | 129.2 | 2.17 | 182.3 | 151.2 |
| 4 | 80.8 | 118.3 | 1.46 | 169.6 | 152.4 |
| 5 | 62.7 | 115.2 | 1.84 | 179.1 | 155.6 |
| Median | 59.6 | 115.2 | 1.46 | 182.3 | 155.6 |

Control on a fresh, empty database (three pairs, no retained data): median 77.9 against 108.3 jobs/s, ratios 1.25, 1.15 and 1.57, WAL equal (63.7 against 63.0 MB). The fixed claim is not slower on a small table either.

### Reading and limits

- The gain should grow with the backlog and with retained data, and shrink when the statistics are fresh. Both arms here start from statistics that show no ready jobs, which is how a store looks right after a burst of enqueues. The runs do not show how often a production store is in that state. Autovacuum may refresh statistics during a long backlog. A quiet-host rerun with a 100,000-job backlog would size the effect.
- Absolute rates are low because the host was loaded. Compare ratios, not rates. The per-pair spread (1.23 to 2.17) is the host's noise.
- The WAL difference is probably the shorter run with fewer checkpoint full-page images, not a change in what the claim writes. The statement's own WAL per sampled call was similar.
- The A4 attempt-close candidate (usage written in the same update as the close) was not rebuilt on the current schema. Its earlier result was 2.8% less WAL with a throughput change inside the noise (median +1.0%), and the plans above show the usage update is small. It stays isolated.
- Not done: index narrowing on `jobs` (every update inserts into the six always-on indexes and the partial ones; the plans did not make any one of them a clear first drop), replay-proof-gated receipt compaction, combined finish and claim, finish batching.

Raw results: `artifacts/perf/round-two/pg-keyed-claim/` (retained plans, paired runs, control, scripts: `plans.sh`, `pairs.sh`, `seed.py`, `summ.py`, `summarize.py`). The `--db-reuse` bench flag runs a bench against an existing database (a copy of a template).

## Redis Sentinel write authority and the Store page topology (work areas R and M)

Written 2026-11-17. Scope of this change: **Sentinel only**. Redis Cluster routing, declared keys and per-node management are not built. Everything below ran on the shared host, with containers on the host network (one primary, two replicas, three Sentinels per test).

**What it does.** An opt-in mode, `RedisOptions.Sentinel` (default off, nothing changes for a standalone primary), lets a Runwick process follow a Sentinel-managed primary under a stricter rule than "reconnect and hope": the process sends a command only while it holds a verified, unexpired authority, and otherwise sends nothing.

- **Verification** (every second by default, on its own connection to the candidate): a majority of all configured Sentinels name the same primary with health flags exactly `master`; the candidate passes the existing settings check (`appendfsync always` and the rest); if it is a different node from the last verified one, its replication history must descend from the verified one and cover its offset; the existing store-identity and revision check passes. Only then do the work, control and admin connections move to it and a lease of four seconds (default) start.
- **Closing authority.** Evidence closes it at once: a Sentinel reporting `s_down`, `o_down` or a failover, two Sentinels naming different primaries, a different primary named, a lost connection to the bound primary. Silence closes it when the lease ends: a process cut off from every Sentinel stops sending after at most the lease. That is the fence for a former primary that stays reachable from this process.
- **Blocked.** If a promoted node does not continue the verified history, or the store identity or revision went backwards, authority is blocked: every call fails as incompatible (`authority_continuity_unverified`, `store_identity_mismatch`, `history_discontinuity`) until an operator reconciles the histories and restarts the process. Operator intervention is "reconcile and restart"; there is no in-product override.
- **Cost of a verification.** The Sentinels are asked two commands each; the bound admin connection is reused while the same node stays primary, with the full settings check (five `CONFIG GET`, `INFO everything`, `MODULE LIST`) every `ProfileRecheckInterval` or after a lost connection, and `INFO replication` in between. A new candidate gets a connection of its own and the full check.
- **Stopped calls are never sent**, fail as `store_authority_unverified` (not applied) and keep their command identity with the caller. A call in flight when the primary fails has the ordinary outcome unknown.
- Profile `redis-aof-always-sentinel-v1`, acknowledgement profile `redis.aof-always-sentinel-v1`, outbox profile `redis.aof-always-sentinel.v1`. The Sentinel mode needs `appendfsync always`.

**Store page.** The Redis diagnostics card gains "Write authority (Sentinel)": state (verified, unresolved, blocked), reason, primary address, verification epoch, how many Sentinels agree, last verification time and the recent changes this process saw (bounded to 16). The common Topology row now shows a line such as "Sentinel-managed primary 127.0.0.1:36939: authority verified" instead of only a status. For a standalone primary the section is hidden (status Unsupported). These are this process's observations, not a cluster-wide verdict. The committed dashboard bundle was rebuilt.

**Tests** (`RedisSentinelTests`, real containers; one primary, two replicas, three Sentinels).

| Case | Result |
|---|---|
| Host verifies the primary before writing; carries the Sentinel profile and the sentinel acknowledgement profile in its receipt | passes |
| Primary killed after the replicas caught up: writes are refused with `store_authority_unverified` until the promoted node is verified, then resume under a higher epoch; all 30 earlier jobs still resolve Confirmed; the returning old primary becomes a replica and the host keeps the new primary | passes |
| Replicas pointed away from the primary, 20 more writes acknowledged and verified, primary killed, a replica promoted: authority blocks with `authority_continuity_unverified`, a dispatch fails with that reason, nothing reaches the new primary | passes |
| All three Sentinels paused: after the lease no dispatch is sent (the primary's key count does not move); when they return, writes resume on the same primary | passes |
| No reachable Sentinel: startup is refused with `sentinel_majority_unreachable` | passes |
| Configuration: Sentinel strings need endpoints and a service name; standalone still refuses them; `everysec` and an invalid lease are refused | passes (3) |

**Recovery time.** In two runs of the failover case, writes resumed 4.5 s and 6.4 s after the primary was killed, with 15 and 17 refused attempts in between (polled every 250 ms). The Sentinels were set to `down-after-milliseconds 3000`, the host to a 2 s lease and a 0.5 s recheck, so most of that time is the Sentinels' own detection and election.

Full numbers (counts of the whole Redis project and others) are in the final message of this task. An earlier version of the lossy-promotion case paused the replica containers; it did not make them miss writes, because a paused container's socket still receives and buffers the replication stream, so the promoted node had everything and the host (correctly) resumed. The case now sends `REPLICAOF 127.0.0.1 1` to both replicas, which is a real loss.

**What this does not claim, and what is not done.**

- Replication is asynchronous and `WAIT`/`WAITAOF` are not used. A failover can lose writes acknowledged after the last verification, up to one recheck interval plus the lease. The continuity check proves the promoted node had everything this process saw at its last verification, nothing more.
- Fencing is on the client side. A program that is not a Runwick process, or a Runwick process that ignores its lease, can still write to a partitioned former primary.
- Test coverage is the five failure cases above. Not exercised: Sentinel and data-node TLS and ACL failures separately, a Sentinel that disagrees for a long time, failover during a pending intent or in the middle of a claim or finish (the existing intent recovery should resume, but it was not injected here), AOF rewrite during failover, failback of the old primary while a process is mid-command, and many hosts at once.
- The authenticated overhead budget (5%) was not measured. In standalone mode the only added work on a command is a null check. In Sentinel mode it is one lock and a clock read per command, plus one verification a second (two Sentinel commands to each Sentinel and one connection setup to the primary).
- The migrator and the CLI take a plain primary address; they do not discover it through Sentinel.
- **Redis Cluster** is a separate opt-in mode, described in the next section. The R3 recovery proof remains a measurement outside the product code.
- No in-product operator action to certify a promotion; the only intervention is reconcile and restart.

## Redis Cluster write authority (work areas R and M, Cluster part)

Written 2026-11-17. Scope of this change: an **opt-in Cluster mode** (`RedisOptions.Cluster`) that gives Redis Cluster the same strict write authority the Sentinel mode has, plus the verified cluster topology on the Store page. Everything ran on the shared host, with throwaway containers on the host network (three primaries and one replica per test). No benchmark was run; the throughput effect of the mode was not measured.

### What was built

- **One application, one slot, one owner.** Application keys already share one hash tag (`rw:{<hex of the name>}:...`), so an application is one slot and every script runs on one node. The client now computes that slot (`RxClient.SlotOf`, CRC16 of the tag; a test compares it with `CLUSTER KEYSLOT` on a real node). Different applications usually land on different primaries and run independently.
- **Authority by slot owner.** The authority of the Sentinel mode is reused: the same lease, states (verified, unresolved, blocked), refusal codes (`store_authority_unverified`, `authority_continuity_unverified`) and diagnostics. Only the voting differs. Each listed node answers `CLUSTER NODES` with its own view; a majority of the listed nodes must name the same primary as owner of the slot, with nobody reporting the slot as migrating or importing and the owner not failing. Silence only stops the lease from being renewed.
- **Two kinds of change of owner.** If every voter still lists the old owner as a healthy primary, the slot was moved by migration and the history check (store identity, no application revision lower than one already seen) is the continuity proof. If the old owner failed or was demoted, the new owner is a promoted replica and must continue the verified replication history, as in the Sentinel mode; a replica that was behind blocks all writes.
- **Routing and recovery.** Scripts are sent by key, so StackExchange.Redis routes to the owner and follows `MOVED` and `ASK`. A `MOVED` reply closes authority at once; a lost connection to the owner closes it (other nodes' sockets are ignored). `NOSCRIPT` is answered by sending the script text by key, so the script loads on the owner (an unkeyed `SCRIPT LOAD` would go to any node). Profile reads (`INFO`, `CONFIG GET`, `MODULE LIST`) are now sent to the exact node. In a cluster the history check reads each application's own meta, not `rw:store`, which may live on another node.
- **Profile.** `redis-aof-always-cluster-v1` (Describe `topology=cluster-primary`, `redis_mode=cluster`, `cluster_enabled=1`), acknowledgement profile `redis.aof-always-cluster-v1`, outbox profile `redis.aof-always-cluster.v1`. Cluster cannot be combined with Sentinel or with `everysec`.
- **Migrator.** `RunwickRedisMigrations.MigrateClusterAsync` checks every primary against the profile, refuses a node that holds `rw:*` keys but no store record, and writes `rw:store` once.
- **Store page.** The authority section now says "Write authority (Cluster)", shows the application's slot, the slot owner, how many listed nodes agree, and one line per node the voters report (address, role, health, slot count, whether it owns this application, whether the slot is moving). The committed dashboard bundle was rebuilt.
- **Docs.** Spec 22 §2.6, rule REDIS-18, the public Redis page, the store and installation pages and the topology guide; the generated search index and `llms-full.txt` were regenerated.

### Tests (`RedisClusterTests`, 15 cases, all pass; 13 m 17 s)

| Case | What it shows |
|---|---|
| Host verifies the owner of its slot | The owner is the node holding the slot, the profile and topology facts are the cluster ones, four nodes are reported with exactly one owner, the receipt carries `redis.aof-always-cluster-v1`, the keys sit on the owner and on no other primary |
| Two applications on two primaries | Slots on different primaries; each application's keys sit on its own owner only; pausing one owner closes that application's authority while the other keeps writing (the test cluster runs `cluster-require-full-coverage no`; with the default `yes` the second application also stopped, because every node refuses all slots until the replica of the failed owner is promoted. The first full run of the whole project caught this: the test had passed once only because it ran before the failure was detected) |
| Forged key | Five forged meta keys are refused before any write; the key count over all three primaries does not change |
| Slot move | Importing/migrating closes authority (`slot_owner_failing_or_moving`), the call is refused with `store_authority_unverified` and the old owner's key count does not move; after the keys are transferred and ownership changes, authority resumes on the new owner under a higher epoch and the 20 earlier jobs all resolve as confirmed |
| Failover that continues the history | The owner is killed after its replica caught up; writes are refused (`store_authority_unverified` seen), then resume on the promoted replica under a higher epoch with all 30 earlier jobs confirmed |
| Failover that does not continue the history | The replica cannot follow (wrong `masterauth`), 20 more writes are acknowledged, the owner is killed; the host blocks with `authority_continuity_unverified`, refuses the next dispatch, stays blocked, and nothing reaches the promoted node |
| Majority unreachable | Two of three listed nodes paused: writes stop when the lease ends, the owner's key count does not move, and they resume when the nodes return |
| Migrator | The standalone migrator refuses a cluster node (`topology_unsupported`); the cluster migrator installs once and a second run changes nothing; a primary on `appendfsync everysec` stops it (`store_profile_mismatch`) |
| Parsing and options | A vote names the owner and is troubled when the owner is failing or the slot is migrating or importing; node lists parse; Cluster plus Sentinel, plus `everysec`, or a bad lease is refused at startup |

**Whole-suite results.** Full Redis project: 112 of 113 in 3 h 11 m on a host at load 45 to 55; the one failure was the two-applications case above (fixed afterwards), and the cluster class then passed 15 of 15 (5 m 45 s). Redis-kind Contract cases: 162 of 164 (24 m); the two failures were `RateScalingContractTests` cases that passed 14 of 14 when rerun alone, so I treat them as load-sensitive. Dashboard bundle and Store snapshot tests: 8 of 8.

### What is not done

- **Scripts do not declare every key.** They derive keys from the application's prefix and declare only the meta key. This is safe while the slot is stable. If a slot starts moving while a script runs, keys that already left the node are not local and the script can fail partway; this is handled as an ordinary unknown outcome (the pending intent is finished on the new owner, as R3 showed), not prevented. A full declared-key discipline for all 48 operations (R3's design) is not built.
- **The first command after a move can reach the new owner before the move is noticed** (the client follows `MOVED`, then closes authority). Fencing of the old owner is by lease only, as in the Sentinel mode: other programs writing to a cut-off former owner are not stopped.
- **Loss detection is partial**, as in the Sentinel mode: replication is asynchronous, `WAIT` is not used, and the continuity check proves the promoted node had what this process saw at its last verification, not that nothing was lost. A migrated slot is only checked through store identity and revisions.
- **Not cluster-wide:** listing applications, the management scans (`rw:{*}:meta`) and the command-line tools read one node. `rw:store` and the migration lock live on one slot's owner, which must be reachable at startup.
- **Not injected:** TLS and ACL failures, failover while a pending intent or a claim or finish is in flight, slot migration while load runs, an AOF rewrite during failover, replicas of the other primaries (only one primary had a replica), cluster resharding with `ASK` under load.
- **Not asserted by a test:** that a `MOVED` reply closes authority (`hash_slot_moved`); the slot-move test shows the closure through the voters' view (`slot_owner_failing_or_moving`), which comes first.
- **Not measured:** the authenticated overhead against the 5% budget, and throughput of one application on a cluster against a standalone primary. The mode adds a lock and a clock read per command (as the Sentinel mode does) and one `CLUSTER NODES` call per listed node per recheck interval.

## Outbox relay at four slots: batched marks, claim batches and slot release (D2)

Written 2026-11-17. Informational: every number was measured on the shared host with other sessions' test suites running (load average 15 to 70 on 8 cores), so absolute rates are a fraction of what a quiet host gives. The writer could not even stage the offered 100 messages/s in most runs. Read the pairs as direction, not as a pass or fail of P-09.

### Where the time goes at four slots

A message's life at four slots is: claim, destination commit, source mark. Little's law then gives the ceiling: throughput is at most the slots divided by that life. A traced run on the unchanged code (host load about 44, so the times are inflated) showed:

| Step | Calls | Mean | Notes |
|---|---:|---:|---|
| Source claim | 662 | 88.5 ms | 1 to 2 messages per claim: a claim fires each time a slot frees |
| Destination `ReceiveOutbox` | 757 | 281 ms | of which commit 196 ms |
| Source delivered mark | 756 | 83 ms | 26 ms of it waiting for the control connection, 57 ms the statement and commit |
| Lease renewal | 18 | 89 ms | waited 123 ms behind marks for the same connection |

Two things stand out. Every mark, renewal and transport record shares one source connection, so marks queue behind each other. And because a claim fires as soon as one slot frees, a claim carries about one message, so the claim connection is busy with a call per delivery. In the earlier D2 sixteen-slot pairs the same effect was hidden by the extra slots.

### What changed (all opt-in, defaults unchanged)

- **`OutboxRelayOptions.BatchDeliveredMarks`.** On the PostgreSQL source, delivered marks that queue behind each other are written in one `UPDATE … FROM unnest(…)` and one commit (up to 64). Each row is still guarded by its own lease token, so a stale token leaves that row alone and reports false for that message only. A lone mark runs the original single-row statement. A caller that gives up before its mark is taken writes nothing; a caller whose deadline ends while it holds the connection leaves its own mark unknown, as for a single mark, and the others wait for the next turn.
- **`ClaimBatchMin` and `ClaimBatchWait`.** With messages in flight, the claim waits until that many slots are free or the wait (5 ms by default) ends, so one claim carries a batch.
- **`ReleaseSlotAfterReply`.** A message stops counting against `MaxInFlight` once the destination has answered, so the next claim and send overlap its mark. Its lease is still held and renewed until the mark is written, shutdown still waits, and at most twice `MaxInFlight` messages hold leases.

Transport identity, receipts, the token-guarded mark, mixed outcomes, lost marks and reclaims are unchanged.

### A bug the measurement found

The first version of the claim wait recomputed the remaining time after the loop test. When the wait expired between the two, the call received a negative timeout, threw, and ended the claim loop for good. One run delivered for 71 seconds and then nothing. The wait is now computed once and the loop breaks when no time remains; a test with a 1 ms wait delivers 40 messages. The affected run is excluded below and was repeated.

### Paired runs

P-09 at four slots, 100 offers/s, 1 minute of warm-up, 3 minutes of measurement, no mark loss, two minutes of drain (the backlog never drained in any arm, so ready-to-confirm latencies are minutes long and mean nothing here). Arms rotated each round. **base** is the current code; **full** is batched marks, claim batches of 2, slot release and destination groups of 4; **marks** is batched marks plus slot release only. Confirmed deliveries per second in the measurement window:

| Round | base | full | full ÷ base | marks | marks ÷ base |
|---|---:|---:|---:|---:|---:|
| 1 | 6.74 | 8.25 | 1.22 | 8.78 | 1.30 |
| 2 | 8.23 | 10.74 | 1.30 | 11.26 | 1.37 |
| 3 | 9.46 | 15.55 | 1.64 | 7.03 | 0.74 |
| 4 | 13.16 | 18.38 | 1.40 | | |
| 5 | 15.82 | 20.06 | 1.27 | | |
| 6 | 13.58 | 17.19 | 1.27 | | |
| 7 | 15.06 | 17.36 | 1.15 | | |
| 8 | 21.77 | 13.39 | 0.62 | | |

Across the eight valid pairs the median ratio for **full** is 1.27 (range 0.62 to 1.64); it won seven of eight. The marks-only arm is mixed (1.30, 1.37, 0.74) on three pairs. The relay made about 40% fewer source claims with **full** (median 2,850 against 4,720 over all runs), and 2,300 to 3,500 marks went out in 840 to 1,260 statements (2.6 to 2.8 per statement). No run left a second mapping for any message and none recorded a correctness violation.

The one pair where **full** lost (round 8) had the writer stage 64 messages/s against 103/s for its base, so the two arms did not see the same load.

### Reading

- Batching helps modestly. About a quarter more deliveries per second at the median is what the pairs support. It does not get anywhere near 100 deliveries/s at four slots on this host, where even the base arm only reached 7 to 22.
- By the slots-over-life arithmetic, four slots cannot reach 100/s unless a message's whole life is under 40 ms. A claim and a destination commit alone are about 25 plus 35 ms on a quiet host. Meeting P-09 at four slots needs the next claim to overlap the current send, which `ReleaseSlotAfterReply` starts to allow, or more slots. The earlier slot sweep reached 100/s at eight slots.
- P-09 at four slots is **not met** and was not expected to be on this host.

### Tests

New `BatchedMarkTests` (4): queued marks share statements and keep each lease token (11 of 12 applied, one stale token left leased, then settled by its owner); a lone mark uses the single-row statement and a cancelled caller writes nothing; a 1 ms claim wait never ends the claim loop; a relay with claim batches, destination groups, slot release and batched marks delivers 24 messages exactly once and every slot is counted back. Outbox project 62 of 63 on the branch (the one failure, `GroupedDeliveryTests.A_lost_group_reply…`, passed on reruns alone and asserts that a group formed, which depends on claim timing); the PostgreSQL and Contract projects were not run because there is no store or migration change.

### Not done

- A quiet-host repeat. The decision to make any of these the default needs it.
- Batching the SQLite source's marks, and batching renewals.
- Counting messages per claim directly (claims and marks per statement are inferred from counters).
- Measuring 8 and 16 slots with the new options.

Raw files: `artifacts/perf/round-two/outbox-d2/` in the worktree (gitignored); the pair scripts are `pairs.sh`, `pairs2.sh` and `pairs3.sh` beside them.

## Redis combined finish and claim (work area F follow-up)

Written 2026-11-18. Informational: measured on the shared host while other sessions ran test suites and benchmarks (load average 20 to 75 on 8 cores). Each round ran all its arms one after another under the shared benchmark lock, with the order rotating between rounds. The numbers show direction and orders of magnitude, not exact gains.

### Short answer

The option is built, tested and safe, but it does not close the gap to BullMQ. Across nine paired drain rounds the combined call changed throughput by between 0.76 and 1.33 times the baseline, with a median close to 1.0. BullMQ stayed 3 times (disk) to 6 times (memory log) faster than Runwick in the same rounds. The reason is that the pool's own claim loop is busy claiming most of the time, so a finishing worker can attach its claim in only about one case in four (see "Why it helps so little"). It stays an opt-in and is not a recommended setting yet.

### What it does

With `WorkerOptions.UseCombinedFinishClaim()` (off by default), a worker that is about to send the finishes of attempts that just ended can add to the same Redis call the claim of that many new jobs. The finished attempts' slots are the claim's slots, so the next job starts without waiting for a separate round trip. Only Redis supports it; with any other store the option does nothing and the worker finishes and claims separately as before.

- **One script call, one intent.** The new script `finish_and_claim.lua` runs the finish items of `FinishMany` and then the claim of `ReserveAndBegin`, inside one script call and one stored intent, so both halves apply or neither does. A permit that a finish frees is visible to the claim in the same call. The shared decision code moved into `jobs.lua`, and `finish_many.lua` and `reserve_and_begin.lua` now call it (their behaviour is unchanged; the existing tests pass).
- **Every command keeps its own receipt.** Each finish has its own durable `Finish` receipt. The claim is framed as the ordinary `ReserveAndBegin` command, so its receipt is an ordinary claim receipt holding only the claim result; sending the same claim command alone later replays it. When no job is claimed (no work, paused queue, capacity blocked) the finishes still apply, the reply carries the claim's status, and no claim receipt is written, as for any claim that did nothing.
- **No stored layout change.** No migration, no new key, no new field. Older workers and the dashboard read the same data.
- **Engine side.** The completion worker takes the finishes of one pool that already wait in the ready channel. It asks the pool for a claim of that many jobs (`TryBeginInlineClaim`). The pool agrees only when the loop itself would batch the same way right now: admission open, not draining, not suspended, the readiness hint is fresh (no probe is started), exactly one lane is claimable and it is not an aging pick, and no claim of that pool is in flight (the call takes the pool's claim gate without waiting). Otherwise the finishes go the ordinary way. An Applied claim moves each used slot to the new attempt before any finish in the call completes, so the finisher's later release of that slot does nothing.
- **Failure.** A refused call (`UnsupportedVersion`, `InvalidRequest`) turns the option off for that worker and every finish resolves alone. A failed or lost call resolves each finish by its own command and the claim by sending the same claim command again, which replays if the call committed and applies if it did not. A replayed claim carries no finish results; each finish then resolves from its own receipt.

Spec: 03 (Changes 2026-11-17), 05 §12.7, 22 (Changes). Public docs: configuration page.

### Tests

| Run | Result |
|---|---|
| `RedisFinishAndClaimTests` (store level, real Redis, 7) | 7 of 7 |
| `FinishAndClaimContractTests` (worker level: SQLite, PostgreSQL, Redis, a refused call, a lost reply; 5) | 5 of 5 |
| Full Redis project | 105 of 105 (2 h 57 m under load) |
| Core | 453 of 453 |
| Contract, Redis kind (`RUNWICK_TEST_STORES=Redis`) | 168 passed; the 4 reported "No data found" are tests that use only SQLite or PostgreSQL, which the filter empties |

The store-level cases: finishes and claim decided together with their own receipts; a resent call replays the claim alone and each finish from its own receipt; a plain claim with the claim command's ID replays it; no work still finishes and leaves no claim receipt; a stale lease is refused alone while the claim applies; the claim sees the permit a finish in the same call frees (a resource with one permit is blocked before, claimed after); invalid calls change nothing; and every write prefix of the call from 1 to 40 (the Redis form of "crash between the halves") leaves each job finished once and each new job started once after the resend. The worker-level cases check that 80 jobs on every store settle exactly once with one finish receipt each (and that Redis really used the call), that a refused call falls back and finishes everything, and that a lost reply resolves without running anything twice. A rate-limited claim item goes through the same script function as a plain claim; no separate case was written for it.

### Measured (drain of 20,000 jobs, 1 KiB, 16 workers, one call per producer, fresh Redis 8.2 per run)

Median jobs per second; the ratio is the combined arm over the baseline in each round.

| Set | Baseline | Combined | BullMQ 6.7.2 | Combined ÷ baseline, per round | Combined ÷ BullMQ |
|---|---|---|---|---|---|
| Disk log, rounds 1 to 3 (load 3 to 59) | 110 | 99 | 332 | 1.11, 0.90, 0.76 | 0.30 |
| Memory log (fsync almost free), rounds 1 to 3 (load 8 to 51) | 189 | 212 | 1,251 | 1.33, 0.97, 1.02 | 0.17 |
| First disk set, run while my own test suite ran (load 30 to 75) | 28 | 58 | 157 | 2.42, 0.90, 1.95 | 0.37 |

A short extra set also ran buffered finish (`UseBufferedFinish`, G1) beside the combined option on a disk log, two rounds each: baseline 54, combined 63, buffered 68, both 73 jobs per second. Two rounds on a busy host cannot separate these.

The first set is shown for completeness; it ran while the host was heavily loaded by my own tests and swings by more than 2 times between rounds, so the other two sets are the ones to read. Raw results are in `eng/bench-compare/redis-group/results/` (drain, drain-r2, drain-tmpfs, drain-g1, drain-diag, drain-diag2) in the worktree and are not committed.

### Why it helps so little

A counter run (memory log, one round each) shows the combined call being used and also how rarely:

- Baseline: 3,161 claim calls plus 1,537 FinishMany calls for 20,000 jobs.
- Combined arm: 720 combined calls, 2,295 plain claim calls and 1,507 FinishMany calls. About a quarter of the claims (720 of 3,015) rode on a finish, but the combined calls themselves are calls, so the total fell only from about 4,700 to about 4,520 (single Finish calls not counted in either).
- The pool declined the combined attempt 3,930 times because its claim gate was taken (the loop was claiming), 17 times because the readiness hint was stale, and never because of the lane shape or a closed admission.

So the loop that fills free slots is busy claiming most of the time, and a finisher that finds it busy sends its finish alone as before. Where the call is used, it saves a claim round trip, but the claims that remain are still one call per batch, and a drained job still costs about five times BullMQ's script time (see the worker path section). The fsync wait is paid per call, and the call count fell by only a few percent.

### What is left

- **Let the pool carry the finishes instead of the finishers carrying the claim.** With buffered finish a finished attempt gives its slot back at once, so the loop has free slots while the finishes are still waiting in the channel. If the loop's claim took those finishes with it, nearly every claim would carry them. That is a larger change to the claim loop and to buffered finish and was not attempted here. The "both" arm above hints that the two modes may add up; two noisy rounds do not prove it.
- **Waiting for the claim gate** (a short bounded wait instead of declining) was considered and not built: it would make the finish wait behind the loop's claim, which lengthens the time a slot is occupied and is likely to lose what the saved call gains.
- **The cost per drained job** (about 1.1 ms of script time and about 17 KB of log against BullMQ's 0.2 ms and 2.8 KB) is untouched by this work and remains the main gap.
- A quiet-host repeat, a measurement with the option on steady and delayed workloads, and a power-loss comparison were not done.

### Tools

`eng/bench-compare/redis-group/pairs-combined.sh <workload> <rounds> <bench-bin-dir> [arms]` runs one Runwick bench build with and without the options (arms `base`, `comb`, `buf`, `both`) and BullMQ, alternating the order, with a fresh Redis 8.2 per run and the shared benchmark lock held for each round. `--combined-finish-claim` is the bench flag. The bench output now reports `finishesSent`, `finishManyCalls`, `combinedCalls`, `combinedClaimed`, `combinedFallbacks` and `inlineDeclined` (why the pool declined).

## Redis: the pool's claim carries the waiting finishes (work area F follow-up, not merged)

Written 2026-11-18. Informational. Measured on the shared host while other sessions ran (load average 17 to 48 on 8 cores). Arm order rotated each round; the numbers show direction, not exact gains.

### Short answer

Built, tested and safe, but it does not pay off. With buffered finish on, the pool's claim now carries the finishes that are waiting, about 55 % of all finishes on a disk log and 80 % on a memory log. Calls went down only a little, and on a disk log (the normal production setting) the worker got slower than buffered finish alone in all three rounds (0.70 to 0.83 times). On a memory log it came out ahead of buffered finish, but those rounds were noisier. BullMQ stayed 2.5 to 4.5 times faster. My recommendation is to keep this on its branch and not merge it.

### What was built

- **One switch, both directions.** `UseCombinedFinishClaim()` now means "whichever side is ready first carries the other". There is no new option, and the default is still off. A finisher that still holds its slot attaches a claim to its finish, as before. The new part: when the pool's claim loop is about to send a batched claim for its free slots, it first takes the finishes that are waiting and sends them and the claim in one `finish_and_claim` call. The waiting finishes are the batch a buffered-finish collector is filling, oldest first, then whatever is still in the ready channel.
- **Each finish is taken exactly once.** The collector's batch is now shared under the buffer lock, so the loop and the collector cannot both take the same finish. Finishes still in the channel are removed by the channel's own read. If the loop takes the collector's whole batch, the collector goes back to waiting. A test that stops the loop from removing what it takes fails at once.
- **No finish waits longer because of the loop.** The collector still flushes whatever is left at the flush interval, when its batch is full, at drain, or near a lease margin. The loop can only send finishes earlier.
- **Slots.** The claim fills the loop's own free slots, so no slot moves between the two halves of the call. Finishes the call decided complete at once. Any other finish is resolved by its own command in the background, so the loop does not wait for it.
- **Failure handling is the same as before.** A refused call turns the option off for the worker. Each finish then resolves alone, and the loop sends the same claim command alone. If a call's reply is lost, each finish resolves by its own command, and the claim is resent with the same command ID. That resend happens even if admission closed in the meantime, because the claim may already be committed.
- **No stored layout change, no script change, no migration.** PostgreSQL and SQLite are unaffected, because the call stays an optional interface.
- **Bench output** gains `loopCarriedCalls` and `loopCarriedFinishes`.

Spec: 05 Changes 2026-11-18, 22 Changes. Public docs: the configuration page, with the docs index regenerated.

### Tests

| Run | Result |
|---|---|
| Core (fake store), including 6 new `LoopCarriedFinishTests` | 459 of 459 |
| `RedisFinishAndClaimTests`, including new FC-08 (the claim's size does not depend on how many finishes ride with it) | 8 of 8 |
| `FinishAndClaimContractTests`, including new FC-13 (all stores, buffered and combined), FC-14 (lost reply of a carried claim) and FC-15 (refused carried claim) | 10 of 10 |
| Full Redis project | 120 of 121 in 45 minutes. The one failure was the Sentinel case "a promotion that does not cover the verified history"; its fixture timed out waiting for replicas to catch up, and the case passed when run alone. An earlier full run stalled after 44 passing tests with the test process idle, while cluster nodes were running; it was stopped. No Redis test turns on buffered or combined finish, so neither run touches the changed code paths. |

The new Core cases cover five behaviours:

- The loop's claim carries the waiting finishes.
- A 300-job race between the loop and the completion lane sends each finish command exactly once.
- A drain with nothing left to claim still sends the buffered finishes.
- A refused call turns the option off, and the same claim is then sent alone.
- A lost reply resolves each finish by its own command and replays the claim.

A sixth case checks that nothing changes when the option is off.

### Measured: drain of 20,000 jobs (1 KiB, 16 workers, fresh Redis 8.2 per run)

Throughput is in jobs per second. "Scripts per job" counts every Redis script call in the run, including the insert and the readiness probes, divided by 20,000. "Script time" is Redis's own script execution time per job.

**Disk log (appendfsync always on the host disk)**

| Round (load) | base | buf | both (new) | BullMQ | both ÷ buf |
|---|---|---|---|---|---|
| 1 (48 / 26 / 25 / 29) | 62 | 87 | 72 | 428 | 0.83 |
| 2 (31 / 17 / 21 / 39) | 72 | 108 | 83 | 268 | 0.77 |
| 3 (22 / 34 / 23 / 25) | 75 | 122 | 85 | 265 | 0.70 |
| Median | 72 | 108 | 83 | 268 | |

| Disk log, per arm (median) | base | buf | both |
|---|---|---|---|
| Scripts per job | 0.40 | 0.23 | 0.22 |
| Worker claim calls | 2,981 | 2,368 | 1,336 + 1,006 carried |
| FinishMany calls | 1,556 | 1,409 | 789 |
| Finishes carried by the loop | 0 | 0 | 11,444 (57 %) |
| Redis script time per job | 5.1 ms | 4.1 ms | 5.2 ms |

**Memory log (fsync almost free)**

| Round (load) | base | buf | both (new) | BullMQ | both ÷ buf |
|---|---|---|---|---|---|
| 1 (34 / 22 / 45 / 22) | 154 | 123 | 160 | 639 | 1.31 |
| 2 (22 / 29 / 32 / 28) | 193 | 128 | 158 | 992 | 1.23 |
| 3 (20 / 38 / 28 / 26) | 132 | 33 (outlier) | 143 | 723 | 4.3 (outlier) |
| Median | 154 | 123 | 158 | 723 | |

On a memory log, scripts per job were 0.42 for base, 0.22 for buf and 0.19 for both. The loop carried about 16,000 of the 20,000 finishes (80 %).

The code stays on branch `worktree-agent-a2560555b3f291e9a` (commits 8f846b3, 7f276e6, a4efa14); it is not merged. Raw results were left uncommitted in that worktree.

### Why it does not help

- **The mechanism works, but it saves few calls.** Against buffered finish alone, the worker's claim and finish calls fell by only about 15 % (round 1 on disk: 3,765 down to 3,200, finishes sent alone not counted), and all script calls by about 5 %. Buffered finish had already merged most finishes into FinishMany calls.
- **Fewer calls do not mean fewer disk syncs.** Redis syncs the log once per event-loop pass for every client that wrote in that pass. Separate claim and finish calls that arrive together already share one sync, so merging them saves little disk time.
- **The combined call costs more Redis time.** Script time per job rose from about 4.1 ms to 5.2 ms on disk, back to the level without buffered finish. The claim now waits for the finishes in its call, so new jobs start later and slots sit empty longer.
- **On a memory log** disk syncs cost nothing, so fewer calls is a small win. The new path beat buffered finish there, but buffered finish alone was slower than the baseline in those rounds, which points to noise. I do not read a gain from it.

### What is left

- Find out why `finish_and_claim` costs more script time than a FinishMany call plus a separate claim. That would need a profile of the script.
- The main gap to BullMQ is still the cost per job inside the scripts: about 4 to 5 ms of script time per job here against BullMQ's 0.5 to 0.8 ms. This change does not touch it.
- A quiet-host repeat, plus steady and delayed workloads, were not run.
