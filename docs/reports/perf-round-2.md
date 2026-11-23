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

### Sustained load at 100 jobs per second (2026-11-23)

The drains above measure how fast a backlog empties. This set measures what a user feels at a steady rate: each run offers 100 jobs per second for 300 seconds (about 30,000 jobs), base against buffered finish, three rounds per store. All 18 runs finished with nothing lost (completed equals inserted in every run). Built from main at 2e9f2c88. The host load ranged from 6 to 41, and runs queued behind other benchmarks on the shared lock, so the numbers are informational. Raw files: `artifacts/perf/round-two/g1-sustained/`.

Median of three rounds, range in brackets. Times are in milliseconds.

| Store, arm | Completions/s | Pickup p50 | Pickup p99 | Latency p99 | Refusals |
|---|---|---|---|---|---|
| SQLite base | 88.2 (79.5–96.7) | 9,114 (71–9,806) | 17,804 (2,623–38,741) | 18,031 (2,874–39,102) | 2,356 (908–3,651) |
| SQLite buffered | 95.5 (94.5–98.2) | 73 (58–92) | 909 (490–1,556) | 1,837 (1,285–2,125) | 1,321 (516–1,626) |
| PostgreSQL base | 94.9 (91.9–95.0) | 3,859 (1,360–4,595) | 23,941 (17,334–24,268) | 24,045 (17,427–24,365) | 378 (0–471) |
| PostgreSQL buffered | 99.8 (98.8–100) | 80 (55–143) | 3,571 (2,826–4,654) | 3,906 (3,156–4,865) | 33 (0–74) |
| Redis base | 100 (99.6–100) | 24 (23–24) | 795 (301–1,292) | 991 (481–1,439) | 0 (0–124) |
| Redis buffered | 99.9 (98.9–99.9) | 28 (20–28) | 1,111 (414–3,937) | 1,481 (664–4,218) | 15 (9–324) |

What it shows:

- **PostgreSQL: buffered finish is a clear win.** Without it the worker built a backlog in all three rounds (pickup p50 about 4 s); with it, it kept up in all three (p50 80 ms). Host load was similar for both arms.
- **SQLite: a big win in this data, with a caveat.** Base fell behind in two of three rounds (pickup p50 about 9 to 10 s, 26k to 27k of 30k done in the window); buffered stayed healthy in all three, including one round at load 41. SQLite buffered meets the pickup p99 under 2 s target in all three rounds, but refusals remain (516 to 1,626), so the "no refusals at 100 jobs/s" target is still not met. The longest writer-queue waits in both arms were background cleanup (up to 9.7 s) and presence writes (about 4 s), not job writes; dispatch waits stayed at 0.4 to 0.6 s.
- **Redis: no gain.** Redis keeps up either way. Buffered was slightly worse (latency p50 104 against 70 ms, and one round with a 4.2 s p99 at the quietest load), so it is not worth turning on for Redis at this rate.
- **Why it helps.** The base arm already batches finishes (about 7 to 11 finishes per call, against 6 to 9 buffered), so the gain is not more batching. It comes from giving the worker slot back as soon as the handler returns.

### Open

- The 16-worker drain pairs are too spread out to fix the size of the gain; a quiet host would help.
- SQLite background cleanup and presence writes wait seconds in the writer queue at 100 jobs/s; they are the next thing to look at for the SQLite tail.
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

## Redis: where the worker scripts spend their time, and what was cut (work area F follow-up)

Written 2026-11-18. Measured on the shared host. Unlike the earlier Redis reports, most of this ran while the host was nearly idle (load average 2 to 16 on 8 cores, recorded per run), which turned out to matter a lot. Every timed drain held the shared bench lock.

### Short answer

- **The "4 to 5 ms of script time per job" was mostly the busy host, not the scripts.** On a quiet host the same drain (20,000 jobs, 1 KiB, 16 workers, main as of 9ec2a34) costs about 1.3 to 1.7 ms of Redis script time per job, and that includes inserting the 20,000 jobs (about 0.2 ms per job). Redis's `INFO commandstats` time is wall-clock time inside Redis. When the host is overloaded, Redis's own thread is paused in the middle of scripts and runs slower when it does run, so the same work reads as about three times more. Redis's own CPU counter (`used_cpu_user_main_thread`) also went from about 29 s per drain on a quiet host to about 80 s on the loaded one.
- **The worker path (claim plus finish) costs about 1.1 ms per job on main. Almost all of it is Lua code, not Redis commands.** The Redis commands a script issues (about 16 per drained job) account for roughly 11% of script time. The rest goes on building and sorting JSON, sorting hash fields, keeping the in-script overlay, packing the stored intent, and running the decision itself.
- **Three changes cut worker-script CPU by about 20 to 27% per job, with byte-identical stored data and reply text.** In paired drains on a disk log, throughput rose 1.46 times without buffered finish (303 to 442 jobs/s, median) and 1.39 times with it (328 to 456). Script time per job fell from 1.68 to 1.17 ms and from 1.71 to 1.25 ms. BullMQ in the same rounds: 1,438 jobs/s and 0.29 ms per job. With the log kept in memory, the gain is smaller and noisier: 1.17 times without buffered finish and 1.31 times with it, over six rounds.
- **Why `finish_and_claim` looked dearer than FinishMany plus a claim:** (1) it encoded the claimed jobs' result twice, once for the claim receipt and once more for the reply that adds the finished list. That was about 1.1 ms per call at 8 jobs, about 12% of the call. (2) The 5.2 against 4.1 ms came from loaded-host rounds. On a quiet host the same two arms of that branch cost 1.49 and 1.40 ms per job (6% apart). Per job, the combined call cost the same as the two separate calls (1,187 against 1,178 µs): the second encode ate the saving from making one call instead of two. With the second encode removed, a combined call now costs 0 to 10% less per job than the two calls.

### What was measured

Tools, all in `eng/bench-compare/redis-group/`:

- `slowdrain.sh`: one real drain with Redis's slow log set to record every command, then Redis time per operation (`slowparse.py`).
- `mondrain.sh`: a 2,000-job drain under `MONITOR`, counting the Redis commands each script issues (`monparse.py`).
- `profile_worker.py`: CPU time inside the real scripts, by phase. It is CPU time (`os.clock`), so host load distorts it much less. New flags: `--combined` also times `finish_and_claim`, and `WRAPS=enc,sorted,...` also times chosen helpers.
- `perf record` on a Redis 8.2 build with symbols, to see which C functions the Lua work lands in.

**Per script on main, quiet host, buffered finish on (one drain, Redis time from the slow log):**

| Script | Calls per job | µs per call | µs per job | Redis commands per call (per job) | Biggest costs inside |
|---|---|---|---|---|---|
| ReserveAndBegin (claim, about 8.3 jobs per call) | 0.12 | 5,254 | 635 | 61 (7.2) | encoding the claim result for the receipt (about 130 µs per job), sorting each job hash's fields before writing (about 100), reserve and begin decisions (about 125), projection writes (about 40) |
| FinishMany (about 18.5 finishes per call) | 0.05 | 9,061 | 487 | 88 (5.8) | decision (about 150 per job: reading the job hash, closing the attempt, encoding each result, the receipt row), sorting job hash and receipt fields (about 105), packing the intent (about 35), projection (about 45) |
| DispatchMany (insert, 100 per call) | 0.01 | 21,618 | 217 | 397 (4.2) | not looked at here (the dispatch diet covered it) |
| Finish (single, at the drain's end) | 0.004 | 1,190 | 5 | 17 | |
| Total script time | | | 1,350 | | |

Without buffered finish the claim is the same, and there are about 2.5 times as many FinishMany calls, each smaller. Script time per job is about the same (27.2 s against 26.9 s per drain).

**Where the CPU goes (perf, whole drain):** the Lua interpreter loop 14%, creating strings 7% (every `..`, `string.sub` and `string.format` makes a new string), function calls 6%, table lookups 4%, `string.byte` 3% (the byte comparator), pattern matching about 2.5% (the JSON string escaper's check), memory allocation about 6%, garbage collection about 2%, SHA-1 of the intent 1%. Nothing is O(backlog): the ready-set scans read one page of 100, and each sorted-set write is O(log n).

**Phase breakdown from `profile_worker.py` (CPU µs per job, 16 jobs per call, before the change):**

| Phase | Claim | Finish |
|---|---|---|
| Frame before the decision (meta read, gates) | 8 | 6 |
| Decision | 125 | 150 |
| Encoding the result for the receipt and reply | 130 | (inside the decision) |
| Event rows | 22 | 17 |
| Ordering the after-images (mostly the field sort) | 102 | 105 |
| Packing the intent | 13 | 34 |
| Writing the guard and projecting | 52 | 53 |
| Total | 453 | 368 |

### What was changed (commit 5bf91db, scripts only)

All of it is in `src/Runwick.Redis/Lua`. Nothing stored changes, no reply changes, and the order of the after-images in the intent does not change. `diffcheck.py` (now also comparing the exact reply text and covering `finish_and_claim` and its replay) shows 0 differences in 49 keys and 12 replies against main's scripts.

1. **The JSON writer fills one buffer.** `U.enc` used to build a string for every value and every object, then join them again one level up. It now appends pieces to one table and joins once. Each distinct key's `"key":` text is built once per call.
2. **Each key set is sorted once per call.** Object keys must come out in byte order. The writer used to sort every object's keys with the Lua comparator, and a claim of 16 jobs has 16 copies of each object shape. The writer now walks a small per-call tree by the object's keys in the order Lua lists them, and keeps the sorted list at the end of that path. The result is exact for any key set. If Lua lists the same keys in another order, that only costs one more sort. The tree lives in script locals, so nothing carries over between calls.
3. **The byte comparator compares eight bytes per call.** After the first byte, `U.blt` now reads eight bytes of each string per `string.byte` call instead of one. Job hash fields share a 35-character prefix (`a:<attempt id>:`), so this is where field sorting spent its time. Sorting a job hash's 18 fields went from 73 to 27 µs. It still compares byte values, never strings with Lua `<` (REDIS-11).
4. **FinishMany receipt rows** are written from the receipt's fixed, already ordered field list (`U.hset_sorted`, as single finishes and dispatch already did), so they are not sorted again.
5. **`finish_and_claim` builds its reply text by adding `"finished":[...]` to the claim receipt's text** (the key orders before `items`, and it checks that `items` is the only other key) instead of encoding the claimed jobs again.

**CPU per job after the change (`profile_worker.py`, base and new scripts back to back, two rounds, medians):**

| Jobs per call | Claim before → after | Finish before → after | Combined call (one finish plus one claim per job) before → after | Claim plus finish as two calls, after |
|---|---|---|---|---|
| 8 | 538 → 392 µs (−27%) | 407 → 342 (−16%) | 1,060 → 657 (−38%) | 734 |
| 16 | 497 → 371 (−25%) | 378 → 299 (−21%) | 1,002 → 675 (−33%) | 670 |

Before the change the combined call cost 12 to 22% more per job than the two separate calls. Now it costs between 10% less and about the same.

### Measured and left alone

- **Deletions of fields that do not exist.** A finish deletes up to four attempt fields that were never written (`delay`, `error`, `next` and similar for a success). Skipping them would save a few µs per job. It would also change the intent's contents against main, so it was left.
- **The remaining sort of each job hash's fields** (about 27 µs per job hash per call). Removing it needs a fixed, pre-ordered write list across reserve, begin and close, which means restructuring how the job hash is written.
- **The JSON escaper's pattern check** (about 0.6 µs per string, about 15 µs per claimed job). A per-call cache only helps for repeated values.
- **The fixed part of every call** (about 100 to 150 µs: building the chunk's functions, reading meta, the gates). Spread over 8 to 16 jobs, it is under 15 µs per job.
- **The model itself**: the packed intent, then the same data written again as it is projected; a receipt, an event row and an event index entry per command; four index sorted sets per state change. This is still most of the gap to BullMQ, as the earlier reports said.

### Tests

| Run | Result |
|---|---|
| Core | 453 of 453 |
| `FinishAndClaimContractTests` (SQLite, PostgreSQL, Redis, a refused call, a lost reply) | 5 of 5 |
| Full Redis project (own Redis 8.2 container, AOF always) | 120 of 120 (35 m 46 s) |
| `diffcheck.py` against main's scripts | 0 differences (49 keys, 12 replies) |

A first full Redis run against a brand-new, empty server failed 52 of 120 tests, all with "The Redis database has rw:* keys but no rw:store". This is a test-setup race, not the change. On an empty server, test classes start in parallel and some write `rw:*` keys before the shared fixture has migrated the server. After that, every later migration refuses. The usual test server (127.0.0.1:56379) is long-lived and already migrated, so it never shows this. I migrated the new server first by running one class alone (`RedisDispatchLayoutTests`, 2 of 2), and then the whole project passed. Worth a fixture fix (migrate once before any class starts); it was not changed here.

### Benchmark: drain of 20,000 jobs, 1 KiB, 16 workers, fresh Redis 8.2 per run

Arms: `old` and `oldbuf` are main at 9ec2a34 without and with buffered finish; `new` and `buf` are this change without and with buffered finish. BullMQ is 6.7.2. Each round runs all five arms back to back under the bench lock, in rotated order. "Script time" is Redis's `commandstats` time for all scripts in the run (insert included) divided by 20,000. "CPU" is Redis main-thread CPU per job.

**Disk log (appendfsync always on the host disk)**

| Arm | Jobs/s per round | Median | Script ms per job | CPU ms per job | Load at start |
|---|---|---|---|---|---|
| old | 303, 301, 388 | 303 | 1.68 | 1.81 | 11.7, 8.3, 10.6 |
| new | 356, 442, 458 | 442 | 1.17 | 1.44 | 13.7, 10.5, 12.3 |
| oldbuf | 314, 526, 328 | 328 | 1.71 | 1.91 | 13.8, 9.5, 9.7 |
| buf | 456, 649, 388 | 456 | 1.25 | 1.49 | 10.9, 7.9, 9.5 |
| BullMQ | 1,438, 1,903, 923 | 1,438 | 0.29 | 0.45 | 10.4, 7.4, 8.3 |

Paired ratios: new against old 1.17, 1.47, 1.18; buf against oldbuf 1.45, 1.23, 1.18. Every round improved. Runwick with buffered finish is at 0.32 times BullMQ; main was at 0.23 times in the same rounds.

**Memory log (appendfsync always on tmpfs, so a disk sync costs almost nothing): two sets of three rounds**

| Arm | Set 1 jobs/s | Set 2 jobs/s | Median of all six | Script ms per job (set 1, set 2) | CPU ms per job (set 1, set 2) | Load at start |
|---|---|---|---|---|---|---|
| old | 653, 585, 494 | 576, 638, 643 | 614 | 1.44, 1.35 | 1.58, 1.49 | 9 to 12 |
| new | 709, 605, 494 | 778, 726, 777 | 718 | 1.26, 1.07 | 1.40, 1.22 | 9 to 13 |
| oldbuf | 784, 695, 542 | 675, 692, 834 | 694 | 1.34, 1.30 | 1.44, 1.41 | 9 to 11 |
| buf | 662, 877, 536 | 932, 1,003, 935 | 906 | 1.27, 0.94 | 1.40, 1.05 | 8 to 13 |
| BullMQ (set 1 only) | 3,091, 3,282, 2,482 | | 3,091 | 0.22 | 0.30 | 9 to 12 |

Paired ratios, new against old: 1.09, 1.03, 1.00 (set 1) and 1.35, 1.14, 1.21 (set 2). Buf against oldbuf: 0.84, 1.26, 0.99 (set 1) and 1.38, 1.45, 1.12 (set 2). My Redis test suite was running during both sets. The first set does not separate the arms. The second set does, in every round. Across all six rounds the change gives 1.17 times without buffered finish and 1.31 times with it. BullMQ is still about 3.4 times faster with a memory log.

### What is left

- On a quiet host, Runwick's drain still costs about 4 times BullMQ's script time per job (about 1.2 against 0.3 ms) and drains 3 times slower. The remaining cost is spread thinly: each of the phases above is 5 to 25% of the script. There is no single large item left that can be cut without changing what each command stores (intent, receipt, events, indexes).
- The earlier reports' Redis timings were taken under load 17 to 75 and overstate script cost by about three times. Their before-and-after ratios still hold, because both arms of each pair saw the same load. Script time per job is only comparable between runs at similar load.
- No steady or delayed workload was rerun. No quiet-host repeat of the loop-carried branch's full benchmark was done (only one slow-log drain per arm, quoted above).

## Redis worker path: store less per job (work area F follow-up)

Written 2026-11-19. Branch `worktree-agent-ad96d1e91ad0d86e1`, base `main` at 790c1ea.

### Short answer

The biggest thing each worker command wrote was not job data but a second copy of it: the stored "intent", a packed copy of every value the command was about to write plus its reply, written to the application's metadata hash before the real writes and cleared after them. It was about 55% of the claim's log bytes and 50% of a finish's. A command that finishes in one script call no longer stores it. Redis already writes a script's changes to the log and to replicas as one block, so nobody can see half of one call; the intent is now stored only when a write inside the script fails partway, which is the only case it still protects.

Effect: log bytes per drained job fall by more than half (claim 7.2 to 3.1 KB, FinishMany 4.4 to 2.1 KB), and script CPU per job by about 10 to 20%. In paired drains of 20,000 jobs, Redis script time per job fell about 16 to 20% (1.25 to 1.01 ms on a disk log; 0.84 ms on a memory log with buffered finish) and throughput rose by a median 1.05 to 1.23 times, with wide round-to-round spread. BullMQ is still about 2 times faster on a disk log and 4 times on a memory log, so the BullMQ target is not met.

No stored layout change, no migration, no reply or receipt change. Every test passes. Recommendation: merge as the default (it is already the default on this branch), The delayed-pickup tail was rechecked on a quiet host before the merge and is not worse (see Benchmarks). Merged to main as the default on 2026-11-19.

### What each command wrote, before and after

Measured with `eng/bench-compare/redis-group/writes.py`: real scripts, 20 calls of 8 jobs each against an AOF-always Redis 8.2, AOF bytes appended per call divided by jobs. "Read by" says who needs the piece.

| Piece (per job) | Claim before | Claim after | FinishMany before | FinishMany after | Read by | Decision |
|---|---:|---:|---:|---:|---|---|
| Stored intent (`meta` guard + packed intent + SHA) | 4,068 | 0 | 2,227 | 0 | recovery only, after a failed or multi-call projection | written only when needed (cut) |
| Publication (`meta` rev, clear guard/intent/sha/ipos) | 17 | 8 | 17 | 8 | readers (`rev`), history check | only `rev` now |
| Receipt row (`rcpt:<id>`) | 1,263 | 1,263 | 651 | 651 | replay after a lost reply | needed; the claim receipt is the claim reply |
| Job hash (attempt and job fields) | 889 | 889 | 875 | 735 | everything | needed; finish no longer deletes fields the job never had |
| Event rows (`events:data`) | 576 | 576 | 348 | 348 | `EventsSince`, dashboard timeline | needed (users see them) |
| Event index (`events`) | 42 | 42 | 24 | 24 | `EventsSince` | needed |
| State index (`idx:created:s:*`) | 113 | 113 | 114 | 114 | `ListJobs` by state, dashboard | needed |
| Lease index | 65 | 65 | 45 | 45 | expired-lease scan, recovery | needed |
| Ready index | 46 | 46 | – | – | claim | needed |
| Attempt → job (`attempts`) | 84 | 84 | – | – | management reads, RevealError | needed |
| Terminal index (`idx:terminal`) | – | – | 66 | 66 | job stats | needed |
| Receipt expiry index | 14 | 14 | 66 | 66 | retention | needed |
| MULTI/EXEC framing | 4 | 4 | 4 | 4 | Redis | – |
| **Total AOF bytes per job** | **7,190** | **3,113** | **4,443** | **2,067** | | |

Single `Finish`: 5,241 → 2,526 bytes. FinishAndClaim (8 finishes plus a claim of 8): 11,602 → 5,198 bytes per job.

Candidates considered and left alone:

- **The claim receipt (1.3 KB per job).** It holds the claim result that a resend must replay byte for byte. Shrinking it means changing the reply the C# client parses. Not done.
- **Attempt fields `token` and `closedby`** (about 150 bytes per job). No current reader, but they mirror PostgreSQL's attempt columns (spec 22 §3.1), so dropping them is a layout change for a 5% gain. Not done.
- **Event rows.** Two per claimed job (`attempt_reserved`, `attempt_started`) and one per finish; users see them. Not changed.
- **Index churn.** Already minimal in the log: Redis does not log a `ZREM` that removed nothing, and the in-script overlay already folds a `ZADD` followed by a `ZREM` of the same member.

### What was cut and why it is safe

1. **Direct projection.** A command whose writes fit its first call's budget (every worker command; most dispatches) writes them straight away and publishes with `HSET meta rev <n+1>`.
   - Redis puts all of one script call's writes into the log, and onto replicas, as one `MULTI`/`EXEC` block, and runs nothing else in between. A log whose last block is cut is refused at load (the profile requires `aof-load-truncated no`) and repaired by cutting back to the block before. So a crash, an AOF restart, a snapshot or a replica never holds part of one call.
   - What is left is a script failing between two of its own writes, which Redis does not undo. Out of memory cannot do it: a script with a shebang is checked only before it starts (checked on Redis 8.2: a script that crosses `maxmemory` halfway keeps writing; the next script is refused). `SCRIPT KILL` is refused once a script has written. Any other failing write (a key of the wrong type, a key that left a Cluster node) is caught: the script then stores the intent exactly as the old order would have stored it (guard, intent, `ipos` 1, the writes made so far already in place) and fails as before. From there recovery is unchanged.
   - Exactly-once, replay after a lost reply, fencing, lease renewal, cancellation, retries, events, retention: the decision and every value written are the same as before (`diffcheck.py`: 49 keys and 12 replies identical; the only differences are the four empty `meta` fields `guard`, `intent`, `intentsha`, `ipos` that are now never created).
   - The size limits (4 MiB intent, 512 KiB inline batch) still apply to the intent whether or not it is stored. An upper bound of the packed size, much cheaper than packing, settles them; when it cannot, the script packs as before.
   - Mixed versions: an intent stored by either binary has the same format and is finished by either. An older binary never reads the fields a direct projection leaves out.
   - The `FailAfter` test seam counts the guard that is no longer written as write 1, so a cut after N writes leaves the same state as before and every write-prefix test still means the same thing.
2. **No sorting of fields inside one write.** The order of fields in one `HSET` or members in one `ZADD` changes nothing stored. Sorting the job hash's fields cost about 25 to 60 µs per job.
3. **No deletes of fields a job never had.** Closing an attempt deleted `delay`, `next`, `error` and `reason` even when they were never written.

Migration: none. Layout revision, manifest and replies are unchanged.

### Tests

| Run | Result |
|---|---|
| Core | 453 of 453 |
| Full Redis project (own Redis 8.2 container, AOF always; server prepared by running one class first) | 124 of 124 (44 min) |
| Contract (SQLite, PostgreSQL via Testcontainers, Redis) | 496 of 496 |
| Dashboard (includes the Redis store snapshot) | 57 of 57 |
| `diffcheck.py` against main's scripts | identical data in 49 keys and 12 replies, apart from the four `meta` fields that are now never created |

New: `RedisDirectProjectionTests` (REDIS-19 in spec 22):

- DP-01: `ReserveAndBegin`, `FinishMany`, `Finish` and FinishAndClaim, each with its reply dropped after the script ran, leave no stored intent (`meta.intent` never written), and the resend replays the same answer.
- DP-02: a claim whose lease-index write hits a wrong key type stores its intent at that point. The application reads as Recovering. Once the key is repaired, the resend publishes the intent and replays the claim (3 jobs running, 3 leases).
- DP-03: the same for a finish batch whose terminal-index write fails. Each finish applies once.
- DP-04: every worker script and dispatch, run with a check that packs each intent, never finds the cheap size bound below the packed size.

The existing write-prefix sweeps (REDIS-01, FC-07, RAB-07), the wrong-key-type case (REDIS-04, now on a directly projected dispatch), memory full, crash and restart from the AOF, Sentinel and Cluster cases all pass unchanged.

### Benchmarks

Drain of 20,000 jobs, 1 KiB, 16 workers, fresh Redis 8.2 per run, `pairs-combined.sh`, two sets of three rounds per log kind. Arm order rotated each round, and every round held `/tmp/runwick-bench.lock`. Arms: `old` and `oldbuf` are main at 790c1ea without and with buffered finish. `new` and `newbuf` are this branch (`base` and `buf` in the result files). Load average at each run start was 4.5 to 15.7 on 8 cores (shared host, not quiet). Script time is Redis `commandstats` time for all scripts (insert included) divided by 20,000. AOF bytes include the insert.

| Log | Arm | Jobs/s, median of 6 | Script ms per job | Redis CPU ms per job | AOF bytes per job |
|---|---|---:|---:|---:|---:|
| disk | old | 409 | 1.25 | 1.41 to 1.52 | 17,740 |
| disk | new | 479 | 1.01 | 1.29 to 1.31 | 8,310 |
| disk | oldbuf | 469 | 1.19 | 1.38 to 1.50 | 17,460 |
| disk | newbuf | 558 | 0.94 | 1.13 to 1.26 | 8,165 |
| disk | BullMQ 6.7.2 (3 rounds) | 1,128 | 0.31 | 0.44 | 2,815 |
| tmpfs | old | 679 | 1.14 | 1.29 | 17,770 |
| tmpfs | new | 767 | 0.96 | 1.10 to 1.18 | 8,333 |
| tmpfs | oldbuf | 800 | 1.05 | 1.10 to 1.22 | 17,440 |
| tmpfs | newbuf | 902 | 0.84 | 0.90 to 1.02 | 8,158 |
| tmpfs | BullMQ (3 rounds) | 3,428 | 0.20 | 0.28 | 2,816 |

Paired ratios of jobs/s per round:

- Disk log, new ÷ old: 0.72, 1.13, 1.31, 1.26, 0.97, 0.93 (median 1.05).
- Disk log, newbuf ÷ oldbuf: 0.92, 0.98, 1.25, 1.49, 1.20, 1.30 (median 1.23).
- Memory log, new ÷ old: 1.15, 0.87, 1.14, 1.14, 1.13, 1.24 (median 1.14).
- Memory log, newbuf ÷ oldbuf: 1.48, 0.97, 1.08, 1.31, 0.92, 1.17 (median 1.12).

Script time and log bytes fell in every round. Throughput is noisier, because the 16 workers mostly wait on round trips rather than keeping Redis busy.

Script CPU inside the scripts (`profile_worker.py`, `os.clock`, 16 jobs per call): claim about 375 → 314 µs per job, FinishMany about 290 → 245, FinishAndClaim about 650 → 510. Of that, ordering the after-images went from about 1,000 to 140 to 220 µs per call, and the intent's guard write and packing (about 200 to 350 µs per call) are gone.

**Steady 500 offers/s (one round, old / new / BullMQ):** accepted 486 / 496 / 468 jobs/s; due-to-handler p99 16.4 s / 12.3 s; BullMQ offer-to-finish p99 0.23 s. Both Runwick arms are still overloaded at this rate.

**Delayed 100 offers/s, 5 s ahead (four rounds old/new, one with BullMQ):** throughput is equal (100 jobs/s; BullMQ 100). The due-to-handler tail is unstable in both arms. Old p99: 139, 102, 7,997 and 6,697 ms. New p99: 4,657, 5,143, 5,716 and 4,907 ms. BullMQ p99: 326 ms. New showed a tail of about 4 to 5 s in all four rounds; old showed one in two of four, and a worse one when it did. The code behind that wait (the pool's idle backoff and the probe's next-due hint) is client-side and unchanged here, and the earlier reports already saw 2 to 7 s tails on main. It may still interact with the cheaper scripts' timing (new made fewer claim and probe calls: 1,636 against 2,086 claims in round 1). Not resolved in these rounds.

**Quiet-host recheck before the merge (2026-11-19, load about 3.5, six paired rounds, arm order rotated):** p99 wait past the due time was 0.40, 0.20, 0.36, 0.15, 0.17 and 1.46 s with the new scripts (median about 0.27 s) and 4.19, 0.13, 1.12, 0.15, 2.39 and 0.24 s on main (median about 0.68 s). Median wait past due was about 30 ms in both. The change does not make the delayed tail worse; the occasional multi-second tail shows up on main too and comes from the pool's idle wait, not from the scripts.

### What is left

- Per job, a drained job still costs about 3 times BullMQ's script time (about 0.95 against 0.3 ms) and writes 3 times its log bytes (8.2 against 2.8 KB, insert included). What remains is spread out: decision logic about 35%, encoding the claim result for its receipt and reply about 20%, event rows about 10%, applying the writes about 15%.
- The next cuts all change something users or the protocol see: a smaller claim receipt (it is the claim reply, so the C# client would change), fewer or smaller event rows, and dropping the unread attempt fields `token` and `closedby` (a layout change for about 5%).
- The occasional multi-second delayed-pickup tail (on main and on this change alike) likely comes from the engine's idle backoff when no job is known; that is client-side work still to do.
- Tools: `writes.py` (AOF bytes per job by key kind), `summarize_drain.py` now prints AOF bytes per job, and `profile_worker.py` understands the new commit path. Raw results are in `eng/bench-compare/redis-group/results/*-rsl*` (git-ignored).

## Q6/Q7 and E close-out (SQLite wake, P-08 analysis, allocation test)

**Short answer:** the code parts are done and now tested. The SQLite wake commits first, then wakes. It sends no wake for a replay or a delayed job, a broken listener can't fail an add, and polling still finds work when every wake is dropped. The P-08 analysis fix is in. The allocation test failure was a test bug (a cold call compared with a warm one), not a product regression. The new steady run at 100 offers/s on current code is much better than the old one: 98.7 completions/s, p99 1.24 s, 0 busy conflicts. It still refused 310 of 24,000 offers (1.3%) on a shared host. Some of the measurements the plan asks for during the load are not covered by the harness: crash/reopen, scheduler and outbox checks, and checkpoint progress over time. So the plan item should stay open (`[~]`) for those.

### Requirements

| Requirement | Status | Evidence |
|---|---|---|
| Wake only after commit | Met | `5813e91`. Test `Local_work_hint_crosses_store_instances_after_commit_and_is_best_effort` holds the commit at a barrier and checks that no wake fires and no row is visible. The wake arrives once the commit is released. |
| A failed wake cannot fail an add | Met | The same test registers a listener that throws, and the dispatch still returns Applied. `PublishLocalWake` catches each listener's error separately. |
| No wake on replay | Met | Single dispatch: the same test resends the command, which comes back `Replayed` and adds no second wake. Batch: the new test `Local_work_hint_for_a_batch_names_each_due_queue_once_and_skips_delayed_items_and_replays` covers this. |
| Batch wake: each due queue once, no wake for a not-yet-due job | Met (new test) | Same new test. A batch of two due jobs on `default` and one job delayed an hour on `other` gives exactly one wake, for `default`. A mutation check that ignores `NotBefore` makes the test fail. |
| Polling fallback when wakes are lost | Met | `PollingContractTests.With_every_wake_dropped_polling_finds_work_controls_and_recovery(Sqlite)` (OBS-01) drops every wake and still finds work, a pause and resume, and a dead lease. It now also asserts that the SQLite local wake actually reached the worker, so the test proves the wake was there and was dropped. Before, it would have passed even if the wake had never been wired to the worker. |
| Wake only within one process | Met by design | Listeners are keyed by database file and application, inside one process. Other processes and staged (caller-transaction) writes rely on polling. `ILocalWorkWakeSource` says so in its documentation. |
| P-08 readiness analysis fix | Met | `e3bfd95`, `tests/Runwick.Workloads/Analysis.cs` now counts every applied reserve counter family (`reserve:`, `reserveAndBegin:`, `reserveMany:`). Later P-08 runs report readiness (P-08c: 6.9 s in `reference-20261117-rest`). |
| Core allocation failure reproduced and traced | Met | See below. |
| SQLite steady 100/s, retained history, cleanup, dashboard reads | Partly met | New run on `9a7e66f` (below). Offers are not fully accepted: 1.3% were refused. |
| Writer queue age, refusals, durable completions, pickup p99 | Met (measured) | New run, below. |
| Checkpoint progress | Partly met | Only an after-run WAL snapshot exists: 268 frames, 4.4 MB, not busy. There is no reading over the course of the run. |
| Crash/reopen, scheduler and outbox checks under that load | Not met as a combined run | The harness has no steady mode that runs these together. Separately they pass in the suites: SQLite fault-point kill tests (`A_writer_killed_at_a_fault_point_...`), the scheduler and outbox-source contract rows on SQLite. |

### The allocation test failure

`Prepare_allocation_is_linear_in_payload_size` (SER-12) failed before `e3bfd95`. Putting the old test back on the current tree reproduces it: ratio 2.60 three runs out of three (3.0 was recorded on 14 November). The preparation source between `e3bfd95` and now differs only by `d44cb01`, which touches only the batch cap.

Allocated bytes per stage on this host:

| Stage | 512 KiB first call | 512 KiB warm | 1 MiB first call | 1 MiB warm |
|---|---:|---:|---:|---:|
| Serialize (includes scan) | 4.13 MB | 4.12 MB | 9.16 MB | 9.16 MB |
| Scan only | 2.22 MB | — | 4.44 MB | 4.43 MB |
| Whole Prepare | 7.29 MB | 5.13 MB | 15.42 MB | 11.21 MB |

Serializing and scanning cost the same on the first call as on later ones. The one-time extra cost is in the digest step, after serialization. Renting and returning two 2 MiB arrays from `ArrayPool<byte>.Shared` before the first 1 MiB call cost 4.20 MB, and after that the "cold" 1 MiB Prepare allocated 11.22 MB, the same as a warm one. The first call at each size fills that size's power-of-two pool bucket: 1 MiB arrays for the 512 KiB payload, 2 MiB arrays for the 1 MiB payload. The old test warmed only the 512 KiB size, so it compared a cold 1 MiB call with a warm 512 KiB call. Once warm, the ratio is 2.19 (serialize 2.22×, scan 2.0×), which is under the 2.2 limit, so allocation is linear. The fix in `e3bfd95` warms both sizes. Risk: 2.19 sits close to 2.2, so a small real regression would first show up as a flaky failure.

### SQLite steady run at 100 offers/s

Run on `9a7e66f` (main `391f520` plus the new tests), with the same command as the 2978f04 run plus `--store-viewers 2`. That means 16 callers, 16 worker slots, 1 KiB jobs, 240 s of offers, and a copy of the 5,000-terminal-job retained fixture. SQLite used WAL with synchronous FULL on local ext4. The run held `/tmp/runwick-bench.lock`. Host load was 4.5 to 6.8 during the run (8 cores, other agents active). Raw results are in `artifacts/perf/round-two/sustained-100-q6q7/` (`sqlite.json`, `load.log`, `host.txt`).

| | 2978f04 (before wake) | 9a7e66f (now) |
|---|---:|---:|
| Offered | 24,000 | 24,000 |
| Accepted and completed once | 21,774 | 23,690 |
| Refused (`local_capacity_exceeded`) | 2,226 | 310 |
| Completions/s | 90.6 | 98.7 |
| Offer-to-finish p50 / p99 | — / 5.61 s | 64 ms / 1.24 s |
| Due-to-handler p99 | 3.94 s | 353 ms |
| Busy conflicts | 0 | 0 |
| Writer queue p99, dispatch / cleanup / presence | 543 ms / 8.51 s / 3.81 s | 283 ms / 2.05 s / 2.45 s |
| Store page snapshot reads | none | 96 polls, p99 17 ms |
| WAL after run | 4.4 MB | 4.4 MB (268 frames, not busy) |

The books balance: offered = accepted + refused, and completed = accepted. The due-to-start goal (p99 under 2 s) was met in this run. The rate goal was nearly met: 1.3% of offers were refused at the local admission limit. The two runs were on different days with different host load, so the improvement is not a paired measurement.

### What I added

- `SqliteDispatchGroupingTests.Local_work_hint_for_a_batch_names_each_due_queue_once_and_skips_delayed_items_and_replays`, which covers the batch wake path. Before this, it had no test.
- An OBS-01 assertion for SQLite only: the local wake reaches the worker engine, which then drops it.
- Nothing was broken, so no product code changed.

### Tests

SQLite 55/55, Core 453/453, Contract 496/496 (Redis-backed rows used a fresh AOF-always Redis with the harness profile; the first Contract run failed only its Redis rows because that container lacked `--aof-load-truncated no`).

### What is left

- A steady run that also does crash/reopen, scheduler and outbox work under the same load, and samples checkpoint progress during the run. The harness for it is now built (next section); the run itself is still to do.
- A quiet-host repeat to confirm whether the remaining 1.3% of refusals is host load or a real limit.

### Harness for the combined run

`bench --store sqlite --mode steady` has new opt-in flags. Without them a run behaves exactly as before.

- `--retention-hours h` sets the application's retention policy to `h` hours for every class (audit keeps its 7-day minimum), and cleanup looks for work every 5 s instead of every 60 s. The smallest allowed horizon is 1 hour, so during a run shorter than that, cleanup deletes the old retained fixture, not the run's own jobs. `--cleanup-batch-rows n` makes batches smaller so cleanup lasts the whole window: the 5,000-job fixture at 5 rows per batch takes about 17 minutes.
- `--schedule` adds one schedule that fires every second. Its jobs go to their own queue (`schedule`) and never count as load jobs.
- `--outbox-rate r` stages `r` outbox messages per second, each in its own transaction, in a second SQLite file next to the store (the application's own database). The relay delivers them to the store's `outbox` queue.
- `--sample-seconds s` records, every `s` seconds: completed and accepted load jobs, refusals, and the WAL counters (frames in the log, frames already copied back to the database, WAL file size). The WAL counters come from a separate connection, so sampling does not wait for the writer.
- `--crash-cycles k` splits the window into `k + 1` parts, each a separate process on the same database file. The first `k` processes are killed (SIGKILL, no graceful stop) at the end of their part, with calls and jobs still in flight. The next process reopens the file and carries on; the last one waits until the store and the outbox source have nothing left unfinished (up to `--drain-store-seconds`, default 300). Each process writes a ledger of accepted, started and finished jobs, and at the end the parent checks the ledgers against the store.

With `--trace`, each part also reports `writerQueueMs` (how long each kind of store call waited for the single SQLite writer: count, p99, max) and `pickupMs` (due to handler start).

A 40-second smoke check (50 offers/s, one crash, all flags on) wrote its JSON and reconciled: 992 accepted jobs, all succeeded, none missing, no job run twice. It is not a measurement.

### Combined run result (2026-11-23)

What ran: 18 minutes at 100 offers/s on the retained-history seed, with all of these at once:
- cleanup at a 1-hour horizon in 5-row batches
- two dashboard viewers
- a schedule firing every second
- an outbox staging 10 messages/s
- two hard kills (SIGKILL) followed by reopening the store

The run used harness commit 2780700 on an 8-core ext4 host. One-minute load was 8.2 at the start and still falling, and 3.2 at the end. Output is in `artifacts/perf/round-two/sqlite-q67-combined/`.

Nothing was lost or run twice:
- All four must-be-zero reconciliation counts are 0: accepted but missing, finish not confirmed, store job without a receipt, and receipt-less job not succeeded.
- No job ran more than once, and no job's execution count in the store is above one.
- All 10,800 outbox messages that were staged were delivered, with no staging errors.
- After the final drain, no job was left unfinished.
- The schedule finished 1,182 jobs.

Throughput and latency, compared with the earlier 240-second run without crashes, cleanup, schedule or outbox:

| | 240 s run | combined 18 min run |
|---|---|---|
| durable completions/s | 90.6 | 91.9 |
| refused (`local_capacity_exceeded`) | 2,226 | 6,081 of 108,001 (5.6%) |
| busy conflicts | 0 | 0 |
| offer to pickup p99 | 5.6 s | 4.1 s (max 7.2 s) |
| offer to finish p50 / p99 | | 0.32 s / 4.2 s |

Refusals fell part by part, from 3,588 to 1,572 to 921, as the start-of-run load went away. The three parts completed 90.0, 95.6 and 82.9 jobs/s.

Findings:
- **Cleanup, schedule materialization and presence wait longest for the writer.**
  - Writer-queue p99 was 5.6 to 7.4 s for cleanup batches, about 4.5 to 4.8 s for materializing schedule occurrences, and about 2.3 to 3.0 s for presence publishing.
  - The calls on the job path (dispatch, reserve, begin, finish) stayed at or under about 0.75 s.
  - So background work queues behind job traffic, not the other way round. This is the behaviour we want, but a 7 s wait means a cleanup batch can time out. A few `command_expired` cleanup failures (1 to 3 per class) did happen, and cleanup still made progress: 3,751 rows deleted over the run.
- **The WAL stayed bounded.**
  - The log peaked at about 1,000 frames (4.5 MB), which is the auto-checkpoint size, and SQLite never reported busy.
  - In the second part, the counter of frames copied back stayed at 0 in every sample while the log sat at its ceiling. The log did not keep growing, so it was probably being reset between samples, but this run does not prove that.
- **The crash check is weaker than it looks.**
  - The reconciliation counts 0 accepted jobs that crossed a crash, so neither kill landed on a job the ledgers saw as started but not finished.
  - "Nothing lost across a crash" is therefore shown for jobs queued at the kill, not for jobs mid-run. Making the kill land mid-handler (a longer handler or a kill timed on a started job) would close that. The rerun below does.

### Rerun with kills that land mid-job (2026-11-23)

Why the first run's kills never caught a job: after a crash segment stopped offering, it waited for the outbox stager and sampler and built its result before killing itself. In those seconds the workers finished every queued job.

The harness change (commit 2ca469d) makes a crash segment keep offering until the moment it dies. The run also used `--job-ms 50`, so each handler takes 50 ms and some jobs are inside their handler at the kill. Everything else matched the first run: 18 minutes, 100 offers/s, cleanup, viewers, schedule, outbox, two kills. Load was 4.2 at the start and 5.4 at the end. Output is in `artifacts/perf/round-two/sqlite-q67-midjob/`.

A 40-second smoke run came first: 5 accepted jobs crossed a crash, and all of them ran again after the restart.

It also showed 2 store jobs without a producer receipt. That is now expected: the kill can land while a dispatch call is in flight, after the store has committed the job but before the reply reaches the producer. Both jobs ran and succeeded. Only receipt-less jobs that did *not* succeed would be a fault.

Full run:
- **Nothing lost.** 25 accepted jobs crossed a crash. Every accepted job is in the store and succeeded, every finish was confirmed to its worker, and nothing was unfinished after the final drain.
- **Reruns.** 11 jobs ran more than once, and every rerun is explained by a crash (the earlier run started in a killed segment that never confirmed its finish). 18 store rows have an execution count above one: these include starts that the killed process never got to write in its ledger.
- **Side work.** The outbox delivered all 10,800 messages it staged, and the schedule finished 1,127 jobs.
- **Throughput and refusals.** 94.8 durable completions/s, a little higher than the first run. 2.3% of offers were refused (`local_capacity_exceeded`), against 5.6%. There were 0 busy conflicts.
- **Pickup has a long tail.** Offer to pickup p50 was 0.42 s and p99 11.1 s, against 4.1 s in the first run.
  - Per segment, pickup p99 was 3.5 s, 4.4 s and 11.2 s, and the last segment's median was 5.3 s.
  - The ledgers show the delay comes in waves. Every few minutes, offers and completions both drop to a quarter or half of normal for 10 to 30 seconds. The backlog that builds up then drains slowly, because completions only rise to about 125/s during catch-up.
  - The job-path store calls are not the cause: in the last segment, dispatch waited 0.46 s p99 for the writer and reserve 0.21 s. Cleanup batches waited 4.8 s, schedule materialization 3.9 s and presence publishing 2.5 s.
  - The WAL stayed small and SQLite never reported busy.
  - The cause of the stalls is not found yet. Candidates are a whole-process pause (garbage collection or the host), the dashboard reads, or catch-up limited by something other than the writer queue. The next section follows this up.

### Where the pickup tail comes from (2026-11-23)

The trace answers most of it:
- In the last segment, a job's handler starts within about 10 ms of the worker reserving it. The whole wait (median 5.3 s) is before the reserve, while the job sits in the store unclaimed.
- A claim round trip takes about 20–40 ms and returns about 4.5 jobs, so the store is not slow to hand out work.
- The limit is worker slots. Each job cycle takes about 127 ms: 77 ms in a 50 ms handler, then finishing and waiting for the next claim. 16 slots therefore top out near 125 jobs/s, only 25% above the 100 offered per second.
- A backlog that builds up during a stall shrinks by only about 25 jobs/s, so a 10-second stall leaves around 40 seconds of late pickups.
- The stalls are moments when commits on every write path (finish, claim and dispatch) take up to about 1 s together. That looks like disk flushes on this shared host, plus bursts of cleanup, schedule and presence writes.
- A single pressure stop (an owner-control wait over 1 s) paused claiming once in the last segment. It pauses for only a few seconds, so it is not the main cause.

To check this, the same run was repeated with 32 workers instead of 16, which roughly doubles the spare capacity. Load was 3.0 at the start and 12.7 at the end, because another session's benchmarks were running alongside. Output is in `artifacts/perf/round-two/sqlite-q67-w32/`.

| | 16 workers | 32 workers |
|---|---|---|
| offer to pickup p50 / p99 / max | 0.42 s / 11.1 s / 70 s | 0.28 s / 4.3 s / 9.0 s |
| pickup p99 per segment | 3.5 s, 4.4 s, 11.2 s | 5.0 s, 2.4 s, 4.7 s |
| largest backlog (accepted minus completed) | | 376 jobs |
| accepted jobs that crossed a crash | 25 | 53 |
| refused (`local_capacity_exceeded`) | 2,475 | 7,416 |

The 32-worker run met all the safety checks: nothing was lost, the 14 reruns are all explained by a crash, and nothing was left unfinished. The worst single commit was under 1 s.

Conclusion:
- Most of the 11 s tail came from the benchmark's own configuration: 16 slots with 50 ms jobs leave too little spare capacity to recover from short stalls.
- It is not a crash-recovery or claiming fault.
- The 2 to 5 s p99 that remains matches the first run (4.1 s). It comes from the stalls themselves, which also show up in the commit times on this host.
- What is still not proven:
  - The stall timing was not lined up with the pickup tail sample by sample.
  - Refusals rose with 32 workers, and why is not explained; the host was busier and more jobs crossed crashes.
- Advice for applications: on SQLite, size worker slots so that capacity is clearly above the arrival rate, not just equal to it. A queue near its ceiling turns every short disk stall into seconds of pickup delay.

## Redis: delayed jobs picked up seconds late (fixed)

### Short answer

The multi-second tail came from the **first delayed jobs of a run**, not from steady running. When a worker had been idle for a while (the benchmark's 12-second warm-up is enough), its idle wait had grown to its 10-second cap. A delayed dispatch sent no wake-up, and the worker's last readiness check had seen nothing scheduled, so it slept through the first due jobs: up to about 4 seconds in our runs, and up to 12 seconds in the worst case. The Redis store now sends a wake-up when a delayed (or retrying) job becomes the earliest waiting job of its queue and priority. The worker wakes, learns the due time from its readiness check, and sleeps until then. In eight paired rounds the multi-second tail went away: the worst round on main had a p99 of 3.8 s, and the worst with the fix was 1.5 s. That 1.5 s round came from slow store calls on a busy host, not from an idle sleep. Median p99 went from 0.69 s to 0.39 s, against 0.35 s for BullMQ.

### How the cause was found

A temporary trace (not committed) logged every idle wait of the pool (back-off step, the next-due hint, how long it slept, and whether a wake-up ended it), every readiness view and every claim, and in the benchmark every job that started more than 1 s after its due time. Fourteen traced rounds of the delayed workload (100 offers/s, 5 s ahead, 16 workers) were run on main's code.

- In every slow round, all the late jobs were among the first offers of the run. Example (round r4): the pool began a 9.5 s idle sleep about 1 s before the first offer. That sleep had no hint, because nothing was scheduled when it started. The first jobs fell due 4 s into the sleep and waited until it ended, so the first ~400 jobs started up to 3.7 s late (p99 3.5 s). Round r6 was the same, with an 8.4 s sleep and up to 3.2 s late.
- Rounds without a tail were lucky in timing: one of the growing idle sleeps happened to end after the first delayed dispatch but before it fell due, the readiness check then named the due time, and the pool slept exactly until then.
- In steady running nothing was missed. Almost every idle wait ended through a wake-up from a finished job, or ran to the next-due hint and claimed at once. No hint was dropped or stale.
- The rare sub-second tails left over were single slow store calls. One `ReserveAndBegin` took 920 ms while the host load was above 10, and the first finishes after start-up also took up to 0.9 s.

So the engine's idle and hint logic worked as designed. The gap was in what the store announced: it sent wake-ups only for jobs that are due now (perf round two F, wake hints), and spec 05's idle-caps entry lists delayed dispatch as "not notified", which costs up to 12 s on a worker idle at the cap.

### The fix

`U.wake` in `src/Runwick.Redis/Lua/prelude.lua` (the hint each write script publishes once its writes are whole) now also publishes the queue name when a written `ready:<queue>:<priority>` member is not due yet but is now the lowest future score in that set. This costs one `ZRANGE … BYSCORE LIMIT 0 1` per written ready set with a future member. Jobs that queue up behind an earlier delayed job publish nothing. In a steady delayed stream that means about two publishes per run (counted in the benchmark's Redis stats), and an empty store gets no extra polling. The engine is unchanged: a wake-up makes the idle pool check readiness, which returns `NextDueAt`, and the pool sleeps until then (05 §7.2, SP-012).

No stored data, reply or receipt changes. Spec: 22 Changes (2026-11-19, delayed pickup) and 05 Changes (2026-11-19, delayed pickup). Public page: `redis.mdx`, "Wake-ups and polling".

PostgreSQL and SQLite are unchanged. Neither sends such a hint nor reports `NextDueAt`, so there a delayed job sent to a long-idle worker is still found within the idle back-off cap (+20 %) plus the hint age. Fixing that would need changes to the PostgreSQL dispatch SQL and its probe, which is a separate piece of work.

### Tests

- Redis: `RedisWorkerPathTests.A_dispatch_publishes_a_wake_when_it_is_due_now_or_the_earliest_delayed_job_of_its_lane`. It replaces the old test, which asserted that a delayed dispatch publishes nothing. It checks four cases: the first delayed job wakes, a later one does not, an earlier one wakes again, and a due-now one wakes. It **fails on main** (no wake for the first delayed job) and passes with the fix.
- Core: `WorkerEngineTests.A_pool_idle_at_the_long_cap_starts_a_delayed_job_at_its_due_time_after_its_wake_hint`. A pool idle at the 10 s cap gets the wake-up and a store that names the due time 3 s ahead. The test checks that the job is reserved within 200 ms of its due time. The engine did not change, so this test passes before and after; it pins the engine half the fix relies on. (A Core test cannot fail before this fix, because the missing piece was the store's wake-up.) The scripted store gained a `NextDueAt` setting.
- Full runs with the fix: Core 454 of 454, Contract 496 of 496, Redis 124 of 124 (51 minutes). All passed.

### Benchmark: delayed 100/s, 5 s ahead, 16 workers

Eight rounds, each with three arms in rotating order, under the shared bench lock, with a fresh Redis 8.2 (AOF always, host disk) per arm. "Fix" is this branch, "main" is 391f520, and BullMQ is 6.7.2. The host was shared with other test suites, so load is recorded. Wait past due is in ms. For Runwick it is `latencyMs − 5000`, which runs from the dispatch call to the finish, so it slightly overstates the wait. For BullMQ it is handler start minus due.

| Round | Load fix / main / BullMQ | Fix p99 | Fix max | Main p99 | Main max | BullMQ p99 | BullMQ max |
|---|---|---|---|---|---|---|---|
| 1 | 10.5 / 8.7 / 9.6 | 359 | 534 | 734 | 895 | 1,357 | 1,625 |
| 2 | 13.5 / 12.2 / 14.1 | 391 | 554 | 750 | 930 | 636 | 797 |
| 3 | 9.2 / 8.0 / 10.4 | 391 | 575 | **3,773** | **3,989** | 255 | 329 |
| 4 | 9.2 / 7.5 / 7.5 | 250 | 341 | **3,235** | **3,498** | 265 | 420 |
| 5 | 6.2 / 8.5 / 6.3 | 727 | 911 | 183 | 318 | 654 | 938 |
| 6 | 5.3 / 5.4 / 6.4 | 1,547 | 1,669 | 498 | 619 | 277 | 383 |
| 7 | 6.8 / 11.0 / 8.4 | 378 | 583 | 652 | 729 | 236 | 345 |
| 8 | 16.6 / 5.7 / 3.9 | 1,105 | 1,288 | 312 | 472 | 414 | 607 |
| **Median** | | **391** | **579** | **693** | **812** | **346** | **514** |

Throughput was the same in all arms (Runwick completions 82 to 86/s over the whole run including drain, every offer settled; BullMQ refused 0 to 150 offers locally). In fix rounds 6 and 8, finish calls were slow (handler-to-finished p99 406 and 226 ms, against about 90 ms in main's rounds 3 and 4). That points to store-call delay on a loaded disk rather than an idle sleep; the traced runs of the fix showed the same pattern (see above). Median wait past due was 36 to 56 ms with the fix and 31 to 77 ms on main.

**Drain check (20,000 jobs, 16 workers, jobs/s, paired rounds).** A drain has no delayed jobs, so the new check never runs. Redis counted the same number of `ZRANGE` calls in both arms (about 3,150 per run) and the same number of `PUBLISH` calls (200 per run).

| Log on | Fix (rounds) | Main (rounds) | Median fix / main |
|---|---|---|---|
| host disk, fsync always (load 5 to 12) | 400, 536, 445, 391, 413, 366 | 506, 518, 278, 415, 447, 425 | 405 / 436 |
| tmpfs, fsync always (load 5 to 8) | 702, 666, 612, 741 | 676, 593, 675, 618 | 684 / 646 |

On the host disk the spread inside each arm (278 to 536) is far wider than the gap between the medians, and on tmpfs the fix comes out ahead. With identical Redis command counts, neither result shows a throughput change.

### What is left

- **PostgreSQL and SQLite:** no `NextDueAt` and no hint for delayed dispatch. A delayed job sent to a long-idle worker can still start up to the back-off cap late: 12 s with a healthy PostgreSQL listener, 2.4 s on SQLite.
- **Other cases with no wake-up on Redis:** jobs released by another process's recovery, a queue resumed by another process, and room freed by rate or concurrency limits in another process are still found by polling (05 idle caps).
- **Sub-second to 1.5 s tails under host load** come from slow store calls (fsync always on a shared disk), not from the idle path. A quiet-host rerun would separate the two more cleanly.
- The idle pool still steps up its back-off from 100 ms while it waits for a known due time, which costs about 6 readiness checks per 5 s wait. That is cheap, so it was left alone.

## PostgreSQL and SQLite: delayed jobs picked up seconds late (fixed)

Written 2026-11-19. Follows the Redis fix in perf-round-2.md ("Redis: delayed jobs picked up seconds late (fixed)"). Branch: the worktree branch of this work, commits `8f2a42c` and `c5d394f` on top of main `a86e48f`.

### Short answer

On PostgreSQL and SQLite a delayed job sent to a worker that had been idle for a while could start long after it fell due: up to the idle back-off cap, which is 10 seconds plus or minus 20 % on PostgreSQL with a working listener, and 2 seconds plus or minus 20 % on SQLite. Both stores now do what Redis does.

- The readiness check now tells the worker when the next delayed or retrying job is due, so an idle worker sleeps until exactly then.
- A delayed dispatch that becomes the earliest waiting job of its queue and priority wakes the idle worker once it has committed. The worker then runs a readiness check and learns the due time.
- Jobs queued behind an earlier delayed job wake nothing, and an empty store gets no extra polling.

On PostgreSQL, at 100 delayed offers per second, five seconds ahead, the median p99 wait past due fell from 8.2 s to 2.6 s over six paired rounds, and the worst round from 14.1 s to 4.4 s. Traced runs at the same host load show the wake arriving within 50 ms and the worker sleeping until the due time. They came in at 0.4 to 0.8 s p99. The remaining seconds in the untraced rounds came from the workers falling behind on a host with a load average of 17 to 28 (p50 up to 1.4 s, not just the first jobs), not from an idle sleep. On SQLite the old worst case was already bounded at 2.4 s, and on this host the tails of both arms (1 to 3 s at p95) come from write and fsync stalls, so the benchmark shows no clear difference. A traced run confirms the wake and the sleep until due. Drain throughput did not drop on either store.

### Cause

The worker engine was already right (perf-round-2.md, Redis section): an idle pool backs off up to its cap between readiness checks, and sleeps until `NextDueAt` when the check names one. Two pieces were missing in the PostgreSQL and SQLite stores:

1. Their readiness check never filled in `NextDueAt`. So an idle pool waiting for a delayed job could only keep backing off, up to the cap.
2. A delayed dispatch sent no wake. Only jobs that were due right away did (PostgreSQL `NOTIFY` after commit, SQLite's in-process hint). A worker that had reached the cap before the first delayed job arrived slept through its due time.

While measuring the first version of the fix, a third problem showed up. The PostgreSQL wake sender skips a send when the background connection lane (two connections, shared with readiness checks and presence) is busy, and until now it dropped the hints it held. For jobs that are due now this costs little, because the next dispatch sends again. For delayed jobs it loses the one hint that matters: the jobs queued behind the first one never send it again. In the first six benchmark rounds this left two fix rounds with multi-second tails (5.2 s and 8.4 s p99).

### Fix

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

### Tests

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

### Probe cost

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

### Benchmarks

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

### What's left

- **Paths without the delayed wake.** Delayed jobs created by staging in a caller's transaction, outbox delivery, management dispatch and schedule materialization still wake only when they are due now. On PostgreSQL these go through the transactional `rw_dispatch_core_v3`, and changing it needs a migration that replaces that function. An idle worker still learns their due time from any readiness check (`NextDueAt` now covers every job), so the worst case is one back-off step after the dispatch, up to the cap.
- **Retries by another process.** A retry is started on time by the process that scheduled it. Retries released by recovery in another process, a queue resumed elsewhere, and limit room freed elsewhere are still found by polling (05, idle caps).
- **SQLite across processes.** The hint is in-process only, as before. A worker in another process learns the due time from its next readiness check (cap 2.4 s).
- **A quiet-host rerun.** The host load average was 12 to 28 for every benchmark. A rerun on a quiet host would show how far the PostgreSQL fix rounds drop once the workers keep up (the traced runs suggest well under a second).

## Redis: worker write cuts (claim receipt compacted)

Date: 2026-11-19. Branch `worktree-agent-af18e524270a3265c`.

### Short answer

The worker path now writes about 1,200 fewer bytes to the Redis append-only log per job at a batch of 8: about 1,050 on the claim and 155 on the finish. In the drain benchmark that comes to about 16% (8,300 down to 6,970 bytes per drained job). Most of the saving comes from the claim receipt, which drops from about 1,260 to about 245 bytes per job.

Replies are byte-identical to before. A lost reply still replays to the same lease or the same result. Old and new clients can share one Redis with no migration.

Jobs per second did not change in a way we can measure. The host was heavily loaded (load 6 to 22), and script time per job is within a few percent of main. Fewer bytes did not turn into more throughput on this machine, because Lua CPU, not the log, is the limit.

BullMQ still writes about 2.5 times fewer bytes per job (2.8 KB against 6.9 KB) and drains about 2 to 4 times faster.

### Where the bytes go

Measured with `eng/bench-compare/redis-group/writes.py`: AOF bytes per job by key, for a claim of 8 jobs and a FinishMany of the same 8.

| Key | Claim before | Claim after | FinishMany before | FinishMany after |
|---|---:|---:|---:|---:|
| Receipt (`rcpt:<cmd>`) | 1,263 | 244 | 651 | 495 |
| Job hash | 889 | 862 | 735 | 735 |
| Event row | 576 | 576 | 348 | 348 |
| State index | 113 | 113 | 114 | 114 |
| Lease index | 65 | 65 | 45 | 45 |
| Attempt to job | 84 | 84 | – | – |
| Receipt expiry index | 14 | 14 | 66 | 66 |
| Terminal index | – | – | 66 | 66 |
| Event index | 42 | 42 | 24 | 24 |
| Ready index | 46 | 46 | – | – |
| Meta | 16 | 16 | 16 | 16 |
| MULTI/EXEC | 4 | 4 | 4 | 4 |

Totals per job:

| Command | Batch | Before | After |
|---|---:|---:|---:|
| Claim | 8 | 3,113 | 2,067 |
| FinishMany | 8 | 2,067 | 1,911 |
| Finish (single) | 8 | 2,526 | 2,370 |
| FinishAndClaim | 8 | 5,198 | 3,996 |
| Claim | 1 | 4,031 | 3,128 |
| Finish | 1 | 2,523 | 2,367 |
| FinishAndClaim | 1 | 6,346 | 5,287 |

At a batch of 1, the claim receipt goes from 1,627 to 751 bytes and the finish receipt from 651 to 495.

### What was cut and why it is safe

#### 1. Compact claim and finish receipts

A receipt stores the reply so that a resent command (after a lost reply) gets the same answer. Before, it stored the whole reply as JSON, with every key name spelled out for every job.

Now a claim or finish receipt stores only the values, in a fixed order, under a version marker: `{"~":1,"c":[...]}` for a claim and `{"~":1,"f":[...]}` for a finish. Inside a claim, a value that equals the previous job's value is written as `0` (most jobs in a batch share a queue, profile, worker, lease and so on). On replay, the same builder functions that made the original reply rebuild it from these values, so the replied text is byte-for-byte what the first call sent.

Safety rules:

- The stored values are the reply's own values, not values read again from the job. We checked rebuilding the reply from the job hash instead, and it is unsafe: the revision, lease, eligible time and state change after the claim, and retention can delete the job.
- Anything the builders do not cover falls back to storing the plain reply text, as before. That includes blocked, deferred, closed or fenced claim items, an unexpected key, and a value that is not text.
- A receipt with an unknown version marker is never reported as success. Replay returns `UnsupportedVersion` with `receipt_codec_unsupported`, and resolve throws `StoreIncompatibleException`. This follows spec 23 §8 (UPG-07).
- `eng/bench-compare/redis-group/diffcheck.py` runs the old and new scripts side by side, replays each command, and compares every key after expanding receipts. Result: 49 keys compared, 0 differences.

#### 2. Empty `code` and `detail` fields left out

Claim and finish receipts no longer write `code=""` and `detail=""`. Readers already treat a missing field as empty.

#### 3. Unchanged job fields not rewritten

The job-field writer now skips an HSET when the value is already the same. On the claim path this stops the `unstarted` field from being rewritten with its old value, which saves about 25 bytes per claimed job.

#### 4. Quoted-text memo (CPU only)

The JSON quoting helper keeps the quoted form of each string for the rest of the call. A claim repeats ids, states and names, and the compact codec quotes them again when it rebuilds, so this made encoding a claim about 25% faster and paid for the codec's own cost.

#### Mixed versions (rolling upgrade)

- The new code replays old plain receipts as they are stored, because plain text is passed through unchanged.
- Old code never replays a new claim or finish receipt. Those commands are resent only by the process that first sent them, and that process runs the same version.
- The old `ResolveOperation` path parses the stored result as JSON and reports "not a dispatch" for a claim, which is what it did before for any non-dispatch receipt. The new one expands the result first.
- Staged completion is PostgreSQL-only, so it is not affected.
- Nothing stored needs a migration. Old receipts expire on their usual schedule.

#### Ideas checked and dropped

| Idea | Why not |
|---|---|
| Rebuild the claim reply from the job hash | Unsafe. The job changes after the claim, and retention can delete it. |
| Delete or trim the receipt after the finish | A DEL or HSET adds log bytes instead of saving them. Receipts already live only until NotAfter plus a 10-minute floor. |
| Shorter receipt expiry | Same as above: expiry costs a write, and the floor exists for replay. |
| Drop `"actorId":null` and `"attemptId":null` from event rows | Would save about 76 bytes per job (about 1%). The layout test (`RedisDispatchLayoutTests`) pins every key, and old `EventsSince` readers tolerate it, but it is a layout change. Left for a separate decision. |
| Drop the attempt fields `token` and `closedby` | A layout change. Left alone. |
| A structural codec (key-name table) and a text-substitution codec | Both cut the bytes, but they added about 60 µs of Lua per claimed job. The builder-based codec replaced them. |

### Tests

New file `tests/Runwick.Redis.Tests/RedisCompactReceiptTests.cs` (spec row REDIS-20):

- RC-01 runs the codec directly in Redis Lua: a 4-item claim with awkward texts, an absent timeout after a present one, and repeated values; two finishes; fallbacks for an extra key, a non-text value and a decision without lists; an unknown version giving nil; and plain text passed through unchanged.
- RC-02 checks that claim, Finish and FinishMany write compact receipts, that a resend returns a reply equal to the original, that a reply lost after commit replays correctly, and that `ResolveOperationAsync` expands the result.
- RC-03 writes plain receipts the way the old version did, then has the new scripts replay them for claim, Finish, FinishMany and FinishAndClaim.
- RC-04 plants receipts with version 2 and expects `UnsupportedVersion` / `receipt_codec_unsupported` on replay, `StoreIncompatibleException` from resolve, and the job left as it was.

Full runs against Redis 8.2 with `--appendonly yes --appendfsync always --aof-load-truncated no --maxmemory-policy noeviction --save ''`:

| Suite | First codec (14297cf) | Final codec (319ef10) |
|---|---|---|
| Core | 454 of 454 | 454 of 454 |
| Contract | 496 of 496 | 496 of 496 |
| Redis | 128 of 128 | 128 of 128 (2,466 s) |

### Benchmarks

Paired against main, arms rotated each round, with `pairs-combined.sh` under `flock /tmp/runwick-bench.lock`. The arms are `base` (this branch), `buf` (this branch with buffered finish), `old` and `oldbuf` (main through `BASE_BIN`), and BullMQ. Each log type ran 4 rounds. Host load ranged from 6 to 22, so treat single rounds as noisy and read the medians and paired ratios.

#### Drain, final codec

| Log | Arm | Jobs/s per round | Median | Script ms/job | CPU ms/job | AOF bytes/job |
|---|---|---|---:|---:|---:|---:|
| disk | base | 287, 386, 418, 370 | 378 | 1.15 | 1.37 | 6,967 |
| disk | buf | 540, 494, 701, 401 | 517 | 0.98 | 1.12 | 6,827 |
| disk | old | 380, 457, 403, 351 | 391 | 1.12 | 1.30 | 8,298 |
| disk | oldbuf | 419, 414, 666, 535 | 477 | 1.01 | 1.13 | 8,167 |
| disk | BullMQ | 1229, 1605, 1179, 912 | 1,204 | 0.29 | 0.45 | 2,815 |
| tmpfs | base | 580, 701, 820, 580 | 641 | 1.09 | 1.16 | 6,984 |
| tmpfs | buf | 924, 852, 859, 791 | 855 | 0.92 | 0.96 | 6,820 |
| tmpfs | old | 677, 661, 778, 622 | 669 | 1.04 | 1.13 | 8,315 |
| tmpfs | oldbuf | 957, 994, 858, 744 | 907 | 0.87 | 0.92 | 8,158 |
| tmpfs | BullMQ | 2078, 2419, 3640, 3403 | 2,911 | 0.21 | 0.27 | 2,815 |

Paired ratios (new over main, same round):

| Log | Pair | Per round | Median |
|---|---|---|---:|
| disk | base / old | 0.75, 0.84, 1.04, 1.06 | 0.94 |
| disk | buf / oldbuf | 1.29, 1.19, 1.05, 0.75 | 1.12 |
| tmpfs | base / old | 0.86, 1.06, 1.05, 0.93 | 0.99 |
| tmpfs | buf / oldbuf | 0.97, 0.86, 1.00, 1.06 | 0.98 |

The bytes drop by 16% in every arm. Throughput moves both ways from round to round and the medians sit within noise of 1.0.

The first (structural) codec's run, before it was replaced, showed the same byte cut (8,311 down to 7,065 on disk) and the same pattern of throughput within noise, but with script time about 10% higher. That led to the builder codec.

#### In-script CPU

`profile_worker.py`, rotated, median µs per job:

| Batch | Step | Main | This branch |
|---:|---|---:|---:|
| 8 | Claim | 346 | 340 |
| 8 | FinishMany | 275 | 263 |
| 8 | Both | 508 | 503 |
| 1 | Claim | 1,060 | 1,060 |
| 1 | Finish | 753 | 788 |

The k=1 finish difference is within this host's run-to-run noise. The variants without the memo or without compact receipts measured 750 and 762.

#### Delayed jobs (delayed100, 2 rounds, means)

| Measure | main | this branch | BullMQ |
|---|---:|---:|---:|
| Completed per second | 100 | 100 | 98.4 |
| Pickup p95 (ms) | 142 | 106 | 256 |
| Pickup p99 (ms) | 339 | 225 | 414 |
| Enqueue p99 (ms) | 255 | 151 | 177 |
| Offer to finish p99 (ms) | 5,375 | 5,291 | – |

Delayed pickup is not worse.

### What's left

- **Event rows** are the next biggest write after the job hash (576 bytes per claimed job, 348 per finished one). Dropping null `actorId` and `attemptId` would save about 76 bytes per job. That needs a decision on the layout test and on outside readers.
- **The job hash** (about 860 and 735 bytes) is mostly field names and fresh values. Shorter field names would be a real layout migration.
- **Attempt fields** `token` and `closedby` could go, but that is also a layout change.
- **The gap to BullMQ** is about 4 KB per job and about 0.8 ms of Lua per job. Byte cuts alone will not close the throughput gap on this host. Lua CPU per job is the limit, so the next round should target script work (fewer index updates, fewer events per step) rather than log bytes.
- **summarize_steady.py** crashes in its ratio section on delayed100 results (`KeyError: 'off'`). The per-metric lines print fine.

## Scheduler S: P-06 lag under 13 s (set-based schedule groups)

Written 2026-11-19 and 2026-11-20. Plan item "Scheduler S" in `docs/plans/perf-plan-2-remaining.md`: P-06 scheduler lag p99 ≤ 13 s with 1,000 minute schedules at the default 100 jobs/s allowance, measured from the scheduled instant to durable materialization, with every occurrence accounted for.

### Short answer

**The 13 s gate is met, by main and by this branch, on this host (8 CPUs, not the reference host).** Across 20 paired runs on the branch, the worst p99 was 12.85 s (groups of four, 1-minute load average about 10) and 10.89 s with groups of eight. Main met the gate in 15 of its 16 runs; the exception was a group-of-eight run during a burst of other activity on the host (load average median 28, peak 108), where it fell 520 occurrences behind and its p99 went past 60 s. The branch's group-of-eight run in the same round, at load median 21 and peak 37, kept p99 at 10.58 s.

What changed is the database cost of a schedule group. Migration 0026 writes a whole group with one statement per table instead of running the single-plan path once per plan. Server time per plan fell from 6 to 11 ms to 2 to 3 ms with groups of eight, and from 8 to 11 ms to 4 to 5.6 ms with groups of four. WAL per plan did not change (about 5.5 to 6 kB). Lag p99 hardly moves on a calm host, because it is pinned near 10 s by the 100 jobs/s allowance (1,000 schedules ÷ 100/s), not by the database. The cut buys headroom: a group of eight now needs about 20 ms of database time per 80 ms of allowance, against 50 to 90 ms before.

Every occurrence was accounted for in every run. There were no duplicates, no orphan jobs and no shared jobs. Every expected occurrence was materialized, apart from the 520 in the main run above, which are counted as unmaterialized. Every catch-up completed without needing review.

### Where the time goes

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

### What changed and why it is safe

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

### Tests

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

### Benchmark tables

All runs used P-06s: 1,000 minute schedules, the default 100 jobs/s allowance, two workers, grouping on with two groups in flight, and a fresh PostgreSQL 18.6 container per run (`fsync` and `synchronous_commit` on). Main is 86c0407, the commit before 0026. The branch was measured at its key-array fix, and the rebase onto 997b8a0 changed nothing in the measured path. Arm order was rotated each round, and every run held `flock /tmp/runwick-bench.lock`. "Server ms/plan" is the top-level `pg_stat_statements` time of the group function divided by the plans it wrote, so unlike the nested profile above it includes everything. Load is the 1-minute load average on this 8-CPU host, sampled every 30 s. No run saw an evaluation-budget failure.

#### Full runs: 15-minute steady window plus a 1,000-occurrence catch-up

| Arm | Load (median, max) | Acknowledged | p50 | p95 | p99 | Server ms/plan | WAL/plan | Catch-up, restart to last occurrence |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| main, groups of 4 | 3.5, 7.7 | 15,000 / 15,000 | 5.28 s | 10.27 s | 10.86 s | 7.74 | 5,922 B | 41 s |
| main, groups of 8 | 4.4, 9.7 | 15,000 / 15,000 | 5.34 s | 10.32 s | 11.23 s | 8.17 | 6,075 B | 45 s |
| branch, groups of 4 | 3.3, 7.1 | 15,000 / 15,000 | 5.35 s | 10.36 s | 11.24 s | 4.18 | 5,991 B | 52 s |
| branch, groups of 8 | 2.6, 12.3 | 15,000 / 15,000 | 5.28 s | 10.34 s | 10.86 s | 2.36 | 6,248 B | 41 s |

Each catch-up emitted all 1,000 missed occurrences (1,001 to 1,005 including occurrences due during the restart), in chunks of at most 32, with `needsReview` false. All four arms had 0 duplicates, 0 orphan jobs, 0 shared jobs and 0 violations.

#### Short paired runs: 2-minute warm-up, 3-minute window, three rounds

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

#### Synthetic load: eight busy-loop processes beside each run

Same short shape, two rounds, with eight CPU-bound processes started for each run (load median 17 to 33):

| Arm | p99, round 1 | p99, round 2 | Server ms/plan | Load median |
|---|---:|---:|---:|---|
| main, 4 | 10.95 s | 10.39 s | 10.7, 10.5 | 26.4, 19.1 |
| main, 8 | 10.17 s | 10.37 s | 10.1, 10.4 | 28.0, 17.5 |
| branch, 4 | 10.12 s | 10.11 s | 4.5, 5.6 | 31.9, 19.0 |
| branch, 8 | 10.01 s | 10.25 s | 2.3, 2.8 | 32.9, 25.5 |

Pure CPU load roughly doubles main's per-plan cost, but it still fits in the allowance, so both builds pass. The one failure (main, round 3 above) came with a load spike far above the core count, which points to I/O or memory pressure from other work on the host rather than CPU alone. I can't reproduce that on demand, so treat it as one data point, not a rate.

### What's left

- **Reference-host run.** These numbers come from a shared 8-CPU development host. A run on the reference host would confirm the gate where it is officially judged.
- **Group size default.** `MaterializationGroupSize` stays at 4 and `GroupMaterializations` stays off by default. Groups of eight now cost about half as much per plan as groups of four. Whether to turn grouping on by default, or raise the size, is a separate decision; I changed no defaults.
- **Replica claim (step 4).** Not built. The database is no longer the limit at the default allowance, so an opt-in replica claim isn't needed for this gate.
- **The fallback path.** Replays, expiry, existing occurrences and server fault hooks still go through version 2 one plan at a time, at the old cost. They are rare in steady operation.
- **SQLite and Redis** are unchanged. This work is PostgreSQL only.

## Outbox D: the relay at four slots (P-09)

Written 2026-11-20. Every number here was measured on the shared host (8 cores) while other sessions ran their own work; the load average during the measure windows was 7 to 29. PostgreSQL 18 ran in a local container with fsync, synchronous commit and full-page writes on, with source and destination as separate databases on the same server. This is not the reference host.

### Short answer

The relay was mostly waiting, not doing database work. At four slots a freed slot sat idle until the claim loop came round, the claim loop made one source claim per freed slot, and each claim read and sorted the whole backlog, so the slower it got the more backlog there was. A slot that did get a message then held it through the destination commit and the source mark.

The fix has four parts: the relay now claims ahead (claimed messages wait beside busy slots, so a freed slot starts its next message at once), the source claim walks the indexes in order instead of sorting the backlog, a slot carries one destination call that can hold a group of up to four messages, and slot release on reply and batched marks are on by default. No schema change and no migration.

**P-09 at four slots is met on this host at load average 7 to 13**: 97.6 to 99.8 confirmed deliveries/s against the offered 100/s (every staged message confirmed in the window), ready-to-confirm p99 1.13 to 1.41 s, no backlog. On the same host main confirmed 26 to 54/s and its backlog grew by 50 to 75 messages a second. Under heavier load (average 16 to 29) the new relay still kept up with the offered rate, but p99 was 2.1 to 2.3 s, just over the 2 s line. When the host was saturated (round 13, load average about 48, so busy that the business writers could stage only 72 of the offered 100/s) the new relay fell behind: 62.8 confirmed/s with p99 33 s, though it drained its backlog; main in that round confirmed 46.8/s with p99 183 s and did not drain. So P-09 holds at moderate load on this shared host, not under saturation, and a reference-host run is still needed. At 16 slots the new relay confirmed 96 to 100/s with p99 1.2 s, against main's 74.8/s with p99 115 s. The paired tables below give all of it.

### Where the time goes

Traced P-09 runs on main (4 slots, 100 offers/s, 2-minute measure window; the relay's own timers plus `pg_stat_statements` with nested tracking). Load average 18 for the first run, 23 to 47 for the second.

| Step, per delivery | Main, load 18 | Main, load 23 to 47 |
|---|---:|---:|
| Confirmed deliveries/s | 51.5 | 32.7 |
| Source claim (round trip) | 24.7 ms, 1.43 messages per claim | 34.8 ms, 1.37 messages per claim |
| of which statement time in PostgreSQL | 13.4 ms, 2,047 buffers read per claim | 17.7 ms, 1,817 buffers per claim |
| Claim loop busy | 89% of the window | 83% of the window |
| Build the dispatch command (import, checks, fingerprint) | under 1 ms | 6.1 ms mean, 0.26 ms median |
| Destination `ReceiveOutbox` round trip | 31.3 ms (function 16.6, commit 14.4) | 50.8 ms (function 29.7, commit 18.0) |
| Source delivered mark | 10.9 ms (1.9 ms waiting for the connection) | 18.9 ms (4.9 ms waiting) |
| Time a message holds its slot | about 43 ms | 75.8 ms |
| Share of slot time in use | about 55% | 62% |

Reading it against the budget of 40 ms per delivery per slot:

- **Slots were idle 38% to 45% of the time.** A slot freed when its mark finished, then waited for the single claim loop, which was busy 83% to 89% of the time making claims of one or two messages. That is waiting, not work.
- **Each claim read the whole backlog.** The claim's `WHERE (state = 'pending' AND …) OR (state = 'leased' AND …) ORDER BY next_attempt_at` could not use either partial index in order, so PostgreSQL read every due row and sorted it. With a backlog of a few thousand rows that was about 2,000 buffers and 13 to 18 ms per claim, growing with the backlog: a feedback loop that turned a small shortfall into minutes of delay.
- **A busy slot took 43 to 76 ms per message**, already over the 40 ms budget on its own, because it held the slot through the destination commit (14 to 18 ms of fsync wait) and the source mark (11 to 19 ms).
- The database work itself was small: the destination function took 12 to 17 ms of PostgreSQL time per message and the mark 0.4 to 0.5 ms. Poll waits played no part on main, because there was always a backlog.

### What changed and why it is safe

1. **Claim ahead.** `MaxInFlight` now counts slots, that is destination calls in progress at once. The relay keeps up to twice what the slots carry leased (8 messages with a destination that cannot group, 32 with groups of four), counting messages that are waiting for their mark. Claimed messages wait in order, leased and renewed, and a slot that frees starts the next one at once. The claim asks for the room left under that cap, so claims grew from 1.4 to about 3.7 to 4.3 messages each. A busy relay whose claim came back short re-claims when a lease ends instead of waiting out the 1 s idle poll; an idle relay polls exactly as before.
2. **Index-ordered claims.** The PostgreSQL claim reads `pending` rows in `next_attempt_at` order and expired leases in `lease_expires_at` order, each from its own partial index, with the clock read once as an index bound, and leases the oldest `max` of the two. It still locks with `FOR UPDATE SKIP LOCKED`. Claim statement time fell from 13 to 18 ms to 0.7 to 1.3 ms, and buffers per claim from about 2,000 to 33 to 72. SQLite uses the same two ordered branches. The existing indexes serve both, so there is no schema change and no migration.
3. **A slot carries one destination call, and groups of four are the default.** With a PostgreSQL destination a freed slot takes up to `DeliveryGroupSize` waiting plain messages (now 4 by default) as one destination transaction. Groups now form from the waiting messages, so they averaged 3.7 to 4 messages instead of the 2.3 to 2.9 that groups from a single claim managed. SQLite and Redis destinations ignore the setting, as before.
4. **`ReleaseSlotAfterReply` and `BatchDeliveredMarks` are on by default.** With claim ahead, slots turn over faster than one mark statement per message can follow on the single control connection. Marks averaged about 3 per statement (4.6 in the batched statements, single statements when only one mark was waiting), and releasing the slot on the destination's reply keeps the mark off the slot. The paired runs show each step helps (see the tables).
5. A renewal that races its own message's mark (the mark won) is no longer counted as a lease loss. Earlier runs counted 20 to 260 of these; the traced runs of the final code count 0.

Why the safety rules hold:

- **Token-guarded confirmation.** Unchanged. Each message is marked under its own lease token; a batched mark guards every row with its own token.
- **Transport identity and receipts.** Unchanged. Each message keeps the transport command assigned at claim, and a group sends each command with its own ID, receipt and mapping. A waiting message checks its lease lifetime before its first send, like any other.
- **Leases.** Waiting messages are in the relay's in-flight set, so they are renewed and shutdown waits for them. After the host deadline nothing new starts and their leases expire and are reclaimed.
- **Lost replies, reclaims, mixed outcomes.** Handled by the same code as before: a lost destination reply resends the same command, a lost mark is repaired by the delivery mapping, a refused group falls back to single sends with the same commands.
- **No duplicate effects.** Every paired run reconciled every staged message to exactly one delivery mapping and one destination job, with no second mapping and no mark mismatch.

### Tests

New tests in `tests/Runwick.Outbox.Tests/ClaimAheadTests.cs` (7 runs, both source adapters where it applies):

- claimed messages wait for a slot: never more than 2 sends at once at `MaxInFlight = 2`, never more than 4 leases, and at least one message did wait; 30 messages, 30 jobs, 30 mappings;
- a relay that dies with two sends hung and two claimed messages waiting: nothing is marked, a new relay takes the pending ones at once and the four leased ones after their leases expire, each delivered once with one mapping;
- mixed outcomes in one claim: one malformed message blocks, two destination replies are lost after commit (resent and replayed), one mark fails before the write and one after it; 7 jobs, 7 mappings and exactly 7 receipts (resends and reclaims reused each message's transport command);
- with groups on, a slot carries one destination group: at most 2 destination calls at once at `MaxInFlight = 2`, groups of 3 or 4 formed from waiting messages, 24 jobs and 24 mappings.

OUTBOX-07 now checks in-flight and claim batch against the relay's lease cap instead of 4.

Results:

On the merged code (2026-11-20): Outbox 70 of 70 (with a Redis server, so the Redis-store cleanup tests ran), Core 455 of 455 (after renaming the `MaxInFlight` row the defaults test reads), Contract 502 of 502, PostgreSQL 406 of 406.

### Benchmarks

P-09 workload (`tests/Runwick.Workloads`, `run P-09`): 8 business writers staging Poisson arrivals at 100/s with 1 KiB intents, 1 minute of warm-up, 3 minutes of measure, no injected mark loss in the timing runs, 3 minutes of drain, a fresh database pair per run. Every run held `flock /tmp/runwick-bench.lock`. Arms ran one after another in each round, order rotated between rounds. Load is the 1-minute load average at the start of the measure window and the mean of samples every 10 s during it. Note that the workload's relay wraps the source in a fault-injection adapter that has no wake listener, so every arm polls the source instead of being woken by notifications; a production PostgreSQL source would be woken.

#### First series, 4 slots: which part helps

`main` is the current main; `claim ahead` is claim ahead, index-ordered claims, slot release and batched marks, sending each message alone; `claim ahead, old options` is the same with slot release and batched marks off; `+ groups of 4` adds groups of four. (This series ran before the last fix, which re-claims on lease end instead of waiting out the idle poll.)

| Round (load) | Arm | Confirmed/s | p50 | p95 | p99 | Backlog |
|---|---|---:|---:|---:|---:|---|
| 1 (23 to 29) | main | 25.8 | 229 s | 271 s | not reached | grew, 10,210 unconfirmed at end |
| | claim ahead, old options | 58.2 | 94 s | 107 s | 110 s | grew, drained after the writers stopped |
| | claim ahead | 72.9 | 22 s | 62 s | 64 s | grew, drained |
| | + groups of 4 | 99.6 | 0.87 s | 1.81 s | 2.29 s | none |
| 2 (19 to 26) | main | 42.3 | 150 s | 247 s | not reached | grew, 7,093 unconfirmed at end |
| | claim ahead, old options | 50.8 | 144 s | 161 s | 161 s | grew, drained |
| | claim ahead | 84.2 | 33 s | 39 s | 40 s | grew, drained |
| | + groups of 4 | 95.8 (staged 95.9) | 0.83 s | 1.68 s | 2.22 s | none |
| 3 (11 to 22) | main | 53.6 | 101 s | 160 s | 161 s | grew, drained |
| | claim ahead, old options | 72.7 | 50 s | 85 s | 85 s | grew, drained |
| | claim ahead | 102.0 | 14 s | 25 s | 26 s | grew early, drained |
| | + groups of 4 | 96.6 (staged 96.3) | 0.78 s | 1.62 s | 2.12 s | none |

#### Second series, 4 slots: main against the final code

Arm order rotated.

| Round (load) | Arm | Confirmed/s | p50 | p95 | p99 | Backlog |
|---|---|---:|---:|---:|---:|---|
| 11 | final (load 9.3, mean 10.0) | 99.8 (staged 99.7) | 0.32 s | 1.00 s | 1.13 s | none |
| | main (load 19.5, mean 18.5) | 49.8 | 141 s | 178 s | 178 s | grew, drained |
| 12 | final (load 10.2, mean 12.0) | 99.7 (staged 99.8) | 0.17 s | 0.94 s | 1.17 s | none |
| | main (load 7.0, mean 10.0) | 78.4 | 14.8 s | 49 s | 50 s | grew, drained |
| 13 | final (load 27.2, mean 47.8) | 62.8 (staged only 72.4) | 12.7 s | 30.2 s | 33.0 s | grew, drained |
| | main (load 11.6, mean 22.8) | 46.8 (staged 99.7) | 139 s | 182 s | 183 s | 105 unconfirmed, not drained |

16 slots, same workload:

| Round | Arm | Confirmed/s | p50 | p95 | p99 | Backlog |
|---|---|---:|---:|---:|---:|---|
| 1 | final (load 32.9, mean 30.4) | 100.4 | 0.14 s | 0.81 s | 1.19 s | none |
| | main (load 16.0, mean 20.9) | 74.8 | 55 s | 113 s | 115 s | grew, drained |
| 2 | final (load 12.9, mean 15.5) | 96.2 (staged 96.2) | 0.15 s | 0.78 s | 1.22 s | none |
| | main (load 12.0, mean 14.8) | 65.5 | 67 s | 95 s | 98 s | grew, drained |

Above the target, 4 slots at 120 offers/s (final code, load 9.1, mean 10.1): 119.3 confirmed/s, p50 0.17 s, p95 1.02 s, p99 1.35 s, no backlog, 0 lease losses.

Every run: 0 violations, no second mappings, destination jobs equal to mappings; final code 0 lease losses.

### What's left

- **The 2 s tail under heavy load.** With the host at load 16 to 29 the p99 sat at 2.1 to 2.3 s although throughput kept up. The traced runs put the remaining time in destination commits (fsync wait of 20 to 50 ms per group on this host) and in the mark connection; a quiet or reference host should be checked before calling P-09 met in general.
- **A reference-host run** with separate source and destination servers, as 14 §9.4 describes, and the full 15-minute measure with three repetitions.
- **SQLite was not measured.** The P-09 workload has only a PostgreSQL source. The SQLite source gets the same relay changes and the same ordered claim, but the SQLite destination does not group, so a slot there is one message; whether that meets 100/s is untested.
- **The workload's relay has no wake listener** (its fault-injection wrapper does not forward it), so these numbers are for a polling relay. Forwarding it would make the workload match a production PostgreSQL source.
- Redis destinations still send each message alone.

Raw results: the run directories under the session scratchpad (`runs/s4-r*`, `runs/s16-r*`, `runs/*-trace*`), each with `out/result.json`, the relay summary, load samples and a `pg_stat_statements` snapshot for its measure window.

## Redis: worker script cuts (script work per job)

### Short answer

The Lua scripts a Redis worker runs to claim and finish jobs now do about a fifth less work per job. Nothing stored changed: job hashes, event rows, receipts, AOF bytes and every reply, including replays after a lost reply, are byte for byte what main writes. No migration, and old and new binaries can run side by side.

On a disk log, paired against main, buffered-finish drains are 1.20× faster (median of four rounds). With the AOF on memory, both arms gain 1.16×. Redis script time per drained job fell 12% to 18%.

BullMQ is still about three times faster. Runwick with buffered finish reaches 0.39× of BullMQ's drain rate on disk (main: 0.33×) and 0.33× on memory (main: 0.29×). The 90% target is not met. The cost that remains is the work Runwick does per job and BullMQ does not: receipts for exact replay, attempt records, event rows and the state indexes.

### Where the script time goes

Measured with `eng/bench-compare/redis-group/profile_worker.py`, which times each phase inside the script on a quiet Redis. The figures are CPU microseconds per job at eight jobs per call.

| Path | main | this change |
|---|---:|---:|
| Claim (ReserveAndBegin) | 403–422 | 313–342 |
| Finish (FinishMany) | 305–349 | 270–288 |
| Claim + finish | 672–801 | 506–574 |

At sixteen jobs per call, the claim went from 327–374 to 314–334 µs and the finish from 272–288 to 245–247 µs.

These are the claim phases that moved, in µs per call of eight jobs:

| Phase | main | this change | What it is |
|---|---:|---:|---|
| Reply encoding | 516 | 10 | Turning the claimed jobs into the JSON reply |
| Events | 216 | 63 | Building the reserved and started event rows |
| Size bound | 104 | 10 | Upper bound of the packed intent size (§4.3) |
| Payloads | 144 | 58 | Reading each claimed job's headers and created time |

The time that is left is mostly fixed per call, about 150 µs before any job is touched: setup, reading `meta` and the receipt lookup. After that come the index updates and receipts for each job. Redis commands themselves are only about 11% of script time; the rest is Lua work. Lua garbage collection does not matter (pausing it changed nothing).

Redis commands per claimed job went from 7.1 to 6.2. BullMQ's finish-and-fetch is about 25 commands per job, but each is cheap: it has no receipts, no attempt records, no state index by created time, one event instead of three, and it replies with a raw `HGETALL` instead of JSON.

### What was cut and why it is safe

- **Writing JSON texts directly.** The claim item, the finish result, the FinishMany item reply and the details of the `attempt_reserved`, `attempt_started` and attempt-closing events used to be built as Lua tables and run through the generic encoder, which sorts keys and checks every value's type. When the values have their usual types, they are now written as text in the order the encoder would give. Any other value falls back to the encoder, so the output is the same either way. Test SC-01 compares each writer with the encoder on usual and awkward values: quotes, control bytes, non-ASCII, raw JSON, missing fields and wrong types.
- **One encode per reply.** A claim's reply text is built once and reused for its receipt and for FinishAndClaim's claim receipt. Before, it was encoded up to three times. Compact receipts (REDIS-20) rebuild replays with the same writers, so a replay gives the same bytes.
- **No second pass for the size bound.** The packed-size upper bound is summed while the writes are listed. The existing DP-04 test still checks that the bound is never below the real packed size.
- **No second read of the job.** A claimed job's payload fields come from the job hash the claim had already read, not from another `HMGET`. The first page of candidates asks for the claim size plus four instead of 100.
- **No full sort in FinishMany.** Receipts are checked in item order. Only the confirmations written for replayed items are sorted, using the byte comparator (REDIS-11).
- **Fewer lookups.** Writes to a job's fields and to the event rows find their write record once per call, not once per field. The key list of a key set is built only the first time that set is seen.

Not changed: the job-hash and event-row layouts, receipts, the intent format, direct projection and recovery. Because the stored data is identical, the AOF/fsync profile is unchanged too.

### Tests

- `eng/bench-compare/redis-group/diffcheck.py` runs main's scripts and these scripts side by side on the same data. It now also covers a mixed claim (applied with a fencing scope, capacity blocked, invalid state, budget exhausted), a single reserve, a begin and a finish, each sent twice so the replay is compared. All 75 keys and every reply are byte-identical.
- New `RedisScriptCutTests` (REDIS-21):
  - SC-01: the writers against the encoder.
  - SC-02: a FinishMany with items out of order and items repeated from an earlier batch. Only the repeats replay and get confirmed, and every state is right.
  - SC-03: a claim whose reply is lost replays exactly. The claim is followed by finishes of every outcome (succeeded, permanent failure, retry with a delay, cancelled), and all 16 event rows hold canonical details with the expected keys.

  A deliberate key-name change in a writer was checked to fail both SC-01 and SC-03.
- Full suites against Redis 8.2 with `appendfsync always`:
  - Runwick.Redis.Tests: 131 of 131 passed.
  - Runwick.Core.Tests: 455 of 455 passed.
  - Runwick.Contract.Tests: 501 of 502 passed. The one failure was `A_cpu_burning_allocating_handler_records_plausible_usage(Sqlite)`, a CPU-time check on SQLite that this change does not touch, run while the host load was high. It passed when rerun alone on a quiet host (load 3.5).

### Benchmarks

Each run drained 20,000 jobs with 16 workers on a fresh Redis 8.2 (`appendfsync always`, AOF on). Arms ran in rotating order inside one round, and every round held the shared bench lock. `base` and `buf` are this change without and with buffered finish; `old` and `oldbuf` are main. Script ms/job is Redis's own wall time inside scripts, divided by jobs. The host was shared, with load averages between 4 and 36, so the paired ratios within a round are the numbers to trust.

#### Disk log (fsync about 7 ms)

| Arm | jobs/s per round | median | script ms/job | AOF B/job |
|---|---|---:|---:|---:|
| old (main) | 334, 370, 302, 208 | 318 | 1.36 | 6,981 |
| base | 326, 289, 329, 318 | 322 | 1.20 | 6,978 |
| oldbuf (main) | 525, 486, 345, 344 | 415 | 1.17 | 6,832 |
| buf | 623, 593, 386, 435 | 514 | 0.96 | 6,827 |
| BullMQ | 1569, 762, 1048, 1117 | 1,082 | 0.35 | 2,815 |

| Paired ratio | per round | median |
|---|---|---:|
| base / old | 0.98, 0.78, 1.09, 1.53 | 1.03 |
| buf / oldbuf | 1.19, 1.22, 1.12, 1.27 | 1.20 |
| buf / BullMQ | 0.40, 0.78, 0.37, 0.39 | 0.39 |
| oldbuf / BullMQ | 0.33, 0.64, 0.33, 0.31 | 0.33 |

Without buffered finish, the disk-log drain is limited by fsync waits rather than script time, so the cut does not show there.

#### AOF on memory (FSYNC=tmpfs)

| Arm | jobs/s per round | median | script ms/job | AOF B/job |
|---|---|---:|---:|---:|
| old (main) | 485, 464, 409, 439 | 452 | 1.47 | 6,984 |
| base | 488, 548, 466, 526 | 507 | 1.30 | 6,988 |
| oldbuf (main) | 572, 609, 544, 605 | 588 | 1.27 | 6,823 |
| buf | 786, 659, 673, 584 | 666 | 1.10 | 6,821 |
| BullMQ | 1826, 2155, 1861, 2223 | 2,008 | 0.30 | 2,815 |

| Paired ratio | per round | median |
|---|---|---:|
| base / old | 1.01, 1.18, 1.14, 1.20 | 1.16 |
| buf / oldbuf | 1.37, 1.08, 1.24, 0.96 | 1.16 |
| buf / BullMQ | 0.43, 0.31, 0.36, 0.26 | 0.33 |
| oldbuf / BullMQ | 0.31, 0.28, 0.29, 0.27 | 0.29 |

AOF bytes per job are unchanged, as expected, since nothing stored changed.

#### Delayed pickup (delayed100)

Same arms (old is main, base is this change, no buffered finish) and BullMQ, four rounds, 100 jobs/s each delayed 5 s, for 30 s after a 12 s warm-up. Pickup is the time from when a job falls due to when its handler starts (for BullMQ, its own delayed latency). Load averages ranged from 2.6 to 10.5.

| Round | old p95 / p99 (ms) | base p95 / p99 (ms) | BullMQ p95 / p99 (ms) |
|---|---|---|---|
| 1 | 193 / 342 | 328 / 569 | 356 / 639 |
| 2 | 788 / 1,166 | 488 / 664 | 252 / 409 |
| 3 | 497 / 775 | 377 / 566 | 241 / 387 |
| 4 | 3,258 / 3,734 | 450 / 683 | 610 / 788 |
| Median | 643 / 971 | 414 / 617 | 304 / 524 |

Pickup is not worse: base won three rounds of four, and its median p99 is 617 ms against main's 971 ms. It is within about 18% of BullMQ on p99, not within the 10% target. Enqueue p99 was noisy, with base worse in two rounds, equal in one and better in one (median 372 ms against 305 ms). The dispatch path shares only the prelude changes (the size bound and the event and job write records), so this is most likely host noise, but it was not rerun.

### What's left

- **The 90% target is far off.** BullMQ uses about 0.3 ms of script time per job, against Runwick's 1.0 to 1.1 ms. What remains is the model, not encoding: about 150 µs fixed per call (setup, `meta`, receipt lookup), then per job the receipt, the attempt record, three event rows and the state indexes. Closing the gap means doing less of that per job, which is a design decision. Options include batching receipts per call instead of per job, or one event row per step instead of three. Both change stored data.
- **Smaller cuts looked at and not made:**
  - reading only the needed job fields (`HMGET`) instead of `HGETALL`;
  - skipping `ZREM`s that remove nothing;
  - caching the per-call setup;
  - restructuring compact receipts;
  - reading `meta` with fewer fields.

  Each looked too small to pay for its risk, which for some was to mixed versions.
- The event-row null fields and the job-hash field names still need a layout decision (unchanged from the write-cuts report).

## Claims in flight (claim pipelining)

### Short answer

- A worker pool can now keep more than one claim waiting for the store at once: `WorkerOptions.ClaimsInFlight`, 1 to 4. **The default stays 1**, which is exactly the old behaviour.
- With **2 in flight**, a Redis drain at 16 slots with buffered finish ran **1.18× faster on a disk log and 1.30× faster with the log in memory** (median of paired rounds against main). PostgreSQL drained **1.08× faster** (1.13× with 4 in flight).
- **SQLite did not gain** (0.98× at 2, 0.73× at 4). Its single writer makes overlapping claims wait for each other.
- On Redis, **delayed jobs were picked up a little later** with 2 in flight: p95 higher in all 4 rounds, p99 median 399 ms against 310 ms. Throughput was the same.
- Ratio to BullMQ on Redis (16 slots and concurrency 16, buffered finish): disk **0.30 → 0.52**, memory **0.30 → 0.45**. The host was heavily loaded during these runs (load 9 to 72), so the absolute numbers are low for every arm. The paired ratios are the useful figures.
- So the option helps where a pool waits on claim round trips (Redis, PostgreSQL), costs pickup delay, and hurts SQLite. It is not on by default.

### What changed

Before, a pool sent one claim (`ReserveAndBegin`, which claims and starts up to 32 jobs in one call) and waited for its reply before it looked for more work. Slots that freed up during that wait stayed empty until the reply came back.

With `ClaimsInFlight = 2` or more:

- The pool sends a claim and goes on. Slots that free up while it waits can be claimed by another request at once. Replies are applied one at a time, under the same lock as before.
- **A request sent next to another must be large.** It is sent only when at least `target ÷ ClaimsInFlight` slots are free (8 of 16 at 2). The first version had no such floor. It cut the jobs per claim from 8.4 to 5.3 (2 in flight) and 3.1 (4 in flight), raised Redis script time per job by about 20%, and was no faster on Redis (disk: 378 jobs/s main, 358 at 2, 349 at 4). With the floor, claims stay near main's size (about 2,700 claims per 20,000 jobs against 2,420).
- Only the simple case overlaps: one queue and one priority have work. When several lanes have work, the pool claims one job at a time and waits, as before.

The spec is 05 §9.7 (Changes 2026-11-20); the default and the new rule V-25 are in 06.

### Why it is safe

| Guarantee | How it is kept |
|---|---|
| Never claim more than free slots | Each request takes slot tickets before it is sent and asks for exactly that many jobs. There is no claim-ahead: prefetch stays 0. A test checks that the jobs asked for by the requests in flight plus the slots held never exceed the pool size, at 1, 2 and 4 in flight. |
| Lane fairness and aging | Overlap happens only when one lane is eligible; any choice between lanes waits for its own reply. Share counters are updated when a reply is applied. The CAP-02 fairness case (a hidden queue returns with exactly 1 of 4 admissions) passes at 1 and 2 in flight. |
| Per-queue concurrency and rate limits | Enforced by the store per claim, unchanged. Each claim is its own command with its own receipt. |
| Pool target and slot accounting | Unchanged tickets; every reply path returns the tickets it did not use. Tests end with zero outstanding tickets and zero claims in flight. |
| Cancellation and shutdown | Drain stops new requests. Shutdown waits for every request in flight and for its reply to be applied. A request still waiting for an I/O slot when admission closes is not sent. Tested with a claim held at the store until the drain begins. |
| Lease timing of claimed jobs | Every claimed job becomes a tracked attempt as soon as its reply is applied, and its lease deadline counts from when the request was sent. A reply that waits for the lock shortens the deadline instead of stretching it. No claimed job waits for a slot. |
| Lost or slow replies | Each request resolves its own command by resending it with the same ID, as before. The pool stops claiming while any of its requests is unresolved past the resolution budget. Tested with a lost reply and with a request that hangs until its timeout while another is in flight; every job ran exactly once. |
| Idle back-off and wake hints | A request sent without waiting does not reset the idle back-off; an applied reply does, as before. A "no work" reply sets the lane aside only under the readiness snapshot it was chosen from. (Without that rule, a late reply could hide a lane that a newer snapshot shows has work. One delayed100 round showed a 2.5 s pickup p99 before this fix; none after.) |

### Tests

On the branch rebased onto origin/main (1d45bc0):

| Suite | Result |
|---|---|
| Core | 464 passed, 0 failed |
| Contract | 502 passed, 0 failed |
| SQLite | 56 passed, 0 failed |
| Redis (Redis 8.2, `appendfsync always`) | 131 passed, 0 failed |
| PostgreSQL | 406 passed, 0 failed |

By mistake, two full runs overlapped and shared one Redis container. In that overlap, one Redis test and the SQLite writer-timing test (`While_another_process_holds_the_writer_…`) each failed once. Each passed when its suite ran again by itself. The default is 1, so these stores take the old code path.

New Core tests (`WorkerEngineTests`, `CapacityCaseTests`): option default and V-25; requests only fill free slots (1, 2 and 4 in flight); with 2 in flight a drain finishes sooner in fake time than with 1; shutdown with a claim outstanding; a claim whose reply is lost, or that times out, while another is in flight; CAP-02 at 2 in flight.

### Benchmarks

Paired runs, arm order rotated each round, every round under `flock /tmp/runwick-bench.lock`, load average recorded at the start of each run, kernel disk pressure (PSI) before and after each Redis run. Base: origin/main at 1d45bc0. Candidate: this branch. 20,000 jobs, 1 KiB, 16 slots, one producer call. Redis 8.2 with `appendfsync always`, fresh per run. Tools: `eng/bench-compare/redis-group/pairs-combined.sh` (arms `oldbuf`, `cif2`, `bullmq`) and `eng/bench-compare/claims-pairs.sh` (PostgreSQL 18 in its own container, SQLite on a fresh file).

**Redis drain, disk log, buffered finish, 5 rounds**

| Arm | Jobs/s per round | Median | Script ms/job | Claims | Load at start |
|---|---|---:|---:|---:|---|
| main | 162, 387, 274, 427, 394 | 387 | 1.18 | 2,424 | 37, 19, 15, 21, 46 |
| 2 in flight | 327, 427, 426, 504, 253 | 426 | 1.09 | 2,673 | 42, 27, 26, 14, 17 |
| BullMQ 6.7.2 | 629, 759, 1,161, 971, 1,334 | 971 | 0.36 | – | 42, 22, 19, 20, 66 |

| Pair | Per round | Median |
|---|---|---:|
| 2 in flight / main | 2.01, 1.10, 1.56, 1.18, 0.64 | **1.18** |
| 2 in flight / BullMQ | 0.52, 0.56, 0.37, 0.52, 0.19 | **0.52** |
| main / BullMQ | 0.26, 0.51, 0.24, 0.44, 0.30 | **0.30** |

**Redis drain, log in memory (FSYNC=tmpfs), buffered finish, 4 rounds**

| Arm | Jobs/s per round | Median | Script ms/job | Claims | Load at start |
|---|---|---:|---:|---:|---|
| main | 980, 640, 824, 879 | 851 | 0.83 | 2,396 | 72, 24, 15, 11 |
| 2 in flight | 1,153, 1,249, 1,083, 1,129 | 1,141 | 0.74 | 2,668 | 62, 37, 12, 9 |
| BullMQ 6.7.2 | 2,306, 3,170, 2,145, 4,156 | 2,738 | 0.23 | – | 43, 28, 17, 9 |

| Pair | Per round | Median |
|---|---|---:|
| 2 in flight / main | 1.18, 1.95, 1.32, 1.28 | **1.30** |
| 2 in flight / BullMQ | 0.50, 0.39, 0.51, 0.27 | **0.45** |
| main / BullMQ | 0.43, 0.20, 0.38, 0.21 | **0.30** |

Main's ratio to BullMQ here (0.30) is lower than the 0.41 in the stored-data design. The host was much busier during these runs, and BullMQ ran very fast in some rounds.

**Redis delayed100 (100 jobs/s, each due 5 s after dispatch), after the "no work" fix, 4 rounds.** Pickup is the time from when a job is due to when its handler starts.

| Round | main p95 / p99 ms | 2 in flight p95 / p99 ms | Jobs/s (both) | Load at start |
|---|---|---|---|---|
| 1 | 163 / 309 | 245 / 442 | 85.7 / 85.6 | 63 / 51 |
| 2 | 203 / 312 | 268 / 356 | 85.6 / 85.6 | 51 / 53 |
| 3 | 224 / 573 | 244 / 472 | 85.5 / 85.6 | 45 / 35 |
| 4 | 135 / 261 | 157 / 307 | 85.6 / 85.6 | 42 / 36 |

The p95 is higher with 2 in flight in every round; the p99 medians are 310 and 399 ms. Before the fix, 3 rounds gave p99 of 133, 482 and 244 ms (main) against 439, 358 and 2,471 ms; the 2.5 s round also lost throughput (80 jobs/s). BullMQ's own delayed figures (p99 342 to 926 ms) are measured differently and are not comparable.

**PostgreSQL drain, default finish, 3 rounds**

| Arm | Jobs/s per round | Median | Ratio to main per round | Median ratio |
|---|---|---:|---|---:|
| main | 219.8, 260.4, 206.3 | 219.8 | – | – |
| 2 in flight | 238.4, 271.7, 263.1 | 263.1 | 1.08, 1.04, 1.28 | **1.08** |
| 4 in flight | 231.7, 295.0, 291.8 | 291.8 | 1.05, 1.13, 1.41 | **1.13** |

**SQLite drain, default finish, 3 rounds**

| Arm | Jobs/s per round | Median | Ratio to main per round | Median ratio |
|---|---|---:|---|---:|
| main | 231.4, 225.2, 260.7 | 231.4 | – | – |
| 2 in flight | 239.4, 220.0, 205.6 | 220.0 | 1.03, 0.98, 0.79 | **0.98** |
| 4 in flight | 168.3, 191.0, 173.9 | 173.9 | 0.73, 0.85, 0.67 | **0.73** |

Load at the start of the PostgreSQL and SQLite runs was 5 to 13.

**Earlier rounds on the previous base (ff1374d, before origin/main's Redis script cuts).** Redis without buffered finish, 3 rounds: 2 in flight against main 1.06, 1.42, 0.82, so no clear effect. Without buffered finish, slots free up in bursts when a batch of finishes is confirmed, and claims are already large. Two disk rounds there were 3 to 7 times slower for 2 in flight (205 s and 374 s against about 69 s for main). Redis was mostly idle during those runs, the producer's health check also timed out, and host load was 14 to 129. Six further disk rounds with disk pressure recorded (PSI 3 to 5%) had 2 in flight faster in every round (1.03 to 2.69). That points to disk contention on the host, not the engine, but it was not proven.

### Decision on the default

The default stays **1**. Two in flight is a clear win only for Redis with buffered finish and a modest one for PostgreSQL. It slows SQLite, and it delays the pickup of due jobs a little. A store-dependent default would need its own option design; that is left for the user to decide.

### What's left

- **A per-store default**, for example 2 for Redis with buffered finish, needs a decision and probably an option that can mean "the store's choice".
- **The delayed pickup tail.** Why p95 and p99 rise with 2 in flight at a low arrival rate (each due job is claimed alone, so overlap only adds requests) is not explained. Worth one traced run.
- **The two very slow disk rounds** on the earlier base were not reproduced and not explained.
- **Quieter host.** Every set ran at load 9 to 72. More rounds on a quieter host would narrow the ratios.
- Not measured: steady500, unbuffered Redis on the new base, 32 slots with claims in flight, and the combined finish-and-claim option (which declines while a pipelined claim is out).

## Redis: lean worker scripts (perf round two, Redis F)

### Short answer

The scripts a Redis worker runs to claim, finish and renew jobs now have a hand-written fast path for the usual case. Nothing stored changed: job hashes, receipts, event rows, AOF bytes and every reply, replays included, are byte for byte what main writes. No migration, and old and new binaries can run side by side.

Script CPU per job fell by about a quarter. A claim at eight jobs per call went from 270–278 to 196–207 µs per job, and a FinishMany from 250–259 to 193–199 µs.

On this shared host that saving did not show up as faster drains. Paired against main with buffered finish, the drain rate was 0.90× on the disk log and 0.96× with the AOF on memory (medians of four rounds, with wide spread). BullMQ is still about two to three times faster. The 90% target is not met by this change alone. The stored-data cuts that build on it (layout revision 2, the "Redis: storing less per job (layout revision 2)" section below) are what move the drain rate.

### What changed and why it is safe

Each of `ReserveAndBegin`, `Finish`, `FinishMany`, `Renew` and `RenewMany` now runs in two layers inside one script:

- **The generic steps** (gates, receipt lookup, the intent, recovery) are defined as one function, `GENERIC()`, but not run.
- **The lean path** runs first and only reads. As soon as it sees anything unusual, it calls `GENERIC()` before it has written anything, so the generic steps decide the command from the start exactly as before. Unusual means a gate or admission refusal, a pending guard, a receipt to replay, a deadline problem, a paused or missing queue, concurrency or rate scopes, a cancel request, a refused ownership, a duplicate job or command in one call, more than 64 items, large finish bodies, or a counter at its limit.
- **When the lean path decides**, it writes the after-image the generic steps would write: the same keys, fields, members, receipt and event texts, and reply. It writes them as one direct projection under the existing crash rule. If a write fails partway, the intent is stored with `ipos` 1 and recovery finishes it, as for any other command.

Most of the old cost was the generic machinery itself (defining and walking hundreds of helpers per call), not the data written. That is the part the lean path skips.

### Tests

- `eng/bench-compare/redis-group/diffcheck.py` runs main's scripts and these scripts on the same data: dispatch, claim, renew, finish, FinishMany and FinishAndClaim, every call sent twice so replays are compared, plus fenced, blocked, cancelled and exhausted claims, a uniqueness key, usage and error texts and a retry. Every key and every reply is byte-identical.
- New `RedisLeanScriptTests` (REDIS-22):
  - LS-01: the lean helpers (claim item, finish result, compact receipt, quoting, byte order, counters) give exactly what the generic helpers give, for usual and awkward texts.
  - LS-02: claim, Renew, RenewMany, Finish, FinishMany and a claim that finds no work take the lean path and never call the generic one. A replayed claim does call it.
  - LS-03: every write prefix of a lean claim, RenewMany, Finish and FinishMany (the FailAfter seam) leaves either a stored intent or a lost reply. The resend recovers it and answers as the first call would, each job finishes once, and the store's invariants hold.
- `RedisCompactReceiptTests` RC-03, which simulates an older binary, now forces that binary onto the generic path.
- Full suites against Redis 8.2 with `appendfsync always`, built from this commit alone:
  - Runwick.Redis.Tests: 133 of 134 passed. The one failure was `RedisSentinelTests`, which timed out waiting for its replicas to sync during test setup, before any Runwick write. Rerun alone, the Sentinel class passed 8 of 8.
  - Runwick.Core.Tests: 455 of 455 passed.
  - Runwick.Contract.Tests: 502 of 502 passed.
- An earlier run failed the kill -9 crash test (`RedisCrashTests`, REDIS-01) for Finish and Cancel. That run had host load averages of 40 to 52. The resend arrived after the command's 60-second deadline, and the client correctly gave up with `store_recovering`. Cancel never uses a lean script, which pointed at the host. Rerun at load averages of 15 to 18, both cases passed, as did all other crash cases. Main passed the same cases at similar load.

### Benchmarks

#### Script CPU per job

Measured with `profile_worker.py --plain`, which loads the scripts as the binary composes them and times them with Lua's process CPU clock (not distorted by other load), on a Redis with the AOF on. Two rounds each. µs per job:

| Path | Jobs per call | main | this change |
|---|---:|---:|---:|
| Claim (ReserveAndBegin) | 1 | 764–826 | 585–650 |
| Finish (FinishMany) | 1 | 680–698 | 502–509 |
| Claim (ReserveAndBegin) | 8 | 270–278 | 196–207 |
| Finish (FinishMany) | 8 | 250–259 | 193–199 |

#### Drains

Each run drained 20,000 jobs with 16 workers on a fresh Redis 8.2 (`appendfsync always`). Arms ran in rotating order within each round, and each round held the shared bench lock. All arms here use buffered finish: `oldbuf` is main, `midbuf` is this change. The host was shared, with load averages of 7 to 25, so read the paired ratios, not single numbers.

| Arm | Disk: jobs/s per round | median | script ms/job | AOF B/job |
|---|---|---:|---:|---:|
| oldbuf (main) | 464, 545, 475, 464 | 470 | 0.93 | 6,829 |
| midbuf (this change) | 328, 392, 582, 497 | 445 | 0.87 | 6,830 |
| BullMQ | 1236, 1232, 870, 501 | 1,051 | 0.34 | 2,815 |

| Arm | AOF on memory: jobs/s per round | median | script ms/job | AOF B/job |
|---|---|---:|---:|---:|
| oldbuf (main) | 912, 1163, 994, 994 | 994 | 0.74 | 6,818 |
| midbuf (this change) | 1064, 1228, 861, 728 | 962 | 0.71 | 6,818 |
| BullMQ | 3112, 3235, 3461, 3942 | 3,348 | 0.21 | 2,816 |

| Paired ratio | Disk, per round | median | Memory, per round | median |
|---|---|---:|---|---:|
| midbuf / oldbuf | 0.71, 0.72, 1.22, 1.07 | 0.90 | 1.17, 1.06, 0.87, 0.73 | 0.96 |
| midbuf / BullMQ | 0.27, 0.32, 0.67, 0.99 | 0.49 | 0.34, 0.38, 0.25, 0.18 | 0.30 |
| oldbuf / BullMQ | 0.38, 0.44, 0.55, 0.93 | 0.49 | 0.29, 0.36, 0.29, 0.25 | 0.29 |

AOF bytes per job are unchanged, as expected. Redis's own script time per job fell only 4% to 7% in the drains, much less than the profile's quarter. The two measure different things. The profile counts Lua CPU in the claim and finish scripts alone. The drain figure is Redis's wall time across every script call, including the dispatches that insert the jobs, which this change does not touch.

### What's left

- **The drain gain is not measurable on this host.** The CPU saving is real in the profile, but the round-to-round spread (0.71 to 1.22) is larger than the effect. A quiet host would be needed to see it, if it is there.
- **The 90% target is far off.** What is left is the data written per job (receipts, attempt records, event rows), which the layout revision 2 cuts address.

## Redis: storing less per job (layout revision 2)

### Short answer

A claimed and finished job now writes about a quarter fewer bytes to Redis. The cuts come from docs/plans/redis-stored-data-design.md (options a, b, c and e). Replies, management reads and the `/v1/events` feed are unchanged, byte for byte.

Paired against main with buffered finish, drains are 1.19× faster on the disk log and 1.21× with the AOF on memory (medians of four rounds). Without buffered finish they are 1.00× on disk and 1.44× on memory. AOF bytes per drained job fell from about 6,830 to about 5,050.

Runwick with buffered finish now reaches 0.59× of BullMQ's drain rate on disk (main: 0.49×) and 0.36× on memory (main: 0.29×). The 90% target is not met.

Upgrading needs the usual layout steps: drain, stop, migrate, start. No data is rewritten, and data written by the old layout stays readable.

### What changed and why it is safe

- **One finish receipt per call (option a).** The finishes first sent in one FinishMany or FinishAndClaim call now share one receipt hash, `rcpt:<call>:f`, with one expiry entry. Before, each finished job had its own nine-field hash and its own expiry entry. Each field still holds that job's fingerprint, deadline, decision time and result, so a resend still replays the exact reply. When the process resends a finish, it names the call that first carried it (the client remembers this until the finish's deadline plus 30 seconds), so the script finds the receipt in one lookup. A finish resent without that hint, or decided in a later call, gets an ordinary per-job receipt, as before. This is a new receipt form, not a layout change.
- **Attempt records packed at close (option c).** While an attempt is open, nothing is written under `a:<attempt>`: the job's own fields plus two new ones, `ares` (reserved at) and `aauth` (authorized at), describe it. When the attempt closes, one field `a:<attempt>` holds the whole record as a short JSON array. The copies of the ownership token and the closing command that nothing ever read are no longer stored. Errors and usage stay in their own fields. An attempt opened under layout 1 closes in the layout-1 form, so a store never mixes forms within one attempt.
- **Events packed per call (option b).** A call's events are stored as one entry with a short head line and one line per event. A claim's reserved and started events for one attempt fold into one line. Event IDs stay consecutive. `EventsSince` expands the entries into exactly the rows it returned before, byte for byte, including paging that starts or stops inside an entry. If any value would not split cleanly, the call falls back to the old one-row-per-event form.
- **No stored `null` (option e).** Event rows no longer store `actorId` or `attemptId` when they are absent; readers already treated a missing field as null.

Options b, c and e change what readers must understand, so they come together as layout revision 2:

- The upgrade is drain, stop, migrate, start (spec 23).
- `migrate` raises each application's layout first and refuses with `intent_pending` if a command is half-applied, then raises the store record.
- A layout-1 binary refuses to run against a layout-2 store, as for any layout change. The new binary also refuses a store that has not been migrated yet (L2-05).
- No data is rewritten. Every reader understands both forms indefinitely.

### Tests

- `diffcheck.py`, lean path against generic path, both in layout 2: 74 keys compared, 0 differences.
- `diffcheck.py --replies-only` against main: every reply, every replay and the `EventsSince` feed read at page sizes 1 to 6 are byte-identical (0 differences). The stored keys differ by design.
- New `RedisLayout2Tests`:
  - L2-01: per-call finish receipts: after a lost reply, every resend (alone, in another batch, or the whole call again) replays the same answer; a changed body is a conflict; another process's resend applies nothing twice.
  - L2-02: event packs: the feed is the same rows at every page size from 1 to 12, and from a cursor in the middle of a pack.
  - L2-03: layout-1 data mixed with new data, read through `GetJob` and events.
  - L2-04: a layout-1 open attempt closes in its own form.
  - L2-05: migrate from layout 1, with the `intent_pending` refusal.
- Tests updated deliberately for the new layout:
  - `RedisDispatchLayoutTests` (the event-pack shape);
  - `RedisScriptCutTests` SC-02 (markers now live in the call's receipt);
  - `RedisControlTests` REDIS-12 (cleanup removes whole packs);
  - `RedisCrashTests`, which now ignores the resend's confirmation time in the call's receipt (`c:<job>` fields), as it already ignored `confirmed` in a per-job receipt;
  - the CLI test, which expects layout revision 2;
  - the harness snapshot and admin readers, which read both forms.
- Full suites against Redis 8.2 with `appendfsync always`:
  - Runwick.Redis.Tests: 138 of 139 passed. The failure was the kill -9 crash test for FinishMany: the recovered state differed from the clean run only in the confirmation time written by the resend. After the test fix above, the FinishMany and Finish crash cases pass. The whole Redis suite was not rerun after that fix.
  - Runwick.Core.Tests: 455 of 455 passed.
  - Runwick.Contract.Tests: 502 of 502 passed.
  - Runwick.Cli.Tests: 20 of 20 passed after the layout-revision fix (19 of 20 before).

### Benchmarks

#### Bytes written per job

Measured with `writes.py` at eight jobs per call: AOF bytes per job for each call. The lean scripts alone (the previous change) write exactly what main writes.

| Call | main | layout 2 |
|---|---:|---:|
| Claim | 2,067 | 1,146 |
| FinishMany | 1,911 | 1,288 |
| Finish (single) | 2,370 | 2,078 |
| Dispatch | 3,587 | 3,491 |
| FinishAndClaim | 3,996 | 2,431 |
| RenewMany | 182 | 182 |

#### Script CPU per job

`profile_worker.py --plain`, two rounds, µs per job. "Lean" is the previous change alone.

| Path | Jobs per call | main | lean | layout 2 |
|---|---:|---:|---:|---:|
| Claim | 1 | 764–826 | 585–650 | 532–648 |
| Finish | 1 | 680–698 | 502–509 | 516–530 |
| Claim | 8 | 270–278 | 196–207 | 185–199 |
| Finish | 8 | 250–259 | 193–199 | 172–186 |

#### Drains

20,000 jobs, 16 workers, a fresh Redis 8.2 per run (`appendfsync always`), arms in rotating order, each round under the shared bench lock. `old` and `oldbuf` are main without and with buffered finish; `base` and `buf` are this change without and with buffered finish. Load averages were 10 to 25 on disk and 7 to 20 on memory, so the paired ratios are the numbers to trust.

Disk log:

| Arm | jobs/s per round | median | script ms/job | AOF B/job |
|---|---|---:|---:|---:|
| old (main) | 294, 416, 366, 340 | 353 | 1.08 | 6,963 |
| base | 301, 390, 359, 416 | 374 | 0.84 | 5,255 |
| oldbuf (main) | 464, 545, 475, 464 | 470 | 0.93 | 6,829 |
| buf | 614, 734, 505, 334 | 559 | 0.71 | 5,059 |
| BullMQ | 1236, 1232, 870, 501 | 1,051 | 0.34 | 2,815 |

| Paired ratio | per round | median |
|---|---|---:|
| base / old | 1.02, 0.94, 0.98, 1.22 | 1.00 |
| buf / oldbuf | 1.32, 1.35, 1.06, 0.72 | 1.19 |
| buf / BullMQ | 0.50, 0.60, 0.58, 0.67 | 0.59 |
| oldbuf / BullMQ | 0.38, 0.44, 0.55, 0.93 | 0.49 |
| base / BullMQ | 0.24, 0.32, 0.41, 0.83 | 0.36 |

AOF on memory (`FSYNC=tmpfs`):

| Arm | jobs/s per round | median | script ms/job | AOF B/job |
|---|---|---:|---:|---:|
| old (main) | 558, 896, 815, 641 | 728 | 0.95 | 7,000 |
| base | 957, 1191, 952, 993 | 975 | 0.64 | 5,325 |
| oldbuf (main) | 912, 1163, 994, 994 | 994 | 0.74 | 6,818 |
| buf | 1020, 1278, 1297, 1380 | 1,288 | 0.53 | 5,046 |
| BullMQ | 3112, 3235, 3461, 3942 | 3,348 | 0.21 | 2,816 |

| Paired ratio | per round | median |
|---|---|---:|
| base / old | 1.72, 1.33, 1.17, 1.55 | 1.44 |
| buf / oldbuf | 1.12, 1.10, 1.31, 1.39 | 1.21 |
| buf / BullMQ | 0.33, 0.40, 0.37, 0.35 | 0.36 |
| oldbuf / BullMQ | 0.29, 0.36, 0.29, 0.25 | 0.29 |
| base / BullMQ | 0.31, 0.37, 0.28, 0.25 | 0.29 |

Without buffered finish, the disk-log drain waits on fsync rather than on scripts, so fewer bytes do not show there.

#### Delayed pickup (delayed100)

One round: 100 jobs/s, each delayed 5 s, for 30 s after a 12 s warm-up. Pickup is the time from when a job falls due to when its handler starts.

| | main | layout 2 | BullMQ |
|---|---:|---:|---:|
| Pickup p95 (ms) | 256 | 152 | 280 |
| Pickup p99 (ms) | 450 | 547 | 412 |
| Enqueue p99 (ms) | 315 | 94 | 122 |

With one round this is within noise. There is no sign that pickup got worse.

### What's left

- **The 90% target is not met.** BullMQ uses about 0.2 to 0.3 ms of script time per job; Runwick now uses 0.5 to 0.7 ms. What is left per job is the claim receipt, the state indexes and the fixed cost of each call.
- **Single Finish barely moved** (2,370 → 2,078 bytes): a lone finish still gets its own receipt. Batching via FinishMany or buffered finish is where the cut applies.
- **The whole Redis suite was not rerun** after the crash-test fix; the affected crash cases were.

## Delayed jobs from staging, outbox, management and schedules now wake idle workers

Written 2026-11-22. Follows "PostgreSQL and SQLite: delayed jobs picked up seconds late (fixed)" in perf-round-2.md, which fixed direct `Dispatch` and `DispatchMany` only. Branch: `claude/frosty-wing-ac9ea5`, on top of main `1d45bc0`.

### Short answer

A delayed job (one sent with `NotBefore`) that becomes the earliest waiting job of its queue and priority now wakes idle workers on every path that creates jobs, not only direct dispatch. Before, a job created by staging inside the caller's transaction, by outbox delivery or by management dispatch sent no wake, so a worker that had been idle for a while found it only when its back-off poll fired: up to 10 seconds (plus or minus 20 %) late on PostgreSQL, up to 2 seconds on SQLite.

The rule is the same as for direct dispatch: wake only when the new job is its lane's earliest not-yet-due job, only after the transaction has committed, never for a resent (replayed) command. Jobs queued behind an earlier delayed job send nothing, so a steady stream of delayed jobs costs almost no extra wakes.

| Path | PostgreSQL | SQLite | Redis |
| --- | --- | --- | --- |
| Staging (caller's transaction) | fixed: new SQL function `dispatch_v5`, migration 0027 | fixed | no staging on Redis |
| Outbox delivery | fixed (single and grouped) | fixed | already woke |
| Management dispatch | fixed | fixed | already woke |
| Schedule materialization | nothing to fix (never delayed) | nothing to fix for delayed; due jobs now wake too | already woke |

### What changed

**Schedules never create delayed jobs.** A schedule template refuses `NotBefore`, and an occurrence is only materialized once it is due, so a scheduled job is always due when it is created. Its queue was already woken on PostgreSQL and Redis. No delayed-wake code was needed there.

**Redis: no code change.** Every Redis script that creates jobs ends in the same projection step (`U.project` in `prelude.lua`), and that step already calls `U.wake`, which publishes a wake for a due job or a lane's new earliest delayed job. New contract tests confirm this for outbox delivery, management dispatch and schedule materialization. This keeps the branch clear of the Redis script work going on elsewhere.

**PostgreSQL, staging.** A staged dispatch runs in the caller's own transaction, and the client has no way to act after that transaction commits. So the notification has to be sent from SQL, inside the transaction: PostgreSQL holds a `NOTIFY` until `COMMIT` and drops it on rollback. Migration `0027_staged_delayed_wake` (schema revision 27) adds `dispatch_v5`, which runs `dispatch_v3` unchanged and then, for each created job not yet due, sends the notification if no other not-yet-due job of its lane comes before it in claim order. That check is the one the client-side wake pump already uses (one primary-key and one ready-index lookup per job). Staged calls now use version 5. `dispatch_v3` and `rw_dispatch_core_v3` are untouched, so binaries at earlier revisions keep working against them.

I chose a thin wrapper over a new copy of `rw_dispatch_core_v3`. A copy would also need new copies of every function that calls it (`dispatch_v3`, `receive_outbox_v3`, `receive_outbox_group_v1`, and the schedule functions), several hundred lines of duplicated SQL, while only staging needs SQL at all.

**PostgreSQL, outbox and management.** These run in transactions the driver owns, so they use the same post-commit wake pump as direct dispatch, with no SQL change. After a committed first delivery (single or grouped) or management dispatch that created a delayed job, the client hands the job to the pump. A redelivery answered from the outbox mapping, and a replay, hand over nothing. For management dispatch this goes through a small "after commit" list on the driver's transaction object, which the runner runs only after `COMMIT` succeeds.

**SQLite, outbox, management and schedules.** These paths sent no in-process wake at all, not even for jobs due now. They now send the same hint as direct dispatch, after commit, never on replay. So on SQLite, jobs due now from these paths also wake workers in the same process at once (before: up to the 2 s back-off).

**SQLite, staging.** The .NET SQLite library has no commit callback. For a staged delayed job the store waits for the caller's transaction to end (it checks every 20 ms, for at most one minute), then runs the usual "is it the lane's earliest" read. A rolled-back job is not in the database, so it wakes nothing. Staged jobs that are due now still send no hint (unchanged; outside this task).

### Tests

New tests, each checking: a wake for the lane's first delayed job, none for a later one, a wake again for an earlier one, and none for a replay.

- Contract suite, run on PostgreSQL, SQLite and Redis:
  - `OutboxContractTests.A_delivered_delayed_job_wakes_its_queue_only_as_its_lanes_earliest_and_never_on_replay` (also checks that a redelivery does not wake, and on PostgreSQL a grouped delivery wakes once for its earliest item).
  - `ManagementContractTests.A_delayed_management_dispatch_wakes_its_queue_only_as_its_lanes_earliest_and_never_on_replay`.
  - `ScheduleContractTests.A_materialized_occurrence_wakes_its_queue_once_and_never_on_replay` (the job is due, so this checks the one wake and no wake on replay).
- `Runwick.Postgres.Tests.WakeNotificationTests.A_staged_delayed_job_notifies_on_commit_only_as_its_lanes_earliest_and_never_on_replay_or_rollback`: nothing is sent before the caller commits, nothing after a rollback; a batch wakes once for its earliest item.
- `Runwick.Sqlite.Tests.SqliteStagingTests.A_staged_delayed_job_wakes_local_workers_after_commit_only_as_its_lanes_earliest_and_never_on_replay_or_rollback`: the same on SQLite.
- `MigrationTests` updated for revision 27.

Results so far (2026-11-22 and 2026-11-23, shared host):

- All the new tests pass: the contract wake cases (9 of 9: outbox, management and schedule on PostgreSQL, SQLite and Redis), the SQLite staged case, and the PostgreSQL `WakeNotificationTests` class (6 of 6, including the new staged case).
- The new PostgreSQL staged test fails on a control build with the PostgreSQL change switched off, so it does catch the gap.
- Full Core suite: 455 passed, 0 failed.
- Full Contract, SQLite, Redis and PostgreSQL suites: not yet run (the run was stopped when the work was handed to another session).

### Quick pickup check

PostgreSQL only, on the shared host (not the reference host), holding `/tmp/runwick-bench.lock`, 2026-11-23 after a reboot (load average 11 to 85 during the runs, mostly from builds). A temporary test (not committed) started one worker with production timings, let it sit idle for 14 seconds so its back-off reached the 10-second cap, then created one job due 3 seconds later, first by staging in a committed transaction and then by management dispatch, three rounds each. It recorded how long after the due time the handler started.

| Arm | Staged: started after due | Management: started after due |
| --- | --- | --- |
| This branch | 138, 39, 55 ms | 138, 83, 30 ms |
| Control (same build with the two PostgreSQL changes switched off: staging back on `dispatch_v3`, management not handed to the pump) | 7.3, 6.8, 6.9 s | 5.1, 6.4, 5.1 s |

The control shows the old behaviour: the job waits for the worker's next back-off poll. With the change the worker is woken when the job is committed, reads the due time from its readiness check and sleeps until then. On the same control build the new staged wake test fails, as expected.

An earlier run on 2026-11-22, on a host at load 30 to 100, gave the same picture on the branch (19 to 143 ms) except one management round at 19.8 s, longer than the 12-second worst case of the old back-off poll, during a load spike to 102; I read that as the host starving the process, not a missing wake. Its control arm was lost to the host reboot. SQLite and Redis were not timed: Redis needed no change, and on SQLite the old worst case is the 2-second back-off and host fsync stalls dominate its tails (see the 2026-11-19 report).

### Not done

- Retry back-off still sends no store wake. A running worker's own finish wakes its pool, so its retries start on time (see the 2026-11-19 report); this is unchanged.
- SQLite staged jobs that are due now still send no in-process hint.
- The plan line in `docs/plans/perf-plan-2-remaining.md` ("still not woken: ...") is not edited here, to avoid merge conflicts with the other session editing that line; it can be updated when this branch is merged.

## Redis Sentinel and Cluster: the open gaps (work areas R and M)

Written 2026-11-23. Branch `redis-ha-gaps`, from `main-merge`. No layout, script, receipt or migration change.

### Short answer

- The operator tools now work on Sentinel and Cluster setups. Status, admission, capability activation, re-identification and the migrator take the same `RedisOptions` a host takes. On a cluster they look at every primary. Through Sentinel they find the primary the same way a host does. The CLI has a matching `--redis-topology` flag.
- A test now shows that a `MOVED` reply alone closes write authority. Writing it turned up a small race, which is fixed: a check that was already running when the `MOVED` reply arrived could reopen authority with its older view.
- A wrong password, a missing ACL permission and a TLS mismatch each stop the process with a clear reason before anything is written. The wrong-password test found a real problem. With a named user and a wrong password, StackExchange.Redis 3.4.20 quietly falls back to the `default` user when `default` has no password, so the process connected and wrote as `default`. This was confirmed with a small separate program: `ACL WHOAMI` returned `default`. The settings check now requires `ACL WHOAMI` to equal the configured user, for every topology including standalone. A wrong login is refused as `primary_authentication_failed` and a missing permission as `primary_permission_denied`. Before, these read as "unreachable", or were not caught at all.
- Making every script declare all its keys is not small, so it was not built. The reasons are below and in spec 22 §2.6.
- The overhead (Cluster and Sentinel against standalone, same drain) is **not measured yet**. The bench now accepts `--redis-topology`; the commands are at the end.

### What changed and why it is safe

1. **Operator commands follow the topology** (`src/Runwick.Redis/RunwickRedisMigrations.cs`, `RedisOps.cs`). There is one connection step for every operator command:
   - **Standalone:** the old path, unchanged. The old method signatures still use it.
   - **Sentinel:** a one-off check runs with the same code a host uses: a majority of the Sentinels must name the same healthy primary, and that primary must pass the settings check. The command then works only on that primary. If the check fails, the command refuses with its reason.
   - **Cluster:** every primary is checked against the settings, and every primary is scanned for `rw:{*}:meta`. If a primary that owns slots cannot be reached, the command refuses with `store_unavailable`. A partial answer would look complete, so refusing is the safe choice.

   Scripts still go by key, so each change lands on the owner of that application's slot. Messages never name a server; a failure says "primary 2 of 3", because the CLI prints the message.
2. **CLI** (`src/Runwick.Cli/CliCommands.cs`). `--redis-topology standalone|sentinel|cluster` works on `migrate`, `doctor` and every `store` command. The default is standalone, so existing commands are unchanged. Using the flag with a store other than Redis is a usage error. `doctor` now checks each primary.
3. **Clearer reasons** (`RxAuthority.Unreachable`). Only the reason text changes. The behaviour is the same: nothing is sent and the next check decides.
4. **The configured user is checked** (`RxProfile.CheckAsync`, all topologies). With `user=` set, `ACL WHOAMI` must return that user. This is stricter than before, which is the safe direction. The one cost: a configured user now needs permission for `ACL WHOAMI` (part of `+@all` and `+@connection`).
5. **Lost nodes during an operator command** become `store_unavailable`, not a raw `RedisTimeoutException`. The first test run caught this when the paused primary owned the migration lock's slot.
6. **A stale check opens nothing** (`RxAuthority`). Each piece of evidence (a `MOVED` reply, a lost connection) raises a counter. A check that started before the latest piece of evidence finishes without granting authority, and the check the evidence queued decides instead. This can only keep authority closed for longer, never open it.
7. **Bench** (`tests/Runwick.Workloads/BenchMode.cs`). New `--redis-topology sentinel|cluster`. The bench flushes every primary it reaches, migrates with the matching options and starts the host in that mode.

### Declared keys: assessed, not built

Today every script lists only `meta` in `KEYS`. Redis refuses a script up front (`ASK`/`TRYAGAIN`) only for the keys it lists. So listing every key would turn "can fail partway while its slot moves" into "is refused before it runs". The catch is that most of the 48 operations cannot know their keys before they run:

- A claim, Recover, ReserveAndBegin, cleanup and schedule catch-up pick their jobs from an index inside the script.
- A finish or cancel reaches the scope, rate, uniqueness and lane keys that are stored on the job.

Listing all keys up front would take a read call followed by a write call that re-checks what the read saw. That means one more round trip on every worker call, plus a retry path for each operation, which is the opposite of the current performance work. The case is already handled in two other ways:

- A slot marked as moving closes authority at the next check.
- A script that fails partway leaves a pending intent, and the new owner finishes it.

What is left is a window of at most one recheck interval, plus the first command after `MOVED`.

### Tests

New class `tests/Runwick.Redis.Tests/RedisHaGapsTests.cs` (REDIS-24), real containers:

| Case | Shows |
|---|---|
| Operator commands on a cluster | Three applications on three primaries: status lists all three; drain, capability and re-identify change all three (the new store ID is checked on each node); one-application admission finds the one on node 3; with node 3 paused, status refuses with `store_unavailable` |
| `MOVED` closes authority | Verifications are held through a test hook, and the slot and its keys are moved in one shell call. The next dispatch gets `MOVED`, and authority closes with `hash_slot_moved` (seen in the recent events). The dispatch after that is refused. Releasing the held check opens nothing. Authority resumes on the new owner, and all five earlier jobs are confirmed |
| Cluster ACL and TLS | One primary's ACL denies `CONFIG`: the cluster migrator refuses (`store_profile_mismatch`, "of 3"), and a host whose slot that node owns does not start, with nothing written to the slot. `ssl=true` against plain nodes: no start (`cluster_majority_unreachable`), nothing written |
| Sentinel discovery, password, TLS, failover | The migrator and status go through the Sentinels. A wrong data-node password is refused as `primary_authentication_failed` by the migrator and at startup, with the primary's key count unchanged. `ssl=true` against plain Sentinels is refused. After the primary is killed, status reaches the promoted node with the same store and application |
| CLI (`CliRedisTests`) | `--redis-topology sentinel` without `serviceName` gives `topology_unsupported`. Cluster and Sentinel strings that nothing answers give `store_unavailable` (exit 4), and doctor reports the store as unavailable |

Results on the final code: `RedisHaGapsTests` 4 of 4 (1 m 46 s), `CliRedisTests` 4 of 4; whole CLI project 21 of 21 on the first version. The first runs caught three defects, all fixed here: a server address in a migration message (the CLI prints it), a timeout leaking as a raw client exception, and the silent fall-back to the `default` user.

### Not done

- **Overhead is over the 5% budget and not yet explained.** See "Measured results" below: Sentinel and Cluster drain about 15% slower than one standalone node. Nobody has looked into why yet.
- TLS was tested only as a mismatch (TLS asked of plain servers). A server certificate the client rejects was not set up. It takes the same path (the connection fails, so no vote is counted), but it was not exercised.
- The operator commands check the topology once per command and hold no lease. A failover in the middle of a long command is met by Redis itself (`READONLY` on a demoted node), not by Runwick.
- Not injected: a missing permission on a node that does not own the slot (it costs that node's vote only), and a wrong Sentinel password (it reads as `sentinel_majority_unreachable`).

### Commands for the follow-up runs

```bash
export PATH=/root/.dotnet:$PATH DOTNET_ROOT=/root/.dotnet
cd /root/work/Spindle   # on branch redis-ha-gaps
dotnet build Runwick.slnx -c Release -v q
dotnet test tests/Runwick.Redis.Tests -c Release --no-build
RUNWICK_TEST_REDIS=127.0.0.1:<throwaway always-profile redis> dotnet test tests/Runwick.Contract.Tests -c Release --no-build   # count the Redis-kind rows
dotnet test tests/Runwick.Cli.Tests -c Release --no-build
dotnet test tests/Runwick.Dashboard.Tests -c Release --no-build
```

Overhead (same drain, three topologies, under `flock /tmp/runwick-bench.lock`; see the task's final message for the container setup):

```bash
W=tests/Runwick.Workloads/bin/Release/net10.0/Runwick.Workloads.dll
flock /tmp/runwick-bench.lock dotnet $W bench --store redis --redis 127.0.0.1:<standalone> --mode drain --n 20000 --workers 16 --calls 8 --out sa.json
flock /tmp/runwick-bench.lock dotnet $W bench --store redis --redis-topology sentinel --redis "127.0.0.1:<s1>,127.0.0.1:<s2>,127.0.0.1:<s3>,serviceName=mymaster" --mode drain --n 20000 --workers 16 --calls 8 --out sen.json
flock /tmp/runwick-bench.lock dotnet $W bench --store redis --redis-topology cluster --redis "127.0.0.1:<p1>,127.0.0.1:<p2>,127.0.0.1:<p3>" --mode drain --n 20000 --workers 16 --calls 8 --out cl.json
```

### Measured results (2026-11-23, Sonnet run on this branch)

Tests, Release build: Redis 143/143, Cli 21/21, Dashboard 56 passed and 1 skipped, Core 464/464, Contract 502/502 against a throwaway always-fsync Redis 8.2. No failures.

Overhead, 20,000-job drain, 16 workers, 8 calls, always-fsync, all nodes on one host, three rounds in rotating order (jobs finished per second):

| Round | Standalone | Sentinel | Cluster |
|---|---|---|---|
| 1 | 617 | 562 | 486 |
| 2 | 521 | 406 | 441 |
| 3 | 555 | 474 | 500 |
| Median | 555 | 474 | 486 |

- Sentinel finished 85% as many jobs per second as standalone (median of the per-round ratios: 0.85), and Cluster 85% too (0.85). The budget was 95%, so both miss it by about 10 points.
- Standalone itself varied by about 16% between rounds, so single rounds are noisy, but the gap held in every round.
- Load was 3 to 6 during the runs. Every run finished all 20,000 jobs with no failed dispatch.
- Why it is slower is not known yet. Two likely causes to check first: with Cluster, the three primaries share one host's CPU and disk; with Sentinel, the primary also streams every write to two replicas on the same host. A test with nodes on separate machines would show how much is the host and how much is Runwick.
- Raw files: `artifacts/perf/round-two/redis-ha-overhead`.

## Redis: lean-path trims and a Functions library, merged as cleanup

### Short answer

- **Test fixture race: fixed and merged.** On a brand-new, empty Redis, a test class that writes `rw:*` keys without migrating first (for example `StoreDiagnosticsTests`) could run before the first migration. Every later migration then refused, which caused the 52 setup failures seen earlier. The Redis test project now migrates the shared test server once, before any test class starts (`tests/Runwick.Redis.Tests/RedisServerMigrated.cs`, an xUnit assembly fixture). Full Redis suite on a fresh server: 143 of 143.
- **Three small trims to the lean claim and finish scripts: merged as cleanup on 2026-11-23.** They remove three Redis commands per claim call and two per finish call, but the drain speed did not change (median ratio 0.96 on disk, 1.02 in memory over five rounds). The time per call is spread over many small steps, and no single step is big enough to matter. Branch `redis-next` (7ac41df), merged as cleanup on 2026-11-23 because the code is simpler and sends fewer commands.
- **The worker commands as a Redis Functions library: merged as cleanup on 2026-11-23, opt-in (`RedisOptions.UseFunctions`, off by default because it measured slower).** The Functions version defines the shared helpers once per server instead of once per call, and calls them with `FCALL`. It stores, replies and replays exactly what the scripts do (0 differences). On drains it was not faster: buffered finish with Functions finished 0.83 times as many jobs per second as without it on disk, and 0.89 times in memory (medians of five rounds; the rounds scatter widely, from 0.71 to 1.17). Branch `redis-functions` (8cbb8f7), merged as cleanup on 2026-11-23. Off by default, nothing changes: EVALSHA only, no library loaded, no `FUNCTION` command sent. With the option on, the host's profile check loads the library, and a server that refuses `FUNCTION` or `FCALL` gets the same commands by `EVALSHA`. No migration either way.
- **Shorter job-hash field names (layout revision 3): not done.** The stored-data design measured them at about 3% of the log bytes and no CPU change.
- The tools stay: `profile_worker.py --lean-marks` times the steps inside the lean scripts, and `diffcheck.py` now matches any UUIDv7 decision time.

### Drain results (2026-11-23)

Five rounds per build, 20,000 jobs, always fsync, load 2.5 to 6.5 on disk and 2.5 to 12 in memory. `old` and `oldbuf` are main (ce9d0b5) without and with buffered finish. `base` and `buf` are the Functions build without and with it. Median jobs finished per second:

| Build | Disk | Memory |
|---|---:|---:|
| old (main) | 565 | 913 |
| base (Functions) | 554 | 972 |
| oldbuf (main, buffered finish) | 821 | 1,177 |
| buf (Functions, buffered finish) | 710 | 1,043 |
| BullMQ | 1,507 | 3,157 |

Buffered finish on main is about 55% of BullMQ on disk and 37% in memory. AOF bytes per job did not change (about 5.0 to 5.3 KB; BullMQ writes about 2.8 KB). The next real gain on Redis has to come from writing less per job or from fewer calls per job, not from trimming script steps.

### Redis: test fixture race and where the lean scripts' time goes (perf round two, Redis F)

#### Short answer

- **Test fixture race: fixed.** The Redis test project now migrates the shared test server once, before any test class starts. On a brand-new empty server, a class that writes `rw:*` keys without migrating first (for example `StoreDiagnosticsTests`) could run before the first fixture had migrated, and every later migration then refused with "rw:* keys but no rw:store" (the 52 setup failures seen earlier).
- **Lean script time: profiled, three small trims made, no measurable gain.** The trims are real (three fewer Redis commands per claim call, two fewer per finish call, and no JSON parsing in the event pack), but on this host the whole-script cost per job did not move outside the noise. No single piece of the lean path is large enough to matter on its own: the time is spread across reading jobs, building the after-image and texts, and the writes, and on a loaded host most of each call's cost is fixed per call.
- **Shorter job-hash field names (layout revision 3): not done.** The stored-data design measured them at about 3% of the log bytes and no CPU change. That is not worth a layout change.
- Nothing stored, replied or replayed changed. No migration.

#### The fixture race

`RedisServer.EnsureMigratedAsync()` (tests/Runwick.Harness) already migrated the server once per process, but only when a test asked for it through `DriverFixture` or by calling it. Test classes run in parallel, and some write `rw:*` keys directly: `StoreDiagnosticsTests` writes an application's meta hash by hand and never migrates. On an empty server that class could go first. After that, the migration finds `rw:*` keys but no `rw:store` and refuses, so every fixture after it fails.

The fix is an xUnit assembly fixture, `RedisServerMigrated` (tests/Runwick.Redis.Tests/RedisServerMigrated.cs), which xUnit creates before any test class in the project runs. It calls the same once-per-process migration. Nothing else changed.

Checked on brand-new empty servers: `StoreDiagnosticsTests` alone (a class that never migrates) passed 3 of 3 and left `rw:store` behind, so the migration ran first. Five classes together (below) passed 15 of 15 on another empty server.

#### Where the lean path's time goes

Measured with `profile_worker.py --lean-marks` (new): the scripts as the binary composes them, with CPU-clock marks between the lean path's steps. One call of 8 jobs, median of 60 calls, on main-merge's scripts. Host load averages were 60 to 115 during these runs, so absolute numbers are high; read them as shares.

Claim of 8 jobs (about 1,380 µs in all):

| Step | µs per call | What it is |
|---|---:|---|
| Script start and helpers | 50 | the frame's local helpers, the server time |
| Meta and gates | 80 | `HGETALL` meta, gates, receipt lookup |
| Queue row and contracts | 30 | |
| Candidates | 229 | `ZRANGE` of the ready set, `HGETALL` of each job |
| Decision | 134 | checks, attempt ids, counters |
| After-image and texts | 371 | job fields, index changes, the reply item and receipt texts |
| Receipt | 31 | |
| Event pack | 108 | **66 after this change** |
| Writes | 268 | the Redis commands themselves |
| Wake check, payloads | 67 | |

Finish of 8 jobs (about 1,220 µs in all): script start 46, meta and gates 57, reading and deciding the items 359 (of which about 200 is `HGETALL` of the job hashes and about 85 decoding the bodies), after-image and texts 326, event pack 53, writes 338, the rest about 45.

Two things the profile ruled out:

- **Lua garbage collection is not the per-call cost.** With the collector stopped for the duration of each script, the per-job figures were the same (claim 477 and 172 µs per job at 1 and 8 jobs per call, finish 444 and 175).
- **The per-call cost is mostly fixed work done cold.** Fitting the profile's numbers at 1 and 8 jobs per call gives about 310 to 420 µs per call plus about 120 µs per job. A tight loop of the same Redis commands inside one script costs a small fraction of what the first ones cost in a real call (for example `HGETALL` of a meta hash: about 7 µs warm, against about 70 µs for the whole meta-and-gates step in a call). On this loaded host, each call starts with cold caches. Batching (more jobs per call) is what spreads that cost, which buffered finish and claims in flight already do.

#### What changed and why it is safe

All in `src/Runwick.Redis/Lua/lean/` (the lean claim and finish paths). The generic path is unchanged.

- **No-op removals are no longer sent.** The lean claim no longer sends `ZREM idx:created:s:reserved` and `ZREM idx:terminal`; the lean finish no longer sends `ZREM ready:<queue>:<priority>` for a job that ends, or `ZREM idx:terminal` for a job that retries. Every state change goes through the same index update (`U.set_state`), so a pending or retrying job is never in those two indexes, and a running job is never in a ready set or the terminal index. The removals could not change anything, and Redis does not write a removal that removes nothing to the AOF, so the log is the same too.
- **The claim's event pair is written as its pack line.** Before, the lean claim built two JSON detail texts per job, and the commit step matched them with patterns to fold them into the pack's `~rs` line. Now the claim gives the folded line directly. The line is built from the same values with the same formatting.
- **One `HSET` on meta at commit.** The event pack's `nextevent` and the revision now go out in one `HSET` instead of two. If a write fails partway, the stored intent is the same as before, and recovery writes both fields.

Proof that nothing stored or replied changed (`diffcheck.py`, which also gained a fix: it now normalizes the time inside any UUIDv7 attempt id, not only times that start with the digits 01a1, which the clock has since passed):

- These scripts against main-merge's scripts: 74 keys compared, 0 differences (keys, replies, replays and the events feed at page sizes 1 to 6).
- These scripts' lean path against their own generic path: 74 keys compared, 0 differences.
- Main-merge against itself, as a control: 0 differences.

REDIS-11 holds: no new text comparison was added.

#### Before and after

`profile_worker.py --plain` (whole script, Lua CPU clock), four rounds alternating which build went first, 60 calls per size, load averages 61 to 72. In-script µs per job, median of rounds:

| | Claim, 1 per call | Finish, 1 per call | Claim, 8 per call | Finish, 8 per call |
|---|---:|---:|---:|---:|
| main-merge | 543 | 430 | 176 | 158 |
| this change | 533 | 464 | 177 | 162 |

The differences are inside the round-to-round spread. The event pack step, timed on its own, went from 108 to 66 µs per claim of 8 jobs, about 5 µs per job, which is about 1.5% of a claimed-and-finished job.

#### Tests

Built with `dotnet build Runwick.slnx -c Release`. Against a brand-new Redis 8.2 (`appendfsync always`), run together: `StoreDiagnosticsTests`, `RedisLeanScriptTests` (LS-01 to LS-03, including every write prefix of a lean claim and finish), `RedisLayout2Tests` (event packs read back at every page size), `RedisDispatchLayoutTests` and `RedisWorkerPathTests` (wake hints): 15 of 15 passed. The full Redis, Core and Contract suites were not run here.

#### What is not done

- **The 90% of BullMQ target is still not met,** and this change does not move it. What is left in the lean path is spread thin; the biggest single steps are building the after-image and its texts and the Redis writes themselves.
- **The per-call fixed cost** (about 310 to 420 µs per call on this host) is the largest piece at small batch sizes. Cutting it inside Lua is not possible: the work is already small, and the cost is that work running cold. A real change would be Redis Functions (`FUNCTION LOAD`), which define the helpers once per server instead of once per call. That is a larger design change (every script reads `KEYS`/`ARGV` as globals and keeps per-call state at file level, and functions must be loaded on every primary) and was not attempted.
- **Field names** stay long (see the short answer).
- Drain benchmarks were not run; see the commands in the hand-off.

### Redis: the worker commands as a Redis Functions library (perf round two, Redis F)

#### Short answer

- **Done.** Claim, Finish, FinishMany, Renew and RenewMany (the operations with a lean path) are now also one Redis Functions library. The client loads it once per server and calls it with `FCALL`. The lean frame's helper functions are defined once, when the library loads, not on every call.
- **Nothing stored, replied or replayed changed.** `diffcheck.py` found 0 differences in 74 keys between this branch called by `FCALL` and redis-next called by `EVALSHA`. No migration, no layout change.
- **The gain is small: about 6 to 7% of the in-script time, and less at one job per call than the earlier profile suggested.** A claim of 1 job went from about 540 to about 504 µs of Lua time; claims and finishes of 8 went from 176 to 166 and from 160 to 151 µs per job. The earlier estimate of "310 to 420 µs fixed cost per call" was mostly not the helper definitions. It is the real work of the first commands in each call, which runs with cold caches on this loaded host, and a library cannot change that.
- **EVALSHA is still there** as the fallback when a server refuses `FUNCTION` or `FCALL` (for example an ACL), and Describe says so.
- FinishAndClaim and Dispatch were **not** put in the library. They have no lean path, so their generic frame would still define its 250 helpers on every call inside a function. Getting a gain there means rewriting the generic frame (`prelude.lua`, `jobs.lua`) to set its per-call state in one place. That is a larger change.

#### What changed

**The library.** `RxScript.Library` (src/Runwick.Redis/Store/RxClient.cs) builds one text from the files that already exist:

- each lean operation's generic frame (prelude, jobs.lua, the operation body) as a function, `G['finish']` and so on, defined first so its names resolve exactly as in the script;
- `lean/frame.lua`, the lean helpers;
- for each operation a registered function, `rw_<hash>_<op>`, that sets `KEYS` and `ARGV` from its arguments and runs the lean body. That body calls the same `GENERIC()` as before when it cannot decide the command itself.

The library is named `rw_` plus the first 16 hex digits of the SHA-1 of its text. A binary with different scripts gets a different name, so an old and a new binary can run side by side on one server. The client never uses `FUNCTION LOAD REPLACE`, so one binary cannot replace another binary's code. (The original 2026-11-08 reason for not using Functions was exactly that risk. A name made from the content removes it.)

**One change inside `lean/frame.lua`.** The frame used to read `KEYS`, `ARGV` and the server time at the top of the script. In a library that top part runs once per server, so these per-call values now get set in `L.frame()`, the first step of every lean body. The after-image records, the event rows, the quoting cache and the write count are reset there too. It reads the same values in the same order: nothing before `L.frame()` ever ran a Redis command. A Functions library also cannot read globals such as `string` while it loads, so the frame now reads `string.format` and its neighbours inside `L.frame()` as well (`L.libs()`). The same file still serves the `EVALSHA` scripts unchanged in form.

**Loading.**

- The profile check (startup, every recheck, and every primary that the Sentinel or Cluster authority verifies) lists the library and sends `FUNCTION LOAD` only if the server does not have it. This covers a failover: the new primary is checked before any command goes to it. In Cluster mode the check runs on the slot's owner, which is the primary this application uses.
- A call answered `ERR Function not found` did not run. The client loads the library on the node that owns the key and resends once. This covers `FUNCTION FLUSH` and a server that lost it. If the resend is answered "not found" again, the call ends as not applied (`store_unavailable`). It never ends as "outcome unknown".
- **Fallback.** If the server refuses `FUNCTION LOAD` (ACL, renamed command, or a library it will not compile), or refuses `FCALL` itself (`NOPERM` for `fcall`, unknown command), nothing ran. The client switches to `EVALSHA` of the same scripts for the rest of the process. Describe's new `functions` fact gives the library name, or `evalsha:` followed by the server's refusal. This fact is not part of the profile verdict.

**AOF and replicas.** Redis stores loaded libraries in the AOF and the RDB and sends them to replicas. The tests check both on throwaway servers. With `appendfsync always`, the library survives a `kill -9` and restart. A new replica has it after its first sync.

**Crash rule.** The lean commit, which stores the intent when a write fails partway, is the same code in both forms. LS-03 runs every write prefix of every lean command, and these calls now go through `FCALL`.

#### Proof that nothing changed

`diffcheck.py` gained `--functions`, which runs the new side's lean operations by `FCALL` from the library, composed by `luascript.library` exactly as the binary composes it. The library name the Python tool computes matches the one the binary loaded.

- This branch by `FCALL` against redis-next by `EVALSHA`: 74 keys, 0 differences (keys, replies, replays and the events feed).
- This branch by `EVALSHA` against redis-next: 0 differences.
- This branch by `FCALL` against redis-next's generic path: 0 differences.

REDIS-11 holds: no text comparison was added.

#### Before and after

`profile_worker.py` (Lua CPU time inside the script, median of 60 calls, three rounds rotating the order, a throwaway Redis 8.2 with `appendfsync always`, host load average 46 to 56, under `/tmp/runwick-bench.lock`). The new `--functions` mode calls the library by `FCALL` and times the lean body inside each function. The `EVALSHA` modes time the whole script, which includes defining the helpers. The difference between the two is what the library saves. In-script µs per job, median of the three rounds:

| | Claim, 1 per call | Finish, 1 per call | Claim, 8 per call | Finish, 8 per call |
|---|---:|---:|---:|---:|
| redis-next, EVALSHA | 540 | 472 | 176 | 160 |
| this branch, FCALL | 504 | 470 | 166 | 151 |
| this branch, EVALSHA fallback | 552 | 471 | 173 | 161 |

Round by round, FCALL was lower than redis-next in 8 of the 9 rounds of the two claims and the 8-job finish. The single-job finish is inside the noise (434 to 490 µs between rounds). Redis's own time per call (`INFO commandstats`, which includes work outside Lua) moves too much on this host to read a difference from three rounds, though it points the same way. The fallback costs the same as before.

What the numbers say: the library saves about 35 to 40 µs per call. That matches the "script start and helpers" step of the earlier lean-path profile (about 50 µs). The rest of the fixed per-call cost is the work itself. Functions do not reduce it.

The library takes about 0.9 MB of Lua memory on the server (`used_memory_vm_functions`), because it holds four copies of the generic frame, one per operation.

#### Tests

New class `RedisFunctionsTests` (REDIS-25 since the merge), each case on its own throwaway Redis 8.2 container:

- the profile check loads the library before any call, Describe names it, and claim and finish go by `FCALL` with no failed calls;
- after `FUNCTION FLUSH` the next claim is answered "not found" once, the library is loaded again, and the call is applied;
- a user whose ACL denies `FUNCTION`, `FCALL` and `FCALL_RO` runs claims and finishes by `EVALSHA`, and Describe reports the refusal;
- `kill -9` and restart from the AOF keep the library, a new replica receives it in its first sync, and a new client's calls after the restart have no failed `FCALL`.

`RedisLeanScriptTests` (LS-01 to LS-03) still pass. LS-01 now calls `L.libs()` itself, because it uses the frame's helpers without a call frame. Run together against a throwaway Redis 8.2 (`appendfsync always`): 7 of 7 passed. The full Redis, Core and Contract suites and the drain pairs were not run here (commands in the hand-off).

#### What is not done

- **FinishAndClaim and Dispatch are not in the library** (see the short answer). Putting them in as they are would cost a larger library and save nothing.
- **No Sentinel or Cluster failover test.** A promoted replica already has the library (the replica test). A primary that lacks it is handled by the same "not found, load, resend" step that `FUNCTION FLUSH` tests, and the authority's profile check also loads it on every newly verified primary. The Sentinel and Cluster suites (REDIS-17, REDIS-18) run through this code, but nothing in them checks it specifically.
- **The fallback is for the life of the process.** If an ACL that refused `FCALL` is later relaxed, the process keeps using `EVALSHA` until it restarts.
- **The 90% of BullMQ target is still not met.** This change is about 6% of the Lua time of a claimed and finished job.

## PostgreSQL `jobs` index diet (migration 0028)

Written 2026-11-23. Branch `pg-index`, from `main-merge`; built as migration 0029 and renumbered to 0028 at merge, because B1's migration was not merged ahead of it. Measurements on a throwaway PostgreSQL 18 server on the shared host (load average 60 to 70 during the runs). Sizes, plans and WAL attribution do not depend on load; no timing claim is made here.

### Short answer

Every `jobs` update writes a new entry into every index whose condition the new row meets, because no update can be HOT (each state change touches `state`, `lease_expires_at` or `eligible_at`). Two of those entries were not needed, and migration `0028_jobs_index_diet` (schema revision 28) removes them:

1. **`jobs_profile_uq` is gone.** It was a unique index on (application, job ID, serializer profile, profile version) that existed only so the payload table could point at it. Every version of every job wrote to it. The payload now points at the job's primary key, and three small triggers keep the rule the old key enforced: a payload's serializer profile must equal its job's.
2. **`jobs_terminal_retention` now holds only finished jobs whose detail has expired.** That is the only set the history purge reads. Finished jobs that still have their detail are already in `jobs_detail_retention`, so a finish now writes one retention entry instead of two.

On a 205,000-job retained store the `jobs` indexes shrink from 97 MB to 70 MB. During a 2,000-job drain the WAL written to `jobs_profile_uq` and `jobs_terminal_retention` was 2.6 kB per job (0.3 kB of record data, the rest full-page images after the checkpoint); in the candidate those two write nothing during a drain. Claim, probe, recovery and list plans use the same indexes as before. Two reads had to change to keep their index use: the job stats read of ended jobs and the detail-expiry read. The second was slow before this change too and is now 60 times faster.

### Every index on `jobs` and who needs it

Entries written per job on the success path (create, claim, finish) before → after: **22 → 18**. Each renewal and each detail expiry: 7 → 6.

| Index | Key / condition | Read by | Written on | Decision |
|---|---|---|---|---|
| `jobs_pk` | (application, job_id) | every keyed read, claim by key (0025), foreign keys | every version | keep |
| `jobs_profile_uq` | UNIQUE (application, job_id, profile id, profile version) | only the payload foreign key check | every version | **dropped**, triggers keep the rule |
| `jobs_ready` | (application, queue, priority, eligible_at, job_id) where pending/retrying | claim candidates, readiness probe, wake check, telemetry, scaler, unsupported backlog, live stats | creation, retry | keep (small: 0.35 MB) |
| `jobs_expired_lease` | (application, lease_expires_at, job_id) where reserved/running | ListExpiredLeases (recovery), live stats | claim, renewal | keep (small) |
| `jobs_created_list` | (application, created_at desc, job_id desc) | unfiltered job list, created stats | every version | keep |
| `jobs_state_list` | (application, state, created_at desc, job_id desc) | state-filtered job list | every version | keep (see "Not done") |
| `jobs_queue_list` | (application, queue, created_at, job_id) | queue-filtered job list, queue stats, created stats by queue | every version | keep |
| `jobs_name_list` | (application, queue, job_name) | ListJobNames skip scan | every version | keep (1.4 MB, deduplicated) |
| `jobs_detail_retention` | (application, state, terminal_at, job_id) where terminal and detail kept | detail expiry; ended stats (detail kept) | finish | keep |
| `jobs_terminal_retention` | (application, terminal_at, job_id) where terminal | history purge; ended stats | finish, detail expiry | **narrowed** to `detail_expired_at IS NOT NULL` |

### Sizes (205,000 succeeded jobs, heap 39 MB, after VACUUM ANALYZE)

| Index | Revision 26 | Revision 28 |
|---|---:|---:|
| `jobs_queue_list` | 15.1 MB | 15.1 MB |
| `jobs_profile_uq` | 15.1 MB | – |
| `jobs_state_list` | 15.0 MB | 15.0 MB |
| `jobs_detail_retention` | 14.9 MB | 14.9 MB |
| `jobs_terminal_retention` | 12.5 MB | 8 kB (no detail expired yet in this fixture) |
| `jobs_created_list` | 12.4 MB | 12.4 MB |
| `jobs_pk` | 10.4 MB | 10.4 MB |
| `jobs_name_list`, `jobs_ready`, `jobs_expired_lease` | 1.9 MB | 1.9 MB |
| **All `jobs` indexes** | **97 MB** | **70 MB** |

Once detail expires, `jobs_terminal_retention` grows again, but only for the jobs waiting for their history purge, and each such job leaves `jobs_detail_retention` at the same time. So every finished job sits in one retention index, not two.

### WAL per job

Method: `pg_walinspect` over a 2,000-job drain (`--db-reuse`) on a fresh copy of each template, `CHECKPOINT` just before, block data and full-page images grouped by relation (`eng/perf-pg-index/walattr.sh`). `full_page_writes` on.

| Relation | Revision 26 bytes/job (record data + full pages) | Revision 28 bytes/job |
|---|---:|---:|
| `jobs_profile_uq` | 2,522 (241 + 2,282) | – |
| `jobs_terminal_retention` | 109 (74 + 35) | 6 |
| `jobs_pk` | 2,206 | 942 |
| `jobs_state_list` | 363 | 320 |
| other `jobs` indexes | 1,048 | 711 |

Estimate of WAL saved: **about 0.3 kB of record data per job** for certain (the two indexes' entries; plus a WAL record header for each of the 4 avoided entries), **plus up to about 2.3 kB per job of full-page images** when a checkpoint falls inside the work. `jobs_profile_uq`, like `jobs_pk`, has a random-UUID key, so after each checkpoint nearly every insert dirties a fresh page and writes a full image of it. The full-page share depends on checkpoint timing: the two single runs had different checkpoint luck (`jobs_pk` itself shows 2,206 against 942), so the paired drains below should settle the total. The total server WAL of the two single runs (63.7 MB against 45.6 MB) is not a fair comparison for the same reason.

### Plan proofs

Fixture: the 205,000-job template, with detail expiry applied to every job that ended more than 7 days ago (a store whose cleanup has kept up), then `VACUUM ANALYZE` (`eng/perf-pg-index/explain.sh`). `EXPLAIN (ANALYZE, BUFFERS)`.

**History purge (17 §3.2).** Same plan in both: `Index Scan using jobs_terminal_retention`, `Index Cond: application, terminal_at < cutoff`, 500 rows, 1,507 → 1,505 buffers. The narrowed index matches the purge's own conditions exactly.

**Detail expiry (17 §3.1).** The old read was already poor: its state test is a parameter array (`state = ANY(@states)`), which cannot prove the partial `jobs_detail_retention` condition, so the planner used `jobs_terminal_retention` and stepped over every job whose detail had already expired:

```
Revision 26, old read (165 rows):  300 ms, 178,217 buffers
  Index Scan using jobs_terminal_retention on jobs j
    Filter: ((detail_expired_at IS NULL) AND (state = ANY ('{succeeded,failed,cancelled}')))
    Rows Removed by Filter: 175526
```

With the narrowed index that path is gone, and the old read fell back to a sequential scan of `jobs` (202 ms, a hash join over all payloads). So the read was rewritten, same rows, same order, same cursor: one ordered `jobs_detail_retention` range per state (state is that index's first column, so a state array gives no order on its own), merged and cut to the batch size, with the literal terminal state list repeated so the partial index can be proven, the rows locked inside each range (`FOR NO KEY UPDATE SKIP LOCKED`), and the payload size read by key:

```
Revision 28, new read (164 rows):  4.8 ms, 1,490 buffers
  Nested Loop
    Function Scan on unnest s
    Limit -> LockRows -> Index Scan using jobs_detail_retention on jobs x
      Index Cond: (application = 'bench' AND state = s.state AND terminal_at < cutoff)
  SubPlan: Index Scan using job_payloads_pk
```

The new read would be just as fast on revision 26; the speed-up comes from the rewrite. A side effect: a range may lock up to one batch per state of rows that the merged batch does not keep. They are released at commit and are the next batch's rows anyway.

**Job stats, ended jobs.** Rewritten as two branches, detail kept and detail expired, each matching one partial index:

```
1 day, revision 26:  Bitmap Index Scan on jobs_terminal_retention              143 ms, 8,068 rows
1 day, revision 28:  Append: Index Scan using jobs_detail_retention (8,065 rows)
                           + Index Scan using jobs_terminal_retention (0 rows) 114 to 131 ms
30 days, both:       Seq Scan on jobs (74% of the table qualifies)             2.07 s → 1.88 s
```

A 30-day window covering most of the table is a sequential scan before and after; that is the planner's correct choice for a whole-range aggregate and is not new.

**Claim, probe, recovery, lists.** None of their indexes changed. The only plan test that named a removed index was the wake check in `ReadinessPlanTests` (PGX-07), which allowed `jobs_profile_uq` as an alternative to `jobs_pk`; it now allows `jobs_ready` and `jobs_pk` only. That test runs at 10⁵ and 10⁶ rows and is part of the full PostgreSQL suite below.

### Why dropping the composite key is safe

The old key `job_payloads (application, job_id, profile id, profile version) → jobs (…)` refused three things: a payload with no job, a payload whose profile differs from its job's, and a profile change on a job that has a payload. After 0028:

- no job: `job_payloads_job_fk (application, job_id) → jobs`, `ON DELETE CASCADE` as before;
- profile differs: `job_payloads_profile_insert` and `job_payloads_profile_update`, statement-level triggers that join the new payload rows to their jobs by key once per statement (so a set insert of 1,000 payloads costs one join, not 1,000 calls);
- job profile change: `jobs_profile_fixed`, a non-deferrable constraint trigger. It fires only when an UPDATE names a profile column and its value really changes, and it refuses if a payload of that job is then on a different profile. Like the old key (`ON UPDATE NO ACTION`, not deferrable) the check runs at the end of the statement, so one statement that moves a payload and its job to a new profile together is accepted (the encoding tests do exactly that to plant an unsupported profile), while two separate statements are refused. No released statement names those columns, so it never runs in normal work.

Both triggers raise `foreign_key_violation` (23503) with the old constraint name, so error handling that looked at the code or name sees the same thing. Schema validation now fails a store that lacks any of the three triggers. Payloads are never updated and a job's profile is never changed by Runwick, so this is a safety net either way.

Migration cost: on the 205,000-job store the whole migration took 1.9 s. Adding the new foreign key checks every payload, and the index rebuild reads `jobs`; writes to both tables wait meanwhile. The upgrade page now says so.

### Tests

- New `PayloadProfileAgreementTests` (PostgreSQL project): the composite key and unique index are gone; matching payloads (single and set insert) are accepted; a different profile on insert (single, and one row of a set), a payload profile update, a profile change on a job with a payload, and a job-then-payload change in two statements are each refused with 23503 and the old constraint name; a same-value update, a change on a job without a payload, and one statement changing payload and job together (either order) are accepted; an orphan payload is still refused. Passes.
- `MigrationTests`: manifest at revision 28, kill-and-resume after each 0028 phase (expand 27, validate 28, activate 28). With the new test class: 85 of 85 passed.
- `RetentionTests` (PostgreSQL) and the new test: 15 of 15 passed. Contract `JobStatsContractTests` and retention contracts: 46 of 46 passed.
- Smoke: a 200-job drain on a fresh database at revision 28 reconciled; the 2,000-job drain on the retained copy reconciled with no diagnostics.
- Not run by me (for Sonnet): the full PostgreSQL and Contract projects, including `ReadinessPlanTests` at 10⁶ rows.

### Not done

- `jobs_state_list`, `jobs_created_list`, `jobs_queue_list` and `jobs_name_list` are written on every version. Making the state list partial would mean serving live-state filters (pending, running) from `jobs_ready` or `jobs_expired_lease` plus a sort, which costs in proportion to the backlog on every dashboard page. Folding `jobs_queue_list` and `jobs_name_list` (perf plan C1) needs the queue-filtered list rewritten as a per-name merge. Both change dashboard read cost, so they are left for a decision with list-latency measurements.
- Receipt compaction (`storage_commands_durable`, `storage_commands_ownership`): not changed. Their updates are already 78% and 69% HOT, and the remaining gains need the replay-proof work; not low risk enough for this migration.
- The detail-expiry read's error-size lookup uses `job_attempts_errors` with a filter on job ID (it reads every errored attempt of the application per row). That was true before this change and is unchanged; with many errored attempts it would want a keyed index on `job_attempts`.

### Measured results (2026-11-23, Sonnet run on this branch)

Tests, Release build: PostgreSQL 408 of 410 and Contract 501 of 502 in the full runs. The three failures (`EventCursorTests.Rereading_from_a_cursor_returns_the_same_ordered_rows`, `ScheduleCaseTests.Serialized_catch_up_jobs_never_run_concurrently`, and the Redis case of `WorkerEndContractTests.Timeout_versus_delayed_acknowledgement`) each passed 3 of 3 when run alone; they are timing flakes under load, not caused by this change.

Paired drains, 5,000 jobs on a store with 205,000 retained jobs, PostgreSQL 18, base = main before this change, five pairs:

| Pair | Before (jobs/s) | After (jobs/s) | Ratio |
|---|---:|---:|---:|
| 1 | 252 | 261 | 1.04 |
| 2 | 255 | 280 | 1.10 |
| 3 | 284 | 181 | 0.64 |
| 4 | 224 | 271 | 1.21 |
| 5 | 238 | 280 | 1.18 |

- Median ratio 1.10: about 10% more jobs finished per second. Pair 3 is an outlier: in that pair the new build also wrote more WAL than in the other four, which points at something else on the shared host at that moment.
- WAL per job fell from about 25.7 KB to about 23.2 KB (four of five pairs; 26.3 KB in pair 3).
- The `jobs` indexes grew about 3.6 MB per 5,000-job run instead of 4.6 MB.
- Load was 17 at the start of the pairs and 5 at the end, so read the numbers as a direction, not a final figure. Raw files: `artifacts/perf/round-two/pg-index`.

## PostgreSQL grouped producer admission, redesigned (B1)

Written 2026-11-23. Merged as opt-in (`PostgresOptions.GroupProducerAdmission`, default off) with migration `0029_dispatch_group` (built as 0028 and renumbered at merge, after the jobs index diet).

### Short answer

The first version lost 35% to 40% at 8 callers because it let only one (or two) transactions of grouped dispatches run at a time, while the version without grouping ran one transaction per caller side by side. The new version never reduces how many transactions run at once. It groups only callers that would have waited for a producer slot anyway, and every free slot still carries a transaction. With fewer callers than slots it behaves exactly like no grouping, so it cannot lose there by design. It can only help when there are more callers than slots, which is when callers used to queue.

### Why the first version lost

- **One group in flight serialized the callers (the main cause).** The bench gives the producer one slot per caller (`MaxConcurrentCalls = calls`), so without grouping 8 callers had 8 transactions in flight, and PostgreSQL already flushes their commits to disk together. With grouping, every plain dispatch, including the lone one, counted against a limit of one transaction in flight (two in the "g2" arm). 8 callers became one transaction of about 4.5 commands at a time: fewer commits in flight, not fewer per job.
- **Waiters paid two round trips.** A caller that arrived during a send waited for that whole send to finish, then for its own group's send.
- **The collection wait was not the cause.** There was no timer; the queue filled while the previous send was on the wire.
- **The grouped SQL was not the main cause, but it still looped.** It wrote receipts with one `rw_receipt_v1` call per command. The new function writes them with one statement for the whole group.
- Two smaller costs: every group ran on a new background task, and the caller's admission lease was given back when the caller stopped waiting, even though its group was still in flight.

### What changed

- **Grouping moved in front of local admission (core, `DispatchGrouper`).** Local admission is the gate that already limits a process to `MaxConcurrentCalls` producer calls, with at most 64 waiting for up to 1 second. The PostgreSQL store never saw the waiting callers, so grouping there could only take slots away. Now:
  - A caller that finds a slot free sends at once, through the ordinary single path (`dispatch_v4`). Nobody waits to collect company.
  - A caller that finds every slot busy waits for a slot exactly as before: same waiting limit, same wait time, same `LocalCapacityExceededException` on refusal.
  - When a slot frees, the waiting caller that gets it takes up to 8 waiting commands (2 MiB, counting each payload plus 2 KiB) with it in one transaction. The callers it took stop waiting for a slot.
  - The slot stays taken until the group's transaction ends, even if the caller that took it stops waiting. So at most one transaction per slot is in flight, as without grouping, and the admission lease no longer leaks while a group is in flight.
- **A store offers grouping through an optional interface** (`IGroupedDispatchStore`). Only PostgreSQL offers it, and only with `PostgresOptions.GroupProducerAdmission = true` (default off).
- **Set-based SQL** (migration `0029_dispatch_group`, schema revision 29, function `dispatch_group_v1`). The gates are checked once. One insert writes every receipt placeholder in command-ID order. One query checks the other retention class. Only a command whose receipt already existed (a replay or a conflict) goes through `rw_receipt_v1` on its own. Then come one insert each into jobs, payloads, generations and events, and one receipt finalization. The stored rows equal a single dispatch's. A test compares them column by column, receipt included.
- **Failure handling.**
  - A group the server refuses or rolls back (gates, a command that passed its deadline after the writes, a job ID collision, a lock timeout) is resent command by command under the original IDs.
  - A connection failure before COMMIT leaves every command not applied.
  - A lost COMMIT reply makes the outcome unknown for each command under its own ID. `ResolveAsync`, or resending the same prepared dispatch, returns the original receipt.
- **Never grouped:** dispatches with a uniqueness key, concurrency or rate scopes; `DispatchManyAsync`; calls on your own transaction; the outbox, schedules and management dispatch.
- **Bench flags:** `--pg-group` turns grouping on. `--producer-calls n` sets the producer slots independently of the caller count, which is needed to have more callers than slots. The result JSON gains `producerCalls` and `pgGroup` (groups, grouped commands, fallbacks, mean group size).

### Why it is safe

Every command keeps its own operation ID, fingerprint, deadline, receipt and reply. The function decides each command as `dispatch_v4` would, using one decision time for the group. A refused command keeps no receipt, as when a single dispatch rolls back.

Cancellation:
- A caller cancelled while waiting is removed from the queue, and nothing is sent for it.
- A caller cancelled after a group took its command gets `DispatchCanceledAfterSendException` with its own ID. The transaction still settles its receipt.

Concurrency is never above what it was without grouping: at most one transaction per producer slot.

### Tests

Nine tests in `GroupedAdmissionTests`. The client has one producer slot, held by a dispatch stopped at its commit by a fault barrier. They cover:
- Waiting callers commit as one group, with single-dispatch rows and receipts, and each command replays alone to its own receipt.
- A lone caller is sent at once and never waits or groups.
- Mixed outcomes in one group each get their own reply: fresh, replay, command conflict, expired, unregistered queue, fresh. Refused commands keep no receipt.
- A job ID collision rolls the group back and each command is sent alone.
- A caller cancelled while waiting sends nothing.
- A caller cancelled after its group was sent can resolve and resend its operation.
- A lost group reply is outcome unknown for each member under its own ID, and each resends to its receipt.
- A waiting caller refused by local admission sends nothing.
- Keyed dispatches and calls on your own transaction take the ordinary path.

Results: the first run passed 8 of 9. The mixed-outcomes test had put the same command ID in one group twice, which the function rightly refuses; the test was fixed. The rerun of `GroupedAdmissionTests` together with `MigrationTests` (which gained the three 0028 phase cases) passed 93 of 93. The full Core, Postgres and Contract suites have not been run yet.

### Not done

- Storage telemetry records one `Dispatch` operation per group.
- `ManagementSession` dispatch shares the same admission gate but is never grouped.
- Not measured with more than 8 producer slots, or on a quiet host.

### Measured results (2026-11-23, Sonnet run on this branch)

Producer throughput, 8 producer slots (connections), PostgreSQL 18, five pairs per caller count, order alternated, each pair started only when the 1-minute load was under 8 (it was 2 to 7.7). Median inserts per second, without grouping and with it:

| Callers | Without | With | Ratio per pair | p99 latency, ms (without / with) | Jobs per group |
|---:|---:|---:|---|---|---:|
| 1 | 77 | 74 | 1.18, 1.05, 0.87, 1.01, 0.82 | 57 / 57 | no groups |
| 4 | 228 | 238 | 0.87, 0.95, 1.04, 1.46, 0.69 | 86 / 80 | no groups |
| 8 | 388 | 448 | 1.00, 0.83, 1.06, 1.07, 1.16 | 92 / 79 | no groups |
| 16 | 409 | 622 | 1.42, 1.61, 1.60, (base run failed), 1.36 | 151 / 110 | 3.7 to 3.9 |
| 32 | 453 | 1,022 | 2.39, 2.65, 2.17, 2.17, 1.53 | 247 / 111 | 4.9 to 5.1 |

- As designed, nothing is grouped while there are no more callers than slots (1, 4 and 8 callers); those ratios are noise around 1.0.
- With twice as many callers as slots, grouping finished about 1.5 times as many inserts per second; with four times as many, about 2.2 times. p99 latency fell from 151 to 110 ms and from 247 to 111 ms.
- No group fell back to single dispatches.
- In one 16-caller run without grouping, a caller was refused with "Local producer admission is full" (`LocalCapacityExceededException`); that run was dropped from the table. No run with grouping was refused.
- Server time per job (traced 16-caller pair): about 10.2 ms in `dispatch_v4` without grouping, about 7.0 ms per job in `dispatch_group_v1` (12.0 ms per call, 2,922 calls for 5,000 jobs).
- Tests on the branch: Core 464/464, Contract 502/502, Outbox 70/70, Redis 139/139, PostgreSQL 417/418 (an upgrade test that assumed consecutive migration numbers; with the migrations renumbered to 27, 28, 29 that assumption holds again), SQLite 55/56 (`While_another_process_holds_the_writer…`, passed 1 of 3 alone; timing-sensitive under load, not touched by this change).
- Raw files: `artifacts/perf/round-two/b1-redesign`.

## Outbox on a SQLite source: batched delivered marks

**Short answer.** The SQLite outbox source now batches delivered marks the same way the PostgreSQL source does. Marks that wait behind each other are written in one statement and one commit, and each row still checks its own lease token. The option `BatchDeliveredMarks` (already on by default) now applies to SQLite too. Nothing changes in the schema.

### What changed

- `SqliteOutboxSource.MarkDeliveredAsync`: each mark joins a queue. Whoever holds the control lane next writes every queued mark (up to 64) in one `UPDATE … FROM (VALUES …)` inside one write transaction, then commits. A mark that is alone in the queue runs the old single-row statement.
- The relay registration (`AddRunwickSqliteOutbox`) passes `BatchDeliveredMarks` to the source. Setting it to false gives back one statement per mark.
- The single-row guarded update and the "open, begin, commit" steps are now small shared helpers, so renew, transport, retry and block run exactly the SQL they ran before.

### Why it is safe

- Every row is updated only if its `state` is still `leased` and its `lease_token` matches the caller's token. A relay that lost its lease gets `false` for its own message, its row stays as it is, and other marks in the same statement are not affected.
- A caller that gives up (is cancelled) before its mark is taken writes nothing; the next holder of the lane skips it.
- A caller cancelled while it holds the lane rolls the transaction back. Its own mark reports cancelled. The other marks go back in the queue and are written on the next turn. A repeat is harmless because of the token check.
- Any other failure reports the outcome as unknown for every mark in that transaction, the same as one failed mark today. The destination's mapping makes a retry safe.

### Tests

New class `tests/Runwick.Outbox.Tests/SqliteBatchedMarkTests.cs`:

- 12 concurrent marks with one stale token: 11 are written, possibly in shared statements; the stale one returns false and its row stays leased; afterwards the real owner can still mark it.
- A cancelled caller writes nothing, and a lone mark uses the single-row path (no batch statement is counted).
- A relay with four slots and batched marks delivers 24 messages exactly once (24 jobs, 24 mappings).
- A timing test that runs only when the environment variable `RUNWICK_OUTBOX_SQLITE_BENCH` is set to a message count. It stages that many messages and measures delivered per second while a four-slot relay drains them, with batching on and off.

All three contract tests passed. A smoke run of the timing test with 40 messages (not a measurement) showed 54 delivered/s with batching and 19 without.

### Not done

- Lease renewals are still one statement per lease. They happen periodically and are much less frequent than marks, so batching them is not worth the extra queue.
- The P-09 workload (`tests/Runwick.Workloads`, G2 roles) supports only a PostgreSQL source, so there is no paired P-09 run on SQLite. The drain timing test above is the measurement for SQLite.

### Measured results (2026-11-23, Sonnet run on this branch)

Tests, Release build: Outbox 73 passed and 2 skipped, SQLite 56/56, Contract outbox cases 6/6. No failures.

Drain timing, 3,000 messages from a SQLite source, batched marks on and off in the same run (load 3.9 to 7.3):

| Run | Batched marks | One mark per commit | Ratio |
|---:|---:|---:|---:|
| 1 | 127 delivered/s (23.6 s) | 38 delivered/s (79.0 s) | 3.3 |
| 2 | 64 delivered/s (46.9 s) | 41 delivered/s (73.8 s) | 1.6 |
| 3 | 108 delivered/s (27.8 s) | 35 delivered/s (84.8 s) | 3.1 |

Batching delivered 1.6 to 3.3 times as many messages per second. Without it, the speed held steady at 35 to 41 per second. With it, the speed varied more between runs, from 64 to 127 per second.

## Redis against BullMQ with `appendfsync everysec` (G4)

### What we wanted to know

Runwick's `AofEverySecond` profile was measured before, but only against Runwick's own always-fsync profile. The open question was how Runwick compares with BullMQ when both run under everysec, which is what most BullMQ users run.

### How it was run

- Script: `eng/bench-compare/redis-group/pairs-combined.sh`, which now takes `APPENDFSYNC=everysec`. That starts every Redis with `--appendfsync everysec` and passes `--redis-everysec` to the Runwick arms.
- Arms: `base` (Runwick plain), `buf` (Runwick with buffered finish), `bullmq`.
- 20,000-job drains: 5 rounds under everysec, and 3 rounds under always in the same session as a control.
- Steady 500 jobs/s offered: 5 rounds under everysec.
- All 39 runs exited cleanly. Results are in `artifacts/perf/round-two/g4-everysec/`.

### Drain throughput (median jobs/s)

| Arm | everysec | always |
|---|---|---|
| Runwick base | 817 | 431 |
| Runwick buf | 1,178 | 650 |
| BullMQ | 3,183 | 1,275 |

Per-round ratios against BullMQ:

| | Rounds | buf / BullMQ | base / BullMQ |
|---|---|---|---|
| everysec | 1–5 | 0.46, 0.36, 0.43, 0.39, 0.37 | 0.31, 0.22, 0.45, 0.21, 0.30 |
| always | 1–3 | 0.57, 0.34, 0.51 | 0.37, 0.30, 0.34 |

- Everysec speeds up every arm: Runwick base about 1.9×, buf about 1.8×, BullMQ about 2.5×.
- BullMQ gains the most, so the gap gets a little wider. Under everysec, BullMQ drains about 2.7× faster than Runwick buf and about 3.9× faster than base.

### Bytes written to the AOF per job

| Arm | everysec | always |
|---|---|---|
| Runwick base | 5,321 | 5,300 |
| Runwick buf | 5,046 | 5,055 |
| BullMQ | 2,815 | 2,815 |

The fsync setting doesn't change how much is written. Runwick still writes about 1.8–1.9× more per job than BullMQ, and that matches the throughput gap seen under always fsync. It points at the same cause as before: Runwick's job record and its per-job bookkeeping keys, not the fsync cost.

### Steady 500 jobs/s offered, everysec (medians of 5 rounds)

| Arm | completions/s | offer-to-finish p50 | p99 |
|---|---|---|---|
| Runwick base | 488 | 64 ms | 1,119 ms |
| Runwick buf | 494 | 47 ms | 552 ms |
| BullMQ | 487 | 8 ms | 144 ms |

- All three keep up with the offered rate.
- Runwick's latency is much higher and swings more from round to round. One base round had p50 1,211 ms and p99 2,678 ms, and one buf round had p99 1,472 ms.
- Both sides turned away some offers at the producer's local limit: Runwick 41–723 per round (`local_capacity_exceeded`; median base 301, buf 158), BullMQ 52–766 (median 398).

### Caveats

- The host was busy. The 1-minute load at the start of runs ranged from 0.97 to 12.1, and several drain rounds started above 8. The runner only waited for low load before each set, not before each round. The round-to-round spread in the ratios above reflects this.
- BullMQ's latency figure comes from its `latencyMs` field, which we take to mean the same offer-to-finish time.

### What this means

Everysec doesn't change the picture: Runwick is about 2.5–4× behind BullMQ on raw Redis drain speed, under either fsync setting. Buffered finish narrows the gap more than any other setting we have. The remaining gap is about bytes and commands per job, which is the lean-path work (work area F), not durability settings.

## Why Redis Sentinel and Cluster looked about 15% slower

### The question

An earlier three-round result put Sentinel at about 85% of standalone throughput and Cluster at about 88%. The budget is no more than 5% slower. The question was how much of that gap comes from Redis itself (replication, or several nodes sharing one host) and how much from Runwick's Sentinel and Cluster client code.

### How it was run

Five setups, each running a 20,000-job drain with 16 workers, 8 calls and always-fsync. Every run used fresh containers. There were 3 rounds, with the order of the setups rotated each round.

- **A:** standalone Redis.
- **B:** standalone Redis plus 2 replicas, with Runwick still in standalone mode. This isolates the cost of replication.
- **C:** Sentinel (B plus 3 sentinels). Compared with B, this is Runwick's Sentinel client path.
- **D:** a one-node Cluster that owns all slots. Compared with A, this is Runwick's Cluster client path.
- **E:** a three-node Cluster. Compared with D, this is three nodes sharing the host.

Results are in `artifacts/perf/round-two/redis-ha-cause/`.

### Throughput (jobs/s)

| Setup | Round 1 | Round 2 | Round 3 | Median |
|---|---|---|---|---|
| A standalone | 452 | 509 | 247 | 452 |
| B + 2 replicas | 433 | 421 | 390 | 421 |
| C Sentinel | 334 | 599 | 369 | 369 |
| D one-node Cluster | 565 | 710 | 511 | 565 |
| E three-node Cluster | 603 | 461 | 521 | 521 |

Ratios of the medians:

| Comparison | Ratio | What it measures |
|---|---|---|
| B / A | 0.93 | replication |
| C / B | 0.88 | Sentinel client path |
| D / A | 1.25 | Cluster client path |
| E / D | 0.92 | three nodes on one host |

### Work on the primaries per job (all primaries added together)

| | A | B | C | D | E |
|---|---|---|---|---|---|
| evalsha | 0.42 | 0.44 | 0.43 | 0.47 | 0.46 |
| hset | 4.83 | 4.90 | 4.84 | 5.01 | 5.01 |
| zadd | 1.69 | 1.75 | 1.70 | 1.87 | 1.87 |
| server busy time (µs) | 909 | 955 | 1,011 | 862 | 912 |

### What we found

- **Replication** costs about 7%. Primary server time per job goes up about 5%, which matches.
- **Sentinel's client path** looks 12% slower, but Sentinel's own three rounds range from 334 to 599 jobs/s, a wider spread than the effect. Its commands per job match B's, apart from a handful of ping, info and replconf calls. No Runwick overhead shows up.
- **Cluster's client path** costs nothing measurable. The one-node Cluster beat standalone in every round. It sends about 8% more scripts and 4% more hash writes per job, and about 17% more finish round trips, because work that standalone sends in one batch is split by slot. That shows up in the command counts but not in throughput.
- **Three Cluster nodes on one host** look 8% slower on medians, but the per-round ratios disagree (1.07, 0.65, 1.02).
- The per-phase trace (claim, then handler to finish) follows throughput. When the host was slower, every phase was slower. No single phase stands out in C or D.

### Caveats

The host was shared with other sessions' benchmarks and builds. The load gate passed before each round (6.9, 3.8, 5.1), but the load when a setup actually started ranged from 2.2 to 10.1. The same setup swung up to 2× between rounds, and the two slowest runs (A round 3, C round 1) were the most loaded.

### What this means

The 15% gap is not explained by Runwick's Sentinel or Cluster client code. Replication accounts for about 7%, and the rest is within the host noise. These three rounds can't settle whether the true cost is under the 5% budget. That needs alternating A/C and A/E pairs with more rounds on a quiet host.

## Outbox D: grouped delivery under mixed outcomes, stalled owners and crashes

**What this round was for.** The original Outbox D item asks for grouped destination delivery where every message keeps its own transport identity and receipt, where source confirmation stays guarded by each message's lease token, and where mixed outcomes, lost replies and reclaims are tested. Grouped delivery itself was already built (merged 2026-11-15) and has been on by default with groups of four since 2026-11-20. On a PostgreSQL destination, one relay slot sends up to `DeliveryGroupSize` plain messages in one call (`receive_outbox_group_v1`). Each message still gets its own command ID, deadline, receipt and delivery mapping. The relay then marks each message delivered under its own lease token. This round adds no code to the relay or the stores. It adds tests for the three failure shapes that were not yet tested for groups, run on both the PostgreSQL and SQLite sources.

**What is proven, and how.**

- **Mixed outcomes in one group (OUTBOX-08).** Four messages go out in one group call, with no fallback. Two are new. One was already delivered earlier, as if its reply had been lost; the destination answers it from the existing mapping, with the same job. One message's identity already maps to a different job, so the destination reports a conflict. Result: three messages are delivered with their own job IDs, and the conflicting one is blocked with `delivery_conflict` while its mapping still points at the other job. The bad item does not hold back the others. This test passed on both sources in a smoke run.
- **A stalled relay cannot confirm after a takeover (OUTBOX-09).** Relay 1's group hangs at the destination. Its lease renewals report success but never reach the source, which is what a frozen process looks like to its own code. Relay 2 takes over the expired leases and delivers all four messages. When relay 1's group finally returns, each delivered mark relay 1 tries is refused because its lease token is stale. Relay 1 confirms nothing, every row keeps relay 2's result, and there is one job per message.
- **A group that commits after its relay crashed (OUTBOX-10).** Relay 1 dies with a group in flight. The group then commits at the destination with no one listening for the reply, so four jobs exist while the source rows are still leased. A new relay reclaims the leases once they expire. The destination recognises each message from its receipt or mapping, every message is delivered with its proposed job ID, and there are still four jobs.
- **Lost group replies** were already tested (`A_lost_group_reply_sends_the_same_commands_alone_and_creates_no_second_job`). If a group reply is lost, the relay resends the same commands one at a time instead of resending the group, and the per-command receipts replay what the group already applied.

**Not changed.** Grouping still happens only on a PostgreSQL destination. A SQLite destination already shares commits through its producer group, and a Redis destination sends each message alone. Redis grouping is still open.

**Test results.** On the loaded host, all 17 grouped-delivery tests passed when run on their own. The whole outbox suite passed 78 of 79, with 2 bench tests skipped by design. The one failure was OUTBOX-09 on PostgreSQL: it timed out after 30 s waiting for relay 1's stale marks to be refused. It passed 3 of 3 times alone at load 21 to 30, so its wait was raised to 90 s.

**P-09 qualification (2026-11-23).** The host was at load 17 to 37, so the timings are informational. Each run offered 100 messages/s and measured 3 minutes.

| | confirmed/s | ready-to-confirm p50 | p99 | max |
|---|---|---|---|---|
| 4 slots, groups of 4 | 99.5 | 0.30 s | 2.18 s | 2.48 s |
| 16 slots, groups of 4 | 101.6 | 0.15 s | 1.29 s | 3.77 s |
| 4 slots, no grouping | 45.7 | 127 s | 147 s | 148 s |

In every run nothing was lost or delivered twice, and no message was left unconfirmed.

At four slots the relay kept up with the offered rate, but p99 was 0.18 s over the 2 s limit. That matches the 2.1 to 2.3 s seen at similar load on 2026-11-20, against a pass at load 7 to 13. Sixteen slots met both targets. Without grouping, four slots could not keep up at all and the backlog grew for the whole run. So grouping is what makes four slots viable, and the four-slot p99 target is met only on a quieter host.

## SQLite: group commit across write kinds (tried, not merged)

**Idea.** Every SQLite commit waits for a disk flush (`synchronous=FULL`). Only queued dispatches shared a commit, at most 8 per group. The Q6/Q7 runs showed that a short disk stall leaves a backlog, and the backlog drains slowly because every claim, finish and background write pays its own flush. Branch `sqlite-group-commit` (commit abcb5f4) let compatible queued writes of the same priority class share one transaction and one commit:
- each write ran inside its own savepoint, so one failure rolled back only that write
- groups held up to 32 writes and stopped early if a higher-priority write arrived or 25 ms passed
- it was an opt-in setting, `SqliteOptions.GroupCommit`

**Tests (2026-11-23).** These failures are why it is not merged.
- With the option off, one existing test (`Queued_dispatches_share_a_commit_with_independent_results_and_receipts`) failed 3 of 3 times. It passes on main, so the rebuilt default dispatch grouping changed behaviour.
- With the option on, three of the new tests failed every time: the crash image taken mid-group, the failed shared commit, and the queued-dispatch test. A fourth was timing-dependent.
- The Contract suite passed both ways (511 of 511 off, 510 of 511 on, the failure an unrelated load-flaky Redis case).

**Benchmarks.** Load was 10 to 18 and the figures are from single rounds, so they are informational.
- **Plain steady run** (300 s, 3 rounds each way, one dispatch call at a time): no difference. Completions were 39 to 46 jobs/s on both sides, pickup p99 was about 0.2 s, and nothing was grouped at that load.
- **Combined 18-minute run** (32 workers, crashes, cleanup, schedule, outbox):
  - commits per completed job fell 27% (0.83 to 0.61), and dispatch groups grew to 16
  - throughput did not improve: 84.4 against 77.3 durable completions/s
  - pickup p50 got worse: 0.83 s against 0.39 s
  - refusals rose: 15,000 against 10,700
  - both runs lost their last segment at startup (see below), so there was no final drain

**Conclusion.** Fewer commits did not turn into faster pickup or fewer refusals on this host. The idea is not worth the risk it adds to the writer, so it stays on its branch. The SQLite targets of under 2 s pickup p99 and no refusals at 100 offers/s remain unmet on this shared host; the Q6/Q7 sections show they depend on spare worker capacity and on disk stalls.

**Harness fix found on the way.** A crash-run segment that reopens a large, busy store (about 670 MB here) could fail its first management query, setting the retention policy, with `ManagementQueryBudgetException`, and exit before running. The bench now retries that query up to 10 times, 3 s apart.

## G5 fewer lease renewals: measurement modes

The fewer-renewals profile (`WorkerOptions.UseFewerLeaseRenewals()`: a 5 minute lease renewed about every 90 s) only had a
fake-time test. The `bench` command in `tests/Runwick.Workloads` can now measure it on a real store in three ways. Every flag is
opt-in; a bench run without them behaves and reports exactly as before.

### What was built

1. **Lease settings and a cost report.** `--fewer-lease-renewals` turns the profile on. `--lease-seconds n` and
   `--renew-seconds m` set the lease and renewal interval by hand (jitter is kept at no more than a fifth of the interval). Any of
   these flags, or `--lease-report` for the default profile, adds a `lease` block to the result JSON.
2. **Crash-detection latency.** `bench --lease-crash` starts a worker process that takes one long job per slot, kills it with
   SIGKILL as soon as every slot is busy, and starts a second worker on the same store at once. It reports how long each of the
   killed worker's jobs took to start on the second worker. Works on SQLite, PostgreSQL and Redis.
3. **Store outage.** In steady mode, `--outage-container NAME --outage-at s --outage-seconds d` runs `docker pause NAME` s seconds
   into the run and `docker unpause NAME` d seconds later, then reports what happened to the jobs that were running. PostgreSQL and
   Redis only (SQLite has no container).

### The new output fields

`lease` block (sweep and outage runs):

| Field | Meaning |
|---|---|
| `leaseSeconds`, `renewSeconds`, `renewJitterSeconds` | The lease settings the worker actually ran with. |
| `runSeconds` | Time from just before the worker started to the end of the measurement. |
| `jobsFinished`, `completionsPerSec` | Load jobs whose finish the store confirmed, and that number per second of `runSeconds`. |
| `renewsSent` | Lease renewals the worker sent (one per running job per renewal, whether sent alone or in a batch). |
| `renewManyCalls` | How many of those went out as one batched RenewMany store call. |
| `renewsPerJob`, `renewsPerSec` | `renewsSent` per finished job and per second. |
| `lostByCause` | Leases the worker lost, by cause (for example `SafetyDeadline`, `StaleLease`). Empty means none was lost. |
| `storeCounters` | What the store itself counted during the run (after minus before). Redis: `cpuSeconds` (server CPU), `commandCalls`, `commandUsec` and `calls:<command>`. PostgreSQL: `transactions` and `rowsWritten`; with `--trace` also `functionCalls:<fn>` and `functionMs:<fn>` per Runwick SQL function (PostgreSQL does not report its own CPU). SQLite: `null`; the existing `sqlite.writeCommits` field gives the write count. |

`outage` block (outage runs):

| Field | Meaning |
|---|---|
| `pauseExit`, `unpauseExit`, `pausedForMs` | Exit codes of the docker commands and how long the store was actually paused. |
| `runningAtPause` | Load jobs whose handler was running when the store was paused. |
| `keptLeaseAndFinishedOnce` | Of those, how many kept their lease, ran once and finished. |
| `lostLease` | Of those, how many lost their lease (the worker gave the job up; it runs again later). |
| `ranTwice` | Of those, how many had their handler started more than once. |
| `finishedAfterStoreBack` | Of those, how many finished after the store came back. |
| `notFinished` | Of those, how many had not finished when the run ended. |
| `allJobsRunMoreThanOnce` | Every load job of the run whose handler started more than once. |

Error codes during the outage are in the existing fields: `failedDispatchByCode` (refused dispatch calls), `lease.lostByCause`,
and `diagnostics`.

`--lease-crash` result:

| Field | Meaning |
|---|---|
| `leaseSeconds` | The lease the run used (300 with `--fewer-lease-renewals`). |
| `jobsAccepted` | Jobs the first worker dispatched. |
| `orphans` | Jobs the first worker was running when it was killed. |
| `orphansPickedUp`, `orphansNotPickedUp` | How many of those the second worker started, and how many it never started before `--detect-seconds` (default twice the lease plus 60 s). |
| `detectionMinMs`, `detectionMs` | Time from the kill to the second worker starting each orphan (minimum, and count/p50/p95/p99/max). Expect about the lease length plus up to one recovery scan (5 s) and the second worker's startup. The kill comes right after the jobs started, so this is the upper end: a worker that dies just before a renewal frees its jobs one renewal interval sooner. |
| `secondWorker` | The second worker's own bench result. |
| `directory` | Ledgers, child logs and (SQLite) the database file; kept for inspection. |

### Full-run commands

Run each from `tests/Runwick.Workloads` after `dotnet build -c Release`, one at a time, under the bench lock. `B` stands for
`flock /tmp/runwick-bench.lock dotnet bin/Release/net10.0/Runwick.Workloads.dll bench`. PostgreSQL uses the default bench
container (`runwick-bench-pg` on port 55499). Redis needs a throwaway instance started with `--appendonly yes --appendfsync always
--aof-load-truncated no --maxmemory-policy noeviction --save ''`; `$REDIS` is its `host:port` and `$REDIS_CONTAINER` its container
name. Put the store flags (`--store sqlite`, `--store postgres`, or `--store redis --redis $REDIS`) where `STORE` is written.

**1. Timeout sweep** (three arms per store, about 7 minutes each, about 1 hour for all nine):

```
$B STORE --mode steady --workers 16 --job-ms 120000 --rate 0.12 --seconds 300 --lease-report --out sweep-STORE-default.json
$B STORE --mode steady --workers 16 --job-ms 120000 --rate 0.12 --seconds 300 --lease-seconds 120 --renew-seconds 30 --out sweep-STORE-120.json
$B STORE --mode steady --workers 16 --job-ms 120000 --rate 0.12 --seconds 300 --fewer-lease-renewals --out sweep-STORE-fewer.json
```

Add `--trace` on PostgreSQL to get per-function call counts in `storeCounters`. Compare `lease.renewsPerSec`, `lease.renewsPerJob`,
`lease.completionsPerSec` and `lease.storeCounters` across the arms. Jobs still running at the end are cancelled by the graceful stop.

**2. Crash-detection latency** (three arms per store; about 40 s, 75 s and 5.5 minutes; about 22 minutes for all nine):

```
$B STORE --lease-crash --workers 16 --lease-seconds 30 --renew-seconds 5 --out crash-STORE-30.json
$B STORE --lease-crash --workers 16 --out crash-STORE-default.json
$B STORE --lease-crash --workers 16 --fewer-lease-renewals --out crash-STORE-fewer.json
```

**3. Pause and outage** (PostgreSQL and Redis; four arms each, an outage below and above the lease for each profile; about
5 minutes each except the 360 s arm at about 8 minutes; about 45 minutes for all eight):

```
$B STORE --mode steady --workers 16 --job-ms 20000 --rate 0.5 --seconds 240 --outage-container C --outage-at 60 --outage-seconds 20  --out outage-STORE-default-20.json
$B STORE --mode steady --workers 16 --job-ms 20000 --rate 0.5 --seconds 240 --outage-container C --outage-at 60 --outage-seconds 90  --out outage-STORE-default-90.json
$B STORE --mode steady --workers 16 --job-ms 20000 --rate 0.5 --seconds 240 --outage-container C --outage-at 60 --outage-seconds 120 --fewer-lease-renewals --out outage-STORE-fewer-120.json
$B STORE --mode steady --workers 16 --job-ms 20000 --rate 0.5 --seconds 240 --outage-container C --outage-at 60 --outage-seconds 360 --fewer-lease-renewals --out outage-STORE-fewer-360.json
```

`C` is `runwick-bench-pg` for PostgreSQL and `$REDIS_CONTAINER` for Redis. The outage pauses the whole container, so nothing else
may use it during the run. If a run is killed while the store is paused, run `docker unpause C` by hand.

### Not built

- Store CPU for PostgreSQL and SQLite: neither reports it; PostgreSQL gives transactions, rows and (with `--trace`) function calls.
- The crash run kills right after the jobs start; there is no flag to kill at a chosen point in the renewal cycle.
- No SQLite outage mode (an in-process store has nothing to pause).

### Measured results (2026-11-23, Sonnet run)

All 24 runs exited cleanly. Raw files are in `artifacts/perf/round-two/g5-leases/`. The host was shared: the 1-minute load before a run ranged from 0.6 to 51, and 14 of 26 readings were 8 or more. The crash-detection times are bounded by the lease and agree across stores, so the load hardly touches them. Treat the store CPU figures with care.

#### Lease sweep: what each setting costs

Each run kept long jobs (20 s each) running; 29 jobs finished per run in every arm, and no lease was lost.

| Store | Lease / renew (s) | Renewals per job | Renewals per second | Store counters |
|---|---|---|---|---|
| SQLite | 60 / 15 (default) | 9.07 | 0.73 | 382 write commits |
| SQLite | 120 / 30 | 4.31 | 0.35 | 342 write commits |
| SQLite | 300 / 90 (fewer) | 1.10 | 0.09 | 284 write commits |
| PostgreSQL | 60 / 15 | 9.17 | 0.74 | 1,104 transactions; 41 single and 80 batched renew calls |
| PostgreSQL | 120 / 30 | 4.14 | 0.33 | 1,061 transactions; 58 and 28 |
| PostgreSQL | 300 / 90 | 1.14 | 0.09 | 992 transactions; 12 and 10 |
| Redis | 60 / 15 | 9.21 | 0.74 | 10,072 commands; 6.20 s CPU |
| Redis | 120 / 30 | 4.28 | 0.34 | 9,565 commands; 5.83 s CPU |
| Redis | 300 / 90 | 1.14 | 0.09 | 9,022 commands; 4.69 s CPU |

The fewer-renewals profile sends about 8 times fewer renewals. Total store traffic falls only 10 to 20% in this test, because the jobs' own claim and finish work is most of it. The saving grows with how long jobs run: renewals matter for jobs that run minutes, not milliseconds.

#### Crash detection: how long until another worker takes over

A worker was killed with 16 jobs running. A second worker picked up all 16 in every run; none were missed.

| Store | 30 s lease | Default (60 s) | Fewer renewals (300 s) |
|---|---|---|---|
| SQLite | 36.2 s (35.3–37.5) | 65.6 s (64.6–67.7) | 304.4 s (303.3–305.4) |
| PostgreSQL | 36.2 s (35.4–37.4) | 65.9 s (64.4–66.9) | 307.9 s (307.3–308.5) |
| Redis | 34.3 s (33.7–36.1) | 65.3 s (64.6–66.8) | 305.0 s (304.1–306.2) |

Medians, with minimum and maximum in brackets. Takeover comes at the lease length plus 4 to 8 seconds on every store. That is the price of the fewer-renewals profile: about 5 minutes instead of about 1 before a crashed worker's jobs run again.

#### Store outage: the store paused while 10 jobs run

| Store | Profile | Pause | Kept lease, ran once | Lost lease | Ran twice | Finished after store came back | Not finished at end | Error codes |
|---|---|---|---|---|---|---|---|---|
| PostgreSQL | default | 20 s | 10 | 0 | 0 | 10 | 0 | none |
| PostgreSQL | default | 90 s | 1 | 7 | 9 | 9 | 0 | 7 safety deadline; 17 `store_unavailable` |
| PostgreSQL | fewer | 120 s | 0 | 0 | 0 | 0 | 10 | 23 `store_unavailable` |
| PostgreSQL | fewer | 360 s | 1 | 1 | 9 | 9 | 0 | 1 safety deadline; 36 `store_unavailable` |
| Redis | default | 20 s | 10 | 0 | 0 | 6 | 0 | 3 `outcome_unknown` |
| Redis | default | 90 s | 1 | 0 | 9 | 9 | 0 | 4 `outcome_unknown`; 41 `store_unavailable` |
| Redis | fewer | 120 s | 0 | 0 | 0 | 0 | 10 | 4 `outcome_unknown`; 56 `store_unavailable` |
| Redis | fewer | 360 s | 0 | 0 | 11 | 11 | 0 | 4 `outcome_unknown`; 85 `store_unavailable` |

(Redis fewer 360 s had 11 jobs running at the pause, not 10.)

What it shows:

- **An outage shorter than the lease is harmless.** With the default profile and a 20 s pause, every running job kept its lease and finished once, on both stores.
- **An outage longer than the lease makes jobs run again.** With the default profile and a 90 s pause, 9 of 10 jobs ran twice on both stores. With fewer renewals and a 360 s pause (longer than its 5 min lease), 9 to 11 ran twice. No job was lost; this is the at-least-once rule working as documented.
- **Fewer renewals rides out longer outages, but this run didn't wait long enough to see the end.** With a 120 s pause no job lost its lease or ran twice, but all 10 were still unfinished when the 240 s run ended. The finishes were still backed up behind the pause. A longer run window is needed to confirm they then finish once.
- Every outage longer than 20 s left an "unconfirmed outcome: Finish" entry in diagnostics, as expected when a finish call is in flight while the store disappears.

Not explained yet, and worth a look before trusting the Redis outage rows in detail:

- Redis default 90 s: 9 jobs ran twice, but the worker reported no lost lease. On PostgreSQL the same case reported 7 lost leases. Either the Redis path loses the lease without counting it, or the harness counts it differently on Redis.
- Redis default 20 s: all 10 kept their lease and finished once, but only 6 are counted as finished after the store came back. The other 4 probably finished before the pause took effect, or ended in `outcome_unknown` (3 of those were seen).

## PostgreSQL receipt compaction (migration 0030)

**Result 2026-11-23: merged as the default.** Paired drains, 5 rounds of 5,000 jobs, base main at fb6a493 against this change, private PostgreSQL 18 (no other writers), host load 7 to 12:

| Arm | jobs/s, median (range) | WAL bytes per job, median (range) | Receipt-table growth, median (range) |
|---|---|---|---|
| Base | 309 (223–349) | 10,760 (10,680–10,947) | 6.10 MB (6.03–6.23) |
| Receipt compaction | 330 (205–401) | 10,501 (10,290–10,587) | 6.96 MB (6.87–7.09) |

WAL per job fell 2.4%, close to the expected 3%. The receipt tables grew 14% more during the drain (the 8% above is for tables plus indexes; this is growth of the tables during the run). Throughput is unchanged within noise. Tests: the targeted PostgreSQL classes 155 of 155; the full PostgreSQL project 426 passed, with 3 outage tests that need a Docker-managed server skipped by failure on an external one; the Contract project 508 of 511, the 3 failures being load-flaky Redis cases that pass on rerun (this change touches no Redis code). Raw files: `artifacts/perf/round-two/pg-receipts/`.

Written 2026-11-23. Branch `pg-receipt-compaction`, from `main` at `2e9f2c8`. Migration `0030_receipt_by_key`, schema revision 30, on by default: stored rows, receipt results, replies and replay are unchanged, so nothing a caller sees changes. Measurements are single 2,000-job drains on a private PostgreSQL 18.7 server (no other writers, so the WAL figures are this drain's alone). Byte counts do not depend on host load; the jobs/s figures do and are not a result.

### Short answer

Every command a worker or producer sends leaves a receipt row, so a retried command with the same ID gets the same answer. On PostgreSQL that receipt is written twice: a placeholder is inserted when the command starts and is then updated with the decision. Receipts were about **1.5 kB of WAL per drained job, 14% of all WAL** (11 kB per job). Two changes cut that by **22%, to 1.2 kB per job** (about 3% of all WAL):

1. **One receipt index fewer.** `command_by_id (application, command_id)` was written on every receipt row version but only read by two checks that look a command up without its class. A receipt has one of two classes, so those checks now name the class and use the primary key.
2. **The decision update stays on its page.** The decided row (about 460 bytes for a Finish, about 2 kB for a batched claim) is bigger than its placeholder. On a full page PostgreSQL has to put it elsewhere, and then it writes a new entry into every receipt index. Half of the durable receipts went that way. The receipt tables now keep 30% of each new page free (`fillfactor = 70`), and 89% of those updates now stay on the page, so no index entries are written for them.

The cost is disk space: the receipt tables take about 25% more space for the same rows, and the receipt tables plus their indexes about 8% more. Receipts are kept 10 minutes (claims) or 24 hours (everything else) past their deadline, so this is a bounded amount.

Not built: storing a smaller receipt result, the way Redis did (claim receipt 1,263 → 244 bytes). That is where most of the remaining bytes are (see "What is left"), but it changes the stored receipt format, so old and new receipts would both have to be readable and `ResolveCommand` would return a different raw result. That needs a receipt codec version 2 and its own replay proofs, so it is left for a decision.

### What a receipt costs today (revision 29)

Method (`eng/perf-pg-receipts/measure.sh`): a 20-job warm-up drain creates and migrates the database, the receipts are emptied, CHECKPOINT, then a 2,000-job drain (`--payload 512`, 16 workers, default batched claim and finish). WAL is read with `pg_walinspect` for the drain's LSN range and grouped by the relation each record touches ("record" = the whole WAL record including headers; a record touches one relation here).

| Relation | WAL per job, revision 29 | Revision 30 |
|---|---:|---:|
| `storage_commands_durable` (heap) | 727 | 656 |
| `storage_commands_ownership` (heap) | 269 | 251 |
| durable `command_retention` index | 182 | 141 |
| durable primary key | 142 | 105 |
| durable `command_by_id` | 130 | – |
| ownership indexes (three, then two) | 59 | 31 |
| **All receipt relations** | **1,509** | **1,184** |
| All WAL of the drain | 10,963 | 10,573 |

The total WAL line moves by more than the receipt lines explain (other relations vary run to run by a few hundred bytes per job: autovacuum and checkpoint timing); read the receipt lines, not the total.

Receipts per drained job and their size (both revisions identical):

| Receipt | Per job | Row bytes | Of which the stored result |
|---|---:|---:|---:|
| Finish (durable, one per job, written through FinishMany) | 1.00 | 464 | 311 |
| ReserveAndBegin (ownership, one per claim of about 5.5 jobs) | 0.18 | about 2,100 | about 1,950 |
| DispatchMany (durable, one per 100-job bulk add) | 0.01 | 3,200 | 3,040 |

Placeholder inserts and decision updates, durable / ownership partition:

| | Inserts | Updates | Of which HOT (no index entries) |
|---|---:|---:|---:|
| Revision 29 | 2,045 / 359 | 2,045 / 357 | 1,010 (49%) / 277 (78%) |
| Revision 30 | 2,045 / 348 | 2,045 / 346 | 1,812 (89%) / 346 (100%) |

Sizes after the drain, bytes (heap + indexes): revision 29 durable 1,114,112 + 458,752, ownership 548,864 + 114,688, total 2,236,416; revision 30 durable 1,392,640 + 327,680, ownership 622,592 + 65,536, total 2,408,448. No full-page images worth counting appeared on receipt relations in either run: the drains were short and started right after a checkpoint, and command IDs are time-ordered (UUIDv7), so index inserts land on the same few right-hand pages. Earlier evidence agrees on the scale: making both receipt partitions unlogged (the G2 probe, 2026-11-15) cut WAL 8% to 12%.

### Choosing the fill factor

Probes with the same revision-30 binary, the fill factor set by hand before the drain (`FF=` in `measure.sh`):

| Fill factor | Durable updates HOT | Receipt WAL per job | Durable heap after the drain |
|---|---:|---:|---:|
| 100 (default, index already dropped) | 49% | 1,382 | 1.14 MB |
| 70 | 86% | 1,215 | 1.38 MB |
| 50 | 97% | 1,159 | 1.71 MB |

70 takes most of the gain for half the extra space of 50. A FinishMany call decides up to its whole batch in one update statement, so a page needs room for several decided rows at once; that is why a few updates still miss at 70.

### What changed

- `0030_receipt_by_key.sql`: `DROP INDEX command_by_id`; `fillfactor = 70` on both receipt partitions (new pages only, nothing is rewritten, the lock is brief); `rw_receipt_v1`, `rw_schedule_group_set_v1` and `dispatch_group_v1` dropped and recreated with the same names, arguments and bodies except the one predicate. The R3 check (is this ID already used by a command of the other class?) now reads `retention_class = CASE @class WHEN 'ownership' THEN 'durable' ELSE 'ownership' END`; the schedule group's "does any of these commands already have a receipt" check reads `retention_class IN ('ownership', 'durable')`. Both are primary-key lookups on PostgreSQL 16 to 18 (no skip scan needed).
- The driver's own R3 statement (`PgRunner`) and management's `CommandNotAfter` read the same way.
- Schema validation: an index that a later migration drops (and does not recreate) is no longer expected.
- Replay is untouched: R2 (read the existing receipt and return its stored decision) already used the primary key, and no stored value changed.

### Tests

- New `ReceiptByKeyTests` (PostgreSQL project), both passed on a private PostgreSQL 18 (one run of the class alone, 15 s):
  - RCPT-01 `Receipts_written_at_revision_29_replay_after_the_upgrade_to_30`: puts the store back to exactly revision 29's shape (the index, the default fill factor and the three revision-29 function bodies, checked against revision 29's expected function digests), writes a Dispatch, a ReserveAndBegin, a Finish and 40 more jobs through those functions, replays one, marks the store as revision 29 and runs the migrator, which applies only 0030's three phases. Then: the index is gone, the fill factor is set, the receipt count is unchanged, and the Dispatch, ReserveAndBegin and Finish replay with `Replayed = true`, the same job, attempt, value and decision time, and no new jobs. `CommandNotAfter` finds receipts of both classes. A newly opened client accepts the store's functions and dispatches.
  - RCPT-02 `The_same_id_under_the_other_retention_class_conflicts_in_both_directions`: a Dispatch ID reused for ReserveAndBegin and for Reserve, and a ReserveAndBegin ID reused for Dispatch, are each `CommandConflict` with no effect; the store snapshot and receipt count are unchanged.
- `MigrationTests`: manifest at revision 30, kill-and-resume after each 0030 phase (expand 29, validate 30, activate 30). Not run by me.
- Not run by me (for Sonnet): the full PostgreSQL and Contract projects. The ones that exercise the changed statements: `CommandIdentityTests` (CMD-02 already reuses a Dispatch ID for Reserve), `ScheduleStoreTests` and `ScheduleCaseTests` (the schedule group check), `GroupedAdmissionTests` (dispatch_group_v1), `UpgradeTests`, `MigrationTests`, `SchemaContractTests`.

### Paired drains (for Sonnet)

`eng/perf-pg-receipts/pairs.sh` alternates base and candidate on a fresh database per arm (each binary creates and migrates its own database with a 20-job warm-up, then CHECKPOINT and the measured drain), and prints jobs/s, WAL bytes per job and receipt-table growth per arm. Use a private server so the WAL figure is the drain's own:

```
docker run -d --name runwick-rcpt-pg -e POSTGRES_PASSWORD=pg -p 127.0.0.1:55641:5432 postgres:18 \
  -c shared_buffers=512MB -c max_connections=300 -c max_wal_size=8GB
git worktree add /tmp/rcpt-base 2e9f2c8   # main before this change
dotnet build -c Release /tmp/rcpt-base/tests/Runwick.Workloads -o /tmp/rcpt/base-bin
dotnet build -c Release tests/Runwick.Workloads -o /tmp/rcpt/cand-bin       # this branch
eng/perf-pg-receipts/pairs.sh /tmp/rcpt/base-bin /tmp/rcpt/cand-bin /tmp/rcpt/pairs 5 5000
```

Expected: WAL per job about 3% lower (the receipt share above), receipt-table growth about 8% higher, jobs/s unchanged within noise. `eng/perf-pg-receipts/measure.sh <bin> <db> 2000` gives the per-relation breakdown for one binary.

### What is left

- The stored results are now the largest part: about 310 bytes per Finish and about 2 kB per batched claim, written in full in the WAL of the decision update. A compact result (field codes, no repeated job ID, no nulls, no lease and policy echo that the claim reply can rebuild) would roughly halve the heap WAL, about another 400 bytes per job by the Redis precedent. That needs receipt codec version 2, readers for both versions, and replay tests across the switch; it is a decision, not a free change.
- The placeholder insert plus decision update is the receipt protocol (R1 to R4) itself; folding them would change how concurrent sends of the same command wait for each other, so it is not touched.
- SQLite has the same `command_by_id` index; not changed here.
