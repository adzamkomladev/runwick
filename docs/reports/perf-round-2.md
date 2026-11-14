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

