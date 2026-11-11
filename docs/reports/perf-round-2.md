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

The initial steady fixture uses `--mode steady --rate 100 --seconds 30 --workers 16 --calls 16 --payload 1024 --trace`. These are no-op jobs with a nominal 1 KiB payload. The harness can now preserve a drained fixture with `--sqlite-keep` and copy it into a fresh database with `--sqlite-seed`. An eight-job, nominal 1 MiB drain and a 20-job, 64 KiB drain copied from a 100-terminal-job fixture both settled every job. Larger retained-history and multi-process runs are still pending.

In the 64 KiB smoke run, the writer recorded 818 statements during drain and opened one connection. After shutdown the WAL held 953 log frames at the configured 1,000-page automatic checkpoint threshold. The harness queries `PRAGMA wal_checkpoint(NOOP)` after the timed interval on SQLite 3.51 and later. This reads frame counts without running a checkpoint. A WAL hook would replace SQLite's automatic checkpoint callback. [SQLite checkpoint interface](https://www.sqlite.org/c3ref/wal_checkpoint_v2.html), [SQLite automatic checkpoint interface](https://www.sqlite.org/c3ref/wal_autocheckpoint.html).

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

### B4 SQLite settlement experiment

An isolated build used one completion consumer to give ready finishes time to collect behind the previous call. Five paired 1,000-job drains with 16 workers reduced median committed settlement calls from 289 to 183 and total writer commits from 442 to 376. Median throughput was 101.6 jobs/s with four consumers and 104.4 with one. The median paired gain was 3.9%, and individual pairs varied from a 16.6% drop to a 39.9% gain. Production uses four completion consumers. The next finish experiment should account for transaction body cost and protect unrelated finishes during receipt resolution.

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

```sh
BENCH_TRACE=1 BENCH_ROUNDS=5 BENCH_ENQUEUE_N=750 BENCH_WARMUP_JOBS=128 \
  BENCH_TESTS='enqueue-c1 enqueue-c16' \
  eng/bench-compare/ab.sh "$transactional_dll" "$post_commit_dll" artifacts/perf/round-two/postgres-a1-warm-ab
```

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

## Worker payload decoding

E1 uses the strict payload scanner without constructing canonical values before worker deserialization. The same scanner still builds the canonical form for fingerprints. All 102 focused boundary, serialization and wire tests passed, including rejection-code parity and the existing canonical vectors.

Five alternating Release builds ran a synchronous decode probe with warm-up and four equally sized ASCII string properties. The table reports median bytes allocated on the calling thread per decode, including deserialization.

| Payload bytes | Baseline | Validation only | Reduction |
|---:|---:|---:|---:|
| 1,021 | 8,984 | 3,176 | 65% |
| 65,533 | 460,536 | 132,200 | 71% |
| 1,048,029 | 6,322,143 | 2,098,112 | 67% |

The probe excludes receipt parsing, store reads and handler execution. The allocation probe source and per-run outputs are under `artifacts/perf/round-two/`.

## Store dashboard

M1 adds a Store page and read-only snapshot for callers with `Settings.Read`. It reuses the process health probe for store and admission metadata, retains the last descriptor for up to five minutes, and labels stale and unavailable facts. Browser refreshes read the cached snapshot. Chromium checked the page at desktop and mobile widths. A follow-up uses the same probe to show PostgreSQL server and SQLite library versions with their respective node and process scope. All seven focused dashboard tests passed, including the real PostgreSQL outage, version expiry and generated asset checks. Provider collectors, topology observations and paired dashboard overhead measurements are still pending.
