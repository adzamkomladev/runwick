# G-2 measurement workloads: design, parameters and smoke results

Date: 2026-11-01. Scope: the 15 §11 measurements that need new workloads — P-04, P-05, P-06 (rate and scheduler halves) and P-09 — in `tests/Runwick.Workloads`. The G1-M1 and G1-M3 reruns use the unchanged G-1 workloads. This document describes each workload, its parameters and its deviations from [performance-acceptance-plan](../architecture/performance-acceptance-plan.md), and records one short smoke run per workload. **No result here is a G-2 measurement**: the smoke runs are minutes long, on a shared host, and only show that each workload runs end to end.

## 1. Runner

```
tests/Runwick.Workloads/bin/Release/net10.0/Runwick.Workloads run <name> [--out dir] [overrides]
```

| Name | Item | Measures |
|---|---|---|
| `P-04` | SP-012 | Fairness: queue weights 1:3, lanes 6:3:1 without and with aging, and a 100 ms CPU-bound handler run |
| `P-05` | SP-012 | Contention at 1, 3 and 6 worker replicas: unconstrained, and shared ceilings with heterogeneous capability sets and one hot key |
| `P-06r` | SP-013 | Rate half: 100 authorizations/s rolling window with 1 and 8 scopes, plus rejected-Begin churn |
| `P-06s` | SP-016 | Scheduler half: 1,000 minute schedules, then a 1,000-occurrence catch-up episode |
| `P-09` | SP-017 | Outbox: 100 messages/s, 4 relay slots, 1 KiB intent; injected lost marks |

The defaults are the full-length gate parameters, so `eng/run-g2.sh` can call `run <name> --out <dir>/<timestamp>-<name>` with no other arguments. Every option below overrides one default; a smoke run passes shorter durations and counts.

Shared behaviour, as in G-1 (07 §14):

- one Testcontainers PostgreSQL 18 per invocation, or `RUNWICK_TEST_PG` when set;
- one database per repetition (`wl_<name>_<label>`), migrated to schema revision 7, dropped after a run without failure;
- `env.json` with the 07 §14.2 fields;
- the 5 s store sampler (`db-samples.jsonl`) and the per-process sample and summary files are kept as raw samples;
- workers are the shipped `AddRunwick(...).UsePostgres(...).AddWorker(...)` composition with `TimingStore`, in their own processes;
- a top-level `result.json` with `runs[]` (each with `failure`, `correctness.violationCount`, `extra`), `correctnessViolations`, `runFailures` and a per-workload `summary`;
- the exit code is 0 when there are no correctness violations and 1 otherwise. A threshold miss is reported, not failed (07 §14.1).

Correctness per run: the G-1 checks (ReferenceModel snapshot invariants, at most one succeeded attempt per job, handler invocations ≤ begun executions, every accepted dispatch stored, nothing open after a drain, connection and ticket caps), plus three G-2 checks:

- **Lease losses.** Any `lost:*` engine event in any worker counts as a violation, because no G-2 workload injects a lease fault.
- **Workload-specific identity checks.** These are listed per workload below: rolling-window bound, occurrence identities, delivery mappings.
- **Each violation is counted.** Every violation adds 1 to `correctness.violationCount`.

New process modes, used by the orchestrator and not meant to be called by hand:

| Mode | Process |
|---|---|
| `worker --g2` | A worker for the G-2 contracts. `--queues q:w,…`, `--lane-mode`, `--handlers` (a subset of `work,a,b,c,rated1,rated8,tick,out`) and `--spin` (CPU-bound handler) |
| `produce2` | Two modes. `poisson`: open-loop arrivals at `--rate`, as G-1. `topup`: a lane feeder that keeps every lane continuously claimable |
| `scheduler` | `AddScheduler()` with `--schedules N` minute schedules, plus the catch-up schedule with `--catchup` |
| `relay` | Destination host plus `AddRunwickPostgresOutbox` with `--slots`. The source adapter is wrapped so that a fraction of delivered marks can be lost |
| `stage` | Outbox business writers on the source database. The relay is disabled in this process |

Contracts (`G2Jobs.cs`) are registered identically in every child of a run. A run's policy arguments (ceilings, rate limit) are passed to all of its children.

## 2. P-04 fairness/CPU (SP-012)

Each variant is a separate run with its own database:

- one worker process with fixed 16 and a no-op handler;
- a lane feeder (`produce2 --mode topup`) that keeps each lane continuously claimable. Every 250 ms it reads the lane's pending count from the store and dispatches (DispatchMany, ≤ 32 per batch) up to `max(--min-backlog, ⌈consumption rate × h⌉)` jobs, where `h` is the lane's horizon. With a short horizon the oldest job stays young; a long horizon makes a lane age past 60 s.

After warm-up, the run counts the first `--admissions` Applied reservations (`job_attempts.reserved_at` ≥ measure start, in order) by queue and lane. It compares them with the expected vector, which comes from **replaying the engine's own `LaneSelector`** (05 §8.4–§8.5) over the same lane set and aging state. The deviation is reported in percentage points.

| Variant | Setup | Expected | Threshold |
|---|---|---|---|
| a | queues `fa:1, fb:3`, one lane each | 25 / 75 | ≤ 2 pp over ≥ 10,000 admissions |
| b | queue `fl`, lanes high/medium/low 6:3:1, horizon 5 s | 60 / 30 / 10 | ≤ 2 pp over ≥ 10,000 admissions |
| c | as b, medium/low horizon 120 s. Measurement starts once both lower lanes are ≥ 65 s old | selector replay with the lower lanes aged (55.6 / 33.3 / 11.1) | recorded ("aging follows the CAP vectors") |
| d | as a, `--cpu-concurrency 4` slots, 100 ms **spinning** handler | 25 / 75 | recorded; reports CPU ms per job |

Every run also records:

- each lane's minimum and maximum oldest-eligible age during the window. For b, `agingPossible` is true if a lower lane reached 60 s;
- worker CPU per finished job, and the same minus the handler's 100 ms in d.

| Option | Default |
|---|---|
| `--variants` | `a,b,c,d` |
| `--admissions` | 10000 |
| `--warmup-min` | 2 |
| `--max-measure-min` | 300 per variant (the run stops early when the count is reached) |
| `--lane-horizon-s` / `--aged-horizon-s` | 5 / 120 |
| `--aged-wait-min` | 15 (c only) |
| `--concurrency` / `--cpu-concurrency` / `--cpu-handler-ms` | 16 / 4 / 100 |
| `--min-backlog` | 8 |
| `--drain-min` | 60 |

Deviations from the plan:

1. **"Without aging" is not a configuration in `Weighted` mode.** In `Weighted` mode aging is always enabled (05 §8.5), and `Strict` disables aging but also the weights. So b keeps aging *inactive* instead: every lane's oldest job stays under the 60 s threshold, and the run records the lane ages that show it.
2. **c ages the lanes by backlog depth (Little's law), not with a clock.** Its expected vector is a replay of the selector, not a hand-written CAP vector.
3. **d runs the queue-weight setup with 4 slots, one per reference-worker vCPU,** not 16. A CPU-bound handler at 16 slots would measure host oversubscription rather than the engine. Use `--cpu-concurrency 16` for the P-01 slot count.
4. One repetition per variant (the plan's three repetitions apply to ordinary 15 min runs).

## 3. P-05 contention subset (SP-012)

There are two scenarios. Each runs with `--replicas 1,3,6` worker processes, fixed 16 each, a 100 ms asynchronous handler, and a feeder holding `--backlog` (500) claimable jobs:

- **unconstrained**: plain work on queue `pu`, with no ceilings;
- **constrained**: queue `pc` with these ceilings and capability sets:
  - queue ceiling 24;
  - job types A and B at 8 each;
  - job type C at 2 per resource key, with `--hot-fraction` 0.5 of C jobs on the key `hot` and the rest spread over 64 keys;
  - heterogeneous capability sets: with one replica, the worker registers A, B and C; with more, replica *i* registers {A,B}, {B,C} or {C,A}.

Per run it reports:

- confirmed finishes/s;
- mean Reserve, Begin and Finish latencies, the store-time floor, and the serial-claim and slot-bound ceilings;
- the fractions of Reserves that returned NoWork and CapacityBlocked;
- lock waiters and transactions/s;
- for constrained runs, the occupancy of the queue, job-type and hot-resource scopes (sampled every 5 s: fraction of samples at capacity, mean held).

The `bottleneck` field is derived from these measurements: a scope at capacity in ≥ 50 % of samples; store-time floor ≥ 1.5× the one-replica floor; ≥ 20 % CapacityBlocked or NoWork Reserves; lock waiters. It does not assume linearity. `summary.<scenario>.throughputRatios` gives 3v1 and 6v1. For unconstrained runs, `threshold` compares 3v1 with ≥ 2×.

| Option | Default |
|---|---|
| `--replicas` | `1,3,6` |
| `--scenarios` | `unconstrained,constrained` |
| `--warmup-min` / `--measure-min` | 5 / 15 per run |
| `--concurrency` / `--handler-ms` | 16 / 100 |
| `--backlog` | 500 |
| `--queue-ceiling` / `--type-ceiling` / `--resource-ceiling` / `--hot-fraction` | 24 / 8 / 2 / 0.5 |

Deviations: one repetition per configuration (six configurations). Load is a fixed claimable backlog rather than an open-loop rate, so throughput is capacity, not offered load.

## 4. P-06 rate half (SP-013)

Each run uses one worker (fixed 16, no-op handler), an open-loop Poisson producer, and `rate_admission` activated before the children start. The rate limit is `--rate-limit` per `--rate-window-ms`, default **100 per 1 s**, on every scope.

| Run | Job | Scopes | Offered |
|---|---|---|---|
| `scopes1` | `g2.rated1` | job type | `--rate` 100/s |
| `scopes8` | `g2.rated8` | job type + 7 resource scopes | `--rate` 100/s |
| `churn` | `g2.rated1` | job type, limit `--churn-limit` | `--churn-rate` 200/s |

`--churn-limit auto` (the default) uses half the authorizations/s that `scopes1` achieved, clamped to 1…100. This guarantees that offered Begins exceed the limit on any host, including one that cannot reach 100 Begins/s.

Recorded per run:

- authorizations/s and rejected Begins/s (`admission_deferred` closures), and rejected per authorized;
- **rate decision latency**: Begin latency including the rate step, over all Begins and split by reply (`begin@Applied`, `begin@RateLimited`, from `TimingStore` in G-2 workers);
- **grant rows per authorized job** (expected 1 or 8);
- **WAL bytes per grant** and per authorized job (`pg_current_wal_lsn` diff over the window).

Correctness addition: for every scope over the whole run, the maximum number of grants in any `(t − W, t]` must not exceed the limit (10 §1). Each scope above its limit is a violation.

| Option | Default |
|---|---|
| `--variants` | `scopes1,scopes8,churn` |
| `--rate-limit` / `--rate-window-ms` | 100 / 1000 |
| `--rate` / `--churn-rate` / `--churn-limit` | 100 / 200 / auto |
| `--warmup-min` / `--measure-min` | 2 / 15 per run |
| `--drain-min` | 60 |

Deviations:

1. The auto churn limit (above).
2. Rate scopes are all on one worker process. The "last grant across replicas" case is a correctness case owed by SP-013, not this measurement.

## 5. P-06 scheduler half (SP-016)

There is one run with `scheduler` activated and `--workers` (2) worker processes (fixed 16, handler `--handler-ms` 0) running the `g2.tick` occurrences. The run has two phases.

**Phase 1, steady state:**

1. A scheduler process declares `--schedules` (1,000) schedules `s0000…s0999`, cron `* * * * *`, misfire run_once.
2. Its `ready` comes after the registration seed (1,000 RegisterSchedule calls).
3. Warm-up, then the measured window.
4. After the window, the run waits `--lag-tail-s` (60) so late occurrences can still materialize, then stops the scheduler.

Materialization lag is `materialized_at − scheduled_at` (both store time; materialized is the transaction that creates the job, i.e. dispatched). It is computed over every minute-schedule occurrence scheduled in the window. Expected occurrences = schedules × minute boundaries in the window. If more than 1 % were not materialized by the stop, p99 is reported as a censored lower bound and the threshold is a miss. Threshold: p99 ≤ 3 s.

**Phase 2, catch-up:**

1. A short-lived scheduler registers `catchup` (six-field `* * * * * *`, misfire catch_up, grace `--catchup-grace-s` 30, episode limit `--catchup-limit` 10,000), so its cursor is current. That scheduler then stops.
2. Nothing materializes for `--catchup-occurrences` (1,000) seconds, so about 1,000 occurrences of `catchup` become due. The minute schedules also misfire.
3. The scheduler restarts with all schedules. The run waits until the catch-up episode closes (`episode_id` null and `evaluated_through` ≥ restart) or the schedule needs review.

Recorded for phase 2:

- downtime, occurrences emitted, restart → ready (re-seeding), ready → first materialized, first → last materialized, restart → last;
- the maximum occurrences per MaterializeOccurrences command. More than 32 is a violation;
- the maximum lag and the review state;
- for the minute schedules, the number of run_once misfire ranges.

Correctness additions:

- one occurrence row per (schedule, instant);
- one job per created occurrence;
- created occurrences = jobs created with reason `scheduled`;
- no occurrence whose job is missing (a lost job).

Worker lease losses must be 0; this is the common check.

| Option | Default |
|---|---|
| `--schedules` | 1000 |
| `--workers` / `--concurrency` / `--handler-ms` | 2 / 16 / 0 |
| `--warmup-min` / `--measure-min` / `--lag-tail-s` | 3 / 15 / 60 |
| `--catchup-occurrences` | 1000 (0 skips phase 2) |
| `--catchup-grace-s` / `--catchup-limit` | 30 / 10000 |
| `--catchup-timeout-min` / `--seed-timeout-min` | 60 / 60 |

Deviations:

1. **The 1,000-occurrence episode is one per-second schedule stopped for 1,000 s,** not a minute schedule stopped for 1,000 minutes. An episode is per schedule (13 §5.3) and its default limit is 1,000, which this matches. The episode limit is raised to 10,000 so that the occurrences accrued during the restart's re-seeding do not end the episode in `catch_up_limit` review.
2. The workers do not need to keep up with 16.7 jobs/s. Lag measures materialization, not execution, and the run does not drain the worker backlog (`Drain = false`).

## 6. P-09 outbox (SP-017)

The source is a PostgreSQL database (`<db>_src`, `PostgresOutboxSchema.InstallAsync` plus a `business_rows` table) on the same server as the destination store but **a separate database**.

**Writers (`stage` process):**

- open-loop Poisson arrivals at `--rate` (100/s), served by `--writers` (8) concurrent business transactions;
- each transaction inserts a business row, stages a 1 KiB `g2.out` with `IPostgresOutbox.StageAsync`, commits, then logs `messageId, commit-returned time` (source-visible-ready).

**Relay (`relay` process):**

- one relay group with `--slots` (4) `MaxInFlight` and default budgets;
- it logs `messageId, confirmed-mark time, jobId` after each successful delivered mark.

**Phases:**

1. **Measure.** Warm-up, then the measured window. Recorded: confirmed deliveries/s (threshold ≥ 100), and p50/p95/p99/max from commit returned to delivered mark (threshold p99 ≤ 2 s) for messages committed in the window. Messages unconfirmed at the end count against p99.
2. **Lost marks.** The relay loses `--mark-loss` (5 %) of delivered marks for `--loss-min` (3). Each lost mark is lost after the destination applied the delivery (`ReceiveOutbox` committed and replied Applied), so the queue accepted before the mark was lost. The lease then expires (60 s) and another claim resends the same delivery identity.
3. **Drain.** Writers stop, and the relay runs until no source row is pending or leased (`--drain-min` 60). A source that has not drained by then is a run failure (backlog), not a correctness violation.

**Correctness additions across both databases:**

- every committed message exists in the source; when drained, every one ends delivered, and a blocked row is a violation;
- exactly one `outbox_deliveries` mapping per source message, and one destination job per mapping;
- every source `delivered_job_id` equals its mapping's job;
- every lost-mark message has one mapping, and its later confirmation names the same job.

**Also recorded:**

- source sessions by application name (max);
- destination connections (store sampler);
- relay counters (delivered, retries, lease losses, sends, marks lost);
- writer stage+commit latency.

| Option | Default |
|---|---|
| `--rate` / `--writers` / `--payload` | 100 / 8 / 1024 |
| `--slots` | 4 |
| `--warmup-min` / `--measure-min` | 1 / 15 |
| `--mark-loss` / `--loss-min` | 0.05 / 3 |
| `--drain-min` | 60 |

**7-day cutoff: not run.** There is no cheap time seam:

- `staged_at` and `AcceptUntil` come from the source database clock (`clock_timestamp()`);
- the destination checks the horizon against its own database time;
- the relay's `TimeProvider` governs neither.

Simulating 7 days would need a faketime PostgreSQL for both source and destination (07 I-9) plus a clock step. The cutoff behaviour is covered as a correctness case by OUTBOX-04 with a short horizon. The result says so in `extra.sevenDayCutoff`.

Deviations:

1. 14 §9.4 says `MaxInFlight = 16`, 8 writers and 20,000 messages; the plan and 15 §11 say four relay slots at 100/s. The default follows the plan (4 slots, rate-based, 15 min). `--slots 16` gives the 14 §9.4 profile.
2. No workers execute the delivered jobs. The measurement ends at the delivered mark, and the destination backlog is not drained.
3. Source and destination share one PostgreSQL server (separate databases).

## 7. Smoke results

Setup:

- Release build, Testcontainers `postgres:18` (`RUNWICK_TEST_PG` unset), runs one after another.
- The host was shared with other sessions, load average 15–33 throughout, so the numbers say nothing about D-14.
- Raw artifacts are in the session scratchpad and are not committed.

Every smoke run had **0 correctness violations**.

| Workload | Overrides | Result |
|---|---|---|
| P-04 a | `--admissions 200 --warmup-min 0.3` | 50 / 150 = 25.0 / 75.0 % (deviation 0 pp). Worker CPU 70 ms/job (no-op handler). Drained |
| P-04 b | `--admissions 200` | 60 / 30 / 10 % exactly. Lower-lane oldest age max 42 s, so `agingPossible = false` |
| P-04 c | `--admissions 200 --aged-wait-min 5` | Lanes aged (medium/low oldest age 66–140 s). Observed 55.5 / 33.5 / 11.0 % against the replayed vector 55.6 / 33.3 / 11.1 % |
| P-04 d | `--admissions 200` | 25 / 75 %. 173 ms CPU/job with the 100 ms spin, so about 73 ms engine and store client per job |
| P-05 | `--warmup-min 0.5 --measure-min 1 --backlog 60`, 1/3/6 replicas, both scenarios | See below |
| P-06r | `--warmup-min 0.3 --measure-min 1` | See below |
| P-06s | `--schedules 50 --warmup-min 1 --measure-min 2 --lag-tail-s 20 --catchup-occurrences 30 --catchup-grace-s 10` | See below |
| P-09 | `--rate 10 --warmup-min 0.3 --measure-min 1 --loss-min 1 --mark-loss 0.1 --drain-min 5` | See below |

**P-05.** Unconstrained finishes/s were 5.56 / 6.76 / 6.44 at 1 / 3 / 6 replicas, so 3v1 = 1.22 (threshold miss). The bottleneck reported was store-call latency: the Reserve+Begin+Finish floor went 629 → 1026 → 2067 ms. Constrained finishes/s were 3.27 / 3.96 / 2.59. Store-call latency again dominated (735 → 1606 → 2826 ms), with lock waiters at a mean of 3.2 and 6.1. No ceiling was at capacity at this throughput: the hot key was at capacity in 8 % of samples at 6 replicas. The first pass lost the 3-replica unconstrained run to the `Metrics` race (§8); the rerun after the fix completed (1: 2.85/s, 3: 5.92/s).

**P-06r:**

- **scopes1.** 4.03 authorizations/s at 100/s offered (the producer is locally bounded, as in G-1). 1.00 grant rows per job, 17.1 KB WAL per grant, Begin p50 226 ms and p99 1332 ms.
- **scopes8.** 8.00 grant rows per job, 2.9 KB WAL per grant (23.0 KB per job), Begin p99 1078 ms.
- **churn.** The auto limit came out at 2/s. 1.45 authorizations/s, 1.92 rejected Begins/s (1.32 per authorization), Begin p50 Applied 282 ms vs RateLimited 142 ms. The maximum in any 1 s window was 2 against a limit of 2, and no scope exceeded its limit.

**P-06s, steady state.** 100 of 100 expected occurrences were materialized. Lag p50 10.0 s, p99 22.5 s (miss against 3 s): the scheduler materializes one schedule per command, about 0.2 s each on this host. Identities were distinct: 262 created occurrences, 262 scheduled jobs, no duplicates. 0 lease losses.

**P-06s, catch-up.** About 35 occurrences were emitted after 30 s of downtime, in batches of at most 32. The restarted scheduler took 6.9 s to become ready and 14.2 s more to the first catch-up materialization. The episode closed 22.6 s after restart without review.

**P-09.** Two passes ran. Both had one mapping and one job per message.

- **First pass:** 1,387 messages committed, 9.6 commits/s and 10.0 confirmed deliveries/s, ready→confirm p50 3.2 s and p99 7.1 s.
- **Second pass, after the correctness rework:** 1,375 messages committed, relay throughput 4.6/s under host load, so p99 was 146 s.
- **Lost marks:** 30 and 15 lost marks were all redelivered to the same mapping and job, with 0 second mappings.
- **Sessions:** source sessions max 2 relay and 8 writer.

## 8. Harness fixes found by the smoke runs

No product (src/) defect was found, and no src/ file was changed. Two harness races shared with G-1 were fixed:

1. **`Metrics` enumeration race.** `Metrics.CountersJson`, `MaximaJson` and `LatenciesJson` enumerated a `ConcurrentDictionary` through LINQ. When a key was added during that enumeration, `ICollection.CopyTo` threw. In the P-05 smoke this killed a worker's sampler, and the worker exited with 134. The methods now snapshot with `ConcurrentDictionary.ToArray()` first. The race was latent in G-1; the new per-status latency keys made it frequent.
2. **`DbSampler` disposal race.** Disposing `DbSampler` while a sample query was in flight threw `OperationCanceledException` out of the loop, which aborted the P-06s smoke after its measurement. The loop now ends quietly when the cancellation is its own.

The P-09 smoke also led to a correctness rework. Messages still pending or leased when the drain times out are now **backlog**: the run records a failure (as G-1 does for an undrained run), not a correctness violation. Lost, blocked, unmapped, doubly mapped and mismatched messages remain violations.

Things to read correctly in the results:

- The relay summary counter `leaseLosses` counts delivered marks that found another owner after a lost mark. That is expected under injected mark loss, and it is not a worker lease loss.
- The G-1 one-line summary shows "finishes/s 0" for P-09, because no workers run there.

## 9. Expected full-length durations

Estimates are for this host at the smoke runs' rates: about 3–6 claims/s per worker, and a relay at 5–15 deliveries/s. On the reference environment the count-bound parts (P-04) finish much faster; the time-bound parts do not change.

| Workload | Default shape | Estimate on this host |
|---|---|---|
| P-04 | 4 variants × (2 min warm-up + 10,000 admissions at ~3/s + drain), plus up to 15 min aging wait for c | **≈ 4–4.5 h** (bounded by 300 min per variant) |
| P-05 | 6 runs × (5 + 15 min + ~3 min drain and start) | **≈ 2.3 h** |
| P-06r | 3 runs × (2 + 15 min + drain). Churn drains its backlog at the 2/s auto limit | **≈ 1.5–2 h** |
| P-06s | seed 1,000 schedules (~5 min) + 3 + 15 + 1 min + 1,000 s downtime + re-seed (~5 min) + catch-up | **≈ 1 h** |
| P-09 | 1 + 15 + 3 min, then drain up to 60 min. At 100/s offered against a 5–15/s relay, the source backlog will not drain in 60 min, so the run records a failure (backlog) while the identity checks still run | **≈ 1.3 h** |

Total ≈ 10–11 h sequentially.
