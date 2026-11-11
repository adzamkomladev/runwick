# 06 — Defaults, derived values and startup validation

**Sources:** [execution-defaults-and-timing.md](../architecture/execution-defaults-and-timing.md) (EDT), [capacity-defaults-and-budgets.md](../architecture/capacity-defaults-and-budgets.md) (CDB), [management-retention-defaults.md](../architecture/management-retention-defaults.md) (MRD), [worker-engine-internals.md](../architecture/worker-engine-internals.md) (WEI), [identity-and-serialization.md](../architecture/identity-and-serialization.md) (IDS); also [capacity-control.md](../architecture/capacity-control.md) (CC), [storage-command-protocol.md](../architecture/storage-command-protocol.md) (SCP), [public-api-contract.md](../architecture/public-api-contract.md) (API) and [review-2026-10-30.md](../review-2026-10-30.md) (REV).

Every numeric default used by milestone M2 is in this file. Behavior that uses each value is specified in [05-worker-engine.md](05-worker-engine.md) (cited as `05 §n`) and the storage/protocol specs. A value listed here is the default; a host may change configurable values only within the startup inequalities of §3. Values marked *profile constant* are not host-configurable (raising them requires a new serializer profile/format version).

## Changes (2026-11-09, attempt usage)

`WorkerOptions.RecordResourceUsage` = **true**. Per attempt the worker reads process CPU time and allocated bytes at both ends of the attempt (measured cost about 2 × 6 µs and 2 × 13 ns), and one shared timer reads the working set every 250 ms (about 0.1 ms per read) while any attempt runs. End to end, three alternating on/off drain runs (20 000 jobs, 16 workers, PostgreSQL) gave a median of 103.5 jobs/s on and 100.2 off: no difference above the run-to-run spread of about 15%. Details in `docs/reports/attempt-usage.md`. With it off nothing is read and attempts store no usage (16 §2.2).

## Changes (2026-11-08, SP-025 spec)

The Redis store's own numbers (intent limits 256 keys / 1,024 after-images / 4 MiB, 100 after-images per call, 5,000-entry management page bound, profile recheck 30 s, migration lock 60 s, renewal-wait test bound p99 ≤ 250 ms) are in 22 §10. The call-class ceilings of §1.7 apply to it unchanged, as logical in-flight limits (22 §5.1).

## Changes (2026-11-08, SP-019 spec)

The SQLite store's own numbers (writer busy timeout 2 s, reader threads 2, one writer thread, WAL size limit 64 MiB, migration lock wait 60 s) are in 20 §10. The call-class ceilings of §1.7 apply to it unchanged.

## Changes (2026-11-07, wider producer lane)

More producer calls may reach the database at once, and an application that raises `ProducerOptions.MaxConcurrentCalls` now gets that many in the PostgreSQL store too (before, the store's producer lane stayed at 2 whatever the option said). Measured on the 20,000-job drain bench (4 callers): insert 9.6 s with lane 2, 6.6–7.4 s with a wider lane (trace-adding report, 2026-11-07). The default was first set to 4 and raised to 8 the same day at the owner's request.

- §1.7 producer/admin mutation class: 2 → **8** concurrent calls by default, and it **follows `MaxConcurrentCalls`** (the local admission gate and the PostgreSQL producer lane are the same number). `MaxConcurrentCalls` must be at least 1 (startup error otherwise).
- §1.7 work data source: maximum 8 → **admission 4 + producer/admin + background 2** = **14** by default, so V-16 holds by construction for any `MaxConcurrentCalls`.
- F-13 connection ceiling per worker group: 17 → **23** by default (`8 + 14 + 1`); a group that raises `MaxConcurrentCalls` adds the same number of work connections.
- Not changed: admission class 4, background class 2, control data source 8, local admission waiters 64 and wait 1 s; producer-only processes still create no control data source.

## Changes (2026-11-07, wider settlement lane)

The settlement lane is widened so more Finish calls run at once (with the set-based finish, 04 Changes 2026-11-07). Measured on the 20,000-job drain bench (busy host, 4 rounds each against the same build with lane 2): 325 → 364 completions/s on average at 64 slots (+12 %, 3 of 4 rounds faster), 148 → 157 at 16 slots (2 of 4 rounds faster, within noise).

- §1.7 settlement lane: 2 → **4** concurrent calls. The completion workers (05 §12) follow it.
- §1.7 control data source: maximum 6 → **8** connections, so V-16 holds (`4 + 4 ≤ 8`); the warm minimum stays 2.
- F-13 connection ceiling per worker group: 15 → **17**.
- Not changed: `FinishMany/1` stays 1–32 items. A 64-item batch was measured and gave no clear gain (at 16 slots no more than 16 Finishes can wait anyway).

## Changes (2026-11-02, idle caps)

Idle polling backs off further while a wake listener is healthy (05/13/14 Changes 2026-11-02, idle caps). Every value below applies only from a successful (re)subscription until the session is lost or refused. Otherwise the earlier value stays in force.

- §1.5 idle probe backoff cap: 2,000 → **10,000 ms** while the worker's listener is healthy; F-10 and V-22 read the cap that applies at each wait. The blocked lane/queue cooldown cap stays 2,000 ms.
- §1.12 relay idle poll: 1 s → **5 s** (5 × `IdlePoll`) while the relay's listener is healthy. A full or renewal-paused relay keeps `IdlePoll`.
- §1.14 scheduler idle back-off: 5 s → **15 s** (3 × `IdlePollInterval`) while the scheduler's listener is healthy. An idle wait never runs past the next due instant the scheduler knows of.
- No option is added. The worker cap is a constant, as before. The relay and scheduler values scale with their existing `IdlePoll` and `IdlePollInterval` options.

## Changes (2026-10-31, SP-016)

- §1.14 adds the scheduler defaults and bounds (13 §9–§10).

## Changes (2026-10-31, SP-014)

- §1.13 adds the scaler constants (11 §3). V-15 now accepts `AutoConcurrency(min, max)` with `0 ≤ min ≤ max`, `max ≥ 1`, and refuses a pool that sets both modes. The group sum uses each pool's maximum. The "AutoConcurrency rejected" row and *[choice]* are superseded.

## Changes (2026-10-31, SP-012)

- §1.5: probe spacing 50 ms is now enforced per polling group; first presence publication attempts 3; diagnostic sample 256 per lane (1 – 1,000) (09 §3–§5).

## Changes (2026-10-31, SP-017)

- §1.12 adds the outbox relay budgets and horizon bounds (14 §4.2, §6).

## Changes (2026-10-31, SP-013)

- §1.11 adds the rate policy bounds and deferral timing (10 §2, §4). V-24 now reads "the binary knows every capability the application requires".

## Changes (2026-10-31, SP-015)

- §1.10 adds the schedule evaluator bounds (12 §2.4, §8, §9). They are constants of the evaluator profile `runwick.cron.v1`, not options, so §3 gains no startup validation for them.

## Changes (2026-10-31, SP-011)

- Administrative commands: default NotAfter 5 min (`Operations.AdministrativeLifetime`), cap 10 min, receipts NotAfter + 24 h (§1.4 unchanged values, now in use).
- Actor bounds: subject 1–256 bytes, display ≤ 256, scheme ≤ 64 ASCII, reason ≤ 512 (08 §2). Scope capacity edits 0–10,000. Event page size 1–1,000, default 100.

## Changes (2026-10-30)

Contradictions resolved:

| # | Conflict | Resolution |
|---|---|---|
| C-1 | EDT receipt-floor list and `OwnershipCommandLifetime` list omit ReleaseReservation; MRD includes it | ReleaseReservation is an ownership command: lifetime 30 s, receipt kept until `NotAfter + 10 min` |
| C-2 | Aging: CC "exceeds 60 s" vs CDB "at least 60 s" | `age >= 60 s` |
| C-3 | CDB first draft: pressure margins fixed at 25 s / 30 s | Derived `StopMargin`/`ResumeMargin` (review correction); defaults reproduce 25 s / 30 s |
| C-4 | EDT first listed a Renew receipt floor | R1: Renew writes no receipt |
| C-5 | Capability-set bound required but never given | 512 tuples per probe/Reserve (*[choice]*) |
| C-6 | Historical uniqueness-and-execution.md retry defaults 10 s / 5 min | Superseded by EDT: 5 s / 60 s |

Implementation choices (marked *[choice]* below): whole-millisecond durations (V-01); `OwnershipCommandLifetime > ControlRequestTimeout` (V-09); each resolution budget ≥ `ControlRequestTimeout` (V-12); presence heartbeat upper bound < expiry (V-19); background heavy ≤ background − 1 (V-16); `ReservesPerRound ≥ BlockedCandidatesPerLaneVisit` (V-21); warning, not failure, when `ShutdownBudget ≥ HostOptions.ShutdownTimeout` (V-11); `AutoConcurrency` rejected in M2 (V-15); `LeaseWatchdogInterval` default exactly 1 s; recovery batch 32; liveness stall 10 × watchdog; ±20 % jitter applied multiplicatively after the cap; `storage_commands` retention classes `ownership` / `durable` derived from operation name.

## 1. Default table

### 1.1 Generation policy — persisted at dispatch (snapshotted into `job_generations`)

| Name | Value | Unit | Where used | Source | Validation |
|---|---|---|---|---|---|
| MaxExecutions | 3 | executions | Begin accounting; Finish/Recover retry decision. Includes the first committed Begin (≤ 2 automatic retries) | EDT §2, SCP §5 | V-14 |
| ExecutionTimeout | none | ms | Pipeline timeout (05 §10.4); when configured, positive | EDT §2 | V-14 |
| Retry evaluator | `exponential-v1`, evaluator version 1, jitter kind `equal` | — | Finish/Recover retry delay (formula F-1) | EDT §2, §5 | V-14 |
| Retry InitialDelay | 5,000 | ms | F-1 | EDT §2 | V-14 |
| Retry MaximumDelay | 60,000 | ms | F-1; cap for requested retry delay (F-2) | EDT §2 | V-14 |
| MaxConsecutiveUnstartedAbandonments | 5 | abandonments | Recover of a reserved lease (F-3) | EDT §2 | V-14 |
| Unstarted retry evaluator | `exponential-v1`, version 1, `equal` | — | F-3 | EDT §2 | V-14 |
| Unstarted InitialDelay | 1,000 | ms | F-3 | EDT §2 | V-14 |
| Unstarted MaximumDelay | 30,000 | ms | F-3 | EDT §2 | V-14 |
| Duration quantum | 1 | ms | All persisted durations; requested delay rounding | EDT §1 | V-01 |

### 1.2 Worker lease and control timing

| Name | Value | Unit | Where used | Source | Validation |
|---|---|---|---|---|---|
| LeaseDuration | 60,000 | ms | Reserve/Renew `LeaseDuration` (05 §9.1, §11) | EDT §2 | V-01, V-02, V-04, V-23 |
| LeaseSafetyMargin | 10,000 | ms | `d = s + r − safety` (F-5) | EDT §2 | V-02, V-04 |
| RenewalInterval (nominal) | 15,000 | ms | Renewal scheduling (F-6) | EDT §2 | V-03 |
| RenewalIntervalMin / Max | 12,000 / 18,000 | ms | Jitter range J (F-6); Max feeds V-04 and StopMargin | EDT §2 | V-03, V-04 |
| LeaseWatchdogInterval | 1,000 (*[choice]* default; must be ≤ 1,000) | ms | Safety watchdog (05 §11.4) | EDT §2 | V-07 |
| SchedulerAllowance | 2,000 | ms | V-04, V-05, StopMargin; timer-lag stop gate | EDT §2, CDB §5 | V-04, V-05 |
| ControlRequestTimeout | 5,000 | ms | Every ownership request's end-to-end local wait incl. I/O admission | EDT §2 | V-04, V-05, V-09, V-12 |
| ControlRetryBackoff initial / max | 100 / 1,000 | ms | Storage retry/resolution backoff (F-7), equal jitter | EDT §2 | V-08 |
| MinimumInvocationMargin | 15,000 | ms | Begin pre-check and invocation admission (05 §9.3, §9.5) | EDT §2 | V-05 |
| ClaimResolutionBudget | 15,000 | ms | From first Reserve submission; expiry suspends pool admission | EDT §2 | V-12 |
| BeginResolutionBudget | 15,000 | ms | From first Begin submission; expiry forbids invocation | EDT §2 | V-12 |
| CompletionResolutionBudget | 30,000 | ms | From publication of frozen Finish after cleanup | EDT §2 | V-12 |
| CancellationGrace | 10,000 | ms | From first User/Timeout/Shutdown cancellation request to pipeline exit | EDT §2 | V-12 |
| CleanupBudget | 10,000 | ms | From start of scope disposal | EDT §2 | V-12 |
| OwnershipCommandLifetime | 30,000 | ms | `NotAfter = wallNow + 30 s` for Reserve, Begin, Renew, Finish, Recover, **ReleaseReservation** | EDT §2, MRD (C-1) | V-09 |
| RecoveryScanInterval | 5,000 nominal, uniform [4,000, 6,000] | ms | RecoveryLoop (05 §13) | EDT §2 | V-13 |
| RecoveryBatch | 32 (*[choice]*) | candidates/scan | Expired-lease probe `LIMIT` | — | V-21 |
| ShutdownBudget | 25,000 | ms | Overall Runwick shutdown work | EDT §2 | V-10, V-11 |
| ShutdownDrain | 15,000 | ms | Drain slice → DrainCutoff T0 + 15 s | EDT §2, §7 | V-10 |
| ShutdownCancellation | 5,000 | ms | Cancellation slice → CancellationCutoff T0 + 20 s | EDT §2, §7 | V-10 |
| ShutdownFinalization | 5,000 | ms | Finalization slice → ControlStopCutoff T0 + 25 s | EDT §2, §7 | V-10 |
| Host ShutdownTimeout (reference, not set by Runwick) | 30,000 | ms | .NET 10 `HostOptions` default; compared in V-11 | EDT §7 | V-11 |
| Liveness stall bound | 10 × LeaseWatchdogInterval = 10,000 (*[choice]*) | ms | Liveness health (05 §17) | — | — |

### 1.3 Derived worker values

| Name | Value (defaults) | Unit | Formula | Where used | Source | Validation |
|---|---|---|---|---|---|---|
| usableAuthority | 50,000 | ms | `LeaseDuration − LeaseSafetyMargin` | V-04, StopMargin | CDB §5 | V-02 |
| StopMargin | 25,000 | ms | `usableAuthority − RenewalIntervalMax − ControlRequestTimeout − SchedulerAllowance` = 50 − 18 − 5 − 2 | Pressure stop gate (05 §15) | CDB §5, REV §2 | V-06 |
| ResumeMargin | 30,000 | ms | `StopMargin + ControlRequestTimeout` = 25 + 5 | Pressure resume gate | CDB §5, REV §2 | — |
| Renewal cap offset | 13,000 | ms | `2 × ControlRequestTimeout + ControlRetryBackoffMax + SchedulerAllowance` | F-6 `due` cap; V-05 | EDT §3 | V-05 |
| Renewal check sum | 31,000 | ms | `RenewalIntervalMax + 2 × ControlRequestTimeout + ControlRetryBackoffMax + SchedulerAllowance` = 18 + 10 + 1 + 2 (< 50) | V-04 | EDT §3 | V-04 |
| DrainCutoff / CancellationCutoff / ControlStopCutoff | T0 + 15,000 / T0 + 20,000 / T0 + 25,000 | ms | F-8 | Staged shutdown (05 §16) | EDT §7 | V-10 |
| Healthy lowest margin before due renewal | ≈ 31,000 | ms | `usableAuthority − RenewalIntervalMax − RTT` (RTT ≈ 0–1 s) | CAP-10 flap check | CDB §5 | — |

### 1.4 Receipts and command lifetimes

| Name | Value | Unit | Where used | Source | Validation |
|---|---|---|---|---|---|
| Ownership receipt retention (class `ownership`) | `NotAfter + 10 min` | — | Reserve, Begin, Recover, **ReleaseReservation** | EDT §2, MRD, REV R2 | — |
| Durable receipt retention (class `durable`) | `NotAfter + 24 h` | — | Finish; Dispatch/prepared dispatch; (admin, scheduler: not in M2) | EDT §2, MRD | — |
| Renew receipt | none | — | Renew keeps a command ID for correlation only | SCP §4, REV R1 | — |
| Dispatch / admin command lifetime | 5 min default, 5 min maximum | min | `NotAfter` of ordinary prepared dispatch | MRD | NotAfter − now ≤ 5 min |

`storage_commands` is `PARTITION BY LIST (retention_class)` with partitions `ownership` and `durable`, PK `(application, retention_class, command_id)`; the class is derived from the operation name; `retain_until` = the class formula F-9. A pending publication/recovery record is never pruned.

### 1.5 Local capacity, fairness and probing

| Name | Value | Unit | Where used | Source | Validation |
|---|---|---|---|---|---|
| Default pool mode | fixed, 4 | tickets | Pool with no mode configured; `FixedConcurrency(n)` sets target = maximum = n | CDB §1, REV §2 | V-15 |
| AutoConcurrency | `(min, max)`, initial target min (1 when min = 0) | tickets | Automatic pool (11) | CDB §1, §4 | V-15 |
| Worker/store group maximum | 16 | outstanding attempts | Σ pool maxima bound | CDB §1 | V-15 |
| Per-pool claim concurrency | 1 | Reserve in flight (incl. resolution) | Pool coordinator | CDB §1 | — |
| Prefetch beyond tickets | 0 | reservations | Pool coordinator | CDB §1 | — |
| Queue weight | 1 default; allowed 1–1,000 | — | Queue-level SWRR | CDB §2 | V-15 |
| Lane weights high : medium : low | 6 : 3 : 1 | — | Lane-level SWRR | CDB §2, CC §2 | V-20 |
| Priority mode | weighted with aging | — | Lane selection | CDB §2 | — |
| AgingEvery | 8 | successful queue admissions (counter saturates at 8) | Aging | CDB §2, CC §2 | V-20 |
| AgingThreshold | 60,000 (`age >= threshold`) | ms | Aging | CDB §2 (C-2) | V-20 |
| BlockedCandidatesPerLaneVisit | 8 | candidates | Lane visit scan budget | WEI §5, CDB §2 | V-21 |
| ReservesPerRound | 32 | Reserve calls incl. successes | Pool scheduling round, then yield | WEI §5, CDB §2 | V-21 |
| Readiness maximum hint age | 2,000 | ms | Snapshot refresh | CDB §2 | V-22 |
| Idle probe backoff | 100 → 2,000 (→ 10,000 while the wake listener is healthy), doubling, jitter ±20 % | ms | Readiness polling (F-10) | CDB §2 | V-22 |
| Blocked lane/queue cooldown | 250 → 2,000, doubling, jitter ±20 % | ms | Lane/queue cooldown, QueuePaused backoff (F-10) | CDB §2 | V-22 |
| Notification probe spacing | ≥ 50 | ms per polling group; 1 probe in flight | Readiness | CDB §2 | V-22 |
| Capability-set bound | 512 (*[choice]*) | deduplicated decoding tuples per probe / Reserve | Readiness, Reserve, presence | C-5 | V-17 |
| Completion ready-ID channel capacity | = group maximum (16) | IDs | Completion coordinator, `FullMode.Wait` | CDB §6 | — |

### 1.6 Pressure gates

| Name | Value | Unit | Where used | Source |
|---|---|---|---|---|
| Control item wait → stop refill | 1,000 | ms | Group pressure stop | CDB §5 |
| Margin → stop refill | `< StopMargin` (25,000) | ms | Group pressure stop | CDB §5 |
| Oldest pending Finish → stop refill | 5,000 | ms | Group pressure stop | CDB §5 |
| Timer-lag sample period | 1,000 | ms | Coordinator timer lag | CDB §5 |
| Timer lag → stop refill | > SchedulerAllowance (2,000) | ms | Group pressure stop | CDB §5 |
| Resume: consecutive healthy checks | 3 × 1,000 ms checks | — | Group resume | CDB §5 |
| Resume: max control wait | 250 | ms | Group resume | CDB §5 |
| Resume: min confirmed margin | `≥ ResumeMargin` (30,000) | ms | Group resume | CDB §5 |
| Resume: oldest pending Finish | ≤ 1,000 | ms | Group resume | CDB §5 |
| Growth freezes (p95 > 250 ms over 30 s with ≥ 20 samples; ≥ 75 % blocked of ≥ 20 cycles in 30 s; lag > 250 ms ×3) | — | — | Automatic controller only: not in M2 | CDB §5 |

### 1.7 PostgreSQL connections and I/O classes (per worker/store group)

| Name | Value | Unit | Where used | Source | Validation |
|---|---|---|---|---|---|
| Control data source | 8 max (6 before 2026-11-07), 2 min | connections | Ownership operations only (`Maximum Pool Size=8;Minimum Pool Size=2`) | CDB §6 | V-16 |
| Renewal lane | 4 | concurrent calls | Renew (earliest safety deadline first) | CDB §6 | V-16 |
| Settlement lane | 4 (2 before 2026-11-07) | concurrent calls | Finish, Finish resolution, ReleaseReservation | CDB §6 | V-16 |
| Work data source | admission + producer/admin + background = 14 max by default (8 before 2026-11-07), 0 min | connections | All non-control operations | CDB §6 | V-16 |
| Admission class | 4 | concurrent calls | Reserve, Begin, their resolution | CDB §1, §6 | V-16 |
| Producer/admin mutation class | 8 (2 before 2026-11-07); follows `ProducerOptions.MaxConcurrentCalls` (≥ 1) | concurrent calls | Dispatch | CDB §6 | V-16 |
| Background class | 2 | concurrent calls | Presence, probes, recovery, cleanup | CDB §6 | V-16 |
| Background heavy work | ≤ 1 of the 2 | concurrent calls | Cleanup/history | CDB §6 | V-16 |
| Optional listener | 1 | dedicated session | LISTEN | CDB §6 | — |
| Migration runner | 1 | dedicated session per runner | Deployment migration, outside worker budget | CDB §6 | — |
| Local admission waiters | 64 | waiting operations per group | Producer/admin admission; excess → `local_capacity_exceeded` (not submitted) | CDB §6, API | V-18 |
| Local admission wait cap | 1,000 | ms (or caller's earlier cancellation) | Same | CDB §6 | V-18 |

### 1.8 Presence

| Name | Value | Unit | Where used | Source | Validation |
|---|---|---|---|---|---|
| Presence heartbeat | 5,000 ± 10 % (uniform [4,500, 5,500]) | ms | Presence loop | CDB §4 | V-19 |
| Presence expiry | 20,000 (store time) | ms | Presence record | CDB §4 | V-19 |

### 1.9 Identity, payload and record bounds

| Name | Value | Unit | Where used | Source |
|---|---|---|---|---|
| Application, queue, schedule, pool, serializer-profile name | 1–128 ASCII; `[A-Za-z0-9][A-Za-z0-9._-]*` | characters | Registration, dispatch | IDS §1 |
| Job name | 1–200, same grammar | characters | Registration | IDS §1 |
| Payload / profile version | 1 … 2,147,483,647 | — | Registry, jobs | IDS §1 |
| Uniqueness / resource key | 1–512, valid Unicode, no control chars, case-sensitive | UTF-8 bytes | Dispatch keys, PerResource scopes | IDS §1 |
| Serialized payload | 1 MiB (1,048,576) | UTF-8 bytes | Prepare/dispatch; profile constant | IDS §4 |
| Nested JSON containers | 32 (root = level 1) | levels | Profile constant | IDS §4 |
| Decoded string value | 256 KiB | UTF-8 bytes | Profile constant | IDS §4 |
| Decoded property name | 256 | UTF-8 bytes | Profile constant | IDS §4 |
| Properties per object | 256 | — | Profile constant | IDS §4 |
| Elements per array | 16,384 | — | Profile constant | IDS §4 |
| Numeric token | 128 | ASCII bytes | Profile constant | IDS §4 |
| Business + diagnostic metadata | 16 KiB, 32 entries | encoded bytes / entries | Headers | IDS §4 |
| Metadata key | 128, identifier grammar | ASCII characters | Headers | IDS §4 |
| Exported prepared envelope | 2 MiB | bytes (incl. base64 payload) | Prepared dispatch | IDS §4 |
| Batch dispatch | 100 items, 2 MiB total | items / bytes | `DispatchManyAsync`/`PrepareMany`/`StageManyAsync` | REV §5, API §5 |
| Stored detailed exception | 16 KiB, explicit truncation marker | UTF-8 bytes | `AttemptError` in Finish | MRD |

Not in M2 (listed so no M2 component invents them): rate-policy bounds (1–100,000 per window, window 1 s–24 h, ≤ 8 rate scopes per generation, 250 ms minimum defer + 0–100 ms jitter); automatic controller inputs (5 s samples, 3/6 evidence, 30 s cooldown, 25 % step, 80 % utilization, 30 s horizon); management limits and retention periods (7/30/90 days, cleanup batches); scheduler lifetimes.

### 1.10 Schedule evaluator (SP-015; profile constants)

| Name | Value | Unit | Where used | Source |
|---|---|---|---|---|
| Expression length | 256, printable ASCII; space/tab separate fields | characters | Grammar input check | 12 §2.4 |
| List terms | 64 across all fields | terms | Grammar | 12 §2.4 |
| Zone ID | 64, printable ASCII, exact artifact key | characters | Zone mapping | 12 §6.2 |
| Lookup horizon | 8 calendar years + 2 days after the cursor | — | Planner | 12 §8.1 |
| Supported instant range | 0001-01-02T00:00Z … 9999-12-30T00:00Z | — | Planner | 12 §8.1 |
| Steps per plan | 4,096 | steps | Planner | 12 §8.2 |
| Cooperative budget, materialization | 250 | ms | Planner | 12 §8.2 |
| Cooperative budget, registration / edit / preview | 1,000 | ms | Planner | 12 §8.2 |
| Preview length | 10 | occurrences | Preview | 12 §9 |
| Evaluator slots | 1 per scheduler/store group | — | `EvaluatorSlot` | 12 §8.3 |

### 1.11 Rate admission (SP-013)

| Name | Value | Unit | Where used | Source |
|---|---|---|---|---|
| Rate limit | 1 – 100,000 | grants per window | Registration, Dispatch, EditRatePolicy | 10 §2 |
| Rate window | 1,000 – 86,400,000, whole ms | ms | Same | 10 §2 |
| Rate scopes per job | 8 (at most 1 job-type scope) | scopes | Registration, Dispatch | 10 §2 |
| Minimum deferral | 250 | ms of logical time | Begin T20 | 10 §4 |
| Deferral jitter | 0 – 100, drawn once per decision | ms | Begin T20 | 10 §4 |
| Edit freeze | new window, from the scope's logical time | — | EditRatePolicy | 10 §6.3 |

### 1.12 Outbox relay and horizon (SP-017)

| Name | Value | Unit | Where used | Source |
|---|---|---|---|---|
| Acceptance horizon | 7 d default; (0, 30 d], whole ms | — | Stage | 14 §6.1 |
| Mapping retention | ≥ AcceptUntil + 24 h | — | outbox_deliveries | 14 §6.4 |
| Transport NotAfter | ≤ 30 s and ≤ AcceptUntil | — | ReceiveOutbox/1 | 14 §6.2 |
| MaxInFlight / claim batch | 4 (1 – 64) | messages | Relay | 14 §4.2 |
| Source lease / renewal | 60 s / 15 s (lease ≥ 5 s, ≥ 3 × renewal) | s | Relay | 14 §4.2 |
| Provider call timeout | 5 s (< renewal) | s | Relay | 14 §4.2 |
| Idle poll | 1 s (5 s while the wake listener is healthy) | s | Relay | 14 §4.2 |
| Retry | 1 s → 60 s, exponential, equal jitter | s | Relay | 14 §4.2 |
| Source connections | 2 per relay group (claim lane, control lane) | connections | Relay | 14 §4.2 |

### 1.13 Local autoscaler (SP-014)

| Setting | Default | Unit | Note |
|---|---|---|---|
| Sample interval | 5 s ± 10 % | — | one shared estimate per sample |
| Estimate deadline | 1 s | — | late = unknown |
| Due rows counted per lane | 2,000 | rows | cap sets `Truncated` |
| Occupancy window | 30 s | — | time-weighted mean |
| Drain horizon / target utilization | 30 s / 0.80 | — | `desired = ceil((occ + work/30 s)/0.8)` |
| Band / step | 20 % (≥ 1) / 25 % (≥ 1) | of current | boundary waiver at min and max |
| Evidence | 3 grow, 6 shrink | samples | consecutive |
| Change cooldown | 30 s | — | — |
| Service EWMA | α 0.20, cold 1 s, confident at 20 | — | longest live ticket is a lower bound |
| Blocked-ratio freeze | ≥ 75 % of ≥ 20 cycles in 30 s | — | growth only |
| Override target / lifetime | 0 – 10,000 / ≤ 7 days | tickets / — | per instance and pool |

These are benchmark inputs, not tuned release values.

### 1.14 Scheduler (SP-016)

| Setting | Default | Bound |
|---|---|---|
| Due read | 32 schedules per page with a local scan position. Empty head reads include the earliest eligible future scan time. | — |
| Polling | Full productive pages continue promptly. Base poll 1 s after a short productive page. Unproductive pages and failures back off to 5 s (15 s with a healthy schedule listener), × (1 ± 0.10). Idle waits respect known due instants. | — |
| Materializations in flight | 1 per scheduler/store group | — |
| Local allowance | 100 jobs/s, burst 32 | — |
| Request / resolution budget | 5 s / 30 s | — |
| Plan | 32 jobs, 2 MiB payload | hard |
| Misfire grace | 60 s | 0 – 24 h |
| Catch-up episode limit | 1,000 | 1 – 10,000 |
| Catch-up oldest age | 7 d | 1 min – 31 d |
| Catch-up pacing | 250 ms | ≥ 250 ms |
| `RegisterSchedule` NotAfter | 5 min | 10 min |
| Other schedule operations NotAfter | 30 s | 60 s |

## 2. Formulas

**F-1 Execution retry (exponential-v1, equal jitter).** `n` = committed execution number of the failed attempt within its generation (starting at 1). Retry is possible only if `n < MaxExecutions`.

```text
b(n)  = min(MaximumDelay, InitialDelay × 2^(n−1))     ms, saturating (no overflow)
delay = uniform integer in [ceil(b/2), b]            entropy sampled once, explicit input
eligible_at = storage decision time (Finish/Recover) + delay
```

Defaults: n = 1 → [2,500, 5,000] ms; n = 2 → [5,000, 10,000]; n = 3 terminal (MaxExecutions 3). Cap reached at n = 5 (5 × 16 = 80 s → 60 s). The chosen delay/eligible instant is persisted with the outcome and receipt; replay returns it, never redraws.

**F-2 Requested retry delay** (middleware `Retry(reason, delay)`): `delay < 0` → pipeline contract fault (`PermanentFailure`); else `delay' = min(MaximumDelay, max(1 ms, ceil_ms(delay)))`; no jitter; execution still charged.

**F-3 Unstarted recovery.** `k` = consecutive-unstarted count after increment on a reserved-lease Recover.

```text
if k >= MaxConsecutiveUnstartedAbandonments (5): generation fails, reason reservation_abandonment_exhausted
else delay = uniform integer in [ceil(b/2), b], b = min(30,000, 1,000 × 2^(k−1)) ms
restored state = pending if started_count = 0 else retrying
```

k = 1..4 → [500, 1,000], [1,000, 2,000], [2,000, 4,000], [4,000, 8,000] ms. A successful Begin resets k to 0. ReleaseReservation never increments k or the execution count.

**F-4 Running-crash recovery.** Uses the already-consumed execution number `n` with F-1 from the Recover decision; no extra execution is charged. Durable cancellation wins for unsuccessful recovery.

**F-5 Authority deadline.** For a fresh reply: `s` = monotonic send time, `r = ExpiresAt − DecidedAt`, `d = s + r − LeaseSafetyMargin`; margin at receive `t` is `d − t`. Replays never set `d`. `now >= d` is expiry.

**F-6 Renewal due time.**

```text
J   = uniform [RenewalIntervalMin, RenewalIntervalMax]  (12–18 s), sampled per scheduled renewal
due = min(s + J, d − (2 × ControlRequestTimeout + ControlRetryBackoffMax + SchedulerAllowance))
```

Defaults: `d = s + 50 s`, cap `d − 13 s = s + 37 s`, so `due = s + J ≤ s + 18 s`. A reply arriving after `due` triggers an immediate renewal.

**F-7 Control retry backoff** (attempt i ≥ 1): `b_i = min(1,000, 100 × 2^(i−1))` ms; actual = uniform integer in `[ceil(b_i/2), b_i]`. Command identity is preserved across these retries (except Renew, which has none to preserve).

**F-8 Shutdown cutoffs** (T0 = first observed stop signal; absolute, never restarted):

```text
DrainCutoff        = T0 + ShutdownDrain                                       (15 s)
CancellationCutoff = T0 + ShutdownDrain + ShutdownCancellation                (20 s)
ControlStopCutoff  = T0 + ShutdownDrain + ShutdownCancellation + ShutdownFinalization  (25 s)
effective budget end = min(ControlStopCutoff, host token cancellation)
```

**F-9 Receipt retention:** `retain_until = NotAfter + 10 min` (class `ownership`) or `NotAfter + 24 h` (class `durable`).

**F-10 Jittered doubling backoff** (idle probe base 100 ms, cooldown base 250 ms, cap 2,000 ms; the idle probe cap is 10,000 ms while the wake listener is healthy): consecutive step `i ≥ 0`: `nominal_i = min(cap, base × 2^i)`; `actual_i = nominal_i × (1 + u)`, `u` uniform in `[−0.20, +0.20]` (*[choice]* jitter applied after the cap); always > 0. Reset to `i = 0` on an Applied reservation in that lane/queue (cooldown) or actual work / useful wake hint (idle).

**F-11 Smooth weighted round robin** (per level): trial `t_x = score_x + w_x`; winner = argmax (rotating tie-break); on Applied only: `score_x := t_x` for all eligible x, then `score_winner −= Σ w_x`. Shares under a stable set: queues 1:3 → 2,500/7,500 per 10,000; lanes 6:3:1 → 6,000/3,000/1,000 per 10,000.

**F-12 Lane age** = `(snapshot.ObservedAt − OldestEligibleAt) + monotonic elapsed since snapshot receipt`; aged iff `age >= AgingThreshold`.

**F-13 Connection ceiling.** Per worker group: `8 control + 14 work + 1 listener = 23` (work = 4 + `MaxConcurrentCalls` + 2; default 8). Six such workers plus one migration runner: `6 × 23 + 1 = 139`. Deployment ceiling = Σ worker/producer/management pool ceilings + listeners + migration runners + business connections (plus administration/failover headroom).

**F-14 Renewal-wave check** (planning, not proof): `ceil(GroupMaximum / RenewalLane) = ceil(16/4) = 4` service waves; at `ControlRequestTimeout` each, `4 × 5 s = 20 s`; plus `RenewalIntervalMax + SchedulerAllowance = 20 s` → 40 s of the 50 s usable authority. A second full timeout wave would exceed it; hence the pressure gates.

**F-15 Local admission memory:** `64 waiters × 2 MiB envelope = 128 MiB` before object overhead (hosts may lower the waiter bound).

## 3. Startup validation

Startup rejects any violation with an error naming the setting(s) and the inequality; it never silently adjusts a value. The inequalities are evaluated on the effective configuration in milliseconds with checked arithmetic.

| ID | Inequality / rule | Defaults |
|---|---|---|
| V-01 | Every duration is finite, positive (except where 0 is explicitly allowed), a whole number of milliseconds *[choice]*, and representable without overflow in signed 64-bit ms | pass |
| V-02 | `0 < LeaseSafetyMargin < LeaseDuration` | 0 < 10 s < 60 s |
| V-03 | `0 < RenewalIntervalMin ≤ RenewalInterval ≤ RenewalIntervalMax` | 12 ≤ 15 ≤ 18 s |
| V-04 | `RenewalIntervalMax + 2·ControlRequestTimeout + ControlRetryBackoffMax + SchedulerAllowance < LeaseDuration − LeaseSafetyMargin` | 31 s < 50 s |
| V-05 | `MinimumInvocationMargin ≥ 2·ControlRequestTimeout + ControlRetryBackoffMax + SchedulerAllowance` | 15 s ≥ 13 s |
| V-06 | `StopMargin = (LeaseDuration − LeaseSafetyMargin) − RenewalIntervalMax − ControlRequestTimeout − SchedulerAllowance > 0` (implied by V-04, checked explicitly) | 25 s > 0 |
| V-07 | `0 < LeaseWatchdogInterval ≤ 1 s` | 1 s |
| V-08 | `0 < ControlRetryBackoffInitial ≤ ControlRetryBackoffMax` | 100 ms ≤ 1 s |
| V-09 | `ControlRequestTimeout < OwnershipCommandLifetime` *[choice]* | 5 s < 30 s |
| V-10 | Each shutdown slice > 0 and `ShutdownDrain + ShutdownCancellation + ShutdownFinalization ≤ ShutdownBudget`; hence `DrainCutoff < CancellationCutoff < ControlStopCutoff ≤ T0 + ShutdownBudget` | 15 + 5 + 5 = 25 ≤ 25 |
| V-11 | If `ShutdownBudget ≥ HostOptions.ShutdownTimeout`: startup **warning** (not failure) *[choice]*; host options are never overwritten | 25 s < 30 s |
| V-12 | `ClaimResolutionBudget`, `BeginResolutionBudget`, `CompletionResolutionBudget`, `CancellationGrace`, `CleanupBudget` > 0; each resolution budget ≥ `ControlRequestTimeout` *[choice]* | 15/15/30/10/10 s; 15 ≥ 5 |
| V-13 | `0 < RecoveryScanMin ≤ RecoveryScanNominal ≤ RecoveryScanMax` | 4 ≤ 5 ≤ 6 s |
| V-14 | Per generation policy: `MaxExecutions ≥ 1`; `MaxConsecutiveUnstartedAbandonments ≥ 1`; `ExecutionTimeout` absent or > 0; for both retry policies `0 < InitialDelay ≤ MaximumDelay`, evaluator `exponential-v1` version 1, jitter `equal`; all within int32 counts / int64 ms | 3; 5; none; 5 s ≤ 60 s; 1 s ≤ 30 s |
| V-15 | Each pool subscribes ≥ 1 queue; `FixedConcurrency(n)` with `n ≥ 1`; `AutoConcurrency(min, max)` with `0 ≤ min ≤ max`, `max ≥ 1`, not both modes (SP-014); `Σ pool maxima ≤ GroupMaximum`; queue weights integers in [1, 1,000] | 4 ≤ 16 |
| V-16 | `RenewalLane + SettlementLane ≤ ControlPoolMax`; `Admission + ProducerAdmin + Background ≤ WorkPoolMax`; `1 ≤ BackgroundHeavy ≤ Background − 1` *[choice]*; all > 0 | 4 + 4 ≤ 8; 4 + 8 + 2 ≤ 14 (the work maximum is this sum); 1 ≤ 1 |
| V-17 | Deduplicated capability tuples per polling group ≤ 512 *[choice]* | registry-dependent |
| V-18 | `LocalAdmissionWaiters ≥ 1`; `0 < LocalAdmissionWait` | 64; 1 s |
| V-19 | `PresenceHeartbeat × 1.1 < PresenceExpiry` *[choice]* | 5.5 s < 20 s |
| V-20 | Lane weights positive integers; `AgingEvery ≥ 1`; `AgingThreshold > 0`; mode ∈ {weighted, strict-with-aging, strict} | 6/3/1; 8; 60 s |
| V-21 | `BlockedCandidatesPerLaneVisit ≥ 1`; `ReservesPerRound ≥ BlockedCandidatesPerLaneVisit` *[choice]*; `RecoveryBatch ≥ 1` | 8 ≤ 32; 32 |
| V-22 | For idle backoff and cooldown: `0 < base ≤ cap`; jitter fraction in [0, 1); hint max age > 0; probe spacing > 0 | 100 ms ≤ 2 s (10 s listening); 250 ms ≤ 2 s; 0.20; 2 s; 50 ms |
| V-23 | `LeaseDuration` within the store's supported lease bounds (checked against the store compatibility manifest) | 60 s |
| V-24 | The binary knows every capability the application requires (SP-013 builds know `rate_admission`; earlier binaries refuse a rate-enabled application) | pass |

Worked failures (TIME-01): `LeaseDuration = 30 s` → V-04 (31 ≮ 20) and V-06 (StopMargin = −5 s); `ControlRequestTimeout = 10 s` → V-05 (15 ≱ 23) although V-04 holds (41 < 50); `ShutdownDrain = 20 s` → V-10 (30 > 25); `MinimumInvocationMargin = 12 s` → V-05; `LeaseWatchdogInterval = 2 s` → V-07. A host may scale leases and intervals together (e.g. lease 120 s, safety 20 s, renewal [24 s, 36 s]: 36 + 10 + 1 + 2 = 49 < 100; StopMargin = 100 − 36 − 5 − 2 = 57 s; ResumeMargin = 62 s). Static inequalities do not prove group-wide capacity, thread-pool health or store clock stability; runtime margin checks always override them.
