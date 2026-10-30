> History; see [docs/spec](../spec/00-overview.md) for the normative specification.

# Capacity defaults, fairness and connection budgets

Status: concrete D-07 recommendation, 2026-10-29. Planning only. These are initial implementation/test settings, not measured production limits or user-approved numerical defaults. This packet refines [capacity control](capacity-control.md), [worker ownership](worker-engine-internals.md), [execution timing](execution-defaults-and-timing.md) and [provider dependencies](provider-dependency-plan.md).

## 1. Capacity ownership and default mode

Recommend **fixed concurrency by default**, with autoscaling explicitly enabled per pool. A worker must have explicit queue subscriptions; registering handlers alone does not create additional pools. Both embedded and external workers use the same settings.

| Setting | Initial recommendation |
|---|---|
| Default fixed pool | 4 outstanding attempts when no mode is configured. `FixedConcurrency(n)` sets both the target and that pool's hard maximum to `n`; there is no separate maximum setting for fixed pools |
| Automatic pool | Explicit min/max required; getting-started example min=1, max=16 |
| Worker/store group maximum | 16 outstanding attempts across its pools |
| Configured pool maxima | Sum must fit the group maximum; reject invalid configuration |
| Per-pool claim concurrency | 1, including resolution of an uncertain claim |
| Reserve/Begin I/O across pools | At most 4 calls in the work connection pool |
| Prefetch beyond tickets | 0 |
| Completion and renewal queues | At most one current scheduled item per outstanding attempt per coordinator |

A group is the set of pools sharing one worker/store budget, even if they serve multiple applications. Separate stores/groups add their budgets; startup reports their sum for the process. Registration-time maxima are memory/admission bounds, not instructions to open that many connections. Manual target overrides cannot exceed a pool's configured maximum. Fixed target zero suspends admission; auto min zero retains the existing bounded bootstrap behavior.

Tickets remain held through pending claims, handler work, cleanup and settlement. Lost authority does not free a ticket while user code remains alive. Reducing a target stops refill, including when outstanding tickets already exceed it. No controller allocates replacement attempts around hung handlers. Configured payload-byte limits bound retained payload buffers, but cannot bound handler allocations or decoded object graphs.

Do not derive a default from processor count: asynchronous external I/O, CPU-heavy work and handler memory have different constraints. Hosts can use separate pools with explicit maxima. Runwick changes no process-wide ThreadPool settings. Runtime starvation is an operational input, not something extra queue tickets fix. [ThreadPool starvation guidance](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/debug-threadpool-starvation)

## 2. Fairness, probing and cooldowns

Retain the two-level smooth weighted round robin algorithm: queue selection, then lane selection. Selection/settlement is serial per pool; execution remains concurrent.

| Setting | Initial recommendation |
|---|---|
| Queue weights | 1 each; configured enabled weights 1–1,000 |
| Lane weights | high=6, medium=3, low=1 |
| Default mode | Weighted with aging |
| Aging | After 8 successful queue admissions, try a lower lane aged at least 60 s |
| Blocked-candidate scan | At most 8 candidates per lane visit |
| Scheduling round | At most 32 Reserve calls, including successes; then yield |
| Readiness maximum hint age | 2 s; expired hints require a refreshed probe |
| Idle probe backoff | 100 ms doubling to 2 s, jitter ±20%; reset after actual work |
| Blocked lane/queue cooldown | 250 ms doubling to 2 s, jitter ±20% |
| Notification-triggered probe spacing | At least 50 ms per polling group; one probe in flight |

Positive-weight lanes receive weighted admission opportunities when continuously claimable. Aging changes those ratios intentionally. Strict mode remains opt-in; strict-with-aging and strict-without-aging are distinct settings. Queue/lane weights do not reserve CPU time, cancel work or guarantee per-job start deadlines.

Bound the aging counter by saturating at 8. Increment on each successful admission in the queue, including when no lane is yet aged. Reset on a successful aging selection; rotate the lower-lane pointer on failed aging attempts. Failed claims do not commit weighted scores or consume successful-admission shares. Reset lane scores after an aging success and all applicable scores after a weight revision. A lane returning from ineligibility starts at zero.

Cooldown scope must follow evidence. Queue-wide blocking can suppress its lanes; type/resource blocking cannot suppress unrelated work. Preserve cursor continuation for resource-blocked candidates. Notifications coalesce and cannot repeatedly erase a cooldown; only a relevant control revision or known release can prompt an earlier recheck, still subject to probe spacing. NoWork invalidates the lane hint; polling repairs missed notifications. Hints are never ownership authority.

When pools contend for the shared claim-I/O allowance, serve their ready requests round robin, one request per turn. Each has at most one outstanding claim request. This bounds cross-pool queueing without claiming globally synchronized fairness across replicas. Begin and claim-resolution work already owning tickets precede fresh Reserve requests within the admission class.

## 3. Rate-policy disposition

Retain exact rolling-window Begin authorization, all-or-none across required scopes. There is **no implicit rate limit**: a configured policy must supply its count and window. Initial validation bounds: 1–100,000 authorizations per window; window 1 s–24 h in whole milliseconds; at most 8 distinct rate scopes per job generation. These are implementation resource bounds, not a certified throughput range. Reject values outside them explicitly rather than approximating with another algorithm.

Recommend retaining the conservative edit rule: a semantic count/window change blocks new authorizations for one full new window in the scope's logical time domain. An identical-policy update does not restart the freeze. Existing grants remain; cleanup uses the existing scope lock and current policy. Removing a requirement from a running generation is disallowed; changed requirements apply to newly prepared generations. Operational changes never turn a missing scope into unlimited admission.

Rate rejection closes the reservation without consuming execution or abandonment budget and releases concurrency permits. Use a minimum defer interval of 250 ms. Compute eligibility as the later of the authoritative grant-expiry/not-before hint and that minimum; add 0–100 ms jitter, never subtract it. Persist the chosen result in the command receipt so replay cannot move the deadline again. A distant eligibility hint is not capped to the ordinary 2 s probe cooldown.

Resource cardinality remains a storage sizing concern. The per-job scope cap bounds one transaction, not the number of distinct resource keys an application can create. Measure retained grants, scope rows and cleanup/WAL cost; do not present the maximum count/window as a safe workload recommendation.

## 4. Automatic controller: a measurable initial estimator

Refine the earlier arrival-plus-backlog sketch for v1: **do not require a new durable ready-arrival counter**. Delayed jobs, retries, rate deferrals and capability changes make enqueue rate an unsuitable substitute. Use observed local occupancy plus allocated due backlog:

```text
queuedWork_i = sum(allocated due backlog_k * estimated ticket seconds_k)
desired_i = ceil((meanHealthyOccupiedTickets_i + queuedWork_i / 30s) / 0.80)
candidate_i = clamp(desired_i, MinConcurrency, MaxConcurrency)
```

Occupancy is the time-weighted mean over the last 30 s, integrated from local ticket changes. It includes normal claim/setup/cleanup/settlement time; an unhealthy storage interval invalidates the estimator instead of teaching it inflated service cost. Due backlog excludes already reserved/running attempts, so those jobs are represented by occupancy only. A worker that holds a local outcome while storage has already made the job retryable can temporarily overestimate demand; the bounded controller tolerates that observation lag without claiming an exact workload count.

Divide each due capability category among fresh eligible pool subscriptions using configured share weights, default 1. Include auto pools at target zero so they can discover demand; exclude draining, faulted and manually suspended pools. Use complete decoding tuples and queue subscriptions. Replica disappearance changes the next share snapshot, never durable concurrency limits. Use fractional shares in arithmetic and round only the final target.

| Input/control | Initial recommendation |
|---|---|
| Sample interval | 5 s, independently jittered ±10% |
| Presence heartbeat / expiry | 5 s ±10% / 20 s in store time |
| Backlog/share snapshot age | At most 15 s; carry observed time and coverage |
| Service estimate | EWMA alpha=0.20 of healthy completed ticket durations per registered job type |
| Cold estimate | 1 s, explicitly overridable per type; low confidence until 20 healthy samples |
| Long active work | Use the largest elapsed healthy active-ticket duration for that type as a lower bound on its estimate |
| Drain horizon / target utilization | 30 s / 80% |
| Grow threshold | Candidate ≥ current + max(1, ceil(current × 0.20)) |
| Shrink threshold | Candidate ≤ current − max(1, ceil(current × 0.20)) |
| Consecutive evidence | 3 qualifying samples to grow; 6 to shrink |
| Change cooldown | At least 30 s since last change |
| Maximum step | max(1, ceil(current × 0.25)), clamped toward candidate |

Boundary rule (accepted 2026-10-30, R4): when `candidate == MaxConcurrency` and `current < MaxConcurrency`, the growth threshold is waived — the sample qualifies as growth regardless of the 20 % band, subject to the same consecutive-evidence, cooldown and step rules. Symmetrically, when `candidate == MinConcurrency` and `current > MinConcurrency`, the shrink threshold is waived. Without this, the proportional band makes the last step unreachable (from target 15 the band demands 18 before moving to a maximum of 16).

Require positive allocated due backlog on every qualifying growth sample. Occupancy alone cannot grow a pool after its backlog has drained. Reset the opposite-direction count on each sample; reset both on a neutral/invalid sample, membership/configuration revision, override transition or applied change. Qualifying samples may accumulate during cooldown, but no target change occurs before the cooldown expires. Jitter means three samples are not an exact 15 s promise.

Incomplete/truncated capability coverage, stale observations or exhausted sampling budgets mean unknown, not zero. Hold the target and reset evidence. Cache one shared estimate per compatible group; use bounded indexed queries with a 1 s read deadline and no overlapping sample requests. An estimate that cannot complete inside this budget disables automatic changes and reports why; normal fixed-target execution can continue when storage remains healthy. Exact SQL/query-plan work belongs to SP-012/SP-029.

Auto min zero permits one bootstrap ticket when no fresh demand snapshot exists and max is positive. A bootstrap is a temporary target of one, not an additional ticket beyond the hard maximum; it cannot override pause, host pressure, storage trouble or command uncertainty. Growth beyond one needs fresh feedback. Remove the bootstrap only after trusted idle evidence or an explicit control change, avoiding repeated zero/one oscillation on every failed sample.

The previous arrival-rate term is deferred as an estimator enhancement. This initial controller reacts to backlog and occupied capacity; it does not predict future arrivals. Very short bursts may finish before scaling reacts, and a cold estimate can understate long-running work. These are intentional measurable limitations, not hidden accuracy claims.

## 5. Pressure and uncertainty gates

Control observations are independent of exported metrics. A missing exporter must not disable renewal or alter capacity. Use monotonic local times and the existing independent watchdog.

- Freeze automatic increases while control-I/O p95 exceeds 250 ms over a rolling 30 s with at least 20 samples, or while any ownership command is unresolved. Too few latency samples alone does not mark the store unhealthy; local deadlines still apply.
- Freeze growth when at least 20 completed admission cycles in the last 30 s show 75% or more ending in concurrency blockage or rate deferral. Count one final admission outcome per cycle, not both its successful Reserve and rejected Begin. Also suppress demand for a queue while authoritative queue-wide saturation is known; unrelated queues remain eligible. The ratio is a heuristic, never a permit check.
- Stop new Reserve admission for the affected worker/store group when an actionable control item has waited 1 s, any still-owned attempt has less than `StopMargin` confirmed local safety margin, or the oldest pending Finish has waited 5 s. Continue resolving and settling existing attempts; an arbiter-blocked item is also an unresolved-command signal.
- Existing claim/Begin resolution expiry and critical coordinator faults retain their stronger suspend/drain behavior. A newly observed unresolved Reserve/Begin pauses fresh claims in its pool immediately; do not use other free tickets to build an uncertainty backlog.
- Resume a pressure-based admission pause only after three consecutive 1 s healthy checks: no expired or unresolved owner command, no control wait above 250 ms, minimum confirmed live margin at least `ResumeMargin`, and no pending Finish older than 1 s. No owned attempts means the margin condition is satisfied. Authority loss stays latched for the affected attempt.

Review correction, 2026-10-30: the two margin gates are **derived from the validated worker timing policy**, not fixed constants. An earlier draft hard-coded 25 s and 30 s, which only hold for the 60 s lease / 10 s safety-margin defaults; with a shorter configured lease (for example 30 s, whose maximum confirmed margin is 20 s) a worker could pause admission immediately and never satisfy the resume condition. Define:

```text
usableAuthority = leaseDuration - leaseSafetyMargin
StopMargin      = usableAuthority - renewalIntervalMax - controlRequestTimeout - schedulerAllowance
ResumeMargin    = StopMargin + controlRequestTimeout
```

With the D-05 defaults this is `50 - 18 - 5 - 2 = 25 s` and `25 + 5 = 30 s`, reproducing the earlier numbers. Startup validation must reject `StopMargin <= 0`; the existing renewal inequality in the timing packet already implies a positive value for the defaults, but the check is explicit so a host changing only `LeaseDuration` cannot silently produce an unreachable resume condition. In healthy operation the lowest confirmed margin just before a due renewal is roughly `usableAuthority - renewalIntervalMax - RTT` (about 31 s by default), so one renewal delayed by more than `controlRequestTimeout + schedulerAllowance` triggers the stop gate: that is deliberate, but CAP-10 must confirm it does not flap under ordinary jitter.
- Sample local coordinator timer lag once per second. Lag above 250 ms for three consecutive observations freezes growth; lag above the existing 2 s scheduler allowance stops refill. A configured host-pressure signal can also stop refill. Recovery uses three healthy checks. Do not equate CPU utilization or a ThreadPool thread count with proof of starvation.

These gates may temporarily pause admission despite a configured minimum. MinConcurrency is a target floor for normal control decisions, never permission to ignore safety. Conservative pressure thresholds are candidates for measurement; an unavailable server or arbitrary process pause can still cause lease loss.

## 6. PostgreSQL connection and I/O budgets

For the initial direct-primary profile, propose two long-lived Npgsql data sources per worker/store group, with explicit bounded operation admission ahead of connection acquisition. Npgsql exposes per-pool minimum/maximum sizes; set them explicitly instead of inheriting the documented maximum of 100. [Npgsql pooling settings](https://www.npgsql.org/doc/connection-string-parameters.html)

| Pool/class | Maximum connections/calls | Rule |
|---|---|---|
| Control data source | 6 connections, minimum 0 | Worker ownership operations only |
| Renewal lane | 4 concurrent calls | Renew and resolution of unknown Renew; earliest local safety deadline first |
| Settlement lane | 2 concurrent calls | Finish and resolution of unknown Finish; earliest resolution/safety deadline first |
| Work data source | 8 connections, minimum 0 | All non-control operations |
| Admission class | 4 concurrent calls | Reserve, Begin and their receipt resolution |
| Producer/admin mutation class | 2 concurrent calls | Dispatch and operational mutations |
| Background class | 2 concurrent calls | Presence, probes, recovery, scheduling, cleanup and projections |
| Background heavy work | At most 1 of its 2 calls | Dashboard/history/cleanup cannot occupy both; presence/readiness take precedence |
| Optional listener | 1 dedicated session | Not taken from either pooled allowance |
| Deployment migration | 1 dedicated session per runner | Separate from worker process budget |

The class ceilings are partitions, not additional allowances above the pool maximum. No borrowing between renewal and settlement initially, so a burst of finishes cannot occupy every renewal slot. Per-attempt command serialization still wins; a Finish with unknown outcome prevents a new Renew for that attempt.

Calls retain their I/O slot until the actual provider task terminates and its connection is disposed, even after the local wait deadline expires. Never free a slot because Task.WhenAny returned while old I/O remains alive. Unresolved waiters live in the bounded attempt registry. Public producer/admin admission allows at most 64 waiting operations per group, with a 1 s local admission wait cap (or caller's earlier cancellation); excess work returns a distinct not-submitted local-capacity result. Preparation may have occurred, but no storage command was sent. Do not report this as a durable queue rejection or an unknown commit. Retain at most one prepared envelope per admitted waiter and avoid extra copies; with the proposed 2 MiB envelope cap, 64 waiters alone can retain up to 128 MiB before object overhead, so hosts may lower this bound.

Background loops coalesce pending work by role; do not allocate one waiting task per discovered job. The completion channel holds IDs only, bounded by the group attempt maximum, with the immutable outcome retained on its entry. No default transport batching: each ownership command keeps its own identity and outcome. Batching is a later measured optimization.

Connection planning for one single-endpoint group with a listener:

```text
worker connection ceiling = 6 control + 8 work + 1 listener = 15
six such workers + one migration runner = 6 × 15 + 1 = 91
deployment ceiling = all worker/producer/management pool ceilings
                   + listeners + migration runners + business connections
```

Server administration, failover overlap and other applications need additional capacity. This is a planning ceiling for normal sessions, not a count of currently open sockets; provider cancellation/reconnection may require transient connections. Multiple hosts/endpoints and multiple configured stores require explicit aggregate accounting, not multiplication hidden behind a single pool setting. Producer-only processes create no control pool; caller-owned transaction connections belong to the business application's budget.

Do not multiply these connection pools by the number of execution pools. With 16 leases and 4 renewal calls, a simultaneous renewal wave requires four service waves. At an assumed 100 ms per operation it takes about 0.4 s; at the 5 s request budget it can take 20 s, before retries. Combined with the latest 18 s renewal due time and 2 s scheduling allowance, that reaches 40 s of a nominal 50 s safety window. A full second timeout wave would exceed it. This demonstrates why per-attempt timing arithmetic does not prove group-wide capacity and why pressure must stop refill early. The 100 ms value is an experiment input, not a measured result.

Higher attempt maxima require an explicit budget change and new renewal/settlement saturation tests. The same logical priorities apply to SQLite's bounded executor, but extra connections do not create parallel writers. Redis connection counts remain behind its physical-connection/persistence prototype; do not copy PostgreSQL's numbers into a multiplexer configuration.

## 7. Acceptance workloads and evidence

Use fake time for selector/controller decisions and real-store barriers for contention. All cases below remain unexecuted against Runwick.

| Case | Required evidence |
|---|---|
| CAP-01 | Invalid pool sums/bounds fail; fixed and automatic modes respect group limits, including hung handlers and target reductions |
| CAP-02 | Stable queue/lane shares; failed claims leave scores unchanged; membership/weight changes cannot bank credit |
| CAP-03 | Aging after 8 successes, threshold equality, lower-lane rotation and strict-mode starvation behave as specified |
| CAP-04 | Notification storms, blocked resources and empty lanes obey probe/scan/cooldown bounds without hiding unrelated work |
| CAP-05 | Rate boundaries, identical edits, semantic-edit freeze, jitter/replay and multi-scope rejection preserve existing admission invariants |
| CAP-06 | Estimator excludes future/unsupported work, allocates capability shares once, handles long active work and holds on partial snapshots |
| CAP-07 | Hysteresis, evidence resets, cooldown, step sizes, zero bootstrap and override expiry produce deterministic target traces |
| CAP-08 | Stale presence and 1→3→6 replica changes cannot multiply demand estimates or weaken distributed limits |
| CAP-09 | Saturated dispatch/dashboard lanes leave renewal/settlement access; background work cannot starve presence indefinitely |
| CAP-10 | All leases due together, slow replies and concurrent finishes expose wave queueing; pressure gates activate and late responses never revive authority |
| CAP-11 | Timed-out provider tasks retain slots; public overload is not-submitted; all internal queues stay bounded during a 60 s outage |
| CAP-12 | Host timer lag, noncooperative handlers and recovery preserve tickets; scale-down cancels no running work |

Workload matrix: 1/3/6 replicas; one hot queue versus queues weighted 1:3; 10 ms, 1 s and 60 s handlers; mixed capability support; payloads 1 KiB/64 KiB/1 MiB; no rate limit versus a hot shared rolling-window scope; steady load versus 1,000-job bursts; normal control latency versus injected 100 ms/1 s/5 s delays; full dashboard/producer load and 60 s storage outage. Record exact server/host configuration and test seed.

Compare fixed 4, fixed 16 and auto 1–16. Measure successful Begin/Finish throughput, queue-age percentiles, admission shares, rate deferrals, control queue wait/latency, safety margin/losses, unsettled age, connections, retained memory and rate-grant/receipt/WAL growth. Do not assert throughput targets before the first measured baseline. CAP-09/10 must verify no lease loss in the declared healthy-load profile; deliberately over-budget failures must prove safe abandonment and bounded resources, not uninterrupted execution.

Planning arithmetic check performed with a standalone Python model, not Runwick: 10,000 selections gave queue shares 2,500/7,500 for weights 1:3 and lane shares 6,000/3,000/1,000 for 6:3:1 without aging. With exact 5 s samples, continuously qualifying backlog and candidate 16, target 4 grows to 5/7/9/12/15 at 15/45/75/105/135 s, then to 16 at 165 s under the R4 boundary rule (before R4 it stayed at 15 because candidate 16 lay inside the hysteresis band). From target 16 with candidate 1, the shrink trace is 12/9/6/4/3/2/1 at 30/60/90/120/150/180/210 s. Connection and renewal-wave arithmetic above was also checked. Dynamic membership, aging, estimator accuracy, storage contention and fault behavior remain unvalidated.

Map CAP-01–04 to SP-009/SP-012; CAP-05 to SP-013; CAP-06–08 to SP-014; CAP-09–12 to SP-005/SP-009/SP-027–029. Cross-check DEP-03/04 and TIME-13. The [D-08 scheduler packet](scheduler-evaluator-and-defaults.md) now supplies parser/time-zone selections, defaults and compatibility vectors using these I/O budgets. The [outbox design](transactional-outbox.md) and [final baseline](../architecture-baseline.md) complete the remaining planning.
