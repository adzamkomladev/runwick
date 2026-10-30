# Fairness, distributed rate admission, and scaling

Status: engineering proposal, 2026-10-29. Extends [worker internals](worker-engine-internals.md) and the [storage protocol](storage-command-protocol.md). No runtime or performance validation has been performed. Proposed policies and tuning values are not user-approved defaults.

The [capacity defaults/budgets packet](capacity-defaults-and-budgets.md) now supplies concrete D-07 values, rate-policy bounds, pressure gates and provider budgets. It refines the initial scaler to use observed occupancy plus due backlog; an arrival-rate estimator is deferred. Use that packet for exact defaults and CAP acceptance cases.

## 1. Three independent controls

| Control | Authority | Unit |
|---|---|---|
| Queue/lane fairness | Each local pool | Reservation opportunities among eligible lanes |
| Concurrency | Local tickets plus atomic distributed permits | Outstanding local attempts / valid distributed grants |
| Rate admission | Shared store at BeginExecution | Authorized executions in a rolling time window |

Scaling changes a pool's local ticket target. It neither changes a shared ceiling nor bypasses admission rules. Queue priority remains a string equality predicate in storage, not a numeric database sort. Queue weight divides opportunities between queues; priority weight divides them within a queue.

Time-based and concurrency limiters are different algorithms. Microsoft's rate-limiter documentation also notes that chained time-based acquisitions need not refund an earlier charge when a later limiter rejects. Runwick instead proposes an all-or-none transaction across its rate scopes. This is an architectural choice, not a claim that .NET's process-local limiter supplies distributed admission. [Algorithm reference](https://learn.microsoft.com/en-us/aspnet/core/performance/rate-limit?view=aspnetcore-10.0)

## 2. Exact local selection rule

Use smooth weighted round robin at two levels: queues, then the selected queue's priority lanes. Weights are positive bounded integers; zero disables a subscription explicitly rather than participating in the arithmetic. Initial lane weights of high:medium:low = 6:3:1 are a benchmark proposal only. Queue weights default to equal in the examples.

For one selection opportunity:

1. Build the eligible set from the shared readiness snapshot, subscriptions, pause flags, cooldowns, and remaining round scan budget.
2. For each candidate, compute a trial score `storedScore + weight`. Select the highest score, with a rotating stable tie-breaker. Do the same for lanes within the chosen queue.
3. On Applied Reserve, commit the trial scores and subtract the sum of eligible weights from the winner at each level. Charge exactly one reservation, irrespective of handler duration. Keep the scores for the next opportunity.
4. On NoWork, QueuePaused, or CapacityBlocked, discard the trial score changes. Exclude/cool down the relevant lane or queue for this round and retry within the worker's existing 8-blocked-candidates/32-total-attempts budgets. A failed storage claim does not consume a successful-admission share.
5. When membership changes, clamp carried scores to ± the new total weight. Returning lanes start at zero; they do not bank unlimited credit while idle. Reset scores on a weight configuration revision.

Claims within a pool are selected/settled serially in the first implementation so parallel requests cannot spend the same tentative scores. Handler execution remains concurrent. Measure whether claim round-trip latency limits filling the pool before adding parallel selections with an explicit credit-reservation protocol. Different pools/replicas may claim concurrently.

Under a stable, continuously claimable set, this produces weighted admission shares. It promises neither CPU-time fairness, strict FIFO, nor a globally synchronized cross-replica sequence. A long job can occupy a slot much longer than a short job. If all slots are occupied, priority does not preempt a running handler.

### Aging without rewriting priority

Each queue tracks successful admissions since its last aging selection. After `AgingEvery` successes, try an aged lower lane before its next weighted selection. A lane is aged when storage-observed oldest eligible age exceeds `AgingThreshold`. Pick among aged lanes with a rotating pointer. Proposed test settings: AgingEvery=8 and AgingThreshold=60 seconds; both remain tunable.

An Applied aging reservation resets the counter. A failed aging attempt moves its pointer and obeys normal scan/cooldown budgets; then regular selection may proceed. Charge the outer queue selection normally. Within the queue, reset lane scores after an aging success so the override cannot accumulate priority debt. Aging is an intentional modification of the weighted ratios.

Strict priority selects the highest available lane. Configuration must explicitly say whether aging can override it: `strict-with-aging` permits periodic lower-lane selection; `strict` disables aging and may starve lower lanes. In weighted mode, every positive-weight claimable lane already receives opportunities; aging helps responsiveness, not a guarantee that a particular locked/resource-blocked job will run by a deadline.

Use observation time and a monotonic local elapsed delta for age between probes, with a maximum hint age. Refresh stale hints. An old capacity-blocked head may make a lane appear aged; bounded cursor scans are still necessary. Queue weights never become job priorities.

## 3. Distributed rate contract

Propose an **exact rolling-window count of Begin authorizations**, with parameters `Limit = N` and `Window = W`. For a scope at database decision time `t`, admit only if fewer than N grants have timestamps in `(t-W, t]`. Each execution costs one grant in every required scope. Support queue, job-type, and resource-key scopes; local rate hints have no authority.

This choice directly expresses “at most N authorizations per rolling W” without fixed-window boundary bursts or token-bucket burst allowances. It costs one durable grant row per applicable scope per authorized execution. High-rate scopes may justify another explicitly named algorithm after measurement; a token bucket must not silently replace rolling-window semantics.

The counted event is the same transaction that changes reserved → running and increments execution budget. A crash after that commit still consumes the rate grant. Handler retries and manual-retry executions obtain new grants; command replay does not. Renew and Finish do not charge or refund them. Refunds after a failed handler could allow an unbounded failing start rate.

These are authorization timestamps, not measured first instructions or downstream API calls. A paused process can execute later than its authorization; applications requiring an external provider's exact request-rate guarantee need admission at the external call boundary. No queue engine can atomically couple a database commit to arbitrary user code.

## 4. Records, locks, and Begin changes

Proposed additional PostgreSQL records, all application-scoped:

```text
rate_scopes:
  application, rate_scope_id uuid                    PK
  scope_kind text, scope_key text                    UNIQUE with application
  limit_count integer > 0, window_ms bigint > 0
  policy_revision bigint, last_decision_at timestamptz
  not_before timestamptz?

job_rate_requirements:
  application, job_id uuid, rate_scope_id uuid        PK; FKs to jobs/rate_scopes

rate_grants:
  application, rate_scope_id uuid, attempt_id uuid    PK
  authorized_at timestamptz, policy_revision bigint
  INDEX(application, rate_scope_id, authorized_at)
```

Grant identity deliberately does not cascade with job-history retention. Rate history must survive deletion of completed job payloads. Requirements are immutable during a generation; scope policy is a revisioned operational setting. Bound scope cardinality, window length, and grant retention. Unknown required scope or malformed policy is an invariant fault, never “unlimited.”

After the [store/application compatibility gates](postgres-schema-and-operations.md), use command → queue control when needed → job → active uniqueness keys when required → concurrency scopes in UUID order → rate scopes in UUID order → dependent writes. Rate-only policy/cleanup operations never acquire jobs/concurrency afterward. Batch provisioning processes whole scope families in canonical order. Durations use the common millisecond quantum; window_us from the earlier sketch is superseded.

Begin now does the following in one transaction:

1. Resolve its command receipt; lock the job and validate exact ownership/state. If cancellation or exhausted execution budget can terminalize it, acquire any required active-key locks before lower-level locks.
2. Lock all associated concurrency scopes, then all rate scopes. Recheck command deadline and lease validity using fresh database time after blocking locks. Cancellation/terminal-budget handling happens before charging rate.
3. For rate arithmetic set `t = max(databaseTime, last_decision_at of all participating rate scopes)`. This prevents a backward clock adjustment from refilling a window. Store this logical rate time separately from actual Begin authorized_at if they differ; the rate_grants timestamp is t. A forward database clock jump remains an operational limitation and must be monitored.
4. In fresh statements after the locks, count grants newer than `t-W` in each scope. Admission requires every scope to have capacity and satisfy not_before.
5. If allowed, insert one grant per scope and perform the existing Begin state/counter writes plus receipt. All charges commit together; failure in any scope leaves no partial grant.
6. If blocked, charge no grant and no execution budget. Close the unstarted attempt as `admission_deferred`, release its exact concurrency permits, clear ownership, and restore pending (generation started_count=0) or retrying (otherwise). Preserve active uniqueness. Set eligible_at to a future admission hint, increment lifecycle revision, and record the result/event atomically.

For a blocked scope with at least N grants, find the grant whose expiry first reduces count below N (the `(count-N+1)`th oldest retained grant). For multiple scopes take the maximum required instant, also respecting not_before and a positive minimum rescan delay. Translate a logical-time hint conservatively back to storage wall time if the clock moved backward. The hint is not a future reservation: another worker may consume capacity first.

Add `StoreStatus.RateLimited` and bounded result metadata containing blocking scope IDs and `EligibleAt`. An Applied Begin still requires BegunExecution. A RateLimited Begin has no BegunExecution value but **does have committed effects and a retained receipt**. The worker retires the entry after confirming that result; it does not create a scope, invoke middleware, increment the unstarted-crash guard, or hold a local slot until EligibleAt. A lost reply replays the same deferral; no double grant or duplicate close.

This deliberately revises the original lightweight Begin protocol. Add an operation/schema version and reject incompatible worker implementations before enabling rate policies; a driver that ignores these checks cannot continue claiming work. Deployment must drain/replace older workers before activation. Registry payload capabilities alone do not prove support for this protocol feature.

Reserve may later consult a cheap rate-blocked hint to avoid churn, but it never precharges a grant. Correctness remains in Begin. Rejected Begins can be costly under contention; measure them explicitly and apply queue/type/resource backoff with the same conservative scope classification as concurrency.

### Edits, retention, and time

Code seeds an absent rate policy and reports mismatches; it never overwrites live settings. Policy edits use expected revision and the same nondecreasing scope time. Conservatively freeze new authorization for one full **new** window after a policy change, preserving existing grants; not_before uses that logical rate-time domain. This avoids pretending to know older history after an enlarged window. This conservative edit behavior is a proposal to review, not a necessary property of every limiter implementation.

Cleanup locks a scope, obtains the same nondecreasing logical time, and deletes grants outside its active window in bounded batches. Policy-edit freeze covers any history already pruned. Never delete grants while an unlocked reader might use an incompatible policy snapshot. Scope rows are not evicted while requirements or window history need them. Use parameterized SQL and checked arithmetic for intervals/counts.

## 5. Autoscaling inputs and control law

Each pool owns its local scaler. No permanent cluster scaling leader is required. Shared ceilings remain enforced by storage even when several pools make simultaneous target changes.

Use sampled management snapshots, separate from cheap readiness probes:

- Due pending/retrying backlog by queue and complete supported decoding tuple (name/payload version/serializer profile ID/version); future scheduled work is excluded until due. This follows the [serialization capability extension](identity-and-serialization.md).
- Fresh worker/pool subscriptions and capability sets, with configured capacity-share weights.
- Local ticket occupancy, execution duration and full ticket-holding duration, and completion/renewal latency. A ready-arrival estimator is deferred; producer enqueue rate is not its substitute.
- Known queue/type rate or concurrency saturation, outstanding unresolved completions, and thread-pool/host pressure.

Count backlog as an estimate; supported and due does not prove immediately admissible under every resource key. Unknown-capability backlog and paused queues are separate metrics. Do not full-count the jobs table on every admission. Sample/cache bounded queries; high-cardinality resource-level demand is not required in the first estimator.

For each backlog category, divide its estimated demand among fresh eligible pool subscriptions in proportion to their configured share weights. A category unsupported by a pool contributes zero to that pool. This avoids every replica independently treating the entire global backlog as its private demand. Presence lag can over/underestimate shares; it cannot weaken store admission checks. If no trustworthy share snapshot exists, hold a conservative local target rather than multiplying global backlog.

For pool i, the concrete D-07 refinement is:

```text
workBacklog_i = sum(allocated due backlog_k * mean service seconds_k)
desired_i = ceil((meanHealthyOccupiedTickets_i + workBacklog_i / DrainHorizon) / TargetUtilization)
targetCandidate_i = clamp(desired_i, MinConcurrency, MaxConcurrency)
```

Service estimates use an EWMA of completed attempt ticket durations under healthy storage, partitioned by job type where sample counts allow; include ordinary cleanup/ack cost. Do not learn enormous “service time” from an acknowledgement outage and scale up into it. Cold types use a configured conservative estimate with a low-confidence flag; long active durations provide a lower bound. Retries enter backlog when due; future retry delays are excluded. Growth requires positive due backlog. The defaults packet specifies observation windows, coverage/freshness and exact control rules; the previous arrival-rate term is deferred.

Rate saturation and shared-permit saturation can reduce estimated useful demand/headroom. For limits shared across many categories, treat this as a conservative heuristic, not an exact packing solution. If a queue has no immediate shared headroom, extra local reservations cannot improve it. If the estimator cannot distinguish blocked work, use the observed admission success/block ratio to suppress further scale-up.

### Stability rules

Proposed test profile: 5-second samples, three consecutive high-demand samples to grow, six low-demand samples to shrink, a 30-second change cooldown, and a maximum change of `max(1, ceil(currentTarget * 0.25))` per adjustment. These constants are benchmark inputs, not release defaults.

- Require a hysteresis band around current target; no adjustment for small estimate changes.
- Add per-process sample jitter so replicas do not change together on one boundary.
- Freeze increases during stale metrics, unresolved command buildup, renewal safety incidents, critical coordinator faults, or host pressure. Surface the reason.
- Decrease target by stopping refill, never by killing owned executions or revoking their tickets. Outstanding tickets may temporarily exceed the new target.
- If MinConcurrency=0, discovery, presence, and scaler loops remain alive and may raise target when trusted demand appears. With no fresh metrics, bootstrap at one ticket within MaxConcurrency, then require feedback before growth.
- A manual fixed target disables automatic target changes until its revisioned override expires or is removed. Record source, expiry, and expected revision. Shared capacity edits remain separate controls.

Record each applied target change with old/new values, input snapshot time, desired estimate, constraint reason, and configuration revision. Avoid writing a history row on every unchanged sample. Infrastructure replica scaling consumes metrics externally; Runwick does not launch containers.

## 6. Validation and acceptance gates

| Case | Required evidence |
|---|---|
| Stable 1:3 queue weights and 6:3:1 lanes | Deterministic simulation shows expected shares across complete cycles |
| Empty/blocked lane returns | No accumulated-credit burst; failed claims do not count as service |
| Aging with continuously busy high priority | Lower claimable lanes receive bounded selection opportunities; no physical priority mutation |
| Strict priority without aging | Documented starvation reproduced rather than falsely claiming fairness |
| Last rate grant across replicas | Only N Begin authorizations in each rolling window; no partial multi-scope charge |
| Rate rejection after reservation | Exact permits released, attempt closed, budget/abandonment unchanged, future eligibility set |
| Begin commit response lost | Replay neither charges nor starts twice |
| Handler failure / crash after Begin | Grant remains counted; no refund loophole |
| Multiple concurrency/rate scopes | Opposite input ordering cannot introduce a lock-order cycle |
| Policy window enlargement / cleanup race | Freeze prevents admission based on history that was already pruned |
| Backward database time | Logical rate time never reverses; no early refill |
| Replica count and capability mix change | Demand shares adapt; durable ceilings still hold during stale membership |
| Oscillating backlog and long jobs | Target changes obey sample/cooldown/step rules and never cancel work |
| Storage outage and pending finishes | No scale-up feedback loop or unbounded attempts |
| Zero minimum and missing metrics | Discovery survives and bounded bootstrap can recover |

Use deterministic selector/controller simulations plus real PostgreSQL tests with process barriers for admission. Benchmark one saturated queue at 1/3/6 replicas, grants per second, Begin latency, rejected reservation churn, rate-grant row/WAL growth, cleanup cost, and renewal deadlines while rate locks are busy. No runtime/database contract test or benchmark has run in this planning pass.

Planning sanity check: a standalone 10,000-selection arithmetic simulation produced 2,500/7,500 selections for weights 1:3 and 6,000/3,000/1,000 for 6:3:1 with a fixed eligible set. It does not validate dynamic membership, aging, storage failures, or the future implementation.

## 7. Implementation dependencies

1. Selector and scaler pure decision components with deterministic clocks/input snapshots.
2. Rate records, protocol-version checks, and atomic Begin/defer statements.
3. Worker handling for committed RateLimited replies and queue/scope cooldowns.
4. Shared telemetry/read-side queries and revisioned manual overrides.
5. Multi-replica/rate-history tests, then performance tuning.

The rolling-window algorithm, policy-edit freeze, and tuning constants remain proposals. They make the engineering decision reviewable without asserting performance that has not been measured. Recurring job creation is specified separately in [scheduling internals](scheduling-internals.md).
