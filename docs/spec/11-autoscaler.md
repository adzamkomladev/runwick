# 11 — Local autoscaler (SP-014)

**Sources:** [capacity-control.md](../architecture/capacity-control.md) §5; [capacity-defaults-and-budgets.md](../architecture/capacity-defaults-and-budgets.md) §4–§5, §7 (CAP-06 … CAP-08, CAP-12); [driver-contract-suite.md](../architecture/driver-contract-suite.md) (SCL-01, WRK-02); 05 §4 (pools, tickets), §9; 06 V-15; 08 (actor, audit, events); 09 §3 (saturation evidence, presence).

## Changes (2026-10-31, SP-014)

New document. It lifts the M2 refusal of `AutoConcurrency` (06 V-15).

Contradictions and gaps resolved:

| # | Conflict or gap | Resolution |
|---|---|---|
| C-1 | capacity-control §5 says a pool "scales to demand" without saying whose demand when several workers subscribe to a queue | Demand is shared: each (queue, tuple) backlog is divided by the live workers that advertise the tuple **and** subscribe to the queue, then by queue weight among this worker's pools on that queue (§4.2). No coordinator, no leader |
| C-2 | capacity-defaults §4 allows a minimum of zero but also requires "one ticket to discover work" | Min 0 starts at one bootstrap ticket and drops to zero only on a trusted idle estimate (§3.4) |
| C-3 | The hysteresis band can stop a pool one ticket short of its bound forever (R4) | Boundary waiver: a candidate equal to max (or min) applies even inside the band (§3.3) |
| C-4 | Overrides: "per worker" or "per pool"? | Per worker incarnation and pool `(instance_id, pool_name)`, revisioned, optional expiry ≤ 7 days (§5). A new incarnation starts with no override |
| C-5 | "Unknown demand" was unspecified | A late, failed, maintenance or missing estimate is unknown; unknown holds every target and resets evidence. It is never read as zero (§4.3) |

Implementation choices *[choice]*:

1. The controller (`PoolScaler`) is pure: time and inputs are supplied by the engine loop, which makes the traces testable without a clock.
2. A target reduction only stops refill. Tickets in use are never revoked; outstanding may exceed the target until holders retire.
3. History rows are diagnostic: a lost `RecordScalingEvent` write never changes the applied target.
4. `SetPoolOverride/1` is an administrative operation with the usual receipt, event and audit (08). `EstimateDemand` and `RecordScalingEvent` are worker-internal, like `PublishPresence`.

## 1. Contract

An `AutoConcurrency(min, max)` pool changes its local ticket target between `min` and `max` from local occupancy and a shared estimate of due backlog. It never touches ownership, leases, admission or other workers. A failing scaler freezes targets; it is not a critical loop.

## 2. Registration

| Rule | Value |
|---|---|
| Bounds | `0 ≤ min ≤ max`, `max ≥ 1` |
| Fixed and Auto together | refused (`invalid_pool`, V-15) |
| Initial target | `min`, or 1 when `min = 0` |
| Worker group ticket sum | uses each pool's `max` |

## 3. Controller

### 3.1 Inputs (every 5 s ± 10 %)

| Input | Source |
|---|---|
| Mean occupied tickets | time-weighted mean of outstanding tickets over the last 30 s |
| Queued work seconds | Σ allocated due rows × service estimate (§4.2); null when unknown |
| Positive backlog | at least one allocated due row |
| Growth freeze | unresolved command, pressure stop, admission closed, presence failing, or ≥ 75 % of ≥ 20 admission cycles blocked or deferred in 30 s |
| Override | the instance's unexpired override for the pool |
| Membership | digest of live worker ids |

Service estimate per job type: EWMA (α 0.20) of healthy completed ticket durations (Reserve sent → retire), cold 1 s until the first sample, low confidence below 20 samples; the longest live healthy ticket is a lower bound. Lost, pressure-stopped or unbegun attempts teach nothing.

### 3.2 Desired

`desired = ceil((meanOccupied + queuedWork / 30 s) / 0.80)`, `candidate = clamp(desired, min, max)`.

### 3.3 Decision

| Rule | Value |
|---|---|
| Band | `max(1, ceil(0.20 · current))`; grow when `candidate ≥ current + band`, shrink when `candidate ≤ current − band` |
| Boundary waiver | `candidate = max > current` grows; `candidate = min < current` shrinks, even inside the band |
| Evidence | 3 consecutive growth samples; 6 consecutive shrink samples; any other sample resets |
| Cooldown | 30 s between applied changes; evidence keeps accumulating |
| Step | `max(1, ceil(0.25 · current))`, never past the candidate |
| No due backlog | growth holds (occupancy alone never grows) |
| Freeze | growth holds and evidence resets; shrinking continues |
| Membership change | evidence resets |

Example (min 4, max 16, sustained demand): 4 → 5 → 7 → 9 → 12 → 15 → 16, one change per 30 s; idle again: 16 → 12 → 9 → 6 → 4.

### 3.4 Bootstrap

A `min = 0` pool starts at 1. The first trusted estimate ends the bootstrap: zero candidate, no due backlog and no occupancy drops it to 0 at once; otherwise normal rules apply. Unknown demand keeps the bootstrap ticket.

### 3.5 Applying a target

The engine sets the pool gate target and wakes the pool. A reduction stops refill only: no ticket, attempt or lease is cancelled, released or shortened.

## 4. Demand estimate (`EstimateDemand`, worker-internal)

### 4.1 Read

One read-only statement behind the store gate, deadline 1 s, never overlapping. Per subscribed lane (queue × priority), at most 2,000 due rows (`pending`/`retrying`, `eligible_at ≤ now`) in `jobs_ready` order, grouped by supported tuple. A lane that hits the cap sets `Truncated` (its rows are lower bounds). Also returned: live worker count, membership digest, capable-instance count per (queue, tuple), and this instance's unexpired overrides.

### 4.2 Allocation

For pool P and queue q with weight w: `share = w / Σ weights of this worker's pools on q`. Each row contributes `due × share / max(1, capableInstances) × serviceSeconds`. Queues known saturated (09 §3) contribute nothing.

### 4.3 Unknown

Failure, timeout, maintenance gate or no `IScalingStore`: unknown. Unknown holds and resets evidence. An empty successful estimate is known zero.

## 5. Overrides (`SetPoolOverride/1`, migration `0007_scaling`)

`pool_controls(application, instance_id, pool_name)`: `target` (0 – 10,000 or null), `revision`, `expires_at`, `reason`, `updated_at`, `updated_by`. No foreign key: an override for an expired incarnation is inert.

| Field | Rule |
|---|---|
| `expectedRevision` | 0 = no row; mismatch → `VersionConflict` / `revision_conflict` with the actual revision |
| `target` | null clears; 0 is allowed (stop refill) |
| `expiresAt` | only with a target; future; ≤ now + 7 days; floored to ms |
| Same target and expiry | `InvalidState` / `no_change` (an expired row reads as absent) |
| Applied | revision + 1, event `pool_override_set`, audit `SetPoolOverride` / target kind `pool` / id `instance/pool` |

Administrative class: default `NotAfter` 5 min, cap 10 min, receipt and replay as 03 §4.

In the controller, an override applies at once (clamped to max, never to min), resets evidence and stops automatic changes. Removal or expiry resumes from `clamp(current, min, max)`.

## 6. History (`scaling_events`)

One row per applied target change: instance, pool, old and new target, desired, candidate, reason, snapshot time, membership digest. Unchanged samples write nothing. Retention follows the event retention job (owed, 06 §1.13).

## 7. Cases

| Case | Test |
|---|---|
| SCL-01 | `ScalerTests.Sustained_demand_grows_…`, `…_shrinks_…`, `Changes_are_at_least_one_cooldown_apart_…` |
| CAP-06 | `ScalerTests.The_service_estimate_…`, `Occupancy_is_a_time_weighted_mean_…` |
| CAP-07 | `ScalerTests` band, waiver, freeze, unknown, bootstrap, override, membership; `ScalingTests.Pool_overrides_are_revisioned_…`, `Unchanged_targets_are_never_recorded_…` |
| CAP-08 | `ScalerTests.The_blocked_ratio_…`; `ScalingTests.The_estimate_shares_backlog_…` |
| WRK-02 / CAP-12 | `ScalingTests.The_estimate_shares_backlog_and_an_override_to_zero_cancels_nothing` |

Owed: multi-worker share test (two hosts, one queue); truncation at 2,000 per lane; estimate deadline fault; scaling-event retention.

## 8. Must not claim

- Cluster-wide or coordinated scaling; any worker count or process scaling.
- Scaling on CPU, memory or latency.
- That a reduction frees capacity before holders retire.
- That the constants (§3) are tuned release values; they are benchmark inputs.
