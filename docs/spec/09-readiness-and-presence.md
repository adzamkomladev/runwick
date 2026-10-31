# 09 — Readiness evidence, presence and the unsupported-backlog diagnostic (SP-012)

**Sources:** [worker-engine-internals.md](../architecture/worker-engine-internals.md) §4–§5, §11; [capacity-defaults-and-budgets.md](../architecture/capacity-defaults-and-budgets.md) §2, §4; [capacity-control.md](../architecture/capacity-control.md) (CapacityBlocked classification); [postgres-schema-and-operations.md](../architecture/postgres-schema-and-operations.md) §3, §5, §6; [observability-and-operations.md](../architecture/observability-and-operations.md) §4; [driver-contract-suite.md](../architecture/driver-contract-suite.md) (OBS-01, CAP-01 … CAP-04, WRK-15, WRK-19, PGX-07); [performance-acceptance-plan.md](../architecture/performance-acceptance-plan.md) (P-04, P-05, G1-M1, G1-M3); 04 §7.3–§7.4, §7.12; 05 §7–§8, §14, §17; 06 §1.5, §1.8.

## Changes (2026-10-31, SP-012)

New document. It extends 05 §7 (readiness) and §14 (presence) and 04 §7.4 (probe SQL).

Contradictions and gaps resolved:

| # | Conflict or gap | Resolution |
|---|---|---|
| C-1 | 00 §3 lists aging and cooldowns as M3; 05 §8.5–§8.6 and the M2 engine already ship them | 00 corrected. SP-012 adds evidence from the probe, not the mechanisms |
| C-2 | 05 §7.1 probe SQL uses `statement_timestamp()` and `NOT EXISTS paused` (unregistered queues included); 04 §7.4 and the code use `clock_timestamp()` and an inner join on unpaused `queue_controls` (unregistered queues excluded) | 04 wins: an unregistered queue cannot hold jobs (Dispatch rejects it). §2 restates the one form |
| C-3 | The plan reserves capability `readiness_v2`; the read gate already requires the exact schema revision (04 §7.4 `CheckGate`) | Not added. Probe/2 is read-only and no binary below revision 5 can read a revision-5 store, so there is no mixed-version window to guard |
| C-4 | The manifest builder writes version `[1]` for every operation | Additions may name a version (`ReadinessProbe@2`); the manifest lists every accepted version (`"ReadinessProbe":[1,2]`) |
| C-5 | 10 §5 hands "queue/scope cooldowns on deferral" to SP-012; the plan's deliver list omits it | No cooldown on a rate deferral. The deferred job's `eligible_at` moves to the hint, so it is no longer due and the probe stops reporting it; other jobs of the lane are unaffected. Cooling on rate scopes would suppress unrelated work, which capacity-defaults §2 forbids |
| C-6 | `BlockedScopeId` (04 §7.3) is the first blocked scope in UUID order, so a full queue scope can hide behind a type scope | Unchanged in Reserve. Queue saturation now comes from the probe (§2), so classification no longer depends on which scope Reserve reports first |
| C-7 | The record inventory names `worker_subscriptions`; M2 keeps subscriptions in a jsonb column | `0005_readiness` adds the join table (§3). The jsonb column stays for display |
| C-8 | A failed first presence publication was swallowed and readiness ignored `Presence.Failing`, against 05 §14 / §17 | Startup retries the first publication 3 times; persistent failure opens admission with readiness `Degraded` (§3.2) |

Implementation choices *[choice]*: saturation is counted live per probe, bounded by the scope capacity, with no durable "full scope" table (worker-engine-internals §5 defers it until measured); the diagnostic samples 256 due jobs per lane by default.

## 1. Rules

1. The probe is a hint. Reserve, with its locks and checks, stays the only admission authority. A stale hint can cost a `NoWork` or `CapacityBlocked` reply, never a wrong reservation.
2. Evidence suppresses only what it is about: a saturated queue scope excludes that queue's lanes; nothing about one queue delays another.
3. Notifications and wakes coalesce and never erase evidence. Only a relevant control revision or a known local release rechecks early, and still within probe spacing.
4. Presence is observational. Its expiry or deletion never changes a lease, attempt, permit or recovery decision.
5. "No recently advertised capable worker" is a statement about advertisements in the last presence window, not proof that no worker could run the job.

## 2. ReadinessProbe/2

Same request as /1: application, lanes (≤ 384), supported contract tuples (1 – 512). One round trip, no locks, no payload, no unbounded count. The reply (`ReadySnapshot`) has:

| Member | Meaning |
|---|---|
| `ObservedAt` | Store time (`clock_timestamp()` under the gate row's share lock) |
| `Lanes` | Per unpaused registered lane, the oldest due job whose tuple is supported (as /1) |
| `UnsupportedLanes` | Lanes whose oldest due job has a tuple outside the request (one more `LIMIT 1` lookup on `jobs_ready`) |
| `Queues` | Per requested queue: `Paused`, `ControlRevision`, the queue scope's ID and `Capacity`, and `Live` = unexpired permits counted `LIMIT capacity`; `Saturated = Live ≥ Capacity` |

The three row kinds come from one statement (`UNION ALL` under the gate CTE). The permit count uses `permits_live_count`; the scope lookup uses `concurrency_scopes_natural_uq`. A driver without probe/2 (and the scripted test store) returns empty `Queues` and `UnsupportedLanes`, which the engine reads as "no evidence".

## 3. Presence

### 3.1 Records (`0005_readiness`, revision 5)

`worker_subscriptions(application, instance_id, queue)`, PK on all three, FK to `worker_instances` `ON DELETE CASCADE`, index `(application, queue, instance_id)`. `PublishPresence/1` inserts the instance's queues with `ON CONFLICT DO NOTHING`: an instance ID is never reused and a process's subscriptions are fixed for its lifetime, so insert-if-absent is complete. No lease, attempt or permit row references presence. Pruning expired rows (24 h after expiry) is SP-021.

### 3.2 Engine

Cadence 5 s ± 10 %, expiry 20 s in store time (unchanged). The first publication precedes admission; it is retried up to 3 times with the control backoff. While publication fails, readiness is `Degraded` (05 §17) and the presence status reported on the next success is `degraded`; ownership, renewal and recovery are unaffected.

## 4. Engine use of the evidence

### 4.1 Probe spacing

At most one probe in flight per polling group (unchanged), and probe starts are ≥ 50 ms apart whatever requested them: invalidations, wakes and hint expiry coalesce into the next spaced probe.

### 4.2 Saturation and early recheck

Per pool, once per new snapshot:

- every queue with `Saturated` (and not paused) is excluded from lane selection for the snapshot's life (≤ 2 s hint age). No Reserve is sent for its lanes and cooldown steps are not escalated; `SaturationSkips` counts the exclusions;
- a queue whose `ControlRevision` differs from the last one this pool saw has its queue and lane cooldowns cleared (a relevant control change: pause/resume, scope edit through the queue control);
- when every remaining lane is excluded, the snapshot is invalidated and the pool backs off as when idle (100 ms → 2 s ± 20 %).

A local attempt that retires is a known release: its queue's exclusion is lifted at once, before the next probe. Type and resource blocking keep the M2 rules (05 §8.6): a single-type lane cools the lane, anything else is bounded by 8 blocked candidates per visit and 32 Reserves per round.

### 4.3 Polling repair

M3 has no LISTEN (the decision waits for G1-M1, plan Q6). Every wake source (ticket refill, admission change, pressure resume) is an optimisation: with every signal dropped, polling alone finds due work, pause/resume and recovery work within the idle backoff cap plus the hint age plus one probe and one claim (driver-contract-suite:OBS-01). Recovery is timer-driven and never waits on a wake (05 §13).

## 5. ListUnsupportedBacklog/1

Internal, preview (`IReadinessDiagnostics`; SP-020 exposes it). Read-only, background lane, no receipt. For every registered queue and priority it samples up to `samplePerLane` (default 256, 1 – 1,000) due jobs in `jobs_ready` order, groups them by tuple, and returns the tuples for which no live worker (unexpired, status `starting`, `ready` or `degraded`) both advertises the tuple at its active capability revision and subscribes to the queue:

| Reason | When |
|---|---|
| `no_live_worker_advertises_tuple` | No live worker advertises the tuple |
| `advertised_but_not_subscribed` | Some live worker advertises it, none subscribes to the queue |

Each entry carries the lane, tuple, oldest sampled `eligible_at`, the sampled count and `Truncated` (the lane's sample was full, so the count is a lower bound). The reply carries `ObservedAt` and `LiveWorkers`; an empty list with `LiveWorkers = 0` is not "all clear". No type-oriented index is added: the PGX-07 plans (§6) decide whether one is needed.

## 6. Cases

| Case | Test (`ReadinessTests`) | Proves |
|---|---|---|
| RDY-01, WRK-15 | `A_saturated_queue_sends_no_reserves_and_writes_nothing_per_job` | A queue scope of 2 held by hung handlers over a 500-job backlog: over 10 s, zero Reserve commands and receipts, zero events, probes ≤ about one per 1.6 s; the backlog drains once released. The 10,000-job scale runs in the workload (§7) |
| CAP-04 | `A_saturated_queue_does_not_delay_another_queue_and_a_control_change_rechecks_at_once` | A saturated queue does not delay an unrelated queue's job; 5,000 wakes and invalidations keep probes ≥ 50 ms apart |
| RDY-03 | `Unsupported_backlog_stays_visible_and_is_diagnosed_without_claims` | An unadvertised tuple and an advertised but unsubscribed queue: never reserved, `UnsupportedLanes` set, both diagnostic reasons, a later supported job still runs, readiness not unhealthy |
| RDY-02, WRK-19 | `Expired_or_pruned_presence_changes_no_ownership_and_renewals_continue` | Presence expired and then deleted while a job runs: renewals continue, ownership unchanged, no Recover, readiness `Degraded` while publication fails, the job settles |
| driver-contract-suite:OBS-01 | `With_every_wake_dropped_polling_finds_work_controls_and_recovery` | Wakes suppressed: another process's job, pause then resume, and an abandoned reservation's recovery are all found by polling |
| MODE presence repeat | `Each_worker_role_publishes_its_subscriptions_and_capabilities_and_producers_publish_none` | Exact subscription and capability rows for the worker; none for a producer-only client; stopping status after stop |

Owed before SP-012 acceptance (not yet written): CAP-01 fixed-mode remainder and CAP-02/CAP-03 real-store halves (the G-1 case table lists them without `Case` traits, 15 §5 note), the RDY-01 type-scope half at 10,000 jobs, PGX-07 `EXPLAIN (ANALYZE, BUFFERS)` plans at 10⁵ / 10⁶ rows for probe/2, Reserve and the diagnostic, MODE-01 … MODE-05 with peer-process workers, and the measurements G1-M1 / G1-M3 rerun, P-04 and P-05 at 1/3/6 replicas.

## 7. Must not claim

- strict global fairness across replicas, or FIFO;
- that presence grants or revokes ownership;
- that "no capable worker" proves no worker could run a job;
- that the probe's saturation flag is exact at Reserve time.
