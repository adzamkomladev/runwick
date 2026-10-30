# Performance acceptance — planning closure 5 of 6

Status: D-14 workload and acceptance baseline, 2026-10-29. These are **targets to test**, not benchmark results. The numbers close the initial planning decision and may change only in a recorded benchmark-driven revision with the old/new result visible.

## Reproducible environments

Primary reference: Linux x64, .NET 10 pinned SDK/runtime, Npgsql baseline, PostgreSQL 18 reference target, one worker with 4 vCPU/8 GiB and a separate database with 8 vCPU/16 GiB on local SSD/NVMe, same-network measured RTT at most 1 ms. Record OS/kernel, CPU model, container limits, disk/fsync configuration, provider/server versions and image digests, exact source commit, seed and load generator configuration. No performance numbers are portable without these fields.

Run correctness compatibility on PostgreSQL 16–18 and Windows x64 as already planned. SQLite qualification uses one host with local durable storage and real separate processes; Redis uses its selected AOF-always single-primary profile. Do not benchmark memory-only persistence and label it the production result. Source outbox and dashboard have separately recorded process/connection budgets.

No shared-resource CI runner is a release benchmark authority. CI runs deterministic/correctness and small regression checks; dedicated repeatable hosts produce performance acceptance reports. Build the harness at SP-003, record initial results at SP-010, then broaden with each component. SP-029 is final qualification, not the first performance measurement.

## Fixed workload set

| Workload | Parameters |
|---|---|
| P-01 ordinary async work | 1 KiB payload, fixed 16, 100 ms asynchronous handler, 100 arrivals/s, one queue |
| P-02 store overhead | No-op handler, fixed 16; ramp offered rate until saturation, report sustainable throughput and all latency percentiles |
| P-03 payload pressure | 64 KiB and 1 MiB payloads, reduced offered rate to keep ingress bytes explicit; verify limits and allocation cost |
| P-04 fairness/CPU | Queue weights 1:3, lanes 6:3:1, with/without aging; separate 100 ms CPU-bound handler run |
| P-05 distributed contention | 1/3/6 replicas, shared queue/type/resource ceilings, heterogeneous capability sets and one hot resource |
| P-06 rate/scheduler | 100 authorizations/second rolling window, 1/8 rate scopes; 1,000 minute schedules; 1,000-occurrence catch-up episode |
| P-07 dashboard/cleanup | 1 million terminal jobs/attempt summaries, 20 viewers polling every 5 s, cleanup active, concurrent P-01 |
| P-08 fault/recovery | Lost replies, process death at named boundaries, 60 s store outage, 1 s/5 s injected control latency and ignored cancellation |
| P-09 outbox | 100 source messages/s, four relay slots, 1 KiB intent; queue accepts before lost source mark; 7-day cutoff simulated with controlled time |
| P-10 soak | 24 h at 70% of measured sustainable rate, realistic terminal retention, periodic cleanup, retries, schedules and bounded failures |

Warm up 5 min; ordinary measured runs last 15 min, three repetitions. Drain all accepted work or account for its remaining durable state. Keep enough histogram samples for claimed percentiles; low-volume results report sample count and omit unsupported tail estimates. Report failures, throttles and offered load alongside successful throughput, never only successful-operation latency. Keep the raw measurement artifacts.

## Acceptance thresholds

| Property | Initial pass criterion |
|---|---|
| Correctness under every workload | Zero violated state/identity/ownership/atomicity invariants; no false durable-success reports |
| P-01 single-worker throughput | At least 100 confirmed finishes/s in steady state, with no sustained backlog growth |
| P-01 latency | Dispatch p95 ≤50 ms / p99 ≤200 ms; due-to-Begin p99 ≤2 s; pipeline-end-to-confirmed-Finish p99 ≤500 ms |
| Healthy control I/O | p99 ≤100 ms including local admission; zero lease-safety losses in declared healthy-load runs |
| P-04 fairness | Stable continuously claimable shares within 2 percentage points over ≥10,000 admissions with aging disabled; aging separately follows CAP vectors |
| P-05 efficiency | With unconstrained independent work, three replicas deliver ≥2× single-replica throughput before a measured shared bottleneck; hot-key runs report the bottleneck rather than claiming linearity |
| P-06 scheduler | Steady materialization lag p99 ≤3 s, distinct occurrence identities, bounded catch-up and no worker lease loss |
| P-07 management | Safe list queries p95 ≤250 ms / p99 ≤1 s; concurrent worker throughput degradation ≤20%; no unbounded exact-count scan |
| Instrumentation cost | Exporter absent vs bounded telemetry enabled: throughput degradation ≤10%, allocations/job increase ≤15% on P-01; include raw allocation totals |
| P-08 outage | Resource counts remain within declared caps; admission pauses; after restoration readiness recovers within 30 s absent a declared review/history fault |
| P-09 relay | At least 100 confirmed deliveries/s under the reference profile; p99 source-visible-ready to confirmation ≤2 s; lost marks never add a second delivery mapping |
| P-10 bounded growth | No growth proportional to total completed jobs in live attempt/task/coordinator caches; post-GC managed heap in last 6 h ≤110% of the first comparable steady 6 h with equivalent live inputs |

The 100 ms handler makes 100/s a plausible target under 16 slots, not evidence that the implementation achieves it. P-02's absolute maximum, CPU-bound throughput and write-amplification numbers are measurements, not invented guarantees. Record bytes/WAL/AOF, rows per job/grant/receipt, CPU, GC, connection count, control wait, oldest unsettled age and all heap/RSS data even when no absolute threshold is set. A flat cache count does not prove native memory is flat; investigate RSS growth separately.

Recovery tests may produce repeated handler execution under at-least-once semantics. They must reject stale mutations and preserve history, not pretend all external side effects are exactly once. Known-loss failover/restore is a documented discontinuity and cannot pass as seamless recovery. P-08 resource boundedness still applies when timers/providers do not return promptly.

## Failure disposition

Correctness failures always block the affected milestone. A performance miss produces a profile, trace and comparison against the baseline: optimize the measured path, rerun the relevant workload plus regressions, or submit an explicit threshold/profile revision with its consequence. Do not quietly lower persistence, remove required audit or replace exact rolling-window semantics to obtain a passing number. No blanket rerun of every scenario is needed for a documentation change.

SP-010 accepts the first PostgreSQL execution subset (P-01/P-02/P-08 with its available features); full v1 requires all advertised components/profiles and P-03–10. Performance qualification does not expand the six-item planning list.
