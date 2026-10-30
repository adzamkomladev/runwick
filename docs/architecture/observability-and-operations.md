# Observability and operational behavior

Status: engineering proposal, 2026-10-29. Instrument names, timings and acceptance cases are proposed; no telemetry, health probes or benchmarks have run. Extends the [worker](worker-engine-internals.md), [capacity](capacity-control.md), [scheduling](scheduling-internals.md) and [management](management-and-dashboard.md) designs.

## 1. Instrumentation ownership

Core emits through ActivitySource and Meter named Runwick, with a versioned instrumentation schema. Hosts choose listeners, exporters, sampling, log providers and endpoints. No exporter is required to dispatch, renew, finish or recover work. Keep dashboard queries and scaler inputs independent of exported telemetry availability.

Instrument semantic boundaries inside coordinators and driver adapters. A storage retry is not another job execution; a replayed receipt is not another state transition. Local counters are operational observations and can be lost on crash or observed again after recovery. Durable history/receipts answer correctness questions; metric totals are not billing, audit or exactly-once accounting.

Log stable event IDs, safe error codes and bounded structured fields. Correlation IDs may appear in access-controlled logs/traces; payloads, credentials, raw uniqueness/resource keys and SQL parameters do not. Default exception reporting records classification/type and a safe summary. Detailed messages/stack traces need an explicit bounded diagnostic policy because exception text can contain secrets. Apply sanitization before persistence and before export, not only when the UI renders it.

Observers must not participate in ownership decisions. Library-owned optional telemetry callbacks are guarded and bounded; exporters run through host-managed asynchronous pipelines. Since arbitrary in-process listeners can block or throw, test the chosen integration and avoid promising isolation from hostile application code. Do not invoke user exporters under storage locks or allocate an unbounded telemetry queue.

## 2. Traces and timing definitions

| Span/event | Boundary and interpretation |
|---|---|
| Dispatch | Prepare/send through confirmed acknowledgement or explicit unknown result; records Created/Reused separately |
| Staged dispatch | Records staging in caller transaction; does not claim acceptance before the caller's commit |
| Reserve / Begin | Internal storage operations; Begin authorization is the durable execution-budget/rate accounting point |
| Execute | One attempt's middleware/handler invocation; ends when the pipeline returns/throws, with logical outcome |
| Cleanup | Scope/attempt cleanup after invocation; separately bounded and measured |
| Settle | Outcome submission through confirmed Finish, definitive loss, or unresolved shutdown; includes acknowledgement retries |
| Renew / Recover | Independent bounded control operations with ownership result, not children tied to a disposed handler scope |
| Materialize schedule | Bounded occurrence/cursor command and result; separates recurrence computation from store time |
| Management command | Authorized action and durable command outcome; sensitive request bodies excluded |

For attempts, start a new processing span linked to validated dispatch trace context; retries get distinct attempt spans linked to the same origin, rather than keeping one span open over hours of backoff. Include job ID/generation/attempt ID only in traces/logs, subject to privacy policy. Child application spans may use the execution activity; renewal and completion use explicit captured correlation without inheriting a disposed DI scope. Batch operations link only a bounded number of contexts.

Handler success followed by unknown Finish must be visible as two different results: execution succeeded, settlement unresolved. Loss of authority cannot be labeled successful durable completion. A handler still running after local lease loss is reported as cancellation requested/lost authority with active local work, not as physically stopped.

Use monotonic elapsed time for local durations and the driver's authoritative instants for durable lifecycle decisions. Record reservation-to-Begin separately from invocation duration and invocation-end-to-settlement. Define scheduled delay as observation/materialization time minus due instant, and start delay as Begin minus eligibility; retries use their current eligibility, not original dispatch time. Account for clock uncertainty in cross-host diagnostic displays; no client clock grants a lease.

Validate incoming trace IDs/state and bound metadata size. Baggage propagation is deny-by-default with explicit allowlisted keys. Trace metadata never selects tenant, permission or execution authority. Producers cannot force the host's sampling/export policy through stored metadata.

OpenTelemetry messaging conventions distinguish processing and settlement, but the inspected messaging-span specification is marked Development. Use a documented Runwick schema and a pinned mapping where useful; do not claim automatic compatibility with all future convention revisions. [Messaging spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)

## 3. Metric surface and cardinality

Proposed instrument set; durations use seconds and counts use the indicated unit:

| Instrument | Kind / unit | Meaning |
|---|---|---|
| runwick.dispatch.operations | Counter / operation | Client-observed dispatch outcomes; created/reused/unknown/rejected are distinct |
| runwick.execution.authorized | Counter / execution | Newly observed successful Begin transitions; known receipt replays excluded |
| runwick.execution.duration | Histogram / s | Middleware/handler invocation duration, excluding waiting and Finish |
| runwick.cleanup.duration | Histogram / s | Attempt cleanup duration |
| runwick.settlement.duration | Histogram / s | Invocation end to confirmed Finish or definitive loss; result dimension distinguishes them |
| runwick.storage.operation.duration | Histogram / s | Driver call latency by bounded operation/result |
| runwick.lease.renewals | Counter / renewal | Observed renewed/stale/error results |
| runwick.lease.margin | Histogram / s | Conservative remaining local safety margin when renewal result is received |
| runwick.worker.tickets | Observable gauge / ticket | Local outstanding tickets by phase, including completion/cleanup |
| runwick.worker.target | Observable gauge / ticket | Local configured/controller target, not free slots |
| runwick.worker.oldest_unsettled.age | Observable gauge / s | Local age of oldest outstanding settlement |
| runwick.schedule.materialization.delay | Histogram / s | Due-to-materialization delay for observed occurrences |
| runwick.scaling.decisions | Counter / decision | Increase/decrease/hold by bounded reason code |
| runwick.telemetry.dropped | Counter / record | Drops in library-owned bounded diagnostic buffers, where such buffers exist |

Allowed dimensions are finite driver/operation/result/reason codes, configured pool names and optionally bounded registered queue/job names. Dynamically created queues/types fall into an overflow grouping when the configured series budget is reached. No job ID, attempt ID, command ID, tenant ID, arbitrary resource key, exception message or URL as a metric label. Cap combinations as well as individual labels; avoid constructing a series cache before applying the bound.

Publish recommended SDK views and limits, and verify support in the selected .NET SDK version. OTel specifies attribute filtering, cardinality limits and an overflow aggregation, which complement rather than replace the library's bounded tag vocabulary. [Metrics SDK cardinality](https://opentelemetry.io/docs/specs/otel/metrics/sdk/)

Observable callbacks read bounded in-memory snapshots only: no database calls, locks held by handlers or synchronous network I/O. Background samplers use deadlines, indexed bounded queries and backoff. Expose freshness; a failed read does not mean queue depth zero. Metrics/export failure must not reset the scaler's independent observation state.

## 4. Local versus application-wide statistics

Per-process tickets, targets and execution observations can be aggregated with their process/resource identity understood. Application-wide depth must not be emitted by every worker and then summed: that multiplies the same backlog by replica count.

Proposed default: workers export local metrics; dashboard queries return authorized application/queue snapshots with observedAt and coverage. Optional global metric export uses a designated management collector per application, configured separately from worker replicas. Automatic collector failover/election is deferred unless required; collector loss means missing metrics, not execution failure. If operators deploy duplicate collectors, dashboards must select/deduplicate their global series rather than sum them.

Separate these queue measures:

- Due waiting count: due pending/retrying work, independent of whether a compatible worker can start it.
- Compatible candidate estimate: due work matching a specified capability set; distributed eligibility can change immediately.
- Oldest due age: backlog age, not a promise the job could pass all permit/rate checks.
- Deferred/blocked observations: explicit sampled reasons such as rate, capacity, pause or unsupported type/version.

Do not label a snapshot an exact runnable count when dynamic admission has not been executed. Counts may be approximate where documented; lists and command results retain their stronger semantics. Redis needs explicit query indexes and bounded scans, SQLite short readers, and PostgreSQL indexed queries. No whole-history scan per scrape.

## 5. Health and readiness by role

| Signal | Meaning | Response to store outage |
|---|---|---|
| Liveness | Process/supervisor makes local progress | Remains live if loops are handling the outage; avoid restart storms |
| Producer readiness | Required schema/profile compatible and dispatch dependency currently usable | Unready; report unknown in-flight dispatch separately |
| Worker readiness | Engine initialized, ownership coordinators functioning, compatible storage and admission enabled | Stop admission; renew/settle/recover within existing safety rules |
| Scheduler readiness | Evaluator/zone versions compatible, cursor work can be persisted | Unready; resume bounded misfire handling when restored |
| Management readiness | Auth/policy/antiforgery configuration and store query/control dependency usable | Readiness degraded/unready; cached views explicitly stale and mutations unavailable |

During drain, worker readiness is false while liveness remains true. Zero backlog or a paused queue is not a process failure. Unsupported jobs are a workload compatibility diagnostic; they need not make a healthy worker for other queues unready. A recovering Redis partition or incompatible persistence profile must not be shown as ready just because PING succeeds.

Health checks read recent coordinator state or perform tightly bounded dependency checks; they do not reserve a job. Public probes return minimal status. Protected diagnostics may show component state, version compatibility and profile, never connection strings or secret configuration. A process stalled enough to prevent heartbeat progress is distinct from an unavailable store; detection and restart policy belong to the host.

## 6. Operational procedures

| Situation | Required behavior |
|---|---|
| Graceful deploy/drain | Stop new admission; preserve renewals and bounded finalization; report outstanding work; shutdown budget does not authorize force-releasing a live owner's permits |
| Unknown dispatch/management/Finish | Resolve original operation identity and fingerprint; do not rerun a successful handler merely because its acknowledgement was uncertain |
| Lease loss | Cancel cooperatively, latch authority loss, keep local work visible until it ends; stale owner cannot finish or release replacement ownership |
| Sustained store outage | Back off bounded loops; retain completion/local-ticket limits; surface unresolved work and safety margin; telemetry failure adds no new retry mechanism |
| Schema upgrade | Deployment-controlled migration and compatibility gates; stop incompatible new admission while safe supported renewal/finalization remains possible; never auto-migrate from a dashboard request |
| Restore/lossy failover | Quiesce old writers, establish history continuity and reconcile external effects/fences; follow the driver restore boundary before resuming |
| Retention cleanup | Bounded batches with resumable cursor, dependency-aware eligibility, progress and failure records; preserve live correctness obligations |
| Schedule backlog | Show misfire episode/cutoff, catch-up budget and needs-review state; do not silently discard excess occurrences |

Upgrade compatibility requires an explicit version matrix for readers, writers, schema and command codecs; this stage identifies the operational obligation, not a completed rolling-upgrade protocol. Include that matrix and migration failure tests in the implementation backlog.

Alerts should distinguish actionable conditions: sustained low lease margin or lost authority; old unresolved completions; rising due age with unsupported capability/rate/capacity reason; repeated failures by stable error class; scheduler lag/needs-review; retention failures; and persistence-profile mismatch. Ordinary full utilization is contextual evidence, not automatically an incident. Thresholds require workload measurements and are not release defaults yet.

Maintain separate retention budgets for payloads, attempt details, receipts, fixed-window identities, active uniqueness, schedule occurrence history, audit, rate grants and fences. Some can expire only after their correctness dependency ends; a user shortening history retention cannot erase a live deduplication window or reset a fence. Link diagnostics to the [management audit policy](management-and-dashboard.md), but keep security-audit durability distinct from best-effort telemetry.

## 7. Acceptance cases

| ID | Required proof |
|---|---|
| OBS-01 | Dispatch/worker/scheduler work with no listener/exporter; unavailable exporters do not delay leases or Finish |
| OBS-02 | Handler success plus uncertain Finish shows separate execution and settlement results; same-command retries do not become new executions |
| OBS-03 | Millions of varying IDs/resource keys produce bounded series/cache memory; supported SDK overflow behavior is verified |
| OBS-04 | Observable callbacks make no database/network calls and remain bounded during store outage |
| OBS-05 | Separate attempts/requests do not inherit each other's activities, scopes, baggage or sensitive fields |
| OBS-06 | Duplicate global collectors cannot silently multiply dashboard depth; stale snapshots remain stale rather than becoming zero |
| OBS-07 | Drain/outage/unsupported payload/profile mismatch produce the specified per-role readiness while preserving liveness semantics |
| OBS-08 | Crash between durable transition and observation demonstrates metric loss without lost durable history; receipt replay avoids known duplicate observations |
| OBS-09 | Exception/payload/credential fixtures stay out of default persistence/logs/traces/props; privileged detail follows reveal policy |
| OBS-10 | Retention cannot delete live receipts, pending recovery intents, uniqueness identities, rate obligations or required fences |
| OBS-11 | Telemetry-enabled versus disabled representative load measures allocation/latency overhead and renewal margin; targets set from the workload |

These are planned tests, not results. The [implementation backlog](../implementation-backlog.md) and [acceptance checklist](../architecture-acceptance.md) now track this work, unresolved defaults, dependency versions, Redis feasibility and migration compatibility. The [final architecture baseline](../architecture-baseline.md) completes planning and fixes the implementation sequence. Runtime implementation follows architecture review.
