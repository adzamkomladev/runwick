# 18 — Telemetry, health and operations signals (SP-022)

**Sources:** [observability-and-operations.md](../architecture/observability-and-operations.md) (primary: ownership, spans, metrics, local vs global, health, alerts, OBS-01–11), [management-retention-defaults.md](../architecture/management-retention-defaults.md) (1,000-series cap, 1,024-record buffers, no I/O in callbacks), [00-overview.md](00-overview.md) §6 and §9 (packages, honesty rules 4, 5, 9), [02-serialization-and-wire.md](02-serialization-and-wire.md) IC-07 (diagnostic metadata), [05-worker-engine.md](05-worker-engine.md) §15–§17 (pressure, shutdown, health), [09-readiness-and-presence.md](09-readiness-and-presence.md), [13-scheduler.md](13-scheduler.md), [16-management.md](16-management.md), [17-retention.md](17-retention.md), [v1-delivery-plan.md](../v1-delivery-plan.md) §6 (SP-022).

## In short

SP-022 makes Runwick observable with the standard .NET tools, without making it depend on any of them.

- **Core emits, the host decides.** The `Runwick` package publishes traces through one `ActivitySource` and metrics through one `Meter`, both named `Runwick`, and logs through `ILogger` with fixed event IDs. Nothing is exported unless the host adds a listener or exporter. Jobs run, renew and finish the same way with no listener, with a slow one, or with a broken one.
- **Fixed vocabulary.** This document freezes the span names, metric names, units and allowed attributes. Attributes come only from small fixed sets (operation names, result codes, registered queue and job names, pool names). Job IDs, keys, error messages and URLs never become metric attributes. Each metric has at most 1,000 attribute combinations; beyond that, measurements go to one "overflow" series.
- **Honest numbers.** A handler that succeeded and a Finish that was not confirmed are shown as two different results. A replayed command is not counted again. Application-wide numbers (queue depth, oldest waiting job) are measured by one designated collector, not by every worker, so they are not multiplied by the number of replicas. When a measurement is stale, it is left out — never reported as zero.
- **No secrets.** Payloads, credentials, keys, reasons typed by operators and exception text stay out of spans, metrics and logs by default. Exception text can be turned on explicitly, bounded and sanitized.
- **Health per role.** Producer, worker, scheduler and management each get a readiness check; there is one liveness check. `Runwick.AspNetCore` registers them with ASP.NET Core health checks.
- **`Runwick.OpenTelemetry`** (new package) gives `AddRunwickInstrumentation()` for OpenTelemetry users: it subscribes to the source and meter, sets the cardinality limits and filters attributes. It contains no exporter.
- **Shipped operations assets:** a Grafana dashboard and Prometheus alert rules over these metrics.

Not in SP-022: exporters, a hosted metrics backend, collector failover or election, tail sampling, log shipping, and alert thresholds tuned to a measured workload (the shipped thresholds are starting points, §9).

## Changes

- (2026-11-08) New document.
- (2026-11-08) Contradiction: `eng/package-graph.json` lists `Runwick.OpenTelemetry` with no allowed packages and milestone M5; 00 §6 says "OpenTelemetry SDK", M4. 00 wins: the package may reference `OpenTelemetry` (the SDK package, no exporter); the graph file is updated with the pin when the package is created.
- (2026-11-08) The architecture's `runwick.cleanup.duration` (attempt-scope cleanup) is renamed `runwick.attempt.cleanup.duration` so it is not confused with retention cleanup (17).
- (2026-11-08) SP-022 re-pin: semantic conventions 1.37.0 → 1.44.0 (1.45.0 was published 2026-11-02, under two weeks old); the messaging attributes Runwick uses are unchanged and still Development. OpenTelemetry SDK pinned at 1.20.1 (and `OpenTelemetry.Exporter.InMemory` 1.20.1 for tests).
- (2026-11-08) Listener exceptions are now caught at the instrument and span call instead of passing through: a throwing listener reached Begin, Finish and drain paths in OBS-01 (c). Failures are counted and logged once at Debug.
- (2026-11-08) `runwick.execution.authorized` and `runwick.execution.start_delay` count once per attempt, including when the Begin receipt was replayed in the same process (OBS-02). A replay after a restart is a new process with a new attempt; OBS-08 checks it.
- (2026-11-08) Alert `RunwickStoreIncompatible` reads a host-provided `runwick_host_readiness_incompatible` gauge (health is not a Runwick metric); `RunwickFailuresRepeated` groups by queue and job name with `runwick_outcome != "succeeded"`, because `error.type` is not a metric attribute.
- (2026-11-08) New option `RunwickTelemetryOptions.CollectorName` (default `default`) gives the `runwick.collector` value of §4.
- (2026-11-08) The worker's free-text diagnostics (`Diagnose`) are logged at Debug; the fixed event IDs of §5 carry the operator-facing records.
- (2026-11-08, SP-022 close-out) Where the remaining §5 events come from: 1014 is logged by the application collector, at most once a minute and once per new (queue, job, version) tuple, at most 16 per pass; 1201 is a scheduler evaluation that failed locally, 1202 a failure the store recorded (Applied); 1203's `skipped` is the number of misfire ranges, not occurrences; 1402's `targetKind` is the permission area (e.g. `jobs`), not a resource type; 1702/1703 `actual` is the store's sanitized message (≤ 256 characters), and 1702 is chosen when that message names the profile. The outbox relay keeps its logger category `Runwick.Outbox.OutboxRelay`.
- (2026-11-08, SP-022 close-out) `runwick.storage.operation.duration` is recorded around each driver call including its retries (PostgreSQL and SQLite runners); `io_class` comes from the call's lane. Transactional staging is not timed by this instrument, because it runs inside the caller's transaction; it is covered by the `stage {queue}` span. New case TEL-03 checks both on PostgreSQL.
- (2026-11-08, SP-022 close-out) OBS-11 records traces and metrics with plain `ActivityListener`/`MeterListener` instances (with a 1 s collection timer) standing in for the SDK's in-memory exporter, since the Postgres test project does not reference the SDK. The 90 % bar is enforced only when `RUNWICK_OBS11_ENFORCE=1`, on a quiet reference host; elsewhere the numbers are recorded.

## 1. Sources, scope and options

```csharp
public static class RunwickTelemetry
{
    public const string SourceName = "Runwick";      // ActivitySource and Meter
    public const int SchemaVersion = 1;               // this document's tables
}

public sealed class RunwickTelemetryOptions          // services.AddRunwick(r => r.Telemetry(t => …))
{
    public bool RecordExceptionDetails { get; set; }          // default false (§6)
    public int MaxSeriesPerInstrument { get; set; } = 1_000;  // 100 – 10,000
    public bool CollectApplicationMetrics { get; set; }       // default false: the global collector (§4)
    public TimeSpan ApplicationSamplePeriod { get; set; } = TimeSpan.FromSeconds(15); // 5 s – 5 min
}
```

- `ActivitySource` and `Meter` are created once per process with name `Runwick`, version = the `Runwick` assembly informational version, the scope tag `runwick.telemetry.schema = 1`, and `TelemetrySchemaUrl = https://opentelemetry.io/schemas/1.44.0` (the pinned conventions, §2.3).
- Core uses only `System.Diagnostics` and `Microsoft.Extensions.Logging.Abstractions`; no package is added to `Runwick`. Histogram bucket boundaries are given through `InstrumentAdvice<double>` in core, so they apply with or without the OpenTelemetry SDK.
- **Isolation from the hot path.** No instrument call happens while a storage lock or a database transaction is open in the caller's thread, or inside a lease, ticket or completion critical section. Calls are made after the state change is decided. Listener exceptions are caught at the instrument call and counted (a Debug log once per process), so they cannot reach the caller; they cannot affect ownership either: Renew, Finish and Recover decisions are taken before the measurement and do not depend on it (OBS-01). Runwick owns no exporter queue.

## 2. Traces

### 2.1 Spans

| Span name | Kind | Starts → ends | Parent / links | Key attributes |
|---|---|---|---|---|
| `send {queue}` | Producer | Dispatch prepare → confirmed reply or explicit unknown | caller's `Activity.Current` | `runwick.job.id`, `runwick.dispatch.result` (created/reused/unknown/rejected/conflict), `runwick.dispatch.kind` (single/batch/prepared/import) |
| `stage {queue}` | Producer | staging inside the caller's transaction | caller | `runwick.dispatch.kind = staged`; never `created` (00 §9 rule 3) |
| `process {queue}` | Consumer | reservation received → settlement confirmed, lost or unresolved at shutdown | **new trace**, link to the dispatch context from stored `traceparent` (validated, 02 IC-07) | `runwick.job.id`, `runwick.job.generation`, `runwick.attempt.id`, `runwick.execution.number`, `runwick.outcome`, `runwick.settlement.result` |
| `runwick.begin` | Internal | Begin request → reply | child of `process` | `runwick.result` (authorized/deferred/cancelled/exhausted/stale) |
| `runwick.execute` | Internal | middleware + handler invocation → return or throw | child of `process`; application spans are its children | `runwick.outcome` |
| `runwick.attempt.cleanup` | Internal | scope disposal | child of `process` | `runwick.result` (completed/budget_exceeded) |
| `settle {queue}` | Internal | invocation end → confirmed Finish, definitive loss, or unresolved at shutdown | child of `process` | `runwick.settlement.result` (confirmed/lost/unresolved), `runwick.storage.attempts` |
| `runwick.recover` | Internal | one Recover command | new root | `runwick.result` |
| `runwick.schedule.materialize` | Internal | one materialization command | new root | `runwick.schedule.result`, `runwick.schedule.occurrences` (count) |
| `runwick.management.command` | Internal | session mutation → committed result | ASP.NET Core request activity | `runwick.management.operation`, `runwick.result`, `runwick.replayed`; never the body |
| `runwick.retention.batch` | Internal | one cleanup batch (17 §4.1) | new root | `runwick.retention.class`, `runwick.result`, `runwick.rows` |

Rules:

- Each attempt gets its own `process` span and trace, linked to the dispatch context. Retries are new attempts with new spans linked to the same dispatch context; no span stays open across a backoff.
- Handler success followed by an unconfirmed Finish is `runwick.outcome = succeeded` with `runwick.settlement.result = unresolved` (OBS-02). Lost authority is `runwick.settlement.result = lost`, never success. A handler still running after authority loss gets the event `runwick.authority_lost` on `process` with `runwick.loss.reason`; the span does not end until the handler returns.
- Renew is not a span (it repeats every few seconds per lease); it is measured by metrics and logged when stale (§5).
- Batched storage calls (claim of several jobs, FinishMany, RenewMany) emit no span of their own; each `process`/`settle` span records its own part. A span links at most 8 contexts.
- Span status is `Error` only for Runwick failures (store unavailable, invariant fault, lost authority) and handler failures; `error.type` is the bounded code (§3.2), never the exception message.

### 2.2 Context and baggage

- Dispatch stores `traceparent`/`tracestate` from `Activity.Current` (existing, 01 §10.3). Invalid stored values are ignored, not repaired.
- Before invoking a handler the runner sets `Activity.Current` to its `runwick.execute` span and clears `Baggage.Current`: **baggage is never propagated** (02 IC-07). Ambient context of the worker loop never leaks into a handler and one attempt's context never leaks into another (OBS-05).
- Renew, Finish and Recover run on captured identifiers, never on the handler's `Activity`, DI scope or `AsyncLocal` state.
- Stored trace context never selects sampling: the `process` span is a root, and the host's sampler decides.

### 2.3 Messaging-conventions mapping (pinned)

Pinned to OpenTelemetry semantic conventions **1.44.0** (published 2026-08-04; re-pinned by SP-022 on 2026-11-08 as the latest release at least two weeks old), whose messaging spans are marked Development. Runwick does not claim compatibility with later revisions.

| Attribute | Value |
|---|---|
| `messaging.system` | `runwick` |
| `messaging.operation.type` | `send` (send/stage), `process` (process), `settle` (settle) |
| `messaging.operation.name` | `dispatch`, `stage`, `process`, `finish` |
| `messaging.destination.name` | queue name |
| `messaging.message.id` | job ID (spans only) |
| `messaging.batch.message_count` | batch dispatch item count |

Runwick attributes (`runwick.*`) are always present; the `messaging.*` set is added to the same spans.

## 3. Metrics

### 3.1 Instruments (frozen, schema 1)

Durations are seconds (`s`); histograms use the bucket set named in the last column.

| Instrument | Kind / unit | Attributes | Meaning | Buckets |
|---|---|---|---|---|
| `runwick.dispatch.operations` | Counter `{operation}` | `runwick.dispatch.result`, `runwick.dispatch.kind`, `runwick.queue` | Client-observed dispatch outcomes; created/reused/unknown/rejected/conflict/`local_capacity_exceeded` are distinct | — |
| `runwick.dispatch.duration` | Histogram s | `runwick.dispatch.result` | Prepare → reply | fast |
| `runwick.execution.authorized` | Counter `{execution}` | `runwick.queue`, `runwick.job.name`, `runwick.pool` | Newly applied Begin authorizations; replayed receipts excluded | — |
| `runwick.admission.deferred` | Counter `{begin}` | `runwick.reason` (rate) | Begins deferred (10 T20) | — |
| `runwick.execution.duration` | Histogram s | `runwick.queue`, `runwick.job.name`, `runwick.outcome` | Handler invocation only | long |
| `runwick.execution.start_delay` | Histogram s | `runwick.queue` | Begin instant − eligibility instant, both from the store | long |
| `runwick.attempt.cleanup.duration` | Histogram s | `runwick.result` | Attempt scope disposal | fast |
| `runwick.settlement.duration` | Histogram s | `runwick.settlement.result` | Invocation end → confirmed Finish or definitive loss | fast |
| `runwick.storage.operation.duration` | Histogram s | `runwick.operation`, `runwick.result`, `runwick.io_class` | One driver call | fast |
| `runwick.lease.renewals` | Counter `{renewal}` | `runwick.result` (renewed/stale/error) | Renew results | — |
| `runwick.lease.margin` | Histogram s | `runwick.pool` | Remaining local safety margin when a Renew reply arrives | margin |
| `runwick.lease.authority_lost` | Counter `{attempt}` | `runwick.loss.reason` | Attempts that lost authority (05 loss causes) | — |
| `runwick.recoveries` | Counter `{recovery}` | `runwick.result` | Recover outcomes | — |
| `runwick.worker.tickets` | ObservableGauge `{ticket}` | `runwick.pool`, `runwick.ticket.phase` (reserving/reserved/running/cleanup/settling) | Local outstanding tickets | — |
| `runwick.worker.target` | ObservableGauge `{ticket}` | `runwick.pool` | Local target, not free slots | — |
| `runwick.worker.oldest_unsettled.age` | ObservableGauge s | `runwick.pool` | Age of the oldest local unconfirmed settlement; no measurement when none | — |
| `runwick.worker.admission` | ObservableGauge `{state}` | `runwick.pool` | 1 open, 0 closed | — |
| `runwick.scaling.decisions` | Counter `{decision}` | `runwick.pool`, `runwick.scaling.direction`, `runwick.reason` | Controller decisions (11) | — |
| `runwick.schedule.materialization.delay` | Histogram s | — | Materialization time − due instant | long |
| `runwick.schedule.occurrences` | Counter `{occurrence}` | `runwick.schedule.disposition` | created/linked_existing/skipped | — |
| `runwick.management.requests` | Counter `{request}` | `runwick.management.operation`, `runwick.result` | Management session calls (16) | — |
| `runwick.management.duration` | Histogram s | `runwick.management.operation` | Session call duration | fast |
| `runwick.retention.rows` | Counter `{row}` | `runwick.retention.class` | Rows deleted or detail-expired (17) | — |
| `runwick.retention.batches` | Counter `{batch}` | `runwick.retention.class`, `runwick.result` (completed/empty/skipped_locked/failed) | Cleanup batches | — |
| `runwick.retention.batch.duration` | Histogram s | `runwick.retention.class` | Batch database time | fast |
| `runwick.retention.caught_up.age` | ObservableGauge s | `runwick.retention.class` | Time since the class last completed a pass with nothing to delete, as seen by this process; no measurement before the first pass | — |
| `runwick.telemetry.dropped` | Counter `{record}` | `runwick.buffer` | Records dropped by bounded diagnostic buffers and log limits | — |
| `runwick.telemetry.overflow` | Counter `{measurement}` | `runwick.instrument` | Measurements folded into the overflow series (§3.3) | — |
| **Global (collector only, §4)** | | | | |
| `runwick.queue.due` | ObservableGauge `{job}` | `runwick.queue`, `runwick.collector` | Due `pending`/`retrying` jobs, counted up to 10,000 | — |
| `runwick.queue.oldest_due.age` | ObservableGauge s | `runwick.queue`, `runwick.collector` | Snapshot time − oldest due `eligible_at` | — |
| `runwick.schedule.needs_review` | ObservableGauge `{schedule}` | `runwick.collector` | Schedules needing review | — |
| `runwick.schedule.oldest_overdue.age` | ObservableGauge s | `runwick.collector` | Oldest `next_due` behind snapshot time among unpaused schedules | — |
| `runwick.collector.snapshot.age` | ObservableGauge s | `runwick.collector` | Time since the last successful snapshot; reported even when the snapshot is stale | — |

Bucket sets (seconds): **fast** 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10; **long** 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300, 900, 3600, 14400; **margin** 0, 1, 2, 5, 10, 15, 20, 25, 30, 45, 60, 120.

### 3.2 Attribute vocabulary

| Attribute | Values |
|---|---|
| `runwick.queue`, `runwick.job.name` | Registered names only; anything else is `_other` |
| `runwick.pool` | Configured pool names |
| `runwick.operation` | Operation names of the 03 §4 catalog (no versions) |
| `runwick.result`, `runwick.dispatch.result`, `runwick.settlement.result`, `runwick.outcome` | Fixed per instrument as listed; anything unexpected is `other` |
| `runwick.reason`, `runwick.loss.reason` | Stable reason codes from 03/05/10/11 |
| `runwick.io_class` | renewal, settlement, admission, producer, background |
| `runwick.retention.class` | 17 §1 class names |
| `runwick.management.operation` | Session method names of 16 (e.g. `ListJobs`, `CancelJob`) |
| `runwick.collector` | `RunwickTelemetryOptions` collector name (default `default`) |
| `error.type` | Runwick error code (03 §9) or handler exception type's full name, ≤ 128 characters; spans only |

Forbidden as metric attributes: job, attempt, command, schedule-occurrence or worker-instance IDs, tenant IDs, uniqueness or resource keys, exception messages, URLs, actor identities, reasons.

### 3.3 Cardinality bound

Each instrument has a fixed-size admission set of at most `MaxSeriesPerInstrument` (1,000) attribute combinations, filled first come. A measurement whose combination is not in a full set is recorded with every attribute replaced by `runwick.overflow = true` and counts once in `runwick.telemetry.overflow`. The check happens before any per-series state is allocated; the set's memory is bounded by the limit. Queue and job names are mapped to `_other` before the check when they are not registered, so attacker-supplied strings never reach the set (OBS-03).

### 3.4 Observable callbacks

Callbacks read an in-memory snapshot (an immutable object swapped by the owning loop) and return. They never call the store, take a lock held by handlers or coordinators, wait, or do network I/O (OBS-04). A missing snapshot produces no measurement.

## 4. Local and application-wide numbers

- Every process reports **local** instruments only (tickets, targets, its own executions). They can be summed across processes because each describes different work.
- **Application-wide** instruments (§3.1, "Global") are produced only by a process with `CollectApplicationMetrics = true` — normally the standalone management host. Its sampler runs every `ApplicationSamplePeriod` on the background class, with a 1 s query budget, using indexed bounded queries:
  - due count per registered queue: `SELECT count(*) FROM (SELECT 1 FROM jobs WHERE application = $1 AND queue = $2 AND state IN ('pending','retrying') AND eligible_at <= now() LIMIT 10000)`, served by `jobs_ready`;
  - oldest due per queue: the first row of the same index;
  - schedules: one query over `schedules` for needs-review count and oldest overdue.
  At most 64 registered queues per sample [choice]; more queues are sampled round-robin over successive periods, each keeping its own observed time.
- **Staleness.** A queue's measurement is reported only while its snapshot is younger than 3 × `ApplicationSamplePeriod`. After that the gauge reports nothing for that queue (absent, not zero), and `runwick.collector.snapshot.age` keeps growing (OBS-06, 00 §9 rule 9).
- **Duplicate collectors.** Runwick does not elect a collector. Every global series carries `runwick.collector`; the shipped dashboard and alerts aggregate global series with `max by (queue)`, never `sum`, so two collectors do not double the depth (OBS-06).
- No schema change: the sampler uses existing indexes (`jobs_ready`, `schedules_due`).
- The due count is "due waiting", not "runnable": it ignores capacity, rates, pauses and worker capability (observability §4).

## 5. Logs

Category names: `Runwick.Worker`, `Runwick.Dispatch`, `Runwick.Scheduler`, `Runwick.Outbox`, `Runwick.Management`, `Runwick.Retention`, `Runwick.Store`, `Runwick.Telemetry`. Messages use `LoggerMessage` source generation with structured fields; free text is never interpolated from user data.

| ID | Name | Level | Fields |
|---|---|---|---|
| 1001 | AdmissionClosed | Warning | pool, reason |
| 1002 | AdmissionOpened | Information | pool |
| 1003 | AuthorityLost | Warning | jobId, attemptId, lossReason, handlerStillRunning |
| 1004 | SettlementUnresolved | Warning | jobId, attemptId, outcome, elapsedMs |
| 1005 | AttemptCleanupBudgetExceeded | Warning | jobId, attemptId, budgetMs |
| 1006 | PresencePublicationFailing | Warning | errorCode, consecutiveFailures |
| 1007 | PressureStop | Warning | trigger, valueMs |
| 1008 | PressureResume | Information | — |
| 1009 | LeaseRenewalStale | Warning | jobId, attemptId |
| 1010 | AttemptRecovered | Information | jobId, attemptId, result |
| 1011 | HandlerFailed | Information | jobId, attemptId, jobName, outcome, errorType |
| 1012 | DrainStarted / 1013 DrainCompleted | Information | outstanding |
| 1014 | UnsupportedBacklog | Information | queue, jobName, payloadVersion |
| 1101 | DispatchOutcomeUnknown | Warning | commandId, queue |
| 1201 | ScheduleMaterializationFailed | Warning | scheduleKey, errorCode |
| 1202 | ScheduleNeedsReview | Warning | scheduleKey, reason |
| 1203 | ScheduleMisfireEpisode | Information | scheduleKey, policy, skipped |
| 1301 | OutboxDeliveryBlocked | Warning | messageId, blockedReason |
| 1302 | OutboxDestinationUnavailable | Warning | errorCode |
| 1401 | ManagementAuthenticationFailed | Warning | scheme, sourceBucket |
| 1402 | ManagementAccessDenied | Information | operation, targetKind |
| 1403 | ManagementRateLimited | Information | limit |
| 1404 | ManagementDisclosure | Information | operation, targetKind, targetId, commandId |
| 1405 | ManagementOutcomeUnknown | Warning | operation, commandId |
| 1501 | RetentionBatchFailed | Warning | recordClass, errorCode |
| 1502 | RetentionPolicyEdited | Information | revision |
| 1503 | RetentionBehind | Warning | recordClass, caughtUpAt |
| 1601 | CollectorSampleFailed | Warning | errorCode |
| 1602 | TelemetryOverflow | Warning | instrument (once per instrument per process) |
| 1603 | LogsSuppressed | Warning | eventId, suppressed |
| 1701 | StoreUnavailable | Warning | role, errorCode |
| 1702 | StoreProfileMismatch | Error | expected, actual |
| 1703 | StoreSchemaIncompatible | Error | expected, actual |

- Job, attempt and command IDs may appear in logs and spans (access-controlled by the host), never in metrics.
- **Log bounds.** Events 1401–1403 and any event at Warning or above are limited to 10 per minute per event ID per process; the excess is counted and summarized once a minute by 1603 and in `runwick.telemetry.dropped` (management-retention OPS-06). The existing worker `Diagnose(string)` messages become event IDs from this table; no new free-text warning is added.
- **Buffers.** Library-owned diagnostic buffers (worker diagnostics list, recent loss records) hold at most 1,024 records and count drops.

## 6. Redaction

| Data | Default | With `RecordExceptionDetails` |
|---|---|---|
| Payload bytes, business metadata values, headers | never | never |
| Connection strings, credentials, authorization headers, cookies | never | never |
| Lease/ownership tokens, uniqueness keys, resource keys, SQL parameters | never | never |
| Operator reasons, cancel reasons, actor display names | never (audit only) | never |
| Exception type | `error.type` | same |
| Exception message | never | spans and log 1011 only: sanitized (control characters removed), ≤ 1 KiB UTF-8 with truncation marker |
| Stack trace | never | never in telemetry (stored attempt error only, behind `Jobs.ReadError`, 16 §3) |

Sanitization runs before the value reaches `Activity` or `ILogger`, not at export. The stored attempt error (16 KiB, 03 §3.4 `AttemptError`) is a storage concern and is not changed here.

## 7. Health and readiness

Core exposes a snapshot API; `Runwick.AspNetCore` adapts it to `Microsoft.Extensions.Diagnostics.HealthChecks` (part of the ASP.NET Core shared framework, so no new package).

```csharp
public enum RunwickHealthStatus { Healthy, Degraded, Unhealthy }
public sealed record RunwickRoleHealth(string Role, RunwickHealthStatus Status, string Reason, DateTimeOffset ObservedAt);
public interface IRunwickHealth { RunwickRoleHealth Liveness(); IReadOnlyList<RunwickRoleHealth> Readiness(); }

// Runwick.AspNetCore
services.AddHealthChecks().AddRunwick();   // checks "runwick-live" (tag "live"), "runwick-{role}" (tag "ready") per enabled role
```

| Role | Healthy | Degraded | Unhealthy |
|---|---|---|---|
| Liveness | Supervisor tick advanced within 10 × `LeaseWatchdogInterval` (05 §17) | — | Tick stalled. A store outage alone never makes liveness unhealthy |
| Producer | Startup compatibility check passed and the last store call (dispatch or the 10 s compatibility refresh, 1 s budget) succeeded | Last call failed < 30 s ago | Incompatible schema/profile, or failing for ≥ 30 s |
| Worker | 05 §17 table, unchanged | 05 §17 | 05 §17 (draining, store outage, pressure stop on every pool, incompatible) |
| Scheduler | Evaluator and TZDB compatible; last cursor write succeeded within 3 × due poll | Last write failed < 3 × due poll ago | Incompatible evaluator, or writes failing ≥ 3 × due poll |
| Management | Configuration valid (startup) and the 10 s store read refresh succeeded | Read failed < 30 s ago (reads may be stale, mutations fail with 503) | Failing ≥ 30 s |

- Checks return the cached state; the refreshers run on the background class. A health request never waits on the store, never reserves a job.
- Public responses carry only the status. The `Reason` (a bounded code) is included only when the host maps health checks with a detailed response writer behind its own authorization.
- Drain: worker readiness Unhealthy, liveness Healthy. Zero backlog, a paused queue, or an unsupported backlog does not change readiness (OBS-07).
- Stale data is never presented as healthy: a refresher that has not run for 3 × its period reports Degraded with reason `stale`.

## 8. `Runwick.OpenTelemetry`

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddRunwickInstrumentation())
    .WithMetrics(m => m.AddRunwickInstrumentation(o => o.MaxSeriesPerInstrument = 1_000));
```

- `TracerProviderBuilder.AddRunwickInstrumentation()` adds the `Runwick` source. `MeterProviderBuilder.AddRunwickInstrumentation()` adds the `Runwick` meter and one view per instrument that keeps only the attributes of §3.1 and sets `CardinalityLimit = MaxSeriesPerInstrument + 1` (Runwick's own bound plus its overflow series), so the SDK's overflow attribute (`otel.metric.overflow`) appears only if Runwick's bound failed.
- Dependency: the `OpenTelemetry` SDK package, pinned exactly at **1.20.1** (published 2026-10-20, the latest stable at least two weeks old on 2026-11-08; per-view cardinality limits are stable since 1.10.0). No exporter, no instrumentation package, no `OpenTelemetry.Extensions.Hosting`. DEP-01 checks the graph.
- Tests use `OpenTelemetry.Exporter.InMemory` (test projects only).

## 9. Shipped dashboard and alert rules

Files in the repository and in the `Runwick.OpenTelemetry` package's `content/`:

- `ops/grafana/runwick-overview.json` — Grafana dashboard over a Prometheus data source, using the OpenTelemetry → Prometheus name mapping (dots → `_`, unit suffix `_seconds`, counters `_total`). Rows: dispatch, execution and settlement, leases, workers and pools, queues (global, `max by (queue)`), schedules, management, retention, telemetry health. Every global panel shows `runwick_collector_snapshot_age_seconds` next to the value.
- `ops/prometheus/runwick-alerts.yml` — Prometheus rule groups. Thresholds are **starting points**, to be tuned per workload:

| Alert | Expression (shape) | For |
|---|---|---|
| RunwickLeaseMarginLow | p05 of `runwick_lease_margin_seconds` < 25 (StopMargin) | 5 m |
| RunwickAuthorityLost | `increase(runwick_lease_authority_lost_total[10m]) > 0` | 0 |
| RunwickSettlementOld | `runwick_worker_oldest_unsettled_age_seconds > 60` | 5 m |
| RunwickDueAgeRising | `max by (queue) (runwick_queue_oldest_due_age_seconds) > 600` | 15 m |
| RunwickFailuresRepeated | failure-outcome rate by `error_type` > 10 % of executions | 15 m |
| RunwickScheduleLag | `runwick_schedule_oldest_overdue_age_seconds > 120` or `runwick_schedule_needs_review > 0` | 10 m |
| RunwickRetentionFailing | `increase(runwick_retention_batches_total{runwick_result="failed"}[1h]) > 3` or caught-up age > 24 h | 1 h |
| RunwickCollectorStale | `runwick_collector_snapshot_age_seconds > 60` or absent | 5 m |
| RunwickStoreIncompatible | readiness reason `incompatible` (via the host's health metric) | 0 |

A rules test (`promtool check rules` in CI when available, else a YAML lint plus a name check) verifies that every metric name used exists in §3.1.

## 10. Cases (OBS, telemetry plan)

Test projects: `tests/Runwick.Core.Tests` (listeners, bounds, redaction), `tests/Runwick.Postgres.Tests` (store-backed and peer-process cases), `tests/Runwick.AspNetCore.Tests` (health); trait `Item=SP-022`. IDs are cited as `observability-and-operations:OBS-nn` (00 §10).

| Case | What it runs |
|---|---|
| OBS-01 | Real worker, dispatch and scheduler with (a) no listener, (b) an `ActivityListener` and `MeterListener` that block 2 s per call, (c) listeners that throw: every job settles once, renewal margin stays above StopMargin in (a) and (c), and in (b) leases renew and Finish confirms (listener delay only adds latency to the instrumented thread after the decision) |
| OBS-02 | Handler succeeds, Finish reply dropped (`AfterCommitBeforeReply` + `DropReply`) and later resolved; and Finish made unresolved until shutdown: spans show `outcome=succeeded` with `settlement.result=confirmed` / `unresolved`; `runwick.execution.authorized` counts 1 although Begin was replayed |
| OBS-03 | 1,000,000 measurements with 5,000 registered queue names and random unregistered names: series per instrument ≤ 1,001, overflow counted, admission-set memory bounded (allocation measured); with the SDK and the in-memory exporter, exported points per instrument ≤ limit and no `otel.metric.overflow` |
| OBS-04 | Store made to hang (fault proxy): `MeterListener.RecordObservableInstruments` returns in < 10 ms, the store wrapper sees zero calls from callback threads |
| OBS-05 | Two concurrent jobs dispatched under different traces with baggage set: each handler sees its own `runwick.execute` span, the right link, empty baggage, no other attempt's tags, no leaked `AsyncLocal` value |
| OBS-06 | Two collectors: global series identical except `runwick.collector`; the dashboard's queries over them (evaluated by a small PromQL subset evaluator in the test, or `max`-only lint) give the single-collector value; store outage: queue gauges stop reporting after 3 periods, snapshot age keeps rising, nothing reports 0 |
| OBS-07 | Per-role readiness under drain, store outage, unsupported payload version backlog, and persistence-profile mismatch matches §7; liveness stays Healthy except for a stalled tick |
| OBS-08 | Peer process killed right after Finish commits and before the metric call: durable history has the transition, the metric does not; restart and replay of a Begin/Finish receipt produce no second `execution.authorized` |
| OBS-09 | Fixtures: exception message containing a password, payload with a credit-card-like value, connection string with password, uniqueness key with an e-mail. Captured logs, spans, metric tags and the management default projections contain none of them; with `RecordExceptionDetails` the message appears only sanitized and bounded on the span and 1011 |
| OBS-10 | Run by SP-021 (17 §8) |
| OBS-11 | Representative load (P-03 shape) with telemetry off, with listeners and SDK in-memory export on: throughput, p50/p99 execution and settlement latency, allocations per job and minimum lease margin recorded. Pass: telemetry-on throughput ≥ 90 % of off and minimum margin ≥ StopMargin [choice]; numbers are recorded, not promised for other hosts |
| TEL-01 | Name freeze: reflection over the meter and source lists exactly the instruments, units and span names of §2.1/§3.1; a rename fails the test |
| TEL-02 | Log bounds: 10,000 authentication failures in a minute produce ≤ 10 events 1401 plus one 1603 with the count |

TEL-01 and TEL-02 are new IDs for behaviour the plan does not name.

## 11. Must not claim

- that metric totals are exact, billable or audit-grade (they can be lost on crash, 00 §9);
- that the due count is a runnable count;
- compatibility with OpenTelemetry semantic conventions other than the pinned version;
- that telemetry isolates Runwick from a hostile listener in the same process;
- that the shipped alert thresholds suit any workload.
