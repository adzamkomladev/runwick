> History; see [docs/spec](spec/00-overview.md) for the normative specification.

# Runwick architecture plan

**Planning complete:** all six remaining items are consolidated in the [architecture baseline](architecture-baseline.md). Start there for current decisions and the fixed implementation sequence. Earlier discussion below is design history; runtime evidence remains pending.

Status: planning draft. No implementation has started. Requirements below come from the supplied handoff; recommendations remain proposals until reviewed.

Continuity correction: the original [Queue Library Design conversation](https://chatgpt.com/share/6ae25a83-ff20-83e9-aadb-b4d731f0e2e0) already covered public APIs, storage architecture, and the execution-engine flow. Read [planning continuity](planning-continuity.md) before continuing. Current work is detailed internal engineering design; earlier decisions must not be repeatedly reopened or silently replaced.

## Product and agreed scope

Runwick is a free, open-source, monolith-first .NET background job library. It should be approachable in one application and support distributed production deployments without changing job definitions or execution semantics.

The project name and namespace prefix are **Runwick**, replacing the provisional JobQueues name. Package-name availability has not been checked.

The user selected **.NET 10 and newer, with current/newer C#**. The [platform/package foundation](architecture/platform-and-packages.md) proposes net10.0/C# 14 initially, compatible newer stable language/runtime adoption and pinned builds. No older .NET/Framework/Standard targets are planned.

### Developer experience

- Class-based job payloads with separate typed handlers, native dependency injection, async/await, and Task-based execution pools.
- String queue names and priority labels such as `"low"`, `"medium"`, and `"high"`.
- Attributes and fluent configuration; automatic discovery and explicit registration.
- Global and per-job middleware supporting dependency injection, logging, validation, tenancy, tracing, and extension points.
- No mandatory Entity Framework Core dependency.

### Deployment

Support all five modes with the same engine and handler contracts:

1. One application with embedded workers.
2. One application with an external worker.
3. Multiple application replicas with embedded workers.
4. Application replicas with multiple external workers.
5. Embedded and external workers together.

### Storage and execution

- PostgreSQL as the reference production driver, SQLite for local development and single-host use, and Redis as a distributed alternative.
- Application-owned or dedicated queue databases; configurable PostgreSQL schema.
- Transactional dispatch when business changes and queue insertion share a compatible transaction; planned outbox integration for separate storage.
- Immediate, delayed, and scheduled jobs; priorities; fixed and automatic worker concurrency.
- Atomic reservations, renewable leases, fencing, at-least-once processing, crash recovery, retries with classification/backoff/jitter, permanent failure, and dead-letter management.
- Worker-local, per-queue, per-job-type, and keyed concurrency controls, including distributed ceilings.
- Distributed rate limits remain distinct from concurrency limits. The [capacity design](architecture/capacity-control.md) now proposes rolling-window Begin authorization counts with atomic deferral; the first execution milestone does not claim to implement them.
- Queue pause prevents new reservations but permits dispatch and completion of running work.
- Cooperative cancellation and graceful shutdown; arbitrary in-process handlers cannot be forcibly terminated safely.
- Manual retry preserves the job ID and all attempt history, starting a new retry generation with its own budget.
- Atomic unique dispatch, explicit idempotency keys and retention windows, resource execution locks, and duplicate suppression across replicas. No exactly-once promise.
- Confirmed uniqueness scope: support both active-job uniqueness and fixed-window dispatch idempotency.

### Scheduling, operations, and dashboard

- One-time and recurring schedules, cron, time zones, explicit DST policies, overlap prevention, and `"skip"`, `"run-once"`, and `"catch-up"` misfire policies.
- Distributed scheduling and atomic deduplication of logical occurrences; schedule pause/resume and manual triggering.
- Automatic concurrency using queue depth or estimated workload, minimum/maximum bounds, cooldowns, step limits, weighted fairness, and scaling history.
- Infrastructure autoscaling remains external and can consume exported metrics.
- Optional free dashboard, embedded or standalone, sharing a management backend; Inertia.NET with bundled assets is the handoff's frontend choice, pending integration validation.
- Basic authentication, host ASP.NET Core authentication/policies, and host-configured OIDC; HTTPS, fail-closed authorization, CSRF protection, secret handling, permission checks, and administrative audit records.
- Dashboard coverage: jobs and attempts, queues, workers and capacity, schedules, scaling, settings, and retention.
- OpenTelemetry traces and metrics; polling plus optional real-time updates.

Durable workflows, chains, batches, DAGs, checkpoints/replay, compensation, and workflow visualization remain v2 work. Preserve useful identities and metadata without introducing a workflow engine in v1.

## Architecture recommendations for review

### 1. Separate host roles through configuration

Provide producer registration independently of worker startup. A web application can dispatch without running handlers; adding worker registration enables execution. Embedded and external workers use the same hosted service. Scheduler and management hosting are separately enabled roles over the same durable store.

### 2. Keep job identity independent of CLR identity

Persist an explicit stable job-type name, a payload schema version, and serialized data. Avoid making assembly-qualified class names the durable identity. A renamed namespace or deployed assembly should not silently make queued jobs unreadable.

Handler registration maps stable names and supported payload versions to handlers. Unknown names/versions need an observable, non-destructive policy, including a rolling-deployment strategy. Exact behavior is still open.

### 3. Establish ownership before exposing a storage abstraction

Every reservation receives a new ownership token. Renewal and durable completion must check current ownership and lease validity atomically. An expired owner cannot acknowledge a replacement attempt, release its permits, or overwrite its outcome.

Lease expiry cannot prove that the previous handler stopped. Distributed concurrency limits therefore bound valid reservations/permits, not every physical handler or external operation during a partition. Resource locks have the same limitation unless the resource validates fencing tokens itself.

Reserve only against available local execution capacity. Acquire distributed permits and the reservation atomically, or provide an equivalent driver-specific protocol with proven recovery behavior. Capacity contention should not consume a handler retry budget.

### 4. Model jobs and attempts separately

Preserve the original states: `pending`, `reserved`, `running`, `retrying`, `succeeded`, `failed`, and `cancelled`. Eligibility time distinguishes immediate and delayed work. Reservation establishes ownership; a separate execution-start record distinguishes authorization to run from merely holding a lease.

The [internal storage protocol](architecture/storage-command-protocol.md) now proposes exact transitions, command results, and race handling. Create an attempt at reservation, consume its execution budget at BeginExecution, and bound repeated pre-start abandonment separately. This accounting choice remains a proposal; a committed begin cannot prove user code actually ran.

Cancellation requests remain durable metadata until an owned attempt is finalized. The protocol defines cancellation-versus-completion handling and proposes manual retry from failed/cancelled into a new generation, preserving lifetime history.

### 5. Separate three meanings often called idempotency

- Dispatch uniqueness: equivalent dispatches share an existing logical job under defined conditions.
- Execution exclusion: valid reservations serialize access to a key while permits remain valid.
- Side-effect idempotency: the application's database or external service recognizes repeated operations.

None substitutes for the others. Define key scope, retention anchors, payload mismatch behavior, and manual-retry interactions independently.

### 6. Make transaction support explicit

The PostgreSQL driver should accept a caller-owned compatible connection/transaction without committing or disposing it. Being on the same database server alone does not make dispatch atomic with business changes.

Cross-store dispatch needs an outbox protocol. Outbox delivery can repeat, so dispatch deduplication and retention must cover the delivery/replay contract. Optional PostgreSQL and SQLite source adapters are included in the v1 baseline; see [transactional outbox](architecture/transactional-outbox.md).

### 7. Keep packages proportional to real dependencies

Proposed initial packages, refined in the [platform/package design](architecture/platform-and-packages.md):

- `Runwick.Contracts`: lightweight job/handler/context contracts and metadata attributes for shared application contracts.
- `Runwick`: dispatcher/recovery APIs, configuration, worker execution, middleware, scheduling and management application services; includes Contracts transitively.
- `Runwick.Postgres`, `Runwick.Sqlite`, `Runwick.Redis`: storage dependencies and implementations.
- `Runwick.AspNetCore`: HTTP management integration.
- `Runwick.Dashboard`: bundled UI over the management backend.
- `Runwick.Testing`: test-host and application-testing helpers.

Keep execution and scheduling as internal modules initially. The Contracts split is now the concrete recommendation to keep shared application contracts independent of hosting and provider dependencies. Public package names/availability and the graph remain reviewable; no runtime packages exist yet.

## Research notes

- [River transactional enqueueing](https://riverqueue.com/docs/transactional-enqueueing): committing application data and queue records together closes the failure window between separate writes. Runwick should make this a first-class PostgreSQL path.
- [River unique jobs](https://riverqueue.com/docs/unique-jobs): insertion uniqueness does not guarantee exactly-once execution. Runwick must document uniqueness independently from execution and retention.
- [Laravel Horizon](https://laravel.com/framework/docs/horizon): process balancing and timeout behavior provide operational ideas, but process termination does not translate directly to Runwick's in-process async handlers. Runwick should scale available concurrency and drain running handlers on scale-down.
- [Hatchet durable execution](https://docs.hatchet.run/v1/durable-execution): replay/checkpoint semantics introduce additional execution contracts. These belong in Runwick v2; ordinary v1 handlers must not imply resumable execution.

These are an initial documentation review, not a completed source-code or algorithm review.

The comparison now also includes Coravel, Hangfire, Wolverine, FastEndpoints.JobQueues, TickerQ, and Quartz.NET. See [existing-library research](library-research.md) for documented behavior, proposed lessons, and remaining validation. Public API and storage decisions remain provisional pending this broader comparison.

The [implementation review](implementation-research.md) adds pinned source/test evidence and upstream tradeoffs. Its proposed acceptance criteria cover durable enqueue failure, ownership checks, idle polling load, capacity-aware claims, shutdown bookkeeping, keyed-gate reclamation, dashboard authorization, driver-specific contention, and payload registration. Architecture review must account for these before interfaces are frozen. Upstream omissions must not be attributed to security or performance without evidence.

## Engineering plan still to complete

1. Review the [consolidated public API](architecture/public-api-contract.md) and [wire/fingerprint proposal](architecture/dispatch-wire-contract.md): exact names, serializer/codec compatibility, defaults, and remaining release choices. The baseline and precedence are now reconciled; examples still need compilation once a contract project exists.
2. Specify lifecycle invariants: retry accounting, cancellation races, timeouts, lease loss, shutdown, permit release, manual retry, and unavailable handlers.
3. Design PostgreSQL records, indexes, schema migrations, reservation SQL, permit acquisition, renewal, recovery, and retention. Core records include jobs, attempts, schedules/occurrences, workers, controls, permits, deduplication keys, and audit events.
4. Define the storage capability matrix and shared contract suite. Unsupported guarantees must be explicit; a driver cannot silently approximate an advertised atomic operation.
5. Specify the execution loop, DI-scope lifetime, middleware outcomes, wake-up/polling behavior, and recovery maintenance.
6. Define scheduling/DST/occurrence identity, bounded misfire catch-up, priority fairness, distributed limits, and scaling control behavior.
7. Validate the dashboard integration and management security boundary, observability, audit/retention policies, and compatibility during upgrades.

## Proposed delivery phases and acceptance gates

All agreed v1 features remain in scope. Phases describe build order, not feature removal.

1. **Architecture review:** approve public API examples, lifecycle/ownership invariants, PostgreSQL algorithms, and driver guarantees before implementation.
2. **PostgreSQL execution milestone:** typed dispatch/handlers, embedded workers, middleware, reservations, retries, leases, recovery, and concurrency tests. Prove rollback removes dispatch, competing workers cannot share valid ownership, expired owners cannot acknowledge, and recovery retains attempt history.
3. **Distributed operation:** validate all five hosting modes, permit scopes, unique dispatch, cancellation, pause/resume, and manual retry. Exercise process death and network interruption with concurrent replicas.
4. **Scheduling and capacity:** specify and implement distributed rate admission separately from concurrency, including its accounting point and blocked-start behavior; prove single logical occurrence creation under competing schedulers, specified DST/misfire results, bounded catch-up, fairness, and concurrency scaling bounds. Rate admission must be complete before v1 admission controls are considered complete.
5. **Additional drivers:** SQLite and Redis pass the applicable shared contract suite with documented persistence, topology, and transactional-dispatch limits.
6. **Operations and release:** embedded/standalone dashboard, authentication/authorization, telemetry, audit, retention, migrations, upgrade compatibility, documentation, and representative load tests. Set performance targets after workload assumptions are agreed.

## Design history and supporting packets

The [public API entry point](public-api-proposal.md) now links the reconciled current contract and the archived initial draft. The [consolidated proposal](architecture/public-api-contract.md) covers attributed/fluent contracts, handler discovery, dispatch/staging/recovery results, all five hosting modes, middleware, schedules and revisioned controls.

Confirmed documentation preference: **attributes first**, keeping defaults beside each job class. Fluent configuration remains supported for overrides and unannotated contracts.

Preserve the original public baseline: `IQueueJob`, `IJobDispatcher.DispatchAsync`, `IJobHandler<TJob>.HandleAsync(job, CancellationToken)`, and optional injected `IJobContext`. The initial draft's marker-free payload and mandatory context-argument alternatives are archived. Proposed scalar precedence is library defaults → application defaults → attributes → fluent job configuration → permitted dispatch overrides; identities, shared live controls and collection ordering have explicit separate rules.

Both uniqueness modes are now confirmed. The [uniqueness and execution proposal](uniqueness-and-execution.md) specifies recommended key behavior, retry budgets/backoff, cancellation, timeout, lease recovery, and shutdown semantics. Its detailed policies and numeric defaults remain proposals.

The [internal storage command protocol](architecture/storage-command-protocol.md) and [PostgreSQL transaction design](architecture/postgres-storage-design.md) are now drafted. They specify records/indexes, atomic boundaries, lock ordering, command replay, ownership checks, and a race-test matrix; they are not implemented or experimentally validated.

[Storage design inputs](storage-design-inputs.md) reviews the storage draft against the original conversation on four points (priority lanes, name-keyed handlers with payload upgrades, capability filtering, permits). It accepts the draft's lane model and lease-backed permits, and adds: a single multi-lane ready probe, a lane short-circuit when the blocking scope is queue-wide, the missing rate-limit representation, and the measurements to run first.

The [worker-engine internals](architecture/worker-engine-internals.md) now develop those storage inputs into service/task ownership, local capacity tickets, name-keyed handler upgrades, coordinated lane probing, lease renewal, bounded completion handling, DI/middleware lifetime, and staged shutdown. The document identifies read-side/storage additions rather than silently changing mutation contracts. It is a proposal with an unexecuted validation matrix.

The [capacity-control proposal](architecture/capacity-control.md) now defines weighted queue/lane selection and aging, rolling-window rate admission at Begin, atomic deferral without execution-budget charge, and a bounded local scaling controller. The [scheduling proposal](architecture/scheduling-internals.md) defines occurrence identity, transactional materialization, bounded misfire episodes, DST mapping, overlap, and revisioned controls. Algorithm/default choices remain proposals and the validation matrices are unexecuted.

The [driver capability matrix](architecture/driver-capabilities.md) and [shared contract suite](architecture/driver-contract-suite.md) now define topology, transaction enlistment, acknowledgement profiles, operation parity, driver-specific algorithms, and release gates. SQLite targets same-host processes with serialized writes. Redis has explicit publication/recovery and persistence/failover feasibility gates; target scope is not a claim of implemented parity. The [BullMQ review](architecture/bullmq-redis-review.md) supplies pinned Redis source/test evidence.

The [dispatch wire/fingerprint proposal](architecture/dispatch-wire-contract.md) now specifies immutable preparation, command replay versus business-window comparison, included effective settings, canonical encoding, and required compatibility vectors. Numeric normalization, exported-envelope ergonomics, exact public names and final defaults remain review gates.

The [management/dashboard proposal](architecture/management-and-dashboard.md) now defines scoped permissions, Basic/host/OIDC integration, browser antiforgery, safe queries and Inertia transport, revisioned commands, atomic audit, retention and adapter integration gates. The [observability/operations proposal](architecture/observability-and-operations.md) defines trace/timing boundaries, bounded metric dimensions, application-wide collection, role readiness and operating procedures. No adapter integration, security test or telemetry benchmark has run.

The [implementation backlog](implementation-backlog.md) defines 30 dependent work items, early feasibility spikes and acceptance evidence. The [architecture acceptance checklist](architecture-acceptance.md) tracks 14 decisions and gates for architecture, PostgreSQL execution, distributed features, management, drivers, operations and release. D-01's platform floor is now accepted; remaining implementation evidence and all runtime gates remain open.

The [platform/package foundation](architecture/platform-and-packages.md) records the .NET 10+ decision, C# pairing/build policy, package graph and role/dependency checks. The [identity/serialization packet](architecture/identity-and-serialization.md) now recommends explicit identities, deterministic configuration, the built-in JSON profile, size bounds and replay behavior. It reconciles original-byte storage and profile-aware capabilities into PostgreSQL, protocol and worker drafts. All new engineering selections remain reviewable.

The [execution timing packet](architecture/execution-defaults-and-timing.md) now proposes D-05 values and relationships for retries, pre-start abandonment, leases/renewals, pipeline timeouts, cancellation/cleanup, ambiguous outcomes and shutdown. These are concrete review values, not benchmarked defaults.

The [consolidated PostgreSQL contract](architecture/postgres-schema-and-operations.md) and [upgrade protocol](architecture/postgres-upgrades-and-compatibility.md) now provide D-06/D-13 recommendations: full schema inventory, gate-first locks, operation/effect semantics, independent retention, migration phases and mixed-version matrix. DDL, query plans and PGX/UPG test evidence remain outstanding.

The [provider dependency plan](architecture/provider-dependency-plan.md) records the user's latest-Npgsql direction with the verified 10.0.3 baseline, recommended SQLite/Redis candidates, PostgreSQL server targets and connection/transaction ownership. DEP-01–08 remain unexecuted.

The [capacity defaults/budgets packet](architecture/capacity-defaults-and-budgets.md) develops D-07: fixed/automatic targets and shared attempt bounds, fairness/probe settings, rate-edit policy, an occupancy-plus-backlog controller, pressure gates and partitioned connection budgets. It adds typed known-not-submitted local overload handling to the API proposal. Planning arithmetic was checked; CAP-01–12 runtime cases remain unexecuted.

The [scheduler evaluator/defaults packet](architecture/scheduler-evaluator-and-defaults.md) now develops D-08: Cronos 0.13.0/NodaTime 3.3.7 recommendations, restricted cron grammar with an OR adapter, TZDB artifact/hash identity, UTC-interval DST evaluation, misfire/overlap/pacing budgets and prepared forward cutovers. Source inspection and an artifact hash were checked; SCHED-01–15 remain unexecuted against Runwick. The [final baseline](architecture-baseline.md) closes all six remaining planning items. Runtime evidence belongs to the implementation backlog.

The acceptance checklist's decision register now tracks remaining choices, distinguishing established behavior from exact algorithms, defaults, dependency versions and unverified guarantees.
