# Planning continuity and reconciliation

> **Renamed 2026-10-30: Spindle → Runwick.** NuGet `Spindle`/`Spindle.Testing` belong to an unrelated workflow library, so every package ID, namespace, hash domain, instrument name, schema name and prefix was renamed before any release. No wire compatibility impact: nothing had been published. Normative specifications now live in [docs/spec](spec/00-overview.md).

**Current position: all six planning items complete.** Read the [final architecture baseline](architecture-baseline.md) first. The discussion history below explains how the design developed; its former open choices are superseded where the baseline records a selection. Implementation and runtime validation have not begun.

Reconciled on 2026-10-28 against [Queue Library Design](https://chatgpt.com/share/6ae25a83-ff20-83e9-aadb-b4d731f0e2e0), the supplied handoff, and the current Runwick conversation.

## Where the original conversation stopped

The original conversation had already moved from product features through public API exploration into **Stage 2 — Core Execution Engine & Internal Contracts**. Its handoff explicitly called for architecture validation and implementation-ready engineering artifacts, not another feature-by-feature planning cycle.

The current conversation repeated material already covered and introduced alternatives without consistently comparing them to the earlier design. This document corrects that continuity. Historical assistant code samples remain proposals rather than automatically becoming user-approved exact signatures.

## Carry forward the existing architecture

- One execution engine serves all five deployment modes through the .NET host.
- Async Task-based pools manage execution slots; they do not allocate a dedicated process or OS thread per job.
- Acquire local capacity before reserving work. Storage owns atomic admission, queue controls, leases, distributed permits, and lifecycle transitions.
- PostgreSQL reservation uses short transactions and `FOR UPDATE SKIP LOCKED`; user handlers execute after commit. Optional wake-up signals complement polling.
- The execution path already includes reservation, attempt identity, a per-attempt DI scope, middleware/handler execution, independent renewal, outcome classification, atomic finalization, and release of local capacity.
- Successful handler execution and durable acknowledgement are separate events. Uncertain acknowledgement can lead to at-least-once recovery; stale ownership cannot acknowledge.
- Middleware is global or per job, including suitable attributes. Compile/cache pipeline structure per job type while resolving scoped instances per attempt. Intentional short-circuiting requires an explicit outcome.
- Local automatic scaling is separate from execution and coordinated with backend global limits. Ordinary local scaling does not require a permanent cluster scaling leader.
- Scheduling materializes occurrences separately from workers. Occurrence uniqueness, not coordinator election alone, provides duplicate prevention. Code-defined structure and operational pause/resume overrides are separate.
- A single configured storage driver supports execution, scheduling, history, controls, and dashboard queries. No second storage service is mandatory.
- PostgreSQL is the final handoff's first implementation milestone. An earlier SQLite-first phase list was superseded by the later engine discussion and handoff.
- SQLite is single-host, with a same-host external worker possible; it is not the distributed replica backend. Redis requires its own atomic protocol and durability contract.
- Manual retry retains the original job ID, increments retry generation, and preserves total attempt history.
- Queue pause blocks new reservations, not dispatch. Cancellation is cooperative; execution timeout and reservation lease timeout are distinct.
- Migrations belong to Runwick independently of EF. The earlier direction was deployment-controlled production migrations, optional development initialization, and startup compatibility checks.
- Dashboard remains optional, embedded or standalone, with Inertia.NET. Basic Auth is the simple authentication path; host authentication and OIDC remain supported. Permissions and operational audit are part of the management backend.

## Changes introduced in this chat that were not approved replacements

| Area | Earlier design | Later draft in this chat | Reconciliation |
|---|---|---|---|
| Handler metadata | `HandleAsync(job, CancellationToken)` with optional scoped `IJobContext` injection | Mandatory `JobContext` argument | Use the earlier minimal signature as the working baseline; any change needs an explicit rationale |
| Job marker | `IQueueJob` in the public-contract sketches | No marker required | Retain as an unresolved contract comparison; do not describe removal as agreed |
| Dispatch naming | `IJobDispatcher.DispatchAsync` | `IJobClient.EnqueueAsync` and provider staging | Earlier naming remains the baseline; staging is an additional proposal, not a settled rename |
| Job name attribute | `[JobName(...)]` | `[Job(..., Version = 1)]` | Exact syntax remains provisional; stable name and version are the architectural requirement |
| Lifecycle representation | Reserved and running distinguished; retry waiting visible | Five persisted states combining some phases | Do not silently collapse the earlier model. Decide physical states versus attempt metadata through the storage design |
| Crash attempt accounting | Pre-start crash accounting explicitly open | Every committed reservation consumes retry budget | This remains an engineering decision, not an accepted rule |
| Retry defaults | Example proposal: three attempts, five-second initial delay | Five attempts, ten-second initial delay, equal jitter | Neither numerical set is a confirmed requirement; do not spend another product discussion choosing tuning constants |
| Middleware data | Execution context included job payload and scoped metadata/items | Metadata-only context, typed access deferred | Preserve payload-aware middleware as the earlier design input; finalize its safe contract |
| Middleware outcomes | Explicit outcomes already proposed | Treated as a new unresolved capability | Implement the existing direction through an exact outcome/continuation contract |
| Packages | Internal modular boundaries, public package consolidation permitted | Consolidated `Runwick` core package | Consolidation is allowed to be evaluated, but package topology remains an engineering deliverable |
| Distributed permits | Atomic admission and permit release established | Numbered slot rows mentioned in commentary | Slot rows are only a candidate algorithm. No such implementation has been selected |

## Confirmed additions in this chat

1. The name is **Runwick**.
2. Getting-started examples lead with attributes; fluent configuration remains supported.
3. Support both active-job uniqueness and fixed-window dispatch idempotency. Composition details remain proposed.
4. Expand implementation research to Coravel, Hangfire, Wolverine, FastEndpoints.JobQueues, TickerQ, and Quartz.NET, including upstream reasoning about performance, security, and maintenance.
5. Continue at the internal engineering-design stage. Do not restart discovery or re-present the established execution flow as a new proposal.

## Actual unfinished engineering deliverables

| Deliverable | Required detail beyond the earlier discussion |
|---|---|
| Internal contracts | Exact request/result records for reserve, begin execution if separate, renew, finalize, recover, cancel, and manual retry; atomic boundaries and stale/idempotent/ambiguous outcomes |
| PostgreSQL design | Column types, keys, constraints, indexes, transition SQL, permit representation, lock order, isolation, time authority, and transaction-failure handling |
| State machine | Decide how reservation versus invocation is recorded, when budgets are consumed, and how competing cancellation/completion/recovery commands serialize |
| Worker implementation | Hosted services, per-pool task ownership, execution registry, batched renewal, bounded completion handling, cached DI pipeline factories, shutdown ownership, and notification reconnect behavior |
| Driver matrix | Required semantic operations, supported topology, transaction capabilities, and explicit durability differences; no universal SQL algorithm |
| Scheduling/scaling | Occurrence-key construction, cursor advancement, bounded misfire catch-up, DST mapping, live overrides, fairness algorithm, capacity measurements, and scaling stability |
| Validation plan | Deterministic races and fault injection mapped to each invariant; process-crash scenarios, schema-upgrade tests, and workload-defined performance measurements |
| Implementation backlog | Concrete work items, dependencies, acceptance gates, and first PostgreSQL milestone; runtime implementation begins only after architecture review |

The [internal storage command protocol](architecture/storage-command-protocol.md) and [PostgreSQL transaction design](architecture/postgres-storage-design.md) are now drafted for review. They cover exact execution commands, schema/index shapes, transaction ordering, commit ambiguity, and planned race tests. Their start-based accounting and counted-permit algorithm remain proposals, with no runtime implementation or benchmark validation.

The updated [storage review inputs](storage-design-inputs.md) accept the seven-state lifecycle and Begin-based accounting as the working direction and add readiness, compatibility, saturation, and rate-limit concerns. The [worker-engine design](architecture/worker-engine-internals.md) now carries these into service/task ownership, local tickets, the execution registry, renewal/finalization, middleware/DI lifetime, and shutdown. It preserves the review as written and explicitly distinguishes adopted recommendations, refinements, and deferred storage additions.

The [capacity-control design](architecture/capacity-control.md) and [scheduling design](architecture/scheduling-internals.md) now specify fairness/aging, rate admission, scaling stability, occurrence identity/materialization, misfires, and DST. They explicitly extend Begin with committed rate deferral and introduce scheduler commands; the earlier protocol remains a baseline, not an assertion that these additions already exist.

The [driver matrix](architecture/driver-capabilities.md) and [contract suite](architecture/driver-contract-suite.md) now define driver/profile capabilities and their validation gates. The user's added BullMQ research request is addressed by a [pinned implementation review](architecture/bullmq-redis-review.md). Redis script publication/recovery and acknowledged-history preservation remain explicit feasibility gates, not verified features. The matrix proposes a common millisecond time quantum and driver-specific exact encodings; this is a new reviewable cross-driver decision.

The [public API/configuration contract](architecture/public-api-contract.md) now consolidates the original handler/dispatcher baseline with these internals. The initial API draft is archived, and its former path points to the current proposal. The [dispatch wire contract](architecture/dispatch-wire-contract.md) defines prepared replay, effective-intent comparison and canonical fingerprints; exact codec/default choices remain reviewable proposals.

The [management/dashboard design](architecture/management-and-dashboard.md) now specifies scoped authorization, Basic/host/OIDC integration, browser antiforgery, safe projections, revisioned commands and atomic administrative audit. Its pinned InertiaCore candidate review identifies adapter/client compatibility and embedded-host isolation gates; at that stage no adapter had been selected or tested. The final closure now selects Inertia.Net 1.5.3; runtime tests remain pending. The [observability/operations design](architecture/observability-and-operations.md) separates execution from settlement, bounds metric cardinality, distinguishes local from application-wide measurements, and defines role readiness and operational procedures. These remain proposals with unexecuted acceptance cases.

The [implementation backlog](implementation-backlog.md) now orders 30 work items by dependency and acceptance evidence. The [architecture acceptance checklist](architecture-acceptance.md) tracks 14 engineering decisions, required reconciliation of layered contracts, and gates separating first PostgreSQL execution from full v1 release. The platform portion of D-01 is settled below; other choices/evidence remain open. Redis and Inertia feasibility spikes are scheduled early. No runtime gate has been accepted and no implementation has started.

The user subsequently selected **.NET 10+ with current/newer C#**. The [platform/package foundation](architecture/platform-and-packages.md) records that accepted floor, proposes the compatible stable-language/build policy and refines package boundaries into Contracts/core/providers/optional web/dashboard/testing. SDK/provider versions and compatibility tests remain open; the platform floor is no longer an unanswered question.

The [identity/configuration/serialization decision packet](architecture/identity-and-serialization.md) now provides concrete D-03/D-04 recommendations: explicit names through attributes or fluent, deterministic configuration, low/medium/high v1 lanes, a strict versioned JSON profile, limits and replay behavior. Two cross-cutting refinements are reflected in the protocol/PostgreSQL/worker drafts: preserve original payload bytes and include serializer profile ID/version in claim capabilities. These recommendations and SER-01–12 cases are not approved/validated implementation claims.

The [execution defaults/timing packet](architecture/execution-defaults-and-timing.md) now proposes concrete D-05 values, retry/abandonment arithmetic, cancellation-grace abandonment, separate claim/Begin/Finish budgets and absolute shutdown phases. It adds explicit evaluator/jitter and unstarted-retry policy to generation/wire records. The values and TIME-01–13 cases remain unvalidated recommendations.

The [consolidated PostgreSQL contract](architecture/postgres-schema-and-operations.md) and [upgrade protocol](architecture/postgres-upgrades-and-compatibility.md) now develop D-06/D-13: full record inventory, store/application transaction gates, explicit operation/effect versions, consolidated lock order, audit/retention separation and mixed-version migration rules. Rate/schedule duration fields now use milliseconds. PGX-01–09 and UPG-01–10 remain unexecuted; no actual legacy database or migration has been invented.

The user subsequently selected **the latest Npgsql driver**. The [provider dependency plan](architecture/provider-dependency-plan.md) records the current stable 10.0.3 baseline, a stable-version recheck/pin policy, recommended SQLite/Redis candidates, PostgreSQL targets and connection/transaction ownership. Driver selection does not certify commit recovery, native loading or Redis persistence; DEP-01–08 remain unexecuted.

The [capacity defaults/budgets packet](architecture/capacity-defaults-and-budgets.md) now develops D-07 into concrete recommendations: fixed 4 by default, explicit auto bounds, shared attempt limits, queue/lane fairness, rate-policy bounds/edit freeze, pressure gates and partitioned PostgreSQL I/O. The initial scaler now uses local occupancy plus allocated due backlog; the earlier arrival-rate term is deferred. Local overload is a typed known-not-submitted API error. Arithmetic-only models were checked; CAP-01–12 remain unexecuted against Runwick.

The [scheduler evaluator/defaults packet](architecture/scheduler-evaluator-and-defaults.md) now develops D-08. It recommends Cronos 0.13.0 for nominal calendar matching, NodaTime 3.3.7 for mapping, and a hashed TZDB 2026f artifact. Pinned-source inspection exposed Cronos's AND/day and built-in DST differences, addressed by explicit OR and UTC-interval adapters. Defaults, bounded planning, profile upgrades and SCHED-01–15 are specified. The initial schema adds the zone artifact hash and a revision-checked evaluation-failure command. Prepared cutovers keep evaluation outside write locks. System-zoneinfo fixture checks used 2026e and are not pinned-adapter/runtime validation.

The six-item closure is complete in the [architecture baseline](architecture-baseline.md): outbox, Redis, dashboard integration, management/retention defaults, performance acceptance and consolidation. There is no further scheduled planning item.

## Document reading order

1. This continuity record and the original handoff establish provenance and current planning position.
2. `architecture-plan.md` records scope and the engineering backlog.
3. `implementation-research.md` supplies inspected upstream evidence; it does not imply every observed approach is appropriate for Runwick.
4. `architecture/storage-command-protocol.md` and `architecture/postgres-storage-design.md` develop the next level of internal detail; choices marked as proposed still require review.
5. `storage-design-inputs.md` records the updated review; `architecture/worker-engine-internals.md` applies it to runtime ownership and records outstanding storage additions.
6. `architecture/capacity-control.md` and `architecture/scheduling-internals.md` extend the worker/storage design with explicit proposed admission and scheduler operations.
7. `architecture/driver-capabilities.md`, `architecture/driver-contract-suite.md`, and `architecture/bullmq-redis-review.md` define profile-specific obligations and source evidence; no driver is implemented/certified yet.
8. `public-api-proposal.md` points to `architecture/public-api-contract.md` and `architecture/dispatch-wire-contract.md`, the current consolidated API proposals. `history/public-api-initial-draft.md` preserves the superseded alternatives.
9. `uniqueness-and-execution.md` is a historical behavioral draft; its five-state model, reservation-counted budget and numerical defaults must not override the internal designs or consolidated API.
10. `architecture/management-and-dashboard.md` and `architecture/observability-and-operations.md` develop authorization, audit, query/transport safety, adapter integration, telemetry and operational acceptance cases.
11. `implementation-backlog.md` and `architecture-acceptance.md` turn the designs into dependent work items, review decisions and evidence gates; use them to track the current planning position.
12. `architecture/platform-and-packages.md` records the user-selected .NET 10+ floor and proposed language/build/package policy; it refines the earlier package options.
13. `architecture/identity-and-serialization.md` develops D-03/D-04 into concrete recommendations and reconciles serializer-profile compatibility and exact payload preservation across the internal contracts.
14. `architecture/execution-defaults-and-timing.md` develops D-05 into proposed values, timing relationships, cancellation/cleanup/uncertainty limits and acceptance cases.
15. `architecture/postgres-schema-and-operations.md` and `architecture/postgres-upgrades-and-compatibility.md` consolidate D-06/D-13 and supersede earlier integration sketches where explicitly noted.
16. `architecture/provider-dependency-plan.md` records the selected Npgsql baseline, other provider candidates, connection ownership and dependency certification cases.
17. `architecture/capacity-defaults-and-budgets.md` develops D-07 and refines the earlier controller formula; use it for proposed numeric defaults, I/O budgets, pressure gates and CAP evidence.
18. `architecture/scheduler-evaluator-and-defaults.md` develops D-08 with package/source evidence, exact grammar, data/profile identity, bounded UTC mapping and SCHED cases.

19. [Architecture baseline](architecture-baseline.md) completes the six-item closure and links the outbox, Redis, dashboard, operations and performance decisions. This is the current entry point; earlier “next” discussions are history.
20. [Review 2026-10-30](review-2026-10-30.md) is the G-0 review of that baseline: corrections applied, recommendations R1–R5 awaiting decision, and implementation observations. [Runwick explained](runwick-explained.md) is the plain-language walkthrough of the whole design for newcomers.
