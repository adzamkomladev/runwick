# Dependency-ordered implementation backlog

Status: implementation plan following the [completed six-item architecture baseline](architecture-baseline.md), 2026-10-29. SP-001 has its planning deliverable ready for review; no runtime implementation item has started and no runtime gate has passed. The 30-item dependency graph below is unchanged. Read alongside the [architecture acceptance checklist](architecture-acceptance.md).

## 1. Execution rules and milestone boundaries

The first delivery is a PostgreSQL execution milestone proving dispatch, ownership, Begin accounting, middleware/handler invocation, settlement and recovery under a declared persistence profile. It is not a completed v1 release. Distributed rates, scheduling, additional drivers, management, dashboard and operational gates remain in the delivery plan.

Each item produces an acceptance report: design revision, implementation commit, environment/profile, named cases, results, limits and supporting artifacts. Status progresses through not-started, in-progress, review-ready and accepted only with evidence. Planned tests, compiled examples and real-store fault results are different evidence types.

Dependencies are acceptance prerequisites; independent design work can overlap. Feasibility spikes may proceed after architecture review permits their scope. A failed spike returns to its decision; it does not silently remove a feature or weaken a guarantee. Runtime code does not start merely because this backlog exists.

## 2. Proposed source and package boundaries

The user selected .NET 10+. The [platform/package foundation](architecture/platform-and-packages.md) specifies initial net10.0/C# 14, compatible newer stable versions and a concrete package graph. Review D-02 and pin D-01's SDK/dependencies before creating projects. Working dependency direction:

| Boundary | Responsibilities | Dependencies |
|---|---|---|
| Contracts | IQueueJob, handler/context contracts, attributes, public values | No database provider or ASP.NET requirement |
| Core | Configuration, dispatch preparation, execution, scheduling/capacity algorithms, management application contracts | Contracts and selected hosting/DI abstractions; no concrete driver/dashboard |
| PostgreSQL / SQLite / Redis | Commands, projections, schema management, capability/profile checks | Core contracts and own provider |
| ASP.NET management | Authentication/policies, request validation, endpoint/result mapping | Core management; no worker startup requirement |
| Dashboard | Inertia integration and prebuilt assets | ASP.NET management; optional in producer/worker hosts |
| Outbox adapter | Source-store enlistment, relay/recovery, declared deduplication horizon | Prepared dispatch and selected source provider |
| Tests and samples | Independent model, contract suite, fault fixtures, workloads and deployment examples | Relevant components; never runtime dependencies |

The platform document refines these boundaries into proposed Runwick.Contracts, Runwick, provider, ASP.NET, Dashboard and Testing packages. Keep hosting/scheduling as internal core modules. Producer/worker installations require no EF, web server or frontend toolchain. The optional packages are Runwick.Outbox, Runwick.Outbox.Postgres and Runwick.Outbox.Sqlite; their [source adapters and delivery contract](architecture/transactional-outbox.md) are selected.

## 3. Foundation and first PostgreSQL milestone

| ID | Deliverable | Depends on | Acceptance evidence |
|---|---|---|---|
| SP-001 | Review the completed architecture baseline, reconciled protocol/schema and fixed workloads | None | G-0 disposition recorded; D-01–D-06 and D-13 have first-milestone decisions; later gates remain explicit |
| SP-002 | Solution/package boundaries, pinned dependencies, build/CI entry points and contract compilation samples | SP-001 | Attribute/fluent examples compile; producer installation excludes web/provider implementation dependencies; graph is acyclic; framework matrix recorded |
| SP-003 | Independent state model, fake-time engine fixture and real-store fault/barrier harness | SP-002 | Arranges commit/reply loss and competing connections; snapshots distinguish recovering from empty; fixture rejects illegal transitions |
| SP-004 | Immutable registration/policy registry, upgrades, serializer profile and prepared dispatch codec | SP-003 | Identity conflicts rejected; snapshots survive caller/config changes; wire vectors pass; no runtime type loading from untrusted payload metadata |
| SP-005 | PostgreSQL schema/migrations, receipts, capability/version handshake and transaction runner | SP-003 | Fresh/interrupted initialization, isolation, exact encodings, fresh-time checks and lock order tested; incompatible writers rejected |
| SP-006 | Dispatch, **batch dispatch** (`DispatchManyAsync` / `PrepareMany` / `StageManyAsync`, added 2026-10-30), active/window uniqueness, receipt resolution and transaction staging | SP-004, SP-005 | Applicable CMD/UNI/TX cases pass; lost COMMIT response remains resolvable; rollback removes job/bindings; staging never implies commit; a batch is all-or-nothing with per-ordinal receipt results, duplicate keys inside one batch and across concurrent batches resolve without deadlock, `ContinueOnConflict` commits the non-conflicting remainder atomically |
| SP-007 | Reserve, multi-scope permits and bounded capability-aware candidate scanning | SP-006 | Independent clients prove all-or-none permits and exclusive valid ownership; unsupported versions stay waiting; contention cost recorded |
| SP-008 | Begin, Renew, Finish, Recover, retry eligibility and abandonment accounting; **ReleaseReservation** (neutral, ownership-checked release of a reserved-not-begun attempt: closes it as `released`, deletes its exact permits, restores the prior waiting state, charges neither execution nor abandonment budget — accepted 2026-10-30, R2) | SP-007 | Applicable RUN/OWN/END cases pass; replay does not double-charge; expired owners cannot alter replacements; replay preserves retry decision; graceful drain releases unbegun reservations immediately instead of waiting for lease expiry |
| SP-009 | Supervisor, tickets/registry, per-attempt arbiter, DI/middleware, watchdog, completion and drain | SP-004, SP-008 | Worker matrix passes with barriers; user work cannot run inline on lease coordinator; uncertain claims/completions bound admission |
| SP-010 | First PostgreSQL end-to-end execution/fault report | SP-003, SP-006, SP-009 | G-1 passes for stated cases/profile: rollback, death before/after Begin, stale Finish, dropped replies and ignored cancellation; limits explicit |

SP-005/008 include version negotiation for later rate-aware Begin; do not ship an unversioned policy bypass. SP-009 includes basic diagnostic boundaries/readiness; SP-022 completes instrumentation. Detailed cancellation/manual retry and all five deployments follow next.

## 4. Distributed controls, capacity and scheduling

| ID | Deliverable | Depends on | Acceptance evidence |
|---|---|---|---|
| SP-011 | Revisioned cancel/retry/queue/scope controls, trusted actor and atomic successful-operation audit; **transactional completion** (`CompleteInTransactionAsync` for PostgreSQL/SQLite, R6 accepted 2026-10-30) | SP-010 | Cancel/Finish, retry/uniqueness, pause/Reserve races; one receipt/audit per successful command; generation conflicts; no unsafe live membership edits; caller rollback leaves the attempt owned and recoverable, caller commit finalizes exactly once, lost commit reply resolves by receipt without re-running the handler |
| SP-012 | Multi-lane readiness, saturation classification, bounded presence/capabilities and fairness | SP-011 | Coordinated idle probe respects budget; queue-wide saturation avoids repeated scans; mixed-resource lanes progress; stale worker presence expires |
| SP-013 | Rolling-window rate records and versioned Begin/defer transaction | SP-011 | RATE-01–04 and CMD-06; deferral closes attempt/releases permits without execution/abandonment charge; edit freeze tested; old writers cannot bypass rates |
| SP-014 | Bounded local autoscaler and revisioned overrides | SP-012, SP-013 | Simulations and real replicas cover hysteresis, target decreases, stale inputs, shared demand estimates, outage suppression and override expiry |
| SP-015 | Versioned recurrence/time-zone evaluator and planner | SP-004 | D-08 vectors for cron grammar, gaps/folds, UTC identity and zone-data updates; bounded planning slices |
| SP-016 | Schedule records, atomic materialization, misfire episodes and manual triggers | SP-011, SP-013, SP-015 | SCH-01–05 and scheduler matrix; occurrence/job/cursor/audit consistency; edit/pause cannot duplicate or erase unprocessed work |
| SP-017 | PostgreSQL/SQLite outbox adapters, relay and ReceiveOutbox destination operation | SP-006 | OUT-01–02, OUTBOX-01–07 and transaction cases pass; immutable delivery identity/cutoff, lost delivered-mark recovery and source lease fencing |

Verify all five hosting modes on PostgreSQL in SP-011/012 and repeat relevant scenarios after rate/scheduler changes. Ceilings constrain valid grants; local pool capacity and noncooperative expired handlers remain separate facts. Scaler acceptance cannot claim strict global fairness or infrastructure autoscaling.

## 5. Driver feasibility and management

| ID | Deliverable | Depends on | Acceptance evidence |
|---|---|---|---|
| SP-018 | Early Redis prototype: same-slot layout, interrupted-write recovery, publication and persistence confirmation | SP-003, SP-004 | Failures at every write/publication boundary; authoritative reads hide partial state; AOF-always acknowledgement, fresh replay confirmation, reconnect and exact counters tested; selected profile passes or its specified failure disposition applies |
| SP-019 | SQLite provider, bounded executor and same-host transaction/query implementation | SP-010, SP-011, SP-013, SP-016 | Real multi-process contention, rollback, checkpoint/read pressure and provider threading; unsupported topology rejected; full certification awaits G-4 |
| SP-020 | Authorized management services/HTTP, bounded projections and operation resolution | SP-011, SP-012, SP-014, SP-016 | Applicable MGMT cases pass: replay reauthorization, scoped queries, audit failure, Basic/cookie CSRF; standalone host runs without workers |
| SP-021 | Dependency-aware retention/cleanup and detail-expiry projections | SP-011, SP-013, SP-016 | RET-01, MGMT-11 and telemetry retention case; resumable bounded cleanup preserves required receipts, grants, uniqueness, occurrences, intents and fences |
| SP-022 | Trace/metric schema, redaction, role health and bounded samplers | SP-009, SP-012, SP-014, SP-016 | Telemetry cases; execution/settlement distinction; no exporter dependency; bounded cardinality; stale global snapshots never become zero |
| SP-023 | Selected Inertia.Net 1.5.3/client integration and mount-isolation spike | SP-002 | D-10 records pinned versions and two-host/existing-host, prop, path, auth, CSRF, encoding, CSP and asset-upgrade evidence |
| SP-024 | Embedded/standalone bundled dashboard | SP-020, SP-022, SP-023 | MGMT-01–12 across response paths; jobs/attempts, queues, workers/capacity, schedules, scaling, settings/retention usable; no sensitive props/history leakage |
| SP-025 | Full Redis provider using accepted prototype design | SP-013, SP-016, SP-018, SP-020, SP-021 | Execution/schedules/audit/retention share publication protocol; indexed bounded projections; certification remains pending SP-028 |

Run SP-018 and SP-023 early after their dependencies and review gates; they need not wait for PostgreSQL scheduling. Redis failure may require representation/contract review before SP-025. BullMQ research is evidence, not a complete implementation of Runwick's command contract.

SP-019 starts the SQLite port; later management/retention/telemetry cases also apply before certification. PostgreSQL needs those later schema/query changes too. No driver gets an exemption because it was implemented first.

## 6. Compatibility, certification and release

| ID | Deliverable | Depends on | Acceptance evidence |
|---|---|---|---|
| SP-026 | Upgrade/rollback matrix, migration tooling and restore/drain procedures | SP-005, SP-013, SP-016, SP-021 | Supported old/new readers/writers versus schema/protocol; interrupted migration; no policy bypass; rollback/history-discontinuity limits documented |
| SP-027 | Integrated deployment/security/operations scenarios and samples | SP-014, SP-016, SP-017, SP-019, SP-022, SP-024, SP-025, SP-026 | Five deployments on supported profiles, authentication modes, capacity/schedules and drain/outage; examples compile/run; outbox scope follows D-12 |
| SP-028 | Driver/profile conformance reports and complete coverage | SP-003, SP-027 | Every mandatory applicable case has a result; unsupported capabilities explicitly rejected; no hidden skips; G-2–G-5 evidence linked |
| SP-029 | Performance, contention, recovery and long-run resource qualification | SP-010, SP-014, SP-016, SP-021, SP-022, SP-024, SP-025 | D-14 thresholds set before qualification; raw latency/margin/memory/write costs and persistence settings; dashboard/telemetry/cleanup preserve safety |
| SP-030 | Release packaging, guidance and acceptance | SP-028, SP-029 | G-6 passes; defaults/limits match code; package/licensing/dependency checks; no unsupported durability or exactly-once claims |

SP-026 specifies its matrix early under D-13 and completes evidence as components become executable. SP-029 workloads run incrementally from SP-010; final placement is qualification, not postponing performance discovery. Do not mark SP-017 not-applicable to pass SP-027 without an explicit D-12 delivery-scope decision.

## 6b. Hosted-store and CLI additions (product decision 2026-10-30)

Added after the user selected a hosted job store as the commercial product; see [commercial strategy](commercial-strategy-and-seams.md). These follow the original 30 items and do not alter SP-001–010.

| ID | Deliverable | Depends on | Acceptance evidence |
|---|---|---|---|
| SP-031 | Versioned protocol additions: `ReserveAndBegin/1`, batched Reserve, batched Renew with independent per-lease outcomes, `RerunSucceeded` (new generation of a succeeded job, confirmed 2026-10-30), `job_generations.created_reason`, `application`-kind concurrency scope, typed `QuotaExceeded` known-not-applied rejection, push-assignment hint channel alongside pull | SP-008, SP-011, SP-013 | Combined operation preserves Begin accounting and rate semantics; batch failures never roll back unrelated healthy leases; old operation versions cannot bypass the application ceiling; quota rejection produces no receipt and a stable code; generation reason is immutable and audited; push hints never bypass Reserve validation |
| SP-032 | Management API as a public versioned surface; `Runwick.Cli` (`dotnet tool` + single-file) over it with human output, `--output json`, and a distinct `--agent` contract (schema-versioned envelopes, JSON Lines streaming, stable error codes with hints, `nextActions`, `--agent describe` manifest); `--expected-revision` and `--command-id` on every mutation | SP-020 | CLI drives every MGMT control against self-hosted and reference server; replayed CLI commands resolve by receipt; human and agent outputs generated from one command model; agent envelopes validate against their published schema; no credential or payload leakage in output by default |
| SP-033 | Cloud wire contract: gRPC operation set mirroring the storage command protocol, pull (long-poll) and push (stream) delivery modes, per-environment API-key scopes, tenant/store binding in every command, reconnect rules | SP-031, SP-028 | Contract vectors for every operation; both delivery modes pass the same ownership/admission cases; disconnect at every boundary resolves old commands without fresh IDs; cross-tenant ISO cases pass |
| SP-034 | `Runwick.Cloud` client driver passing the applicable shared contract suite against a reference server | SP-033 | Driver report per §7 with topology "hosted"; `StageAsync` explicitly unsupported; outbox cases pass as the dispatch path |

## 7. Validation traceability

Test IDs are scoped to their source. Driver OBS-01 tests notification loss; telemetry OBS-01 tests missing exporters. Record document plus case ID rather than merging an ID-only tracker.

| Evidence source | Work items |
|---|---|
| [Driver suite](architecture/driver-contract-suite.md): CMD/ADM/RUN/OWN/END/UNI/ISO/ENC/CFG/TX | SP-003–011 and all providers in SP-028 |
| Driver suite: RATE/SCH/OUT/RET/OBS and provider fault sections | SP-012–019, SP-021, SP-025–028 |
| [Worker matrix](architecture/worker-engine-internals.md) | SP-009, SP-010, SP-012, SP-027 |
| [Capacity validation](architecture/capacity-control.md) | SP-012–014, SP-029 |
| [Capacity CAP-01–12](architecture/capacity-defaults-and-budgets.md) | SP-005, SP-009, SP-012–014, SP-027–029 as mapped per case |
| [Scheduler validation](architecture/scheduling-internals.md) | SP-015–016, SP-027 |
| [Scheduler SCHED-01–15](architecture/scheduler-evaluator-and-defaults.md) | SP-015–016, SP-026–029 |
| [API](architecture/public-api-contract.md) and [wire vectors](architecture/dispatch-wire-contract.md) | SP-002, SP-004, SP-006, SP-017, SP-027 |
| [Identity/serialization SER-01–12](architecture/identity-and-serialization.md) | SP-004–007, SP-019, SP-025, SP-028–029 |
| [Execution TIME-01–13](architecture/execution-defaults-and-timing.md) | SP-004, SP-008–011, SP-026–029 |
| [PostgreSQL PGX-01–09](architecture/postgres-schema-and-operations.md) and [UPG-01–10](architecture/postgres-upgrades-and-compatibility.md) | SP-005–008, SP-011–016, SP-020–021, SP-026–029 |
| [Provider DEP-01–08](architecture/provider-dependency-plan.md) | SP-002–009, SP-018–019, SP-022, SP-025–029 as mapped per case |
| [Management MGMT-01–12](architecture/management-and-dashboard.md) | SP-011, SP-020–021, SP-023–025, SP-027 |
| [Outbox OUTBOX-01–07](architecture/transactional-outbox.md) | SP-017, SP-025–028 |
| [Redis REDIS-01–06](architecture/redis-design-closure.md) | SP-018, SP-025, SP-028–029 |
| [Dashboard DASH-01–07](architecture/dashboard-integration-closure.md) | SP-023–024, SP-027 |
| [Operations OPS-01–06](architecture/management-retention-defaults.md) | SP-011, SP-020–022, SP-026–029 |
| [Performance P-01–10](architecture/performance-acceptance-plan.md) | SP-010, SP-017–018, SP-029 |
| [Telemetry OBS-01–11](architecture/observability-and-operations.md) | SP-009, SP-021–022, SP-027, SP-029 |

Retain exact case sets and contract versions in reports. When a scenario spans items, link integration proof; isolated unit tests do not prove the combined behavior.

## 8. Planning complete; implementation entry

All six remaining planning outputs are complete in the [architecture baseline](architecture-baseline.md). No additional planning chapter is required. SP-001 now reviews that concrete deliverable; SP-002 begins scaffolding when implementation is requested. The first runnable milestone is PostgreSQL dispatch → reserve → execute → finish/recover. Early Redis/dashboard experiments have specified failure paths, and all remaining work is in the existing 30-item graph.

Recheck dependency releases when pinning executable projects. Prototype results, compiled examples, real-store faults and benchmarks remain unexecuted implementation evidence; they are not further planning deliverables. Preserve the user's [storage review](storage-design-inputs.md).
