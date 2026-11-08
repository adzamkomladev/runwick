# 00 — Runwick specification overview

**Sources:** docs/architecture-baseline.md, docs/architecture-plan.md, docs/architecture-acceptance.md, docs/runwick-explained.md, docs/review-2026-10-30.md, docs/v1-delivery-plan.md, docs/architecture/platform-and-packages.md, docs/architecture/provider-dependency-plan.md.

## Changes (2026-11-08, SP-023)

- §6 `Runwick.Dashboard` pin: Inertia.Net 1.5.3 → 1.7.0. 1.5.3 had one option set, version, root view and state store per app, so a dashboard could not be mounted inside a host that already uses Inertia. 1.7.0 adds named mounts (`AddInertia(name, ...)`, `UseInertia(name)`), and each dashboard mount is one of them. What 1.7.0 still leaves to Runwick is listed in [docs/reports/sp-023.md](../reports/sp-023.md).

## Changes (2026-10-31, SP-016)

- Schedules (registration, materialization, misfire and catch-up, pause/resume, edits, manual triggers) are specified in [13-scheduler.md](13-scheduler.md), built from schema revision 6. §10 index gains 13.

## Changes (2026-10-31, SP-014)

- The local autoscaler (`AutoConcurrency(min, max)`, shared demand estimate, `SetPoolOverride/1`, scaling history) is specified in [11-autoscaler.md](11-autoscaler.md), built from schema revision 7. §10 index gains 11.

## Changes (2026-10-31, SP-013)

- Rate admission (exact rolling window, rate-aware Begin/1, T20 deferral, `ActivateCapabilities/1`, `EditRatePolicy/1`, edit freeze) is specified in [10-rate-admission.md](10-rate-admission.md), built from schema revision 4. §10 index gains 10.

## Changes (2026-10-31, SP-011)

- M3 begins. Revisioned controls, management audit, the event cursor and transactional completion (R6) are specified in [08-controls-and-completion.md](08-controls-and-completion.md), and the evaluator in 12 (SP-015). §10 index gains 08, 12 and 15 (G-2 test plan).
- "Not in M2" items that SP-011 delivers (Cancel/ManualRetry/RerunSucceeded, the public SetQueueState, scope capacity edits, audit, R6) are built from schema revision 2 on.

## Changes (2026-10-30)

| # | Change | Kind |
|---|---|---|
| C-1 | `docs/spec/` is made the single normative source; every other file under `docs/` becomes design history (§2). | Precedence decision |
| C-2 | Review decisions R1–R7 and batch dispatch are folded in as normative (R1 receipt-free Renew, R2 ReleaseReservation, R3 two-digest fingerprint, R4 controller boundary rule, R6 transactional completion, R7 hosted-store seams). R5 (dashboard transport) is out of M2 scope. | Contradiction resolved (later wins) |
| C-3 | runwick-explained.md's single-digest fingerprint and its `_runwick.CompleteInTransactionAsync(tx, ct)` form that ignores the returned token are superseded by the R3 two-digest fingerprint and the R6 `StagedCompletion` forms (01-public-api §13). | Contradiction resolved |
| C-4 | postgres-upgrades-and-compatibility.md's "no server/driver version selected" is superseded: Npgsql 10.0.3, PostgreSQL 16–18 with 18 as the reference major (§7). | Contradiction resolved |
| C-5 | Package list extended with `Runwick.Cli` and `Runwick.OpenTelemetry` (v1-delivery-plan SP-002/SP-022, review R7) and `Runwick.Cloud` (v1.x). None are built in M1/M2. | Consolidation |
| C-6 | Test stack fixed: xunit v3, Testcontainers.PostgreSql, `RUNWICK_TEST_PG` escape hatch, `RUNWICK_TEST_PG_IMAGE` (default `postgres:18`). | Implementation choice |
| C-7 | runwick-explained.md's "`reserved → pending` on recovery" is superseded: recovery of a reserved-not-begun attempt returns the job to `pending` only when the generation's started count is 0, else to `retrying` (03-storage-protocol). | Contradiction resolved |

## 1. What Runwick is

Runwick is a free, open-source, monolith-first .NET background-job library. One engine and one set of job/handler contracts serve a single application with embedded workers and distributed production deployments alike. Job definitions and execution semantics do not change between deployment modes.

The durable store is the single source of truth. Workers do not talk to each other; they talk to the store. PostgreSQL is the reference production driver; SQLite (same-host) and Redis (explicit persistence profile) follow after M2.

Delivery semantics, stated exactly as public documentation must state them:

> Runwick is at-least-once. Exactly-once is available for effects inside the same database via transactional completion; for everything else, use idempotency keys.

Transactional completion (R6) ships in M3 (SP-011); the sentence is the v1 release wording, not an M2 claim.

## 2. Precedence

1. The files in `docs/spec/` are **normative**. An implementer codes from them alone.
2. Every other file under `docs/` (architecture packets, baseline, review, delivery plan, explained, history) is **design history**. It explains why; it does not override a spec file. Where a history document disagrees with a spec file, the spec file wins.
3. The roadmap and task/milestone tracking live in `docs/v1-delivery-plan.md`. It is the authority for *what is scheduled when*, not for behavior.
4. Within `docs/spec/`, a subject-specific file wins for its subject: 01 for public API shapes, 02 for serialization/fingerprints/wire encodings, 03 for the storage protocol, 04 for PostgreSQL SQL/schema, 05 for the worker engine, 06 for numeric defaults, 07 for test-case definitions. A conflict between two spec files is a defect: fix it in both files and record the fix in each file's Changes section, dated.
5. Each spec file starts with its title, a **Sources** line and a **Changes** section listing every contradiction resolved and every implementation choice made, dated.
6. A spec changes only for a concrete contradiction, failed prototype/test, measured budget miss or explicit changed requirement. Record the affected invariant, evidence and smallest resulting change. Speculative features are not added.

## 3. Milestone scope of this specification

The specification covers **M1 + M2**, i.e. everything needed to pass gate **G-1** (PostgreSQL execution). The roadmap beyond G-1 is in `docs/v1-delivery-plan.md`.

| Milestone | Gate | Tasks | Outcome |
|---|---|---|---|
| M1 — Foundation | — | SP-002 scaffolding; SP-003 test harness; SP-004 registry/codec/fingerprint/prepared envelope; SP-005 PostgreSQL schema, migration runner, gates, receipts, transaction runner | Compilable packages, harness, registry/codec, schema |
| M2 — PostgreSQL execution | **G-1** | SP-006 dispatch (single, batch, prepared, staged, uniqueness, receipts, `local_capacity_exceeded`); SP-007 Reserve; SP-008 Begin/Renew/Finish/Recover/ReleaseReservation; SP-009 worker engine; SP-010 end-to-end fault report | First runnable system: dispatch → reserve → begin → execute → finish/recover |

G-1 passes when the SP-010 report shows that rollback, death before/after Begin, stale Finish, dropped replies, ignored cancellation and every shutdown phase preserve the specified history, and first measurements (P-01, P-02, P-08; contention, query plans, idle cost, renewal margin) are recorded with milestone limits explicit. 07-test-plan-g1 lists the cases.

### In M2

- Contracts, registration builder, attributes and fluent twins, payload versions/upgrades, middleware, `IJobContext`.
- `runwick.json` v1 serializer profile, canonical encoding, two-digest window fingerprint, prepared-dispatch export envelope.
- Single and batch dispatch, `IPreparedJobDispatcher` (Prepare/Dispatch/Resolve/Import), PostgreSQL `StageAsync`/`StageManyAsync` on a caller transaction.
- Active and fixed-window uniqueness, `PolicyDrift`, command receipts with operation-class retention.
- Reserve with capability-tuple filtering (bounded to 512 tuples per probe/Reserve), `SKIP LOCKED`, all-or-none concurrency permits for queue/job-type/resource scopes, resource fencing.
- Begin (execution accounting), Renew (no receipt), Finish, Recover, ReleaseReservation.
- Worker hosted service: local tickets, weighted lane selection, attempt runner, lease coordinator and watchdog, completion coordinator, recovery loop, staged shutdown, derived pressure gates, basic health.
- Minimal scope surface: `runwick.Queue(name, q => q.MaxConcurrent(n))`, `ConfigureJob<T>().MaxConcurrent(n)`, `ConfigureJob<T>().PerResource(selector, capacity)`; scopes are seeded if absent at startup.
- `queue_events` rows for `dispatched`, `policy_drift` and attempt lifecycle events (no heartbeat events).

### Not in M2 (later milestones; interfaces leave room)

| Feature | Milestone / task | M2 behavior |
|---|---|---|
| Cancel, ManualRetry, RerunSucceeded, queue pause/resume, scope policy edits, management audit | M3 / SP-011 | No public control API; schema columns/enums that need room are defined in 03/04 |
| Transactional completion `CompleteInTransactionAsync` (R6) | M3 / SP-011 | Types specified in 01 §13; not registered in M2 |
| Probe saturation evidence, presence subscriptions, "no capable worker" diagnostic | M3 / SP-012 ([09](09-readiness-and-presence.md)) | Probe/1, aging and cooldowns from Reserve replies only (05 §7–§8; corrected 2026-10-31, 09 C-1) |
| Rolling-window rate limits, rate-aware Begin/1 | M3 / SP-013 | A worker refuses to start against an application with rate policies enabled |
| Automatic concurrency (autoscaler) | M3 / SP-014 (delivered, 11) | — |
| Scheduler (cron, materialization, misfire) | M3 / SP-015–016 (delivered, 12, 13) | `IJobContext.Schedule` is set for scheduled jobs (13, Changes 2026-11-01) |
| Transactional outbox | M3 / SP-017 ([14](14-outbox.md)) | — |
| SQLite, Redis providers | M4 / SP-019, SP-025 (spike SP-018) | — |
| Management API, CLI, retention cleanup, telemetry package, dashboard | M4 / SP-020–024, SP-032 | — |
| Hosted store (`Runwick.Cloud`) | v1.x / SP-031–034 | — |

## 4. Roles

Roles are enabled by configuration, not by separate programs or packages. No deployment-mode flag is stored on jobs.

| Role | What it does | How it is enabled | M2 |
|---|---|---|---|
| **Producer** | Calls `DispatchAsync`/`DispatchManyAsync`/`StageAsync` to create jobs | Any host that calls `AddRunwick` with a storage provider | Yes |
| **Worker** | Reserves jobs from the store and runs handlers | `runwick.AddWorker(...)` | Yes |
| **Scheduler** | Materializes schedule occurrences into jobs | `runwick.AddScheduler()`; independent of workers | No |
| **Management** | Queries and revisioned controls (cancel, retry, pause, capacity) | `Runwick.AspNetCore` / management services | No |
| **Dashboard** | UI over the management backend, embedded or standalone | `Runwick.Dashboard` | No |
| **Store** | Durable truth: jobs, attempts, leases, permits, receipts, keys | Provider (`UsePostgres`) | Yes |

Rules:

- Installing the engine starts nothing: `AddWorker` and `AddScheduler` are explicit. A producer-only registration starts no worker or scheduler; standalone management starts neither implicitly.
- A producer needs the job contract assembly, not the handler assembly.
- Every process that shares a logical application uses the same application name, store identity/profile and job contracts.

## 5. Deployment modes

All five modes use the same engine, the same hosted worker service and the same job/handler contracts. Only the `AddRunwick(...)` configuration differs.

| # | Mode | Process roles |
|---|---|---|
| 1 | One application with embedded workers | One process: producer + worker |
| 2 | One application with an external worker | Application: producer only; separate process: worker |
| 3 | Multiple application replicas with embedded workers | Every replica: producer + worker |
| 4 | Application replicas with multiple external workers | Web tier: producer only; worker tier: worker only |
| 5 | Embedded and external workers together | Some replicas producer + worker; additional worker-only processes |

M2 supports all five structurally (same hosted service, store-side ownership). Demonstration of all five with independently configured roles is a G-2 criterion (SP-011).

## 6. Packages and allowed dependency edges

All packages target `net10.0` only (no .NET 8/9, .NET Framework or .NET Standard targets). Namespaces use the `Runwick` prefix.

| Package | Contents | Allowed direct Runwick dependency | Allowed external dependencies | Built in |
|---|---|---|---|---|
| `Runwick.Contracts` | `IQueueJob`, `IJobHandler<TJob>`, `IJobContext`, job metadata attributes, context value types | none | none (framework only) | M1 |
| `Runwick` | Dispatcher, preparation/recovery APIs, registration/configuration, worker engine, middleware, (later) scheduler/capacity/management services, storage extension contracts | `Runwick.Contracts` | Microsoft.Extensions abstractions/hosting/options/logging (exact packages pinned at SP-002) | M1–M2 |
| `Runwick.Postgres` | PostgreSQL commands/projections, migrations, profile checks, `IPostgresJobDispatcher` staging, (M3) `IPostgresJobCompletion` | `Runwick` | Npgsql 10.0.3 | M1–M2 |
| `Runwick.Testing` | Public application test helpers (registration, dispatch capture, controlled handler invocation) | `Runwick` | — | scaffolded M1 |
| `Runwick.Sqlite` | SQLite provider and staging | `Runwick` | Microsoft.Data.Sqlite 10.0.14 | M4 |
| `Runwick.Redis` | Redis provider | `Runwick` | StackExchange.Redis 3.4.20 | M4 |
| `Runwick.AspNetCore` | Auth/policy adapters, management endpoints, antiforgery, HTTP mapping | `Runwick` | ASP.NET Core shared framework | M4 |
| `Runwick.Dashboard` | Inertia server adapter, page projections, prebuilt frontend assets | `Runwick.AspNetCore` | Inertia.Net 1.7.0 (one named mount per dashboard mount; SPA mode, no SSR); bundled Inertia React/core 3.9.1, React 19.3.1 | M4 |
| `Runwick.Outbox` | Optional relay and delivery contracts | `Runwick` | — | M3 |
| `Runwick.Outbox.Postgres` | Source staging/relay persistence | `Runwick.Outbox` | Npgsql | M3 |
| `Runwick.Outbox.Sqlite` | Source staging/relay persistence | `Runwick.Outbox` | Microsoft.Data.Sqlite | M3 |
| `Runwick.OpenTelemetry` | `AddRunwickInstrumentation()`, views, cardinality limits | `Runwick` | OpenTelemetry SDK | M4 |
| `Runwick.Cli` | `dotnet tool` + single-file CLI over the public management API | (management API client) | — | M4 |
| `Runwick.Cloud` | Hosted-store client driver | `Runwick` | gRPC client | v1.x |

Dependency rules (enforced by SP-002 packed-graph inspection, DEP-01):

- The graph is acyclic and points toward the core. Core never references a concrete provider, web package or exporter.
- `Runwick.Contracts` has no ASP.NET, provider, EF, hosting or storage dependency.
- `Runwick` (core) has no ASP.NET Core (`Microsoft.AspNetCore.App`), provider, EF, Dapper, telemetry-exporter or Node dependency. Core emits through .NET diagnostic primitives (`ActivitySource`, `Meter`) only.
- Producer and worker installs (`Runwick` + one provider) pull no web, dashboard, EF or exporter dependency.
- Cronos 0.13.0 and NodaTime 3.3.7 (with the pinned TZDB 2026f artifact/hash) become internal dependencies of core when the scheduler lands (SP-015); `Runwick.Contracts` never exposes them. They are not referenced in M2.
- Frontend assets are built at package production; consumers never need Node/npm.
- The driver service-provider interface is public and versioned; being public does not establish third-party driver compatibility. Core and providers ship together and negotiate operation capabilities at startup.

## 7. Platform and dependency pins

| Item | Pin |
|---|---|
| Target framework | `net10.0` |
| Language | C# 14 (`LangVersion` set explicitly to `14.0`; never `latest`) |
| SDK | Latest stable .NET 10 SDK at SP-002, recorded in `global.json` with `allowPrerelease: false`, `rollForward: disable`; CI installs exactly that SDK |
| Build defaults | Nullable reference types enabled; deterministic builds; central package version management; pinned restore graph; no floating versions; reviewed analyzers; serialization/trimming/platform warnings are not suppressed to make builds pass |
| Npgsql | 10.0.3 |
| PostgreSQL server | Certification targets 16, 17, 18; **18 is the reference major** (minor versions at selection: 16.15, 17.11, 18.6). Baseline SQL stays compatible with 16. Exact images/digests pinned in the harness |
| Microsoft.Data.Sqlite / StackExchange.Redis | 10.0.14 / 3.4.20 (M4) |
| Test framework | xunit v3; Testcontainers.PostgreSql |
| Test database selection | `RUNWICK_TEST_PG` = a PostgreSQL connection string; when set, tests use it and start no container (no Docker required). Otherwise Testcontainers starts `RUNWICK_TEST_PG_IMAGE` (default `postgres:18`) |
| Host certification (initial) | .NET 10 on Linux x64 and Windows x64; exact OS/container versions recorded at SP-002 |

Version policy: recheck the latest stable releases at project creation and pin exactly; upgrades are explicit changes validated against provider contract, fault and wire-vector tests. A dependency release never silently changes payload profiles, command semantics or the .NET floor. Raising the minimum above .NET 10 is an explicit breaking release decision. No Native AOT or trimming-safety claim is made until published-application tests pass. NuGet name availability for `Runwick` is checked at SP-002.

## 8. Core vocabulary

| Term | Meaning |
|---|---|
| Application | Namespace inside a store (e.g. `customer-service`); every record and command is application-scoped |
| Store identity | UUID identifying one logical database/schema installation; bound into prepared commands |
| Job | Logical unit of work with a permanent `JobId`; identified durably by its stable job name, never a CLR type name |
| Generation | One budgeted lifetime of a job; created at dispatch (later: manual retry/rerun create new generations of the same job ID) |
| Attempt | One reservation of a job by a worker; carries a random ownership token and a lease |
| Execution | A committed Begin; `MaxExecutions` counts executions, not reservations |
| States | `pending`, `reserved`, `running`, `retrying`, `succeeded`, `failed`, `cancelled` |
| Lane | One `(queue, priority)` pair; priorities are exactly `low`, `medium`, `high` |
| Scope / permit | Distributed concurrency scope (`queue`, `job-type`, `resource`) and a lease-bound permit row in it |
| Fence | Monotonic counter value handed to each successive holder of a fencing-enabled scope |
| Command / receipt | Mutating storage command with operation ID, immutable body, absolute `NotAfter`; its receipt row is written in the same transaction as its effect (Renew has no receipt) |
| Ticket | Local in-memory slot held from claim until the attempt is retired |

## 9. Honesty rules — what the system must never claim

These are invariants for code, API results, logs, telemetry and documentation. A test that observes a violation fails the gate.

1. **No general exactly-once.** A handler may run more than once (crash after side effect, lease loss, redelivery). Exactly-once is claimed only for effects inside the same database as Runwick's tables via transactional completion (R6, M3). Uniqueness is insertion uniqueness, never an execution guarantee.
2. **No receipt before durability.** A `DispatchReceipt` is returned only after the configured acknowledgement profile is confirmed. An in-memory state change is not confirmation.
3. **Staging makes no commit claim.** `StagedDispatch`/`StagedBatch` (and later `StagedOutboxMessage`, `StagedCompletion`) report what was staged inside the caller's transaction; the caller owns the outcome. Runwick never commits, rolls back, reopens or retries a caller's transaction.
4. **Three answers to "did my write happen?"** — known applied, known not applied, outcome unknown — are always distinguished. `NotObserved` is never reported or treated as rolled back. Local overload (`local_capacity_exceeded`) is known-not-submitted and is never presented as a durable rejection or an unknown commit. A command is never re-issued under a fresh operation ID to hide uncertainty. An expired receipt yields an explicit unknown, never an invented success or failure.
5. **Cancellation is cooperative.** No API, log or result claims that cancellation, timeout, lease expiry or shutdown stopped user code. Runwick cannot terminate arbitrary in-process handlers.
6. **Lease expiry does not prove the old handler stopped.** Concurrency limits bound valid leases/permits, not physical activity; fencing protects a resource only when the resource checks the fence.
7. **A committed Begin does not prove user code ran.** An abandoned execution is labelled as abandoned.
8. **Timeouts measure the pipeline, not the world.** There is no universal default timeout.
9. **No fabricated values.** Stale or missing telemetry is never reported as zero; snapshots carry `observedAt` and coverage. Counts are labelled approximate when they are.
10. **No silent downgrade.** A requested guarantee the driver cannot provide fails startup. No fallback to local locks, approximate rate windows or best-effort uniqueness; no per-dispatch option downgrades persistence. Unverifiable server settings are reported as unverified, never as verified.
11. **Presence is not authority.** Worker presence/heartbeats never grant or revoke job ownership; "no capable worker" means "no recently advertised capable worker", not proof that none could run the job.
12. **A restore is a history discontinuity.** Restored receipts, windows and fences are not presented as uninterrupted history.
13. **No untested platform claims.** No Native AOT, OS/architecture, HA or persistence-profile claim before its tests pass. `Runwick.Testing` labels simulated storage/dispatch as simulated; it is not a certified in-memory driver.
14. **Never persist or load CLR type names** from payloads or metadata; never execute code selected by stored data.

## 10. Specification index

| File | Subject | Primary sources |
|---|---|---|
| [00-overview.md](00-overview.md) | Scope, roles, deployment modes, packages, pins, honesty rules, precedence | baseline, plan, acceptance, platform/provider docs |
| [01-public-api.md](01-public-api.md) | Every public type and signature; identity grammar; registration builder; attributes and fluent twins; middleware; dispatch, batch, prepared and staged APIs; receipts; errors; `IJobContext`; `JobExecutionResult`; `StagedCompletion` | public-api-contract, identity-and-serialization, uniqueness-and-execution, review |
| [02-serialization-and-wire.md](02-serialization-and-wire.md) | `runwick.json` v1 profile and bounds; canonical typed encoding; command fingerprint; two-digest window fingerprint; prepared envelope export/import; UUID byte ordering; time quantum | identity-and-serialization, dispatch-wire-contract, driver-capabilities |
| [03-storage-protocol.md](03-storage-protocol.md) | Driver-neutral commands (Dispatch, Stage, Reserve, Begin, Renew, Finish, Recover, ReleaseReservation, receipt resolution), state machine, results, receipts and retention classes, lock order | storage-command-protocol, postgres-storage-design, execution-defaults |
| [04-postgres.md](04-postgres.md) | PostgreSQL schema, partitions, indexes, SQL shapes, gates, migrations, connection roles | postgres-schema-and-operations, postgres-storage-design, postgres-upgrades, provider-dependency-plan |
| [05-worker-engine.md](05-worker-engine.md) | Hosted service, tickets, lane selection, attempt runner, middleware pipeline, lease safety, completion, recovery, shutdown, pressure gates | worker-engine-internals, execution-defaults-and-timing, capacity-defaults-and-budgets |
| [06-defaults.md](06-defaults.md) | Every numeric default and validation bound with its inequality checks | execution-defaults-and-timing, capacity-defaults-and-budgets, management-retention-defaults, identity-and-serialization |
| [07-test-plan-g1.md](07-test-plan-g1.md) | G-1 test cases (cited as `document:case`), harness, fault points, first measurements | driver-contract-suite, acceptance, SER/TIME/DEP/PGX cases, performance plan |
| [08-controls-and-completion.md](08-controls-and-completion.md) | M3 SP-011: Cancel/ManualRetry/RerunSucceeded, public SetQueueState, EditScopePolicy, trusted actor, `management_audit`, event schema and cursor, R6 engine path, five deployments, preview control services | storage-command-protocol §6, management-and-dashboard, management-retention-defaults, review R6/R7 |
| [09-readiness-and-presence.md](09-readiness-and-presence.md) | M3 SP-012: ReadinessProbe/2 (queue saturation, revisions, unsupported heads), evidence-scoped exclusion and early recheck, probe spacing, presence subscriptions, unsupported-backlog diagnostic, polling repair | worker-engine-internals §4–§5, capacity-defaults §2, postgres-schema-and-operations §3, §6 |
| [10-rate-admission.md](10-rate-admission.md) | M3 SP-013: rolling-window rate scopes, records, rate step in Begin/1, T20 deferral, worker retirement, capability activation, policy edits and freeze | capacity-control §3–§4, §6; capacity-defaults §3, §7 |
| [11-autoscaler.md](11-autoscaler.md) | M3 SP-014: `AutoConcurrency` controller (occupancy + shared due backlog, band, boundary waiver, evidence, cooldown, step, bootstrap), `EstimateDemand`, `SetPoolOverride/1`, `scaling_events` | capacity-control §5, capacity-defaults §4–§5, §7 |
| [12-schedule-evaluator.md](12-schedule-evaluator.md) | M3 SP-015: cron grammar, normalization and fingerprint, Cronos adapter, NodaTime mapping, TZDB artifact, bounded planner and preview | scheduler-evaluator, scheduling-internals §6 |
| [13-scheduler.md](13-scheduler.md) | M3 SP-016: schedule records, materialization, misfire plans, catch-up episodes, pause/resume, edits, manual triggers, overlap, lock order, lifetimes | scheduling-internals, scheduler-evaluator |
| [14-outbox.md](14-outbox.md) | M3 SP-017: `IPostgresOutbox`/`ISqliteOutbox` Stage, source schema v1, relay state machine and budgets, `ReceiveOutbox/1`, `outbox_deliveries`, hash domains, horizon rules, OUTBOX/OUT/TX/CMD/ISO cases, P-09 | transactional-outbox, storage-command-protocol (outbox destination), driver-contract-suite |
| [15-test-plan-g2.md](15-test-plan-g2.md) | G-2 case inventory for SP-011 … SP-017, inferred IDs (TXC, CTL, EVT, MODE, RDY, SCL), harness additions, measurements, pass rule | 07, driver-contract-suite, acceptance G-2, performance plan |

Test-case citations use `document:case` wherever an ID is ambiguous; `OBS-01` in particular exists in both the driver contract suite and the telemetry plan and is always cited with its document.
