> History; see [docs/spec](spec/00-overview.md) for the normative specification.

# Architecture acceptance and decision register

**Planning closure:** the [final baseline](architecture-baseline.md) supplies selections for D-01–D-14 and completes the six requested planning outputs. The register below now tracks review and executable evidence, not an expanding list of planning sessions.

Status: review checklist, 2026-10-29. Design documents exist; no runtime milestone is accepted and no test or benchmark is claimed to pass. The [implementation backlog](implementation-backlog.md) identifies work and dependencies. This checklist identifies decisions and evidence needed before each gate can close.

## 1. Baseline to preserve

These are carried-forward requirements/working directions, not a request to reopen them:

- Runwick; IQueueJob, IJobDispatcher.DispatchAsync, IJobHandler<TJob>.HandleAsync(job, CancellationToken), optional injected IJobContext.
- User-selected platform floor: .NET 10 and newer, with current/newer C#. The [platform/package design](architecture/platform-and-packages.md) specifies compatible stable-language adoption and reproducible builds.
- User-selected latest Npgsql driver; [provider planning](architecture/provider-dependency-plan.md) records today's stable 10.0.3 baseline and explicit version-update policy.
- Attribute-first examples; fluent-only registration and permitted fluent overrides remain supported.
- One engine with producer/worker/scheduler/management roles across five deployment modes; PostgreSQL first, SQLite same-host, Redis with explicit persistence guarantees.
- Separate reserved/running/retrying states; Begin-based execution accounting and a separate pre-start abandonment guard.
- Atomic reservation/permits, current lease/token validation, uncertain-command receipts, bounded local tickets and completion handling.
- Active uniqueness and fixed-window dispatch idempotency; manual retry keeps job ID/history and advances generation.
- Concurrency and rate controls remain distinct. Proposed rate algorithm and tuning still require review.
- Optional embedded/standalone Inertia.NET dashboard, Basic/host/OIDC integration, scoped authorization, audit and telemetry.
- At-least-once recovery: expiry cannot physically stop user code; downstream fencing requires resource participation; no exactly-once external-side-effect promise.
- Runtime implementation follows architecture review; workflows/DAGs remain v2.

The final baseline selects API spelling, algorithms, defaults and dependency baselines as engineering decisions. User-selected requirements remain distinct from those selections. Neither design selection nor acceptance is proof that an implementation works.

## 2. Decision register

D-01’s .NET 10+ floor and latest-Npgsql direction were selected by the user. The [final baseline](architecture-baseline.md) records the remaining engineering selections; the rows below describe their outstanding review/implementation evidence. A failed test may require a focused correction, but no new planning sequence is scheduled.

| ID | Decision / current direction | Close before | Required output |
|---|---|---|---|
| D-01 | .NET 10+ and latest Npgsql accepted; initial net10.0/C# 14; Npgsql 10.0.3 baseline per [provider plan](architecture/provider-dependency-plan.md) | SP-002 | Pin SDK/restore graph; review remaining provider candidates; execute DEP cases and certify runtime/OS matrix; document trimming/AOT limits |
| D-02 | Contracts/core/providers/optional web/dashboard/testing proposal in platform design | SP-002 | Review concrete graph, producer dependency budget, dashboard/EF isolation and package availability |
| D-03 | Recommend explicit durable names, deterministic precedence/tie ordering and low/medium/high v1 lanes | SP-004 | Review identity/configuration packet, validation/limits and fixed-lane recommendation; preserve attribute/fluent choice |
| D-04 | Recommend runwick.json v1, exact payload bytes, profile-aware capabilities and strict canonical replay | SP-004 | Review profile/bounds/export format; implement SER and wire vectors; verify SDK behavior and cross-driver bytes before freeze |
| D-05 | [Execution timing packet](architecture/execution-defaults-and-timing.md): concrete retry/abandonment, authority, grace and shutdown recommendations | SP-008; policy schema for SP-004 | Review proposed defaults; TIME cases and contention/fault evidence; reconcile policy fields and operation-class receipt retention |
| D-06 | [Consolidated schema/operations](architecture/postgres-schema-and-operations.md) drafted; provider plan recommends PostgreSQL 16–18 targets, 18 reference, explicit connection ownership | SP-005–007 | Review integration; author DDL; PGX/DEP cases, plans, connection/gate/permit contention and actual constraints remain unvalidated |
| D-07 | [Capacity packet](architecture/capacity-defaults-and-budgets.md) drafted: fixed/auto bounds, fairness, rate-edit freeze, occupancy/backlog estimator, pressure gates and I/O budgets | SP-012–014; group budgets in SP-009 | Review numeric settings and local-overload API; CAP-01–12 plus DEP-03/TIME-13; contention/renewal capacity and workload evidence remain open |
| D-08 | [Scheduler packet](architecture/scheduler-evaluator-and-defaults.md) drafted: Cronos/NodaTime and hashed TZDB baseline, OR grammar, UTC mapping, defaults and prepared cutovers | SP-015–016 | Review adapter/profile/data and budgets; execute SCHED-01–15; test mismatch/review commands, bounded computation, mixed-profile upgrades and worker-I/O isolation |
| D-09 | [Recoverable intent + direct-primary AOF-always](architecture/redis-design-closure.md) | SP-025; SP-018 spike permitted by review | Failure-prefix/restart evidence, same-slot/AOF-always acknowledgement and fresh replay-confirmation proof, topology/failover limits, go/revise decision |
| D-10 | [Inertia.Net 1.5.3 + React pairing and mount isolation](architecture/dashboard-integration-closure.md) | SP-024; SP-023 spike permitted by review | Pinned compatibility, host/prop/auth/CSRF/encoding/assets tests; disposition of static/global integration concerns |
| D-11 | [Selected permissions, limits, retention and cleanup](architecture/management-retention-defaults.md) | SP-011 audit core; SP-020–021 full surface | Trusted actor/audit fields, scoped projections, detail access, retention dependencies and request/query limits |
| D-12 | [PostgreSQL/SQLite sources, ReceiveOutbox identity and horizons](architecture/transactional-outbox.md) | SP-017 and integrated release scope | Concrete integration or explicit delivery-scope change; restore/replay limits; no native cross-store transaction implication |
| D-13 | [Upgrade protocol](architecture/postgres-upgrades-and-compatibility.md) drafted: transactional compatibility checks, expand/activate/contract, mixed-client and recovery rules | SP-005 handshake; SP-026 full matrix | Review manifests and maintenance semantics; UPG cases, provider versions and executable migration evidence remain open |
| D-14 | [Fixed P-01–10 workloads and acceptance thresholds](architecture/performance-acceptance-plan.md) | Baseline reviewed SP-001; evidence SP-029 | Sizes/counts/replicas/history/outage/persistence profiles; latency/memory/renewal targets and tuned defaults |

Not every later decision blocks PostgreSQL execution work. G-0 records which decisions close for that milestone and which remain feature-gated. Required unresolved features still block full release acceptance.

## 3. Reconcile before freezing contracts

The [final baseline](architecture-baseline.md) now supplies the operation/version map and the [consolidated PostgreSQL specification](architecture/postgres-schema-and-operations.md) includes the outbox delta. Older type sketches describe subsets. SP-001 reviews the reconciliation; implementation must not select whichever draft is easiest.

| Addition | Source | Reconciliation |
|---|---|---|
| Rate-aware Begin/committed deferral | [Capacity](architecture/capacity-control.md) | Result variants, grants, locks, logical time/freeze, receipts/accounting and rejection of incompatible writers |
| Scheduler commands | [Scheduling](architecture/scheduling-internals.md) | Definition/control/cursor revisions, occurrence identity, materialization/catch-up receipts, audit and lock order |
| Readiness/presence | [Worker](architecture/worker-engine-internals.md) | Multi-lane DTO, capability revision/expiry, scope classification, bounded queries; presence is not lease authority |
| Prepared wire identity | [Wire contract](architecture/dispatch-wire-contract.md) | Canonical fingerprints/time/numbers, frozen metadata/policy, store binding and versioned envelopes/receipts |
| Administrative audit/projections | [Management](architecture/management-and-dashboard.md) | Audit/indexes in same atomic control operation, trusted actor/scope, authorization before pagination/counts |
| Precision/encodings | [Driver matrix](architecture/driver-capabilities.md) | Millisecond quantum proposal, rounding before hashing, exact fences/counters and overflow rejection |
| Redis pending recovery | Driver matrix | Explicit recovering/unknown results, guarded reads, intent after-images and publication/durability under the selected AOF-always profile |
| Outbox delivery identity | [Outbox closure](architecture/transactional-outbox.md) | ReceiveOutbox/1, distinct delivery/transport fingerprints, mapping retention/cutoff and lock rank; source schema separate |
| Context metadata | [Public API](architecture/public-api-contract.md) and worker | Bounded immutable trace/application projection; no arbitrary type activation or auth from baggage |
| Serializer identity and original bytes | [Identity/serialization](architecture/identity-and-serialization.md) | Profile ID/version in capability tuple/jobs/payloads; byte-preserving storage; versioned ready/Reserve and presence contracts; reject incompatible old claiming behavior |
| Execution policy/timing | [Timing packet](architecture/execution-defaults-and-timing.md) | Explicit evaluator/jitter and unstarted-delay policy in generation/wire; Begin/grace budgets; receipt retention and absolute shutdown boundaries |

The [historical API draft](history/public-api-initial-draft.md) and [older behavioral draft](uniqueness-and-execution.md) remain provenance. Their context parameter, five-state lifecycle and reservation-counted budget cannot override current contracts. Preserve [storage review inputs](storage-design-inputs.md) as the user's record.

## 4. Planning completion and implementation gates

- [x] Transactional outbox design.
- [x] Redis design closure.
- [x] Dashboard integration closure.
- [x] Management/retention defaults.
- [x] Performance acceptance plan.
- [x] Final architecture consolidation.

All G-0–G-6 boxes below remain unchecked. A written plan completes a planning deliverable; it does not pass a runtime test or record user approval.

### G-0 — Ready for the first implementation milestone

- [ ] Baseline and PostgreSQL milestone scope reviewed; D-01–D-06 have sufficient first-item dispositions and D-13 a compatibility baseline.
- [ ] Public examples/package graph approved; required runtime configuration is fully specified for the milestone.
- [ ] Reconciled operation/version map and schema-change list identify later feature gates.
- [ ] Independent model, fault points, transaction/lock ordering and first workloads reviewed.
- [ ] Redis, Inertia, recurrence and outbox feasibility obligations remain visible.
- [ ] Outcome recorded as approved, approved with feature gates, or changes required; continued planning is not approval.

### G-1 — PostgreSQL execution

- [ ] Declared-target builds/examples, fresh schema and namespace isolation pass.
- [ ] Real-store dispatch/staging/uniqueness and receipt resolution pass using actual codecs.
- [ ] Competing claims, atomic permits, Begin accounting, renew/recover races and stale-owner rejection pass.
- [ ] Tickets, scoped DI/middleware, watchdog, cleanup and uncertain-completion bounds pass.
- [ ] Crashes around Begin, lost replies, ignored cancellation and shutdown preserve specified history.
- [ ] First measurements cover contention, query plans, idle cost and renewal margin; milestone limits explicit.

### G-2 — Distributed controls, capacity and scheduling

- [ ] All five deployments demonstrated with independently configured roles.
- [ ] Revisioned cancel/retry/pause/policy controls and atomic audit race tests pass.
- [ ] Readiness/fairness/capability diagnostics remain bounded; unsupported jobs stay visible.
- [ ] Rate grant/defer/edit tests pass across replicas; old operation versions cannot bypass policy.
- [ ] Scaler/override tests cover stale observations, demand changes and target decreases.
- [ ] Cron/DST and materialization/misfire/edit/retention cases pass without silent occurrence loss.
- [ ] Outbox meets declared integration/replay contract, or explicit scope review records separate delivery without claiming it shipped.

### G-3 — Management/dashboard

- [ ] Basic, host schemes and OIDC work embedded and standalone.
- [ ] All protected response paths enforce authorization/antiforgery, including partial requests, polling and hints.
- [ ] Reveal, revision/replay, counts/cursors and cache scope pass access tests.
- [ ] Administrative changes and audit commit together; failed audit cannot publish success.
- [ ] Adapter/client pair passes host/prop isolation, encoding, proxy/path, CSP and asset tests.
- [ ] Required dashboard areas work within query/request budgets at realistic history/user counts.

### G-4 — Driver/profile certification

- [ ] PostgreSQL passes the full applicable contract, including later rate/schedule/audit/retention additions.
- [ ] SQLite passes same-host multi-process, executor, reader/checkpoint and transaction cases.
- [ ] Redis passes interrupted writes, recovery/publication, exact encoding and selected persistence confirmation.
- [ ] Reports identify topology/enlistment, cases/results and fault classes actually tested.
- [ ] Known-loss promotion tests document limits rather than certify history preservation.
- [ ] Unsupported capabilities fail explicitly; mandatory cases are not hidden as not-applicable.

### G-5 — Operations/compatibility

- [ ] Execution/settlement traces, redaction and bounded telemetry work with absent/failing exporters.
- [ ] Global counts are not multiplied by replicas; stale data and role readiness remain explicit.
- [ ] Bounded resumable cleanup preserves all correctness identities for required horizons.
- [ ] Supported old/new readers/writers, interrupted migrations and rollback limits tested per driver.
- [ ] Drain/outage/restore runbooks have executable scenarios; history loss is distinct from reconnect.

### G-6 — v1 release

- [ ] G-1–G-5 pass for advertised scope/profiles; all agreed v1 requirements have evidence or an explicit reviewed scope change.
- [ ] D-01–D-14 resolved for shipped surfaces; examples use validated defaults or explicit required configuration.
- [ ] Performance/long-run reports meet predeclared budgets under production-like persistence/history.
- [ ] Public examples run, optional dependencies stay optional, packages/licenses/dependencies and compatibility checked.
- [ ] Documentation matches at-least-once behavior, cooperative cancellation, finite replay, durability and topology limits.
- [ ] No unexplained mandatory failure/unimplemented case; final release evidence approved.

## 5. Report format and current position

Each gate/work item records:

1. Scope/exclusions and design/decision revisions.
2. Implementation commit, artifact versions and environment/persistence/topology profile.
3. Applicable cases by document plus ID/section; passed/failed/unimplemented/not-applicable with reasons.
4. Injected fault classes, workload/thresholds and raw results.
5. Limits/dependencies, reviewer, decision and date.

Current position: **all six planning outputs complete; baseline ready for review; runtime implementation not started**. G-0 records the review disposition of that concrete baseline. G-1–G-6 require executable evidence. The next work is the implementation backlog, not another planning topic.