# Runwick architecture baseline — planning closure 6 of 6

**Review 2026-10-30:** the [v1 delivery plan](v1-delivery-plan.md) is the consolidated task/slice/milestone list to track. The [architecture review](review-2026-10-30.md) is the G-0 input for this baseline. It records two in-place corrections, five recommendations awaiting decision (R1–R5), and implementation observations. A plain-language walkthrough of the whole design is in [Runwick explained](runwick-explained.md).

Status: **all six remaining planning items completed**, 2026-10-29. Baseline version 1 is the consolidated design to implement and review. This records engineering selections made to finish the requested planning; it does not claim that every numerical setting was separately approved by the user, that runtime checks passed, or that coding has begun. There is no further scheduled planning topic after this document.

## Completion and document precedence

| Remaining item promised to the user | Completed output |
|---|---|
| 1. Transactional outbox | [Source adapters, ReceiveOutbox identity, relay and horizons](architecture/transactional-outbox.md) |
| 2. Redis design closure | [Recoverable intent, AOF-always profile, client boundaries and fixed fallback](architecture/redis-design-closure.md) |
| 3. Dashboard integration closure | [Selected Inertia.NET/React pairing, isolation and adapter fallback](architecture/dashboard-integration-closure.md) |
| 4. Management and retention defaults | [Permissions, limits, lifetimes and cleanup](architecture/management-retention-defaults.md) |
| 5. Performance acceptance plan | [Fixed workloads, environments, targets and failure disposition](architecture/performance-acceptance-plan.md) |
| 6. Final consolidation | This baseline, reconciled operation/schema additions and ordered implementation backlog |

For conflicting drafts, this baseline and its five closure documents take precedence, followed by the concrete platform, identity, timing, capacity and scheduler packets; then consolidated PostgreSQL operations/upgrades; then the original engine/provider sketches. This precedence applies only to stated refinements, not permission to ignore an invariant elsewhere. The user's storage review and historical handoff remain unchanged provenance. Historical “next” discussions are not additional requirements.

## Frozen implementation choices

| Decision | Baseline disposition |
|---|---|
| D-01 platform/dependencies | .NET 10+, initial net10.0/C#14; latest stable Npgsql direction, current planned pin 10.0.3; exact SDK/restore/image locks produced at scaffolding |
| D-02 packages | Contracts, core Runwick, Postgres, Sqlite, Redis, AspNetCore, Dashboard, Testing; optional Outbox, Outbox.Postgres, Outbox.Sqlite |
| D-03 identity/configuration | Explicit stable name through attribute or fluent; ordinal identity; fixed low/medium/high lanes; deterministic precedence and collection composition |
| D-04 serialization | runwick.json v1, original UTF-8 bytes, 1 MiB payload/2 MiB prepared envelope, exact profile capability tuples and canonical fingerprint rules |
| D-05 execution timing | 3 executions; pre-start guard 5; 60 s lease, 10 s safety margin, 15 s jittered renewal; no universal pipeline timeout; bounded grace/cleanup/shutdown as specified |
| D-06 storage | PostgreSQL first; seven states; Begin accounting, counted permits, gate-first transactions, original-byte payloads and versioned command receipts |
| D-07 capacity | Fixed 4 default; explicit auto bounds, group cap 16; weighted 6:3:1 lanes and aging; exact rolling-window rates; occupancy/backlog controller and control-I/O budgets |
| D-08 scheduling | Cronos 0.13.0 + NodaTime 3.3.7 adapter, TZDB 2026f artifact/hash; explicit cron grammar; UTC/skip-gap/earlier-fold/run-once defaults |
| D-09 Redis | Recoverable application intent; persistent direct-primary AOF-always profile; no automatic promotion/cluster equivalence claim; mandatory SP-018 proof |
| D-10 dashboard | Inertia.Net 1.5.3 source/API baseline, Inertia React/core 3.9.1, React 19.3.1; mount-scoped integration and fixed isolation fallback |
| D-11 operations | Named permissions, keyset queries, bounded requests, operation-class receipt horizons and dependency-aware retention |
| D-12 outbox | Optional PostgreSQL/SQLite source adapters; stable delivery identity independent of request IDs; 7-day acceptance default, retained mapping through cutoff +24 h |
| D-13 upgrades | Store/app capability gates and expand/backfill/validate/activate/contract; explicit scheduler profile/data cutovers; restore is a history discontinuity |
| D-14 acceptance | P-01–10 workloads, fixed healthy/fault budgets, incremental measurement and explicit qualification targets |

SQLite 10.0.14 and StackExchange.Redis 3.4.20 remain planned provider pins. Restore/provenance checks and later dependency servicing are normal implementation/release work. NuGet package-name availability is a publication check, not a new architecture topic. No Native AOT claim until the selected combinations pass publication tests.

## Public contract and package boundaries

Keep `IQueueJob`, `IJobHandler<TJob>.HandleAsync(job, CancellationToken)`, `IJobDispatcher.DispatchAsync` and optional scoped `IJobContext`. Select the current public-contract spellings `[JobName]`, `[PayloadVersion]`, `[Queue]`, `[Priority]`, `[MaxExecutions]`, `[UseJobMiddleware]`, `AddRunwick`, explicit worker/pool roles, `IPreparedJobDispatcher`, provider-specific StageAsync and `IJobMiddleware.InvokeAsync`. Attribute-first examples and fluent-only definitions both ship. No mandatory context argument, persisted CLR type name or automatic handler scan of all loaded assemblies.

Use DispatchReceipt only for confirmed destination acknowledgement; StagedDispatch and StagedOutboxMessage make no caller-commit claim. Unknown-outcome handles retain immutable requests. Local overload uses known-not-submitted errors. Middleware's single continuation and explicit outcome rules stay intact. Compilation may require narrow signature/type corrections, recorded with examples; it does not reopen the behavioral baseline.

The core holds scheduling/capacity/management application services and driver extension contracts. Contracts depends on neither hosting nor storage/web; concrete providers depend on core; AspNetCore depends on core; Dashboard depends on AspNetCore plus selected UI transport; optional outbox source packages depend on Outbox and their source provider client. Avoid mandatory EF, dashboard, telemetry exporter or Node in producer/worker deployments. Named packages are logical publication boundaries; no packages exist in this repository yet.

## Consolidated operations and schema delta

Epoch 1 and operation version 1 describe the complete initial contract; incomplete milestones advertise only implemented operations/features. A worker unable to enforce configured rates cannot claim those jobs. Payload decoder compatibility is separate from operation capability, scheduler evaluator/data compatibility and physical schema version.

| Family | Commands / required records |
|---|---|
| Dispatch | Dispatch, Stage, receipt resolution; exact payloads/generations, active/window keys and policy snapshots |
| Execution | Reserve, Begin, Renew, Finish, Recover; attempts, leases, concurrency scopes/permits, rate scopes/grants and immutable outcomes |
| Operational control | Cancel, ManualRetry, queue/scope policy, pool overrides, app capability/mode controls; expected revisions and atomic audit |
| Scheduling | Register/Materialize/Pause/Resume/Update/Trigger/ResolveCatchUp; definitions, occurrences, high-water marks, episodes, profile/data hash |
| Scheduler review | RecordScheduleEvaluationFailure, RevalidateSchedule; guarded review/audit without accidental cursor advancement |
| Outbox destination | ReceiveOutbox; unique outbox_deliveries with immutable cutoff/fingerprint and retained result mapping |
| Outbox source | Stage/Claim/RenewSourceLease/RecordTransportAttempt/MarkDelivered/Block/Retry; separate source metadata/messages schema, caller transaction for Stage |
| Queries/retention | Bounded authorized projections, readiness/presence, disclosure audit, CleanupBatch and retention progress |

Normal mutation prefix: store gate → application gate → command receipt. Additional delivery identities lock next, before ordinary schedule/queue/job/key/scope ranks. ReceiveOutbox uses delivery identity → dispatch keys → brand-new jobs → concurrency/rate scope provisioning. It never locks a reused existing job afterward. Cleanup respects the same identity ordering and retains no cascading correctness dependency on deletable job detail. All full-driver implementations include outbox/scheduler/audit additions; no parallel shadow schema is authoritative.

Two important distinctions are now closed: rate-limited Begin has committed effects; Redis pending intent does not yet have published effects. Either can require receipt resolution, but neither is interchangeable with a pure NoWork observation. Native staged writes, relay delivery and handler side effects are three separate atomicity boundaries.

## Fixed implementation sequence

Preserve the 30-item dependency graph; do not create a new planning milestone before each work item.

1. **Foundation and PostgreSQL execution — SP-001–010.** Use this baseline as the SP-001 review deliverable; scaffold packages/tests, implement registry/codec, schema/transactions, dispatch/uniqueness, Reserve/Begin/Renew/Finish/Recover, then worker/middleware/shutdown and the first fault report. Cancellation/manual retry controls arrive in SP-011; earlier schema sketches calling them part of the first milestone are superseded.
2. **Early bounded experiments — SP-018 and SP-023.** Run once their existing dependencies are available, alongside core development: prove the selected Redis profile and dashboard isolation. Their failure paths are already specified; they do not require another open-ended planning round.
3. **Controls/capacity/scheduling/outbox — SP-011–017.** Implement revisioned controls, readiness/fairness, rates, autoscaling, recurrence/materialization and both source outbox adapters.
4. **Remaining providers and management — SP-019–025.** SQLite, HTTP management, retention, telemetry, dashboard and full Redis parity.
5. **Qualification/release — SP-026–030.** Compatibility, deployment/fault/security scenarios, complete driver reports, performance/soak and packaging. Measurements start during step 1, not only at step 5.

The first runnable milestone is **dispatch → reserve → execute → finish/recover on PostgreSQL**. Full v1 still includes the planned additional providers/dashboard/outbox; failed experiments cannot silently remove them. SQL files, compiled projects, executable tests, package locks and runtime benchmarks are implementation outputs, not missing planning chapters.

## Exit and change control

All six planning deliverables are written and cross-referenced. Remaining G-0 review is disposition of this concrete baseline before runtime work; G-1–G-6 remain evidence gates and are not marked passed. The instruction to finish planning is not treated as an instruction to build or deploy the library.

Reopen a design only for a concrete contradiction, failed prototype/test, measured budget miss or explicit changed requirement. Record the affected invariant, evidence and smallest resulting change. Do not add speculative features or repeatedly re-ask settled platform/API questions. Design corrections stay within implementation unless they change agreed scope/guarantees. No seventh planning item is scheduled.
