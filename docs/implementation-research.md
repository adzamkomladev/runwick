# Implementation lessons for Spindle

Reviewed: 2026-10-28. This is a focused static review of selected production paths, tests, and upstream discussions. It is not an exhaustive audit of these libraries. Tests were inspected, not executed; upstream performance measurements were not reproduced.

## Evidence and scope

Source links are pinned to default-branch commits, not assumed to be released NuGet versions:

| Repository | Commit |
|---|---|
| jamesmh/coravel | [4072e041820f](https://github.com/jamesmh/coravel/commit/4072e041820f2d14529276340abb7150a83945da) |
| HangfireIO/Hangfire | [ad2f963ea903](https://github.com/HangfireIO/Hangfire/commit/ad2f963ea9032e84de2c5233e11d52a88e26d3e4) |
| JasperFx/wolverine | [9daa5165a049](https://github.com/JasperFx/wolverine/commit/9daa5165a0499aae3dd7e64152e1e726f7466cea) |
| FastEndpoints/FastEndpoints | [46416dd4f54a](https://github.com/FastEndpoints/FastEndpoints/commit/46416dd4f54a71b25200f5b516bccbc505f32655) |
| quartznet/quartznet | [3e10f0f8d055](https://github.com/quartznet/quartznet/commit/3e10f0f8d05591cbb2e75c18f10c838cb12b5efc) |
| Arcenox-co/TickerQ | [f366cf7fa325](https://github.com/Arcenox-co/TickerQ/commit/f366cf7fa32575be12e3638546d04ed6d0708c63) |

Distinguish observed implementation, upstream rationale, reported behavior, and our proposed design. An issue alone is not proof of a current defect. A regression test documents a contract but does not establish coverage of every failure mode. An absent feature does not prove its authors rejected it.

The earlier documentation comparison remains useful for API exploration. This document adds implementation evidence; it does not validate every claim in that comparison.

## 1. Hangfire: control idle polling separately from execution concurrency

**Observed:** SQL Server dequeue first attempts a fetch, then coordinates waiting pollers with a semaphore keyed by storage and queue set. The source explicitly explains that coordination reduces database load. This is process-local coordination, not one global poller across a cluster.

**Spindle proposal:** separate the number of active handlers from the number of idle polling loops. Use wake-up signals as an optimization with periodic polling for recovery. Design bounded polling/backoff and account for replicas and queue counts.

**Acceptance:** measure idle database queries as worker slots, queue counts, and replicas increase; dropped notifications must not strand work. Do not derive Spindle throughput claims from Hangfire's implementation.

Evidence: [dequeue implementation](https://github.com/HangfireIO/Hangfire/blob/ad2f963ea9032e84de2c5233e11d52a88e26d3e4/src/Hangfire.SqlServer/SqlServerJobQueue.cs).

## 2. Hangfire: renewal and queue mutations check ownership

**Observed:** SQL Server timeout jobs renew their fetched timestamp periodically; renewal, deletion, and requeue match the prior fetched timestamp. Renewal uses database time and notices when no matching row remains. Tests cover renewal and removal/requeue after renewal. This observation concerns queue rows, not every durable job-state transition.

**Spindle proposal:** use an explicit reservation identity and ownership-checked renewal, completion, retry, and permit release. Decide expiry validation separately from token equality. A token does not stop external side effects.

**Acceptance:** after worker B takes over, worker A cannot renew B's reservation, acknowledge it, or release B's permits.

Evidence: [timeout job](https://github.com/HangfireIO/Hangfire/blob/ad2f963ea9032e84de2c5233e11d52a88e26d3e4/src/Hangfire.SqlServer/SqlServerTimeoutJob.cs) and [tests](https://github.com/HangfireIO/Hangfire/blob/ad2f963ea9032e84de2c5233e11d52a88e26d3e4/tests/Hangfire.SqlServer.Tests/SqlServerTimeoutJobFacts.cs).

## 3. Wolverine: initial persistence cannot be a detached background retry

**Observed:** the current durable local queue awaits a bounded persistence retry before marking the envelope persisted. Its regression test explains that the earlier retry-block shape could return success to an outside-transaction publisher while later retries discarded the message. Tests cover publish/schedule failure and recovery within the retry budget. The test also distinguishes cascaded messages without an enlisted outbox; do not generalize the fix to every publishing path.

**Spindle proposal:** ordinary durable enqueue must await persistence. Enqueue inside a caller-owned transaction returns before the caller's commit, so its result must not imply committed durability. Ambiguous commit outcomes require idempotent retry support.

**Acceptance:** storage failure cannot produce a false durable-success result; rollback removes the job; a lost commit response can be retried without creating a second logical job when a dispatch key is supplied.

Evidence: [queue implementation](https://github.com/JasperFx/wolverine/blob/9daa5165a0499aae3dd7e64152e1e726f7466cea/src/Wolverine/Transports/Local/DurableLocalQueue.cs), [regression tests](https://github.com/JasperFx/wolverine/blob/9daa5165a0499aae3dd7e64152e1e726f7466cea/src/Testing/CoreTests/Runtime/durable_publish_failure_reaches_the_caller_4662.cs), and [issue #4662](https://github.com/JasperFx/wolverine/issues/4662).

## 4. FastEndpoints: refill free slots and protect shutdown bookkeeping

**Observed:** the distributed executor requests only available capacity, tracks executions individually, and waits for any task to finish when full. Completion/failure writes use a token separate from application-stopping cancellation. Tests cover slot refill, claim limits, and draining during shutdown.

**Spindle proposal:** avoid waiting for a whole batch when one job is slow. Reserve against free slots. Separate stop-reserving, handler cancellation, and final bookkeeping, with a bounded cleanup deadline rather than an unbounded shutdown.

**Acceptance:** one slow job does not block refilling other slots; reservations never exceed available local capacity; shutdown still attempts ownership-checked completion; forced termination remains recoverable.

Evidence: [executor](https://github.com/FastEndpoints/FastEndpoints/blob/46416dd4f54a71b25200f5b516bccbc505f32655/Src/JobQueues/JobQueue.cs) and [executor tests](https://github.com/FastEndpoints/FastEndpoints/blob/46416dd4f54a71b25200f5b516bccbc505f32655/Tests/UnitTests/FastEndpoints/JobQueueTests.Executor.cs).

## 5. Coravel: local exclusion and distributed coordination are different commitments

**Observed:** the examined mutex uses an in-memory dictionary and process-local lock. The scheduler supplies a fixed expiry. This is not cross-replica ownership.

**Upstream rationale:** in the historical distributed-scheduling discussion, the maintainer identified maintaining database-specific mutex providers as a difficulty and later deprioritized the feature. The polling discussion cites control over batch timing and preserving simplicity. These are maintenance/product tradeoffs, not evidence of a security-based rejection.

**Spindle proposal:** keep distributed guarantees explicit and budget for each driver's maintenance. PostgreSQL-first sequencing remains sensible; SQLite and Redis still require their own contract validation.

**Acceptance:** run multi-process exclusion and recovery tests for each driver claiming those guarantees. Never use an in-memory mutex to satisfy a global ceiling.

Evidence: [mutex](https://github.com/jamesmh/coravel/blob/4072e041820f2d14529276340abb7150a83945da/Src/Coravel/Scheduling/Schedule/Mutex/InMemoryMutex.cs), [scheduler](https://github.com/jamesmh/coravel/blob/4072e041820f2d14529276340abb7150a83945da/Src/Coravel/Scheduling/Schedule/Scheduler.cs), [maintainer on provider maintenance](https://github.com/jamesmh/coravel/issues/101#issuecomment-516025063), and [maintainer on polling](https://github.com/jamesmh/coravel/issues/188#issuecomment-687100367).

## 6. TickerQ: keyed concurrency needs a memory-lifetime design

**Observed:** owned function permits count holders and waiters; an entry is removed and its semaphore disposed after the last reference. Cancellation releases a reference, and permit disposal is idempotent. Legacy raw-semaphore access pins entries. Tests exercise dynamic names, cancelled waiters, and concurrent acquire/release.

**Spindle proposal:** resource-keyed local gates need safe reclamation. Distributed permit rows need retention too. Do not expose mutable/raw semaphore objects as the public ownership contract.

**Acceptance:** many distinct keys do not accumulate indefinitely; cancelling a waiter does not release a held permit; reclaim/recreate races never admit two independent gates for one active key.

Evidence: [gate](https://github.com/Arcenox-co/TickerQ/blob/f366cf7fa32575be12e3638546d04ed6d0708c63/src/TickerQ/Src/TickerFunctionConcurrencyGate.cs) and [tests](https://github.com/Arcenox-co/TickerQ/blob/f366cf7fa32575be12e3638546d04ed6d0708c63/tests/TickerQ.Tests/TickerFunctionConcurrencyGateTests.cs).

## 7. Hangfire and TickerQ: dashboard hosting is part of the security boundary

**Observed:** Hangfire's ASP.NET Core middleware runs authorization filters and, when enabled and available, antiforgery validation before dispatch. TickerQ validates anonymous access acknowledgement, explicit CORS origins, and trusted forwarding configuration. Its tests also reproduce a branched-pipeline authorization integration failure.

**Spindle proposal:** enforce authorization server-side for management operations; correctly wire embedded and standalone middleware. Validate required security services at startup. Host authentication alone does not imply permission to retry, cancel, edit schedules, or view sensitive payloads. Explicit proxy and origin trust are configuration requirements.

**Acceptance:** unauthorized mutations fail, cookie-authenticated mutations require CSRF protection, untrusted origins/forwarded headers do not grant access, and both hosting modes enforce the same permissions.

Evidence: [Hangfire middleware](https://github.com/HangfireIO/Hangfire/blob/ad2f963ea9032e84de2c5233e11d52a88e26d3e4/src/Hangfire.AspNetCore/Dashboard/AspNetCoreDashboardMiddleware.cs), [TickerQ validation](https://github.com/Arcenox-co/TickerQ/blob/f366cf7fa32575be12e3638546d04ed6d0708c63/src/TickerQ.Dashboard/DashboardOptionsBuilder.cs), [pipeline tests](https://github.com/Arcenox-co/TickerQ/blob/f366cf7fa32575be12e3638546d04ed6d0708c63/tests/TickerQ.Tests/DashboardAuthorizationPipelineTests.cs), and [TickerQ issue #408](https://github.com/Arcenox-co/TickerQ/issues/408). The reported exception is an integration failure, not evidence of an authorization bypass.

## 8. Quartz: an optimization can help one database and hurt another

**Observed:** current firing-round code can combine acquisition and firing, batch reads, and avoid the global trigger-access lock when conditions allow. It retains locked paths for concurrency policies and compatibility, and a separate transaction shape for SQL Server. Tests explicitly count transaction/delegate operations.

**Upstream rationale:** issue #4117 records why the folded round was held back on SQL Server: multi-node tests showed deadlocks and scheduling delays. The source describes the subsequent split-transaction path. These are upstream results for that workload, not universal database rankings.

**Spindle proposal:** define shared semantics while allowing driver-specific algorithms. Measure round trips, lock duration, fairness, and tail latency; fewer transactions is not automatically faster. Do not add SQL Server to scope merely because it illustrates the tradeoff.

**Acceptance:** validate correctness first, then benchmark contention and scheduling lag under representative multi-replica load. Preserve invariants when changing batch size or transaction boundaries.

Evidence: [implementation](https://github.com/quartznet/quartznet/blob/3e10f0f8d05591cbb2e75c18f10c838cb12b5efc/src/Quartz/Impl/AdoJobStore/AdoJobStoreBase.FireOnAcquire.cs), [operation-count tests](https://github.com/quartznet/quartznet/blob/3e10f0f8d05591cbb2e75c18f10c838cb12b5efc/src/Quartz.Tests.Unit/Impl/AdoJobStore/FireOnAcquireRoundTripTest.cs), [optimization proposal #3864](https://github.com/quartznet/quartznet/issues/3864), and [SQL Server tradeoff #4117](https://github.com/quartznet/quartznet/issues/4117).

## 9. Quartz: persisted types need explicit registration and upgrade behavior

**Observed:** JSON serializer registries resolve registered trigger/calendar serializers and reject unknown names. Tests verify useful errors when a reader lacks the registration used by a writer. This does not establish that every Quartz serialization path uses an identical type policy.

**Spindle proposal:** resolve stable job identifiers through approved registrations. Do not let stored payloads choose arbitrary CLR types. Define unknown-type/version behavior during rolling deployment, payload size/depth limits, and safe dashboard rendering.

**Acceptance:** unknown identities do not instantiate arbitrary types or erase jobs; old/new worker versions coexist under a documented policy; malformed or oversized payloads fail predictably. Size limits and rendering protections are our proposals, not claims about the examined Quartz code.

Evidence: [name resolution](https://github.com/quartznet/quartznet/blob/3e10f0f8d05591cbb2e75c18f10c838cb12b5efc/src/Quartz/Util/SerializerMap.cs), [registry](https://github.com/quartznet/quartznet/blob/3e10f0f8d05591cbb2e75c18f10c838cb12b5efc/src/Quartz/Serialization/SystemTextJson/SystemTextJsonSerializerRegistry.cs), and [compatibility tests](https://github.com/quartznet/quartznet/blob/3e10f0f8d05591cbb2e75c18f10c838cb12b5efc/src/Quartz.Tests.Unit/Simpl/UnregisteredCustomSerializerTest.cs).

## 10. BullMQ: Redis transitions, ownership, recovery, and deployment boundaries

Added 2026-10-29 at [f2fe2eff958267df9a9e7dea3fea66e14343563c](https://github.com/taskforcesh/bullmq/commit/f2fe2eff958267df9a9e7dea3fea66e14343563c), following the user's request to inspect its Redis implementation. See the [detailed source/test review](architecture/bullmq-redis-review.md).

Observed: server-side admission, token-checked lock extension/finalization, stalled-candidate rechecks, delayed sorted-set promotion, and an expiring rate counter. Selected tests cover missing/stolen locks, long-running renewal, stalled recovery, and rate timing; they were inspected, not run.

Adapt server-side transitions, independent renewal results, bounded delayed work, noeviction, and explicit producer/worker reconnect behavior. Preserve Spindle's separate Begin, command receipts, and rolling-window semantics. Queue-level Redis Cluster hash tags do not cover Spindle's cross-queue resource scopes; the first atomic partition is the application namespace. Atomic script execution alone is not a proof of SQL-like rollback or acknowledged-write survival across failover.

These findings inform the [driver matrix](architecture/driver-capabilities.md) and [shared contract suite](architecture/driver-contract-suite.md), including explicit Redis recovery/durability release gates. No upstream motive for omitting Spindle's particular semantics is assumed.

## Research still required before architecture approval

This pass establishes concrete implementation precedents for all six libraries. It does not finish the engineering plan. Next, trace complete transaction/outbox paths, scheduler occurrence deduplication, PostgreSQL query/index design, Redis persistence/failover assumptions, SQLite contention, priority fairness, and autoscaling control behavior.

For each major Spindle feature, record: requirement; pinned code/test evidence; explicit upstream rationale if available; adopt/adapt/defer decision; remaining uncertainty; and an acceptance test or benchmark. Where no upstream rejection rationale is found, record **unknown**, not an invented security or performance explanation.

All recommendations remain proposals. No Spindle runtime implementation has begun.

