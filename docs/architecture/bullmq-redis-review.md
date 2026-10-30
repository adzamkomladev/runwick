# BullMQ implementation review for Runwick Redis

Reviewed 2026-10-29 at commit [f2fe2eff958267df9a9e7dea3fea66e14343563c](https://github.com/taskforcesh/bullmq/commit/f2fe2eff958267df9a9e7dea3fea66e14343563c). This pins inspected source, not a claimed released package version. Focus: Redis admission, lock renewal/finalization, stalled recovery, rate limiting, and deployment constraints. Source and selected test bodies were inspected; BullMQ tests were not executed. This is not an exhaustive audit.

## 1. Admission is a server-side transition

`moveToActive-11.lua` reads queue metadata, promotes due delayed jobs, checks the queue rate-limit TTL and pause/concurrency condition, then moves a waiting/prioritized job to active. Its helper installs a token-bearing expiring lock, updates processing metadata and the started counter, and emits an event before returning the job.

Runwick should adopt bounded server-side state transitions and explicit blocked/delayed responses. It cannot copy this command unchanged: Runwick has separate reserved/running states, Begin-based execution and rate accounting, application-wide resource scopes, and command receipts.

The script receives a timestamp argument. Runwick's ownership/eligibility decisions should retain their store-time authority instead of importing a caller wall clock by accident.

Evidence: [moveToActive](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/src/commands/moveToActive-11.lua), [prepareJobForProcessing](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/src/commands/includes/prepareJobForProcessing.lua).

## 2. Renewal is token-checked and reports individual failures

`extendLocks-1.lua` compares each current lock token, extends only a matching existing lock, removes successfully renewed jobs from the stalled candidate set, and returns failed job IDs. `LockManager` tracks active jobs and periodically submits renewals independently of their processors.

Adapt the batch shape and independent results. Runwick additionally renews job ownership and every distributed permit together and must resolve command ambiguity. It should not renew unrelated healthy leases in a transaction that a stale lease can roll back. BullMQ's timer mechanism does not establish Runwick's monotonic safety-deadline behavior.

Evidence: [renewal script](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/src/commands/extendLocks-1.lua), [lock manager](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/src/classes/lock-manager.ts), [renewal tests](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/tests/lock_manager.test.ts).

## 3. Completion rejects missing or mismatched ownership

The inspected finish path calls `removeLock`, which distinguishes missing locks from mismatched tokens. Worker tests deliberately let a lock expire or replace its value, then assert completion errors. This directly supports Runwick's stale-owner tests.

Runwick must retain application/job/attempt/worker/generation matching as well as expiry. It should not expose the helper's special token bypass as an ordinary completion path.

The finish script is not a SQL rollback template: it removes the lock before some later operations/validation, including checking removal from the active list. Therefore a returned non-success code does not universally mean that nothing was written. This is a concrete reason to define Runwick's command-effect classifications rather than infer them from the word “atomic.” It is not a claim of an exploitable BullMQ defect.

Evidence: [removeLock](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/src/commands/includes/removeLock.lua), [finish path](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/src/commands/moveToFinished-14.lua#L77), [missing/stolen-lock tests](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/tests/worker.redis.test.ts#L430).

## 4. Stalled detection uses candidates plus an ownership recheck

`moveStalledJobsToWait-9.lua` uses a short-lived checker key, examines the previous stalled candidate set, and only requeues a job if its lock is absent and it can remove that job from active. It increments a stalled counter. At this commit ordinary jobs over the threshold receive a deferred failure reason; recognized repeatable jobs are exempt from that threshold branch. It then marks currently active jobs as the next candidates.

Adapt the recheck-before-recovery and bounded repeated-abandonment ideas. Preserve Runwick's different pre-Begin and running budgets, with no automatic recurring-job exemption. Its explicit lease-expiry index can avoid importing a full-active-list scan. BullMQ batches SADD arguments here but first reads the full active list; that is not a bounded total scan guarantee.

Evidence: [stalled recovery](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/src/commands/moveStalledJobsToWait-9.lua), [recovery tests](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/tests/stalled_jobs.test.ts), [extended-lock case](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/tests/stalled_jobs.test.ts#L1144).

## 5. Its rate algorithm differs from Runwick's current proposal

The inspected helper increments a shared counter and starts its expiry when the counter first becomes one; admission checks that counter and its remaining TTL. This is an expiring counter window, not an exact sliding log of each authorization. The rate test for max=2 checks elapsed time across ten jobs; it does not prove Runwick's every-rolling-window property.

Keep BullMQ's early blocked response and useful wake-up delay. Do not silently replace Runwick's proposed rolling-window Begin grants with this cheaper algorithm. If benchmarks justify changing the public rate contract, review that tradeoff explicitly across every driver.

Evidence: [counter increment](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/src/commands/includes/prepareJobForProcessing.lua), [TTL check](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/src/commands/includes/getRateLimitTTL.lua), [rate tests](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/tests/rate_limiter.test.ts#L531).

## 6. Delayed work and cluster placement offer reusable patterns

Delayed promotion uses a sorted set and limits each promotion pass to 1,000 jobs. Adopt explicit bounded promotion/query work, but benchmark Runwick's own limits and timestamp representation. Runwick's scheduler creates occurrences separately; this helper moves already-existing delayed jobs.

BullMQ's cluster guidance co-locates keys with a hash tag and suggests distributing independent queues across slots. Runwick's cross-queue resource/type scopes and schedule transactions mean a queue-level tag is insufficient: its initial atomic partition must be the application namespace. That limits horizontal sharding within one application and must be visible in the capability matrix.

Evidence: [delayed promotion](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/src/commands/includes/promoteDelayedJobs.lua), [cluster guidance](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/docs/gitbook/patterns/redis-cluster.md).

## 7. Operational guidance is part of the design

BullMQ recommends AOF persistence, requires noeviction for correct queue behavior, and distinguishes producer failure/retry needs from worker reconnect behavior. Adopt explicit persistence/eviction validation and bounded producer commands. Runwick workers may keep reconnecting, but expired mutation commands and lost leases must not remain executable indefinitely in an offline buffer.

AOF every-second guidance is not a no-loss guarantee. Neither these inspected scripts nor these tests establish acknowledged-write survival across arbitrary Redis failover. No evidence found here justifies claiming BullMQ rejected Runwick's receipt or rate semantics for security/performance reasons; those motives remain unknown.

Evidence: [pinned production guidance](https://github.com/taskforcesh/bullmq/blob/f2fe2eff958267df9a9e7dea3fea66e14343563c/docs/gitbook/guide/going-to-production.md).

## Decisions carried into Runwick

- Adopt/adapt server-side transitions, token checks, independent batch-renewal results, delayed indexes, and production configuration checks.
- Preserve separate Begin accounting, full command receipts, application-wide atomic scopes, and the chosen rate semantics.
- Prove partial-write recovery, numeric precision, cluster placement, and persistence/failover behavior before calling the Redis driver conformant.
- Keep BullMQ as an implementation reference, not a second backend/service dependency and not a reason to add workflow features to v1.

See the [driver capability matrix](driver-capabilities.md) and [shared contract suite](driver-contract-suite.md) for concrete release gates.
