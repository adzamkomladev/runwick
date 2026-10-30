> History; see [docs/spec](spec/00-overview.md) for the normative specification.

# Existing-library research for Runwick

Reviewed: 2026-10-28. Status: initial official-documentation comparison, not a source-code audit, benchmark, or exhaustive feature matrix. Missing evidence does not establish a missing feature. Version-specific guarantees must be checked against pinned releases before adopting algorithms.

A subsequent [implementation review](implementation-research.md) now records selected source paths, tests, and issue discussions across all six libraries, pinned to commit IDs. Tests were inspected, not executed. This document remains the documentation comparison; the implementation review records stronger evidence and its limits.

The initial handoff review covered River, Laravel Horizon, and Hatchet. The .NET comparison below was added after the user explicitly identified these six libraries.

## Coravel

Its queue is explicitly in-memory and exposes invocables, payload-bearing invocables, and cancellation. Its scheduler offers fluent intervals, cron, and keyed overlap prevention. Sources: [queue](https://docs.coravel.net/Queuing/) and [scheduler](https://docs.coravel.net/Scheduler/).

Runwick proposal: use Coravel as a reference for approachable setup and readable configuration. Keep durable dispatch explicit. Do not infer cross-process exclusion from a fluent overlap-prevention method; verify its implementation and scope before comparing guarantees.

## Hangfire

Hangfire persists method invocation metadata, including type, method, parameter types, and arguments, and models processing through job states. Its throttling documentation explicitly describes best-effort behavior and warns that a lost connection can release an execution lock while the method continues. Sources: [background methods](https://docs.hangfire.io/en/latest/background-methods/index.html) and [concurrency](https://docs.hangfire.io/en/latest/background-processing/throttling.html).

Runwick proposal: study state transitions, filter extension points, and operational controls. Retain explicit payload/handler contracts and stable persisted names. Include stale ownership and duplicate side-effect scenarios in the concurrency contract. Review storage implementations individually rather than treating all drivers as equivalent.

## Wolverine

Wolverine provides named local queues, configurable parallelism, and optional durable inbox storage. Its durable messaging integration combines persistent message storage, endpoint durability configuration, and transactional middleware or outbox publishing services. Sources: [local queues](https://wolverinefx.net/guide/messaging/transports/local) and [durable messaging](https://wolverinefx.io/guide/durability/).

Runwick proposal: use Wolverine as a primary .NET reference for transaction boundaries and middleware. Distinguish durable local processing, failed-node recovery, and ordinary competing-consumer distribution; those are separate capabilities to verify for each transport. Keep Runwick's public scope centered on background jobs.

## FastEndpoints.JobQueues

The package name is `FastEndpoints.JobQueues`. It uses command payloads with separate handlers, automatic discovery, per-command-type queues, and configurable parallelism. Users supply storage. Distributed mode requires atomic claiming and limits claims to free execution slots. Sources: [job queue documentation](https://fast-endpoints.com/docs/job-queues).

Runwick proposal: compare this API directly with the proposed typed handler contract. Provide built-in storage drivers, independently named queues, and tested reservation semantics. Explicitly inspect ownership checks, renewal, and expired-attempt completion before drawing conclusions about its recovery guarantees. Job results and progress are additional API questions to discuss, not silently added v1 requirements.

## TickerQ

TickerQ distinguishes registered functions, individual time jobs, recurring definitions, and occurrences. It supports embedded or separate workers over shared persistent storage, optional dashboard packages, and class or attribute-based function registration. Its EF integration distinguishes independently persisted `AddAsync` from transactionally staged `StageAsync`. Sources: [concepts](https://tickerq.net/docs/5.x/what-is-tickerq) and [transactional scheduling](https://tickerq.net/docs/5.x/entity-framework/transactional-scheduling).

Runwick proposal: preserve the schedule/occurrence distinction and clearly document when enqueueing becomes durable. Review source-generated discovery as an optional future implementation choice. Keep the base transaction API independent of EF Core.

Version caveat: current 5.x documentation describes a source-available commercial release line, with eligibility-based Community licensing; earlier artifacts retain their original terms. Do not describe all versions as having the same license. Source: [official site](https://tickerq.net/). This is a comparison note, not a proposal to change Runwick's free, open-source commitment.

## Quartz.NET

Quartz separates jobs from triggers and provides trigger-specific misfire policies. Its concurrency guidance distinguishes exclusion for a job key from limits on categories of work, and explains how delayed acquisition interacts with misfires. Sources: [triggers, 3.x](https://www.quartz-scheduler.net/documentation/quartz-3.x/tutorial/more-about-triggers.html) and [best practices](https://www.quartz-scheduler.net/documentation/best-practices.html).

Runwick proposal: use Quartz to challenge cron, missed-run, identity, and overlap semantics. Specify whether an occurrence is skipped, retained, or coalesced when execution capacity is unavailable. Keep job-type, job-instance, schedule, and resource-key concurrency scopes distinct. Pin the Quartz version when reviewing algorithms; current and maintained documentation lines differ.

## Decisions this research should inform

1. Compare the same three use cases across relevant libraries: request dispatch, dispatch within a business transaction, and execution by separate worker processes.
2. Choose an explicit transaction API and document visibility before commit, rollback, ambiguous commit outcomes, and post-commit wake-up failure.
3. Specify persistence identities, handler discovery, and rolling-deployment compatibility before finalizing registration syntax.
4. Define concurrency scope and failure behavior, including stale completion after takeover and handlers ignoring cancellation.
5. Define recurring occurrence identity and how overlap, capacity limits, pause, and misfire policies interact.
6. Compare dashboard/operator flows and package boundaries before freezing the public management API.

Before calling research implementation-ready, inspect selected source paths at pinned releases for reservation/acquisition, acknowledgement, recovery, transaction enlistment, and schedule coordination. Record evidence and unresolved differences, then turn Runwick's chosen guarantees into acceptance tests. No implementation should start solely on the strength of this documentation pass.
