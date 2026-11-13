# Round two performance handoff

Updated 2026-11-13 15:51 UTC. Start here when resuming in a new model or session. Keep this file current after each result, decision, failed run, or change to the work queue. Keep detailed results in the report and raw evidence in the artifact directory.

- Scope and acceptance gates: [perf-plan-2.md](perf-plan-2.md).
- Measurements, changes, and exact test revisions: [performance report](../reports/perf-round-2.md).
- Redis authority and durability model: [Redis failover contract](../reports/redis-ha-contract.md).
- Detailed chronological checkpoint and raw logs on the current host: `artifacts/perf/round-two/resume.md` and its neighboring experiment directories. Git ignores these artifacts. Preserve or copy them separately if moving hosts.
- Workspace: `/root/work/Spindle`, local `main`. Preserve the pre-existing untracked cloud plan and cloud report. No remote publication or deployment requested.

## Current state

SQLite producer grouping and retained receipt indexing landed, along with PostgreSQL direct dispatch wakes, worker claim and allocation improvements, Redis bounded inline projection, opt-in scheduler grouping, and the Store page. The report records the exact commits, suite results, and durability settings.

The PostgreSQL destination wake candidate passed 107 focused tests and improved every matched outbox pair. Median throughput was 88.71 → 96.10/s and p99 12.97 → 10.70 s. All 89,831 deliveries reconciled. Keep the candidate isolated because no arm met both P-09 targets. Its patch and qualification are under `p09-d4/`.

The authenticated Store HTTP benchmark, PostgreSQL B1 SQL proof, and B2 paired experiment have success markers. Read their summaries and proof logs next. The delayed Redis/BullMQ runner stopped at pair four at 40/s. The saved Runwick JSON passes the job and identity checks. The corresponding Redis INFO file is missing. Resume with a fresh fourth pair, preserve the previous evidence, and finish the remaining 40/s and 100/s pairs.

## Remaining work

- Record HTTP viewer throughput and latency for SQLite, PostgreSQL, and Redis, including diagnostics availability and the 5% budget.
- Record B1 proof scope and B2 results. The B1 SQL proof does not include a production producer queue or cancellation and recovery qualification. B2 tests a SQL change at the existing 100-item cap.
- Finish the delayed Redis/BullMQ comparison and report enqueue and due-to-handler tails alongside durable completions and refusals.
- Keep the P-01 and P-09 gates, scheduler p99 gate, strict Sentinel and Cluster authority, and full SP-029 and quiet-host soak open until qualified. See the plan and report for the remaining implementation work and owner decisions.

## Reproducing runs

Use .NET SDK 10.0.403 with `DOTNET_ROOT=/root/.dotnet` and its directory on `PATH`. Use pinned fixture images from each runner and retain default durable settings, including Redis AOF always and SQLite WAL with synchronous FULL. The host is shared. Run timing workloads serially and verify that an existing runner has exited before restarting. Check each experiment's `run.done` exit status, logs, and reconciliation evidence. Treat wide paired spreads as inconclusive and keep experiments with unresolved correctness or regression checks isolated.
