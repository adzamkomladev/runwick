# Round two performance handoff

Updated 2026-11-13 15:53 UTC. Start here when resuming in a new model or session. Keep this file current after each result, decision, failed run, or change to the work queue. Keep detailed results in the report and raw evidence in the artifact directory.

- Scope and acceptance gates: [perf-plan-2.md](perf-plan-2.md).
- Measurements, changes, and exact test revisions: [performance report](../reports/perf-round-2.md).
- Redis authority and durability model: [Redis failover contract](../reports/redis-ha-contract.md).
- Detailed chronological checkpoint and raw logs on the current host: `artifacts/perf/round-two/resume.md` and its neighboring experiment directories. Git ignores these artifacts. Preserve or copy them separately if moving hosts.
- Workspace: `/root/work/Spindle`, local `main`. Preserve the pre-existing untracked cloud plan and cloud report. No remote publication or deployment requested.

## Current state

SQLite producer grouping and retained receipt indexing landed, along with PostgreSQL direct dispatch wakes, worker claim and allocation improvements, Redis bounded inline projection, opt-in scheduler grouping, and the Store page. The report records the exact commits, suite results, and durability settings.

The PostgreSQL destination wake candidate passed 107 focused tests and improved every matched outbox pair. Median throughput was 88.71 → 96.10/s and p99 12.97 → 10.70 s. All 89,831 deliveries reconciled. Keep the candidate isolated because no arm met both P-09 targets. Its patch and qualification are under `p09-d4/`.

The authenticated Store HTTP run completed five pairs per provider. All 900 viewer polls returned diagnostics. Completion rates stayed close with and without viewers. The wide paired tail spread leaves the 5% p99 budget open. Results are in the report and `store-viewer-cost/http/results/summary.json`.

The B1 SQL proof passed mixed reply, replay, and rollback checks on a disposable PostgreSQL fixture. It has no production queue integration or timing. B2 passed seven focused tests per arm and reconciled all measured and warm-up jobs. At eight callers, the ordinal join lost throughput in all five pairs, with median throughput 3,651 → 2,955/s and median paired p99 51% higher. Leave B2 isolated. Artifacts are under `pg-b1/` and `pg-b2-membership/`.

The delayed Redis/BullMQ run restarted from pair four at 40/s in exec session 62007. Three earlier pairs were retained. The interrupted fourth pair is archived in `redis-bullmq-delayed/interrupted-pair-4/`. Its saved Runwick JSON passed identity and accounting checks, but Redis INFO was missing and the original runner exited. The resumed runner preserves diagnostic errors and continues when only INFO collection fails. Check `resume-run.log`, `run.done`, and `summary.json`.

## Remaining work

- Finish the delayed Redis/BullMQ comparison and report enqueue and due-to-handler tails alongside durable completions and refusals.
- Prototype and qualify production B1 admission, cancellation, and replay if proceeding with grouped producer dispatch. The cap increase remains a separate B2 decision.
- Keep the P-01 and P-09 gates, scheduler p99 gate, strict Sentinel and Cluster authority, and full SP-029 and quiet-host soak open until qualified. See the plan and report for the remaining implementation work and owner decisions.

## Reproducing runs

Use .NET SDK 10.0.403 with `DOTNET_ROOT=/root/.dotnet` and its directory on `PATH`. Use pinned fixture images from each runner and retain default durable settings, including Redis AOF always and SQLite WAL with synchronous FULL. The host is shared. Run timing workloads serially and verify that an existing runner has exited before restarting. Check each experiment's `run.done` exit status, logs, and reconciliation evidence. Treat wide paired spreads as inconclusive and keep experiments with unresolved correctness or regression checks isolated.
