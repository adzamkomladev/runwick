# Round two performance handoff

Updated 2026-11-13 16:25 UTC. Start here when resuming in a new model or session. Keep this file current after each result, decision, failed run, or change to the work queue. Keep detailed results in the report and raw evidence in the artifact directory.

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

The delayed Redis/BullMQ comparison completed five pairs each at 40 and 100 offers/s. Runwick settled all 21,000 offers. BullMQ settled all 20,692 accepted offers. Both rates met the throughput band. At 100/s, due-to-handler p99 was 1,719 ms versus 461 ms by arm median, with Runwick outside the latency band in all five pairs. Enqueue and resource measurements are in the report. The original interrupted fourth pair is preserved under `redis-bullmq-delayed/interrupted-pair-4/`. The final summaries are `summary.json` and `resources.json` in that experiment directory.

## Remaining work

- B1 transport finished. The corrected Npgsql proof passed, and all 29,952 jobs reconciled. At 16 callers, groups of eight had median paired throughput ratios of 1.17× at 1 KiB and 1.13× at 64 KiB. Both ranges crossed parity. Eight callers and oversized 1 MiB groups regressed. Keep the experiment isolated. If continuing B1, test byte-bounded small groups with realistic collection and cancellation before changing production admission. Reproducer, results, and the archived deadline and build failures are under `pg-b1/transport/`.
- Keep the P-01 and P-09 gates, scheduler p99 gate, strict Sentinel and Cluster authority, and full SP-029 and quiet-host soak open until qualified. See the plan and report for the remaining implementation work and owner decisions.

## Overall execution queue

| Workstream | Current step | What remains |
|---|---|---|
| SQLite (Q) | Preserve the landed grouping, receipt seek, and worker changes. The integrated process-local wake comparison is queued in session 24566 behind SP-029. Exact arms, marker, and pair logs are under `sqlite-q6-integrated/`. | Verify the wake under low traffic and saturation, and repeat sustained retained runs against the throughput and latency gates. |
| PostgreSQL (A–D) | Use the B1 proof to design byte-bounded producer admission. A retained-data C5 storage diagnostic is being prepared under `pg-c5/`. The B2 membership and D4 wake candidates remain isolated based on measured gates. | Qualify per-command cancellation, recovery, and real group sizes. Measure physical writes and retained claim cost before adopting further schema candidates. |
| Redis throughput (F) | Prepare an isolated F3 grouped dispatch proof under `redis-f3/`. The delayed comparison provides a measured latency and storage baseline. | Verify independent receipts and recovery before paired timing. Compare latency, throughput, and AOF against BullMQ under the same durability. |
| Redis topology (R) | Prepare an executable Cluster declared-key proof for a real operation or recovery segment under `redis-r3-next/`. | Qualify route changes and preserve continuity and old-primary fencing before write admission. Sentinel observer evidence is in the report. |
| Scheduler and management (S/M) | Keep the scheduler and Store page results documented with their open p99 gates. | Repeat scheduler and HTTP tails on a quiet host. Reconcile topology display with the eventual Redis authority model. |
| Qualification (H) | The integrated SP-029 short matrix started in session 41871. It covers all cases including a 10-minute soak smoke. Exact archive, pinned PostgreSQL fixture, log, and marker are under `sp029-integrated-short/`. | Check every case and record its verdict. The archived launcher omitted Git metadata, so use its saved `revision.txt`. A separate verifier in session 18429 will write `case-status.json` and `verification.done`. Commit `357a119` fixes future launcher runs to return failure if any workload exits nonzero while still writing the summary. Full SP-029 and the 24-hour reference soak require the plan's quiet host. |

The Redis proofs can be prepared while the short matrix runs. Schedule timed experiments serially on this host.

## Reproducing runs

Use .NET SDK 10.0.403 with `DOTNET_ROOT=/root/.dotnet` and its directory on `PATH`. Use pinned fixture images from each runner and retain default durable settings, including Redis AOF always and SQLite WAL with synchronous FULL. The host is shared. Run timing workloads serially and verify that an existing runner has exited before restarting. Check each experiment's `run.done` exit status, logs, and reconciliation evidence. Treat wide paired spreads as inconclusive and keep experiments with unresolved correctness or regression checks isolated.
