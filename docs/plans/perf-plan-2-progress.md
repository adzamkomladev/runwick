# Round two performance handoff

Updated 2026-11-14 UTC. Start here when resuming in a new model or session. Keep this file current after each result, decision, failed run, or change to the work queue. Keep detailed results in the report and raw evidence in the artifact directory.

- Scope and acceptance gates: [perf-plan-2.md](perf-plan-2.md).
- Remaining implementation and qualification: [perf-plan-2-remaining.md](perf-plan-2-remaining.md).
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

The integrated short SP-029 matrix finished on exact `88a8016`: all 14 cases exited 0 with zero correctness violations. Throughput and latency gates still miss on this shared host, so every verdict is informational. P-08 showed NO DATA only because the readiness analysis looked for a renamed counter. That is fixed in `tests/Runwick.Workloads/Analysis.cs` and compiles, and the corrected rule reproduces 6.1 s from the archived samples.

The SQLite process-local wake improved low-traffic due-to-handler p50 from 125 to 34 ms in all five pairs and left saturated throughput unchanged. Its patch is applied in the working tree with a test, and the SQLite project passes 54 of 54. The Core project now passes 439 of 439. `Prepare_allocation_is_linear_in_payload_size` failed on a clean `HEAD` (ratio 3.0 against 2.2) because it warmed the 512 KiB case but not the 1 MiB case, so it compared a cold call with a warm one. Warmed per stage, serialize is 2.22×, scan 2.0× and prepare 2.19× from 512 KiB to 1 MiB, so steady-state allocation is linear. The test now warms both sizes and passed three runs in a row. Prepare at 2.19× sits close to the 2.2 limit, so a real regression would show up first as a flaky run. On the final tree the Contract project passes 480 of 480 and the Outbox project 48 of 48 (with a Redis server on `127.0.0.1:56379`; logs in `q6-integrate/`).

C5 found zero HOT updates on `jobs` because every state change touches an indexed column, and its indexes are 129 MB against a 47 MB heap. A fill-factor change cannot help alone. R3 proved that a pending `RegisterQueue` completes across Cluster slot migration with declared keys. F3 proved independent receipts across 17 injected write boundaries for grouped Redis dispatch. Neither has timing or production wiring.

The Laravel database-queue comparison finished. On SQLite (Laravel on PHP 8.4) Runwick drains faster (195 against 125 jobs/s) and has lower latency at 100 offers/s, and the two are equal at 40. On PostgreSQL (Laravel on PHP 8.3) Laravel drains faster (213 against 174 jobs/s) and has lower tail latency at 100 offers/s. Laravel has no lease fence, so this is not like-for-like work. Laravel's SQLite queue is unreliable on PHP 8.3 because `transaction_mode` applies only from 8.4. Results and caveats are in the report section "Laravel database queue comparison", and the runs are under `laravel-queues/`.

## PostgreSQL bulk cap (2026-11-14)

The public PostgreSQL bulk cap is now 1,000 items with the 2 MiB batch envelope as the byte cap. It is a client-side per-store limit with no migration. PostgreSQL tests, the cap-aware contract tests (24 of 24 on SQLite, PostgreSQL and Redis) and a Core test pass. The paired measurement showed +14% at one caller (crossing parity) and 35% lower throughput at eight callers, so 100 stays the recommended size. Details are in the report. The bench harness gained `--batch-items` and per-call latency percentiles in bulk mode. A Redis 8.2 container with the required profile is `runwick-test-redis` on `127.0.0.1:56379` and a PostgreSQL 18 bench container is `runwick-bench-pg` on `127.0.0.1:55499`; neither survives a host reboot.

## G4 and G5 opt-in profiles (2026-11-14)

G5 landed as `WorkerOptions.UseFewerLeaseRenewals()` (5 minute lease, 90 s renewals, a sixth of the renewal commands in a fake-time test; real-store sweep, outage and crash-detection measurements are open). G4 landed as the Redis everysec persistence profile (see the report section "Redis appendfsync everysec profile (G4)"): 1.5× drain in ten of ten pairs, with SIGKILL survival tested and OS-crash loss not simulated. G1, G2 and G3 are not started: they change finish buffering, the replay horizon and per-job events across all three stores and need their own designs and migrations.

## Host reboot recovery (completed)

The host rebooted about 21:05 local on 2026-11-13 and killed every queued session. Recovery used root scripts instead of agent sessions: `sp029-integrated-short/resume.sh` finished P-08a to P-10, and `chain.sh` then ran Q6, C5, R3 and F3. R3 and F3 first failed with exit 127 because `rg` was missing from `PATH` after the reboot. Their outputs are kept under `failed-no-rg/`, a symlink `/usr/local/bin/rg` was added, and `chain3.sh` reran them successfully. `chain2.sh` ran the Laravel comparison, and `laravel-queues/sqlite84.sh` reran its SQLite half. All markers are written. Check `pgrep -f "chain|laravel-queues|suites.sh"` before starting anything new, and confirm `rg --version` works after any future reboot. The Contract and Outbox suites also need a Redis server with AOF always on `127.0.0.1:56379` (`RUNWICK_TEST_REDIS` overrides it), which does not survive a reboot.

## Owner decisions adopted

- Keep the P-01, P-06 and P-09 gates and the 5% Store overhead budget. Use the PostgreSQL throughput proposals, 60 KiB/job allocation target and 3× SQLite drain goal as optimization targets pending a repeatable reference baseline. Use the proposed BullMQ acceptance band of at least 90% throughput and at most 10% worse p95/p99 enqueue and pickup latency. Parity remains the goal.
- Keep four relay slots for P-09 acceptance at 100 deliveries/s and p99 at or below 2 s. Measure 16 slots separately.
- Prototype opt-in producer grouping for PostgreSQL and Redis with at most 2 ms of collection wait, immediate submission for a lone caller, independent receipts and bounded count and bytes. Set count and byte defaults from paired measurements.
- Target a public PostgreSQL bulk cap of 1,000 jobs with a byte cap. Keep per-admission fairness and both lifecycle events. Implement all five work area G modes as opt-in profiles.
- For the first Sentinel and Cluster profiles, pause write admission during unresolved failover. Resume when continuity is verified and the former primary is fenced, with operator intervention if needed. Qualify distribution across independent applications first.
- Use this machine for the full qualification and soak starting at 18:00 GMT on 2026-11-14. Record the actual database placement and host characteristics with the results.

## Remaining work

- Laravel follow-ups, only if wanted: rerun the PostgreSQL arm on PHP 8.4 for one runtime across both databases (the stock image lacks `pdo_pgsql`, so it needs a small derived image), and rerun the SQLite Runwick arm with the wake patch. The harness is `eng/bench-compare/laravel/` and is also wired into `shootout.py` as `laravel-sqlite` and `laravel-pg`.
- Commit the working-tree changes after review: the SQLite wake patch and test, the readiness analysis fix, the Laravel harness, `shootout.py` configs, the report and this file. Nothing is committed yet.
- Done 2026-11-14: the Core allocation test failure was a missing warm-up in the test, not a product regression (see Current state).
- B1 transport finished. The corrected Npgsql proof passed, and all 29,952 jobs reconciled. At 16 callers, groups of eight had median paired throughput ratios of 1.17× at 1 KiB and 1.13× at 64 KiB. Both ranges crossed parity. Eight callers and oversized 1 MiB groups regressed. Keep the experiment isolated. If continuing B1, test byte-bounded small groups with realistic collection and cancellation before changing production admission. Reproducer, results, and the archived deadline and build failures are under `pg-b1/transport/`.
- Keep the P-01 and P-09 gates, scheduler p99 gate, strict Sentinel and Cluster authority, and full SP-029 and quiet-host soak open until qualified. See the plan and report for the remaining implementation work and owner decisions.

## Overall execution queue

| Workstream | Current step | What remains |
|---|---|---|
| SQLite (Q) | Grouping, receipt seek and worker changes landed. The process-local wake is applied in the working tree after five pairs (low-traffic p50 125 → 34 ms, saturation unchanged), with SQLite 54/54 and Core 439/439. Evidence is in `sqlite-q6-integrated/` and `q6-integrate/`. | Commit after review. Repeat sustained retained runs against the throughput and latency gates, and rerun the Laravel SQLite comparison with the wake build. |
| PostgreSQL (A–D) | The B1 proof is the base for byte-bounded producer admission. The C5 retained-data diagnostic finished: zero HOT updates on `jobs`, indexes 2.7× the heap, autovacuum keeping dead tuples near 12% or less, on a fixture with `full_page_writes` off (see `pg-c5/`). B2 membership and D4 wake stay isolated on measured gates. | Decide on narrowing `jobs` indexes, which needs query-plan and correctness checks. Qualify per-command cancellation, recovery and real group sizes. Measure retained claim cost before more schema candidates. |
| Redis throughput (F) | The F3 grouped dispatch proof passed: 17 injected write boundaries, 39 jobs, independent receipts and events (`redis-f3/`). It is synthetic and untimed, and not wired into the client. The delayed BullMQ comparison remains the latency and storage baseline. | Wire grouped dispatch into the client with admission, cancellation and fault mapping, then run paired timing against BullMQ under the same durability. |
| Redis topology (R) | The Cluster declared-key proof passed: a pending `RegisterQueue` survived TRYAGAIN, ASK, NOSCRIPT and MOVED and completed on replay (`redis-r3-next/run-9ZZrdvFI/`). | Qualify route changes and preserve continuity and old-primary fencing before write admission. Sentinel observer evidence is in the report. |
| Scheduler and management (S/M) | Keep the scheduler and Store page results documented with their open p99 gates. | Repeat scheduler and HTTP tails on a quiet host. Reconcile topology display with the eventual Redis authority model. |
| Qualification (H) | The integrated SP-029 short matrix finished: 14 of 14 cases exit 0, zero violations (`sp029-integrated-short/case-status.json`, `verification.done`). Gates are informational on this host. | Repeat on the quiet reference host for verdicts, and run the 24-hour soak. |

The queued experiments have all finished: SP-029, SQLite wake pairs, PostgreSQL retained observation, Redis Cluster recovery proof, Redis grouped dispatch proof, and the Laravel comparison. Each wrote a completion marker. Verify the exit status and inspect the logs before promoting any candidate.

## Plan item index

| Items | State and next evidence |
|---|---|
| Q0–Q5 | Cost baseline, durable producer grouping, claim and finish changes, writer/reader experiments, and receipt indexing are recorded. The 50,000-retained sustained run missed both gates. |
| Q6–Q7 | Wake pairs finished and the patch is applied in the working tree. SQLite project passed on the final tree. Contract 480/480 and Outbox 48/48 passed on the final tree (`q6-integrate/`). |
| A1–A3, A7 | Landed. Recheck on the integrated workload matrix. |
| A4–A6 | Attempt-close and allocator experiments are isolated. Set-based receipt creation needs a design that preserves replay, cross-class checks, and fresh decision time. |
| B1–B2 | Explicit group transport and ordinal join were measured. The 1,000-item PostgreSQL cap landed 2026-11-14 (client-side, 2 MiB envelope as the byte cap). At 8 callers it lost 35% throughput against 100-item calls, so 100 stays the recommended size. Byte-bounded producer grouping (B1 wiring) remains. |
| B3–B7 | Single-lane refill landed. SQLite finish grouping stayed isolated. Multi-lane fairness and combined finish/claim need separate ownership and scheduling proofs before any default change. |
| C1–C8 | Claim index candidate stayed isolated. C5 measured: no HOT updates on `jobs`, so storage settings alone will not help and index narrowing is the open choice. Receipt compaction needs a replay proof. Gate locking and row-layout changes remain open. Event shape requires an explicit decision. |
| D1–D4 | Transport identity in claim landed. Mark batching, slot sweep, and destination wake measured. True grouped destination delivery and P-09 qualification remain open. |
| E1–E5 | Decoder, fingerprint, settlement reply, dispatch argument, and attempt allocation improvements landed. Integrated allocation gate still needs qualification. |
| F1–F6 | Wake and encoding candidates remain isolated. Producer depth measured. F3 correctness proof passed, untimed and not wired into the client. Combined finish/claim and additional sockets remain conditional on bottleneck evidence. |
| R0–R5 | Authority contract and Sentinel observer tested. Cluster key audit and synthetic migration proved the need for declared keys. Real recovery migration proof passed with declared keys. Strict write authority and topology integration remain open. |
| G, N | Optional durability and event tradeoffs require owner decisions. .NET 11 migration deferred by owner. |
| S0–S6 | Scheduler attribution, productive scans, and durable idle time landed. Grouping remains opt-in. Eight-plan prototype missed one long-run p99 gate. Replica contention and definition caching stay evidence-driven. |
| M1–M4 | Store page, bounded collectors, and history landed. HTTP viewer costs measured on three providers. Tail and topology gates remain open. |
| H1–H4 | Focused and full suite evidence is in the report. Integrated short matrix finished with no correctness violations. Full reference qualification remains open. |

## Scope and completion checks

The source of truth is `perf-plan-2.md`. This file tracks execution. The plan is complete when each required item has verified evidence and the acceptance gates are closed. Record conditional experiments and owner decisions explicitly. A successful process exit, a short smoke run, or a prototype proof alone does not close a release gate.

Phase 0 housekeeping is complete. The plan and round-one status table were committed in `49c1b7c`, and the shoot-out branch is an ancestor of main. The report contains the phase 0 claim/pressure counters, notification waits, physical writes, Redis script/recovery traces, relay phases, allocations, finish batch counts, nested SQL and scheduler attribution. Phase timings are from specific fixtures. The full matrix of retained-data, contention and payload cases remains part of qualification.

The integrated short SP-029 matrix finished: all 14 cases exited 0 with zero correctness violations and zero run failures. Throughput and latency gates still miss on this shared single-worker host, so every verdict is informational. P-08's summary row reads NO DATA only because the analysis needs a reserve sample after readiness. By hand the worker was ready 6.1 s after restoration. See the report section "Integrated short SP-029 matrix".

The adopted decisions above resolve the scope choices in §11. Producer count and byte defaults and the Redis authority mechanism still require implementation evidence. The quiet reference host remains required for final qualification.

## Reproducing runs

Use .NET SDK 10.0.403 with `DOTNET_ROOT=/root/.dotnet` and its directory on `PATH`. Use pinned fixture images from each runner and retain default durable settings, including Redis AOF always and SQLite WAL with synchronous FULL. The host is shared. Run timing workloads serially and verify that an existing runner has exited before restarting. Check each experiment's `run.done` exit status, logs, and reconciliation evidence. Treat wide paired spreads as inconclusive and keep experiments with unresolved correctness or regression checks isolated.
