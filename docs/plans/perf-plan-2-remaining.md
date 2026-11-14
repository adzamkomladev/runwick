# Round two remaining implementation and qualification

Use the [round-two plan](perf-plan-2.md) for invariants and the [handoff](perf-plan-2-progress.md) for completed work. Each item below names the next implementation step and its verification.

## Start here

- [ ] **Q6/Q7 and E.** Review the working-tree SQLite wake and P-08 analysis fixes. Verify wake-after-commit, replay, and polling fallback. Reproduce the Core allocation failure on a clean revision and trace the 512 KiB and 1 MiB preparation paths. Repeat SQLite steady load at 100 offers/s with retained history, cleanup and dashboard reads, plus crash/reopen, scheduler and outbox checks. Measure writer queue age, refusals, checkpoint progress, durable completions and pickup p99.
- [ ] **PostgreSQL B1.** Wire the grouped SQL proof into opt-in producer admission with count and byte limits, a collection wait of at most 2 ms and immediate submission for a lone caller. Preserve independent receipts, caller-owned transactions, and same-ID recovery. Test cancellation before submission and after commit, and benchmark real group sizes across caller counts and payload sizes.
- [x] **PostgreSQL B2 (cap).** Done 2026-11-14: the public PostgreSQL cap is 1,000 items, bounded in bytes by the 2 MiB batch envelope, and rejected before mutation. Tests and the paired 100 versus 1,000 measurement are in the report section "PostgreSQL bulk cap of 1,000 items (B2)". Open: lock-time and WAL profiling at 100 before any SQL change, 64 KiB and mixed payloads, and documenting 100 as the recommended size.
- [ ] **PostgreSQL A/C and worker B.** Recheck the isolated attempt-close and keyed claim candidates against retained data. Use nested query plans to pick the first index change. Preserve serializer-profile equality and claim ordering. Advance set-based receipts, compaction, gate locking and row layout when measured cost warrants them and the applicable replay and migration proofs pass. Keep per-admission fairness and both lifecycle events. Revisit finish batching and combined finish/claim after measuring batch fullness and lease margins.
- [ ] **Outbox D.** Build grouped destination delivery with independent transport identities and receipts, and preserve token-guarded source confirmation. Exercise mixed outcomes, lost replies and reclaims. Qualify P-09 at four relay slots, 100 deliveries/s and ready-to-confirm p99 ≤2 s. Measure 16 slots separately.
- [ ] **Redis F.** Integrate the grouped dispatch proof with opt-in bounded client admission and per-command fault mapping. Verify cancellation, renewal latency, AOF restart and recovery. Revisit work hints, pipeline depth and intent encoding using measured script time and AOF bytes. Compare saturated, steady and delayed workloads against BullMQ under matched persistence. Target at least 90% throughput and p95/p99 enqueue and pickup latency within 10%.
- [ ] **Redis R and Store M.** Bind writes to verified authority. Start with Sentinel, then add Cluster routing, declared keys and per-node management. Pause admission during unresolved failover and resume after continuity and former-primary fencing are verified, with operator intervention as needed. Qualify independent applications across primaries. Surface verified topology in the Store page and rerun authenticated overhead checks against the 5% budget.
- [ ] **Scheduler S.** Compare four- and eight-plan grouping with the existing allowance on sustained minute-boundary bursts and catch-up. Require P-06 p99 ≤13 s with complete occurrence accounting. Use replica and evaluator timings to decide whether further contention or caching work is needed.

## Implement all five opt-in modes

Each mode needs a named configuration, documented guarantees, focused failure tests and separate benchmark results. Qualify the default profile and each optional profile independently.

| Mode | Implementation and verification |
|---|---|
| G1 Buffered finish | Bound pending finishes by count, bytes and age. Keep storage permits and lease handling through durable settlement. Test pressure, graceful drain and crashes that can rerun unsettled work. |
| G2 Reduced replay protection | Specify a shorter replay horizon and preserve historical finish identity within it. Test lost replies inside and outside the horizon, terminal jobs, retry/rerun and cleanup. Document when resending can create another job. |
| G3 Lean history | Add a per-queue profile that keeps current state and attempts while omitting per-job events. Verify dashboard and event API behavior, retention and the profile's effect on write volume. |
| G4 Redis fsync every second | Add a separately identified persistence profile and validate configuration on reconnect. Measure throughput and acknowledged-loss exposure under kill, restart and rewrite. Document the durability and failover assumptions. |
| G5 Fewer lease renewals | **Profile landed 2026-11-14** as `WorkerOptions.UseFewerLeaseRenewals()` (5 min lease, 90 s renewals; sixth of the renewal commands in a fake-time test; V rules pass). Still open: measured timeout sweep on a real store, pause and outage behavior, and crash-detection latency. |

## Qualification window

**Start on this machine at 18:00 GMT on Saturday, 14 November 2026.** Keep timing workloads idle before the window. Pin the tested revision and record actual CPU, memory, database placement, RTT, images and durability settings. The reference runbook specifies a separate PostgreSQL host. If PostgreSQL shares this machine, record the deviation and treat the results as evidence for the measured setup. [SP-029 runbook](../reports/sp-029.md#what-you-need-to-run-on-a-dedicated-quiet-host)

- [ ] Start the full SP-029 matrix at 18:00 GMT, then run the 24-hour soak at 70% of the sustainable P-02 rate measured in that run. Preserve per-case results, reconciliation and environment metadata. Record any open gates and the exact revision tested.
- [ ] After implementation, run the affected provider suites and repeat final qualification on the integrated release candidate. Include admission closure during batching and the load-sensitive ownership and outage cases. Judge P-01 at 100 offers/s against stable backlog and due-to-start p99 <2 s. Keep the PostgreSQL throughput proposals, 60 KiB/job and SQLite 3× figures as optimization targets pending a repeatable reference baseline.
